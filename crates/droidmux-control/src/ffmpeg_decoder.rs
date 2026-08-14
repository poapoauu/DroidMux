use std::{cell::RefCell, collections::VecDeque, fmt, sync::OnceLock};

use ffmpeg_next::{
    Error as FfmpegError, Packet, codec,
    error::EAGAIN,
    format,
    frame::Video,
    software::scaling::{context::Context as ScalingContext, flag::Flags},
};

use crate::{
    DecodedPixelFormat, DecodedVideoFrame, MirrorDecoder, MirrorError, VideoCodec,
    VideoFrameMetadata,
    decoder::{MAX_DECODED_FRAME_BYTES, PixelBufferPool, RETAINED_PIXEL_BUFFERS},
};

static FFMPEG_INITIALIZATION: OnceLock<Result<(), String>> = OnceLock::new();

thread_local! {
    // Each mirror session owns one dedicated decoder thread. Keeping swscale
    // here reuses its conversion tables without moving its raw context pointer
    // across threads or weakening MirrorDecoder's Send boundary.
    static SCALER_CACHE: RefCell<Option<CachedScaler>> = const { RefCell::new(None) };
}

struct CachedScaler {
    context: ScalingContext,
    input_format: format::Pixel,
    output_format: DecodedPixelFormat,
    width: u32,
    height: u32,
}

/// FFmpeg-backed software decoder for H.264, H.265, and AV1 mirror streams.
///
/// This backend is available only with the `decoder-ffmpeg` feature. It links
/// to `FFmpeg` libraries selected at build time and never starts `ffmpeg.exe`.
pub struct FfmpegDecoder {
    video_codec: VideoCodec,
    decoder: codec::decoder::Video,
    decoded: Video,
    converted: Video,
    output_format: Option<DecodedPixelFormat>,
    pending_config: Vec<u8>,
    pending_metadata: VecDeque<VideoFrameMetadata>,
    buffers: PixelBufferPool,
}

impl FfmpegDecoder {
    /// Creates an `FFmpeg` H.264 software decoder.
    ///
    /// # Errors
    ///
    /// Returns an error when `FFmpeg` cannot initialize or does not expose H.264.
    pub fn new() -> Result<Self, MirrorError> {
        Self::for_codec(VideoCodec::H264)
    }

    /// Creates an `FFmpeg` decoder for the requested mirror video codec.
    ///
    /// # Errors
    ///
    /// Returns an error when `FFmpeg` cannot initialize or does not expose the
    /// requested decoder.
    pub fn for_codec(video_codec: VideoCodec) -> Result<Self, MirrorError> {
        FFMPEG_INITIALIZATION
            .get_or_init(|| ffmpeg_next::init().map_err(|error| error.to_string()))
            .as_ref()
            .map_err(|error| {
                MirrorError::Decode(format!("FFmpeg initialization failed: {error}"))
            })?;
        let codec_id = match video_codec {
            VideoCodec::H264 => codec::Id::H264,
            VideoCodec::H265 => codec::Id::HEVC,
            VideoCodec::Av1 => codec::Id::AV1,
        };
        let codec_name = video_codec.scrcpy_name();
        let codec = codec::decoder::find(codec_id).ok_or_else(|| {
            MirrorError::Decode(format!("FFmpeg does not provide a {codec_name} decoder"))
        })?;
        let decoder = codec::Context::new_with_codec(codec)
            .decoder()
            .video()
            .map_err(|error| {
                MirrorError::Decode(format!("FFmpeg {codec_name} decoder setup failed: {error}"))
            })?;
        Ok(Self {
            video_codec,
            decoder,
            decoded: Video::empty(),
            converted: Video::empty(),
            output_format: None,
            pending_config: Vec::new(),
            pending_metadata: VecDeque::new(),
            buffers: PixelBufferPool::new(RETAINED_PIXEL_BUFFERS),
        })
    }

    fn receive_frames(
        &mut self,
        output: Option<DecodedPixelFormat>,
        emit: &mut dyn FnMut(DecodedVideoFrame),
    ) -> Result<(), MirrorError> {
        loop {
            match self.decoder.receive_frame(&mut self.decoded) {
                Ok(()) => {}
                Err(FfmpegError::Other { errno }) if errno == EAGAIN => break,
                Err(FfmpegError::Eof) => break,
                Err(error) => {
                    return Err(MirrorError::Decode(format!(
                        "FFmpeg frame receive failed: {error}"
                    )));
                }
            }
            let decoded_pts = self.decoded.pts();
            let metadata_index = decoded_pts.and_then(|pts| {
                self.pending_metadata.iter().position(|metadata| {
                    i64::try_from(metadata.presentation_time_us).ok() == Some(pts)
                })
            });
            let metadata = metadata_index
                .and_then(|index| self.pending_metadata.remove(index))
                .or_else(|| self.pending_metadata.pop_front())
                .ok_or_else(|| {
                    MirrorError::Decode("FFmpeg emitted a frame without queued metadata".to_owned())
                })?;
            let Some(format) = output else {
                continue;
            };
            convert_frame(&self.decoded, &mut self.converted, format)?;
            let width = self.converted.width();
            let height = self.converted.height();
            let row_bytes = usize::try_from(width)
                .ok()
                .and_then(|width| width.checked_mul(format.bytes_per_pixel()))
                .ok_or_else(|| MirrorError::Frame("decoded row size overflowed".to_owned()))?;
            let output_len = usize::try_from(height)
                .ok()
                .and_then(|height| row_bytes.checked_mul(height))
                .ok_or_else(|| {
                    MirrorError::Frame("decoded dimensions overflow memory size".to_owned())
                })?;
            if output_len > MAX_DECODED_FRAME_BYTES {
                return Err(MirrorError::Frame(format!(
                    "decoded frame requires {output_len} bytes"
                )));
            }
            let source = self.converted.data(0);
            let stride = self.converted.stride(0);
            let height = usize::try_from(height)
                .map_err(|_| MirrorError::Frame("decoded height exceeds usize".to_owned()))?;
            if stride < row_bytes || source.len() < stride.saturating_mul(height) {
                return Err(MirrorError::Frame(
                    "FFmpeg returned an invalid converted frame layout".to_owned(),
                ));
            }
            let mut pixels = self.buffers.acquire(output_len);
            for (row, destination) in pixels.chunks_exact_mut(row_bytes).enumerate() {
                let offset = row.checked_mul(stride).ok_or_else(|| {
                    MirrorError::Frame("decoded row offset overflowed".to_owned())
                })?;
                destination.copy_from_slice(&source[offset..offset + row_bytes]);
            }
            emit(DecodedVideoFrame {
                pixels: self.buffers.freeze(pixels),
                width,
                height: u32::try_from(height)
                    .map_err(|_| MirrorError::Frame("decoded height exceeds u32".to_owned()))?,
                format,
                metadata,
            });
        }
        Ok(())
    }
}

fn convert_frame(
    decoded: &Video,
    converted: &mut Video,
    output: DecodedPixelFormat,
) -> Result<(), MirrorError> {
    let width = decoded.width();
    let height = decoded.height();
    let input_format = decoded.format();
    SCALER_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let needs_rebuild = cache.as_ref().is_none_or(|scaler| {
            scaler.input_format != input_format
                || scaler.output_format != output
                || scaler.width != width
                || scaler.height != height
        });
        if needs_rebuild {
            let context = ScalingContext::get(
                input_format,
                width,
                height,
                pixel_format(output),
                width,
                height,
                Flags::BILINEAR,
            )
            .map_err(|error| {
                MirrorError::Frame(format!("FFmpeg pixel converter setup failed: {error}"))
            })?;
            *cache = Some(CachedScaler {
                context,
                input_format,
                output_format: output,
                width,
                height,
            });
        }
        cache
            .as_mut()
            .expect("FFmpeg scaler cache must exist after configuration")
            .context
            .run(decoded, converted)
            .map_err(|error| MirrorError::Frame(format!("FFmpeg pixel conversion failed: {error}")))
    })
}

impl fmt::Debug for FfmpegDecoder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FfmpegDecoder")
            .finish_non_exhaustive()
    }
}

impl MirrorDecoder for FfmpegDecoder {
    fn backend_name(&self) -> &'static str {
        "ffmpeg"
    }

    fn video_codec(&self) -> VideoCodec {
        self.video_codec
    }

    fn decode(
        &mut self,
        access_unit: &[u8],
        metadata: Option<VideoFrameMetadata>,
        output: Option<DecodedPixelFormat>,
        emit: &mut dyn FnMut(DecodedVideoFrame),
    ) -> Result<(), MirrorError> {
        let Some(metadata) = metadata else {
            if self.video_codec == VideoCodec::Av1 {
                self.decoder
                    .send_packet(&Packet::copy(access_unit))
                    .map_err(|error| {
                        MirrorError::Decode(format!(
                            "FFmpeg AV1 configuration packet submission failed: {error}"
                        ))
                    })?;
                return self.receive_frames(None, emit);
            }
            self.pending_config.clear();
            self.pending_config.extend_from_slice(access_unit);
            return Ok(());
        };
        self.output_format = output;
        let packet_data = if self.pending_config.is_empty() {
            access_unit.to_vec()
        } else {
            let mut combined =
                Vec::with_capacity(self.pending_config.len().saturating_add(access_unit.len()));
            combined.extend_from_slice(&self.pending_config);
            combined.extend_from_slice(access_unit);
            combined
        };
        let mut packet = Packet::copy(&packet_data);
        let timestamp = i64::try_from(metadata.presentation_time_us).ok();
        packet.set_pts(timestamp);
        self.decoder.send_packet(&packet).map_err(|error| {
            let prefix = packet_data
                .iter()
                .take(16)
                .map(|byte| format!("{byte:02x}"))
                .collect::<Vec<_>>()
                .join(" ");
            MirrorError::Decode(format!(
                "FFmpeg frame packet submission failed: {error}; length={}; prefix={prefix}",
                packet_data.len()
            ))
        })?;
        self.pending_config.clear();
        self.pending_metadata.push_back(metadata);
        self.receive_frames(output, emit)
    }

    fn flush(&mut self, emit: &mut dyn FnMut(DecodedVideoFrame)) -> Result<(), MirrorError> {
        self.decoder.send_eof().map_err(|error| {
            MirrorError::Decode(format!("FFmpeg decoder flush failed: {error}"))
        })?;
        self.receive_frames(self.output_format, emit)
    }
}

fn pixel_format(format: DecodedPixelFormat) -> format::Pixel {
    match format {
        DecodedPixelFormat::Rgb24 => format::Pixel::RGB24,
        DecodedPixelFormat::Rgba8888 => format::Pixel::RGBA,
        DecodedPixelFormat::Bgra8888 => format::Pixel::BGRA,
    }
}
