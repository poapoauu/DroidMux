use std::{
    sync::atomic::{AtomicBool, AtomicU32, Ordering},
    time::Duration,
};

use adb_client::{AdbClient, AdbStream};
use adb_shell::{ShellOptions, ShellSession, open_shell};
use adb_sync::{TransferOptions, push_bytes_with_options};
use bytes::Bytes;
use image::{ExtendedColorType, codecs::jpeg::JpegEncoder};
use tokio::{task::spawn_blocking, time::sleep};

#[cfg(feature = "decoder-openh264")]
use crate::OpenH264Decoder;
use crate::{
    DecodedPixelFormat, MirrorDecoder, MirrorError, MirrorPipelineStats, VideoCodec,
    pipeline::{VideoPipeline, VideoStreamReader},
    protocol::{TouchAction, encode_key, encode_touch},
};

const SCRCPY_VERSION: &str = "4.1";
const DEVICE_SERVER_PATH: &str = "/data/local/tmp/droidmux-scrcpy-server-v4.1.jar";
const SERVER_BYTES: &[u8] = include_bytes!("../resources/scrcpy-server-v4.1");
const SOCKET_ATTEMPTS: usize = 60;
const SOCKET_RETRY_DELAY: Duration = Duration::from_millis(50);
static NEXT_SCID: AtomicU32 = AtomicU32::new(0x0d10_0001);

/// Android key codes exposed by the first remote-control milestone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AndroidKey {
    /// Android Back.
    Back = 4,
    /// Android Home.
    Home = 3,
    /// Android recent-apps switcher.
    AppSwitch = 187,
    /// Android power button.
    Power = 26,
    /// Increase media volume.
    VolumeUp = 24,
    /// Decrease media volume.
    VolumeDown = 25,
}

/// Resource and encoder settings for one mirror session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MirrorOptions {
    /// Android logical display identifier to capture and control.
    pub display_id: u32,
    /// Video codec requested from the Android encoder.
    pub video_codec: VideoCodec,
    /// Maximum encoded frame dimension. Zero preserves the native display size.
    pub max_size: u16,
    /// Maximum frames requested from the Android encoder each second.
    pub max_fps: u8,
    /// Requested encoded video bit rate.
    pub video_bit_rate: u32,
    /// JPEG quality used by callers of the compatibility preview API.
    pub preview_quality: u8,
}

impl Default for MirrorOptions {
    fn default() -> Self {
        Self {
            display_id: 0,
            video_codec: VideoCodec::H264,
            max_size: 1280,
            max_fps: 15,
            video_bit_rate: 4_000_000,
            preview_quality: 78,
        }
    }
}

impl MirrorOptions {
    fn validate(self) -> Result<Self, MirrorError> {
        if self.display_id > i32::MAX as u32 {
            return Err(MirrorError::InvalidInput(
                "display_id must be between 0 and 2147483647".to_owned(),
            ));
        }
        if self.max_size != 0 && !(256..=4096).contains(&self.max_size) {
            return Err(MirrorError::InvalidInput(
                "max_size must be zero or between 256 and 4096".to_owned(),
            ));
        }
        if !(1..=120).contains(&self.max_fps) {
            return Err(MirrorError::InvalidInput(
                "max_fps must be between 1 and 120".to_owned(),
            ));
        }
        if !(100_000..=100_000_000).contains(&self.video_bit_rate) {
            return Err(MirrorError::InvalidInput(
                "video_bit_rate must be between 100000 and 100000000".to_owned(),
            ));
        }
        if !(1..=100).contains(&self.preview_quality) {
            return Err(MirrorError::InvalidInput(
                "preview_quality must be between 1 and 100".to_owned(),
            ));
        }
        Ok(self)
    }
}

/// One decoded frame ready to render in the desktop webview.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorFrame {
    /// JPEG-encoded RGB pixels.
    pub jpeg: Bytes,
    /// Decoded frame width.
    pub width: u32,
    /// Decoded frame height.
    pub height: u32,
    /// Device encoder presentation timestamp in microseconds.
    pub presentation_time_us: u64,
    /// Whether the source packet was a key frame.
    pub key_frame: bool,
}

/// One decoded RGBA frame ready for a native texture or other zero-copy UI
/// bridge.
///
/// Unlike [`MirrorFrame`], this representation avoids JPEG encoding and keeps
/// the decoded pixels in a form suitable for high-frequency presentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorPixelFrame {
    /// Pixels in RGBA8888 order, tightly packed by row.
    pub rgba: Bytes,
    /// Decoded frame width.
    pub width: u32,
    /// Decoded frame height.
    pub height: u32,
    /// Device encoder presentation timestamp in microseconds.
    pub presentation_time_us: u64,
    /// Whether the source packet was a key frame.
    pub key_frame: bool,
}

/// One decoded CPU pixel buffer in a caller-selected layout.
///
/// Use [`MirrorSession::next_pixel_frame_with_format`] when a native renderer
/// can consume a layout other than RGBA and should avoid a bridge-side copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorPixelBuffer {
    /// Tightly packed pixels in [`Self::format`].
    pub pixels: Bytes,
    /// Decoded frame width.
    pub width: u32,
    /// Decoded frame height.
    pub height: u32,
    /// Pixel layout used by [`Self::pixels`].
    pub format: DecodedPixelFormat,
    /// Device encoder presentation timestamp in microseconds.
    pub presentation_time_us: u64,
    /// Whether the source packet was a key frame.
    pub key_frame: bool,
}

/// A running embedded scrcpy-server video and control session.
pub struct MirrorSession {
    device_name: String,
    pipeline: VideoPipeline,
    video_stream: AdbStream,
    control_stream: AdbStream,
    server: ShellSession,
    preview_quality: u8,
    touch_move_in_flight: AtomicBool,
    stopped: AtomicBool,
}

impl MirrorSession {
    /// Starts the embedded Android server and opens native ADB video/control streams.
    ///
    /// # Errors
    ///
    /// Returns an error when options are invalid, upload or launch fails, the
    /// local abstract sockets do not become ready, or the video header is invalid.
    #[cfg(feature = "decoder-openh264")]
    pub async fn start(client: &AdbClient, options: MirrorOptions) -> Result<Self, MirrorError> {
        let decoder = Box::new(OpenH264Decoder::new()?);
        Self::start_with_decoder(client, options, decoder).await
    }

    /// Starts a mirror session with a caller-provided video decoder backend.
    ///
    /// This entry point lets applications use another software decoder or a
    /// platform-specific backend without changing the ADB and scrcpy protocol
    /// implementation.
    ///
    /// # Errors
    ///
    /// Returns an error when options are invalid, upload or launch fails, the
    /// local abstract sockets do not become ready, the video header is invalid,
    /// or the decoder worker cannot start.
    pub async fn start_with_decoder(
        client: &AdbClient,
        options: MirrorOptions,
        decoder: Box<dyn MirrorDecoder>,
    ) -> Result<Self, MirrorError> {
        let options = options.validate()?;
        if decoder.video_codec() != options.video_codec {
            return Err(MirrorError::InvalidInput(format!(
                "decoder {} accepts {}, but mirror options request {}",
                decoder.backend_name(),
                decoder.video_codec().scrcpy_name(),
                options.video_codec.scrcpy_name()
            )));
        }
        let transfer_options = TransferOptions {
            file_mode: 0o644,
            ..TransferOptions::default()
        };
        push_bytes_with_options(
            client,
            SERVER_BYTES,
            DEVICE_SERVER_PATH,
            &transfer_options,
            |_| {},
        )
        .await?;

        let scid = next_scid();
        let command = server_command(scid, options);
        let server = open_shell(client, &command, ShellOptions::default()).await?;
        let socket_name = format!("localabstract:scrcpy_{scid:08x}");

        let video_stream = match open_server_socket(client, &socket_name).await {
            Ok(stream) => stream,
            Err(error) => {
                let _ = server.cancel().await;
                return Err(error);
            }
        };
        let control_stream = match open_server_socket(client, &socket_name).await {
            Ok(stream) => stream,
            Err(error) => {
                let _ = video_stream.close().await;
                let _ = server.cancel().await;
                return Err(error);
            }
        };

        let video = match VideoStreamReader::open(video_stream.clone(), options.video_codec).await {
            Ok(reader) => reader,
            Err(error) => {
                let _ = control_stream.close().await;
                let _ = video_stream.close().await;
                let _ = server.cancel().await;
                return Err(error);
            }
        };
        let device_name = video.device_name().to_owned();
        let pipeline = match VideoPipeline::start(video, decoder) {
            Ok(pipeline) => pipeline,
            Err(error) => {
                let _ = control_stream.close().await;
                let _ = video_stream.close().await;
                let _ = server.cancel().await;
                return Err(error);
            }
        };

        Ok(Self {
            device_name,
            pipeline,
            video_stream,
            control_stream,
            server,
            preview_quality: options.preview_quality,
            touch_move_in_flight: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        })
    }

    /// Returns the UTF-8 device name reported by the Android server.
    #[must_use]
    pub fn device_name(&self) -> &str {
        &self.device_name
    }

    /// Waits for and decodes the next available video frame.
    ///
    /// Session metadata and codec-configuration packets are consumed internally.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream closes, framing is invalid, video
    /// decoding fails, or JPEG conversion fails.
    pub async fn next_frame(&self) -> Result<MirrorFrame, MirrorError> {
        self.next_preview_frame(Duration::ZERO).await
    }

    /// Waits for the next decoded frame that is due for presentation.
    ///
    /// Every encoded packet is submitted to the long-lived decoder worker to
    /// preserve codec state. CPU pixel output is produced while a consumer is
    /// waiting, and JPEG encoding is limited by `minimum_interval`. The worker
    /// keeps codec and conversion work off the async executor.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream closes, the decode worker fails, or
    /// frame conversion cannot complete.
    pub async fn next_preview_frame(
        &self,
        minimum_interval: Duration,
    ) -> Result<MirrorFrame, MirrorError> {
        let frame = self.next_pixel_frame(minimum_interval).await?;
        encode_preview(frame, self.preview_quality).await
    }

    /// Waits for the next decoded RGBA frame that is due for presentation.
    ///
    /// Every packet is still submitted to the decoder so inter-frame codec
    /// state remains valid. Pixel conversion runs on the long-lived decoder
    /// worker and does not perform JPEG encoding.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream closes, decoding fails, or the decoded
    /// dimensions cannot fit in memory.
    pub async fn next_pixel_frame(
        &self,
        minimum_interval: Duration,
    ) -> Result<MirrorPixelFrame, MirrorError> {
        let frame = self
            .next_pixel_frame_with_format(minimum_interval, DecodedPixelFormat::Rgba8888)
            .await?;
        Ok(MirrorPixelFrame {
            rgba: frame.pixels,
            width: frame.width,
            height: frame.height,
            presentation_time_us: frame.presentation_time_us,
            key_frame: frame.key_frame,
        })
    }

    /// Waits for the next decoded frame in the requested pixel layout.
    ///
    /// Every packet is still submitted to the decoder so inter-frame codec
    /// state remains valid. The selected layout is produced on the decoder
    /// worker and can be passed directly to a matching native texture backend.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream closes, decoding fails, the decoder
    /// returns another layout, or the decoded dimensions cannot fit in memory.
    pub async fn next_pixel_frame_with_format(
        &self,
        minimum_interval: Duration,
        format: DecodedPixelFormat,
    ) -> Result<MirrorPixelBuffer, MirrorError> {
        self.ensure_running()?;
        self.pipeline
            .next_pixel_frame(minimum_interval, format)
            .await
    }

    /// Returns the decoder backend selected for this session.
    #[must_use]
    pub fn decoder_backend_name(&self) -> &'static str {
        self.pipeline.backend_name()
    }

    /// Returns cumulative native video-pipeline counters.
    #[must_use]
    pub fn pipeline_stats(&self) -> MirrorPipelineStats {
        self.pipeline.stats()
    }

    /// Injects one finger event in encoded-video coordinates.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid coordinates or a closed control stream.
    pub async fn touch(
        &self,
        action: MirrorTouchAction,
        x: u32,
        y: u32,
        width: u32,
        height: u32,
    ) -> Result<(), MirrorError> {
        self.ensure_running()?;
        let reserved_move = action == MirrorTouchAction::Move;
        let action = match action {
            MirrorTouchAction::Down => TouchAction::Down,
            MirrorTouchAction::Move => TouchAction::Move,
            MirrorTouchAction::Up => TouchAction::Up,
        };
        let payload = encode_touch(action, x, y, width, height)?;
        if reserved_move && self.touch_move_in_flight.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let result = self.control_stream.write(payload).await;
        if reserved_move {
            self.touch_move_in_flight.store(false, Ordering::Release);
        }
        result.map_err(MirrorError::from)
    }

    /// Injects one complete Android key press.
    ///
    /// # Errors
    ///
    /// Returns an error when the mirror is stopped or the control stream fails.
    pub async fn press_key(&self, key: AndroidKey) -> Result<(), MirrorError> {
        self.ensure_running()?;
        let keycode = key as u32;
        self.control_stream.write(encode_key(0, keycode)).await?;
        self.control_stream.write(encode_key(1, keycode)).await?;
        Ok(())
    }

    /// Stops the video, control, and Android server streams.
    ///
    /// Repeated calls are harmless.
    ///
    /// # Errors
    ///
    /// Returns the first stream shutdown error.
    pub async fn stop(&self) -> Result<(), MirrorError> {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let video_result = self.video_stream.close().await.map_err(MirrorError::from);
        let pipeline_result = self.pipeline.stop().await;
        let control_result = self.control_stream.close().await.map_err(MirrorError::from);
        let server_result = self.server.cancel().await.map_err(MirrorError::from);
        video_result?;
        pipeline_result?;
        control_result?;
        server_result?;
        Ok(())
    }

    /// Returns whether stop has been requested.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    fn ensure_running(&self) -> Result<(), MirrorError> {
        if self.is_stopped() {
            Err(MirrorError::Stopped)
        } else {
            Ok(())
        }
    }
}

async fn encode_preview(
    frame: MirrorPixelFrame,
    preview_quality: u8,
) -> Result<MirrorFrame, MirrorError> {
    spawn_blocking(move || {
        let width = usize::try_from(frame.width)
            .map_err(|_| MirrorError::Frame("decoded width exceeds usize".to_owned()))?;
        let height = usize::try_from(frame.height)
            .map_err(|_| MirrorError::Frame("decoded height exceeds usize".to_owned()))?;
        let expected_rgba_bytes = width
            .checked_mul(height)
            .and_then(|pixels| pixels.checked_mul(4))
            .ok_or_else(|| {
                MirrorError::Frame("decoded dimensions overflow memory size".to_owned())
            })?;
        if frame.rgba.len() != expected_rgba_bytes {
            return Err(MirrorError::Frame(format!(
                "decoded RGBA frame contains {} bytes, expected {expected_rgba_bytes}",
                frame.rgba.len()
            )));
        }
        let rgb_capacity = width
            .checked_mul(height)
            .and_then(|pixels| pixels.checked_mul(3))
            .ok_or_else(|| {
                MirrorError::Frame("decoded dimensions overflow memory size".to_owned())
            })?;
        let mut rgb = Vec::with_capacity(rgb_capacity);
        for pixel in frame.rgba.chunks_exact(4) {
            rgb.extend_from_slice(&pixel[..3]);
        }
        let mut jpeg = Vec::with_capacity(rgb_capacity / 8);
        JpegEncoder::new_with_quality(&mut jpeg, preview_quality)
            .encode(&rgb, frame.width, frame.height, ExtendedColorType::Rgb8)
            .map_err(|error| MirrorError::Frame(error.to_string()))?;
        Ok(MirrorFrame {
            jpeg: Bytes::from(jpeg),
            width: frame.width,
            height: frame.height,
            presentation_time_us: frame.presentation_time_us,
            key_frame: frame.key_frame,
        })
    })
    .await
    .map_err(|error| MirrorError::Frame(format!("preview encode worker failed: {error}")))?
}

/// Finger lifecycle action sent to Android.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorTouchAction {
    /// Begin a touch gesture.
    Down,
    /// Continue a touch gesture.
    Move,
    /// Finish a touch gesture.
    Up,
}

fn next_scid() -> u32 {
    NEXT_SCID.fetch_add(1, Ordering::Relaxed) & 0x7fff_ffff
}

fn server_command(scid: u32, options: MirrorOptions) -> String {
    format!(
        "CLASSPATH={DEVICE_SERVER_PATH} app_process / com.genymobile.scrcpy.Server {SCRCPY_VERSION} scid={scid:08x} log_level=warn audio=false video_codec={} display_id={} max_size={} max_fps={} video_bit_rate={} tunnel_forward=true send_dummy_byte=false cleanup=true",
        options.video_codec.scrcpy_name(),
        options.display_id,
        options.max_size,
        options.max_fps,
        options.video_bit_rate
    )
}

async fn open_server_socket(client: &AdbClient, service: &str) -> Result<AdbStream, MirrorError> {
    let mut last_error = None;
    for _ in 0..SOCKET_ATTEMPTS {
        match client.open_service(service).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error.to_string()),
        }
        sleep(SOCKET_RETRY_DELAY).await;
    }
    Err(MirrorError::Startup(last_error.unwrap_or_else(|| {
        "Android local socket was not created".to_owned()
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_command_is_fixed_and_argument_safe() {
        let command = server_command(0x1234_abcd, MirrorOptions::default());
        assert!(command.contains("com.genymobile.scrcpy.Server 4.1"));
        assert!(command.contains("scid=1234abcd"));
        assert!(command.contains("audio=false"));
        assert!(command.contains("video_codec=h264"));
        assert!(command.contains("display_id=0"));
        assert!(command.contains("tunnel_forward=true"));
        assert!(!command.contains('\n'));
    }

    #[test]
    fn mirror_options_accept_native_size_and_reject_unbounded_values() {
        let options = MirrorOptions {
            max_size: 0,
            ..MirrorOptions::default()
        };
        assert!(options.validate().is_ok());

        let options = MirrorOptions {
            max_size: 128,
            ..MirrorOptions::default()
        };
        assert!(options.validate().is_err());

        let options = MirrorOptions {
            display_id: i32::MAX as u32 + 1,
            ..MirrorOptions::default()
        };
        assert!(options.validate().is_err());
    }

    #[test]
    fn server_command_selects_a_non_primary_display() {
        let options = MirrorOptions {
            display_id: 2,
            ..MirrorOptions::default()
        };
        let command = server_command(1, options);
        assert!(command.contains("display_id=2"));
    }

    #[test]
    fn server_command_selects_h265_and_av1() {
        for (video_codec, argument) in [
            (VideoCodec::H265, "video_codec=h265"),
            (VideoCodec::Av1, "video_codec=av1"),
        ] {
            let options = MirrorOptions {
                video_codec,
                ..MirrorOptions::default()
            };
            assert!(server_command(1, options).contains(argument));
        }
    }
}
