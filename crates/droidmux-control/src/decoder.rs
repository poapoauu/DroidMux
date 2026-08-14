#[cfg(any(feature = "decoder-openh264", feature = "decoder-ffmpeg"))]
use std::{
    fmt,
    sync::{Arc, Mutex, PoisonError, Weak},
};

use bytes::Bytes;
#[cfg(feature = "decoder-openh264")]
use openh264::{decoder::Decoder, formats::YUVSource};

#[cfg(any(feature = "decoder-openh264", feature = "decoder-ffmpeg"))]
use crate::protocol::MAX_VIDEO_PACKET_SIZE;
use crate::{MirrorError, VideoCodec};

#[cfg(any(feature = "decoder-openh264", feature = "decoder-ffmpeg"))]
pub(crate) const RETAINED_PIXEL_BUFFERS: usize = 3;
#[cfg(any(feature = "decoder-openh264", feature = "decoder-ffmpeg"))]
pub(crate) const MAX_DECODED_FRAME_BYTES: usize = MAX_VIDEO_PACKET_SIZE * 4;

/// CPU pixel formats that a mirror decoder can produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodedPixelFormat {
    /// Packed red, green, and blue bytes.
    Rgb24,
    /// Packed red, green, blue, and alpha bytes.
    Rgba8888,
    /// Packed blue, green, red, and alpha bytes.
    Bgra8888,
}

impl DecodedPixelFormat {
    /// Returns the number of tightly packed bytes used by one pixel.
    #[must_use]
    pub const fn bytes_per_pixel(self) -> usize {
        match self {
            Self::Rgb24 => 3,
            Self::Rgba8888 | Self::Bgra8888 => 4,
        }
    }
}

/// Timing and frame-type metadata associated with one encoded access unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VideoFrameMetadata {
    /// Presentation timestamp reported by the Android mirror server.
    pub presentation_time_us: u64,
    /// Whether the encoded access unit is an independently decodable key frame.
    pub key_frame: bool,
    /// Visible encoded frame dimensions reported by the mirror server.
    ///
    /// This excludes codec alignment padding (for example, a 1080-pixel edge
    /// stored in a 1088-pixel decoder surface). It is absent only when a peer
    /// sends frame packets before its session metadata.
    pub frame_size: Option<(u32, u32)>,
}

/// One CPU-backed frame produced by a [`MirrorDecoder`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedVideoFrame {
    /// Tightly packed pixels in [`Self::format`].
    pub pixels: Bytes,
    /// Decoded width in pixels.
    pub width: u32,
    /// Decoded height in pixels.
    pub height: u32,
    /// Pixel layout used by [`Self::pixels`].
    pub format: DecodedPixelFormat,
    /// Metadata for the encoded access unit that produced this frame.
    pub metadata: VideoFrameMetadata,
}

/// Pluggable decoder used by a mirror session's long-lived decode worker.
///
/// Implementations must submit every access unit to their codec, including
/// units for which `output` is `None`, so inter-frame decoder state remains
/// valid. Returning owned bytes lets the session publish a frame after this
/// call without borrowing the decoder.
pub trait MirrorDecoder: Send + 'static {
    /// Returns a stable diagnostic name for this backend.
    fn backend_name(&self) -> &'static str;

    /// Returns the encoded video format accepted by this decoder instance.
    fn video_codec(&self) -> VideoCodec {
        VideoCodec::H264
    }

    /// Submits one encoded access unit and optionally materializes CPU pixels.
    ///
    /// `metadata` is absent for codec-configuration packets. A codec can consume
    /// a packet without producing a frame, or invoke `emit` more than once when
    /// one packet releases multiple delayed frames. Backends that reorder frames
    /// must retain the input metadata and attach the matching value to each
    /// emitted [`DecodedVideoFrame`].
    ///
    /// # Errors
    ///
    /// Returns an error when the packet is invalid, the decoder fails, or the
    /// requested output cannot be represented safely in memory.
    fn decode(
        &mut self,
        access_unit: &[u8],
        metadata: Option<VideoFrameMetadata>,
        output: Option<DecodedPixelFormat>,
        emit: &mut dyn FnMut(DecodedVideoFrame),
    ) -> Result<(), MirrorError>;

    /// Flushes a delayed frame when the encoded stream ends.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot flush its buffered state.
    fn flush(&mut self, _emit: &mut dyn FnMut(DecodedVideoFrame)) -> Result<(), MirrorError> {
        Ok(())
    }
}

/// Source-built `OpenH264` software decoder used by the default configuration.
#[cfg(feature = "decoder-openh264")]
pub struct OpenH264Decoder {
    decoder: Decoder,
    buffers: PixelBufferPool,
}

#[cfg(feature = "decoder-openh264")]
impl OpenH264Decoder {
    /// Creates an `OpenH264` decoder and a small recyclable pixel-buffer pool.
    ///
    /// # Errors
    ///
    /// Returns an error when `OpenH264` cannot initialize.
    pub fn new() -> Result<Self, MirrorError> {
        let decoder = Decoder::new().map_err(|error| MirrorError::Decode(error.to_string()))?;
        Ok(Self {
            decoder,
            buffers: PixelBufferPool::new(RETAINED_PIXEL_BUFFERS),
        })
    }
}

#[cfg(feature = "decoder-openh264")]
impl fmt::Debug for OpenH264Decoder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenH264Decoder")
            .field("buffers", &self.buffers)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "decoder-openh264")]
impl MirrorDecoder for OpenH264Decoder {
    fn backend_name(&self) -> &'static str {
        "openh264"
    }

    fn decode(
        &mut self,
        access_unit: &[u8],
        metadata: Option<VideoFrameMetadata>,
        output: Option<DecodedPixelFormat>,
        emit: &mut dyn FnMut(DecodedVideoFrame),
    ) -> Result<(), MirrorError> {
        let Some(decoded) = self
            .decoder
            .decode(access_unit)
            .map_err(|error| MirrorError::Decode(error.to_string()))?
        else {
            return Ok(());
        };
        let Some(format) = output else {
            return Ok(());
        };
        let metadata = metadata.ok_or_else(|| {
            MirrorError::Frame("pixel output was requested without frame metadata".to_owned())
        })?;

        let (width, height) = decoded.dimensions();
        let output_len = width
            .checked_mul(height)
            .and_then(|pixels| pixels.checked_mul(format.bytes_per_pixel()))
            .ok_or_else(|| {
                MirrorError::Frame("decoded dimensions overflow memory size".to_owned())
            })?;
        if output_len > MAX_DECODED_FRAME_BYTES {
            return Err(MirrorError::Frame(format!(
                "decoded frame requires {output_len} bytes"
            )));
        }

        let mut pixels = self.buffers.acquire(output_len);
        match format {
            DecodedPixelFormat::Rgb24 => decoded.write_rgb8(&mut pixels),
            DecodedPixelFormat::Rgba8888 => decoded.write_rgba8(&mut pixels),
            DecodedPixelFormat::Bgra8888 => {
                // OpenH264 exposes RGB and RGBA conversion only. Swizzle the
                // reusable output buffer in place so callers still receive
                // BGRA without an additional full-frame allocation or copy.
                decoded.write_rgba8(&mut pixels);
                rgba_to_bgra_in_place(&mut pixels);
            }
        }
        let width = u32::try_from(width)
            .map_err(|_| MirrorError::Frame("decoded width exceeds u32".to_owned()))?;
        let height = u32::try_from(height)
            .map_err(|_| MirrorError::Frame("decoded height exceeds u32".to_owned()))?;

        emit(DecodedVideoFrame {
            pixels: self.buffers.freeze(pixels),
            width,
            height,
            format,
            metadata,
        });
        Ok(())
    }
}

#[cfg(feature = "decoder-openh264")]
fn rgba_to_bgra_in_place(pixels: &mut [u8]) {
    for pixel in pixels.chunks_exact_mut(4) {
        pixel.swap(0, 2);
    }
}

#[derive(Clone)]
#[cfg(any(feature = "decoder-openh264", feature = "decoder-ffmpeg"))]
pub(crate) struct PixelBufferPool {
    inner: Arc<PixelBufferPoolInner>,
}

#[cfg(any(feature = "decoder-openh264", feature = "decoder-ffmpeg"))]
impl PixelBufferPool {
    pub(crate) fn new(max_retained: usize) -> Self {
        Self {
            inner: Arc::new(PixelBufferPoolInner {
                available: Mutex::new(Vec::with_capacity(max_retained)),
                max_retained,
            }),
        }
    }

    pub(crate) fn acquire(&self, length: usize) -> Vec<u8> {
        let mut available = self
            .inner
            .available
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut buffer = available.pop().unwrap_or_default();
        drop(available);
        buffer.resize(length, 0);
        buffer
    }

    pub(crate) fn freeze(&self, buffer: Vec<u8>) -> Bytes {
        Bytes::from_owner(PooledPixelBuffer {
            buffer: Some(buffer),
            pool: Arc::downgrade(&self.inner),
        })
    }

    #[cfg(all(test, feature = "decoder-openh264"))]
    fn retained_len(&self) -> usize {
        self.inner
            .available
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

#[cfg(any(feature = "decoder-openh264", feature = "decoder-ffmpeg"))]
impl fmt::Debug for PixelBufferPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PixelBufferPool")
            .field("max_retained", &self.inner.max_retained)
            .finish_non_exhaustive()
    }
}

#[cfg(any(feature = "decoder-openh264", feature = "decoder-ffmpeg"))]
struct PixelBufferPoolInner {
    available: Mutex<Vec<Vec<u8>>>,
    max_retained: usize,
}

#[cfg(any(feature = "decoder-openh264", feature = "decoder-ffmpeg"))]
impl PixelBufferPoolInner {
    fn recycle(&self, mut buffer: Vec<u8>) {
        let mut available = self
            .available
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if available.len() < self.max_retained {
            buffer.clear();
            available.push(buffer);
        }
    }
}

#[cfg(any(feature = "decoder-openh264", feature = "decoder-ffmpeg"))]
struct PooledPixelBuffer {
    buffer: Option<Vec<u8>>,
    pool: Weak<PixelBufferPoolInner>,
}

#[cfg(any(feature = "decoder-openh264", feature = "decoder-ffmpeg"))]
impl AsRef<[u8]> for PooledPixelBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_deref().unwrap_or_default()
    }
}

#[cfg(any(feature = "decoder-openh264", feature = "decoder-ffmpeg"))]
impl Drop for PooledPixelBuffer {
    fn drop(&mut self) {
        let Some(buffer) = self.buffer.take() else {
            return;
        };
        if let Some(pool) = self.pool.upgrade() {
            pool.recycle(buffer);
        }
    }
}

#[cfg(all(test, feature = "decoder-openh264"))]
mod tests {
    use super::{PixelBufferPool, rgba_to_bgra_in_place};

    #[test]
    fn rgba_pixels_are_swizzled_to_bgra_in_place() {
        let mut pixels = vec![1, 2, 3, 4, 10, 20, 30, 40, 50, 60, 70, 80];
        rgba_to_bgra_in_place(&mut pixels);
        assert_eq!(pixels, [3, 2, 1, 4, 30, 20, 10, 40, 70, 60, 50, 80]);
    }

    #[test]
    fn pixel_buffer_returns_after_the_last_bytes_clone_drops() {
        let pool = PixelBufferPool::new(1);
        let pixels = pool.freeze(vec![1, 2, 3, 4]);
        let clone = pixels.clone();

        drop(pixels);
        assert_eq!(pool.retained_len(), 0);
        drop(clone);
        assert_eq!(pool.retained_len(), 1);

        let reused = pool.acquire(8);
        assert_eq!(reused.len(), 8);
        assert!(reused.capacity() >= 8);
    }

    #[test]
    fn pixel_buffer_pool_does_not_retain_more_than_its_limit() {
        let pool = PixelBufferPool::new(1);
        let first = pool.freeze(vec![1]);
        let second = pool.freeze(vec![2]);

        drop(first);
        drop(second);

        assert_eq!(pool.retained_len(), 1);
    }
}
