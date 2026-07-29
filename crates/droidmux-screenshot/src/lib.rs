//! Native, bounded screenshot capture over an ADB logical stream.

use std::{fmt::Write as _, time::Duration};

use adb_client::{AdbClient, AdbClientError, AdbStreamError};
use bytes::{Bytes, BytesMut};
use thiserror::Error;
use tokio::{sync::watch, time::Instant};

const SCREENSHOT_SERVICE: &str = "exec:screencap -p 2>/dev/null";
const PNG_SIGNATURE: &[u8; 8] = b"\x89PNG\r\n\x1a\n";
const PNG_IHDR_SIZE: usize = 33;

/// Default maximum PNG payload accepted from a device.
pub const DEFAULT_MAX_OUTPUT_SIZE: usize = 32 * 1024 * 1024;
/// Default maximum width or height accepted from a device.
pub const DEFAULT_MAX_DIMENSION: u32 = 16_384;
/// Default maximum decoded pixel count accepted from a device.
pub const DEFAULT_MAX_PIXELS: u64 = 100_000_000;

/// Cloneable, latched cancellation signal for one screenshot capture.
#[derive(Debug, Clone)]
pub struct ScreenshotCancellation {
    sender: watch::Sender<bool>,
}

impl ScreenshotCancellation {
    /// Creates a signal in the active state.
    #[must_use]
    pub fn new() -> Self {
        let (sender, _) = watch::channel(false);
        Self { sender }
    }

    /// Requests cancellation. Repeated calls are harmless.
    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }

    /// Reports whether cancellation was already requested.
    #[must_use]
    pub fn is_canceled(&self) -> bool {
        *self.sender.borrow()
    }

    async fn canceled(&self) {
        let mut receiver = self.sender.subscribe();
        if *receiver.borrow() {
            return;
        }
        while receiver.changed().await.is_ok() {
            if *receiver.borrow() {
                return;
            }
        }
    }
}

impl Default for ScreenshotCancellation {
    fn default() -> Self {
        Self::new()
    }
}

/// Limits and cancellation state for a screenshot request.
#[derive(Debug, Clone)]
pub struct ScreenshotOptions {
    /// Maximum elapsed time for opening, reading, and validating the stream.
    pub timeout: Duration,
    /// Maximum compressed PNG byte length.
    pub max_output_size: usize,
    /// Maximum accepted image width.
    pub max_width: u32,
    /// Maximum accepted image height.
    pub max_height: u32,
    /// Maximum accepted width multiplied by height.
    pub max_pixels: u64,
    /// Caller-controlled cancellation signal.
    pub cancellation: ScreenshotCancellation,
}

impl Default for ScreenshotOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(15),
            max_output_size: DEFAULT_MAX_OUTPUT_SIZE,
            max_width: DEFAULT_MAX_DIMENSION,
            max_height: DEFAULT_MAX_DIMENSION,
            max_pixels: DEFAULT_MAX_PIXELS,
            cancellation: ScreenshotCancellation::new(),
        }
    }
}

/// Dimensions read from the PNG `IHDR` chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PngMetadata {
    /// Image width in pixels.
    pub width: u32,
    /// Image height in pixels.
    pub height: u32,
}

/// Errors returned by native screenshot capture and validation.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ScreenshotError {
    /// Opening the ADB execution service failed.
    #[error(transparent)]
    Client(#[from] AdbClientError),
    /// Reading or closing the ADB logical stream failed.
    #[error(transparent)]
    Stream(#[from] AdbStreamError),
    /// The capture exceeded its total deadline.
    #[error("screenshot capture timed out after {0:?}")]
    Timeout(Duration),
    /// The caller canceled the capture.
    #[error("screenshot capture was canceled")]
    Canceled,
    /// The device closed the stream without returning bytes.
    #[error("screenshot command returned no data")]
    EmptyOutput,
    /// The compressed output crossed the configured byte limit.
    #[error("screenshot output is too large: limit {limit} bytes, got at least {actual} bytes")]
    OutputTooLarge {
        /// Maximum accepted bytes.
        limit: usize,
        /// First observed size beyond the limit.
        actual: usize,
    },
    /// The output did not start with the fixed PNG signature.
    #[error("screenshot output has an invalid PNG signature (first bytes: {actual_hex})")]
    InvalidPngSignature {
        /// Up to the first eight returned bytes encoded as hexadecimal.
        actual_hex: String,
    },
    /// The PNG ended before its complete `IHDR` chunk.
    #[error("screenshot PNG header is truncated: expected at least {expected} bytes, got {actual}")]
    TruncatedPngHeader {
        /// Minimum required byte count.
        expected: usize,
        /// Actual byte count.
        actual: usize,
    },
    /// The first PNG chunk was not a 13-byte `IHDR` chunk.
    #[error("screenshot PNG has an invalid IHDR chunk")]
    InvalidIhdr,
    /// The PNG advertises a zero width or height.
    #[error("screenshot PNG has invalid dimensions {width}x{height}")]
    InvalidDimensions {
        /// Advertised width.
        width: u32,
        /// Advertised height.
        height: u32,
    },
    /// One PNG dimension crossed the configured limit.
    #[error(
        "screenshot dimensions {width}x{height} exceed the configured limits {max_width}x{max_height}"
    )]
    DimensionsTooLarge {
        /// Advertised width.
        width: u32,
        /// Advertised height.
        height: u32,
        /// Maximum accepted width.
        max_width: u32,
        /// Maximum accepted height.
        max_height: u32,
    },
    /// The decoded image would contain too many pixels.
    #[error("screenshot pixel count {actual} exceeds the configured limit {limit}")]
    PixelCountTooLarge {
        /// Maximum accepted pixel count.
        limit: u64,
        /// Advertised pixel count.
        actual: u64,
    },
}

/// Captures a PNG using the default timeout, size limits, and cancellation state.
///
/// # Errors
///
/// Returns an error when the execution stream fails, a limit is crossed, or
/// the device output is not a structurally valid PNG header.
pub async fn capture_png(client: &AdbClient) -> Result<Bytes, ScreenshotError> {
    capture_png_with_options(client, &ScreenshotOptions::default()).await
}

/// Captures a PNG using explicit limits and cancellation state.
///
/// This opens an independent `exec:screencap -p` ADB stream, equivalent to
/// `adb exec-out screencap -p`. Standard error is discarded so Android cannot
/// prepend a command warning to the PNG byte stream.
///
/// # Errors
///
/// Returns an error when the execution stream fails, the deadline expires,
/// cancellation is requested, a limit is crossed, or PNG validation fails.
pub async fn capture_png_with_options(
    client: &AdbClient,
    options: &ScreenshotOptions,
) -> Result<Bytes, ScreenshotError> {
    if options.cancellation.is_canceled() {
        return Err(ScreenshotError::Canceled);
    }
    let deadline = Instant::now() + options.timeout;
    let stream = tokio::select! {
        () = options.cancellation.canceled() => return Err(ScreenshotError::Canceled),
        result = tokio::time::timeout_at(deadline, client.open_service(SCREENSHOT_SERVICE)) => {
            result.map_err(|_| ScreenshotError::Timeout(options.timeout))??
        }
    };

    let mut output = BytesMut::new();
    loop {
        let read = tokio::select! {
            () = options.cancellation.canceled() => {
                let _ = stream.close().await;
                return Err(ScreenshotError::Canceled);
            }
            result = tokio::time::timeout_at(deadline, stream.read()) => {
                if let Ok(result) = result {
                    result
                } else {
                    let _ = stream.close().await;
                    return Err(ScreenshotError::Timeout(options.timeout));
                }
            }
        }?;
        let Some(chunk) = read else {
            break;
        };
        if let Err(error) = append_bounded(&mut output, &chunk, options.max_output_size) {
            let _ = stream.close().await;
            return Err(error);
        }
    }

    let png = output.freeze();
    validate_png(&png, options)?;
    Ok(png)
}

/// Validates the PNG signature and first `IHDR` chunk against capture limits.
///
/// # Errors
///
/// Returns an error for truncated or malformed headers and disallowed image
/// dimensions.
pub fn validate_png(
    png: &[u8],
    options: &ScreenshotOptions,
) -> Result<PngMetadata, ScreenshotError> {
    if png.is_empty() {
        return Err(ScreenshotError::EmptyOutput);
    }
    if png.len() < PNG_SIGNATURE.len() || &png[..PNG_SIGNATURE.len()] != PNG_SIGNATURE {
        return Err(ScreenshotError::InvalidPngSignature {
            actual_hex: hex_prefix(png),
        });
    }
    if png.len() < PNG_IHDR_SIZE {
        return Err(ScreenshotError::TruncatedPngHeader {
            expected: PNG_IHDR_SIZE,
            actual: png.len(),
        });
    }
    let ihdr_length = u32::from_be_bytes([png[8], png[9], png[10], png[11]]);
    if ihdr_length != 13 || &png[12..16] != b"IHDR" {
        return Err(ScreenshotError::InvalidIhdr);
    }
    let width = u32::from_be_bytes([png[16], png[17], png[18], png[19]]);
    let height = u32::from_be_bytes([png[20], png[21], png[22], png[23]]);
    if width == 0 || height == 0 {
        return Err(ScreenshotError::InvalidDimensions { width, height });
    }
    if width > options.max_width || height > options.max_height {
        return Err(ScreenshotError::DimensionsTooLarge {
            width,
            height,
            max_width: options.max_width,
            max_height: options.max_height,
        });
    }
    let pixels = u64::from(width) * u64::from(height);
    if pixels > options.max_pixels {
        return Err(ScreenshotError::PixelCountTooLarge {
            limit: options.max_pixels,
            actual: pixels,
        });
    }
    Ok(PngMetadata { width, height })
}

fn hex_prefix(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(PNG_SIGNATURE.len())
        .fold(String::new(), |mut output, byte| {
            let _ = write!(output, "{byte:02x}");
            output
        })
}

fn append_bounded(
    output: &mut BytesMut,
    chunk: &[u8],
    limit: usize,
) -> Result<(), ScreenshotError> {
    let actual = output.len().saturating_add(chunk.len());
    if actual > limit {
        return Err(ScreenshotError::OutputTooLarge { limit, actual });
    }
    output.extend_from_slice(chunk);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        PNG_IHDR_SIZE, ScreenshotCancellation, ScreenshotError, ScreenshotOptions, append_bounded,
        validate_png,
    };
    use bytes::BytesMut;

    fn png_header(width: u32, height: u32) -> Vec<u8> {
        let mut png = Vec::with_capacity(PNG_IHDR_SIZE);
        png.extend_from_slice(b"\x89PNG\r\n\x1a\n");
        png.extend_from_slice(&13_u32.to_be_bytes());
        png.extend_from_slice(b"IHDR");
        png.extend_from_slice(&width.to_be_bytes());
        png.extend_from_slice(&height.to_be_bytes());
        png.extend_from_slice(&[8, 6, 0, 0, 0]);
        png.extend_from_slice(&[0; 4]);
        png
    }

    #[test]
    fn reads_dimensions_from_a_valid_ihdr() {
        let metadata = validate_png(&png_header(1080, 2400), &ScreenshotOptions::default())
            .expect("valid PNG header");
        assert_eq!(metadata.width, 1080);
        assert_eq!(metadata.height, 2400);
    }

    #[test]
    fn rejects_invalid_and_truncated_png_headers() {
        assert!(matches!(
            validate_png(b"not a png", &ScreenshotOptions::default()),
            Err(ScreenshotError::InvalidPngSignature { .. })
        ));
        assert!(matches!(
            validate_png(b"\x89PNG\r\n\x1a\n", &ScreenshotOptions::default()),
            Err(ScreenshotError::TruncatedPngHeader { .. })
        ));
        let mut invalid_ihdr = png_header(10, 10);
        invalid_ihdr[12..16].copy_from_slice(b"IDAT");
        assert!(matches!(
            validate_png(&invalid_ihdr, &ScreenshotOptions::default()),
            Err(ScreenshotError::InvalidIhdr)
        ));
    }

    #[test]
    fn enforces_dimension_pixel_and_output_limits() {
        let dimension_options = ScreenshotOptions {
            max_width: 100,
            ..ScreenshotOptions::default()
        };
        assert!(matches!(
            validate_png(&png_header(101, 50), &dimension_options),
            Err(ScreenshotError::DimensionsTooLarge { .. })
        ));
        let pixel_options = ScreenshotOptions {
            max_pixels: 100,
            ..ScreenshotOptions::default()
        };
        assert!(matches!(
            validate_png(&png_header(11, 10), &pixel_options),
            Err(ScreenshotError::PixelCountTooLarge { .. })
        ));
        let mut output = BytesMut::from(&b"1234"[..]);
        assert!(matches!(
            append_bounded(&mut output, b"56", 5),
            Err(ScreenshotError::OutputTooLarge {
                limit: 5,
                actual: 6
            })
        ));
    }

    #[tokio::test]
    async fn cancellation_is_cloneable_and_latched() {
        let cancellation = ScreenshotCancellation::new();
        let observer = cancellation.clone();
        cancellation.cancel();
        observer.canceled().await;
        assert!(observer.is_canceled());
    }
}
