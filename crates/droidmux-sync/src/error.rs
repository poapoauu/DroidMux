use adb_client::{AdbClientError, AdbStreamError};
use thiserror::Error;

/// Invalid data received from or sent to an ADB Sync service.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SyncProtocolError {
    /// A remote path was empty or contained a NUL byte.
    #[error("remote path must be non-empty and contain no NUL bytes")]
    InvalidRemotePath,
    /// A remote path exceeded the Sync protocol limit.
    #[error("remote path is too long: limit {limit} bytes, got {actual}")]
    PathTooLong {
        /// Maximum accepted path length.
        limit: usize,
        /// Supplied path length.
        actual: usize,
    },
    /// Upload permissions exceeded Android's supported permission bits.
    #[error("remote file mode must be between 0 and 0o7777, got {0:#o}")]
    InvalidFileMode(u32),
    /// The negotiated ADB stream payload limit was unusable.
    #[error("ADB stream advertised a zero-byte payload limit")]
    InvalidPayloadLimit,
    /// The Sync stream closed before a complete response arrived.
    #[error("Sync stream ended while reading {context}")]
    UnexpectedEof {
        /// Response field being read when the stream ended.
        context: &'static str,
    },
    /// A response used an identifier that is not valid in the current state.
    #[error("unexpected Sync response {actual}; expected {expected}")]
    UnexpectedResponse {
        /// Human-readable set of expected identifiers.
        expected: &'static str,
        /// Four-byte identifier rendered lossily for diagnostics.
        actual: String,
    },
    /// A variable-length field exceeded its defensive limit.
    #[error("Sync {field} is too large: limit {limit} bytes, got {actual}")]
    FieldTooLarge {
        /// Field being decoded.
        field: &'static str,
        /// Maximum accepted field size.
        limit: usize,
        /// Advertised field size.
        actual: usize,
    },
    /// A directory response contained an unreasonable number of entries.
    #[error("Sync directory contains more than {limit} entries")]
    TooManyDirectoryEntries {
        /// Maximum number of retained entries.
        limit: usize,
    },
    /// A remote file name was not valid UTF-8.
    #[error("remote file name is not valid UTF-8")]
    InvalidFileName(#[source] std::string::FromUtf8Error),
    /// The device rejected the requested Sync operation.
    #[error("device rejected Sync operation: {0}")]
    Remote(String),
    /// A requested Sync v2 compression algorithm is not available on the device.
    #[error("Sync v2 compression algorithm is not negotiated: {0}")]
    CompressionUnavailable(&'static str),
    /// Compression or decompression failed after the peer negotiated the algorithm.
    #[error("Sync v2 {operation} compression failed: {reason}")]
    CompressionFailed {
        /// Operation that was being compressed or decompressed.
        operation: &'static str,
        /// Codec error description.
        reason: String,
    },
    /// Sync v2 returned a non-zero platform error code.
    #[error("device rejected Sync v2 {operation} with error code {code}")]
    RemoteCode {
        /// Operation being performed.
        operation: &'static str,
        /// Raw errno-style value returned by Android.
        code: u32,
    },
}

/// Errors returned by high-level ADB Sync operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SyncError {
    /// Opening the underlying ADB service failed.
    #[error(transparent)]
    Client(#[from] AdbClientError),
    /// The logical ADB stream failed.
    #[error(transparent)]
    Stream(#[from] AdbStreamError),
    /// Sync framing, limits, or sequencing were invalid.
    #[error(transparent)]
    Protocol(#[from] SyncProtocolError),
    /// A local filesystem operation failed.
    #[error("local file operation failed: {0}")]
    Io(#[from] std::io::Error),
    /// A download would replace an existing local path.
    #[error("download destination already exists: {}", .0.display())]
    DestinationExists(std::path::PathBuf),
    /// A remote file or its decompressed representation exceeded the configured limit.
    #[error("download exceeds the {limit}-byte limit: attempted {actual} bytes")]
    DownloadTooLarge {
        /// Maximum decompressed bytes accepted for this transfer.
        limit: u64,
        /// Size reported or produced by the device.
        actual: u64,
    },
    /// The caller canceled an active transfer.
    #[error("file transfer was canceled")]
    Canceled,
}
