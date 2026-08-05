use adb_client::{AdbClientError, AdbStreamError};
use thiserror::Error;

/// Shell v2 framing and semantic errors.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShellProtocolError {
    /// The peer supplied an unknown Shell v2 channel identifier.
    #[error("unknown Shell v2 packet identifier: {actual}")]
    UnknownPacketId {
        /// Raw one-byte identifier.
        actual: u8,
    },

    /// A Shell v2 length cannot be represented on this platform.
    #[error("Shell v2 packet length cannot be represented: {actual}")]
    LengthOutOfRange {
        /// Length advertised on the wire.
        actual: u32,
    },

    /// A Shell v2 packet exceeds the AOSP protocol buffer limit.
    #[error("Shell v2 packet is too large: limit {limit} bytes, got {actual}")]
    PacketTooLarge {
        /// Maximum accepted data length.
        limit: usize,
        /// Advertised or supplied data length.
        actual: usize,
    },

    /// The logical stream closed in the middle of a Shell v2 frame.
    #[error("truncated Shell v2 frame: {remaining} buffered bytes")]
    TruncatedFrame {
        /// Bytes left without a complete frame.
        remaining: usize,
    },

    /// An exit packet did not contain exactly one status byte.
    #[error("invalid Shell v2 exit packet length: expected 1, got {actual}")]
    InvalidExitLength {
        /// Supplied exit payload length.
        actual: usize,
    },

    /// The stream closed without the exit packet required by Shell v2.
    #[error("Shell v2 stream closed without an exit code")]
    MissingExitCode,

    /// The device sent a host-to-device-only packet identifier.
    #[error("unexpected device-to-host Shell v2 packet identifier: {actual}")]
    UnexpectedPacket {
        /// Raw one-byte identifier.
        actual: u8,
    },

    /// Bytes followed the terminal Shell v2 exit packet.
    #[error("data followed the terminal Shell v2 exit packet")]
    DataAfterExit,
}

/// Errors returned by ADB shell operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ShellError {
    /// Opening the underlying ADB service failed.
    #[error(transparent)]
    Client(#[from] AdbClientError),

    /// The underlying logical ADB stream failed.
    #[error(transparent)]
    Stream(#[from] AdbStreamError),

    /// Shell v2 framing or sequencing was invalid.
    #[error(transparent)]
    Protocol(#[from] ShellProtocolError),

    /// The command contains a NUL byte and cannot form an ADB service name.
    #[error("shell command contains a NUL byte")]
    InvalidCommand,

    /// Shell v2 was requested but not advertised by the device.
    #[error("device does not advertise the shell_v2 feature")]
    ShellV2Unsupported,

    /// A terminal dimension is zero.
    #[error("terminal rows and columns must both be non-zero")]
    InvalidWindowSize,

    /// The operation is unavailable for this shell mode.
    #[error("{operation} requires {requirement}")]
    UnsupportedOperation {
        /// Requested operation.
        operation: &'static str,
        /// Required shell mode.
        requirement: &'static str,
    },

    /// A convenience API exceeded its configured combined output limit.
    #[error("shell output exceeds the {limit}-byte limit after receiving {actual} bytes")]
    OutputTooLarge {
        /// Maximum combined stdout and stderr size.
        limit: usize,
        /// Combined size after receiving the rejected chunk.
        actual: usize,
    },

    /// The caller canceled the shell session.
    #[error("shell session was canceled")]
    Canceled,

    /// The background shell reader stopped without publishing a result.
    #[error("shell reader task stopped unexpectedly")]
    ReaderStopped,
}
