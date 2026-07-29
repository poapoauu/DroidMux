use thiserror::Error;

/// Errors returned while constructing or decoding ADB packets.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AdbProtocolError {
    /// The input does not contain the complete fixed-size header.
    #[error("incomplete ADB header: expected {expected} bytes, got {actual}")]
    IncompleteHeader {
        /// Required header size.
        expected: usize,
        /// Number of available bytes.
        actual: usize,
    },

    /// The input does not contain the payload declared by its header.
    #[error("incomplete ADB payload: expected {expected} bytes, got {actual}")]
    IncompletePayload {
        /// Payload size declared by the header.
        expected: usize,
        /// Number of payload bytes currently available.
        actual: usize,
    },

    /// A command word is not recognized by this protocol version.
    #[error("unknown ADB command: {0:#010x}")]
    UnknownCommand(u32),

    /// The header magic does not match the bitwise inverse of its command.
    #[error("invalid ADB magic: expected {expected:#010x}, got {actual:#010x}")]
    InvalidMagic {
        /// Magic derived from the decoded command.
        expected: u32,
        /// Magic read from the wire.
        actual: u32,
    },

    /// The payload checksum does not match the header.
    #[error("ADB checksum mismatch: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch {
        /// Checksum declared by the header.
        expected: u32,
        /// Checksum computed from the payload.
        actual: u32,
    },

    /// The declared or supplied payload exceeds the codec limit.
    #[error("ADB payload is too large: {length} bytes exceeds the {max}-byte limit")]
    PayloadTooLarge {
        /// Supplied or declared payload size.
        length: usize,
        /// Maximum accepted payload size.
        max: usize,
    },

    /// The wire payload length cannot be represented on the current platform.
    #[error("ADB payload length cannot be represented on this platform: {length}")]
    PayloadLengthOutOfRange {
        /// Payload length read from the wire.
        length: u32,
    },
}
