//! Bounded wire-level primitives for the Android Debug Bridge protocol.
//!
//! This crate defines packet framing only. It does not perform I/O, manage ADB
//! sessions, or implement higher-level services.

mod command;
mod error;
mod header;
mod packet;

pub use command::AdbCommand;
pub use error::AdbProtocolError;
pub use header::AdbHeader;
pub use packet::AdbPacket;

/// Size of an ADB packet header in bytes.
pub const ADB_HEADER_LEN: usize = 24;

/// Current host protocol version advertised during connection setup.
pub const ADB_VERSION: u32 = 0x0100_0001;

/// Original ADB protocol version before checksum omission was introduced.
pub const ADB_VERSION_MIN: u32 = 0x0100_0000;

/// ADB protocol version whose peers may omit payload checksums.
pub const ADB_VERSION_SKIP_CHECKSUM: u32 = 0x0100_0001;

/// Maximum payload used by legacy ADB peers before modern negotiation.
pub const MAX_PAYLOAD_V1: usize = 4 * 1024;

/// Maximum payload accepted by the codec.
pub const MAX_PAYLOAD: usize = 1024 * 1024;

/// Computes the additive checksum used by ADB packet payloads.
#[must_use]
pub fn checksum(payload: &[u8]) -> u32 {
    payload
        .iter()
        .fold(0_u32, |sum, byte| sum.wrapping_add(u32::from(*byte)))
}
