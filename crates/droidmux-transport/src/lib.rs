//! Runtime-independent transport contract for framed ADB packets.

use std::{fmt, io, time::Duration};

use adb_protocol::{AdbPacket, AdbProtocolError};
use async_trait::async_trait;
use thiserror::Error;

/// A transport operation associated with an error or timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportOperation {
    /// Opening the underlying connection.
    Connect,
    /// Reading one complete packet.
    Read,
    /// Writing one complete packet.
    Write,
    /// Closing the underlying connection.
    Close,
}

impl fmt::Display for TransportOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Connect => "connect",
            Self::Read => "read",
            Self::Write => "write",
            Self::Close => "close",
        };
        formatter.write_str(name)
    }
}

/// Errors produced by an ADB transport.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AdbTransportError {
    /// An operating-system I/O operation failed.
    #[error("ADB transport {operation} failed: {source}")]
    Io {
        /// Operation that was running.
        operation: TransportOperation,
        /// Original operating-system error.
        #[source]
        source: io::Error,
    },

    /// An operation exceeded its configured deadline.
    #[error("ADB transport {operation} timed out after {timeout:?}")]
    Timeout {
        /// Operation that timed out.
        operation: TransportOperation,
        /// Configured deadline.
        timeout: Duration,
    },

    /// The peer closed the connection during an operation.
    #[error("ADB transport connection closed during {operation}")]
    ConnectionClosed {
        /// Operation interrupted by the disconnect.
        operation: TransportOperation,
    },

    /// A received packet failed protocol validation.
    #[error(transparent)]
    Protocol(#[from] AdbProtocolError),
}

/// Reliable, ordered transport for complete ADB packets.
///
/// Implementations own framing and transport deadlines. ADB authentication,
/// logical streams, services, and reconnection policies belong to higher
/// layers.
#[async_trait]
pub trait AdbTransport: Send + Sync {
    /// Reads and validates one complete packet.
    ///
    /// This future must be cancellation-safe: if it is dropped after receiving
    /// part of a frame, the next call must resume from those buffered bytes.
    /// A read timeout must preserve the same partial-frame state.
    ///
    /// # Errors
    ///
    /// Returns a transport, timeout, disconnect, or protocol error.
    async fn read_packet(&mut self) -> Result<AdbPacket, AdbTransportError>;

    /// Encodes and writes one complete packet.
    ///
    /// # Errors
    ///
    /// Returns a transport, timeout, disconnect, or protocol error.
    async fn write_packet(&mut self, packet: &AdbPacket) -> Result<(), AdbTransportError>;

    /// Actively closes the underlying connection.
    ///
    /// # Errors
    ///
    /// Returns a transport, timeout, or disconnect error when shutdown fails.
    async fn close(&mut self) -> Result<(), AdbTransportError>;

    /// Returns a human-readable description of the connected peer.
    fn peer_description(&self) -> String;
}
