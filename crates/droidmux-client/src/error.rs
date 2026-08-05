use adb_auth::AdbAuthError;
use adb_protocol::{AdbCommand, AdbProtocolError};
use adb_transport::AdbTransportError;
use thiserror::Error;

/// Errors returned while establishing or closing an ADB client session.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AdbClientError {
    /// The underlying transport failed.
    #[error(transparent)]
    Transport(#[from] AdbTransportError),

    /// A local packet could not be constructed.
    #[error(transparent)]
    Protocol(#[from] AdbProtocolError),

    /// A logical ADB stream failed while servicing a request.
    #[error(transparent)]
    Stream(#[from] AdbStreamError),

    /// RSA authentication failed.
    #[error(transparent)]
    Authentication(#[from] AdbAuthError),

    /// A multi-key connection was requested without any host identity.
    #[error("at least one ADB authenticator is required")]
    NoAuthenticators,

    /// The device sent a packet that is not valid during connection setup.
    #[error("unexpected ADB packet during handshake: {command:?}")]
    UnexpectedPacket {
        /// Command received from the device.
        command: AdbCommand,
    },

    /// The device sent an unsupported `AUTH` subtype.
    #[error("unexpected ADB authentication subtype: {actual}")]
    UnexpectedAuthType {
        /// Authentication subtype received in `arg0`.
        actual: u32,
    },

    /// The device rejected the public key after it was offered for approval.
    #[error("device did not authorize the ADB public key")]
    AuthorizationRejected,

    /// The device connection banner is empty, contains an embedded NUL, or is not UTF-8.
    #[error("device returned an invalid ADB connection banner")]
    InvalidDeviceBanner,

    /// The device advertised an unusable packet payload limit.
    #[error("device returned an invalid ADB payload limit: {actual}")]
    InvalidPayloadLimit {
        /// Payload limit advertised by the device.
        actual: u32,
    },

    /// The local payload limit cannot be represented on the ADB wire.
    #[error("local ADB payload limit cannot be represented on the wire")]
    InvalidLocalPayloadLimit,

    /// Burst mode requires a non-zero receive window in `OPEN` packets.
    #[error("ADB delayed-ack receive window must be greater than zero")]
    InvalidDelayedAckWindow,

    /// A service name is empty or contains an embedded NUL byte.
    #[error("invalid ADB service name")]
    InvalidServiceName,

    /// A service name does not fit in one negotiated ADB packet.
    #[error("ADB service name is too long: limit {limit} bytes, got {actual}")]
    ServiceNameTooLong {
        /// Negotiated payload limit.
        limit: usize,
        /// NUL-terminated service-name size.
        actual: usize,
    },

    /// No unused non-zero local stream identifier remains.
    #[error("ADB local stream identifiers are exhausted")]
    LocalIdExhausted,

    /// The peer rejected an `OPEN` request before assigning a remote ID.
    #[error("ADB peer rejected stream {local_id}")]
    StreamRejected {
        /// Host-local stream identifier.
        local_id: u32,
    },

    /// The connected session stopped before an operation completed.
    #[error("ADB session is closed: {reason}")]
    SessionClosed {
        /// Human-readable termination reason.
        reason: String,
    },

    /// A request/response service returned an ADB `FAIL` response.
    #[error("ADB service request failed: {message}")]
    ServiceRequestFailed {
        /// Device-provided failure message.
        message: String,
    },

    /// A service response did not begin with a recognized status word.
    #[error("invalid ADB service response")]
    InvalidServiceResponse,

    /// A bounded service response exceeded the local memory limit.
    #[error("ADB service response exceeds the {limit}-byte limit")]
    ServiceResponseTooLarge {
        /// Maximum response size accepted by the convenience API.
        limit: usize,
    },
}

/// Errors returned by logical ADB stream operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AdbStreamError {
    /// The peer closed this logical stream.
    #[error("ADB peer closed the stream")]
    RemoteClosed,

    /// The local side already closed this logical stream.
    #[error("ADB stream is closed")]
    StreamClosed,

    /// An outgoing payload exceeds the negotiated packet limit.
    #[error("ADB stream payload is too large: limit {limit} bytes, got {actual}")]
    PayloadTooLarge {
        /// Negotiated payload limit.
        limit: usize,
        /// Supplied payload size.
        actual: usize,
    },

    /// The underlying ADB session stopped.
    #[error("ADB session is closed: {reason}")]
    SessionClosed {
        /// Human-readable termination reason.
        reason: String,
    },
}
