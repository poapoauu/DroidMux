use std::{io, time::Duration};

use adb_auth::AdbAuthError;
use adb_client::AdbClientError;
use thiserror::Error;

use crate::CredentialStoreError;

/// Failures produced while pairing or connecting an Android wireless device.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PairingError {
    /// The pairing code must contain exactly six ASCII digits.
    #[error("pairing code must contain exactly six ASCII digits")]
    InvalidPairingCode,
    /// The remote endpoint is not valid.
    #[error("invalid wireless endpoint: {0}")]
    InvalidEndpoint(String),
    /// The client name cannot be represented as an ADB key comment.
    #[error("invalid pairing client name")]
    InvalidClientName,
    /// A network operation exceeded its deadline.
    #[error("wireless pairing operation timed out after {0:?}")]
    Timeout(Duration),
    /// The TCP or TLS connection failed.
    #[error("wireless pairing connection failed: {0}")]
    Connection(#[source] io::Error),
    /// The selected socket speaks ADB directly instead of the pairing TLS protocol.
    #[error(
        "remote endpoint is not a wireless pairing TLS service; use the dynamic pairing port, not the ADB connection port"
    )]
    NotPairingEndpoint,
    /// TLS setup or certificate processing failed.
    #[error("wireless pairing TLS failed: {0}")]
    Tls(String),
    /// The peer sent an invalid or unsupported pairing message.
    #[error("wireless pairing protocol error: {0}")]
    Protocol(String),
    /// `PeerInfo` authentication failed, normally because the code was wrong.
    #[error("the six-digit pairing code was rejected")]
    IncorrectPairingCode,
    /// Android returned an invalid device GUID.
    #[error("the paired device returned an invalid device identifier")]
    InvalidDeviceId,
    /// No reusable host credential was available in protected storage.
    #[error("no saved ADB host credential is available")]
    MissingHostCredential,
    /// The requested device has not previously been paired.
    #[error("device {device_id} has no saved pairing record")]
    DeviceNotPaired {
        /// Stable device identifier requested by the caller.
        device_id: String,
    },
    /// Protected credential persistence failed.
    #[error("credential storage failed: {0}")]
    CredentialStore(#[from] CredentialStoreError),
    /// The reusable ADB RSA credential was invalid.
    #[error("ADB credential failed: {0}")]
    Credential(#[from] AdbAuthError),
    /// The post-pairing ADB session failed.
    #[error("paired ADB connection failed: {0}")]
    AdbClient(#[from] AdbClientError),
}

impl From<rustls::Error> for PairingError {
    fn from(error: rustls::Error) -> Self {
        Self::Tls(error.to_string())
    }
}

impl From<rcgen::Error> for PairingError {
    fn from(error: rcgen::Error) -> Self {
        Self::Tls(error.to_string())
    }
}
