//! Native Android 11+ wireless debugging pairing and TLS connection support.
//!
//! The crate implements the AOSP pairing wire protocol directly. It does not
//! invoke `adb`, depend on an ADB server, perform device discovery, or choose a
//! concrete persistence backend.

mod credential;
mod crypto;
mod error;
mod pair;
mod protocol;
mod tls;
mod transport;

#[cfg(test)]
mod integration_tests;

pub use credential::{
    CredentialStore, CredentialStoreError, StoredHostCredential, StoredPairedDevice,
};
pub use error::PairingError;
pub use pair::{
    DirectConnectionRequest, PairingRequest, PairingResult, connect_known_tcp_device,
    connect_paired_device, connect_tcp_device, pair_and_connect, pair_device,
};
pub use secrecy::SecretString;
pub use transport::{WirelessTransport, WirelessTransportConfig};
