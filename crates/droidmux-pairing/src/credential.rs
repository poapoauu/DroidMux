use std::fmt;

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use thiserror::Error;

/// A host RSA identity record suitable for protected persistence.
#[derive(Clone)]
pub struct StoredHostCredential {
    private_key_pem: SecretString,
    client_name: String,
}

impl StoredHostCredential {
    /// Creates a persisted host identity from PKCS#8 PEM and its ADB comment.
    #[must_use]
    pub fn new(private_key_pem: SecretString, client_name: impl Into<String>) -> Self {
        Self {
            private_key_pem,
            client_name: client_name.into(),
        }
    }

    /// Exposes the private key only for cryptographic import or protected storage.
    #[must_use]
    pub fn expose_private_key_pem(&self) -> &str {
        self.private_key_pem.expose_secret()
    }

    /// Returns the stable ADB public-key comment associated with the identity.
    #[must_use]
    pub fn client_name(&self) -> &str {
        &self.client_name
    }
}

impl fmt::Debug for StoredHostCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredHostCredential")
            .field("private_key_pem", &"[REDACTED]")
            .field("client_name", &self.client_name)
            .finish()
    }
}

/// Non-secret metadata retained after a successful wireless pairing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPairedDevice {
    /// Stable GUID returned by the Android pairing service.
    pub device_id: String,
    /// Host used for the successful pairing request.
    pub host: String,
    /// SHA-256 fingerprint of the device pairing certificate.
    pub certificate_fingerprint: String,
}

/// An error returned by a protected credential storage implementation.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct CredentialStoreError {
    message: String,
}

impl CredentialStoreError {
    /// Creates a storage error with a non-secret diagnostic message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Replaceable persistence boundary for the reusable host identity and paired devices.
///
/// Implementations should use Credential Manager, Keychain, Secret Service, or
/// another protected backend. Implementations must not include private key
/// contents in errors or logs.
#[async_trait]
pub trait CredentialStore: Send + Sync {
    /// Loads the process-wide ADB host identity, if one has been created.
    async fn load_host_credential(
        &self,
    ) -> Result<Option<StoredHostCredential>, CredentialStoreError>;

    /// Atomically persists the process-wide ADB host identity.
    async fn save_host_credential(
        &self,
        credential: &StoredHostCredential,
    ) -> Result<(), CredentialStoreError>;

    /// Persists or replaces metadata for one successfully paired device.
    async fn save_paired_device(
        &self,
        device: &StoredPairedDevice,
    ) -> Result<(), CredentialStoreError>;

    /// Loads a paired device by the stable GUID returned by Android.
    async fn load_paired_device(
        &self,
        device_id: &str,
    ) -> Result<Option<StoredPairedDevice>, CredentialStoreError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_credential_debug_redacts_the_private_key() {
        let credential = StoredHostCredential::new(
            SecretString::from("sensitive-private-key".to_owned()),
            "droidmux@test",
        );
        let debug = format!("{credential:?}");

        assert!(!debug.contains("sensitive-private-key"));
        assert!(debug.contains("[REDACTED]"));
    }
}
