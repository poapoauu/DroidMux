use std::{fmt, sync::Arc, time::Duration};

use adb_auth::{AdbAuthenticator, RsaAdbCredential};
use adb_client::AdbClient;
use adb_transport_tcp::{TcpTransport, TcpTransportConfig};
use secrecy::{ExposeSecret, SecretString};
use tokio::{
    net::{TcpStream, lookup_host},
    time,
};
use zeroize::Zeroizing;

use crate::{
    CredentialStore, PairingError, StoredHostCredential, StoredPairedDevice,
    crypto::{PairingCipher, Spake2State},
    protocol::{PEER_INFO_SIZE, PacketType, PeerInfo, read_packet, write_packet},
    tls::{TlsIdentity, connect_tls, export_pairing_key, peer_certificate_fingerprint},
    transport::{WirelessTransport, WirelessTransportConfig},
};

const PAIRING_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PAIRING_IO_TIMEOUT: Duration = Duration::from_secs(30);

/// Parameters entered from Android's "Pair device with pairing code" dialog.
pub struct PairingRequest {
    /// IPv4, IPv6, or DNS host of the Android device.
    pub host: String,
    /// Dynamic `_adb-tls-pairing._tcp` port.
    pub port: u16,
    /// Six-digit one-time code displayed by Android.
    pub pairing_code: SecretString,
    /// Comment embedded in the reusable ADB RSA public key.
    pub client_name: String,
}

/// Parameters for a native, non-TLS ADB connection over TCP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectConnectionRequest {
    /// IPv4, IPv6, or DNS host of the Android device.
    pub host: String,
    /// Fixed TCP port exposed by `adbd`.
    pub port: u16,
    /// Comment embedded in the reusable ADB RSA public key.
    pub client_name: String,
}

impl fmt::Debug for PairingRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PairingRequest")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("pairing_code", &"[REDACTED]")
            .field("client_name", &self.client_name)
            .finish()
    }
}

/// Stable metadata produced by a successful Android wireless pairing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairingResult {
    /// Device GUID advertised by Android's pairing service.
    pub device_id: String,
    /// SHA-256 fingerprint of the TLS certificate used during pairing.
    pub certificate_fingerprint: String,
}

/// Pairs an Android 11+ device and persists its reusable host identity.
///
/// # Errors
///
/// Returns a validation, timeout, TLS, protocol, incorrect-code, credential,
/// or persistence error. The pairing code and private key are never included
/// in returned diagnostics.
pub async fn pair_device(
    request: PairingRequest,
    credential_store: &dyn CredentialStore,
) -> Result<PairingResult, PairingError> {
    validate_request(&request)?;
    let credential = load_or_create_credential(&request.client_name, credential_store).await?;
    let identity = TlsIdentity::from_credential(&credential)?;

    let stream = match time::timeout(
        PAIRING_CONNECT_TIMEOUT,
        TcpStream::connect((request.host.as_str(), request.port)),
    )
    .await
    {
        Ok(result) => result.map_err(PairingError::Connection)?,
        Err(_) => return Err(PairingError::Timeout(PAIRING_CONNECT_TIMEOUT)),
    };
    stream.set_nodelay(true).map_err(PairingError::Connection)?;
    let mut stream = match time::timeout(
        PAIRING_CONNECT_TIMEOUT,
        connect_tls(stream, &request.host, &identity),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => return Err(PairingError::Timeout(PAIRING_CONNECT_TIMEOUT)),
    };

    let fingerprint = peer_certificate_fingerprint(&stream)?;
    let exporter = export_pairing_key(&stream)?;
    let mut password = Zeroizing::new(Vec::with_capacity(6 + exporter.len()));
    password.extend_from_slice(request.pairing_code.expose_secret().as_bytes());
    password.extend_from_slice(&*exporter);

    let (spake, our_spake_message) = Spake2State::start_client(&password)?;
    run_pairing_io(write_packet(
        &mut stream,
        PacketType::Spake2,
        &our_spake_message,
    ))
    .await?;
    let peer_spake_message = run_pairing_io(read_packet(&mut stream, PacketType::Spake2)).await?;
    let key_material = spake.finish(&peer_spake_message)?;
    let mut cipher = PairingCipher::new(&*key_material)?;

    let public_key = credential.public_key_payload()?;
    let peer_info = PeerInfo::adb_public_key(&public_key)?;
    let encrypted_peer_info = cipher.encrypt(peer_info.as_bytes())?;
    run_pairing_io(write_packet(
        &mut stream,
        PacketType::PeerInfo,
        &encrypted_peer_info,
    ))
    .await?;
    let encrypted_device_info =
        run_pairing_io(read_packet(&mut stream, PacketType::PeerInfo)).await?;
    let device_info = cipher.decrypt(&encrypted_device_info)?;
    if device_info.len() != PEER_INFO_SIZE {
        return Err(PairingError::Protocol(format!(
            "decrypted PeerInfo has length {}, expected {PEER_INFO_SIZE}",
            device_info.len()
        )));
    }
    let device_id = PeerInfo::device_guid(&device_info)?;

    let stored_device = StoredPairedDevice {
        device_id: device_id.clone(),
        host: request.host,
        certificate_fingerprint: fingerprint.clone(),
    };
    credential_store.save_paired_device(&stored_device).await?;
    Ok(PairingResult {
        device_id,
        certificate_fingerprint: fingerprint,
    })
}

/// Reconnects to a previously paired device using a known TLS connect port.
///
/// Task 8 supplies this dynamic endpoint through mDNS. Until then callers may
/// provide the port manually.
///
/// # Errors
///
/// Returns an error if the device or host identity was not persisted, or if
/// TCP, TLS, or the ADB connection handshake fails.
pub async fn connect_paired_device(
    device_id: &str,
    host: impl Into<String>,
    port: u16,
    credential_store: &dyn CredentialStore,
) -> Result<AdbClient, PairingError> {
    if credential_store
        .load_paired_device(device_id)
        .await?
        .is_none()
    {
        return Err(PairingError::DeviceNotPaired {
            device_id: device_id.to_owned(),
        });
    }
    let stored = credential_store
        .load_host_credential()
        .await?
        .ok_or(PairingError::MissingHostCredential)?;
    let credential = Arc::new(RsaAdbCredential::from_pkcs8_pem(
        stored.expose_private_key_pem(),
        stored.client_name(),
    )?);
    let transport =
        WirelessTransport::connect(host, port, &credential, WirelessTransportConfig::default())
            .await?;
    Ok(AdbClient::connect(Box::new(transport), credential).await?)
}

/// Connects directly to a TCP ADB endpoint without Android pairing TLS.
///
/// The reusable RSA host identity is still offered when Android requires
/// classic ADB authorization. No external ADB server or executable is used.
///
/// # Errors
///
/// Returns an error when the request is invalid, the host cannot be resolved,
/// protected credentials cannot be loaded, or the native ADB handshake fails.
pub async fn connect_tcp_device(
    request: DirectConnectionRequest,
    credential_store: &dyn CredentialStore,
) -> Result<AdbClient, PairingError> {
    validate_endpoint(&request.host, request.port)?;
    validate_client_name(&request.client_name)?;
    let credential =
        Arc::new(load_or_create_credential(&request.client_name, credential_store).await?);
    let addresses = lookup_host((request.host.as_str(), request.port))
        .await
        .map_err(PairingError::Connection)?;
    let mut last_error = None;
    for address in addresses {
        match TcpTransport::connect(address, TcpTransportConfig::default()).await {
            Ok(transport) => {
                return Ok(AdbClient::connect(Box::new(transport), credential).await?);
            }
            Err(error) => last_error = Some(error),
        }
    }
    match last_error {
        Some(error) => Err(PairingError::AdbClient(error.into())),
        None => Err(PairingError::InvalidEndpoint(
            "host resolved to no socket addresses".to_owned(),
        )),
    }
}

/// Connects to TCP ADB only when the device already trusts the saved host key.
///
/// This passive variant never offers a new public key, making it suitable for
/// background identity checks that must not trigger an authorization prompt.
///
/// # Errors
///
/// Returns an error when no host credential exists, the endpoint is invalid,
/// the existing key is not authorized, or the transport fails.
pub async fn connect_known_tcp_device(
    request: DirectConnectionRequest,
    credential_store: &dyn CredentialStore,
) -> Result<AdbClient, PairingError> {
    validate_endpoint(&request.host, request.port)?;
    validate_client_name(&request.client_name)?;
    let stored = credential_store
        .load_host_credential()
        .await?
        .ok_or(PairingError::MissingHostCredential)?;
    let credential = Arc::new(RsaAdbCredential::from_pkcs8_pem(
        stored.expose_private_key_pem(),
        stored.client_name(),
    )?);
    let addresses = lookup_host((request.host.as_str(), request.port))
        .await
        .map_err(PairingError::Connection)?;
    let mut last_error = None;
    for address in addresses {
        match TcpTransport::connect(address, TcpTransportConfig::default()).await {
            Ok(transport) => {
                return Ok(AdbClient::connect_authorized(Box::new(transport), credential).await?);
            }
            Err(error) => last_error = Some(error),
        }
    }
    match last_error {
        Some(error) => Err(PairingError::AdbClient(error.into())),
        None => Err(PairingError::InvalidEndpoint(
            "host resolved to no socket addresses".to_owned(),
        )),
    }
}

/// Pairs and then connects when the dynamic TLS connect port is already known.
///
/// # Errors
///
/// Returns any error from [`pair_device`] or [`connect_paired_device`].
pub async fn pair_and_connect(
    request: PairingRequest,
    connect_port: u16,
    credential_store: &dyn CredentialStore,
) -> Result<(PairingResult, AdbClient), PairingError> {
    let host = request.host.clone();
    let result = pair_device(request, credential_store).await?;
    let client =
        connect_paired_device(&result.device_id, host, connect_port, credential_store).await?;
    Ok((result, client))
}

fn validate_request(request: &PairingRequest) -> Result<(), PairingError> {
    let code = request.pairing_code.expose_secret().as_bytes();
    if code.len() != 6 || !code.iter().all(u8::is_ascii_digit) {
        return Err(PairingError::InvalidPairingCode);
    }
    validate_endpoint(&request.host, request.port)?;
    validate_client_name(&request.client_name)
}

fn validate_endpoint(host: &str, port: u16) -> Result<(), PairingError> {
    if host.trim().is_empty() || port == 0 {
        return Err(PairingError::InvalidEndpoint(
            "host must be non-empty and port must be non-zero".to_owned(),
        ));
    }
    Ok(())
}

fn validate_client_name(client_name: &str) -> Result<(), PairingError> {
    if client_name.is_empty()
        || client_name.len() > 128
        || client_name.chars().any(char::is_control)
    {
        return Err(PairingError::InvalidClientName);
    }
    Ok(())
}

async fn load_or_create_credential(
    client_name: &str,
    store: &dyn CredentialStore,
) -> Result<RsaAdbCredential, PairingError> {
    if let Some(stored) = store.load_host_credential().await? {
        return Ok(RsaAdbCredential::from_pkcs8_pem(
            stored.expose_private_key_pem(),
            stored.client_name(),
        )?);
    }

    let credential = RsaAdbCredential::generate(client_name.to_owned())?;
    let pem = credential.to_pkcs8_pem()?;
    let stored = StoredHostCredential::new(SecretString::from(pem.to_string()), client_name);
    store.save_host_credential(&stored).await?;
    Ok(credential)
}

async fn run_pairing_io<T>(
    future: impl std::future::Future<Output = Result<T, PairingError>>,
) -> Result<T, PairingError> {
    match time::timeout(PAIRING_IO_TIMEOUT, future).await {
        Ok(result) => result,
        Err(_) => Err(PairingError::Timeout(PAIRING_IO_TIMEOUT)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairing_request_debug_redacts_the_code() {
        let request = PairingRequest {
            host: "127.0.0.1".to_owned(),
            port: 37_001,
            pairing_code: SecretString::from("123456".to_owned()),
            client_name: "droidmux@test".to_owned(),
        };
        let debug = format!("{request:?}");
        assert!(!debug.contains("123456"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn only_six_ascii_digits_are_accepted() {
        for invalid in ["12345", "1234567", "12a456", "１２３４５６"] {
            let request = PairingRequest {
                host: "127.0.0.1".to_owned(),
                port: 37_001,
                pairing_code: SecretString::from(invalid.to_owned()),
                client_name: "droidmux@test".to_owned(),
            };
            assert!(matches!(
                validate_request(&request),
                Err(PairingError::InvalidPairingCode)
            ));
        }
    }
}
