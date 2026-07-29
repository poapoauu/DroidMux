use std::{collections::BTreeSet, fmt, str, sync::Arc};

use adb_auth::{ADB_AUTH_RSAPUBLICKEY, ADB_AUTH_SIGNATURE, ADB_AUTH_TOKEN, AdbAuthenticator};
use adb_protocol::{ADB_VERSION, AdbCommand, AdbPacket, MAX_PAYLOAD};
use adb_transport::AdbTransport;
use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{
    AdbClientError, AdbStream,
    session::{SessionCommand, SessionHandle, SessionStatus, spawn_session},
};

const HOST_FEATURES: [&str; 5] = ["shell_v2", "cmd", "stat_v2", "ls_v2", "sendrecv_v2"];
const HOST_BANNER: &[u8] = b"host::features=shell_v2,cmd,stat_v2,ls_v2,sendrecv_v2";

/// High-level state of an ADB client connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// The host is sending its connection banner.
    Connecting,
    /// The host is signing a device authentication token.
    Authorizing,
    /// The public key was sent and the device is waiting for user approval.
    AwaitingAuthorization,
    /// The handshake completed successfully.
    Connected,
    /// The transport has been closed.
    Closed,
}

/// An authenticated ADB client that multiplexes logical service streams.
pub struct AdbClient {
    session: SessionHandle,
    protocol_version: u32,
    max_payload: usize,
    device_banner: String,
    features: BTreeSet<String>,
    peer_description: String,
}

impl AdbClient {
    /// Establishes an ADB session using a reusable host identity.
    ///
    /// # Errors
    ///
    /// Returns an error when transport, packet validation, authentication, or
    /// connection-banner negotiation fails.
    pub async fn connect(
        transport: Box<dyn AdbTransport>,
        authenticator: Arc<dyn AdbAuthenticator>,
    ) -> Result<Self, AdbClientError> {
        Self::connect_with_authenticators(transport, vec![authenticator]).await
    }

    /// Establishes an ADB session by trying reusable host identities in order.
    ///
    /// Each device `AUTH TOKEN` advances to the next authenticator. If every
    /// signature is rejected, the first authenticator's public key is offered
    /// for interactive authorization.
    ///
    /// # Errors
    ///
    /// Returns an error when no authenticator is supplied, or when transport,
    /// packet validation, authentication, or connection negotiation fails.
    pub async fn connect_with_authenticators(
        transport: Box<dyn AdbTransport>,
        authenticators: Vec<Arc<dyn AdbAuthenticator>>,
    ) -> Result<Self, AdbClientError> {
        Self::connect_with_authenticators_and_observer(transport, authenticators, |_| {}).await
    }

    /// Establishes an ADB session only when the device already trusts the host key.
    ///
    /// Unlike [`Self::connect`], this method never offers the public key for new
    /// authorization, so background discovery cannot trigger an Android prompt.
    ///
    /// # Errors
    ///
    /// Returns [`AdbClientError::AuthorizationRejected`] when the stored key is
    /// not already authorized, in addition to normal transport and protocol errors.
    pub async fn connect_authorized(
        transport: Box<dyn AdbTransport>,
        authenticator: Arc<dyn AdbAuthenticator>,
    ) -> Result<Self, AdbClientError> {
        Self::connect_authorized_with_authenticators(transport, vec![authenticator]).await
    }

    /// Tries trusted host identities in order without requesting new approval.
    ///
    /// This variant is suitable for passive discovery because it never sends a
    /// public key that could trigger an Android authorization prompt.
    ///
    /// # Errors
    ///
    /// Returns [`AdbClientError::NoAuthenticators`] for an empty list and
    /// [`AdbClientError::AuthorizationRejected`] after every signature is
    /// rejected, in addition to normal transport and protocol errors.
    pub async fn connect_authorized_with_authenticators(
        transport: Box<dyn AdbTransport>,
        authenticators: Vec<Arc<dyn AdbAuthenticator>>,
    ) -> Result<Self, AdbClientError> {
        Self::connect_with_policy(transport, authenticators, false, |_| {}).await
    }

    /// Establishes an ADB session and reports each handshake state transition.
    ///
    /// The observer is called synchronously and must not perform blocking work.
    ///
    /// # Errors
    ///
    /// Returns an error when transport, packet validation, authentication, or
    /// connection-banner negotiation fails.
    pub async fn connect_with_observer<F>(
        transport: Box<dyn AdbTransport>,
        authenticator: Arc<dyn AdbAuthenticator>,
        observer: F,
    ) -> Result<Self, AdbClientError>
    where
        F: FnMut(ConnectionState) + Send,
    {
        Self::connect_with_authenticators_and_observer(transport, vec![authenticator], observer)
            .await
    }

    /// Establishes a multi-key ADB session and reports handshake transitions.
    ///
    /// Authenticators are tried in order. The first authenticator supplies the
    /// public key if all signatures are rejected. The observer is called
    /// synchronously and must not perform blocking work.
    ///
    /// # Errors
    ///
    /// Returns an error when no authenticator is supplied, or when transport,
    /// packet validation, authentication, or connection negotiation fails.
    pub async fn connect_with_authenticators_and_observer<F>(
        transport: Box<dyn AdbTransport>,
        authenticators: Vec<Arc<dyn AdbAuthenticator>>,
        observer: F,
    ) -> Result<Self, AdbClientError>
    where
        F: FnMut(ConnectionState) + Send,
    {
        Self::connect_with_policy(transport, authenticators, true, observer).await
    }

    async fn connect_with_policy<F>(
        mut transport: Box<dyn AdbTransport>,
        authenticators: Vec<Arc<dyn AdbAuthenticator>>,
        offer_public_key: bool,
        mut observer: F,
    ) -> Result<Self, AdbClientError>
    where
        F: FnMut(ConnectionState) + Send,
    {
        let default_authenticator = authenticators
            .first()
            .ok_or(AdbClientError::NoAuthenticators)?;
        observer(ConnectionState::Connecting);
        transport.write_packet(&host_connect_packet()?).await?;

        let mut next_authenticator = 0;
        let mut public_key_sent = false;
        loop {
            let packet = transport.read_packet().await?;
            match packet.command {
                AdbCommand::Connect => {
                    let (protocol_version, max_payload, device_banner, features) =
                        negotiate_connection(&packet)?;
                    observer(ConnectionState::Connected);
                    let peer_description = transport.peer_description();
                    let session = spawn_session(transport, max_payload);
                    return Ok(Self {
                        session,
                        protocol_version,
                        max_payload,
                        device_banner,
                        features,
                        peer_description,
                    });
                }
                AdbCommand::Auth => {
                    if packet.arg0 != ADB_AUTH_TOKEN {
                        return Err(AdbClientError::UnexpectedAuthType {
                            actual: packet.arg0,
                        });
                    }

                    if let Some(authenticator) = authenticators.get(next_authenticator) {
                        if next_authenticator == 0 {
                            observer(ConnectionState::Authorizing);
                        }
                        let signature = authenticator.sign_token(&packet.payload)?;
                        let response =
                            AdbPacket::new(AdbCommand::Auth, ADB_AUTH_SIGNATURE, 0, signature)?;
                        transport.write_packet(&response).await?;
                        next_authenticator += 1;
                    } else if offer_public_key && !public_key_sent {
                        observer(ConnectionState::AwaitingAuthorization);
                        let public_key = default_authenticator.public_key_payload()?;
                        let response =
                            AdbPacket::new(AdbCommand::Auth, ADB_AUTH_RSAPUBLICKEY, 0, public_key)?;
                        transport.write_packet(&response).await?;
                        public_key_sent = true;
                    } else {
                        return Err(AdbClientError::AuthorizationRejected);
                    }
                }
                command => return Err(AdbClientError::UnexpectedPacket { command }),
            }
        }
    }

    /// Opens a logical ADB service stream.
    ///
    /// The service name is NUL-terminated on the wire. Multiple calls may run
    /// concurrently and are routed by independent local and remote IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if the name is invalid, the peer rejects the service,
    /// or the underlying session disconnects.
    pub async fn open_service(&self, service: &str) -> Result<AdbStream, AdbClientError> {
        if service.is_empty() || service.as_bytes().contains(&0) {
            return Err(AdbClientError::InvalidServiceName);
        }
        let payload_len =
            service
                .len()
                .checked_add(1)
                .ok_or(AdbClientError::ServiceNameTooLong {
                    limit: self.max_payload,
                    actual: usize::MAX,
                })?;
        if payload_len > self.max_payload {
            return Err(AdbClientError::ServiceNameTooLong {
                limit: self.max_payload,
                actual: payload_len,
            });
        }
        self.ensure_connected()?;

        let mut payload = Vec::with_capacity(payload_len);
        payload.extend_from_slice(service.as_bytes());
        payload.push(0);

        let (response_sender, response_receiver) = oneshot::channel();
        self.session
            .commands
            .send(SessionCommand::Open {
                service: Bytes::from(payload),
                response: response_sender,
            })
            .await
            .map_err(|_| self.current_session_error())?;

        let opened = response_receiver
            .await
            .unwrap_or_else(|_| Err(self.current_session_error()))?;
        Ok(AdbStream::new(
            service.to_owned(),
            self.max_payload,
            self.session.commands.clone(),
            opened,
        ))
    }

    /// Returns the current connection state.
    #[must_use]
    pub fn state(&self) -> ConnectionState {
        match &*self.session.status.borrow() {
            SessionStatus::Connected => ConnectionState::Connected,
            SessionStatus::Closed | SessionStatus::Disconnected(_) => ConnectionState::Closed,
        }
    }

    /// Returns the negotiated ADB protocol version.
    #[must_use]
    pub const fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    /// Returns the negotiated maximum packet payload.
    #[must_use]
    pub const fn max_payload(&self) -> usize {
        self.max_payload
    }

    /// Returns the device connection banner without its terminating NUL byte.
    #[must_use]
    pub fn device_banner(&self) -> &str {
        &self.device_banner
    }

    /// Returns whether both this client and the device implement an exact feature.
    #[must_use]
    pub fn supports_feature(&self, feature: &str) -> bool {
        self.features.contains(feature)
    }

    /// Iterates over features implemented by both this client and the device.
    pub fn features(&self) -> impl Iterator<Item = &str> {
        self.features.iter().map(String::as_str)
    }

    /// Returns the transport's peer description captured during connection.
    #[must_use]
    pub fn peer_description(&self) -> &str {
        &self.peer_description
    }

    /// Closes the session and notifies every open stream.
    ///
    /// Repeated calls are harmless.
    ///
    /// # Errors
    ///
    /// Returns a transport error if the first shutdown attempt fails.
    pub async fn close(&self) -> Result<(), AdbClientError> {
        match &*self.session.status.borrow() {
            SessionStatus::Closed => return Ok(()),
            SessionStatus::Disconnected(reason) => {
                return Err(AdbClientError::SessionClosed {
                    reason: reason.clone(),
                });
            }
            SessionStatus::Connected => {}
        }

        let (response_sender, response_receiver) = oneshot::channel();
        self.session
            .commands
            .send(SessionCommand::CloseSession {
                response: response_sender,
            })
            .await
            .map_err(|_| self.current_session_error())?;
        response_receiver
            .await
            .unwrap_or_else(|_| Err(self.current_session_error()))
    }

    fn ensure_connected(&self) -> Result<(), AdbClientError> {
        if matches!(*self.session.status.borrow(), SessionStatus::Connected) {
            Ok(())
        } else {
            Err(self.current_session_error())
        }
    }

    fn current_session_error(&self) -> AdbClientError {
        AdbClientError::SessionClosed {
            reason: self
                .session
                .status
                .borrow()
                .closure_reason()
                .unwrap_or_else(|| "session task stopped".to_owned()),
        }
    }
}

impl fmt::Debug for AdbClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdbClient")
            .field("state", &self.state())
            .field("protocol_version", &self.protocol_version)
            .field("max_payload", &self.max_payload)
            .field("device_banner", &self.device_banner)
            .field("peer_description", &self.peer_description)
            .finish_non_exhaustive()
    }
}

fn host_connect_packet() -> Result<AdbPacket, AdbClientError> {
    let max_payload =
        u32::try_from(MAX_PAYLOAD).map_err(|_| AdbClientError::InvalidLocalPayloadLimit)?;
    Ok(AdbPacket::new(
        AdbCommand::Connect,
        ADB_VERSION,
        max_payload,
        Bytes::from_static(HOST_BANNER),
    )?)
}

fn negotiate_connection(
    packet: &AdbPacket,
) -> Result<(u32, usize, String, BTreeSet<String>), AdbClientError> {
    let remote_payload_limit =
        usize::try_from(packet.arg1).map_err(|_| AdbClientError::InvalidPayloadLimit {
            actual: packet.arg1,
        })?;
    if remote_payload_limit == 0 {
        return Err(AdbClientError::InvalidPayloadLimit { actual: 0 });
    }

    let banner_bytes = packet.payload.strip_suffix(&[0]).unwrap_or(&packet.payload);
    if banner_bytes.is_empty() || banner_bytes.contains(&0) {
        return Err(AdbClientError::InvalidDeviceBanner);
    }
    let device_banner = str::from_utf8(banner_bytes)
        .map_err(|_| AdbClientError::InvalidDeviceBanner)?
        .to_owned();
    let device_features = parse_features(&device_banner);
    let features = HOST_FEATURES
        .into_iter()
        .filter(|feature| device_features.contains(*feature))
        .map(str::to_owned)
        .collect();

    Ok((
        packet.arg0.min(ADB_VERSION),
        remote_payload_limit.min(MAX_PAYLOAD),
        device_banner,
        features,
    ))
}

fn parse_features(banner: &str) -> BTreeSet<&str> {
    banner
        .split(';')
        .map(|property| property.strip_prefix("device::").unwrap_or(property))
        .filter_map(|property| property.strip_prefix("features="))
        .flat_map(|features| features.split(','))
        .map(str::trim)
        .filter(|feature| !feature.is_empty())
        .collect()
}
