use std::{collections::BTreeSet, fmt, str, sync::Arc, time::Duration};

use adb_auth::{ADB_AUTH_RSAPUBLICKEY, ADB_AUTH_SIGNATURE, ADB_AUTH_TOKEN, AdbAuthenticator};
use adb_protocol::{ADB_VERSION, AdbCommand, AdbPacket, MAX_PAYLOAD};
use adb_transport::{AdbTransport, AdbTransportError};
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{
    AdbClientError, AdbStream,
    session::{
        ReconnectOptions, ReconnectedTransport, SessionCommand, SessionControl, SessionHandle,
        SessionStatus, spawn_session,
    },
};

const HOST_FEATURES: [&str; 10] = [
    "shell_v2",
    "cmd",
    "stat_v2",
    "ls_v2",
    "sendrecv_v2",
    "sendrecv_v2_brotli",
    "sendrecv_v2_lz4",
    "sendrecv_v2_zstd",
    "abb",
    "abb_exec",
];
const MAX_SERVICE_RESPONSE: usize = 16 * 1024 * 1024;

/// AOSP's default delayed-ack receive window for one logical stream.
pub const DEFAULT_DELAYED_ACK_RECEIVE_WINDOW: u32 = 32 * 1024 * 1024;

/// Client-side protocol options negotiated during the ADB handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdbClientConfig {
    /// Enables ADB's delayed-ack/burst mode. This changes the host banner and
    /// permits multiple `WRTE` packets to be in flight on one stream.
    pub burst_mode: bool,
    /// Maximum unacknowledged device-to-host bytes buffered for each stream.
    ///
    /// This limit belongs to one client connection. Applications managing many
    /// devices can lower it independently for each device.
    pub delayed_ack_receive_window: u32,
}

impl Default for AdbClientConfig {
    fn default() -> Self {
        Self {
            burst_mode: false,
            delayed_ack_receive_window: DEFAULT_DELAYED_ACK_RECEIVE_WINDOW,
        }
    }
}

/// Retry policy used by [`AdbClient::connect_with_auto_reconnect`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdbReconnectConfig {
    /// Number of reconnect attempts after the initial connection fails.
    pub max_attempts: u32,
    /// Delay before the first retry.
    pub initial_delay: Duration,
    /// Upper bound for exponential backoff.
    pub max_delay: Duration,
}

impl Default for AdbReconnectConfig {
    fn default() -> Self {
        Self {
            max_attempts: 20,
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(3),
        }
    }
}

/// Creates a fresh raw transport for an automatic reconnect attempt.
#[async_trait]
pub trait AdbTransportFactory: Send + Sync {
    /// Opens a new transport to the same ADB peer.
    async fn connect(&self) -> Result<Box<dyn AdbTransport>, AdbTransportError>;
}

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
    /// The transport was lost and the client is retrying the handshake.
    Reconnecting,
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
        Self::connect_with_authenticators_and_config(
            transport,
            vec![authenticator],
            AdbClientConfig::default(),
        )
        .await
    }

    /// Establishes an ADB session with explicit protocol options.
    ///
    /// # Errors
    ///
    /// Returns an error when handshake, authentication, or transport setup fails.
    pub async fn connect_with_config(
        transport: Box<dyn AdbTransport>,
        authenticator: Arc<dyn AdbAuthenticator>,
        config: AdbClientConfig,
    ) -> Result<Self, AdbClientError> {
        Self::connect_with_authenticators_and_config(transport, vec![authenticator], config).await
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
        Self::connect_with_authenticators_and_config(
            transport,
            authenticators,
            AdbClientConfig::default(),
        )
        .await
    }

    /// Establishes a multi-key ADB session with explicit protocol options.
    ///
    /// # Errors
    ///
    /// Returns an error when no authenticator is supplied or handshake fails.
    pub async fn connect_with_authenticators_and_config(
        transport: Box<dyn AdbTransport>,
        authenticators: Vec<Arc<dyn AdbAuthenticator>>,
        config: AdbClientConfig,
    ) -> Result<Self, AdbClientError> {
        Self::connect_with_policy(transport, authenticators, true, config, |_| {}, None).await
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
        Self::connect_with_policy(
            transport,
            authenticators,
            false,
            AdbClientConfig::default(),
            |_| {},
            None,
        )
        .await
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
        Self::connect_with_policy(
            transport,
            authenticators,
            true,
            AdbClientConfig::default(),
            observer,
            None,
        )
        .await
    }

    /// Establishes a session that retries transport loss in the background.
    ///
    /// The raw transport factory is called again after a disconnect, and the
    /// ADB handshake is repeated with the supplied trusted identities. Streams
    /// that belonged to the failed connection are closed; new service opens
    /// work after [`ConnectionState::Connected`] is reported again.
    ///
    /// # Errors
    ///
    /// Returns an error when the initial factory connection or handshake fails.
    pub async fn connect_with_auto_reconnect(
        factory: Arc<dyn AdbTransportFactory>,
        authenticator: Arc<dyn AdbAuthenticator>,
        config: AdbClientConfig,
        reconnect: AdbReconnectConfig,
    ) -> Result<Self, AdbClientError> {
        Self::connect_with_auto_reconnect_and_authenticators(
            factory,
            vec![authenticator],
            config,
            reconnect,
        )
        .await
    }

    /// Multi-key variant of [`Self::connect_with_auto_reconnect`].
    ///
    /// # Errors
    ///
    /// Returns an error when the initial factory connection or handshake fails.
    pub async fn connect_with_auto_reconnect_and_authenticators(
        factory: Arc<dyn AdbTransportFactory>,
        authenticators: Vec<Arc<dyn AdbAuthenticator>>,
        config: AdbClientConfig,
        reconnect: AdbReconnectConfig,
    ) -> Result<Self, AdbClientError> {
        let transport = factory.connect().await?;
        let reconnect_factory = factory.clone();
        let reconnect_authenticators = authenticators.clone();
        let reconnect_options = ReconnectOptions {
            policy: reconnect,
            connector: Arc::new(move || {
                let factory = reconnect_factory.clone();
                let authenticators = reconnect_authenticators.clone();
                Box::pin(async move {
                    let transport = factory.connect().await?;
                    let connection =
                        establish_connection(transport, authenticators, true, config, |_| {})
                            .await?;
                    Ok(ReconnectedTransport {
                        transport: connection.transport,
                        max_payload: connection.max_payload,
                        delayed_ack: connection.delayed_ack,
                    })
                })
            }),
        };
        Self::connect_with_policy(
            transport,
            authenticators,
            true,
            config,
            |_| {},
            Some(reconnect_options),
        )
        .await
    }

    async fn connect_with_policy<F>(
        transport: Box<dyn AdbTransport>,
        authenticators: Vec<Arc<dyn AdbAuthenticator>>,
        offer_public_key: bool,
        config: AdbClientConfig,
        mut observer: F,
        reconnect: Option<ReconnectOptions>,
    ) -> Result<Self, AdbClientError>
    where
        F: FnMut(ConnectionState) + Send,
    {
        if config.burst_mode && config.delayed_ack_receive_window == 0 {
            return Err(AdbClientError::InvalidDelayedAckWindow);
        }
        let connection = establish_connection(
            transport,
            authenticators,
            offer_public_key,
            config,
            &mut observer,
        )
        .await?;
        observer(ConnectionState::Connected);
        let session = spawn_session(
            connection.transport,
            connection.max_payload,
            connection.delayed_ack,
            connection.delayed_ack,
            config.delayed_ack_receive_window,
            reconnect,
        );
        Ok(Self {
            session,
            protocol_version: connection.protocol_version,
            max_payload: connection.max_payload,
            device_banner: connection.device_banner,
            features: connection.features,
            peer_description: connection.peer_description,
        })
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
            self.session.burst_mode,
            self.session.commands.clone(),
            self.session.controls.clone(),
            opened,
        ))
    }

    /// Opens the raw Android Binder Bridge service.
    ///
    /// The returned stream is intentionally untyped because `abb` carries
    /// Android's versioned protobuf commands rather than an ADB wire format.
    ///
    /// # Errors
    ///
    /// Returns an error when the device does not expose `abb` or the session
    /// is no longer connected.
    pub async fn open_abb(&self) -> Result<AdbStream, AdbClientError> {
        self.open_service("abb:").await
    }

    /// Opens the command-oriented Android Binder Bridge service.
    ///
    /// `command` is encoded in the service name using the same form as the
    /// platform ADB client: `abb_exec:<command>`.
    ///
    /// # Errors
    ///
    /// Returns an error when the command contains a NUL byte, exceeds the
    /// negotiated service-name limit, or the peer rejects the service.
    pub async fn open_abb_exec(&self, command: &str) -> Result<AdbStream, AdbClientError> {
        if command.is_empty() || command.as_bytes().contains(&0) {
            return Err(AdbClientError::InvalidServiceName);
        }
        self.open_service(&format!("abb_exec:{command}")).await
    }

    /// Executes an `abb_exec:<command>` request and collects its response.
    ///
    /// This is a bounded convenience API. Use [`Self::open_abb_exec`] when a
    /// command has a large or long-lived response.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is invalid, the device rejects the
    /// service, or the response exceeds the bounded collection limit.
    pub async fn abb_exec(&self, command: &str) -> Result<Bytes, AdbClientError> {
        let stream = self.open_abb_exec(command).await?;
        read_stream_response(stream).await
    }

    /// Executes the adbd root service and returns its textual response.
    ///
    /// # Errors
    ///
    /// Returns an error when the device rejects the service or the session
    /// disconnects while reading the response.
    pub async fn root(&self) -> Result<Bytes, AdbClientError> {
        self.read_service("root:").await
    }

    /// Executes the adbd unroot service and returns its textual response.
    ///
    /// # Errors
    ///
    /// Returns an error when the device rejects the service or the session
    /// disconnects while reading the response.
    pub async fn unroot(&self) -> Result<Bytes, AdbClientError> {
        self.read_service("unroot:").await
    }

    /// Requests that adbd reconnect its underlying transport.
    ///
    /// A successful request can close the current transport as part of the
    /// daemon restart. Callers should treat the client as unusable after this
    /// method returns and establish a new [`AdbClient`] connection.
    ///
    /// # Errors
    ///
    /// Returns an error when the device rejects the service or the session
    /// disconnects before its response is received.
    pub async fn reconnect(&self) -> Result<Bytes, AdbClientError> {
        self.read_service("reconnect").await
    }

    /// Opens the raw framebuffer service.
    ///
    /// # Errors
    ///
    /// Returns an error when the service is rejected or the session is closed.
    pub async fn open_framebuffer(&self) -> Result<AdbStream, AdbClientError> {
        self.open_service("framebuffer:").await
    }

    /// Requests a daemon remount and returns its response.
    ///
    /// # Errors
    ///
    /// Returns an error when adbd rejects the request or the session is closed.
    pub async fn remount(&self) -> Result<Bytes, AdbClientError> {
        self.read_service("remount:").await
    }

    /// Requests a device reboot. This method only constructs and sends the
    /// request; callers must explicitly opt into this destructive operation.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid target or a failed service request.
    pub async fn reboot(&self, target: Option<&str>) -> Result<Bytes, AdbClientError> {
        self.read_service(&parameterized_service("reboot:", target)?)
            .await
    }

    /// Opens the backup service with the caller-provided argument string.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid argument or a failed service open.
    pub async fn open_backup(&self, arguments: &str) -> Result<AdbStream, AdbClientError> {
        self.open_service(&parameterized_service("backup:", Some(arguments))?)
            .await
    }

    /// Opens the restore service.
    ///
    /// # Errors
    ///
    /// Returns an error when the service is rejected or the session is closed.
    pub async fn open_restore(&self) -> Result<AdbStream, AdbClientError> {
        self.open_service("restore:").await
    }

    /// Disables dm-verity and returns adbd's response.
    ///
    /// # Errors
    ///
    /// Returns an error when adbd rejects the request or the session is closed.
    pub async fn disable_verity(&self) -> Result<Bytes, AdbClientError> {
        self.read_service("disable-verity:").await
    }

    /// Enables dm-verity and returns adbd's response.
    ///
    /// # Errors
    ///
    /// Returns an error when adbd rejects the request or the session is closed.
    pub async fn enable_verity(&self) -> Result<Bytes, AdbClientError> {
        self.read_service("enable-verity:").await
    }

    /// Requests adbd to switch to TCP transport on `port`.
    ///
    /// # Errors
    ///
    /// Returns an error when adbd rejects the request or the session is closed.
    pub async fn tcpip(&self, port: u16) -> Result<Bytes, AdbClientError> {
        self.read_service(&format!("tcpip:{port}")).await
    }

    /// Requests adbd to switch back to USB transport.
    ///
    /// # Errors
    ///
    /// Returns an error when adbd rejects the request or the session is closed.
    pub async fn usb(&self) -> Result<Bytes, AdbClientError> {
        self.read_service("usb:").await
    }

    /// Opens a device Unix-domain socket at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error when the service is rejected or the session is closed.
    pub async fn open_dev(&self, path: &str) -> Result<AdbStream, AdbClientError> {
        self.open_service(&parameterized_service("dev:", Some(path))?)
            .await
    }

    /// Opens a raw device Unix-domain socket at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error when the service is rejected or the session is closed.
    pub async fn open_dev_raw(&self, path: &str) -> Result<AdbStream, AdbClientError> {
        self.open_service(&parameterized_service("dev-raw:", Some(path))?)
            .await
    }

    /// Opens the JDWP process tracker.
    ///
    /// # Errors
    ///
    /// Returns an error when the service is rejected or the session is closed.
    pub async fn open_jdwp(&self) -> Result<AdbStream, AdbClientError> {
        self.open_service("jdwp").await
    }

    /// Opens a JDWP connection for a process ID.
    ///
    /// # Errors
    ///
    /// Returns an error when the service is rejected or the session is closed.
    pub async fn open_jdwp_process(&self, pid: u32) -> Result<AdbStream, AdbClientError> {
        self.open_service(&format!("jdwp:{pid}")).await
    }

    /// Opens a long-lived JDWP tracker.
    ///
    /// # Errors
    ///
    /// Returns an error when the service is rejected or the session is closed.
    pub async fn open_track_jdwp(&self) -> Result<AdbStream, AdbClientError> {
        self.open_service("track-jdwp").await
    }

    /// Opens a long-lived application tracker.
    ///
    /// # Errors
    ///
    /// Returns an error when the service is rejected or the session is closed.
    pub async fn open_track_app(&self) -> Result<AdbStream, AdbClientError> {
        self.open_service("track-app").await
    }

    /// Opens the raw sync service.
    ///
    /// # Errors
    ///
    /// Returns an error when the service is rejected or the session is closed.
    pub async fn open_sync(&self) -> Result<AdbStream, AdbClientError> {
        self.open_service("sync:").await
    }

    /// Opens a shell v2 service. The shell crate provides typed framing on top
    /// of the returned ADB stream.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid command or a failed service open.
    pub async fn open_shell(&self, command: &str) -> Result<AdbStream, AdbClientError> {
        self.open_service(&parameterized_service("shell:", Some(command))?)
            .await
    }

    /// Opens an `exec:` service without a PTY.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid command or a failed service open.
    pub async fn open_exec(&self, command: &str) -> Result<AdbStream, AdbClientError> {
        self.open_service(&parameterized_service("exec:", Some(command))?)
            .await
    }

    /// Opens the daemon's keep-alive spin service.
    ///
    /// # Errors
    ///
    /// Returns an error when the service is rejected or the session is closed.
    pub async fn open_spin(&self) -> Result<AdbStream, AdbClientError> {
        self.open_service("spin").await
    }

    /// Opens a fixed-size sink service.
    ///
    /// # Errors
    ///
    /// Returns an error when the service is rejected or the session is closed.
    pub async fn open_sink(&self, bytes: u64) -> Result<AdbStream, AdbClientError> {
        self.open_service(&format!("sink:{bytes}")).await
    }

    /// Opens a fixed-size source service.
    ///
    /// # Errors
    ///
    /// Returns an error when the service is rejected or the session is closed.
    pub async fn open_source(&self, bytes: u64) -> Result<AdbStream, AdbClientError> {
        self.open_service(&format!("source:{bytes}")).await
    }

    /// Creates a reverse tunnel from a device endpoint to a host endpoint.
    ///
    /// The endpoint strings use ADB's normal forms, for example
    /// `tcp:27183` and `localabstract:my-socket`.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid endpoints, a device `FAIL` response, or a
    /// disconnected session.
    pub async fn reverse_forward(&self, local: &str, remote: &str) -> Result<(), AdbClientError> {
        let response = self
            .read_service(&reverse_service("forward", local, Some(remote))?)
            .await?;
        reverse_okay(response.as_ref()).map(|_| ())
    }

    /// Removes one device reverse tunnel.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid endpoint, a device `FAIL` response, or
    /// a disconnected session.
    pub async fn reverse_kill_forward(&self, local: &str) -> Result<(), AdbClientError> {
        let response = self
            .read_service(&reverse_service("killforward", local, None)?)
            .await?;
        reverse_okay(response.as_ref()).map(|_| ())
    }

    /// Removes all device reverse tunnels owned by this ADB connection.
    ///
    /// # Errors
    ///
    /// Returns an error for a device `FAIL` response or a disconnected
    /// session.
    pub async fn reverse_kill_forward_all(&self) -> Result<(), AdbClientError> {
        let response = self.read_service("reverse:killforward-all").await?;
        reverse_okay(response.as_ref()).map(|_| ())
    }

    /// Lists reverse tunnels as returned by adbd's `reverse:list-forward`
    /// service. The response body is left in the platform's line format.
    ///
    /// # Errors
    ///
    /// Returns an error for a device `FAIL` response, an invalid response, or
    /// a disconnected session.
    pub async fn reverse_list_forward(&self) -> Result<Bytes, AdbClientError> {
        let response = self.read_service("reverse:list-forward").await?;
        let body = reverse_okay(response.as_ref())?;
        reverse_length_prefixed(body.as_ref())
    }

    /// Opens a service, reads its bounded response, and closes its stream.
    ///
    /// This is useful for small request/response services such as `root:`,
    /// `unroot:`, `reconnect:`, and `reverse:`. Responses are capped at 16 MiB
    /// to keep a malformed or compromised device from causing an unbounded
    /// allocation.
    ///
    /// # Errors
    ///
    /// Returns an error when the service name is invalid, the stream fails, or
    /// the response exceeds 16 MiB.
    pub async fn read_service(&self, service: &str) -> Result<Bytes, AdbClientError> {
        let stream = self.open_service(service).await?;
        read_stream_response(stream).await
    }
}

async fn read_stream_response(stream: AdbStream) -> Result<Bytes, AdbClientError> {
    let mut response = Vec::new();
    while let Some(payload) = stream.read().await? {
        let new_len = response.len().checked_add(payload.len()).ok_or(
            AdbClientError::ServiceResponseTooLarge {
                limit: MAX_SERVICE_RESPONSE,
            },
        )?;
        if new_len > MAX_SERVICE_RESPONSE {
            return Err(AdbClientError::ServiceResponseTooLarge {
                limit: MAX_SERVICE_RESPONSE,
            });
        }
        response.extend_from_slice(&payload);
    }
    stream.close().await?;
    Ok(Bytes::from(response))
}

struct EstablishedConnection {
    transport: Box<dyn AdbTransport>,
    protocol_version: u32,
    max_payload: usize,
    device_banner: String,
    features: BTreeSet<String>,
    peer_description: String,
    delayed_ack: bool,
}

async fn establish_connection<F>(
    mut transport: Box<dyn AdbTransport>,
    authenticators: Vec<Arc<dyn AdbAuthenticator>>,
    offer_public_key: bool,
    config: AdbClientConfig,
    mut observer: F,
) -> Result<EstablishedConnection, AdbClientError>
where
    F: FnMut(ConnectionState) + Send,
{
    let default_authenticator = authenticators
        .first()
        .ok_or(AdbClientError::NoAuthenticators)?;
    observer(ConnectionState::Connecting);
    transport
        .write_packet(&host_connect_packet(config)?)
        .await?;

    let mut next_authenticator = 0;
    let mut public_key_sent = false;
    loop {
        let packet = transport.read_packet().await?;
        match packet.command {
            AdbCommand::Connect => {
                let (protocol_version, max_payload, device_banner, features) =
                    negotiate_connection(&packet, config)?;
                let peer_description = transport.peer_description();
                let delayed_ack =
                    config.burst_mode && features.iter().any(|feature| feature == "delayed_ack");
                return Ok(EstablishedConnection {
                    transport,
                    protocol_version,
                    max_payload,
                    device_banner,
                    features,
                    peer_description,
                    delayed_ack,
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

impl AdbClient {
    /// Returns the current connection state.
    #[must_use]
    pub fn state(&self) -> ConnectionState {
        match &*self.session.status.borrow() {
            SessionStatus::Connected => ConnectionState::Connected,
            SessionStatus::Reconnecting => ConnectionState::Reconnecting,
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
            SessionStatus::Connected | SessionStatus::Reconnecting => {}
        }

        let (response_sender, response_receiver) = oneshot::channel();
        self.session
            .controls
            .send(SessionControl::CloseSession {
                response: response_sender,
            })
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

fn host_connect_packet(config: AdbClientConfig) -> Result<AdbPacket, AdbClientError> {
    let max_payload =
        u32::try_from(MAX_PAYLOAD).map_err(|_| AdbClientError::InvalidLocalPayloadLimit)?;
    let mut banner = String::from(
        "host::features=shell_v2,cmd,stat_v2,ls_v2,sendrecv_v2,sendrecv_v2_brotli,sendrecv_v2_lz4,sendrecv_v2_zstd,abb,abb_exec",
    );
    if config.burst_mode {
        banner.push_str(",delayed_ack");
    }
    Ok(AdbPacket::new(
        AdbCommand::Connect,
        ADB_VERSION,
        max_payload,
        Bytes::from(banner),
    )?)
}

fn negotiate_connection(
    packet: &AdbPacket,
    config: AdbClientConfig,
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
    let mut features: BTreeSet<String> = HOST_FEATURES
        .into_iter()
        .filter(|feature| device_features.contains(*feature))
        .map(str::to_owned)
        .collect();
    if config.burst_mode && device_features.contains("delayed_ack") {
        features.insert("delayed_ack".to_owned());
    }

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

fn parameterized_service(prefix: &str, parameter: Option<&str>) -> Result<String, AdbClientError> {
    if parameter.is_some_and(|value| value.as_bytes().contains(&0)) {
        return Err(AdbClientError::InvalidServiceName);
    }
    Ok(match parameter {
        Some(value) => format!("{prefix}{value}"),
        None => prefix.to_owned(),
    })
}

fn reverse_service(
    operation: &str,
    remote: &str,
    local: Option<&str>,
) -> Result<String, AdbClientError> {
    if remote.is_empty()
        || remote.as_bytes().contains(&0)
        || remote.as_bytes().contains(&b';')
        || local.is_some_and(|value| {
            value.is_empty() || value.as_bytes().contains(&0) || value.as_bytes().contains(&b';')
        })
    {
        return Err(AdbClientError::InvalidServiceName);
    }

    Ok(match local {
        Some(local) => format!("reverse:{operation}:{remote};{local}"),
        None => format!("reverse:{operation}:{remote}"),
    })
}

fn reverse_okay(response: &[u8]) -> Result<Bytes, AdbClientError> {
    if response.len() < 4 {
        return Err(AdbClientError::InvalidServiceResponse);
    }
    match &response[..4] {
        b"OKAY" => Ok(Bytes::copy_from_slice(&response[4..])),
        b"FAIL" => {
            let message = if response.len() >= 8 {
                let length = std::str::from_utf8(&response[4..8])
                    .ok()
                    .and_then(|value| usize::from_str_radix(value, 16).ok());
                match length {
                    Some(length) if response.len() >= 8 + length => {
                        String::from_utf8_lossy(&response[8..8 + length]).into_owned()
                    }
                    _ => String::from_utf8_lossy(&response[4..]).into_owned(),
                }
            } else {
                String::new()
            };
            Err(AdbClientError::ServiceRequestFailed { message })
        }
        _ => Err(AdbClientError::InvalidServiceResponse),
    }
}

fn reverse_length_prefixed(response: &[u8]) -> Result<Bytes, AdbClientError> {
    if response.len() < 4 {
        return Err(AdbClientError::InvalidServiceResponse);
    }
    let length = std::str::from_utf8(&response[..4])
        .ok()
        .and_then(|value| usize::from_str_radix(value, 16).ok())
        .ok_or(AdbClientError::InvalidServiceResponse)?;
    if response.len() < 4 + length {
        return Err(AdbClientError::InvalidServiceResponse);
    }
    Ok(Bytes::copy_from_slice(&response[4..4 + length]))
}

#[cfg(test)]
mod service_tests {
    use super::{reverse_length_prefixed, reverse_okay, reverse_service};
    use crate::AdbClientError;

    #[test]
    fn builds_reverse_service_names() {
        assert_eq!(
            reverse_service("forward", "tcp:27183", Some("tcp:27184")).expect("valid endpoints"),
            "reverse:forward:tcp:27183;tcp:27184"
        );
        assert_eq!(
            reverse_service("killforward", "tcp:27183", None).expect("valid endpoint"),
            "reverse:killforward:tcp:27183"
        );
    }

    #[test]
    fn rejects_ambiguous_reverse_endpoints() {
        assert!(matches!(
            reverse_service("forward", "tcp:1;bad", Some("tcp:2")),
            Err(AdbClientError::InvalidServiceName)
        ));
    }

    #[test]
    fn parses_reverse_success_and_failure() {
        assert_eq!(
            reverse_okay(b"OKAYtcp:27183")
                .expect("success response")
                .as_ref(),
            b"tcp:27183"
        );
        assert!(matches!(
            reverse_okay(b"FAIL0004nope"),
            Err(AdbClientError::ServiceRequestFailed { message }) if message == "nope"
        ));
        assert_eq!(
            reverse_length_prefixed(b"000btcp:1 tcp:2")
                .expect("length-prefixed list")
                .as_ref(),
            b"tcp:1 tcp:2"
        );
    }
}
