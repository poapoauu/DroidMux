//! Bounded local TCP forwarding over a direct ADB connection.
//!
//! Each accepted local socket opens an independent `tcp:<port>` logical ADB
//! stream. The listener, connection count, and copy buffers are bounded, and
//! no external `adb` executable or adb server is used.

use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use adb_client::{AdbClient, AdbClientError, AdbStream, AdbStreamError};
use bytes::Bytes;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    sync::{Semaphore, broadcast, watch},
    task::JoinSet,
};

/// Default number of simultaneous local connections accepted by a forwarder.
pub const DEFAULT_MAX_CONNECTIONS: usize = 32;
/// Default memory allocated for each direction of one active connection.
pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;
/// Default number of lifecycle events retained for subscribers.
pub const DEFAULT_EVENT_CAPACITY: usize = 128;

const MAX_CONNECTIONS: usize = 4096;
const MAX_BUFFER_SIZE: usize = 1024 * 1024;
const MAX_EVENT_CAPACITY: usize = 4096;

/// Resource and timeout limits for one local TCP forwarder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardConfig {
    /// Maximum number of active local sockets.
    pub max_connections: usize,
    /// Per-direction copy-buffer size.
    pub buffer_size: usize,
    /// Maximum duration allowed for opening `tcp:<port>` on the device.
    pub open_timeout: Duration,
    /// Number of connection lifecycle events retained for subscribers.
    pub event_capacity: usize,
    /// Whether to disable Nagle's algorithm for accepted local sockets.
    pub no_delay: bool,
}

impl Default for ForwardConfig {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            buffer_size: DEFAULT_BUFFER_SIZE,
            open_timeout: Duration::from_secs(10),
            event_capacity: DEFAULT_EVENT_CAPACITY,
            no_delay: true,
        }
    }
}

/// Byte counts observed before one forwarded connection closed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ForwardStats {
    /// Bytes copied from the local socket to Android.
    pub uploaded_bytes: u64,
    /// Bytes copied from Android to the local socket.
    pub downloaded_bytes: u64,
}

/// Observable lifecycle changes for a running forwarder.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ForwardEvent {
    /// A local socket connected and its ADB service stream opened.
    Connected {
        /// Local peer address.
        peer: SocketAddr,
    },
    /// A local socket and its ADB stream closed.
    Closed {
        /// Local peer address.
        peer: SocketAddr,
        /// Bytes copied in both directions.
        stats: ForwardStats,
        /// Terminal error, or `None` for a normal close.
        error: Option<String>,
    },
    /// The listening socket failed and the forwarder stopped.
    ListenerFailed {
        /// Non-sensitive operating-system error text.
        error: String,
    },
    /// The listener and all active connections have stopped.
    Stopped,
}

/// Errors returned before a forwarder starts listening.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ForwardError {
    /// Device TCP port zero is not a valid ADB destination.
    #[error("device TCP target port must be non-zero")]
    InvalidTargetPort,
    /// A resource limit is zero or exceeds its defensive maximum.
    #[error("invalid forwarding configuration: {0}")]
    InvalidConfig(&'static str),
    /// The local TCP listener could not bind.
    #[error("could not bind local forwarding listener at {address}: {source}")]
    Bind {
        /// Requested local address.
        address: SocketAddr,
        /// Operating-system bind failure.
        #[source]
        source: io::Error,
    },
}

/// A running local TCP listener backed by per-connection ADB streams.
pub struct TcpForwarder {
    local_address: SocketAddr,
    target_port: u16,
    events: broadcast::Sender<ForwardEvent>,
    shutdown: watch::Sender<bool>,
    completion: watch::Receiver<bool>,
}

impl TcpForwarder {
    /// Binds a local TCP listener and starts forwarding to `tcp:<target_port>`.
    ///
    /// Bind a loopback address unless remote hosts explicitly need access.
    /// Port zero may be used in `local_address` to request an ephemeral port.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid target/configuration or local bind
    /// failure. Device service-open failures are reported as connection events.
    pub async fn start(
        client: Arc<AdbClient>,
        local_address: SocketAddr,
        target_port: u16,
        config: ForwardConfig,
    ) -> Result<Self, ForwardError> {
        validate(target_port, config)?;
        let listener =
            TcpListener::bind(local_address)
                .await
                .map_err(|source| ForwardError::Bind {
                    address: local_address,
                    source,
                })?;
        let local_address = listener.local_addr().map_err(|source| ForwardError::Bind {
            address: local_address,
            source,
        })?;
        let (events, _) = broadcast::channel(config.event_capacity);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let (completion_sender, completion) = watch::channel(false);
        tokio::spawn(run_listener(
            listener,
            client,
            target_port,
            config,
            events.clone(),
            shutdown_receiver,
            completion_sender,
        ));
        Ok(Self {
            local_address,
            target_port,
            events,
            shutdown,
            completion,
        })
    }

    /// Returns the bound local address, including an assigned ephemeral port.
    #[must_use]
    pub const fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    /// Returns the Android TCP target port.
    #[must_use]
    pub const fn target_port(&self) -> u16 {
        self.target_port
    }

    /// Subscribes to connection and listener lifecycle events.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<ForwardEvent> {
        self.events.subscribe()
    }

    /// Requests shutdown and waits for the listener and active tunnels to stop.
    pub async fn shutdown(&self) {
        self.shutdown.send_replace(true);
        self.wait().await;
    }

    /// Waits until the forwarder stops without requesting shutdown.
    pub async fn wait(&self) {
        let mut completion = self.completion.clone();
        while !*completion.borrow() && completion.changed().await.is_ok() {}
    }

    /// Returns whether the listener task has stopped.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        *self.completion.borrow()
    }
}

impl Drop for TcpForwarder {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
    }
}

async fn run_listener(
    listener: TcpListener,
    client: Arc<AdbClient>,
    target_port: u16,
    config: ForwardConfig,
    events: broadcast::Sender<ForwardEvent>,
    mut shutdown: watch::Receiver<bool>,
    completion: watch::Sender<bool>,
) {
    let permits = Arc::new(Semaphore::new(config.max_connections));
    let mut connections = JoinSet::new();

    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            finished = connections.join_next(), if !connections.is_empty() => {
                if let Some(Ok((peer, result))) = finished {
                    publish_closed(&events, peer, result);
                }
            }
            accepted = accept_limited(&listener, Arc::clone(&permits)) => {
                match accepted {
                    Ok((socket, peer, permit)) => {
                        let client = Arc::clone(&client);
                        let events = events.clone();
                        connections.spawn(async move {
                            let _permit = permit;
                            let result = forward_connection(
                                client,
                                socket,
                                peer,
                                target_port,
                                config,
                                &events,
                            )
                            .await;
                            (peer, result)
                        });
                    }
                    Err(error) => {
                        let _ = events.send(ForwardEvent::ListenerFailed {
                            error: error.to_string(),
                        });
                        break;
                    }
                }
            }
        }
    }

    connections.abort_all();
    while connections.join_next().await.is_some() {}
    let _ = events.send(ForwardEvent::Stopped);
    completion.send_replace(true);
}

async fn accept_limited(
    listener: &TcpListener,
    permits: Arc<Semaphore>,
) -> io::Result<(TcpStream, SocketAddr, tokio::sync::OwnedSemaphorePermit)> {
    let permit = permits
        .acquire_owned()
        .await
        .map_err(|_| io::Error::other("forwarding connection limiter closed"))?;
    let (socket, peer) = listener.accept().await?;
    Ok((socket, peer, permit))
}

#[derive(Debug, Error)]
enum ConnectionError {
    #[error(transparent)]
    Client(#[from] AdbClientError),
    #[error(transparent)]
    Stream(#[from] AdbStreamError),
    #[error("local TCP socket failed: {0}")]
    Local(#[from] io::Error),
    #[error("opening the device TCP service timed out after {0:?}")]
    OpenTimeout(Duration),
}

async fn forward_connection(
    client: Arc<AdbClient>,
    socket: TcpStream,
    peer: SocketAddr,
    target_port: u16,
    config: ForwardConfig,
    events: &broadcast::Sender<ForwardEvent>,
) -> Result<ForwardStats, ConnectionError> {
    socket.set_nodelay(config.no_delay)?;
    let service = format!("tcp:{target_port}");
    let stream = tokio::time::timeout(config.open_timeout, client.open_service(&service))
        .await
        .map_err(|_| ConnectionError::OpenTimeout(config.open_timeout))??;
    let _ = events.send(ForwardEvent::Connected { peer });

    let uploaded = Arc::new(AtomicU64::new(0));
    let downloaded = Arc::new(AtomicU64::new(0));
    let (local_reader, local_writer) = socket.into_split();
    let copy_size = config.buffer_size.min(stream.max_payload());
    let upload = copy_local_to_device(
        local_reader,
        stream.clone(),
        copy_size,
        Arc::clone(&uploaded),
    );
    let download = copy_device_to_local(stream.clone(), local_writer, Arc::clone(&downloaded));
    let result = tokio::select! {
        result = upload => result,
        result = download => result,
    };
    let close = stream.close().await;
    result?;
    close?;
    Ok(ForwardStats {
        uploaded_bytes: uploaded.load(Ordering::Relaxed),
        downloaded_bytes: downloaded.load(Ordering::Relaxed),
    })
}

async fn copy_local_to_device(
    mut local: OwnedReadHalf,
    device: AdbStream,
    buffer_size: usize,
    transferred: Arc<AtomicU64>,
) -> Result<(), ConnectionError> {
    let mut buffer = vec![0_u8; buffer_size];
    loop {
        let read = match local.read(&mut buffer).await {
            Ok(0) => return Ok(()),
            Ok(read) => read,
            Err(error) if is_normal_disconnect(&error) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        device
            .write(Bytes::copy_from_slice(&buffer[..read]))
            .await?;
        transferred.fetch_add(read as u64, Ordering::Relaxed);
    }
}

async fn copy_device_to_local(
    device: AdbStream,
    mut local: OwnedWriteHalf,
    transferred: Arc<AtomicU64>,
) -> Result<(), ConnectionError> {
    while let Some(payload) = device.read().await? {
        if let Err(error) = local.write_all(&payload).await {
            if is_normal_disconnect(&error) {
                return Ok(());
            }
            return Err(error.into());
        }
        transferred.fetch_add(payload.len() as u64, Ordering::Relaxed);
    }
    let _ = local.shutdown().await;
    Ok(())
}

fn publish_closed(
    events: &broadcast::Sender<ForwardEvent>,
    peer: SocketAddr,
    result: Result<ForwardStats, ConnectionError>,
) {
    let (stats, error) = match result {
        Ok(stats) => (stats, None),
        Err(error) => (ForwardStats::default(), Some(error.to_string())),
    };
    let _ = events.send(ForwardEvent::Closed { peer, stats, error });
}

fn validate(target_port: u16, config: ForwardConfig) -> Result<(), ForwardError> {
    if target_port == 0 {
        return Err(ForwardError::InvalidTargetPort);
    }
    if config.max_connections == 0 || config.max_connections > MAX_CONNECTIONS {
        return Err(ForwardError::InvalidConfig(
            "max_connections must be between 1 and 4096",
        ));
    }
    if config.buffer_size == 0 || config.buffer_size > MAX_BUFFER_SIZE {
        return Err(ForwardError::InvalidConfig(
            "buffer_size must be between 1 byte and 1 MiB",
        ));
    }
    if config.open_timeout.is_zero() {
        return Err(ForwardError::InvalidConfig("open_timeout must be non-zero"));
    }
    if config.event_capacity == 0 || config.event_capacity > MAX_EVENT_CAPACITY {
        return Err(ForwardError::InvalidConfig(
            "event_capacity must be between 1 and 4096",
        ));
    }
    Ok(())
}

fn is_normal_disconnect(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_rejects_unbounded_or_zero_values() {
        assert!(matches!(
            validate(0, ForwardConfig::default()),
            Err(ForwardError::InvalidTargetPort)
        ));
        for config in [
            ForwardConfig {
                max_connections: 0,
                ..ForwardConfig::default()
            },
            ForwardConfig {
                buffer_size: 0,
                ..ForwardConfig::default()
            },
            ForwardConfig {
                open_timeout: Duration::ZERO,
                ..ForwardConfig::default()
            },
            ForwardConfig {
                event_capacity: 0,
                ..ForwardConfig::default()
            },
        ] {
            assert!(matches!(
                validate(7001, config),
                Err(ForwardError::InvalidConfig(_))
            ));
        }
    }
}
