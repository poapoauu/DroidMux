use std::{io, time::Duration};

use adb_auth::RsaAdbCredential;
use adb_protocol::{ADB_HEADER_LEN, AdbCommand, AdbHeader, AdbPacket, AdbProtocolError};
use adb_transport::{AdbTransport, AdbTransportError, TransportOperation};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time,
};
use tokio_rustls::client::TlsStream;

use crate::{
    PairingError,
    tls::{TlsIdentity, connect_tls},
};

const STLS_VERSION: u32 = 0x0100_0000;

/// Deadlines and socket options for an Android wireless TLS connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WirelessTransportConfig {
    /// Maximum time allowed for TCP and TLS connection setup.
    pub connect_timeout: Duration,
    /// Maximum time allowed to receive one ADB packet.
    pub read_timeout: Duration,
    /// Maximum time allowed to write one ADB packet.
    pub write_timeout: Duration,
    /// Maximum time allowed to close the connection.
    pub close_timeout: Duration,
    /// Whether to disable Nagle's algorithm.
    pub no_delay: bool,
}

impl Default for WirelessTransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_secs(5),
            no_delay: true,
        }
    }
}

enum Connection {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
    Closed,
}

/// A TCP transport that performs Android's in-band `STLS` upgrade.
pub struct WirelessTransport {
    connection: Connection,
    read_buffer: BytesMut,
    host: String,
    peer_description: String,
    identity: Option<TlsIdentity>,
    config: WirelessTransportConfig,
}

impl WirelessTransport {
    /// Connects to a known `_adb-tls-connect._tcp` endpoint.
    ///
    /// The TLS upgrade is performed after the ADB client sends its initial
    /// `CNXN` packet and Android replies with `STLS`.
    ///
    /// # Errors
    ///
    /// Returns a timeout, network, credential, or certificate error.
    pub async fn connect(
        host: impl Into<String>,
        port: u16,
        credential: &RsaAdbCredential,
        config: WirelessTransportConfig,
    ) -> Result<Self, PairingError> {
        let host = host.into();
        if host.is_empty() || port == 0 {
            return Err(PairingError::InvalidEndpoint(
                "host must be non-empty and port must be non-zero".to_owned(),
            ));
        }
        let stream = match time::timeout(
            config.connect_timeout,
            TcpStream::connect((host.as_str(), port)),
        )
        .await
        {
            Ok(result) => result.map_err(PairingError::Connection)?,
            Err(_) => return Err(PairingError::Timeout(config.connect_timeout)),
        };
        stream
            .set_nodelay(config.no_delay)
            .map_err(PairingError::Connection)?;
        let peer_description = stream
            .peer_addr()
            .map_err(PairingError::Connection)?
            .to_string();
        let identity = TlsIdentity::from_credential(credential)?;
        Ok(Self {
            connection: Connection::Plain(stream),
            read_buffer: BytesMut::with_capacity(ADB_HEADER_LEN),
            host,
            peer_description,
            identity: Some(identity),
            config,
        })
    }

    async fn upgrade(&mut self) -> Result<(), AdbTransportError> {
        let Connection::Plain(mut stream) =
            std::mem::replace(&mut self.connection, Connection::Closed)
        else {
            return Err(AdbTransportError::Io {
                operation: TransportOperation::Connect,
                source: io::Error::other("wireless transport is not awaiting STLS"),
            });
        };

        let request = run_transport_timeout(
            self.config.read_timeout,
            TransportOperation::Read,
            read_exact_packet(&mut stream),
        )
        .await?;
        if request.command != AdbCommand::StartTls
            || request.arg0 != STLS_VERSION
            || request.arg1 != 0
            || !request.payload.is_empty()
        {
            return Err(AdbTransportError::Io {
                operation: TransportOperation::Read,
                source: io::Error::other("device did not send a valid STLS request"),
            });
        }

        let response = AdbPacket::new(AdbCommand::StartTls, STLS_VERSION, 0, Bytes::new())?;
        let encoded = response.encode()?;
        match time::timeout(self.config.write_timeout, stream.write_all(&encoded)).await {
            Ok(Ok(())) => {}
            Ok(Err(source)) => {
                return Err(transport_io_error(TransportOperation::Write, source));
            }
            Err(_) => {
                return Err(AdbTransportError::Timeout {
                    operation: TransportOperation::Write,
                    timeout: self.config.write_timeout,
                });
            }
        }

        let identity = self.identity.take().ok_or_else(|| AdbTransportError::Io {
            operation: TransportOperation::Connect,
            source: io::Error::other("wireless TLS identity was already consumed"),
        })?;
        let tls = match time::timeout(
            self.config.connect_timeout,
            connect_tls(stream, &self.host, &identity),
        )
        .await
        {
            Ok(Ok(tls)) => tls,
            Ok(Err(error)) => {
                return Err(AdbTransportError::Io {
                    operation: TransportOperation::Connect,
                    source: io::Error::other(error.to_string()),
                });
            }
            Err(_) => {
                return Err(AdbTransportError::Timeout {
                    operation: TransportOperation::Connect,
                    timeout: self.config.connect_timeout,
                });
            }
        };
        self.connection = Connection::Tls(Box::new(tls));
        Ok(())
    }

    async fn read_tls_packet(&mut self) -> Result<AdbPacket, AdbTransportError> {
        loop {
            if let Some(frame_length) = buffered_frame_length(&self.read_buffer)? {
                if self.read_buffer.len() >= frame_length {
                    let frame = self.read_buffer.split_to(frame_length);
                    let (packet, consumed) = AdbPacket::decode(&frame)?;
                    debug_assert_eq!(consumed, frame_length);
                    return Ok(packet);
                }
                self.read_buffer
                    .reserve(frame_length.saturating_sub(self.read_buffer.len()));
            }

            let Connection::Tls(stream) = &mut self.connection else {
                return Err(AdbTransportError::ConnectionClosed {
                    operation: TransportOperation::Read,
                });
            };
            let bytes_read = stream
                .read_buf(&mut self.read_buffer)
                .await
                .map_err(|source| transport_io_error(TransportOperation::Read, source))?;
            if bytes_read == 0 {
                return Err(AdbTransportError::ConnectionClosed {
                    operation: TransportOperation::Read,
                });
            }
        }
    }
}

#[async_trait]
impl AdbTransport for WirelessTransport {
    async fn read_packet(&mut self) -> Result<AdbPacket, AdbTransportError> {
        if matches!(self.connection, Connection::Plain(_)) {
            self.upgrade().await?;
        }
        let deadline = self.config.read_timeout;
        match time::timeout(deadline, self.read_tls_packet()).await {
            Ok(result) => result,
            Err(_) => Err(AdbTransportError::Timeout {
                operation: TransportOperation::Read,
                timeout: deadline,
            }),
        }
    }

    async fn write_packet(&mut self, packet: &AdbPacket) -> Result<(), AdbTransportError> {
        let encoded = packet.encode()?;
        let deadline = self.config.write_timeout;
        let result = match &mut self.connection {
            Connection::Plain(stream) => time::timeout(deadline, stream.write_all(&encoded)).await,
            Connection::Tls(stream) => time::timeout(deadline, stream.write_all(&encoded)).await,
            Connection::Closed => {
                return Err(AdbTransportError::ConnectionClosed {
                    operation: TransportOperation::Write,
                });
            }
        };
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(source)) => Err(transport_io_error(TransportOperation::Write, source)),
            Err(_) => Err(AdbTransportError::Timeout {
                operation: TransportOperation::Write,
                timeout: deadline,
            }),
        }
    }

    async fn close(&mut self) -> Result<(), AdbTransportError> {
        let mut connection = std::mem::replace(&mut self.connection, Connection::Closed);
        let deadline = self.config.close_timeout;
        let result = match &mut connection {
            Connection::Plain(stream) => time::timeout(deadline, stream.shutdown()).await,
            Connection::Tls(stream) => time::timeout(deadline, stream.shutdown()).await,
            Connection::Closed => return Ok(()),
        };
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(source)) => Err(transport_io_error(TransportOperation::Close, source)),
            Err(_) => Err(AdbTransportError::Timeout {
                operation: TransportOperation::Close,
                timeout: deadline,
            }),
        }
    }

    fn peer_description(&self) -> String {
        self.peer_description.clone()
    }
}

async fn read_exact_packet<R>(reader: &mut R) -> Result<AdbPacket, AdbTransportError>
where
    R: AsyncRead + Unpin,
{
    let mut header_bytes = [0_u8; ADB_HEADER_LEN];
    reader
        .read_exact(&mut header_bytes)
        .await
        .map_err(|source| transport_io_error(TransportOperation::Read, source))?;
    let header = AdbHeader::decode(&header_bytes)?;
    let payload_length = payload_length_usize(header)?;
    let mut frame = Vec::with_capacity(ADB_HEADER_LEN + payload_length);
    frame.extend_from_slice(&header_bytes);
    frame.resize(ADB_HEADER_LEN + payload_length, 0);
    if payload_length > 0 {
        reader
            .read_exact(&mut frame[ADB_HEADER_LEN..])
            .await
            .map_err(|source| transport_io_error(TransportOperation::Read, source))?;
    }
    Ok(AdbPacket::decode(&frame)?.0)
}

fn buffered_frame_length(buffer: &[u8]) -> Result<Option<usize>, AdbTransportError> {
    if buffer.len() < ADB_HEADER_LEN {
        return Ok(None);
    }
    let header = AdbHeader::decode(&buffer[..ADB_HEADER_LEN])?;
    Ok(Some(ADB_HEADER_LEN + payload_length_usize(header)?))
}

fn payload_length_usize(header: AdbHeader) -> Result<usize, AdbTransportError> {
    usize::try_from(header.payload_length)
        .map_err(|_| AdbProtocolError::PayloadLengthOutOfRange {
            length: header.payload_length,
        })
        .map_err(AdbTransportError::from)
}

async fn run_transport_timeout<T, F>(
    deadline: Duration,
    operation: TransportOperation,
    future: F,
) -> Result<T, AdbTransportError>
where
    F: FutureIo<T>,
{
    match time::timeout(deadline, future).await {
        Ok(result) => result,
        Err(_) => Err(AdbTransportError::Timeout {
            operation,
            timeout: deadline,
        }),
    }
}

trait FutureIo<T>: std::future::Future<Output = Result<T, AdbTransportError>> {}

impl<T, F> FutureIo<T> for F where F: std::future::Future<Output = Result<T, AdbTransportError>> {}

fn transport_io_error(operation: TransportOperation, source: io::Error) -> AdbTransportError {
    match source.kind() {
        io::ErrorKind::UnexpectedEof
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::NotConnected => AdbTransportError::ConnectionClosed { operation },
        _ => AdbTransportError::Io { operation, source },
    }
}
