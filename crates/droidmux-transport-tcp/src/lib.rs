//! Native Tokio TCP implementation of the ADB transport contract.

use std::{future::Future, io, net::SocketAddr, time::Duration};

use adb_protocol::{ADB_HEADER_LEN, AdbHeader, AdbPacket, AdbProtocolError};
use adb_transport::{AdbTransport, AdbTransportError, TransportOperation};
use async_trait::async_trait;
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
    time,
};

/// Deadlines and socket options for a TCP transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpTransportConfig {
    /// Maximum time allowed to establish the TCP connection.
    pub connect_timeout: Duration,
    /// Maximum time allowed to receive one complete ADB packet.
    pub read_timeout: Duration,
    /// Maximum time allowed to write one complete ADB packet.
    pub write_timeout: Duration,
    /// Maximum time allowed to shut down the TCP stream.
    pub close_timeout: Duration,
    /// Whether to enable operating-system TCP keepalive probes.
    pub keepalive: bool,
    /// Whether to disable Nagle's algorithm.
    pub no_delay: bool,
}

impl Default for TcpTransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_secs(5),
            keepalive: true,
            no_delay: true,
        }
    }
}

/// A connected, packet-framed native TCP transport.
#[derive(Debug)]
pub struct TcpTransport {
    stream: Option<TcpStream>,
    read_buffer: BytesMut,
    peer_address: SocketAddr,
    config: TcpTransportConfig,
}

impl TcpTransport {
    /// Connects to an IPv4 or IPv6 ADB endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`AdbTransportError::Timeout`] if connection setup exceeds the
    /// configured deadline, or [`AdbTransportError::Io`] if socket setup or
    /// connection fails.
    pub async fn connect(
        address: SocketAddr,
        config: TcpTransportConfig,
    ) -> Result<Self, AdbTransportError> {
        let socket = create_socket(address, config)?;
        let stream = run_io_with_timeout(
            config.connect_timeout,
            TransportOperation::Connect,
            socket.connect(address),
        )
        .await?;

        stream
            .set_nodelay(config.no_delay)
            .map_err(|source| io_error(TransportOperation::Connect, source))?;
        let peer_address = stream
            .peer_addr()
            .map_err(|source| io_error(TransportOperation::Connect, source))?;

        Ok(Self {
            stream: Some(stream),
            read_buffer: BytesMut::with_capacity(ADB_HEADER_LEN),
            peer_address,
            config,
        })
    }

    async fn read_packet_inner(&mut self) -> Result<AdbPacket, AdbTransportError> {
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

            let stream = self
                .stream
                .as_mut()
                .ok_or(AdbTransportError::ConnectionClosed {
                    operation: TransportOperation::Read,
                })?;
            let bytes_read = stream
                .read_buf(&mut self.read_buffer)
                .await
                .map_err(|source| io_error(TransportOperation::Read, source))?;
            if bytes_read == 0 {
                return Err(AdbTransportError::ConnectionClosed {
                    operation: TransportOperation::Read,
                });
            }
        }
    }
}

#[async_trait]
impl AdbTransport for TcpTransport {
    async fn read_packet(&mut self) -> Result<AdbPacket, AdbTransportError> {
        let deadline = self.config.read_timeout;
        match time::timeout(deadline, self.read_packet_inner()).await {
            Ok(Err(error)) if is_connection_closed(&error) => {
                self.stream.take();
                Err(error)
            }
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
        let stream = self
            .stream
            .as_mut()
            .ok_or(AdbTransportError::ConnectionClosed {
                operation: TransportOperation::Write,
            })?;
        let result = time::timeout(deadline, stream.write_all(&encoded)).await;

        match result {
            Ok(Err(source)) => {
                let error = io_error(TransportOperation::Write, source);
                if is_connection_closed(&error) {
                    self.stream.take();
                }
                Err(error)
            }
            Ok(Ok(())) => Ok(()),
            Err(_) => {
                self.stream.take();
                Err(AdbTransportError::Timeout {
                    operation: TransportOperation::Write,
                    timeout: deadline,
                })
            }
        }
    }

    async fn close(&mut self) -> Result<(), AdbTransportError> {
        let deadline = self.config.close_timeout;
        let Some(mut stream) = self.stream.take() else {
            return Ok(());
        };
        match time::timeout(deadline, stream.shutdown()).await {
            Ok(result) => result.map_err(|source| io_error(TransportOperation::Close, source)),
            Err(_) => Err(AdbTransportError::Timeout {
                operation: TransportOperation::Close,
                timeout: deadline,
            }),
        }
    }

    fn peer_description(&self) -> String {
        self.peer_address.to_string()
    }
}

fn create_socket(
    address: SocketAddr,
    config: TcpTransportConfig,
) -> Result<TcpSocket, AdbTransportError> {
    let socket = if address.is_ipv4() {
        TcpSocket::new_v4()
    } else {
        TcpSocket::new_v6()
    }
    .map_err(|source| io_error(TransportOperation::Connect, source))?;
    socket
        .set_keepalive(config.keepalive)
        .map_err(|source| io_error(TransportOperation::Connect, source))?;
    Ok(socket)
}

fn io_error(operation: TransportOperation, source: io::Error) -> AdbTransportError {
    match source.kind() {
        io::ErrorKind::UnexpectedEof
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::NotConnected => AdbTransportError::ConnectionClosed { operation },
        _ => AdbTransportError::Io { operation, source },
    }
}

fn is_connection_closed(error: &AdbTransportError) -> bool {
    matches!(error, AdbTransportError::ConnectionClosed { .. })
}

fn buffered_frame_length(buffer: &[u8]) -> Result<Option<usize>, AdbTransportError> {
    if buffer.len() < ADB_HEADER_LEN {
        return Ok(None);
    }
    let header = AdbHeader::decode(&buffer[..ADB_HEADER_LEN])?;
    let payload_length = usize::try_from(header.payload_length).map_err(|_| {
        AdbProtocolError::PayloadLengthOutOfRange {
            length: header.payload_length,
        }
    })?;
    Ok(Some(ADB_HEADER_LEN + payload_length))
}

async fn run_io_with_timeout<T, F>(
    deadline: Duration,
    operation: TransportOperation,
    future: F,
) -> Result<T, AdbTransportError>
where
    F: Future<Output = io::Result<T>>,
{
    match time::timeout(deadline, future).await {
        Ok(result) => result.map_err(|source| io_error(operation, source)),
        Err(_) => Err(AdbTransportError::Timeout {
            operation,
            timeout: deadline,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::future;

    use super::*;

    #[tokio::test]
    async fn pending_connect_obeys_its_deadline() {
        let deadline = Duration::from_millis(1);
        let error = run_io_with_timeout(
            deadline,
            TransportOperation::Connect,
            future::pending::<io::Result<()>>(),
        )
        .await
        .expect_err("a pending connection must time out");

        assert!(matches!(
            error,
            AdbTransportError::Timeout {
                operation: TransportOperation::Connect,
                timeout,
            } if timeout == deadline
        ));
    }
}
