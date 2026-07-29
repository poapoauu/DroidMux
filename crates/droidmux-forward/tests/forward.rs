//! Offline end-to-end tests for native local TCP forwarding.

use std::sync::{Arc, OnceLock};

use adb_auth::RsaAdbCredential;
use adb_client::AdbClient;
use adb_protocol::{ADB_VERSION, AdbCommand, AdbPacket};
use adb_transport::{AdbTransport, AdbTransportError, TransportOperation};
use async_trait::async_trait;
use bytes::Bytes;
use droidmux_forward::{ForwardConfig, ForwardEvent, ForwardStats, TcpForwarder};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
    time::{Duration, timeout},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

struct ChannelTransport {
    inbound: mpsc::UnboundedReceiver<Result<AdbPacket, AdbTransportError>>,
    writes: mpsc::UnboundedSender<AdbPacket>,
}

#[async_trait]
impl AdbTransport for ChannelTransport {
    async fn read_packet(&mut self) -> Result<AdbPacket, AdbTransportError> {
        self.inbound
            .recv()
            .await
            .unwrap_or(Err(AdbTransportError::ConnectionClosed {
                operation: TransportOperation::Read,
            }))
    }

    async fn write_packet(&mut self, packet: &AdbPacket) -> Result<(), AdbTransportError> {
        self.writes
            .send(packet.clone())
            .map_err(|_| AdbTransportError::ConnectionClosed {
                operation: TransportOperation::Write,
            })
    }

    async fn close(&mut self) -> Result<(), AdbTransportError> {
        Ok(())
    }

    fn peer_description(&self) -> String {
        "forward-test-device:5555".to_owned()
    }
}

fn credential() -> Arc<RsaAdbCredential> {
    static CREDENTIAL: OnceLock<RsaAdbCredential> = OnceLock::new();
    Arc::new(
        CREDENTIAL
            .get_or_init(|| {
                RsaAdbCredential::generate("droidmux@forward-test")
                    .expect("a test credential should be generated")
            })
            .clone(),
    )
}

fn packet(command: AdbCommand, arg0: u32, arg1: u32, payload: Bytes) -> AdbPacket {
    AdbPacket::new(command, arg0, arg1, payload).expect("fixture packet should be valid")
}

fn empty_packet(command: AdbCommand, arg0: u32, arg1: u32) -> AdbPacket {
    packet(command, arg0, arg1, Bytes::new())
}

async fn receive_packet(receiver: &mut mpsc::UnboundedReceiver<AdbPacket>) -> AdbPacket {
    timeout(TEST_TIMEOUT, receiver.recv())
        .await
        .expect("the client should write before the test deadline")
        .expect("the client write channel should stay open")
}

async fn connected_client() -> (
    Arc<AdbClient>,
    mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    mpsc::UnboundedReceiver<AdbPacket>,
) {
    let (inbound_sender, inbound_receiver) = mpsc::unbounded_channel();
    let (write_sender, mut write_receiver) = mpsc::unbounded_channel();
    inbound_sender
        .send(Ok(packet(
            AdbCommand::Connect,
            ADB_VERSION,
            4096,
            Bytes::from_static(b"device::product=forward-test\0"),
        )))
        .expect("the handshake input channel should be open");

    let client = AdbClient::connect(
        Box::new(ChannelTransport {
            inbound: inbound_receiver,
            writes: write_sender,
        }),
        credential(),
    )
    .await
    .expect("the test transport should connect");
    assert_eq!(
        receive_packet(&mut write_receiver).await.command,
        AdbCommand::Connect
    );
    (Arc::new(client), inbound_sender, write_receiver)
}

#[tokio::test]
async fn forwards_bytes_in_both_directions_and_reports_lifecycle() {
    let (client, inbound, mut writes) = connected_client().await;
    let forwarder = TcpForwarder::start(
        Arc::clone(&client),
        "127.0.0.1:0"
            .parse()
            .expect("loopback address should parse"),
        7001,
        ForwardConfig::default(),
    )
    .await
    .expect("the local forwarder should start");
    let mut events = forwarder.subscribe();
    let mut local = TcpStream::connect(forwarder.local_address())
        .await
        .expect("the local TCP client should connect");

    let open = receive_packet(&mut writes).await;
    assert_eq!(open.command, AdbCommand::Open);
    assert_eq!(open.arg1, 0);
    assert_eq!(open.payload, Bytes::from_static(b"tcp:7001\0"));
    let local_id = open.arg0;
    let remote_id = 101;
    inbound
        .send(Ok(empty_packet(AdbCommand::Okay, remote_id, local_id)))
        .expect("the OPEN response should reach the client");

    let connected = timeout(TEST_TIMEOUT, events.recv())
        .await
        .expect("the connected event should arrive")
        .expect("the event channel should remain open");
    assert!(matches!(connected, ForwardEvent::Connected { .. }));

    local
        .write_all(b"from-local")
        .await
        .expect("the local payload should be written");
    let upload = receive_packet(&mut writes).await;
    assert_eq!(upload.command, AdbCommand::Write);
    assert_eq!((upload.arg0, upload.arg1), (local_id, remote_id));
    assert_eq!(upload.payload, Bytes::from_static(b"from-local"));
    inbound
        .send(Ok(empty_packet(AdbCommand::Okay, remote_id, local_id)))
        .expect("the upload acknowledgement should reach the client");

    inbound
        .send(Ok(packet(
            AdbCommand::Write,
            remote_id,
            local_id,
            Bytes::from_static(b"from-device"),
        )))
        .expect("the device payload should reach the client");
    let mut downloaded = [0_u8; 11];
    timeout(TEST_TIMEOUT, local.read_exact(&mut downloaded))
        .await
        .expect("the download should finish before the deadline")
        .expect("the device payload should reach the local socket");
    assert_eq!(&downloaded, b"from-device");
    let download_ack = receive_packet(&mut writes).await;
    assert_eq!(download_ack.command, AdbCommand::Okay);
    assert_eq!(
        (download_ack.arg0, download_ack.arg1),
        (local_id, remote_id)
    );

    inbound
        .send(Ok(empty_packet(AdbCommand::Close, remote_id, local_id)))
        .expect("the remote close should reach the client");
    let close_reply = receive_packet(&mut writes).await;
    assert_eq!(close_reply.command, AdbCommand::Close);
    assert_eq!((close_reply.arg0, close_reply.arg1), (local_id, remote_id));

    let closed = timeout(TEST_TIMEOUT, events.recv())
        .await
        .expect("the closed event should arrive")
        .expect("the event channel should remain open");
    assert!(matches!(
        closed,
        ForwardEvent::Closed {
            stats: ForwardStats {
                uploaded_bytes: 10,
                downloaded_bytes: 11,
            },
            error: None,
            ..
        }
    ));

    forwarder.shutdown().await;
    assert!(forwarder.is_stopped());
    let stopped = timeout(TEST_TIMEOUT, events.recv())
        .await
        .expect("the stopped event should arrive")
        .expect("the event channel should remain open");
    assert_eq!(stopped, ForwardEvent::Stopped);
    client.close().await.expect("the ADB client should close");
}
