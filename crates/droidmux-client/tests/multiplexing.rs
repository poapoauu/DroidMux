//! Offline integration tests for ADB logical-stream multiplexing.

use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
};

use adb_auth::RsaAdbCredential;
use adb_protocol::{ADB_VERSION, AdbCommand, AdbPacket};
use adb_transport::{AdbTransport, AdbTransportError, TransportOperation};
use async_trait::async_trait;
use bytes::Bytes;
use droidmux_client::{AdbClient, AdbClientError, AdbStream, AdbStreamError, ConnectionState};
use tokio::{
    sync::mpsc,
    time::{Duration, timeout},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

struct ChannelTransport {
    inbound: mpsc::UnboundedReceiver<Result<AdbPacket, AdbTransportError>>,
    writes: mpsc::UnboundedSender<AdbPacket>,
    closed: Arc<AtomicBool>,
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
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn peer_description(&self) -> String {
        "channel-device:5555".to_owned()
    }
}

fn credential() -> Arc<RsaAdbCredential> {
    static CREDENTIAL: OnceLock<RsaAdbCredential> = OnceLock::new();
    Arc::new(
        CREDENTIAL
            .get_or_init(|| {
                RsaAdbCredential::generate("droidmux@multiplex-test")
                    .expect("a test credential should be generated")
            })
            .clone(),
    )
}

fn device_connect() -> AdbPacket {
    AdbPacket::new(
        AdbCommand::Connect,
        ADB_VERSION,
        4096,
        Bytes::from_static(b"device::product=multiplex-test\0"),
    )
    .expect("CNXN fixture should be valid")
}

fn empty_packet(command: AdbCommand, arg0: u32, arg1: u32) -> AdbPacket {
    AdbPacket::new(command, arg0, arg1, Bytes::new()).expect("empty fixture should be valid")
}

async fn receive_packet(receiver: &mut mpsc::UnboundedReceiver<AdbPacket>) -> AdbPacket {
    timeout(TEST_TIMEOUT, receiver.recv())
        .await
        .expect("the client should write before the test deadline")
        .expect("the client write channel should stay open")
}

async fn connected_client() -> (
    AdbClient,
    mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    mpsc::UnboundedReceiver<AdbPacket>,
    Arc<AtomicBool>,
) {
    let (inbound_sender, inbound_receiver) = mpsc::unbounded_channel();
    let (write_sender, mut write_receiver) = mpsc::unbounded_channel();
    let closed = Arc::new(AtomicBool::new(false));
    inbound_sender
        .send(Ok(device_connect()))
        .expect("the handshake input channel should be open");

    let transport = ChannelTransport {
        inbound: inbound_receiver,
        writes: write_sender,
        closed: closed.clone(),
    };
    let client = AdbClient::connect(Box::new(transport), credential())
        .await
        .expect("the channel transport should connect");
    let connect = receive_packet(&mut write_receiver).await;
    assert_eq!(connect.command, AdbCommand::Connect);

    (client, inbound_sender, write_receiver, closed)
}

async fn open_two_streams(
    client: &AdbClient,
    inbound: &mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    writes: &mut mpsc::UnboundedReceiver<AdbPacket>,
) -> (AdbStream, AdbStream) {
    let open_calls = async {
        tokio::join!(
            client.open_service("shell:first"),
            client.open_service("shell:second")
        )
    };
    let peer = async {
        let first = receive_packet(writes).await;
        let second = receive_packet(writes).await;
        assert_eq!(first.command, AdbCommand::Open);
        assert_eq!(second.command, AdbCommand::Open);
        assert_ne!(first.arg0, second.arg0);
        assert_eq!(first.arg1, 0);
        assert_eq!(second.arg1, 0);
        assert_eq!(first.payload.last(), Some(&0));
        assert_eq!(second.payload.last(), Some(&0));

        inbound
            .send(Ok(empty_packet(
                AdbCommand::Okay,
                first.arg0 + 100,
                first.arg0,
            )))
            .expect("the first OPEN response should be accepted");
        inbound
            .send(Ok(empty_packet(
                AdbCommand::Okay,
                second.arg0 + 100,
                second.arg0,
            )))
            .expect("the second OPEN response should be accepted");
    };

    let ((first, second), ()) = tokio::join!(open_calls, peer);
    (
        first.expect("the first stream should open"),
        second.expect("the second stream should open"),
    )
}

#[tokio::test]
async fn multiplexes_two_streams_and_isolates_stream_closure() {
    let (client, inbound, mut writes, closed) = connected_client().await;
    let (first, second) = open_two_streams(&client, &inbound, &mut writes).await;
    assert_ne!(first.local_id(), second.local_id());
    assert_ne!(first.remote_id(), second.remote_id());

    inbound
        .send(Ok(AdbPacket::new(
            AdbCommand::Write,
            first.remote_id(),
            first.local_id(),
            Bytes::from_static(b"first-output"),
        )
        .expect("the first WRTE fixture should be valid")))
        .expect("the first WRTE should be accepted");
    inbound
        .send(Ok(AdbPacket::new(
            AdbCommand::Write,
            second.remote_id(),
            second.local_id(),
            Bytes::from_static(b"second-output"),
        )
        .expect("the second WRTE fixture should be valid")))
        .expect("the second WRTE should be accepted");

    tokio::task::yield_now().await;
    assert!(matches!(
        writes.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    let (first_payload, second_payload) = tokio::join!(first.read(), second.read());
    assert_eq!(
        first_payload.expect("the first read should succeed"),
        Some(Bytes::from_static(b"first-output"))
    );
    assert_eq!(
        second_payload.expect("the second read should succeed"),
        Some(Bytes::from_static(b"second-output"))
    );

    let first_ack = receive_packet(&mut writes).await;
    let second_ack = receive_packet(&mut writes).await;
    assert_eq!(first_ack.command, AdbCommand::Okay);
    assert_eq!(second_ack.command, AdbCommand::Okay);
    let acknowledged_local_ids = [first_ack.arg0, second_ack.arg0];
    assert!(acknowledged_local_ids.contains(&first.local_id()));
    assert!(acknowledged_local_ids.contains(&second.local_id()));

    inbound
        .send(Ok(empty_packet(
            AdbCommand::Close,
            first.remote_id(),
            first.local_id(),
        )))
        .expect("the remote CLSE should be accepted");
    let close_reply = receive_packet(&mut writes).await;
    assert_eq!(close_reply.command, AdbCommand::Close);
    assert_eq!(close_reply.arg0, first.local_id());
    assert_eq!(
        first.read().await.expect("remote close should be orderly"),
        None
    );

    let peer_write_ack = async {
        let write = receive_packet(&mut writes).await;
        assert_eq!(write.command, AdbCommand::Write);
        assert_eq!(write.arg0, second.local_id());
        assert_eq!(write.arg1, second.remote_id());
        assert_eq!(write.payload, Bytes::from_static(b"still-running"));
        inbound
            .send(Ok(empty_packet(
                AdbCommand::Okay,
                second.remote_id(),
                second.local_id(),
            )))
            .expect("the remaining stream's OKAY should be accepted");
    };
    let (write_result, ()) = tokio::join!(
        second.write(Bytes::from_static(b"still-running")),
        peer_write_ack
    );
    write_result.expect("closing the first stream must not affect the second");

    let peer_close = async {
        let close = receive_packet(&mut writes).await;
        assert_eq!(close.command, AdbCommand::Close);
        assert_eq!(close.arg0, second.local_id());
    };
    let (close_result, ()) = tokio::join!(second.close(), peer_close);
    close_result.expect("the second stream should close");

    client.close().await.expect("the session should close");
    assert!(closed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn device_disconnect_notifies_every_open_stream() {
    let (client, inbound, mut writes, _) = connected_client().await;
    let (first, second) = open_two_streams(&client, &inbound, &mut writes).await;

    inbound
        .send(Err(AdbTransportError::ConnectionClosed {
            operation: TransportOperation::Read,
        }))
        .expect("the disconnect should reach the session task");

    let (first_result, second_result) = tokio::join!(first.read(), second.read());
    assert!(matches!(
        first_result,
        Err(AdbStreamError::SessionClosed { .. })
    ));
    assert!(matches!(
        second_result,
        Err(AdbStreamError::SessionClosed { .. })
    ));
    assert_eq!(client.state(), ConnectionState::Closed);
}

#[tokio::test]
async fn rejected_open_does_not_close_the_session() {
    let (client, inbound, mut writes, _) = connected_client().await;

    let peer = async {
        let open = receive_packet(&mut writes).await;
        assert_eq!(open.command, AdbCommand::Open);
        inbound
            .send(Ok(empty_packet(AdbCommand::Close, 0, open.arg0)))
            .expect("the rejected OPEN response should be accepted");
    };
    let (result, ()) = tokio::join!(client.open_service("shell:missing"), peer);

    assert!(matches!(result, Err(AdbClientError::StreamRejected { .. })));
    assert_eq!(client.state(), ConnectionState::Connected);
    client
        .close()
        .await
        .expect("the session should remain usable");
}

#[tokio::test]
async fn unknown_stream_packet_is_closed_without_ending_the_session() {
    let (client, inbound, mut writes, _) = connected_client().await;
    inbound
        .send(Ok(AdbPacket::new(
            AdbCommand::Write,
            900,
            700,
            Bytes::from_static(b"orphan"),
        )
        .expect("the orphan WRTE fixture should be valid")))
        .expect("the orphan WRTE should reach the router");

    let close = receive_packet(&mut writes).await;
    assert_eq!(close.command, AdbCommand::Close);
    assert_eq!(close.arg0, 0);
    assert_eq!(close.arg1, 900);
    assert_eq!(client.state(), ConnectionState::Connected);
    client
        .close()
        .await
        .expect("the session should remain usable");
}

#[test]
fn client_and_stream_handles_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<AdbClient>();
    assert_send_sync::<AdbStream>();
}
