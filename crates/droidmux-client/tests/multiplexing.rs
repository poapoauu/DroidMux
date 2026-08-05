//! Offline integration tests for ADB logical-stream multiplexing.

use std::{
    collections::VecDeque,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

use adb_auth::RsaAdbCredential;
use adb_protocol::{ADB_VERSION, AdbCommand, AdbPacket};
use adb_transport::{AdbTransport, AdbTransportError, TransportOperation};
use async_trait::async_trait;
use bytes::Bytes;
use droidmux_client::{
    AdbClient, AdbClientConfig, AdbClientError, AdbReconnectConfig, AdbStream, AdbStreamError,
    AdbTransportFactory, ConnectionState,
};
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

fn burst_device_connect() -> AdbPacket {
    AdbPacket::new(
        AdbCommand::Connect,
        ADB_VERSION,
        4096,
        Bytes::from_static(b"device::features=shell_v2,cmd,delayed_ack;product=burst-test\0"),
    )
    .expect("burst CNXN fixture should be valid")
}

fn empty_packet(command: AdbCommand, arg0: u32, arg1: u32) -> AdbPacket {
    AdbPacket::new(command, arg0, arg1, Bytes::new()).expect("empty fixture should be valid")
}

fn delayed_ack_packet(remote_id: u32, local_id: u32, bytes: i32) -> AdbPacket {
    AdbPacket::new(
        AdbCommand::Okay,
        remote_id,
        local_id,
        Bytes::copy_from_slice(&bytes.to_le_bytes()),
    )
    .expect("delayed ACK fixture should be valid")
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

async fn connected_burst_client() -> (
    AdbClient,
    mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    mpsc::UnboundedReceiver<AdbPacket>,
    Arc<AtomicBool>,
) {
    let (inbound_sender, inbound_receiver) = mpsc::unbounded_channel();
    let (write_sender, mut write_receiver) = mpsc::unbounded_channel();
    let closed = Arc::new(AtomicBool::new(false));
    inbound_sender
        .send(Ok(burst_device_connect()))
        .expect("the burst handshake input channel should be open");

    let transport = ChannelTransport {
        inbound: inbound_receiver,
        writes: write_sender,
        closed: closed.clone(),
    };
    let client = AdbClient::connect_with_config(
        Box::new(transport),
        credential(),
        AdbClientConfig {
            burst_mode: true,
            ..AdbClientConfig::default()
        },
    )
    .await
    .expect("the burst channel transport should connect");
    let connect = receive_packet(&mut write_receiver).await;
    assert_eq!(connect.command, AdbCommand::Connect);
    assert!(connect.payload.ends_with(b",delayed_ack"));

    (client, inbound_sender, write_receiver, closed)
}

async fn open_stream(
    client: &AdbClient,
    inbound: &mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    writes: &mut mpsc::UnboundedReceiver<AdbPacket>,
    service: &str,
    remote_id: u32,
    delayed_ack_credit: Option<i32>,
) -> AdbStream {
    let peer = async {
        let open = receive_packet(writes).await;
        assert_eq!(open.command, AdbCommand::Open);
        let okay = delayed_ack_credit.map_or_else(
            || empty_packet(AdbCommand::Okay, remote_id, open.arg0),
            |credit| delayed_ack_packet(remote_id, open.arg0, credit),
        );
        inbound
            .send(Ok(okay))
            .expect("the OPEN response should reach the session");
    };
    let (stream, ()) = tokio::join!(client.open_service(service), peer);
    stream.expect("the stream should open")
}

#[tokio::test]
async fn final_payload_remains_readable_when_remote_close_arrives_first() {
    let (client, inbound, mut writes, _) = connected_client().await;
    let stream = open_stream(
        &client,
        &inbound,
        &mut writes,
        "shell:close-after-write",
        101,
        None,
    )
    .await;

    inbound
        .send(Ok(AdbPacket::new(
            AdbCommand::Write,
            stream.remote_id(),
            stream.local_id(),
            Bytes::from_static(b"final"),
        )
        .expect("the final WRTE should be valid")))
        .expect("the final WRTE should reach the session");
    inbound
        .send(Ok(empty_packet(
            AdbCommand::Close,
            stream.remote_id(),
            stream.local_id(),
        )))
        .expect("the remote CLSE should reach the session");

    let close = receive_packet(&mut writes).await;
    assert_eq!(close.command, AdbCommand::Close);
    assert_eq!(
        (close.arg0, close.arg1),
        (stream.local_id(), stream.remote_id())
    );
    assert_eq!(
        stream.read().await.expect("the final read should succeed"),
        Some(Bytes::from_static(b"final"))
    );
    let okay = receive_packet(&mut writes).await;
    assert_eq!(okay.command, AdbCommand::Okay);
    assert_eq!(
        (okay.arg0, okay.arg1),
        (stream.local_id(), stream.remote_id())
    );
    assert_eq!(
        stream.read().await.expect("the closed read should succeed"),
        None
    );

    let sibling = open_stream(
        &client,
        &inbound,
        &mut writes,
        "shell:still-usable",
        202,
        None,
    )
    .await;
    assert_ne!(sibling.local_id(), stream.local_id());
    client
        .close()
        .await
        .expect("the session should remain usable");
}

#[tokio::test]
async fn burst_payloads_are_acknowledged_after_remote_close() {
    let (client, inbound, mut writes, _) = connected_burst_client().await;
    let stream = open_stream(
        &client,
        &inbound,
        &mut writes,
        "shell:burst-close-after-write",
        303,
        Some(32 * 1024 * 1024),
    )
    .await;
    let payloads = [
        Bytes::from_static(b"a"),
        Bytes::from_static(b"bc"),
        Bytes::new(),
    ];
    for payload in &payloads {
        inbound
            .send(Ok(AdbPacket::new(
                AdbCommand::Write,
                stream.remote_id(),
                stream.local_id(),
                payload.clone(),
            )
            .expect("the burst WRTE should be valid")))
            .expect("the burst WRTE should reach the session");
    }
    inbound
        .send(Ok(empty_packet(
            AdbCommand::Close,
            stream.remote_id(),
            stream.local_id(),
        )))
        .expect("the burst CLSE should reach the session");

    let close = receive_packet(&mut writes).await;
    assert_eq!(close.command, AdbCommand::Close);
    for payload in payloads {
        assert_eq!(
            stream.read().await.expect("the queued read should succeed"),
            Some(payload.clone())
        );
        let okay = receive_packet(&mut writes).await;
        assert_eq!(okay.command, AdbCommand::Okay);
        assert_eq!(
            okay.payload,
            Bytes::copy_from_slice(
                &i32::try_from(payload.len())
                    .expect("the fixture length should fit")
                    .to_le_bytes()
            )
        );
    }
    assert_eq!(
        stream.read().await.expect("the closed read should succeed"),
        None
    );
    client
        .close()
        .await
        .expect("the burst session should close");
}

#[tokio::test]
async fn burst_mode_allows_multiple_writes_and_delayed_ack_payloads() {
    let (client, inbound, mut writes, _) = connected_burst_client().await;
    let open_peer = async {
        let open = receive_packet(&mut writes).await;
        inbound
            .send(Ok(delayed_ack_packet(101, open.arg0, 32 * 1024 * 1024)))
            .expect("the burst OPEN response should be accepted");
        open
    };
    let (open_result, open) = tokio::join!(client.open_service("shell:burst"), open_peer);
    assert_eq!(open.command, AdbCommand::Open);
    assert_eq!(open.arg1, 32 * 1024 * 1024);
    let stream = open_result.expect("the burst stream should open");

    let first_stream = stream.clone();
    let first_write =
        tokio::spawn(async move { first_stream.write(Bytes::from_static(b"first")).await });
    let second_stream = stream.clone();
    let second_write =
        tokio::spawn(async move { second_stream.write(Bytes::from_static(b"second")).await });
    let first_packet = receive_packet(&mut writes).await;
    let second_packet = receive_packet(&mut writes).await;
    assert_eq!(first_packet.command, AdbCommand::Write);
    assert_eq!(second_packet.command, AdbCommand::Write);
    assert_eq!(first_packet.arg0, stream.local_id());
    assert_eq!(second_packet.arg0, stream.local_id());
    assert_eq!(first_packet.arg1, stream.remote_id());
    assert_eq!(second_packet.arg1, stream.remote_id());
    assert_eq!(first_write.await.expect("first write task"), Ok(()));
    assert_eq!(second_write.await.expect("second write task"), Ok(()));

    inbound
        .send(Ok(delayed_ack_packet(
            stream.remote_id(),
            stream.local_id(),
            11,
        )))
        .expect("the delayed ACK should reach the session");

    for payload in [b"a".as_slice(), b"bc".as_slice()] {
        inbound
            .send(Ok(AdbPacket::new(
                AdbCommand::Write,
                stream.remote_id(),
                stream.local_id(),
                Bytes::copy_from_slice(payload),
            )
            .expect("burst WRTE should be valid")))
            .expect("the burst WRTE should reach the session");
    }
    assert_eq!(
        stream.read().await.expect("first burst read"),
        Some(Bytes::from_static(b"a"))
    );
    assert_eq!(
        stream.read().await.expect("second burst read"),
        Some(Bytes::from_static(b"bc"))
    );
    let first_ack = receive_packet(&mut writes).await;
    let second_ack = receive_packet(&mut writes).await;
    assert_eq!(first_ack.payload.len(), 4);
    assert_eq!(second_ack.payload.len(), 4);
    client.close().await.expect("burst session should close");
}

#[tokio::test]
async fn burst_receive_window_is_bounded_by_bytes_not_packet_count() {
    let (client, inbound, mut writes, _) = connected_burst_client().await;
    let open_peer = async {
        let open = receive_packet(&mut writes).await;
        inbound
            .send(Ok(delayed_ack_packet(201, open.arg0, 32 * 1024 * 1024)))
            .expect("the burst OPEN response should be accepted");
        open
    };
    let (stream, open) = tokio::join!(client.open_service("shell:many-packets"), open_peer);
    let stream = stream.expect("the burst stream should open");

    for _ in 0..128 {
        inbound
            .send(Ok(AdbPacket::new(
                AdbCommand::Write,
                stream.remote_id(),
                stream.local_id(),
                Bytes::from_static(b"x"),
            )
            .expect("small burst packet should be valid")))
            .expect("small burst packet should reach the session");
    }
    tokio::task::yield_now().await;
    assert!(matches!(
        writes.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    for _ in 0..128 {
        assert_eq!(
            stream.read().await.expect("burst read should succeed"),
            Some(Bytes::from_static(b"x"))
        );
        let ack = receive_packet(&mut writes).await;
        assert_eq!(ack.command, AdbCommand::Okay);
        assert_eq!(ack.payload, Bytes::copy_from_slice(&1_i32.to_le_bytes()));
    }
    assert_eq!(open.arg1, 32 * 1024 * 1024);
    client.close().await.expect("burst session should close");
}

#[tokio::test]
async fn burst_writes_wait_for_peer_byte_credit_before_queueing() {
    let (client, inbound, mut writes, _) = connected_burst_client().await;
    let open_peer = async {
        let open = receive_packet(&mut writes).await;
        inbound
            .send(Ok(delayed_ack_packet(301, open.arg0, 4)))
            .expect("the limited burst OPEN response should be accepted");
    };
    let (stream, ()) = tokio::join!(client.open_service("shell:credit"), open_peer);
    let stream = stream.expect("the limited-credit stream should open");

    let first_stream = stream.clone();
    let first = tokio::spawn(async move { first_stream.write(Bytes::from_static(b"1234")).await });
    let first_packet = receive_packet(&mut writes).await;
    assert_eq!(first_packet.payload, Bytes::from_static(b"1234"));
    assert_eq!(first.await.expect("first writer should join"), Ok(()));

    let second_stream = stream.clone();
    let second =
        tokio::spawn(async move { second_stream.write(Bytes::from_static(b"5678")).await });
    assert!(
        timeout(Duration::from_millis(50), writes.recv())
            .await
            .is_err()
    );
    inbound
        .send(Ok(delayed_ack_packet(
            stream.remote_id(),
            stream.local_id(),
            4,
        )))
        .expect("the replenishing ACK should reach the session");
    let second_packet = receive_packet(&mut writes).await;
    assert_eq!(second_packet.payload, Bytes::from_static(b"5678"));
    assert_eq!(second.await.expect("second writer should join"), Ok(()));
    client.close().await.expect("burst session should close");
}

struct QueueFactory {
    transports: std::sync::Mutex<VecDeque<ChannelTransport>>,
}

#[async_trait]
impl AdbTransportFactory for QueueFactory {
    async fn connect(&self) -> Result<Box<dyn AdbTransport>, AdbTransportError> {
        let transport = self
            .transports
            .lock()
            .expect("factory queue should not be poisoned")
            .pop_back()
            .ok_or(AdbTransportError::ConnectionClosed {
                operation: TransportOperation::Connect,
            })?;
        Ok(Box::new(transport))
    }
}

#[tokio::test]
async fn auto_reconnect_repeats_handshake_after_transport_loss() {
    let (first_inbound, first_inbound_receiver) = mpsc::unbounded_channel();
    let (first_writes, mut first_write_receiver) = mpsc::unbounded_channel();
    let first_closed = Arc::new(AtomicBool::new(false));
    first_inbound
        .send(Ok(device_connect()))
        .expect("the first handshake should be queued");

    let (second_inbound, second_inbound_receiver) = mpsc::unbounded_channel();
    let (second_writes, mut second_write_receiver) = mpsc::unbounded_channel();
    let second_closed = Arc::new(AtomicBool::new(false));
    second_inbound
        .send(Ok(device_connect()))
        .expect("the second handshake should be queued");

    let first = ChannelTransport {
        inbound: first_inbound_receiver,
        writes: first_writes,
        closed: first_closed,
    };
    let second = ChannelTransport {
        inbound: second_inbound_receiver,
        writes: second_writes,
        closed: second_closed,
    };
    let factory = Arc::new(QueueFactory {
        transports: std::sync::Mutex::new(VecDeque::from([second, first])),
    });
    let client = AdbClient::connect_with_auto_reconnect(
        factory,
        credential(),
        AdbClientConfig::default(),
        AdbReconnectConfig {
            max_attempts: 2,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
        },
    )
    .await
    .expect("the initial factory connection should succeed");
    assert_eq!(
        receive_packet(&mut first_write_receiver).await.command,
        AdbCommand::Connect
    );

    first_inbound
        .send(Err(AdbTransportError::ConnectionClosed {
            operation: TransportOperation::Read,
        }))
        .expect("the first connection should be closed");

    timeout(TEST_TIMEOUT, async {
        loop {
            if client.state() == ConnectionState::Connected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the client should reconnect before the test deadline");
    assert_eq!(
        receive_packet(&mut second_write_receiver).await.command,
        AdbCommand::Connect
    );
    client
        .close()
        .await
        .expect("the reconnected client should close");
}

#[tokio::test]
async fn close_interrupts_a_device_reconnect_delay() {
    let (first_inbound, first_inbound_receiver) = mpsc::unbounded_channel();
    let (first_writes, mut first_write_receiver) = mpsc::unbounded_channel();
    first_inbound
        .send(Ok(device_connect()))
        .expect("the initial handshake should be queued");
    let first = ChannelTransport {
        inbound: first_inbound_receiver,
        writes: first_writes,
        closed: Arc::new(AtomicBool::new(false)),
    };
    let factory = Arc::new(QueueFactory {
        transports: std::sync::Mutex::new(VecDeque::from([first])),
    });
    let client = AdbClient::connect_with_auto_reconnect(
        factory,
        credential(),
        AdbClientConfig::default(),
        AdbReconnectConfig {
            max_attempts: 2,
            initial_delay: Duration::from_secs(60),
            max_delay: Duration::from_secs(60),
        },
    )
    .await
    .expect("the initial connection should succeed");
    assert_eq!(
        receive_packet(&mut first_write_receiver).await.command,
        AdbCommand::Connect
    );
    first_inbound
        .send(Err(AdbTransportError::ConnectionClosed {
            operation: TransportOperation::Read,
        }))
        .expect("the disconnect should reach the session");
    timeout(TEST_TIMEOUT, async {
        while client.state() != ConnectionState::Reconnecting {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the client should enter reconnecting state");

    timeout(Duration::from_millis(200), client.close())
        .await
        .expect("close should interrupt the reconnect delay")
        .expect("the reconnecting client should close cleanly");
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
