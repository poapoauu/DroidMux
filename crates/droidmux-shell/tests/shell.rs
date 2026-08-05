//! Offline integration tests for legacy and Shell v2 sessions.

use std::sync::{Arc, OnceLock};

use adb_auth::RsaAdbCredential;
use adb_client::{AdbClient, AdbStreamError};
use adb_protocol::{ADB_VERSION, AdbCommand, AdbPacket};
use adb_transport::{AdbTransport, AdbTransportError, TransportOperation};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use droidmux_shell::{ShellError, ShellOptions, execute, execute_with_options, open_shell};
use tokio::{
    sync::mpsc,
    time::{Duration, timeout},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);
const SHELL_HEADER_LEN: usize = 5;

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
        "shell-test-device:5555".to_owned()
    }
}

fn credential() -> Arc<RsaAdbCredential> {
    static CREDENTIAL: OnceLock<RsaAdbCredential> = OnceLock::new();
    Arc::new(
        CREDENTIAL
            .get_or_init(|| {
                RsaAdbCredential::generate("droidmux@shell-test")
                    .expect("a test credential should be generated")
            })
            .clone(),
    )
}

fn device_connect(shell_v2: bool) -> AdbPacket {
    let banner = if shell_v2 {
        Bytes::from_static(b"device::product=shell-test;features=shell_v2,cmd;\0")
    } else {
        Bytes::from_static(b"device::product=shell-test;features=cmd;\0")
    };
    AdbPacket::new(AdbCommand::Connect, ADB_VERSION, 4096, banner)
        .expect("CNXN fixture should be valid")
}

fn empty_packet(command: AdbCommand, arg0: u32, arg1: u32) -> AdbPacket {
    AdbPacket::new(command, arg0, arg1, Bytes::new()).expect("empty fixture should be valid")
}

fn shell_frame(id: u8, payload: &[u8]) -> Bytes {
    let length = u32::try_from(payload.len()).expect("the test payload should fit on the wire");
    let mut frame = BytesMut::with_capacity(SHELL_HEADER_LEN + payload.len());
    frame.put_u8(id);
    frame.put_u32_le(length);
    frame.extend_from_slice(payload);
    frame.freeze()
}

fn decode_shell_frame(packet: &AdbPacket) -> (u8, Bytes) {
    assert_eq!(packet.command, AdbCommand::Write);
    assert!(packet.payload.len() >= SHELL_HEADER_LEN);
    let length_bytes: [u8; 4] = packet.payload[1..SHELL_HEADER_LEN]
        .try_into()
        .expect("the shell header should contain a length");
    let length = usize::try_from(u32::from_le_bytes(length_bytes))
        .expect("the shell payload length should fit this platform");
    assert_eq!(packet.payload.len(), SHELL_HEADER_LEN + length);
    (packet.payload[0], packet.payload.slice(SHELL_HEADER_LEN..))
}

async fn receive_packet(receiver: &mut mpsc::UnboundedReceiver<AdbPacket>) -> AdbPacket {
    timeout(TEST_TIMEOUT, receiver.recv())
        .await
        .expect("the client should write before the test deadline")
        .expect("the client write channel should stay open")
}

async fn connected_client(
    shell_v2: bool,
) -> (
    AdbClient,
    mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    mpsc::UnboundedReceiver<AdbPacket>,
) {
    let (inbound_sender, inbound_receiver) = mpsc::unbounded_channel();
    let (write_sender, mut write_receiver) = mpsc::unbounded_channel();
    inbound_sender
        .send(Ok(device_connect(shell_v2)))
        .expect("the handshake input channel should be open");
    let transport = ChannelTransport {
        inbound: inbound_receiver,
        writes: write_sender,
    };
    let client = AdbClient::connect(Box::new(transport), credential())
        .await
        .expect("the channel transport should connect");
    assert_eq!(
        receive_packet(&mut write_receiver).await.command,
        AdbCommand::Connect
    );
    (client, inbound_sender, write_receiver)
}

async fn accept_open(
    expected_service: &str,
    inbound: &mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    writes: &mut mpsc::UnboundedReceiver<AdbPacket>,
) -> (u32, u32) {
    let open = receive_packet(writes).await;
    assert_eq!(open.command, AdbCommand::Open);
    assert_eq!(open.arg1, 0);
    assert_eq!(open.payload.last(), Some(&0));
    assert_eq!(
        &open.payload[..open.payload.len() - 1],
        expected_service.as_bytes()
    );
    let local_id = open.arg0;
    let remote_id = local_id + 100;
    inbound
        .send(Ok(empty_packet(AdbCommand::Okay, remote_id, local_id)))
        .expect("the OPEN response should reach the client");
    (local_id, remote_id)
}

async fn acknowledge_host_write(
    inbound: &mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    writes: &mut mpsc::UnboundedReceiver<AdbPacket>,
) -> AdbPacket {
    let write = receive_packet(writes).await;
    assert_eq!(write.command, AdbCommand::Write);
    inbound
        .send(Ok(empty_packet(AdbCommand::Okay, write.arg1, write.arg0)))
        .expect("the WRTE acknowledgement should reach the client");
    write
}

#[tokio::test]
async fn executes_shell_v2_and_separates_output_and_exit_code() {
    let (client, inbound, mut writes) = connected_client(true).await;
    let command = execute(&client, "getprop ro.product.model");
    let peer = async {
        let (local_id, remote_id) = accept_open(
            "shell,v2,raw:getprop ro.product.model",
            &inbound,
            &mut writes,
        )
        .await;

        let mut response = BytesMut::new();
        response.extend_from_slice(&shell_frame(1, b"Pixel\n"));
        response.extend_from_slice(&shell_frame(2, b"warning\n"));
        response.extend_from_slice(&shell_frame(3, &[7]));
        inbound
            .send(Ok(AdbPacket::new(
                AdbCommand::Write,
                remote_id,
                local_id,
                response.freeze(),
            )
            .expect("the Shell v2 response should be valid")))
            .expect("the Shell v2 response should reach the client");

        let okay = receive_packet(&mut writes).await;
        assert_eq!(okay.command, AdbCommand::Okay);
        assert_eq!(okay.arg0, local_id);
        assert_eq!(okay.arg1, remote_id);
        let close = receive_packet(&mut writes).await;
        assert_eq!(close.command, AdbCommand::Close);
        assert_eq!(close.arg0, local_id);
        assert_eq!(close.arg1, remote_id);
    };

    let (output, ()) = tokio::join!(command, peer);
    let output = output.expect("the Shell v2 command should complete");
    assert_eq!(output.stdout, Bytes::from_static(b"Pixel\n"));
    assert_eq!(output.stderr, Bytes::from_static(b"warning\n"));
    assert_eq!(output.exit_code, Some(7));
    client.close().await.expect("the client should close");
}

#[tokio::test]
async fn interactive_shell_supports_input_resize_interrupt_and_cancel() {
    let (client, inbound, mut writes) = connected_client(true).await;
    let options = ShellOptions::interactive();
    let open = open_shell(&client, "", options);
    let peer_open = async {
        let ids = accept_open("shell,v2,pty:", &inbound, &mut writes).await;
        let resize = acknowledge_host_write(&inbound, &mut writes).await;
        let (id, payload) = decode_shell_frame(&resize);
        assert_eq!(id, 5);
        assert_eq!(payload, Bytes::from_static(b"24x80,0x0\0"));
        ids
    };
    let (session, (local_id, remote_id)) = tokio::join!(open, peer_open);
    let session = session.expect("the interactive shell should open");

    let write_input = session.write_stdin(Bytes::from_static(b"echo ready\n"));
    let peer_input = async {
        let write = acknowledge_host_write(&inbound, &mut writes).await;
        let (id, payload) = decode_shell_frame(&write);
        assert_eq!(id, 0);
        assert_eq!(payload, Bytes::from_static(b"echo ready\n"));
    };
    let (input_result, ()) = tokio::join!(write_input, peer_input);
    input_result.expect("stdin should be written");

    let write_tab = session.write_stdin(Bytes::from_static(b"\t"));
    let peer_tab = async {
        let write = acknowledge_host_write(&inbound, &mut writes).await;
        let (id, payload) = decode_shell_frame(&write);
        assert_eq!(id, 0);
        assert_eq!(payload, Bytes::from_static(b"\t"));
    };
    let (tab_result, ()) = tokio::join!(write_tab, peer_tab);
    tab_result.expect("tab should be written as raw terminal input");

    let resize = session.resize(40, 120);
    let peer_resize = async {
        let write = acknowledge_host_write(&inbound, &mut writes).await;
        let (id, payload) = decode_shell_frame(&write);
        assert_eq!(id, 5);
        assert_eq!(payload, Bytes::from_static(b"40x120,0x0\0"));
    };
    let (resize_result, ()) = tokio::join!(resize, peer_resize);
    resize_result.expect("the PTY should resize");

    let interrupt = session.interrupt();
    let peer_interrupt = async {
        let write = acknowledge_host_write(&inbound, &mut writes).await;
        let (id, payload) = decode_shell_frame(&write);
        assert_eq!(id, 0);
        assert_eq!(payload, Bytes::from_static(&[3]));
    };
    let (interrupt_result, ()) = tokio::join!(interrupt, peer_interrupt);
    interrupt_result.expect("Ctrl+C should be sent");

    let close_stdin = session.close_stdin();
    let peer_close_stdin = async {
        let write = acknowledge_host_write(&inbound, &mut writes).await;
        let (id, payload) = decode_shell_frame(&write);
        assert_eq!(id, 4);
        assert!(payload.is_empty());
    };
    let (close_stdin_result, ()) = tokio::join!(close_stdin, peer_close_stdin);
    close_stdin_result.expect("stdin should close independently");

    let cancel = session.cancel();
    let peer_cancel = async {
        let close = receive_packet(&mut writes).await;
        assert_eq!(close.command, AdbCommand::Close);
        assert_eq!(close.arg0, local_id);
        assert_eq!(close.arg1, remote_id);
    };
    let (cancel_result, ()) = tokio::join!(cancel, peer_cancel);
    cancel_result.expect("the shell should cancel");
    assert!(matches!(session.wait().await, Err(ShellError::Canceled)));
    client.close().await.expect("the client should close");
}

#[tokio::test]
async fn legacy_shell_collects_combined_output_without_an_exit_code() {
    let (client, inbound, mut writes) = connected_client(false).await;
    let command = execute_with_options(&client, "echo legacy", ShellOptions::legacy());
    let peer = async {
        let (local_id, remote_id) = accept_open("shell:echo legacy", &inbound, &mut writes).await;
        inbound
            .send(Ok(AdbPacket::new(
                AdbCommand::Write,
                remote_id,
                local_id,
                Bytes::from_static(b"legacy output\n"),
            )
            .expect("the legacy shell output should be valid")))
            .expect("the legacy output should reach the client");
        let okay = receive_packet(&mut writes).await;
        assert_eq!(okay.command, AdbCommand::Okay);
        inbound
            .send(Ok(empty_packet(AdbCommand::Close, remote_id, local_id)))
            .expect("the remote close should reach the client");
        let close = receive_packet(&mut writes).await;
        assert_eq!(close.command, AdbCommand::Close);
    };

    let (output, ()) = tokio::join!(command, peer);
    let output = output.expect("the legacy command should complete");
    assert_eq!(output.stdout, Bytes::from_static(b"legacy output\n"));
    assert!(output.stderr.is_empty());
    assert_eq!(output.exit_code, None);
    client.close().await.expect("the client should close");
}

#[tokio::test]
async fn collected_output_is_limited_per_command() {
    let (client, inbound, mut writes) = connected_client(true).await;
    let options = ShellOptions {
        max_output_bytes: Some(4),
        ..ShellOptions::default()
    };
    let command = execute_with_options(&client, "printf 12345", options);
    let peer = async {
        let (local_id, remote_id) =
            accept_open("shell,v2,raw:printf 12345", &inbound, &mut writes).await;
        inbound
            .send(Ok(AdbPacket::new(
                AdbCommand::Write,
                remote_id,
                local_id,
                shell_frame(1, b"12345"),
            )
            .expect("the Shell v2 response should be valid")))
            .expect("the Shell v2 response should reach the client");

        let okay = receive_packet(&mut writes).await;
        assert_eq!(okay.command, AdbCommand::Okay);
        let close = receive_packet(&mut writes).await;
        assert_eq!(close.command, AdbCommand::Close);
        assert_eq!(close.arg0, local_id);
        assert_eq!(close.arg1, remote_id);
    };

    let (result, ()) = tokio::join!(command, peer);
    assert!(matches!(
        result,
        Err(ShellError::OutputTooLarge {
            limit: 4,
            actual: 5
        })
    ));
    client.close().await.expect("the client should close");
}

#[tokio::test]
async fn rejects_shell_v2_before_opening_on_an_unsupported_device() {
    let (client, _inbound, mut writes) = connected_client(false).await;
    let error = execute(&client, "getprop")
        .await
        .expect_err("Shell v2 should require an advertised feature");

    assert!(matches!(error, ShellError::ShellV2Unsupported));
    assert!(matches!(
        writes.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    client.close().await.expect("the client should close");
}

#[tokio::test]
async fn cancellation_interrupts_output_backpressure() {
    let (client, inbound, mut writes) = connected_client(true).await;
    let open = open_shell(&client, "yes", ShellOptions::default());
    let peer_open = accept_open("shell,v2,raw:yes", &inbound, &mut writes);
    let (session, (local_id, remote_id)) = tokio::join!(open, peer_open);
    let session = session.expect("the long-running shell should open");

    for sequence in 0_u8..17 {
        inbound
            .send(Ok(AdbPacket::new(
                AdbCommand::Write,
                remote_id,
                local_id,
                shell_frame(1, &[sequence]),
            )
            .expect("the backpressure output should be valid")))
            .expect("the backpressure output should reach the client");
        let okay = receive_packet(&mut writes).await;
        assert_eq!(okay.command, AdbCommand::Okay);
    }

    let cancel = session.cancel();
    let peer_cancel = async {
        let close = receive_packet(&mut writes).await;
        assert_eq!(close.command, AdbCommand::Close);
        assert_eq!(close.arg0, local_id);
        assert_eq!(close.arg1, remote_id);
    };
    let (cancel_result, ()) = tokio::join!(cancel, peer_cancel);
    cancel_result.expect("backpressure must not block cancellation");
    let wait_result = timeout(TEST_TIMEOUT, session.wait())
        .await
        .expect("the canceled reader should finish before the deadline");
    assert!(matches!(wait_result, Err(ShellError::Canceled)));
    client.close().await.expect("the client should close");
}

#[test]
fn stream_errors_remain_visible_through_shell_errors() {
    let error = ShellError::from(AdbStreamError::RemoteClosed);
    assert!(matches!(
        error,
        ShellError::Stream(AdbStreamError::RemoteClosed)
    ));
}

#[test]
fn shell_sessions_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<droidmux_shell::ShellSession>();
}
