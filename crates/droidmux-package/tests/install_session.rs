//! Offline ADB protocol tests for streamed package installer sessions.

use std::{
    path::PathBuf,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use adb_auth::RsaAdbCredential;
use adb_client::AdbClient;
use adb_protocol::{ADB_VERSION, AdbCommand, AdbPacket};
use adb_transport::{AdbTransport, AdbTransportError, TransportOperation};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use droidmux_package::{
    InstallOptions, PackageError, PackageManager, PackageTransferCancellation,
    PackageTransferProgress,
};
use tokio::{
    sync::mpsc,
    time::{Duration, timeout},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);
const SHELL_HEADER_LEN: usize = 5;
static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

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
        "package-test-device:5555".to_owned()
    }
}

fn credential() -> Arc<RsaAdbCredential> {
    static CREDENTIAL: OnceLock<RsaAdbCredential> = OnceLock::new();
    Arc::new(
        CREDENTIAL
            .get_or_init(|| {
                RsaAdbCredential::generate("droidmux@package-test")
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

fn shell_frame(id: u8, payload: &[u8]) -> Bytes {
    let mut frame = BytesMut::with_capacity(SHELL_HEADER_LEN + payload.len());
    frame.put_u8(id);
    frame.put_u32_le(u32::try_from(payload.len()).expect("fixture length should fit"));
    frame.extend_from_slice(payload);
    frame.freeze()
}

fn decode_shell_frame(packet: &AdbPacket) -> (u8, Bytes) {
    assert_eq!(packet.command, AdbCommand::Write);
    let length = u32::from_le_bytes(
        packet.payload[1..SHELL_HEADER_LEN]
            .try_into()
            .expect("the frame should contain a length"),
    );
    let length = usize::try_from(length).expect("the frame length should fit");
    assert_eq!(packet.payload.len(), SHELL_HEADER_LEN + length);
    (packet.payload[0], packet.payload.slice(SHELL_HEADER_LEN..))
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
) {
    let (inbound_sender, inbound_receiver) = mpsc::unbounded_channel();
    let (write_sender, mut write_receiver) = mpsc::unbounded_channel();
    inbound_sender
        .send(Ok(packet(
            AdbCommand::Connect,
            ADB_VERSION,
            4096,
            Bytes::from_static(b"device::product=package-test;features=shell_v2,cmd;\0"),
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
    .expect("the channel transport should connect");
    assert_eq!(
        receive_packet(&mut write_receiver).await.command,
        AdbCommand::Connect
    );
    (client, inbound_sender, write_receiver)
}

async fn accept_shell(
    command: &str,
    inbound: &mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    writes: &mut mpsc::UnboundedReceiver<AdbPacket>,
) -> (u32, u32) {
    let open = receive_packet(writes).await;
    assert_eq!(open.command, AdbCommand::Open);
    assert_eq!(
        &open.payload[..open.payload.len() - 1],
        format!("shell,v2,raw:{command}").as_bytes()
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

async fn finish_shell(
    local_id: u32,
    remote_id: u32,
    stdout: &[u8],
    exit_code: u8,
    inbound: &mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    writes: &mut mpsc::UnboundedReceiver<AdbPacket>,
) {
    let mut response = BytesMut::new();
    if !stdout.is_empty() {
        response.extend_from_slice(&shell_frame(1, stdout));
    }
    response.extend_from_slice(&shell_frame(3, &[exit_code]));
    inbound
        .send(Ok(packet(
            AdbCommand::Write,
            remote_id,
            local_id,
            response.freeze(),
        )))
        .expect("the shell result should reach the client");
    let okay = receive_packet(writes).await;
    assert_eq!(okay.command, AdbCommand::Okay);
    let close = receive_packet(writes).await;
    assert_eq!(close.command, AdbCommand::Close);
    assert_eq!((close.arg0, close.arg1), (local_id, remote_id));
}

async fn respond_command(
    command: &str,
    stdout: &[u8],
    exit_code: u8,
    inbound: &mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    writes: &mut mpsc::UnboundedReceiver<AdbPacket>,
) {
    let (local_id, remote_id) = accept_shell(command, inbound, writes).await;
    finish_shell(local_id, remote_id, stdout, exit_code, inbound, writes).await;
}

async fn respond_install_write(
    command: &str,
    expected_apk: &[u8],
    stdout: &[u8],
    exit_code: u8,
    inbound: &mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    writes: &mut mpsc::UnboundedReceiver<AdbPacket>,
) {
    let (local_id, remote_id) = accept_shell(command, inbound, writes).await;
    let data = acknowledge_host_write(inbound, writes).await;
    let (data_id, payload) = decode_shell_frame(&data);
    assert_eq!(data_id, 0);
    assert_eq!(&payload[..], expected_apk);
    let close_stdin = acknowledge_host_write(inbound, writes).await;
    let (close_id, payload) = decode_shell_frame(&close_stdin);
    assert_eq!(close_id, 4);
    assert!(payload.is_empty());
    finish_shell(local_id, remote_id, stdout, exit_code, inbound, writes).await;
}

async fn apk_fixtures() -> Result<(PathBuf, Vec<PathBuf>), Box<dyn std::error::Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let sequence = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!(
        "droidmux-install-session-{}-{nonce}-{sequence}",
        std::process::id()
    ));
    tokio::fs::create_dir_all(&directory).await?;
    let base = directory.join("base.apk");
    let split = directory.join("config.arm64_v8a.apk");
    tokio::fs::write(&base, b"base-data").await?;
    tokio::fs::write(&split, b"split-data").await?;
    Ok((directory, vec![base, split]))
}

#[tokio::test]
async fn streams_two_apks_and_commits_the_install_session() -> Result<(), Box<dyn std::error::Error>>
{
    let (directory, paths) = apk_fixtures().await?;
    let (client, inbound, mut writes) = connected_client().await;
    let manager = PackageManager::new(&client);
    let mut progress = Vec::<PackageTransferProgress>::new();
    let install = manager.install_split_apks(
        &paths,
        InstallOptions::default(),
        PackageTransferCancellation::new(),
        |update| progress.push(update),
    );
    let peer = async {
        respond_command(
            "cmd package install-create -r -S 19",
            b"Success: created install session [481]\n",
            0,
            &inbound,
            &mut writes,
        )
        .await;
        respond_install_write(
            "cmd package install-write -S 9 481 split-0000.apk -",
            b"base-data",
            b"Success: streamed 9 bytes\n",
            0,
            &inbound,
            &mut writes,
        )
        .await;
        respond_install_write(
            "cmd package install-write -S 10 481 split-0001.apk -",
            b"split-data",
            b"Success: streamed 10 bytes\n",
            0,
            &inbound,
            &mut writes,
        )
        .await;
        respond_command(
            "cmd package install-commit 481",
            b"Success\n",
            0,
            &inbound,
            &mut writes,
        )
        .await;
    };

    let (result, ()) = tokio::join!(install, peer);
    result.expect("the split install should commit");
    assert_eq!(progress.first().map(|item| item.transferred_bytes), Some(0));
    assert_eq!(progress.last().map(|item| item.transferred_bytes), Some(19));
    assert!(progress.iter().all(|item| item.total_bytes == Some(19)));
    client.close().await?;
    tokio::fs::remove_dir_all(directory).await?;
    Ok(())
}

#[tokio::test]
async fn abandons_the_session_when_a_split_write_fails() -> Result<(), Box<dyn std::error::Error>> {
    let (directory, paths) = apk_fixtures().await?;
    let (client, inbound, mut writes) = connected_client().await;
    let manager = PackageManager::new(&client);
    let install = manager.install_split_apks(
        &paths,
        InstallOptions::default(),
        PackageTransferCancellation::new(),
        |_| {},
    );
    let peer = async {
        respond_command(
            "cmd package install-create -r -S 19",
            b"Success: created install session [702]\n",
            0,
            &inbound,
            &mut writes,
        )
        .await;
        respond_install_write(
            "cmd package install-write -S 9 702 split-0000.apk -",
            b"base-data",
            b"Success\n",
            0,
            &inbound,
            &mut writes,
        )
        .await;
        respond_install_write(
            "cmd package install-write -S 10 702 split-0001.apk -",
            b"split-data",
            b"Failure [INSTALL_FAILED_INVALID_APK]\n",
            1,
            &inbound,
            &mut writes,
        )
        .await;
        respond_command(
            "cmd package install-abandon 702",
            b"Success\n",
            0,
            &inbound,
            &mut writes,
        )
        .await;
    };

    let (result, ()) = tokio::join!(install, peer);
    assert!(matches!(result, Err(PackageError::CommandFailed(_))));
    client.close().await?;
    tokio::fs::remove_dir_all(directory).await?;
    Ok(())
}

#[tokio::test]
async fn cancellation_after_create_abandons_the_session() -> Result<(), Box<dyn std::error::Error>>
{
    let (directory, paths) = apk_fixtures().await?;
    let (client, inbound, mut writes) = connected_client().await;
    let manager = PackageManager::new(&client);
    let cancellation = PackageTransferCancellation::new();
    let cancel_from_progress = cancellation.clone();
    let install = manager.install_split_apks(
        &paths,
        InstallOptions::default(),
        cancellation,
        move |update| {
            if update.transferred_bytes == 0 {
                cancel_from_progress.cancel();
            }
        },
    );
    let peer = async {
        respond_command(
            "cmd package install-create -r -S 19",
            b"Success: created install session [819]\n",
            0,
            &inbound,
            &mut writes,
        )
        .await;
        respond_command(
            "cmd package install-abandon 819",
            b"Success\n",
            0,
            &inbound,
            &mut writes,
        )
        .await;
    };

    let (result, ()) = tokio::join!(install, peer);
    assert!(matches!(result, Err(PackageError::Canceled)));
    client.close().await?;
    tokio::fs::remove_dir_all(directory).await?;
    Ok(())
}
