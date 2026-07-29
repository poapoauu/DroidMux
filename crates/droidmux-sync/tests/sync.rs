//! Offline integration tests for ADB Sync v1 operations.

use std::{
    sync::{Arc, Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use adb_auth::RsaAdbCredential;
use adb_client::AdbClient;
use adb_protocol::{ADB_VERSION, AdbCommand, AdbPacket};
use adb_transport::{AdbTransport, AdbTransportError, TransportOperation};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use droidmux_sync::{
    RemoteFile, RemoteFileType, SyncError, SyncProtocolError, TransferCancellation,
    TransferDirection, TransferOptions, TransferProgress, list_directory, lstat, pull_file,
    pull_file_with_options, push_bytes, push_file, push_file_with_options, stat,
};
use tokio::{
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
        "sync-test-device:5555".to_owned()
    }
}

fn credential() -> Arc<RsaAdbCredential> {
    static CREDENTIAL: OnceLock<RsaAdbCredential> = OnceLock::new();
    Arc::new(
        CREDENTIAL
            .get_or_init(|| {
                RsaAdbCredential::generate("droidmux@sync-test")
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

fn device_connect(features: &str) -> AdbPacket {
    let banner = format!("device::product=sync-test;features={features};\0");
    packet(AdbCommand::Connect, ADB_VERSION, 4096, Bytes::from(banner))
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
    connected_client_with_features("cmd").await
}

async fn connected_client_with_features(
    features: &str,
) -> (
    AdbClient,
    mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    mpsc::UnboundedReceiver<AdbPacket>,
) {
    let (inbound_sender, inbound_receiver) = mpsc::unbounded_channel();
    let (write_sender, mut write_receiver) = mpsc::unbounded_channel();
    inbound_sender
        .send(Ok(device_connect(features)))
        .expect("the handshake channel should be open");
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
    (client, inbound_sender, write_receiver)
}

async fn accept_sync_open(
    inbound: &mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    writes: &mut mpsc::UnboundedReceiver<AdbPacket>,
) -> (u32, u32) {
    let open = receive_packet(writes).await;
    assert_eq!(open.command, AdbCommand::Open);
    assert_eq!(&open.payload[..], b"sync:\0");
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
) -> Bytes {
    let write = receive_packet(writes).await;
    assert_eq!(write.command, AdbCommand::Write);
    inbound
        .send(Ok(empty_packet(AdbCommand::Okay, write.arg1, write.arg0)))
        .expect("the WRTE acknowledgement should reach the client");
    write.payload
}

async fn send_device_write(
    payload: Bytes,
    local_id: u32,
    remote_id: u32,
    inbound: &mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    writes: &mut mpsc::UnboundedReceiver<AdbPacket>,
) {
    inbound
        .send(Ok(packet(AdbCommand::Write, remote_id, local_id, payload)))
        .expect("the device payload should reach the client");
    let okay = receive_packet(writes).await;
    assert_eq!(okay.command, AdbCommand::Okay);
    assert_eq!((okay.arg0, okay.arg1), (local_id, remote_id));
}

fn length_prefixed(id: [u8; 4], value: &[u8]) -> Bytes {
    let mut message = BytesMut::with_capacity(8 + value.len());
    message.extend_from_slice(&id);
    message.put_u32_le(u32::try_from(value.len()).expect("fixture length should fit"));
    message.extend_from_slice(value);
    message.freeze()
}

fn directory_response() -> Bytes {
    let name = b"hello.txt";
    let mut response = BytesMut::new();
    response.extend_from_slice(b"DENT");
    response.put_u32_le(0o100_644);
    response.put_u32_le(5);
    response.put_u32_le(1_700_000_000);
    response.put_u32_le(u32::try_from(name.len()).expect("fixture length should fit"));
    response.extend_from_slice(name);
    response.extend_from_slice(b"DONE");
    response.extend_from_slice(&[0_u8; 16]);
    response.freeze()
}

fn stat_response(mode: u32, size: u32, modified: u32) -> Bytes {
    let mut response = BytesMut::new();
    response.extend_from_slice(b"STAT");
    response.put_u32_le(mode);
    response.put_u32_le(size);
    response.put_u32_le(modified);
    response.freeze()
}

#[allow(clippy::too_many_arguments)]
fn append_v2_fields(
    response: &mut BytesMut,
    error: u32,
    device: u64,
    inode: u64,
    mode: u32,
    link_count: u32,
    uid: u32,
    gid: u32,
    size: u64,
    accessed: u64,
    modified: u64,
    changed: u64,
) {
    response.put_u32_le(error);
    response.put_u64_le(device);
    response.put_u64_le(inode);
    response.put_u32_le(mode);
    response.put_u32_le(link_count);
    response.put_u32_le(uid);
    response.put_u32_le(gid);
    response.put_u64_le(size);
    response.put_u64_le(accessed);
    response.put_u64_le(modified);
    response.put_u64_le(changed);
}

fn stat_v2_response(id: [u8; 4], mode: u32, size: u64) -> Bytes {
    let mut response = BytesMut::new();
    response.extend_from_slice(&id);
    append_v2_fields(
        &mut response,
        0,
        11,
        22,
        mode,
        2,
        2000,
        2001,
        size,
        1_700_000_000,
        1_700_000_001,
        1_700_000_002,
    );
    response.freeze()
}

fn directory_v2_response() -> Bytes {
    let name = b"large.bin";
    let mut response = BytesMut::new();
    response.extend_from_slice(b"DNT2");
    append_v2_fields(
        &mut response,
        0,
        33,
        44,
        0o100_640,
        1,
        2000,
        2000,
        u64::from(u32::MAX) + 42,
        1_700_000_000,
        1_700_000_001,
        1_700_000_002,
    );
    response.put_u32_le(u32::try_from(name.len()).expect("fixture name length should fit"));
    response.extend_from_slice(name);
    response.extend_from_slice(b"DONE");
    response.extend_from_slice(&[0_u8; 72]);
    response.freeze()
}

fn temporary_file(name: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the system clock should follow the Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "droidmux-sync-{}-{nonce}-{name}",
        std::process::id()
    ))
}

async fn complete_stat_operation(use_lstat: bool, path: &'static str, mode: u32) -> RemoteFile {
    let (client, inbound, mut writes) = connected_client().await;
    let task = tokio::spawn(async move {
        if use_lstat {
            lstat(&client, path).await
        } else {
            stat(&client, path).await
        }
    });
    serve_stat_request(path, mode, 123, &inbound, &mut writes).await;
    task.await
        .expect("the metadata task should not panic")
        .expect("the STAT response should decode")
}

async fn serve_stat_request(
    path: &str,
    mode: u32,
    size: u32,
    inbound: &mpsc::UnboundedSender<Result<AdbPacket, AdbTransportError>>,
    writes: &mut mpsc::UnboundedReceiver<AdbPacket>,
) {
    let (local_id, remote_id) = accept_sync_open(inbound, writes).await;
    assert_eq!(
        acknowledge_host_write(inbound, writes).await,
        length_prefixed(*b"STAT", path.as_bytes())
    );
    send_device_write(
        stat_response(mode, size, 1_700_000_001),
        local_id,
        remote_id,
        inbound,
        writes,
    )
    .await;
    assert_eq!(
        acknowledge_host_write(inbound, writes).await,
        Bytes::from_static(b"QUIT\0\0\0\0")
    );
    assert_eq!(receive_packet(writes).await.command, AdbCommand::Close);
}

#[tokio::test]
async fn lists_a_fragmented_directory_response() {
    let (client, inbound, mut writes) = connected_client().await;
    let task = tokio::spawn(async move { list_directory(&client, "/sdcard").await });
    let (local_id, remote_id) = accept_sync_open(&inbound, &mut writes).await;

    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        length_prefixed(*b"LIST", b"/sdcard")
    );

    let response = directory_response();
    for fragment in [
        response.slice(..3),
        response.slice(3..17),
        response.slice(17..),
    ] {
        send_device_write(fragment, local_id, remote_id, &inbound, &mut writes).await;
    }

    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        Bytes::from_static(b"QUIT\0\0\0\0")
    );
    assert_eq!(receive_packet(&mut writes).await.command, AdbCommand::Close);

    let files = task
        .await
        .expect("the list task should not panic")
        .expect("the directory response should decode");
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].name, "hello.txt");
    assert_eq!(files[0].path, "/sdcard/hello.txt");
    assert_eq!(files[0].size, 5);
    assert_eq!(files[0].mode, 0o100_644);
    assert_eq!(files[0].file_type, RemoteFileType::RegularFile);
    assert!(files[0].modified_at.is_some());
}

#[tokio::test]
async fn stat_and_lstat_have_separate_apis_with_a_v1_fallback() {
    let regular = complete_stat_operation(false, "/sdcard/photo.jpg", 0o100_640).await;
    assert_eq!(regular.name, "photo.jpg");
    assert_eq!(regular.path, "/sdcard/photo.jpg");
    assert_eq!(regular.size, 123);
    assert_eq!(regular.file_type, RemoteFileType::RegularFile);

    let link = complete_stat_operation(true, "/sdcard/latest", 0o120_777).await;
    assert_eq!(link.name, "latest");
    assert_eq!(link.file_type, RemoteFileType::SymbolicLink);
}

#[tokio::test]
async fn stat_uses_sync_v2_and_preserves_64_bit_metadata_when_negotiated() {
    let path = "/sdcard/large.bin";
    let (client, inbound, mut writes) =
        connected_client_with_features("stat_v2,ls_v2,sendrecv_v2").await;
    assert_eq!(
        client.features().collect::<Vec<_>>(),
        vec!["ls_v2", "sendrecv_v2", "stat_v2"]
    );
    let task = tokio::spawn(async move { stat(&client, path).await });
    let (local_id, remote_id) = accept_sync_open(&inbound, &mut writes).await;
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        length_prefixed(*b"STA2", path.as_bytes())
    );
    send_device_write(
        stat_v2_response(*b"STA2", 0o100_640, u64::from(u32::MAX) + 7),
        local_id,
        remote_id,
        &inbound,
        &mut writes,
    )
    .await;
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        Bytes::from_static(b"QUIT\0\0\0\0")
    );
    assert_eq!(receive_packet(&mut writes).await.command, AdbCommand::Close);

    let file = task
        .await
        .expect("the stat task should not panic")
        .expect("the STA2 response should decode");
    assert_eq!(file.size, u64::from(u32::MAX) + 7);
    let metadata = file
        .metadata_v2
        .expect("Sync v2 metadata should be retained");
    assert_eq!(metadata.device, 11);
    assert_eq!(metadata.inode, 22);
    assert_eq!(metadata.uid, 2000);
    assert_eq!(metadata.gid, 2001);
}

#[tokio::test]
async fn list_uses_lis2_and_consumes_the_full_done_record() {
    let path = "/sdcard";
    let (client, inbound, mut writes) = connected_client_with_features("ls_v2").await;
    let task = tokio::spawn(async move { list_directory(&client, path).await });
    let (local_id, remote_id) = accept_sync_open(&inbound, &mut writes).await;
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        length_prefixed(*b"LIS2", path.as_bytes())
    );
    send_device_write(
        directory_v2_response(),
        local_id,
        remote_id,
        &inbound,
        &mut writes,
    )
    .await;
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        Bytes::from_static(b"QUIT\0\0\0\0")
    );
    assert_eq!(receive_packet(&mut writes).await.command, AdbCommand::Close);

    let files = task
        .await
        .expect("the list task should not panic")
        .expect("the LIS2 response should decode");
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].name, "large.bin");
    assert_eq!(files[0].size, u64::from(u32::MAX) + 42);
    assert!(files[0].metadata_v2.is_some());
}

#[tokio::test]
async fn upload_uses_sendrecv_v2_setup_when_negotiated() {
    let path = "/data/local/tmp/v2.bin";
    let (client, inbound, mut writes) = connected_client_with_features("sendrecv_v2").await;
    let task = tokio::spawn(async move { push_bytes(&client, b"v2-data", path, |_| {}).await });
    let (local_id, remote_id) = accept_sync_open(&inbound, &mut writes).await;

    let mut expected_setup = BytesMut::from(&length_prefixed(*b"SND2", path.as_bytes())[..]);
    expected_setup.extend_from_slice(b"SND2");
    expected_setup.put_u32_le(0o644);
    expected_setup.put_u32_le(0);
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        expected_setup.freeze()
    );
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        length_prefixed(*b"DATA", b"v2-data")
    );
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        Bytes::from_static(b"DONE\0\0\0\0")
    );
    send_device_write(
        Bytes::from_static(b"OKAY\0\0\0\0"),
        local_id,
        remote_id,
        &inbound,
        &mut writes,
    )
    .await;
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        Bytes::from_static(b"QUIT\0\0\0\0")
    );
    assert_eq!(receive_packet(&mut writes).await.command, AdbCommand::Close);
    task.await
        .expect("the upload task should not panic")
        .expect("the SND2 upload should complete");
}

#[tokio::test]
async fn download_uses_sendrecv_v2_setup_when_negotiated() {
    let remote_path = "/sdcard/v2.bin";
    let local_path = temporary_file("v2-download.bin");
    let task_path = local_path.clone();
    let (client, inbound, mut writes) = connected_client_with_features("sendrecv_v2").await;
    let task =
        tokio::spawn(async move { pull_file(&client, remote_path, &task_path, |_| {}).await });

    serve_stat_request(remote_path, 0o100_644, 7, &inbound, &mut writes).await;
    let (local_id, remote_id) = accept_sync_open(&inbound, &mut writes).await;
    let mut expected_setup = BytesMut::from(&length_prefixed(*b"RCV2", remote_path.as_bytes())[..]);
    expected_setup.extend_from_slice(b"RCV2");
    expected_setup.put_u32_le(0);
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        expected_setup.freeze()
    );
    let mut response = BytesMut::new();
    response.extend_from_slice(&length_prefixed(*b"DATA", b"v2-data"));
    response.extend_from_slice(b"DONE");
    send_device_write(
        response.freeze(),
        local_id,
        remote_id,
        &inbound,
        &mut writes,
    )
    .await;
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        Bytes::from_static(b"QUIT\0\0\0\0")
    );
    assert_eq!(receive_packet(&mut writes).await.command, AdbCommand::Close);
    task.await
        .expect("the download task should not panic")
        .expect("the RCV2 download should complete");
    assert_eq!(
        std::fs::read(&local_path).expect("the downloaded file should be readable"),
        b"v2-data"
    );
    std::fs::remove_file(local_path).expect("the download fixture should be removed");
}

#[tokio::test]
async fn uploads_in_bounded_adb_packets_and_reports_progress() {
    let local_path = temporary_file("upload.bin");
    let contents: Vec<u8> = (0_u8..=250).cycle().take(10_000).collect();
    std::fs::write(&local_path, &contents).expect("the upload fixture should be written");

    let (client, inbound, mut writes) = connected_client().await;
    let observed = Arc::new(Mutex::new(Vec::<TransferProgress>::new()));
    let callback_observed = observed.clone();
    let task_path = local_path.clone();
    let task = tokio::spawn(async move {
        push_file(&client, &task_path, "/sdcard/upload.bin", move |progress| {
            callback_observed
                .lock()
                .expect("the progress mutex should not be poisoned")
                .push(progress);
        })
        .await
    });
    let (local_id, remote_id) = accept_sync_open(&inbound, &mut writes).await;
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        length_prefixed(*b"SEND", b"/sdcard/upload.bin,420")
    );

    let expected_stream_length = 8 + contents.len();
    let mut upload_stream = BytesMut::with_capacity(expected_stream_length);
    while upload_stream.len() < expected_stream_length {
        let packet_payload = acknowledge_host_write(&inbound, &mut writes).await;
        assert!(packet_payload.len() <= 4096);
        upload_stream.extend_from_slice(&packet_payload);
    }
    assert_eq!(&upload_stream[..4], b"DATA");
    assert_eq!(
        u32::from_le_bytes(
            upload_stream[4..8]
                .try_into()
                .expect("the DATA header should be complete")
        ),
        u32::try_from(contents.len()).expect("the fixture size should fit")
    );
    assert_eq!(&upload_stream[8..], &contents);

    let done = acknowledge_host_write(&inbound, &mut writes).await;
    assert_eq!(&done[..4], b"DONE");
    assert_eq!(done.len(), 8);
    send_device_write(
        Bytes::from_static(b"OKAY\0\0\0\0"),
        local_id,
        remote_id,
        &inbound,
        &mut writes,
    )
    .await;
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        Bytes::from_static(b"QUIT\0\0\0\0")
    );
    assert_eq!(receive_packet(&mut writes).await.command, AdbCommand::Close);
    task.await
        .expect("the upload task should not panic")
        .expect("the upload should succeed");

    let progress = observed
        .lock()
        .expect("the progress mutex should not be poisoned");
    assert_eq!(
        progress.first().map(|value| value.transferred_bytes),
        Some(0)
    );
    assert_eq!(
        progress.last().map(|value| value.transferred_bytes),
        Some(10_000)
    );
    assert!(
        progress
            .iter()
            .all(|value| value.direction == TransferDirection::Upload)
    );
    assert!(
        progress
            .iter()
            .all(|value| value.total_bytes == Some(10_000))
    );
    drop(progress);

    std::fs::remove_file(local_path).expect("the upload fixture should be removed");
}

#[tokio::test]
async fn downloads_coalesced_data_and_commits_after_done() {
    let local_path = temporary_file("download.bin");
    let contents = Bytes::from_static(b"downloaded-through-native-sync");
    let (client, inbound, mut writes) = connected_client().await;
    let observed = Arc::new(Mutex::new(Vec::<TransferProgress>::new()));
    let callback_observed = observed.clone();
    let task_path = local_path.clone();
    let task = tokio::spawn(async move {
        pull_file(
            &client,
            "/sdcard/download.bin",
            &task_path,
            move |progress| {
                callback_observed
                    .lock()
                    .expect("the progress mutex should not be poisoned")
                    .push(progress);
            },
        )
        .await
    });

    serve_stat_request(
        "/sdcard/download.bin",
        0o100_644,
        u32::try_from(contents.len()).expect("fixture size should fit"),
        &inbound,
        &mut writes,
    )
    .await;
    let (local_id, remote_id) = accept_sync_open(&inbound, &mut writes).await;
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        length_prefixed(*b"RECV", b"/sdcard/download.bin")
    );

    let mut response = BytesMut::new();
    response.extend_from_slice(&length_prefixed(*b"DATA", &contents));
    response.extend_from_slice(b"DONE");
    send_device_write(
        response.freeze(),
        local_id,
        remote_id,
        &inbound,
        &mut writes,
    )
    .await;
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        Bytes::from_static(b"QUIT\0\0\0\0")
    );
    assert_eq!(receive_packet(&mut writes).await.command, AdbCommand::Close);
    task.await
        .expect("the download task should not panic")
        .expect("the download should succeed");

    assert_eq!(
        std::fs::read(&local_path).expect("the completed download should be readable"),
        contents
    );
    let progress = observed
        .lock()
        .expect("the progress mutex should not be poisoned");
    assert_eq!(
        progress.first().map(|value| value.transferred_bytes),
        Some(0)
    );
    assert_eq!(
        progress.last().map(|value| value.transferred_bytes),
        Some(contents.len() as u64)
    );
    assert!(
        progress
            .iter()
            .all(|value| value.direction == TransferDirection::Download)
    );
    assert!(
        progress
            .iter()
            .all(|value| value.total_bytes == Some(contents.len() as u64))
    );
    drop(progress);
    std::fs::remove_file(local_path).expect("the download fixture should be removed");
}

#[tokio::test]
async fn cancellation_interrupts_a_blocked_download_without_a_partial_file() {
    let local_path = temporary_file("canceled-download.bin");
    let cancellation = TransferCancellation::new();
    let options = TransferOptions {
        cancellation: cancellation.clone(),
        ..TransferOptions::default()
    };
    let (client, inbound, mut writes) = connected_client().await;
    let task_path = local_path.clone();
    let task = tokio::spawn(async move {
        pull_file_with_options(&client, "/sdcard/blocked.bin", &task_path, &options, |_| {}).await
    });

    serve_stat_request(
        "/sdcard/blocked.bin",
        0o100_644,
        1024,
        &inbound,
        &mut writes,
    )
    .await;
    let _ = accept_sync_open(&inbound, &mut writes).await;
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        length_prefixed(*b"RECV", b"/sdcard/blocked.bin")
    );

    cancellation.cancel();
    assert_eq!(receive_packet(&mut writes).await.command, AdbCommand::Close);
    assert!(matches!(
        task.await.expect("the canceled task should not panic"),
        Err(SyncError::Canceled)
    ));
    assert!(!local_path.exists());
}

#[tokio::test]
async fn remote_failures_are_preserved_and_do_not_send_quit() {
    let (client, inbound, mut writes) = connected_client().await;
    let task = tokio::spawn(async move { list_directory(&client, "/missing").await });
    let (local_id, remote_id) = accept_sync_open(&inbound, &mut writes).await;
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        length_prefixed(*b"LIST", b"/missing")
    );
    send_device_write(
        length_prefixed(*b"FAIL", b"no such file or directory"),
        local_id,
        remote_id,
        &inbound,
        &mut writes,
    )
    .await;
    assert_eq!(receive_packet(&mut writes).await.command, AdbCommand::Close);

    let error = task
        .await
        .expect("the failed list task should not panic")
        .expect_err("the device failure should reach the caller");
    assert!(matches!(
        error,
        SyncError::Protocol(SyncProtocolError::Remote(message))
            if message == "no such file or directory"
    ));
}

#[tokio::test]
async fn oversized_download_chunks_are_rejected_before_allocation() {
    let local_path = temporary_file("oversized-download.bin");
    let (client, inbound, mut writes) = connected_client().await;
    let task_path = local_path.clone();
    let task = tokio::spawn(async move {
        pull_file(&client, "/sdcard/oversized.bin", &task_path, |_| {}).await
    });
    serve_stat_request(
        "/sdcard/oversized.bin",
        0o100_644,
        70_000,
        &inbound,
        &mut writes,
    )
    .await;
    let (local_id, remote_id) = accept_sync_open(&inbound, &mut writes).await;
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        length_prefixed(*b"RECV", b"/sdcard/oversized.bin")
    );
    let mut oversized_header = BytesMut::new();
    oversized_header.extend_from_slice(b"DATA");
    oversized_header.put_u32_le(65_537);
    send_device_write(
        oversized_header.freeze(),
        local_id,
        remote_id,
        &inbound,
        &mut writes,
    )
    .await;
    assert_eq!(receive_packet(&mut writes).await.command, AdbCommand::Close);

    let error = task
        .await
        .expect("the rejected download task should not panic")
        .expect_err("the oversized DATA frame should fail");
    assert!(matches!(
        error,
        SyncError::Protocol(SyncProtocolError::FieldTooLarge {
            field: "DATA payload",
            limit: 65_536,
            actual: 65_537,
        })
    ));
    assert!(!local_path.exists());
}

#[tokio::test]
async fn cancellation_interrupts_upload_backpressure() {
    let local_path = temporary_file("canceled-upload.bin");
    std::fs::write(&local_path, vec![0x5a; 10_000]).expect("the upload fixture should be written");
    let cancellation = TransferCancellation::new();
    let options = TransferOptions {
        cancellation: cancellation.clone(),
        ..TransferOptions::default()
    };
    let (client, inbound, mut writes) = connected_client().await;
    let task_path = local_path.clone();
    let task = tokio::spawn(async move {
        push_file_with_options(
            &client,
            &task_path,
            "/sdcard/canceled.bin",
            &options,
            |_| {},
        )
        .await
    });

    let _ = accept_sync_open(&inbound, &mut writes).await;
    assert_eq!(
        acknowledge_host_write(&inbound, &mut writes).await,
        length_prefixed(*b"SEND", b"/sdcard/canceled.bin,420")
    );
    let blocked_data = receive_packet(&mut writes).await;
    assert_eq!(blocked_data.command, AdbCommand::Write);
    assert_eq!(&blocked_data.payload[..4], b"DATA");

    cancellation.cancel();
    assert_eq!(receive_packet(&mut writes).await.command, AdbCommand::Close);
    assert!(matches!(
        task.await.expect("the canceled upload should not panic"),
        Err(SyncError::Canceled)
    ));
    std::fs::remove_file(local_path).expect("the upload fixture should be removed");
}

#[tokio::test]
async fn download_never_overwrites_an_existing_destination() {
    let local_path = temporary_file("existing-download.bin");
    std::fs::write(&local_path, b"keep-me").expect("the destination fixture should be written");
    let (client, _inbound, mut writes) = connected_client().await;

    let error = pull_file(&client, "/sdcard/replacement.bin", &local_path, |_| {})
        .await
        .expect_err("an existing destination should be rejected");
    assert!(matches!(error, SyncError::DestinationExists(path) if path == local_path));
    assert!(
        timeout(Duration::from_millis(20), writes.recv())
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(&local_path).expect("the destination should remain readable"),
        b"keep-me"
    );
    std::fs::remove_file(local_path).expect("the destination fixture should be removed");
}
