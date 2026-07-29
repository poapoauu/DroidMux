use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use adb_client::AdbClient;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
};

use crate::{
    RemoteFile, RemoteFileMetadataV2, RemoteFileType, SyncError, SyncProtocolError,
    TransferCancellation, TransferDirection, TransferOptions, TransferProgress,
    session::{SYNC_PATH_MAX, SyncSession, validate_remote_path},
};

const MAX_DIRECTORY_ENTRIES: usize = 100_000;
const SYNC_DATA_MAX: usize = 64 * 1024;
const LIST_V1_DONE_TAIL: usize = 16;
const LIST_V2_DONE_TAIL: usize = 72;
const STAT_V2_FEATURE: &str = "stat_v2";
const LIST_V2_FEATURE: &str = "ls_v2";
const SEND_RECV_V2_FEATURE: &str = "sendrecv_v2";
static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Lists one remote directory using Sync v2 when negotiated, or Sync v1.
///
/// # Errors
///
/// Returns an error when the path is invalid, the ADB stream fails, the
/// device rejects the operation, or its response violates Sync framing or
/// defensive size limits.
pub async fn list_directory(client: &AdbClient, path: &str) -> Result<Vec<RemoteFile>, SyncError> {
    validate_remote_path(path)?;
    let mut session = SyncSession::open(client).await?;
    let result = if client.supports_feature(LIST_V2_FEATURE) {
        list_v2_inner(&mut session, path).await
    } else {
        list_v1_inner(&mut session, path).await
    };
    session.finish(result).await
}

/// Reads metadata for a remote path, following the final symbolic link when
/// Sync v2 is negotiated. Sync v1 falls back to its historical lstat behavior.
///
/// # Errors
///
/// Returns an error when the path is invalid, the ADB stream fails, or the
/// device returns an invalid or failed response.
pub async fn stat(client: &AdbClient, path: &str) -> Result<RemoteFile, SyncError> {
    stat_negotiated(client, path, false).await
}

/// Reads metadata without following the final symbolic link.
///
/// Sync v1 only provides its `STAT` operation, which already has `lstat`
/// semantics. This separate API preserves the distinction needed by Sync v2.
///
/// # Errors
///
/// Returns an error when the path is invalid, the ADB stream fails, or the
/// device returns an invalid or failed response.
pub async fn lstat(client: &AdbClient, path: &str) -> Result<RemoteFile, SyncError> {
    stat_negotiated(client, path, true).await
}

/// Uploads a local file with bounded memory and progress callbacks.
///
/// # Errors
///
/// Returns an error when local file I/O fails, the remote path is invalid,
/// the device rejects the upload, the ADB stream fails, or framing is invalid.
pub async fn push_file<F>(
    client: &AdbClient,
    local_path: &std::path::Path,
    remote_path: &str,
    progress: F,
) -> Result<(), SyncError>
where
    F: FnMut(TransferProgress) + Send,
{
    let options = TransferOptions::default();
    push_file_with_options(client, local_path, remote_path, &options, progress).await
}

/// Uploads a local file with explicit permissions and cancellation.
///
/// # Errors
///
/// Returns the same errors as [`push_file`], plus [`SyncError::Canceled`] when
/// the supplied cancellation signal is triggered.
pub async fn push_file_with_options<F>(
    client: &AdbClient,
    local_path: &std::path::Path,
    remote_path: &str,
    options: &TransferOptions,
    mut progress: F,
) -> Result<(), SyncError>
where
    F: FnMut(TransferProgress) + Send,
{
    validate_remote_path(remote_path)?;
    if options.file_mode > 0o7777 {
        return Err(SyncProtocolError::InvalidFileMode(options.file_mode).into());
    }
    let legacy_send_length = remote_path.len() + 1 + options.file_mode.to_string().len();
    if !client.supports_feature(SEND_RECV_V2_FEATURE) && legacy_send_length > SYNC_PATH_MAX {
        return Err(SyncProtocolError::PathTooLong {
            limit: SYNC_PATH_MAX,
            actual: legacy_send_length,
        }
        .into());
    }

    let mut file = File::open(local_path).await?;
    let metadata = file.metadata().await?;
    let total = metadata.len();
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |duration| {
            u32::try_from(duration.as_secs()).unwrap_or(u32::MAX)
        });

    let mut session = SyncSession::open(client).await?;
    let result = upload_inner(
        &mut session,
        &mut file,
        UploadRequest {
            remote_path,
            mode: options.file_mode,
            use_v2: client.supports_feature(SEND_RECV_V2_FEATURE),
            total,
            modified,
        },
        &options.cancellation,
        &mut progress,
    )
    .await;
    session.finish(result).await
}

/// Uploads an in-memory resource with bounded ADB Sync packets.
///
/// This is intended for small application-owned resources that should not be
/// materialized as temporary host files.
///
/// # Errors
///
/// Returns an error when the remote path or mode is invalid, the device
/// rejects the upload, the operation is canceled, or the ADB stream fails.
pub async fn push_bytes<F>(
    client: &AdbClient,
    data: &[u8],
    remote_path: &str,
    progress: F,
) -> Result<(), SyncError>
where
    F: FnMut(TransferProgress) + Send,
{
    let options = TransferOptions::default();
    push_bytes_with_options(client, data, remote_path, &options, progress).await
}

/// Uploads an in-memory resource with explicit permissions and cancellation.
///
/// # Errors
///
/// Returns the same errors as [`push_bytes`].
pub async fn push_bytes_with_options<F>(
    client: &AdbClient,
    data: &[u8],
    remote_path: &str,
    options: &TransferOptions,
    mut progress: F,
) -> Result<(), SyncError>
where
    F: FnMut(TransferProgress) + Send,
{
    validate_remote_path(remote_path)?;
    if options.file_mode > 0o7777 {
        return Err(SyncProtocolError::InvalidFileMode(options.file_mode).into());
    }
    let legacy_send_length = remote_path.len() + 1 + options.file_mode.to_string().len();
    if !client.supports_feature(SEND_RECV_V2_FEATURE) && legacy_send_length > SYNC_PATH_MAX {
        return Err(SyncProtocolError::PathTooLong {
            limit: SYNC_PATH_MAX,
            actual: legacy_send_length,
        }
        .into());
    }

    let total = u64::try_from(data.len()).unwrap_or(u64::MAX);
    let mut reader = data;
    let mut session = SyncSession::open(client).await?;
    let result = upload_inner(
        &mut session,
        &mut reader,
        UploadRequest {
            remote_path,
            mode: options.file_mode,
            use_v2: client.supports_feature(SEND_RECV_V2_FEATURE),
            total,
            modified: 0,
        },
        &options.cancellation,
        &mut progress,
    )
    .await;
    session.finish(result).await
}

/// Downloads a remote file with bounded memory and progress callbacks.
///
/// The completed download is atomically renamed from a same-directory
/// temporary file. Existing destination paths are never overwritten.
///
/// # Errors
///
/// Returns an error when local file I/O fails, the destination already
/// exists, the device rejects the download, the ADB stream fails, or framing
/// is invalid.
pub async fn pull_file<F>(
    client: &AdbClient,
    remote_path: &str,
    local_path: &Path,
    progress: F,
) -> Result<(), SyncError>
where
    F: FnMut(TransferProgress) + Send,
{
    let options = TransferOptions::default();
    pull_file_with_options(client, remote_path, local_path, &options, progress).await
}

/// Downloads a remote file with explicit cancellation.
///
/// # Errors
///
/// Returns the same errors as [`pull_file`], plus [`SyncError::Canceled`] when
/// the supplied cancellation signal is triggered. A canceled transfer removes
/// its temporary file and leaves the destination absent.
pub async fn pull_file_with_options<F>(
    client: &AdbClient,
    remote_path: &str,
    local_path: &Path,
    options: &TransferOptions,
    mut progress: F,
) -> Result<(), SyncError>
where
    F: FnMut(TransferProgress) + Send,
{
    validate_remote_path(remote_path)?;
    if options.cancellation.is_canceled() {
        return Err(SyncError::Canceled);
    }
    if fs::try_exists(local_path).await? {
        return Err(SyncError::DestinationExists(local_path.to_owned()));
    }

    let expected_size = stat(client, remote_path).await?.size;
    if options.cancellation.is_canceled() {
        return Err(SyncError::Canceled);
    }
    let (temporary_path, mut output) = create_temporary_file(local_path).await?;
    let transfer = download_to_writer(
        client,
        remote_path,
        &mut output,
        expected_size,
        &options.cancellation,
        &mut progress,
    )
    .await;
    drop(output);

    if let Err(error) = transfer {
        let _ = fs::remove_file(&temporary_path).await;
        return Err(error);
    }
    if options.cancellation.is_canceled() {
        let _ = fs::remove_file(&temporary_path).await;
        return Err(SyncError::Canceled);
    }
    match fs::try_exists(local_path).await {
        Ok(true) => {
            let _ = fs::remove_file(&temporary_path).await;
            return Err(SyncError::DestinationExists(local_path.to_owned()));
        }
        Ok(false) => {}
        Err(error) => {
            let _ = fs::remove_file(&temporary_path).await;
            return Err(error.into());
        }
    }
    if let Err(error) = fs::rename(&temporary_path, local_path).await {
        let _ = fs::remove_file(&temporary_path).await;
        return Err(error.into());
    }
    Ok(())
}

async fn download_to_writer<W, F>(
    client: &AdbClient,
    remote_path: &str,
    writer: &mut W,
    expected_size: u64,
    cancellation: &TransferCancellation,
    progress: &mut F,
) -> Result<(), SyncError>
where
    W: AsyncWrite + Unpin,
    F: FnMut(TransferProgress),
{
    let mut session = SyncSession::open(client).await?;
    let result = download_inner(
        &mut session,
        writer,
        remote_path,
        client.supports_feature(SEND_RECV_V2_FEATURE),
        expected_size,
        cancellation,
        progress,
    )
    .await;
    session.finish(result).await
}

async fn download_inner<W, F>(
    session: &mut SyncSession,
    writer: &mut W,
    remote_path: &str,
    use_v2: bool,
    expected_size: u64,
    cancellation: &TransferCancellation,
    progress: &mut F,
) -> Result<(), SyncError>
where
    W: AsyncWrite + Unpin,
    F: FnMut(TransferProgress),
{
    if use_v2 {
        session
            .write_recv_v2(remote_path.as_bytes(), cancellation)
            .await?;
    } else {
        session
            .write_length_prefixed_cancellable(b"RECV", remote_path.as_bytes(), cancellation)
            .await?;
    }
    report_progress(
        progress,
        TransferDirection::Download,
        0,
        Some(expected_size),
    );
    let mut transferred = 0_u64;

    loop {
        match session.read_id_cancellable(cancellation).await? {
            id if id == *b"DATA" => {
                let length = usize::try_from(
                    session
                        .read_u32_cancellable("DATA length", cancellation)
                        .await?,
                )
                .expect("u32 fits every supported platform");
                if length > SYNC_DATA_MAX {
                    return Err(SyncProtocolError::FieldTooLarge {
                        field: "DATA payload",
                        limit: SYNC_DATA_MAX,
                        actual: length,
                    }
                    .into());
                }
                let data = session
                    .read_bytes_cancellable(length, "DATA payload", cancellation)
                    .await?;
                write_download_data(writer, &data, cancellation).await?;
                transferred = transferred.saturating_add(length as u64);
                report_progress(
                    progress,
                    TransferDirection::Download,
                    transferred,
                    Some(expected_size),
                );
            }
            id if id == *b"DONE" => {
                flush_download(writer, cancellation).await?;
                return Ok(());
            }
            id if id == *b"FAIL" => {
                return Err(session.read_remote_error_cancellable(cancellation).await?);
            }
            id => {
                return Err(SyncProtocolError::UnexpectedResponse {
                    expected: "DATA, DONE, or FAIL",
                    actual: String::from_utf8_lossy(&id).into_owned(),
                }
                .into());
            }
        }
    }
}

async fn write_download_data<W>(
    writer: &mut W,
    data: &[u8],
    cancellation: &TransferCancellation,
) -> Result<(), SyncError>
where
    W: AsyncWrite + Unpin,
{
    let write = writer.write_all(data);
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SyncError::Canceled),
        result = write => result.map_err(SyncError::from),
    }
}

async fn flush_download<W>(
    writer: &mut W,
    cancellation: &TransferCancellation,
) -> Result<(), SyncError>
where
    W: AsyncWrite + Unpin,
{
    let flush = writer.flush();
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SyncError::Canceled),
        result = flush => result.map_err(SyncError::from),
    }
}

async fn create_temporary_file(destination: &Path) -> Result<(PathBuf, File), SyncError> {
    let file_name = destination.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "download destination must include a file name",
        )
    })?;
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for _ in 0..100 {
        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let mut temporary_name = OsString::from(".");
        temporary_name.push(file_name);
        temporary_name.push(format!(".droidmux-part-{timestamp}-{sequence}"));
        let path = parent.join(temporary_name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not allocate a unique download temporary file",
    )
    .into())
}

struct UploadRequest<'a> {
    remote_path: &'a str,
    mode: u32,
    use_v2: bool,
    total: u64,
    modified: u32,
}

async fn upload_inner<R, F>(
    session: &mut SyncSession,
    reader: &mut R,
    request: UploadRequest<'_>,
    cancellation: &TransferCancellation,
    progress: &mut F,
) -> Result<(), SyncError>
where
    R: AsyncRead + Unpin,
    F: FnMut(TransferProgress),
{
    if request.use_v2 {
        session
            .write_send_v2(request.remote_path.as_bytes(), request.mode, cancellation)
            .await?;
    } else {
        let send_path = format!("{},{}", request.remote_path, request.mode);
        session
            .write_length_prefixed_cancellable(b"SEND", send_path.as_bytes(), cancellation)
            .await?;
    }
    report_progress(progress, TransferDirection::Upload, 0, Some(request.total));

    let mut transferred = 0_u64;
    let mut buffer = vec![0_u8; SYNC_DATA_MAX];
    loop {
        let read = read_chunk(reader, &mut buffer, cancellation).await?;
        if read == 0 {
            break;
        }
        session.write_data(&buffer[..read], cancellation).await?;
        transferred = transferred.saturating_add(read as u64);
        report_progress(
            progress,
            TransferDirection::Upload,
            transferred,
            Some(request.total),
        );
    }

    session
        .write_id_value_cancellable(b"DONE", request.modified, cancellation)
        .await?;
    match session.read_id_cancellable(cancellation).await? {
        id if id == *b"OKAY" => {
            let _ = session
                .read_u32_cancellable("SEND completion value", cancellation)
                .await?;
            Ok(())
        }
        id if id == *b"FAIL" => Err(session.read_remote_error_cancellable(cancellation).await?),
        id => Err(SyncProtocolError::UnexpectedResponse {
            expected: "OKAY or FAIL",
            actual: String::from_utf8_lossy(&id).into_owned(),
        }
        .into()),
    }
}

async fn read_chunk<R>(
    reader: &mut R,
    buffer: &mut [u8],
    cancellation: &TransferCancellation,
) -> Result<usize, SyncError>
where
    R: AsyncRead + Unpin,
{
    let read = reader.read(buffer);
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SyncError::Canceled),
        result = read => result.map_err(SyncError::from),
    }
}

fn report_progress<F>(
    progress: &mut F,
    direction: TransferDirection,
    transferred_bytes: u64,
    total_bytes: Option<u64>,
) where
    F: FnMut(TransferProgress),
{
    progress(TransferProgress {
        direction,
        transferred_bytes,
        total_bytes,
    });
}

async fn stat_negotiated(
    client: &AdbClient,
    path: &str,
    no_follow: bool,
) -> Result<RemoteFile, SyncError> {
    validate_remote_path(path)?;
    let mut session = SyncSession::open(client).await?;
    let result = if client.supports_feature(STAT_V2_FEATURE) {
        stat_v2_inner(&mut session, path, no_follow).await
    } else {
        stat_v1_inner(&mut session, path).await
    };
    session.finish(result).await
}

async fn stat_v1_inner(session: &mut SyncSession, path: &str) -> Result<RemoteFile, SyncError> {
    session
        .write_length_prefixed(b"STAT", path.as_bytes())
        .await?;
    match session.read_id().await? {
        id if id == *b"STAT" => {
            let mode = session.read_u32("STAT mode").await?;
            let size = u64::from(session.read_u32("STAT size").await?);
            let modified_seconds = u64::from(session.read_u32("STAT modification time").await?);
            if mode == 0 && size == 0 && modified_seconds == 0 {
                return Err(SyncProtocolError::Remote(
                    "STAT returned an all-zero failure response".to_owned(),
                )
                .into());
            }
            Ok(RemoteFile {
                name: remote_name(path),
                path: path.to_owned(),
                size,
                modified_at: system_time(modified_seconds),
                mode,
                file_type: RemoteFileType::from_mode(mode),
                metadata_v2: None,
            })
        }
        id if id == *b"FAIL" => Err(session.read_remote_error().await?),
        id => Err(SyncProtocolError::UnexpectedResponse {
            expected: "STAT or FAIL",
            actual: String::from_utf8_lossy(&id).into_owned(),
        }
        .into()),
    }
}

async fn stat_v2_inner(
    session: &mut SyncSession,
    path: &str,
    no_follow: bool,
) -> Result<RemoteFile, SyncError> {
    let request_id = if no_follow { b"LST2" } else { b"STA2" };
    session
        .write_length_prefixed(request_id, path.as_bytes())
        .await?;
    let response_id = session.read_id().await?;
    if response_id == *b"FAIL" {
        return Err(session.read_remote_error().await?);
    }
    if response_id != *b"STA2" && response_id != *b"LST2" {
        return Err(SyncProtocolError::UnexpectedResponse {
            expected: "STA2, LST2, or FAIL",
            actual: String::from_utf8_lossy(&response_id).into_owned(),
        }
        .into());
    }

    let fields = read_v2_fields(session, if no_follow { "LST2" } else { "STA2" }).await?;
    Ok(fields.into_remote_file(remote_name(path), path.to_owned()))
}

async fn list_v1_inner(
    session: &mut SyncSession,
    directory: &str,
) -> Result<Vec<RemoteFile>, SyncError> {
    session
        .write_length_prefixed(b"LIST", directory.as_bytes())
        .await?;
    let mut files = Vec::new();

    loop {
        match session.read_id().await? {
            id if id == *b"DENT" => {
                if files.len() == MAX_DIRECTORY_ENTRIES {
                    return Err(SyncProtocolError::TooManyDirectoryEntries {
                        limit: MAX_DIRECTORY_ENTRIES,
                    }
                    .into());
                }
                files.push(read_directory_entry(session, directory).await?);
            }
            id if id == *b"DONE" => {
                session
                    .skip(LIST_V1_DONE_TAIL, "LIST completion record")
                    .await?;
                return Ok(files);
            }
            id if id == *b"FAIL" => return Err(session.read_remote_error().await?),
            id => {
                return Err(SyncProtocolError::UnexpectedResponse {
                    expected: "DENT, DONE, or FAIL",
                    actual: String::from_utf8_lossy(&id).into_owned(),
                }
                .into());
            }
        }
    }
}

async fn read_directory_entry(
    session: &mut SyncSession,
    directory: &str,
) -> Result<RemoteFile, SyncError> {
    let mode = session.read_u32("DENT mode").await?;
    let size = u64::from(session.read_u32("DENT size").await?);
    let modified_seconds = u64::from(session.read_u32("DENT modification time").await?);
    let name_length = usize::try_from(session.read_u32("DENT name length").await?)
        .expect("u32 fits every supported platform");
    if name_length > SYNC_PATH_MAX {
        return Err(SyncProtocolError::FieldTooLarge {
            field: "directory entry name",
            limit: SYNC_PATH_MAX,
            actual: name_length,
        }
        .into());
    }
    let name = String::from_utf8(
        session
            .read_bytes(name_length, "directory entry name")
            .await?
            .to_vec(),
    )
    .map_err(SyncProtocolError::InvalidFileName)?;
    let path = join_remote_path(directory, &name);
    let modified_at = system_time(modified_seconds);

    Ok(RemoteFile {
        name,
        path,
        size,
        modified_at,
        mode,
        file_type: RemoteFileType::from_mode(mode),
        metadata_v2: None,
    })
}

async fn list_v2_inner(
    session: &mut SyncSession,
    directory: &str,
) -> Result<Vec<RemoteFile>, SyncError> {
    session
        .write_length_prefixed(b"LIS2", directory.as_bytes())
        .await?;
    let mut files = Vec::new();

    loop {
        match session.read_id().await? {
            id if id == *b"DNT2" => {
                if files.len() == MAX_DIRECTORY_ENTRIES {
                    return Err(SyncProtocolError::TooManyDirectoryEntries {
                        limit: MAX_DIRECTORY_ENTRIES,
                    }
                    .into());
                }
                let fields = read_v2_fields(session, "LIS2").await?;
                let name_length = usize::try_from(session.read_u32("DNT2 name length").await?)
                    .expect("u32 fits every supported platform");
                if name_length > SYNC_PATH_MAX {
                    return Err(SyncProtocolError::FieldTooLarge {
                        field: "directory entry name",
                        limit: SYNC_PATH_MAX,
                        actual: name_length,
                    }
                    .into());
                }
                let name =
                    String::from_utf8(session.read_bytes(name_length, "DNT2 name").await?.to_vec())
                        .map_err(SyncProtocolError::InvalidFileName)?;
                files.push(
                    fields.into_remote_file(name.clone(), join_remote_path(directory, &name)),
                );
            }
            id if id == *b"DONE" => {
                session
                    .skip(LIST_V2_DONE_TAIL, "LIS2 completion record")
                    .await?;
                return Ok(files);
            }
            id if id == *b"FAIL" => return Err(session.read_remote_error().await?),
            id => {
                return Err(SyncProtocolError::UnexpectedResponse {
                    expected: "DNT2, DONE, or FAIL",
                    actual: String::from_utf8_lossy(&id).into_owned(),
                }
                .into());
            }
        }
    }
}

struct V2Fields {
    device: u64,
    inode: u64,
    mode: u32,
    link_count: u32,
    uid: u32,
    gid: u32,
    size: u64,
    accessed_seconds: u64,
    modified_seconds: u64,
    changed_seconds: u64,
}

impl V2Fields {
    fn into_remote_file(self, name: String, path: String) -> RemoteFile {
        RemoteFile {
            name,
            path,
            size: self.size,
            modified_at: system_time(self.modified_seconds),
            mode: self.mode,
            file_type: RemoteFileType::from_mode(self.mode),
            metadata_v2: Some(RemoteFileMetadataV2 {
                device: self.device,
                inode: self.inode,
                link_count: self.link_count,
                uid: self.uid,
                gid: self.gid,
                accessed_at: system_time(self.accessed_seconds),
                changed_at: system_time(self.changed_seconds),
            }),
        }
    }
}

async fn read_v2_fields(
    session: &mut SyncSession,
    operation: &'static str,
) -> Result<V2Fields, SyncError> {
    let error_code = session.read_u32("Sync v2 error code").await?;
    let fields = V2Fields {
        device: session.read_u64("Sync v2 device id").await?,
        inode: session.read_u64("Sync v2 inode").await?,
        mode: session.read_u32("Sync v2 mode").await?,
        link_count: session.read_u32("Sync v2 link count").await?,
        uid: session.read_u32("Sync v2 uid").await?,
        gid: session.read_u32("Sync v2 gid").await?,
        size: session.read_u64("Sync v2 size").await?,
        accessed_seconds: session.read_u64("Sync v2 access time").await?,
        modified_seconds: session.read_u64("Sync v2 modification time").await?,
        changed_seconds: session.read_u64("Sync v2 change time").await?,
    };
    if error_code != 0 {
        return Err(SyncProtocolError::RemoteCode {
            operation,
            code: error_code,
        }
        .into());
    }
    Ok(fields)
}

fn system_time(seconds: u64) -> Option<std::time::SystemTime> {
    (seconds != 0)
        .then(|| UNIX_EPOCH.checked_add(Duration::from_secs(seconds)))
        .flatten()
}

fn remote_name(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_owned()
    } else {
        trimmed.rsplit('/').next().unwrap_or(trimmed).to_owned()
    }
}

fn join_remote_path(directory: &str, name: &str) -> String {
    if directory == "/" {
        format!("/{name}")
    } else {
        format!("{}/{name}", directory.trim_end_matches('/'))
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use super::{SYNC_DATA_MAX, read_chunk};
    use crate::TransferCancellation;

    #[tokio::test]
    async fn one_gibibyte_input_uses_a_fixed_sixty_four_kibibyte_buffer() {
        const ONE_GIBIBYTE: u64 = 1024 * 1024 * 1024;

        let mut source = tokio::io::repeat(0x5a).take(ONE_GIBIBYTE);
        let mut buffer = vec![0_u8; SYNC_DATA_MAX];
        let cancellation = TransferCancellation::new();
        let mut transferred = 0_u64;
        let mut largest_read = 0_usize;

        loop {
            let read = read_chunk(&mut source, &mut buffer, &cancellation)
                .await
                .expect("the generated input should be readable");
            if read == 0 {
                break;
            }
            transferred += read as u64;
            largest_read = largest_read.max(read);
        }

        assert_eq!(transferred, ONE_GIBIBYTE);
        assert_eq!(largest_read, SYNC_DATA_MAX);
        assert_eq!(buffer.len(), SYNC_DATA_MAX);
    }
}
