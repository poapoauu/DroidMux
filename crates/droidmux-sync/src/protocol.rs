use std::{
    ffi::OsString,
    io::{Read, Write},
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
    TransferCancellation, TransferCompression, TransferDirection, TransferOptions,
    TransferProgress,
    session::{SYNC_PATH_MAX, SyncSession, validate_remote_path},
};

const MAX_DIRECTORY_ENTRIES: usize = 100_000;
const SYNC_DATA_MAX: usize = 64 * 1024;
const LIST_V1_DONE_TAIL: usize = 16;
const LIST_V2_DONE_TAIL: usize = 72;
const STAT_V2_FEATURE: &str = "stat_v2";
const LIST_V2_FEATURE: &str = "ls_v2";
const SEND_RECV_V2_FEATURE: &str = "sendrecv_v2";
const SEND_RECV_V2_BROTLI_FEATURE: &str = "sendrecv_v2_brotli";
const SEND_RECV_V2_LZ4_FEATURE: &str = "sendrecv_v2_lz4";
const SEND_RECV_V2_ZSTD_FEATURE: &str = "sendrecv_v2_zstd";
static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn select_compression(
    client: &AdbClient,
    requested: TransferCompression,
    use_v2: bool,
) -> Result<TransferCompression, SyncError> {
    if !use_v2 {
        return match requested {
            TransferCompression::Auto | TransferCompression::None => Ok(TransferCompression::None),
            TransferCompression::Brotli => {
                Err(SyncProtocolError::CompressionUnavailable(SEND_RECV_V2_BROTLI_FEATURE).into())
            }
            TransferCompression::Lz4 => {
                Err(SyncProtocolError::CompressionUnavailable(SEND_RECV_V2_LZ4_FEATURE).into())
            }
            TransferCompression::Zstd => {
                Err(SyncProtocolError::CompressionUnavailable(SEND_RECV_V2_ZSTD_FEATURE).into())
            }
        };
    }

    let selected = match requested {
        TransferCompression::Auto => {
            if client.supports_feature(SEND_RECV_V2_ZSTD_FEATURE) {
                TransferCompression::Zstd
            } else if client.supports_feature(SEND_RECV_V2_LZ4_FEATURE) {
                TransferCompression::Lz4
            } else if client.supports_feature(SEND_RECV_V2_BROTLI_FEATURE) {
                TransferCompression::Brotli
            } else {
                TransferCompression::None
            }
        }
        explicit => explicit,
    };

    let supported = match selected {
        TransferCompression::Auto | TransferCompression::None => true,
        TransferCompression::Brotli => client.supports_feature(SEND_RECV_V2_BROTLI_FEATURE),
        TransferCompression::Lz4 => client.supports_feature(SEND_RECV_V2_LZ4_FEATURE),
        TransferCompression::Zstd => client.supports_feature(SEND_RECV_V2_ZSTD_FEATURE),
    };
    if supported {
        Ok(selected)
    } else {
        let feature = match selected {
            TransferCompression::Brotli => SEND_RECV_V2_BROTLI_FEATURE,
            TransferCompression::Lz4 => SEND_RECV_V2_LZ4_FEATURE,
            TransferCompression::Zstd => SEND_RECV_V2_ZSTD_FEATURE,
            TransferCompression::Auto | TransferCompression::None => "none",
        };
        Err(SyncProtocolError::CompressionUnavailable(feature).into())
    }
}

fn compression_flags(compression: TransferCompression) -> u32 {
    match compression {
        TransferCompression::None | TransferCompression::Auto => 0,
        TransferCompression::Brotli => 1,
        TransferCompression::Lz4 => 2,
        TransferCompression::Zstd => 4,
    }
}

enum StreamingEncoder {
    Brotli(Box<brotli::CompressorWriter<Vec<u8>>>),
    Lz4(Box<lz4_flex::frame::FrameEncoder<Vec<u8>>>),
    Zstd(Box<zstd::stream::write::Encoder<'static, Vec<u8>>>),
}

impl StreamingEncoder {
    fn new(compression: TransferCompression) -> Result<Self, SyncError> {
        let encoder =
            match compression {
                TransferCompression::Brotli => Self::Brotli(Box::new(
                    brotli::CompressorWriter::new(Vec::new(), 4096, 5, 22),
                )),
                TransferCompression::Lz4 => {
                    Self::Lz4(Box::new(lz4_flex::frame::FrameEncoder::new(Vec::new())))
                }
                TransferCompression::Zstd => Self::Zstd(Box::new(
                    zstd::stream::write::Encoder::new(Vec::new(), 3)
                        .map_err(|error| compression_error("encoder initialization", error))?,
                )),
                TransferCompression::Auto | TransferCompression::None => {
                    return Err(SyncProtocolError::CompressionFailed {
                        operation: "encoder initialization",
                        reason: "compression is disabled".to_owned(),
                    }
                    .into());
                }
            };
        Ok(encoder)
    }

    fn encode(&mut self, input: &[u8]) -> Result<Vec<u8>, SyncError> {
        match self {
            Self::Brotli(encoder) => {
                encoder
                    .write_all(input)
                    .and_then(|()| encoder.flush())
                    .map_err(|error| compression_error("upload", error))?;
                Ok(std::mem::take(encoder.get_mut()))
            }
            Self::Lz4(encoder) => {
                encoder
                    .write_all(input)
                    .and_then(|()| encoder.flush())
                    .map_err(|error| compression_error("upload", error))?;
                Ok(std::mem::take(encoder.get_mut()))
            }
            Self::Zstd(encoder) => {
                encoder
                    .write_all(input)
                    .and_then(|()| encoder.flush())
                    .map_err(|error| compression_error("upload", error))?;
                Ok(std::mem::take(encoder.get_mut()))
            }
        }
    }

    fn finish(self) -> Result<Vec<u8>, SyncError> {
        match self {
            Self::Brotli(encoder) => Ok(encoder.into_inner()),
            Self::Lz4(encoder) => encoder
                .finish()
                .map_err(|error| compression_error("upload finalization", error)),
            Self::Zstd(encoder) => encoder
                .finish()
                .map_err(|error| compression_error("upload finalization", error)),
        }
    }
}

fn compression_error(operation: &'static str, error: impl std::fmt::Display) -> SyncError {
    SyncProtocolError::CompressionFailed {
        operation,
        reason: error.to_string(),
    }
    .into()
}

struct CompressionInput {
    receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
    current: Vec<u8>,
    offset: usize,
}

impl CompressionInput {
    fn new(receiver: tokio::sync::mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            receiver,
            current: Vec::new(),
            offset: 0,
        }
    }
}

impl Read for CompressionInput {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            if self.offset < self.current.len() {
                let available = &self.current[self.offset..];
                let count = available.len().min(output.len());
                output[..count].copy_from_slice(&available[..count]);
                self.offset += count;
                return Ok(count);
            }
            match self.receiver.blocking_recv() {
                Some(chunk) => {
                    self.current = chunk;
                    self.offset = 0;
                }
                None => return Ok(0),
            }
        }
    }
}

fn decode_stream_worker(
    compression: TransferCompression,
    receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
    output: &tokio::sync::mpsc::Sender<Vec<u8>>,
) -> Result<(), String> {
    let input = CompressionInput::new(receiver);
    match compression {
        TransferCompression::Brotli => {
            let mut decoder = brotli::Decompressor::new(input, 4096);
            pump_decoder(&mut decoder, output)
        }
        TransferCompression::Lz4 => {
            let mut decoder = lz4_flex::frame::FrameDecoder::new(input);
            pump_decoder(&mut decoder, output)
        }
        TransferCompression::Zstd => {
            let mut decoder =
                zstd::stream::read::Decoder::new(input).map_err(|error| error.to_string())?;
            pump_decoder(&mut decoder, output)
        }
        TransferCompression::Auto | TransferCompression::None => {
            Err("compression decoder was started while compression is disabled".to_owned())
        }
    }
}

type DecoderTask = tokio::task::JoinHandle<Result<(), String>>;
type DecoderSetup = (
    Option<tokio::sync::mpsc::Sender<Vec<u8>>>,
    Option<tokio::sync::mpsc::Receiver<Vec<u8>>>,
    Option<DecoderTask>,
);

struct DownloadConfig<'a> {
    expected_size: u64,
    compression: TransferCompression,
    cancellation: &'a TransferCancellation,
    max_download_bytes: Option<u64>,
}

fn start_decoder(compression: TransferCompression) -> DecoderSetup {
    if compression == TransferCompression::None {
        return (None, None, None);
    }

    let (input_sender, input_receiver) = tokio::sync::mpsc::channel(2);
    let (output_sender, output_receiver) = tokio::sync::mpsc::channel(8);
    let handle = tokio::task::spawn_blocking(move || {
        decode_stream_worker(compression, input_receiver, &output_sender)
    });
    (Some(input_sender), Some(output_receiver), Some(handle))
}

fn pump_decoder<R: Read>(
    decoder: &mut R,
    output: &tokio::sync::mpsc::Sender<Vec<u8>>,
) -> Result<(), String> {
    let mut buffer = vec![0_u8; SYNC_DATA_MAX];
    loop {
        let read = decoder
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if read == 0 {
            return Ok(());
        }
        output
            .blocking_send(buffer[..read].to_vec())
            .map_err(|_| "download writer was closed".to_owned())?;
    }
}

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
    let use_v2 = client.supports_feature(SEND_RECV_V2_FEATURE);
    let compression = select_compression(client, options.compression, use_v2)?;
    let result = upload_inner(
        &mut session,
        &mut file,
        UploadRequest {
            remote_path,
            mode: options.file_mode,
            use_v2,
            compression,
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
    let use_v2 = client.supports_feature(SEND_RECV_V2_FEATURE);
    let compression = select_compression(client, options.compression, use_v2)?;
    let result = upload_inner(
        &mut session,
        &mut reader,
        UploadRequest {
            remote_path,
            mode: options.file_mode,
            use_v2,
            compression,
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
    enforce_download_limit(0, expected_size, options.max_download_bytes)?;
    if options.cancellation.is_canceled() {
        return Err(SyncError::Canceled);
    }
    let (temporary_path, mut output) = create_temporary_file(local_path).await?;
    let download = DownloadConfig {
        expected_size,
        compression: select_compression(
            client,
            options.compression,
            client.supports_feature(SEND_RECV_V2_FEATURE),
        )?,
        cancellation: &options.cancellation,
        max_download_bytes: options.max_download_bytes,
    };
    let transfer =
        download_to_writer(client, remote_path, &mut output, download, &mut progress).await;
    drop(output);

    if let Err(error) = transfer {
        let _ = fs::remove_file(&temporary_path).await;
        return Err(error);
    }
    if options.cancellation.is_canceled() {
        let _ = fs::remove_file(&temporary_path).await;
        return Err(SyncError::Canceled);
    }
    persist_temporary_file(&temporary_path, local_path).await
}

async fn download_to_writer<W, F>(
    client: &AdbClient,
    remote_path: &str,
    writer: &mut W,
    config: DownloadConfig<'_>,
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
        &config,
        progress,
    )
    .await;
    session.finish(result).await
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn download_inner<W, F>(
    session: &mut SyncSession,
    writer: &mut W,
    remote_path: &str,
    use_v2: bool,
    config: &DownloadConfig<'_>,
    progress: &mut F,
) -> Result<(), SyncError>
where
    W: AsyncWrite + Unpin,
    F: FnMut(TransferProgress),
{
    if use_v2 {
        session
            .write_recv_v2(
                remote_path.as_bytes(),
                compression_flags(config.compression),
                config.cancellation,
            )
            .await?;
    } else {
        session
            .write_length_prefixed_cancellable(b"RECV", remote_path.as_bytes(), config.cancellation)
            .await?;
    }
    report_progress(
        progress,
        TransferDirection::Download,
        0,
        Some(config.expected_size),
    );
    let mut transferred = 0_u64;
    let (decoder_input, mut decoder_output, decoder_handle) = start_decoder(config.compression);
    let mut decoder_input = decoder_input;

    loop {
        match session.read_id_cancellable(config.cancellation).await? {
            id if id == *b"DATA" => {
                let length = usize::try_from(
                    session
                        .read_u32_cancellable("DATA length", config.cancellation)
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
                    .read_bytes_cancellable(length, "DATA payload", config.cancellation)
                    .await?;
                if config.compression == TransferCompression::None {
                    let next = enforce_download_limit(
                        transferred,
                        length as u64,
                        config.max_download_bytes,
                    )?;
                    write_download_data(writer, &data, config.cancellation).await?;
                    transferred = next;
                } else {
                    send_decoder_input(
                        decoder_input
                            .as_ref()
                            .expect("a decoder is created for compressed transfers"),
                        data.to_vec(),
                        writer,
                        decoder_output
                            .as_mut()
                            .expect("a decoder output is created for compressed transfers"),
                        config,
                        &mut transferred,
                        progress,
                    )
                    .await?;
                    drain_decoded_output(
                        writer,
                        decoder_output
                            .as_mut()
                            .expect("a decoder output is created for compressed transfers"),
                        config,
                        &mut transferred,
                        progress,
                    )
                    .await?;
                }
                report_progress(
                    progress,
                    TransferDirection::Download,
                    transferred,
                    Some(config.expected_size),
                );
            }
            id if id == *b"DONE" => {
                if config.compression != TransferCompression::None {
                    drop(decoder_input.take());
                    finish_decoder(
                        decoder_handle
                            .expect("a decoder handle is created for compressed transfers"),
                        writer,
                        decoder_output
                            .as_mut()
                            .expect("a decoder output is created for compressed transfers"),
                        config,
                        &mut transferred,
                        progress,
                    )
                    .await?;
                }
                flush_download(writer, config.cancellation).await?;
                return Ok(());
            }
            id if id == *b"FAIL" => {
                return Err(session
                    .read_remote_error_cancellable(config.cancellation)
                    .await?);
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

async fn send_decoder_input<W, F>(
    sender: &tokio::sync::mpsc::Sender<Vec<u8>>,
    data: Vec<u8>,
    writer: &mut W,
    output: &mut tokio::sync::mpsc::Receiver<Vec<u8>>,
    config: &DownloadConfig<'_>,
    transferred: &mut u64,
    progress: &mut F,
) -> Result<(), SyncError>
where
    W: AsyncWrite + Unpin,
    F: FnMut(TransferProgress),
{
    loop {
        let permit = tokio::select! {
            biased;
            () = config.cancellation.cancelled() => return Err(SyncError::Canceled),
            result = sender.reserve() => {
                result.map_err(|_| compression_error("download", "decoder stopped"))?
            }
            decoded = output.recv() => {
                let decoded = decoded
                    .ok_or_else(|| compression_error("download", "decoder stopped"))?;
                write_decoded_chunk(
                    writer,
                    &decoded,
                    config,
                    transferred,
                    progress,
                )
                .await?;
                continue;
            }
        };
        permit.send(data);
        return Ok(());
    }
}

async fn finish_decoder<W, F>(
    handle: DecoderTask,
    writer: &mut W,
    output: &mut tokio::sync::mpsc::Receiver<Vec<u8>>,
    config: &DownloadConfig<'_>,
    transferred: &mut u64,
    progress: &mut F,
) -> Result<(), SyncError>
where
    W: AsyncWrite + Unpin,
    F: FnMut(TransferProgress),
{
    let mut handle = Box::pin(handle);
    loop {
        tokio::select! {
            biased;
            () = config.cancellation.cancelled() => return Err(SyncError::Canceled),
            result = &mut handle => {
                result
                    .map_err(|error| compression_error("download", error))?
                    .map_err(|error| compression_error("download", error))?;
                break;
            }
            decoded = output.recv() => {
                let Some(decoded) = decoded else {
                    handle
                        .await
                        .map_err(|error| compression_error("download", error))?
                        .map_err(|error| compression_error("download", error))?;
                    break;
                };
                write_decoded_chunk(
                    writer,
                    &decoded,
                    config,
                    transferred,
                    progress,
                )
                .await?;
            }
        }
    }
    drain_decoded_output(writer, output, config, transferred, progress).await
}

async fn drain_decoded_output<W, F>(
    writer: &mut W,
    output: &mut tokio::sync::mpsc::Receiver<Vec<u8>>,
    config: &DownloadConfig<'_>,
    transferred: &mut u64,
    progress: &mut F,
) -> Result<(), SyncError>
where
    W: AsyncWrite + Unpin,
    F: FnMut(TransferProgress),
{
    while let Ok(data) = output.try_recv() {
        write_decoded_chunk(writer, &data, config, transferred, progress).await?;
    }
    Ok(())
}

async fn write_decoded_chunk<W, F>(
    writer: &mut W,
    data: &[u8],
    config: &DownloadConfig<'_>,
    transferred: &mut u64,
    progress: &mut F,
) -> Result<(), SyncError>
where
    W: AsyncWrite + Unpin,
    F: FnMut(TransferProgress),
{
    let next = enforce_download_limit(*transferred, data.len() as u64, config.max_download_bytes)?;
    write_download_data(writer, data, config.cancellation).await?;
    *transferred = next;
    report_progress(
        progress,
        TransferDirection::Download,
        *transferred,
        Some(config.expected_size),
    );
    Ok(())
}

fn enforce_download_limit(
    transferred: u64,
    additional: u64,
    max_download_bytes: Option<u64>,
) -> Result<u64, SyncError> {
    let actual = transferred.saturating_add(additional);
    if let Some(limit) = max_download_bytes
        && actual > limit
    {
        return Err(SyncError::DownloadTooLarge { limit, actual });
    }
    Ok(actual)
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

async fn persist_temporary_file(temporary: &Path, destination: &Path) -> Result<(), SyncError> {
    match fs::hard_link(temporary, destination).await {
        Ok(()) => {
            fs::remove_file(temporary).await?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(temporary).await;
            Err(SyncError::DestinationExists(destination.to_owned()))
        }
        Err(error) => {
            let _ = fs::remove_file(temporary).await;
            Err(error.into())
        }
    }
}

struct UploadRequest<'a> {
    remote_path: &'a str,
    mode: u32,
    use_v2: bool,
    compression: TransferCompression,
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
            .write_send_v2(
                request.remote_path.as_bytes(),
                request.mode,
                compression_flags(request.compression),
                cancellation,
            )
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
    let mut encoder = (request.compression != TransferCompression::None)
        .then(|| StreamingEncoder::new(request.compression))
        .transpose()?;
    loop {
        let read = read_chunk(reader, &mut buffer, cancellation).await?;
        if read == 0 {
            break;
        }
        if request.compression == TransferCompression::None {
            session.write_data(&buffer[..read], cancellation).await?;
        } else {
            let compressed = encoder
                .as_mut()
                .expect("an encoder is created for compressed transfers")
                .encode(&buffer[..read])?;
            for chunk in compressed.chunks(SYNC_DATA_MAX) {
                session.write_data(chunk, cancellation).await?;
            }
        }
        transferred = transferred.saturating_add(read as u64);
        report_progress(
            progress,
            TransferDirection::Upload,
            transferred,
            Some(request.total),
        );
    }

    if let Some(encoder) = encoder {
        let compressed = encoder.finish()?;
        for chunk in compressed.chunks(SYNC_DATA_MAX) {
            session.write_data(chunk, cancellation).await?;
        }
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
    use std::time::{SystemTime, UNIX_EPOCH};

    use tokio::io::AsyncReadExt;

    use super::{
        SYNC_DATA_MAX, StreamingEncoder, decode_stream_worker, enforce_download_limit,
        persist_temporary_file, read_chunk,
    };
    use crate::{SyncError, TransferCancellation, TransferCompression};

    #[test]
    fn download_limit_counts_decompressed_bytes() {
        assert_eq!(
            enforce_download_limit(4, 4, Some(8)).expect("exact limit should be accepted"),
            8
        );
        assert!(matches!(
            enforce_download_limit(4, 5, Some(8)),
            Err(SyncError::DownloadTooLarge {
                limit: 8,
                actual: 9
            })
        ));
        assert_eq!(
            enforce_download_limit(u64::MAX, 1, None)
                .expect("an explicitly unbounded transfer should saturate safely"),
            u64::MAX
        );
    }

    #[tokio::test]
    async fn temporary_download_commit_never_replaces_a_racing_destination() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock should follow the Unix epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "droidmux-sync-noclobber-{}-{unique}",
            std::process::id()
        ));
        tokio::fs::create_dir(&directory)
            .await
            .expect("test directory should be created");
        let destination = directory.join("download.bin");
        let temporary = directory.join(".download.bin.part");
        tokio::fs::write(&temporary, b"downloaded")
            .await
            .expect("temporary data should be written");
        tokio::fs::write(&destination, b"racing writer")
            .await
            .expect("racing destination should be written");

        let result = persist_temporary_file(&temporary, &destination).await;
        assert!(matches!(result, Err(SyncError::DestinationExists(path)) if path == destination));
        assert_eq!(
            tokio::fs::read(&destination)
                .await
                .expect("racing destination should remain readable"),
            b"racing writer"
        );
        assert!(
            !tokio::fs::try_exists(&temporary)
                .await
                .expect("temporary path should be inspectable")
        );
        tokio::fs::remove_dir_all(directory)
            .await
            .expect("test directory should be removed");
    }

    #[tokio::test]
    async fn streaming_compression_round_trips_across_arbitrary_data_boundaries() {
        let input: Vec<u8> = (0..512 * 1024)
            .map(|index| {
                u8::try_from(index % 256)
                    .expect("the modulo operation keeps the test byte in range")
                    .wrapping_mul(31)
            })
            .collect();

        for compression in [
            TransferCompression::Brotli,
            TransferCompression::Lz4,
            TransferCompression::Zstd,
        ] {
            let mut encoder = StreamingEncoder::new(compression).expect("encoder should start");
            let mut wire = Vec::new();
            for chunk in input.chunks(137) {
                wire.extend(
                    encoder
                        .encode(chunk)
                        .expect("streaming encoding should succeed"),
                );
            }
            wire.extend(encoder.finish().expect("streaming encoding should finish"));

            let (input_sender, input_receiver) = tokio::sync::mpsc::channel(2);
            let (output_sender, mut output_receiver) = tokio::sync::mpsc::channel(8);
            let worker = std::thread::spawn(move || {
                decode_stream_worker(compression, input_receiver, &output_sender)
            });

            let feeder = tokio::spawn(async move {
                for chunk in wire.chunks(11) {
                    input_sender
                        .send(chunk.to_vec())
                        .await
                        .expect("decoder should accept input");
                }
            });
            let mut decoded = Vec::new();
            while let Some(chunk) = output_receiver.recv().await {
                decoded.extend(chunk);
            }
            feeder
                .await
                .expect("input feeder should finish without a panic");
            assert_eq!(
                worker.join().expect("decoder thread should finish"),
                Ok(()),
                "decoder should accept {compression:?}"
            );
            assert_eq!(decoded, input, "round trip failed for {compression:?}");
        }
    }

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
