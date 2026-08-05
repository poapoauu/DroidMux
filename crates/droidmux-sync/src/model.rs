use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::SystemTime,
};

use tokio::sync::Notify;

/// Default per-transfer download limit used by [`TransferOptions`].
pub const DEFAULT_MAX_DOWNLOAD_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// File kind derived from Android's POSIX mode bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RemoteFileType {
    /// Regular file.
    RegularFile,
    /// Directory.
    Directory,
    /// Symbolic link.
    SymbolicLink,
    /// Socket, device, FIFO, or an unknown mode.
    Other,
}

impl RemoteFileType {
    pub(crate) const fn from_mode(mode: u32) -> Self {
        match mode & 0o170_000 {
            0o100_000 => Self::RegularFile,
            0o040_000 => Self::Directory,
            0o120_000 => Self::SymbolicLink,
            _ => Self::Other,
        }
    }
}

/// Metadata returned for a remote Android filesystem entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteFile {
    /// Base name reported by the Sync service.
    pub name: String,
    /// Full remote path assembled by the client.
    pub path: String,
    /// File size in bytes.
    pub size: u64,
    /// Modification time, when the device reports a non-zero Unix timestamp.
    pub modified_at: Option<SystemTime>,
    /// Raw Android POSIX mode bits.
    pub mode: u32,
    /// File kind derived from [`Self::mode`].
    pub file_type: RemoteFileType,
    /// Additional 64-bit POSIX metadata returned by Sync v2.
    pub metadata_v2: Option<RemoteFileMetadataV2>,
}

/// Extended POSIX metadata available from Android's Sync v2 protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteFileMetadataV2 {
    /// Device identifier containing the inode.
    pub device: u64,
    /// Inode number.
    pub inode: u64,
    /// Number of hard links.
    pub link_count: u32,
    /// Android user identifier owning the entry.
    pub uid: u32,
    /// Android group identifier owning the entry.
    pub gid: u32,
    /// Last access time, when reported.
    pub accessed_at: Option<SystemTime>,
    /// Last status-change time, when reported.
    pub changed_at: Option<SystemTime>,
}

/// Direction of a file transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferDirection {
    /// Data is moving from the desktop to Android.
    Upload,
    /// Data is moving from Android to the desktop.
    Download,
}

/// Point-in-time progress for a streaming file transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferProgress {
    /// Transfer direction.
    pub direction: TransferDirection,
    /// Bytes successfully read from or written to the transfer stream.
    pub transferred_bytes: u64,
    /// Expected total size when it is known.
    pub total_bytes: Option<u64>,
}

#[derive(Debug)]
struct CancellationState {
    canceled: AtomicBool,
    notification: Notify,
}

/// Cloneable signal used to cancel an in-flight file transfer.
#[derive(Debug, Clone)]
pub struct TransferCancellation {
    state: Arc<CancellationState>,
}

impl TransferCancellation {
    /// Creates a signal in the active state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(CancellationState {
                canceled: AtomicBool::new(false),
                notification: Notify::new(),
            }),
        }
    }

    /// Requests cancellation and wakes blocked network or filesystem work.
    pub fn cancel(&self) {
        self.state.canceled.store(true, Ordering::Release);
        self.state.notification.notify_waiters();
    }

    /// Returns whether cancellation has been requested.
    #[must_use]
    pub fn is_canceled(&self) -> bool {
        self.state.canceled.load(Ordering::Acquire)
    }

    /// Waits until cancellation is requested.
    pub async fn cancelled(&self) {
        loop {
            let notified = self.state.notification.notified();
            if self.is_canceled() {
                return;
            }
            notified.await;
        }
    }
}

impl Default for TransferCancellation {
    fn default() -> Self {
        Self::new()
    }
}

/// Options shared by streaming upload and download operations.
#[derive(Debug, Clone)]
pub struct TransferOptions {
    /// POSIX permission bits used for uploaded files.
    pub file_mode: u32,
    /// Compression policy for Sync v2 transfers.
    pub compression: TransferCompression,
    /// Signal that can interrupt blocked local or network I/O.
    pub cancellation: TransferCancellation,
    /// Maximum decompressed bytes accepted from the device for one download.
    ///
    /// The limit is per transfer, so concurrent devices do not share a global
    /// counter. Set this to `None` only for explicitly trusted, unbounded pulls.
    pub max_download_bytes: Option<u64>,
}

impl Default for TransferOptions {
    fn default() -> Self {
        Self {
            file_mode: 0o644,
            compression: TransferCompression::Auto,
            cancellation: TransferCancellation::new(),
            max_download_bytes: Some(DEFAULT_MAX_DOWNLOAD_BYTES),
        }
    }
}

/// Compression policy for Sync v2 file transfers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferCompression {
    /// Prefer the best algorithm negotiated with the device, otherwise none.
    Auto,
    /// Disable compression even when the device supports it.
    None,
    /// Use Brotli compression.
    Brotli,
    /// Use LZ4 frame compression.
    Lz4,
    /// Use Zstandard compression.
    Zstd,
}
