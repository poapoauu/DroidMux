//! Native ADB Sync protocol support for `DroidMux`.
//!
//! The crate communicates through [`adb_client::AdbClient`] and never invokes
//! an external `adb` process. Sync v1 operations are streamed over a dedicated
//! `sync:` logical ADB service.

mod error;
mod model;
mod protocol;
mod session;

pub use error::{SyncError, SyncProtocolError};
pub use model::{
    DEFAULT_MAX_DOWNLOAD_BYTES, RemoteFile, RemoteFileMetadataV2, RemoteFileType,
    TransferCancellation, TransferCompression, TransferDirection, TransferOptions,
    TransferProgress,
};
pub use protocol::{
    list_directory, lstat, pull_file, pull_file_with_options, push_bytes, push_bytes_with_options,
    push_file, push_file_with_options, stat,
};
