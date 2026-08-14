use adb_client::{AdbClientError, AdbStreamError};
use adb_shell::ShellError;
use adb_sync::SyncError;

/// Errors produced while starting, decoding, controlling, or stopping a mirror session.
#[derive(Debug, thiserror::Error)]
pub enum MirrorError {
    /// The embedded Android server could not be uploaded.
    #[error("screen server upload failed: {0}")]
    Sync(#[from] SyncError),
    /// The Android server process could not be started or stopped.
    #[error("screen server shell failed: {0}")]
    Shell(#[from] ShellError),
    /// Android's display manager could not enumerate logical displays.
    #[error("display discovery failed: {0}")]
    DisplayQuery(String),
    /// An ADB logical service could not be opened.
    #[error("screen service connection failed: {0}")]
    Client(#[from] AdbClientError),
    /// An open screen service failed while reading or writing.
    #[error("screen stream failed: {0}")]
    Stream(#[from] AdbStreamError),
    /// The server did not expose its local socket before the startup deadline.
    #[error("screen server did not become ready: {0}")]
    Startup(String),
    /// The server sent malformed or unsupported protocol data.
    #[error("invalid screen protocol data: {0}")]
    Protocol(String),
    /// The selected decoder backend could not initialize or decode a video packet.
    #[error("video decode failed: {0}")]
    Decode(String),
    /// A decoded frame could not be converted to a desktop preview.
    #[error("frame conversion failed: {0}")]
    Frame(String),
    /// Input coordinates or dimensions were outside the protocol range.
    #[error("invalid control input: {0}")]
    InvalidInput(String),
    /// The mirror session has already stopped.
    #[error("screen mirror session is stopped")]
    Stopped,
}
