use std::{io, path::PathBuf};

use adb_shell::ShellError;
use adb_sync::SyncError;
use thiserror::Error;

/// Failure returned by native Android package management.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PackageError {
    /// A Shell service could not start or finish.
    #[error(transparent)]
    Shell(#[from] ShellError),
    /// A Sync upload or download failed.
    #[error(transparent)]
    Sync(#[from] SyncError),
    /// A package name was empty, too long, or contained unsafe characters.
    #[error("invalid Android package name: {0}")]
    InvalidPackageName(String),
    /// The selected APK path was not a readable regular file.
    #[error("APK path is not a readable file: {}", .0.display())]
    InvalidApkPath(PathBuf),
    /// Reading a previously validated local APK failed.
    #[error("could not read local APK {}: {source}", path.display())]
    LocalIo {
        /// APK path being read.
        path: PathBuf,
        /// Underlying filesystem failure.
        #[source]
        source: io::Error,
    },
    /// A local APK changed after the install session was sized.
    #[error(
        "local APK size changed while streaming {}: expected {expected} bytes, got {actual}",
        path.display()
    )]
    ApkSizeChanged {
        /// APK path that changed.
        path: PathBuf,
        /// Size included in the Android install session.
        expected: u64,
        /// Bytes observed while streaming.
        actual: u64,
    },
    /// A session install requires at least one APK.
    #[error("split APK installation requires at least one APK")]
    NoApks,
    /// The number of selected APKs exceeded the defensive limit.
    #[error("too many APKs for one install session: limit {limit}, got {actual}")]
    TooManyApks {
        /// Maximum accepted APK count.
        limit: usize,
        /// Supplied APK count.
        actual: usize,
    },
    /// The aggregate APK size cannot be represented by Android's installer.
    #[error("aggregate APK size exceeds the Android package installer limit")]
    InstallSizeOverflow,
    /// Streaming session installation requires Shell v2 stdin framing.
    #[error("split APK installation requires the device shell_v2 feature")]
    SplitInstallUnsupported,
    /// Android returned more output than the defensive bound.
    #[error("package command output exceeded {limit} bytes")]
    OutputTooLarge {
        /// Maximum retained output size.
        limit: usize,
    },
    /// Android returned malformed package-manager output.
    #[error("invalid package-manager response: {0}")]
    InvalidResponse(String),
    /// A package-manager command failed.
    #[error("Android package operation failed: {0}")]
    CommandFailed(String),
    /// Android did not report an APK path for the package.
    #[error("Android did not report an APK path for {0}")]
    ApkPathUnavailable(String),
    /// No running process matched the selected package name.
    #[error("未找到 {0} 正在运行的进程")]
    ProcessNotFound(String),
    /// Android protected a process from all non-force-stop kill strategies.
    #[error(
        "Android 拒绝终止 {package_name} 的进程；该进程可能不可调试或由系统保护，请使用“停止应用”"
    )]
    ProcessKillDenied {
        /// Package whose original processes remain alive.
        package_name: String,
    },
    /// Installation was canceled while uploading the APK.
    #[error("APK installation was canceled")]
    Canceled,
    /// Installation failed and Android also rejected session cleanup.
    #[error(
        "split APK install failed and session {session_id} could not be abandoned: install={install}; abandon={abandon}"
    )]
    SessionAbandonFailed {
        /// Android package installer session identifier.
        session_id: u32,
        /// Original write, commit, or cancellation failure.
        install: Box<PackageError>,
        /// Failure returned by `install-abandon`.
        abandon: Box<PackageError>,
    },
}
