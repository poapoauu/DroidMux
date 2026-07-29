use adb_sync::TransferProgress;

/// Ownership class reported by Android's package manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageKind {
    /// Package installed by the user or device owner.
    User,
    /// Package shipped as part of the Android system image.
    System,
}

/// Filter used when querying installed packages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageListScope {
    /// User-installed packages only.
    User,
    /// System-image packages only.
    System,
}

/// One installed Android package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AndroidPackage {
    /// Java-style package identifier.
    pub package_name: String,
    /// Base APK path reported by `pm list packages -f`.
    pub apk_path: String,
    /// User or system ownership class.
    pub kind: PackageKind,
}

/// Details parsed from `dumpsys package` for one package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageDetails {
    /// Java-style package identifier.
    pub package_name: String,
    /// Human-readable version, when present.
    pub version_name: Option<String>,
    /// Numeric version code, when present.
    pub version_code: Option<u64>,
    /// Android application user ID, when present.
    pub user_id: Option<u32>,
    /// First install time reported by Android.
    pub first_install_time: Option<String>,
    /// Last update time reported by Android.
    pub last_update_time: Option<String>,
    /// Every base or split APK path reported by Android.
    pub apk_paths: Vec<String>,
}

/// APK installation behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstallOptions {
    /// Replace an already-installed package while retaining its data.
    pub replace_existing: bool,
    /// Permit a lower version code than the installed package.
    pub allow_downgrade: bool,
}

impl Default for InstallOptions {
    fn default() -> Self {
        Self {
            replace_existing: true,
            allow_downgrade: false,
        }
    }
}

/// Aggregate APK byte progress while staging or streaming an installation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackageTransferProgress {
    /// Bytes uploaded to Android.
    pub transferred_bytes: u64,
    /// Expected APK byte count.
    pub total_bytes: Option<u64>,
}

impl From<TransferProgress> for PackageTransferProgress {
    fn from(progress: TransferProgress) -> Self {
        Self {
            transferred_bytes: progress.transferred_bytes,
            total_bytes: progress.total_bytes,
        }
    }
}
