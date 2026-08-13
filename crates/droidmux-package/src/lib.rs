//! Native Android package management for `DroidMux`.
//!
//! Operations use logical ADB Shell and Sync streams. No external Android
//! command-line program or ADB server is invoked.

mod error;
mod jdwp;
mod manager;
mod model;
mod parse;

pub use adb_sync::TransferCancellation as PackageTransferCancellation;
pub use error::PackageError;
pub use manager::PackageManager;
pub use model::{
    AndroidPackage, InstallOptions, PackageDetails, PackageKind, PackageListScope,
    PackageTransferProgress,
};

/// The stable Cargo package name for this crate.
pub const CRATE_NAME: &str = env!("CARGO_PKG_NAME");

#[cfg(test)]
mod tests {
    use super::CRATE_NAME;

    #[test]
    fn reports_own_crate_name() {
        assert_eq!(CRATE_NAME, "droidmux-package");
    }
}
