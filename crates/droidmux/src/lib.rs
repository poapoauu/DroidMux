//! `DroidMux`: native Rust building blocks for talking directly to Android's `adbd`.
//!
//! The SDK does not spawn `adb.exe`, `fastboot.exe`, or the scrcpy desktop
//! executable. Optional features expose higher-level services while keeping
//! protocol, transport, authentication, and stream multiplexing reusable.

/// RSA host authentication and Android public-key encoding.
pub use adb_auth as auth;
/// Authenticated ADB connections and multiplexed logical streams.
pub use adb_client as client;
/// Bounded ADB packet framing and validation.
pub use adb_protocol as protocol;
/// Transport abstraction implemented by TCP and wireless TLS backends.
pub use adb_transport as transport;

/// mDNS discovery for Android wireless-debugging endpoints.
#[cfg(feature = "mdns")]
pub use adb_discovery as discovery;

/// scrcpy-server session protocol and screen-control primitives.
#[cfg(feature = "control")]
pub use adb_control as control;
/// Bounded local TCP forwarding over native ADB logical streams.
#[cfg(feature = "forward")]
pub use adb_forward as forward;
/// Streaming logcat support.
#[cfg(feature = "logcat")]
pub use adb_logcat as logcat;
/// Package inspection, install, export, launch, stop, and uninstall operations.
#[cfg(feature = "package")]
pub use adb_package as package;
/// Android 11+ wireless pairing, STLS, and reusable credentials.
#[cfg(feature = "pairing")]
pub use adb_pairing as pairing;
/// PNG screenshot capture.
#[cfg(feature = "screenshot")]
pub use adb_screenshot as screenshot;
/// Shell v2 and legacy shell sessions.
#[cfg(feature = "shell")]
pub use adb_shell as shell;
/// Sync v1/v2 file metadata and transfer operations.
#[cfg(feature = "sync")]
pub use adb_sync as sync;
/// Native Tokio TCP transport.
#[cfg(feature = "tcp")]
pub use adb_transport_tcp as tcp;

/// Native USB Host transport backed by libusb.
#[cfg(feature = "usb")]
pub use adb_transport_usb as usb;
