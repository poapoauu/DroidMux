//! Native low-latency Android screen mirroring and input injection.
//!
//! `DroidMux` embeds the Apache-2.0 licensed scrcpy Android server, uploads it
//! through the native ADB Sync implementation, and speaks its video/control
//! protocol directly. It never launches an external `adb` or `scrcpy` process.

mod display;
mod error;
mod protocol;
mod session;

pub use display::{AndroidDisplay, list_displays};
pub use error::MirrorError;
pub use session::{AndroidKey, MirrorFrame, MirrorOptions, MirrorSession, MirrorTouchAction};
