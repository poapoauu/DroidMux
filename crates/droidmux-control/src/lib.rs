//! Native low-latency Android screen mirroring and input injection.
//!
//! `DroidMux` embeds the Apache-2.0 licensed scrcpy Android server, uploads it
//! through the native ADB Sync implementation, and speaks its video/control
//! protocol directly. It never launches an external `adb` or `scrcpy` process.

mod decoder;
mod display;
mod error;
#[cfg(feature = "decoder-ffmpeg")]
mod ffmpeg_decoder;
mod pipeline;
mod protocol;
mod session;

#[cfg(feature = "decoder-openh264")]
pub use decoder::OpenH264Decoder;
pub use decoder::{DecodedPixelFormat, DecodedVideoFrame, MirrorDecoder, VideoFrameMetadata};
pub use display::{AndroidDisplay, list_displays};
pub use error::MirrorError;
#[cfg(feature = "decoder-ffmpeg")]
pub use ffmpeg_decoder::FfmpegDecoder;
pub use pipeline::MirrorPipelineStats;
pub use protocol::VideoCodec;
pub use session::{
    AndroidKey, MirrorFrame, MirrorOptions, MirrorPixelBuffer, MirrorPixelFrame, MirrorSession,
    MirrorTouchAction,
};
