//! Traditional ADB connection handshake and authenticated client session.

mod client;
mod error;
mod session;
mod stream;

pub use client::{AdbClient, ConnectionState};
pub use error::{AdbClientError, AdbStreamError};
pub use stream::AdbStream;
