//! Traditional ADB connection handshake and authenticated client session.

mod client;
mod error;
mod session;
mod stream;

pub use client::{
    AdbClient, AdbClientConfig, AdbReconnectConfig, AdbTransportFactory, ConnectionState,
    DEFAULT_DELAYED_ACK_RECEIVE_WINDOW,
};
pub use error::{AdbClientError, AdbStreamError};
pub use stream::AdbStream;
