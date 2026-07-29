//! ADB authentication credentials and Android RSA public-key encoding.
//!
//! This crate performs no file I/O. Callers decide how PKCS#8 key material is
//! stored using an appropriate platform credential store.

mod credential;
mod error;

pub use credential::{AdbAuthenticator, RsaAdbCredential};
pub use error::AdbAuthError;

/// Size of an ADB authentication token, which is a SHA-1 digest.
pub const ADB_TOKEN_LEN: usize = 20;

/// Size of an RSA-2048 signature.
pub const ADB_SIGNATURE_LEN: usize = 256;

/// Size of Android's custom encoded RSA-2048 public-key structure.
pub const ANDROID_PUBLIC_KEY_LEN: usize = 524;

/// `AUTH` message subtype containing a device challenge token.
pub const ADB_AUTH_TOKEN: u32 = 1;

/// `AUTH` message subtype containing a host RSA signature.
pub const ADB_AUTH_SIGNATURE: u32 = 2;

/// `AUTH` message subtype containing a host RSA public key.
pub const ADB_AUTH_RSAPUBLICKEY: u32 = 3;
