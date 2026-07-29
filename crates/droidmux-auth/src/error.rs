use thiserror::Error;

/// Errors produced while managing or using ADB authentication credentials.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AdbAuthError {
    /// The authentication token is not a SHA-1-sized challenge.
    #[error("invalid ADB authentication token length: expected {expected}, got {actual}")]
    InvalidTokenLength {
        /// Required token size.
        expected: usize,
        /// Supplied token size.
        actual: usize,
    },

    /// The key comment is empty, too long, or contains a control character.
    #[error("invalid ADB public-key comment")]
    InvalidKeyComment,

    /// Android's public-key format only supports RSA-2048.
    #[error("unsupported RSA key size: expected {expected} bits, got {actual}")]
    UnsupportedKeySize {
        /// Required key size.
        expected: usize,
        /// Supplied key size.
        actual: usize,
    },

    /// The public exponent cannot be represented by Android's key format.
    #[error("unsupported RSA public exponent")]
    UnsupportedPublicExponent,

    /// An RSA key generation, validation, or signing operation failed.
    #[error("RSA authentication operation failed: {0}")]
    Rsa(#[from] rsa::Error),

    /// PKCS#8 private-key encoding or decoding failed.
    #[error("PKCS#8 private-key operation failed: {0}")]
    Pkcs8(#[from] rsa::pkcs8::Error),
}
