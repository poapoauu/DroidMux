use std::fmt;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use rand::rngs::OsRng;
use rsa::{
    BigUint, Pkcs1v15Sign, RsaPrivateKey, RsaPublicKey,
    pkcs8::{DecodePrivateKey, EncodePrivateKey, LineEnding},
    traits::PublicKeyParts,
};
use sha1::Sha1;
use zeroize::Zeroizing;

use crate::{ADB_SIGNATURE_LEN, ADB_TOKEN_LEN, ANDROID_PUBLIC_KEY_LEN, AdbAuthError};

const RSA_MODULUS_BITS: usize = 2048;
const RSA_MODULUS_WORDS: u32 = 64;
const MAX_KEY_COMMENT_LEN: usize = 128;

/// Signing operations required by the traditional ADB authentication handshake.
pub trait AdbAuthenticator: Send + Sync {
    /// Signs one 20-byte ADB challenge token.
    ///
    /// # Errors
    ///
    /// Returns an error when the token length is invalid or RSA signing fails.
    fn sign_token(&self, token: &[u8]) -> Result<Bytes, AdbAuthError>;

    /// Encodes the Android public key, comment, and terminating NUL byte.
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be represented by Android's custom
    /// RSA-2048 format.
    fn public_key_payload(&self) -> Result<Bytes, AdbAuthError>;
}

/// A reusable RSA-2048 identity for traditional ADB authentication.
#[derive(Clone)]
pub struct RsaAdbCredential {
    private_key: RsaPrivateKey,
    comment: String,
}

impl RsaAdbCredential {
    /// Generates a new RSA-2048 identity using the operating-system RNG.
    ///
    /// # Errors
    ///
    /// Returns an error if the comment is invalid or secure key generation
    /// fails.
    pub fn generate(comment: impl Into<String>) -> Result<Self, AdbAuthError> {
        let comment = comment.into();
        validate_comment(&comment)?;
        let private_key = RsaPrivateKey::new(&mut OsRng, RSA_MODULUS_BITS)?;
        Self::from_private_key(private_key, comment)
    }

    /// Creates an identity from an existing RSA private key.
    ///
    /// # Errors
    ///
    /// Returns an error if the key is invalid, is not RSA-2048, or the comment
    /// is invalid.
    pub fn from_private_key(
        private_key: RsaPrivateKey,
        comment: impl Into<String>,
    ) -> Result<Self, AdbAuthError> {
        let comment = comment.into();
        validate_comment(&comment)?;
        private_key.validate()?;
        validate_key_size(&private_key.to_public_key())?;
        Ok(Self {
            private_key,
            comment,
        })
    }

    /// Restores an identity from an unencrypted PKCS#8 PEM document.
    ///
    /// The caller is responsible for sourcing the PEM from protected storage.
    ///
    /// # Errors
    ///
    /// Returns an error if PEM decoding, key validation, or comment validation
    /// fails.
    pub fn from_pkcs8_pem(pem: &str, comment: impl Into<String>) -> Result<Self, AdbAuthError> {
        let private_key = RsaPrivateKey::from_pkcs8_pem(pem)?;
        Self::from_private_key(private_key, comment)
    }

    /// Encodes the private key as an unencrypted, zeroizing PKCS#8 PEM string.
    ///
    /// The returned secret must be handed directly to protected storage and
    /// must never be logged.
    ///
    /// # Errors
    ///
    /// Returns an error if PKCS#8 encoding fails.
    pub fn to_pkcs8_pem(&self) -> Result<Zeroizing<String>, AdbAuthError> {
        Ok(self.private_key.to_pkcs8_pem(LineEnding::LF)?)
    }

    /// Returns the non-secret public key for verification and fingerprinting.
    #[must_use]
    pub fn public_key(&self) -> RsaPublicKey {
        self.private_key.to_public_key()
    }

    fn android_public_key(&self) -> Result<[u8; ANDROID_PUBLIC_KEY_LEN], AdbAuthError> {
        let public_key = self.public_key();
        validate_key_size(&public_key)?;

        let modulus = public_key.n();
        let modulus_bytes = modulus.to_bytes_le();
        let modulus_low = read_low_word(&modulus_bytes)?;
        let n0inv = montgomery_n0inv(modulus_low);
        let rr = (BigUint::from(1_u8) << (RSA_MODULUS_BITS * 2)) % modulus;
        let exponent = biguint_to_u32(public_key.e())?;
        if exponent != 3 && exponent != 65_537 {
            return Err(AdbAuthError::UnsupportedPublicExponent);
        }

        let mut output = [0_u8; ANDROID_PUBLIC_KEY_LEN];
        write_word(&mut output, 0, RSA_MODULUS_WORDS);
        write_word(&mut output, 4, n0inv);
        write_padded(&mut output[8..264], &modulus_bytes)?;
        write_padded(&mut output[264..520], &rr.to_bytes_le())?;
        write_word(&mut output, 520, exponent);
        Ok(output)
    }
}

impl AdbAuthenticator for RsaAdbCredential {
    fn sign_token(&self, token: &[u8]) -> Result<Bytes, AdbAuthError> {
        if token.len() != ADB_TOKEN_LEN {
            return Err(AdbAuthError::InvalidTokenLength {
                expected: ADB_TOKEN_LEN,
                actual: token.len(),
            });
        }

        let signature = self.private_key.sign(Pkcs1v15Sign::new::<Sha1>(), token)?;
        debug_assert_eq!(signature.len(), ADB_SIGNATURE_LEN);
        Ok(Bytes::from(signature))
    }

    fn public_key_payload(&self) -> Result<Bytes, AdbAuthError> {
        let encoded = STANDARD.encode(self.android_public_key()?);
        let mut payload = Vec::with_capacity(encoded.len() + self.comment.len() + 2);
        payload.extend_from_slice(encoded.as_bytes());
        payload.push(b' ');
        payload.extend_from_slice(self.comment.as_bytes());
        payload.push(0);
        Ok(Bytes::from(payload))
    }
}

impl fmt::Debug for RsaAdbCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RsaAdbCredential")
            .field("private_key", &"[REDACTED]")
            .field("comment", &self.comment)
            .finish()
    }
}

fn validate_comment(comment: &str) -> Result<(), AdbAuthError> {
    if comment.is_empty()
        || comment.len() > MAX_KEY_COMMENT_LEN
        || comment.chars().any(char::is_control)
    {
        return Err(AdbAuthError::InvalidKeyComment);
    }
    Ok(())
}

fn validate_key_size(public_key: &RsaPublicKey) -> Result<(), AdbAuthError> {
    if public_key.n().bits() != RSA_MODULUS_BITS {
        return Err(AdbAuthError::UnsupportedKeySize {
            expected: RSA_MODULUS_BITS,
            actual: public_key.n().bits(),
        });
    }
    Ok(())
}

fn read_low_word(bytes: &[u8]) -> Result<u32, AdbAuthError> {
    let word: [u8; 4] = bytes
        .get(..4)
        .and_then(|value| value.try_into().ok())
        .ok_or(AdbAuthError::UnsupportedKeySize {
            expected: RSA_MODULUS_BITS,
            actual: bytes.len() * 8,
        })?;
    Ok(u32::from_le_bytes(word))
}

fn montgomery_n0inv(modulus_low: u32) -> u32 {
    let mut inverse = 1_u32;
    for _ in 0..5 {
        inverse = inverse.wrapping_mul(2_u32.wrapping_sub(modulus_low.wrapping_mul(inverse)));
    }
    inverse.wrapping_neg()
}

fn biguint_to_u32(value: &BigUint) -> Result<u32, AdbAuthError> {
    let bytes = value.to_bytes_le();
    if bytes.len() > 4 {
        return Err(AdbAuthError::UnsupportedPublicExponent);
    }
    let mut word = [0_u8; 4];
    word[..bytes.len()].copy_from_slice(&bytes);
    Ok(u32::from_le_bytes(word))
}

fn write_padded(destination: &mut [u8], value: &[u8]) -> Result<(), AdbAuthError> {
    let Some(prefix) = destination.get_mut(..value.len()) else {
        return Err(AdbAuthError::UnsupportedKeySize {
            expected: RSA_MODULUS_BITS,
            actual: value.len() * 8,
        });
    };
    prefix.copy_from_slice(value);
    Ok(())
}

fn write_word(output: &mut [u8; ANDROID_PUBLIC_KEY_LEN], offset: usize, value: u32) {
    if let Some(destination) = output.get_mut(offset..offset.saturating_add(4)) {
        destination.copy_from_slice(&value.to_le_bytes());
    }
}
