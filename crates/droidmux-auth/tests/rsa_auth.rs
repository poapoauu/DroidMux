//! Compatibility tests for ADB's RSA-2048 authentication format.

use std::sync::OnceLock;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use droidmux_auth::{
    ADB_TOKEN_LEN, ANDROID_PUBLIC_KEY_LEN, AdbAuthError, AdbAuthenticator, RsaAdbCredential,
};
use rand::rngs::OsRng;
use rsa::{BigUint, Pkcs1v15Sign, RsaPrivateKey, traits::PublicKeyParts};
use sha1::Sha1;

const COMMENT: &str = "droidmux@test-host";

fn credential() -> &'static RsaAdbCredential {
    static CREDENTIAL: OnceLock<RsaAdbCredential> = OnceLock::new();
    CREDENTIAL.get_or_init(|| {
        RsaAdbCredential::generate(COMMENT).expect("a test credential should be generated")
    })
}

fn read_word(input: &[u8], offset: usize) -> u32 {
    let bytes: [u8; 4] = input[offset..offset + 4]
        .try_into()
        .expect("test fixture contains a complete word");
    u32::from_le_bytes(bytes)
}

#[test]
fn signs_a_twenty_byte_token_with_rsa_sha1() {
    let token = [0x42_u8; ADB_TOKEN_LEN];
    let signature = credential()
        .sign_token(&token)
        .expect("a valid token should be signed");

    assert_eq!(signature.len(), 256);
    credential()
        .public_key()
        .verify(Pkcs1v15Sign::new::<Sha1>(), &token, &signature)
        .expect("the public key should verify the ADB signature");
}

#[test]
fn rejects_a_token_with_the_wrong_length() {
    let error = credential()
        .sign_token(&[0_u8; ADB_TOKEN_LEN - 1])
        .expect_err("ADB authentication tokens must be SHA-1 sized");

    assert!(matches!(
        error,
        AdbAuthError::InvalidTokenLength {
            expected: ADB_TOKEN_LEN,
            actual,
        } if actual == ADB_TOKEN_LEN - 1
    ));
}

#[test]
fn rejects_a_key_that_is_not_exactly_rsa_2048() {
    let private_key =
        RsaPrivateKey::new(&mut OsRng, 2040).expect("a non-standard test key should be generated");

    let error = RsaAdbCredential::from_private_key(private_key, COMMENT)
        .expect_err("Android's public-key format requires exactly 2048 bits");

    assert!(matches!(
        error,
        AdbAuthError::UnsupportedKeySize {
            expected: 2048,
            actual: 2040,
        }
    ));
}

#[test]
fn encodes_the_android_public_key_structure() {
    let payload = credential()
        .public_key_payload()
        .expect("the public key should encode");
    assert_eq!(payload.last(), Some(&0));

    let without_nul = &payload[..payload.len() - 1];
    let separator = without_nul
        .iter()
        .position(|byte| *byte == b' ')
        .expect("public key payload should contain a comment separator");
    assert_eq!(&without_nul[separator + 1..], COMMENT.as_bytes());

    let encoded = STANDARD
        .decode(&without_nul[..separator])
        .expect("public key should use standard base64");
    assert_eq!(encoded.len(), ANDROID_PUBLIC_KEY_LEN);
    assert_eq!(read_word(&encoded, 0), 64);
    assert_eq!(read_word(&encoded, 520), 65_537);

    let modulus = credential().public_key().n().to_bytes_le();
    assert_eq!(&encoded[8..8 + modulus.len()], modulus);
    assert!(
        encoded[8 + modulus.len()..264]
            .iter()
            .all(|byte| *byte == 0)
    );

    let modulus_low = read_word(&encoded, 8);
    let n0inv = read_word(&encoded, 4);
    assert_eq!(modulus_low.wrapping_mul(n0inv), u32::MAX);

    let rr = BigUint::from_bytes_le(&encoded[264..520]);
    let expected_rr = (BigUint::from(1_u8) << 4096) % credential().public_key().n();
    assert_eq!(rr, expected_rr);
}

#[test]
fn private_key_pem_round_trip_reuses_the_same_identity() {
    let pem = credential()
        .to_pkcs8_pem()
        .expect("private key should encode as PKCS#8");
    let restored = RsaAdbCredential::from_pkcs8_pem(&pem, COMMENT)
        .expect("private key should decode from PKCS#8");
    let token = [0x24_u8; ADB_TOKEN_LEN];

    assert_eq!(restored.public_key(), credential().public_key());
    assert_eq!(
        restored
            .sign_token(&token)
            .expect("restored key should sign"),
        credential()
            .sign_token(&token)
            .expect("original key should sign")
    );
}

#[test]
fn rejects_a_public_key_comment_with_control_characters() {
    let error = RsaAdbCredential::generate("droidmux\ninvalid")
        .expect_err("newlines must not enter the public key payload");

    assert!(matches!(error, AdbAuthError::InvalidKeyComment));
}

#[test]
fn debug_output_redacts_the_private_key() {
    let debug = format!("{:?}", credential());

    assert!(debug.contains(COMMENT));
    assert!(debug.contains("REDACTED"));
    assert!(!debug.contains("PRIVATE KEY"));
}
