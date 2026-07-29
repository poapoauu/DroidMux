use aes_gcm::{
    Aes128Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use curve25519_dalek::{
    constants::ED25519_BASEPOINT_POINT,
    edwards::{CompressedEdwardsY, EdwardsPoint},
    scalar::Scalar,
    traits::Identity,
};
use hkdf::Hkdf;
use rand::{RngCore, rngs::OsRng};
use sha2::{Digest, Sha256, Sha512};
use subtle::{Choice, ConditionallySelectable};
use zeroize::Zeroizing;

use crate::PairingError;

const CLIENT_NAME: &[u8] = b"adb pair client\0";
const SERVER_NAME: &[u8] = b"adb pair server\0";
const AES_INFO: &[u8] = b"adb pairing_auth aes-128-gcm key";

// Encoded points used by BoringSSL's SPAKE2 implementation. They intentionally
// differ from the RFC 9382 points, so a generic SPAKE2 crate is not compatible.
const SPAKE_M: [u8; 32] = [
    0x5a, 0xda, 0x7e, 0x4b, 0xf6, 0xdd, 0xd9, 0xad, 0xb6, 0x62, 0x6d, 0x32, 0x13, 0x1c, 0x6b, 0x5c,
    0x51, 0xa1, 0xe3, 0x47, 0xa3, 0x47, 0x8f, 0x53, 0xcf, 0xcf, 0x44, 0x1b, 0x88, 0xee, 0xd1, 0x2e,
];
const SPAKE_N: [u8; 32] = [
    0x10, 0xe3, 0xdf, 0x0a, 0xe3, 0x7d, 0x8e, 0x7a, 0x99, 0xb5, 0xfe, 0x74, 0xb4, 0x46, 0x72, 0x10,
    0x3d, 0xbd, 0xdc, 0xbd, 0x06, 0xaf, 0x68, 0x0d, 0x71, 0x32, 0x9a, 0x11, 0x69, 0x3b, 0xc7, 0x78,
];
const GROUP_ORDER: [u8; 32] = [
    0xed, 0xd3, 0xf5, 0x5c, 0x1a, 0x63, 0x12, 0x58, 0xd6, 0x9c, 0xf7, 0xa2, 0xde, 0xf9, 0xde, 0x14,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10,
];

#[derive(Debug, Clone, Copy)]
pub(crate) enum SpakeRole {
    Client,
    #[cfg(test)]
    Server,
}

pub(crate) struct Spake2State {
    role: SpakeRole,
    private_scalar: Zeroizing<[u8; 32]>,
    password_scalar: Zeroizing<[u8; 32]>,
    password_hash: Zeroizing<[u8; 64]>,
    our_message: [u8; 32],
}

impl Spake2State {
    pub(crate) fn start_client(password: &[u8]) -> Result<(Self, [u8; 32]), PairingError> {
        let mut random = [0_u8; 64];
        OsRng.fill_bytes(&mut random);
        Self::start(SpakeRole::Client, password, random)
    }

    #[cfg(test)]
    pub(crate) fn start_server(password: &[u8]) -> Result<(Self, [u8; 32]), PairingError> {
        let mut random = [0_u8; 64];
        OsRng.fill_bytes(&mut random);
        Self::start(SpakeRole::Server, password, random)
    }

    fn start(
        role: SpakeRole,
        password: &[u8],
        random: [u8; 64],
    ) -> Result<(Self, [u8; 32]), PairingError> {
        if password.is_empty() {
            return Err(PairingError::Protocol(
                "SPAKE2 password cannot be empty".to_owned(),
            ));
        }

        let mut private_scalar = Scalar::from_bytes_mod_order_wide(&random).to_bytes();
        shift_left_three(&mut private_scalar);

        let password_hash: [u8; 64] = Sha512::digest(password).into();
        let mut password_scalar = Scalar::from_bytes_mod_order_wide(&password_hash).to_bytes();
        clear_cofactor_bits(&mut password_scalar);

        let mask = decode_point(match role {
            SpakeRole::Client => SPAKE_M,
            #[cfg(test)]
            SpakeRole::Server => SPAKE_N,
        })?;
        let public = multiply_raw(&ED25519_BASEPOINT_POINT, &private_scalar);
        let masked = public + multiply_raw(&mask, &password_scalar);
        let our_message = masked.compress().to_bytes();

        let state = Self {
            role,
            private_scalar: Zeroizing::new(private_scalar),
            password_scalar: Zeroizing::new(password_scalar),
            password_hash: Zeroizing::new(password_hash),
            our_message,
        };
        Ok((state, our_message))
    }

    pub(crate) fn finish(self, peer_message: &[u8]) -> Result<Zeroizing<[u8; 64]>, PairingError> {
        let peer_encoded: [u8; 32] = peer_message.try_into().map_err(|_| {
            PairingError::Protocol(format!(
                "SPAKE2 message must be 32 bytes, received {}",
                peer_message.len()
            ))
        })?;
        let peer_masked = decode_point(peer_encoded)?;
        let peer_mask = decode_point(match self.role {
            SpakeRole::Client => SPAKE_N,
            #[cfg(test)]
            SpakeRole::Server => SPAKE_M,
        })?;
        let peer_public = peer_masked - multiply_raw(&peer_mask, &self.password_scalar);
        let shared = multiply_raw(&peer_public, &self.private_scalar)
            .compress()
            .to_bytes();

        let (client_message, server_message) = match self.role {
            SpakeRole::Client => (self.our_message.as_slice(), peer_message),
            #[cfg(test)]
            SpakeRole::Server => (peer_message, self.our_message.as_slice()),
        };
        let mut transcript = Sha512::new();
        update_length_prefixed(&mut transcript, CLIENT_NAME);
        update_length_prefixed(&mut transcript, SERVER_NAME);
        update_length_prefixed(&mut transcript, client_message);
        update_length_prefixed(&mut transcript, server_message);
        update_length_prefixed(&mut transcript, &shared);
        update_length_prefixed(&mut transcript, &*self.password_hash);
        Ok(Zeroizing::new(transcript.finalize().into()))
    }
}

pub(crate) struct PairingCipher {
    cipher: Aes128Gcm,
    encrypt_sequence: u64,
    decrypt_sequence: u64,
}

impl PairingCipher {
    pub(crate) fn new(key_material: &[u8]) -> Result<Self, PairingError> {
        let hkdf = Hkdf::<Sha256>::new(None, key_material);
        let mut key = Zeroizing::new([0_u8; 16]);
        hkdf.expand(AES_INFO, &mut *key).map_err(|_| {
            PairingError::Protocol("unable to derive pairing cipher key".to_owned())
        })?;
        Ok(Self {
            cipher: Aes128Gcm::new((&*key).into()),
            encrypt_sequence: 0,
            decrypt_sequence: 0,
        })
    }

    pub(crate) fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, PairingError> {
        let nonce = sequence_nonce(self.encrypt_sequence);
        let encrypted = self
            .cipher
            .encrypt(Nonce::from_slice(&nonce), Payload::from(plaintext))
            .map_err(|_| PairingError::Protocol("unable to encrypt PeerInfo".to_owned()))?;
        self.encrypt_sequence = self
            .encrypt_sequence
            .checked_add(1)
            .ok_or_else(|| PairingError::Protocol("pairing nonce exhausted".to_owned()))?;
        Ok(encrypted)
    }

    pub(crate) fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, PairingError> {
        let nonce = sequence_nonce(self.decrypt_sequence);
        let plaintext = self
            .cipher
            .decrypt(Nonce::from_slice(&nonce), Payload::from(ciphertext))
            .map_err(|_| PairingError::IncorrectPairingCode)?;
        self.decrypt_sequence = self
            .decrypt_sequence
            .checked_add(1)
            .ok_or_else(|| PairingError::Protocol("pairing nonce exhausted".to_owned()))?;
        Ok(plaintext)
    }
}

fn decode_point(encoded: [u8; 32]) -> Result<EdwardsPoint, PairingError> {
    CompressedEdwardsY(encoded)
        .decompress()
        .ok_or_else(|| PairingError::Protocol("SPAKE2 point is not on Ed25519".to_owned()))
}

fn multiply_raw(point: &EdwardsPoint, scalar: &[u8; 32]) -> EdwardsPoint {
    let mut result = EdwardsPoint::identity();
    let mut multiple = *point;
    for byte in scalar {
        for bit in 0..8 {
            let candidate = result + multiple;
            let selected = Choice::from((byte >> bit) & 1);
            result = EdwardsPoint::conditional_select(&result, &candidate, selected);
            multiple = multiple + multiple;
        }
    }
    result
}

fn shift_left_three(value: &mut [u8; 32]) {
    let mut carry = 0_u8;
    for byte in value {
        let next_carry = *byte >> 5;
        *byte = (*byte << 3) | carry;
        carry = next_carry;
    }
}

fn clear_cofactor_bits(value: &mut [u8; 32]) {
    let mut order = GROUP_ORDER;
    for bit in 0..3 {
        let sum = add_le(value, &order);
        let choice = Choice::from((value[0] >> bit) & 1);
        for (destination, candidate) in value.iter_mut().zip(sum) {
            *destination = u8::conditional_select(destination, &candidate, choice);
        }
        order = add_le(&order, &order);
    }
    debug_assert_eq!(value[0] & 7, 0);
}

fn add_le(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut output = [0_u8; 32];
    let mut carry = 0_u16;
    for index in 0..32 {
        let sum = u16::from(left[index]) + u16::from(right[index]) + carry;
        output[index] = sum.to_le_bytes()[0];
        carry = sum >> 8;
    }
    output
}

fn update_length_prefixed(digest: &mut Sha512, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_le_bytes());
    digest.update(value);
}

fn sequence_nonce(sequence: u64) -> [u8; 12] {
    let mut nonce = [0_u8; 12];
    nonce[..8].copy_from_slice(&sequence.to_le_bytes());
    nonce
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boringssl_pairing_format_has_a_stable_compatibility_vector() {
        let mut password = Vec::from(b"123456".as_slice());
        password.extend_from_slice(&[0xa5; 64]);
        let mut client_random = [0_u8; 64];
        let mut server_random = [0_u8; 64];
        for (index, byte) in client_random.iter_mut().enumerate() {
            *byte = u8::try_from(index).expect("client vector index fits");
        }
        for (index, byte) in server_random.iter_mut().enumerate() {
            *byte = u8::try_from(index + 64).expect("server vector index fits");
        }
        let (client, client_message) =
            Spake2State::start(SpakeRole::Client, &password, client_random)
                .expect("client vector should start");
        let (server, server_message) =
            Spake2State::start(SpakeRole::Server, &password, server_random)
                .expect("server vector should start");
        let key = client
            .finish(&server_message)
            .expect("vector should finish");
        let server_key = server
            .finish(&client_message)
            .expect("vector should finish");
        let mut cipher = PairingCipher::new(&*key).expect("vector cipher should derive");
        // This plaintext is part of the published compatibility vector.
        let ciphertext = cipher.encrypt(b"DroidDeck").expect("vector should encrypt");

        assert_eq!(
            client_message,
            [
                0x06, 0x5f, 0x7c, 0xb4, 0x26, 0x93, 0xcd, 0x91, 0xee, 0x88, 0xcc, 0x20, 0x71, 0x0b,
                0xd8, 0xdd, 0xa6, 0x80, 0xec, 0xf7, 0x27, 0xc1, 0xc6, 0x3a, 0x0d, 0x0b, 0x39, 0x25,
                0x33, 0x97, 0xf2, 0x7b,
            ]
        );
        assert_eq!(
            server_message,
            [
                0xa2, 0xba, 0xde, 0xe9, 0xf9, 0x20, 0x77, 0x3b, 0x29, 0x4b, 0x81, 0x22, 0xdc, 0x9d,
                0x71, 0x3f, 0x6c, 0x9e, 0xfe, 0x58, 0x34, 0x8e, 0x5b, 0xf1, 0x7c, 0xad, 0x75, 0x22,
                0x95, 0x5e, 0xaf, 0x19,
            ]
        );
        assert_eq!(
            &*key,
            &[
                0x02, 0xc2, 0xdd, 0x53, 0x2c, 0x0b, 0x3b, 0x36, 0x8c, 0xb7, 0x94, 0x94, 0x10, 0xef,
                0xc7, 0x1c, 0xe0, 0xeb, 0xa9, 0x8e, 0xa4, 0x4f, 0xc5, 0x31, 0x33, 0x71, 0x41, 0x81,
                0xea, 0xcc, 0x30, 0xe0, 0x8d, 0xa5, 0x06, 0x3a, 0xdf, 0x84, 0xa0, 0x67, 0x26, 0x56,
                0x8f, 0xa2, 0x7b, 0xc9, 0xc3, 0xc8, 0xfc, 0xa5, 0x0c, 0x4a, 0x27, 0x0a, 0x46, 0x84,
                0x0b, 0x74, 0xf4, 0x20, 0x59, 0x10, 0x89, 0x15,
            ]
        );
        assert_eq!(&*server_key, &*key);
        assert_eq!(
            ciphertext,
            [
                0x22, 0x99, 0x5d, 0xaa, 0xc0, 0xe8, 0x69, 0xe3, 0xc1, 0xed, 0xed, 0x3d, 0x16, 0x25,
                0x2b, 0xf6, 0xad, 0x7d, 0x29, 0xf7, 0x22, 0xee, 0x33, 0x89, 0xe2,
            ]
        );
    }

    #[test]
    fn matching_boringssl_spake2_roles_derive_the_same_key() {
        let password = b"123456 plus tls exporter";
        let (client, client_message) =
            Spake2State::start_client(password).expect("client should start");
        let (server, server_message) =
            Spake2State::start_server(password).expect("server should start");

        let client_key = client
            .finish(&server_message)
            .expect("client should finish");
        let server_key = server
            .finish(&client_message)
            .expect("server should finish");
        assert_eq!(&*client_key, &*server_key);
    }

    #[test]
    fn wrong_password_fails_peer_info_authentication() {
        let (client, client_message) =
            Spake2State::start_client(b"123456 exporter").expect("client should start");
        let (server, server_message) =
            Spake2State::start_server(b"654321 exporter").expect("server should start");
        let client_key = client
            .finish(&server_message)
            .expect("client exchange should finish");
        let server_key = server
            .finish(&client_message)
            .expect("server exchange should finish");
        let mut client_cipher = PairingCipher::new(&*client_key).expect("cipher should derive");
        let mut server_cipher = PairingCipher::new(&*server_key).expect("cipher should derive");
        let encrypted = client_cipher
            .encrypt(b"peer info")
            .expect("encryption should succeed");

        assert!(matches!(
            server_cipher.decrypt(&encrypted),
            Err(PairingError::IncorrectPairingCode)
        ));
    }

    #[test]
    fn aes_gcm_sequences_are_independent_in_each_direction() {
        let key_material = [0x42_u8; 64];
        let mut client = PairingCipher::new(&key_material).expect("cipher should derive");
        let mut server = PairingCipher::new(&key_material).expect("cipher should derive");
        let encrypted = client.encrypt(b"hello").expect("should encrypt");
        assert_eq!(
            server.decrypt(&encrypted).expect("should decrypt"),
            b"hello"
        );
    }
}
