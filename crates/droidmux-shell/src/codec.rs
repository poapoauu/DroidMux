use adb_protocol::MAX_PAYLOAD;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::ShellProtocolError;

pub(crate) const SHELL_HEADER_LEN: usize = 5;
pub(crate) const MAX_SHELL_DATA: usize = MAX_PAYLOAD - SHELL_HEADER_LEN;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum ShellPacketId {
    Stdin = 0,
    Stdout = 1,
    Stderr = 2,
    Exit = 3,
    CloseStdin = 4,
    WindowSizeChange = 5,
}

impl TryFrom<u8> for ShellPacketId {
    type Error = ShellProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Stdin),
            1 => Ok(Self::Stdout),
            2 => Ok(Self::Stderr),
            3 => Ok(Self::Exit),
            4 => Ok(Self::CloseStdin),
            5 => Ok(Self::WindowSizeChange),
            actual => Err(ShellProtocolError::UnknownPacketId { actual }),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ShellPacket {
    pub(crate) id: ShellPacketId,
    pub(crate) payload: Bytes,
}

pub(crate) fn encode_packet(
    id: ShellPacketId,
    payload: &[u8],
    wire_limit: usize,
) -> Result<Bytes, ShellProtocolError> {
    let packet_length =
        SHELL_HEADER_LEN
            .checked_add(payload.len())
            .ok_or(ShellProtocolError::PacketTooLarge {
                limit: wire_limit.saturating_sub(SHELL_HEADER_LEN),
                actual: payload.len(),
            })?;
    if payload.len() > MAX_SHELL_DATA || packet_length > wire_limit {
        return Err(ShellProtocolError::PacketTooLarge {
            limit: wire_limit
                .saturating_sub(SHELL_HEADER_LEN)
                .min(MAX_SHELL_DATA),
            actual: payload.len(),
        });
    }
    let payload_length =
        u32::try_from(payload.len()).map_err(|_| ShellProtocolError::PacketTooLarge {
            limit: MAX_SHELL_DATA,
            actual: payload.len(),
        })?;

    let mut encoded = BytesMut::with_capacity(packet_length);
    encoded.put_u8(id as u8);
    encoded.put_u32_le(payload_length);
    encoded.extend_from_slice(payload);
    Ok(encoded.freeze())
}

#[derive(Default)]
pub(crate) struct ShellDecoder {
    buffer: BytesMut,
}

impl ShellDecoder {
    pub(crate) fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    pub(crate) fn next_packet(&mut self) -> Result<Option<ShellPacket>, ShellProtocolError> {
        if self.buffer.len() < SHELL_HEADER_LEN {
            return Ok(None);
        }

        let id = ShellPacketId::try_from(self.buffer[0])?;
        let length_bytes: [u8; 4] = self.buffer[1..SHELL_HEADER_LEN].try_into().map_err(|_| {
            ShellProtocolError::TruncatedFrame {
                remaining: self.buffer.len(),
            }
        })?;
        let wire_length = u32::from_le_bytes(length_bytes);
        let payload_length =
            usize::try_from(wire_length).map_err(|_| ShellProtocolError::LengthOutOfRange {
                actual: wire_length,
            })?;
        if payload_length > MAX_SHELL_DATA {
            return Err(ShellProtocolError::PacketTooLarge {
                limit: MAX_SHELL_DATA,
                actual: payload_length,
            });
        }
        let frame_length = SHELL_HEADER_LEN + payload_length;
        if self.buffer.len() < frame_length {
            return Ok(None);
        }

        self.buffer.advance(SHELL_HEADER_LEN);
        Ok(Some(ShellPacket {
            id,
            payload: self.buffer.split_to(payload_length).freeze(),
        }))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub(crate) fn finish(self) -> Result<(), ShellProtocolError> {
        if self.buffer.is_empty() {
            Ok(())
        } else {
            Err(ShellProtocolError::TruncatedFrame {
                remaining: self.buffer.len(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_a_little_endian_shell_v2_header() {
        let encoded = encode_packet(ShellPacketId::Stdin, b"abc", 64)
            .expect("the stdin packet should encode");

        assert_eq!(&encoded[..SHELL_HEADER_LEN], &[0, 3, 0, 0, 0]);
        assert_eq!(&encoded[SHELL_HEADER_LEN..], b"abc");
    }

    #[test]
    fn decodes_fragmented_and_coalesced_packets() {
        let stdout = encode_packet(ShellPacketId::Stdout, b"hello", 64)
            .expect("the stdout packet should encode");
        let stderr = encode_packet(ShellPacketId::Stderr, b"warning", 64)
            .expect("the stderr packet should encode");
        let mut wire = BytesMut::new();
        wire.extend_from_slice(&stdout);
        wire.extend_from_slice(&stderr);

        let mut decoder = ShellDecoder::default();
        decoder.push(&wire[..3]);
        assert!(
            decoder
                .next_packet()
                .expect("a partial header is valid")
                .is_none()
        );
        decoder.push(&wire[3..]);

        let first = decoder
            .next_packet()
            .expect("the first frame should decode")
            .expect("the first frame should be complete");
        let second = decoder
            .next_packet()
            .expect("the second frame should decode")
            .expect("the second frame should be complete");
        assert_eq!(first.id, ShellPacketId::Stdout);
        assert_eq!(first.payload, Bytes::from_static(b"hello"));
        assert_eq!(second.id, ShellPacketId::Stderr);
        assert_eq!(second.payload, Bytes::from_static(b"warning"));
        assert!(decoder.is_empty());
    }

    #[test]
    fn rejects_an_unknown_packet_identifier() {
        let mut decoder = ShellDecoder::default();
        decoder.push(&[99, 0, 0, 0, 0]);

        let error = decoder
            .next_packet()
            .expect_err("an unknown channel must be rejected");
        assert!(matches!(
            error,
            ShellProtocolError::UnknownPacketId { actual: 99 }
        ));
    }

    #[test]
    fn rejects_an_oversized_length_before_buffering_payload() {
        let oversized = u32::try_from(MAX_SHELL_DATA + 1)
            .expect("the oversized test length should fit on the wire");
        let mut header = [0_u8; SHELL_HEADER_LEN];
        header[0] = ShellPacketId::Stdout as u8;
        header[1..].copy_from_slice(&oversized.to_le_bytes());
        let mut decoder = ShellDecoder::default();
        decoder.push(&header);

        let error = decoder
            .next_packet()
            .expect_err("an oversized frame must be rejected from its header");
        assert!(matches!(
            error,
            ShellProtocolError::PacketTooLarge {
                limit: MAX_SHELL_DATA,
                actual,
            } if actual == MAX_SHELL_DATA + 1
        ));
    }

    #[test]
    fn reports_a_truncated_frame_when_the_stream_ends() {
        let mut decoder = ShellDecoder::default();
        decoder.push(&[ShellPacketId::Stdout as u8, 4, 0, 0, 0, b'a']);

        let error = decoder
            .finish()
            .expect_err("a partial payload must not be accepted");
        assert!(matches!(
            error,
            ShellProtocolError::TruncatedFrame { remaining: 6 }
        ));
    }
}
