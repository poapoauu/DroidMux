//! Black-box tests for the public ADB packet codec.

use bytes::Bytes;
use droidmux_protocol::{
    ADB_HEADER_LEN, ADB_VERSION, AdbCommand, AdbPacket, AdbProtocolError, MAX_PAYLOAD, checksum,
};

const CONNECT_PAYLOAD: &[u8] = b"host::\0";
const MAX_PAYLOAD_WIRE: u32 = 1024 * 1024;

fn connect_packet() -> AdbPacket {
    AdbPacket::new(
        AdbCommand::Connect,
        ADB_VERSION,
        MAX_PAYLOAD_WIRE,
        Bytes::from_static(CONNECT_PAYLOAD),
    )
    .expect("the fixture payload is valid")
}

#[test]
fn command_codes_match_the_wire_protocol() {
    let cases = [
        (AdbCommand::Connect, 0x4e58_4e43),
        (AdbCommand::Sync, 0x434e_5953),
        (AdbCommand::Auth, 0x4854_5541),
        (AdbCommand::Open, 0x4e45_504f),
        (AdbCommand::Okay, 0x5941_4b4f),
        (AdbCommand::Write, 0x4554_5257),
        (AdbCommand::Close, 0x4553_4c43),
        (AdbCommand::StartTls, 0x534c_5453),
    ];

    for (command, wire_value) in cases {
        assert_eq!(command.wire_value(), wire_value);
        assert_eq!(AdbCommand::try_from(wire_value), Ok(command));
        assert_eq!(command.magic(), wire_value ^ u32::MAX);
    }
}

#[test]
fn checksum_sums_payload_bytes() {
    assert_eq!(checksum(CONNECT_PAYLOAD), 562);
    assert_eq!(checksum(&[]), 0);
}

#[test]
fn encodes_a_known_connect_packet() {
    let encoded = connect_packet().encode().expect("fixture should encode");
    let expected_header = [
        0x43, 0x4e, 0x58, 0x4e, // CNXN
        0x01, 0x00, 0x00, 0x01, // version 0x01000001
        0x00, 0x00, 0x10, 0x00, // max payload 1 MiB
        0x07, 0x00, 0x00, 0x00, // payload length
        0x32, 0x02, 0x00, 0x00, // checksum
        0xbc, 0xb1, 0xa7, 0xb1, // magic
    ];

    assert_eq!(&encoded[..ADB_HEADER_LEN], &expected_header);
    assert_eq!(&encoded[ADB_HEADER_LEN..], CONNECT_PAYLOAD);
}

#[test]
fn decodes_one_frame_and_reports_consumed_bytes() {
    let packet = connect_packet();
    let encoded = packet.encode().expect("fixture should encode");
    let mut buffered = encoded.to_vec();
    buffered.extend_from_slice(b"next-frame");

    let (decoded, consumed) = AdbPacket::decode(&buffered).expect("fixture should decode");

    assert_eq!(decoded, packet);
    assert_eq!(consumed, encoded.len());
}

#[test]
fn rejects_a_truncated_header() {
    let error = AdbPacket::decode(&[0_u8; ADB_HEADER_LEN - 1])
        .expect_err("a truncated header must be rejected");

    assert_eq!(
        error,
        AdbProtocolError::IncompleteHeader {
            expected: ADB_HEADER_LEN,
            actual: ADB_HEADER_LEN - 1,
        }
    );
}

#[test]
fn rejects_a_truncated_payload() {
    let encoded = connect_packet().encode().expect("fixture should encode");
    let error = AdbPacket::decode(&encoded[..encoded.len() - 1])
        .expect_err("a truncated payload must be rejected");

    assert_eq!(
        error,
        AdbProtocolError::IncompletePayload {
            expected: CONNECT_PAYLOAD.len(),
            actual: CONNECT_PAYLOAD.len() - 1,
        }
    );
}

#[test]
fn rejects_an_unknown_command() {
    let mut encoded = connect_packet()
        .encode()
        .expect("fixture should encode")
        .to_vec();
    encoded[..4].copy_from_slice(&0xdead_beef_u32.to_le_bytes());

    assert_eq!(
        AdbPacket::decode(&encoded).expect_err("an unknown command must be rejected"),
        AdbProtocolError::UnknownCommand(0xdead_beef)
    );
}

#[test]
fn rejects_an_invalid_magic_value() {
    let mut encoded = connect_packet()
        .encode()
        .expect("fixture should encode")
        .to_vec();
    encoded[20..24].copy_from_slice(&0_u32.to_le_bytes());

    assert_eq!(
        AdbPacket::decode(&encoded).expect_err("invalid magic must be rejected"),
        AdbProtocolError::InvalidMagic {
            expected: AdbCommand::Connect.magic(),
            actual: 0,
        }
    );
}

#[test]
fn rejects_a_checksum_mismatch() {
    let mut encoded = connect_packet()
        .encode()
        .expect("fixture should encode")
        .to_vec();
    encoded[ADB_HEADER_LEN] ^= 0xff;

    assert_eq!(
        AdbPacket::decode(&encoded).expect_err("a checksum mismatch must be rejected"),
        AdbProtocolError::ChecksumMismatch {
            expected: 562,
            actual: 609,
        }
    );
}

#[test]
fn accepts_an_omitted_checksum_from_adb_1_0_1_peers() {
    let packet = connect_packet();
    let mut encoded = packet.encode().expect("fixture should encode").to_vec();
    encoded[16..20].fill(0);

    let (decoded, consumed) =
        AdbPacket::decode(&encoded).expect("a zero checksum marks checksum omission");

    assert_eq!(decoded, packet);
    assert_eq!(consumed, encoded.len());
}

#[test]
fn rejects_an_oversized_payload_before_reading_it() {
    let command = AdbCommand::Write;
    let oversized = MAX_PAYLOAD_WIRE + 1;
    let mut header = [0_u8; ADB_HEADER_LEN];
    header[0..4].copy_from_slice(&command.wire_value().to_le_bytes());
    header[12..16].copy_from_slice(&oversized.to_le_bytes());
    header[20..24].copy_from_slice(&command.magic().to_le_bytes());

    assert_eq!(
        AdbPacket::decode(&header).expect_err("an oversized payload must be rejected"),
        AdbProtocolError::PayloadTooLarge {
            length: MAX_PAYLOAD + 1,
            max: MAX_PAYLOAD,
        }
    );
}

#[test]
fn refuses_to_construct_an_oversized_packet() {
    let payload = Bytes::from(vec![0_u8; MAX_PAYLOAD + 1]);

    assert_eq!(
        AdbPacket::new(AdbCommand::Write, 1, 2, payload)
            .expect_err("an oversized packet must be rejected"),
        AdbProtocolError::PayloadTooLarge {
            length: MAX_PAYLOAD + 1,
            max: MAX_PAYLOAD,
        }
    );
}
