use adb_client::{AdbClient, AdbStream};
use bytes::{Bytes, BytesMut};

const JDWP_HANDSHAKE: &[u8; 14] = b"JDWP-Handshake";
const JDWP_DDM_COMMAND_SET: u8 = 199;
const JDWP_DDM_COMMAND: u8 = 1;
const DDMS_EXIT_CHUNK: &[u8; 4] = b"EXIT";
const DDMS_EXIT_STATUS: u32 = 1;
const JDWP_PACKET_HEADER_LENGTH: usize = 11;
const DDMS_CHUNK_HEADER_LENGTH: usize = 8;
const DDMS_EXIT_PAYLOAD_LENGTH: usize = 4;
const DDMS_EXIT_PAYLOAD_LENGTH_WIRE: u32 = 4;
const DDMS_EXIT_PACKET_LENGTH_WIRE: u32 = 23;

pub(crate) async fn kill_debuggable_process(
    client: &AdbClient,
    process_id: u32,
) -> Result<(), String> {
    let stream = client
        .open_jdwp_process(process_id)
        .await
        .map_err(|error| error.to_string())?;
    let result = send_ddms_exit(&stream).await;
    let _ = stream.close().await;
    result
}

async fn send_ddms_exit(stream: &AdbStream) -> Result<(), String> {
    stream
        .write(Bytes::from_static(JDWP_HANDSHAKE))
        .await
        .map_err(|error| error.to_string())?;
    let response = read_exact(stream, JDWP_HANDSHAKE.len()).await?;
    if response.as_ref() != JDWP_HANDSHAKE {
        return Err("JDWP process returned an invalid handshake".to_owned());
    }
    stream
        .write(ddms_exit_packet())
        .await
        .map_err(|error| error.to_string())
}

async fn read_exact(stream: &AdbStream, length: usize) -> Result<Bytes, String> {
    let mut buffered = BytesMut::with_capacity(length);
    while buffered.len() < length {
        let chunk = stream
            .read()
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "JDWP process closed during handshake".to_owned())?;
        append_prefix(&mut buffered, &chunk, length);
    }
    Ok(buffered.freeze())
}

fn append_prefix(buffered: &mut BytesMut, chunk: &[u8], length: usize) {
    let remaining = length.saturating_sub(buffered.len());
    buffered.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
}

fn ddms_exit_packet() -> Bytes {
    let packet_length =
        JDWP_PACKET_HEADER_LENGTH + DDMS_CHUNK_HEADER_LENGTH + DDMS_EXIT_PAYLOAD_LENGTH;
    let mut packet = BytesMut::with_capacity(packet_length);
    packet.extend_from_slice(&DDMS_EXIT_PACKET_LENGTH_WIRE.to_be_bytes());
    packet.extend_from_slice(&1_u32.to_be_bytes());
    packet.extend_from_slice(&[0, JDWP_DDM_COMMAND_SET, JDWP_DDM_COMMAND]);
    packet.extend_from_slice(DDMS_EXIT_CHUNK);
    packet.extend_from_slice(&DDMS_EXIT_PAYLOAD_LENGTH_WIRE.to_be_bytes());
    packet.extend_from_slice(&DDMS_EXIT_STATUS.to_be_bytes());
    packet.freeze()
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::{
        DDMS_EXIT_CHUNK, JDWP_DDM_COMMAND, JDWP_DDM_COMMAND_SET, JDWP_HANDSHAKE, append_prefix,
        ddms_exit_packet,
    };

    #[test]
    fn ddms_exit_packet_matches_android_studio_wire_format() {
        let packet = ddms_exit_packet();

        assert_eq!(packet.len(), 23);
        assert_eq!(&packet[0..4], &23_u32.to_be_bytes());
        assert_eq!(&packet[4..8], &1_u32.to_be_bytes());
        assert_eq!(packet[8], 0);
        assert_eq!(packet[9], JDWP_DDM_COMMAND_SET);
        assert_eq!(packet[10], JDWP_DDM_COMMAND);
        assert_eq!(&packet[11..15], DDMS_EXIT_CHUNK);
        assert_eq!(&packet[15..19], &4_u32.to_be_bytes());
        assert_eq!(&packet[19..23], &1_u32.to_be_bytes());
    }

    #[test]
    fn handshake_prefix_accepts_fragmented_and_coalesced_stream_data() {
        let mut fragmented = BytesMut::new();
        append_prefix(&mut fragmented, &JDWP_HANDSHAKE[..5], JDWP_HANDSHAKE.len());
        append_prefix(&mut fragmented, &JDWP_HANDSHAKE[5..], JDWP_HANDSHAKE.len());
        assert_eq!(fragmented.as_ref(), JDWP_HANDSHAKE);

        let mut coalesced = BytesMut::new();
        let mut chunk = JDWP_HANDSHAKE.to_vec();
        chunk.extend_from_slice(b"following-jdwp-packet");
        append_prefix(&mut coalesced, &chunk, JDWP_HANDSHAKE.len());
        assert_eq!(coalesced.as_ref(), JDWP_HANDSHAKE);
    }
}
