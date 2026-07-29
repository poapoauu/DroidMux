use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::PairingError;

pub(crate) const PEER_INFO_SIZE: usize = 8192;
const PAIRING_HEADER_SIZE: usize = 6;
const PAIRING_VERSION: u8 = 1;
const MAX_PAIRING_PAYLOAD: usize = PEER_INFO_SIZE * 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PacketType {
    Spake2 = 0,
    PeerInfo = 1,
}

impl TryFrom<u8> for PacketType {
    type Error = PairingError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Spake2),
            1 => Ok(Self::PeerInfo),
            _ => Err(PairingError::Protocol(format!(
                "unknown pairing packet type {value}"
            ))),
        }
    }
}

pub(crate) async fn write_packet<W>(
    writer: &mut W,
    packet_type: PacketType,
    payload: &[u8],
) -> Result<(), PairingError>
where
    W: AsyncWrite + Unpin,
{
    if payload.is_empty() || payload.len() > MAX_PAIRING_PAYLOAD {
        return Err(PairingError::Protocol(format!(
            "pairing payload length {} is outside 1..={MAX_PAIRING_PAYLOAD}",
            payload.len()
        )));
    }
    let payload_length = u32::try_from(payload.len())
        .map_err(|_| PairingError::Protocol("pairing payload length overflow".to_owned()))?;
    let mut header = [0_u8; PAIRING_HEADER_SIZE];
    header[0] = PAIRING_VERSION;
    header[1] = packet_type as u8;
    header[2..].copy_from_slice(&payload_length.to_be_bytes());
    writer
        .write_all(&header)
        .await
        .map_err(PairingError::Connection)?;
    writer
        .write_all(payload)
        .await
        .map_err(PairingError::Connection)
}

pub(crate) async fn read_packet<R>(
    reader: &mut R,
    expected_type: PacketType,
) -> Result<Vec<u8>, PairingError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0_u8; PAIRING_HEADER_SIZE];
    reader
        .read_exact(&mut header)
        .await
        .map_err(PairingError::Connection)?;
    if header[0] != PAIRING_VERSION {
        return Err(PairingError::Protocol(format!(
            "unsupported pairing packet version {}",
            header[0]
        )));
    }
    let packet_type = PacketType::try_from(header[1])?;
    if packet_type != expected_type {
        return Err(PairingError::Protocol(format!(
            "expected pairing packet {expected_type:?}, received {packet_type:?}"
        )));
    }
    let payload_length = u32::from_be_bytes(
        header[2..]
            .try_into()
            .map_err(|_| PairingError::Protocol("invalid pairing header".to_owned()))?,
    );
    let payload_length = usize::try_from(payload_length)
        .map_err(|_| PairingError::Protocol("pairing payload length overflow".to_owned()))?;
    if payload_length == 0 || payload_length > MAX_PAIRING_PAYLOAD {
        return Err(PairingError::Protocol(format!(
            "pairing payload length {payload_length} is outside 1..={MAX_PAIRING_PAYLOAD}"
        )));
    }
    let mut payload = vec![0_u8; payload_length];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(PairingError::Connection)?;
    Ok(payload)
}

#[derive(Clone)]
pub(crate) struct PeerInfo([u8; PEER_INFO_SIZE]);

impl PeerInfo {
    pub(crate) fn adb_public_key(public_key: &[u8]) -> Result<Self, PairingError> {
        if public_key.is_empty() || public_key.len() > PEER_INFO_SIZE - 1 {
            return Err(PairingError::Protocol(
                "ADB public key does not fit in PeerInfo".to_owned(),
            ));
        }
        let mut bytes = [0_u8; PEER_INFO_SIZE];
        bytes[0] = 0;
        bytes[1..=public_key.len()].copy_from_slice(public_key);
        Ok(Self(bytes))
    }

    pub(crate) fn device_guid(bytes: &[u8]) -> Result<String, PairingError> {
        if bytes.len() != PEER_INFO_SIZE || bytes[0] != 1 {
            return Err(PairingError::InvalidDeviceId);
        }
        let data = &bytes[1..];
        let terminator = data
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(PairingError::InvalidDeviceId)?;
        let guid =
            std::str::from_utf8(&data[..terminator]).map_err(|_| PairingError::InvalidDeviceId)?;
        if guid.is_empty() || guid.len() > 255 || guid.chars().any(char::is_control) {
            return Err(PairingError::InvalidDeviceId);
        }
        Ok(guid.to_owned())
    }

    pub(crate) fn as_bytes(&self) -> &[u8; PEER_INFO_SIZE] {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn device(device_id: &str) -> Self {
        let mut bytes = [0_u8; PEER_INFO_SIZE];
        bytes[0] = 1;
        bytes[1..=device_id.len()].copy_from_slice(device_id.as_bytes());
        Self(bytes)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::duplex;

    use super::*;

    #[tokio::test]
    async fn pairing_header_uses_network_byte_order() {
        let (mut client, mut server) = duplex(64);
        let writer = tokio::spawn(async move {
            write_packet(&mut client, PacketType::Spake2, &[1, 2, 3])
                .await
                .expect("packet should encode");
        });
        let mut bytes = [0_u8; 9];
        server
            .read_exact(&mut bytes)
            .await
            .expect("packet should arrive");
        writer.await.expect("writer should finish");
        assert_eq!(bytes, [1, 0, 0, 0, 0, 3, 1, 2, 3]);
    }

    #[test]
    fn peer_info_requires_a_terminated_device_guid() {
        let valid = PeerInfo::device("adb-device-guid");
        assert_eq!(
            PeerInfo::device_guid(valid.as_bytes()).expect("GUID should decode"),
            "adb-device-guid"
        );

        let mut invalid = [b'x'; PEER_INFO_SIZE];
        invalid[0] = 1;
        assert!(matches!(
            PeerInfo::device_guid(&invalid),
            Err(PairingError::InvalidDeviceId)
        ));
    }
}
