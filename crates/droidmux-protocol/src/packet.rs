use bytes::{Bytes, BytesMut};

use crate::{ADB_HEADER_LEN, AdbCommand, AdbHeader, AdbProtocolError, checksum};

/// A validated ADB command packet and its immutable payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdbPacket {
    /// Packet command.
    pub command: AdbCommand,
    /// First command-specific argument.
    pub arg0: u32,
    /// Second command-specific argument.
    pub arg1: u32,
    /// Packet payload.
    pub payload: Bytes,
}

impl AdbPacket {
    /// Creates a packet after enforcing the payload size limit.
    ///
    /// # Errors
    ///
    /// Returns [`AdbProtocolError::PayloadTooLarge`] when the supplied payload
    /// exceeds [`crate::MAX_PAYLOAD`].
    pub fn new(
        command: AdbCommand,
        arg0: u32,
        arg1: u32,
        payload: Bytes,
    ) -> Result<Self, AdbProtocolError> {
        AdbHeader::new(command, arg0, arg1, payload.len(), checksum(&payload))?;

        Ok(Self {
            command,
            arg0,
            arg1,
            payload,
        })
    }

    /// Builds the validated header for this packet's current payload.
    ///
    /// # Errors
    ///
    /// Returns [`AdbProtocolError::PayloadTooLarge`] if the public payload field
    /// was replaced with data larger than [`crate::MAX_PAYLOAD`].
    pub fn header(&self) -> Result<AdbHeader, AdbProtocolError> {
        AdbHeader::new(
            self.command,
            self.arg0,
            self.arg1,
            self.payload.len(),
            checksum(&self.payload),
        )
    }

    /// Encodes this packet as a complete ADB wire frame.
    ///
    /// # Errors
    ///
    /// Returns [`AdbProtocolError::PayloadTooLarge`] if the packet payload
    /// exceeds [`crate::MAX_PAYLOAD`].
    pub fn encode(&self) -> Result<Bytes, AdbProtocolError> {
        let header = self.header()?.encode();
        let mut output = BytesMut::with_capacity(ADB_HEADER_LEN + self.payload.len());
        output.extend_from_slice(&header);
        output.extend_from_slice(&self.payload);
        Ok(output.freeze())
    }

    /// Decodes the first complete ADB frame in `input`.
    ///
    /// The returned byte count allows callers to retain any following frame in
    /// their receive buffer. Payload bytes are copied only after all length and
    /// integrity checks succeed.
    ///
    /// # Errors
    ///
    /// Returns a structured error for truncated input, invalid headers,
    /// oversized payloads, and checksum mismatches.
    pub fn decode(input: &[u8]) -> Result<(Self, usize), AdbProtocolError> {
        let header = AdbHeader::decode(input)?;
        let payload_length = header.payload_length_usize()?;
        let available = input.len().saturating_sub(ADB_HEADER_LEN);

        if available < payload_length {
            return Err(AdbProtocolError::IncompletePayload {
                expected: payload_length,
                actual: available,
            });
        }

        let consumed = ADB_HEADER_LEN + payload_length;
        let payload_slice =
            input
                .get(ADB_HEADER_LEN..consumed)
                .ok_or(AdbProtocolError::IncompletePayload {
                    expected: payload_length,
                    actual: available,
                })?;
        let actual_checksum = checksum(payload_slice);

        // ADB 1.0.1 and newer peers may omit the legacy payload checksum by
        // writing zero in the header.
        if header.payload_checksum != 0 && actual_checksum != header.payload_checksum {
            return Err(AdbProtocolError::ChecksumMismatch {
                expected: header.payload_checksum,
                actual: actual_checksum,
            });
        }

        Ok((
            Self {
                command: header.command,
                arg0: header.arg0,
                arg1: header.arg1,
                payload: Bytes::copy_from_slice(payload_slice),
            },
            consumed,
        ))
    }
}
