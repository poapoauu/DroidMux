use crate::{ADB_HEADER_LEN, AdbCommand, AdbProtocolError, MAX_PAYLOAD};

/// Fixed-size metadata that precedes every ADB packet payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdbHeader {
    /// Packet command.
    pub command: AdbCommand,
    /// First command-specific argument.
    pub arg0: u32,
    /// Second command-specific argument.
    pub arg1: u32,
    /// Payload size encoded on the wire.
    pub payload_length: u32,
    /// Additive checksum of the payload, or zero when ADB 1.0.1+ omits it.
    pub payload_checksum: u32,
}

impl AdbHeader {
    /// Creates a validated header for a payload.
    ///
    /// # Errors
    ///
    /// Returns [`AdbProtocolError::PayloadTooLarge`] when `payload_length`
    /// exceeds [`MAX_PAYLOAD`].
    pub fn new(
        command: AdbCommand,
        arg0: u32,
        arg1: u32,
        payload_length: usize,
        payload_checksum: u32,
    ) -> Result<Self, AdbProtocolError> {
        validate_payload_length(payload_length)?;
        let payload_length =
            u32::try_from(payload_length).map_err(|_| AdbProtocolError::PayloadTooLarge {
                length: payload_length,
                max: MAX_PAYLOAD,
            })?;

        Ok(Self {
            command,
            arg0,
            arg1,
            payload_length,
            payload_checksum,
        })
    }

    /// Decodes and validates one fixed-size ADB header.
    ///
    /// Bytes after the first [`ADB_HEADER_LEN`] bytes are ignored.
    ///
    /// # Errors
    ///
    /// Returns an error when the header is truncated, contains an unknown
    /// command, has invalid magic, or declares an oversized payload.
    pub fn decode(input: &[u8]) -> Result<Self, AdbProtocolError> {
        if input.len() < ADB_HEADER_LEN {
            return Err(AdbProtocolError::IncompleteHeader {
                expected: ADB_HEADER_LEN,
                actual: input.len(),
            });
        }

        let command = AdbCommand::try_from(read_word(input, 0).ok_or(
            AdbProtocolError::IncompleteHeader {
                expected: ADB_HEADER_LEN,
                actual: input.len(),
            },
        )?)?;
        let arg0 = read_header_word(input, 4)?;
        let arg1 = read_header_word(input, 8)?;
        let payload_length = read_header_word(input, 12)?;
        let payload_checksum = read_header_word(input, 16)?;
        let magic = read_header_word(input, 20)?;
        let expected_magic = command.magic();

        if magic != expected_magic {
            return Err(AdbProtocolError::InvalidMagic {
                expected: expected_magic,
                actual: magic,
            });
        }

        let platform_length = usize::try_from(payload_length).map_err(|_| {
            AdbProtocolError::PayloadLengthOutOfRange {
                length: payload_length,
            }
        })?;
        validate_payload_length(platform_length)?;

        Ok(Self {
            command,
            arg0,
            arg1,
            payload_length,
            payload_checksum,
        })
    }

    /// Encodes this header as its 24-byte little-endian wire representation.
    #[must_use]
    pub fn encode(self) -> [u8; ADB_HEADER_LEN] {
        let mut output = [0_u8; ADB_HEADER_LEN];
        write_word(&mut output, 0, self.command.wire_value());
        write_word(&mut output, 4, self.arg0);
        write_word(&mut output, 8, self.arg1);
        write_word(&mut output, 12, self.payload_length);
        write_word(&mut output, 16, self.payload_checksum);
        write_word(&mut output, 20, self.command.magic());
        output
    }

    pub(crate) fn payload_length_usize(self) -> Result<usize, AdbProtocolError> {
        usize::try_from(self.payload_length).map_err(|_| {
            AdbProtocolError::PayloadLengthOutOfRange {
                length: self.payload_length,
            }
        })
    }
}

fn validate_payload_length(payload_length: usize) -> Result<(), AdbProtocolError> {
    if payload_length > MAX_PAYLOAD {
        return Err(AdbProtocolError::PayloadTooLarge {
            length: payload_length,
            max: MAX_PAYLOAD,
        });
    }

    Ok(())
}

fn read_header_word(input: &[u8], offset: usize) -> Result<u32, AdbProtocolError> {
    read_word(input, offset).ok_or(AdbProtocolError::IncompleteHeader {
        expected: ADB_HEADER_LEN,
        actual: input.len(),
    })
}

fn read_word(input: &[u8], offset: usize) -> Option<u32> {
    let bytes: [u8; 4] = input.get(offset..offset.checked_add(4)?)?.try_into().ok()?;
    Some(u32::from_le_bytes(bytes))
}

fn write_word(output: &mut [u8; ADB_HEADER_LEN], offset: usize, value: u32) {
    if let Some(destination) = output.get_mut(offset..offset.saturating_add(4)) {
        destination.copy_from_slice(&value.to_le_bytes());
    }
}
