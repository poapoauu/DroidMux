use crate::AdbProtocolError;

/// Commands that may appear in an ADB packet header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum AdbCommand {
    /// Synchronizes a transport state (`SYNC`).
    Sync = 0x434e_5953,
    /// Starts an ADB connection handshake (`CNXN`).
    Connect = 0x4e58_4e43,
    /// Carries an authentication message (`AUTH`).
    Auth = 0x4854_5541,
    /// Opens a logical stream (`OPEN`).
    Open = 0x4e45_504f,
    /// Acknowledges a logical stream write (`OKAY`).
    Okay = 0x5941_4b4f,
    /// Carries logical stream data (`WRTE`).
    Write = 0x4554_5257,
    /// Closes a logical stream (`CLSE`).
    Close = 0x4553_4c43,
    /// Requests a TLS upgrade (`STLS`).
    StartTls = 0x534c_5453,
}

impl AdbCommand {
    /// Returns the little-endian word written to the packet header.
    #[must_use]
    pub const fn wire_value(self) -> u32 {
        self as u32
    }

    /// Returns the bitwise-inverted command value used as the header magic.
    #[must_use]
    pub const fn magic(self) -> u32 {
        self.wire_value() ^ u32::MAX
    }
}

impl TryFrom<u32> for AdbCommand {
    type Error = AdbProtocolError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            value if value == Self::Sync.wire_value() => Ok(Self::Sync),
            value if value == Self::Connect.wire_value() => Ok(Self::Connect),
            value if value == Self::Auth.wire_value() => Ok(Self::Auth),
            value if value == Self::Open.wire_value() => Ok(Self::Open),
            value if value == Self::Okay.wire_value() => Ok(Self::Okay),
            value if value == Self::Write.wire_value() => Ok(Self::Write),
            value if value == Self::Close.wire_value() => Ok(Self::Close),
            value if value == Self::StartTls.wire_value() => Ok(Self::StartTls),
            unknown => Err(AdbProtocolError::UnknownCommand(unknown)),
        }
    }
}
