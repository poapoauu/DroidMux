use bytes::{Buf, Bytes, BytesMut};

use crate::MirrorError;

/// Video codec requested from and announced by the embedded scrcpy server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum VideoCodec {
    /// H.264/AVC, supported by every `DroidMux` decoder backend.
    #[default]
    H264,
    /// H.265/HEVC, decoded through an HEVC-capable backend such as `FFmpeg`.
    H265,
    /// AV1, decoded through an AV1-capable backend such as `FFmpeg`.
    Av1,
}

impl VideoCodec {
    pub(crate) const fn scrcpy_name(self) -> &'static str {
        match self {
            Self::H264 => "h264",
            Self::H265 => "h265",
            Self::Av1 => "av1",
        }
    }

    pub(crate) const fn codec_id(self) -> u32 {
        match self {
            Self::H264 => 0x6832_3634,
            Self::H265 => 0x6832_3635,
            Self::Av1 => 0x0061_7631,
        }
    }

    pub(crate) const fn from_codec_id(codec_id: u32) -> Option<Self> {
        if codec_id == Self::H264.codec_id() {
            Some(Self::H264)
        } else if codec_id == Self::H265.codec_id() {
            Some(Self::H265)
        } else if codec_id == Self::Av1.codec_id() {
            Some(Self::Av1)
        } else {
            None
        }
    }
}
pub(crate) const DEVICE_NAME_LENGTH: usize = 64;
pub(crate) const VIDEO_HEADER_LENGTH: usize = 12;
pub(crate) const MAX_VIDEO_PACKET_SIZE: usize = 16 * 1024 * 1024;
const PACKET_FLAG_SESSION: u64 = 1 << 63;
const PACKET_FLAG_CONFIG: u64 = 1 << 62;
const PACKET_FLAG_KEY_FRAME: u64 = 1 << 61;
const PACKET_PTS_MASK: u64 = PACKET_FLAG_KEY_FRAME - 1;
const POINTER_ID_GENERIC_FINGER: u64 = u64::MAX - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TouchAction {
    Down = 0,
    Up = 1,
    Move = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VideoPacket {
    Session {
        width: u32,
        height: u32,
    },
    Config(Bytes),
    Frame {
        data: Bytes,
        presentation_time_us: u64,
        key_frame: bool,
    },
}

pub(crate) fn parse_device_name(bytes: &[u8]) -> Result<String, MirrorError> {
    if bytes.len() != DEVICE_NAME_LENGTH {
        return Err(MirrorError::Protocol(format!(
            "device name field must contain {DEVICE_NAME_LENGTH} bytes"
        )));
    }
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end])
        .map(str::to_owned)
        .map_err(|_| MirrorError::Protocol("device name is not valid UTF-8".to_owned()))
}

pub(crate) fn parse_video_header(
    header: &[u8],
    payload: Option<Bytes>,
) -> Result<VideoPacket, MirrorError> {
    if header.len() != VIDEO_HEADER_LENGTH {
        return Err(MirrorError::Protocol(format!(
            "video header must contain {VIDEO_HEADER_LENGTH} bytes"
        )));
    }
    let mut fields = header;
    let pts_and_flags = fields.get_u64();
    let value = fields.get_u32();

    if pts_and_flags & PACKET_FLAG_SESSION != 0 {
        let width = u32::try_from(pts_and_flags & u64::from(u32::MAX))
            .expect("the value was masked to 32 bits");
        if width == 0 || value == 0 || width > u32::from(u16::MAX) || value > u32::from(u16::MAX) {
            return Err(MirrorError::Protocol(format!(
                "invalid video dimensions {width}x{value}"
            )));
        }
        return Ok(VideoPacket::Session {
            width,
            height: value,
        });
    }

    let packet_size = usize::try_from(value).expect("u32 fits every supported platform");
    if packet_size > MAX_VIDEO_PACKET_SIZE {
        return Err(MirrorError::Protocol(format!(
            "video packet size {packet_size} exceeds {MAX_VIDEO_PACKET_SIZE} bytes"
        )));
    }
    let payload = payload.ok_or_else(|| {
        MirrorError::Protocol("video frame header is missing its payload".to_owned())
    })?;
    if payload.len() != packet_size {
        return Err(MirrorError::Protocol(format!(
            "video packet declared {packet_size} bytes but received {}",
            payload.len()
        )));
    }
    if pts_and_flags & PACKET_FLAG_CONFIG != 0 {
        Ok(VideoPacket::Config(payload))
    } else {
        Ok(VideoPacket::Frame {
            data: payload,
            presentation_time_us: pts_and_flags & PACKET_PTS_MASK,
            key_frame: pts_and_flags & PACKET_FLAG_KEY_FRAME != 0,
        })
    }
}

pub(crate) fn video_payload_length(header: &[u8]) -> Result<Option<usize>, MirrorError> {
    if header.len() != VIDEO_HEADER_LENGTH {
        return Err(MirrorError::Protocol("incomplete video header".to_owned()));
    }
    let pts_and_flags = u64::from_be_bytes(
        header[..8]
            .try_into()
            .expect("eight header bytes were validated"),
    );
    if pts_and_flags & PACKET_FLAG_SESSION != 0 {
        return Ok(None);
    }
    let length = usize::try_from(u32::from_be_bytes(
        header[8..]
            .try_into()
            .expect("four header bytes were validated"),
    ))
    .expect("u32 fits every supported platform");
    if length > MAX_VIDEO_PACKET_SIZE {
        return Err(MirrorError::Protocol(format!(
            "video packet size {length} exceeds {MAX_VIDEO_PACKET_SIZE} bytes"
        )));
    }
    Ok(Some(length))
}

pub(crate) fn encode_touch(
    action: TouchAction,
    x: u32,
    y: u32,
    width: u32,
    height: u32,
) -> Result<Bytes, MirrorError> {
    if width == 0 || height == 0 || width > u32::from(u16::MAX) || height > u32::from(u16::MAX) {
        return Err(MirrorError::InvalidInput(format!(
            "screen dimensions {width}x{height} are outside the supported range"
        )));
    }
    if x >= width || y >= height {
        return Err(MirrorError::InvalidInput(format!(
            "point ({x}, {y}) is outside {width}x{height}"
        )));
    }

    let mut bytes = BytesMut::with_capacity(32);
    bytes.extend_from_slice(&[2, action as u8]);
    bytes.extend_from_slice(&POINTER_ID_GENERIC_FINGER.to_be_bytes());
    bytes.extend_from_slice(&x.to_be_bytes());
    bytes.extend_from_slice(&y.to_be_bytes());
    let width = u16::try_from(width)
        .map_err(|_| MirrorError::InvalidInput("screen width exceeds u16".to_owned()))?;
    let height = u16::try_from(height)
        .map_err(|_| MirrorError::InvalidInput("screen height exceeds u16".to_owned()))?;
    bytes.extend_from_slice(&width.to_be_bytes());
    bytes.extend_from_slice(&height.to_be_bytes());
    let pressure = if action == TouchAction::Up {
        0
    } else {
        u16::MAX
    };
    bytes.extend_from_slice(&pressure.to_be_bytes());
    bytes.extend_from_slice(&0_u32.to_be_bytes());
    bytes.extend_from_slice(&0_u32.to_be_bytes());
    Ok(bytes.freeze())
}

pub(crate) fn encode_key(action: u8, keycode: u32) -> Bytes {
    let mut bytes = BytesMut::with_capacity(14);
    bytes.extend_from_slice(&[0, action]);
    bytes.extend_from_slice(&keycode.to_be_bytes());
    bytes.extend_from_slice(&0_u32.to_be_bytes());
    bytes.extend_from_slice(&0_u32.to_be_bytes());
    bytes.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_touch_message_in_scrcpy_wire_format() {
        let encoded = encode_touch(TouchAction::Down, 120, 240, 1080, 1920)
            .expect("valid touch input should encode");
        assert_eq!(encoded.len(), 32);
        assert_eq!(&encoded[..2], &[2, 0]);
        assert_eq!(&encoded[2..10], &(u64::MAX - 1).to_be_bytes());
        assert_eq!(&encoded[10..14], &120_u32.to_be_bytes());
        assert_eq!(&encoded[14..18], &240_u32.to_be_bytes());
        assert_eq!(&encoded[18..20], &1080_u16.to_be_bytes());
        assert_eq!(&encoded[20..22], &1920_u16.to_be_bytes());
        assert_eq!(&encoded[22..24], &u16::MAX.to_be_bytes());
    }

    #[test]
    fn rejects_touch_outside_video_bounds() {
        let error = encode_touch(TouchAction::Move, 1080, 10, 1080, 1920)
            .expect_err("out-of-bounds touch must fail");
        assert!(error.to_string().contains("outside"));
    }

    #[test]
    fn parses_session_and_frame_headers() {
        let mut session = [0_u8; VIDEO_HEADER_LENGTH];
        session[..4].copy_from_slice(&0x8000_0000_u32.to_be_bytes());
        session[4..8].copy_from_slice(&1080_u32.to_be_bytes());
        session[8..].copy_from_slice(&1920_u32.to_be_bytes());
        assert_eq!(
            parse_video_header(&session, None).expect("session header should parse"),
            VideoPacket::Session {
                width: 1080,
                height: 1920
            }
        );

        let mut frame = [0_u8; VIDEO_HEADER_LENGTH];
        frame[..8].copy_from_slice(&(PACKET_FLAG_KEY_FRAME | 0x2a).to_be_bytes());
        frame[8..].copy_from_slice(&3_u32.to_be_bytes());
        assert_eq!(
            parse_video_header(&frame, Some(Bytes::from_static(b"nal")))
                .expect("frame header should parse"),
            VideoPacket::Frame {
                data: Bytes::from_static(b"nal"),
                presentation_time_us: 42,
                key_frame: true,
            }
        );
    }

    #[test]
    fn encodes_key_message_in_scrcpy_wire_format() {
        let encoded = encode_key(0, 3);
        assert_eq!(encoded.len(), 14);
        assert_eq!(&encoded[..2], &[0, 0]);
        assert_eq!(&encoded[2..6], &3_u32.to_be_bytes());
    }

    #[test]
    fn recognizes_scrcpy_video_codec_ids() {
        for codec in [VideoCodec::H264, VideoCodec::H265, VideoCodec::Av1] {
            assert_eq!(VideoCodec::from_codec_id(codec.codec_id()), Some(codec));
        }
        assert_eq!(VideoCodec::from_codec_id(u32::MAX), None);
    }
}
