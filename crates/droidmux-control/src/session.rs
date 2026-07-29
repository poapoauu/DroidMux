use std::{
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};

use adb_client::{AdbClient, AdbStream};
use adb_shell::{ShellOptions, ShellSession, open_shell};
use adb_sync::{TransferOptions, push_bytes_with_options};
use bytes::{Buf, Bytes, BytesMut};
use image::{ExtendedColorType, codecs::jpeg::JpegEncoder};
use openh264::{decoder::Decoder, formats::YUVSource};
use tokio::{sync::Mutex as AsyncMutex, task::spawn_blocking, time::sleep};

use crate::{
    MirrorError,
    protocol::{
        DEVICE_NAME_LENGTH, H264_CODEC_ID, MAX_VIDEO_PACKET_SIZE, TouchAction, VIDEO_HEADER_LENGTH,
        VideoPacket, encode_key, encode_touch, parse_device_name, parse_video_header,
        video_payload_length,
    },
};

const SCRCPY_VERSION: &str = "4.1";
const DEVICE_SERVER_PATH: &str = "/data/local/tmp/droidmux-scrcpy-server-v4.1.jar";
const SERVER_BYTES: &[u8] = include_bytes!("../resources/scrcpy-server-v4.1");
const SOCKET_ATTEMPTS: usize = 60;
const SOCKET_RETRY_DELAY: Duration = Duration::from_millis(50);
static NEXT_SCID: AtomicU32 = AtomicU32::new(0x0d10_0001);

/// Android key codes exposed by the first remote-control milestone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AndroidKey {
    /// Android Back.
    Back = 4,
    /// Android Home.
    Home = 3,
    /// Android recent-apps switcher.
    AppSwitch = 187,
    /// Android power button.
    Power = 26,
    /// Increase media volume.
    VolumeUp = 24,
    /// Decrease media volume.
    VolumeDown = 25,
}

/// Resource and encoder settings for one mirror session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MirrorOptions {
    /// Android logical display identifier to capture and control.
    pub display_id: u32,
    /// Maximum encoded frame dimension. The other dimension preserves aspect ratio.
    pub max_size: u16,
    /// Maximum frames requested from the Android encoder each second.
    pub max_fps: u8,
    /// Requested H.264 bit rate.
    pub video_bit_rate: u32,
    /// JPEG quality used for the Dioxus preview bridge.
    pub preview_quality: u8,
}

impl Default for MirrorOptions {
    fn default() -> Self {
        Self {
            display_id: 0,
            max_size: 1280,
            max_fps: 15,
            video_bit_rate: 4_000_000,
            preview_quality: 78,
        }
    }
}

impl MirrorOptions {
    fn validate(self) -> Result<Self, MirrorError> {
        if self.display_id > i32::MAX as u32 {
            return Err(MirrorError::InvalidInput(
                "display_id must be between 0 and 2147483647".to_owned(),
            ));
        }
        if !(256..=4096).contains(&self.max_size) {
            return Err(MirrorError::InvalidInput(
                "max_size must be between 256 and 4096".to_owned(),
            ));
        }
        if !(1..=120).contains(&self.max_fps) {
            return Err(MirrorError::InvalidInput(
                "max_fps must be between 1 and 120".to_owned(),
            ));
        }
        if !(100_000..=100_000_000).contains(&self.video_bit_rate) {
            return Err(MirrorError::InvalidInput(
                "video_bit_rate must be between 100000 and 100000000".to_owned(),
            ));
        }
        if !(1..=100).contains(&self.preview_quality) {
            return Err(MirrorError::InvalidInput(
                "preview_quality must be between 1 and 100".to_owned(),
            ));
        }
        Ok(self)
    }
}

/// One decoded frame ready to render in the desktop webview.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorFrame {
    /// JPEG-encoded RGB pixels.
    pub jpeg: Bytes,
    /// Decoded frame width.
    pub width: u32,
    /// Decoded frame height.
    pub height: u32,
    /// Device encoder presentation timestamp in microseconds.
    pub presentation_time_us: u64,
    /// Whether the source packet was an H.264 key frame.
    pub key_frame: bool,
}

/// A running embedded scrcpy-server video and control session.
pub struct MirrorSession {
    device_name: String,
    video: AsyncMutex<VideoStreamReader>,
    video_stream: AdbStream,
    control_stream: AdbStream,
    server: ShellSession,
    decoder: Arc<StdMutex<DecoderState>>,
    preview_quality: u8,
    touch_move_in_flight: AtomicBool,
    stopped: AtomicBool,
}

impl MirrorSession {
    /// Starts the embedded Android server and opens native ADB video/control streams.
    ///
    /// # Errors
    ///
    /// Returns an error when options are invalid, upload or launch fails, the
    /// local abstract sockets do not become ready, or the video header is invalid.
    pub async fn start(client: &AdbClient, options: MirrorOptions) -> Result<Self, MirrorError> {
        let options = options.validate()?;
        let decoder = Decoder::new().map_err(|error| MirrorError::Decode(error.to_string()))?;
        let transfer_options = TransferOptions {
            file_mode: 0o644,
            ..TransferOptions::default()
        };
        push_bytes_with_options(
            client,
            SERVER_BYTES,
            DEVICE_SERVER_PATH,
            &transfer_options,
            |_| {},
        )
        .await?;

        let scid = next_scid();
        let command = server_command(scid, options);
        let server = open_shell(client, &command, ShellOptions::default()).await?;
        let socket_name = format!("localabstract:scrcpy_{scid:08x}");

        let video_stream = match open_server_socket(client, &socket_name).await {
            Ok(stream) => stream,
            Err(error) => {
                let _ = server.cancel().await;
                return Err(error);
            }
        };
        let control_stream = match open_server_socket(client, &socket_name).await {
            Ok(stream) => stream,
            Err(error) => {
                let _ = video_stream.close().await;
                let _ = server.cancel().await;
                return Err(error);
            }
        };

        let video = match VideoStreamReader::open(video_stream.clone()).await {
            Ok(reader) => reader,
            Err(error) => {
                let _ = control_stream.close().await;
                let _ = video_stream.close().await;
                let _ = server.cancel().await;
                return Err(error);
            }
        };
        let device_name = video.device_name.clone();

        Ok(Self {
            device_name,
            video: AsyncMutex::new(video),
            video_stream,
            control_stream,
            server,
            decoder: Arc::new(StdMutex::new(DecoderState {
                decoder,
                rgb: Vec::new(),
            })),
            preview_quality: options.preview_quality,
            touch_move_in_flight: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        })
    }

    /// Returns the UTF-8 device name reported by the Android server.
    #[must_use]
    pub fn device_name(&self) -> &str {
        &self.device_name
    }

    /// Waits for and decodes the next available video frame.
    ///
    /// Session metadata and codec-configuration packets are consumed internally.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream closes, framing is invalid, H.264
    /// decoding fails, or JPEG conversion fails.
    pub async fn next_frame(&self) -> Result<MirrorFrame, MirrorError> {
        self.next_preview_frame(Duration::ZERO).await
    }

    /// Waits for the next decoded frame that is due for presentation.
    ///
    /// Every H.264 packet is decoded to preserve codec state, but expensive RGB
    /// and JPEG conversion is skipped until `minimum_interval` has elapsed.
    /// Decode and image conversion run on Tokio's blocking pool so callers such
    /// as a desktop UI executor remain responsive.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream closes, the decode worker fails, or
    /// frame conversion cannot complete.
    pub async fn next_preview_frame(
        &self,
        minimum_interval: Duration,
    ) -> Result<MirrorFrame, MirrorError> {
        self.ensure_running()?;
        let mut video = self.video.lock().await;
        loop {
            let packet = video.next_packet().await?;
            match packet {
                VideoPacket::Session { width, height } => {
                    video.width = width;
                    video.height = height;
                }
                VideoPacket::Config(data) => {
                    decode_video_packet(
                        Arc::clone(&self.decoder),
                        data,
                        self.preview_quality,
                        false,
                    )
                    .await?;
                }
                VideoPacket::Frame {
                    data,
                    presentation_time_us,
                    key_frame,
                } => {
                    let now = Instant::now();
                    let encode_preview =
                        preview_is_due(video.last_preview_at, now, minimum_interval);
                    let Some(preview) = decode_video_packet(
                        Arc::clone(&self.decoder),
                        data,
                        self.preview_quality,
                        encode_preview,
                    )
                    .await?
                    else {
                        continue;
                    };
                    video.last_preview_at = Some(now);
                    return Ok(MirrorFrame {
                        jpeg: preview.jpeg,
                        width: preview.width,
                        height: preview.height,
                        presentation_time_us,
                        key_frame,
                    });
                }
            }
        }
    }

    /// Injects one finger event in encoded-video coordinates.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid coordinates or a closed control stream.
    pub async fn touch(
        &self,
        action: MirrorTouchAction,
        x: u32,
        y: u32,
        width: u32,
        height: u32,
    ) -> Result<(), MirrorError> {
        self.ensure_running()?;
        let reserved_move = action == MirrorTouchAction::Move;
        let action = match action {
            MirrorTouchAction::Down => TouchAction::Down,
            MirrorTouchAction::Move => TouchAction::Move,
            MirrorTouchAction::Up => TouchAction::Up,
        };
        let payload = encode_touch(action, x, y, width, height)?;
        if reserved_move && self.touch_move_in_flight.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let result = self.control_stream.write(payload).await;
        if reserved_move {
            self.touch_move_in_flight.store(false, Ordering::Release);
        }
        result.map_err(MirrorError::from)
    }

    /// Injects one complete Android key press.
    ///
    /// # Errors
    ///
    /// Returns an error when the mirror is stopped or the control stream fails.
    pub async fn press_key(&self, key: AndroidKey) -> Result<(), MirrorError> {
        self.ensure_running()?;
        let keycode = key as u32;
        self.control_stream.write(encode_key(0, keycode)).await?;
        self.control_stream.write(encode_key(1, keycode)).await?;
        Ok(())
    }

    /// Stops the video, control, and Android server streams.
    ///
    /// Repeated calls are harmless.
    ///
    /// # Errors
    ///
    /// Returns the first stream shutdown error.
    pub async fn stop(&self) -> Result<(), MirrorError> {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let video_result = self.video_stream.close().await.map_err(MirrorError::from);
        let control_result = self.control_stream.close().await.map_err(MirrorError::from);
        let server_result = self.server.cancel().await.map_err(MirrorError::from);
        video_result?;
        control_result?;
        server_result?;
        Ok(())
    }

    /// Returns whether stop has been requested.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    fn ensure_running(&self) -> Result<(), MirrorError> {
        if self.is_stopped() {
            Err(MirrorError::Stopped)
        } else {
            Ok(())
        }
    }
}

struct EncodedPreview {
    jpeg: Bytes,
    width: u32,
    height: u32,
}

struct DecoderState {
    decoder: Decoder,
    rgb: Vec<u8>,
}

async fn decode_video_packet(
    decoder: Arc<StdMutex<DecoderState>>,
    data: Bytes,
    preview_quality: u8,
    encode_preview: bool,
) -> Result<Option<EncodedPreview>, MirrorError> {
    spawn_blocking(move || {
        let mut state = decoder
            .lock()
            .map_err(|_| MirrorError::Decode("video decoder lock was poisoned".to_owned()))?;
        let DecoderState { decoder, rgb } = &mut *state;
        let Some(decoded_frame) = decoder
            .decode(&data)
            .map_err(|error| MirrorError::Decode(error.to_string()))?
        else {
            return Ok(None);
        };
        if !encode_preview {
            return Ok(None);
        }

        let (width, height) = decoded_frame.dimensions();
        let rgb_len = width
            .checked_mul(height)
            .and_then(|pixels| pixels.checked_mul(3))
            .ok_or_else(|| {
                MirrorError::Frame("decoded dimensions overflow memory size".to_owned())
            })?;
        if rgb_len > MAX_VIDEO_PACKET_SIZE * 4 {
            return Err(MirrorError::Frame(format!(
                "decoded RGB frame requires {rgb_len} bytes"
            )));
        }
        rgb.resize(rgb_len, 0);
        decoded_frame.write_rgb8(rgb);
        let frame_width = u32::try_from(width)
            .map_err(|_| MirrorError::Frame("decoded width exceeds u32".to_owned()))?;
        let frame_height = u32::try_from(height)
            .map_err(|_| MirrorError::Frame("decoded height exceeds u32".to_owned()))?;
        let mut jpeg = Vec::with_capacity(rgb_len / 8);
        JpegEncoder::new_with_quality(&mut jpeg, preview_quality)
            .encode(rgb, frame_width, frame_height, ExtendedColorType::Rgb8)
            .map_err(|error| MirrorError::Frame(error.to_string()))?;
        Ok(Some(EncodedPreview {
            jpeg: Bytes::from(jpeg),
            width: frame_width,
            height: frame_height,
        }))
    })
    .await
    .map_err(|error| MirrorError::Decode(format!("video decode worker failed: {error}")))?
}

fn preview_is_due(
    last_preview_at: Option<Instant>,
    now: Instant,
    minimum_interval: Duration,
) -> bool {
    last_preview_at.is_none_or(|last| now.saturating_duration_since(last) >= minimum_interval)
}

/// Finger lifecycle action sent to Android.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorTouchAction {
    /// Begin a touch gesture.
    Down,
    /// Continue a touch gesture.
    Move,
    /// Finish a touch gesture.
    Up,
}

struct VideoStreamReader {
    stream: AdbStream,
    buffered: BytesMut,
    device_name: String,
    width: u32,
    height: u32,
    last_preview_at: Option<Instant>,
}

impl VideoStreamReader {
    async fn open(stream: AdbStream) -> Result<Self, MirrorError> {
        let mut reader = Self {
            stream,
            buffered: BytesMut::new(),
            device_name: String::new(),
            width: 0,
            height: 0,
            last_preview_at: None,
        };
        let name = reader.read_exact(DEVICE_NAME_LENGTH).await?;
        reader.device_name = parse_device_name(&name)?;
        let mut codec = reader.read_exact(4).await?;
        let codec_id = codec.get_u32();
        if codec_id != H264_CODEC_ID {
            return Err(MirrorError::Protocol(format!(
                "unsupported video codec id 0x{codec_id:08x}"
            )));
        }
        Ok(reader)
    }

    async fn next_packet(&mut self) -> Result<VideoPacket, MirrorError> {
        let header = self.read_exact(VIDEO_HEADER_LENGTH).await?;
        let payload = match video_payload_length(&header)? {
            Some(length) => Some(self.read_exact(length).await?),
            None => None,
        };
        parse_video_header(&header, payload)
    }

    async fn read_exact(&mut self, length: usize) -> Result<Bytes, MirrorError> {
        while self.buffered.len() < length {
            let Some(payload) = self.stream.read().await? else {
                return Err(MirrorError::Protocol(
                    "video stream closed before a complete field arrived".to_owned(),
                ));
            };
            self.buffered.extend_from_slice(&payload);
            if self.buffered.len() > MAX_VIDEO_PACKET_SIZE + self.stream.max_payload() {
                return Err(MirrorError::Protocol(
                    "video receive buffer exceeded its limit".to_owned(),
                ));
            }
        }
        Ok(self.buffered.split_to(length).freeze())
    }
}

fn next_scid() -> u32 {
    NEXT_SCID.fetch_add(1, Ordering::Relaxed) & 0x7fff_ffff
}

fn server_command(scid: u32, options: MirrorOptions) -> String {
    format!(
        "CLASSPATH={DEVICE_SERVER_PATH} app_process / com.genymobile.scrcpy.Server {SCRCPY_VERSION} scid={scid:08x} log_level=warn audio=false display_id={} max_size={} max_fps={} video_bit_rate={} tunnel_forward=true send_dummy_byte=false cleanup=true",
        options.display_id, options.max_size, options.max_fps, options.video_bit_rate
    )
}

async fn open_server_socket(client: &AdbClient, service: &str) -> Result<AdbStream, MirrorError> {
    let mut last_error = None;
    for _ in 0..SOCKET_ATTEMPTS {
        match client.open_service(service).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error.to_string()),
        }
        sleep(SOCKET_RETRY_DELAY).await;
    }
    Err(MirrorError::Startup(last_error.unwrap_or_else(|| {
        "Android local socket was not created".to_owned()
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_command_is_fixed_and_argument_safe() {
        let command = server_command(0x1234_abcd, MirrorOptions::default());
        assert!(command.contains("com.genymobile.scrcpy.Server 4.1"));
        assert!(command.contains("scid=1234abcd"));
        assert!(command.contains("audio=false"));
        assert!(command.contains("display_id=0"));
        assert!(command.contains("tunnel_forward=true"));
        assert!(!command.contains('\n'));
    }

    #[test]
    fn mirror_options_reject_unbounded_values() {
        let options = MirrorOptions {
            max_size: 0,
            ..MirrorOptions::default()
        };
        assert!(options.validate().is_err());

        let options = MirrorOptions {
            display_id: i32::MAX as u32 + 1,
            ..MirrorOptions::default()
        };
        assert!(options.validate().is_err());
    }

    #[test]
    fn server_command_selects_a_non_primary_display() {
        let options = MirrorOptions {
            display_id: 2,
            ..MirrorOptions::default()
        };
        let command = server_command(1, options);
        assert!(command.contains("display_id=2"));
    }

    #[test]
    fn preview_interval_skips_early_frames_and_accepts_the_boundary() {
        let last = Instant::now();
        let interval = Duration::from_millis(33);

        assert!(preview_is_due(None, last, interval));
        assert!(!preview_is_due(
            Some(last),
            last + Duration::from_millis(32),
            interval
        ));
        assert!(preview_is_due(Some(last), last + interval, interval));
    }
}
