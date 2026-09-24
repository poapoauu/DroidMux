use std::{
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use adb_client::AdbStream;
use bytes::{Buf, Bytes, BytesMut};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, oneshot, watch},
    task::JoinHandle,
    time::timeout,
};

use crate::{
    DecodedPixelFormat, DecodedVideoFrame, MirrorDecoder, MirrorError, MirrorPixelBuffer,
    VideoCodec, VideoFrameMetadata,
    protocol::{
        DEVICE_NAME_LENGTH, MAX_VIDEO_PACKET_SIZE, VIDEO_HEADER_LENGTH, VideoPacket,
        parse_device_name, parse_video_header, video_payload_length,
    },
};

const ENCODED_PACKET_QUEUE_CAPACITY: usize = 2;
const DECODER_STOP_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_PIXEL_FRAME_BYTES: usize = MAX_VIDEO_PACKET_SIZE * 4;

/// Cumulative counters for one mirror session's native video pipeline.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MirrorPipelineStats {
    /// Encoded video frame packets submitted to the decoder.
    pub encoded_frames: u64,
    /// CPU pixel frames produced by the decoder.
    pub decoded_frames: u64,
    /// Frames returned to a presentation consumer.
    pub presented_frames: u64,
    /// Decoded frames superseded or intentionally skipped before presentation.
    pub dropped_frames: u64,
    /// Total nanoseconds spent decoding and converting encoded frame packets.
    pub total_decode_time_ns: u64,
    /// Nanoseconds spent decoding and converting the most recent frame packet.
    pub last_decode_time_ns: u64,
}

pub(crate) struct VideoPipeline {
    backend_name: &'static str,
    frames: AsyncMutex<FrameReceiver>,
    reader_task: Mutex<Option<JoinHandle<()>>>,
    decoder_thread: Mutex<Option<thread::JoinHandle<()>>>,
    decoder_finished: Mutex<Option<oneshot::Receiver<()>>>,
    output_format: Arc<AtomicU8>,
    counters: Arc<PipelineCounters>,
}

impl VideoPipeline {
    pub(crate) fn start(
        reader: VideoStreamReader,
        decoder: Box<dyn MirrorDecoder>,
    ) -> Result<Self, MirrorError> {
        let backend_name = decoder.backend_name();
        let counters = Arc::new(PipelineCounters::default());
        let worker_counters = Arc::clone(&counters);
        let output_format = Arc::new(AtomicU8::new(NO_PIXEL_OUTPUT));
        let worker_output_format = Arc::clone(&output_format);
        let (packet_sender, packet_receiver) = mpsc::channel(ENCODED_PACKET_QUEUE_CAPACITY);
        let (frame_sender, frame_receiver) = watch::channel(PipelineEvent::Waiting);
        let (finished_sender, finished_receiver) = oneshot::channel();
        let decoder_thread = thread::Builder::new()
            .name(format!("droidmux-{backend_name}-decoder"))
            .spawn(move || {
                run_decoder_worker(
                    decoder,
                    packet_receiver,
                    &frame_sender,
                    &worker_counters,
                    &worker_output_format,
                );
                let _ = finished_sender.send(());
            })
            .map_err(|error| {
                MirrorError::Decode(format!("failed to start decoder worker: {error}"))
            })?;
        let reader_task = tokio::spawn(run_video_reader(reader, packet_sender));

        Ok(Self {
            backend_name,
            frames: AsyncMutex::new(FrameReceiver {
                receiver: frame_receiver,
                last_presented_at: None,
                last_sequence: 0,
                pending_terminal: None,
            }),
            reader_task: Mutex::new(Some(reader_task)),
            decoder_thread: Mutex::new(Some(decoder_thread)),
            decoder_finished: Mutex::new(Some(finished_receiver)),
            output_format,
            counters,
        })
    }

    pub(crate) fn backend_name(&self) -> &'static str {
        self.backend_name
    }

    pub(crate) fn stats(&self) -> MirrorPipelineStats {
        self.counters.snapshot()
    }

    pub(crate) async fn next_pixel_frame(
        &self,
        minimum_interval: Duration,
        format: DecodedPixelFormat,
    ) -> Result<MirrorPixelBuffer, MirrorError> {
        let mut frames = self.frames.lock().await;
        let _request = OutputRequest::new(&self.output_format, format);
        if let Some(terminal) = frames.pending_terminal.take() {
            return Err(terminal.into_error());
        }
        loop {
            frames
                .receiver
                .changed()
                .await
                .map_err(|_| MirrorError::Stopped)?;
            let event = frames.receiver.borrow_and_update().clone();
            match event {
                PipelineEvent::Waiting => {}
                PipelineEvent::Frame {
                    frame,
                    sequence,
                    terminal,
                } => {
                    if frame.format != format {
                        continue;
                    }
                    let now = Instant::now();
                    if terminal.is_none()
                        && !preview_is_due(frames.last_presented_at, now, minimum_interval)
                    {
                        continue;
                    }
                    let dropped = sequence
                        .saturating_sub(frames.last_sequence)
                        .saturating_sub(1);
                    frames.last_presented_at = Some(now);
                    frames.last_sequence = sequence;
                    self.counters
                        .presented_frames
                        .fetch_add(1, Ordering::Relaxed);
                    self.counters
                        .dropped_frames
                        .fetch_add(dropped, Ordering::Relaxed);
                    frames.pending_terminal = terminal;
                    return Ok(frame);
                }
                PipelineEvent::Failed(error) => return Err(error.into_error()),
                PipelineEvent::Closed => return Err(MirrorError::Stopped),
            }
        }
    }

    pub(crate) async fn stop(&self) -> Result<(), MirrorError> {
        let reader_task = self
            .reader_task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(reader_task) = reader_task {
            reader_task.abort();
            let _ = reader_task.await;
        }

        let decoder_finished = self
            .decoder_finished
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let decoder_thread = self
            .decoder_thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(decoder_finished) = decoder_finished
            && timeout(DECODER_STOP_TIMEOUT, decoder_finished)
                .await
                .is_err()
        {
            drop(decoder_thread);
            return Err(MirrorError::Decode(format!(
                "decoder worker did not stop within {} seconds",
                DECODER_STOP_TIMEOUT.as_secs()
            )));
        }
        if let Some(decoder_thread) = decoder_thread {
            let joined = tokio::task::spawn_blocking(move || decoder_thread.join())
                .await
                .map_err(|error| {
                    MirrorError::Decode(format!("decoder join worker failed: {error}"))
                })?;
            joined.map_err(|_| MirrorError::Decode("decoder worker panicked".to_owned()))?;
        }
        Ok(())
    }
}

impl Drop for VideoPipeline {
    fn drop(&mut self) {
        let reader_task = self
            .reader_task
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(reader_task) = reader_task {
            reader_task.abort();
        }
    }
}

#[derive(Default)]
struct PipelineCounters {
    encoded_frames: AtomicU64,
    decoded_frames: AtomicU64,
    presented_frames: AtomicU64,
    dropped_frames: AtomicU64,
    total_decode_time_ns: AtomicU64,
    last_decode_time_ns: AtomicU64,
}

impl PipelineCounters {
    fn snapshot(&self) -> MirrorPipelineStats {
        MirrorPipelineStats {
            encoded_frames: self.encoded_frames.load(Ordering::Relaxed),
            decoded_frames: self.decoded_frames.load(Ordering::Relaxed),
            presented_frames: self.presented_frames.load(Ordering::Relaxed),
            dropped_frames: self.dropped_frames.load(Ordering::Relaxed),
            total_decode_time_ns: self.total_decode_time_ns.load(Ordering::Relaxed),
            last_decode_time_ns: self.last_decode_time_ns.load(Ordering::Relaxed),
        }
    }
}

struct FrameReceiver {
    receiver: watch::Receiver<PipelineEvent>,
    last_presented_at: Option<Instant>,
    last_sequence: u64,
    pending_terminal: Option<PipelineFailure>,
}

#[derive(Clone)]
enum PipelineEvent {
    Waiting,
    Frame {
        frame: MirrorPixelBuffer,
        sequence: u64,
        terminal: Option<PipelineFailure>,
    },
    Failed(PipelineFailure),
    Closed,
}

#[derive(Clone)]
enum PipelineFailure {
    Stream(adb_client::AdbStreamError),
    Protocol(String),
    Decode(String),
    Frame(String),
    Stopped,
}

impl PipelineFailure {
    fn from_error(error: MirrorError) -> Self {
        match error {
            MirrorError::Stream(error) => Self::Stream(error),
            MirrorError::Protocol(error) => Self::Protocol(error),
            MirrorError::Decode(error) => Self::Decode(error),
            MirrorError::Frame(error) => Self::Frame(error),
            MirrorError::Stopped => Self::Stopped,
            error => Self::Decode(error.to_string()),
        }
    }

    fn into_error(self) -> MirrorError {
        match self {
            Self::Stream(error) => MirrorError::Stream(error),
            Self::Protocol(error) => MirrorError::Protocol(error),
            Self::Decode(error) => MirrorError::Decode(error),
            Self::Frame(error) => MirrorError::Frame(error),
            Self::Stopped => MirrorError::Stopped,
        }
    }
}

const NO_PIXEL_OUTPUT: u8 = 0;
const RGB24_OUTPUT: u8 = 1;
const RGBA8888_OUTPUT: u8 = 2;
const BGRA8888_OUTPUT: u8 = 3;

fn encode_output_format(format: DecodedPixelFormat) -> u8 {
    match format {
        DecodedPixelFormat::Rgb24 => RGB24_OUTPUT,
        DecodedPixelFormat::Rgba8888 => RGBA8888_OUTPUT,
        DecodedPixelFormat::Bgra8888 => BGRA8888_OUTPUT,
    }
}

fn requested_output_format(output: &AtomicU8) -> Option<DecodedPixelFormat> {
    match output.load(Ordering::Acquire) {
        RGB24_OUTPUT => Some(DecodedPixelFormat::Rgb24),
        RGBA8888_OUTPUT => Some(DecodedPixelFormat::Rgba8888),
        BGRA8888_OUTPUT => Some(DecodedPixelFormat::Bgra8888),
        _ => None,
    }
}

struct OutputRequest<'a> {
    output: &'a AtomicU8,
}

impl<'a> OutputRequest<'a> {
    fn new(output: &'a AtomicU8, format: DecodedPixelFormat) -> Self {
        output.store(encode_output_format(format), Ordering::Release);
        Self { output }
    }
}

impl Drop for OutputRequest<'_> {
    fn drop(&mut self) {
        self.output.store(NO_PIXEL_OUTPUT, Ordering::Release);
    }
}

enum DecodeMessage {
    Config(Bytes),
    Frame {
        data: Bytes,
        presentation_time_us: u64,
        key_frame: bool,
        frame_size: Option<(u32, u32)>,
    },
    Failed(PipelineFailure),
}

async fn run_video_reader(mut reader: VideoStreamReader, packets: mpsc::Sender<DecodeMessage>) {
    let mut frame_size = None;
    loop {
        let packet = match reader.next_packet().await {
            Ok(packet) => packet,
            Err(error) => {
                let _ = packets
                    .send(DecodeMessage::Failed(PipelineFailure::from_error(error)))
                    .await;
                return;
            }
        };
        let message = match packet {
            VideoPacket::Session { width, height } => {
                frame_size = Some((width, height));
                continue;
            }
            VideoPacket::Config(data) => DecodeMessage::Config(data),
            VideoPacket::Frame {
                data,
                presentation_time_us,
                key_frame,
            } => DecodeMessage::Frame {
                data,
                presentation_time_us,
                key_frame,
                frame_size,
            },
        };
        if packets.send(message).await.is_err() {
            return;
        }
    }
}

fn run_decoder_worker(
    mut decoder: Box<dyn MirrorDecoder>,
    mut packets: mpsc::Receiver<DecodeMessage>,
    frames: &watch::Sender<PipelineEvent>,
    counters: &PipelineCounters,
    output_format: &AtomicU8,
) {
    let mut sequence = 0_u64;
    while let Some(packet) = packets.blocking_recv() {
        match packet {
            DecodeMessage::Config(data) => {
                let mut discard = |_frame: DecodedVideoFrame| {};
                if let Err(error) = decoder.decode(&data, None, None, &mut discard) {
                    drop(
                        frames.send_replace(PipelineEvent::Failed(PipelineFailure::from_error(
                            error,
                        ))),
                    );
                    return;
                }
            }
            DecodeMessage::Frame {
                data,
                presentation_time_us,
                key_frame,
                frame_size,
            } => {
                counters.encoded_frames.fetch_add(1, Ordering::Relaxed);
                let started = Instant::now();
                let metadata = VideoFrameMetadata {
                    presentation_time_us,
                    key_frame,
                    frame_size,
                };
                let output = requested_output_format(output_format);
                let backend_name = decoder.backend_name();
                let mut output_error = None;
                let mut emit = |decoded: DecodedVideoFrame| {
                    let Some(expected_format) = output else {
                        output_error = Some(MirrorError::Frame(format!(
                            "decoder {backend_name} emitted pixels when no output was requested"
                        )));
                        return;
                    };
                    if let Err(error) = publish_decoded_frame(
                        decoded,
                        expected_format,
                        frames,
                        counters,
                        &mut sequence,
                        None,
                    ) {
                        output_error = Some(error);
                    }
                };
                let result = decoder.decode(&data, Some(metadata), output, &mut emit);
                let elapsed_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                counters
                    .total_decode_time_ns
                    .fetch_add(elapsed_ns, Ordering::Relaxed);
                counters
                    .last_decode_time_ns
                    .store(elapsed_ns, Ordering::Relaxed);
                if let Err(error) = result {
                    drop(
                        frames.send_replace(PipelineEvent::Failed(PipelineFailure::from_error(
                            error,
                        ))),
                    );
                    return;
                }
                if let Some(error) = output_error {
                    drop(
                        frames.send_replace(PipelineEvent::Failed(PipelineFailure::from_error(
                            error,
                        ))),
                    );
                    return;
                }
            }
            DecodeMessage::Failed(error) => {
                finish_decoder(
                    decoder.as_mut(),
                    frames,
                    counters,
                    output_format,
                    &mut sequence,
                    error,
                );
                return;
            }
        }
    }

    finish_decoder(
        decoder.as_mut(),
        frames,
        counters,
        output_format,
        &mut sequence,
        PipelineFailure::Stopped,
    );
}

fn publish_decoded_frame(
    decoded: DecodedVideoFrame,
    expected_format: DecodedPixelFormat,
    frames: &watch::Sender<PipelineEvent>,
    counters: &PipelineCounters,
    sequence: &mut u64,
    terminal: Option<PipelineFailure>,
) -> Result<(), MirrorError> {
    let frame = validate_decoded_frame(decoded, expected_format)?;
    *sequence = sequence.saturating_add(1);
    counters.decoded_frames.fetch_add(1, Ordering::Relaxed);
    drop(frames.send_replace(PipelineEvent::Frame {
        frame,
        sequence: *sequence,
        terminal,
    }));
    Ok(())
}

fn validate_decoded_frame(
    decoded: DecodedVideoFrame,
    expected_format: DecodedPixelFormat,
) -> Result<MirrorPixelBuffer, MirrorError> {
    if decoded.format != expected_format {
        return Err(MirrorError::Frame(format!(
            "decoder returned {:?} pixels for a {expected_format:?} request",
            decoded.format
        )));
    }
    let width = usize::try_from(decoded.width)
        .map_err(|_| MirrorError::Frame("decoded width exceeds usize".to_owned()))?;
    let height = usize::try_from(decoded.height)
        .map_err(|_| MirrorError::Frame("decoded height exceeds usize".to_owned()))?;
    if width == 0 || height == 0 {
        return Err(MirrorError::Frame(
            "decoder returned an empty pixel frame".to_owned(),
        ));
    }
    let expected_bytes = width
        .checked_mul(height)
        .and_then(|pixels| pixels.checked_mul(expected_format.bytes_per_pixel()))
        .ok_or_else(|| MirrorError::Frame("decoded dimensions overflow memory size".to_owned()))?;
    if expected_bytes > MAX_PIXEL_FRAME_BYTES {
        return Err(MirrorError::Frame(format!(
            "decoded frame requires {expected_bytes} bytes"
        )));
    }
    if decoded.pixels.len() != expected_bytes {
        return Err(MirrorError::Frame(format!(
            "decoded {expected_format:?} frame contains {} bytes, expected {expected_bytes}",
            decoded.pixels.len()
        )));
    }
    Ok(MirrorPixelBuffer {
        pixels: decoded.pixels,
        width: decoded.width,
        height: decoded.height,
        format: decoded.format,
        presentation_time_us: decoded.metadata.presentation_time_us,
        key_frame: decoded.metadata.key_frame,
    })
}

fn finish_decoder(
    decoder: &mut dyn MirrorDecoder,
    frames: &watch::Sender<PipelineEvent>,
    counters: &PipelineCounters,
    output_format: &AtomicU8,
    sequence: &mut u64,
    terminal: PipelineFailure,
) {
    let output = requested_output_format(output_format);
    let mut latest = None;
    let mut output_error = None;
    let mut emit = |decoded: DecodedVideoFrame| {
        let Some(expected_format) = output else {
            return;
        };
        match validate_decoded_frame(decoded, expected_format) {
            Ok(frame) => {
                *sequence = sequence.saturating_add(1);
                counters.decoded_frames.fetch_add(1, Ordering::Relaxed);
                latest = Some((frame, *sequence));
            }
            Err(error) => output_error = Some(error),
        }
    };
    let flush_error = decoder.flush(&mut emit).err();
    let terminal = output_error
        .or(flush_error)
        .map_or(terminal, PipelineFailure::from_error);
    if let Some((frame, sequence)) = latest {
        drop(frames.send_replace(PipelineEvent::Frame {
            frame,
            sequence,
            terminal: Some(terminal),
        }));
    } else if matches!(terminal, PipelineFailure::Stopped) {
        drop(frames.send_replace(PipelineEvent::Closed));
    } else {
        drop(frames.send_replace(PipelineEvent::Failed(terminal)));
    }
}

pub(crate) struct VideoStreamReader {
    stream: AdbStream,
    buffered: BytesMut,
    device_name: String,
}

impl VideoStreamReader {
    pub(crate) async fn open(
        stream: AdbStream,
        expected_codec: VideoCodec,
    ) -> Result<Self, MirrorError> {
        let mut reader = Self {
            stream,
            buffered: BytesMut::new(),
            device_name: String::new(),
        };
        let name = reader.read_exact(DEVICE_NAME_LENGTH).await?;
        reader.device_name = parse_device_name(&name)?;
        let mut codec = reader.read_exact(4).await?;
        let codec_id = codec.get_u32();
        let video_codec = VideoCodec::from_codec_id(codec_id).ok_or_else(|| {
            MirrorError::Protocol(format!("unsupported video codec id 0x{codec_id:08x}"))
        })?;
        if video_codec != expected_codec {
            return Err(MirrorError::Protocol(format!(
                "video server returned codec {}, expected {}",
                video_codec.scrcpy_name(),
                expected_codec.scrcpy_name()
            )));
        }
        Ok(reader)
    }

    pub(crate) fn device_name(&self) -> &str {
        &self.device_name
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

fn preview_is_due(
    last_preview_at: Option<Instant>,
    now: Instant,
    minimum_interval: Duration,
) -> bool {
    last_preview_at.is_none_or(|last| now.saturating_duration_since(last) >= minimum_interval)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU8, Ordering},
        },
        thread,
        time::Duration,
    };

    use bytes::Bytes;
    use tokio::sync::{mpsc, watch};

    use super::{
        DecodeMessage, NO_PIXEL_OUTPUT, OutputRequest, PipelineCounters, PipelineEvent,
        PipelineFailure, RGBA8888_OUTPUT, preview_is_due, requested_output_format,
        run_decoder_worker, validate_decoded_frame,
    };
    use crate::{
        DecodedPixelFormat, DecodedVideoFrame, MirrorDecoder, MirrorError, VideoFrameMetadata,
    };

    struct FakeDecoder;

    impl MirrorDecoder for FakeDecoder {
        fn backend_name(&self) -> &'static str {
            "fake"
        }

        fn decode(
            &mut self,
            access_unit: &[u8],
            metadata: Option<VideoFrameMetadata>,
            output: Option<DecodedPixelFormat>,
            emit: &mut dyn FnMut(DecodedVideoFrame),
        ) -> Result<(), MirrorError> {
            if let Some(format) = output {
                emit(DecodedVideoFrame {
                    pixels: Bytes::copy_from_slice(access_unit),
                    width: 1,
                    height: 1,
                    format,
                    metadata: metadata.expect("pixel frames should have metadata"),
                });
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct DelayedDecoder {
        pending: Option<DecodedVideoFrame>,
    }

    impl MirrorDecoder for DelayedDecoder {
        fn backend_name(&self) -> &'static str {
            "delayed-fake"
        }

        fn decode(
            &mut self,
            access_unit: &[u8],
            metadata: Option<VideoFrameMetadata>,
            output: Option<DecodedPixelFormat>,
            emit: &mut dyn FnMut(DecodedVideoFrame),
        ) -> Result<(), MirrorError> {
            let (Some(metadata), Some(format)) = (metadata, output) else {
                return Ok(());
            };
            let next = DecodedVideoFrame {
                pixels: Bytes::copy_from_slice(access_unit),
                width: 1,
                height: 1,
                format,
                metadata,
            };
            if let Some(previous) = self.pending.replace(next) {
                emit(previous);
            }
            Ok(())
        }

        fn flush(&mut self, emit: &mut dyn FnMut(DecodedVideoFrame)) -> Result<(), MirrorError> {
            if let Some(pending) = self.pending.take() {
                emit(pending);
            }
            Ok(())
        }
    }

    #[test]
    fn preview_interval_skips_early_frames_and_accepts_the_boundary() {
        let last = std::time::Instant::now();
        let interval = Duration::from_millis(33);

        assert!(preview_is_due(None, last, interval));
        assert!(!preview_is_due(
            Some(last),
            last + Duration::from_millis(32),
            interval
        ));
        assert!(preview_is_due(Some(last), last + interval, interval));
    }

    #[test]
    fn output_request_publishes_and_clears_the_selected_layout() {
        let output = AtomicU8::new(NO_PIXEL_OUTPUT);
        {
            let _request = OutputRequest::new(&output, DecodedPixelFormat::Bgra8888);
            assert_eq!(
                requested_output_format(&output),
                Some(DecodedPixelFormat::Bgra8888)
            );
        }
        assert_eq!(requested_output_format(&output), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn decoder_worker_keeps_only_the_latest_decoded_frame() {
        let counters = Arc::new(PipelineCounters::default());
        let worker_counters = Arc::clone(&counters);
        let output_format = Arc::new(AtomicU8::new(RGBA8888_OUTPUT));
        let worker_output_format = Arc::clone(&output_format);
        let (packet_sender, packet_receiver) = mpsc::channel(2);
        let (frame_sender, mut frame_receiver) = watch::channel(PipelineEvent::Waiting);
        let decoder_thread = thread::spawn(move || {
            run_decoder_worker(
                Box::new(FakeDecoder),
                packet_receiver,
                &frame_sender,
                &worker_counters,
                &worker_output_format,
            );
        });

        for presentation_time_us in 1..=3 {
            let packet_byte = u8::try_from(presentation_time_us).unwrap_or_default();
            packet_sender
                .send(DecodeMessage::Frame {
                    data: Bytes::from(vec![packet_byte; 4]),
                    presentation_time_us,
                    key_frame: presentation_time_us == 1,
                    frame_size: Some((1080, 1920)),
                })
                .await
                .expect("decoder worker should remain available");
        }

        let (frame, sequence) = loop {
            frame_receiver
                .changed()
                .await
                .expect("decoder worker should retain the frame sender");
            let latest = frame_receiver.borrow_and_update().clone();
            let PipelineEvent::Frame {
                frame,
                sequence,
                terminal: None,
            } = latest
            else {
                panic!("latest event should contain a decoded frame");
            };
            if sequence == 3 {
                break (frame, sequence);
            }
        };
        assert_eq!(sequence, 3);
        assert_eq!(frame.presentation_time_us, 3);
        assert_eq!(frame.pixels.as_ref(), &[3, 3, 3, 3]);
        assert_eq!(frame.format, DecodedPixelFormat::Rgba8888);

        drop(packet_sender);
        tokio::task::spawn_blocking(move || decoder_thread.join())
            .await
            .expect("join task should complete")
            .expect("decoder worker should not panic");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn decoder_worker_preserves_delayed_metadata_and_flushes_the_tail() {
        let counters = Arc::new(PipelineCounters::default());
        let worker_counters = Arc::clone(&counters);
        let output_format = Arc::new(AtomicU8::new(RGBA8888_OUTPUT));
        let worker_output_format = Arc::clone(&output_format);
        let (packet_sender, packet_receiver) = mpsc::channel(2);
        let (frame_sender, frame_receiver) = watch::channel(PipelineEvent::Waiting);
        let decoder_thread = thread::spawn(move || {
            run_decoder_worker(
                Box::<DelayedDecoder>::default(),
                packet_receiver,
                &frame_sender,
                &worker_counters,
                &worker_output_format,
            );
        });

        for presentation_time_us in 1..=2 {
            packet_sender
                .send(DecodeMessage::Frame {
                    data: Bytes::from_static(&[1, 2, 3, 4]),
                    presentation_time_us,
                    key_frame: presentation_time_us == 1,
                    frame_size: Some((1080, 1920)),
                })
                .await
                .expect("decoder worker should remain available");
        }
        drop(packet_sender);
        tokio::task::spawn_blocking(move || decoder_thread.join())
            .await
            .expect("join task should complete")
            .expect("decoder worker should not panic");

        let latest = frame_receiver.borrow().clone();
        let PipelineEvent::Frame {
            frame,
            sequence,
            terminal: Some(PipelineFailure::Stopped),
        } = latest
        else {
            panic!("the flushed tail should be the terminal frame");
        };
        assert_eq!(sequence, 2);
        assert_eq!(frame.presentation_time_us, 2);
        assert!(!frame.key_frame);
        assert_eq!(counters.decoded_frames.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn decoder_worker_avoids_pixel_conversion_without_a_consumer() {
        let counters = Arc::new(PipelineCounters::default());
        let worker_counters = Arc::clone(&counters);
        let output_format = Arc::new(AtomicU8::new(NO_PIXEL_OUTPUT));
        let worker_output_format = Arc::clone(&output_format);
        let (packet_sender, packet_receiver) = mpsc::channel(1);
        let (frame_sender, frame_receiver) = watch::channel(PipelineEvent::Waiting);
        let decoder_thread = thread::spawn(move || {
            run_decoder_worker(
                Box::new(FakeDecoder),
                packet_receiver,
                &frame_sender,
                &worker_counters,
                &worker_output_format,
            );
        });
        packet_sender
            .send(DecodeMessage::Frame {
                data: Bytes::from_static(&[1, 2, 3, 4]),
                presentation_time_us: 1,
                key_frame: true,
                frame_size: Some((1080, 1920)),
            })
            .await
            .expect("decoder worker should remain available");
        drop(packet_sender);
        tokio::task::spawn_blocking(move || decoder_thread.join())
            .await
            .expect("join task should complete")
            .expect("decoder worker should not panic");

        assert_eq!(counters.encoded_frames.load(Ordering::Relaxed), 1);
        assert_eq!(counters.decoded_frames.load(Ordering::Relaxed), 0);
        assert!(matches!(*frame_receiver.borrow(), PipelineEvent::Closed));
    }

    #[test]
    fn decoded_frame_validation_rejects_a_mismatched_pixel_buffer() {
        let error = validate_decoded_frame(
            DecodedVideoFrame {
                pixels: Bytes::from_static(&[1, 2, 3]),
                width: 1,
                height: 1,
                format: DecodedPixelFormat::Rgba8888,
                metadata: VideoFrameMetadata {
                    presentation_time_us: 1,
                    key_frame: true,
                    frame_size: None,
                },
            },
            DecodedPixelFormat::Rgba8888,
        )
        .expect_err("RGBA pixels must match width times height times four");

        assert!(matches!(error, MirrorError::Frame(_)));
    }

    #[test]
    fn decoded_frame_validation_accepts_bgra_and_preserves_channels() {
        let frame = validate_decoded_frame(
            DecodedVideoFrame {
                pixels: Bytes::from_static(&[30, 20, 10, 255]),
                width: 1,
                height: 1,
                format: DecodedPixelFormat::Bgra8888,
                metadata: VideoFrameMetadata {
                    presentation_time_us: 7,
                    key_frame: false,
                    frame_size: None,
                },
            },
            DecodedPixelFormat::Bgra8888,
        )
        .expect("valid BGRA pixels should be accepted");

        assert_eq!(frame.pixels.as_ref(), &[30, 20, 10, 255]);
        assert_eq!(frame.format, DecodedPixelFormat::Bgra8888);
        assert_eq!(frame.presentation_time_us, 7);
    }

    #[test]
    fn decoded_frame_validation_rejects_an_unrequested_layout() {
        let error = validate_decoded_frame(
            DecodedVideoFrame {
                pixels: Bytes::from_static(&[30, 20, 10, 255]),
                width: 1,
                height: 1,
                format: DecodedPixelFormat::Bgra8888,
                metadata: VideoFrameMetadata {
                    presentation_time_us: 1,
                    key_frame: true,
                    frame_size: None,
                },
            },
            DecodedPixelFormat::Rgba8888,
        )
        .expect_err("BGRA output must not satisfy an RGBA request");

        assert!(matches!(error, MirrorError::Frame(_)));
    }
}
