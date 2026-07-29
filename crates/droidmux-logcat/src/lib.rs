//! Native, bounded Android Logcat streaming over an ADB logical stream.

use std::time::Duration;

use adb_client::{AdbClient, AdbClientError, AdbStream, AdbStreamError};
use thiserror::Error;
use tokio::sync::{Mutex, mpsc, watch};

const LOGCAT_SERVICE: &str = "exec:logcat -v threadtime 2>/dev/null";
const CLEAR_SERVICE: &str = "exec:logcat -c 2>/dev/null";

/// Default maximum accepted UTF-8 line size.
pub const DEFAULT_MAX_LINE_SIZE: usize = 64 * 1024;
/// Default number of parsed entries buffered between the reader and consumer.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1_024;

/// Android Logcat priority ordered from least to most severe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogPriority {
    /// Verbose diagnostic output.
    Verbose,
    /// Debug diagnostic output.
    Debug,
    /// Informational output.
    Info,
    /// Warning output.
    Warn,
    /// Error output.
    Error,
    /// Fatal assertion output.
    Fatal,
    /// A line that did not expose a standard priority field.
    Unknown,
}

impl LogPriority {
    /// Parses the one-character priority emitted by `threadtime` format.
    #[must_use]
    pub fn from_token(token: &str) -> Self {
        match token {
            "V" => Self::Verbose,
            "D" => Self::Debug,
            "I" => Self::Info,
            "W" => Self::Warn,
            "E" => Self::Error,
            "F" | "A" => Self::Fatal,
            _ => Self::Unknown,
        }
    }

    /// Returns the conventional single-character priority label.
    #[must_use]
    pub const fn as_char(self) -> char {
        match self {
            Self::Verbose => 'V',
            Self::Debug => 'D',
            Self::Info => 'I',
            Self::Warn => 'W',
            Self::Error => 'E',
            Self::Fatal => 'F',
            Self::Unknown => '?',
        }
    }

    /// Returns a numeric severity suitable for minimum-level filtering.
    #[must_use]
    pub const fn severity(self) -> u8 {
        match self {
            Self::Verbose | Self::Unknown => 0,
            Self::Debug => 1,
            Self::Info => 2,
            Self::Warn => 3,
            Self::Error => 4,
            Self::Fatal => 5,
        }
    }
}

/// One parsed Logcat line with its original text preserved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogcatEntry {
    /// Original line without its trailing newline.
    pub raw: String,
    /// `MM-DD HH:MM:SS.mmm` timestamp, when parsed.
    pub timestamp: Option<String>,
    /// Android process identifier, when parsed.
    pub pid: Option<u32>,
    /// Android thread identifier, when parsed.
    pub tid: Option<u32>,
    /// Parsed Android priority.
    pub priority: LogPriority,
    /// Log tag, when parsed.
    pub tag: Option<String>,
    /// Message body, or the complete raw line for unstructured output.
    pub message: String,
}

/// Limits controlling a Logcat stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogcatOptions {
    /// Maximum duration allowed for opening the stream.
    pub open_timeout: Duration,
    /// Maximum accepted bytes in one logical line.
    pub max_line_size: usize,
    /// Maximum entries queued for the consumer.
    pub channel_capacity: usize,
}

impl Default for LogcatOptions {
    fn default() -> Self {
        Self {
            open_timeout: Duration::from_secs(5),
            max_line_size: DEFAULT_MAX_LINE_SIZE,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }
}

/// Errors returned by native Logcat operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum LogcatError {
    /// Opening the ADB execution service failed.
    #[error(transparent)]
    Client(#[from] AdbClientError),
    /// Reading or closing the ADB logical stream failed.
    #[error(transparent)]
    Stream(#[from] AdbStreamError),
    /// Opening or clearing Logcat exceeded its deadline.
    #[error("Logcat operation timed out after {0:?}")]
    Timeout(Duration),
    /// A caller supplied a zero line or channel limit.
    #[error("Logcat line and channel limits must be non-zero")]
    InvalidLimits,
    /// A device line crossed the configured byte limit.
    #[error("Logcat line exceeds the configured limit of {limit} bytes")]
    LineTooLong {
        /// Maximum accepted bytes per line.
        limit: usize,
    },
}

/// A running Logcat stream with a bounded parsed-entry receiver.
pub struct LogcatSession {
    receiver: Mutex<mpsc::Receiver<Result<LogcatEntry, LogcatError>>>,
    canceled: watch::Sender<bool>,
    completion: watch::Receiver<bool>,
}

impl LogcatSession {
    /// Waits for the next parsed entry.
    ///
    /// `Ok(None)` means the device stream ended or the session was canceled.
    ///
    /// # Errors
    ///
    /// Returns a stream or line-decoding error published by the reader task.
    pub async fn next_entry(&self) -> Result<Option<LogcatEntry>, LogcatError> {
        match self.receiver.lock().await.recv().await {
            Some(Ok(entry)) => Ok(Some(entry)),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }

    /// Requests cancellation and waits for the ADB stream reader to stop.
    pub async fn cancel(&self) {
        self.canceled.send_replace(true);
        let mut completion = self.completion.clone();
        while !*completion.borrow() && completion.changed().await.is_ok() {}
    }

    /// Reports whether cancellation was requested.
    #[must_use]
    pub fn is_canceled(&self) -> bool {
        *self.canceled.borrow()
    }
}

impl Drop for LogcatSession {
    fn drop(&mut self) {
        self.canceled.send_replace(true);
    }
}

/// Opens a continuous `logcat -v threadtime` stream.
///
/// This uses an independent native ADB execution stream and never invokes an
/// external `adb` executable.
///
/// # Errors
///
/// Returns an error for invalid limits, a timeout, or a rejected ADB service.
pub async fn start_logcat(
    client: &AdbClient,
    options: LogcatOptions,
) -> Result<LogcatSession, LogcatError> {
    if options.max_line_size == 0 || options.channel_capacity == 0 {
        return Err(LogcatError::InvalidLimits);
    }
    let stream = tokio::time::timeout(options.open_timeout, client.open_service(LOGCAT_SERVICE))
        .await
        .map_err(|_| LogcatError::Timeout(options.open_timeout))??;
    let (sender, receiver) = mpsc::channel(options.channel_capacity);
    let (canceled, canceled_receiver) = watch::channel(false);
    let (completion_sender, completion) = watch::channel(false);
    tokio::spawn(run_reader(
        stream,
        options.max_line_size,
        sender,
        canceled_receiver,
        completion_sender,
    ));
    Ok(LogcatSession {
        receiver: Mutex::new(receiver),
        canceled,
        completion,
    })
}

/// Clears Android's in-memory Logcat buffers through a native ADB stream.
///
/// # Errors
///
/// Returns an error when the service cannot open, read, or finish within the
/// default five-second deadline.
pub async fn clear_logcat(client: &AdbClient) -> Result<(), LogcatError> {
    let timeout = Duration::from_secs(5);
    let stream = tokio::time::timeout(timeout, client.open_service(CLEAR_SERVICE))
        .await
        .map_err(|_| LogcatError::Timeout(timeout))??;
    loop {
        let chunk = tokio::time::timeout(timeout, stream.read())
            .await
            .map_err(|_| LogcatError::Timeout(timeout))??;
        if chunk.is_none() {
            return Ok(());
        }
    }
}

async fn run_reader(
    stream: AdbStream,
    max_line_size: usize,
    sender: mpsc::Sender<Result<LogcatEntry, LogcatError>>,
    mut canceled: watch::Receiver<bool>,
    completion: watch::Sender<bool>,
) {
    let mut decoder = LineDecoder::new(max_line_size);
    loop {
        let read = tokio::select! {
            biased;
            changed = canceled.changed() => {
                if changed.is_err() || *canceled.borrow() {
                    let _ = stream.close().await;
                    break;
                }
                continue;
            }
            result = stream.read() => result,
        };
        match read {
            Ok(Some(chunk)) => match decoder.push(&chunk) {
                Ok(entries) => {
                    for entry in entries {
                        if !send_entry(&sender, &mut canceled, Ok(entry)).await {
                            let _ = stream.close().await;
                            completion.send_replace(true);
                            return;
                        }
                    }
                }
                Err(error) => {
                    let _ = send_entry(&sender, &mut canceled, Err(error)).await;
                    let _ = stream.close().await;
                    break;
                }
            },
            Ok(None) => {
                if let Some(entry) = decoder.finish() {
                    let _ = send_entry(&sender, &mut canceled, Ok(entry)).await;
                }
                break;
            }
            Err(error) => {
                let _ = send_entry(&sender, &mut canceled, Err(error.into())).await;
                break;
            }
        }
    }
    completion.send_replace(true);
}

async fn send_entry(
    sender: &mpsc::Sender<Result<LogcatEntry, LogcatError>>,
    canceled: &mut watch::Receiver<bool>,
    entry: Result<LogcatEntry, LogcatError>,
) -> bool {
    tokio::select! {
        biased;
        changed = canceled.changed() => changed.is_ok() && !*canceled.borrow(),
        result = sender.send(entry) => result.is_ok(),
    }
}

struct LineDecoder {
    buffer: Vec<u8>,
    max_line_size: usize,
}

impl LineDecoder {
    fn new(max_line_size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(4_096),
            max_line_size,
        }
    }

    fn push(&mut self, chunk: &[u8]) -> Result<Vec<LogcatEntry>, LogcatError> {
        let mut entries = Vec::new();
        for &byte in chunk {
            if byte == b'\n' {
                entries.push(self.take_entry());
            } else {
                if self.buffer.len() >= self.max_line_size {
                    return Err(LogcatError::LineTooLong {
                        limit: self.max_line_size,
                    });
                }
                self.buffer.push(byte);
            }
        }
        Ok(entries)
    }

    fn finish(&mut self) -> Option<LogcatEntry> {
        (!self.buffer.is_empty()).then(|| self.take_entry())
    }

    fn take_entry(&mut self) -> LogcatEntry {
        if self.buffer.last() == Some(&b'\r') {
            self.buffer.pop();
        }
        let raw = String::from_utf8_lossy(&self.buffer).into_owned();
        self.buffer.clear();
        parse_threadtime(&raw)
    }
}

/// Parses one Android `threadtime` line while preserving unstructured lines.
#[must_use]
pub fn parse_threadtime(raw: &str) -> LogcatEntry {
    let fallback = || LogcatEntry {
        raw: raw.to_owned(),
        timestamp: None,
        pid: None,
        tid: None,
        priority: LogPriority::Unknown,
        tag: None,
        message: raw.to_owned(),
    };
    let Some((date, rest)) = take_field(raw) else {
        return fallback();
    };
    let Some((time, rest)) = take_field(rest) else {
        return fallback();
    };
    let Some((pid, rest)) = take_field(rest) else {
        return fallback();
    };
    let Some((tid, rest)) = take_field(rest) else {
        return fallback();
    };
    let Some((priority, rest)) = take_field(rest) else {
        return fallback();
    };
    if date.len() != 5 || date.as_bytes().get(2).is_none_or(|byte| *byte != b'-') {
        return fallback();
    }
    let (Some(pid), Some(tid)) = (pid.parse::<u32>().ok(), tid.parse::<u32>().ok()) else {
        return fallback();
    };
    let Some((tag, message)) = rest.trim_start().split_once(':') else {
        return fallback();
    };
    let tag = tag.trim();
    if tag.is_empty() {
        return fallback();
    }
    LogcatEntry {
        raw: raw.to_owned(),
        timestamp: Some(format!("{date} {time}")),
        pid: Some(pid),
        tid: Some(tid),
        priority: LogPriority::from_token(priority),
        tag: Some(tag.to_owned()),
        message: message.trim_start().to_owned(),
    }
}

fn take_field(input: &str) -> Option<(&str, &str)> {
    let input = input.trim_start();
    let boundary = input.find(char::is_whitespace)?;
    Some((&input[..boundary], &input[boundary..]))
}

#[cfg(test)]
mod tests {
    use super::{LineDecoder, LogPriority, LogcatError, parse_threadtime};

    #[test]
    fn parses_threadtime_fields_and_message_colons() {
        let entry = parse_threadtime("07-27 14:05:06.123  1234  5678 W DroidMux: state: connected");
        assert_eq!(entry.timestamp.as_deref(), Some("07-27 14:05:06.123"));
        assert_eq!(entry.pid, Some(1234));
        assert_eq!(entry.tid, Some(5678));
        assert_eq!(entry.priority, LogPriority::Warn);
        assert_eq!(entry.tag.as_deref(), Some("DroidMux"));
        assert_eq!(entry.message, "state: connected");
    }

    #[test]
    fn preserves_logcat_markers_as_unstructured_lines() {
        let entry = parse_threadtime("--------- beginning of main");
        assert_eq!(entry.priority, LogPriority::Unknown);
        assert_eq!(entry.message, "--------- beginning of main");
        assert_eq!(entry.raw, entry.message);
    }

    #[test]
    fn decoder_handles_fragmented_crlf_and_multiple_lines() {
        let mut decoder = LineDecoder::new(1_024);
        assert!(
            decoder
                .push(b"07-27 14:05")
                .expect("first fragment")
                .is_empty()
        );
        let entries = decoder
            .push(b":06.123 1 2 I Tag: one\r\nraw line\n")
            .expect("remaining data");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].message, "one");
        assert_eq!(entries[1].message, "raw line");
    }

    #[test]
    fn decoder_rejects_an_oversized_line_before_growing() {
        let mut decoder = LineDecoder::new(4);
        let error = decoder.push(b"12345").expect_err("oversized line");
        assert!(matches!(error, LogcatError::LineTooLong { limit: 4 }));
    }
}
