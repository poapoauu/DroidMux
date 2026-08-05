use std::fmt;

use adb_client::{AdbClient, AdbStream, AdbStreamError};
use bytes::{Bytes, BytesMut};
use tokio::{
    sync::{Mutex, mpsc, watch},
    task::JoinHandle,
};

use crate::{
    ShellError, ShellOptions, ShellProtocolError,
    codec::{SHELL_HEADER_LEN, ShellDecoder, ShellPacketId, encode_packet},
};

const SHELL_V2_FEATURE: &str = "shell_v2";
const OUTPUT_CHANNEL_CAPACITY: usize = 16;
const INTERRUPT_BYTE: u8 = 0x03;

/// Collected output from a completed shell command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellOutput {
    /// Bytes written to stdout.
    pub stdout: Bytes,
    /// Bytes written to stderr, empty for legacy or PTY sessions.
    pub stderr: Bytes,
    /// Process status from Shell v2, or `None` for the legacy protocol.
    pub exit_code: Option<u8>,
}

#[derive(Debug, Clone)]
enum TerminalError {
    Stream(AdbStreamError),
    Protocol(ShellProtocolError),
    Canceled,
}

impl From<TerminalError> for ShellError {
    fn from(error: TerminalError) -> Self {
        match error {
            TerminalError::Stream(error) => Self::Stream(error),
            TerminalError::Protocol(error) => Self::Protocol(error),
            TerminalError::Canceled => Self::Canceled,
        }
    }
}

#[derive(Debug, Clone)]
enum CompletionState {
    Running,
    Finished(Result<Option<u8>, TerminalError>),
}

/// A streaming ADB shell subprocess.
///
/// The background reader separates stdout and stderr without buffering an
/// entire command result. Dropping the session aborts that reader and closes
/// the underlying logical ADB stream on a best-effort basis.
pub struct ShellSession {
    stream: AdbStream,
    options: ShellOptions,
    stdout: Mutex<mpsc::Receiver<Bytes>>,
    stderr: Mutex<mpsc::Receiver<Bytes>>,
    completion: watch::Receiver<CompletionState>,
    canceled: watch::Sender<bool>,
    reader_task: JoinHandle<()>,
}

impl ShellSession {
    /// Returns the options used to create this subprocess.
    #[must_use]
    pub const fn options(&self) -> ShellOptions {
        self.options
    }

    /// Reads the next stdout chunk, or `None` after the reader finishes.
    pub async fn read_stdout(&self) -> Option<Bytes> {
        self.stdout.lock().await.recv().await
    }

    /// Reads the next stderr chunk, or `None` after the reader finishes.
    ///
    /// Legacy and PTY sessions merge stderr into stdout.
    pub async fn read_stderr(&self) -> Option<Bytes> {
        self.stderr.lock().await.recv().await
    }

    /// Writes bytes to the subprocess stdin.
    ///
    /// Large input is split into frames that fit the negotiated ADB payload.
    ///
    /// # Errors
    ///
    /// Returns an error if framing fails or the stream/session is closed.
    pub async fn write_stdin(&self, data: impl Into<Bytes>) -> Result<(), ShellError> {
        self.ensure_not_canceled()?;
        let data = data.into();
        if data.is_empty() {
            return Ok(());
        }

        if self.options.use_v2 {
            let chunk_size = shell_data_limit(self.stream.max_payload())?;
            for chunk in data.chunks(chunk_size) {
                self.send_v2_packet(ShellPacketId::Stdin, chunk).await?;
            }
        } else {
            for chunk in data.chunks(self.stream.max_payload()) {
                self.stream.write(Bytes::copy_from_slice(chunk)).await?;
            }
        }
        Ok(())
    }

    /// Sends Ctrl+C (`ETX`) to the remote terminal or raw stdin pipe.
    ///
    /// # Errors
    ///
    /// Returns an error if stdin cannot be written.
    pub async fn interrupt(&self) -> Result<(), ShellError> {
        self.write_stdin(Bytes::from_static(&[INTERRUPT_BYTE]))
            .await
    }

    /// Closes subprocess stdin while leaving Shell v2 output open.
    ///
    /// # Errors
    ///
    /// Returns an error for legacy sessions or if the stream is closed.
    pub async fn close_stdin(&self) -> Result<(), ShellError> {
        self.ensure_not_canceled()?;
        if !self.options.use_v2 {
            return Err(ShellError::UnsupportedOperation {
                operation: "close_stdin",
                requirement: "Shell v2",
            });
        }
        self.send_v2_packet(ShellPacketId::CloseStdin, &[]).await
    }

    /// Changes the size of an interactive Shell v2 pseudo-terminal.
    ///
    /// # Errors
    ///
    /// Returns an error for non-PTY or legacy sessions, zero dimensions, or a
    /// closed stream.
    pub async fn resize(&self, rows: u16, columns: u16) -> Result<(), ShellError> {
        self.ensure_not_canceled()?;
        if !self.options.use_v2 || !self.options.use_pty {
            return Err(ShellError::UnsupportedOperation {
                operation: "resize",
                requirement: "a Shell v2 PTY",
            });
        }
        send_window_size(&self.stream, rows, columns).await
    }

    /// Waits for the subprocess to finish.
    ///
    /// Shell v2 returns `Some(exit_code)`; the legacy protocol returns `None`.
    /// This method may be called more than once. Consume stdout and stderr
    /// concurrently so their bounded channels can continue applying orderly
    /// backpressure to a long-running command.
    ///
    /// # Errors
    ///
    /// Returns a stream, protocol, or cancellation error.
    pub async fn wait(&self) -> Result<Option<u8>, ShellError> {
        let mut completion = self.completion.clone();
        loop {
            let state = completion.borrow().clone();
            match state {
                CompletionState::Running => {}
                CompletionState::Finished(result) => return result.map_err(ShellError::from),
            }
            completion
                .changed()
                .await
                .map_err(|_| ShellError::ReaderStopped)?;
        }
    }

    /// Cancels the subprocess by closing its logical ADB stream.
    ///
    /// The daemon observes the closed protocol endpoint and terminates the
    /// child process. Repeated calls are harmless.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying stream cannot be closed cleanly.
    pub async fn cancel(&self) -> Result<(), ShellError> {
        if *self.canceled.borrow() {
            return Ok(());
        }
        self.canceled.send_replace(true);
        self.stream.close().await?;
        Ok(())
    }

    /// Closes this shell session, equivalent to [`Self::cancel`].
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying stream cannot be closed cleanly.
    pub async fn close(&self) -> Result<(), ShellError> {
        self.cancel().await
    }

    /// Returns whether the background reader has published a terminal result.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        matches!(&*self.completion.borrow(), CompletionState::Finished(_))
    }

    async fn send_v2_packet(&self, id: ShellPacketId, payload: &[u8]) -> Result<(), ShellError> {
        let encoded = encode_packet(id, payload, self.stream.max_payload())?;
        self.stream.write(encoded).await?;
        Ok(())
    }

    fn ensure_not_canceled(&self) -> Result<(), ShellError> {
        if *self.canceled.borrow() {
            Err(ShellError::Canceled)
        } else {
            Ok(())
        }
    }
}

impl fmt::Debug for ShellSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShellSession")
            .field("stream", &self.stream)
            .field("options", &self.options)
            .field("finished", &self.is_finished())
            .finish_non_exhaustive()
    }
}

impl Drop for ShellSession {
    fn drop(&mut self) {
        self.canceled.send_replace(true);
        self.reader_task.abort();
    }
}

/// Opens an interactive or command-oriented ADB shell subprocess.
///
/// An empty command starts an interactive shell. Use
/// [`ShellOptions::interactive`] to allocate a PTY for that case.
///
/// # Errors
///
/// Returns an error if options are invalid, Shell v2 is unsupported, or the
/// ADB service cannot be opened.
pub async fn open_shell(
    client: &AdbClient,
    command: &str,
    options: ShellOptions,
) -> Result<ShellSession, ShellError> {
    validate_request(client, command, options)?;
    let service = build_service(command, options);
    let stream = client.open_service(&service).await?;

    if options.use_v2 && options.use_pty {
        if let Err(error) = send_window_size(&stream, options.rows, options.columns).await {
            let _ = stream.close().await;
            return Err(error);
        }
    }

    let (stdout_sender, stdout_receiver) = mpsc::channel(OUTPUT_CHANNEL_CAPACITY);
    let (stderr_sender, stderr_receiver) = mpsc::channel(OUTPUT_CHANNEL_CAPACITY);
    let (completion_sender, completion_receiver) = watch::channel(CompletionState::Running);
    let (canceled, canceled_receiver) = watch::channel(false);
    let reader_stream = stream.clone();
    let reader_task = tokio::spawn(async move {
        let mut result = if options.use_v2 {
            read_v2(
                &reader_stream,
                &stdout_sender,
                &stderr_sender,
                canceled_receiver,
            )
            .await
        } else {
            read_legacy(&reader_stream, &stdout_sender, canceled_receiver).await
        };
        drop(stdout_sender);
        drop(stderr_sender);
        if let Err(error) = reader_stream.close().await {
            if result.is_ok() {
                result = Err(TerminalError::Stream(error));
            }
        }
        completion_sender.send_replace(CompletionState::Finished(result));
    });

    Ok(ShellSession {
        stream,
        options,
        stdout: Mutex::new(stdout_receiver),
        stderr: Mutex::new(stderr_receiver),
        completion: completion_receiver,
        canceled,
        reader_task,
    })
}

/// Executes one non-interactive command using raw Shell v2 defaults.
///
/// # Errors
///
/// Returns an error if the shell cannot start or finish successfully.
pub async fn execute(client: &AdbClient, command: &str) -> Result<ShellOutput, ShellError> {
    execute_with_options(client, command, ShellOptions::default()).await
}

/// Executes one command and collects all output with explicit options.
///
/// For unbounded or continuous output, use [`open_shell`] and consume the
/// streaming receivers instead.
///
/// # Errors
///
/// Returns an error if the shell cannot start or finish successfully.
pub async fn execute_with_options(
    client: &AdbClient,
    command: &str,
    options: ShellOptions,
) -> Result<ShellOutput, ShellError> {
    let session = open_shell(client, command, options).await?;
    let mut stdout = BytesMut::new();
    let mut stderr = BytesMut::new();
    let mut total_output_bytes = 0;
    let mut stdout_open = true;
    let mut stderr_open = true;
    let mut exit_code = None;
    let wait = session.wait();
    tokio::pin!(wait);

    while stdout_open || stderr_open || exit_code.is_none() {
        tokio::select! {
            chunk = session.read_stdout(), if stdout_open => {
                match chunk {
                    Some(chunk) => {
                        if let Err(error) = append_collected_output(
                            &mut stdout,
                            &chunk,
                            options.max_output_bytes,
                            &mut total_output_bytes,
                        ) {
                            let _ = session.cancel().await;
                            return Err(error);
                        }
                    }
                    None => stdout_open = false,
                }
            }
            chunk = session.read_stderr(), if stderr_open => {
                match chunk {
                    Some(chunk) => {
                        if let Err(error) = append_collected_output(
                            &mut stderr,
                            &chunk,
                            options.max_output_bytes,
                            &mut total_output_bytes,
                        ) {
                            let _ = session.cancel().await;
                            return Err(error);
                        }
                    }
                    None => stderr_open = false,
                }
            }
            result = &mut wait, if exit_code.is_none() => exit_code = Some(result),
        }
    }

    let exit_code = exit_code.ok_or(ShellError::ReaderStopped)??;
    Ok(ShellOutput {
        stdout: stdout.freeze(),
        stderr: stderr.freeze(),
        exit_code,
    })
}

fn append_collected_output(
    output: &mut BytesMut,
    chunk: &[u8],
    limit: Option<usize>,
    total: &mut usize,
) -> Result<(), ShellError> {
    let actual = total
        .checked_add(chunk.len())
        .ok_or_else(|| ShellError::OutputTooLarge {
            limit: limit.unwrap_or(usize::MAX),
            actual: usize::MAX,
        })?;
    if let Some(limit) = limit {
        if actual > limit {
            return Err(ShellError::OutputTooLarge { limit, actual });
        }
    }
    output.extend_from_slice(chunk);
    *total = actual;
    Ok(())
}

fn validate_request(
    client: &AdbClient,
    command: &str,
    options: ShellOptions,
) -> Result<(), ShellError> {
    if command.as_bytes().contains(&0) {
        return Err(ShellError::InvalidCommand);
    }
    if options.use_v2 && !client.supports_feature(SHELL_V2_FEATURE) {
        return Err(ShellError::ShellV2Unsupported);
    }
    if options.use_pty && (options.rows == 0 || options.columns == 0) {
        return Err(ShellError::InvalidWindowSize);
    }
    Ok(())
}

fn build_service(command: &str, options: ShellOptions) -> String {
    if options.use_v2 {
        let mode = if options.use_pty { "pty" } else { "raw" };
        format!("shell,v2,{mode}:{command}")
    } else if options.use_pty {
        format!("shell,pty:{command}")
    } else {
        format!("shell:{command}")
    }
}

async fn send_window_size(stream: &AdbStream, rows: u16, columns: u16) -> Result<(), ShellError> {
    if rows == 0 || columns == 0 {
        return Err(ShellError::InvalidWindowSize);
    }
    let mut payload = format!("{rows}x{columns},0x0").into_bytes();
    payload.push(0);
    let encoded = encode_packet(
        ShellPacketId::WindowSizeChange,
        &payload,
        stream.max_payload(),
    )?;
    stream.write(encoded).await?;
    Ok(())
}

fn shell_data_limit(wire_limit: usize) -> Result<usize, ShellError> {
    wire_limit
        .checked_sub(SHELL_HEADER_LEN)
        .filter(|limit| *limit != 0)
        .ok_or_else(|| {
            ShellProtocolError::PacketTooLarge {
                limit: 0,
                actual: 1,
            }
            .into()
        })
}

async fn read_legacy(
    stream: &AdbStream,
    stdout: &mpsc::Sender<Bytes>,
    mut canceled: watch::Receiver<bool>,
) -> Result<Option<u8>, TerminalError> {
    loop {
        match stream.read().await {
            Ok(Some(payload)) => {
                forward_output(stdout, payload, &mut canceled).await?;
            }
            Ok(None) => {
                return if *canceled.borrow() {
                    Err(TerminalError::Canceled)
                } else {
                    Ok(None)
                };
            }
            Err(error) => {
                return if *canceled.borrow() {
                    Err(TerminalError::Canceled)
                } else {
                    Err(TerminalError::Stream(error))
                };
            }
        }
    }
}

async fn read_v2(
    stream: &AdbStream,
    stdout: &mpsc::Sender<Bytes>,
    stderr: &mpsc::Sender<Bytes>,
    mut canceled: watch::Receiver<bool>,
) -> Result<Option<u8>, TerminalError> {
    let mut decoder = ShellDecoder::default();
    loop {
        let chunk = match stream.read().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => {
                if *canceled.borrow() {
                    return Err(TerminalError::Canceled);
                }
                decoder.finish().map_err(TerminalError::Protocol)?;
                return Err(TerminalError::Protocol(ShellProtocolError::MissingExitCode));
            }
            Err(error) => {
                return if *canceled.borrow() {
                    Err(TerminalError::Canceled)
                } else {
                    Err(TerminalError::Stream(error))
                };
            }
        };
        decoder.push(&chunk);

        while let Some(packet) = decoder.next_packet().map_err(TerminalError::Protocol)? {
            match packet.id {
                ShellPacketId::Stdout => {
                    forward_output(stdout, packet.payload, &mut canceled).await?;
                }
                ShellPacketId::Stderr => {
                    forward_output(stderr, packet.payload, &mut canceled).await?;
                }
                ShellPacketId::Exit => {
                    if packet.payload.len() != 1 {
                        return Err(TerminalError::Protocol(
                            ShellProtocolError::InvalidExitLength {
                                actual: packet.payload.len(),
                            },
                        ));
                    }
                    if !decoder.is_empty() {
                        return Err(TerminalError::Protocol(ShellProtocolError::DataAfterExit));
                    }
                    return Ok(Some(packet.payload[0]));
                }
                ShellPacketId::Stdin
                | ShellPacketId::CloseStdin
                | ShellPacketId::WindowSizeChange => {
                    return Err(TerminalError::Protocol(
                        ShellProtocolError::UnexpectedPacket {
                            actual: packet.id as u8,
                        },
                    ));
                }
            }
        }
    }
}

async fn forward_output(
    output: &mpsc::Sender<Bytes>,
    payload: Bytes,
    canceled: &mut watch::Receiver<bool>,
) -> Result<(), TerminalError> {
    if *canceled.borrow() {
        return Err(TerminalError::Canceled);
    }

    tokio::select! {
        result = output.send(payload) => {
            let _ = result;
            Ok(())
        }
        changed = canceled.changed() => {
            if changed.is_err() || *canceled.borrow() {
                Err(TerminalError::Canceled)
            } else {
                Ok(())
            }
        }
    }
}
