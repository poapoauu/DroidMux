use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc, oneshot, watch};

use crate::{
    AdbStreamError,
    session::{OpenedStream, OutboundFlow, SessionCommand, SessionControl, StreamStatus},
};

/// A concurrently usable logical service stream multiplexed over one ADB session.
#[derive(Clone)]
pub struct AdbStream {
    inner: Arc<AdbStreamInner>,
}

struct AdbStreamInner {
    service: String,
    local_id: u32,
    remote_id: u32,
    max_payload: usize,
    burst_mode: bool,
    commands: mpsc::Sender<SessionCommand>,
    controls: mpsc::UnboundedSender<SessionControl>,
    outbound: Arc<OutboundFlow>,
    incoming: Mutex<mpsc::UnboundedReceiver<Bytes>>,
    status: watch::Receiver<StreamStatus>,
    write_lock: Mutex<()>,
    close_started: AtomicBool,
}

impl AdbStream {
    pub(crate) fn new(
        service: String,
        max_payload: usize,
        burst_mode: bool,
        commands: mpsc::Sender<SessionCommand>,
        controls: mpsc::UnboundedSender<SessionControl>,
        opened: OpenedStream,
    ) -> Self {
        Self {
            inner: Arc::new(AdbStreamInner {
                service,
                local_id: opened.local_id,
                remote_id: opened.remote_id,
                max_payload,
                burst_mode,
                commands,
                controls,
                outbound: opened.outbound,
                incoming: Mutex::new(opened.incoming),
                status: opened.status,
                write_lock: Mutex::new(()),
                close_started: AtomicBool::new(false),
            }),
        }
    }

    /// Returns the NUL-free service name supplied to `open_service`.
    #[must_use]
    pub fn service(&self) -> &str {
        &self.inner.service
    }

    /// Returns the host-local stream identifier.
    #[must_use]
    pub fn local_id(&self) -> u32 {
        self.inner.local_id
    }

    /// Returns the device-local stream identifier.
    #[must_use]
    pub fn remote_id(&self) -> u32 {
        self.inner.remote_id
    }

    /// Returns the maximum payload accepted by one ADB `WRTE` packet.
    #[must_use]
    pub fn max_payload(&self) -> usize {
        self.inner.max_payload
    }

    /// Receives one `WRTE` payload and acknowledges it to the peer.
    ///
    /// `Ok(None)` means the logical stream was closed normally. A transport or
    /// session failure is returned as an error and is visible to every stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying session disconnects.
    pub async fn read(&self) -> Result<Option<Bytes>, AdbStreamError> {
        let mut incoming = self.inner.incoming.lock().await;
        let mut status = self.inner.status.clone();

        let payload = loop {
            match incoming.try_recv() {
                Ok(payload) => break payload,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return stream_read_result(&status.borrow());
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
            }

            match &*status.borrow() {
                StreamStatus::Open => {}
                other => return stream_read_result(other),
            }

            tokio::select! {
                biased;
                payload = incoming.recv() => {
                    let Some(payload) = payload else {
                        return stream_read_result(&status.borrow());
                    };
                    break payload;
                }
                changed = status.changed() => {
                    if changed.is_err() {
                        return Err(stopped_session_error());
                    }
                }
            }
        };

        drop(incoming);
        let (response_sender, response_receiver) = oneshot::channel();
        self.inner
            .commands
            .send(SessionCommand::AcknowledgeInbound {
                local_id: self.inner.local_id,
                remote_id: self.inner.remote_id,
                bytes: u32::try_from(payload.len()).map_err(|_| self.current_session_error())?,
                response: response_sender,
            })
            .await
            .map_err(|_| self.current_session_error())?;
        response_receiver
            .await
            .map_err(|_| self.current_session_error())??;
        Ok(Some(payload))
    }

    /// Sends one `WRTE` payload and waits for the peer's `OKAY` response.
    ///
    /// Concurrent writes on clones of the same stream are serialized. Writes
    /// on different streams can be in flight independently.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream is closed, the payload is too large, or
    /// the underlying session disconnects.
    pub async fn write(&self, payload: impl Into<Bytes>) -> Result<(), AdbStreamError> {
        let payload = payload.into();
        if payload.len() > self.inner.max_payload {
            return Err(AdbStreamError::PayloadTooLarge {
                limit: self.inner.max_payload,
                actual: payload.len(),
            });
        }

        if self.inner.burst_mode {
            return self.write_inner(payload).await;
        }
        let _write_guard = self.inner.write_lock.lock().await;
        self.write_inner(payload).await
    }

    async fn write_inner(&self, payload: Bytes) -> Result<(), AdbStreamError> {
        self.ensure_writable()?;
        let reservation = if self.inner.burst_mode {
            Some(self.inner.outbound.reserve(payload.len()).await?)
        } else {
            None
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.inner
            .commands
            .send(SessionCommand::Write {
                local_id: self.inner.local_id,
                remote_id: self.inner.remote_id,
                payload,
                response: response_sender,
            })
            .await
            .map_err(|_| self.current_session_error())?;
        if let Some(reservation) = reservation {
            reservation.commit();
        }

        response_receiver
            .await
            .unwrap_or_else(|_| Err(self.current_session_error()))
    }

    /// Closes this logical stream without affecting sibling streams.
    ///
    /// Repeated calls and closing a stream already closed by the peer are
    /// harmless.
    ///
    /// # Errors
    ///
    /// Returns an error if the close packet cannot be delivered because the
    /// underlying session disconnected.
    pub async fn close(&self) -> Result<(), AdbStreamError> {
        if self.inner.close_started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        match &*self.inner.status.borrow() {
            StreamStatus::LocalClosed | StreamStatus::RemoteClosed => return Ok(()),
            StreamStatus::SessionClosed(reason) => {
                return Err(AdbStreamError::SessionClosed {
                    reason: reason.clone(),
                });
            }
            StreamStatus::Open => {}
        }

        let (response_sender, response_receiver) = oneshot::channel();
        self.inner
            .controls
            .send(SessionControl::CloseStream {
                local_id: self.inner.local_id,
                remote_id: self.inner.remote_id,
                response: Some(response_sender),
            })
            .map_err(|_| self.current_session_error())?;

        response_receiver
            .await
            .unwrap_or_else(|_| Err(self.current_session_error()))
    }

    /// Returns whether this stream or its parent session has closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        !matches!(*self.inner.status.borrow(), StreamStatus::Open)
    }

    fn ensure_writable(&self) -> Result<(), AdbStreamError> {
        match &*self.inner.status.borrow() {
            StreamStatus::Open => Ok(()),
            StreamStatus::LocalClosed => Err(AdbStreamError::StreamClosed),
            StreamStatus::RemoteClosed => Err(AdbStreamError::RemoteClosed),
            StreamStatus::SessionClosed(reason) => Err(AdbStreamError::SessionClosed {
                reason: reason.clone(),
            }),
        }
    }

    fn current_session_error(&self) -> AdbStreamError {
        match &*self.inner.status.borrow() {
            StreamStatus::LocalClosed => AdbStreamError::StreamClosed,
            StreamStatus::RemoteClosed => AdbStreamError::RemoteClosed,
            StreamStatus::SessionClosed(reason) => AdbStreamError::SessionClosed {
                reason: reason.clone(),
            },
            StreamStatus::Open => stopped_session_error(),
        }
    }
}

impl fmt::Debug for AdbStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdbStream")
            .field("service", &self.inner.service)
            .field("local_id", &self.inner.local_id)
            .field("remote_id", &self.inner.remote_id)
            .field("max_payload", &self.inner.max_payload)
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

impl Drop for AdbStreamInner {
    fn drop(&mut self) {
        if self.close_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.controls.send(SessionControl::CloseStream {
            local_id: self.local_id,
            remote_id: self.remote_id,
            response: None,
        });
    }
}

fn stream_read_result(status: &StreamStatus) -> Result<Option<Bytes>, AdbStreamError> {
    match status {
        StreamStatus::Open => Err(stopped_session_error()),
        StreamStatus::LocalClosed | StreamStatus::RemoteClosed => Ok(None),
        StreamStatus::SessionClosed(reason) => Err(AdbStreamError::SessionClosed {
            reason: reason.clone(),
        }),
    }
}

fn stopped_session_error() -> AdbStreamError {
    AdbStreamError::SessionClosed {
        reason: "session task stopped".to_owned(),
    }
}
