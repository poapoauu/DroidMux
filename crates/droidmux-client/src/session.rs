use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
};

use adb_protocol::{AdbCommand, AdbPacket};
use adb_transport::{AdbTransport, AdbTransportError, TransportOperation};
use bytes::Bytes;
use tokio::sync::{Notify, mpsc, oneshot, watch};

use crate::{AdbClientError, AdbStreamError};

const COMMAND_CAPACITY: usize = 64;
const FIRST_LOCAL_ID: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SessionStatus {
    Connected,
    Reconnecting,
    Closed,
    Disconnected(String),
}

impl SessionStatus {
    pub(crate) fn closure_reason(&self) -> Option<String> {
        match self {
            Self::Connected | Self::Reconnecting => None,
            Self::Closed => Some("session closed locally".to_owned()),
            Self::Disconnected(reason) => Some(reason.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StreamStatus {
    Open,
    LocalClosed,
    RemoteClosed,
    SessionClosed(String),
}

pub(crate) struct OpenedStream {
    pub(crate) local_id: u32,
    pub(crate) remote_id: u32,
    pub(crate) incoming: mpsc::UnboundedReceiver<Bytes>,
    pub(crate) status: watch::Receiver<StreamStatus>,
    pub(crate) outbound: Arc<OutboundFlow>,
}

pub(crate) enum SessionCommand {
    Open {
        service: Bytes,
        response: oneshot::Sender<Result<OpenedStream, AdbClientError>>,
    },
    Write {
        local_id: u32,
        remote_id: u32,
        payload: Bytes,
        response: oneshot::Sender<Result<(), AdbStreamError>>,
    },
    AcknowledgeInbound {
        local_id: u32,
        remote_id: u32,
        bytes: u32,
        response: oneshot::Sender<Result<(), AdbStreamError>>,
    },
}

pub(crate) enum SessionControl {
    CloseStream {
        local_id: u32,
        remote_id: u32,
        response: Option<oneshot::Sender<Result<(), AdbStreamError>>>,
    },
    CloseSession {
        response: oneshot::Sender<Result<(), AdbClientError>>,
    },
}

pub(crate) struct SessionHandle {
    pub(crate) commands: mpsc::Sender<SessionCommand>,
    pub(crate) controls: mpsc::UnboundedSender<SessionControl>,
    pub(crate) status: watch::Receiver<SessionStatus>,
    pub(crate) burst_mode: bool,
}

pub(crate) struct ReconnectedTransport {
    pub(crate) transport: Box<dyn AdbTransport>,
    pub(crate) max_payload: usize,
    pub(crate) delayed_ack: bool,
}

type ReconnectFuture =
    Pin<Box<dyn Future<Output = Result<ReconnectedTransport, AdbClientError>> + Send>>;
type ReconnectConnector = Arc<dyn Fn() -> ReconnectFuture + Send + Sync>;

pub(crate) struct ReconnectOptions {
    pub(crate) policy: crate::client::AdbReconnectConfig,
    pub(crate) connector: ReconnectConnector,
}

#[derive(Clone, Copy)]
struct ConnectedSessionConfig {
    max_payload: usize,
    delayed_ack: bool,
    burst_mode: bool,
    delayed_ack_receive_window: u32,
}

struct SessionConfig {
    connected: ConnectedSessionConfig,
    reconnect: Option<ReconnectOptions>,
}

#[derive(Clone)]
enum FlowClosure {
    Local,
    Remote,
    Session(String),
}

struct OutboundFlowState {
    available: i64,
    closed: Option<FlowClosure>,
}

pub(crate) struct OutboundFlow {
    state: StdMutex<OutboundFlowState>,
    changed: Notify,
}

impl OutboundFlow {
    fn new(initial_credit: i64) -> Arc<Self> {
        Arc::new(Self {
            state: StdMutex::new(OutboundFlowState {
                available: initial_credit,
                closed: None,
            }),
            changed: Notify::new(),
        })
    }

    pub(crate) async fn reserve(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<OutboundReservation, AdbStreamError> {
        let bytes = i64::try_from(bytes).map_err(|_| AdbStreamError::SessionClosed {
            reason: "stream payload length overflow".to_owned(),
        })?;
        loop {
            let changed = self.changed.notified();
            {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(closed) = &state.closed {
                    return Err(closed.error());
                }
                if state.available >= bytes {
                    state.available -= bytes;
                    return Ok(OutboundReservation {
                        flow: Arc::clone(self),
                        bytes,
                        committed: false,
                    });
                }
            }
            changed.await;
        }
    }

    fn adjust(&self, delta: i64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.available = state.available.saturating_add(delta);
        drop(state);
        self.changed.notify_waiters();
    }

    fn close(&self, status: &StreamStatus) {
        let closure = match status {
            StreamStatus::Open => return,
            StreamStatus::LocalClosed => FlowClosure::Local,
            StreamStatus::RemoteClosed => FlowClosure::Remote,
            StreamStatus::SessionClosed(reason) => FlowClosure::Session(reason.clone()),
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed.get_or_insert(closure);
        drop(state);
        self.changed.notify_waiters();
    }
}

impl FlowClosure {
    fn error(&self) -> AdbStreamError {
        match self {
            Self::Local => AdbStreamError::StreamClosed,
            Self::Remote => AdbStreamError::RemoteClosed,
            Self::Session(reason) => AdbStreamError::SessionClosed {
                reason: reason.clone(),
            },
        }
    }
}

pub(crate) struct OutboundReservation {
    flow: Arc<OutboundFlow>,
    bytes: i64,
    committed: bool,
}

impl OutboundReservation {
    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for OutboundReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.flow.adjust(self.bytes);
        }
    }
}

struct StreamEntry {
    remote_id: Option<u32>,
    open_response: Option<oneshot::Sender<Result<OpenedStream, AdbClientError>>>,
    incoming_sender: mpsc::UnboundedSender<Bytes>,
    incoming_receiver: Option<mpsc::UnboundedReceiver<Bytes>>,
    status_sender: watch::Sender<StreamStatus>,
    write_response: Option<oneshot::Sender<Result<(), AdbStreamError>>>,
    outbound: Arc<OutboundFlow>,
    inbound_unacknowledged_bytes: u64,
    inbound_unacknowledged_packets: u64,
}

struct ClosedInboundAck {
    remote_id: u32,
    inbound_unacknowledged_bytes: u64,
    inbound_unacknowledged_packets: u64,
}

struct RouterState {
    streams: HashMap<u32, StreamEntry>,
    closed_inbound_acks: HashMap<u32, ClosedInboundAck>,
    next_local_id: u32,
}

impl RouterState {
    fn new() -> Self {
        Self {
            streams: HashMap::new(),
            closed_inbound_acks: HashMap::new(),
            next_local_id: FIRST_LOCAL_ID,
        }
    }

    fn allocate_local_id(&mut self) -> Option<u32> {
        for _ in 0..u32::MAX {
            let candidate = self.next_local_id;
            self.next_local_id = if candidate == u32::MAX {
                FIRST_LOCAL_ID
            } else {
                candidate + 1
            };

            if !self.streams.contains_key(&candidate)
                && !self.closed_inbound_acks.contains_key(&candidate)
            {
                return Some(candidate);
            }
        }
        None
    }

    fn remove_stream(&mut self, local_id: u32, status: StreamStatus) -> Option<StreamEntry> {
        let entry = self.streams.remove(&local_id)?;
        entry.outbound.close(&status);
        entry.status_sender.send_replace(status);
        Some(entry)
    }
}

enum SessionEnd {
    Closed,
    Disconnected(String),
}

impl SessionEnd {
    fn reason(&self) -> String {
        match self {
            Self::Closed => "session closed locally".to_owned(),
            Self::Disconnected(reason) => reason.clone(),
        }
    }

    fn status(&self) -> SessionStatus {
        match self {
            Self::Closed => SessionStatus::Closed,
            Self::Disconnected(reason) => SessionStatus::Disconnected(reason.clone()),
        }
    }
}

pub(crate) fn spawn_session(
    transport: Box<dyn AdbTransport>,
    max_payload: usize,
    delayed_ack: bool,
    burst_mode: bool,
    delayed_ack_receive_window: u32,
    reconnect: Option<ReconnectOptions>,
) -> SessionHandle {
    let (command_sender, command_receiver) = mpsc::channel(COMMAND_CAPACITY);
    let (control_sender, control_receiver) = mpsc::unbounded_channel();
    let (status_sender, status_receiver) = watch::channel(SessionStatus::Connected);
    let config = SessionConfig {
        connected: ConnectedSessionConfig {
            max_payload,
            delayed_ack,
            burst_mode,
            delayed_ack_receive_window,
        },
        reconnect,
    };

    tokio::spawn(run_session(
        transport,
        config,
        command_receiver,
        control_receiver,
        status_sender,
    ));

    SessionHandle {
        commands: command_sender,
        controls: control_sender,
        status: status_receiver,
        burst_mode,
    }
}

async fn run_session(
    mut transport: Box<dyn AdbTransport>,
    config: SessionConfig,
    mut commands: mpsc::Receiver<SessionCommand>,
    mut controls: mpsc::UnboundedReceiver<SessionControl>,
    status_sender: watch::Sender<SessionStatus>,
) {
    let SessionConfig {
        mut connected,
        reconnect,
    } = config;
    let session_end = loop {
        let mut router = RouterState::new();
        let session_end = run_connected_session(
            &mut transport,
            &mut router,
            connected,
            &mut commands,
            &mut controls,
        )
        .await;

        if !matches!(session_end, SessionEnd::Disconnected(_)) {
            finish_streams(&mut router, &session_end);
            break session_end;
        }
        finish_streams(&mut router, &session_end);

        let Some(options) = reconnect.as_ref() else {
            break session_end;
        };
        let _ = transport.close().await;
        status_sender.send_replace(SessionStatus::Reconnecting);
        let mut delay = options.policy.initial_delay;
        let mut reconnected = None;
        for _ in 0..options.policy.max_attempts {
            let sleep = tokio::time::sleep(delay);
            tokio::pin!(sleep);
            loop {
                tokio::select! {
                    biased;
                    control = controls.recv() => {
                        let Some(control) = control else {
                            return finish_session(
                                &mut transport,
                                SessionEnd::Closed,
                                &status_sender,
                            ).await;
                        };
                        if let Some(end) = handle_disconnected_control(control, session_end.reason()) {
                            return finish_session(&mut transport, end, &status_sender).await;
                        }
                    }
                    () = &mut sleep => break,
                }
            }

            let reconnect_attempt = (options.connector)();
            tokio::pin!(reconnect_attempt);
            let reconnect_result = loop {
                tokio::select! {
                    biased;
                    control = controls.recv() => {
                        let Some(control) = control else {
                            return finish_session(
                                &mut transport,
                                SessionEnd::Closed,
                                &status_sender,
                            ).await;
                        };
                        if let Some(end) = handle_disconnected_control(control, session_end.reason()) {
                            return finish_session(&mut transport, end, &status_sender).await;
                        }
                    }
                    result = &mut reconnect_attempt => break result,
                }
            };
            match reconnect_result {
                Ok(connection) => {
                    reconnected = Some(connection);
                    break;
                }
                Err(_) => {
                    delay = delay
                        .checked_mul(2)
                        .unwrap_or(options.policy.max_delay)
                        .min(options.policy.max_delay);
                }
            }
        }
        let Some(connection) = reconnected else {
            break session_end;
        };
        transport = connection.transport;
        connected.max_payload = connection.max_payload;
        connected.delayed_ack = connection.delayed_ack;
        status_sender.send_replace(SessionStatus::Connected);
    };
    finish_session(&mut transport, session_end, &status_sender).await;
}

async fn finish_session(
    transport: &mut Box<dyn AdbTransport>,
    session_end: SessionEnd,
    status_sender: &watch::Sender<SessionStatus>,
) {
    status_sender.send_replace(session_end.status());
    if matches!(session_end, SessionEnd::Disconnected(_)) {
        let _ = transport.close().await;
    }
}

fn handle_disconnected_control(control: SessionControl, reason: String) -> Option<SessionEnd> {
    match control {
        SessionControl::CloseStream { response, .. } => {
            if let Some(response) = response {
                let _ = response.send(Err(AdbStreamError::SessionClosed { reason }));
            }
            None
        }
        SessionControl::CloseSession { response } => {
            let _ = response.send(Ok(()));
            Some(SessionEnd::Closed)
        }
    }
}

async fn run_connected_session(
    transport: &mut Box<dyn AdbTransport>,
    router: &mut RouterState,
    config: ConnectedSessionConfig,
    commands: &mut mpsc::Receiver<SessionCommand>,
    controls: &mut mpsc::UnboundedReceiver<SessionControl>,
) -> SessionEnd {
    loop {
        if commands.is_closed() && controls.is_closed() {
            return close_abandoned_session(&mut **transport).await;
        }
        tokio::select! {
            biased;
            control = controls.recv(), if !controls.is_closed() => {
                let Some(control) = control else {
                    continue;
                };
                match handle_control(&mut **transport, router, control).await {
                    Ok(Some(session_end)) => return session_end,
                    Ok(None) => {}
                    Err(reason) => return SessionEnd::Disconnected(reason),
                }
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    return close_abandoned_session(&mut **transport).await;
                };

                match handle_command(
                    &mut **transport,
                    router,
                    command,
                    config.max_payload,
                    config.delayed_ack,
                    config.burst_mode,
                    config.delayed_ack_receive_window,
                )
                .await
                {
                    Ok(Some(session_end)) => return session_end,
                    Ok(None) => {}
                    Err(reason) => return SessionEnd::Disconnected(reason),
                }
            }
            packet = transport.read_packet() => {
                match packet {
                    Ok(packet) => {
                        if let Err(reason) = handle_packet(
                            &mut **transport,
                            router,
                            packet,
                            config.delayed_ack,
                            config.burst_mode,
                            config.delayed_ack_receive_window,
                        ).await {
                            return SessionEnd::Disconnected(reason);
                        }
                    }
                    Err(error) if is_idle_read_timeout(&error) => tokio::task::yield_now().await,
                    Err(error) => return SessionEnd::Disconnected(error.to_string()),
                }
            }
        }
    }
}

async fn close_abandoned_session(transport: &mut dyn AdbTransport) -> SessionEnd {
    match transport.close().await {
        Ok(()) => SessionEnd::Closed,
        Err(error) => SessionEnd::Disconnected(error.to_string()),
    }
}

fn is_idle_read_timeout(error: &AdbTransportError) -> bool {
    matches!(
        error,
        AdbTransportError::Timeout {
            operation: TransportOperation::Read,
            ..
        }
    )
}

async fn handle_command(
    transport: &mut dyn AdbTransport,
    router: &mut RouterState,
    command: SessionCommand,
    max_payload: usize,
    delayed_ack: bool,
    burst_mode: bool,
    delayed_ack_receive_window: u32,
) -> Result<Option<SessionEnd>, String> {
    match command {
        SessionCommand::Open { service, response } => {
            handle_open(
                transport,
                router,
                service,
                response,
                delayed_ack,
                delayed_ack_receive_window,
            )
            .await?;
            Ok(None)
        }
        SessionCommand::Write {
            local_id,
            remote_id,
            payload,
            response,
        } => {
            handle_write(
                transport,
                router,
                local_id,
                remote_id,
                payload,
                response,
                max_payload,
                burst_mode,
            )
            .await?;
            Ok(None)
        }
        SessionCommand::AcknowledgeInbound {
            local_id,
            remote_id,
            bytes,
            response,
        } => {
            match acknowledge_inbound(transport, router, local_id, remote_id, bytes, delayed_ack)
                .await
            {
                Ok(true) => {
                    let _ = response.send(Ok(()));
                }
                Ok(false) => {
                    let _ = response.send(Err(AdbStreamError::StreamClosed));
                }
                Err(reason) => {
                    let _ = response.send(Err(AdbStreamError::SessionClosed {
                        reason: reason.clone(),
                    }));
                    return Err(reason);
                }
            }
            Ok(None)
        }
    }
}

async fn handle_control(
    transport: &mut dyn AdbTransport,
    router: &mut RouterState,
    control: SessionControl,
) -> Result<Option<SessionEnd>, String> {
    match control {
        SessionControl::CloseStream {
            local_id,
            remote_id,
            response,
        } => {
            close_stream(transport, router, local_id, remote_id, response).await?;
            Ok(None)
        }
        SessionControl::CloseSession { response } => {
            let result = transport.close().await;
            match result {
                Ok(()) => {
                    let _ = response.send(Ok(()));
                    Ok(Some(SessionEnd::Closed))
                }
                Err(error) => {
                    let reason = error.to_string();
                    let _ = response.send(Err(AdbClientError::Transport(error)));
                    Ok(Some(SessionEnd::Disconnected(reason)))
                }
            }
        }
    }
}

async fn handle_open(
    transport: &mut dyn AdbTransport,
    router: &mut RouterState,
    service: Bytes,
    response: oneshot::Sender<Result<OpenedStream, AdbClientError>>,
    delayed_ack: bool,
    delayed_ack_receive_window: u32,
) -> Result<(), String> {
    let Some(local_id) = router.allocate_local_id() else {
        let _ = response.send(Err(AdbClientError::LocalIdExhausted));
        return Ok(());
    };

    let send_window = if delayed_ack {
        delayed_ack_receive_window
    } else {
        0
    };
    let packet = match AdbPacket::new(AdbCommand::Open, local_id, send_window, service) {
        Ok(packet) => packet,
        Err(error) => {
            let _ = response.send(Err(AdbClientError::Protocol(error)));
            return Ok(());
        }
    };

    let (incoming_sender, incoming_receiver) = mpsc::unbounded_channel();
    let (status_sender, _) = watch::channel(StreamStatus::Open);
    let outbound = OutboundFlow::new(0);
    router.streams.insert(
        local_id,
        StreamEntry {
            remote_id: None,
            open_response: Some(response),
            incoming_sender,
            incoming_receiver: Some(incoming_receiver),
            status_sender,
            write_response: None,
            outbound,
            inbound_unacknowledged_bytes: 0,
            inbound_unacknowledged_packets: 0,
        },
    );

    transport
        .write_packet(&packet)
        .await
        .map_err(|error| error.to_string())
}

#[allow(clippy::too_many_arguments)]
async fn handle_write(
    transport: &mut dyn AdbTransport,
    router: &mut RouterState,
    local_id: u32,
    remote_id: u32,
    payload: Bytes,
    response: oneshot::Sender<Result<(), AdbStreamError>>,
    max_payload: usize,
    burst_mode: bool,
) -> Result<(), String> {
    if payload.len() > max_payload {
        let _ = response.send(Err(AdbStreamError::PayloadTooLarge {
            limit: max_payload,
            actual: payload.len(),
        }));
        return Ok(());
    }

    let Some(entry) = router.streams.get_mut(&local_id) else {
        let _ = response.send(Err(AdbStreamError::StreamClosed));
        return Ok(());
    };
    if entry.remote_id != Some(remote_id) {
        let _ = response.send(Err(AdbStreamError::RemoteClosed));
        return Ok(());
    }
    if !burst_mode && entry.write_response.is_some() {
        let _ = response.send(Err(AdbStreamError::SessionClosed {
            reason: "stream write flow-control state is inconsistent".to_owned(),
        }));
        return Ok(());
    }

    let packet = match AdbPacket::new(AdbCommand::Write, local_id, remote_id, payload) {
        Ok(packet) => packet,
        Err(error) => {
            let _ = response.send(Err(AdbStreamError::SessionClosed {
                reason: error.to_string(),
            }));
            return Ok(());
        }
    };
    transport
        .write_packet(&packet)
        .await
        .map_err(|error| error.to_string())?;

    if burst_mode {
        let _ = response.send(Ok(()));
    } else {
        entry.write_response = Some(response);
    }
    Ok(())
}

async fn acknowledge_inbound(
    transport: &mut dyn AdbTransport,
    router: &mut RouterState,
    local_id: u32,
    remote_id: u32,
    bytes: u32,
    delayed_ack: bool,
) -> Result<bool, String> {
    let should_acknowledge = router.streams.get(&local_id).is_some_and(|entry| {
        entry.remote_id == Some(remote_id)
            && entry.inbound_unacknowledged_packets != 0
            && (!delayed_ack || u64::from(bytes) <= entry.inbound_unacknowledged_bytes)
    }) || router
        .closed_inbound_acks
        .get(&local_id)
        .is_some_and(|entry| {
            entry.remote_id == remote_id
                && entry.inbound_unacknowledged_packets != 0
                && (!delayed_ack || u64::from(bytes) <= entry.inbound_unacknowledged_bytes)
        });
    if !should_acknowledge {
        return Ok(false);
    }

    let payload = if delayed_ack {
        Bytes::copy_from_slice(&bytes.to_le_bytes())
    } else {
        Bytes::new()
    };
    let packet = AdbPacket::new(AdbCommand::Okay, local_id, remote_id, payload)
        .map_err(|error| error.to_string())?;
    transport
        .write_packet(&packet)
        .await
        .map_err(|error| error.to_string())?;
    if let Some(entry) = router.streams.get_mut(&local_id) {
        entry.inbound_unacknowledged_packets =
            entry.inbound_unacknowledged_packets.saturating_sub(1);
        if delayed_ack {
            entry.inbound_unacknowledged_bytes = entry
                .inbound_unacknowledged_bytes
                .saturating_sub(u64::from(bytes));
        }
    }
    let remove_closed_ack = router
        .closed_inbound_acks
        .get_mut(&local_id)
        .is_some_and(|entry| {
            entry.inbound_unacknowledged_packets =
                entry.inbound_unacknowledged_packets.saturating_sub(1);
            if delayed_ack {
                entry.inbound_unacknowledged_bytes = entry
                    .inbound_unacknowledged_bytes
                    .saturating_sub(u64::from(bytes));
            }
            entry.inbound_unacknowledged_packets == 0
        });
    if remove_closed_ack {
        router.closed_inbound_acks.remove(&local_id);
    }
    Ok(true)
}

async fn close_stream(
    transport: &mut dyn AdbTransport,
    router: &mut RouterState,
    local_id: u32,
    remote_id: u32,
    response: Option<oneshot::Sender<Result<(), AdbStreamError>>>,
) -> Result<(), String> {
    let Some(mut entry) = router.remove_stream(local_id, StreamStatus::LocalClosed) else {
        router.closed_inbound_acks.remove(&local_id);
        if let Some(response) = response {
            let _ = response.send(Ok(()));
        }
        return Ok(());
    };

    if let Some(open_response) = entry.open_response.take() {
        let _ = open_response.send(Err(AdbClientError::StreamRejected { local_id }));
    }
    if let Some(write_response) = entry.write_response.take() {
        let _ = write_response.send(Err(AdbStreamError::StreamClosed));
    }
    if entry.remote_id == Some(remote_id) {
        let packet = empty_packet(AdbCommand::Close, local_id, remote_id)?;
        if let Err(error) = transport.write_packet(&packet).await {
            let reason = error.to_string();
            if let Some(response) = response {
                let _ = response.send(Err(AdbStreamError::SessionClosed {
                    reason: reason.clone(),
                }));
            }
            return Err(reason);
        }
    }

    if let Some(response) = response {
        let _ = response.send(Ok(()));
    }
    Ok(())
}

async fn handle_packet(
    transport: &mut dyn AdbTransport,
    router: &mut RouterState,
    packet: AdbPacket,
    delayed_ack: bool,
    burst_mode: bool,
    delayed_ack_receive_window: u32,
) -> Result<(), String> {
    match packet.command {
        AdbCommand::Okay => handle_okay(transport, router, packet, delayed_ack, burst_mode).await,
        AdbCommand::Write => {
            handle_incoming_write(
                transport,
                router,
                packet,
                delayed_ack,
                delayed_ack_receive_window,
            )
            .await
        }
        AdbCommand::Close => handle_remote_close(transport, router, packet).await,
        AdbCommand::Open => reject_remote_open(transport, packet).await,
        command => Err(format!(
            "unexpected ADB packet after connection: {command:?}"
        )),
    }
}

async fn handle_okay(
    transport: &mut dyn AdbTransport,
    router: &mut RouterState,
    packet: AdbPacket,
    delayed_ack: bool,
    burst_mode: bool,
) -> Result<(), String> {
    let delayed_ack_delta = validate_okay_payload(&packet, delayed_ack)?;
    let remote_id = packet.arg0;
    let local_id = packet.arg1;
    if local_id == 0 {
        return close_unknown_stream(transport, remote_id).await;
    }
    if remote_id == 0 {
        return if router.streams.contains_key(&local_id) {
            close_invalid_stream(transport, router, local_id, remote_id).await
        } else {
            Ok(())
        };
    }

    let Some(entry) = router.streams.get_mut(&local_id) else {
        return close_unknown_stream(transport, remote_id).await;
    };

    match entry.remote_id {
        None => {
            entry.remote_id = Some(remote_id);
            let Some(response) = entry.open_response.take() else {
                return close_invalid_stream(transport, router, local_id, remote_id).await;
            };
            let Some(incoming) = entry.incoming_receiver.take() else {
                return close_invalid_stream(transport, router, local_id, remote_id).await;
            };
            let opened = OpenedStream {
                local_id,
                remote_id,
                incoming,
                status: entry.status_sender.subscribe(),
                outbound: Arc::clone(&entry.outbound),
            };
            if let Some(delta) = delayed_ack_delta {
                entry.outbound.adjust(delta);
            }
            if response.send(Ok(opened)).is_err() {
                return close_invalid_stream(transport, router, local_id, remote_id).await;
            }
        }
        Some(expected_remote_id) if expected_remote_id == remote_id => {
            if let Some(response) = entry.write_response.take() {
                let _ = response.send(Ok(()));
            }
            if delayed_ack && burst_mode {
                if let Some(delta) = delayed_ack_delta {
                    entry.outbound.adjust(delta);
                }
            }
        }
        Some(_) => {
            return close_unknown_stream(transport, remote_id).await;
        }
    }
    let _ = entry;
    Ok(())
}

async fn handle_incoming_write(
    transport: &mut dyn AdbTransport,
    router: &mut RouterState,
    packet: AdbPacket,
    delayed_ack: bool,
    delayed_ack_receive_window: u32,
) -> Result<(), String> {
    let remote_id = packet.arg0;
    let local_id = packet.arg1;
    if local_id == 0 {
        return close_unknown_stream(transport, remote_id).await;
    }
    if remote_id == 0 {
        return if router.streams.contains_key(&local_id) {
            close_invalid_stream(transport, router, local_id, remote_id).await
        } else {
            Ok(())
        };
    }

    let Some(entry) = router.streams.get_mut(&local_id) else {
        return close_unknown_stream(transport, remote_id).await;
    };
    if entry.remote_id != Some(remote_id) {
        return close_unknown_stream(transport, remote_id).await;
    }
    let payload_bytes =
        u64::try_from(packet.payload.len()).map_err(|_| "payload length overflow")?;
    let exceeds_window = delayed_ack
        && entry
            .inbound_unacknowledged_bytes
            .saturating_add(payload_bytes)
            > u64::from(delayed_ack_receive_window);
    if (!delayed_ack && entry.inbound_unacknowledged_packets != 0)
        || exceeds_window
        || entry.incoming_sender.send(packet.payload).is_err()
    {
        return close_invalid_stream(transport, router, local_id, remote_id).await;
    }
    if delayed_ack {
        entry.inbound_unacknowledged_bytes = entry
            .inbound_unacknowledged_bytes
            .saturating_add(payload_bytes);
    }
    entry.inbound_unacknowledged_packets = entry.inbound_unacknowledged_packets.saturating_add(1);
    Ok(())
}

async fn handle_remote_close(
    transport: &mut dyn AdbTransport,
    router: &mut RouterState,
    packet: AdbPacket,
) -> Result<(), String> {
    require_empty_payload(&packet)?;
    let remote_id = packet.arg0;
    let local_id = packet.arg1;
    let Some(entry) = router.streams.get(&local_id) else {
        return Ok(());
    };
    if entry.remote_id.is_some() && entry.remote_id != Some(remote_id) {
        return close_unknown_stream(transport, remote_id).await;
    }

    let Some(mut entry) = router.remove_stream(local_id, StreamStatus::RemoteClosed) else {
        return Ok(());
    };
    if entry.inbound_unacknowledged_packets != 0 {
        router.closed_inbound_acks.insert(
            local_id,
            ClosedInboundAck {
                remote_id,
                inbound_unacknowledged_bytes: entry.inbound_unacknowledged_bytes,
                inbound_unacknowledged_packets: entry.inbound_unacknowledged_packets,
            },
        );
    }
    if let Some(response) = entry.open_response.take() {
        let _ = response.send(Err(AdbClientError::StreamRejected { local_id }));
    }
    if let Some(response) = entry.write_response.take() {
        let _ = response.send(Err(AdbStreamError::RemoteClosed));
    }
    if remote_id != 0 {
        let response = empty_packet(AdbCommand::Close, local_id, remote_id)?;
        transport
            .write_packet(&response)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn reject_remote_open(
    transport: &mut dyn AdbTransport,
    packet: AdbPacket,
) -> Result<(), String> {
    if packet.arg0 == 0 {
        return Ok(());
    }
    let response = empty_packet(AdbCommand::Close, 0, packet.arg0)?;
    transport
        .write_packet(&response)
        .await
        .map_err(|error| error.to_string())
}

async fn close_invalid_stream(
    transport: &mut dyn AdbTransport,
    router: &mut RouterState,
    local_id: u32,
    remote_id: u32,
) -> Result<(), String> {
    if let Some(mut entry) = router.remove_stream(local_id, StreamStatus::RemoteClosed) {
        if let Some(response) = entry.open_response.take() {
            let _ = response.send(Err(AdbClientError::StreamRejected { local_id }));
        }
        if let Some(response) = entry.write_response.take() {
            let _ = response.send(Err(AdbStreamError::RemoteClosed));
        }
    }
    let response = empty_packet(AdbCommand::Close, local_id, remote_id)?;
    transport
        .write_packet(&response)
        .await
        .map_err(|error| error.to_string())
}

async fn close_unknown_stream(
    transport: &mut dyn AdbTransport,
    remote_id: u32,
) -> Result<(), String> {
    if remote_id == 0 {
        return Ok(());
    }
    let response = empty_packet(AdbCommand::Close, 0, remote_id)?;
    transport
        .write_packet(&response)
        .await
        .map_err(|error| error.to_string())
}

fn require_empty_payload(packet: &AdbPacket) -> Result<(), String> {
    if packet.payload.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "ADB {:?} packet contains an unexpected payload",
            packet.command
        ))
    }
}

fn validate_okay_payload(packet: &AdbPacket, delayed_ack: bool) -> Result<Option<i64>, String> {
    if delayed_ack {
        delayed_ack_delta(packet).map(Some)
    } else if packet.payload.is_empty() {
        Ok(None)
    } else {
        Err(format!(
            "ADB OKAY packet contains an unexpected payload of {} bytes",
            packet.payload.len()
        ))
    }
}

fn delayed_ack_delta(packet: &AdbPacket) -> Result<i64, String> {
    if packet.payload.len() != std::mem::size_of::<u32>() {
        return Err("ADB delayed-ack packet does not contain a byte count".to_owned());
    }
    let bytes = i32::from_le_bytes(
        packet.payload[..std::mem::size_of::<u32>()]
            .try_into()
            .map_err(|_| "invalid delayed-ack byte count")?,
    );
    Ok(i64::from(bytes))
}

fn empty_packet(command: AdbCommand, arg0: u32, arg1: u32) -> Result<AdbPacket, String> {
    AdbPacket::new(command, arg0, arg1, Bytes::new()).map_err(|error| error.to_string())
}

fn finish_streams(router: &mut RouterState, session_end: &SessionEnd) {
    let reason = session_end.reason();
    for (_, mut entry) in router.streams.drain() {
        entry
            .status_sender
            .send_replace(StreamStatus::SessionClosed(reason.clone()));
        entry
            .outbound
            .close(&StreamStatus::SessionClosed(reason.clone()));
        if let Some(response) = entry.open_response.take() {
            let _ = response.send(Err(AdbClientError::SessionClosed {
                reason: reason.clone(),
            }));
        }
        if let Some(response) = entry.write_response.take() {
            let _ = response.send(Err(AdbStreamError::SessionClosed {
                reason: reason.clone(),
            }));
        }
    }
}
