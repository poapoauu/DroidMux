use std::collections::HashMap;

use adb_protocol::{AdbCommand, AdbPacket};
use adb_transport::{AdbTransport, AdbTransportError, TransportOperation};
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, watch};

use crate::{AdbClientError, AdbStreamError};

const COMMAND_CAPACITY: usize = 64;
const INCOMING_STREAM_CAPACITY: usize = 1;
const FIRST_LOCAL_ID: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SessionStatus {
    Connected,
    Closed,
    Disconnected(String),
}

impl SessionStatus {
    pub(crate) fn closure_reason(&self) -> Option<String> {
        match self {
            Self::Connected => None,
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
    pub(crate) incoming: mpsc::Receiver<Bytes>,
    pub(crate) status: watch::Receiver<StreamStatus>,
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
    },
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
    pub(crate) status: watch::Receiver<SessionStatus>,
}

struct StreamEntry {
    remote_id: Option<u32>,
    open_response: Option<oneshot::Sender<Result<OpenedStream, AdbClientError>>>,
    incoming_sender: mpsc::Sender<Bytes>,
    incoming_receiver: Option<mpsc::Receiver<Bytes>>,
    status_sender: watch::Sender<StreamStatus>,
    write_response: Option<oneshot::Sender<Result<(), AdbStreamError>>>,
    awaiting_inbound_ack: bool,
}

struct RouterState {
    streams: HashMap<u32, StreamEntry>,
    next_local_id: u32,
}

impl RouterState {
    fn new() -> Self {
        Self {
            streams: HashMap::new(),
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

            if !self.streams.contains_key(&candidate) {
                return Some(candidate);
            }
        }
        None
    }

    fn remove_stream(&mut self, local_id: u32, status: StreamStatus) -> Option<StreamEntry> {
        let entry = self.streams.remove(&local_id)?;
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

pub(crate) fn spawn_session(transport: Box<dyn AdbTransport>, max_payload: usize) -> SessionHandle {
    let (command_sender, command_receiver) = mpsc::channel(COMMAND_CAPACITY);
    let (status_sender, status_receiver) = watch::channel(SessionStatus::Connected);

    tokio::spawn(run_session(
        transport,
        max_payload,
        command_receiver,
        status_sender,
    ));

    SessionHandle {
        commands: command_sender,
        status: status_receiver,
    }
}

async fn run_session(
    mut transport: Box<dyn AdbTransport>,
    max_payload: usize,
    mut commands: mpsc::Receiver<SessionCommand>,
    status_sender: watch::Sender<SessionStatus>,
) {
    let mut router = RouterState::new();

    let session_end = loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    break close_abandoned_session(&mut *transport).await;
                };

                match handle_command(&mut *transport, &mut router, command, max_payload).await {
                    Ok(Some(session_end)) => break session_end,
                    Ok(None) => {}
                    Err(reason) => break SessionEnd::Disconnected(reason),
                }
            }
            packet = transport.read_packet() => {
                match packet {
                    Ok(packet) => {
                        if let Err(reason) = handle_packet(&mut *transport, &mut router, packet).await {
                            break SessionEnd::Disconnected(reason);
                        }
                    }
                    Err(error) if is_idle_read_timeout(&error) => tokio::task::yield_now().await,
                    Err(error) => break SessionEnd::Disconnected(error.to_string()),
                }
            }
        }
    };

    let disconnected = matches!(&session_end, SessionEnd::Disconnected(_));
    finish_streams(&mut router, &session_end);
    status_sender.send_replace(session_end.status());
    if disconnected {
        let _ = transport.close().await;
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
) -> Result<Option<SessionEnd>, String> {
    match command {
        SessionCommand::Open { service, response } => {
            handle_open(transport, router, service, response).await?;
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
            )
            .await?;
            Ok(None)
        }
        SessionCommand::AcknowledgeInbound {
            local_id,
            remote_id,
        } => {
            acknowledge_inbound(transport, router, local_id, remote_id).await?;
            Ok(None)
        }
        SessionCommand::CloseStream {
            local_id,
            remote_id,
            response,
        } => {
            close_stream(transport, router, local_id, remote_id, response).await?;
            Ok(None)
        }
        SessionCommand::CloseSession { response } => {
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
) -> Result<(), String> {
    let Some(local_id) = router.allocate_local_id() else {
        let _ = response.send(Err(AdbClientError::LocalIdExhausted));
        return Ok(());
    };

    let packet = match AdbPacket::new(AdbCommand::Open, local_id, 0, service) {
        Ok(packet) => packet,
        Err(error) => {
            let _ = response.send(Err(AdbClientError::Protocol(error)));
            return Ok(());
        }
    };

    let (incoming_sender, incoming_receiver) = mpsc::channel(INCOMING_STREAM_CAPACITY);
    let (status_sender, _) = watch::channel(StreamStatus::Open);
    router.streams.insert(
        local_id,
        StreamEntry {
            remote_id: None,
            open_response: Some(response),
            incoming_sender,
            incoming_receiver: Some(incoming_receiver),
            status_sender,
            write_response: None,
            awaiting_inbound_ack: false,
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
    if entry.write_response.is_some() {
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
    entry.write_response = Some(response);

    transport
        .write_packet(&packet)
        .await
        .map_err(|error| error.to_string())
}

async fn acknowledge_inbound(
    transport: &mut dyn AdbTransport,
    router: &mut RouterState,
    local_id: u32,
    remote_id: u32,
) -> Result<(), String> {
    let should_acknowledge = router
        .streams
        .get(&local_id)
        .is_some_and(|entry| entry.remote_id == Some(remote_id) && entry.awaiting_inbound_ack);
    if !should_acknowledge {
        return Ok(());
    }

    let packet = empty_packet(AdbCommand::Okay, local_id, remote_id)?;
    transport
        .write_packet(&packet)
        .await
        .map_err(|error| error.to_string())?;
    if let Some(entry) = router.streams.get_mut(&local_id) {
        entry.awaiting_inbound_ack = false;
    }
    Ok(())
}

async fn close_stream(
    transport: &mut dyn AdbTransport,
    router: &mut RouterState,
    local_id: u32,
    remote_id: u32,
    response: Option<oneshot::Sender<Result<(), AdbStreamError>>>,
) -> Result<(), String> {
    let Some(mut entry) = router.remove_stream(local_id, StreamStatus::LocalClosed) else {
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
) -> Result<(), String> {
    match packet.command {
        AdbCommand::Okay => handle_okay(transport, router, packet).await,
        AdbCommand::Write => handle_incoming_write(transport, router, packet).await,
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
) -> Result<(), String> {
    require_empty_payload(&packet)?;
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
            };
            if response.send(Ok(opened)).is_err() {
                return close_invalid_stream(transport, router, local_id, remote_id).await;
            }
        }
        Some(expected_remote_id) if expected_remote_id == remote_id => {
            if let Some(response) = entry.write_response.take() {
                let _ = response.send(Ok(()));
            }
        }
        Some(_) => {
            return close_unknown_stream(transport, remote_id).await;
        }
    }
    Ok(())
}

async fn handle_incoming_write(
    transport: &mut dyn AdbTransport,
    router: &mut RouterState,
    packet: AdbPacket,
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
    if entry.awaiting_inbound_ack || entry.incoming_sender.try_send(packet.payload).is_err() {
        return close_invalid_stream(transport, router, local_id, remote_id).await;
    }
    entry.awaiting_inbound_ack = true;
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

fn empty_packet(command: AdbCommand, arg0: u32, arg1: u32) -> Result<AdbPacket, String> {
    AdbPacket::new(command, arg0, arg1, Bytes::new()).map_err(|error| error.to_string())
}

fn finish_streams(router: &mut RouterState, session_end: &SessionEnd) {
    let reason = session_end.reason();
    for (_, mut entry) in router.streams.drain() {
        entry
            .status_sender
            .send_replace(StreamStatus::SessionClosed(reason.clone()));
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
