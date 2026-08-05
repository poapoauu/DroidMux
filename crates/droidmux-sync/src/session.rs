use adb_client::{AdbClient, AdbStream};
use bytes::{Bytes, BytesMut};

use crate::{SyncError, SyncProtocolError, TransferCancellation};

pub(crate) const SYNC_PATH_MAX: usize = 1024;
pub(crate) const SYNC_ERROR_MAX: usize = 64 * 1024;

pub(crate) struct SyncSession {
    stream: AdbStream,
    buffered: BytesMut,
}

impl SyncSession {
    pub(crate) async fn open(client: &AdbClient) -> Result<Self, SyncError> {
        Ok(Self {
            stream: client.open_service("sync:").await?,
            buffered: BytesMut::new(),
        })
    }

    pub(crate) async fn write_length_prefixed(
        &self,
        id: &[u8; 4],
        payload: &[u8],
    ) -> Result<(), SyncError> {
        let message = length_prefixed(*id, payload)?;
        self.write_all(&message).await
    }

    pub(crate) async fn write_length_prefixed_cancellable(
        &self,
        id: &[u8; 4],
        payload: &[u8],
        cancellation: &TransferCancellation,
    ) -> Result<(), SyncError> {
        let message = length_prefixed(*id, payload)?;
        self.write_all_cancellable(&message, cancellation).await
    }

    pub(crate) async fn write_id_value(&self, id: &[u8; 4], value: u32) -> Result<(), SyncError> {
        let message = id_value(*id, value);
        self.write_all(&message).await
    }

    pub(crate) async fn write_id_value_cancellable(
        &self,
        id: &[u8; 4],
        value: u32,
        cancellation: &TransferCancellation,
    ) -> Result<(), SyncError> {
        let message = id_value(*id, value);
        self.write_all_cancellable(&message, cancellation).await
    }

    pub(crate) async fn write_data(
        &self,
        data: &[u8],
        cancellation: &TransferCancellation,
    ) -> Result<(), SyncError> {
        self.write_length_prefixed_cancellable(b"DATA", data, cancellation)
            .await
    }

    pub(crate) async fn write_send_v2(
        &self,
        path: &[u8],
        mode: u32,
        flags: u32,
        cancellation: &TransferCancellation,
    ) -> Result<(), SyncError> {
        let mut message = length_prefixed(*b"SND2", path)?;
        message.extend_from_slice(b"SND2");
        message.extend_from_slice(&mode.to_le_bytes());
        message.extend_from_slice(&flags.to_le_bytes());
        self.write_all_cancellable(&message, cancellation).await
    }

    pub(crate) async fn write_recv_v2(
        &self,
        path: &[u8],
        flags: u32,
        cancellation: &TransferCancellation,
    ) -> Result<(), SyncError> {
        let mut message = length_prefixed(*b"RCV2", path)?;
        message.extend_from_slice(b"RCV2");
        message.extend_from_slice(&flags.to_le_bytes());
        self.write_all_cancellable(&message, cancellation).await
    }

    pub(crate) async fn read_id(&mut self) -> Result<[u8; 4], SyncError> {
        let bytes = self.read_exact(4, "response identifier").await?;
        Ok(four_bytes(&bytes))
    }

    pub(crate) async fn read_id_cancellable(
        &mut self,
        cancellation: &TransferCancellation,
    ) -> Result<[u8; 4], SyncError> {
        let bytes = self
            .read_exact_cancellable(4, "response identifier", cancellation)
            .await?;
        Ok(four_bytes(&bytes))
    }

    pub(crate) async fn read_u32(&mut self, context: &'static str) -> Result<u32, SyncError> {
        let bytes = self.read_exact(4, context).await?;
        Ok(u32::from_le_bytes(four_bytes(&bytes)))
    }

    pub(crate) async fn read_u64(&mut self, context: &'static str) -> Result<u64, SyncError> {
        let bytes = self.read_exact(8, context).await?;
        Ok(u64::from_le_bytes(eight_bytes(&bytes)))
    }

    pub(crate) async fn read_u32_cancellable(
        &mut self,
        context: &'static str,
        cancellation: &TransferCancellation,
    ) -> Result<u32, SyncError> {
        let bytes = self
            .read_exact_cancellable(4, context, cancellation)
            .await?;
        Ok(u32::from_le_bytes(four_bytes(&bytes)))
    }

    pub(crate) async fn read_bytes(
        &mut self,
        length: usize,
        context: &'static str,
    ) -> Result<Bytes, SyncError> {
        self.read_exact(length, context).await
    }

    pub(crate) async fn skip(
        &mut self,
        length: usize,
        context: &'static str,
    ) -> Result<(), SyncError> {
        let _ = self.read_exact(length, context).await?;
        Ok(())
    }

    pub(crate) async fn read_bytes_cancellable(
        &mut self,
        length: usize,
        context: &'static str,
        cancellation: &TransferCancellation,
    ) -> Result<Bytes, SyncError> {
        self.read_exact_cancellable(length, context, cancellation)
            .await
    }

    pub(crate) async fn read_remote_error(&mut self) -> Result<SyncError, SyncError> {
        let length = usize::try_from(self.read_u32("FAIL message length").await?)
            .expect("u32 fits every supported platform");
        if length > SYNC_ERROR_MAX {
            return Err(field_too_large("FAIL message", SYNC_ERROR_MAX, length));
        }
        let message = self.read_bytes(length, "FAIL message").await?;
        Ok(remote_error(&message))
    }

    pub(crate) async fn read_remote_error_cancellable(
        &mut self,
        cancellation: &TransferCancellation,
    ) -> Result<SyncError, SyncError> {
        let length = usize::try_from(
            self.read_u32_cancellable("FAIL message length", cancellation)
                .await?,
        )
        .expect("u32 fits every supported platform");
        if length > SYNC_ERROR_MAX {
            return Err(field_too_large("FAIL message", SYNC_ERROR_MAX, length));
        }
        let message = self
            .read_exact_cancellable(length, "FAIL message", cancellation)
            .await?;
        Ok(remote_error(&message))
    }

    pub(crate) async fn finish<T>(self, operation: Result<T, SyncError>) -> Result<T, SyncError> {
        let cleanup = if operation.is_ok() {
            self.write_id_value(b"QUIT", 0).await
        } else {
            Ok(())
        };
        let close = self.stream.close().await.map_err(SyncError::from);

        match operation {
            Err(error) => Err(error),
            Ok(value) => {
                cleanup?;
                close?;
                Ok(value)
            }
        }
    }

    async fn write_all(&self, bytes: &[u8]) -> Result<(), SyncError> {
        let max_payload = self.stream.max_payload();
        if max_payload == 0 {
            return Err(SyncProtocolError::InvalidPayloadLimit.into());
        }
        for chunk in bytes.chunks(max_payload) {
            self.stream.write(Bytes::copy_from_slice(chunk)).await?;
        }
        Ok(())
    }

    async fn write_all_cancellable(
        &self,
        bytes: &[u8],
        cancellation: &TransferCancellation,
    ) -> Result<(), SyncError> {
        let max_payload = self.stream.max_payload();
        if max_payload == 0 {
            return Err(SyncProtocolError::InvalidPayloadLimit.into());
        }
        for chunk in bytes.chunks(max_payload) {
            let write = self.stream.write(Bytes::copy_from_slice(chunk));
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(SyncError::Canceled),
                result = write => result?,
            }
        }
        Ok(())
    }

    async fn read_exact(
        &mut self,
        length: usize,
        context: &'static str,
    ) -> Result<Bytes, SyncError> {
        while self.buffered.len() < length {
            let Some(payload) = self.stream.read().await? else {
                return Err(SyncProtocolError::UnexpectedEof { context }.into());
            };
            self.buffered.extend_from_slice(&payload);
        }
        Ok(self.buffered.split_to(length).freeze())
    }

    async fn read_exact_cancellable(
        &mut self,
        length: usize,
        context: &'static str,
        cancellation: &TransferCancellation,
    ) -> Result<Bytes, SyncError> {
        while self.buffered.len() < length {
            let read = self.stream.read();
            let payload = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(SyncError::Canceled),
                result = read => result?,
            };
            let Some(payload) = payload else {
                return Err(SyncProtocolError::UnexpectedEof { context }.into());
            };
            self.buffered.extend_from_slice(&payload);
        }
        Ok(self.buffered.split_to(length).freeze())
    }
}

pub(crate) fn validate_remote_path(path: &str) -> Result<(), SyncProtocolError> {
    if path.is_empty() || path.as_bytes().contains(&0) {
        return Err(SyncProtocolError::InvalidRemotePath);
    }
    if path.len() > SYNC_PATH_MAX {
        return Err(SyncProtocolError::PathTooLong {
            limit: SYNC_PATH_MAX,
            actual: path.len(),
        });
    }
    Ok(())
}

fn length_prefixed(id: [u8; 4], payload: &[u8]) -> Result<BytesMut, SyncError> {
    let length = u32::try_from(payload.len())
        .map_err(|_| field_too_large("request payload", u32::MAX as usize, payload.len()))?;
    let mut message = BytesMut::with_capacity(8 + payload.len());
    message.extend_from_slice(&id);
    message.extend_from_slice(&length.to_le_bytes());
    message.extend_from_slice(payload);
    Ok(message)
}

fn id_value(id: [u8; 4], value: u32) -> [u8; 8] {
    let mut message = [0_u8; 8];
    message[..4].copy_from_slice(&id);
    message[4..].copy_from_slice(&value.to_le_bytes());
    message
}

fn four_bytes(bytes: &[u8]) -> [u8; 4] {
    bytes
        .try_into()
        .expect("four bytes were requested from the stream buffer")
}

fn eight_bytes(bytes: &[u8]) -> [u8; 8] {
    bytes
        .try_into()
        .expect("eight bytes were requested from the stream buffer")
}

fn field_too_large(field: &'static str, limit: usize, actual: usize) -> SyncError {
    SyncProtocolError::FieldTooLarge {
        field,
        limit,
        actual,
    }
    .into()
}

fn remote_error(message: &[u8]) -> SyncError {
    SyncProtocolError::Remote(String::from_utf8_lossy(message).into_owned()).into()
}
