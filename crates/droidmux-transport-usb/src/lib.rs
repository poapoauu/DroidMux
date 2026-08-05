//! Asynchronous ADB USB Host transport.
//!
//! A dedicated reader thread keeps libusb's blocking reads independent from
//! Tokio task cancellation. Writes use the blocking pool and preserve the USB
//! transfer boundary between each ADB header and payload. Authentication and
//! stream multiplexing remain in the normal client layer.

use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use adb_protocol::{ADB_HEADER_LEN, AdbHeader, AdbPacket};
use adb_transport::{AdbTransport, AdbTransportError, TransportOperation};
use async_trait::async_trait;
use bytes::BytesMut;
use rusb::{Context, Device, DeviceHandle, Direction, TransferType, UsbContext};
use thiserror::Error;
use tokio::sync::mpsc;

const ADB_INTERFACE_SUBCLASS: u8 = 0x42;
const ADB_INTERFACE_PROTOCOL: u8 = 0x01;
const BULK_CLASS: u8 = 0xdc;
const BULK_SUBCLASS: u8 = 0x02;
const USB_READ_BUFFER: usize = 16 * 1024;
const USB_READ_POLL_INTERVAL: Duration = Duration::from_millis(100);
const USB_STALE_INPUT_LIMIT: usize = 32 * 1024 * 1024;
/// Default number of decoded ADB packets buffered per USB device.
pub const DEFAULT_READ_QUEUE_CAPACITY: usize = 16;

/// USB transport configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsbTransportConfig {
    /// Timeout for one USB bulk read.
    pub read_timeout: Duration,
    /// Timeout for one USB bulk write.
    pub write_timeout: Duration,
    /// Timeout for closing and releasing the USB interface.
    pub close_timeout: Duration,
    /// Number of decoded packets buffered by this device's reader thread.
    pub read_queue_capacity: usize,
}

impl Default for UsbTransportConfig {
    fn default() -> Self {
        Self {
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_secs(5),
            read_queue_capacity: DEFAULT_READ_QUEUE_CAPACITY,
        }
    }
}

/// A USB device that advertises an ADB interface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsbDeviceInfo {
    /// USB vendor identifier.
    pub vendor_id: u16,
    /// USB product identifier.
    pub product_id: u16,
    /// USB bus number.
    pub bus_number: u8,
    /// USB device address on the bus.
    pub address: u8,
    /// Best-effort manufacturer/product description.
    pub description: String,
}

/// Errors raised while discovering or opening a USB ADB device.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum UsbTransportError {
    /// A libusb operation failed.
    #[error("USB ADB operation failed: {0}")]
    Usb(#[from] rusb::Error),
    /// No matching device was found.
    #[error("no USB ADB device matched {vendor_id:04x}:{product_id:04x}")]
    DeviceNotFound {
        /// Requested USB vendor identifier.
        vendor_id: u16,
        /// Requested USB product identifier.
        product_id: u16,
    },
    /// The device did not expose a usable bulk ADB interface.
    #[error("USB device has no usable ADB bulk endpoints")]
    NoEndpoints,
    /// A bounded queue cannot be created with zero capacity.
    #[error("USB read queue capacity must be greater than zero")]
    InvalidReadQueueCapacity,
    /// A blocking USB worker could not be joined.
    #[error("USB worker failed: {0}")]
    Worker(String),
    /// An internal connection lock was poisoned.
    #[error("USB connection lock was poisoned")]
    LockPoisoned,
}

#[derive(Debug, Clone, Copy)]
struct Endpoint {
    interface: u8,
    address: u8,
    max_packet_size: usize,
}

#[derive(Debug, Clone, Copy)]
struct DeviceSelector {
    vendor_id: u16,
    product_id: u16,
    bus_number: Option<u8>,
    address: Option<u8>,
}

impl DeviceSelector {
    const fn by_product(vendor_id: u16, product_id: u16) -> Self {
        Self {
            vendor_id,
            product_id,
            bus_number: None,
            address: None,
        }
    }

    const fn exact(device: &UsbDeviceInfo) -> Self {
        Self {
            vendor_id: device.vendor_id,
            product_id: device.product_id,
            bus_number: Some(device.bus_number),
            address: Some(device.address),
        }
    }

    fn matches(self, device: &UsbDeviceInfo) -> bool {
        self.vendor_id == device.vendor_id
            && self.product_id == device.product_id
            && self
                .bus_number
                .is_none_or(|bus_number| bus_number == device.bus_number)
            && self.address.is_none_or(|address| address == device.address)
    }
}

#[derive(Debug)]
struct UsbConnection {
    handle: Arc<DeviceHandle<Context>>,
    write_endpoint: Endpoint,
    claimed_interfaces: Vec<u8>,
    closed: bool,
}

/// A connected packet-framed USB ADB transport.
#[derive(Debug)]
pub struct UsbTransport {
    connection: Arc<Mutex<UsbConnection>>,
    read_packets: mpsc::Receiver<Result<AdbPacket, AdbTransportError>>,
    reader_stop: Arc<AtomicBool>,
    reader_thread: Option<JoinHandle<()>>,
    peer: UsbDeviceInfo,
    config: UsbTransportConfig,
}

impl UsbTransport {
    /// Connects to the first USB ADB device with the given VID/PID.
    ///
    /// # Errors
    ///
    /// Returns an error when libusb cannot enumerate or open the requested
    /// device, or when the device has no usable ADB bulk endpoints.
    pub async fn connect(
        vendor_id: u16,
        product_id: u16,
        config: UsbTransportConfig,
    ) -> Result<Self, UsbTransportError> {
        run_blocking(move || {
            connect_matching(DeviceSelector::by_product(vendor_id, product_id), config)
        })
        .await
    }

    /// Connects to the exact USB device returned by [`Self::discover`].
    ///
    /// Unlike [`Self::connect`], this also matches the USB bus number and
    /// device address so identical devices cannot be confused.
    ///
    /// # Errors
    ///
    /// Returns an error when the selected device is no longer attached or its
    /// ADB interface cannot be opened.
    pub async fn connect_device(
        device: &UsbDeviceInfo,
        config: UsbTransportConfig,
    ) -> Result<Self, UsbTransportError> {
        let selector = DeviceSelector::exact(device);
        run_blocking(move || connect_matching(selector, config)).await
    }

    /// Lists currently attached USB devices with an ADB interface.
    ///
    /// # Errors
    ///
    /// Returns an error when libusb cannot create the context or enumerate the
    /// device list. Devices whose individual descriptors cannot be read are
    /// skipped.
    pub fn discover() -> Result<Vec<UsbDeviceInfo>, UsbTransportError> {
        let context = Context::new()?;
        let mut devices = Vec::new();
        for device in context.devices()?.iter() {
            if let Ok(Some(info)) = device_info(&device) {
                devices.push(info);
            }
        }
        Ok(devices)
    }

    /// Returns the USB device selected for this connection.
    #[must_use]
    pub const fn device_info(&self) -> &UsbDeviceInfo {
        &self.peer
    }

    fn peer(&self) -> String {
        format!(
            "usb://{:04x}:{:04x}/{}:{}",
            self.peer.vendor_id, self.peer.product_id, self.peer.bus_number, self.peer.address
        )
    }
}

#[async_trait]
impl AdbTransport for UsbTransport {
    async fn read_packet(&mut self) -> Result<AdbPacket, AdbTransportError> {
        let timeout = self.config.read_timeout;
        match tokio::time::timeout(timeout, self.read_packets.recv()).await {
            Ok(Some(result)) => result,
            Ok(None) => Err(AdbTransportError::ConnectionClosed {
                operation: TransportOperation::Read,
            }),
            Err(_) => Err(AdbTransportError::Timeout {
                operation: TransportOperation::Read,
                timeout,
            }),
        }
    }

    async fn write_packet(&mut self, packet: &AdbPacket) -> Result<(), AdbTransportError> {
        let connection = Arc::clone(&self.connection);
        let timeout = self.config.write_timeout;
        let packet = packet.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = connection
                .lock()
                .map_err(|_| UsbTransportError::LockPoisoned)
                .map_err(|error| transport_error(TransportOperation::Write, error, timeout))?;
            write_packet_blocking(&mut connection, &packet, timeout)
                .map_err(|error| transport_error(TransportOperation::Write, error, timeout))
        })
        .await
        .map_err(|error| worker_error(TransportOperation::Write, &error))?
    }

    async fn close(&mut self) -> Result<(), AdbTransportError> {
        self.reader_stop.store(true, Ordering::Release);
        self.read_packets.close();
        let connection = Arc::clone(&self.connection);
        let timeout = self.config.close_timeout;
        let reader_thread = self.reader_thread.take();
        let close_task = tokio::task::spawn_blocking(move || {
            if let Some(reader_thread) = reader_thread {
                reader_thread
                    .join()
                    .map_err(|_| UsbTransportError::Worker("USB reader panicked".to_owned()))
                    .map_err(|error| transport_error(TransportOperation::Close, error, timeout))?;
            }
            let mut connection = connection
                .lock()
                .map_err(|_| UsbTransportError::LockPoisoned)
                .map_err(|error| transport_error(TransportOperation::Close, error, timeout))?;
            close_blocking(&mut connection)
                .map_err(|error| transport_error(TransportOperation::Close, error, timeout))
        });
        match tokio::time::timeout(timeout, close_task).await {
            Ok(result) => {
                result.map_err(|error| worker_error(TransportOperation::Close, &error))?
            }
            Err(_) => Err(AdbTransportError::Timeout {
                operation: TransportOperation::Close,
                timeout,
            }),
        }
    }

    fn peer_description(&self) -> String {
        self.peer()
    }
}

impl Drop for UsbTransport {
    fn drop(&mut self) {
        self.reader_stop.store(true, Ordering::Release);
    }
}

async fn run_blocking<T>(
    operation: impl FnOnce() -> Result<T, UsbTransportError> + Send + 'static,
) -> Result<T, UsbTransportError>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| UsbTransportError::Worker(error.to_string()))?
}

fn connect_matching(
    selector: DeviceSelector,
    config: UsbTransportConfig,
) -> Result<UsbTransport, UsbTransportError> {
    validate_config(config)?;
    let context = Context::new()?;
    for device in context.devices()?.iter() {
        let Ok(Some(info)) = device_info(&device) else {
            continue;
        };
        if selector.matches(&info) {
            return open_device(&device, info, config);
        }
    }
    Err(UsbTransportError::DeviceNotFound {
        vendor_id: selector.vendor_id,
        product_id: selector.product_id,
    })
}

fn open_device(
    device: &Device<Context>,
    peer: UsbDeviceInfo,
    config: UsbTransportConfig,
) -> Result<UsbTransport, UsbTransportError> {
    let handle = device.open()?;
    let (read_endpoint, write_endpoint) = find_endpoints(device)?;
    let mut claimed_interfaces = Vec::new();
    handle.claim_interface(read_endpoint.interface)?;
    claimed_interfaces.push(read_endpoint.interface);
    if write_endpoint.interface != read_endpoint.interface {
        if let Err(error) = handle.claim_interface(write_endpoint.interface) {
            let _ = handle.release_interface(read_endpoint.interface);
            return Err(error.into());
        }
        claimed_interfaces.push(write_endpoint.interface);
    }
    if let Err(error) = prepare_endpoints(&handle, read_endpoint, write_endpoint) {
        for interface in claimed_interfaces {
            let _ = handle.release_interface(interface);
        }
        return Err(error);
    }
    let handle = Arc::new(handle);
    let reader_stop = Arc::new(AtomicBool::new(false));
    let (read_sender, read_packets) = mpsc::channel(config.read_queue_capacity);
    let reader_handle = Arc::clone(&handle);
    let reader_stop_flag = Arc::clone(&reader_stop);
    let read_timeout = config.read_timeout;
    let reader_thread = match thread::Builder::new()
        .name("droidmux-usb-reader".to_owned())
        .spawn(move || {
            run_usb_reader(
                &reader_handle,
                read_endpoint,
                read_timeout,
                &reader_stop_flag,
                &read_sender,
            );
        }) {
        Ok(reader_thread) => reader_thread,
        Err(error) => {
            for interface in claimed_interfaces {
                let _ = handle.release_interface(interface);
            }
            return Err(UsbTransportError::Worker(error.to_string()));
        }
    };
    Ok(UsbTransport {
        connection: Arc::new(Mutex::new(UsbConnection {
            handle,
            write_endpoint,
            claimed_interfaces,
            closed: false,
        })),
        read_packets,
        reader_stop,
        reader_thread: Some(reader_thread),
        peer,
        config,
    })
}

fn validate_config(config: UsbTransportConfig) -> Result<(), UsbTransportError> {
    if config.read_queue_capacity == 0 {
        return Err(UsbTransportError::InvalidReadQueueCapacity);
    }
    Ok(())
}

fn prepare_endpoints(
    handle: &DeviceHandle<Context>,
    read_endpoint: Endpoint,
    write_endpoint: Endpoint,
) -> Result<(), UsbTransportError> {
    handle.clear_halt(read_endpoint.address)?;
    handle.clear_halt(write_endpoint.address)?;

    let mut buffer = vec![0_u8; USB_READ_BUFFER.max(read_endpoint.max_packet_size)];
    let mut drained = 0_usize;
    loop {
        match handle.read_bulk(read_endpoint.address, &mut buffer, USB_READ_POLL_INTERVAL) {
            Err(rusb::Error::Timeout) => return Ok(()),
            Ok(read) => {
                drained = drained.saturating_add(read);
                if drained > USB_STALE_INPUT_LIMIT {
                    return Err(UsbTransportError::Worker(format!(
                        "USB input did not quiesce within {USB_STALE_INPUT_LIMIT} bytes"
                    )));
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn device_info(device: &Device<Context>) -> Result<Option<UsbDeviceInfo>, UsbTransportError> {
    let descriptor = device.device_descriptor()?;
    if !has_adb_interface(device, &descriptor)? {
        return Ok(None);
    }
    let description = device
        .open()
        .ok()
        .map(|handle| {
            let manufacturer = handle
                .read_manufacturer_string_ascii(&descriptor)
                .unwrap_or_default();
            let product = handle
                .read_product_string_ascii(&descriptor)
                .unwrap_or_default();
            [manufacturer, product]
                .into_iter()
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "Unknown USB ADB device".to_owned());
    Ok(Some(UsbDeviceInfo {
        vendor_id: descriptor.vendor_id(),
        product_id: descriptor.product_id(),
        bus_number: device.bus_number(),
        address: device.address(),
        description,
    }))
}

fn has_adb_interface(
    device: &Device<Context>,
    descriptor: &rusb::DeviceDescriptor,
) -> Result<bool, UsbTransportError> {
    for index in 0..descriptor.num_configurations() {
        let configuration = device.config_descriptor(index)?;
        for interface in configuration.interfaces() {
            for descriptor in interface.descriptors() {
                if is_adb_interface(
                    descriptor.class_code(),
                    descriptor.sub_class_code(),
                    descriptor.protocol_code(),
                ) {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

fn find_endpoints(device: &Device<Context>) -> Result<(Endpoint, Endpoint), UsbTransportError> {
    let descriptor = device.device_descriptor()?;
    for index in 0..descriptor.num_configurations() {
        let configuration = device.config_descriptor(index)?;
        for interface in configuration.interfaces() {
            for descriptor in interface.descriptors() {
                if !is_adb_interface(
                    descriptor.class_code(),
                    descriptor.sub_class_code(),
                    descriptor.protocol_code(),
                ) {
                    continue;
                }
                let mut read_endpoint = None;
                let mut write_endpoint = None;
                for endpoint in descriptor.endpoint_descriptors() {
                    if endpoint.transfer_type() != TransferType::Bulk {
                        continue;
                    }
                    let candidate = Endpoint {
                        interface: descriptor.interface_number(),
                        address: endpoint.address(),
                        max_packet_size: usize::from(endpoint.max_packet_size()),
                    };
                    match endpoint.direction() {
                        Direction::In => read_endpoint = Some(candidate),
                        Direction::Out => write_endpoint = Some(candidate),
                    }
                }
                if let (Some(read), Some(write)) = (read_endpoint, write_endpoint)
                    && read.max_packet_size > 0
                    && write.max_packet_size > 0
                {
                    return Ok((read, write));
                }
            }
        }
    }
    Err(UsbTransportError::NoEndpoints)
}

fn is_adb_interface(class: u8, subclass: u8, protocol: u8) -> bool {
    protocol == ADB_INTERFACE_PROTOCOL
        && ((class == rusb::constants::LIBUSB_CLASS_VENDOR_SPEC
            && subclass == ADB_INTERFACE_SUBCLASS)
            || (class == BULK_CLASS && subclass == BULK_SUBCLASS))
}

fn run_usb_reader(
    handle: &DeviceHandle<Context>,
    endpoint: Endpoint,
    configured_timeout: Duration,
    stop: &AtomicBool,
    sender: &mpsc::Sender<Result<AdbPacket, AdbTransportError>>,
) {
    let mut read_buffer = BytesMut::with_capacity(ADB_HEADER_LEN);
    loop {
        if stop.load(Ordering::Acquire) {
            return;
        }

        match take_buffered_packet(&mut read_buffer) {
            Ok(Some(packet)) => {
                if sender.blocking_send(Ok(packet)).is_err() {
                    return;
                }
                continue;
            }
            Ok(None) => {}
            Err(error) => {
                let error = transport_error(TransportOperation::Read, error, configured_timeout);
                let _ = sender.blocking_send(Err(error));
                return;
            }
        }

        let mut chunk = vec![0_u8; USB_READ_BUFFER.max(endpoint.max_packet_size)];
        match handle.read_bulk(endpoint.address, &mut chunk, USB_READ_POLL_INTERVAL) {
            Ok(0) | Err(rusb::Error::Timeout) => {}
            Ok(read) => read_buffer.extend_from_slice(&chunk[..read]),
            Err(error) => {
                let error = transport_error(
                    TransportOperation::Read,
                    UsbTransportError::Usb(error),
                    configured_timeout,
                );
                let _ = sender.blocking_send(Err(error));
                return;
            }
        }
    }
}

fn take_buffered_packet(
    read_buffer: &mut BytesMut,
) -> Result<Option<AdbPacket>, UsbTransportError> {
    if read_buffer.len() < ADB_HEADER_LEN {
        return Ok(None);
    }
    let header = AdbHeader::decode(&read_buffer[..ADB_HEADER_LEN])
        .map_err(|error| UsbTransportError::Worker(error.to_string()))?;
    let payload_length = usize::try_from(header.payload_length)
        .map_err(|_| UsbTransportError::Worker("invalid USB ADB payload length".to_owned()))?;
    let frame_length = ADB_HEADER_LEN
        .checked_add(payload_length)
        .ok_or_else(|| UsbTransportError::Worker("USB ADB frame length overflow".to_owned()))?;
    if read_buffer.len() < frame_length {
        return Ok(None);
    }
    let frame = read_buffer.split_to(frame_length);
    AdbPacket::decode(&frame)
        .map(|(packet, _)| Some(packet))
        .map_err(|error| UsbTransportError::Worker(error.to_string()))
}

fn write_packet_blocking(
    connection: &mut UsbConnection,
    packet: &AdbPacket,
    timeout: Duration,
) -> Result<(), UsbTransportError> {
    let endpoint = connection.write_endpoint;
    write_packet_parts(packet, |part| {
        write_bulk_data(connection, endpoint, part, timeout)
    })
}

fn write_packet_parts(
    packet: &AdbPacket,
    mut write_part: impl FnMut(&[u8]) -> Result<(), UsbTransportError>,
) -> Result<(), UsbTransportError> {
    let header = packet
        .header()
        .map_err(|error| UsbTransportError::Worker(error.to_string()))?;
    write_part(&header.encode())?;
    if !packet.payload.is_empty() {
        write_part(&packet.payload)?;
    }
    Ok(())
}

fn write_bulk_data(
    connection: &mut UsbConnection,
    endpoint: Endpoint,
    data: &[u8],
    timeout: Duration,
) -> Result<(), UsbTransportError> {
    write_complete_transfer(data, endpoint.max_packet_size, |part| {
        connection
            .handle
            .write_bulk(endpoint.address, part, timeout)
            .map_err(UsbTransportError::from)
    })
}

fn write_complete_transfer(
    data: &[u8],
    max_packet_size: usize,
    mut write: impl FnMut(&[u8]) -> Result<usize, UsbTransportError>,
) -> Result<(), UsbTransportError> {
    let written = write(data)?;
    if written != data.len() {
        return Err(UsbTransportError::Worker(format!(
            "short USB bulk write: wrote {written} of {} bytes",
            data.len()
        )));
    }
    if !data.is_empty() && data.len() % max_packet_size == 0 {
        let zlp_written = write(&[])?;
        if zlp_written != 0 {
            return Err(UsbTransportError::Worker(format!(
                "invalid USB zero-length packet result: wrote {zlp_written} bytes"
            )));
        }
    }
    Ok(())
}

fn close_blocking(connection: &mut UsbConnection) -> Result<(), UsbTransportError> {
    if connection.closed {
        return Ok(());
    }
    for interface in connection.claimed_interfaces.drain(..) {
        connection.handle.release_interface(interface)?;
    }
    connection.closed = true;
    Ok(())
}

fn worker_error(
    operation: TransportOperation,
    error: &tokio::task::JoinError,
) -> AdbTransportError {
    AdbTransportError::Io {
        operation,
        source: io::Error::other(error.to_string()),
    }
}

fn transport_error(
    operation: TransportOperation,
    error: UsbTransportError,
    timeout: Duration,
) -> AdbTransportError {
    match error {
        UsbTransportError::Usb(rusb::Error::Timeout) => {
            AdbTransportError::Timeout { operation, timeout }
        }
        UsbTransportError::Usb(rusb::Error::NoDevice | rusb::Error::Io) => {
            AdbTransportError::ConnectionClosed { operation }
        }
        other => AdbTransportError::Io {
            operation,
            source: io::Error::other(other),
        },
    }
}

#[cfg(test)]
mod tests {
    use adb_protocol::AdbCommand;
    use bytes::Bytes;

    use super::*;

    #[test]
    fn recognizes_standard_adb_interfaces() {
        assert!(is_adb_interface(
            rusb::constants::LIBUSB_CLASS_VENDOR_SPEC,
            ADB_INTERFACE_SUBCLASS,
            ADB_INTERFACE_PROTOCOL,
        ));
        assert!(is_adb_interface(
            BULK_CLASS,
            BULK_SUBCLASS,
            ADB_INTERFACE_PROTOCOL,
        ));
    }

    #[test]
    fn rejects_non_adb_interfaces() {
        assert!(!is_adb_interface(
            rusb::constants::LIBUSB_CLASS_VENDOR_SPEC,
            ADB_INTERFACE_SUBCLASS,
            0,
        ));
        assert!(!is_adb_interface(3, 1, ADB_INTERFACE_PROTOCOL));
    }

    #[test]
    fn rejects_a_zero_read_queue_capacity() {
        let config = UsbTransportConfig {
            read_queue_capacity: 0,
            ..UsbTransportConfig::default()
        };

        assert!(matches!(
            validate_config(config),
            Err(UsbTransportError::InvalidReadQueueCapacity)
        ));
    }

    #[test]
    fn writes_adb_header_and_payload_as_separate_usb_transfers()
    -> Result<(), Box<dyn std::error::Error>> {
        let packet = AdbPacket::new(
            AdbCommand::Connect,
            0x0100_0001,
            4096,
            Bytes::from_static(b"host::features=shell_v2"),
        )?;
        let mut transfers = Vec::new();

        write_packet_parts(&packet, |part| {
            transfers.push(part.to_vec());
            Ok(())
        })?;

        assert_eq!(transfers.len(), 2);
        assert_eq!(transfers[0].len(), ADB_HEADER_LEN);
        assert_eq!(transfers[1], packet.payload);
        Ok(())
    }

    #[test]
    fn omits_the_payload_transfer_for_empty_packets() -> Result<(), Box<dyn std::error::Error>> {
        let packet = AdbPacket::new(AdbCommand::Okay, 1, 2, Bytes::new())?;
        let mut transfer_count = 0;

        write_packet_parts(&packet, |_| {
            transfer_count += 1;
            Ok(())
        })?;

        assert_eq!(transfer_count, 1);
        Ok(())
    }

    #[test]
    fn writes_a_large_payload_as_one_transfer_followed_by_a_zlp()
    -> Result<(), Box<dyn std::error::Error>> {
        let payload = vec![0_u8; 1024];
        let mut transfer_lengths = Vec::new();

        write_complete_transfer(&payload, 512, |part| {
            transfer_lengths.push(part.len());
            Ok(part.len())
        })?;

        assert_eq!(transfer_lengths, [1024, 0]);
        Ok(())
    }

    #[test]
    fn rejects_a_short_usb_bulk_write_without_retrying() {
        let payload = vec![0_u8; 1024];
        let mut transfer_count = 0;

        let result = write_complete_transfer(&payload, 512, |part| {
            transfer_count += 1;
            Ok(part.len() - 1)
        });

        assert!(matches!(result, Err(UsbTransportError::Worker(_))));
        assert_eq!(transfer_count, 1);
    }

    #[test]
    fn retains_partial_usb_frames_until_the_packet_is_complete()
    -> Result<(), Box<dyn std::error::Error>> {
        let packet = AdbPacket::new(
            AdbCommand::Write,
            3,
            4,
            Bytes::from_static(b"fragmented payload"),
        )?;
        let encoded = packet.encode()?;
        let split = ADB_HEADER_LEN + 3;
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&encoded[..split]);

        assert!(take_buffered_packet(&mut buffer)?.is_none());
        buffer.extend_from_slice(&encoded[split..]);

        assert_eq!(take_buffered_packet(&mut buffer)?, Some(packet));
        assert!(buffer.is_empty());
        Ok(())
    }

    #[test]
    fn exact_device_selector_includes_bus_and_address() {
        let selected = UsbDeviceInfo {
            vendor_id: 0x2717,
            product_id: 0xff48,
            bus_number: 1,
            address: 7,
            description: "selected".to_owned(),
        };
        let same_product = UsbDeviceInfo {
            address: 8,
            description: "other".to_owned(),
            ..selected.clone()
        };

        assert!(DeviceSelector::exact(&selected).matches(&selected));
        assert!(!DeviceSelector::exact(&selected).matches(&same_product));
        assert!(
            DeviceSelector::by_product(selected.vendor_id, selected.product_id)
                .matches(&same_product)
        );
    }
}
