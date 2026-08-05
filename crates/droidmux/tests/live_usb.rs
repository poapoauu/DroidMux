#![cfg(all(feature = "usb", feature = "shell"))]

//! Opt-in end-to-end coverage for a directly connected USB ADB device.

use std::{io, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use droidmux::{
    auth::RsaAdbCredential,
    client::AdbClient,
    shell::execute,
    transport::{AdbTransport, AdbTransportError},
    usb::{UsbDeviceInfo, UsbTransport, UsbTransportConfig},
};

struct PacketLogTransport<T>(T);

#[async_trait]
impl<T> AdbTransport for PacketLogTransport<T>
where
    T: AdbTransport + Send,
{
    async fn read_packet(&mut self) -> Result<droidmux::protocol::AdbPacket, AdbTransportError> {
        let result = self.0.read_packet().await;
        if let Ok(packet) = &result {
            eprintln!(
                "USB READ  {:?} arg0={} arg1={} payload={}",
                packet.command,
                packet.arg0,
                packet.arg1,
                packet.payload.len()
            );
        }
        result
    }

    async fn write_packet(
        &mut self,
        packet: &droidmux::protocol::AdbPacket,
    ) -> Result<(), AdbTransportError> {
        eprintln!(
            "USB WRITE {:?} arg0={} arg1={} payload={}",
            packet.command,
            packet.arg0,
            packet.arg1,
            packet.payload.len()
        );
        self.0.write_packet(packet).await
    }

    async fn close(&mut self) -> Result<(), AdbTransportError> {
        self.0.close().await
    }

    fn peer_description(&self) -> String {
        self.0.peer_description()
    }
}

fn select_device(devices: &[UsbDeviceInfo]) -> Result<&UsbDeviceInfo, io::Error> {
    let selector = std::env::var("DROIDMUX_LIVE_USB_DEVICE").ok();
    devices
        .iter()
        .find(|device| {
            selector.as_ref().is_none_or(|selector| {
                selector.eq_ignore_ascii_case(&format!(
                    "{:04x}:{:04x}",
                    device.vendor_id, device.product_id
                )) || selector == &format!("{}:{}", device.bus_number, device.address)
            })
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no matching USB ADB device"))
}

#[tokio::test]
#[ignore = "requires DROIDMUX_LIVE_ADB_KEY, a directly accessible USB ADB device, and no adb-server"]
async fn authenticates_and_multiplexes_shell_over_usb() -> Result<(), Box<dyn std::error::Error>> {
    let key_path = std::env::var_os("DROIDMUX_LIVE_ADB_KEY")
        .map(PathBuf::from)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "DROIDMUX_LIVE_ADB_KEY is not set")
        })?;
    let private_key = std::fs::read_to_string(key_path)?;
    let credential = Arc::new(RsaAdbCredential::from_pkcs8_pem(
        &private_key,
        "droidmux@usb-live-test",
    )?);

    let devices = UsbTransport::discover()?;
    let selected = select_device(&devices)?.clone();
    eprintln!(
        "testing USB ADB device {:04x}:{:04x} at {}:{} ({})",
        selected.vendor_id,
        selected.product_id,
        selected.bus_number,
        selected.address,
        selected.description
    );
    let transport = UsbTransport::connect_device(&selected, UsbTransportConfig::default()).await?;
    let client = AdbClient::connect(Box::new(PacketLogTransport(transport)), credential).await?;

    let (first, second) = tokio::join!(
        execute(&client, "printf droidmux-usb-a"),
        execute(&client, "printf droidmux-usb-b")
    );
    eprintln!("first command: {first:?}");
    eprintln!("second command: {second:?}");
    let first = first?;
    let second = second?;
    assert_eq!(first.exit_code, Some(0));
    assert_eq!(first.stdout.as_ref(), b"droidmux-usb-a");
    assert_eq!(second.exit_code, Some(0));
    assert_eq!(second.stdout.as_ref(), b"droidmux-usb-b");

    client.close().await?;
    Ok(())
}
