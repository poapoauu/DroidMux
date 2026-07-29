//! Opt-in native ADB TCP-forwarding smoke test for a live Android device.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use adb_auth::RsaAdbCredential;
use adb_client::AdbClient;
use adb_shell::execute;
use adb_transport_tcp::{TcpTransport, TcpTransportConfig};
use droidmux_forward::{ForwardConfig, TcpForwarder};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{sleep, timeout},
};

const DEVICE_MARKER: &[u8] = b"droidmux-device-to-host";
const HOST_MARKER: &[u8] = b"droidmux-host-to-device";

#[tokio::test]
#[ignore = "requires DROIDMUX_LIVE_ADB_ENDPOINT and toybox nc on a reachable device"]
async fn forwards_a_live_device_tcp_socket() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("DROIDMUX_LIVE_ADB_ENDPOINT")?;
    let address: SocketAddr = endpoint.parse()?;
    let target_port = std::env::var("DROIDMUX_LIVE_FORWARD_PORT")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(39_771_u16);
    let transport = TcpTransport::connect(
        address,
        TcpTransportConfig {
            connect_timeout: Duration::from_secs(3),
            read_timeout: Duration::from_secs(15),
            write_timeout: Duration::from_secs(5),
            close_timeout: Duration::from_secs(5),
            ..TcpTransportConfig::default()
        },
    )
    .await?;
    let credential = Arc::new(RsaAdbCredential::generate("droidmux@forward-live-test")?);
    let client = Arc::new(AdbClient::connect(Box::new(transport), credential).await?);
    println!("[ADB_SESSION] endpoint={endpoint} target_port={target_port}");

    let command = format!(
        "sh -c '(printf {}; sleep 1) | toybox nc -l -p {}'",
        String::from_utf8_lossy(DEVICE_MARKER),
        target_port
    );
    println!("[ADB_COMMAND] shell:{command}");
    let shell_client = Arc::clone(&client);
    let shell = tokio::spawn(async move { execute(&shell_client, &command).await });
    sleep(Duration::from_millis(300)).await;

    let forwarder = TcpForwarder::start(
        Arc::clone(&client),
        "127.0.0.1:0".parse()?,
        target_port,
        ForwardConfig::default(),
    )
    .await?;
    println!(
        "[ADB_FORWARD] local={} target=tcp:{}",
        forwarder.local_address(),
        target_port
    );
    let mut local = TcpStream::connect(forwarder.local_address()).await?;
    let mut device_marker = vec![0_u8; DEVICE_MARKER.len()];
    timeout(Duration::from_secs(5), local.read_exact(&mut device_marker)).await??;
    assert_eq!(device_marker, DEVICE_MARKER);
    println!(
        "[ADB_RESULT] direction=device-to-host bytes={}",
        device_marker.len()
    );

    local.write_all(HOST_MARKER).await?;
    let output = timeout(Duration::from_secs(5), shell).await???;
    assert_eq!(&output.stdout[..], HOST_MARKER);
    assert!(output.stderr.is_empty());
    println!(
        "[ADB_RESULT] direction=host-to-device bytes={} exit_code={:?}",
        output.stdout.len(),
        output.exit_code
    );

    forwarder.shutdown().await;
    client.close().await?;
    println!("[ADB_SESSION] status=closed");
    Ok(())
}
