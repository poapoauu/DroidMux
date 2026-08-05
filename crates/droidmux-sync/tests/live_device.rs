//! Opt-in Sync v2 compression smoke test against a user-provided device.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use adb_auth::RsaAdbCredential;
use adb_client::AdbClient;
use adb_transport_tcp::{TcpTransport, TcpTransportConfig};
use droidmux_sync::{
    TransferCompression, TransferOptions, pull_file_with_options, push_bytes_with_options,
};

#[tokio::test]
#[ignore = "requires DROIDMUX_LIVE_ADB_ENDPOINT and a reachable ADB device"]
async fn sync_v2_compression_round_trips_through_device() -> Result<(), Box<dyn std::error::Error>>
{
    let endpoint = std::env::var("DROIDMUX_LIVE_ADB_ENDPOINT")?;
    let address: SocketAddr = endpoint.parse()?;
    let config = TcpTransportConfig {
        connect_timeout: Duration::from_secs(3),
        read_timeout: Duration::from_secs(30),
        write_timeout: Duration::from_secs(10),
        close_timeout: Duration::from_secs(5),
        ..TcpTransportConfig::default()
    };
    let transport = TcpTransport::connect(address, config).await?;
    let authenticator = Arc::new(RsaAdbCredential::generate("droidmux@sync-compression")?);
    let client = AdbClient::connect(Box::new(transport), authenticator).await?;

    let data = (0..(256 * 1024))
        .map(|index| b"droidmux-sync-compression-"[index % 26])
        .collect::<Vec<_>>();
    let sequence = std::process::id();
    let mut paths = Vec::new();
    let mut local_paths = Vec::new();
    let result = async {
        for (index, compression) in [
            TransferCompression::Brotli,
            TransferCompression::Lz4,
            TransferCompression::Zstd,
        ]
        .into_iter()
        .enumerate()
        {
            let feature = match compression {
                TransferCompression::Brotli => "sendrecv_v2_brotli",
                TransferCompression::Lz4 => "sendrecv_v2_lz4",
                TransferCompression::Zstd => "sendrecv_v2_zstd",
                TransferCompression::Auto | TransferCompression::None => unreachable!(),
            };
            assert!(client.supports_feature(feature), "device lacks {feature}");

            let remote =
                format!("/data/local/tmp/droidmux-sync-compression-{sequence}-{index}.bin");
            let local = std::env::temp_dir()
                .join(format!("droidmux-sync-compression-{sequence}-{index}.bin"));
            paths.push(remote.clone());
            local_paths.push(local.clone());

            let options = TransferOptions {
                compression,
                ..TransferOptions::default()
            };
            push_bytes_with_options(&client, &data, &remote, &options, |_| {}).await?;
            pull_file_with_options(&client, &remote, &local, &options, |_| {}).await?;
            let received = tokio::fs::read(&local).await?;
            assert_eq!(received, data, "round trip failed for {compression:?}");
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    for remote in &paths {
        let cleanup = client
            .open_service(&format!("shell:rm -f -- {remote}"))
            .await;
        if let Ok(stream) = cleanup {
            while stream.read().await?.is_some() {}
            stream.close().await?;
        }
    }
    for local in local_paths {
        let _ = tokio::fs::remove_file(local).await;
    }
    client.close().await?;
    result
}
