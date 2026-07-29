//! Opt-in package installer session compatibility smoke test.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use adb_auth::RsaAdbCredential;
use adb_client::AdbClient;
use adb_shell::{ShellOptions, execute, open_shell};
use adb_transport_tcp::{TcpTransport, TcpTransportConfig};

#[tokio::test]
#[ignore = "requires DROIDMUX_LIVE_ADB_ENDPOINT and a reachable Android device"]
async fn creates_and_abandons_an_empty_install_session() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("DROIDMUX_LIVE_ADB_ENDPOINT")?;
    let address: SocketAddr = endpoint.parse()?;
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
    let credential = Arc::new(RsaAdbCredential::generate(
        "droidmux@package-session-live-test",
    )?);
    let client = AdbClient::connect(Box::new(transport), credential).await?;

    let create_command = "cmd package install-create -S 1";
    println!("[ADB_COMMAND] shell:{create_command}");
    let create = execute(&client, create_command).await?;
    println!(
        "[ADB_RESULT] exit_code={:?} stdout={} stderr={}",
        create.exit_code,
        String::from_utf8_lossy(&create.stdout).trim(),
        String::from_utf8_lossy(&create.stderr).trim()
    );
    assert_eq!(create.exit_code, Some(0));
    let stdout = String::from_utf8(create.stdout.to_vec())?;
    let session_id = parse_session_id(&stdout)?;

    let write_command = format!("cmd package install-write -S 1 {session_id} split-0000.apk -");
    println!("[ADB_COMMAND] shell:{write_command} stdin_bytes=1");
    let write = stream_one_byte(&client, &write_command).await;

    let abandon_command = format!("cmd package install-abandon {session_id}");
    println!("[ADB_COMMAND] shell:{abandon_command}");
    let abandon = execute(&client, &abandon_command).await?;
    println!(
        "[ADB_RESULT] exit_code={:?} stdout={} stderr={}",
        abandon.exit_code,
        String::from_utf8_lossy(&abandon.stdout).trim(),
        String::from_utf8_lossy(&abandon.stderr).trim()
    );
    assert_eq!(abandon.exit_code, Some(0));
    assert!(String::from_utf8_lossy(&abandon.stdout).contains("Success"));
    let (write_stdout, write_stderr, write_exit) = write?;
    println!(
        "[ADB_RESULT] install-write exit_code={write_exit:?} stdout={} stderr={}",
        String::from_utf8_lossy(&write_stdout).trim(),
        String::from_utf8_lossy(&write_stderr).trim()
    );
    assert_eq!(write_exit, Some(0));
    assert!(String::from_utf8_lossy(&write_stdout).contains("Success"));
    client.close().await?;
    Ok(())
}

async fn stream_one_byte(
    client: &AdbClient,
    command: &str,
) -> Result<(Vec<u8>, Vec<u8>, Option<u8>), Box<dyn std::error::Error>> {
    let session = Arc::new(open_shell(client, command, ShellOptions::default()).await?);
    let input_session = Arc::clone(&session);
    let stdout_session = Arc::clone(&session);
    let stderr_session = Arc::clone(&session);
    let input = async move {
        input_session.write_stdin(vec![0_u8]).await?;
        input_session.close_stdin().await
    };
    let (input, stdout, stderr, exit_code) = tokio::join!(
        input,
        collect_stdout(stdout_session),
        collect_stderr(stderr_session),
        session.wait(),
    );
    input?;
    Ok((stdout, stderr, exit_code?))
}

async fn collect_stdout(session: Arc<adb_shell::ShellSession>) -> Vec<u8> {
    let mut output = Vec::new();
    while let Some(chunk) = session.read_stdout().await {
        output.extend_from_slice(&chunk);
    }
    output
}

async fn collect_stderr(session: Arc<adb_shell::ShellSession>) -> Vec<u8> {
    let mut output = Vec::new();
    while let Some(chunk) = session.read_stderr().await {
        output.extend_from_slice(&chunk);
    }
    output
}

fn parse_session_id(output: &str) -> Result<u32, Box<dyn std::error::Error>> {
    let id = output.lines().find_map(|line| {
        let success = line.trim().strip_prefix("Success:")?;
        let start = success.rfind('[')? + 1;
        let end = success.get(start..)?.find(']')? + start;
        success.get(start..end)?.parse::<u32>().ok()
    });
    id.filter(|id| *id != 0)
        .ok_or_else(|| "install-create did not return a session ID".into())
}
