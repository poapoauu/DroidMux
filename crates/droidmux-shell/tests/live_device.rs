//! Opt-in native ADB smoke test for a user-provided live device endpoint.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use adb_auth::RsaAdbCredential;
use adb_client::{AdbClient, AdbClientConfig};
use adb_transport_tcp::{TcpTransport, TcpTransportConfig};
use droidmux_shell::{ShellOptions, ShellOutput, execute, open_shell};

async fn execute_logged(
    client: &AdbClient,
    command: &str,
) -> Result<ShellOutput, droidmux_shell::ShellError> {
    println!("[ADB_COMMAND] shell:{command}");
    let started = std::time::Instant::now();
    match execute(client, command).await {
        Ok(output) => {
            println!(
                "[ADB_RESULT] status=ok exit_code={:?} elapsed_ms={}",
                output.exit_code,
                started.elapsed().as_millis()
            );
            println!("[ADB_STDOUT] {}", escaped_output(&output.stdout));
            println!("[ADB_STDERR] {}", escaped_output(&output.stderr));
            Ok(output)
        }
        Err(error) => {
            println!(
                "[ADB_RESULT] status=error elapsed_ms={} error={error}",
                started.elapsed().as_millis()
            );
            Err(error)
        }
    }
}

fn escaped_output(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .replace('\r', "\\r")
        .replace('\n', "\\n")
}

#[tokio::test]
#[ignore = "requires DROIDMUX_LIVE_ADB_ENDPOINT and a reachable ADB device"]
async fn runs_compatibility_suite_on_a_live_device() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("DROIDMUX_LIVE_ADB_ENDPOINT")?;
    let address: SocketAddr = endpoint.parse()?;
    let config = TcpTransportConfig {
        connect_timeout: Duration::from_secs(3),
        read_timeout: Duration::from_secs(15),
        write_timeout: Duration::from_secs(5),
        close_timeout: Duration::from_secs(5),
        ..TcpTransportConfig::default()
    };
    let transport = TcpTransport::connect(address, config).await?;
    let authenticator = Arc::new(RsaAdbCredential::generate("droidmux@live-test")?);
    let client = AdbClient::connect(Box::new(transport), authenticator).await?;

    println!(
        "[ADB_SESSION] endpoint={endpoint} peer={} protocol=0x{:08x} max_payload={}",
        client.peer_description(),
        client.protocol_version(),
        client.max_payload()
    );
    println!("[ADB_BANNER] {}", client.device_banner());

    let shell_v2 = client.supports_feature("shell_v2");
    for command in [
        "getprop ro.product.model",
        "getprop ro.build.version.release",
        "getprop ro.build.version.sdk",
        "getprop ro.product.cpu.abi",
        "id",
        "printf droidmux-shell-ok",
    ] {
        let output = execute_logged(&client, command).await?;
        assert!(output.stderr.is_empty());
        assert_eq!(output.exit_code, shell_v2.then_some(0));
        assert!(!output.stdout.is_empty());
    }

    if shell_v2 {
        let output = execute_logged(
            &client,
            "sh -c 'printf droidmux-stdout; printf droidmux-stderr >&2; exit 7'",
        )
        .await?;
        assert_eq!(output.exit_code, Some(7));
        assert_eq!(output.stdout, "droidmux-stdout");
        assert_eq!(output.stderr, "droidmux-stderr");
    }

    client.close().await?;
    println!("[ADB_SESSION] status=closed");
    Ok(())
}

#[tokio::test]
#[ignore = "requires DROIDMUX_LIVE_ADB_ENDPOINT and a reachable ADB device"]
async fn runs_interactive_terminal_on_a_live_device() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("DROIDMUX_LIVE_ADB_ENDPOINT")?;
    let address: SocketAddr = endpoint.parse()?;
    let config = TcpTransportConfig {
        connect_timeout: Duration::from_secs(3),
        read_timeout: Duration::from_secs(15),
        write_timeout: Duration::from_secs(5),
        close_timeout: Duration::from_secs(5),
        ..TcpTransportConfig::default()
    };
    let transport = TcpTransport::connect(address, config).await?;
    let authenticator = Arc::new(RsaAdbCredential::generate("droidmux@interactive-test")?);
    let client = AdbClient::connect(Box::new(transport), authenticator).await?;
    println!("[ADB_SESSION] endpoint={endpoint} mode=interactive");

    let options = if client.supports_feature("shell_v2") {
        ShellOptions::interactive()
    } else {
        ShellOptions::legacy()
    };
    println!(
        "[ADB_COMMAND] service=shell interactive=true shell_v2={} pty={} rows={} columns={}",
        options.use_v2, options.use_pty, options.rows, options.columns
    );
    let session = Arc::new(open_shell(&client, "", options).await?);
    let input = "printf droidmux-interactive-ok; exit";
    println!("[ADB_STDIN] {input}\\n");
    session.write_stdin(format!("{input}\n")).await?;

    let stdout_session = Arc::clone(&session);
    let stderr_session = Arc::clone(&session);
    let (stdout, stderr, exit_code) = tokio::join!(
        async move {
            let mut output = Vec::new();
            while let Some(chunk) = stdout_session.read_stdout().await {
                output.extend_from_slice(&chunk);
            }
            output
        },
        async move {
            let mut output = Vec::new();
            while let Some(chunk) = stderr_session.read_stderr().await {
                output.extend_from_slice(&chunk);
            }
            output
        },
        session.wait(),
    );
    let exit_code = exit_code?;
    println!("[ADB_STDOUT] {}", escaped_output(&stdout));
    println!("[ADB_STDERR] {}", escaped_output(&stderr));
    println!("[ADB_RESULT] status=ok exit_code={exit_code:?}");
    assert!(String::from_utf8_lossy(&stdout).contains("droidmux-interactive-ok"));
    assert!(stderr.is_empty());
    assert_eq!(exit_code, options.use_v2.then_some(0));

    client.close().await?;
    println!("[ADB_SESSION] status=closed");
    Ok(())
}

#[tokio::test]
#[ignore = "requires DROIDMUX_LIVE_ADB_ENDPOINT and a reachable ADB device"]
async fn runs_burst_mode_shell_write_on_a_live_device() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("DROIDMUX_LIVE_ADB_ENDPOINT")?;
    let address: SocketAddr = endpoint.parse()?;
    let config = TcpTransportConfig {
        connect_timeout: Duration::from_secs(3),
        read_timeout: Duration::from_secs(15),
        write_timeout: Duration::from_secs(5),
        close_timeout: Duration::from_secs(5),
        ..TcpTransportConfig::default()
    };
    let transport = TcpTransport::connect(address, config).await?;
    let authenticator = Arc::new(RsaAdbCredential::generate("droidmux@burst-live-test")?);
    let client = AdbClient::connect_with_config(
        Box::new(transport),
        authenticator,
        AdbClientConfig {
            burst_mode: true,
            ..AdbClientConfig::default()
        },
    )
    .await?;
    assert!(client.supports_feature("delayed_ack"));

    let options = ShellOptions::interactive();
    let session = Arc::new(open_shell(&client, "printf droidmux-burst-ok; exit", options).await?);
    session.write_stdin("burst-input\n").await?;
    let stdout_session = Arc::clone(&session);
    let stdout = async move {
        let mut output = Vec::new();
        while let Some(chunk) = stdout_session.read_stdout().await {
            output.extend_from_slice(&chunk);
        }
        output
    };
    let (stdout, exit_code) = tokio::join!(stdout, session.wait());
    assert!(String::from_utf8_lossy(&stdout).contains("droidmux-burst-ok"));
    assert_eq!(exit_code?, Some(0));
    client.close().await?;
    Ok(())
}
