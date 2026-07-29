//! Loopback integration tests for the native TCP transport.

use std::time::Duration;

use adb_protocol::{ADB_VERSION, AdbCommand, AdbPacket, MAX_PAYLOAD};
use adb_transport::{AdbTransport, AdbTransportError, TransportOperation};
use bytes::Bytes;
use droidmux_transport_tcp::{TcpTransport, TcpTransportConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

fn test_packet() -> AdbPacket {
    AdbPacket::new(
        AdbCommand::Connect,
        ADB_VERSION,
        u32::try_from(MAX_PAYLOAD).expect("the payload limit fits on the wire"),
        Bytes::from_static(b"host::\0"),
    )
    .expect("the test packet is valid")
}

fn test_config() -> TcpTransportConfig {
    TcpTransportConfig {
        connect_timeout: TEST_TIMEOUT,
        read_timeout: TEST_TIMEOUT,
        write_timeout: TEST_TIMEOUT,
        close_timeout: TEST_TIMEOUT,
        keepalive: true,
        no_delay: true,
    }
}

#[tokio::test]
async fn reads_a_packet_that_arrives_in_small_segments() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have a local address");
    let expected_packet = test_packet();
    let encoded = expected_packet.encode().expect("test packet should encode");

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("client should connect");
        for chunk in encoded.chunks(3) {
            stream
                .write_all(chunk)
                .await
                .expect("server should write a packet segment");
            tokio::task::yield_now().await;
        }
    });

    let mut transport: Box<dyn AdbTransport> = Box::new(
        TcpTransport::connect(address, test_config())
            .await
            .expect("transport should connect"),
    );
    let packet = transport
        .read_packet()
        .await
        .expect("segmented packet should be reassembled");

    assert_eq!(packet, expected_packet);
    assert_eq!(transport.peer_description(), address.to_string());
    server.await.expect("server task should finish");
}

#[tokio::test]
async fn connects_to_an_ipv6_loopback_endpoint() {
    let listener = TcpListener::bind("[::1]:0")
        .await
        .expect("IPv6 loopback listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have a local address");

    let server = tokio::spawn(async move {
        listener.accept().await.expect("client should connect");
    });

    let mut transport = TcpTransport::connect(address, test_config())
        .await
        .expect("transport should connect over IPv6");

    assert!(address.is_ipv6());
    assert_eq!(transport.peer_description(), address.to_string());
    transport
        .close()
        .await
        .expect("transport should close cleanly");
    server.await.expect("server task should finish");
}

#[tokio::test]
async fn writes_one_complete_packet() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have a local address");
    let packet = test_packet();
    let expected = packet.encode().expect("test packet should encode").to_vec();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("client should connect");
        let mut received = vec![0_u8; expected.len()];
        stream
            .read_exact(&mut received)
            .await
            .expect("server should read a complete packet");
        assert_eq!(received, expected);
    });

    let mut transport = TcpTransport::connect(address, test_config())
        .await
        .expect("transport should connect");
    transport
        .write_packet(&packet)
        .await
        .expect("transport should write the complete packet");

    server.await.expect("server task should finish");
}

#[tokio::test]
async fn times_out_the_entire_read_operation() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have a local address");
    let mut config = test_config();
    config.read_timeout = Duration::from_millis(20);

    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.expect("client should connect");
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let mut transport = TcpTransport::connect(address, config)
        .await
        .expect("transport should connect");
    let error = transport
        .read_packet()
        .await
        .expect_err("a silent peer should trigger the read timeout");

    assert!(matches!(
        error,
        AdbTransportError::Timeout {
            operation: TransportOperation::Read,
            timeout,
        } if timeout == Duration::from_millis(20)
    ));

    let error = transport
        .read_packet()
        .await
        .expect_err("a silent peer should continue to time out");
    assert!(matches!(
        error,
        AdbTransportError::Timeout {
            operation: TransportOperation::Read,
            timeout,
        }
        if timeout == Duration::from_millis(20)
    ));
    server.await.expect("server task should finish");
}

#[tokio::test]
async fn resumes_a_partial_packet_after_read_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have a local address");
    let mut config = test_config();
    config.read_timeout = Duration::from_millis(20);
    let expected = test_packet();
    let encoded = expected.encode().expect("the test packet should encode");

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("client should connect");
        stream
            .write_all(&encoded[..10])
            .await
            .expect("the partial header should be written");
        tokio::time::sleep(Duration::from_millis(30)).await;
        stream
            .write_all(&encoded[10..])
            .await
            .expect("the remaining frame should be written");
    });

    let mut transport = TcpTransport::connect(address, config)
        .await
        .expect("transport should connect");
    let error = transport
        .read_packet()
        .await
        .expect_err("the partial packet should cross the first deadline");
    assert!(matches!(
        error,
        AdbTransportError::Timeout {
            operation: TransportOperation::Read,
            ..
        }
    ));

    let packet = transport
        .read_packet()
        .await
        .expect("the buffered partial packet should resume");
    assert_eq!(packet, expected);
    server.await.expect("server task should finish");
}

#[tokio::test]
async fn resumes_a_partial_packet_after_read_cancellation() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have a local address");
    let expected = test_packet();
    let encoded = expected.encode().expect("the test packet should encode");

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("client should connect");
        stream
            .write_all(&encoded[..10])
            .await
            .expect("the partial header should be written");
        tokio::time::sleep(Duration::from_millis(40)).await;
        stream
            .write_all(&encoded[10..])
            .await
            .expect("the remaining frame should be written");
    });

    let mut transport = TcpTransport::connect(address, test_config())
        .await
        .expect("transport should connect");
    tokio::select! {
        () = tokio::time::sleep(Duration::from_millis(20)) => {}
        result = transport.read_packet() => {
            assert!(result.is_err(), "a partial packet must not complete early");
        }
    }

    let packet = transport
        .read_packet()
        .await
        .expect("the canceled partial read should resume");
    assert_eq!(packet, expected);
    server.await.expect("server task should finish");
}

#[tokio::test]
async fn reports_when_the_peer_disconnects_mid_header() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have a local address");

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("client should connect");
        stream
            .write_all(&[0_u8; 4])
            .await
            .expect("server should write a partial header");
    });

    let mut transport = TcpTransport::connect(address, test_config())
        .await
        .expect("transport should connect");
    let error = transport
        .read_packet()
        .await
        .expect_err("a partial header followed by EOF must fail");

    assert!(matches!(
        error,
        AdbTransportError::ConnectionClosed {
            operation: TransportOperation::Read,
        }
    ));
    server.await.expect("server task should finish");
}

#[tokio::test]
async fn active_close_sends_eof_to_the_peer() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have a local address");

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("client should connect");
        let mut received = Vec::new();
        stream
            .read_to_end(&mut received)
            .await
            .expect("server should observe EOF");
        received
    });

    let mut transport = TcpTransport::connect(address, test_config())
        .await
        .expect("transport should connect");
    transport
        .close()
        .await
        .expect("transport should shut down cleanly");

    assert!(server.await.expect("server task should finish").is_empty());
}
