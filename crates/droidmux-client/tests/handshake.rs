//! State-machine tests for the traditional ADB authentication handshake.

use std::{
    collections::VecDeque,
    future,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

use adb_auth::{
    ADB_AUTH_RSAPUBLICKEY, ADB_AUTH_SIGNATURE, ADB_AUTH_TOKEN, ADB_TOKEN_LEN, AdbAuthError,
    AdbAuthenticator, RsaAdbCredential,
};
use adb_protocol::{ADB_VERSION, AdbCommand, AdbPacket};
use adb_transport::{AdbTransport, AdbTransportError};
use async_trait::async_trait;
use bytes::Bytes;
use droidmux_client::{AdbClient, AdbClientError, ConnectionState};

#[derive(Clone)]
struct MockRecords {
    writes: Arc<Mutex<Vec<AdbPacket>>>,
    closed: Arc<AtomicBool>,
}

struct MockTransport {
    responses: VecDeque<AdbPacket>,
    records: MockRecords,
}

impl MockTransport {
    fn new(responses: Vec<AdbPacket>) -> (Self, MockRecords) {
        let records = MockRecords {
            writes: Arc::new(Mutex::new(Vec::new())),
            closed: Arc::new(AtomicBool::new(false)),
        };
        (
            Self {
                responses: responses.into(),
                records: records.clone(),
            },
            records,
        )
    }
}

#[async_trait]
impl AdbTransport for MockTransport {
    async fn read_packet(&mut self) -> Result<AdbPacket, AdbTransportError> {
        match self.responses.pop_front() {
            Some(packet) => Ok(packet),
            None => future::pending().await,
        }
    }

    async fn write_packet(&mut self, packet: &AdbPacket) -> Result<(), AdbTransportError> {
        self.records
            .writes
            .lock()
            .expect("mock write log should not be poisoned")
            .push(packet.clone());
        Ok(())
    }

    async fn close(&mut self) -> Result<(), AdbTransportError> {
        self.records.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn peer_description(&self) -> String {
        "mock-device:5555".to_owned()
    }
}

fn credential() -> Arc<RsaAdbCredential> {
    static CREDENTIAL: OnceLock<RsaAdbCredential> = OnceLock::new();
    Arc::new(
        CREDENTIAL
            .get_or_init(|| {
                RsaAdbCredential::generate("droidmux@test-host")
                    .expect("a test credential should be generated")
            })
            .clone(),
    )
}

fn auth_token(payload: Bytes) -> AdbPacket {
    AdbPacket::new(AdbCommand::Auth, ADB_AUTH_TOKEN, 0, payload)
        .expect("AUTH fixture should be valid")
}

fn device_connect() -> AdbPacket {
    AdbPacket::new(
        AdbCommand::Connect,
        ADB_VERSION,
        4096,
        Bytes::from_static(b"device::features=shell_v2,cmd;product=test;\0"),
    )
    .expect("CNXN fixture should be valid")
}

fn recorded_writes(records: &MockRecords) -> Vec<AdbPacket> {
    records
        .writes
        .lock()
        .expect("mock write log should not be poisoned")
        .clone()
}

struct FixtureAuthenticator {
    marker: u8,
    public_key: Bytes,
}

impl FixtureAuthenticator {
    fn new(marker: u8, public_key: &'static [u8]) -> Self {
        Self {
            marker,
            public_key: Bytes::from_static(public_key),
        }
    }
}

impl AdbAuthenticator for FixtureAuthenticator {
    fn sign_token(&self, token: &[u8]) -> Result<Bytes, AdbAuthError> {
        if token.len() != ADB_TOKEN_LEN {
            return Err(AdbAuthError::InvalidTokenLength {
                expected: ADB_TOKEN_LEN,
                actual: token.len(),
            });
        }
        Ok(Bytes::from(vec![self.marker ^ token[0]; 256]))
    }

    fn public_key_payload(&self) -> Result<Bytes, AdbAuthError> {
        Ok(self.public_key.clone())
    }
}

#[tokio::test]
async fn connects_without_authentication_when_the_device_allows_it() {
    let (transport, records) = MockTransport::new(vec![device_connect()]);
    let mut states = Vec::new();
    let client = AdbClient::connect_with_observer(Box::new(transport), credential(), |state| {
        states.push(state);
    })
    .await
    .expect("handshake should complete");

    assert_eq!(
        states,
        vec![ConnectionState::Connecting, ConnectionState::Connected]
    );
    assert_eq!(client.state(), ConnectionState::Connected);
    assert_eq!(client.protocol_version(), ADB_VERSION);
    assert_eq!(client.max_payload(), 4096);
    assert_eq!(
        client.device_banner(),
        "device::features=shell_v2,cmd;product=test;"
    );
    assert!(client.supports_feature("shell_v2"));
    assert!(client.supports_feature("cmd"));
    assert!(!client.supports_feature("shell"));
    assert_eq!(client.peer_description(), "mock-device:5555");

    let writes = recorded_writes(&records);
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].command, AdbCommand::Connect);
    assert_eq!(
        writes[0].payload,
        Bytes::from_static(
            b"host::features=shell_v2,cmd,stat_v2,ls_v2,sendrecv_v2,sendrecv_v2_brotli,sendrecv_v2_lz4,sendrecv_v2_zstd,abb,abb_exec",
        )
    );

    client.close().await.expect("client should close");
    assert_eq!(client.state(), ConnectionState::Closed);
    assert!(records.closed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn accepts_a_modern_device_banner_without_a_nul_terminator() {
    let packet = AdbPacket::new(
        AdbCommand::Connect,
        ADB_VERSION,
        1_048_576,
        Bytes::from_static(
            b"device::ro.product.name=rk3576_yc;ro.product.model=rk3576_yc;features=shell_v2,cmd,delayed_ack,abb",
        ),
    )
    .expect("CNXN fixture should be valid");
    let (transport, _) = MockTransport::new(vec![packet]);

    let client = AdbClient::connect_with_config(
        Box::new(transport),
        credential(),
        droidmux_client::AdbClientConfig {
            burst_mode: true,
            ..droidmux_client::AdbClientConfig::default()
        },
    )
    .await
    .expect("a non-NUL-terminated modern banner should connect");

    assert_eq!(client.protocol_version(), ADB_VERSION);
    assert_eq!(client.max_payload(), 1_048_576);
    assert!(client.supports_feature("shell_v2"));
    assert!(client.supports_feature("cmd"));
    assert!(client.supports_feature("delayed_ack"));
    assert!(client.supports_feature("abb"));
    client.close().await.expect("client should close");
}

#[tokio::test]
async fn signs_the_device_token_with_an_existing_key() {
    let token = Bytes::from(vec![0x42_u8; ADB_TOKEN_LEN]);
    let (transport, records) = MockTransport::new(vec![auth_token(token), device_connect()]);
    let mut states = Vec::new();

    let client = AdbClient::connect_with_observer(Box::new(transport), credential(), |state| {
        states.push(state);
    })
    .await
    .expect("authorized key should connect");

    assert_eq!(client.state(), ConnectionState::Connected);
    assert_eq!(
        states,
        vec![
            ConnectionState::Connecting,
            ConnectionState::Authorizing,
            ConnectionState::Connected,
        ]
    );
    let writes = recorded_writes(&records);
    assert_eq!(writes.len(), 2);
    assert_eq!(writes[1].command, AdbCommand::Auth);
    assert_eq!(writes[1].arg0, ADB_AUTH_SIGNATURE);
    assert_eq!(writes[1].payload.len(), 256);
}

#[tokio::test]
async fn sends_the_public_key_and_waits_for_first_authorization() {
    let first_token = Bytes::from(vec![0x11_u8; ADB_TOKEN_LEN]);
    let second_token = Bytes::from(vec![0x22_u8; ADB_TOKEN_LEN]);
    let (transport, records) = MockTransport::new(vec![
        auth_token(first_token),
        auth_token(second_token),
        device_connect(),
    ]);
    let mut states = Vec::new();

    let client = AdbClient::connect_with_observer(Box::new(transport), credential(), |state| {
        states.push(state);
    })
    .await
    .expect("new key should connect after user authorization");

    assert_eq!(client.state(), ConnectionState::Connected);
    assert_eq!(
        states,
        vec![
            ConnectionState::Connecting,
            ConnectionState::Authorizing,
            ConnectionState::AwaitingAuthorization,
            ConnectionState::Connected,
        ]
    );
    let writes = recorded_writes(&records);
    assert_eq!(writes.len(), 3);
    assert_eq!(writes[1].arg0, ADB_AUTH_SIGNATURE);
    assert_eq!(writes[2].command, AdbCommand::Auth);
    assert_eq!(writes[2].arg0, ADB_AUTH_RSAPUBLICKEY);
    assert_eq!(writes[2].payload.last(), Some(&0));
}

#[tokio::test]
async fn tries_multiple_keys_before_offering_the_first_public_key() {
    let token = |value| Bytes::from(vec![value; ADB_TOKEN_LEN]);
    let (transport, records) = MockTransport::new(vec![
        auth_token(token(0x11)),
        auth_token(token(0x22)),
        auth_token(token(0x33)),
        device_connect(),
    ]);
    let authenticators: Vec<Arc<dyn AdbAuthenticator>> = vec![
        Arc::new(FixtureAuthenticator::new(0xa0, b"first-public-key\0")),
        Arc::new(FixtureAuthenticator::new(0xb0, b"second-public-key\0")),
    ];
    let mut states = Vec::new();

    let client = AdbClient::connect_with_authenticators_and_observer(
        Box::new(transport),
        authenticators,
        |state| states.push(state),
    )
    .await
    .expect("authorization should continue after the public key is accepted");

    assert_eq!(
        states,
        vec![
            ConnectionState::Connecting,
            ConnectionState::Authorizing,
            ConnectionState::AwaitingAuthorization,
            ConnectionState::Connected,
        ]
    );
    let writes = recorded_writes(&records);
    assert_eq!(writes.len(), 4);
    assert_eq!(writes[1].arg0, ADB_AUTH_SIGNATURE);
    assert_eq!(writes[1].payload, Bytes::from(vec![0xb1; 256]));
    assert_eq!(writes[2].arg0, ADB_AUTH_SIGNATURE);
    assert_eq!(writes[2].payload, Bytes::from(vec![0x92; 256]));
    assert_eq!(writes[3].arg0, ADB_AUTH_RSAPUBLICKEY);
    assert_eq!(writes[3].payload, Bytes::from_static(b"first-public-key\0"));
    client.close().await.expect("the client should close");
}

#[tokio::test]
async fn authorized_multi_key_connection_can_succeed_with_a_later_key() {
    let token = |value| Bytes::from(vec![value; ADB_TOKEN_LEN]);
    let (transport, records) = MockTransport::new(vec![
        auth_token(token(0x41)),
        auth_token(token(0x42)),
        device_connect(),
    ]);
    let authenticators: Vec<Arc<dyn AdbAuthenticator>> = vec![
        Arc::new(FixtureAuthenticator::new(1, b"first\0")),
        Arc::new(FixtureAuthenticator::new(2, b"second\0")),
    ];

    let client =
        AdbClient::connect_authorized_with_authenticators(Box::new(transport), authenticators)
            .await
            .expect("the second trusted key should connect without a prompt");

    let writes = recorded_writes(&records);
    assert_eq!(writes.len(), 3);
    assert!(
        writes[1..]
            .iter()
            .all(|packet| packet.arg0 == ADB_AUTH_SIGNATURE)
    );
    client.close().await.expect("the client should close");
}

#[tokio::test]
async fn rejects_an_empty_authenticator_list_before_writing_connect() {
    let (transport, records) = MockTransport::new(vec![device_connect()]);

    let error = AdbClient::connect_with_authenticators(Box::new(transport), Vec::new())
        .await
        .expect_err("an empty authenticator list must be rejected");

    assert!(matches!(error, AdbClientError::NoAuthenticators));
    assert!(recorded_writes(&records).is_empty());
}

#[tokio::test]
async fn passive_connection_never_offers_a_new_public_key() {
    let token = || Bytes::from(vec![0x2a_u8; ADB_TOKEN_LEN]);
    let (transport, records) = MockTransport::new(vec![auth_token(token()), auth_token(token())]);

    let error = AdbClient::connect_authorized(Box::new(transport), credential())
        .await
        .expect_err("an unknown host key should be rejected without prompting");

    assert!(matches!(error, AdbClientError::AuthorizationRejected));
    let writes = recorded_writes(&records);
    assert_eq!(writes.len(), 2);
    assert_eq!(writes[1].arg0, ADB_AUTH_SIGNATURE);
    assert!(
        writes
            .iter()
            .all(|packet| packet.arg0 != ADB_AUTH_RSAPUBLICKEY)
    );
}

#[tokio::test]
async fn rejects_a_malformed_authentication_token() {
    let token = Bytes::from(vec![0_u8; ADB_TOKEN_LEN - 1]);
    let (transport, _) = MockTransport::new(vec![auth_token(token)]);

    let error = AdbClient::connect(Box::new(transport), credential())
        .await
        .expect_err("a malformed token must fail the handshake");

    assert!(matches!(
        error,
        AdbClientError::Authentication(AdbAuthError::InvalidTokenLength {
            expected: ADB_TOKEN_LEN,
            actual,
        }) if actual == ADB_TOKEN_LEN - 1
    ));
}

#[tokio::test]
async fn reports_authorization_rejection_after_sending_the_public_key() {
    let token = || Bytes::from(vec![0x33_u8; ADB_TOKEN_LEN]);
    let (transport, _) = MockTransport::new(vec![
        auth_token(token()),
        auth_token(token()),
        auth_token(token()),
    ]);

    let error = AdbClient::connect(Box::new(transport), credential())
        .await
        .expect_err("a third token means the public key was not accepted");

    assert!(matches!(error, AdbClientError::AuthorizationRejected));
}

#[tokio::test]
async fn rejects_an_unexpected_handshake_packet() {
    let packet =
        AdbPacket::new(AdbCommand::Okay, 1, 1, Bytes::new()).expect("OKAY fixture should be valid");
    let (transport, _) = MockTransport::new(vec![packet]);

    let error = AdbClient::connect(Box::new(transport), credential())
        .await
        .expect_err("OKAY is invalid during connection setup");

    assert!(matches!(
        error,
        AdbClientError::UnexpectedPacket {
            command: AdbCommand::Okay,
        }
    ));
}
