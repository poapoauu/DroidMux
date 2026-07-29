use std::{error::Error, sync::Arc};

use adb_auth::RsaAdbCredential;
use adb_protocol::{ADB_HEADER_LEN, ADB_VERSION, AdbCommand, AdbHeader, AdbPacket};
use adb_shell::execute;
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use rustls::{
    DigitallySignedStruct, DistinguishedName, ServerConfig, SignatureScheme,
    crypto::{WebPkiSupportedAlgorithms, ring, verify_tls12_signature, verify_tls13_signature},
    pki_types::{CertificateDer, UnixTime},
    server::danger::{ClientCertVerified, ClientCertVerifier},
    version::TLS13,
};
use secrecy::SecretString;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
};
use tokio_rustls::TlsAcceptor;
use zeroize::Zeroizing;

use crate::{
    CredentialStore, CredentialStoreError, DirectConnectionRequest, PairingError, PairingRequest,
    StoredHostCredential, StoredPairedDevice, connect_paired_device, connect_tcp_device,
    crypto::{PairingCipher, Spake2State},
    pair_device,
    protocol::{PacketType, PeerInfo, read_packet, write_packet},
    tls::{TLS_EXPORTER_LABEL, TLS_EXPORTER_SIZE, TlsIdentity},
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Default)]
struct MemoryStore {
    state: Arc<Mutex<MemoryStoreState>>,
}

#[derive(Default)]
struct MemoryStoreState {
    host: Option<StoredHostCredential>,
    devices: Vec<StoredPairedDevice>,
}

impl MemoryStore {
    async fn with_credential(credential: &RsaAdbCredential) -> TestResult<Self> {
        let pem = credential.to_pkcs8_pem()?;
        let store = Self::default();
        store.state.lock().await.host = Some(StoredHostCredential::new(
            SecretString::from(pem.to_string()),
            "droidmux@integration-test",
        ));
        Ok(store)
    }
}

#[async_trait]
impl CredentialStore for MemoryStore {
    async fn load_host_credential(
        &self,
    ) -> Result<Option<StoredHostCredential>, CredentialStoreError> {
        Ok(self.state.lock().await.host.clone())
    }

    async fn save_host_credential(
        &self,
        credential: &StoredHostCredential,
    ) -> Result<(), CredentialStoreError> {
        self.state.lock().await.host = Some(credential.clone());
        Ok(())
    }

    async fn save_paired_device(
        &self,
        device: &StoredPairedDevice,
    ) -> Result<(), CredentialStoreError> {
        let mut state = self.state.lock().await;
        state
            .devices
            .retain(|candidate| candidate.device_id != device.device_id);
        state.devices.push(device.clone());
        Ok(())
    }

    async fn load_paired_device(
        &self,
        device_id: &str,
    ) -> Result<Option<StoredPairedDevice>, CredentialStoreError> {
        Ok(self
            .state
            .lock()
            .await
            .devices
            .iter()
            .find(|device| device.device_id == device_id)
            .cloned())
    }
}

#[tokio::test]
async fn pairs_persists_reconnects_after_restart_and_executes_shell() -> TestResult {
    const DEVICE_ID: &str = "adb-test-device-guid";
    const CODE: &str = "123456";

    let host_credential = RsaAdbCredential::generate("droidmux@integration-test")?;
    let store = MemoryStore::with_credential(&host_credential).await?;
    let device_credential = RsaAdbCredential::generate("android@test-device")?;

    let pairing_listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let pairing_port = pairing_listener.local_addr()?.port();
    let pairing_host = host_credential.clone();
    let pairing_device = device_credential.clone();
    let pairing_server = tokio::spawn(async move {
        run_pairing_server(
            pairing_listener,
            &pairing_host,
            &pairing_device,
            CODE,
            DEVICE_ID,
        )
        .await
    });

    let result = pair_device(
        PairingRequest {
            host: "127.0.0.1".to_owned(),
            port: pairing_port,
            pairing_code: SecretString::from(CODE.to_owned()),
            client_name: "ignored-after-credential-load".to_owned(),
        },
        &store,
    )
    .await?;
    assert_eq!(result.device_id, DEVICE_ID);
    assert_eq!(result.certificate_fingerprint.len(), 95);
    pairing_server.await??;

    let connect_listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let connect_port = connect_listener.local_addr()?.port();
    let connect_host = host_credential;
    let connect_device = device_credential;
    let connect_server = tokio::spawn(async move {
        run_connect_server(connect_listener, &connect_host, &connect_device).await
    });

    // A cloned store models a new application service instance after restart.
    let restarted_store = store.clone();
    let client =
        connect_paired_device(DEVICE_ID, "127.0.0.1", connect_port, &restarted_store).await?;
    let output = execute(&client, "getprop ro.product.model").await?;
    assert_eq!(output.stdout, Bytes::from_static(b"DroidMux Test Device\n"));
    assert!(output.stderr.is_empty());
    assert_eq!(output.exit_code, Some(0));
    client.close().await?;
    connect_server.await??;
    Ok(())
}

#[tokio::test]
async fn direct_tcp_connects_without_pairing_tls_and_executes_shell() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(run_plain_connect_server(listener));
    let store = MemoryStore::default();

    let client = connect_tcp_device(
        DirectConnectionRequest {
            host: address.ip().to_string(),
            port: address.port(),
            client_name: "droidmux@direct-test".to_owned(),
        },
        &store,
    )
    .await?;
    let output = execute(&client, "getprop ro.product.model").await?;
    assert_eq!(output.stdout, "DroidMux Test Device\n");
    assert_eq!(output.exit_code, Some(0));
    client.close().await?;
    server.await??;
    assert!(store.load_host_credential().await?.is_some());
    Ok(())
}

#[tokio::test]
async fn reports_a_wrong_six_digit_code_clearly() -> TestResult {
    let host_credential = RsaAdbCredential::generate("droidmux@wrong-code-test")?;
    let store = MemoryStore::with_credential(&host_credential).await?;
    let device_credential = RsaAdbCredential::generate("android@wrong-code-test")?;
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let port = listener.local_addr()?.port();
    let server = tokio::spawn(async move {
        run_pairing_server(
            listener,
            &host_credential,
            &device_credential,
            "654321",
            "wrong-code-device",
        )
        .await
    });

    let error = pair_device(
        PairingRequest {
            host: "127.0.0.1".to_owned(),
            port,
            pairing_code: SecretString::from("123456".to_owned()),
            client_name: "ignored-after-credential-load".to_owned(),
        },
        &store,
    )
    .await
    .expect_err("different pairing codes must fail");
    assert!(matches!(error, PairingError::IncorrectPairingCode));
    server.await??;
    Ok(())
}

async fn run_pairing_server(
    listener: TcpListener,
    host_credential: &RsaAdbCredential,
    device_credential: &RsaAdbCredential,
    code: &str,
    device_id: &str,
) -> TestResult {
    let (stream, _) = listener.accept().await?;
    let acceptor = tls_acceptor(host_credential, device_credential)?;
    let mut stream = acceptor.accept(stream).await?;

    let mut exporter = Zeroizing::new([0_u8; TLS_EXPORTER_SIZE]);
    stream
        .get_ref()
        .1
        .export_keying_material(&mut *exporter, TLS_EXPORTER_LABEL, None)?;
    let mut password = Zeroizing::new(Vec::with_capacity(6 + exporter.len()));
    password.extend_from_slice(code.as_bytes());
    password.extend_from_slice(&*exporter);

    let (spake, message) = Spake2State::start_server(&password)?;
    write_packet(&mut stream, PacketType::Spake2, &message).await?;
    let peer_message = read_packet(&mut stream, PacketType::Spake2).await?;
    let key = spake.finish(&peer_message)?;
    let mut cipher = PairingCipher::new(&*key)?;

    let encrypted_device = cipher.encrypt(PeerInfo::device(device_id).as_bytes())?;
    write_packet(&mut stream, PacketType::PeerInfo, &encrypted_device).await?;
    let encrypted_host = read_packet(&mut stream, PacketType::PeerInfo).await?;
    let host_info = cipher.decrypt(&encrypted_host);
    if code == "654321" {
        assert!(matches!(host_info, Err(PairingError::IncorrectPairingCode)));
    } else {
        let host_info = host_info?;
        assert_eq!(host_info[0], 0);
        assert!(host_info[1..].contains(&0));
    }
    Ok(())
}

async fn run_connect_server(
    listener: TcpListener,
    host_credential: &RsaAdbCredential,
    device_credential: &RsaAdbCredential,
) -> TestResult {
    let (mut stream, _) = listener.accept().await?;
    let connect = read_adb_packet(&mut stream).await?;
    assert_eq!(connect.command, AdbCommand::Connect);

    write_adb_packet(
        &mut stream,
        &AdbPacket::new(AdbCommand::StartTls, 0x0100_0000, 0, Bytes::new())?,
    )
    .await?;
    let stls = read_adb_packet(&mut stream).await?;
    assert_eq!(stls.command, AdbCommand::StartTls);

    let acceptor = tls_acceptor(host_credential, device_credential)?;
    let mut stream = acceptor.accept(stream).await?;
    write_adb_packet(
        &mut stream,
        &AdbPacket::new(
            AdbCommand::Connect,
            ADB_VERSION,
            4096,
            Bytes::from_static(b"device::product=droidmux-test;features=shell_v2,cmd;\0"),
        )?,
    )
    .await?;

    serve_one_shell_command(&mut stream).await
}

async fn run_plain_connect_server(listener: TcpListener) -> TestResult {
    let (mut stream, _) = listener.accept().await?;
    let connect = read_adb_packet(&mut stream).await?;
    assert_eq!(connect.command, AdbCommand::Connect);
    write_adb_packet(
        &mut stream,
        &AdbPacket::new(
            AdbCommand::Connect,
            ADB_VERSION,
            4096,
            Bytes::from_static(b"device::product=droidmux-test;features=shell_v2,cmd;\0"),
        )?,
    )
    .await?;
    serve_one_shell_command(&mut stream).await
}

async fn serve_one_shell_command<S>(stream: &mut S) -> TestResult
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let open = read_adb_packet(stream).await?;
    assert_eq!(open.command, AdbCommand::Open);
    assert_eq!(
        open.payload,
        Bytes::from_static(b"shell,v2,raw:getprop ro.product.model\0")
    );
    let local_id = open.arg0;
    let remote_id = 101;
    write_adb_packet(
        stream,
        &AdbPacket::new(AdbCommand::Okay, remote_id, local_id, Bytes::new())?,
    )
    .await?;

    let mut shell_payload = BytesMut::new();
    shell_payload.extend_from_slice(&shell_frame(1, b"DroidMux Test Device\n"));
    shell_payload.extend_from_slice(&shell_frame(3, &[0]));
    write_adb_packet(
        stream,
        &AdbPacket::new(
            AdbCommand::Write,
            remote_id,
            local_id,
            shell_payload.freeze(),
        )?,
    )
    .await?;
    assert_eq!(read_adb_packet(stream).await?.command, AdbCommand::Okay);
    let close = read_adb_packet(stream).await?;
    assert_eq!(close.command, AdbCommand::Close);
    Ok(())
}

fn tls_acceptor(
    _host_credential: &RsaAdbCredential,
    device_credential: &RsaAdbCredential,
) -> TestResult<TlsAcceptor> {
    let device_identity = TlsIdentity::from_credential(device_credential)?;
    let provider = Arc::new(ring::default_provider());
    let verifier = Arc::new(AcceptAnyClientCertificate::new(
        provider.signature_verification_algorithms,
    ));
    let config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&TLS13])?
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            vec![device_identity.certificate()],
            device_identity.private_key(),
        )?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[derive(Debug)]
struct AcceptAnyClientCertificate {
    supported: WebPkiSupportedAlgorithms,
    hints: Vec<DistinguishedName>,
}

impl AcceptAnyClientCertificate {
    fn new(supported: WebPkiSupportedAlgorithms) -> Self {
        Self {
            supported,
            hints: Vec::new(),
        }
    }
}

impl ClientCertVerifier for AcceptAnyClientCertificate {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &self.hints
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, certificate, signature, &self.supported)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, certificate, signature, &self.supported)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported.supported_schemes()
    }
}

async fn read_adb_packet<R>(reader: &mut R) -> TestResult<AdbPacket>
where
    R: AsyncRead + Unpin,
{
    let mut header_bytes = [0_u8; ADB_HEADER_LEN];
    reader.read_exact(&mut header_bytes).await?;
    let header = AdbHeader::decode(&header_bytes)?;
    let payload_length = usize::try_from(header.payload_length)?;
    let mut frame = Vec::with_capacity(ADB_HEADER_LEN + payload_length);
    frame.extend_from_slice(&header_bytes);
    frame.resize(ADB_HEADER_LEN + payload_length, 0);
    reader.read_exact(&mut frame[ADB_HEADER_LEN..]).await?;
    Ok(AdbPacket::decode(&frame)?.0)
}

async fn write_adb_packet<W>(writer: &mut W, packet: &AdbPacket) -> TestResult
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(&packet.encode()?).await?;
    Ok(())
}

fn shell_frame(id: u8, payload: &[u8]) -> Bytes {
    let mut frame = BytesMut::with_capacity(5 + payload.len());
    frame.put_u8(id);
    frame.put_u32_le(u32::try_from(payload.len()).expect("test shell frame should fit"));
    frame.extend_from_slice(payload);
    frame.freeze()
}
