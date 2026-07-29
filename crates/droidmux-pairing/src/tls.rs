use std::{fmt::Write as _, net::IpAddr, sync::Arc};

use adb_auth::RsaAdbCredential;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair, KeyUsagePurpose,
    PKCS_RSA_SHA256, SerialNumber,
};
use rustls::{
    ClientConfig, DigitallySignedStruct, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{WebPkiSupportedAlgorithms, ring, verify_tls12_signature, verify_tls13_signature},
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime},
    version::TLS13,
};
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};
use zeroize::Zeroizing;

use crate::PairingError;

pub(crate) const TLS_EXPORTER_LABEL: &[u8] = b"adb-label\0";
pub(crate) const TLS_EXPORTER_SIZE: usize = 64;

pub(crate) struct TlsIdentity {
    certificate: CertificateDer<'static>,
    private_key: PrivateKeyDer<'static>,
}

impl TlsIdentity {
    pub(crate) fn from_credential(credential: &RsaAdbCredential) -> Result<Self, PairingError> {
        let private_key_pem = credential.to_pkcs8_pem()?;
        Self::from_pkcs8_pem(&private_key_pem)
    }

    fn from_pkcs8_pem(private_key_pem: &str) -> Result<Self, PairingError> {
        let key_pair = KeyPair::from_pkcs8_pem_and_sign_algo(private_key_pem, &PKCS_RSA_SHA256)?;
        let mut params = CertificateParams::default();
        params.serial_number = Some(SerialNumber::from(1_u64));
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let mut distinguished_name = DistinguishedName::new();
        distinguished_name.push(DnType::CountryName, "US");
        distinguished_name.push(DnType::OrganizationName, "Android");
        distinguished_name.push(DnType::CommonName, "Adb");
        params.distinguished_name = distinguished_name;
        let certificate = params.self_signed(&key_pair)?.der().clone();
        let private_key = PrivatePkcs8KeyDer::from(key_pair.serialize_der()).into();
        Ok(Self {
            certificate,
            private_key,
        })
    }

    pub(crate) fn client_config(&self) -> Result<ClientConfig, PairingError> {
        let provider = Arc::new(ring::default_provider());
        let verifier = Arc::new(AcceptAnyCertificate::new(
            provider.signature_verification_algorithms,
        ));
        Ok(ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&TLS13])?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(vec![self.certificate.clone()], self.private_key.clone_key())?)
    }

    #[cfg(test)]
    pub(crate) fn certificate(&self) -> CertificateDer<'static> {
        self.certificate.clone()
    }

    #[cfg(test)]
    pub(crate) fn private_key(&self) -> PrivateKeyDer<'static> {
        self.private_key.clone_key()
    }
}

pub(crate) async fn connect_tls(
    stream: TcpStream,
    host: &str,
    identity: &TlsIdentity,
) -> Result<TlsStream<TcpStream>, PairingError> {
    let server_name = server_name(host)?;
    let connector = TlsConnector::from(Arc::new(identity.client_config()?));
    connector
        .connect(server_name, stream)
        .await
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                PairingError::NotPairingEndpoint
            } else {
                PairingError::Connection(error)
            }
        })
}

pub(crate) fn export_pairing_key(
    stream: &TlsStream<TcpStream>,
) -> Result<Zeroizing<[u8; TLS_EXPORTER_SIZE]>, PairingError> {
    let mut key = Zeroizing::new([0_u8; TLS_EXPORTER_SIZE]);
    stream
        .get_ref()
        .1
        .export_keying_material(&mut *key, TLS_EXPORTER_LABEL, None)?;
    Ok(key)
}

pub(crate) fn peer_certificate_fingerprint(
    stream: &TlsStream<TcpStream>,
) -> Result<String, PairingError> {
    let certificate = stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .ok_or_else(|| PairingError::Tls("peer did not provide a certificate".to_owned()))?;
    let digest = Sha256::digest(certificate.as_ref());
    let mut output = String::with_capacity(digest.len() * 3 - 1);
    for (index, byte) in digest.iter().enumerate() {
        if index > 0 {
            output.push(':');
        }
        write!(&mut output, "{byte:02X}").map_err(|_| {
            PairingError::Tls("unable to format certificate fingerprint".to_owned())
        })?;
    }
    Ok(output)
}

fn server_name(host: &str) -> Result<ServerName<'static>, PairingError> {
    if let Ok(address) = host.parse::<IpAddr>() {
        return Ok(ServerName::IpAddress(address.into()));
    }
    ServerName::try_from(host.to_owned())
        .map_err(|_| PairingError::InvalidEndpoint("invalid DNS host name".to_owned()))
}

#[derive(Debug)]
struct AcceptAnyCertificate {
    supported: WebPkiSupportedAlgorithms,
}

impl AcceptAnyCertificate {
    fn new(supported: WebPkiSupportedAlgorithms) -> Self {
        Self { supported }
    }
}

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        // Authenticity is established by SPAKE2 and the TLS exporter during
        // pairing, and by Android's allow-list of the client public key later.
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, certificate, signature, &self.supported)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, certificate, signature, &self.supported)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported.supported_schemes()
    }
}
