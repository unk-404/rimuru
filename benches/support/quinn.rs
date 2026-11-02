//! Quinn ordered-stream adapter used only by the comparison benchmark.
//!
//! The comparison is an ordered-stream reference on one local connection, not a
//! transport-equivalence claim. Quinn provides encryption and stream semantics;
//! Rimuru provides datagram messages with per-message confirmation.

use quinn::{Endpoint, EndpointConfig, TokioRuntime, TransportConfig, VarInt};
use rimuru::benchmarking::checksum;
use std::{net::UdpSocket, sync::Arc};

const MESSAGE_BYTES: usize = 1_048_576;
const WINDOW: usize = 4;
const SOCKET_BUFFER: usize = 4 << 20;
const STREAM_WINDOW: u32 = 8 * 1024 * 1024;

pub struct Fixture {
    _client_endpoint: Endpoint,
    _server_endpoint: Endpoint,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    sequence: u64,
}

impl Fixture {
    pub async fn new(datagram: u16) -> Self {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let key = quinn::rustls::pki_types::PrivatePkcs8KeyDer::from(
            certificate.signing_key.serialize_der(),
        );
        let certificate = quinn::rustls::pki_types::CertificateDer::from(certificate.cert);
        let mut server =
            quinn::ServerConfig::with_single_cert(vec![certificate.clone()], key.into()).unwrap();
        let mut transport = TransportConfig::default();
        transport.initial_mtu(datagram);
        transport.mtu_discovery_config(None);
        transport.receive_window(VarInt::from_u32(STREAM_WINDOW));
        transport.stream_receive_window(VarInt::from_u32(STREAM_WINDOW));
        transport.send_window(u64::from(STREAM_WINDOW));
        let transport = Arc::new(transport);
        server.transport = transport.clone();
        let mut roots = quinn::rustls::RootCertStore::empty();
        roots.add(certificate).unwrap();
        let mut client = quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        client.transport_config(transport);
        let mut socket_config = EndpointConfig::default();
        socket_config.max_udp_payload_size(datagram).unwrap();
        let socket = || {
            let socket = socket2::Socket::new(
                socket2::Domain::IPV4,
                socket2::Type::DGRAM,
                Some(socket2::Protocol::UDP),
            )
            .unwrap();
            socket.set_recv_buffer_size(SOCKET_BUFFER).unwrap();
            socket.set_send_buffer_size(SOCKET_BUFFER).unwrap();
            socket
                .bind(
                    &"127.0.0.1:0"
                        .parse::<std::net::SocketAddr>()
                        .unwrap()
                        .into(),
                )
                .unwrap();
            UdpSocket::from(socket)
        };
        let server_endpoint = Endpoint::new(
            socket_config.clone(),
            Some(server),
            socket(),
            Arc::new(TokioRuntime),
        )
        .unwrap();
        let mut client_endpoint =
            Endpoint::new(socket_config, None, socket(), Arc::new(TokioRuntime)).unwrap();
        client_endpoint.set_default_client_config(client);
        let connecting = client_endpoint
            .connect(server_endpoint.local_addr().unwrap(), "localhost")
            .unwrap();
        let (client, server) = tokio::join!(connecting, async {
            server_endpoint.accept().await.unwrap().await
        });
        let client = client.unwrap();
        let server = server.unwrap();
        tokio::spawn(async move {
            let (mut send, mut recv) = server.accept_bi().await.unwrap();
            let mut payload = vec![0; MESSAGE_BYTES];
            let mut expected = 0_u64;
            loop {
                let mut header = [0; 12];
                if recv.read_exact(&mut header).await.is_err() {
                    break;
                }
                recv.read_exact(&mut payload).await.unwrap();
                let sequence = u64::from_le_bytes(header[..8].try_into().unwrap());
                let crc = u32::from_le_bytes(header[8..].try_into().unwrap());
                assert_eq!(sequence, expected);
                assert_eq!(checksum(0, &payload), crc);
                send.write_all(&sequence.to_le_bytes()).await.unwrap();
                expected += 1;
            }
        });
        let (send, recv) = client.open_bi().await.unwrap();
        Self {
            _client_endpoint: client_endpoint,
            _server_endpoint: server_endpoint,
            send,
            recv,
            sequence: 0,
        }
    }

    pub async fn window(&mut self, payload: &[u8]) {
        assert_eq!(payload.len(), MESSAGE_BYTES);
        for _ in 0..WINDOW {
            let crc = checksum(0, payload);
            self.send
                .write_all(&self.sequence.to_le_bytes())
                .await
                .unwrap();
            self.send.write_all(&crc.to_le_bytes()).await.unwrap();
            self.send.write_all(payload).await.unwrap();
            self.sequence += 1;
        }
        for sequence in self.sequence - WINDOW as u64..self.sequence {
            let mut acknowledgement = [0; 8];
            self.recv.read_exact(&mut acknowledgement).await.unwrap();
            assert_eq!(u64::from_le_bytes(acknowledgement), sequence);
        }
    }
}
