use crate::channel::ObjectRpcChannelWireFrame;
use crate::daemon_auth::{
    DAEMON_AUTH_PROTOCOL, SignedDaemonCommand, daemon_auth_decode_public_key,
    daemon_auth_encode_public_key, daemon_auth_signing_payload, daemon_rpc_body_sha256,
    sign_daemon_rpc_request_from_data_dir,
};
use crate::{NetworkError, ObjectRpcRequest, SinomoObjectCodec};
use ring::signature::{ED25519, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use sinomo_local::SinomoDataDir;

pub const CLUSTER_TCP_TUNNEL_OPEN_ROUTE: &str = "/cluster/tcp-tunnel/open";
pub const CLUSTER_TCP_TUNNEL_RPC_CATEGORY: &str = "cluster.tcp-tunnel";
pub const CLUSTER_TCP_TUNNEL_MIN_PROTOCOL_VERSION: u16 = 1;
pub const CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION: u16 = 2;
pub const CLUSTER_TCP_TUNNEL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION: u16 = 2;
pub const CLUSTER_TCP_TUNNEL_ENDPOINT_ADDRESS_PREFIX: &str = "tcp-tunnel://";
pub const CLUSTER_TCP_TUNNEL_HANDSHAKE_PROTOCOL: &str = "sinomo.cluster-tcp-tunnel-handshake.v2";
const CLUSTER_TCP_TUNNEL_HANDSHAKE_PROOF_ROUTE: &str = "/cluster/tcp-tunnel/handshake-proof";
const CLUSTER_TCP_TUNNEL_HANDSHAKE_PROOF_CATEGORY: &str = "cluster.tcp-tunnel.handshake";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterTcpTunnelOpenRequest {
    pub protocol_version: u16,
}

/// The one-time connection transcript authenticated before a v2 tunnel can
/// expose its unsigned object-RPC channel. The dialer's already-signed open
/// nonce is the challenge, so replaying a prior response cannot authenticate a
/// new physical connection.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterTcpTunnelHandshakeTranscript {
    pub protocol: String,
    pub requester_node_id: String,
    pub requester_public_key: String,
    pub responder_node_id: String,
    pub responder_public_key: String,
    pub requested_protocol_version: u16,
    pub negotiated_protocol_version: u16,
    pub dialer_nonce: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_context: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterTcpTunnelHandshakeProof {
    pub transcript: ClusterTcpTunnelHandshakeTranscript,
    pub signed_command: SignedDaemonCommand,
}

impl Default for ClusterTcpTunnelOpenRequest {
    fn default() -> Self {
        Self {
            protocol_version: CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterTcpTunnelOpenResponse {
    pub protocol_version: u16,
    pub accepted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handshake_proof: Option<ClusterTcpTunnelHandshakeProof>,
}

impl ClusterTcpTunnelOpenResponse {
    pub fn accepted() -> Self {
        Self::accepted_version(CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION)
    }

    pub const fn accepted_version(protocol_version: u16) -> Self {
        Self {
            protocol_version,
            accepted: true,
            handshake_proof: None,
        }
    }

    pub fn accepted_version_with_handshake(
        protocol_version: u16,
        handshake_proof: ClusterTcpTunnelHandshakeProof,
    ) -> Self {
        Self {
            protocol_version,
            accepted: true,
            handshake_proof: Some(handshake_proof),
        }
    }
}

fn cluster_tcp_tunnel_handshake_proof_request(
    transcript: &ClusterTcpTunnelHandshakeTranscript,
) -> Result<ObjectRpcRequest, NetworkError> {
    ObjectRpcRequest::encode_body(
        CLUSTER_TCP_TUNNEL_HANDSHAKE_PROOF_ROUTE,
        CLUSTER_TCP_TUNNEL_HANDSHAKE_PROOF_CATEGORY,
        transcript,
    )
}

/// Signs one physical-tunnel transcript with the process-cached daemon
/// identity. This runs only during connection setup; tunnel data frames carry
/// no signatures or authentication timers.
pub fn sign_cluster_tcp_tunnel_handshake_from_data_dir(
    data_dir: &SinomoDataDir,
    transcript: ClusterTcpTunnelHandshakeTranscript,
) -> Result<ClusterTcpTunnelHandshakeProof, NetworkError> {
    let request = cluster_tcp_tunnel_handshake_proof_request(&transcript)?;
    let encoded = sign_daemon_rpc_request_from_data_dir(data_dir, &request)?;
    let signed_command: SignedDaemonCommand = SinomoObjectCodec::default().decode(&encoded)?;
    if signed_command.node_id != transcript.responder_node_id
        || daemon_auth_encode_public_key(&signed_command.public_key)
            != transcript.responder_public_key
    {
        return Err(NetworkError::AuthDenied);
    }
    Ok(ClusterTcpTunnelHandshakeProof {
        transcript,
        signed_command,
    })
}

/// Verifies both the exact dialer challenge transcript and the endpoint's
/// authorized node identity before a v2 no-per-packet-auth channel is exposed.
pub fn verify_cluster_tcp_tunnel_handshake(
    proof: &ClusterTcpTunnelHandshakeProof,
    expected_transcript: &ClusterTcpTunnelHandshakeTranscript,
) -> Result<(), NetworkError> {
    if &proof.transcript != expected_transcript
        || proof.transcript.protocol != CLUSTER_TCP_TUNNEL_HANDSHAKE_PROTOCOL
        || proof.signed_command.protocol != DAEMON_AUTH_PROTOCOL
        || proof.signed_command.node_id != expected_transcript.responder_node_id
        || daemon_auth_encode_public_key(&proof.signed_command.public_key)
            != expected_transcript.responder_public_key
    {
        return Err(NetworkError::AuthDenied);
    }
    let expected_public_key =
        daemon_auth_decode_public_key(&expected_transcript.responder_public_key)?;
    if proof.signed_command.public_key != expected_public_key {
        return Err(NetworkError::AuthDenied);
    }
    let request = cluster_tcp_tunnel_handshake_proof_request(expected_transcript)?;
    let body_sha256 = daemon_rpc_body_sha256(&request);
    if proof.signed_command.rpc_body_sha256.as_deref() != Some(body_sha256.as_slice()) {
        return Err(NetworkError::AuthDenied);
    }
    let payload = daemon_auth_signing_payload(
        &proof.signed_command.node_id,
        &expected_public_key,
        &proof.signed_command.timestamp,
        &proof.signed_command.nonce,
        CLUSTER_TCP_TUNNEL_HANDSHAKE_PROOF_ROUTE,
        CLUSTER_TCP_TUNNEL_HANDSHAKE_PROOF_CATEGORY,
        Some(&body_sha256),
    );
    UnparsedPublicKey::new(&ED25519, expected_public_key)
        .verify(&payload, &proof.signed_command.signature)
        .map_err(|_| NetworkError::AuthDenied)
}

pub const fn negotiate_cluster_tcp_tunnel_protocol(requested_maximum: u16) -> Option<u16> {
    if requested_maximum < CLUSTER_TCP_TUNNEL_MIN_PROTOCOL_VERSION {
        None
    } else if requested_maximum > CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION {
        Some(CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION)
    } else {
        Some(requested_maximum)
    }
}

pub const fn cluster_tcp_tunnel_protocol_supports_object_rpc_channel(
    protocol_version: u16,
) -> bool {
    protocol_version >= CLUSTER_TCP_TUNNEL_OBJECT_RPC_CHANNEL_PROTOCOL_VERSION
        && protocol_version <= CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ClusterTcpTunnelFrame {
    /// Carries one bounded logical object-RPC channel frame. This variant is
    /// emitted only when tunnel protocol v2 or newer was negotiated.
    ObjectRpcChannel {
        frame: ObjectRpcChannelWireFrame,
    },
    /// Opens one canonical full-duplex object-RPC byte stream inside the
    /// persistent tunnel. Stream identifiers are allocated from disjoint
    /// odd/even spaces by the two physical tunnel endpoints.
    StreamOpen {
        id: u64,
    },
    /// Confirms that the peer created the logical stream and attached its
    /// canonical daemon session handler. Callers do not consume one-pass body
    /// sources until this acknowledgement arrives.
    StreamAccept {
        id: u64,
    },
    /// Carries an ordered byte-stream fragment for one logical session.
    StreamData {
        id: u64,
        bytes: Vec<u8>,
    },
    /// Half-closes the sender-to-receiver direction of one logical session.
    StreamFinish {
        id: u64,
    },
    /// Aborts both directions of one logical session without closing the
    /// persistent tunnel or disturbing other multiplexed sessions.
    StreamReset {
        id: u64,
        detail: String,
    },
    Close {
        detail: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterPaths, ObjectRpcChannelCallId, SinomoObjectCodec};

    struct HandshakeFixture {
        _root: sinomo_testdir::TestDir,
        data_dir: SinomoDataDir,
        transcript: ClusterTcpTunnelHandshakeTranscript,
    }

    fn handshake_fixture(name: &str) -> HandshakeFixture {
        let root = sinomo_testdir::TestDir::new(name);
        let data_dir = SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().expect("root layout");
        let identity = sinomo_node::load_or_create_main_node_identity(&data_dir.root())
            .expect("responder identity");
        let responder_node_id =
            sinomo_base::Id::from_legacy_named(format!("{name}_responder")).expect("responder id");
        let cluster_id =
            sinomo_base::Id::from_legacy_named(format!("{name}_cluster")).expect("cluster id");
        let state = sinomo_base::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "handshake test",
            responder_node_id.clone(),
            "responder",
            identity.public_key.clone(),
            time::OffsetDateTime::now_utc(),
        );
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        crate::object_file_store::write_object_file(&cluster_paths.state, &state)
            .expect("cluster state");
        HandshakeFixture {
            _root: root,
            data_dir,
            transcript: ClusterTcpTunnelHandshakeTranscript {
                protocol: CLUSTER_TCP_TUNNEL_HANDSHAKE_PROTOCOL.to_string(),
                requester_node_id: format!("{name}_requester"),
                requester_public_key: daemon_auth_encode_public_key(&[7; 32]),
                responder_node_id: responder_node_id.to_string(),
                responder_public_key: identity.public_key,
                requested_protocol_version: CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION,
                negotiated_protocol_version: CLUSTER_TCP_TUNNEL_PROTOCOL_VERSION,
                dialer_nonce: format!("{name}-fresh-dialer-nonce"),
                cluster_context: Some(cluster_id.to_string()),
            },
        }
    }

    #[test]
    fn tunnel_protocol_negotiates_v1_fallback_and_v2_channel() {
        assert_eq!(negotiate_cluster_tcp_tunnel_protocol(0), None);
        assert_eq!(negotiate_cluster_tcp_tunnel_protocol(1), Some(1));
        assert_eq!(negotiate_cluster_tcp_tunnel_protocol(2), Some(2));
        assert_eq!(negotiate_cluster_tcp_tunnel_protocol(3), Some(2));
        assert!(!cluster_tcp_tunnel_protocol_supports_object_rpc_channel(1));
        assert!(cluster_tcp_tunnel_protocol_supports_object_rpc_channel(2));
    }

    #[test]
    fn tunnel_frame_round_trips_legacy_stream_and_v2_channel_variants() {
        let codec = SinomoObjectCodec::network_frame();
        let frames = [
            ClusterTcpTunnelFrame::StreamOpen { id: 7 },
            ClusterTcpTunnelFrame::ObjectRpcChannel {
                frame: ObjectRpcChannelWireFrame::Reset {
                    call_id: ObjectRpcChannelCallId(9),
                    reason: "test reset".to_string(),
                },
            },
        ];
        for frame in frames {
            let encoded = codec.encode(&frame).expect("encode tunnel frame");
            let decoded: ClusterTcpTunnelFrame =
                codec.decode(&encoded).expect("decode tunnel frame");
            assert_eq!(decoded, frame);
        }
    }

    #[test]
    fn v2_tunnel_handshake_accepts_valid_target_signed_transcript() {
        let fixture = handshake_fixture("tcp_tunnel_handshake_valid");
        let proof = sign_cluster_tcp_tunnel_handshake_from_data_dir(
            &fixture.data_dir,
            fixture.transcript.clone(),
        )
        .expect("sign handshake");

        verify_cluster_tcp_tunnel_handshake(&proof, &fixture.transcript)
            .expect("valid target proof");
    }

    #[test]
    fn v2_tunnel_handshake_rejects_tampered_protocol_or_cluster_context() {
        let fixture = handshake_fixture("tcp_tunnel_handshake_protocol");
        let proof =
            sign_cluster_tcp_tunnel_handshake_from_data_dir(&fixture.data_dir, fixture.transcript)
                .expect("sign handshake");
        let mut tampered_protocol = proof.clone();
        tampered_protocol.transcript.negotiated_protocol_version = 1;
        let expected_protocol = tampered_protocol.transcript.clone();

        assert_eq!(
            verify_cluster_tcp_tunnel_handshake(&tampered_protocol, &expected_protocol),
            Err(NetworkError::AuthDenied)
        );

        let mut tampered_cluster = proof;
        tampered_cluster.transcript.cluster_context = Some("different-cluster".to_string());
        let expected_cluster = tampered_cluster.transcript.clone();
        assert_eq!(
            verify_cluster_tcp_tunnel_handshake(&tampered_cluster, &expected_cluster),
            Err(NetworkError::AuthDenied)
        );
    }

    #[test]
    fn v2_tunnel_handshake_rejects_replayed_challenge() {
        let fixture = handshake_fixture("tcp_tunnel_handshake_replay");
        let proof = sign_cluster_tcp_tunnel_handshake_from_data_dir(
            &fixture.data_dir,
            fixture.transcript.clone(),
        )
        .expect("sign handshake");
        let mut next_connection = fixture.transcript;
        next_connection.dialer_nonce = "different-fresh-dialer-nonce".to_string();

        assert_eq!(
            verify_cluster_tcp_tunnel_handshake(&proof, &next_connection),
            Err(NetworkError::AuthDenied)
        );
    }

    #[test]
    fn v2_tunnel_handshake_rejects_wrong_target_identity() {
        let fixture = handshake_fixture("tcp_tunnel_handshake_target");
        let proof = sign_cluster_tcp_tunnel_handshake_from_data_dir(
            &fixture.data_dir,
            fixture.transcript.clone(),
        )
        .expect("sign handshake");
        let mut wrong_node = fixture.transcript.clone();
        wrong_node.responder_node_id = "different-target-node".to_string();
        assert_eq!(
            verify_cluster_tcp_tunnel_handshake(&proof, &wrong_node),
            Err(NetworkError::AuthDenied)
        );

        let mut wrong_key = fixture.transcript;
        wrong_key.responder_public_key = daemon_auth_encode_public_key(&[9; 32]);
        assert_eq!(
            verify_cluster_tcp_tunnel_handshake(&proof, &wrong_key),
            Err(NetworkError::AuthDenied)
        );
    }
}
