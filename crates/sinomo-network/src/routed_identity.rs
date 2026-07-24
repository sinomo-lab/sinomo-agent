//! Daemon-identity adapter for direct-transport authentication.
//!
//! Built-in transports authenticate their adjacent peer once. Routed sessions
//! and object RPC reuse that established identity and do not sign their own
//! records.

use crate::daemon_auth::{
    DAEMON_AUTH_PROTOCOL, SignedDaemonCommand, cached_daemon_signing_public_key,
    daemon_auth_decode_public_key, daemon_auth_signing_payload, daemon_rpc_body_sha256,
    sign_daemon_rpc_request_from_data_dir,
};
use crate::routing::RoutedSessionIdentity;
use crate::{NetworkError, ObjectRpcRequest, SinomoObjectCodec};
use ring::signature::{ED25519, UnparsedPublicKey};
use sinomo_local::SinomoDataDir;
use sinomo_node::NodeId;

const TRANSPORT_AUTH_ROUTE: &str = "/internal/network/transport-transcript";
const TRANSPORT_AUTH_CATEGORY: &str = "network.transport";

fn transport_auth_request(transcript: &[u8]) -> ObjectRpcRequest {
    ObjectRpcRequest::from_encoded_body(
        TRANSPORT_AUTH_ROUTE,
        TRANSPORT_AUTH_CATEGORY,
        transcript.to_vec(),
    )
}

/// Uses the process-cached main-daemon identity only for adjacent transport
/// establishment.
pub struct DaemonRoutedSessionIdentity {
    local_node_id: NodeId,
    data_dir: SinomoDataDir,
}

impl DaemonRoutedSessionIdentity {
    pub fn new(local_node_id: NodeId, data_dir: SinomoDataDir) -> Result<Self, NetworkError> {
        let _ = cached_daemon_signing_public_key(&data_dir)?;
        Ok(Self {
            local_node_id,
            data_dir,
        })
    }
}

impl RoutedSessionIdentity for DaemonRoutedSessionIdentity {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    fn sign(&self, transcript: &[u8]) -> Result<Vec<u8>, NetworkError> {
        let request = transport_auth_request(transcript);
        let encoded = sign_daemon_rpc_request_from_data_dir(&self.data_dir, &request)?;
        let signed: SignedDaemonCommand = SinomoObjectCodec::default().decode(&encoded)?;
        if signed.node_id != self.local_node_id.as_str() {
            return Err(NetworkError::AuthDenied);
        }
        Ok(encoded)
    }

    fn verify(
        &self,
        signer_node_id: &NodeId,
        authorized_public_key: &str,
        transcript: &[u8],
        signature: &[u8],
    ) -> Result<(), NetworkError> {
        let signed: SignedDaemonCommand = SinomoObjectCodec::default().decode(signature)?;
        let expected_public_key = daemon_auth_decode_public_key(authorized_public_key)?;
        let request = transport_auth_request(transcript);
        let body_sha256 = daemon_rpc_body_sha256(&request);
        if signed.protocol != DAEMON_AUTH_PROTOCOL
            || signed.node_id != signer_node_id.as_str()
            || signed.public_key != expected_public_key
            || signed.rpc_body_sha256.as_deref() != Some(body_sha256.as_slice())
        {
            return Err(NetworkError::AuthDenied);
        }
        let payload = daemon_auth_signing_payload(
            &signed.node_id,
            &expected_public_key,
            &signed.timestamp,
            &signed.nonce,
            TRANSPORT_AUTH_ROUTE,
            TRANSPORT_AUTH_CATEGORY,
            Some(&body_sha256),
        );
        UnparsedPublicKey::new(&ED25519, expected_public_key)
            .verify(&payload, &signed.signature)
            .map_err(|_| NetworkError::AuthDenied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon_auth::daemon_auth_encode_public_key;

    #[test]
    fn daemon_identity_signs_exact_transport_transcript() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-transport-identity");
        let data_dir = SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().expect("data root");
        let node_identity = sinomo_node::load_or_create_main_node_identity(&data_dir.root())
            .expect("main node identity");
        let node_id = NodeId::from_legacy_named("node_transport_identity").expect("local node id");
        let cluster_id = sinomo_node::ClusterId::from_legacy_named("cluster_transport_identity")
            .expect("cluster id");
        let cluster_paths = crate::ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let state = sinomo_base::ClusterBootstrapState::initialized_locally(
            cluster_id,
            "transport identity",
            node_id.clone(),
            "transport-node",
            node_identity.public_key,
            time::OffsetDateTime::now_utc(),
        );
        crate::object_file_store::write_object_file(&cluster_paths.state, &state)
            .expect("cluster state");
        let identity = DaemonRoutedSessionIdentity::new(node_id.clone(), data_dir.clone())
            .expect("transport identity");
        let public_key = daemon_auth_encode_public_key(
            &cached_daemon_signing_public_key(&data_dir).expect("public key"),
        );
        let transcript = b"bounded-transport-transcript";
        let signature = identity.sign(transcript).expect("transport signature");

        identity
            .verify(&node_id, &public_key, transcript, &signature)
            .expect("verify transcript");
        assert_eq!(
            identity.verify(&node_id, &public_key, b"tampered", &signature),
            Err(NetworkError::AuthDenied)
        );
    }
}
