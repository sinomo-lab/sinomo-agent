use crate::{ClusterPaths, NetworkError, ObjectRpcRequest, SinomoObjectCodec};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::digest;
use serde::{Deserialize, Serialize};
use sinomo_local::SinomoDataDir;
use sinomo_node::load_or_create_main_node_identity;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const DAEMON_AUTH_PROTOCOL: &str = "sinomo.daemon-auth.v1";

const DAEMON_RPC_REQUEST_DIGEST_DOMAIN: &[u8] = b"sinomo.daemon-rpc-request\0";

static DAEMON_SIGNING_IDENTITIES: OnceLock<Mutex<HashMap<PathBuf, CachedDaemonSigningIdentity>>> =
    OnceLock::new();
static DAEMON_AUTH_NONCE_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct CachedDaemonSigningIdentity {
    node_id: String,
    public_key: Vec<u8>,
    key_pair: Arc<ring::signature::Ed25519KeyPair>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedDaemonCommand {
    pub protocol: String,
    pub node_id: String,
    #[serde(
        serialize_with = "crate::fallible_serde::serialize_bytes",
        deserialize_with = "crate::fallible_serde::deserialize_bytes"
    )]
    pub public_key: Vec<u8>,
    pub timestamp: String,
    pub nonce: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::fallible_serde::serialize_optional_bytes",
        deserialize_with = "crate::fallible_serde::deserialize_optional_bytes"
    )]
    pub rpc_body_sha256: Option<Vec<u8>>,
    #[serde(
        serialize_with = "crate::fallible_serde::serialize_bytes",
        deserialize_with = "crate::fallible_serde::deserialize_bytes"
    )]
    pub signature: Vec<u8>,
}

pub fn sign_daemon_rpc_request_for_local_node(
    request: &ObjectRpcRequest,
) -> Result<Vec<u8>, NetworkError> {
    let data_dir = SinomoDataDir::from_env_or_home().map_err(|source| NetworkError::Io {
        operation: "daemon RPC auth data directory".to_string(),
        detail: source.to_string(),
    })?;
    sign_daemon_rpc_request_from_data_dir(&data_dir, request)
}

pub fn sign_daemon_rpc_request_from_data_dir(
    data_dir: &SinomoDataDir,
    request: &ObjectRpcRequest,
) -> Result<Vec<u8>, NetworkError> {
    sign_daemon_rpc_request_from_data_dir_with_body_sha256(
        data_dir,
        &request.route,
        &request.category,
        Some(daemon_rpc_body_sha256(request)),
    )
}

pub fn sign_daemon_rpc_request_digest_for_local_node(
    route: &str,
    category: &str,
    rpc_body_sha256: Vec<u8>,
) -> Result<Vec<u8>, NetworkError> {
    let data_dir = SinomoDataDir::from_env_or_home().map_err(|source| NetworkError::Io {
        operation: "daemon RPC auth data directory".to_string(),
        detail: source.to_string(),
    })?;
    sign_daemon_rpc_request_digest_from_data_dir(&data_dir, route, category, rpc_body_sha256)
}

pub fn sign_daemon_rpc_request_digest_from_data_dir(
    data_dir: &SinomoDataDir,
    route: &str,
    category: &str,
    rpc_body_sha256: Vec<u8>,
) -> Result<Vec<u8>, NetworkError> {
    sign_daemon_rpc_request_from_data_dir_with_body_sha256(
        data_dir,
        route,
        category,
        Some(rpc_body_sha256),
    )
}

/// Digests the RPC body and its sender-controlled metadata for daemon authentication.
///
/// The historical name remains part of the public API, but the digest deliberately
/// binds the complete request metadata map so routing retries cannot alter delivery
/// identity or retry-safety declarations after signing.
pub fn daemon_rpc_body_sha256(request: &ObjectRpcRequest) -> Vec<u8> {
    let mut digest = digest::Context::new(&digest::SHA256);
    digest.update(DAEMON_RPC_REQUEST_DIGEST_DOMAIN);
    daemon_rpc_digest_update_bytes(&mut digest, &request.body);
    daemon_rpc_digest_update_len(&mut digest, request.metadata.len());
    for (key, value) in &request.metadata {
        daemon_rpc_digest_update_bytes(&mut digest, key.as_bytes());
        daemon_rpc_digest_update_bytes(&mut digest, value.as_bytes());
    }
    digest.finish().as_ref().to_vec()
}

/// Computes the body-only digest used by daemon-auth implementations before
/// request metadata became part of the authenticated request.
///
/// New requests must use [`daemon_rpc_body_sha256`]. This is exposed only so a
/// rolling-upgrade client can retry a request after an older daemon explicitly
/// rejects the metadata-bound signature, and so a new daemon can recognize the
/// narrowly allowed legacy status probe.
pub fn daemon_rpc_legacy_body_sha256(request: &ObjectRpcRequest) -> Vec<u8> {
    digest::digest(&digest::SHA256, &request.body)
        .as_ref()
        .to_vec()
}

fn daemon_rpc_digest_update_bytes(digest: &mut digest::Context, bytes: &[u8]) {
    daemon_rpc_digest_update_len(digest, bytes.len());
    digest.update(bytes);
}

fn daemon_rpc_digest_update_len(digest: &mut digest::Context, len: usize) {
    let len = u64::try_from(len).expect("daemon RPC auth input length must fit in u64");
    digest.update(&len.to_be_bytes());
}

fn sign_daemon_rpc_request_from_data_dir_with_body_sha256(
    data_dir: &SinomoDataDir,
    route: &str,
    category: &str,
    rpc_body_sha256: Option<Vec<u8>>,
) -> Result<Vec<u8>, NetworkError> {
    let identity = cached_daemon_signing_identity(data_dir)?;
    let timestamp = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let nonce = format!("{}-{}", std::process::id(), auth_nonce_suffix());
    let signing_payload = daemon_auth_signing_payload(
        &identity.node_id,
        &identity.public_key,
        &timestamp,
        &nonce,
        route,
        category,
        rpc_body_sha256.as_deref(),
    );
    let signature = identity.key_pair.sign(&signing_payload);
    let envelope = SignedDaemonCommand {
        protocol: DAEMON_AUTH_PROTOCOL.to_string(),
        node_id: identity.node_id,
        public_key: identity.public_key,
        timestamp,
        nonce,
        rpc_body_sha256,
        signature: signature.as_ref().to_vec(),
    };
    SinomoObjectCodec::default().encode(&envelope)
}

fn cached_daemon_signing_identity(
    data_dir: &SinomoDataDir,
) -> Result<CachedDaemonSigningIdentity, NetworkError> {
    let root = data_dir.root();
    let cached = DAEMON_SIGNING_IDENTITIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|source| NetworkError::Io {
            operation: "daemon RPC auth identity cache".to_string(),
            detail: source.to_string(),
        })?
        .get(&root)
        .cloned();
    if let Some(cached) = cached {
        let Some(node_id) = local_cluster_node_id(data_dir) else {
            return Ok(cached);
        };
        if node_id != cached.node_id {
            let refreshed = CachedDaemonSigningIdentity { node_id, ..cached };
            DAEMON_SIGNING_IDENTITIES
                .get_or_init(|| Mutex::new(HashMap::new()))
                .lock()
                .map_err(|source| NetworkError::Io {
                    operation: "daemon RPC auth identity cache".to_string(),
                    detail: source.to_string(),
                })?
                .insert(root, refreshed.clone());
            return Ok(refreshed);
        }
        return Ok(cached);
    }

    data_dir
        .ensure_root_layout()
        .map_err(|source| NetworkError::Io {
            operation: "daemon RPC auth data directory".to_string(),
            detail: source.to_string(),
        })?;
    let identity = load_or_create_main_node_identity(&root).map_err(|source| NetworkError::Io {
        operation: "daemon RPC auth identity".to_string(),
        detail: source.to_string(),
    })?;
    let node_id = local_cluster_node_id(data_dir).unwrap_or_else(|| "node_local".to_string());
    let public_key = daemon_auth_decode_public_key(&identity.public_key)?;
    let key_pair = Arc::new(identity.key_pair().map_err(|source| NetworkError::Io {
        operation: "daemon RPC auth identity".to_string(),
        detail: source.to_string(),
    })?);
    let loaded = CachedDaemonSigningIdentity {
        node_id,
        public_key,
        key_pair,
    };

    let mut guard = DAEMON_SIGNING_IDENTITIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|source| NetworkError::Io {
            operation: "daemon RPC auth identity cache".to_string(),
            detail: source.to_string(),
        })?;
    Ok(guard.entry(root).or_insert_with(|| loaded).clone())
}

fn local_cluster_node_id(data_dir: &SinomoDataDir) -> Option<String> {
    // Raw peek at the bootstrap state file for the signing label only; the
    // canonical cached cluster-state reader lives in sinomo-cluster.
    crate::object_file_store::read_object_file::<sinomo_base::ClusterBootstrapState>(
        &ClusterPaths::new(data_dir).state,
    )
    .ok()
    .map(|state| state.local_node_id.to_string())
}

pub fn daemon_auth_decode_public_key(public_key: &str) -> Result<Vec<u8>, NetworkError> {
    URL_SAFE_NO_PAD
        .decode(public_key.as_bytes())
        .map_err(|_| NetworkError::AuthDenied)
}

pub fn daemon_auth_encode_public_key(public_key: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(public_key)
}

pub fn daemon_auth_signing_payload(
    node_id: &str,
    public_key: &[u8],
    timestamp: &str,
    nonce: &str,
    route: &str,
    category: &str,
    rpc_body_sha256: Option<&[u8]>,
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(DAEMON_AUTH_PROTOCOL.as_bytes());
    payload.push(b'\n');
    payload.extend_from_slice(node_id.as_bytes());
    payload.push(b'\n');
    payload.extend_from_slice(public_key);
    payload.push(b'\n');
    payload.extend_from_slice(timestamp.as_bytes());
    payload.push(b'\n');
    payload.extend_from_slice(nonce.as_bytes());
    payload.push(b'\n');
    payload.extend_from_slice(route.as_bytes());
    payload.push(b'\n');
    payload.extend_from_slice(category.as_bytes());
    payload.push(b'\n');
    if let Some(rpc_body_sha256) = rpc_body_sha256 {
        payload.extend_from_slice(rpc_body_sha256);
    }
    payload
}

fn auth_nonce_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let sequence = DAEMON_AUTH_NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos}-{sequence}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectRpcRequest;
    use std::collections::BTreeSet;
    use time::OffsetDateTime;

    #[test]
    fn daemon_auth_nonce_suffix_is_unique_under_concurrent_signing() {
        let mut workers = Vec::new();
        for _ in 0..16 {
            workers.push(std::thread::spawn(|| {
                let mut values = Vec::new();
                for _ in 0..512 {
                    values.push(auth_nonce_suffix());
                }
                values
            }));
        }

        let mut seen = BTreeSet::new();
        for worker in workers {
            for value in worker.join().expect("nonce worker should finish") {
                assert!(seen.insert(value), "duplicate daemon auth nonce suffix");
            }
        }
    }

    #[test]
    fn daemon_rpc_request_digest_binds_all_sender_metadata() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-daemon-auth-metadata-binding");
        let data_dir = SinomoDataDir::new(root.path());
        let request = ObjectRpcRequest::from_encoded_body(
            "/test/daemon-auth",
            "test",
            b"request-body".to_vec(),
        )
        .with_operation_id("operation-1")
        .with_response_retry_safe()
        .with_metadata("sinomo-test-metadata", "value-1");
        let auth = sign_daemon_rpc_request_from_data_dir(&data_dir, &request)
            .expect("sign metadata-bearing request");
        let envelope: SignedDaemonCommand = SinomoObjectCodec::default()
            .decode(&auth)
            .expect("decode signed request");
        let signed_digest = envelope
            .rpc_body_sha256
            .expect("signed request must carry its digest");
        assert_eq!(signed_digest, daemon_rpc_body_sha256(&request));

        let mut changed_operation_id = request.clone();
        changed_operation_id.metadata.insert(
            crate::rpc::OBJECT_RPC_OPERATION_ID_METADATA_KEY.to_string(),
            "operation-2".to_string(),
        );
        assert_ne!(
            signed_digest,
            daemon_rpc_body_sha256(&changed_operation_id),
            "changing the operation ID must invalidate the request digest"
        );

        let mut changed_retry_safety = request.clone();
        changed_retry_safety.metadata.insert(
            crate::rpc::OBJECT_RPC_RESPONSE_RETRY_SAFE_METADATA_KEY.to_string(),
            "false".to_string(),
        );
        assert_ne!(
            signed_digest,
            daemon_rpc_body_sha256(&changed_retry_safety),
            "changing retry safety must invalidate the request digest"
        );

        let mut changed_other_metadata = request;
        changed_other_metadata
            .metadata
            .insert("sinomo-test-metadata".to_string(), "value-2".to_string());
        assert_ne!(
            signed_digest,
            daemon_rpc_body_sha256(&changed_other_metadata),
            "changing any sender metadata must invalidate the request digest"
        );
    }

    #[test]
    fn daemon_rpc_request_digest_is_independent_of_metadata_insertion_order() {
        let request_a =
            ObjectRpcRequest::from_encoded_body("/test/daemon-auth", "test", vec![1, 2, 3])
                .with_metadata("alpha", "one")
                .with_metadata("beta", "two");
        let request_b =
            ObjectRpcRequest::from_encoded_body("/test/daemon-auth", "test", vec![1, 2, 3])
                .with_metadata("beta", "two")
                .with_metadata("alpha", "one");

        assert_eq!(
            daemon_rpc_body_sha256(&request_a),
            daemon_rpc_body_sha256(&request_b)
        );
    }

    #[test]
    fn daemon_rpc_request_digest_length_prefixes_metadata_fields() {
        let request_a = ObjectRpcRequest::from_encoded_body(
            "/test/daemon-auth",
            "test",
            b"request-body".to_vec(),
        )
        .with_metadata("a", "bc");
        let request_b = ObjectRpcRequest::from_encoded_body(
            "/test/daemon-auth",
            "test",
            b"request-body".to_vec(),
        )
        .with_metadata("ab", "c");

        assert_ne!(
            daemon_rpc_body_sha256(&request_a),
            daemon_rpc_body_sha256(&request_b)
        );
    }

    #[test]
    fn legacy_daemon_rpc_digest_is_body_only() {
        let request = ObjectRpcRequest::from_encoded_body(
            "/test/daemon-auth",
            "test",
            b"request-body".to_vec(),
        )
        .with_metadata("sender-controlled", "value");
        let changed_metadata = request
            .clone()
            .with_metadata("sender-controlled", "changed");
        let expected = digest::digest(&digest::SHA256, b"request-body");

        assert_eq!(daemon_rpc_legacy_body_sha256(&request), expected.as_ref());
        assert_eq!(
            daemon_rpc_legacy_body_sha256(&request),
            daemon_rpc_legacy_body_sha256(&changed_metadata)
        );
        assert_ne!(
            daemon_rpc_body_sha256(&request),
            daemon_rpc_legacy_body_sha256(&request)
        );
    }

    #[test]
    fn cached_daemon_signing_identity_refreshes_changed_cluster_node_id() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-daemon-auth-node-id-refresh");
        let data_dir = SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().expect("data dir");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let cluster_id =
            sinomo_node::ClusterId::from_legacy_named("cluster_daemon_auth_node_id_refresh")
                .expect("cluster id");
        let first_node =
            sinomo_node::NodeId::from_legacy_named("node_daemon_auth_node_id_refresh_first")
                .expect("first node");
        let second_node =
            sinomo_node::NodeId::from_legacy_named("node_daemon_auth_node_id_refresh_second")
                .expect("second node");
        let now = OffsetDateTime::now_utc();
        let mut state = sinomo_base::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "auth node id refresh",
            first_node.clone(),
            "first",
            "pk-first",
            now,
        );
        crate::object_file_store::write_object_file(&cluster_paths.state, &state)
            .expect("write first state");
        let request =
            ObjectRpcRequest::encode_body("/test/daemon-auth", "test", &()).expect("request");

        let first_auth =
            sign_daemon_rpc_request_from_data_dir(&data_dir, &request).expect("first auth");
        let first_envelope: SignedDaemonCommand = SinomoObjectCodec::default()
            .decode(&first_auth)
            .expect("decode first envelope");
        assert_eq!(first_envelope.node_id, first_node.to_string());

        state.local_node_id = second_node.clone();
        state.members[0].node_id = second_node.clone();
        state.members[0].display_name = "second".to_string();
        crate::object_file_store::write_object_file(&cluster_paths.state, &state)
            .expect("write second state");

        let second_auth =
            sign_daemon_rpc_request_from_data_dir(&data_dir, &request).expect("second auth");
        let second_envelope: SignedDaemonCommand = SinomoObjectCodec::default()
            .decode(&second_auth)
            .expect("decode second envelope");
        assert_eq!(second_envelope.node_id, second_node.to_string());
    }
}
