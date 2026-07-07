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

pub fn daemon_rpc_body_sha256(request: &ObjectRpcRequest) -> Vec<u8> {
    digest::digest(&digest::SHA256, &request.body)
        .as_ref()
        .to_vec()
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
        if cached.node_id != "node_local" {
            return Ok(cached);
        }
        if let Some(node_id) = local_cluster_node_id(data_dir)
            && node_id != cached.node_id
        {
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
    use std::collections::BTreeSet;

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
}
