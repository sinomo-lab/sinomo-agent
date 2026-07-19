use crate::transfer::TransferId;
use crate::{ClusterPaths, NetworkError, ObjectRpcRequest, SinomoObjectCodec};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::digest;
use ring::signature::{ED25519, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use sinomo_local::SinomoDataDir;
use sinomo_node::{NodeIdentityPaths, load_or_create_main_node_identity};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const DAEMON_AUTH_PROTOCOL: &str = "sinomo.daemon-auth.v1";
pub(crate) const RESPONSE_IDENTITY_PROOF_PROTOCOL: &str = "sinomo.rpc-response-identity.v1";

const DAEMON_RPC_REQUEST_DIGEST_DOMAIN: &[u8] = b"sinomo.daemon-rpc-request\0";
const LOCAL_CHANNEL_AUTH_PROTOCOL: &[u8] = b"sinomo.local-object-rpc-channel.v2\0";
const DAEMON_SIGNING_IDENTITY_MAX_AGE: Duration = Duration::from_secs(1);

static DAEMON_SIGNING_IDENTITIES: OnceLock<Mutex<HashMap<PathBuf, CachedDaemonSigningIdentity>>> =
    OnceLock::new();
static DAEMON_AUTH_NONCE_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct CachedDaemonSigningIdentity {
    node_id: String,
    public_key: Vec<u8>,
    key_pair: Arc<ring::signature::Ed25519KeyPair>,
    identity_mark: Option<crate::file_mark::ClusterFileMark>,
    cluster_state_mark: Option<crate::file_mark::ClusterFileMark>,
    checked_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LocalChannelProofRole {
    Server,
    Client,
}

impl LocalChannelProofRole {
    fn domain_byte(self) -> u8 {
        match self {
            Self::Server => 1,
            Self::Client => 2,
        }
    }
}

/// Process-cached daemon principal captured once for a physical local channel.
///
/// Keeping the key pair private to this module prevents the transport from
/// accidentally growing per-call signing. The channel uses this handle only
/// during its nonce-bound connection handshake.
#[derive(Clone)]
pub(crate) struct LocalChannelAuthIdentity {
    identity: CachedDaemonSigningIdentity,
}

impl LocalChannelAuthIdentity {
    pub(crate) fn from_data_dir(data_dir: &SinomoDataDir) -> Result<Self, NetworkError> {
        Ok(Self {
            identity: cached_daemon_signing_identity(data_dir)?,
        })
    }

    pub(crate) fn binding(&self) -> [u8; 32] {
        let mut payload = Vec::with_capacity(
            LOCAL_CHANNEL_AUTH_PROTOCOL.len()
                + self.identity.node_id.len()
                + self.identity.public_key.len()
                + 32,
        );
        payload.extend_from_slice(LOCAL_CHANNEL_AUTH_PROTOCOL);
        payload.extend_from_slice(b"identity\0");
        local_channel_append_bytes(&mut payload, self.identity.node_id.as_bytes());
        local_channel_append_bytes(&mut payload, &self.identity.public_key);
        let digest = digest::digest(&digest::SHA256, &payload);
        digest
            .as_ref()
            .try_into()
            .expect("SHA-256 daemon identity binding")
    }

    pub(crate) fn sign(&self, role: LocalChannelProofRole, transcript: &[u8]) -> Vec<u8> {
        let payload = local_channel_proof_payload(
            role,
            &self.identity.node_id,
            &self.identity.public_key,
            transcript,
        );
        self.identity.key_pair.sign(&payload).as_ref().to_vec()
    }

    pub(crate) fn verify(
        &self,
        role: LocalChannelProofRole,
        transcript: &[u8],
        signature: &[u8],
    ) -> Result<(), NetworkError> {
        let payload = local_channel_proof_payload(
            role,
            &self.identity.node_id,
            &self.identity.public_key,
            transcript,
        );
        UnparsedPublicKey::new(&ED25519, &self.identity.public_key)
            .verify(&payload, signature)
            .map_err(|_| NetworkError::AuthDenied)
    }
}

fn local_channel_proof_payload(
    role: LocalChannelProofRole,
    node_id: &str,
    public_key: &[u8],
    transcript: &[u8],
) -> Vec<u8> {
    let mut payload = Vec::with_capacity(
        LOCAL_CHANNEL_AUTH_PROTOCOL.len()
            + node_id.len()
            + public_key.len()
            + transcript.len()
            + 32,
    );
    payload.extend_from_slice(LOCAL_CHANNEL_AUTH_PROTOCOL);
    payload.push(role.domain_byte());
    local_channel_append_bytes(&mut payload, node_id.as_bytes());
    local_channel_append_bytes(&mut payload, public_key);
    local_channel_append_bytes(&mut payload, transcript);
    payload
}

fn local_channel_append_bytes(payload: &mut Vec<u8>, bytes: &[u8]) {
    payload.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    payload.extend_from_slice(bytes);
}

/// Session-local identity signer loaded once when a duplex stream is established.
///
/// The signer attests that the expected node produced a response for a logical
/// session. It deliberately does not sign response content or transport data.
#[derive(Clone)]
pub(crate) struct SessionIdentitySigner {
    identity: CachedDaemonSigningIdentity,
}

impl SessionIdentitySigner {
    pub(crate) fn from_data_dir(data_dir: &SinomoDataDir) -> Result<Self, NetworkError> {
        Ok(Self {
            identity: cached_daemon_signing_identity(data_dir)?,
        })
    }

    pub(crate) fn sign_response_identity(
        &self,
        input: ResponseIdentityProofInput,
    ) -> SignedResponseIdentityProof {
        let identity = &self.identity;
        let payload =
            response_identity_signing_payload(&identity.node_id, &identity.public_key, &input);
        let signature = identity.key_pair.sign(&payload);
        SignedResponseIdentityProof {
            protocol: RESPONSE_IDENTITY_PROOF_PROTOCOL.to_string(),
            signer_node_id: identity.node_id.clone(),
            public_key: identity.public_key.clone(),
            input,
            signature: signature.as_ref().to_vec(),
        }
    }
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ResponseIdentityProofInput {
    pub session_id: TransferId,
}

/// Target identity attestation carried in a response head. The proof is bound
/// only to the logical session, not to response or body content.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct SignedResponseIdentityProof {
    pub protocol: String,
    pub signer_node_id: String,
    #[serde(
        serialize_with = "crate::fallible_serde::serialize_bytes",
        deserialize_with = "crate::fallible_serde::deserialize_bytes"
    )]
    pub public_key: Vec<u8>,
    pub input: ResponseIdentityProofInput,
    #[serde(
        serialize_with = "crate::fallible_serde::serialize_bytes",
        deserialize_with = "crate::fallible_serde::deserialize_bytes"
    )]
    pub signature: Vec<u8>,
}

fn session_frame_append_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    output.extend_from_slice(&len.to_be_bytes());
    output.extend_from_slice(bytes);
}

fn response_identity_signing_payload(
    signer_node_id: &str,
    public_key: &[u8],
    input: &ResponseIdentityProofInput,
) -> Vec<u8> {
    let mut payload = Vec::with_capacity(
        RESPONSE_IDENTITY_PROOF_PROTOCOL.len()
            + signer_node_id.len()
            + public_key.len()
            + input.session_id.as_str().len()
            + 32,
    );
    payload.extend_from_slice(RESPONSE_IDENTITY_PROOF_PROTOCOL.as_bytes());
    payload.push(0);
    session_frame_append_bytes(&mut payload, signer_node_id.as_bytes());
    session_frame_append_bytes(&mut payload, public_key);
    session_frame_append_bytes(&mut payload, input.session_id.as_str().as_bytes());
    payload
}

pub(crate) fn verify_response_identity_proof(
    proof: &SignedResponseIdentityProof,
    expected_public_key: &[u8],
    expected_session_id: &TransferId,
) -> Result<(), NetworkError> {
    if proof.protocol != RESPONSE_IDENTITY_PROOF_PROTOCOL
        || proof.public_key != expected_public_key
        || &proof.input.session_id != expected_session_id
    {
        return Err(NetworkError::AuthDenied);
    }
    let payload =
        response_identity_signing_payload(&proof.signer_node_id, expected_public_key, &proof.input);
    UnparsedPublicKey::new(&ED25519, expected_public_key)
        .verify(&payload, &proof.signature)
        .map_err(|_| NetworkError::AuthDenied)
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

/// Signs from the process cache without touching the filesystem.
///
/// Callers on latency-sensitive async workers can use this first and move the
/// ordinary signing path to a blocking worker only when the cache is absent or
/// due for its bounded refresh.
pub fn try_sign_daemon_rpc_request_digest_from_cached_data_dir(
    data_dir: &SinomoDataDir,
    route: &str,
    category: &str,
    rpc_body_sha256: &[u8],
) -> Result<Option<Vec<u8>>, NetworkError> {
    let Some(identity) = fresh_cached_daemon_signing_identity(data_dir)? else {
        return Ok(None);
    };
    sign_daemon_rpc_request_with_identity(identity, route, category, Some(rpc_body_sha256.to_vec()))
        .map(Some)
}

/// Digests the RPC body and its sender-controlled metadata for daemon authentication.
///
/// The historical name remains part of the public API, but the digest deliberately
/// binds the complete request metadata map so routing retries cannot alter delivery
/// identity or retry-safety declarations after signing.
pub fn daemon_rpc_body_sha256(request: &ObjectRpcRequest) -> Vec<u8> {
    let mut digest = digest::Context::new(&digest::SHA256);
    digest.update(DAEMON_RPC_REQUEST_DIGEST_DOMAIN);
    daemon_rpc_digest_update_bytes(
        &mut digest,
        crate::session::daemon_auth_request_head_body(request),
    );
    daemon_rpc_digest_update_len(&mut digest, request.metadata.len());
    for (key, value) in &request.metadata {
        daemon_rpc_digest_update_bytes(&mut digest, key.as_bytes());
        daemon_rpc_digest_update_bytes(&mut digest, value.as_bytes());
    }
    digest.finish().as_ref().to_vec()
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
    sign_daemon_rpc_request_with_identity(identity, route, category, rpc_body_sha256)
}

fn sign_daemon_rpc_request_with_identity(
    identity: CachedDaemonSigningIdentity,
    route: &str,
    category: &str,
    rpc_body_sha256: Option<Vec<u8>>,
) -> Result<Vec<u8>, NetworkError> {
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

fn fresh_cached_daemon_signing_identity(
    data_dir: &SinomoDataDir,
) -> Result<Option<CachedDaemonSigningIdentity>, NetworkError> {
    let root = data_dir.root();
    let checked_at = Instant::now();
    let guard = DAEMON_SIGNING_IDENTITIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|source| NetworkError::Io {
            operation: "daemon RPC auth identity cache".to_string(),
            detail: source.to_string(),
        })?;
    Ok(guard
        .get(&root)
        .filter(|cached| {
            cached.identity_mark.is_some_and(|mark| mark.exists)
                && cached.cluster_state_mark.is_some_and(|mark| mark.exists)
                && checked_at.duration_since(cached.checked_at) < DAEMON_SIGNING_IDENTITY_MAX_AGE
        })
        .cloned())
}

fn cached_daemon_signing_identity(
    data_dir: &SinomoDataDir,
) -> Result<CachedDaemonSigningIdentity, NetworkError> {
    let root = data_dir.root();
    let checked_at = Instant::now();
    let cached = DAEMON_SIGNING_IDENTITIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|source| NetworkError::Io {
            operation: "daemon RPC auth identity cache".to_string(),
            detail: source.to_string(),
        })?
        .get(&root)
        .cloned();
    if let Some(cached) = cached.as_ref()
        && cached.identity_mark.is_some_and(|mark| mark.exists)
        && cached.cluster_state_mark.is_some_and(|mark| mark.exists)
        && checked_at.duration_since(cached.checked_at) < DAEMON_SIGNING_IDENTITY_MAX_AGE
    {
        return Ok(cached.clone());
    }

    let identity_path = NodeIdentityPaths::new(&root).main;
    let cluster_state_path = ClusterPaths::new(data_dir).state;
    let identity_mark = crate::file_mark::cluster_file_mark(&identity_path);
    let cluster_state_mark = crate::file_mark::cluster_file_mark(&cluster_state_path);
    if let Some(cached) = cached {
        if identity_mark.is_some()
            && identity_mark == cached.identity_mark
            && cluster_state_mark.is_some()
            && cluster_state_mark == cached.cluster_state_mark
        {
            let refreshed = CachedDaemonSigningIdentity {
                checked_at,
                ..cached
            };
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
        // Retain the marks sampled before the reads. If either file was
        // replaced concurrently, the next lookup observes the new mark and
        // refreshes instead of pairing stale contents with a post-read mark.
        identity_mark,
        cluster_state_mark,
        checked_at,
    };

    let mut guard = DAEMON_SIGNING_IDENTITIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|source| NetworkError::Io {
            operation: "daemon RPC auth identity cache".to_string(),
            detail: source.to_string(),
        })?;
    guard.insert(root, loaded.clone());
    Ok(loaded)
}

/// Returns the process-cached main-daemon public key, refreshing identity
/// state only when its file marks change. Persistent transports use this once
/// while establishing their connection principal.
pub fn cached_daemon_signing_public_key(data_dir: &SinomoDataDir) -> Result<Vec<u8>, NetworkError> {
    Ok(cached_daemon_signing_identity(data_dir)?.public_key)
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
    use std::hint::black_box;
    use time::OffsetDateTime;

    fn diagnostic_ns_per_iteration(iterations: usize, mut operation: impl FnMut()) -> u128 {
        let started = Instant::now();
        for _ in 0..iterations {
            operation();
        }
        started.elapsed().as_nanos() / iterations.max(1) as u128
    }

    /// Opt-in microbenchmark for separating cryptographic work from transport
    /// and scheduler latency. This deliberately has no timing assertion: CI
    /// hosts are not stable benchmark environments.
    #[test]
    #[ignore = "diagnostic microbenchmark; run explicitly with --ignored --nocapture"]
    fn diagnostic_daemon_auth_costs() {
        const ITERATIONS: usize = 20_000;
        let root = sinomo_testdir::TestDir::new("sinomo-network-auth-cost-diagnostic");
        let data_dir = SinomoDataDir::new(root.path());
        let request =
            ObjectRpcRequest::encode_body("/test/auth-cost-diagnostic", "test", &"small request")
                .expect("diagnostic request");
        let body_sha256 = daemon_rpc_body_sha256(&request);
        let identity = cached_daemon_signing_identity(&data_dir).expect("cached identity");
        let auth = sign_daemon_rpc_request_with_identity(
            identity.clone(),
            &request.route,
            &request.category,
            Some(body_sha256.clone()),
        )
        .expect("request signature");
        let envelope: SignedDaemonCommand = SinomoObjectCodec::default()
            .decode(&auth)
            .expect("decode request signature");
        let auth_payload = daemon_auth_signing_payload(
            &envelope.node_id,
            &envelope.public_key,
            &envelope.timestamp,
            &envelope.nonce,
            &request.route,
            &request.category,
            envelope.rpc_body_sha256.as_deref(),
        );

        // Warm code and allocator paths before reporting steady-state costs.
        for _ in 0..1_000 {
            black_box(daemon_rpc_body_sha256(black_box(&request)));
        }

        let digest_ns = diagnostic_ns_per_iteration(ITERATIONS, || {
            black_box(daemon_rpc_body_sha256(black_box(&request)));
        });
        let request_sign_ns = diagnostic_ns_per_iteration(ITERATIONS, || {
            black_box(
                sign_daemon_rpc_request_with_identity(
                    identity.clone(),
                    black_box(&request.route),
                    black_box(&request.category),
                    Some(body_sha256.clone()),
                )
                .expect("diagnostic request sign"),
            );
        });
        let request_verify_ns = diagnostic_ns_per_iteration(ITERATIONS, || {
            UnparsedPublicKey::new(&ED25519, black_box(&envelope.public_key))
                .verify(black_box(&auth_payload), black_box(&envelope.signature))
                .expect("diagnostic request verify");
        });
        eprintln!(
            "auth-cost ns/op: digest={digest_ns} request-sign={request_sign_ns} request-verify={request_verify_ns}"
        );
    }

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
        DAEMON_SIGNING_IDENTITIES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .expect("identity cache")
            .get_mut(&data_dir.root())
            .expect("cached identity")
            .checked_at = Instant::now()
            .checked_sub(DAEMON_SIGNING_IDENTITY_MAX_AGE + Duration::from_millis(1))
            .expect("expired cache timestamp");

        let second_auth =
            sign_daemon_rpc_request_from_data_dir(&data_dir, &request).expect("second auth");
        let second_envelope: SignedDaemonCommand = SinomoObjectCodec::default()
            .decode(&second_auth)
            .expect("decode second envelope");
        assert_eq!(second_envelope.node_id, second_node.to_string());
    }

    #[test]
    fn response_identity_proof_binds_the_expected_node_and_session() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-response-identity-proof");
        let data_dir = SinomoDataDir::new(root.path());
        let session_id = TransferId::generate();
        let proof = SessionIdentitySigner::from_data_dir(&data_dir)
            .expect("identity signer")
            .sign_response_identity(ResponseIdentityProofInput {
                session_id: session_id.clone(),
            });
        verify_response_identity_proof(&proof, &proof.public_key, &session_id)
            .expect("valid response identity proof");
        assert!(matches!(
            verify_response_identity_proof(&proof, &[0_u8; 32], &session_id),
            Err(NetworkError::AuthDenied)
        ));
        assert!(matches!(
            verify_response_identity_proof(&proof, &proof.public_key, &TransferId::generate()),
            Err(NetworkError::AuthDenied)
        ));
    }
}
