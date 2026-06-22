use crate::{DaemonError, DaemonPaths};
#[cfg(test)]
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::signature::{ED25519, UnparsedPublicKey};
use sinomo_config::{
    CONFIG_CATALOG_ROUTE, CONFIG_GET_ROUTE, CONFIG_SEARCH_ROUTE, CONFIG_SET_ROUTE,
};
use sinomo_core::{ClusterJoinInvite, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState};
use sinomo_kv::{ClusterKvRouteAuth, cluster_kv_rpc_route_policy};
use sinomo_local::SinomoDataDir;
use sinomo_network::{
    ClusterControlPlaneTimeouts, ClusterInviteChallengeProof, ClusterInviteChallengeRequest,
    ClusterPaths,
};
#[cfg(test)]
use sinomo_node::load_or_create_main_node_identity;
use sinomo_node::{ClusterInviteId, NodeIdentity, NodeIdentityPaths, load_or_create_node_identity};
use sinomo_rollout::{
    ROLLOUT_BINARY_ROUTE, ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE, ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE,
    ROLLOUT_BUILD_ROUTE, ROLLOUT_CLEAR_STATUS_ROUTE, ROLLOUT_DIAGNOSTICS_ROUTE,
    ROLLOUT_FINALIZE_SHADOW_ROUTE, ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
};
use sinomo_storage::durable_file::file_ino;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const DAEMON_AUTH_PROTOCOL: &str = sinomo_network::DAEMON_AUTH_PROTOCOL;
pub const DAEMON_AUTH_MAX_CLOCK_SKEW_SECONDS: i64 = 300;
pub const DAEMON_AUTH_NONCE_RETENTION_SECONDS: i64 = DAEMON_AUTH_MAX_CLOCK_SKEW_SECONDS * 2;
const DAEMON_AUTH_NONCE_PRUNE_INTERVAL_SECONDS: i64 = 1;
const DAEMON_AUTH_NONCE_PRUNE_MAX_ENTRIES: usize = 8_192;
const DAEMON_AUTH_VERIFIER_CONTEXT_MAX_AGE: Duration = Duration::from_secs(1);
pub use sinomo_network::SignedDaemonCommand;
/// In-memory replay-protection nonce sets, keyed by daemon data root. Replacing the
/// previous per-RPC `auth-nonces.sobjlog` append (a disk read + write on *every*
/// authenticated request, including idle cluster heartbeats) keeps idle disk I/O at
/// zero. Replay protection is bounded by the signed-command clock-skew freshness
/// check, so the only behavioral change is that a process restart forgets nonces
/// from the trailing skew window — which the freshness check already constrains.
static DAEMON_AUTH_NONCES: OnceLock<Mutex<HashMap<PathBuf, DaemonAuthNonceSet>>> = OnceLock::new();
static DAEMON_AUTH_VERIFIER_CONTEXTS: OnceLock<
    Mutex<HashMap<PathBuf, CachedDaemonAuthVerifierContext>>,
> = OnceLock::new();

#[derive(Clone)]
struct CachedDaemonAuthVerifierContext {
    mark: DaemonAuthVerifierContextMark,
    checked_at: Instant,
    main_public_key: String,
    shadow_public_key: Option<String>,
    members_by_node_and_key: BTreeMap<(String, String), DaemonSignerAuthority>,
    members_by_key: BTreeMap<String, DaemonSignerAuthority>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DaemonAuthVerifierContextMark {
    main_identity: DaemonAuthFileMark,
    shadow_identity: DaemonAuthFileMark,
    cluster_state: DaemonAuthFileMark,
    kv_snapshot: DaemonAuthFileMark,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DaemonAuthFileMark {
    exists: bool,
    len: u64,
    modified: Option<SystemTime>,
    ino: u64,
}

#[derive(Debug)]
struct DaemonAuthNonceSet {
    nonces: BTreeMap<Vec<u8>, OffsetDateTime>,
    last_pruned_at: OffsetDateTime,
}

impl DaemonAuthNonceSet {
    fn new(now: OffsetDateTime) -> Self {
        Self {
            nonces: BTreeMap::new(),
            last_pruned_at: now,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedDaemonCommand {
    pub route: String,
    pub signer: DaemonSignerAuthority,
    pub signer_node_id: String,
    /// Existing cluster membership state stores node public keys as URL-safe base64 strings.
    pub signer_public_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DaemonSignerAuthority {
    Local,
    Voter,
    Member,
    JoinCandidate,
    StatusProbe,
}

impl DaemonSignerAuthority {
    pub fn can_run_admin_command(&self) -> bool {
        matches!(self, Self::Local | Self::Voter)
    }

    pub fn can_run_local_command(&self) -> bool {
        matches!(self, Self::Local)
    }
}

pub fn sign_daemon_rpc_request_for_local_node(
    request: &sinomo_network::ObjectRpcRequest,
) -> Result<Vec<u8>, DaemonError> {
    sinomo_network::sign_daemon_rpc_request_for_local_node(request).map_err(DaemonError::from)
}

pub fn sign_daemon_rpc_request_from_data_dir(
    data_dir: &SinomoDataDir,
    request: &sinomo_network::ObjectRpcRequest,
) -> Result<Vec<u8>, DaemonError> {
    sinomo_network::sign_daemon_rpc_request_from_data_dir(data_dir, request)
        .map_err(DaemonError::from)
}

pub fn daemon_rpc_body_sha256(request: &sinomo_network::ObjectRpcRequest) -> Vec<u8> {
    sinomo_network::daemon_rpc_body_sha256(request)
}

pub fn authenticate_daemon_control_request(
    paths: &DaemonPaths,
    request: &sinomo_network::DaemonControlRequest,
) -> Result<AuthenticatedDaemonCommand, DaemonError> {
    let auth = request.auth.as_deref().ok_or(DaemonError::AuthDenied)?;
    let envelope: SignedDaemonCommand =
        sinomo_network::SinomoObjectCodec::default().decode(auth)?;
    verify_daemon_rpc_body_binding(&envelope, request)?;
    let signer = verify_signed_daemon_command(paths, &envelope, request)?;
    remember_daemon_auth_nonce(paths, &envelope)?;
    let signer_public_key = sinomo_network::daemon_auth_encode_public_key(&envelope.public_key);
    let authenticated = AuthenticatedDaemonCommand {
        route: request.route.clone(),
        signer,
        signer_node_id: envelope.node_id,
        signer_public_key,
    };
    authorize_authenticated_daemon_command(&authenticated)?;
    Ok(authenticated)
}

fn verify_daemon_rpc_body_binding(
    envelope: &SignedDaemonCommand,
    request: &sinomo_network::ObjectRpcRequest,
) -> Result<(), DaemonError> {
    let Some(expected) = envelope.rpc_body_sha256.as_deref() else {
        return Err(DaemonError::AuthDenied);
    };
    let actual = daemon_rpc_body_sha256(request);
    if expected == actual.as_slice() {
        Ok(())
    } else {
        Err(DaemonError::AuthDenied)
    }
}

pub fn create_cluster_invite_challenge_proof(
    paths: &DaemonPaths,
    request: &ClusterInviteChallengeRequest,
) -> Result<ClusterInviteChallengeProof, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let identity_paths = NodeIdentityPaths::new(data_dir);
    let identity = load_existing_node_identity(&identity_paths.main)?;
    sinomo_network::create_cluster_invite_challenge_proof(
        &cluster_paths,
        &identity,
        request,
        OffsetDateTime::now_utc(),
    )
    .map_err(Into::into)
}

pub fn authorize_authenticated_daemon_command(
    authenticated: &AuthenticatedDaemonCommand,
) -> Result<(), DaemonError> {
    let route = authenticated.route.as_str();
    if let Some(policy) = cluster_kv_rpc_route_policy(route) {
        if daemon_signer_authorizes_cluster_kv_route(&authenticated.signer, policy.auth) {
            return Ok(());
        }
        return Err(DaemonError::AuthDenied);
    }
    let allowed = match route {
        route if route == crate::DAEMON_STATUS_ROUTE => true,
        route if route == crate::DAEMON_SHUTDOWN_ROUTE => {
            authenticated.signer.can_run_local_command()
        }
        route if route == ROLLOUT_FINALIZE_SHADOW_ROUTE => {
            authenticated.signer.can_run_local_command()
        }
        route if route == ROLLOUT_DIAGNOSTICS_ROUTE => authenticated.signer.can_run_admin_command(),
        route if route == ROLLOUT_CLEAR_STATUS_ROUTE => {
            authenticated.signer.can_run_admin_command()
        }
        route
            if route == sinomo_observe::OBSERVE_EVENTS_ROUTE
                || route == sinomo_observe::OBSERVE_LOGS_ROUTE
                || route == sinomo_observe::OBSERVE_METRICS_ROUTE
                || route == sinomo_observe::OBSERVE_TRACES_ROUTE =>
        {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        route if route == sinomo_observe::OBSERVE_CONTROL_ROUTE => {
            authenticated.signer.can_run_admin_command()
        }
        route if route == sinomo_dfs::DFS_EXPOSE_CONFIG_ROUTE => {
            authenticated.signer.can_run_local_command()
        }
        route if is_config_rpc_route(route) => authenticated.signer.can_run_local_command(),
        route if route == sinomo_dfs::DFS_STATUS_ROUTE => matches!(
            authenticated.signer,
            DaemonSignerAuthority::Local
                | DaemonSignerAuthority::Voter
                | DaemonSignerAuthority::Member
        ),
        route if route == sinomo_dfs::DFS_INSPECTION_ROUTE => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        route if route == sinomo_dfs::DFS_SMOKE_ROUTE => {
            authenticated.signer.can_run_local_command()
        }
        route if route == sinomo_dfs::DFS_METADATA_JOURNAL_BATCH_ROUTE => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        route if route == sinomo_dfs::DFS_METADATA_SNAPSHOT_INSTALL_ROUTE => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        route if sinomo_dfs::dfs_route_is_message_data_plane(route) => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        route if route == sinomo_dfs::DFS_DIRECT_READ_ROUTE => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        route if is_agent_session_rpc_route(route) => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        route if route == sinomo_module::MODULE_NATIVE_DYLIB_CANCEL_ROUTE => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        route if route == "/benchmarks/cancel-probe" => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        route if is_local_sandbox_rpc_route(route) => authenticated.signer.can_run_local_command(),
        route if route == sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
                    | DaemonSignerAuthority::JoinCandidate
            )
        }
        route
            if route == sinomo_network::CLUSTER_INVITE_PUBLISH_ROUTE
                || route == sinomo_network::CLUSTER_MEMBER_PUBLISH_ROUTE =>
        {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        route
            if route == sinomo_network::CLUSTER_HEALTH_ROUTE
                || route == sinomo_network::CLUSTER_NETWORK_PING_ROUTE =>
        {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        route
            if route == ROLLOUT_BINARY_ROUTE
                || route == ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE
                || route == ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE
                || route == ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE
                || route == ROLLOUT_BUILD_ROUTE =>
        {
            authenticated.signer.can_run_admin_command()
        }
        route if route == sinomo_network::CLUSTER_RELAY_ROUTE => {
            // A relay envelope is forwarded on behalf of another node by any authenticated
            // cluster member (e.g. a bridge learner relaying a joiner's challenge toward a
            // voter). The wrapped inner request carries its own signature/invite-proof and is
            // authorized independently when it is finally delivered and dispatched locally.
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        _ => false,
    };
    if allowed {
        Ok(())
    } else {
        Err(DaemonError::AuthDenied)
    }
}

fn daemon_signer_authorizes_cluster_kv_route(
    signer: &DaemonSignerAuthority,
    auth: ClusterKvRouteAuth,
) -> bool {
    match auth {
        ClusterKvRouteAuth::Authenticated => true,
        ClusterKvRouteAuth::LocalOrClusterMember => matches!(
            signer,
            DaemonSignerAuthority::Local
                | DaemonSignerAuthority::Voter
                | DaemonSignerAuthority::Member
        ),
        ClusterKvRouteAuth::LocalOrRaftVoter => {
            matches!(
                signer,
                DaemonSignerAuthority::Local | DaemonSignerAuthority::Voter
            )
        }
    }
}

fn is_agent_session_rpc_route(route: &str) -> bool {
    route.strip_prefix("/agent/session/").is_some()
}

fn is_config_rpc_route(route: &str) -> bool {
    route == CONFIG_CATALOG_ROUTE
        || route == CONFIG_SEARCH_ROUTE
        || route == CONFIG_GET_ROUTE
        || route == CONFIG_SET_ROUTE
}

fn is_local_sandbox_rpc_route(route: &str) -> bool {
    route == sinomo_sandbox::SANDBOX_CREATE_ROUTE
        || route == sinomo_sandbox::SANDBOX_STATUS_ROUTE
        || route == sinomo_sandbox::SANDBOX_UPDATE_ROUTE
        || route == sinomo_sandbox::SANDBOX_CHECKPOINT_ROUTE
        || route == sinomo_sandbox::SANDBOX_RESUME_ROUTE
        || route == sinomo_sandbox::SANDBOX_REBOOT_ROUTE
        || route == sinomo_sandbox::SANDBOX_DESTROY_ROUTE
        || route == sinomo_sandbox::SANDBOX_LIST_ROUTE
        || route == sinomo_sandbox::SANDBOX_READ_ROUTE
        || route == sinomo_sandbox::SANDBOX_WRITE_ROUTE
        || route == sinomo_sandbox::SANDBOX_HTTP_FETCH_ROUTE
        || route == sinomo_sandbox::SANDBOX_HTTP_FETCH_TO_FILE_ROUTE
        || route == sinomo_sandbox::SANDBOX_RUN_TOOLCHAIN_ROUTE
        || route == sinomo_sandbox::SANDBOX_RUN_PROCESS_PLAN_ROUTE
        || route == sinomo_sandbox::SANDBOX_RUN_CANCEL_ROUTE
        || route == sinomo_sandbox::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE
        || route == sinomo_sandbox::SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE
        || route == sinomo_sandbox::SANDBOX_IMPORT_BUSYBOX_ROUTE
        || route == sinomo_sandbox::SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_ROUTE
        || route == sinomo_sandbox::SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE
        || route == sinomo_sandbox::SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE
        || route == sinomo_sandbox::SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE
}

fn verify_signed_daemon_command(
    paths: &DaemonPaths,
    envelope: &SignedDaemonCommand,
    request: &sinomo_network::ObjectRpcRequest,
) -> Result<DaemonSignerAuthority, DaemonError> {
    if envelope.protocol != DAEMON_AUTH_PROTOCOL {
        return Err(DaemonError::AuthDenied);
    }
    let signed_at = OffsetDateTime::parse(&envelope.timestamp, &Rfc3339)
        .map_err(|_| DaemonError::AuthDenied)?;
    let now = OffsetDateTime::now_utc();
    let skew = (now - signed_at).whole_seconds().abs();
    if skew > DAEMON_AUTH_MAX_CLOCK_SKEW_SECONDS {
        return Err(DaemonError::AuthExpired);
    }
    if envelope.nonce.trim().is_empty() || request.route.trim().is_empty() {
        return Err(DaemonError::AuthDenied);
    }
    let signing_payload = sinomo_network::daemon_auth_signing_payload(
        &envelope.node_id,
        &envelope.public_key,
        &envelope.timestamp,
        &envelope.nonce,
        &request.route,
        &request.category,
        envelope.rpc_body_sha256.as_deref(),
    );
    UnparsedPublicKey::new(&ED25519, &envelope.public_key)
        .verify(&signing_payload, &envelope.signature)
        .map_err(|_| DaemonError::AuthDenied)?;
    authorize_daemon_signer(paths, envelope, request)
}

fn authorize_daemon_signer(
    paths: &DaemonPaths,
    envelope: &SignedDaemonCommand,
    request: &sinomo_network::ObjectRpcRequest,
) -> Result<DaemonSignerAuthority, DaemonError> {
    let envelope_public_key = sinomo_network::daemon_auth_encode_public_key(&envelope.public_key);
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let verifier = cached_daemon_auth_verifier_context(data_dir)?;
    if envelope_public_key == verifier.main_public_key
        || verifier
            .shadow_public_key
            .as_ref()
            .is_some_and(|public_key| envelope_public_key == *public_key)
    {
        return Ok(DaemonSignerAuthority::Local);
    }

    if let Some(authority) = verifier
        .members_by_node_and_key
        .get(&(envelope.node_id.clone(), envelope_public_key.clone()))
    {
        return Ok(authority.clone());
    }
    if let Some(authority) = verifier.members_by_key.get(&envelope_public_key) {
        return Ok(authority.clone());
    }

    if request.route == sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE {
        authorize_cluster_join_rpc_signer(paths, Some(request), &envelope_public_key)?;
        return Ok(DaemonSignerAuthority::JoinCandidate);
    }
    if request.route == crate::DAEMON_STATUS_ROUTE {
        return Ok(DaemonSignerAuthority::StatusProbe);
    }
    if let Some(authority) =
        authorize_daemon_signer_after_membership_refresh(data_dir, envelope, &envelope_public_key)
    {
        return Ok(authority);
    }
    Err(DaemonError::AuthDenied)
}

fn verifier_authority_for_signer(
    verifier: &CachedDaemonAuthVerifierContext,
    node_id: &str,
    public_key: &str,
) -> Option<DaemonSignerAuthority> {
    verifier
        .members_by_node_and_key
        .get(&(node_id.to_string(), public_key.to_string()))
        .cloned()
        .or_else(|| verifier.members_by_key.get(public_key).cloned())
}

fn authorize_daemon_signer_after_membership_refresh(
    data_dir: &Path,
    envelope: &SignedDaemonCommand,
    envelope_public_key: &str,
) -> Option<DaemonSignerAuthority> {
    if let Some(authority) = load_uncached_daemon_auth_verifier_context(data_dir)
        .ok()
        .and_then(|verifier| {
            verifier_authority_for_signer(&verifier, &envelope.node_id, envelope_public_key)
        })
    {
        replace_cached_daemon_auth_verifier_context(data_dir, None);
        return Some(authority);
    }

    refresh_daemon_auth_membership_snapshot(data_dir).ok()?;
    replace_cached_daemon_auth_verifier_context(data_dir, None);
    let verifier = cached_daemon_auth_verifier_context(data_dir).ok()?;
    verifier_authority_for_signer(&verifier, &envelope.node_id, envelope_public_key)
}

fn load_uncached_daemon_auth_verifier_context(
    data_dir: &Path,
) -> Result<CachedDaemonAuthVerifierContext, DaemonError> {
    let checked_at = Instant::now();
    let mark = daemon_auth_verifier_context_mark(data_dir)?;
    load_daemon_auth_verifier_context(data_dir, mark, checked_at)
}

fn replace_cached_daemon_auth_verifier_context(
    data_dir: &Path,
    replacement: Option<CachedDaemonAuthVerifierContext>,
) {
    let Some(cache) = DAEMON_AUTH_VERIFIER_CONTEXTS.get() else {
        return;
    };
    let Ok(mut guard) = cache.lock() else {
        return;
    };
    match replacement {
        Some(context) => {
            guard.insert(data_dir.to_path_buf(), context);
        }
        None => {
            guard.remove(data_dir);
        }
    }
}

fn refresh_daemon_auth_membership_snapshot(data_dir: &Path) -> Result<(), DaemonError> {
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = sinomo_network::read_cluster_state_without_kv(&cluster_paths)?;
    let options =
        ClusterControlPlaneTimeouts::default_daemon_transport().kv_snapshot_refresh_options();
    sinomo_network::refresh_current_cluster_kv_store_with_local_storage_and_transport(
        &cluster_paths,
        &state,
        options,
        |endpoint, timeout| {
            crate::client::fetch_cluster_kv_snapshot_from_endpoint(endpoint, timeout).map_err(
                |error| sinomo_network::NetworkError::Io {
                    operation: "cluster auth membership refresh".to_string(),
                    detail: error.to_string(),
                },
            )
        },
    )
    .map(|_| ())
    .map_err(DaemonError::from)
}

fn cached_daemon_auth_verifier_context(
    data_dir: &Path,
) -> Result<CachedDaemonAuthVerifierContext, DaemonError> {
    let root = data_dir.to_path_buf();
    let checked_at = Instant::now();
    {
        let guard = DAEMON_AUTH_VERIFIER_CONTEXTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|source| DaemonError::Io {
                path: root.display().to_string(),
                detail: source.to_string(),
            })?;
        if let Some(cached) = guard.get(&root)
            && checked_at.duration_since(cached.checked_at) < DAEMON_AUTH_VERIFIER_CONTEXT_MAX_AGE
        {
            return Ok(cached.clone());
        }
    }

    let mark = daemon_auth_verifier_context_mark(data_dir)?;
    {
        let mut guard = DAEMON_AUTH_VERIFIER_CONTEXTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|source| DaemonError::Io {
                path: root.display().to_string(),
                detail: source.to_string(),
            })?;
        if let Some(cached) = guard.get_mut(&root)
            && cached.mark == mark
        {
            cached.checked_at = checked_at;
            return Ok(cached.clone());
        }
    }

    let loaded = load_daemon_auth_verifier_context(data_dir, mark, checked_at)?;
    let after_mark = daemon_auth_verifier_context_mark(data_dir)?;
    if after_mark == mark {
        let mut guard = DAEMON_AUTH_VERIFIER_CONTEXTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|source| DaemonError::Io {
                path: root.display().to_string(),
                detail: source.to_string(),
            })?;
        guard.insert(root, loaded.clone());
        Ok(loaded)
    } else {
        Ok(loaded)
    }
}

fn load_daemon_auth_verifier_context(
    data_dir: &Path,
    mark: DaemonAuthVerifierContextMark,
    checked_at: Instant,
) -> Result<CachedDaemonAuthVerifierContext, DaemonError> {
    let identity_paths = NodeIdentityPaths::new(data_dir);
    let main_identity = load_existing_node_identity(&identity_paths.main)?;
    let shadow_identity = load_optional_node_identity(&identity_paths.shadow)?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let mut members_by_node_and_key = BTreeMap::new();
    let mut members_by_key = BTreeMap::new();
    if cluster_paths.state.exists() {
        let state = sinomo_network::read_cluster_state(&cluster_paths)?;
        for member in state
            .members
            .iter()
            .filter(|member| member.state.is_authorized())
        {
            let authority = signer_authority_for_member(member);
            members_by_node_and_key.insert(
                (member.node_id.to_string(), member.node_public_key.clone()),
                authority.clone(),
            );
            members_by_key
                .entry(member.node_public_key.clone())
                .or_insert(authority);
        }
    }

    Ok(CachedDaemonAuthVerifierContext {
        mark,
        checked_at,
        main_public_key: main_identity.public_key,
        shadow_public_key: shadow_identity.map(|identity| identity.public_key),
        members_by_node_and_key,
        members_by_key,
    })
}

fn daemon_auth_verifier_context_mark(
    data_dir: &Path,
) -> Result<DaemonAuthVerifierContextMark, DaemonError> {
    let identity_paths = NodeIdentityPaths::new(data_dir);
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    Ok(DaemonAuthVerifierContextMark {
        main_identity: daemon_auth_file_mark(&identity_paths.main)?,
        shadow_identity: daemon_auth_file_mark(&identity_paths.shadow)?,
        cluster_state: daemon_auth_file_mark(&cluster_paths.state)?,
        kv_snapshot: daemon_auth_file_mark(&cluster_paths.kv_snapshot)?,
    })
}

fn daemon_auth_file_mark(path: &Path) -> Result<DaemonAuthFileMark, DaemonError> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(DaemonAuthFileMark {
            exists: true,
            len: metadata.len(),
            modified: metadata.modified().ok(),
            ino: file_ino(&metadata),
        }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(DaemonAuthFileMark {
            exists: false,
            len: 0,
            modified: None,
            ino: 0,
        }),
        Err(source) => Err(DaemonError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

fn signer_authority_for_member(member: &ClusterMemberRecord) -> DaemonSignerAuthority {
    if matches!(member.state, ClusterMemberState::Voter)
        || (matches!(member.state, ClusterMemberState::Active)
            && matches!(member.role, ClusterMemberRole::Voter))
    {
        DaemonSignerAuthority::Voter
    } else {
        DaemonSignerAuthority::Member
    }
}

fn authorize_cluster_join_rpc_signer(
    paths: &DaemonPaths,
    rpc_request: Option<&sinomo_network::ObjectRpcRequest>,
    public_key: &str,
) -> Result<(), DaemonError> {
    let request = rpc_request.ok_or(DaemonError::AuthDenied)?;
    if request.route != sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE {
        return Err(DaemonError::AuthDenied);
    }
    let request: sinomo_network::ClusterJoinRegistrationRequest = request.decode_body()?;
    authorize_cluster_join_request_signer(paths, &request, public_key)
}

pub(crate) fn is_cluster_join_register_control_request(
    request: &sinomo_network::DaemonControlRequest,
) -> bool {
    request.route == sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE
        && request.category == sinomo_network::CLUSTER_MEMBERSHIP_RPC_CATEGORY
}

fn authorize_cluster_join_request_signer(
    paths: &DaemonPaths,
    request: &sinomo_network::ClusterJoinRegistrationRequest,
    public_key: &str,
) -> Result<(), DaemonError> {
    let requested_member = request
        .members
        .iter()
        .find(|member| member.node_public_key == public_key)
        .ok_or(DaemonError::AuthDenied)?;
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let state = sinomo_network::read_cluster_state(&cluster_paths)?;

    let persisted_invite = read_active_cluster_invite(&cluster_paths, &request.invite.invite_id)?;
    if !persisted_invite
        .allowed_member_roles
        .contains(&requested_member.role)
    {
        return Err(DaemonError::AuthDenied);
    }
    verify_cluster_join_invite_signature(&persisted_invite)?;
    if persisted_invite.expires_at <= OffsetDateTime::now_utc() {
        return Err(DaemonError::AuthExpired);
    }
    if persisted_invite.cluster_id != state.cluster_id {
        return Err(DaemonError::AuthDenied);
    }
    if !state.members.iter().any(|member| {
        member.node_id == persisted_invite.issuer_node_id
            && member.node_public_key == persisted_invite.issuer_public_key
            && member.state.is_authorized()
    }) {
        return Err(DaemonError::AuthDenied);
    }

    if request.invite.cluster_public_key != persisted_invite.cluster_public_key {
        return Err(DaemonError::AuthDenied);
    }
    if request.invitation_secret.trim().is_empty()
        || request.invitation_secret != persisted_invite.invitation_secret
    {
        return Err(DaemonError::AuthDenied);
    }
    Ok(())
}

fn read_active_cluster_invite(
    cluster_paths: &ClusterPaths,
    invite_id: &ClusterInviteId,
) -> Result<ClusterJoinInvite, DaemonError> {
    sinomo_network::read_cluster_invitation_from_kv(cluster_paths, invite_id).map_err(Into::into)
}

fn verify_cluster_join_invite_signature(invite: &ClusterJoinInvite) -> Result<(), DaemonError> {
    sinomo_network::verify_cluster_join_invite_signature(invite).map_err(Into::into)
}

fn load_existing_node_identity(path: &Path) -> Result<NodeIdentity, DaemonError> {
    load_or_create_node_identity(path).map_err(DaemonError::from)
}

fn load_optional_node_identity(path: &Path) -> Result<Option<NodeIdentity>, DaemonError> {
    if !path.exists() {
        return Ok(None);
    }
    load_existing_node_identity(path).map(Some)
}

fn remember_daemon_auth_nonce(
    paths: &DaemonPaths,
    envelope: &SignedDaemonCommand,
) -> Result<(), DaemonError> {
    let now = OffsetDateTime::now_utc();
    // The signing timestamp is the nonce's freshness anchor; reject malformed ones.
    let signed_at = OffsetDateTime::parse(&envelope.timestamp, &Rfc3339)
        .map_err(|_| DaemonError::AuthDenied)?;
    let mut guard = DAEMON_AUTH_NONCES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|source| DaemonError::Io {
            path: paths.root.display().to_string(),
            detail: source.to_string(),
        })?;
    let nonce_set = guard
        .entry(paths.root.clone())
        .or_insert_with(|| DaemonAuthNonceSet::new(now));
    let prune_due = (now - nonce_set.last_pruned_at).whole_seconds().abs()
        >= DAEMON_AUTH_NONCE_PRUNE_INTERVAL_SECONDS
        || nonce_set.nonces.len() >= DAEMON_AUTH_NONCE_PRUNE_MAX_ENTRIES;
    if prune_due {
        // Drop nonces older than the retention window so the set stays bounded; anything
        // that old is already rejected by the clock-skew freshness check.
        nonce_set.nonces.retain(|_, seen_at| {
            (now - *seen_at).whole_seconds().abs() <= DAEMON_AUTH_NONCE_RETENTION_SECONDS
        });
        nonce_set.last_pruned_at = now;
    }
    let nonce_key = daemon_auth_nonce_key(envelope);
    if nonce_set.nonces.contains_key(&nonce_key) {
        return Err(DaemonError::AuthDenied);
    }
    nonce_set.nonces.insert(nonce_key, signed_at);
    Ok(())
}

#[cfg(test)]
fn daemon_auth_decode_public_key(public_key: &str) -> Result<Vec<u8>, DaemonError> {
    sinomo_network::daemon_auth_decode_public_key(public_key).map_err(DaemonError::from)
}

fn daemon_auth_nonce_key(envelope: &SignedDaemonCommand) -> Vec<u8> {
    let mut key = Vec::with_capacity(envelope.public_key.len() + 1 + envelope.nonce.len());
    key.extend_from_slice(&envelope.public_key);
    key.push(0);
    key.extend_from_slice(envelope.nonce.as_bytes());
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_kv::{
        CLUSTER_KV_TXN_ROUTE, CLUSTER_KV_WATCH_CANCEL_ROUTE, CLUSTER_KV_WATCH_DRAIN_ROUTE,
        CLUSTER_KV_WATCH_EVENTS_ROUTE, CLUSTER_KV_WATCH_PREFIX_ROUTE,
    };

    fn authenticated_command(
        route: String,
        signer: DaemonSignerAuthority,
    ) -> AuthenticatedDaemonCommand {
        AuthenticatedDaemonCommand {
            route,
            signer,
            signer_node_id: "node_test".to_string(),
            signer_public_key: "public-key".to_string(),
        }
    }

    fn signed_nonce(nonce: &str) -> SignedDaemonCommand {
        SignedDaemonCommand {
            protocol: DAEMON_AUTH_PROTOCOL.to_string(),
            node_id: "node_test".to_string(),
            public_key: b"public-key".to_vec(),
            timestamp: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .expect("timestamp"),
            nonce: nonce.to_string(),
            rpc_body_sha256: None,
            signature: b"signature".to_vec(),
        }
    }

    fn unique_auth_test_root(name: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(name)
    }

    fn unique_auth_test_paths(name: &str) -> (sinomo_testdir::TestDir, DaemonPaths) {
        let root = unique_auth_test_root(name);
        let paths = DaemonPaths::with_root(root.to_path_buf());
        (root, paths)
    }

    fn has_byte_window(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    #[test]
    fn signed_daemon_rpc_auth_uses_binary_body_hash_and_signature() {
        let data_root = unique_auth_test_root("sinomo-daemon-auth-binary");
        let data_dir = SinomoDataDir::new(&data_root);
        let request = sinomo_network::ObjectRpcRequest::encode_body(
            crate::DAEMON_STATUS_ROUTE,
            crate::DAEMON_RPC_CATEGORY,
            &(),
        )
        .expect("rpc request");

        let auth_bytes =
            sign_daemon_rpc_request_from_data_dir(&data_dir, &request).expect("sign rpc auth");
        let auth: SignedDaemonCommand = sinomo_network::SinomoObjectCodec::default()
            .decode(&auth_bytes)
            .expect("decode signed auth");
        let expected_hash = daemon_rpc_body_sha256(&request);

        assert_eq!(
            auth.rpc_body_sha256.as_deref(),
            Some(expected_hash.as_slice())
        );
        assert_eq!(auth.public_key.len(), 32);
        assert_eq!(auth.rpc_body_sha256.as_ref().map(Vec::len), Some(32));
        assert_eq!(auth.signature.len(), 64);

        let public_key_base64 = URL_SAFE_NO_PAD.encode(&auth.public_key);
        let hash_base64 = URL_SAFE_NO_PAD.encode(&expected_hash);
        let signature_base64 = URL_SAFE_NO_PAD.encode(&auth.signature);
        assert!(!has_byte_window(&auth_bytes, public_key_base64.as_bytes()));
        assert!(!has_byte_window(&auth_bytes, hash_base64.as_bytes()));
        assert!(!has_byte_window(&auth_bytes, signature_base64.as_bytes()));
    }

    #[test]
    fn daemon_auth_nonce_rejects_replays_without_touching_disk() {
        let (_root, paths) = unique_auth_test_paths("sinomo-daemon-auth-nonce-mem");
        let first = signed_nonce("nonce-one");
        remember_daemon_auth_nonce(&paths, &first).expect("remember first nonce");

        // Nonces are tracked in memory: no per-request disk files are created, so an
        // idle cluster (which still authenticates periodic RPCs) does no disk I/O.
        assert!(!paths.root.join("auth-nonces.sobj").exists());
        assert!(!paths.root.join("auth-nonces.sobjlog").exists());

        // A replay of the same signed command is rejected.
        assert!(matches!(
            remember_daemon_auth_nonce(&paths, &first),
            Err(DaemonError::AuthDenied)
        ));
        // A distinct nonce is accepted.
        let second = signed_nonce("nonce-two");
        remember_daemon_auth_nonce(&paths, &second).expect("remember second nonce");
    }

    #[test]
    fn daemon_auth_nonce_sets_are_isolated_per_daemon_root() {
        let (_root_a, paths_a) = unique_auth_test_paths("sinomo-daemon-auth-nonce-root-a");
        let (_root_b, paths_b) = unique_auth_test_paths("sinomo-daemon-auth-nonce-root-b");
        let nonce = signed_nonce("shared-nonce");
        remember_daemon_auth_nonce(&paths_a, &nonce).expect("remember under root a");
        // Replay under the same root is rejected...
        assert!(matches!(
            remember_daemon_auth_nonce(&paths_a, &nonce),
            Err(DaemonError::AuthDenied)
        ));
        // ...but a different daemon root tracks its nonces independently.
        remember_daemon_auth_nonce(&paths_b, &nonce).expect("distinct root accepts nonce");
    }

    #[test]
    fn cluster_kv_txn_authorizes_cluster_members_for_voter_forwarding() {
        let authenticated = authenticated_command(
            CLUSTER_KV_TXN_ROUTE.to_string(),
            DaemonSignerAuthority::Member,
        );

        authorize_authenticated_daemon_command(&authenticated)
            .expect("cluster members must be able to forward KV txns to voters");
    }

    #[test]
    fn cluster_health_authorizes_cluster_members_for_routed_inspection() {
        for route in [
            sinomo_network::CLUSTER_HEALTH_ROUTE,
            sinomo_network::CLUSTER_NETWORK_PING_ROUTE,
        ] {
            for signer in [
                DaemonSignerAuthority::Local,
                DaemonSignerAuthority::Voter,
                DaemonSignerAuthority::Member,
            ] {
                let authenticated = authenticated_command(route.to_string(), signer);
                authorize_authenticated_daemon_command(&authenticated)
                    .expect("cluster members must be able to use routed network inspection");
            }
            let authenticated =
                authenticated_command(route.to_string(), DaemonSignerAuthority::JoinCandidate);
            assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
        }
    }

    #[test]
    fn observe_routes_split_cluster_read_from_admin_control() {
        for route in [
            sinomo_observe::OBSERVE_EVENTS_ROUTE,
            sinomo_observe::OBSERVE_LOGS_ROUTE,
            sinomo_observe::OBSERVE_METRICS_ROUTE,
            sinomo_observe::OBSERVE_TRACES_ROUTE,
        ] {
            for signer in [
                DaemonSignerAuthority::Local,
                DaemonSignerAuthority::Voter,
                DaemonSignerAuthority::Member,
            ] {
                let authenticated = authenticated_command(route.to_string(), signer);
                authorize_authenticated_daemon_command(&authenticated)
                    .expect("cluster members must be able to read observe data");
            }
            for signer in [
                DaemonSignerAuthority::JoinCandidate,
                DaemonSignerAuthority::StatusProbe,
            ] {
                let authenticated = authenticated_command(route.to_string(), signer);
                assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
            }
        }

        for signer in [DaemonSignerAuthority::Local, DaemonSignerAuthority::Voter] {
            let authenticated =
                authenticated_command(sinomo_observe::OBSERVE_CONTROL_ROUTE.to_string(), signer);
            authorize_authenticated_daemon_command(&authenticated)
                .expect("observe control requires an admin signer");
        }
        for signer in [
            DaemonSignerAuthority::Member,
            DaemonSignerAuthority::JoinCandidate,
            DaemonSignerAuthority::StatusProbe,
        ] {
            let authenticated =
                authenticated_command(sinomo_observe::OBSERVE_CONTROL_ROUTE.to_string(), signer);
            assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
        }
    }

    #[test]
    fn status_probe_authority_is_limited_to_daemon_status() {
        let status = authenticated_command(
            crate::DAEMON_STATUS_ROUTE.to_string(),
            DaemonSignerAuthority::StatusProbe,
        );
        authorize_authenticated_daemon_command(&status)
            .expect("unknown signed probes may read daemon status");

        let cluster_health = authenticated_command(
            sinomo_network::CLUSTER_HEALTH_ROUTE.to_string(),
            DaemonSignerAuthority::StatusProbe,
        );
        assert!(authorize_authenticated_daemon_command(&cluster_health).is_err());
    }

    #[test]
    fn config_routes_authorize_only_local_signer() {
        for route in [
            CONFIG_CATALOG_ROUTE,
            CONFIG_SEARCH_ROUTE,
            CONFIG_GET_ROUTE,
            CONFIG_SET_ROUTE,
        ] {
            let authenticated =
                authenticated_command(route.to_string(), DaemonSignerAuthority::Local);
            authorize_authenticated_daemon_command(&authenticated)
                .expect("local clients must be able to use daemon config service routes");

            for signer in [
                DaemonSignerAuthority::Voter,
                DaemonSignerAuthority::Member,
                DaemonSignerAuthority::JoinCandidate,
                DaemonSignerAuthority::StatusProbe,
            ] {
                let authenticated = authenticated_command(route.to_string(), signer);
                assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
            }
        }
    }

    #[test]
    fn authorized_same_key_member_survives_node_id_drift() {
        let data_root = unique_auth_test_root("sinomo-daemon-auth-same-key-drift");
        let data_dir = SinomoDataDir::new(&data_root);
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        let local_identity = load_or_create_main_node_identity(&data_root).expect("local identity");
        let peer_root = unique_auth_test_root("sinomo-daemon-auth-same-key-drift-peer");
        let peer_identity = load_or_create_main_node_identity(&peer_root).expect("peer identity");
        let peer_public_key =
            daemon_auth_decode_public_key(&peer_identity.public_key).expect("peer public key");
        let cluster_id =
            sinomo_node::ClusterId::from_legacy_named("cluster_daemon_auth_same_key_drift")
                .expect("cluster id");
        let local_node = sinomo_core::Id::from_legacy_named("node_daemon_auth_same_key_local")
            .expect("local node");
        let stored_peer = sinomo_core::Id::from_legacy_named("node_daemon_auth_same_key_stored")
            .expect("stored peer");
        let drifted_peer = sinomo_core::Id::from_legacy_named("node_daemon_auth_same_key_drifted")
            .expect("drifted peer");
        let now = OffsetDateTime::now_utc();
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "auth-same-key-drift",
            local_node,
            "local",
            local_identity.public_key,
            now,
        );
        let mut peer = ClusterMemberRecord::pending_learner(
            cluster_id,
            stored_peer,
            "peer",
            peer_identity.public_key,
            now,
        );
        let cluster_paths = ClusterPaths::new(&data_dir);
        let envelope = SignedDaemonCommand {
            protocol: DAEMON_AUTH_PROTOCOL.to_string(),
            node_id: drifted_peer.to_string(),
            public_key: peer_public_key,
            timestamp: now.format(&Rfc3339).expect("timestamp"),
            nonce: "same-key-drift".to_string(),
            rpc_body_sha256: None,
            signature: Vec::new(),
        };
        let request = sinomo_network::ObjectRpcRequest::encode_body(
            sinomo_network::CLUSTER_HEALTH_ROUTE,
            sinomo_network::CLUSTER_HEALTH_RPC_CATEGORY,
            &(),
        )
        .expect("health rpc request");

        state.members.push(peer.clone());
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write pending state");
        assert!(matches!(
            authorize_daemon_signer(&daemon_paths, &envelope, &request),
            Err(DaemonError::AuthDenied)
        ));

        peer.state = ClusterMemberState::Learner;
        *state.members.last_mut().expect("peer member") = peer;
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write accepted state");
        assert_eq!(
            authorize_daemon_signer(&daemon_paths, &envelope, &request)
                .expect("authorize accepted same-key member"),
            DaemonSignerAuthority::Member
        );
    }

    #[test]
    fn cluster_kv_watch_routes_authorize_cluster_members() {
        for route in [
            CLUSTER_KV_WATCH_PREFIX_ROUTE,
            CLUSTER_KV_WATCH_DRAIN_ROUTE,
            CLUSTER_KV_WATCH_CANCEL_ROUTE,
            CLUSTER_KV_WATCH_EVENTS_ROUTE,
        ] {
            for signer in [
                DaemonSignerAuthority::Local,
                DaemonSignerAuthority::Voter,
                DaemonSignerAuthority::Member,
            ] {
                let authenticated = authenticated_command(route.to_string(), signer);
                authorize_authenticated_daemon_command(&authenticated)
                    .expect("cluster members must be able to manage cluster KV watches");
            }
            let authenticated =
                authenticated_command(route.to_string(), DaemonSignerAuthority::JoinCandidate);
            assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
        }
    }

    #[test]
    fn rollout_clear_status_authorizes_admin_signers() {
        for signer in [DaemonSignerAuthority::Local, DaemonSignerAuthority::Voter] {
            let authenticated =
                authenticated_command(ROLLOUT_CLEAR_STATUS_ROUTE.to_string(), signer);
            authorize_authenticated_daemon_command(&authenticated)
                .expect("admin signers must be able to clear stale rollout status");
        }
        for signer in [
            DaemonSignerAuthority::Member,
            DaemonSignerAuthority::JoinCandidate,
        ] {
            let authenticated =
                authenticated_command(ROLLOUT_CLEAR_STATUS_ROUTE.to_string(), signer);
            assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
        }
    }

    #[test]
    fn rollout_upload_routes_authorize_admin_signers() {
        for route in [
            ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE,
            ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE,
            ROLLOUT_NATIVE_DYLIB_MODULE_ROUTE,
        ] {
            for signer in [DaemonSignerAuthority::Local, DaemonSignerAuthority::Voter] {
                let authenticated = authenticated_command(route.to_string(), signer);
                authorize_authenticated_daemon_command(&authenticated)
                    .expect("rollout upload routes require admin signers");
            }
            for signer in [
                DaemonSignerAuthority::Member,
                DaemonSignerAuthority::JoinCandidate,
            ] {
                let authenticated = authenticated_command(route.to_string(), signer);
                assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
            }
        }
    }

    #[test]
    fn dfs_metadata_journal_batch_authorizes_cluster_members() {
        for signer in [
            DaemonSignerAuthority::Local,
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
        ] {
            let authenticated = authenticated_command(
                sinomo_dfs::DFS_METADATA_JOURNAL_BATCH_ROUTE.to_string(),
                signer,
            );
            authorize_authenticated_daemon_command(&authenticated)
                .expect("cluster members must be able to replicate DFS metadata journals");
        }
    }

    #[test]
    fn dfs_metadata_snapshot_install_authorizes_cluster_members() {
        for signer in [
            DaemonSignerAuthority::Local,
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
        ] {
            let authenticated = authenticated_command(
                sinomo_dfs::DFS_METADATA_SNAPSHOT_INSTALL_ROUTE.to_string(),
                signer,
            );
            authorize_authenticated_daemon_command(&authenticated)
                .expect("cluster members must be able to repair DFS metadata replicas");
        }
    }

    #[test]
    fn dfs_status_authorizes_cluster_members() {
        for signer in [
            DaemonSignerAuthority::Local,
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
        ] {
            let authenticated =
                authenticated_command(sinomo_dfs::DFS_STATUS_ROUTE.to_string(), signer);
            authorize_authenticated_daemon_command(&authenticated)
                .expect("cluster members must be able to inspect local DFS metadata status");
        }
    }

    #[test]
    fn dfs_inspection_authorizes_cluster_members() {
        for signer in [
            DaemonSignerAuthority::Local,
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
        ] {
            let authenticated =
                authenticated_command(sinomo_dfs::DFS_INSPECTION_ROUTE.to_string(), signer);
            authorize_authenticated_daemon_command(&authenticated)
                .expect("cluster members must be able to inspect local DFS metadata");
        }
    }

    #[test]
    fn dfs_smoke_authorizes_only_local_signer() {
        authorize_authenticated_daemon_command(&authenticated_command(
            sinomo_dfs::DFS_SMOKE_ROUTE.to_string(),
            DaemonSignerAuthority::Local,
        ))
        .expect("local operators must be able to run DFS smoke through the daemon");
        for signer in [
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
            DaemonSignerAuthority::JoinCandidate,
        ] {
            let authenticated =
                authenticated_command(sinomo_dfs::DFS_SMOKE_ROUTE.to_string(), signer);
            assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
        }
    }

    #[test]
    fn dfs_object_transfer_routes_authorize_cluster_members() {
        for route in std::iter::once(sinomo_dfs::DFS_DIRECT_READ_ROUTE)
            .chain(sinomo_dfs::DFS_MESSAGE_DATA_PLANE_ROUTES.iter().copied())
        {
            for signer in [
                DaemonSignerAuthority::Local,
                DaemonSignerAuthority::Voter,
                DaemonSignerAuthority::Member,
            ] {
                let authenticated = authenticated_command(route.to_string(), signer);
                authorize_authenticated_daemon_command(&authenticated)
                    .expect("cluster members must be able to exchange DFS object data");
            }
        }
    }

    #[test]
    fn agent_session_routes_authorize_cluster_members() {
        let route = "/agent/session/inspect".to_string();
        for signer in [
            DaemonSignerAuthority::Local,
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
        ] {
            let authenticated = authenticated_command(route.clone(), signer);
            authorize_authenticated_daemon_command(&authenticated)
                .expect("cluster members must be able to access agent session routes");
        }
        let authenticated = authenticated_command(route, DaemonSignerAuthority::JoinCandidate);
        assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
    }

    #[test]
    fn native_dylib_cancel_route_authorizes_cluster_members() {
        for signer in [
            DaemonSignerAuthority::Local,
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
        ] {
            let authenticated = authenticated_command(
                sinomo_module::MODULE_NATIVE_DYLIB_CANCEL_ROUTE.to_string(),
                signer,
            );
            authorize_authenticated_daemon_command(&authenticated)
                .expect("cluster members must be able to cancel native dylib module operations");
        }
        let authenticated = authenticated_command(
            sinomo_module::MODULE_NATIVE_DYLIB_CANCEL_ROUTE.to_string(),
            DaemonSignerAuthority::JoinCandidate,
        );
        assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
    }

    #[test]
    fn benchmark_cancel_probe_route_authorizes_cluster_members() {
        let route = "/benchmarks/cancel-probe".to_string();
        for signer in [
            DaemonSignerAuthority::Local,
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
        ] {
            let authenticated = authenticated_command(route.clone(), signer);
            authorize_authenticated_daemon_command(&authenticated)
                .expect("cluster members must be able to invoke benchmark cancel probes");
        }
        let authenticated = authenticated_command(route, DaemonSignerAuthority::JoinCandidate);
        assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
    }
}
