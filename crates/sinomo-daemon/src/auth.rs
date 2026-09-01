use crate::{DaemonError, DaemonPaths};
#[cfg(test)]
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::signature::{ED25519, UnparsedPublicKey};
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::cluster::{
    ClusterControlPlaneTimeouts, ClusterInviteChallengeProof, ClusterInviteChallengeRequest,
};
use sinomo_core::kv::cluster_kv_rpc_route_policy;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::ClusterPaths;
#[cfg(test)]
use sinomo_core::node::load_or_create_main_node_identity;
use sinomo_core::node::{
    ClusterInviteId, NodeIdentity, NodeIdentityPaths, load_or_create_node_identity,
};
use sinomo_core::storage::durable_file::file_ino;
use sinomo_core::{ClusterJoinInvite, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState};
#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub use sinomo_api::{AuthenticatedDaemonCommand, DaemonSignerAuthority};

pub const DAEMON_AUTH_PROTOCOL: &str = sinomo_core::network::DAEMON_AUTH_PROTOCOL;
pub const DAEMON_AUTH_MAX_CLOCK_SKEW_SECONDS: i64 = 300;
pub const DAEMON_AUTH_NONCE_RETENTION_SECONDS: i64 = DAEMON_AUTH_MAX_CLOCK_SKEW_SECONDS * 2;
pub use sinomo_core::network::SignedDaemonCommand;
/// In-memory replay-protection nonce sets, keyed by daemon data root. Replacing the
/// previous per-RPC `auth-nonces.sobjlog` append (a disk read + write on *every*
/// authenticated request, including idle cluster heartbeats) keeps idle disk I/O at
/// zero. Replay protection is bounded by the signed-command clock-skew freshness
/// check, so the only behavioral change is that a process restart forgets nonces
/// from the trailing skew window — which the freshness check already constrains.
static DAEMON_AUTH_NONCES: OnceLock<sinomo_api::ScopedReplayNonceCache<PathBuf>> = OnceLock::new();
static DAEMON_AUTH_VERIFIER_CONTEXTS: OnceLock<
    Mutex<HashMap<PathBuf, CachedDaemonAuthVerifierContext>>,
> = OnceLock::new();
static DAEMON_AUTH_VERIFIER_CONTEXT_GENERATIONS: OnceLock<Mutex<HashMap<PathBuf, u64>>> =
    OnceLock::new();
static DAEMON_AUTH_VERIFIER_CONTEXT_WATCHERS: OnceLock<
    Mutex<HashMap<PathBuf, DaemonAuthVerifierContextWatcher>>,
> = OnceLock::new();
static DAEMON_AUTH_VERIFIER_CONTEXT_REARM: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();
#[cfg(test)]
thread_local! {
    static DAEMON_AUTH_FILE_MARK_READS: Cell<u64> = const { Cell::new(0) };
}

#[derive(Clone)]
struct CachedDaemonAuthVerifierContext {
    main_public_key: String,
    shadow_public_key: Option<String>,
    members_by_node_and_key: BTreeMap<(String, String), DaemonSignerAuthority>,
    members_by_key: BTreeMap<String, DaemonSignerAuthority>,
}

struct DaemonAuthVerifierContextWatcher {
    watcher: notify::RecommendedWatcher,
    watched_paths: BTreeSet<PathBuf>,
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

pub fn sign_daemon_rpc_request_for_local_node(
    request: &sinomo_core::network::ObjectRpcRequest,
) -> Result<Vec<u8>, DaemonError> {
    sinomo_core::network::sign_daemon_rpc_request_for_local_node(request).map_err(DaemonError::from)
}

pub fn sign_daemon_rpc_request_digest_for_local_node(
    route: &str,
    category: &str,
    rpc_body_sha256: Vec<u8>,
) -> Result<Vec<u8>, DaemonError> {
    sinomo_core::network::sign_daemon_rpc_request_digest_for_local_node(
        route,
        category,
        rpc_body_sha256,
    )
    .map_err(DaemonError::from)
}

pub fn sign_daemon_rpc_request_from_data_dir(
    data_dir: &SinomoDataDir,
    request: &sinomo_core::network::ObjectRpcRequest,
) -> Result<Vec<u8>, DaemonError> {
    sinomo_core::network::sign_daemon_rpc_request_from_data_dir(data_dir, request)
        .map_err(DaemonError::from)
}

pub fn daemon_rpc_body_sha256(request: &sinomo_core::network::ObjectRpcRequest) -> Vec<u8> {
    sinomo_core::network::daemon_rpc_body_sha256(request)
}

pub fn authenticate_daemon_control_request(
    paths: &DaemonPaths,
    request: &sinomo_core::network::DaemonControlRequest,
) -> Result<AuthenticatedDaemonCommand, DaemonError> {
    authenticate_daemon_control_request_inner(paths, request, true, true, None)
}

/// Authenticates the signed request and its signer without applying the static
/// route allowlist.
///
/// Dynamic routes must use this only when their live catalog descriptor is
/// authorized separately. Keeping this entry point crate-private prevents a
/// caller from accidentally treating signature verification as route authority.
#[cfg(test)]
pub(crate) fn authenticate_daemon_control_request_without_route_authorization(
    paths: &DaemonPaths,
    request: &sinomo_core::network::DaemonControlRequest,
) -> Result<AuthenticatedDaemonCommand, DaemonError> {
    authenticate_daemon_control_request_inner(paths, request, true, false, None)
}

pub(crate) fn authenticate_daemon_control_request_for_service(
    paths: &DaemonPaths,
    request: &sinomo_core::network::DaemonControlRequest,
    exact_member_identity: bool,
) -> Result<AuthenticatedDaemonCommand, DaemonError> {
    authenticate_daemon_control_request_inner(
        paths,
        request,
        true,
        false,
        Some(exact_member_identity),
    )
}

pub(crate) fn authenticate_daemon_control_request_without_nonce(
    paths: &DaemonPaths,
    request: &sinomo_core::network::DaemonControlRequest,
) -> Result<AuthenticatedDaemonCommand, DaemonError> {
    authenticate_daemon_control_request_inner(paths, request, false, true, None)
}

#[inline]
fn auth_timeline_started_ns(
    timeline: Option<sinomo_core::network::NetworkTimelineTraceContext>,
) -> u64 {
    timeline
        .map(|_| sinomo_core::network::network_timeline_monotonic_ns())
        .unwrap_or(0)
}

#[inline]
fn record_auth_timeline_stage(
    timeline: Option<sinomo_core::network::NetworkTimelineTraceContext>,
    stage: u16,
    started_ns: u64,
    success: bool,
) {
    let Some(trace) = timeline else {
        return;
    };
    sinomo_core::network::record_network_timeline_event_now(
        sinomo_core::network::NetworkTimelineEvent::new(
            trace,
            stage,
            sinomo_core::network::network_timeline_flag::REQUEST
                | if success {
                    sinomo_core::network::network_timeline_flag::SUCCESS
                } else {
                    0
                },
            0,
            0,
            0,
            0,
            0,
            sinomo_core::network::network_timeline_monotonic_ns().saturating_sub(started_ns),
        ),
    );
}

fn authenticate_daemon_control_request_inner(
    paths: &DaemonPaths,
    request: &sinomo_core::network::DaemonControlRequest,
    remember_nonce: bool,
    authorize_route: bool,
    exact_member_identity: Option<bool>,
) -> Result<AuthenticatedDaemonCommand, DaemonError> {
    let auth = request.auth.as_deref().ok_or(DaemonError::AuthDenied)?;
    let envelope: SignedDaemonCommand =
        sinomo_core::network::SinomoObjectCodec::default().decode(auth)?;
    verify_daemon_rpc_body_binding(&envelope, request)?;
    let signer = verify_signed_daemon_command(
        paths,
        &envelope,
        request,
        exact_member_identity.unwrap_or(false),
    )?;
    if remember_nonce {
        remember_daemon_auth_nonce(paths, &envelope)?;
    }
    let signer_public_key =
        sinomo_core::network::daemon_auth_encode_public_key(&envelope.public_key);
    let authenticated = AuthenticatedDaemonCommand {
        route: request.route.clone(),
        signer,
        signer_node_id: envelope.node_id,
        signer_public_key,
    };
    if authorize_route {
        authorize_authenticated_daemon_command(&authenticated)?;
    }
    Ok(authenticated)
}

fn verify_daemon_rpc_body_binding(
    envelope: &SignedDaemonCommand,
    request: &sinomo_core::network::ObjectRpcRequest,
) -> Result<(), DaemonError> {
    let Some(expected) = envelope.rpc_body_sha256.as_deref() else {
        return Err(DaemonError::AuthDenied);
    };
    let actual = daemon_rpc_body_sha256(request);
    if expected == actual.as_slice() {
        return Ok(());
    }
    Err(DaemonError::AuthDenied)
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
    sinomo_core::cluster::create_cluster_invite_challenge_proof(
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
        if authenticated
            .signer
            .authorizes_cluster_kv_route(policy.auth)
        {
            return Ok(());
        }
        return Err(DaemonError::AuthDenied);
    }
    if let Some(policy) = sinomo_core::cluster::cluster_control_rpc_route_policy(route) {
        if authenticated
            .signer
            .authorizes_cluster_kv_route(policy.auth)
        {
            return Ok(());
        }
        return Err(DaemonError::AuthDenied);
    }
    if matches!(
        route,
        sinomo_core::network::CLUSTER_HEALTH_ROUTE
            | sinomo_core::network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE
            | sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE
    ) {
        if matches!(
            authenticated.signer,
            DaemonSignerAuthority::Local
                | DaemonSignerAuthority::Voter
                | DaemonSignerAuthority::Member
        ) {
            return Ok(());
        }
        return Err(DaemonError::AuthDenied);
    }
    let allowed = match route {
        route if route == crate::DAEMON_STATUS_ROUTE => true,
        route if route == crate::DAEMON_SHUTDOWN_ROUTE => {
            authenticated.signer.can_run_local_command()
        }
        route if route == crate::DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local | DaemonSignerAuthority::Voter
            )
        }
        route
            if route == crate::DAEMON_OBJECT_RPC_FORWARD_ROUTE
                || route == crate::DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE
                || route == crate::DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE
                || route == crate::DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE
                || route == crate::DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE
                || route == crate::DAEMON_NETWORK_CHUNK_METRICS_ROUTE
                || route == crate::DAEMON_EVENT_SUBSCRIBE_ROUTE =>
        {
            authenticated.signer.can_run_local_command()
        }
        route if route == sinomo_core::network::CLUSTER_JOIN_CHALLENGE_ROUTE => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        route if route == sinomo_core::network::CLUSTER_JOIN_REGISTER_ROUTE => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
                    | DaemonSignerAuthority::JoinCandidate
            )
        }
        route
            if route == sinomo_core::network::CLUSTER_INVITE_PUBLISH_ROUTE
                || route == sinomo_core::network::CLUSTER_MEMBER_PUBLISH_ROUTE =>
        {
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

fn verify_signed_daemon_command(
    paths: &DaemonPaths,
    envelope: &SignedDaemonCommand,
    request: &sinomo_core::network::ObjectRpcRequest,
    require_exact_member_identity: bool,
) -> Result<DaemonSignerAuthority, DaemonError> {
    verify_signed_daemon_command_signature(envelope, request)?;
    authorize_daemon_signer(paths, envelope, request, require_exact_member_identity)
}

fn verify_signed_daemon_command_signature(
    envelope: &SignedDaemonCommand,
    request: &sinomo_core::network::ObjectRpcRequest,
) -> Result<(), DaemonError> {
    let timeline = sinomo_core::network::network_timeline_trace_context_for_request(request);
    if envelope.protocol != DAEMON_AUTH_PROTOCOL {
        return Err(DaemonError::AuthDenied);
    }
    let timestamp_started_ns = auth_timeline_started_ns(timeline);
    let timestamp_result = (|| {
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
        Ok(())
    })();
    record_auth_timeline_stage(
        timeline,
        sinomo_core::network::network_timeline_stage::AUTH_TIMESTAMP,
        timestamp_started_ns,
        timestamp_result.is_ok(),
    );
    timestamp_result?;

    let payload_started_ns = auth_timeline_started_ns(timeline);
    let signing_payload = sinomo_core::network::daemon_auth_signing_payload(
        &envelope.node_id,
        &envelope.public_key,
        &envelope.timestamp,
        &envelope.nonce,
        &request.route,
        &request.category,
        envelope.rpc_body_sha256.as_deref(),
    );
    record_auth_timeline_stage(
        timeline,
        sinomo_core::network::network_timeline_stage::AUTH_SIGNING_PAYLOAD,
        payload_started_ns,
        true,
    );

    let verify_started_ns = auth_timeline_started_ns(timeline);
    let verify_result = UnparsedPublicKey::new(&ED25519, &envelope.public_key)
        .verify(&signing_payload, &envelope.signature)
        .map_err(|_| DaemonError::AuthDenied);
    record_auth_timeline_stage(
        timeline,
        sinomo_core::network::network_timeline_stage::AUTH_ED25519,
        verify_started_ns,
        verify_result.is_ok(),
    );
    verify_result
}

fn authorize_daemon_signer(
    paths: &DaemonPaths,
    envelope: &SignedDaemonCommand,
    request: &sinomo_core::network::ObjectRpcRequest,
    require_exact_member_identity: bool,
) -> Result<DaemonSignerAuthority, DaemonError> {
    let envelope_public_key =
        sinomo_core::network::daemon_auth_encode_public_key(&envelope.public_key);
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let verifier = cached_daemon_auth_verifier_context(data_dir)?;
    let local_identity_key = envelope_public_key == verifier.main_public_key
        || verifier
            .shadow_public_key
            .as_ref()
            .is_some_and(|public_key| envelope_public_key == *public_key);
    if local_identity_key
        && (!require_exact_member_identity
            || verifier
                .members_by_node_and_key
                .contains_key(&(envelope.node_id.clone(), envelope_public_key.clone())))
    {
        return Ok(DaemonSignerAuthority::Local);
    }

    if let Some(authority) = verifier
        .members_by_node_and_key
        .get(&(envelope.node_id.clone(), envelope_public_key.clone()))
    {
        return Ok(authority.clone());
    }
    if !require_exact_member_identity
        && let Some(authority) = verifier.members_by_key.get(&envelope_public_key)
    {
        return Ok(authority.clone());
    }

    if request.route == sinomo_core::network::CLUSTER_JOIN_REGISTER_ROUTE {
        authorize_cluster_join_rpc_signer(paths, Some(request), &envelope_public_key)?;
        return Ok(DaemonSignerAuthority::JoinCandidate);
    }
    if request.route == crate::DAEMON_STATUS_ROUTE {
        return Ok(DaemonSignerAuthority::StatusProbe);
    }
    if let Some(authority) = authorize_connectivity_signer_from_locally_accepted_state(
        data_dir,
        envelope,
        &envelope_public_key,
        request,
    ) {
        // A bridge persists the authority's accepted join response before it
        // sends that response back to the new member. Its merged KV read model
        // can still trail that write for a few milliseconds, though, because
        // non-local members are replaced by the last applied KV snapshot. The
        // post-join connectivity handoff is read-only and identity-exact, so
        // admit it from that trusted accepted state instead of blocking on a
        // fresh quorum snapshot inside the formation gate.
        crate::cluster_join_trace(format_args!(
            "connectivity auth accepted signer={} from locally accepted membership while merged KV visibility was pending",
            envelope.node_id
        ));
        tracing::info!(
            target: "sinomo.cluster.formation",
            stage = "connectivity_auth_local_accepted_state",
            signer_node_id = %envelope.node_id,
            route = %request.route,
            "authenticated connectivity handoff from locally accepted member state"
        );
        return Ok(authority);
    }
    if let Some(authority) = authorize_daemon_signer_after_membership_refresh(
        data_dir,
        envelope,
        &envelope_public_key,
        require_exact_member_identity,
    ) {
        return Ok(authority);
    }
    Err(DaemonError::AuthDenied)
}

fn authorize_connectivity_signer_from_locally_accepted_state(
    data_dir: &Path,
    envelope: &SignedDaemonCommand,
    envelope_public_key: &str,
    request: &sinomo_core::network::ObjectRpcRequest,
) -> Option<DaemonSignerAuthority> {
    if request.route != sinomo_core::network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE {
        return None;
    }
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths).ok()?;
    if sinomo_core::network::cluster_state_needs_join_registration(&state) {
        return None;
    }
    state
        .members
        .iter()
        .find(|member| {
            member.state.is_authorized()
                && member.node_id.as_str() == envelope.node_id
                && member.node_public_key == envelope_public_key
        })
        .map(signer_authority_for_member)
}

fn verifier_authority_for_signer(
    verifier: &CachedDaemonAuthVerifierContext,
    node_id: &str,
    public_key: &str,
    require_exact_member_identity: bool,
) -> Option<DaemonSignerAuthority> {
    let exact = verifier
        .members_by_node_and_key
        .get(&(node_id.to_string(), public_key.to_string()))
        .cloned();
    if require_exact_member_identity {
        exact
    } else {
        exact.or_else(|| verifier.members_by_key.get(public_key).cloned())
    }
}

fn authorize_daemon_signer_after_membership_refresh(
    data_dir: &Path,
    envelope: &SignedDaemonCommand,
    envelope_public_key: &str,
    require_exact_member_identity: bool,
) -> Option<DaemonSignerAuthority> {
    if let Some(authority) = load_uncached_daemon_auth_verifier_context(data_dir)
        .ok()
        .and_then(|verifier| {
            verifier_authority_for_signer(
                &verifier,
                &envelope.node_id,
                envelope_public_key,
                require_exact_member_identity,
            )
        })
    {
        replace_cached_daemon_auth_verifier_context(data_dir, None);
        return Some(authority);
    }

    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    if sinomo_core::kv::process_local_raft_access_is_denied(&cluster_paths.kv_paths().raft) {
        // The shadow verifies only the accepted bootstrap/materialized
        // projection. An unknown signer must fail closed instead of repairing
        // the main voter's local Raft store from an authority response.
        return None;
    }

    refresh_daemon_auth_membership_snapshot(data_dir).ok()?;
    replace_cached_daemon_auth_verifier_context(data_dir, None);
    let verifier = cached_daemon_auth_verifier_context(data_dir).ok()?;
    verifier_authority_for_signer(
        &verifier,
        &envelope.node_id,
        envelope_public_key,
        require_exact_member_identity,
    )
}

fn load_uncached_daemon_auth_verifier_context(
    data_dir: &Path,
) -> Result<CachedDaemonAuthVerifierContext, DaemonError> {
    load_daemon_auth_verifier_context(data_dir)
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

fn invalidate_cached_daemon_auth_verifier_context(data_dir: &Path) {
    let root = data_dir.to_path_buf();
    if let Ok(mut generations) = DAEMON_AUTH_VERIFIER_CONTEXT_GENERATIONS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
    {
        let generation = generations.entry(root.clone()).or_default();
        *generation = generation.saturating_add(1);
    }
    replace_cached_daemon_auth_verifier_context(&root, None);
}

fn request_daemon_auth_verifier_context_rearm(data_dir: &Path) {
    if let Ok(mut roots) = DAEMON_AUTH_VERIFIER_CONTEXT_REARM
        .get_or_init(|| Mutex::new(BTreeSet::new()))
        .lock()
    {
        roots.insert(data_dir.to_path_buf());
    }
}

fn daemon_auth_verifier_context_generation(data_dir: &Path) -> Result<u64, DaemonError> {
    DAEMON_AUTH_VERIFIER_CONTEXT_GENERATIONS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map(|generations| generations.get(data_dir).copied().unwrap_or_default())
        .map_err(|source| DaemonError::Io {
            path: data_dir.display().to_string(),
            detail: source.to_string(),
        })
}

fn daemon_auth_verifier_context_paths(data_dir: &Path) -> BTreeSet<PathBuf> {
    let identity_paths = NodeIdentityPaths::new(data_dir);
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let kv_snapshot = cluster_paths.kv_paths().snapshot;
    [
        identity_paths.main,
        identity_paths.shadow,
        cluster_paths.state,
        kv_snapshot,
    ]
    .into_iter()
    .collect()
}

fn daemon_auth_verifier_event_invalidates(
    event: &notify::Event,
    watched_paths: &BTreeSet<PathBuf>,
) -> bool {
    !matches!(event.kind, notify::EventKind::Access(_))
        && event.paths.iter().any(|path| watched_paths.contains(path))
}

fn handle_daemon_auth_verifier_event(
    data_dir: &Path,
    watched_paths: &BTreeSet<PathBuf>,
    event: &notify::Event,
) {
    if daemon_auth_verifier_event_invalidates(event, watched_paths) {
        // Atomic replacement retires a file's inotify watch along with the old
        // inode. The next verifier lookup re-arms direct file watches after it
        // observes this invalidation.
        request_daemon_auth_verifier_context_rearm(data_dir);
        invalidate_cached_daemon_auth_verifier_context(data_dir);
    }
}

fn daemon_auth_verifier_watch_paths(data_dir: &Path) -> BTreeSet<PathBuf> {
    let mut watch_paths = BTreeSet::new();
    let identity_paths = NodeIdentityPaths::new(data_dir);
    // Main and shadow identities share one quiet directory. Watching it also
    // observes first creation of an optional shadow identity without watching
    // the noisy data-root directory.
    if let Some(identity_dir) = identity_paths.main.parent() {
        watch_paths.insert(identity_dir.to_path_buf());
    }
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let kv_snapshot = cluster_paths.kv_paths().snapshot;
    for path in [cluster_paths.state, kv_snapshot] {
        if path.exists() {
            watch_paths.insert(path);
        } else if let Some(parent) = path.parent() {
            // Cluster initialization may not have materialized the target yet.
            // This parent watch is replaced by a direct file watch immediately
            // after the first relevant creation event.
            watch_paths.insert(parent.to_path_buf());
        }
    }
    watch_paths
}

fn ensure_daemon_auth_verifier_context_watcher(data_dir: &Path) -> Result<(), DaemonError> {
    let root = data_dir.to_path_buf();
    let watched_paths = daemon_auth_verifier_context_paths(data_dir);
    let rearm_requested = DAEMON_AUTH_VERIFIER_CONTEXT_REARM
        .get_or_init(|| Mutex::new(BTreeSet::new()))
        .lock()
        .map_err(|source| DaemonError::Io {
            path: root.display().to_string(),
            detail: source.to_string(),
        })?
        .remove(&root);
    let mut watchers = DAEMON_AUTH_VERIFIER_CONTEXT_WATCHERS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|source| DaemonError::Io {
            path: root.display().to_string(),
            detail: source.to_string(),
        })?;
    if watchers.contains_key(&root) && !rearm_requested {
        return Ok(());
    }

    let watch_paths = daemon_auth_verifier_watch_paths(data_dir);
    if let Some(registered) = watchers.get_mut(&root) {
        for path in std::mem::take(&mut registered.watched_paths) {
            let _ = notify::Watcher::unwatch(&mut registered.watcher, &path);
        }
        for path in &watch_paths {
            notify::Watcher::watch(
                &mut registered.watcher,
                path,
                notify::RecursiveMode::NonRecursive,
            )
            .map_err(|source| DaemonError::Io {
                path: path.display().to_string(),
                detail: format!("re-arm authentication authority watch: {source}"),
            })?;
        }
        registered.watched_paths = watch_paths;
        return Ok(());
    }

    let callback_root = root.clone();
    let callback_paths = watched_paths.clone();
    let mut watcher =
        notify::recommended_watcher(move |result: notify::Result<notify::Event>| match result {
            Ok(event) => {
                handle_daemon_auth_verifier_event(&callback_root, &callback_paths, &event);
            }
            Err(error) => {
                request_daemon_auth_verifier_context_rearm(&callback_root);
                invalidate_cached_daemon_auth_verifier_context(&callback_root);
                tracing::warn!(
                    data_dir = %callback_root.display(),
                    %error,
                    "authentication authority watcher failed; invalidated cached verifier context"
                );
            }
        })
        .map_err(|source| DaemonError::Io {
            path: root.display().to_string(),
            detail: format!("start authentication authority watcher: {source}"),
        })?;
    for path in &watch_paths {
        notify::Watcher::watch(&mut watcher, path, notify::RecursiveMode::NonRecursive).map_err(
            |source| DaemonError::Io {
                path: path.display().to_string(),
                detail: format!("watch authentication authority path: {source}"),
            },
        )?;
    }
    watchers.insert(
        root,
        DaemonAuthVerifierContextWatcher {
            watcher,
            watched_paths: watch_paths,
        },
    );
    Ok(())
}

fn refresh_daemon_auth_membership_snapshot(data_dir: &Path) -> Result<(), DaemonError> {
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if sinomo_core::kv::process_local_raft_access_is_denied(&cluster_paths.kv_paths().raft) {
        return Err(sinomo_core::kv::KvError::AuthorityUnavailable {
            detail: "this process role cannot refresh authentication membership through local Raft storage"
                .to_string(),
        }
        .into());
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let options =
        ClusterControlPlaneTimeouts::default_daemon_transport().kv_snapshot_refresh_options();
    let payload = sinomo_core::kv::cluster_kv_authoritative_snapshot_rpc_payload()?;
    let request = sinomo_core::network::ObjectRpcRequest::from_encoded_body(
        payload.route,
        payload.category,
        payload.body,
    );
    let mut authoritative_snapshot = None;

    // Reuse the bounded authority-target iteration and local Raft cache repair,
    // but require every successful fetch to carry a fresh quorum-backed proof.
    // A local snapshot fallback is never sufficient for an authentication
    // decision, so the captured authoritative response is checked below.
    let refreshed =
        sinomo_core::cluster::refresh_current_cluster_kv_store_with_local_storage_and_transport(
            &cluster_paths,
            &state,
            options,
            |target_node_id, timeout| {
                let response = crate::client::send_object_rpc_to_target_with_options(
                    target_node_id,
                    &request,
                    daemon_auth_membership_rpc_options(timeout),
                    false,
                )
                .map_err(|error| sinomo_core::network::NetworkError::Io {
                    operation: "cluster auth membership refresh".to_string(),
                    detail: error.to_string(),
                })?;
                response.error_detail()?;
                let authoritative =
                    sinomo_core::kv::decode_cluster_kv_authoritative_snapshot_rpc_body(
                        &response.body,
                    )
                    .map_err(|source| {
                        sinomo_core::network::NetworkError::Io {
                            operation: "decode authoritative cluster auth membership snapshot"
                                .to_string(),
                            detail: source.to_string(),
                        }
                    })?;
                let proof = authoritative.authority;
                if proof.revision != authoritative.value.revision {
                    return Err(sinomo_core::network::NetworkError::Io {
                        operation: "validate authoritative cluster auth membership snapshot"
                            .to_string(),
                        detail: format!(
                            "authority proof revision {:?} does not match snapshot revision {:?}",
                            proof.revision, authoritative.value.revision
                        ),
                    });
                }
                match proof.basis {
                    sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
                        voter_acks,
                        required_quorum,
                    } if required_quorum > 0 && voter_acks >= required_quorum => {}
                    basis => {
                        return Err(sinomo_core::network::NetworkError::Io {
                            operation: "validate authoritative cluster auth membership snapshot"
                                .to_string(),
                            detail: format!(
                                "authentication membership refresh requires a fresh current-term quorum proof, got {basis:?}"
                            ),
                        });
                    }
                }
                authoritative_snapshot = Some(authoritative.value.clone());
                Ok(authoritative)
            },
        )?;

    let authoritative_snapshot = authoritative_snapshot.ok_or_else(|| {
        DaemonError::ClusterReadOnly {
            detail: "authentication membership refresh could not obtain an authoritative cluster KV snapshot"
            .to_string(),
        }
    })?;
    let refreshed_snapshot = refreshed.try_snapshot()?;
    if refreshed_snapshot != authoritative_snapshot {
        return Err(DaemonError::ClusterReadOnly {
            detail: format!(
                "authentication membership refresh selected local revision {:?} instead of the authoritative revision {:?}",
                refreshed_snapshot.revision, authoritative_snapshot.revision
            ),
        });
    }
    Ok(())
}

fn daemon_auth_membership_rpc_options(
    timeout: Duration,
) -> sinomo_core::network::ClusterObjectRpcOptions {
    sinomo_core::cluster::main_daemon_cluster_rpc_options(
        sinomo_core::network::ClusterObjectRpcOptions::new(
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
        ),
    )
}

fn cached_daemon_auth_verifier_context(
    data_dir: &Path,
) -> Result<CachedDaemonAuthVerifierContext, DaemonError> {
    let root = data_dir.to_path_buf();
    {
        let guard = DAEMON_AUTH_VERIFIER_CONTEXTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|source| DaemonError::Io {
                path: root.display().to_string(),
                detail: source.to_string(),
            })?;
        if let Some(cached) = guard.get(&root) {
            return Ok(cached.clone());
        }
    }

    // Authority files are replaced atomically by their owners. Watch their
    // parent directories before loading so steady authentication never polls
    // filesystem metadata, while cross-process identity and membership changes
    // still invalidate this process's verifier immediately.
    ensure_daemon_auth_verifier_context_watcher(data_dir)?;
    let generation = daemon_auth_verifier_context_generation(data_dir)?;
    let mark = daemon_auth_verifier_context_mark(data_dir)?;
    let loaded = load_daemon_auth_verifier_context(data_dir)?;
    let after_mark = daemon_auth_verifier_context_mark(data_dir)?;
    let after_generation = daemon_auth_verifier_context_generation(data_dir)?;
    if after_mark == mark && after_generation == generation {
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
) -> Result<CachedDaemonAuthVerifierContext, DaemonError> {
    let identity_paths = NodeIdentityPaths::new(data_dir);
    let main_identity = load_existing_node_identity(&identity_paths.main)?;
    let shadow_identity = load_optional_node_identity(&identity_paths.shadow)?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let mut members_by_node_and_key = BTreeMap::new();
    let mut members_by_key = BTreeMap::new();
    if cluster_paths.state.exists() {
        let state = sinomo_core::cluster::read_cluster_state(&cluster_paths)?;
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
        kv_snapshot: daemon_auth_file_mark(&cluster_paths.kv_paths().snapshot)?,
    })
}

fn daemon_auth_file_mark(path: &Path) -> Result<DaemonAuthFileMark, DaemonError> {
    #[cfg(test)]
    DAEMON_AUTH_FILE_MARK_READS.with(|reads| reads.set(reads.get().saturating_add(1)));
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
    rpc_request: Option<&sinomo_core::network::ObjectRpcRequest>,
    public_key: &str,
) -> Result<(), DaemonError> {
    let request = rpc_request.ok_or(DaemonError::AuthDenied)?;
    if request.route != sinomo_core::network::CLUSTER_JOIN_REGISTER_ROUTE {
        return Err(DaemonError::AuthDenied);
    }
    let request: sinomo_core::cluster::ClusterJoinRegistrationRequest = request.decode_body()?;
    authorize_cluster_join_request_signer(paths, &request, public_key)
}

fn authorize_cluster_join_request_signer(
    paths: &DaemonPaths,
    request: &sinomo_core::cluster::ClusterJoinRegistrationRequest,
    public_key: &str,
) -> Result<(), DaemonError> {
    let requested_member = request
        .members
        .iter()
        .find(|member| member.node_public_key == public_key)
        .ok_or_else(|| {
            crate::cluster_join_trace(format_args!(
                "join auth denied: signer key does not match any of {} requested members",
                request.members.len()
            ));
            DaemonError::AuthDenied
        })?;
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let state = sinomo_core::cluster::read_cluster_state(&cluster_paths)?;

    let persisted_invite =
        read_active_cluster_invite(&cluster_paths, &state, &request.invite.invite_id).inspect_err(
            |error| {
                crate::cluster_join_trace(format_args!(
                    "join auth denied: active invite lookup failed: {error}"
                ));
            },
        )?;
    if !persisted_invite
        .allowed_member_roles
        .contains(&requested_member.role)
    {
        crate::cluster_join_trace(format_args!(
            "join auth denied: requested role {:?} is not allowed",
            requested_member.role
        ));
        return Err(DaemonError::AuthDenied);
    }
    verify_cluster_join_invite_signature(&persisted_invite).inspect_err(|error| {
        crate::cluster_join_trace(format_args!(
            "join auth denied: persisted invite signature is invalid: {error}"
        ));
    })?;
    if persisted_invite.expires_at <= OffsetDateTime::now_utc() {
        crate::cluster_join_trace(format_args!("join auth denied: persisted invite expired"));
        return Err(DaemonError::AuthExpired);
    }
    if persisted_invite.cluster_id != state.cluster_id {
        crate::cluster_join_trace(format_args!(
            "join auth denied: persisted invite cluster does not match local cluster"
        ));
        return Err(DaemonError::AuthDenied);
    }
    if !state.members.iter().any(|member| {
        member.node_id == persisted_invite.issuer_node_id
            && member.node_public_key == persisted_invite.issuer_public_key
            && member.state.is_authorized()
    }) {
        crate::cluster_join_trace(format_args!(
            "join auth denied: invite issuer is not an authorized local member"
        ));
        return Err(DaemonError::AuthDenied);
    }

    if request.invite.cluster_public_key != persisted_invite.cluster_public_key {
        crate::cluster_join_trace(format_args!(
            "join auth denied: invite cluster public key does not match persisted invite"
        ));
        return Err(DaemonError::AuthDenied);
    }
    if request.invitation_secret.trim().is_empty()
        || request.invitation_secret != persisted_invite.invitation_secret
    {
        crate::cluster_join_trace(format_args!(
            "join auth denied: invitation secret is empty or does not match"
        ));
        return Err(DaemonError::AuthDenied);
    }
    crate::cluster_join_trace(format_args!(
        "join auth accepted for requested node {}",
        requested_member.node_id
    ));
    Ok(())
}

fn read_active_cluster_invite(
    cluster_paths: &ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
    invite_id: &ClusterInviteId,
) -> Result<ClusterJoinInvite, DaemonError> {
    sinomo_core::cluster::read_cluster_invitation_for_join_introducer(
        cluster_paths,
        state,
        invite_id,
    )
    .map_err(Into::into)
}

fn verify_cluster_join_invite_signature(invite: &ClusterJoinInvite) -> Result<(), DaemonError> {
    sinomo_core::cluster::verify_cluster_join_invite_signature(invite).map_err(Into::into)
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
    let expires_at = signed_at
        .checked_add(time::Duration::seconds(DAEMON_AUTH_NONCE_RETENTION_SECONDS))
        .ok_or(DaemonError::AuthDenied)?;
    let remembered = DAEMON_AUTH_NONCES
        .get_or_init(sinomo_api::ScopedReplayNonceCache::default)
        .remember(
            paths.root.clone(),
            daemon_auth_nonce_key(envelope),
            expires_at,
            now,
        )
        .map_err(|source| DaemonError::Io {
            path: paths.root.display().to_string(),
            detail: source.to_string(),
        })?;
    if !remembered {
        return Err(DaemonError::AuthDenied);
    }
    Ok(())
}

#[cfg(test)]
fn daemon_auth_decode_public_key(public_key: &str) -> Result<Vec<u8>, DaemonError> {
    sinomo_core::network::daemon_auth_decode_public_key(public_key).map_err(DaemonError::from)
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
    use sinomo_core::kv::{
        CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE, CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE,
        CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE, CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE,
        CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE, CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE,
        CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE, CLUSTER_KV_TXN_ROUTE, CLUSTER_KV_WATCH_EVENTS_ROUTE,
    };

    #[test]
    fn auth_membership_snapshot_refresh_is_main_daemon_affine() {
        assert_eq!(
            daemon_auth_membership_rpc_options(Duration::from_secs(5))
                .destination_process_affinity(),
            sinomo_core::network::DestinationProcessAffinity::MainDaemon
        );
    }

    #[test]
    fn auth_verifier_cache_hits_do_not_touch_authority_files() {
        let data_root = unique_auth_test_root("sinomo-daemon-auth-event-cache-hit");
        let data_dir = SinomoDataDir::new(&data_root);
        load_or_create_main_node_identity(&data_root).expect("main identity");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        std::fs::create_dir_all(
            cluster_paths
                .kv_paths()
                .snapshot
                .parent()
                .expect("KV snapshot parent"),
        )
        .expect("KV snapshot directory");

        DAEMON_AUTH_FILE_MARK_READS.with(|reads| reads.set(0));
        let first = cached_daemon_auth_verifier_context(&data_root).expect("first verifier");
        let reads_after_load = DAEMON_AUTH_FILE_MARK_READS.with(Cell::get);
        assert!(reads_after_load > 0);

        let second = cached_daemon_auth_verifier_context(&data_root).expect("cached verifier");
        assert_eq!(first.main_public_key, second.main_public_key);
        assert_eq!(
            DAEMON_AUTH_FILE_MARK_READS.with(Cell::get),
            reads_after_load,
            "a steady cache hit must not poll authority-file metadata"
        );
    }

    #[test]
    fn auth_verifier_events_ignore_reads_and_invalidate_changes() {
        let data_root = unique_auth_test_root("sinomo-daemon-auth-event-invalidation");
        let watched_paths = daemon_auth_verifier_context_paths(&data_root);
        let watched = watched_paths.iter().next().expect("watched path").clone();
        let unrelated = data_root.join("unrelated.sobj");
        let before = daemon_auth_verifier_context_generation(&data_root).expect("generation");

        let access = notify::Event::new(notify::EventKind::Access(notify::event::AccessKind::Read))
            .add_path(watched.clone());
        handle_daemon_auth_verifier_event(&data_root, &watched_paths, &access);
        assert_eq!(
            daemon_auth_verifier_context_generation(&data_root).expect("access generation"),
            before
        );

        let unrelated_change = notify::Event::new(notify::EventKind::Modify(
            notify::event::ModifyKind::Data(notify::event::DataChange::Any),
        ))
        .add_path(unrelated);
        handle_daemon_auth_verifier_event(&data_root, &watched_paths, &unrelated_change);
        assert_eq!(
            daemon_auth_verifier_context_generation(&data_root).expect("unrelated generation"),
            before
        );

        let authority_change = notify::Event::new(notify::EventKind::Modify(
            notify::event::ModifyKind::Data(notify::event::DataChange::Any),
        ))
        .add_path(watched);
        handle_daemon_auth_verifier_event(&data_root, &watched_paths, &authority_change);
        assert_eq!(
            daemon_auth_verifier_context_generation(&data_root).expect("changed generation"),
            before.saturating_add(1)
        );
    }

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
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            crate::DAEMON_STATUS_ROUTE,
            crate::DAEMON_RPC_CATEGORY,
            &(),
        )
        .expect("rpc request");

        let auth_bytes =
            sign_daemon_rpc_request_from_data_dir(&data_dir, &request).expect("sign rpc auth");
        let auth: SignedDaemonCommand = sinomo_core::network::SinomoObjectCodec::default()
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
    fn catalog_authentication_separates_signature_from_static_route_authority() {
        let data_root = unique_auth_test_root("sinomo-daemon-auth-dynamic-route");
        std::fs::create_dir_all(data_root.path().join("cluster"))
            .expect("create authority watch root");
        let data_dir = SinomoDataDir::new(&data_root);
        let paths = DaemonPaths::new(&data_dir);
        let unsigned = sinomo_core::network::ObjectRpcRequest::encode_body(
            "/test/dynamic-module-route",
            "test-module",
            &(),
        )
        .expect("dynamic module request");

        let mut static_route_request = unsigned.clone();
        static_route_request.auth = Some(
            sign_daemon_rpc_request_from_data_dir(&data_dir, &static_route_request)
                .expect("sign static-route request"),
        );
        let static_result = authenticate_daemon_control_request(&paths, &static_route_request);
        assert!(
            matches!(static_result, Err(DaemonError::AuthDenied)),
            "unexpected static authorization result: {static_result:?}"
        );

        let mut catalog_request = unsigned;
        catalog_request.auth = Some(
            sign_daemon_rpc_request_from_data_dir(&data_dir, &catalog_request)
                .expect("sign catalog request"),
        );
        let authenticated = authenticate_daemon_control_request_without_route_authorization(
            &paths,
            &catalog_request,
        )
        .expect("catalog will authorize the route descriptor separately");
        assert_eq!(authenticated.route, "/test/dynamic-module-route");
        assert_eq!(authenticated.signer, DaemonSignerAuthority::Local);
    }

    #[test]
    fn metadata_bound_status_binding_remains_valid() {
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            crate::DAEMON_STATUS_ROUTE,
            crate::DAEMON_RPC_CATEGORY,
            &(),
        )
        .expect("status request")
        .with_metadata("traceparent", "test-trace");
        let mut envelope = signed_nonce("bound-status-poll");
        envelope.rpc_body_sha256 = Some(daemon_rpc_body_sha256(&request));

        verify_daemon_rpc_body_binding(&envelope, &request)
            .expect("new status requests keep metadata binding");
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
    fn cluster_control_identity_changes_require_a_local_signer() {
        for route in [
            sinomo_core::cluster::CLUSTER_CONTROL_INIT_ROUTE,
            sinomo_core::cluster::CLUSTER_CONTROL_JOIN_ROUTE,
            sinomo_core::cluster::CLUSTER_CONTROL_LEAVE_ROUTE,
        ] {
            authorize_authenticated_daemon_command(&authenticated_command(
                route.to_string(),
                DaemonSignerAuthority::Local,
            ))
            .expect("local operators may change this node's cluster identity");
            assert!(
                authorize_authenticated_daemon_command(&authenticated_command(
                    route.to_string(),
                    DaemonSignerAuthority::Voter,
                ))
                .is_err(),
                "remote voters must not invoke local identity route {route}"
            );
        }
    }

    #[test]
    fn cluster_kv_snapshot_routes_enforce_explicit_authority_scopes() {
        for signer in [
            DaemonSignerAuthority::Local,
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
            DaemonSignerAuthority::JoinCandidate,
            DaemonSignerAuthority::StatusProbe,
        ] {
            authorize_authenticated_daemon_command(&authenticated_command(
                CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE.to_string(),
                signer,
            ))
            .expect("the explicitly local snapshot route accepts authenticated diagnostics");
        }

        for route in [
            CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE,
            CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE,
            CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE,
        ] {
            for signer in [
                DaemonSignerAuthority::Local,
                DaemonSignerAuthority::Voter,
                DaemonSignerAuthority::Member,
            ] {
                authorize_authenticated_daemon_command(&authenticated_command(
                    route.to_string(),
                    signer,
                ))
                .expect("cluster members may request barrier-backed reads");
            }
            for signer in [
                DaemonSignerAuthority::JoinCandidate,
                DaemonSignerAuthority::StatusProbe,
            ] {
                assert!(
                    authorize_authenticated_daemon_command(&authenticated_command(
                        route.to_string(),
                        signer,
                    ))
                    .is_err(),
                    "non-members must not request authoritative cluster state"
                );
            }
        }
    }

    #[test]
    fn network_object_rpc_routes_authorize_cluster_members_for_inspection() {
        for route in [
            sinomo_core::network::CLUSTER_HEALTH_ROUTE,
            sinomo_core::network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
            sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE,
        ] {
            for signer in [
                DaemonSignerAuthority::Local,
                DaemonSignerAuthority::Voter,
                DaemonSignerAuthority::Member,
            ] {
                let authenticated = authenticated_command(route.to_string(), signer);
                authorize_authenticated_daemon_command(&authenticated)
                    .unwrap_or_else(|_| panic!("cluster members must be able to use {route}"));
            }
            let authenticated =
                authenticated_command(route.to_string(), DaemonSignerAuthority::JoinCandidate);
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
            sinomo_core::network::CLUSTER_HEALTH_ROUTE.to_string(),
            DaemonSignerAuthority::StatusProbe,
        );
        assert!(authorize_authenticated_daemon_command(&cluster_health).is_err());
    }

    #[test]
    fn join_candidate_auth_uses_newer_invite_cache_on_non_voter_bridge() {
        let data_root = unique_auth_test_root("sinomo-daemon-auth-join-bridge-cache");
        let data_dir = SinomoDataDir::new(&data_root);
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let issuer_root = unique_auth_test_root("sinomo-daemon-auth-join-bridge-cache-issuer");
        let issuer_identity =
            load_or_create_main_node_identity(&issuer_root).expect("issuer identity");
        let candidate_root =
            unique_auth_test_root("sinomo-daemon-auth-join-bridge-cache-candidate");
        let candidate_identity =
            load_or_create_main_node_identity(&candidate_root).expect("candidate identity");
        let now = OffsetDateTime::now_utc();
        let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
            "cluster_daemon_auth_join_bridge_cache",
        )
        .expect("cluster");
        let issuer_node = sinomo_core::node::NodeId::from_legacy_named(
            "node_daemon_auth_join_bridge_cache_issuer",
        )
        .expect("issuer");
        let bridge_node = sinomo_core::node::NodeId::from_legacy_named(
            "node_daemon_auth_join_bridge_cache_bridge",
        )
        .expect("bridge");
        let candidate_node = sinomo_core::node::NodeId::from_legacy_named(
            "node_daemon_auth_join_bridge_cache_candidate",
        )
        .expect("candidate");
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "auth join bridge cache",
            issuer_node.clone(),
            "issuer",
            issuer_identity.public_key.clone(),
            now,
        );
        state.members[0].capabilities =
            sinomo_core::node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        let mut bridge = ClusterMemberRecord::pending_learner(
            cluster_id.clone(),
            bridge_node.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        bridge.state = ClusterMemberState::Learner;
        bridge.capabilities = sinomo_core::node::main_daemon_capabilities(&bridge.role);
        bridge.allowed_capabilities = bridge.capabilities.clone();
        state.members.push(bridge);

        let mut invite = ClusterJoinInvite {
            invite_id: ClusterInviteId::from_legacy_named("invite_daemon_auth_bridge_cache")
                .expect("invite"),
            cluster_id: cluster_id.clone(),
            cluster_name: Some("auth join bridge cache".to_string()),
            cluster_public_key: issuer_identity.public_key.clone(),
            invitation_secret: "secret-daemon-auth-bridge-cache".to_string(),
            communication_channels: Vec::new(),
            introducers: Vec::new(),
            token_id: "token-daemon-auth-bridge-cache".to_string(),
            expires_at: now + time::Duration::hours(1),
            trust_domain: sinomo_core::TrustDomain::new("trust.local").expect("trust domain"),
            allowed_member_roles: vec![ClusterMemberRole::Learner],
            issuer_node_id: issuer_node,
            issuer_public_key: issuer_identity.public_key.clone(),
            signature: String::new(),
        };
        invite.signature =
            sinomo_core::cluster::sign_cluster_join_invite(&invite, &issuer_identity)
                .expect("sign invite");

        state.local_node_id = bridge_node;
        state.local_node_display_name = "bridge".to_string();
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write bridge state");
        let local_cache = cluster_paths.kv_paths().local_cache();
        let mut raft_node = local_cache
            .open_raft_node(state.local_node_id.clone())
            .expect("open learner raft");
        let baseline = raft_node
            .append_local_command(
                1,
                sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Put {
                    key: "/test/join-bridge-applied-baseline".to_string(),
                    value: b"baseline".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append learner baseline");
        raft_node
            .commit_to(baseline.index)
            .expect("commit learner baseline");
        let applied = raft_node
            .store()
            .try_snapshot()
            .expect("applied learner snapshot");
        drop(raft_node);
        let mut cache =
            sinomo_core::kv::InProcessKvStore::restore(applied).expect("restore learner snapshot");
        cache
            .put_serde(
                sinomo_core::node::cluster_invitation_key(&invite.invite_id),
                &invite,
                None,
            )
            .expect("cache invitation");
        local_cache
            .write_snapshot(&cache.snapshot().expect("cached snapshot"))
            .expect("write invitation cache");

        let request = sinomo_core::cluster::ClusterJoinRegistrationRequest {
            invite: invite.clone(),
            invitation_secret: invite.invitation_secret.clone(),
            members: vec![ClusterMemberRecord::pending_learner(
                cluster_id,
                candidate_node,
                "candidate",
                candidate_identity.public_key.clone(),
                now,
            )],
        };
        authorize_cluster_join_request_signer(
            &daemon_paths,
            &request,
            &candidate_identity.public_key,
        )
        .expect("authorize candidate through converging bridge cache");
        assert!(matches!(
            authorize_cluster_join_request_signer(
                &daemon_paths,
                &request,
                &issuer_identity.public_key,
            ),
            Err(DaemonError::AuthDenied)
        ));

        let mut bad_secret = request;
        bad_secret.invitation_secret.push_str("-tampered");
        assert!(matches!(
            authorize_cluster_join_request_signer(
                &daemon_paths,
                &bad_secret,
                &candidate_identity.public_key,
            ),
            Err(DaemonError::AuthDenied)
        ));
    }

    #[test]
    fn connectivity_handoff_accepts_exact_member_from_locally_accepted_state_only() {
        let data_root = unique_auth_test_root("sinomo-daemon-auth-connectivity-handoff");
        let data_dir = SinomoDataDir::new(&data_root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
            "cluster_daemon_auth_connectivity_handoff",
        )
        .expect("cluster id");
        let local_node = sinomo_core::node::NodeId::from_legacy_named(
            "node_daemon_auth_connectivity_handoff_local",
        )
        .expect("local node");
        let peer_node = sinomo_core::node::NodeId::from_legacy_named(
            "node_daemon_auth_connectivity_handoff_peer",
        )
        .expect("peer node");
        let now = OffsetDateTime::now_utc();
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity handoff",
            local_node,
            "local",
            "pk-local",
            now,
        );
        let mut peer = ClusterMemberRecord::pending_learner(
            cluster_id,
            peer_node.clone(),
            "peer",
            "pk-peer",
            now,
        );
        peer.state = ClusterMemberState::Learner;
        state.members.push(peer);
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write locally accepted state");

        let envelope = SignedDaemonCommand {
            protocol: DAEMON_AUTH_PROTOCOL.to_string(),
            node_id: peer_node.to_string(),
            public_key: Vec::new(),
            timestamp: now.format(&Rfc3339).expect("timestamp"),
            nonce: "connectivity-handoff".to_string(),
            rpc_body_sha256: None,
            signature: Vec::new(),
        };
        let connectivity = sinomo_core::network::ObjectRpcRequest::encode_body(
            sinomo_core::network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
            sinomo_core::network::CLUSTER_NETWORK_RPC_CATEGORY,
            &(),
        )
        .expect("connectivity request");
        assert_eq!(
            authorize_connectivity_signer_from_locally_accepted_state(
                data_root.path(),
                &envelope,
                "pk-peer",
                &connectivity,
            ),
            Some(DaemonSignerAuthority::Member)
        );
        assert_eq!(
            authorize_connectivity_signer_from_locally_accepted_state(
                data_root.path(),
                &envelope,
                "pk-wrong",
                &connectivity,
            ),
            None,
            "the locally accepted fast path must bind the exact member key"
        );

        let health = sinomo_core::network::ObjectRpcRequest::encode_body(
            sinomo_core::network::CLUSTER_HEALTH_ROUTE,
            sinomo_core::network::CLUSTER_HEALTH_RPC_CATEGORY,
            &(),
        )
        .expect("health request");
        assert_eq!(
            authorize_connectivity_signer_from_locally_accepted_state(
                data_root.path(),
                &envelope,
                "pk-peer",
                &health,
            ),
            None,
            "locally accepted state must not widen authorization for other RPC routes"
        );
    }

    #[test]
    fn exact_member_service_policy_rejects_same_key_node_id_drift() {
        let data_root = unique_auth_test_root("sinomo-daemon-auth-same-key-drift");
        let data_dir = SinomoDataDir::new(&data_root);
        let daemon_paths = DaemonPaths::with_root(data_root.join("daemon"));
        let local_identity = load_or_create_main_node_identity(&data_root).expect("local identity");
        let local_public_key =
            daemon_auth_decode_public_key(&local_identity.public_key).expect("local public key");
        let peer_root = unique_auth_test_root("sinomo-daemon-auth-same-key-drift-peer");
        let peer_identity = load_or_create_main_node_identity(&peer_root).expect("peer identity");
        let peer_public_key =
            daemon_auth_decode_public_key(&peer_identity.public_key).expect("peer public key");
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_daemon_auth_same_key_drift")
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
            local_node.clone(),
            "local",
            local_identity.public_key,
            now,
        );
        let mut peer = ClusterMemberRecord::pending_learner(
            cluster_id,
            stored_peer.clone(),
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
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            sinomo_core::network::CLUSTER_HEALTH_ROUTE,
            sinomo_core::network::CLUSTER_HEALTH_RPC_CATEGORY,
            &(),
        )
        .expect("health rpc request");

        state.members.push(peer.clone());
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write pending state");
        assert!(matches!(
            authorize_daemon_signer(&daemon_paths, &envelope, &request, false),
            Err(DaemonError::AuthDenied)
        ));

        peer.state = ClusterMemberState::Learner;
        *state.members.last_mut().expect("peer member") = peer;
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state)
            .expect("write accepted state");
        assert_eq!(
            authorize_daemon_signer(&daemon_paths, &envelope, &request, false)
                .expect("authorize accepted same-key member"),
            DaemonSignerAuthority::Member
        );

        for route in ["/test/exact-authority-a", "/test/exact-authority-b"] {
            let authority_request = sinomo_core::network::ObjectRpcRequest::encode_body(
                route,
                "test.exact-authority",
                &(),
            )
            .expect("authority rpc request");
            assert!(matches!(
                authorize_daemon_signer(&daemon_paths, &envelope, &authority_request, true),
                Err(DaemonError::AuthDenied)
            ));

            let exact_envelope = SignedDaemonCommand {
                node_id: stored_peer.to_string(),
                ..envelope.clone()
            };
            assert_eq!(
                authorize_daemon_signer(&daemon_paths, &exact_envelope, &authority_request, true,)
                    .expect("authorize exact member identity"),
                DaemonSignerAuthority::Member
            );
        }

        let authority_request = sinomo_core::network::ObjectRpcRequest::encode_body(
            "/test/exact-local-authority",
            "test.exact-authority",
            &(),
        )
        .expect("local authority rpc request");
        let local_drift_envelope = SignedDaemonCommand {
            node_id: drifted_peer.to_string(),
            public_key: local_public_key,
            ..envelope.clone()
        };
        assert!(matches!(
            authorize_daemon_signer(
                &daemon_paths,
                &local_drift_envelope,
                &authority_request,
                true,
            ),
            Err(DaemonError::AuthDenied)
        ));
        let exact_local_envelope = SignedDaemonCommand {
            node_id: local_node.to_string(),
            ..local_drift_envelope
        };
        assert_eq!(
            authorize_daemon_signer(
                &daemon_paths,
                &exact_local_envelope,
                &authority_request,
                true,
            )
            .expect("authorize exact local member identity"),
            DaemonSignerAuthority::Local
        );
    }

    #[test]
    fn cluster_kv_watch_routes_authorize_cluster_members() {
        for route in [
            CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE,
            CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE,
            CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE,
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
    fn daemon_event_subscriptions_are_local_only() {
        authorize_authenticated_daemon_command(&authenticated_command(
            crate::DAEMON_EVENT_SUBSCRIBE_ROUTE.to_string(),
            DaemonSignerAuthority::Local,
        ))
        .expect("local app clients may subscribe to daemon events");

        for signer in [
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
            DaemonSignerAuthority::JoinCandidate,
        ] {
            let authenticated =
                authenticated_command(crate::DAEMON_EVENT_SUBSCRIBE_ROUTE.to_string(), signer);
            assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
        }
    }

    #[test]
    fn remote_shadow_shutdown_is_voter_only_and_does_not_widen_generic_shutdown() {
        for signer in [DaemonSignerAuthority::Local, DaemonSignerAuthority::Voter] {
            authorize_authenticated_daemon_command(&authenticated_command(
                crate::DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE.to_string(),
                signer,
            ))
            .expect("local operators and cluster voters may stop a proven shadow process");
        }
        for signer in [
            DaemonSignerAuthority::Member,
            DaemonSignerAuthority::JoinCandidate,
            DaemonSignerAuthority::StatusProbe,
        ] {
            assert!(
                authorize_authenticated_daemon_command(&authenticated_command(
                    crate::DAEMON_REMOTE_SHADOW_SHUTDOWN_ROUTE.to_string(),
                    signer,
                ))
                .is_err(),
                "non-voter signer must not stop a shadow process"
            );
        }
        assert!(
            authorize_authenticated_daemon_command(&authenticated_command(
                crate::DAEMON_SHUTDOWN_ROUTE.to_string(),
                DaemonSignerAuthority::Voter,
            ))
            .is_err(),
            "the generic daemon shutdown route must remain local-only"
        );
    }

    #[test]
    fn daemon_object_rpc_forward_authorizes_only_local_signer() {
        for route in [
            crate::DAEMON_OBJECT_RPC_FORWARD_ROUTE,
            crate::DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE,
            crate::DAEMON_OBJECT_RPC_FORWARD_MAIN_ROUTE,
            crate::DAEMON_DFS_OBJECT_RPC_FORWARD_MAIN_ROUTE,
            crate::DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE,
            crate::DAEMON_NETWORK_CHUNK_METRICS_ROUTE,
        ] {
            authorize_authenticated_daemon_command(&authenticated_command(
                route.to_string(),
                DaemonSignerAuthority::Local,
            ))
            .expect("local operators must be able to forward object RPCs through the daemon");
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
}
