use crate::{DaemonError, DaemonPaths};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::digest;
use ring::signature::{ED25519, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use sinomo_core::{ClusterJoinInvite, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState};
use sinomo_kv::{
    CLUSTER_KV_RANGE_PREFIX_ROUTE, CLUSTER_KV_SNAPSHOT_ROUTE, CLUSTER_KV_TXN_ROUTE,
    CLUSTER_KV_WATCH_CANCEL_ROUTE, CLUSTER_KV_WATCH_DRAIN_ROUTE, CLUSTER_KV_WATCH_EVENTS_ROUTE,
    CLUSTER_KV_WATCH_PREFIX_ROUTE,
};
use sinomo_local::SinomoDataDir;
use sinomo_network::{
    ClusterInviteChallengeProof, ClusterInviteChallengeRequest, ClusterInviteId, ClusterPaths,
};
use sinomo_node::{
    NodeIdentity, NodeIdentityPaths, load_or_create_main_node_identity,
    load_or_create_node_identity,
};
use sinomo_rollout::{
    ROLLOUT_BINARY_ROUTE, ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE, ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE,
    ROLLOUT_BUILD_ROUTE, ROLLOUT_CLEAR_STATUS_ROUTE, ROLLOUT_DIAGNOSTICS_ROUTE,
    ROLLOUT_FINALIZE_SHADOW_ROUTE,
};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const DAEMON_AUTH_PROTOCOL: &str = "sinomo.daemon-auth.v1";
pub const DAEMON_AUTH_MAX_CLOCK_SKEW_SECONDS: i64 = 300;
pub const DAEMON_AUTH_NONCE_RETENTION_SECONDS: i64 = DAEMON_AUTH_MAX_CLOCK_SKEW_SECONDS * 2;
/// In-memory replay-protection nonce sets, keyed by daemon data root. Replacing the
/// previous per-RPC `auth-nonces.sobjlog` append (a disk read + write on *every*
/// authenticated request, including idle cluster heartbeats) keeps idle disk I/O at
/// zero. Replay protection is bounded by the signed-command clock-skew freshness
/// check, so the only behavioral change is that a process restart forgets nonces
/// from the trailing skew window — which the freshness check already constrains.
static DAEMON_AUTH_NONCES: OnceLock<Mutex<HashMap<PathBuf, BTreeMap<Vec<u8>, OffsetDateTime>>>> =
    OnceLock::new();

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedDaemonCommand {
    pub protocol: String,
    pub node_id: String,
    #[serde(with = "serde_bytes")]
    pub public_key: Vec<u8>,
    pub timestamp: String,
    pub nonce: String,
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "serde_bytes")]
    pub rpc_body_sha256: Option<Vec<u8>>,
    #[serde(with = "serde_bytes")]
    pub signature: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedDaemonCommand {
    pub command: String,
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

pub fn daemon_rpc_command(route: &str) -> String {
    format!("RPC {route}")
}

pub fn sign_daemon_rpc_request_for_local_node(
    request: &sinomo_network::ObjectRpcRequest,
) -> Result<Vec<u8>, DaemonError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    sign_daemon_rpc_request_from_data_dir(&data_dir, request)
}

pub fn sign_daemon_rpc_request_from_data_dir(
    data_dir: &SinomoDataDir,
    request: &sinomo_network::ObjectRpcRequest,
) -> Result<Vec<u8>, DaemonError> {
    sign_daemon_command_from_data_dir_with_rpc_body_sha256(
        data_dir,
        &daemon_rpc_command(&request.route),
        Some(daemon_rpc_body_sha256(request)),
    )
}

pub fn daemon_rpc_body_sha256(request: &sinomo_network::ObjectRpcRequest) -> Vec<u8> {
    digest::digest(&digest::SHA256, &request.body)
        .as_ref()
        .to_vec()
}

fn sign_daemon_command_from_data_dir_with_rpc_body_sha256(
    data_dir: &SinomoDataDir,
    command: &str,
    rpc_body_sha256: Option<Vec<u8>>,
) -> Result<Vec<u8>, DaemonError> {
    let identity = load_or_create_main_node_identity(&data_dir.root())?;
    let node_id = sinomo_network::read_cluster_state(&ClusterPaths::new(data_dir))
        .map(|state| state.local_node_id.to_string())
        .unwrap_or_else(|_| "node_local".to_string());
    let timestamp = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let nonce = format!("{}-{}", std::process::id(), auth_nonce_suffix());
    let key_pair = identity.key_pair()?;
    let public_key = daemon_auth_decode_public_key(&identity.public_key)?;
    let signing_payload = daemon_auth_signing_payload(
        &node_id,
        &public_key,
        &timestamp,
        &nonce,
        command,
        rpc_body_sha256.as_deref(),
    );
    let signature = key_pair.sign(&signing_payload);
    let envelope = SignedDaemonCommand {
        protocol: DAEMON_AUTH_PROTOCOL.to_string(),
        node_id,
        public_key,
        timestamp,
        nonce,
        command: command.to_string(),
        rpc_body_sha256,
        signature: signature.as_ref().to_vec(),
    };
    sinomo_network::SinomoObjectCodec::default()
        .encode(&envelope)
        .map_err(DaemonError::from)
}

pub fn authenticate_daemon_control_request(
    paths: &DaemonPaths,
    request: &sinomo_network::DaemonControlRequest,
) -> Result<AuthenticatedDaemonCommand, DaemonError> {
    authenticate_daemon_control_request_with_rpc_request(paths, request, request.rpc.as_ref())
}

fn authenticate_daemon_control_request_with_rpc_request(
    paths: &DaemonPaths,
    request: &sinomo_network::DaemonControlRequest,
    rpc_request: Option<&sinomo_network::ObjectRpcRequest>,
) -> Result<AuthenticatedDaemonCommand, DaemonError> {
    let auth = request.auth.as_deref().ok_or(DaemonError::AuthDenied)?;
    let envelope: SignedDaemonCommand =
        sinomo_network::SinomoObjectCodec::default().decode(auth)?;
    if envelope.command != request.command {
        return Err(DaemonError::AuthDenied);
    }
    verify_daemon_rpc_body_binding(&envelope, rpc_request)?;
    let signer = verify_signed_daemon_command(paths, &envelope, rpc_request)?;
    remember_daemon_auth_nonce(paths, &envelope)?;
    let signer_public_key = daemon_auth_encode_public_key(&envelope.public_key);
    let authenticated = AuthenticatedDaemonCommand {
        command: envelope.command,
        signer,
        signer_node_id: envelope.node_id,
        signer_public_key,
    };
    authorize_authenticated_daemon_command(&authenticated)?;
    Ok(authenticated)
}

fn verify_daemon_rpc_body_binding(
    envelope: &SignedDaemonCommand,
    rpc_request: Option<&sinomo_network::ObjectRpcRequest>,
) -> Result<(), DaemonError> {
    let is_rpc_command = envelope.command.starts_with("RPC ");
    let Some(expected) = envelope.rpc_body_sha256.as_deref() else {
        return if is_rpc_command {
            Err(DaemonError::AuthDenied)
        } else {
            Ok(())
        };
    };
    let request = rpc_request.ok_or(DaemonError::AuthDenied)?;
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
    let command = authenticated.command.as_str();
    let allowed = match command {
        command if command == daemon_rpc_command(sinomo_network::DAEMON_STATUS_ROUTE) => true,
        command if command == daemon_rpc_command(CLUSTER_KV_SNAPSHOT_ROUTE) => true,
        command if command == daemon_rpc_command(sinomo_network::DAEMON_SHUTDOWN_ROUTE) => {
            authenticated.signer.can_run_local_command()
        }
        command if command == daemon_rpc_command(ROLLOUT_FINALIZE_SHADOW_ROUTE) => {
            authenticated.signer.can_run_local_command()
        }
        command if command == daemon_rpc_command(ROLLOUT_DIAGNOSTICS_ROUTE) => {
            authenticated.signer.can_run_admin_command()
        }
        command if command == daemon_rpc_command(ROLLOUT_CLEAR_STATUS_ROUTE) => {
            authenticated.signer.can_run_admin_command()
        }
        command
            if command == daemon_rpc_command(sinomo_observe::OBSERVE_EVENTS_ROUTE)
                || command == daemon_rpc_command(sinomo_observe::OBSERVE_LOGS_ROUTE) =>
        {
            authenticated.signer.can_run_local_command()
        }
        command if command == daemon_rpc_command(sinomo_dfs::DFS_EXPOSE_CONFIG_ROUTE) => {
            authenticated.signer.can_run_local_command()
        }
        command if command == daemon_rpc_command(sinomo_dfs::DFS_STATUS_ROUTE) => matches!(
            authenticated.signer,
            DaemonSignerAuthority::Local
                | DaemonSignerAuthority::Voter
                | DaemonSignerAuthority::Member
        ),
        command if command == daemon_rpc_command(sinomo_dfs::DFS_INSPECTION_ROUTE) => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        command if command == daemon_rpc_command(sinomo_dfs::DFS_SMOKE_ROUTE) => {
            authenticated.signer.can_run_local_command()
        }
        command if command == daemon_rpc_command(sinomo_dfs::DFS_METADATA_JOURNAL_BATCH_ROUTE) => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        command
            if command == daemon_rpc_command(sinomo_dfs::DFS_METADATA_SNAPSHOT_INSTALL_ROUTE) =>
        {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        command if command == daemon_rpc_command(sinomo_dfs::DFS_PROTOCOL_FRAME_ROUTE) => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        command if is_local_sandbox_rpc_command(command) => {
            authenticated.signer.can_run_local_command()
        }
        command if command == daemon_rpc_command(sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE) => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
                    | DaemonSignerAuthority::JoinCandidate
            )
        }
        command
            if command == daemon_rpc_command(sinomo_network::CLUSTER_INVITE_PUBLISH_ROUTE)
                || command == daemon_rpc_command(sinomo_network::CLUSTER_MEMBER_PUBLISH_ROUTE) =>
        {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        command
            if command == daemon_rpc_command(sinomo_network::CLUSTER_RAFT_APPEND_ROUTE)
                || command == daemon_rpc_command(sinomo_network::CLUSTER_RAFT_VOTE_ROUTE) =>
        {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local | DaemonSignerAuthority::Voter
            )
        }
        command if command == daemon_rpc_command(CLUSTER_KV_RANGE_PREFIX_ROUTE) => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        command if command == daemon_rpc_command(sinomo_network::CLUSTER_HEALTH_ROUTE) => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        command
            if command == daemon_rpc_command(CLUSTER_KV_WATCH_PREFIX_ROUTE)
                || command == daemon_rpc_command(CLUSTER_KV_WATCH_DRAIN_ROUTE)
                || command == daemon_rpc_command(CLUSTER_KV_WATCH_CANCEL_ROUTE)
                || command == daemon_rpc_command(CLUSTER_KV_WATCH_EVENTS_ROUTE) =>
        {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        command
            if command == daemon_rpc_command(ROLLOUT_BINARY_ROUTE)
                || command == daemon_rpc_command(ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE)
                || command == daemon_rpc_command(ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE)
                || command == daemon_rpc_command(ROLLOUT_BUILD_ROUTE) =>
        {
            authenticated.signer.can_run_admin_command()
        }
        command if command == daemon_rpc_command(CLUSTER_KV_TXN_ROUTE) => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        command if command == daemon_rpc_command(sinomo_network::CLUSTER_RELAY_ROUTE) => {
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

fn is_local_sandbox_rpc_command(command: &str) -> bool {
    command == daemon_rpc_command(sinomo_network::SANDBOX_CREATE_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_STATUS_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_UPDATE_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_CHECKPOINT_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_RESUME_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_REBOOT_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_DESTROY_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_LIST_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_READ_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_WRITE_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_HTTP_FETCH_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_HTTP_FETCH_TO_FILE_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_RUN_TOOLCHAIN_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_RUN_PROCESS_PLAN_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_IMPORT_BUSYBOX_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_ROUTE)
        || command
            == daemon_rpc_command(sinomo_network::SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE)
        || command == daemon_rpc_command(sinomo_network::SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE)
}

fn verify_signed_daemon_command(
    paths: &DaemonPaths,
    envelope: &SignedDaemonCommand,
    rpc_request: Option<&sinomo_network::ObjectRpcRequest>,
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
    if envelope.nonce.trim().is_empty() || envelope.command.trim().is_empty() {
        return Err(DaemonError::AuthDenied);
    }
    let signing_payload = daemon_auth_signing_payload(
        &envelope.node_id,
        &envelope.public_key,
        &envelope.timestamp,
        &envelope.nonce,
        &envelope.command,
        envelope.rpc_body_sha256.as_deref(),
    );
    UnparsedPublicKey::new(&ED25519, &envelope.public_key)
        .verify(&signing_payload, &envelope.signature)
        .map_err(|_| DaemonError::AuthDenied)?;
    authorize_daemon_signer(paths, envelope, rpc_request)
}

fn daemon_auth_signing_payload(
    node_id: &str,
    public_key: &[u8],
    timestamp: &str,
    nonce: &str,
    command: &str,
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
    payload.extend_from_slice(command.as_bytes());
    payload.push(b'\n');
    if let Some(rpc_body_sha256) = rpc_body_sha256 {
        payload.extend_from_slice(rpc_body_sha256);
    }
    payload
}

fn authorize_daemon_signer(
    paths: &DaemonPaths,
    envelope: &SignedDaemonCommand,
    rpc_request: Option<&sinomo_network::ObjectRpcRequest>,
) -> Result<DaemonSignerAuthority, DaemonError> {
    let envelope_public_key = daemon_auth_encode_public_key(&envelope.public_key);
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let identity_paths = NodeIdentityPaths::new(data_dir);
    let main_identity = load_existing_node_identity(&identity_paths.main)?;
    let shadow_identity = load_optional_node_identity(&identity_paths.shadow)?;
    if envelope_public_key == main_identity.public_key
        || shadow_identity
            .as_ref()
            .is_some_and(|identity| envelope_public_key == identity.public_key)
    {
        return Ok(DaemonSignerAuthority::Local);
    }

    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    if cluster_paths.state.exists() {
        let state = sinomo_network::read_cluster_state(&cluster_paths)?;
        if let Some(member) = state.members.iter().find(|member| {
            member.node_id.as_str() == envelope.node_id
                && member.node_public_key == envelope_public_key
                && member.state.is_authorized()
        }) {
            return Ok(signer_authority_for_member(member));
        }
        if let Some(member) = state.members.iter().find(|member| {
            member.node_public_key == envelope_public_key && member.state.is_authorized()
        }) {
            return Ok(signer_authority_for_member(member));
        }
    }

    if envelope.command == daemon_rpc_command(sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE) {
        authorize_cluster_join_rpc_signer(paths, rpc_request, &envelope_public_key)?;
        return Ok(DaemonSignerAuthority::JoinCandidate);
    }
    if envelope.command == daemon_rpc_command(sinomo_network::DAEMON_STATUS_ROUTE) {
        return Ok(DaemonSignerAuthority::StatusProbe);
    }
    Err(DaemonError::AuthDenied)
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
    let nonces = guard.entry(paths.root.clone()).or_default();
    // Drop nonces older than the retention window so the set stays bounded; anything
    // that old is already rejected by the clock-skew freshness check.
    nonces.retain(|_, seen_at| {
        (now - *seen_at).whole_seconds().abs() <= DAEMON_AUTH_NONCE_RETENTION_SECONDS
    });
    let nonce_key = daemon_auth_nonce_key(envelope);
    if nonces.contains_key(&nonce_key) {
        return Err(DaemonError::AuthDenied);
    }
    nonces.insert(nonce_key, signed_at);
    Ok(())
}

fn daemon_auth_decode_public_key(public_key: &str) -> Result<Vec<u8>, DaemonError> {
    URL_SAFE_NO_PAD
        .decode(public_key.as_bytes())
        .map_err(|_| DaemonError::AuthDenied)
}

fn daemon_auth_encode_public_key(public_key: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(public_key)
}

fn daemon_auth_nonce_key(envelope: &SignedDaemonCommand) -> Vec<u8> {
    let mut key = Vec::with_capacity(envelope.public_key.len() + 1 + envelope.nonce.len());
    key.extend_from_slice(&envelope.public_key);
    key.push(0);
    key.extend_from_slice(envelope.nonce.as_bytes());
    key
}

fn auth_nonce_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{nanos}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authenticated_command(
        command: String,
        signer: DaemonSignerAuthority,
    ) -> AuthenticatedDaemonCommand {
        AuthenticatedDaemonCommand {
            command,
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
            command: daemon_rpc_command(sinomo_network::DAEMON_STATUS_ROUTE),
            rpc_body_sha256: None,
            signature: b"signature".to_vec(),
        }
    }

    fn unique_auth_test_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ))
    }

    fn unique_auth_test_paths(name: &str) -> DaemonPaths {
        DaemonPaths::with_root(unique_auth_test_root(name))
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
            sinomo_network::DAEMON_STATUS_ROUTE,
            sinomo_network::DAEMON_RPC_CATEGORY,
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
        let paths = unique_auth_test_paths("sinomo-daemon-auth-nonce-mem");
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
        let paths_a = unique_auth_test_paths("sinomo-daemon-auth-nonce-root-a");
        let paths_b = unique_auth_test_paths("sinomo-daemon-auth-nonce-root-b");
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
        let command = daemon_rpc_command(CLUSTER_KV_TXN_ROUTE);
        let authenticated = authenticated_command(command, DaemonSignerAuthority::Member);

        authorize_authenticated_daemon_command(&authenticated)
            .expect("cluster members must be able to forward KV txns to voters");
    }

    #[test]
    fn cluster_health_authorizes_cluster_members_for_routed_inspection() {
        let command = daemon_rpc_command(sinomo_network::CLUSTER_HEALTH_ROUTE);
        for signer in [
            DaemonSignerAuthority::Local,
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
        ] {
            let authenticated = authenticated_command(command.clone(), signer);
            authorize_authenticated_daemon_command(&authenticated)
                .expect("cluster members must be able to read routed health");
        }
        let authenticated = authenticated_command(command, DaemonSignerAuthority::JoinCandidate);
        assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
    }

    #[test]
    fn status_probe_authority_is_limited_to_daemon_status() {
        let status = authenticated_command(
            daemon_rpc_command(sinomo_network::DAEMON_STATUS_ROUTE),
            DaemonSignerAuthority::StatusProbe,
        );
        authorize_authenticated_daemon_command(&status)
            .expect("unknown signed probes may read daemon status");

        let cluster_health = authenticated_command(
            daemon_rpc_command(sinomo_network::CLUSTER_HEALTH_ROUTE),
            DaemonSignerAuthority::StatusProbe,
        );
        assert!(authorize_authenticated_daemon_command(&cluster_health).is_err());
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
            sinomo_network::ClusterId::from_legacy_named("cluster_daemon_auth_same_key_drift")
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
            command: daemon_rpc_command(sinomo_network::CLUSTER_HEALTH_ROUTE),
            rpc_body_sha256: None,
            signature: Vec::new(),
        };

        state.members.push(peer.clone());
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write pending state");
        assert!(matches!(
            authorize_daemon_signer(&daemon_paths, &envelope, None),
            Err(DaemonError::AuthDenied)
        ));

        peer.state = ClusterMemberState::Learner;
        *state.members.last_mut().expect("peer member") = peer;
        sinomo_network::write_cluster_state(&cluster_paths, &state).expect("write accepted state");
        assert_eq!(
            authorize_daemon_signer(&daemon_paths, &envelope, None)
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
            let command = daemon_rpc_command(route);
            for signer in [
                DaemonSignerAuthority::Local,
                DaemonSignerAuthority::Voter,
                DaemonSignerAuthority::Member,
            ] {
                let authenticated = authenticated_command(command.clone(), signer);
                authorize_authenticated_daemon_command(&authenticated)
                    .expect("cluster members must be able to manage cluster KV watches");
            }
            let authenticated =
                authenticated_command(command, DaemonSignerAuthority::JoinCandidate);
            assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
        }
    }

    #[test]
    fn rollout_clear_status_authorizes_admin_signers() {
        let command = daemon_rpc_command(ROLLOUT_CLEAR_STATUS_ROUTE);
        for signer in [DaemonSignerAuthority::Local, DaemonSignerAuthority::Voter] {
            let authenticated = authenticated_command(command.clone(), signer);
            authorize_authenticated_daemon_command(&authenticated)
                .expect("admin signers must be able to clear stale rollout status");
        }
        for signer in [
            DaemonSignerAuthority::Member,
            DaemonSignerAuthority::JoinCandidate,
        ] {
            let authenticated = authenticated_command(command.clone(), signer);
            assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
        }
    }

    #[test]
    fn rollout_upload_routes_authorize_admin_signers() {
        for route in [
            ROLLOUT_BINARY_UPLOAD_CHUNK_ROUTE,
            ROLLOUT_BINARY_UPLOAD_FINALIZE_ROUTE,
        ] {
            let command = daemon_rpc_command(route);
            for signer in [DaemonSignerAuthority::Local, DaemonSignerAuthority::Voter] {
                let authenticated = authenticated_command(command.clone(), signer);
                authorize_authenticated_daemon_command(&authenticated)
                    .expect("rollout upload routes require admin signers");
            }
            for signer in [
                DaemonSignerAuthority::Member,
                DaemonSignerAuthority::JoinCandidate,
            ] {
                let authenticated = authenticated_command(command.clone(), signer);
                assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
            }
        }
    }

    #[test]
    fn dfs_metadata_journal_batch_authorizes_cluster_members() {
        let command = daemon_rpc_command(sinomo_dfs::DFS_METADATA_JOURNAL_BATCH_ROUTE);
        for signer in [
            DaemonSignerAuthority::Local,
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
        ] {
            let authenticated = authenticated_command(command.clone(), signer);
            authorize_authenticated_daemon_command(&authenticated)
                .expect("cluster members must be able to replicate DFS metadata journals");
        }
    }

    #[test]
    fn dfs_metadata_snapshot_install_authorizes_cluster_members() {
        let command = daemon_rpc_command(sinomo_dfs::DFS_METADATA_SNAPSHOT_INSTALL_ROUTE);
        for signer in [
            DaemonSignerAuthority::Local,
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
        ] {
            let authenticated = authenticated_command(command.clone(), signer);
            authorize_authenticated_daemon_command(&authenticated)
                .expect("cluster members must be able to repair DFS metadata replicas");
        }
    }

    #[test]
    fn dfs_status_authorizes_cluster_members() {
        let command = daemon_rpc_command(sinomo_dfs::DFS_STATUS_ROUTE);
        for signer in [
            DaemonSignerAuthority::Local,
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
        ] {
            let authenticated = authenticated_command(command.clone(), signer);
            authorize_authenticated_daemon_command(&authenticated)
                .expect("cluster members must be able to inspect local DFS metadata status");
        }
    }

    #[test]
    fn dfs_inspection_authorizes_cluster_members() {
        let command = daemon_rpc_command(sinomo_dfs::DFS_INSPECTION_ROUTE);
        for signer in [
            DaemonSignerAuthority::Local,
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
        ] {
            let authenticated = authenticated_command(command.clone(), signer);
            authorize_authenticated_daemon_command(&authenticated)
                .expect("cluster members must be able to inspect local DFS metadata");
        }
    }

    #[test]
    fn dfs_smoke_authorizes_only_local_signer() {
        let command = daemon_rpc_command(sinomo_dfs::DFS_SMOKE_ROUTE);
        authorize_authenticated_daemon_command(&authenticated_command(
            command.clone(),
            DaemonSignerAuthority::Local,
        ))
        .expect("local operators must be able to run DFS smoke through the daemon");
        for signer in [
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
            DaemonSignerAuthority::JoinCandidate,
        ] {
            let authenticated = authenticated_command(command.clone(), signer);
            assert!(authorize_authenticated_daemon_command(&authenticated).is_err());
        }
    }

    #[test]
    fn dfs_protocol_frame_authorizes_cluster_members() {
        let command = daemon_rpc_command(sinomo_dfs::DFS_PROTOCOL_FRAME_ROUTE);
        for signer in [
            DaemonSignerAuthority::Local,
            DaemonSignerAuthority::Voter,
            DaemonSignerAuthority::Member,
        ] {
            let authenticated = authenticated_command(command.clone(), signer);
            authorize_authenticated_daemon_command(&authenticated)
                .expect("cluster members must be able to exchange DFS protocol frames");
        }
    }
}
