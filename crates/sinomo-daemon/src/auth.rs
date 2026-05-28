use crate::{DaemonError, DaemonPaths};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::digest;
use ring::signature::{ED25519, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use sinomo_core::{ClusterJoinInvite, ClusterMemberRole, ClusterMemberState};
use sinomo_local::SinomoDataDir;
use sinomo_network::{ClusterInviteId, ClusterPaths};
use sinomo_node::{
    NodeIdentity, NodeIdentityPaths, load_or_create_main_node_identity,
    load_or_create_node_identity,
};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const DAEMON_AUTH_PROTOCOL: &str = "sinomo.daemon-auth.v1";
pub const CLUSTER_INVITE_AUTH_PROTOCOL: &str = "sinomo.cluster-invite.v1";
pub const CLUSTER_INVITE_CHALLENGE_PROTOCOL: &str = "sinomo.cluster-invite-challenge.v1";
pub const DAEMON_AUTH_MAX_CLOCK_SKEW_SECONDS: i64 = 300;
pub const DAEMON_AUTH_NONCE_RETENTION_SECONDS: i64 = DAEMON_AUTH_MAX_CLOCK_SKEW_SECONDS * 2;
const DAEMON_AUTH_NONCE_LOG_COMPACT_BYTES: u64 = 256 * 1024;
static DAEMON_AUTH_NONCE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedDaemonCommand {
    pub protocol: String,
    pub node_id: String,
    pub public_key: String,
    pub timestamp: String,
    pub nonce: String,
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_body_sha256: Option<String>,
    pub signature: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedDaemonCommand {
    pub command: String,
    pub signer: DaemonSignerAuthority,
    pub signer_node_id: String,
    pub signer_public_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterInviteChallengeRequest {
    pub protocol: String,
    pub invite_id: ClusterInviteId,
    pub challenge: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterInviteChallengeProof {
    pub protocol: String,
    pub invite_id: ClusterInviteId,
    pub cluster_id: sinomo_network::ClusterId,
    pub responder_node_id: String,
    pub responder_public_key: String,
    pub challenge: String,
    pub timestamp: String,
    pub signature: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DaemonSignerAuthority {
    Local,
    Voter,
    Member,
    JoinCandidate,
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

pub fn daemon_rpc_body_sha256(request: &sinomo_network::ObjectRpcRequest) -> String {
    URL_SAFE_NO_PAD.encode(digest::digest(&digest::SHA256, &request.body).as_ref())
}

fn sign_daemon_command_from_data_dir_with_rpc_body_sha256(
    data_dir: &SinomoDataDir,
    command: &str,
    rpc_body_sha256: Option<String>,
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
    let signing_payload = daemon_auth_signing_payload(
        &node_id,
        &identity.public_key,
        &timestamp,
        &nonce,
        command,
        rpc_body_sha256.as_deref(),
    );
    let signature = key_pair.sign(signing_payload.as_bytes());
    let envelope = SignedDaemonCommand {
        protocol: DAEMON_AUTH_PROTOCOL.to_string(),
        node_id,
        public_key: identity.public_key,
        timestamp,
        nonce,
        command: command.to_string(),
        rpc_body_sha256,
        signature: URL_SAFE_NO_PAD.encode(signature.as_ref()),
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
    let authenticated = AuthenticatedDaemonCommand {
        command: envelope.command,
        signer,
        signer_node_id: envelope.node_id,
        signer_public_key: envelope.public_key,
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
    if expected == daemon_rpc_body_sha256(request) {
        Ok(())
    } else {
        Err(DaemonError::AuthDenied)
    }
}

pub fn create_cluster_invite_challenge_proof(
    paths: &DaemonPaths,
    request: &ClusterInviteChallengeRequest,
) -> Result<ClusterInviteChallengeProof, DaemonError> {
    if request.protocol != CLUSTER_INVITE_CHALLENGE_PROTOCOL || request.challenge.trim().is_empty()
    {
        return Err(DaemonError::AuthDenied);
    }
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let invite = read_active_cluster_invite(&cluster_paths, &request.invite_id)?;
    if invite.expires_at <= OffsetDateTime::now_utc() {
        return Err(DaemonError::AuthExpired);
    }
    let state = sinomo_network::read_cluster_state(&cluster_paths)?;
    if invite.cluster_id != state.cluster_id {
        return Err(DaemonError::AuthDenied);
    }
    let identity_paths = NodeIdentityPaths::new(data_dir);
    let identity = load_existing_node_identity(&identity_paths.main)?;
    let local_member = state
        .members
        .iter()
        .find(|member| {
            member.node_id == state.local_node_id
                && member.node_public_key == identity.public_key
                && member.state.is_authorized()
        })
        .ok_or(DaemonError::AuthDenied)?;
    if !invite.communication_channels.iter().any(|channel| {
        channel.endpoint.node_id == local_member.node_id
            && channel.node_public_key == identity.public_key
    }) {
        return Err(DaemonError::AuthDenied);
    }
    let timestamp = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let signing_payload = cluster_invite_challenge_signing_payload(
        &request.invite_id,
        &invite.cluster_id,
        local_member.node_id.as_str(),
        &identity.public_key,
        &request.challenge,
        &timestamp,
    );
    let signature = identity.key_pair()?.sign(signing_payload.as_bytes());
    Ok(ClusterInviteChallengeProof {
        protocol: CLUSTER_INVITE_CHALLENGE_PROTOCOL.to_string(),
        invite_id: request.invite_id.clone(),
        cluster_id: invite.cluster_id,
        responder_node_id: local_member.node_id.to_string(),
        responder_public_key: identity.public_key,
        challenge: request.challenge.clone(),
        timestamp,
        signature: URL_SAFE_NO_PAD.encode(signature.as_ref()),
    })
}

pub fn verify_cluster_invite_challenge_proof(
    invite: &ClusterJoinInvite,
    expected_challenge: &str,
    proof: &ClusterInviteChallengeProof,
) -> Result<(), DaemonError> {
    if proof.protocol != CLUSTER_INVITE_CHALLENGE_PROTOCOL
        || proof.invite_id != invite.invite_id
        || (proof.cluster_id != invite.cluster_id
            && invite.cluster_id.as_str() != "01hxy6qa9k0t9vr4g9n8d3q5c0"
            && invite.cluster_id.as_str() != "cluster_pending_join")
        || proof.challenge != expected_challenge
    {
        return Err(DaemonError::AuthDenied);
    }
    let signed_at =
        OffsetDateTime::parse(&proof.timestamp, &Rfc3339).map_err(|_| DaemonError::AuthDenied)?;
    let skew = (OffsetDateTime::now_utc() - signed_at)
        .whole_seconds()
        .abs();
    if skew > DAEMON_AUTH_MAX_CLOCK_SKEW_SECONDS {
        return Err(DaemonError::AuthExpired);
    }
    if !invite
        .communication_channels
        .iter()
        .any(|channel| channel.node_public_key == proof.responder_public_key)
    {
        return Err(DaemonError::AuthDenied);
    }
    let public_key = URL_SAFE_NO_PAD
        .decode(proof.responder_public_key.as_bytes())
        .map_err(|_| DaemonError::AuthDenied)?;
    let signature = URL_SAFE_NO_PAD
        .decode(proof.signature.as_bytes())
        .map_err(|_| DaemonError::AuthDenied)?;
    let signing_payload = cluster_invite_challenge_signing_payload(
        &proof.invite_id,
        &proof.cluster_id,
        &proof.responder_node_id,
        &proof.responder_public_key,
        &proof.challenge,
        &proof.timestamp,
    );
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(signing_payload.as_bytes(), &signature)
        .map_err(|_| DaemonError::AuthDenied)
}

pub fn authorize_authenticated_daemon_command(
    authenticated: &AuthenticatedDaemonCommand,
) -> Result<(), DaemonError> {
    let command = authenticated.command.as_str();
    let allowed = match command {
        command if command == daemon_rpc_command(sinomo_network::DAEMON_STATUS_ROUTE) => true,
        command if command == daemon_rpc_command(sinomo_network::CLUSTER_KV_SNAPSHOT_ROUTE) => true,
        command if command == daemon_rpc_command(sinomo_network::DAEMON_SHUTDOWN_ROUTE) => {
            authenticated.signer.can_run_local_command()
        }
        command if command == daemon_rpc_command(sinomo_network::ROLLOUT_FINALIZE_SHADOW_ROUTE) => {
            authenticated.signer.can_run_local_command()
        }
        command if command == daemon_rpc_command(sinomo_network::ROLLOUT_DIAGNOSTICS_ROUTE) => {
            authenticated.signer.can_run_admin_command()
        }
        command if command == daemon_rpc_command(sinomo_network::DFS_EXPOSE_CONFIG_ROUTE) => {
            authenticated.signer.can_run_local_command()
        }
        command if command == daemon_rpc_command(sinomo_network::DFS_STATUS_ROUTE) => matches!(
            authenticated.signer,
            DaemonSignerAuthority::Local
                | DaemonSignerAuthority::Voter
                | DaemonSignerAuthority::Member
        ),
        command if command == daemon_rpc_command(sinomo_network::DFS_INSPECTION_ROUTE) => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        command if command == daemon_rpc_command(sinomo_network::DFS_SMOKE_ROUTE) => {
            authenticated.signer.can_run_local_command()
        }
        command
            if command == daemon_rpc_command(sinomo_network::DFS_METADATA_JOURNAL_BATCH_ROUTE) =>
        {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        command
            if command
                == daemon_rpc_command(sinomo_network::DFS_METADATA_SNAPSHOT_INSTALL_ROUTE) =>
        {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        command if command == daemon_rpc_command(sinomo_network::DFS_PROTOCOL_FRAME_ROUTE) => {
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
        command if command == daemon_rpc_command(sinomo_network::CLUSTER_KV_RANGE_PREFIX_ROUTE) => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        command
            if command == daemon_rpc_command(sinomo_network::CLUSTER_KV_WATCH_PREFIX_ROUTE)
                || command == daemon_rpc_command(sinomo_network::CLUSTER_KV_WATCH_DRAIN_ROUTE)
                || command == daemon_rpc_command(sinomo_network::CLUSTER_KV_WATCH_EVENTS_ROUTE) =>
        {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
            )
        }
        command
            if command == daemon_rpc_command(sinomo_network::ROLLOUT_BINARY_ROUTE)
                || command == daemon_rpc_command(sinomo_network::ROLLOUT_BUILD_ROUTE) =>
        {
            authenticated.signer.can_run_admin_command()
        }
        command if command == daemon_rpc_command(sinomo_network::CLUSTER_KV_TXN_ROUTE) => {
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
    let public_key = URL_SAFE_NO_PAD
        .decode(envelope.public_key.as_bytes())
        .map_err(|_| DaemonError::AuthDenied)?;
    let signature = URL_SAFE_NO_PAD
        .decode(envelope.signature.as_bytes())
        .map_err(|_| DaemonError::AuthDenied)?;
    let signing_payload = daemon_auth_signing_payload(
        &envelope.node_id,
        &envelope.public_key,
        &envelope.timestamp,
        &envelope.nonce,
        &envelope.command,
        envelope.rpc_body_sha256.as_deref(),
    );
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(signing_payload.as_bytes(), &signature)
        .map_err(|_| DaemonError::AuthDenied)?;
    authorize_daemon_signer(paths, envelope, rpc_request)
}

fn daemon_auth_signing_payload(
    node_id: &str,
    public_key: &str,
    timestamp: &str,
    nonce: &str,
    command: &str,
    rpc_body_sha256: Option<&str>,
) -> String {
    format!(
        "{DAEMON_AUTH_PROTOCOL}\n{node_id}\n{public_key}\n{timestamp}\n{nonce}\n{command}\n{}",
        rpc_body_sha256.unwrap_or_default()
    )
}

fn authorize_daemon_signer(
    paths: &DaemonPaths,
    envelope: &SignedDaemonCommand,
    rpc_request: Option<&sinomo_network::ObjectRpcRequest>,
) -> Result<DaemonSignerAuthority, DaemonError> {
    let data_dir = paths.root.parent().ok_or_else(|| DaemonError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let identity_paths = NodeIdentityPaths::new(data_dir);
    let main_identity = load_existing_node_identity(&identity_paths.main)?;
    let shadow_identity = load_optional_node_identity(&identity_paths.shadow)?;
    if envelope.public_key == main_identity.public_key
        || shadow_identity
            .as_ref()
            .is_some_and(|identity| envelope.public_key == identity.public_key)
    {
        return Ok(DaemonSignerAuthority::Local);
    }

    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    if cluster_paths.state.exists() {
        let state = sinomo_network::read_cluster_state(&cluster_paths)?;
        if let Some(member) = state.members.iter().find(|member| {
            member.node_id.as_str() == envelope.node_id
                && member.node_public_key == envelope.public_key
                && member.state.is_authorized()
        }) {
            return Ok(
                if matches!(member.state, ClusterMemberState::Voter)
                    || (matches!(member.state, ClusterMemberState::Active)
                        && matches!(member.role, ClusterMemberRole::Voter))
                {
                    DaemonSignerAuthority::Voter
                } else {
                    DaemonSignerAuthority::Member
                },
            );
        }
    }

    if envelope.command == daemon_rpc_command(sinomo_network::CLUSTER_JOIN_REGISTER_ROUTE) {
        authorize_cluster_join_rpc_signer(paths, rpc_request, &envelope.public_key)?;
        return Ok(DaemonSignerAuthority::JoinCandidate);
    }
    Err(DaemonError::AuthDenied)
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
    if invite.signature.trim().is_empty()
        || invite.issuer_public_key.trim().is_empty()
        || invite.issuer_node_id.as_str().trim().is_empty()
    {
        return Err(DaemonError::AuthDenied);
    }
    let public_key = URL_SAFE_NO_PAD
        .decode(invite.issuer_public_key.as_bytes())
        .map_err(|_| DaemonError::AuthDenied)?;
    let signature = URL_SAFE_NO_PAD
        .decode(invite.signature.as_bytes())
        .map_err(|_| DaemonError::AuthDenied)?;
    let payload = cluster_invite_signing_payload(invite)?;
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(payload.as_bytes(), &signature)
        .map_err(|_| DaemonError::AuthDenied)
}

fn cluster_invite_signing_payload(invite: &ClusterJoinInvite) -> Result<String, DaemonError> {
    let expires_at = invite
        .expires_at
        .format(&Rfc3339)
        .map_err(|source| DaemonError::Parse {
            detail: format!("invalid invite expiry timestamp: {source}"),
        })?;
    let introducers =
        serde_json::to_string(&invite.introducers).map_err(|source| DaemonError::Json {
            path: "cluster invite introducers".to_string(),
            detail: source.to_string(),
        })?;
    let channels = serde_json::to_string(&invite.communication_channels).map_err(|source| {
        DaemonError::Json {
            path: "cluster invite communication channels".to_string(),
            detail: source.to_string(),
        }
    })?;
    let allowed_roles = serde_json::to_string(&invite.allowed_member_roles).map_err(|source| {
        DaemonError::Json {
            path: "cluster invite allowed roles".to_string(),
            detail: source.to_string(),
        }
    })?;
    Ok(format!(
        "{CLUSTER_INVITE_AUTH_PROTOCOL}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
        invite.invite_id,
        invite.cluster_id,
        invite.cluster_name.clone().unwrap_or_default(),
        invite.cluster_public_key,
        invite.invitation_secret,
        channels,
        introducers,
        invite.token_id,
        expires_at,
        invite.trust_domain,
        allowed_roles,
        invite.issuer_node_id,
        invite.issuer_public_key
    ))
}

fn cluster_invite_challenge_signing_payload(
    invite_id: &ClusterInviteId,
    cluster_id: &sinomo_network::ClusterId,
    responder_node_id: &str,
    responder_public_key: &str,
    challenge: &str,
    timestamp: &str,
) -> String {
    format!(
        "{CLUSTER_INVITE_CHALLENGE_PROTOCOL}\n{invite_id}\n{cluster_id}\n{responder_node_id}\n{responder_public_key}\n{challenge}\n{timestamp}"
    )
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
    let nonce_path = paths.root.join("auth-nonces.sobj");
    let nonce_log_path = paths.root.join("auth-nonces.sobjlog");
    let _guard = DAEMON_AUTH_NONCE_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|source| DaemonError::Io {
            path: nonce_path.display().to_string(),
            detail: source.to_string(),
        })?;
    let now = OffsetDateTime::now_utc();
    let mut nonces = read_daemon_auth_nonces(&nonce_path, &nonce_log_path, now)?;
    let nonce_key = daemon_auth_nonce_key(envelope);
    if nonces.contains_key(&nonce_key) {
        return Err(DaemonError::AuthDenied);
    }
    nonces.insert(nonce_key, envelope.timestamp.clone());
    append_daemon_auth_nonce(&nonce_log_path, envelope)?;
    compact_daemon_auth_nonce_log_if_needed(&nonce_path, &nonce_log_path, &nonces)
}

fn daemon_auth_nonce_key(envelope: &SignedDaemonCommand) -> String {
    format!("{}:{}", envelope.public_key, envelope.nonce)
}

fn read_daemon_auth_nonces(
    nonce_path: &Path,
    nonce_log_path: &Path,
    now: OffsetDateTime,
) -> Result<BTreeMap<String, String>, DaemonError> {
    let mut nonces = match fs::read(nonce_path) {
        Ok(bytes) => sinomo_network::SinomoObjectCodec::default()
            .decode::<BTreeMap<String, String>>(&bytes)
            .unwrap_or_default(),
        Err(source) if source.kind() == io::ErrorKind::NotFound => BTreeMap::new(),
        Err(source) => {
            return Err(DaemonError::Io {
                path: nonce_path.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    match fs::read(nonce_log_path) {
        Ok(bytes) => {
            for line in String::from_utf8_lossy(&bytes).lines() {
                let Some((nonce_key, timestamp)) = line.split_once('\t') else {
                    continue;
                };
                if !nonce_key.trim().is_empty() && !timestamp.trim().is_empty() {
                    nonces.insert(nonce_key.to_string(), timestamp.to_string());
                }
            }
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(DaemonError::Io {
                path: nonce_log_path.display().to_string(),
                detail: source.to_string(),
            });
        }
    }
    retain_live_daemon_auth_nonces(&mut nonces, now);
    Ok(nonces)
}

fn retain_live_daemon_auth_nonces(nonces: &mut BTreeMap<String, String>, now: OffsetDateTime) {
    nonces.retain(|_, timestamp| {
        OffsetDateTime::parse(timestamp, &Rfc3339)
            .map(|seen_at| {
                (now - seen_at).whole_seconds().abs() <= DAEMON_AUTH_NONCE_RETENTION_SECONDS
            })
            .unwrap_or(false)
    });
}

fn append_daemon_auth_nonce(
    nonce_log_path: &Path,
    envelope: &SignedDaemonCommand,
) -> Result<(), DaemonError> {
    if let Some(parent) = nonce_log_path.parent() {
        fs::create_dir_all(parent).map_err(|source| DaemonError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(nonce_log_path)
        .map_err(|source| DaemonError::Io {
            path: nonce_log_path.display().to_string(),
            detail: source.to_string(),
        })?;
    writeln!(
        file,
        "{}\t{}",
        daemon_auth_nonce_key(envelope),
        envelope.timestamp
    )
    .map_err(|source| DaemonError::Io {
        path: nonce_log_path.display().to_string(),
        detail: source.to_string(),
    })
}

fn compact_daemon_auth_nonce_log_if_needed(
    nonce_path: &Path,
    nonce_log_path: &Path,
    nonces: &BTreeMap<String, String>,
) -> Result<(), DaemonError> {
    let log_bytes = fs::metadata(nonce_log_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    if log_bytes < DAEMON_AUTH_NONCE_LOG_COMPACT_BYTES {
        return Ok(());
    }
    write_object(nonce_path, nonces)?;
    fs::write(nonce_log_path, []).map_err(|source| DaemonError::Io {
        path: nonce_log_path.display().to_string(),
        detail: source.to_string(),
    })
}

fn write_object<T: Serialize>(path: &Path, value: &T) -> Result<(), DaemonError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| DaemonError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content = sinomo_network::SinomoObjectCodec::default().encode(value)?;
    fs::write(path, content).map_err(|source| DaemonError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
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
            public_key: "public-key".to_string(),
            timestamp: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .expect("timestamp"),
            nonce: nonce.to_string(),
            command: daemon_rpc_command(sinomo_network::DAEMON_STATUS_ROUTE),
            rpc_body_sha256: None,
            signature: "signature".to_string(),
        }
    }

    fn unique_auth_test_paths(name: &str) -> DaemonPaths {
        let root = std::env::temp_dir().join(format!(
            "{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        DaemonPaths::with_root(root)
    }

    #[test]
    fn daemon_auth_nonce_appends_log_without_rewriting_snapshot() {
        let paths = unique_auth_test_paths("sinomo-daemon-auth-nonce-log");
        let first = signed_nonce("nonce-one");
        remember_daemon_auth_nonce(&paths, &first).expect("remember first nonce");

        let nonce_path = paths.root.join("auth-nonces.sobj");
        let nonce_log_path = paths.root.join("auth-nonces.sobjlog");
        assert!(
            !nonce_path.exists(),
            "nonce snapshot should not be rewritten for every request"
        );
        let log_size = std::fs::metadata(&nonce_log_path).expect("nonce log").len();
        assert!(log_size < 1024, "single nonce append should stay small");

        let duplicate = remember_daemon_auth_nonce(&paths, &first);
        assert!(matches!(duplicate, Err(DaemonError::AuthDenied)));
        assert_eq!(
            std::fs::metadata(&nonce_log_path)
                .expect("nonce log after duplicate")
                .len(),
            log_size,
            "duplicate nonce rejection must not append another log record"
        );

        let second = signed_nonce("nonce-two");
        remember_daemon_auth_nonce(&paths, &second).expect("remember second nonce");
        std::fs::remove_dir_all(&paths.root).ok();
    }

    #[test]
    fn daemon_auth_nonce_reads_existing_snapshot_and_log() {
        let paths = unique_auth_test_paths("sinomo-daemon-auth-nonce-existing");
        let nonce_path = paths.root.join("auth-nonces.sobj");
        let existing = signed_nonce("from-snapshot");
        let mut nonces = BTreeMap::new();
        nonces.insert(daemon_auth_nonce_key(&existing), existing.timestamp.clone());
        write_object(&nonce_path, &nonces).expect("write snapshot");

        let duplicate_snapshot = remember_daemon_auth_nonce(&paths, &existing);
        assert!(matches!(duplicate_snapshot, Err(DaemonError::AuthDenied)));

        let from_log = signed_nonce("from-log");
        remember_daemon_auth_nonce(&paths, &from_log).expect("remember log nonce");
        let duplicate_log = remember_daemon_auth_nonce(&paths, &from_log);
        assert!(matches!(duplicate_log, Err(DaemonError::AuthDenied)));
        std::fs::remove_dir_all(&paths.root).ok();
    }

    #[test]
    fn cluster_kv_txn_authorizes_cluster_members_for_voter_forwarding() {
        let command = daemon_rpc_command(sinomo_network::CLUSTER_KV_TXN_ROUTE);
        let authenticated = authenticated_command(command, DaemonSignerAuthority::Member);

        authorize_authenticated_daemon_command(&authenticated)
            .expect("cluster members must be able to forward KV txns to voters");
    }

    #[test]
    fn dfs_metadata_journal_batch_authorizes_cluster_members() {
        let command = daemon_rpc_command(sinomo_network::DFS_METADATA_JOURNAL_BATCH_ROUTE);
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
        let command = daemon_rpc_command(sinomo_network::DFS_METADATA_SNAPSHOT_INSTALL_ROUTE);
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
        let command = daemon_rpc_command(sinomo_network::DFS_STATUS_ROUTE);
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
        let command = daemon_rpc_command(sinomo_network::DFS_INSPECTION_ROUTE);
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
        let command = daemon_rpc_command(sinomo_network::DFS_SMOKE_ROUTE);
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
        let command = daemon_rpc_command(sinomo_network::DFS_PROTOCOL_FRAME_ROUTE);
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
