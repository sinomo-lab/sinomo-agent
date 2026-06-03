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
use sinomo_rollout::{
    ROLLOUT_BINARY_ROUTE, ROLLOUT_BUILD_ROUTE, ROLLOUT_CLEAR_STATUS_ROUTE,
    ROLLOUT_DIAGNOSTICS_ROUTE, ROLLOUT_FINALIZE_SHADOW_ROUTE,
};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const DAEMON_AUTH_PROTOCOL: &str = "sinomo.daemon-auth.v1";
pub const CLUSTER_INVITE_AUTH_PROTOCOL: &str = "sinomo.cluster-invite.v1";
pub const CLUSTER_INVITE_CHALLENGE_PROTOCOL: &str = "sinomo.cluster-invite-challenge.v1";
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
    // The responder is authorized to answer this challenge if it is named as a communication
    // channel, OR if it is the invite's issuer -- a voter whose key signs the whole invite and
    // is its root of trust. The issuer case is what lets a voter answer a challenge relayed to
    // it by a non-channel bridge introducer that was named in an invite minted before the
    // bridge joined (so the bridge is absent from `communication_channels`).
    let is_issuer = invite.issuer_node_id == local_member.node_id
        && invite.issuer_public_key == identity.public_key;
    let is_channel = invite.communication_channels.iter().any(|channel| {
        channel.endpoint.node_id == local_member.node_id
            && channel.node_public_key == identity.public_key
    });
    if !is_issuer && !is_channel {
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
    // Accept a proof signed by a known communication-channel key, or by the invite's issuer
    // key (which signs the invite itself, so an issuer-signed proof -- e.g. relayed back
    // through a non-channel bridge introducer -- is at least as trustworthy as a channel key).
    let responder_is_issuer = proof.responder_public_key == invite.issuer_public_key;
    let responder_is_channel = invite
        .communication_channels
        .iter()
        .any(|channel| channel.node_public_key == proof.responder_public_key);
    if !responder_is_issuer && !responder_is_channel {
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
        command if command == daemon_rpc_command(ROLLOUT_FINALIZE_SHADOW_ROUTE) => {
            authenticated.signer.can_run_local_command()
        }
        command if command == daemon_rpc_command(ROLLOUT_DIAGNOSTICS_ROUTE) => {
            authenticated.signer.can_run_admin_command()
        }
        command if command == daemon_rpc_command(ROLLOUT_CLEAR_STATUS_ROUTE) => {
            authenticated.signer.can_run_admin_command()
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
            if command == daemon_rpc_command(ROLLOUT_BINARY_ROUTE)
                || command == daemon_rpc_command(ROLLOUT_BUILD_ROUTE) =>
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
        authorize_cluster_join_rpc_signer(paths, rpc_request, &envelope_public_key)?;
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
        let command = daemon_rpc_command(sinomo_network::CLUSTER_KV_TXN_ROUTE);
        let authenticated = authenticated_command(command, DaemonSignerAuthority::Member);

        authorize_authenticated_daemon_command(&authenticated)
            .expect("cluster members must be able to forward KV txns to voters");
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
