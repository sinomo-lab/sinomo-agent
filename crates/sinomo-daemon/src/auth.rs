use crate::{DaemonError, DaemonPaths};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::signature::{ED25519, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use sinomo_core::ClusterMemberRole;
use sinomo_local::SinomoDataDir;
use sinomo_network::ClusterPaths;
use sinomo_node::{NodeIdentity, NodeIdentityPaths, load_or_create_main_node_identity};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const DAEMON_AUTH_PROTOCOL: &str = "sinomo.daemon-auth.v1";
pub const DAEMON_AUTH_MAX_CLOCK_SKEW_SECONDS: i64 = 300;
pub const DAEMON_AUTH_NONCE_RETENTION_SECONDS: i64 = DAEMON_AUTH_MAX_CLOCK_SKEW_SECONDS * 2;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedDaemonCommand {
    pub protocol: String,
    pub node_id: String,
    pub public_key: String,
    pub timestamp: String,
    pub nonce: String,
    pub command: String,
    pub signature: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedDaemonCommand {
    pub command: String,
    pub signer: DaemonSignerAuthority,
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

pub fn sign_daemon_command_for_local_node(command: &str) -> Result<String, DaemonError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    sign_daemon_command_from_data_dir(&data_dir, command)
}

pub fn sign_daemon_command_from_data_dir(
    data_dir: &SinomoDataDir,
    command: &str,
) -> Result<String, DaemonError> {
    let identity = load_or_create_main_node_identity(&data_dir.root())?;
    let node_id = sinomo_network::read_cluster_state(&ClusterPaths::new(data_dir))
        .map(|state| state.local_node_id.to_string())
        .unwrap_or_else(|_| "node_local".to_string());
    let timestamp = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let nonce = format!("{}-{}", std::process::id(), auth_nonce_suffix());
    let key_pair = identity.key_pair()?;
    let signing_payload =
        daemon_auth_signing_payload(&node_id, &identity.public_key, &timestamp, &nonce, command);
    let signature = key_pair.sign(signing_payload.as_bytes());
    let envelope = SignedDaemonCommand {
        protocol: DAEMON_AUTH_PROTOCOL.to_string(),
        node_id,
        public_key: identity.public_key,
        timestamp,
        nonce,
        command: command.to_string(),
        signature: URL_SAFE_NO_PAD.encode(signature.as_ref()),
    };
    let payload = serde_json::to_string(&envelope).map_err(|source| DaemonError::Json {
        path: "daemon auth envelope".to_string(),
        detail: source.to_string(),
    })?;
    Ok(format!("AUTH {payload}"))
}

pub fn authenticate_daemon_command(
    paths: &DaemonPaths,
    raw: &str,
) -> Result<AuthenticatedDaemonCommand, DaemonError> {
    let payload = raw
        .strip_prefix("AUTH ")
        .ok_or_else(|| DaemonError::AuthDenied)?;
    let envelope: SignedDaemonCommand =
        serde_json::from_str(payload).map_err(|source| DaemonError::Json {
            path: "daemon auth envelope".to_string(),
            detail: source.to_string(),
        })?;
    let signer = verify_signed_daemon_command(paths, &envelope)?;
    remember_daemon_auth_nonce(paths, &envelope)?;
    let authenticated = AuthenticatedDaemonCommand {
        command: envelope.command,
        signer,
    };
    authorize_authenticated_daemon_command(&authenticated)?;
    Ok(authenticated)
}

pub fn authorize_authenticated_daemon_command(
    authenticated: &AuthenticatedDaemonCommand,
) -> Result<(), DaemonError> {
    let command = authenticated.command.as_str();
    let allowed = match command {
        "STATUS" | "CLUSTER_KV_SNAPSHOT" => true,
        "SHUTDOWN" | "FINALIZE_SHADOW_ROLLOUT" => authenticated.signer.can_run_local_command(),
        "ROLLOUT_DIAGNOSTICS" => authenticated.signer.can_run_admin_command(),
        command if command.starts_with("CLUSTER_JOIN ") => {
            matches!(
                authenticated.signer,
                DaemonSignerAuthority::Local
                    | DaemonSignerAuthority::Voter
                    | DaemonSignerAuthority::Member
                    | DaemonSignerAuthority::JoinCandidate
            )
        }
        command if command.starts_with("CLUSTER_SYNC ") => matches!(
            authenticated.signer,
            DaemonSignerAuthority::Local
                | DaemonSignerAuthority::Voter
                | DaemonSignerAuthority::Member
        ),
        command if command.starts_with("CLUSTER_KV_RANGE_PREFIX ") => matches!(
            authenticated.signer,
            DaemonSignerAuthority::Local
                | DaemonSignerAuthority::Voter
                | DaemonSignerAuthority::Member
        ),
        command
            if command.starts_with("CLUSTER_KV_TXN ")
                || command.starts_with("ROLLOUT_BINARY ")
                || command.starts_with("ROLLOUT_BUILD ") =>
        {
            authenticated.signer.can_run_admin_command()
        }
        _ => true,
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
    );
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(signing_payload.as_bytes(), &signature)
        .map_err(|_| DaemonError::AuthDenied)?;
    authorize_daemon_signer(paths, envelope)
}

fn daemon_auth_signing_payload(
    node_id: &str,
    public_key: &str,
    timestamp: &str,
    nonce: &str,
    command: &str,
) -> String {
    format!("{DAEMON_AUTH_PROTOCOL}\n{node_id}\n{public_key}\n{timestamp}\n{nonce}\n{command}")
}

fn authorize_daemon_signer(
    paths: &DaemonPaths,
    envelope: &SignedDaemonCommand,
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
            return Ok(if member.role == ClusterMemberRole::Voter {
                DaemonSignerAuthority::Voter
            } else {
                DaemonSignerAuthority::Member
            });
        }
    }

    if envelope.command.starts_with("CLUSTER_JOIN ") {
        authorize_cluster_join_signer(&envelope.command, &envelope.public_key)?;
        return Ok(DaemonSignerAuthority::JoinCandidate);
    }
    Err(DaemonError::AuthDenied)
}

fn authorize_cluster_join_signer(command: &str, public_key: &str) -> Result<(), DaemonError> {
    let payload = command
        .strip_prefix("CLUSTER_JOIN ")
        .unwrap_or_default()
        .trim();
    let request: sinomo_network::ClusterJoinRegistrationRequest = serde_json::from_str(payload)
        .map_err(|source| DaemonError::Json {
            path: "cluster join registration".to_string(),
            detail: source.to_string(),
        })?;
    if request
        .members
        .iter()
        .any(|member| member.node_public_key == public_key)
    {
        return Ok(());
    }
    Err(DaemonError::AuthDenied)
}

fn load_existing_node_identity(path: &Path) -> Result<NodeIdentity, DaemonError> {
    let content = fs::read_to_string(path).map_err(|source| DaemonError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| DaemonError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
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
    let nonce_path = paths.root.join("auth-nonces.json");
    let mut nonces = match fs::read_to_string(&nonce_path) {
        Ok(content) => {
            serde_json::from_str::<BTreeMap<String, String>>(&content).unwrap_or_default()
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => BTreeMap::new(),
        Err(source) => {
            return Err(DaemonError::Io {
                path: nonce_path.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    let now = OffsetDateTime::now_utc();
    nonces.retain(|_, timestamp| {
        OffsetDateTime::parse(timestamp, &Rfc3339)
            .map(|seen_at| {
                (now - seen_at).whole_seconds().abs() <= DAEMON_AUTH_NONCE_RETENTION_SECONDS
            })
            .unwrap_or(false)
    });
    let nonce_key = daemon_auth_nonce_key(envelope);
    if nonces.contains_key(&nonce_key) {
        return Err(DaemonError::AuthDenied);
    }
    nonces.insert(nonce_key, envelope.timestamp.clone());
    write_json(&nonce_path, &nonces)
}

fn daemon_auth_nonce_key(envelope: &SignedDaemonCommand) -> String {
    format!("{}:{}", envelope.public_key, envelope.nonce)
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), DaemonError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| DaemonError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let content = serde_json::to_string_pretty(value).map_err(|source| DaemonError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
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
