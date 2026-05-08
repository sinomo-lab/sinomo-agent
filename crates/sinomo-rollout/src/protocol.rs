use crate::RolloutError;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sinomo_core::{ClusterBootstrapState, ClusterEndpointPrivacy};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutBinaryRequest {
    pub source: String,
    pub binary_base64: String,
    #[serde(default)]
    pub target_os: String,
    #[serde(default)]
    pub target_arch: String,
    pub host: String,
    pub port: u16,
    pub shadow_port: u16,
    pub requested_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutBuildRequest {
    pub source: String,
    #[serde(default)]
    pub remote_repo: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_bundle_base64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_head: Option<String>,
    pub host: String,
    pub port: u16,
    pub shadow_port: u16,
    pub requested_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutBinaryResponse {
    pub status: String,
    pub detail: String,
    pub main_binary: PathBuf,
    pub shadow_binary: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_head: Option<String>,
}

pub fn prepare_cluster_rollout_binary_request(
    source: impl Into<String>,
    binary: &Path,
    host: impl Into<String>,
    port: u16,
    shadow_port: u16,
    requested_at: impl Into<String>,
) -> Result<ClusterRolloutBinaryRequest, RolloutError> {
    let binary_bytes = std::fs::read(binary).map_err(|source| RolloutError::Io {
        path: binary.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(ClusterRolloutBinaryRequest {
        source: source.into(),
        binary_base64: URL_SAFE_NO_PAD.encode(binary_bytes),
        target_os: std::env::consts::OS.to_string(),
        target_arch: std::env::consts::ARCH.to_string(),
        host: host.into(),
        port,
        shadow_port,
        requested_at: requested_at.into(),
    })
}

pub fn prepare_cluster_rollout_build_request(
    source: impl Into<String>,
    remote_repo: Option<PathBuf>,
    host: impl Into<String>,
    port: u16,
    shadow_port: u16,
    requested_at: impl Into<String>,
) -> ClusterRolloutBuildRequest {
    ClusterRolloutBuildRequest {
        source: source.into(),
        remote_repo,
        source_bundle_base64: None,
        source_head: None,
        host: host.into(),
        port,
        shadow_port,
        requested_at: requested_at.into(),
    }
}

pub fn unique_rollout_targets(targets: Vec<String>) -> Vec<String> {
    let mut unique = Vec::new();
    for target in targets {
        if !unique.contains(&target) {
            unique.push(target);
        }
    }
    unique
}

pub fn cluster_rollout_targets_from_state(state: &ClusterBootstrapState) -> Vec<String> {
    let mut targets = Vec::new();
    for member in &state.members {
        if member.node_id == state.local_node_id {
            continue;
        }
        let is_main_daemon = member
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == "daemon.main.run");
        if !is_main_daemon {
            continue;
        }
        for endpoint in &member.endpoints {
            if matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly) {
                continue;
            }
            targets.push(endpoint.address.clone());
        }
    }
    unique_rollout_targets(targets)
}

pub fn prepare_rollout_cluster_targets(
    explicit_targets: Vec<String>,
    local_target: Option<String>,
    cluster_state: Option<&ClusterBootstrapState>,
) -> Vec<String> {
    if !explicit_targets.is_empty() {
        return unique_rollout_targets(explicit_targets);
    }

    let mut targets = Vec::new();
    if let Some(local_target) = local_target {
        targets.push(local_target);
    }
    if let Some(state) = cluster_state {
        targets.extend(cluster_rollout_targets_from_state(state));
    }
    unique_rollout_targets(targets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        CapabilityName, ClusterEndpointRecord, ClusterId, ClusterMemberRecord,
        ClusterTransportKind, NodeId,
    };
    use std::time::{SystemTime, UNIX_EPOCH};
    use time::OffsetDateTime;

    #[test]
    fn prepare_cluster_rollout_binary_request_reads_and_encodes_binary() {
        let root = temp_root("cluster-binary-request");
        std::fs::create_dir_all(&root).expect("root");
        let binary = root.join("sinomo");
        std::fs::write(&binary, b"binary-bytes").expect("binary");

        let request = prepare_cluster_rollout_binary_request(
            "source",
            &binary,
            "127.0.0.1",
            21353,
            21354,
            "now",
        )
        .expect("request");

        assert_eq!(request.source, "source");
        assert_eq!(
            request.binary_base64,
            URL_SAFE_NO_PAD.encode(b"binary-bytes")
        );
        assert_eq!(request.host, "127.0.0.1");
        assert_eq!(request.port, 21353);
        assert_eq!(request.shadow_port, 21354);
        assert!(!request.target_os.is_empty());
        assert!(!request.target_arch.is_empty());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_cluster_rollout_build_request_sets_remote_build_fields() {
        let request = prepare_cluster_rollout_build_request(
            "source",
            Some(PathBuf::from("/repo")),
            "127.0.0.1",
            21353,
            21354,
            "now",
        );

        assert_eq!(request.source, "source");
        assert_eq!(request.remote_repo, Some(PathBuf::from("/repo")));
        assert_eq!(request.source_bundle_base64, None);
        assert_eq!(request.source_head, None);
        assert_eq!(request.host, "127.0.0.1");
        assert_eq!(request.port, 21353);
        assert_eq!(request.shadow_port, 21354);
        assert_eq!(request.requested_at, "now");
    }

    #[test]
    fn unique_rollout_targets_preserves_first_seen_order() {
        assert_eq!(
            unique_rollout_targets(vec![
                "127.0.0.1:21353".to_string(),
                "192.168.1.10:21353".to_string(),
                "127.0.0.1:21353".to_string(),
            ]),
            vec![
                "127.0.0.1:21353".to_string(),
                "192.168.1.10:21353".to_string(),
            ]
        );
    }

    #[test]
    fn cluster_rollout_targets_from_state_uses_remote_main_daemon_public_endpoints() {
        let timestamp = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("rollout_targets_cluster").expect("cluster");
        let local_node = NodeId::from_legacy_named("rollout_targets_local").expect("local");
        let remote_node = NodeId::from_legacy_named("rollout_targets_remote").expect("remote");
        let shadow_node = NodeId::from_legacy_named("rollout_targets_shadow").expect("shadow");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "rollout targets",
            local_node.clone(),
            "local",
            "local-public-key",
            timestamp,
        );
        let mut remote = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            remote_node.clone(),
            "remote",
            "remote-public-key",
            timestamp,
        );
        remote
            .capabilities
            .push(CapabilityName::new("daemon.main.run").expect("capability"));
        remote.endpoints.push(ClusterEndpointRecord::loopback(
            remote_node.clone(),
            "127.0.0.1:21353",
        ));
        let mut lan_endpoint =
            ClusterEndpointRecord::loopback(remote_node.clone(), "192.168.1.10:21353");
        lan_endpoint.transport = ClusterTransportKind::LanTcp;
        lan_endpoint.privacy = ClusterEndpointPrivacy::Lan;
        remote.endpoints.push(lan_endpoint.clone());
        remote.endpoints.push(lan_endpoint);
        let mut shadow = ClusterMemberRecord::local_voter(
            cluster_id,
            shadow_node.clone(),
            "shadow",
            "shadow-public-key",
            timestamp,
        );
        shadow.endpoints.push(ClusterEndpointRecord::loopback(
            shadow_node,
            "192.168.1.11:21353",
        ));
        state.members.push(remote);
        state.members.push(shadow);

        assert_eq!(
            cluster_rollout_targets_from_state(&state),
            vec!["192.168.1.10:21353".to_string()]
        );
    }

    #[test]
    fn prepare_rollout_cluster_targets_prefers_explicit_targets() {
        let timestamp = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("explicit_rollout_targets_cluster").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("explicit_rollout_targets_local").expect("local");
        let remote_node =
            NodeId::from_legacy_named("explicit_rollout_targets_remote").expect("remote");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "rollout targets",
            local_node,
            "local",
            "local-public-key",
            timestamp,
        );
        let mut remote = ClusterMemberRecord::local_voter(
            cluster_id,
            remote_node.clone(),
            "remote",
            "remote-public-key",
            timestamp,
        );
        remote
            .capabilities
            .push(CapabilityName::new("daemon.main.run").expect("capability"));
        let mut endpoint = ClusterEndpointRecord::loopback(remote_node, "192.168.1.10:21353");
        endpoint.privacy = ClusterEndpointPrivacy::Lan;
        state.members.push(remote);
        state.members.last_mut().unwrap().endpoints.push(endpoint);

        assert_eq!(
            prepare_rollout_cluster_targets(
                vec!["10.0.0.2:21353".to_string(), "10.0.0.2:21353".to_string(),],
                Some("127.0.0.1:21353".to_string()),
                Some(&state),
            ),
            vec!["10.0.0.2:21353".to_string()]
        );
    }

    #[test]
    fn prepare_rollout_cluster_targets_combines_local_and_cluster_targets() {
        let timestamp = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("combined_rollout_targets_cluster").expect("cluster");
        let local_node =
            NodeId::from_legacy_named("combined_rollout_targets_local").expect("local");
        let remote_node =
            NodeId::from_legacy_named("combined_rollout_targets_remote").expect("remote");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "rollout targets",
            local_node,
            "local",
            "local-public-key",
            timestamp,
        );
        let mut remote = ClusterMemberRecord::local_voter(
            cluster_id,
            remote_node.clone(),
            "remote",
            "remote-public-key",
            timestamp,
        );
        remote
            .capabilities
            .push(CapabilityName::new("daemon.main.run").expect("capability"));
        let mut endpoint = ClusterEndpointRecord::loopback(remote_node, "192.168.1.10:21353");
        endpoint.transport = ClusterTransportKind::LanTcp;
        endpoint.privacy = ClusterEndpointPrivacy::Lan;
        remote.endpoints.push(endpoint);
        state.members.push(remote);

        assert_eq!(
            prepare_rollout_cluster_targets(
                Vec::new(),
                Some("127.0.0.1:21353".to_string()),
                Some(&state),
            ),
            vec![
                "127.0.0.1:21353".to_string(),
                "192.168.1.10:21353".to_string(),
            ]
        );
    }

    fn temp_root(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("sinomo-rollout-{name}-{stamp}"))
    }
}
