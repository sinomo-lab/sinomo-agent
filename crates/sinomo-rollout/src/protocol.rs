use crate::{RolloutError, source::LocalRolloutNativeDylibModuleArtifact};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use sinomo_core::{ClusterBootstrapState, ClusterEndpointPrivacy};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutNativeDylibModuleArtifact {
    pub module_id: String,
    pub interface_version: String,
    pub cache_key: String,
    pub library_name: String,
    pub source: String,
    #[serde(with = "serde_bytes")]
    pub library: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutNativeDylibModuleRequest {
    pub source: String,
    pub host: String,
    pub port: u16,
    pub shadow_port: u16,
    pub requested_at: String,
    pub artifact: ClusterRolloutNativeDylibModuleArtifact,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutNativeDylibModuleResponse {
    pub status: String,
    pub detail: String,
    pub module_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    pub interface_version: String,
    pub cache_key: String,
    pub content_hash: String,
    pub cache_path: PathBuf,
    pub state_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutUploadArtifactSpec {
    pub artifact_id: String,
    pub total_bytes: u64,
    #[serde(with = "serde_bytes")]
    pub blake3: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutNativeDylibModuleUploadSpec {
    pub module_id: String,
    pub interface_version: String,
    pub cache_key: String,
    pub library_name: String,
    pub source: String,
    pub artifact: ClusterRolloutUploadArtifactSpec,
}

/// Small object-RPC head for a streamed cluster-binary rollout.
///
/// Artifact bytes are never embedded in this object. Each artifact is sent as
/// one body attachment on [`crate::ROLLOUT_BINARY_ROUTE`], so
/// `sinomo-network` owns transport chunking, progress, and retries.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutBinaryManifest {
    pub upload_id: String,
    pub source: String,
    #[serde(default)]
    pub target_os: String,
    #[serde(default)]
    pub target_arch: String,
    pub host: String,
    pub port: u16,
    pub shadow_port: u16,
    pub requested_at: String,
    pub binary: ClusterRolloutUploadArtifactSpec,
    #[serde(default)]
    pub native_dylib_modules: Vec<ClusterRolloutNativeDylibModuleUploadSpec>,
}

impl ClusterRolloutBinaryManifest {
    pub fn artifact(&self, artifact_id: &str) -> Option<&ClusterRolloutUploadArtifactSpec> {
        if self.binary.artifact_id == artifact_id {
            return Some(&self.binary);
        }
        self.native_dylib_modules
            .iter()
            .map(|module| &module.artifact)
            .find(|artifact| artifact.artifact_id == artifact_id)
    }

    pub fn artifacts(&self) -> impl Iterator<Item = &ClusterRolloutUploadArtifactSpec> {
        std::iter::once(&self.binary).chain(
            self.native_dylib_modules
                .iter()
                .map(|module| &module.artifact),
        )
    }

    pub fn total_bytes(&self) -> Option<u64> {
        self.artifacts().try_fold(0_u64, |total, artifact| {
            total.checked_add(artifact.total_bytes)
        })
    }
}

/// One whole-artifact transfer on [`crate::ROLLOUT_BINARY_ROUTE`].
///
/// `activate` is set only on the last semantic artifact. It replaces the old
/// protocol-level chunk/finalize routes without making callers shard files.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutBinaryRequest {
    pub manifest: ClusterRolloutBinaryManifest,
    pub artifact_id: String,
    #[serde(default)]
    pub activate: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRolloutBuildRequest {
    pub source: String,
    #[serde(default)]
    pub remote_repo: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "serde_bytes")]
    pub source_bundle: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_head: Option<String>,
    #[serde(default)]
    pub force_source_overwrite: bool,
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

pub fn prepare_cluster_rollout_binary_manifest(
    source: impl Into<String>,
    binary: &Path,
    native_dylib_modules: &[LocalRolloutNativeDylibModuleArtifact],
    host: impl Into<String>,
    port: u16,
    shadow_port: u16,
    requested_at: impl Into<String>,
) -> Result<ClusterRolloutBinaryManifest, RolloutError> {
    let binary = rollout_upload_artifact_spec("binary", binary)?;
    let native_dylib_modules = native_dylib_modules
        .iter()
        .enumerate()
        .map(|(index, artifact)| {
            Ok::<_, RolloutError>(ClusterRolloutNativeDylibModuleUploadSpec {
                module_id: artifact.module_id.clone(),
                interface_version: artifact.interface_version.clone(),
                cache_key: artifact.cache_key.clone(),
                library_name: artifact.library_name.clone(),
                source: artifact.source.clone(),
                artifact: rollout_upload_artifact_spec(
                    &format!("module-{index}"),
                    &artifact.built_library,
                )?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ClusterRolloutBinaryManifest {
        upload_id: format!(
            "rollout-{}-{}",
            std::process::id(),
            time::OffsetDateTime::now_utc().unix_timestamp_nanos()
        ),
        source: source.into(),
        target_os: std::env::consts::OS.to_string(),
        target_arch: std::env::consts::ARCH.to_string(),
        host: host.into(),
        port,
        shadow_port,
        requested_at: requested_at.into(),
        binary,
        native_dylib_modules,
    })
}

fn rollout_upload_artifact_spec(
    artifact_id: &str,
    path: &Path,
) -> Result<ClusterRolloutUploadArtifactSpec, RolloutError> {
    let total_bytes = std::fs::metadata(path)
        .map_err(|source| RolloutError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?
        .len();
    let mut file = std::fs::File::open(path).map_err(|source| RolloutError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read =
            std::io::Read::read(&mut file, &mut buffer).map_err(|source| RolloutError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(ClusterRolloutUploadArtifactSpec {
        artifact_id: artifact_id.to_string(),
        total_bytes,
        blake3: hasher.finalize().as_bytes().to_vec(),
    })
}

fn cluster_native_dylib_module_artifact_from_local(
    artifact: &LocalRolloutNativeDylibModuleArtifact,
) -> Result<ClusterRolloutNativeDylibModuleArtifact, RolloutError> {
    let library = std::fs::read(&artifact.built_library).map_err(|source| RolloutError::Io {
        path: artifact.built_library.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(ClusterRolloutNativeDylibModuleArtifact {
        module_id: artifact.module_id.clone(),
        interface_version: artifact.interface_version.clone(),
        cache_key: artifact.cache_key.clone(),
        library_name: artifact.library_name.clone(),
        source: artifact.source.clone(),
        library,
    })
}

pub fn prepare_cluster_rollout_native_dylib_module_request(
    source: impl Into<String>,
    artifact: &LocalRolloutNativeDylibModuleArtifact,
    host: impl Into<String>,
    port: u16,
    shadow_port: u16,
    requested_at: impl Into<String>,
) -> Result<ClusterRolloutNativeDylibModuleRequest, RolloutError> {
    Ok(ClusterRolloutNativeDylibModuleRequest {
        source: source.into(),
        host: host.into(),
        port,
        shadow_port,
        requested_at: requested_at.into(),
        artifact: cluster_native_dylib_module_artifact_from_local(artifact)?,
    })
}

pub fn prepare_cluster_rollout_build_request(
    source: impl Into<String>,
    remote_repo: Option<PathBuf>,
    force_source_overwrite: bool,
    host: impl Into<String>,
    port: u16,
    shadow_port: u16,
    requested_at: impl Into<String>,
) -> ClusterRolloutBuildRequest {
    ClusterRolloutBuildRequest {
        source: source.into(),
        remote_repo,
        source_bundle: None,
        source_head: None,
        force_source_overwrite,
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

#[cfg(test)]
fn cluster_rollout_targets_from_state(state: &ClusterBootstrapState) -> Vec<String> {
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

#[cfg(test)]
fn prepare_rollout_cluster_targets(
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
    use sinomo_core::node::NodeId;
    use sinomo_core::{
        CapabilityName, ClusterEndpointRecord, ClusterMemberRecord, ClusterTransportKind, Id,
    };
    use time::OffsetDateTime;

    type ClusterId = Id;

    #[test]
    fn prepare_cluster_rollout_binary_manifest_describes_binary_without_embedding_it() {
        let root = temp_root("cluster-binary-request");
        let binary = root.join("sinomo");
        std::fs::write(&binary, b"binary-bytes").expect("binary");

        let manifest = prepare_cluster_rollout_binary_manifest(
            "source",
            &binary,
            &[],
            "127.0.0.1",
            21353,
            21354,
            "now",
        )
        .expect("request");

        assert_eq!(manifest.source, "source");
        assert_eq!(manifest.binary.artifact_id, "binary");
        assert_eq!(manifest.binary.total_bytes, 12);
        assert_eq!(manifest.binary.blake3.len(), 32);
        assert_eq!(manifest.host, "127.0.0.1");
        assert_eq!(manifest.port, 21353);
        assert_eq!(manifest.shadow_port, 21354);
        assert!(!manifest.target_os.is_empty());
        assert!(!manifest.target_arch.is_empty());
        assert_eq!(manifest.total_bytes(), Some(12));
    }

    #[test]
    fn prepare_cluster_rollout_binary_manifest_describes_native_module_artifacts() {
        let root = temp_root("cluster-binary-module-request");
        let binary = root.join("sinomo");
        let module = root.join("libmodule.so");
        std::fs::write(&binary, b"binary-bytes").expect("binary");
        std::fs::write(&module, b"module-bytes").expect("module");

        let manifest = prepare_cluster_rollout_binary_manifest(
            "source",
            &binary,
            &[LocalRolloutNativeDylibModuleArtifact {
                module_id: "module.test".to_string(),
                interface_version: "test.v1".to_string(),
                cache_key: "module/test".to_string(),
                library_name: "libmodule.so".to_string(),
                built_library: module,
                source: "test".to_string(),
            }],
            "127.0.0.1",
            21353,
            21354,
            "now",
        )
        .expect("request");

        assert_eq!(manifest.native_dylib_modules.len(), 1);
        let module = &manifest.native_dylib_modules[0];
        assert_eq!(module.module_id, "module.test");
        assert_eq!(module.interface_version, "test.v1");
        assert_eq!(module.cache_key, "module/test");
        assert_eq!(module.library_name, "libmodule.so");
        assert_eq!(module.source, "test");
        assert_eq!(module.artifact.artifact_id, "module-0");
        assert_eq!(module.artifact.total_bytes, 12);
        assert_eq!(module.artifact.blake3.len(), 32);
        assert_eq!(manifest.total_bytes(), Some(24));
    }

    #[test]
    fn prepare_cluster_rollout_native_dylib_module_request_reads_module_artifact() {
        let root = temp_root("cluster-module-request");
        let module = root.join("libmodule.so");
        std::fs::write(&module, b"module-bytes").expect("module");

        let request = prepare_cluster_rollout_native_dylib_module_request(
            "source",
            &LocalRolloutNativeDylibModuleArtifact {
                module_id: "module.test".to_string(),
                interface_version: "test.v1".to_string(),
                cache_key: "module/test".to_string(),
                library_name: "libmodule.so".to_string(),
                built_library: module,
                source: "test".to_string(),
            },
            "127.0.0.1",
            21353,
            21354,
            "now",
        )
        .expect("request");

        assert_eq!(request.source, "source");
        assert_eq!(request.host, "127.0.0.1");
        assert_eq!(request.port, 21353);
        assert_eq!(request.shadow_port, 21354);
        assert_eq!(request.artifact.module_id, "module.test");
        assert_eq!(request.artifact.library, b"module-bytes".to_vec());
    }

    #[test]
    fn prepare_cluster_rollout_build_request_sets_remote_build_fields() {
        let request = prepare_cluster_rollout_build_request(
            "source",
            Some(PathBuf::from("/repo")),
            true,
            "127.0.0.1",
            21353,
            21354,
            "now",
        );

        assert_eq!(request.source, "source");
        assert_eq!(request.remote_repo, Some(PathBuf::from("/repo")));
        assert_eq!(request.source_bundle, None);
        assert_eq!(request.source_head, None);
        assert!(request.force_source_overwrite);
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

    fn temp_root(name: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(&format!("sinomo-rollout-{name}"))
    }
}
