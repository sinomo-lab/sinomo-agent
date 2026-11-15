//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;
use serde::Deserialize;
use serde::Serialize;
use sinomo_core::DfsObjectPolicy;
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;

/// Exact prior activation retained while a new generation is pending.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsMetadataStorageActivation {
    pub generation: String,
    pub target_format: u32,
    pub catalog_generation: u64,
    pub activation_digest: String,
}
impl DfsMetadataStorageActivation {
    pub fn validate(&self) -> Result<(), String> {
        DfsMetadataStorageState::Active {
            generation: self.generation.clone(), target_format: self.target_format,
            catalog_generation: self.catalog_generation, activation_digest: self.activation_digest.clone(),
        }.validate()
    }
}

/// Generic control-plane generation transition. Conversion interpretation and
/// scheduling belong to the migration owner; these opaque identities do not
/// certify local installation or grant metadata publication authority.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DfsMetadataStorageState {
    PendingTransition {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_activation: Option<DfsMetadataStorageActivation>,
        transition_id: String,
        source_catalog_generation: u64,
        target_generation: String,
        target_format: u32,
        expected_catalog_digest: String,
    },
    Active {
        generation: String,
        target_format: u32,
        catalog_generation: u64,
        activation_digest: String,
    },
}
impl DfsMetadataStorageState {
    pub fn validate(&self) -> Result<(), String> {
        let identity = |value: &str| !value.is_empty() && value.len() <= 1024 && !value.contains('\0');
        let digest = |value: &str| value.strip_prefix("blake3:").is_some_and(|hash|
            hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
        let valid = match self {
            Self::PendingTransition { source_activation, source_catalog_generation, transition_id, target_generation, target_format, expected_catalog_digest, .. } =>
                identity(transition_id) && identity(target_generation) && *target_format > 0 && digest(expected_catalog_digest)
                && source_activation.as_ref().is_none_or(|prior| prior.validate().is_ok() && prior.catalog_generation <= *source_catalog_generation && prior.generation != *target_generation),
            Self::Active { generation, target_format, activation_digest, .. } =>
                identity(generation) && *target_format > 0 && digest(activation_digest),
        };
        if valid { Ok(()) } else { Err("invalid metadata storage generation configuration".into()) }
    }
    pub fn active_descriptor(&self) -> Option<DfsMetadataStorageActivation> {
        match self {
            Self::Active { generation, target_format, catalog_generation, activation_digest } =>
                Some(DfsMetadataStorageActivation { generation: generation.clone(), target_format: *target_format,
                    catalog_generation: *catalog_generation, activation_digest: activation_digest.clone() }),
            Self::PendingTransition { .. } => None,
        }
    }
    pub fn validate_at_catalog_generation(&self, current: u64) -> Result<(), String> {
        self.validate()?;
        let bound = match self {
            Self::PendingTransition { source_catalog_generation, .. } => *source_catalog_generation,
            Self::Active { catalog_generation, .. } => *catalog_generation,
        };
        if bound <= current { Ok(()) } else { Err("metadata storage generation names a future catalog".into()) }
    }
}

/// A DFS namespace and its default ownership metadata.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(try_from = "DfsFileSystemWire")]
pub struct DfsFileSystem {
    /// Monotonic generation of this filesystem's complete metadata-shard catalog.
    #[serde(default, skip_serializing_if = "is_zero_catalog_generation")]
    pub catalog_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_storage: Option<DfsMetadataStorageState>,
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
    pub retention_policies: std::collections::BTreeMap<String, DfsRetentionPolicy>,
    pub module_metadata: std::collections::BTreeMap<String, DfsModuleMetadataValue>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DfsFileSystemWire {
    /// Monotonic generation of this filesystem's complete metadata-shard catalog.
    #[serde(default, skip_serializing_if = "is_zero_catalog_generation")]
    pub catalog_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_storage: Option<DfsMetadataStorageState>,
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
    pub retention_policies: std::collections::BTreeMap<String, DfsRetentionPolicy>,
    pub module_metadata: std::collections::BTreeMap<String, DfsModuleMetadataValue>,
}

impl TryFrom<DfsFileSystemWire> for DfsFileSystem {
    type Error = String;
    fn try_from(wire: DfsFileSystemWire) -> Result<Self, Self::Error> {
        let policies = wire.retention_policies;
        let module_metadata = wire.module_metadata;
        if let Some(activation) = &wire.metadata_storage { activation.validate_at_catalog_generation(wire.catalog_generation)?; }
        validate_retention_policies(&policies)?;
        validate_module_metadata_map(&module_metadata)?;
        Ok(Self {
            catalog_generation: wire.catalog_generation,
            metadata_storage: wire.metadata_storage,
            file_system_id: wire.file_system_id,
            display_name: wire.display_name,
            owner_node_id: wire.owner_node_id,
            subtree_policies: wire.subtree_policies,
            retention_policies: policies,
            module_metadata,
        })
    }
}

/// Policy override that applies to objects below a path prefix.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsSubtreePolicy {
    pub path_prefix: String,
    pub policy: DfsObjectPolicy,
    /// Whether the override affects only `path_prefix` or is inherited by all
    /// descendants.
    pub scope: DfsPlacementScope,
    /// Explicit commit and placement replica counts.
    pub min_commit_replicas: usize,
    pub desired_replicas: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_node_ids: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forbidden_node_ids: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub min_distinct_zones: usize,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPlacementScope {
    SelfOnly,
    #[default]
    Subtree,
}

impl DfsSubtreePolicy {
    pub fn new(
        path_prefix: impl Into<String>,
        policy: DfsObjectPolicy,
        min_commit_replicas: usize,
        desired_replicas: usize,
    ) -> Self {
        Self {
            path_prefix: path_prefix.into(),
            policy,
            scope: DfsPlacementScope::Subtree,
            min_commit_replicas,
            desired_replicas,
            required_node_ids: Vec::new(),
            forbidden_node_ids: Vec::new(),
            min_distinct_zones: 0,
        }
    }
}

impl DfsFileSystem {
    pub fn sifs(owner_node_id: NodeId) -> Self {
        Self {
            catalog_generation: 0,
            metadata_storage: None,
            file_system_id: sifs_file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id,
            subtree_policies: Vec::new(),
            retention_policies: Default::default(),
            module_metadata: Default::default(),
        }
    }
}

/// Current metadata for one DFS object in a filesystem.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsObjectMetadata {
    pub object_id: DfsObjectId,
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub kind: DfsObjectKind,
    /// Authoritative number of namespace links to this canonical object.
    /// Directories include their two implicit links and immediate subdirectories.
    /// Absent on legacy records and hardlink markers; absence is not a count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace_link_count: Option<u64>,
    /// Storage-level provenance for a tombstone that only evicts a physical
    /// source-shard copy after a cross-shard move. `None` means an authoritative
    /// namespace deletion.
    ///
    /// This distinction is part of durable metadata, rather than an inference
    /// from timestamps or paths: a source eviction is intentionally newer than
    /// the source live record, but it must never delete the live object already
    /// installed on `target_shard_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tombstone_provenance: Option<DfsTombstoneProvenance>,
    pub owner_node_id: NodeId,
    pub current_locations: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_target_object_id: Option<DfsObjectId>,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub accessed_at_unix_nanos: u64,
    pub modified_at_unix_nanos: u64,
    pub created_at_unix_nanos: u64,
    pub changed_at_unix_nanos: u64,
    pub version: DfsVersion,
    pub policy: DfsObjectPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified_by_event_id: Option<EventId>,
}

/// Logical kind of a DFS object.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsObjectKind {
    File,
    Directory,
    Symlink,
    Hardlink,
    Artifact,
    Mount,
    Tombstone,
}

impl DfsObjectKind {
    /// Initial count for a newly created namespace object, never a legacy default.
    pub const fn initial_namespace_link_count(&self) -> Option<u64> {
        match self {
            Self::Directory => Some(2),
            Self::Hardlink => None,
            Self::Tombstone => Some(0),
            _ => Some(1),
        }
    }
}

/// Why a durable DFS tombstone exists.
///
/// Ordinary namespace deletes use `None` on
/// [`DfsObjectMetadata::tombstone_provenance`]. Only physical source eviction
/// needs an explicit marker because it is not authoritative deletion evidence.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsTombstoneProvenance {
    SourceEviction {
        source_shard_id: crate::dfs::DfsMetadataShardId,
        target_shard_id: crate::dfs::DfsMetadataShardId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pin_transfer_id: Option<String>,
    },
}

fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}

#[cfg(test)]
mod metadata_storage_tests {
    use super::*;

    #[test]
    fn storage_generation_roundtrips_preserve_catalog_and_reject_old_or_invalid_records() {
        let mut filesystem = DfsFileSystem::sifs(NodeId::generate());
        let empty = serde_json::to_value(&filesystem).unwrap();
        assert!(empty.get("metadata_storage").is_none());
        assert!(empty.get("catalog_generation").is_none());
        filesystem.catalog_generation = 7;
        filesystem.metadata_storage = Some(DfsMetadataStorageState::Active {
            generation: "native-generation".into(), target_format: 12, catalog_generation: 7,
            activation_digest: format!("blake3:{}", "a".repeat(64)),
        });
        let config = DfsFileSystemConfig::from(&filesystem);
        assert_eq!(DfsFileSystem::from(config.clone()), filesystem);
        let value = serde_json::to_value(&config).unwrap();
        assert_eq!(serde_json::from_value::<DfsFileSystemConfig>(value.clone()).unwrap(), config);
        let mut invalid = value.clone();
        invalid["metadata_storage"]["target_format"] = serde_json::json!(0);
        assert!(serde_json::from_value::<DfsFileSystemConfig>(invalid).is_err());
        let mut future = value.clone();
        future["metadata_storage"]["catalog_generation"] = serde_json::json!(8);
        assert!(serde_json::from_value::<DfsFileSystemConfig>(future).is_err());
        let mut old = value;
        old["namespace_count_activation"] = serde_json::json!({"schema_version":1});
        assert!(serde_json::from_value::<DfsFileSystemConfig>(old).is_err(),
            "old activation interpretation belongs to migration, not current runtime decoding");
    }
}

fn is_zero_catalog_generation(value: &u64) -> bool { *value == 0 }
