//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;
use serde::Deserialize;
use serde::Serialize;
use sinomo_core::DfsObjectPolicy;
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;

/// A DFS namespace and its default ownership metadata.
#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "DfsFileSystemWire")]
pub struct DfsFileSystem {
    #[serde(skip)]
    pub metadata_format: DfsRevisionMetadataFormat,
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub retention_policies: std::collections::BTreeMap<String, DfsRetentionPolicy>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub module_metadata: std::collections::BTreeMap<String, DfsModuleMetadataValue>,
}

#[derive(Serialize)]
struct DfsFileSystemRef<'a> {
    file_system_id: &'a DfsFileSystemId,
    display_name: &'a String,
    owner_node_id: &'a NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    subtree_policies: &'a Vec<DfsSubtreePolicy>,
    retention_policies: &'a std::collections::BTreeMap<String, DfsRetentionPolicy>,
    module_metadata: &'a std::collections::BTreeMap<String, DfsModuleMetadataValue>,
}

impl Serialize for DfsFileSystem {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.metadata_format == DfsRevisionMetadataFormat::Legacy {
            return crate::dfs::file_history::serialize_legacy_file_system(
                &self.file_system_id,
                &self.display_name,
                &self.owner_node_id,
                &self.subtree_policies,
                &self.retention_policies,
                &self.module_metadata,
                serializer,
            );
        }
        DfsFileSystemRef {
            file_system_id: &self.file_system_id,
            display_name: &self.display_name,
            owner_node_id: &self.owner_node_id,
            subtree_policies: &self.subtree_policies,
            retention_policies: &self.retention_policies,
            module_metadata: &self.module_metadata,
        }
        .serialize(serializer)
    }
}

impl PartialEq for DfsFileSystem {
    fn eq(&self, other: &Self) -> bool {
        self.file_system_id == other.file_system_id
            && self.display_name == other.display_name
            && self.owner_node_id == other.owner_node_id
            && self.subtree_policies == other.subtree_policies
            && self.retention_policies == other.retention_policies
            && self.module_metadata == other.module_metadata
    }
}
impl Eq for DfsFileSystem {}

#[derive(Deserialize)]
struct DfsFileSystemWire {
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
    #[serde(
        default,
        deserialize_with = "crate::dfs::metadata_records::deserialize_present_value"
    )]
    pub retention_policies: Option<std::collections::BTreeMap<String, DfsRetentionPolicy>>,
    #[serde(
        default,
        deserialize_with = "crate::dfs::metadata_records::deserialize_present_value"
    )]
    pub module_metadata: Option<std::collections::BTreeMap<String, DfsModuleMetadataValue>>,
    #[serde(default)]
    file_history: DfsFileHistoryPolicy,
}

impl TryFrom<DfsFileSystemWire> for DfsFileSystem {
    type Error = String;
    fn try_from(wire: DfsFileSystemWire) -> Result<Self, Self::Error> {
        let metadata_format = if wire.retention_policies.is_some() || wire.module_metadata.is_some()
        {
            DfsRevisionMetadataFormat::NamespacedV6
        } else {
            DfsRevisionMetadataFormat::Legacy
        };
        let mut policies = wire.retention_policies.unwrap_or_default();
        let module_metadata = wire.module_metadata.unwrap_or_default();
        crate::dfs::file_history::migrate_legacy_retention_policy(
            &mut policies,
            wire.file_history,
        )?;
        validate_retention_policies(&policies)?;
        validate_module_metadata_map(&module_metadata)?;
        Ok(Self {
            metadata_format,
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
    /// descendants. Older records default to subtree behavior.
    #[serde(default, skip_serializing_if = "DfsPlacementScope::is_subtree")]
    pub scope: DfsPlacementScope,
    /// Explicit W and N. Absent values retain the legacy policy defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_commit_replicas: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired_replicas: Option<usize>,
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

impl DfsPlacementScope {
    fn is_subtree(value: &Self) -> bool {
        matches!(value, Self::Subtree)
    }
}

impl DfsSubtreePolicy {
    pub fn legacy(path_prefix: impl Into<String>, policy: DfsObjectPolicy) -> Self {
        Self {
            path_prefix: path_prefix.into(),
            policy,
            scope: DfsPlacementScope::Subtree,
            min_commit_replicas: None,
            desired_replicas: None,
            required_node_ids: Vec::new(),
            forbidden_node_ids: Vec::new(),
            min_distinct_zones: 0,
        }
    }
}

impl DfsFileSystem {
    pub fn sifs(owner_node_id: NodeId) -> Self {
        Self {
            metadata_format: DfsRevisionMetadataFormat::NamespacedV6,
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
pub struct DfsObjectMetadata {
    pub object_id: DfsObjectId,
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub kind: DfsObjectKind,
    /// Storage-level provenance for a tombstone that only evicts a physical
    /// source-shard copy after a cross-shard move. `None` is the backwards
    /// compatible representation of an authoritative namespace deletion.
    ///
    /// This distinction is part of durable metadata, rather than an inference
    /// from timestamps or paths: a source eviction is intentionally newer than
    /// the source live record, but it must never delete the live object already
    /// installed on `target_shard_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tombstone_provenance: Option<DfsTombstoneProvenance>,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub current_locations: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_target_object_id: Option<DfsObjectId>,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub mode: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub uid: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub gid: u32,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub accessed_at_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub modified_at_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub created_at_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub changed_at_unix_nanos: u64,
    pub version: DfsVersion,
    pub policy: DfsObjectPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified_by_event_id: Option<EventId>,
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
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

/// Why a durable DFS tombstone exists.
///
/// Ordinary namespace deletes keep the legacy `None` representation on
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
