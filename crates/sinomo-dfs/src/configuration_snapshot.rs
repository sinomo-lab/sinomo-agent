//! One immutable DFS configuration publication.
//!
//! The metadata-authority runtime is the only writer. It replaces the whole
//! snapshot on a committed delta, a completed bootstrap or an invalidation and
//! publishes it through a `tokio::sync::watch` channel. A consumer loads the
//! current `Arc` once per operation and uses that same value for shard
//! routing, filesystem policy, members and providers. Its commit-point fence
//! is `snapshot.generation() == runtime.generation()`.

use crate::metadata_authority_projection::DfsMetadataShardsByFileSystem;
use crate::write_configuration_projection::DFS_WRITE_CONFIGURATION_PREFIXES;
use crate::{
    DfsAuthoritativePrefixProjection, DfsError, DfsFileSystemConfig, DfsFileSystemId,
    DfsMetadataReplicaShardContext, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsNamespaceCommand, DfsWriteConfigurationProjection,
};
use sinomo_api::dfs::{
    DFS_CONFIGURATION_SNAPSHOT_MAX_RESPONSE_BYTES, DFS_CONFIGURATION_SNAPSHOT_ROUTE,
    DFS_CONFIGURATION_SNAPSHOT_UNSUPPORTED_DETAIL, DfsConfigurationPublication,
    DfsConfigurationSnapshotRequest, DfsConfigurationSnapshotResponse,
    DfsConfigurationSnapshotWire,
};
use sinomo_core::kv::{KvEntry, KvError, KvRevision};
use sinomo_core::network::{ObjectRpcCall, ObjectRpcResponse};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Decoded filesystem configurations keyed by their exact configuration key.
pub(crate) type DfsFileSystemConfigsByKey = BTreeMap<String, Result<DfsFileSystemConfig, String>>;

/// The complete `/dfs/filesystems/` scope at one committed revision, with its
/// decoded shard and filesystem indexes. Built only from native proofs and
/// committed deltas carrying an applied index.
#[derive(Clone, Debug)]
pub(crate) struct DfsMetadataAuthorityScope {
    pub(crate) revision: KvRevision,
    pub(crate) entries: Arc<Vec<KvEntry>>,
    pub(crate) shards: Arc<DfsMetadataShardsByFileSystem>,
    pub(crate) file_systems: Arc<DfsFileSystemConfigsByKey>,
}

#[derive(Clone, Debug)]
pub struct DfsConfigurationSnapshot {
    pub(crate) generation: u64,
    pub(crate) projection_generation: u64,
    pub(crate) storage_generation: u64,
    pub(crate) metadata: Option<DfsMetadataAuthorityScope>,
    pub(crate) write: Option<Arc<DfsWriteConfigurationProjection>>,
}

impl DfsConfigurationSnapshot {
    pub(crate) fn initial() -> Self {
        Self {
            generation: 1,
            projection_generation: 1,
            storage_generation: 1,
            metadata: None,
            write: None,
        }
    }

    /// Exact topology/authority fence. It advances whenever exact shard,
    /// handoff, watermark or filesystem configuration changes, when provider
    /// or content scope changes, and when a committed change, discontinuity or
    /// floor advance invalidates a published write configuration. Revision
    /// catch-up with unchanged exact authority keeps it.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Advances on every change to the published filesystem scope.
    pub fn projection_generation(&self) -> u64 {
        self.projection_generation
    }

    /// Physical metadata-store lifecycle.
    pub fn storage_generation(&self) -> u64 {
        self.storage_generation
    }

    /// Revision of the complete filesystem scope, `None` while withdrawn.
    pub fn revision(&self) -> Option<KvRevision> {
        self.metadata.as_ref().map(|metadata| metadata.revision)
    }

    /// The complete four-scope configuration with its native proofs and floor.
    /// Present only after a coherent bootstrap at a write-capable host.
    pub fn write_configuration(&self) -> Option<&Arc<DfsWriteConfigurationProjection>> {
        self.write.as_ref()
    }

    /// The generations and revision identifying this publication.
    pub fn publication(&self) -> DfsConfigurationPublication {
        DfsConfigurationPublication {
            generation: self.generation,
            projection_generation: self.projection_generation,
            storage_generation: self.storage_generation,
            revision: self.revision(),
        }
    }

    /// Whether `other` publishes different configuration. A replacement that
    /// only replays later unrelated revisions is not a configuration change.
    pub fn same_configuration_content(&self, other: &Self) -> bool {
        self.generation == other.generation
            && self.projection_generation == other.projection_generation
            && self.storage_generation == other.storage_generation
            && self.metadata.is_some() == other.metadata.is_some()
            && match (&self.write, &other.write) {
                (None, None) => true,
                (Some(left), Some(right)) => Arc::ptr_eq(left, right) || left.same_content(right),
                _ => false,
            }
    }

    /// Wire form for an out-of-process client. `None` while the filesystem
    /// scope is withdrawn.
    pub fn to_wire(&self) -> Option<DfsConfigurationSnapshotWire> {
        let metadata = self.metadata.as_ref()?;
        Some(DfsConfigurationSnapshotWire {
            publication: self.publication(),
            filesystem_entries: metadata.entries.as_ref().clone(),
            write: self.write.as_ref().map(|write| write.to_wire()),
        })
    }

    /// Rebuild a publication served by the local daemon. Every scope is
    /// validated as a complete bounded prefix at its own revision.
    pub fn from_wire(wire: DfsConfigurationSnapshotWire) -> Result<Self, KvError> {
        let revision = wire
            .publication
            .revision
            .ok_or_else(|| KvError::Serialization {
                detail: "published DFS configuration has no filesystem revision".to_string(),
            })?;
        let (entries, _, _) = DfsAuthoritativePrefixProjection::complete(
            crate::metadata_authority_runtime::DFS_METADATA_AUTHORITY_PREFIX,
            wire.filesystem_entries,
            revision,
        )?
        .into_parts();
        let write = wire
            .write
            .map(DfsWriteConfigurationProjection::from_wire)
            .transpose()?
            .map(Arc::new);
        Ok(Self {
            generation: wire.publication.generation,
            projection_generation: wire.publication.projection_generation,
            storage_generation: wire.publication.storage_generation,
            metadata: Some(DfsMetadataAuthorityScope {
                revision,
                shards: Arc::new(crate::metadata_shard_index_from_authority_entries(&entries)),
                file_systems: Arc::new(file_system_configs_from_authority_entries(&entries)),
                entries: Arc::new(entries),
            }),
            write,
        })
    }

    /// Revision of the published scope that answers `prefix`, matching the
    /// scope [`Self::entries`] reads.
    pub fn covering_revision(&self, prefix: &str) -> Option<KvRevision> {
        if let Some(write) = self.write.as_ref() {
            return write.entries(prefix).map(|_| write.revision);
        }
        let metadata = self.metadata.as_ref()?;
        prefix
            .starts_with(crate::metadata_authority_runtime::DFS_METADATA_AUTHORITY_PREFIX)
            .then_some(metadata.revision)
    }

    pub(crate) fn same_publication(&self, other: &Self) -> bool {
        self.generation == other.generation
            && self.projection_generation == other.projection_generation
            && self.storage_generation == other.storage_generation
            && match (&self.metadata, &other.metadata) {
                (None, None) => true,
                (Some(left), Some(right)) => {
                    left.revision == right.revision && Arc::ptr_eq(&left.entries, &right.entries)
                }
                _ => false,
            }
            && match (&self.write, &other.write) {
                (None, None) => true,
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                _ => false,
            }
    }

    pub fn filesystem_entries(&self) -> Option<&[KvEntry]> {
        self.metadata
            .as_ref()
            .map(|metadata| metadata.entries.as_slice())
    }

    /// Copy the complete filesystem scope into an owned prefix certificate.
    pub fn complete_prefix(&self) -> Option<DfsAuthoritativePrefixProjection> {
        let metadata = self.metadata.as_ref()?;
        DfsAuthoritativePrefixProjection::complete(
            crate::metadata_authority_runtime::DFS_METADATA_AUTHORITY_PREFIX,
            metadata.entries.as_ref().clone(),
            metadata.revision,
        )
        .ok()
    }

    /// Decoded shard routing for one filesystem. `None` means the filesystem
    /// scope is withdrawn; an unknown filesystem has no shards.
    pub fn metadata_shards(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<Vec<DfsMetadataShardConfig>>, KvError> {
        let Some(metadata) = self.metadata.as_ref() else {
            return Ok(None);
        };
        match metadata.shards.get(file_system_id.as_str()) {
            Some(Ok(shards)) => Ok(Some(shards.clone())),
            Some(Err(detail)) => Err(KvError::Serialization {
                detail: detail.clone(),
            }),
            None => Ok(Some(Vec::new())),
        }
    }

    /// Decoded filesystem configuration. `None` means the scope is withdrawn.
    pub fn file_system_config(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<Option<DfsFileSystemConfig>>, KvError> {
        let Some(metadata) = self.metadata.as_ref() else {
            return Ok(None);
        };
        match metadata
            .file_systems
            .get(&crate::file_system_config_key(file_system_id))
        {
            Some(Ok(config)) => Ok(Some(Some(config.clone()))),
            Some(Err(detail)) => Err(KvError::Serialization {
                detail: detail.clone(),
            }),
            None => Ok(Some(None)),
        }
    }

    /// Shards and filesystem configuration from this same publication.
    pub(crate) fn publication_configuration(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<(Vec<DfsMetadataShardConfig>, Option<DfsFileSystemConfig>)>, KvError> {
        let Some(shards) = self.metadata_shards(file_system_id)? else {
            return Ok(None);
        };
        let config = self.file_system_config(file_system_id)?.flatten();
        Ok(Some((shards, config)))
    }

    /// Complete entries for a prefix inside one published scope. The four
    /// scopes require the write configuration; the filesystem scope is also
    /// available from the metadata publication alone.
    pub fn entries(&self, prefix: &str) -> Option<&[KvEntry]> {
        if let Some(write) = self.write.as_ref() {
            return write.entries(prefix);
        }
        let metadata = self.metadata.as_ref()?;
        if !prefix.starts_with(crate::metadata_authority_runtime::DFS_METADATA_AUTHORITY_PREFIX) {
            return None;
        }
        let entries = metadata.entries.as_slice();
        let start = entries.partition_point(|entry| entry.key.as_str() < prefix);
        let count = entries[start..].partition_point(|entry| entry.key.starts_with(prefix));
        Some(&entries[start..start + count])
    }

    /// One exact key from a published scope. The outer `None` means this
    /// publication does not cover the key; the inner value proves absence.
    pub fn entry(&self, key: &str) -> Option<Option<&KvEntry>> {
        let entries = self.entries(key)?;
        Some(entries.iter().find(|entry| entry.key == key))
    }

    pub(crate) fn contains_shard_key(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> bool {
        let key = crate::metadata_shard_key(file_system_id, &shard_id.0);
        self.metadata.as_ref().is_some_and(|metadata| {
            metadata
                .entries
                .binary_search_by(|entry| entry.key.as_str().cmp(key.as_str()))
                .is_ok()
        })
    }

    pub(crate) fn namespace_read_stamp_matches(
        &self,
        command: &DfsNamespaceCommand,
    ) -> Result<bool, DfsError> {
        let Some(metadata) = self.metadata.as_ref() else {
            return Ok(false);
        };
        crate::namespace_command_matches_authority_index(&metadata.shards, command)
    }

    pub(crate) fn replica_context_matches(
        &self,
        context: &DfsMetadataReplicaShardContext,
    ) -> Result<bool, DfsError> {
        let Some(metadata) = self.metadata.as_ref() else {
            return Ok(false);
        };
        crate::metadata_replica_context_matches_authority_entries(&metadata.entries, context)
    }
}

pub(crate) fn file_system_configs_from_authority_entries(
    entries: &[KvEntry],
) -> DfsFileSystemConfigsByKey {
    entries
        .iter()
        .filter(|entry| {
            crate::metadata_authority_projection::dfs_metadata_file_system_config_key_is_exact(
                &entry.key,
            )
        })
        .map(|entry| {
            (
                entry.key.clone(),
                entry
                    .decode::<DfsFileSystemConfig>()
                    .map_err(|error| error.to_string()),
            )
        })
        .collect()
}

/// Whether a key or prefix lies inside one of the published scopes.
pub fn dfs_configuration_scope_contains(key_or_prefix: &str) -> bool {
    DFS_WRITE_CONFIGURATION_PREFIXES
        .iter()
        .any(|scope| key_or_prefix.starts_with(scope))
}

/// Whether a deleted prefix can remove keys of a published scope.
pub fn dfs_configuration_scopes_overlap(prefix: &str) -> bool {
    DFS_WRITE_CONFIGURATION_PREFIXES
        .iter()
        .any(|scope| scope.starts_with(prefix) || prefix.starts_with(scope))
}

/// DFS protocol peers named by one published configuration: authorized
/// members with an active current DFS provider offer, plus the local node.
/// `None` when the publication carries no membership and provider scopes.
/// This reads no KV state; the snapshot is the operation's configuration.
pub fn dfs_peer_node_ids_from_configuration(
    snapshot: &DfsConfigurationSnapshot,
    state: &sinomo_core::ClusterBootstrapState,
) -> Result<Option<Vec<sinomo_core::node::NodeId>>, DfsError> {
    let (Some(members), Some(services)) = (
        snapshot.entries(DFS_WRITE_CONFIGURATION_PREFIXES[3]),
        snapshot.entries(DFS_WRITE_CONFIGURATION_PREFIXES[2]),
    ) else {
        return Ok(None);
    };
    let members = members
        .iter()
        .map(|entry| entry.decode::<sinomo_core::ClusterMemberRecord>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(DfsError::from)?;
    let mut peers = crate::cluster_service::dfs_provider_node_ids_from_current_entries(
        state, &members, services,
    )?;
    if !peers.contains(&state.local_node_id) {
        peers.push(state.local_node_id.clone());
        peers.sort();
    }
    Ok(Some(peers))
}

/// What the local daemon currently publishes.
#[derive(Clone, Debug)]
pub enum DfsFetchedConfiguration {
    Published(Arc<DfsConfigurationSnapshot>),
    /// No coherent publication yet; the next one emits an event.
    Pending(DfsConfigurationPublication),
}

/// One local-node call for the current publication.
pub fn dfs_configuration_snapshot_call() -> Result<ObjectRpcCall, KvError> {
    ObjectRpcCall::encode_value(
        DFS_CONFIGURATION_SNAPSHOT_ROUTE,
        crate::DFS_RPC_CATEGORY,
        DfsConfigurationSnapshotRequest::default(),
    )
    .map_err(|error| KvError::Serialization {
        detail: error.to_string(),
    })
}

/// Largest materialized response a client accepts from the route.
pub const fn dfs_configuration_snapshot_max_response_bytes() -> usize {
    DFS_CONFIGURATION_SNAPSHOT_MAX_RESPONSE_BYTES
}

pub fn decode_dfs_configuration_snapshot_response(
    response: &ObjectRpcResponse,
) -> Result<DfsFetchedConfiguration, KvError> {
    if response.route != DFS_CONFIGURATION_SNAPSHOT_ROUTE {
        return Err(KvError::Serialization {
            detail: format!(
                "DFS configuration snapshot returned mismatched route {}",
                response.route
            ),
        });
    }
    let response: DfsConfigurationSnapshotResponse =
        response
            .decode_body()
            .map_err(|error| KvError::Serialization {
                detail: error.to_string(),
            })?;
    Ok(match response {
        DfsConfigurationSnapshotResponse::Published { snapshot } => {
            DfsFetchedConfiguration::Published(Arc::new(DfsConfigurationSnapshot::from_wire(
                snapshot,
            )?))
        }
        DfsConfigurationSnapshotResponse::Pending { publication } => {
            DfsFetchedConfiguration::Pending(publication)
        }
    })
}

/// The typed error for a local daemon that predates the snapshot route. It
/// is returned as is; callers never fall back to their own configuration
/// reads.
pub fn dfs_configuration_snapshot_unsupported() -> KvError {
    KvError::AuthorityUnavailable {
        detail: DFS_CONFIGURATION_SNAPSHOT_UNSUPPORTED_DETAIL.to_string(),
    }
}

pub fn is_dfs_configuration_snapshot_unsupported(error: &KvError) -> bool {
    matches!(error, KvError::AuthorityUnavailable { detail }
        if detail == DFS_CONFIGURATION_SNAPSHOT_UNSUPPORTED_DETAIL)
}
