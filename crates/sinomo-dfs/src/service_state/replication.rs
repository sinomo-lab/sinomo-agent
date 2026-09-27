//! Replication state owns its live-operation quiescence predicate.

use super::*;
use crate::DfsMetadataSnapshotInstallObserver;

#[derive(Clone, Debug)]
pub(crate) struct PendingRoutedSnapshotRefresh {
    pub(crate) authority: DfsMetadataShardConfig,
    pub(crate) observer: DfsMetadataSnapshotInstallObserver,
    pub(crate) observed_generation: u64,
}

#[derive(Debug, Default)]
pub(crate) struct DfsReplicationState {
    pub(crate) local_metadata_journal_cursors: BTreeMap<DfsMetadataShardId, u64>,
    pub(crate) pending_routed_snapshot_refreshes:
        BTreeMap<DfsMetadataShardId, PendingRoutedSnapshotRefresh>,
    pub(crate) metadata_replication_cursors: BTreeMap<(NodeId, DfsMetadataShardId), u64>,
    pub(crate) recent_namespace_freshness_syncs:
        BTreeMap<(DfsFileSystemId, DfsMetadataShardId, NodeId), Instant>,
    pub(crate) pending_metadata_replications: BTreeMap<String, PendingMetadataReplication>,
    pub(crate) pending_metadata_authority_probes: BTreeMap<String, PendingMetadataAuthorityProbe>,
    pub(crate) metadata_authority_probe_results: BTreeMap<String, bool>,
}

impl DfsReplicationState {
    /// Only outbound peer cursors survive rebuilding the request shell. Other
    /// projections are refreshed from the exact durable backend generation.
    pub(crate) fn into_lazy(self) -> Self {
        Self {
            metadata_replication_cursors: self.metadata_replication_cursors,
            ..Self::default()
        }
    }

    pub(crate) fn is_quiescent(&self) -> bool {
        self.pending_metadata_replications.is_empty()
            && self.pending_metadata_authority_probes.is_empty()
            && self.metadata_authority_probe_results.is_empty()
    }
}
