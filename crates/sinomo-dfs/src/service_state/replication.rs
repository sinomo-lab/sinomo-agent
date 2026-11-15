//! Replication state owns its live-operation quiescence predicate.

use super::*;
use crate::DfsMetadataReplicaObserver;

#[derive(Clone, Debug)]
pub(crate) struct RoutedReplicaProjection {
    pub(crate) authority: DfsMetadataShardConfig,
    pub(crate) observer: DfsMetadataReplicaObserver,
    pub(crate) observed_generation: u64,
}

#[derive(Debug, Default)]
pub(crate) struct DfsReplicationState {
    pub(crate) local_metadata_journal_cursors: BTreeMap<DfsMetadataShardId, u64>,
    pub(crate) routed_replica_projections: BTreeMap<DfsMetadataShardId, RoutedReplicaProjection>,
    pub(crate) metadata_replication_cursors: BTreeMap<(NodeId, DfsMetadataShardId), u64>,
    pub(crate) pending_metadata_replications: BTreeMap<String, PendingMetadataReplication>,
    pub(crate) pending_metadata_authority_probes: BTreeMap<String, PendingMetadataAuthorityProbe>,
    pub(crate) metadata_authority_probe_results: BTreeMap<String, bool>,
    /// A member's typed rejection of an authority probe that no catch-up can
    /// answer, by request id: it refused the source epoch (with its record
    /// view), or it must first be aligned with the epoch start.
    pub(crate) metadata_authority_rejections: BTreeMap<
        String,
        (
            crate::DfsMetadataRejectionCode,
            Option<crate::DfsMetadataMemberRecordView>,
        ),
    >,
    /// Members a running durability barrier sends nothing to this round:
    /// their last attempt did not take, and their re-send interval has not
    /// passed.
    pub(crate) metadata_sync_held: BTreeSet<NodeId>,
    /// Members whose last routed barrier send the network ended as definitely
    /// not sent.
    pub(crate) metadata_sync_unreachable: BTreeSet<NodeId>,
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
            && self.metadata_authority_rejections.is_empty()
    }
}
