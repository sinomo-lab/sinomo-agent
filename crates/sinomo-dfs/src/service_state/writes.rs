//! Writes state owns its live-operation quiescence predicate.

use super::*;

#[derive(Debug, Default)]
pub(crate) struct DfsWriteState {
    pub(crate) active_replica_stages: BTreeMap<DfsObjectId, ActiveReplicaStage>,
    pub(crate) pending_writes: BTreeMap<String, PendingNetworkedWrite>,
    pub(crate) pending_routed_writes: BTreeMap<String, PendingRoutedWrite>,
    pub(crate) pending_remote_write_responses: BTreeMap<String, PendingRemoteWriteResponse>,
    pub(crate) deferred_routed_create_parent_touches: BTreeSet<String>,
    pub(crate) deferred_untracked_routed_write_responses: BTreeMap<String, DfsObjectId>,
    pub(crate) stable_write_reconciliation_results:
        BTreeMap<String, DfsStableWriteOperationReconciliation>,
    pub(crate) stable_write_reconciliation_targets: BTreeMap<String, NodeId>,
}

impl DfsWriteState {
    pub(crate) fn is_quiescent(&self) -> bool {
        self.active_replica_stages.is_empty()
            && self.pending_writes.is_empty()
            && self.pending_routed_writes.is_empty()
            && self.pending_remote_write_responses.is_empty()
            && self.deferred_routed_create_parent_touches.is_empty()
            && self.deferred_untracked_routed_write_responses.is_empty()
    }
}
