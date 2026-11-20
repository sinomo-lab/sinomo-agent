//! Writes state owns its live-operation quiescence predicate.

use super::*;

#[derive(Debug, Default)]
pub(crate) struct DfsWriteState {
    pub(crate) active_replica_stages: BTreeMap<DfsObjectId, ActiveReplicaStage>,
    pub(crate) pending_writes: BTreeMap<String, PendingNetworkedWrite>,
    pub(crate) pending_routed_writes: BTreeMap<String, PendingRoutedWrite>,
    /// The caller of the next routed-write dispatch waits for the write's
    /// outcome, so a dispatch that cannot be delivered follows the shard's
    /// record instead of ending the call. The dispatch consumes it.
    pub(crate) follow_next_dispatch: bool,
    /// A test pins what one attempt returns: this requester reads its
    /// projection once instead of waiting for a record change.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) single_attempt_for_test: bool,
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
