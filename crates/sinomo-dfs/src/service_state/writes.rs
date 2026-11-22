//! Writes state owns its live-operation quiescence predicate.

use super::*;

#[derive(Debug, Default)]
pub(crate) struct DfsWriteState {
    pub(crate) active_replica_stages: BTreeMap<DfsObjectId, ActiveReplicaStage>,
    pub(crate) pending_writes: BTreeMap<String, PendingNetworkedWrite>,
    pub(crate) pending_routed_writes: BTreeMap<String, PendingRoutedWrite>,
    /// The write whose caller waits for its outcome: a dispatch of this
    /// request that cannot be delivered follows the shard's record instead
    /// of ending the call. It names the request, so no other dispatch made
    /// on the way takes it; the request's own dispatch consumes it.
    pub(crate) followed_write: Option<(DfsObjectId, DfsVersion)>,
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
    /// Say whether the caller of the write of `object_id` to
    /// `target_version` waits for its outcome.
    pub(crate) fn follow_write(
        &mut self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        follow: bool,
    ) {
        self.followed_write = follow.then(|| (object_id.clone(), target_version.clone()));
    }

    /// Whether the caller of the routed write tracked under `request_id`
    /// waits for its outcome. Only that write's dispatch takes the answer.
    pub(crate) fn take_followed_write(&mut self, request_id: &str) -> bool {
        let follows = self
            .pending_routed_writes
            .get(request_id)
            .and_then(|pending| pending.command.as_ref())
            .zip(self.followed_write.as_ref())
            .is_some_and(|(command, (object_id, target_version))| {
                command.request.object_id == *object_id
                    && command.request.target_version == *target_version
            });
        if follows {
            self.followed_write = None;
        }
        follows
    }

    pub(crate) fn is_quiescent(&self) -> bool {
        self.active_replica_stages.is_empty()
            && self.pending_writes.is_empty()
            && self.pending_routed_writes.is_empty()
            && self.pending_remote_write_responses.is_empty()
            && self.deferred_routed_create_parent_touches.is_empty()
            && self.deferred_untracked_routed_write_responses.is_empty()
    }
}
