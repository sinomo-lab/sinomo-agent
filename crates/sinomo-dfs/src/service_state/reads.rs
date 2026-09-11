//! Reads state owns its live-operation quiescence predicate.

use super::*;

#[derive(Debug, Default)]
pub(crate) struct DfsReadState {
    pub(crate) pending_reads: BTreeMap<String, PendingNetworkedRead>,
    pub(crate) transient_remote_read_pruned_locations:
        BTreeMap<(DfsObjectId, DfsVersion, Option<String>), BTreeSet<NodeId>>,
}

impl DfsReadState {
    pub(crate) fn is_quiescent(&self) -> bool {
        self.pending_reads.is_empty()
    }
}
