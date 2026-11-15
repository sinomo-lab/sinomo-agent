//! Demands DFS operations leave for this node's write-set keeper.
//!
//! The commit path is a DFS operation root and reads no KV; changing the
//! write-set record is the keeper's. When a commit ends unknown because the
//! record must change first (its content W is above the W the record accepts,
//! a required member could not be reached, or one refused the epoch for a
//! takeover candidate), or a member must first be aligned with the epoch
//! start, it posts the demand here and wakes the keeper. A requester that
//! could not deliver to a shard primary posts a hint for the candidate role. Demands are in memory only: a lost demand is posted
//! again by the next attempt on the same prepared body, including the
//! recovery pass that runs when the primary activates.

use crate::{DfsFileSystemId, DfsMetadataShardId};
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, LazyLock, Mutex};

/// What one commit attempt needs from the keeper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsWriteSetCommitDemand {
    /// The shard must accept content W at least this large.
    RaiseN(usize),
    /// These required members could not be reached by any attempt.
    Unreachable(BTreeSet<NodeId>),
    /// These required members refused the primary's epoch: each promised a
    /// takeover candidate to accept nothing more at it.
    Fenced(BTreeSet<NodeId>),
    /// These members must be aligned with the epoch start before they can
    /// acknowledge anything at this epoch.
    AlignmentRequired(BTreeSet<NodeId>),
    /// A request from this node could not be delivered to the shard primary:
    /// the candidate role asks the network to verify it.
    PrimaryUnreachable,
}

/// The demands collected for one shard since the keeper last took them.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct DfsWriteSetShardDemands {
    pub(crate) raise_n: usize,
    pub(crate) unreachable: BTreeSet<NodeId>,
    pub(crate) fenced: BTreeSet<NodeId>,
    pub(crate) alignment: BTreeSet<NodeId>,
    pub(crate) primary_unreachable: bool,
}

#[derive(Default)]
struct Signals {
    demands: BTreeMap<(NodeId, DfsFileSystemId, DfsMetadataShardId), DfsWriteSetShardDemands>,
    wakes: BTreeMap<NodeId, Arc<tokio::sync::Notify>>,
}

static SIGNALS: LazyLock<Mutex<Signals>> = LazyLock::new(Default::default);

/// Record `demand` for the shard `local` is primary of and wake its keeper.
pub(crate) fn post_commit_demand(
    local: &NodeId,
    file_system_id: &DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
    demand: DfsWriteSetCommitDemand,
) {
    let wake = {
        let mut signals = SIGNALS.lock().unwrap_or_else(|error| error.into_inner());
        let entry = signals
            .demands
            .entry((local.clone(), file_system_id.clone(), shard_id.clone()))
            .or_default();
        match demand {
            DfsWriteSetCommitDemand::RaiseN(w) => entry.raise_n = entry.raise_n.max(w),
            DfsWriteSetCommitDemand::Unreachable(members) => entry.unreachable.extend(members),
            DfsWriteSetCommitDemand::Fenced(members) => entry.fenced.extend(members),
            DfsWriteSetCommitDemand::AlignmentRequired(members) => entry.alignment.extend(members),
            DfsWriteSetCommitDemand::PrimaryUnreachable => entry.primary_unreachable = true,
        }
        signals.wakes.get(local).cloned()
    };
    if let Some(wake) = wake {
        wake.notify_one();
    }
}

/// Wake `local`'s keeper without a demand: another member asked it about a
/// shard primary it judges lost.
pub(crate) fn wake_write_set_keeper(local: &NodeId) {
    let wake = SIGNALS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .wakes
        .get(local)
        .cloned();
    if let Some(wake) = wake {
        wake.notify_one();
    }
}

/// Take every demand posted for `local`'s shards.
pub(crate) fn take_commit_demands(
    local: &NodeId,
) -> BTreeMap<(DfsFileSystemId, DfsMetadataShardId), DfsWriteSetShardDemands> {
    let mut signals = SIGNALS.lock().unwrap_or_else(|error| error.into_inner());
    let keys = signals
        .demands
        .keys()
        .filter(|(node, _, _)| node == local)
        .cloned()
        .collect::<Vec<_>>();
    keys.into_iter()
        .filter_map(|key| {
            let demands = signals.demands.remove(&key)?;
            Some(((key.1, key.2), demands))
        })
        .collect()
}

/// The notification `local`'s keeper waits on for commit demands.
pub(crate) fn commit_demand_wake(local: &NodeId) -> Arc<tokio::sync::Notify> {
    Arc::clone(
        SIGNALS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .wakes
            .entry(local.clone())
            .or_default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demands_merge_per_shard_until_taken() {
        let local = NodeId::from_legacy_named("signals-local").unwrap();
        let other = NodeId::from_legacy_named("signals-member").unwrap();
        let fs = crate::sifs_file_system_id();
        let shard = DfsMetadataShardId::new("signals-shard");
        post_commit_demand(&local, &fs, &shard, DfsWriteSetCommitDemand::RaiseN(2));
        post_commit_demand(&local, &fs, &shard, DfsWriteSetCommitDemand::RaiseN(3));
        post_commit_demand(
            &local,
            &fs,
            &shard,
            DfsWriteSetCommitDemand::Unreachable([other.clone()].into_iter().collect()),
        );
        let taken = take_commit_demands(&local);
        let demands = taken.get(&(fs, shard)).unwrap();
        assert_eq!(demands.raise_n, 3);
        assert!(demands.unreachable.contains(&other));
        assert!(take_commit_demands(&local).is_empty());
    }
}
