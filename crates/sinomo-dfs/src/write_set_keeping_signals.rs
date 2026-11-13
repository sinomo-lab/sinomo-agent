//! Demands a commit attempt leaves for the shard primary's write-set keeper.
//!
//! The commit path is a DFS operation root and reads no KV; changing the
//! write-set record is the keeper's. When a commit ends unknown because the
//! record must change first (its content W is above the W the record accepts,
//! or a required member could not be reached), it posts the demand here and
//! wakes the keeper. Demands are in memory only: a lost demand is posted
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
}

/// The demands collected for one shard since the keeper last took them.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct DfsWriteSetShardDemands {
    pub(crate) raise_n: usize,
    pub(crate) unreachable: BTreeSet<NodeId>,
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
        }
        signals.wakes.get(local).cloned()
    };
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
