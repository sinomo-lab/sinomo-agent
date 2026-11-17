//! Namespace work parked on a shard whose storage is closed.
//!
//! A namespace job step that must wait for a shard's storage to be Active (a
//! directory move whose carried alias targets a converting or unprepared
//! shard) is refused with `Busy` and stays Running. The refusal records the
//! shard here for the node that ran the step: the refusal is the event, and
//! nothing scans jobs to rebuild the record. When shards' storage activates,
//! the node's host takes the records naming them and wakes maintenance only
//! if there was one, so a node with no parked work runs no pass.
//!
//! The records live in memory. After a restart the startup maintenance pass
//! runs every Running job again, and a step that still waits records its
//! shard anew.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use sinomo_core::node::NodeId;

use crate::DfsMetadataShardStorageKey;

static PARKED: Mutex<BTreeMap<NodeId, BTreeSet<DfsMetadataShardStorageKey>>> =
    Mutex::new(BTreeMap::new());

/// A job step on `node` was refused until each of `shards` has Active storage.
pub(crate) fn record_parked_on_shard_storage(
    node: &NodeId,
    shards: impl IntoIterator<Item = DfsMetadataShardStorageKey>,
) {
    let mut shards = shards.into_iter().peekable();
    if shards.peek().is_none() {
        return;
    }
    PARKED
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .entry(node.clone())
        .or_default()
        .extend(shards);
}

/// Take `node`'s records for the `activated` shards. Returns whether any work
/// on the node was parked on one of them.
pub(crate) fn take_parked_on_activated_shard_storage(
    node: &NodeId,
    activated: &BTreeSet<DfsMetadataShardStorageKey>,
) -> bool {
    let mut parked = PARKED.lock().unwrap_or_else(|error| error.into_inner());
    let Some(shards) = parked.get_mut(node) else {
        return false;
    };
    let before = shards.len();
    shards.retain(|shard| !activated.contains(shard));
    let resumed = shards.len() < before;
    if shards.is_empty() {
        parked.remove(node);
    }
    resumed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DfsMetadataShardId;

    fn shard(name: &str) -> DfsMetadataShardStorageKey {
        (crate::sifs_file_system_id(), DfsMetadataShardId::new(name))
    }

    #[test]
    fn an_activation_resumes_only_the_work_parked_on_its_shard() {
        let parked = NodeId::from_legacy_named("storage-waits-parked").unwrap();
        let idle = NodeId::from_legacy_named("storage-waits-idle").unwrap();
        record_parked_on_shard_storage(&parked, [shard("users")]);
        let activated = BTreeSet::from([shard("users")]);
        assert!(!take_parked_on_activated_shard_storage(&idle, &activated));
        assert!(!take_parked_on_activated_shard_storage(
            &parked,
            &BTreeSet::from([shard("services")])
        ));
        assert!(take_parked_on_activated_shard_storage(&parked, &activated));
        // Taken once: a later activation of the same shard finds no work.
        assert!(!take_parked_on_activated_shard_storage(&parked, &activated));
    }
}
