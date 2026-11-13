//! The latest write-set member views this process has accepted.
//!
//! A member of a write-set shard attaches `member_view` to every durable
//! acknowledgement: the epoch and write-set generation of its own record view
//! and the exact prefix it holds. Every primary-side path that accepts such an
//! acknowledgement (commit fan-out, ordinary routed replication, the sync
//! barrier and the background publisher) records the view here, keyed by the
//! acknowledging primary's node. The write-set keeper reads it as catch-up
//! evidence for an add.
//!
//! Recording is in memory only. A view replaces the stored one only when it is
//! at least as new by (epoch, generation, prefix sequence), so a delayed reply
//! never hides a newer one. Only views of members the keeper registered
//! interest in wake it, so the acknowledgements of write-set members on every
//! commit cost one store and no wake.

use crate::{DfsMetadataMemberAckView, DfsMetadataShardId};
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, LazyLock, Mutex};

type EvidenceKey = (NodeId, DfsMetadataShardId, NodeId);

#[derive(Default)]
struct Evidence {
    views: BTreeMap<EvidenceKey, DfsMetadataMemberAckView>,
    interest: BTreeSet<EvidenceKey>,
    wakes: BTreeMap<NodeId, Arc<tokio::sync::Notify>>,
}

static EVIDENCE: LazyLock<Mutex<Evidence>> = LazyLock::new(Default::default);

fn view_order(view: &DfsMetadataMemberAckView) -> (u64, u64, u64) {
    (
        view.epoch,
        view.write_set_generation,
        view.durable_prefix.journal_sequence,
    )
}

/// Record `member`'s view from an acknowledgement `local` accepted for
/// `shard_id`. An older view than the stored one is ignored.
pub(crate) fn record_metadata_member_view(
    local: &NodeId,
    shard_id: &DfsMetadataShardId,
    member: &NodeId,
    view: Option<&DfsMetadataMemberAckView>,
) {
    let Some(view) = view else {
        return;
    };
    let wake = {
        let mut evidence = EVIDENCE.lock().unwrap_or_else(|error| error.into_inner());
        let key = (local.clone(), shard_id.clone(), member.clone());
        let newer = evidence
            .views
            .get(&key)
            .is_none_or(|stored| view_order(view) >= view_order(stored) && stored != view);
        if !newer {
            return;
        }
        evidence.views.insert(key.clone(), view.clone());
        if !evidence.interest.contains(&key) {
            return;
        }
        evidence.wakes.get(local).cloned()
    };
    if let Some(wake) = wake {
        wake.notify_one();
    }
}

/// The latest view `local` holds for `member` on `shard_id`.
pub(crate) fn metadata_member_view(
    local: &NodeId,
    shard_id: &DfsMetadataShardId,
    member: &NodeId,
) -> Option<DfsMetadataMemberAckView> {
    EVIDENCE
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .views
        .get(&(local.clone(), shard_id.clone(), member.clone()))
        .cloned()
}

/// Wake `local`'s keeper when a new view of `member` on `shard_id` arrives.
pub(crate) fn register_metadata_member_view_interest(
    local: &NodeId,
    shard_id: &DfsMetadataShardId,
    member: &NodeId,
) {
    EVIDENCE
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .interest
        .insert((local.clone(), shard_id.clone(), member.clone()));
}

/// Stop waking `local` for `member` on `shard_id`.
pub(crate) fn clear_metadata_member_view_interest(
    local: &NodeId,
    shard_id: &DfsMetadataShardId,
    member: &NodeId,
) {
    EVIDENCE
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .interest
        .remove(&(local.clone(), shard_id.clone(), member.clone()));
}

/// The notification `local`'s keeper waits on for registered views.
pub(crate) fn metadata_member_view_wake(local: &NodeId) -> Arc<tokio::sync::Notify> {
    Arc::clone(
        EVIDENCE
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .wakes
            .entry(local.clone())
            .or_default(),
    )
}

/// Forget everything `local` recorded for `shard_id`, for example after it
/// stopped being the shard's primary.
pub(crate) fn forget_metadata_member_views(local: &NodeId, shard_id: &DfsMetadataShardId) {
    let mut evidence = EVIDENCE.lock().unwrap_or_else(|error| error.into_inner());
    evidence
        .views
        .retain(|(node, shard, _), _| !(node == local && shard == shard_id));
    evidence
        .interest
        .retain(|(node, shard, _)| !(node == local && shard == shard_id));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).unwrap()
    }

    fn view(epoch: u64, generation: u64, sequence: u64) -> DfsMetadataMemberAckView {
        DfsMetadataMemberAckView {
            epoch,
            write_set_generation: generation,
            durable_prefix: crate::DfsMetadataPrefixProof::from_digest(
                sequence,
                format!("dfs-metadata-prefix-v2:blake3:{sequence:064x}"),
            )
            .unwrap(),
        }
    }

    #[test]
    fn a_view_replaces_only_an_older_one_and_wakes_only_for_interest() {
        let local = node("evidence-local");
        let member = node("evidence-member");
        let shard = DfsMetadataShardId::new("evidence-shard");
        let wake = metadata_member_view_wake(&local);
        record_metadata_member_view(&local, &shard, &member, Some(&view(2, 3, 10)));
        assert_eq!(metadata_member_view(&local, &shard, &member), Some(view(2, 3, 10)));
        // A delayed, older acknowledgement never hides the newer view.
        record_metadata_member_view(&local, &shard, &member, Some(&view(2, 2, 40)));
        assert_eq!(metadata_member_view(&local, &shard, &member), Some(view(2, 3, 10)));
        // No interest: no wake.
        let woken = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        record_metadata_member_view(&local, &shard, &member, Some(&view(2, 3, 11)));
        register_metadata_member_view_interest(&local, &shard, &member);
        record_metadata_member_view(&local, &shard, &member, Some(&view(2, 4, 11)));
        runtime.block_on(async {
            wake.notified().await;
            woken.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        assert!(woken.load(std::sync::atomic::Ordering::SeqCst));
        clear_metadata_member_view_interest(&local, &shard, &member);
        forget_metadata_member_views(&local, &shard);
        assert_eq!(metadata_member_view(&local, &shard, &member), None);
    }
}
