//! Data-write durability survives metadata-only generations and retry GC.

use crate::{
    DfsContentDurability, DfsObjectVersionRecord, DfsReplicaReceiptRecord, DfsReplicationPolicy,
    DistributedWriteIntent,
};

pub(crate) fn from_intent(intent: &DistributedWriteIntent) -> Box<DfsContentDurability> {
    Box::new(DfsContentDurability {
        source_version: intent.target_version.clone(),
        coordinator_node_id: intent.requesting_node_id.clone(),
        owner_epoch: intent.owner_epoch,
        desired_replica_node_ids: intent.desired_replica_nodes.clone(),
        policy: intent.policy.clone(),
    })
}

/// Match an actual receipt to the complete target byte identity. The receipt's
/// owner is its metadata publisher; the recorded plan's coordinator is the
/// original requester. Neither node identity substitutes for this DATA join.
/// Callers retain their existing committed-plan and publication provenance checks.
pub(crate) fn receipt_matches_target_version(
    receipt: &DfsReplicaReceiptRecord,
    target: &DfsObjectVersionRecord,
    replica_node_id: &sinomo_core::node::NodeId,
) -> bool {
    target.size_bytes.is_some()
        && target.content_hash.as_ref().is_some_and(|hash| {
            receipt.replica_node_id == *replica_node_id
                && receipt.receipt.file_system_id == target.file_system_id
                && receipt.receipt.object_id == target.object_id
                && receipt.receipt.committed_version == target.version
                && receipt.content_hash.as_ref() == Some(hash)
        })
}

/// The local-eventual batch contract is exactly one durable caller copy.
/// Its receipt does not carry a distributed write-intent authority epoch.
pub(crate) fn from_local_receipt(receipt: &DfsReplicaReceiptRecord) -> Box<DfsContentDurability> {
    Box::new(DfsContentDurability {
        source_version: receipt.receipt.committed_version.clone(),
        coordinator_node_id: receipt.replica_node_id.clone(),
        owner_epoch: 0,
        desired_replica_node_ids: vec![receipt.replica_node_id.clone()],
        policy: DfsReplicationPolicy::new(1, 1).expect("single-replica batch contract"),
    })
}

/// A shared nonempty modification event proves a
/// metadata-only lineage; equal bytes from unrelated writes do not.
pub(crate) fn same_content_lineage(
    target: &DfsObjectVersionRecord,
    source: &DfsObjectVersionRecord,
) -> bool {
    target.file_system_id == source.file_system_id
        && target.object_id == source.object_id
        && target.content_hash.is_some()
        && target.content_hash == source.content_hash
        && target.size_bytes.is_some()
        && target.size_bytes == source.size_bytes
        && (target.version == source.version
            || (target.created_by_event_id.is_some()
                && target.created_by_event_id == source.created_by_event_id))
}
