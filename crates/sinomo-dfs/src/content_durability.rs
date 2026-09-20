//! Data-write durability survives metadata-only generations and retry GC.

use crate::{
    DfsContentDurability, DfsReplicaReceiptRecord, DfsReplicationPolicy, DfsWriteIntentRecord,
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

pub(crate) fn from_record(intent: &DfsWriteIntentRecord) -> Box<DfsContentDurability> {
    Box::new(DfsContentDurability {
        source_version: intent.target_version.clone(),
        coordinator_node_id: intent.coordinator_node_id.clone(),
        owner_epoch: intent.owner_epoch,
        desired_replica_node_ids: intent.desired_replica_node_ids.clone(),
        policy: intent.policy.clone(),
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
