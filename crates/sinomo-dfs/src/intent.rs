use sinomo_core::node::NodeId;
use time::OffsetDateTime;

use crate::{
    DfsConflictReadView, DfsConflictRecord, DfsDistributedError, DfsDistributedWriteCommand,
    DfsObjectMetadata, DfsWriteIntentRecord, DfsWriteIntentState, DfsWriteIntentStatus,
    DistributedWriteIntent, content_hash,
};

pub(crate) fn routed_write_command_content_hash(command: &DfsDistributedWriteCommand) -> String {
    command
        .content_hash
        .clone()
        .unwrap_or_else(|| content_hash(&command.bytes))
}

pub(crate) fn distributed_intent_matches_routed_write_command(
    intent: &DistributedWriteIntent,
    requester_node_id: &NodeId,
    command: &DfsDistributedWriteCommand,
) -> bool {
    intent.file_system_id == command.request.file_system_id
        && intent.object_id == command.request.object_id
        && intent.base_version == command.expected_base_version
        && intent.target_version == command.request.target_version
        && intent.requesting_node_id == *requester_node_id
        && command.request.requesting_node_id == *requester_node_id
        // Primary-side normalization may inject the object's owner and widen
        // the effective policy. Reusing a stronger plan is safe; reusing an
        // intent that omits a requested node or weakens either threshold is
        // not.
        && command
            .desired_replica_nodes
            .iter()
            .all(|node_id| intent.desired_replica_nodes.contains(node_id))
        && intent.policy.min_commit_replicas >= command.policy.min_commit_replicas
        && intent.policy.desired_replicas >= command.policy.desired_replicas
        // The durable flag is gated by whether the authority actually created
        // a transient object. Existing-object writes may therefore carry a
        // caller `true` while the intent correctly stores `false`; the inverse
        // would lose a required create side effect and is not an exact replay.
        && (!intent.touch_parent_on_create || command.touch_parent_on_create)
        && matches!(
            intent.state,
            DfsWriteIntentState::AwaitingReceipts | DfsWriteIntentState::Committed
        )
}

pub(crate) fn write_intent_record_matches_routed_write_command(
    record: &DfsWriteIntentRecord,
    requester_node_id: &NodeId,
    command: &DfsDistributedWriteCommand,
    expected_content_hash: &str,
) -> bool {
    record.file_system_id == command.request.file_system_id
        && record.object_id == command.request.object_id
        && record.base_version == command.expected_base_version
        && record.target_version == command.request.target_version
        && record.coordinator_node_id == *requester_node_id
        && command.request.requesting_node_id == *requester_node_id
        && record.content_hash.as_deref() == Some(expected_content_hash)
        && command
            .desired_replica_nodes
            .iter()
            .all(|node_id| record.desired_replica_node_ids.contains(node_id))
        && record.policy.min_commit_replicas >= command.policy.min_commit_replicas
        && record.policy.desired_replicas >= command.policy.desired_replicas
        && (!record.touch_parent_on_create || command.touch_parent_on_create)
        && matches!(
            record.status,
            DfsWriteIntentStatus::Staging
                | DfsWriteIntentStatus::ReceiptsPending
                | DfsWriteIntentStatus::Committed
        )
}

pub(crate) fn write_intent_record(
    intent: &DistributedWriteIntent,
    content_hash: Option<&str>,
    object_metadata: Option<DfsObjectMetadata>,
) -> DfsWriteIntentRecord {
    let now = OffsetDateTime::now_utc();
    DfsWriteIntentRecord {
        intent_id: intent.intent_id.clone(),
        file_system_id: intent.file_system_id.clone(),
        object_id: intent.object_id.clone(),
        base_version: intent.base_version.clone(),
        target_version: intent.target_version.clone(),
        coordinator_node_id: intent.requesting_node_id.clone(),
        owner_epoch: intent.owner_epoch,
        desired_replica_node_ids: intent.desired_replica_nodes.clone(),
        policy: intent.policy.clone(),
        status: write_intent_status(&intent.state),
        content_hash: content_hash.map(ToOwned::to_owned),
        object_metadata,
        touch_parent_on_create: intent.touch_parent_on_create,
        create_predecessor_object_id: intent.create_predecessor_object_id.clone(),
        created_at: now,
        updated_at: Some(now),
    }
}

fn write_intent_status(state: &DfsWriteIntentState) -> DfsWriteIntentStatus {
    match state {
        DfsWriteIntentState::AwaitingReceipts => DfsWriteIntentStatus::ReceiptsPending,
        DfsWriteIntentState::Committed => DfsWriteIntentStatus::Committed,
        DfsWriteIntentState::Rejected => DfsWriteIntentStatus::Rejected,
        DfsWriteIntentState::Conflict => DfsWriteIntentStatus::Conflict,
        DfsWriteIntentState::Aborted => DfsWriteIntentStatus::Aborted,
    }
}

pub(crate) fn write_intent_status_is_active(status: &DfsWriteIntentStatus) -> bool {
    matches!(
        status,
        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
    )
}

pub(crate) fn distributed_intent_from_record(
    record: &DfsWriteIntentRecord,
) -> DistributedWriteIntent {
    DistributedWriteIntent {
        intent_id: record.intent_id.clone(),
        file_system_id: record.file_system_id.clone(),
        object_id: record.object_id.clone(),
        base_version: record.base_version.clone(),
        target_version: record.target_version.clone(),
        requesting_node_id: record.coordinator_node_id.clone(),
        owner_epoch: record.owner_epoch,
        desired_replica_nodes: record.desired_replica_node_ids.clone(),
        policy: record.policy.clone(),
        touch_parent_on_create: record.touch_parent_on_create,
        create_predecessor_object_id: record.create_predecessor_object_id.clone(),
        state: distributed_intent_state_from_status(&record.status),
    }
}

pub(crate) fn distributed_intent_matches_record(
    intent: &DistributedWriteIntent,
    record: &DfsWriteIntentRecord,
) -> bool {
    intent.file_system_id == record.file_system_id
        && intent.object_id == record.object_id
        && intent.target_version == record.target_version
        && intent.touch_parent_on_create == record.touch_parent_on_create
        && intent.create_predecessor_object_id == record.create_predecessor_object_id
}

fn distributed_intent_state_from_status(status: &DfsWriteIntentStatus) -> DfsWriteIntentState {
    match status {
        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending => {
            DfsWriteIntentState::AwaitingReceipts
        }
        DfsWriteIntentStatus::Committed => DfsWriteIntentState::Committed,
        DfsWriteIntentStatus::Rejected => DfsWriteIntentState::Rejected,
        DfsWriteIntentStatus::Conflict => DfsWriteIntentState::Conflict,
        DfsWriteIntentStatus::Aborted => DfsWriteIntentState::Aborted,
    }
}

pub(crate) fn write_intent_status_requires_hydrate_restore(status: &DfsWriteIntentStatus) -> bool {
    matches!(
        status,
        DfsWriteIntentStatus::Staging
            | DfsWriteIntentStatus::ReceiptsPending
            | DfsWriteIntentStatus::Committed
    )
}

pub(crate) fn distributed_error_is_stale_or_concurrent_write(error: &DfsDistributedError) -> bool {
    matches!(
        error,
        DfsDistributedError::BaseVersionConflict { .. }
            | DfsDistributedError::DuplicateIntent { .. }
            | DfsDistributedError::PendingIntentConflict { .. }
    )
}

impl From<DfsConflictRecord> for DfsConflictReadView {
    fn from(conflict: DfsConflictRecord) -> Self {
        Self {
            conflict_id: conflict.conflict_id,
            base_version: conflict.base_version,
            local_version: conflict.local_version,
            remote_version: conflict.remote_version,
            detected_by_node_id: conflict.detected_by_node_id,
            reason: conflict.reason,
            local_content_hash: conflict.local_content_hash,
            remote_content_hash: conflict.remote_content_hash,
            status: conflict.status,
        }
    }
}
