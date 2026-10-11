use sinomo_core::{Id, node::NodeId};
use time::OffsetDateTime;

use crate::{
    DfsDistributedError, DfsDistributedWriteCommand, DfsObjectId, DfsObjectMetadata, DfsVersion,
    DfsWriteIntentRecord, DfsWriteIntentState, DfsWriteIntentStatus, DistributedWriteIntent,
    content_hash,
};

/// Durable authority-local identity for one caller-owned write operation.
///
/// The wire request id is intentionally part of the identity: request retries
/// and authority redirects retain it, while the requester and object scopes
/// prevent two nodes (or two objects) from aliasing an otherwise identical
/// caller token.
pub(crate) fn stable_write_intent_id(
    requester_node_id: &NodeId,
    object_id: &DfsObjectId,
    operation_id: &str,
) -> String {
    let identity = format!("{requester_node_id}\0{object_id}\0{operation_id}");
    format!(
        "dfs_write_op_{}",
        &blake3::hash(identity.as_bytes()).to_hex().as_str()[..32]
    )
}

/// Extract the monotonic token carried by current caller-owned operation ids.
///
/// The prefix remains caller-defined; only the final lowercase ULID component
/// participates in retention floors. IDs without that token provide no ordered
/// admission evidence once their exact retained receipt is collected.
pub fn stable_operation_order_token(operation_id: &str) -> Option<String> {
    let candidate = operation_id.rsplit('_').next().unwrap_or(operation_id);
    Id::new(candidate.to_string())
        .ok()
        .map(|id| id.as_str().to_ascii_lowercase())
}

/// Stable target version selected by a caller-owned operation id.
///
/// A retry after a lost response may observe the just-committed version as the
/// current base. Deriving the target from the operation instead of that
/// mutable observation lets the metadata authority find and replay the exact
/// durable receipt after either side restarts.
pub fn stable_write_target_version(
    requester_node_id: &NodeId,
    object_id: &DfsObjectId,
    operation_id: &str,
) -> DfsVersion {
    let identity = format!("{requester_node_id}\0{object_id}\0{operation_id}");
    DfsVersion::derived("sinomo.dfs.write.operation.version.v1", identity.as_bytes())
}

/// Stable object identity for an exclusive create operation.
pub fn stable_create_object_id(
    requester_node_id: &NodeId,
    normalized_path: &str,
    operation_id: &str,
) -> DfsObjectId {
    let identity = format!("{requester_node_id}\0{normalized_path}\0{operation_id}");
    DfsObjectId::derived("sinomo.dfs.create.operation.object.v1", identity.as_bytes())
}

pub(crate) fn routed_write_command_content_hash(command: &DfsDistributedWriteCommand) -> String {
    command
        .content_hash
        .clone()
        .unwrap_or_else(|| content_hash(&command.bytes))
}

/// Match the durable fields which define a caller-owned operation replay.
///
/// The observed base version is deliberately excluded. A blind-write retry
/// after response loss sees the operation's committed target as current; the
/// durable operation id and content identity still bind it to the original
/// mutation. Compare-and-replace callers retain their explicit generation
/// fence at the POSIX boundary.
pub(crate) fn stable_distributed_intent_matches_routed_write_command(
    intent: &DistributedWriteIntent,
    requester_node_id: &NodeId,
    command: &DfsDistributedWriteCommand,
) -> bool {
    intent.publication == command.publication
        && intent.file_system_id == command.request.file_system_id
        && intent.object_id == command.request.object_id
        && intent.target_version == command.request.target_version
        && intent.requesting_node_id == *requester_node_id
        && command.request.requesting_node_id == *requester_node_id
        && matches!(
            intent.state,
            DfsWriteIntentState::AwaitingReceipts | DfsWriteIntentState::Committed
        )
}

pub(crate) fn stable_write_intent_record_matches_routed_write_command(
    record: &DfsWriteIntentRecord,
    requester_node_id: &NodeId,
    command: &DfsDistributedWriteCommand,
    expected_content_hash: &str,
) -> bool {
    record.publication == command.publication
        && record.file_system_id == command.request.file_system_id
        && record.object_id == command.request.object_id
        && record.target_version == command.request.target_version
        && record.coordinator_node_id == *requester_node_id
        && command.request.requesting_node_id == *requester_node_id
        && record.content_hash.as_deref() == Some(expected_content_hash)
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
        publication: intent.publication.clone(),
        intent_id: intent.intent_id.clone(),
        file_system_id: intent.file_system_id.clone(),
        object_id: intent.object_id.clone(),
        base_version: intent.base_version.clone(),
        target_version: intent.target_version.clone(),
        coordinator_node_id: intent.requesting_node_id.clone(),
        operation_id: intent.operation_id.clone(),
        operation_order_token: intent.operation_order_token.clone(),
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
        publication: record.publication.clone(),
        intent_id: record.intent_id.clone(),
        file_system_id: record.file_system_id.clone(),
        object_id: record.object_id.clone(),
        base_version: record.base_version.clone(),
        target_version: record.target_version.clone(),
        requesting_node_id: record.coordinator_node_id.clone(),
        operation_id: record.operation_id.clone(),
        operation_order_token: record.operation_order_token.clone(),
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
        && intent.publication == record.publication
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

#[cfg(test)]
mod stable_operation_tests {
    use super::*;
    use crate::{
        DfsDistributedWriteCommand, DfsFileSystemId, DfsReplicationPolicy,
        DistributedEventualCoordinator,
    };
    use sinomo_core::DfsWriteRequest;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("node id")
    }

    fn file_system_id() -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named("sifs-stable-write").expect("filesystem id")
    }

    fn object_id() -> DfsObjectId {
        DfsObjectId::from_legacy_named("object-stable-write").expect("object id")
    }

    fn command(
        requester: &NodeId,
        object_id: &DfsObjectId,
        operation_id: &str,
        content: &[u8],
    ) -> DfsDistributedWriteCommand {
        let target_version = stable_write_target_version(requester, object_id, operation_id);
        DfsDistributedWriteCommand {
            publication: Default::default(),
            patch: None,
            request_id: operation_id.to_string(),
            request: DfsWriteRequest {
                file_system_id: file_system_id(),
                object_id: object_id.clone(),
                target_version: target_version.clone(),
                requesting_node_id: requester.clone(),
            },
            // Models retry after the first response was dropped: the caller's
            // refreshed view now observes the committed target as its base.
            expected_base_version: Some(target_version),
            metadata_journal_after_sequence: None,
            bytes: content.to_vec(),
            content_hash: None,
            content_size_bytes: None,
            requester_blob_pre_staged: false,
            desired_replica_nodes: vec![requester.clone()],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            object_metadata: None,
            authority_shard: None,
            touch_parent_on_create: false,
            foreground_replica_repair: true,
            exclusive_create: false,
        }
    }

    #[test]
    fn applied_response_dropped_restart_replays_same_durable_identity() {
        let requester = node("node-stable-write-requester");
        let object_id = object_id();
        let operation_id = "editor-save-42";
        let initial_base =
            DfsVersion::from_legacy_named("version-stable-base").expect("base version");
        let target_version = stable_write_target_version(&requester, &object_id, operation_id);
        let intent_id = stable_write_intent_id(&requester, &object_id, operation_id);
        let policy = DfsReplicationPolicy::new(1, 1).expect("policy");

        let mut before_restart = DistributedEventualCoordinator::new();
        before_restart.set_committed_version(
            file_system_id(),
            object_id.clone(),
            initial_base.clone(),
        );
        let intent = before_restart
            .create_intent_at_epoch_with_parent_touch_and_id(
                file_system_id(),
                object_id.clone(),
                Some(initial_base),
                target_version.clone(),
                requester.clone(),
                7,
                vec![requester.clone()],
                policy,
                false,
                None,
                Some(intent_id.clone()),
                Some(operation_id.to_string()),
                stable_operation_order_token(operation_id),
            )
            .expect("create stable intent");
        before_restart
            .record_durable_receipt(&intent_id, requester.clone())
            .expect("record receipt");
        before_restart
            .commit_intent(&intent_id)
            .expect("commit before dropped response");
        let committed = before_restart
            .intent(&intent_id)
            .cloned()
            .expect("committed intent remains indexed");
        let content = b"durable content";
        let hash = content_hash(content);
        let durable_record = write_intent_record(&committed, Some(&hash), None);

        // Round-trip the durable record to model metadata recovery after the
        // authority process exits before its response reaches the requester.
        let recovered_record: DfsWriteIntentRecord = serde_json::from_slice(
            &serde_json::to_vec(&durable_record).expect("encode durable intent"),
        )
        .expect("decode durable intent after restart");
        let mut after_restart = DistributedEventualCoordinator::new();
        after_restart
            .restore_intent(distributed_intent_from_record(&recovered_record))
            .expect("restore committed intent");

        let retry = command(&requester, &object_id, operation_id, content);
        assert_eq!(retry.request.target_version, target_version);
        assert!(stable_write_intent_record_matches_routed_write_command(
            &recovered_record,
            &requester,
            &retry,
            &routed_write_command_content_hash(&retry),
        ));
        let replayed = after_restart.intent(&intent_id).expect("replayed intent");
        assert_eq!(replayed.intent_id, intent.intent_id);
        assert_eq!(replayed.operation_id.as_deref(), Some(operation_id));
        assert_eq!(replayed.target_version, target_version);
        assert_eq!(replayed.state, DfsWriteIntentState::Committed);
    }

    #[test]
    fn same_operation_id_with_different_content_is_not_a_replay() {
        let requester = node("node-stable-write-digest");
        let object_id = object_id();
        let operation_id = "upload-7";
        let original = command(&requester, &object_id, operation_id, b"first");
        let different = command(&requester, &object_id, operation_id, b"second");
        let mut record = DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: stable_write_intent_id(&requester, &object_id, operation_id),
            file_system_id: file_system_id(),
            object_id: object_id.clone(),
            base_version: None,
            target_version: original.request.target_version.clone(),
            coordinator_node_id: requester.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 1,
            desired_replica_node_ids: vec![requester.clone()],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Committed,
            content_hash: Some(routed_write_command_content_hash(&original)),
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };
        assert!(!stable_write_intent_record_matches_routed_write_command(
            &record,
            &requester,
            &different,
            &routed_write_command_content_hash(&different),
        ));
        record.content_hash = Some(routed_write_command_content_hash(&different));
        assert!(stable_write_intent_record_matches_routed_write_command(
            &record,
            &requester,
            &different,
            &routed_write_command_content_hash(&different),
        ));
    }

    #[test]
    fn exclusive_create_identity_is_stable_and_operation_scoped() {
        let requester = node("node-stable-create");
        let first = stable_create_object_id(&requester, "/docs/report", "create-1");
        assert_eq!(
            first,
            stable_create_object_id(&requester, "/docs/report", "create-1")
        );
        assert_ne!(
            first,
            stable_create_object_id(&requester, "/docs/report", "create-2")
        );
        assert_ne!(
            stable_write_target_version(&requester, &first, "create-1"),
            stable_write_target_version(&requester, &first, "create-2")
        );
    }
}
