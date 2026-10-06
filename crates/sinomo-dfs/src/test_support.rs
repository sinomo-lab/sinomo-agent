//! Narrow fixtures and inspection hooks for tests in independent feature crates.
//! This module is absent from ordinary production builds.

use crate::*;
use sinomo_core::DfsWriteRequest;
use sinomo_core::node::NodeId;

pub fn object_version_record(
    object: &DfsObjectMetadata,
    size: Option<u64>,
) -> DfsObjectVersionRecord {
    crate::metadata_records::object_version_record(object, size)
}
pub fn write_intent_record(
    intent: &DistributedWriteIntent,
    hash: Option<&str>,
    object: Option<DfsObjectMetadata>,
) -> DfsWriteIntentRecord {
    crate::intent::write_intent_record(intent, hash, object)
}
pub fn recover_content_durability(
    version: &DfsObjectVersionRecord,
    backend: &dyn DfsMetadataBackend,
) -> Result<Option<Box<DfsContentDurability>>, MetadataBackendError> {
    crate::content_durability::recover_from_backend(version, backend)
}
pub fn retained_repair_intent_id(version: &DfsObjectVersionRecord) -> String {
    crate::file_history::retained_repair_intent_id(version)
}
pub fn content_copy_job_state(
    job: &DfsNamespaceJobRecord,
) -> Result<&DfsContentCopyJobState, DfsError> {
    crate::service_restore_jobs::content_copy_job_state(job)
}
pub fn content_copy_job_id(requester: &NodeId, operation: &str) -> String {
    crate::service_restore_jobs::content_copy_job_id(requester, operation)
}
pub fn stable_write_intent_id(requester: &NodeId, object: &DfsObjectId, operation: &str) -> String {
    crate::intent::stable_write_intent_id(requester, object, operation)
}
pub fn now_unix_nanos() -> u64 {
    crate::metadata_records::dfs_now_unix_nanos()
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn test_coordinator_mut(&mut self) -> &mut DistributedEventualCoordinator {
        &mut self.coordinator
    }
    pub fn test_transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }
    pub fn test_pending_write_contains(&self, intent: &str) -> bool {
        self.writes.pending_writes.contains_key(intent)
    }
    pub fn test_pending_write_versions(&self) -> Vec<DfsVersion> {
        self.writes
            .pending_writes
            .values()
            .map(|pending| pending.version.clone())
            .collect()
    }
    pub fn test_seed_pending_write(
        &mut self,
        intent: String,
        object: DfsObjectId,
        version: DfsVersion,
        hash: String,
        size: u64,
    ) {
        self.writes.pending_writes.insert(
            intent,
            crate::transfer::PendingNetworkedWrite::new(object, version, hash, size),
        );
    }
    pub fn test_has_active_replica_stage(&self, object: &DfsObjectId) -> bool {
        self.writes.active_replica_stages.contains_key(object)
    }
    pub fn test_begin_message_repair_job_capture(&mut self) {
        self.coordinator.begin_message_repair_job_capture();
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn test_open_verified_content_input(
        &mut self,
        identity: &DfsRevisionReference,
        expected_hash: &str,
        expected_size: u64,
    ) -> Result<VerifiedContentInput, DfsError> {
        self.open_verified_content_input(identity, expected_hash, expected_size)
    }
    pub fn test_retained_file_content_is_live(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<bool, DfsError> {
        self.retained_file_content_is_live(object_id, version, content_hash)
    }
    pub fn test_begin_content_copy_job(
        &mut self,
        path: &str,
        request: &DfsContentCopyRequest,
        options: &DfsWriteOptions,
    ) -> Result<DfsNamespaceJobRecord, DfsError> {
        self.begin_content_copy_job(path, request, options)
    }
    pub fn test_open_content_hash_stream(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        expected_size_bytes: Option<u64>,
    ) -> Result<(u64, ObjectVersionReader), DfsError> {
        self.open_content_hash_stream(object_id, version, content_hash, expected_size_bytes)
    }
    pub fn test_hydrate_from_metadata_backend_for_commit_promotion_recovery_until_cancelled(
        &mut self,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<bool, DfsError> {
        self.hydrate_from_metadata_backend_for_commit_promotion_recovery_until_cancelled(
            should_cancel,
        )
    }
    pub fn test_current_read_object_metadata(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectMetadata, DfsError> {
        self.current_read_object_metadata(object_id)
    }
    pub fn test_handle_stage_retained_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
        blob: Option<ContentStoreQuarantinedBlob>,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        self.handle_stage_retained_blob(requester_node_id, command, blob)
    }
    pub fn test_handle_repair_retained_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
        blob: Option<ContentStoreQuarantinedBlob>,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        self.handle_repair_retained_blob(requester_node_id, command, blob)
    }
    pub fn test_posix_copy_file_content_for_requester(
        &mut self,
        path: &str,
        request: &crate::DfsContentCopyRequest,
        input: VerifiedContentInput,
        options: DfsWriteOptions,
        requester: &NodeId,
        operation_id: &str,
    ) -> Result<DfsStat, DfsPosixError> {
        self.posix_copy_file_content_for_requester(
            path,
            request,
            input,
            options,
            requester,
            operation_id,
        )
    }
    pub fn test_begin_message_committed_work(&mut self) {
        self.begin_message_committed_work()
    }
    pub fn test_take_message_committed_work(&mut self) -> DfsMessageCommittedWork {
        self.take_message_committed_work()
    }
    pub fn test_reconcile_stale_conflicts_bounded(
        &mut self,
        max_records: usize,
    ) -> Result<DfsConflictMaintenanceReport, DfsError> {
        self.reconcile_stale_conflicts_bounded(max_records)
    }
    pub fn test_take_message_committed_repair_queue_records(
        &mut self,
    ) -> Vec<DfsRepairAttemptRecord> {
        self.take_message_committed_repair_queue_records()
    }
    pub fn test_activate_file_history_repair_roots(
        &mut self,
        roots: impl IntoIterator<Item = (DfsObjectId, DfsVersion)>,
    ) -> Result<(), DfsError> {
        self.activate_file_history_repair_roots(roots)
    }
    pub fn test_stage_missing_desired_replicas_for_intent(
        &mut self,
        intent_id: &str,
        object_metadata: DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        self.stage_missing_desired_replicas_for_intent(intent_id, object_metadata)
    }
    pub fn test_apply_committed_intent(&mut self, intent_id: &str) -> Result<(), DfsError> {
        self.apply_committed_intent(intent_id)
    }
    pub fn test_restore_pending_write_for_intent_if_missing(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
    ) -> Result<(), DfsError> {
        self.restore_pending_write_for_intent_if_missing(intent_id, object_id)
    }
    pub fn test_put_metadata_receipt(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
    ) -> Result<bool, DfsError> {
        self.put_metadata_receipt(
            file_system_id,
            object_id,
            version,
            replica_node_id,
            content_hash,
        )
    }
    pub fn test_put_visible_object_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        self.put_visible_object_in_metadata_backend(object)
    }
    pub fn test_put_visible_object_and_version_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
        size_bytes: Option<u64>,
    ) -> Result<(), DfsError> {
        self.put_visible_object_and_version_in_metadata_backend(object, size_bytes)
    }
    pub fn test_store_command_bytes(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        expected_hash: &str,
        bytes: &[u8],
    ) -> Result<(), DfsError> {
        self.store_command_bytes(object_id, version, expected_hash, bytes)
    }
    pub fn test_write_outcome(
        &self,
        intent_id: &str,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.write_outcome(intent_id)
    }
    pub fn test_write_distributed_bytes_with_base(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        options: TestDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.write_distributed_bytes_with_base(
            request,
            expected_base_version,
            bytes,
            desired_replica_nodes,
            policy,
            options.into(),
        )
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub enum TestWriteCreation {
    #[default]
    Upsert,
    Exclusive,
}
#[derive(Clone, Copy, Debug, Default)]
pub enum TestWriteRepair {
    #[default]
    Foreground,
    Background,
}
#[derive(Clone, Copy, Debug, Default)]
pub enum TestWriteDispatch {
    #[default]
    AwaitResponse,
    AwaitCommit,
    Detached,
}
#[derive(Clone, Debug, Default)]
pub struct TestDistributedWriteOptions {
    pub publication: DfsWritePublication,
    pub touch_parent_on_create: bool,
    pub creation: TestWriteCreation,
    pub repair: TestWriteRepair,
    pub dispatch: TestWriteDispatch,
    pub replay_response_journal: bool,
    pub operation_id: Option<String>,
}
impl From<TestDistributedWriteOptions> for crate::service_write::DfsDistributedWriteOptions {
    fn from(options: TestDistributedWriteOptions) -> Self {
        use crate::service_write::*;
        Self {
            publication: options.publication,
            touch_parent_on_create: options.touch_parent_on_create,
            creation: match options.creation {
                TestWriteCreation::Upsert => DfsWriteCreation::Upsert,
                TestWriteCreation::Exclusive => DfsWriteCreation::Exclusive,
            },
            repair: match options.repair {
                TestWriteRepair::Foreground => DfsWriteRepair::Foreground,
                TestWriteRepair::Background => DfsWriteRepair::Background,
            },
            dispatch: match options.dispatch {
                TestWriteDispatch::AwaitResponse => DfsWriteDispatch::AwaitResponse,
                TestWriteDispatch::AwaitCommit => DfsWriteDispatch::AwaitCommit,
                TestWriteDispatch::Detached => DfsWriteDispatch::Detached,
            },
            replay_response_journal: options.replay_response_journal,
            operation_id: options.operation_id,
        }
    }
}
impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    #[allow(clippy::too_many_arguments)]
    pub fn test_send_repair_replica_attachments_if_routable(
        &mut self,
        targets: &[(NodeId, String)],
        intent: &str,
        fs: &DfsFileSystemId,
        object: &DfsObjectId,
        base: Option<DfsVersion>,
        version: &DfsVersion,
        hash: &str,
        size: u64,
        metadata: Option<DfsObjectMetadata>,
    ) -> Result<Vec<bool>, DfsError> {
        self.send_repair_replica_attachments_if_routable(
            targets, intent, fs, object, base, version, hash, size, metadata,
        )
        .map(|outcomes| outcomes.into_iter().map(|outcome| outcome.sent).collect())
    }
}

pub fn write_intent_receipt(
    intent: &DfsWriteIntentRecord,
    retained_at: time::OffsetDateTime,
) -> Option<DfsWriteIntentReceipt> {
    DfsWriteIntentReceipt::from_intent(intent, retained_at)
}
impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn test_advance_repair_tick(&mut self, ticks: u64) {
        self.repair_retry_tick = self.repair_retry_tick.saturating_add(ticks);
    }
}
