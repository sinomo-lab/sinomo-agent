//! Narrow fixtures and inspection hooks for tests in independent feature crates.
//! This module is absent from ordinary production builds.

mod native_trace_artifact;
pub use native_trace_artifact::NativeShardTraceArtifactSession;

/// Bounded observations of actual metadata publication and replica responses.
/// This handle grants no authority and uses the existing packet/byte limits.
pub struct DfsTestMetadataBoundaryCapture {
    root: std::path::PathBuf,
    capture: crate::native_boundary_diagnostics::Capture,
}

impl DfsTestMetadataBoundaryCapture {
    pub fn checkpoint(&self, label: &str) {
        self.capture.checkpoint(label);
    }

    pub fn dump_replication_failure(
        &self,
        label: &str,
        assignment: &crate::DfsMetadataShardConfig,
        error: &str,
        observations: &[DfsTestRpcObservation],
    ) {
        crate::native_boundary_diagnostics::record(
            &self.root,
            "replication-failure-context",
            || serde_json::json!({"assignment":assignment,"error":error}),
            false,
        );
        for (ordinal, call) in observations.iter().enumerate() {
            crate::native_boundary_diagnostics::record(
                &self.root,
                "rpc-observation-at-failure",
                || {
                    serde_json::json!({
                        "ordinal":ordinal,"source":call.source,"target":call.target,
                        "route":call.route,"call_id":call.call_id,
                        "phase":format!("{:?}",call.phase),"origin":format!("{:?}",call.origin),
                        "error":call.outcome_error,"frame_id":call.dfs_frame_id,
                        "intent_id":call.dfs_intent_id,"file_system_id":call.dfs_file_system_id,
                        "object_id":call.dfs_object_id,"version":call.dfs_version,
                        "time_qualification":"dumped at failure; original RPC timestamps unavailable",
                    })
                },
                false,
            );
        }
        self.capture.dump(label, serde_json::Value::Null);
    }
}

/// Record actual test-observed migration decisions within the same bounded capture.
pub fn record_metadata_operation_boundary(
    root: &std::path::Path,
    kind: &str,
    facts: impl FnOnce() -> serde_json::Value,
) {
    crate::native_boundary_diagnostics::record(root, kind, facts, false);
}

pub fn capture_metadata_operation_boundaries(
    root: std::path::PathBuf,
) -> DfsTestMetadataBoundaryCapture {
    DfsTestMetadataBoundaryCapture {
        capture: crate::native_boundary_diagnostics::capture_all_operations(root.clone()),
        root,
    }
}

pub use crate::native_shard_trace::{
    NativeShardTraceCapture, NativeShardTraceRequest, NativeTraceArtifactBindingV1,
    capture_native_shard_trace, request_native_shard_trace,
};
pub use crate::posix::DfsDirectoryMoveScanRefusal;
pub use crate::source_adoption_test::{
    DfsSourcePausePoint as DfsTestSourcePausePoint,
    DfsSourcePreparationCapture as DfsTestSourcePreparationCapture,
};
pub use crate::test_runtime::{
    CapturedStageExchange as DfsTestCapturedStageExchange, CommitAckFault as DfsTestCommitAckFault,
    DfsTestControlPlaneProbe, DfsTestMaintenanceObservation, DfsTestNativeConfigurationFault,
    DfsTestNativeKvHookRegistration, DfsTestRuntime, FaultAction as DfsTestFaultAction,
    FaultRule as DfsTestFaultRule, NativeNamespaceReadCapture as DfsTestNamespaceReadCapture,
    Partition as DfsTestPartition, RpcObservation as DfsTestRpcObservation,
    RpcOrigin as DfsTestRpcOrigin, RpcPhase as DfsTestRpcPhase,
    SourceAdoptionFaultPhase as DfsTestSourceAdoptionFaultPhase,
    StageExchangeCapture as DfsTestStageExchangeCapture,
};
use crate::*;
use sinomo_core::DfsWriteRequest;

use sinomo_core::node::NodeId;

/// One ordinary hosted current metadata authority, retained with its real
/// embedded runtime. Paths select actual committed assignments; this utility
/// does not seed permissions, membership, acknowledgements or author guards.
pub fn current_native_metadata_service(
    name: &str,
    node_count: usize,
    path: &str,
) -> Result<
    (
        DfsTestRuntime,
        NetworkedDfsService<DfsNodeTransport>,
        DfsMetadataShardConfig,
    ),
    String,
> {
    current_native_metadata_service_on(name, node_count, path, false)
}

/// [`current_native_metadata_service`] with every node a cluster-KV voter,
/// so a lost node, the KV leader included, leaves a voter quorum.
pub fn current_native_metadata_service_with_kv_voters(
    name: &str,
    node_count: usize,
    path: &str,
) -> Result<
    (
        DfsTestRuntime,
        NetworkedDfsService<DfsNodeTransport>,
        DfsMetadataShardConfig,
    ),
    String,
> {
    current_native_metadata_service_on(name, node_count, path, true)
}

fn current_native_metadata_service_on(
    name: &str,
    node_count: usize,
    path: &str,
    kv_voters: bool,
) -> Result<
    (
        DfsTestRuntime,
        NetworkedDfsService<DfsNodeTransport>,
        DfsMetadataShardConfig,
    ),
    String,
> {
    let nodes = DfsTestRuntime::ordered_node_ids(name, node_count);
    let mut runtime = DfsTestRuntime::new(name, nodes[0].clone())?;
    if kv_voters {
        runtime.use_kv_voters()?;
    }
    for node in &nodes[1..] {
        runtime.define_node(node.clone())?;
    }
    runtime.start_nodes()?;
    let fs = runtime.file_system().file_system_id;
    let configs = runtime
        .shared_control_plane()?
        .with_client(|kv| DfsControlPlaneStore::new(kv.as_mut()).list_metadata_shards(&fs))
        .map_err(|error| error.to_string())?;
    let config = select_metadata_shard_config_for_path(&fs, &configs, path)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "normal runtime has no assignment for the fixture path".to_string())?;
    let service = runtime.client(&config.primary_node_id)?;
    Ok((runtime, service, config))
}

/// Check an actual handoff proof with the SAME production joint-ACK predicate.
/// This pure DATA validation neither publishes nor authorizes an activation.
pub fn metadata_handoff_proof_acknowledgements_are_safe(
    proof: &DfsMetadataShardMembershipHandoffProof,
) -> bool {
    crate::control_plane::metadata_shard_membership_handoff_acknowledgements_are_safe(
        &proof.retiring_shard,
        &proof.desired_shard,
        &proof.acknowledged_node_ids.iter().cloned().collect(),
    )
}

/// Publish a complete fixture body through the actual service-owned mint and
/// guarded current kernel. The caller supplies its genuine receipts/read set.
pub fn publish_current_payload<T: DfsMessageTransport>(
    service: &mut NetworkedDfsService<T>,
    payload: DfsPreparedOperationPayload,
) -> Result<(), DfsError> {
    service.publish_prepared_operation(payload).map(|_| ())
}

pub fn install_retention_grant(
    record: &mut DfsObjectVersionRecord,
    namespace: &str,
    object: &DfsObjectMetadata,
    granted_at_unix_nanos: u64,
    minimum: Option<DfsOrderedRetentionGrant>,
) {
    record.retention.insert(
        namespace.into(),
        DfsRevisionRetentionState {
            order: minimum.as_ref().map_or(0, |minimum| minimum.order),
            grant: Some(Box::new(DfsRevisionRetentionGrant {
                object: Box::new(object.clone()),
                granted_at_unix_nanos,
                origin: if minimum.is_some() {
                    DfsRetentionGrantOrigin::Rule
                } else {
                    DfsRetentionGrantOrigin::ExplicitOperation
                },
                minimum,
            })),
        },
    );
    record
        .validate_extension_metadata()
        .expect("retention fixture");
}

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
    /// Retain the actual opened backend while releasing its normal service,
    /// provider and data-owner capabilities. This binds no epoch or authority.
    pub fn test_into_retained_metadata_backend(
        mut self,
    ) -> Result<Box<dyn DfsMetadataBackend>, DfsError> {
        if self.publication_execution.current().is_some() {
            return Err(DfsError::Metadata {
                message: "cannot detach a backend during actual publication execution".into(),
            });
        }
        let backend = self
            .metadata_backend
            .take()
            .ok_or_else(|| DfsError::Metadata {
                message: "normal service has no actual metadata backend to retain".into(),
            })?;
        let crate::service_state::DfsMetadataBackendOwner::Owned(backend) = backend else {
            return Err(DfsError::Metadata {
                message: "retained backend fixture requires an independently owned backend, not a resident runtime lease".into(),
            });
        };
        drop(self);
        Ok(backend)
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
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

    pub fn test_has_active_replica_stage(&self, object: &DfsObjectId) -> bool {
        self.writes.active_replica_stages.contains_key(object)
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
        self.check_operation_cancellation("restore file history")?;
        if !self.is_distributed() {
            return self.begin_content_copy_job(path, request, options);
        }
        let shard = self
            .namespace_shards_for_exact_paths(&sifs_file_system_id(), [path])?
            .into_iter()
            .next()
            .ok_or_else(|| DfsError::Metadata {
                message: "content copy fixture has no actual destination authority".into(),
            })?;
        let result = self
            .route_namespace_operation_to_metadata_primary_with_request_id_and_options(
                crate::service_restore_jobs::begin_request_id(&self.node_id, &request.operation_id),
                sifs_file_system_id(),
                shard.primary_node_id,
                DfsNamespaceOperation::BeginContentCopy {
                    path: path.into(),
                    request: request.clone(),
                    options: options.clone(),
                },
                std::time::Duration::from_secs(60),
                false,
                false,
            )?;
        match (result.result, result.posix_coordination_result) {
            (DfsNamespaceResult::Applied, Some(DfsPosixCoordinationResult::NamespaceJob(job))) => {
                Ok(job)
            }
            (DfsNamespaceResult::Rejected { reason, .. }, _) => {
                Err(DfsError::Metadata { message: reason })
            }
            _ => Err(DfsError::Metadata {
                message: "actual content copy admission omitted its durable job".into(),
            }),
        }
    }

    pub fn test_resume_content_copy_job(
        &mut self,
        job: &DfsNamespaceJobRecord,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord, DfsError> {
        self.check_operation_cancellation("namespace job resume")?;
        if !self.is_distributed() {
            return self.run_content_copy_job(job.clone(), max_steps);
        }
        let shard = self
            .namespace_shards_for_exact_paths(&job.file_system_id, [job.root_path.as_str()])?
            .into_iter()
            .next()
            .ok_or_else(|| DfsError::Metadata {
                message: "content copy fixture has no actual continuation authority".into(),
            })?;
        let result = self.route_namespace_operation_to_metadata_primary_with_timeout_and_options(
            job.file_system_id.clone(),
            shard.primary_node_id,
            DfsNamespaceOperation::ResumeNamespaceJobBounded {
                job_id: job.job_id.clone(),
                path: job.root_path.clone(),
                max_steps: u64::try_from(max_steps).map_err(|_| DfsError::Metadata {
                    message: "content copy fixture step count overflows u64".into(),
                })?,
            },
            std::time::Duration::from_secs(60),
            false,
            false,
        )?;
        match (result.result, result.posix_coordination_result) {
            (DfsNamespaceResult::Applied, Some(DfsPosixCoordinationResult::NamespaceJob(job))) => {
                Ok(job)
            }
            (DfsNamespaceResult::Rejected { reason, .. }, _) => {
                Err(DfsError::Metadata { message: reason })
            }
            _ => Err(DfsError::Metadata {
                message: "actual content copy continuation omitted its durable job".into(),
            }),
        }
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
    /// Wait for the existing transport inbox only. This neither retries a
    /// write nor runs maintenance; callers retain their original deadline.
    pub fn test_wait_for_incoming(&mut self, timeout: std::time::Duration) -> Result<(), DfsError> {
        self.transport.wait_for_incoming(timeout).map(|_| ()).map_err(DfsError::from_network)
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
impl<T: DfsMessageTransport> NetworkedDfsService<T> {}

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

/// Encode manually supplied current rows for structural negative and wire
/// fixtures. This helper creates no state, ingestion permit or acknowledgement.
/// Healthy persistence fixtures read exact frames from their native backend.
pub fn structural_journal_frame_batch(
    batch: DfsMetadataJournalBatch,
) -> Result<DfsMetadataJournalFrameBatch, MetadataBackendError> {
    Ok(DfsMetadataJournalFrameBatch {
        shard_id: batch.shard_id,
        next_after_sequence: batch.next_after_sequence,
        journal_frames: batch
            .entries
            .iter()
            .map(crate::metadata_journal::encode_metadata_journal_frame_record)
            .collect::<Result<_, _>>()?,
    })
}

/// Construct the production replica command from actual source journal frames.
/// This grants no authority and never supplies an acknowledgement.
pub fn native_replica_journal_command(
    source: &crate::DfsMetadataShardConfig,
    target: &NodeId,
    frames: crate::DfsMetadataJournalFrameBatch,
) -> crate::DfsMetadataJournalBatchCommand {
    crate::metadata_replication::dfs_metadata_durable_journal_batch_command(
        &source.primary_node_id,
        target,
        &source.file_system_id,
        source.epoch,
        frames,
        false,
    )
}

/// Actual local OS data-owner lease for explicit standalone component tests.
/// This registers only the existing weak owner slot: no native host, cluster
/// client, preparation result, source certificate or storage authority is made.
pub struct StandaloneDataOwnerFixture {
    _owner: std::sync::Arc<crate::runtime_host::DfsDataOwnerLock>,
}
impl StandaloneDataOwnerFixture {
    pub fn acquire(context: &sinomo_module::ComponentContext) -> Result<Self, String> {
        if context.cluster_access().is_some()
            || context.host_role() == sinomo_module::ComponentHostRole::Shadow
            || crate::runtime_host::registered_component_cluster(context).map_err(|e| e.to_string())?.is_some() {
            return Err("standalone owner fixture requires an actual non-native context".into());
        }
        let paths = crate::runtime_host::DfsRuntimePaths::for_host_role(context.data_dir(), context.host_role());
        let owner = std::sync::Arc::new(crate::runtime_host::DfsDataOwnerLock::acquire(&paths).map_err(|e| e.to_string())?);
        crate::runtime_host::register_daemon_dfs_data_owner_lock(&paths, &owner).map_err(|e| e.to_string())?;
        Ok(Self { _owner: owner })
    }
}

/// Announce a namespace job event from `node`, as its coordinator's step
/// would: its listeners on that node hear it at once.
pub fn announce_namespace_job_event(
    node: &sinomo_core::node::NodeId,
    event: crate::DfsNamespaceJobEvent,
) {
    crate::namespace_job_events::announce_namespace_job_event(node, event);
}
