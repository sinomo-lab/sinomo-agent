use std::collections::{BTreeMap, BTreeSet};
use std::io::Read as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sinomo_core::events::EventStore as _;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::node::NodeId;
use sinomo_core::service::{ClusterAccess, ServiceContext, ServiceError};

use crate::*;

mod recovery;
#[cfg(test)]
mod recovery_tests;

pub use recovery::*;
#[cfg(test)]
pub(crate) use recovery::{register_test_recovery_scheduler, test_recovery_scheduler};

const SANDBOX_LIFECYCLE_RECOVERY_STATE_DIR: &str = "daemon/sandbox-lifecycle-recovery";

type ClusteredDfsService = sinomo_dfs::NetworkedDfsService<sinomo_dfs::DfsNodeTransport>;
type ClusteredInstanceStore = SifsSandboxInstanceStore<sinomo_dfs::DfsNodeTransport>;
type ClusteredExecutionLease = ClusteredSandboxExecutionLease<sinomo_dfs::DfsNodeTransport>;

/// Default component backend selection. Clustered requests use the DFS-owned
/// runtime directly; local-only requests retain the standalone filesystem
/// backend.
pub struct ComponentSandboxBackendFactory {
    runtime: Option<tokio::runtime::Handle>,
    lifecycle_recovery: Option<SandboxLifecycleRecoveryScheduler>,
}

impl ComponentSandboxBackendFactory {
    pub fn new(runtime: Option<tokio::runtime::Handle>) -> Self {
        Self {
            runtime,
            lifecycle_recovery: None,
        }
    }

    /// Route fences observed by status and file reads to the component's
    /// recovery worker.
    pub fn with_lifecycle_recovery(mut self, scheduler: SandboxLifecycleRecoveryScheduler) -> Self {
        self.lifecycle_recovery = Some(scheduler);
        self
    }
}

impl SandboxBackendFactory for ComponentSandboxBackendFactory {
    fn open(&self, ctx: &ServiceContext) -> Result<Box<dyn SandboxBackend>, ServiceError> {
        let Some(cluster) = ctx.cluster.as_ref() else {
            return LocalSandboxBackendFactory.open(ctx);
        };
        let runtime = self.runtime.clone().ok_or_else(|| {
            ServiceError::Service(
                "clustered sandbox backend requires the component runtime".to_string(),
            )
        })?;
        let mut backend = ClusteredSandboxBackend::new(
            Arc::clone(&ctx.data_dir),
            Arc::clone(cluster),
            Some(runtime),
        );
        if let Some(scheduler) = &self.lifecycle_recovery {
            backend = backend.with_lifecycle_recovery(scheduler.clone());
        }
        Ok(Box::new(backend))
    }
}

#[derive(Clone)]
pub struct ClusteredSandboxBackend {
    data_dir: Arc<SinomoDataDir>,
    cluster: Arc<dyn ClusterAccess>,
    runtime: Option<tokio::runtime::Handle>,
    recovery: SandboxLifecycleRecoveryState,
    lifecycle_recovery: Option<SandboxLifecycleRecoveryScheduler>,
    /// Exact pending-fence index values this backend published, so settling
    /// its own transaction retires precisely that entry.
    published_fence_index: Arc<Mutex<BTreeMap<SandboxId, Vec<u8>>>>,
}

impl ClusteredSandboxBackend {
    pub fn new(
        data_dir: Arc<SinomoDataDir>,
        cluster: Arc<dyn ClusterAccess>,
        runtime: Option<tokio::runtime::Handle>,
    ) -> Self {
        let recovery = SandboxLifecycleRecoveryState::new(
            data_dir.root().join(SANDBOX_LIFECYCLE_RECOVERY_STATE_DIR),
        );
        Self {
            data_dir,
            cluster,
            runtime,
            recovery,
            lifecycle_recovery: None,
            published_fence_index: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_lifecycle_recovery(mut self, scheduler: SandboxLifecycleRecoveryScheduler) -> Self {
        self.lifecycle_recovery = Some(scheduler);
        self
    }

    fn recovery_state(&self) -> SandboxLifecycleRecoveryState {
        self.recovery.clone()
    }

    /// Hand a fence observed by a read-only request to the recovery worker.
    /// The request itself still reports the fence.
    fn schedule_recovery_for_observed_fence<R>(
        &self,
        sandbox_id: &SandboxId,
        result: &Result<R, SandboxBackendError>,
    ) {
        if let Err(SandboxBackendError::Store(SandboxStoreError::ReconciliationPending { .. })) =
            result
            && let Some(scheduler) = &self.lifecycle_recovery
        {
            scheduler.recover_observed(sandbox_id.clone());
        }
    }

    fn block_on_cluster<F: std::future::Future>(
        &self,
        future: F,
    ) -> Result<F::Output, SandboxBackendError> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            SandboxBackendError::Other(
                "clustered sandbox lifecycle obligations require the component runtime".to_string(),
            )
        })?;
        if let Ok(current) = tokio::runtime::Handle::try_current() {
            if matches!(
                current.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) {
                return Ok(tokio::task::block_in_place(|| runtime.block_on(future)));
            }
            return Err(SandboxBackendError::Other(
                "clustered sandbox lifecycle obligations cannot block a current-thread Tokio runtime"
                    .to_string(),
            ));
        }
        Ok(runtime.block_on(future))
    }

    /// sinomo:kv-consistency: Publish a pending authoritative sandbox fence before the fence exists so a live node can finish it when membership drops this node.
    fn publish_pending_fence_index(
        &self,
        record: &SandboxPendingFenceRecord,
    ) -> Result<(), SandboxBackendError> {
        let key = sandbox_pending_fence_key(&record.sandbox_id);
        let value = sinomo_core::kv::encode_kv_value(record)
            .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
        let request = sinomo_core::kv::KvTxnRequest::generated(sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![sinomo_core::kv::KvOp::Put {
                key,
                value: value.clone(),
                lease_id: None,
            }],
            failure: Vec::new(),
        });
        self.block_on_cluster(self.cluster.kv_commit_compact_async(request))?
            .map_err(|error| {
                SandboxBackendError::Other(format!(
                    "publish sandbox {} pending-fence index: {error}",
                    record.sandbox_id
                ))
            })?;
        self.published_fence_index
            .lock()
            .map_err(|_| {
                SandboxBackendError::Other("sandbox pending-fence index lock poisoned".to_string())
            })?
            .insert(record.sandbox_id.clone(), value);
        Ok(())
    }

    /// sinomo:kv-consistency: Retire a settled sandbox fence's index entry only while it still holds the value this lease holder published or observed.
    fn retire_pending_fence_index(
        &self,
        sandbox_id: &SandboxId,
    ) -> Result<(), SandboxBackendError> {
        let key = sandbox_pending_fence_key(sandbox_id);
        let published = self
            .published_fence_index
            .lock()
            .map_err(|_| {
                SandboxBackendError::Other("sandbox pending-fence index lock poisoned".to_string())
            })?
            .remove(sandbox_id);
        let expected = match published {
            Some(value) => value,
            None => {
                let Some(entry) = self
                    .cluster
                    .kv_get_local(&key)
                    .map_err(|error| SandboxBackendError::Other(error.to_string()))?
                    .value
                else {
                    return Ok(());
                };
                entry.value
            }
        };
        let request = sinomo_core::kv::KvTxnRequest::generated(sinomo_core::kv::KvTxn {
            compares: vec![sinomo_core::kv::KvCompare::ValueEquals {
                key: key.clone(),
                value: expected,
            }],
            success: vec![sinomo_core::kv::KvOp::Delete { key }],
            failure: Vec::new(),
        });
        self.block_on_cluster(self.cluster.kv_commit_compact_async(request))?
            .map(|_| ())
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
    }

    /// sinomo:kv-consistency: Decide which indexed fences a departed member left behind from quorum-confirmed membership and index reads; a lagging local view could miss them.
    fn orphaned_pending_fences(
        &self,
    ) -> Result<Vec<SandboxPendingFenceRecord>, SandboxBackendError> {
        let local_node_id = sinomo_core::cluster::read_cluster_state_without_kv(
            &sinomo_core::network::ClusterPaths::new(self.data_dir.as_ref()),
        )
        .map_err(|error| SandboxBackendError::Other(error.to_string()))?
        .local_node_id;
        let members = self
            .block_on_cluster(
                self.cluster
                    .kv_range_prefix_authoritative_async(CLUSTER_MEMBER_KV_PREFIX.to_string()),
            )?
            .map_err(|error| SandboxBackendError::Other(error.to_string()))?
            .value
            .into_iter()
            .map(|entry| {
                entry
                    .decode::<sinomo_core::ClusterMemberRecord>()
                    .map(|member| member.node_id)
            })
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
        let mut orphaned = Vec::new();
        for entry in self
            .block_on_cluster(
                self.cluster.kv_range_prefix_authoritative_async(
                    SANDBOX_PENDING_FENCE_KV_PREFIX.to_string(),
                ),
            )?
            .map_err(|error| SandboxBackendError::Other(error.to_string()))?
            .value
        {
            let record = entry
                .decode::<SandboxPendingFenceRecord>()
                .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
            if entry.key != sandbox_pending_fence_key(&record.sandbox_id) {
                return Err(SandboxBackendError::Other(format!(
                    "sandbox pending-fence index entry {} names sandbox {}",
                    entry.key, record.sandbox_id
                )));
            }
            if record.node_id != local_node_id && !members.contains(&record.node_id) {
                orphaned.push(record);
            }
        }
        Ok(orphaned)
    }

    fn load_policy_config(&self) -> Result<SandboxClusterPolicyConfig, SandboxBackendError> {
        let definition = sandbox_policy_config_definition()
            .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
        let entry = self
            .cluster
            .kv_get_local(&definition.cluster_key())
            .map_err(|error| SandboxBackendError::Other(error.to_string()))?
            .value;
        let entries = entry.into_iter().collect::<Vec<_>>();
        sandbox_policy_config_from_cluster_kv_entries(&entries)
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
            .map(Option::unwrap_or_default)
    }

    fn event_store(&self) -> sinomo_core::events::JsonlEventStore {
        sinomo_core::events::JsonlEventStore::new(
            self.data_dir.root().join("events/event-log.jsonl"),
        )
    }

    fn append_event(
        &self,
        event: sinomo_core::EventEnvelope,
        durable: bool,
    ) -> Result<(), SandboxBackendError> {
        let event_id = event.event_id.clone();
        let expected_kind = event.kind.clone();
        let expected_payload = event.payload.clone();
        let mut store = self.event_store();
        match store.append(event) {
            Ok(_) => {}
            Err(sinomo_core::events::EventStoreError::DuplicateEventId {
                event_id: duplicate,
            }) if durable && duplicate == event_id => {
                let existing = store
                    .replay(sinomo_core::events::ReplayQuery::all())
                    .map_err(|error| SandboxBackendError::Other(error.to_string()))?
                    .into_iter()
                    .find(|stored| stored.event.event_id == event_id)
                    .ok_or_else(|| {
                        SandboxBackendError::Other(format!(
                            "event store reported duplicate {event_id} but replay could not find it"
                        ))
                    })?;
                if existing.event.kind != expected_kind
                    || existing.event.payload != expected_payload
                {
                    return Err(SandboxBackendError::Other(format!(
                        "event {event_id} duplicate payload does not match authoritative sandbox transaction"
                    )));
                }
            }
            Err(error) => return Err(SandboxBackendError::Other(error.to_string())),
        }
        if durable {
            store
                .sync_all()
                .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
        }
        Ok(())
    }

    fn hooks(&self) -> ClusteredSandboxBackendHooks<'_> {
        ClusteredSandboxBackendHooks { backend: self }
    }

    fn open_dfs_service(&self) -> Result<ClusteredDfsService, SandboxBackendError> {
        sinomo_dfs::runtime_host::open_dfs_runtime_service(&self.data_dir, self.runtime.as_ref())
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
    }

    fn open_dfs_service_with_content(
        &self,
        content: sinomo_dfs::LocalContentStore,
    ) -> Result<ClusteredDfsService, SandboxBackendError> {
        sinomo_dfs::runtime_host::open_dfs_runtime_service_with_content(
            &self.data_dir,
            self.runtime.as_ref(),
            content,
        )
        .map_err(|error| SandboxBackendError::Other(error.to_string()))
    }

    fn open_content_store(&self) -> Result<sinomo_dfs::LocalContentStore, SandboxBackendError> {
        sinomo_dfs::runtime_host::open_dfs_runtime_content_store(&self.data_dir)
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
    }

    fn open_dfs_service_with_candidates(
        &self,
    ) -> Result<(ClusteredDfsService, Vec<NodeId>), SandboxBackendError> {
        let dfs = self.open_dfs_service()?;
        let candidates =
            sinomo_dfs::runtime_host::dfs_runtime_replica_candidates(&self.data_dir, dfs.node_id());
        Ok((dfs, candidates))
    }

    fn open_lifecycle_services(
        &self,
    ) -> Result<(ClusteredDfsService, ClusteredDfsService, Vec<NodeId>), SandboxBackendError> {
        let content = self.open_content_store()?;
        let lease_dfs = self.open_dfs_service_with_content(content.clone())?;
        let replica_candidates = sinomo_dfs::runtime_host::dfs_runtime_replica_candidates(
            &self.data_dir,
            lease_dfs.node_id(),
        );
        let dfs = self.open_dfs_service_with_content(content)?;
        Ok((lease_dfs, dfs, replica_candidates))
    }

    fn open_operation(
        &self,
        sandbox_id: &SandboxId,
        kind: &ClusteredSandboxOperationKind,
    ) -> Result<(ClusteredExecutionLease, ClusteredInstanceStore, Vec<NodeId>), SandboxBackendError>
    {
        let (lease_dfs, dfs, replica_candidates) = self.open_lifecycle_services()?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).open_operation(
            lease_dfs,
            dfs,
            replica_candidates,
            sandbox_id,
            kind,
        )
    }

    fn open_mutation(
        &self,
        sandbox_id: &SandboxId,
    ) -> Result<(ClusteredExecutionLease, ClusteredInstanceStore, Vec<NodeId>), SandboxBackendError>
    {
        self.open_operation(sandbox_id, &ClusteredSandboxOperationKind::Mutation)
    }

    fn open_transaction(
        &self,
        sandbox_id: &SandboxId,
        transaction: SandboxAuthoritativeTransaction,
    ) -> Result<(ClusteredExecutionLease, ClusteredInstanceStore, Vec<NodeId>), SandboxBackendError>
    {
        self.open_operation(
            sandbox_id,
            &ClusteredSandboxOperationKind::AuthoritativeTransaction(transaction),
        )
    }

    /// One event-triggered recovery attempt: finish whatever authoritative
    /// transaction is pending for `sandbox_id` under its lifecycle lease.
    pub fn recover_lifecycle_operation(
        &self,
        sandbox_id: &SandboxId,
    ) -> Result<ClusteredSandboxRecoveryAttempt, SandboxBackendError> {
        let (lease_dfs, dfs, replica_candidates) = self.open_lifecycle_services()?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).recover_operation(
            lease_dfs,
            dfs,
            replica_candidates,
            sandbox_id,
        )
    }

    /// Sandboxes this node marked before an authoritative fence could exist.
    pub fn marked_lifecycle_operations(&self) -> Result<BTreeSet<SandboxId>, SandboxBackendError> {
        self.recovery_state()
            .pending_sandbox_ids()
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
    }

    /// Indexed pending fences whose publishing node is no longer a member.
    pub fn orphaned_lifecycle_operations(&self) -> Result<Vec<SandboxId>, SandboxBackendError> {
        Ok(self
            .orphaned_pending_fences()?
            .into_iter()
            .map(|record| record.sandbox_id)
            .collect())
    }

    fn toolchain_store(
        &self,
    ) -> Result<SifsSandboxToolchainStore<sinomo_dfs::DfsNodeTransport>, SandboxBackendError> {
        let (dfs, candidates) = self.open_dfs_service_with_candidates()?;
        Ok(SifsSandboxToolchainStore::new(dfs).with_replica_candidates(candidates))
    }
}

struct ClusteredSandboxBackendHooks<'a> {
    backend: &'a ClusteredSandboxBackend,
}

impl ClusteredSandboxLifecycleHooks for ClusteredSandboxBackendHooks<'_> {
    type Error = SandboxBackendError;

    fn load_policy_config(&mut self) -> Result<SandboxClusterPolicyConfig, Self::Error> {
        self.backend.load_policy_config()
    }

    fn append_event(
        &mut self,
        event: sinomo_core::EventEnvelope,
        durability: SandboxAuditDurability,
    ) -> Result<(), Self::Error> {
        self.backend
            .append_event(event, durability == SandboxAuditDurability::Durable)
    }

    fn clear_authoritative_recovery_pending(
        &mut self,
        sandbox_id: &SandboxId,
        context: &'static str,
    ) {
        self.backend
            .recovery_state()
            .clear_authoritative_recovery_pending_best_effort(sandbox_id, context);
        if let Err(error) = self.backend.retire_pending_fence_index(sandbox_id) {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                context,
                %error,
                "failed to retire settled sandbox pending-fence index entry"
            );
        }
    }

    fn mark_authoritative_recovery_pending(
        &mut self,
        record: &SandboxPendingFenceRecord,
    ) -> Result<(), Self::Error> {
        self.backend
            .recovery_state()
            .mark_authoritative_recovery_pending(&record.sandbox_id)
            .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
        self.backend.publish_pending_fence_index(record)
    }

    fn claim_authoritative_recovery(&mut self, record: &SandboxPendingFenceRecord) {
        let result = self
            .backend
            .recovery_state()
            .mark_authoritative_recovery_pending(&record.sandbox_id)
            .map_err(|error| SandboxBackendError::Other(error.to_string()))
            .and_then(|()| self.backend.publish_pending_fence_index(record));
        if let Err(error) = result {
            tracing::warn!(
                sandbox_id = %record.sandbox_id,
                %error,
                "recovering a sandbox fence without taking over its recovery obligations"
            );
        }
    }

    fn invalid(&mut self, detail: String) -> Self::Error {
        SandboxBackendError::Other(detail)
    }
}

impl ClusteredSandboxToolchainHooks for ClusteredSandboxBackendHooks<'_> {
    type Error = SandboxBackendError;

    fn load_policy_config(&mut self) -> Result<SandboxClusterPolicyConfig, Self::Error> {
        self.backend.load_policy_config()
    }

    fn append_event(&mut self, event: sinomo_core::EventEnvelope) -> Result<(), Self::Error> {
        self.backend.append_event(event, false)
    }

    fn invalid(&mut self, detail: String) -> Self::Error {
        SandboxBackendError::Other(detail)
    }
}

impl SandboxBackend for ClusteredSandboxBackend {
    fn create(
        &mut self,
        request: SandboxCreateInstanceRequest,
    ) -> Result<SandboxCreateInstanceResponse, SandboxBackendError> {
        let sandbox_id = request
            .sandbox_id
            .clone()
            .unwrap_or_else(SandboxId::generate);
        let (lease, mut store, _) =
            self.open_transaction(&sandbox_id, SandboxAuthoritativeTransaction::Create)?;
        let mut hooks = self.hooks();
        let mut runtime = ClusteredSandboxLifecycleRuntime::new(&mut hooks);
        let result = runtime.create(&lease, &mut store, sandbox_id.clone(), request);
        if result.is_err() {
            runtime.retire_unfenced_obligations(&lease, &mut store, &sandbox_id);
        }
        result
    }

    fn status(
        &mut self,
        request: SandboxStatusRequest,
    ) -> Result<SandboxStatusResponse, SandboxBackendError> {
        let sandbox_id = request.sandbox_id.clone();
        let mut store = SifsSandboxInstanceStore::new(self.open_dfs_service()?);
        let mut hooks = self.hooks();
        let result = ClusteredSandboxLifecycleRuntime::new(&mut hooks).status(&mut store, request);
        self.schedule_recovery_for_observed_fence(&sandbox_id, &result);
        result
    }

    fn update(
        &mut self,
        request: SandboxUpdateInstanceRequest,
    ) -> Result<SandboxUpdateInstanceResponse, SandboxBackendError> {
        let (lease, store, candidates) = self.open_mutation(&request.sandbox_id)?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).update(&lease, store, candidates, request)
    }

    fn checkpoint(
        &mut self,
        request: SandboxCheckpointRequest,
    ) -> Result<SandboxCheckpointResponse, SandboxBackendError> {
        let (lease, mut store, _) = self.open_mutation(&request.sandbox_id)?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).checkpoint(&lease, &mut store, request)
    }

    fn resume(
        &mut self,
        request: SandboxResumeRequest,
    ) -> Result<SandboxResumeResponse, SandboxBackendError> {
        let sandbox_id = request.sandbox_id.clone();
        let (lease, mut store, _) = self.open_transaction(
            &sandbox_id,
            SandboxAuthoritativeTransaction::ResumeCheckpoint {
                checkpoint_id: request.checkpoint_id.clone(),
            },
        )?;
        let mut hooks = self.hooks();
        let mut runtime = ClusteredSandboxLifecycleRuntime::new(&mut hooks);
        let result = runtime.resume(&lease, &mut store, request);
        if result.is_err() {
            runtime.retire_unfenced_obligations(&lease, &mut store, &sandbox_id);
        }
        result
    }

    fn reboot(
        &mut self,
        request: SandboxRebootRequest,
    ) -> Result<SandboxRebootResponse, SandboxBackendError> {
        let sandbox_id = request.sandbox_id.clone();
        let (lease, mut store, _) =
            self.open_transaction(&sandbox_id, SandboxAuthoritativeTransaction::Reboot)?;
        let mut hooks = self.hooks();
        let mut runtime = ClusteredSandboxLifecycleRuntime::new(&mut hooks);
        let result = runtime.reboot(&lease, &mut store, request);
        if result.is_err() {
            runtime.retire_unfenced_obligations(&lease, &mut store, &sandbox_id);
        }
        result
    }

    fn destroy(
        &mut self,
        request: SandboxDestroyRequest,
    ) -> Result<SandboxDestroyResponse, SandboxBackendError> {
        let sandbox_id = request.sandbox_id.clone();
        let (lease, mut store, _) =
            self.open_transaction(&sandbox_id, SandboxAuthoritativeTransaction::Destroy)?;
        let mut hooks = self.hooks();
        let mut runtime = ClusteredSandboxLifecycleRuntime::new(&mut hooks);
        let result = runtime.destroy(&lease, &mut store, request);
        if result.is_err() {
            runtime.retire_unfenced_obligations(&lease, &mut store, &sandbox_id);
        }
        result
    }

    fn list(
        &mut self,
        request: SandboxListRequest,
    ) -> Result<SandboxListResponse, SandboxBackendError> {
        let sandbox_id = request.sandbox_id.clone();
        let store = SifsSandboxInstanceStore::new(self.open_dfs_service()?);
        let mut hooks = self.hooks();
        let result = ClusteredSandboxLifecycleRuntime::new(&mut hooks).list(store, request);
        self.schedule_recovery_for_observed_fence(&sandbox_id, &result);
        result
    }

    fn read(
        &mut self,
        request: SandboxReadRequest,
    ) -> Result<SandboxReadResponse, SandboxBackendError> {
        let sandbox_id = request.sandbox_id.clone();
        let store = SifsSandboxInstanceStore::new(self.open_dfs_service()?);
        let mut hooks = self.hooks();
        let result = ClusteredSandboxLifecycleRuntime::new(&mut hooks).read(store, request);
        self.schedule_recovery_for_observed_fence(&sandbox_id, &result);
        result
    }

    fn write(
        &mut self,
        request: SandboxWriteRequest,
    ) -> Result<SandboxWriteResponse, SandboxBackendError> {
        let (lease, store, candidates) = self.open_mutation(&request.sandbox_id)?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).write(&lease, store, candidates, request)
    }

    fn http_fetch(
        &mut self,
        request: SandboxHttpRequest,
    ) -> Result<SandboxHttpResponse, SandboxBackendError> {
        self.load_policy_config()?
            .policy
            .validate_http_request(&request)
            .map_err(|error| SandboxBackendError::PolicyDenied(error.to_string()))?;
        sandbox_http_fetch_blocking(request)
    }

    fn http_fetch_to_file(
        &mut self,
        request: SandboxHttpFetchToFileRequest,
    ) -> Result<SandboxHttpFetchFileResult, SandboxBackendError> {
        let (lease, store, candidates) = self.open_mutation(&request.sandbox_id)?;
        let mut hooks = self.hooks();
        ClusteredSandboxLifecycleRuntime::new(&mut hooks).http_fetch_to_file(
            &lease,
            store,
            candidates,
            &ComponentSandboxHttpTransport,
            request,
        )
    }

    fn import_busybox(
        &mut self,
        request: SandboxImportBusyBoxToolchainRequest,
    ) -> Result<SandboxImportBusyBoxToolchainResponse, SandboxBackendError> {
        let store = self.toolchain_store()?;
        let mut hooks = self.hooks();
        ClusteredSandboxToolchainRuntime::new(&mut hooks).import_busybox(store, request)
    }

    fn toolchain_add_command(
        &mut self,
        request: SandboxAddToolchainCommandRequest,
    ) -> Result<SandboxAddToolchainCommandResponse, SandboxBackendError> {
        let store = self.toolchain_store()?;
        let mut hooks = self.hooks();
        ClusteredSandboxToolchainRuntime::new(&mut hooks).add_command(store, request)
    }

    fn toolchain_add_artifact(
        &mut self,
        request: SandboxAddToolchainArtifactRequest,
    ) -> Result<SandboxAddToolchainArtifactResponse, SandboxBackendError> {
        let store = self.toolchain_store()?;
        let mut hooks = self.hooks();
        ClusteredSandboxToolchainRuntime::new(&mut hooks).add_artifact(store, request)
    }

    fn import_toolchain_package(
        &mut self,
        request: SandboxImportToolchainPackageRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
        let store = self.toolchain_store()?;
        let mut hooks = self.hooks();
        ClusteredSandboxToolchainRuntime::new(&mut hooks).import_package(store, request)
    }

    fn import_toolchain_package_from_path(
        &mut self,
        request: SandboxImportToolchainPackageFromPathRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
        let request = load_sandbox_toolchain_package_from_path(request)
            .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
        self.import_toolchain_package(request)
    }

    fn run_toolchain(
        &mut self,
        _request: SandboxRunToolchainRequest,
    ) -> Result<SandboxLocalCommandOutcome, SandboxBackendError> {
        Err(SandboxBackendError::Other(
            "clustered dynamic SIFS sandbox commands execute through an external sandbox executor or the live DFS/FUSE simple sandbox"
                .to_string(),
        ))
    }

    fn run_process_plan(
        &mut self,
        _request: SandboxRunProcessPlanRequest,
    ) -> Result<SandboxProcessPlanOutcome, SandboxBackendError> {
        Err(SandboxBackendError::Other(
            "clustered dynamic SIFS sandbox process plans execute through an external sandbox executor or the live DFS/FUSE simple sandbox"
                .to_string(),
        ))
    }
}

pub fn sandbox_http_fetch_blocking(
    request: SandboxHttpRequest,
) -> Result<SandboxHttpResponse, SandboxBackendError> {
    let timeout = Duration::from_millis(request.timeout_ms);
    let client = reqwest::blocking::Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
    let method = match request.method {
        SandboxHttpMethod::Get => reqwest::Method::GET,
        SandboxHttpMethod::Post => reqwest::Method::POST,
        SandboxHttpMethod::Put => reqwest::Method::PUT,
        SandboxHttpMethod::Delete => reqwest::Method::DELETE,
        SandboxHttpMethod::Head => reqwest::Method::HEAD,
    };
    let mut builder = client.request(method, &request.url).timeout(timeout);
    for (name, value) in &request.headers {
        builder = builder.header(name, value);
    }
    if !request.body.is_empty() {
        builder = builder.body(request.body);
    }
    let mut response = builder
        .send()
        .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
    if let Some(length) = response.content_length()
        && length > request.max_response_bytes
    {
        return Err(SandboxBackendError::Other(format!(
            "sandbox HTTP response is {length} bytes; max is {}",
            request.max_response_bytes
        )));
    }
    let status = response.status().as_u16();
    let mut headers = std::collections::BTreeMap::new();
    for (name, value) in response.headers() {
        if let Ok(value) = value.to_str() {
            headers.insert(name.as_str().to_string(), value.to_string());
        }
    }
    let mut body = Vec::new();
    response
        .by_ref()
        .take(request.max_response_bytes.saturating_add(1))
        .read_to_end(&mut body)
        .map_err(|error| SandboxBackendError::Other(error.to_string()))?;
    if body.len() as u64 > request.max_response_bytes {
        return Err(SandboxBackendError::Other(format!(
            "sandbox HTTP response exceeded max of {} bytes",
            request.max_response_bytes
        )));
    }
    Ok(SandboxHttpResponse {
        status,
        headers,
        body,
    })
}

struct ComponentSandboxHttpTransport;

impl SandboxHttpTransport for ComponentSandboxHttpTransport {
    fn fetch(
        &self,
        request: &SandboxHttpRequest,
    ) -> Result<SandboxHttpResponse, SandboxNetworkError> {
        sandbox_http_fetch_blocking(request.clone()).map_err(|error| {
            SandboxNetworkError::Transport {
                detail: error.to_string(),
            }
        })
    }
}

/// Compatibility entry points for direct integration harnesses. Production
/// daemon traffic uses [`crate::SandboxService`]; these helpers keep tests and
/// embedders on the owner crate while they migrate to component dispatch.
pub fn handle_daemon_sandbox_http_fetch(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxHttpRequest,
) -> Result<SandboxHttpResponse, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.http_fetch(request)
}

pub fn handle_daemon_sandbox_http_fetch_to_file(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxHttpFetchToFileRequest,
) -> Result<SandboxHttpFetchFileResult, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.http_fetch_to_file(request)
}

pub fn handle_daemon_sandbox_import_busybox(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxImportBusyBoxToolchainRequest,
) -> Result<SandboxImportBusyBoxToolchainResponse, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.import_busybox(request)
}

pub fn handle_daemon_sandbox_toolchain_add_command(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxAddToolchainCommandRequest,
) -> Result<SandboxAddToolchainCommandResponse, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.toolchain_add_command(request)
}

pub fn handle_daemon_sandbox_toolchain_add_artifact(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxAddToolchainArtifactRequest,
) -> Result<SandboxAddToolchainArtifactResponse, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.toolchain_add_artifact(request)
}

pub fn handle_daemon_sandbox_toolchain_import_package(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxImportToolchainPackageRequest,
) -> Result<SandboxImportToolchainPackageResponse, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.import_toolchain_package(request)
}

pub fn handle_daemon_sandbox_read(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxReadRequest,
) -> Result<SandboxReadResponse, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.read(request)
}

pub fn handle_daemon_sandbox_write(
    paths: &sinomo_api::DaemonPaths,
    request: SandboxWriteRequest,
) -> Result<SandboxWriteResponse, SandboxBackendError> {
    compatibility_clustered_backend(paths)?.write(request)
}

pub fn open_daemon_sandbox_dfs_service(
    paths: &sinomo_api::DaemonPaths,
) -> Result<ClusteredDfsService, SandboxBackendError> {
    let data_dir = compatibility_data_dir(paths)?;
    sinomo_dfs::runtime_host::open_dfs_runtime_service(&data_dir, None)
        .map_err(|error| SandboxBackendError::Other(error.to_string()))
}

fn compatibility_clustered_backend(
    paths: &sinomo_api::DaemonPaths,
) -> Result<ClusteredSandboxBackend, SandboxBackendError> {
    let data_dir = Arc::new(compatibility_data_dir(paths)?);
    let cluster: Arc<dyn ClusterAccess> = Arc::new(LocalSnapshotSandboxClusterAccess::new(
        Arc::clone(&data_dir),
    ));
    Ok(ClusteredSandboxBackend::new(data_dir, cluster, None))
}

fn compatibility_data_dir(
    paths: &sinomo_api::DaemonPaths,
) -> Result<SinomoDataDir, SandboxBackendError> {
    paths.root.parent().map(SinomoDataDir::new).ok_or_else(|| {
        SandboxBackendError::Other(format!(
            "daemon root {} has no data-directory parent",
            paths.root.display()
        ))
    })
}

struct LocalSnapshotSandboxClusterAccess {
    data_dir: Arc<SinomoDataDir>,
}

impl LocalSnapshotSandboxClusterAccess {
    fn new(data_dir: Arc<SinomoDataDir>) -> Self {
        Self { data_dir }
    }

    fn snapshot(&self) -> Result<sinomo_core::kv::KvSnapshot, sinomo_core::service::ServiceError> {
        sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot(
            &sinomo_core::network::ClusterPaths::new(self.data_dir.as_ref()),
        )
        .map_err(sinomo_core::service::ServiceError::Network)
    }

    fn state(
        &self,
    ) -> Result<sinomo_core::ClusterBootstrapState, sinomo_core::service::ServiceError> {
        sinomo_core::cluster::read_cluster_state_without_kv(
            &sinomo_core::network::ClusterPaths::new(self.data_dir.as_ref()),
        )
        .map_err(sinomo_core::service::ServiceError::Network)
    }
}

impl ClusterAccess for LocalSnapshotSandboxClusterAccess {
    fn kv_get_local(
        &self,
        key: &str,
    ) -> Result<
        sinomo_core::kv::KvLocalRead<Option<sinomo_core::kv::KvEntry>>,
        sinomo_core::service::ServiceError,
    > {
        let snapshot = self.snapshot()?;
        let state = self.state()?;
        Ok(sinomo_core::kv::KvLocalRead {
            value: snapshot.get(key).cloned(),
            revision: snapshot.revision,
            observed_term: state.term,
            applied_index: snapshot.revision.0,
            known_leader: Some(state.local_node_id),
        })
    }

    fn kv_range_prefix_local(
        &self,
        prefix: &str,
    ) -> Result<
        sinomo_core::kv::KvLocalRead<Vec<sinomo_core::kv::KvEntry>>,
        sinomo_core::service::ServiceError,
    > {
        let snapshot = self.snapshot()?;
        let state = self.state()?;
        let value = snapshot
            .range_prefix_limited(prefix, sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
            .map_err(sinomo_core::service::ServiceError::Kv)?;
        Ok(sinomo_core::kv::KvLocalRead {
            value,
            revision: snapshot.revision,
            observed_term: state.term,
            applied_index: snapshot.revision.0,
            known_leader: Some(state.local_node_id),
        })
    }

    fn kv_get_authoritative_async<'a>(
        &'a self,
        _key: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        sinomo_core::kv::KvAuthoritativeRead<Option<sinomo_core::kv::KvEntry>>,
                        sinomo_core::service::ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async {
            Err(sinomo_core::service::ServiceError::Service(
                "direct Sandbox compatibility access does not provide authoritative KV reads"
                    .to_string(),
            ))
        })
    }

    fn kv_commit_async<'a>(
        &'a self,
        _request: sinomo_core::kv::KvTxnRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        sinomo_core::kv::ClusterKvTxnCommitResult,
                        sinomo_core::service::ServiceError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async {
            Err(sinomo_core::service::ServiceError::Service(
                "direct Sandbox compatibility access does not provide cluster commits".to_string(),
            ))
        })
    }
}
