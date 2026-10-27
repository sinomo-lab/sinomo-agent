//! Real daemon lane/admission regressions for typed pre-execution retry.

use super::super::message_host::DaemonDfsPublishPlanningAttemptOutcome;
use super::super::message_host::{
    DaemonDfsPreDispatchTestHook, DaemonDfsPreDispatchTestPoint,
    install_daemon_dfs_pre_dispatch_test_hook,
};
use super::*;

mod replica_storage_scope;
mod response_custody;

struct RetryFixture {
    runtime: &'static tokio::runtime::Runtime,
    data_dir: SinomoDataDir,
    paths: DaemonPaths,
    cluster_paths: ClusterPaths,
    local: NodeId,
    remote: NodeId,
    shard: DfsMetadataShardConfig,
    _authority: Arc<DaemonDfsMetadataAuthorityGeneration>,
    scheduler: Arc<DaemonDfsMutationScheduler>,
    journal_sequence: u64,
    _owner: Arc<DaemonDataOwnerLock>,
    native: Mutex<Option<crate::test_runtime::DfsTestRuntime>>,
}
impl RetryFixture {
    fn new(name: &str) -> Self {
        let ids = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2);
        let local = ids[0].clone();
        let remote = ids[1].clone();
        let mut native = crate::test_runtime::DfsTestRuntime::new(name, local.clone()).unwrap();
        native.define_node(remote.clone()).unwrap();
        native.start_nodes().unwrap();
        let data_dir = native.data_dir(&local).clone();
        let paths = DaemonPaths::new(&data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let owner = require_daemon_dfs_data_owner_lock(&paths).unwrap();
        let shard = native
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|cfg| cfg.assignment == DfsMetadataShardAssignment::Root)
            .unwrap();
        assert_eq!(shard.primary_node_id, local);
        let runtime = crate::test_runtime::executor();
        let peer = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
            .unwrap()
            .unwrap();
        let authority = daemon_dfs_metadata_authority_generation_for_identity(
            &cluster_paths,
            &peer.cluster_id,
            &peer.local_node_id,
            Some(runtime.handle()),
        )
        .unwrap();
        let scheduler = daemon_dfs_mutation_scheduler(&data_dir).unwrap();
        let journal_sequence = LocalSobjMetadataBackend::open_for_inspection(
            DfsMetadataShardId::new(&shard.shard_id),
            crate::default_dfs_metadata_shards_root(&data_dir).join(&shard.shard_id),
        )
        .unwrap()
        .journal_last_sequence();
        let fixture = Self {
            runtime,
            data_dir,
            paths,
            cluster_paths,
            local,
            remote,
            shard,
            _authority: authority,
            scheduler,
            journal_sequence,
            _owner: owner,
            native: Mutex::new(Some(native)),
        };
        for frame in 1..=2 {
            let response = fixture
                .runtime
                .block_on(handle_daemon_dfs_message_async(
                    &fixture.paths,
                    fixture.command(frame, "pre-dispatch-warm"),
                    None,
                    None,
                    fixture.runtime.handle(),
                ))
                .unwrap();
            assert_eq!(
                namespace_result(&response),
                &sinomo_dfs::DfsNamespaceResult::Applied
            );
        }
        assert!(
            fixture
                ._authority
                .write_configuration_projection()
                .unwrap()
                .is_some()
        );
        fixture
    }
    fn command(&self, frame: u64, request: &str) -> DfsMessageCommand {
        let mut routed = test_dfs_lookup_routed_message(&self.remote, &self.local, frame, "/");
        let DfsNetworkMessage::NamespaceRequest(namespace) = &mut routed.message else {
            unreachable!();
        };
        namespace.authority_shards = vec![self.shard.clone()];
        test_dfs_message_command(request, routed)
    }

    fn warm_global_lane(&self) {
        // The reuse cases compare existing resident lanes. Run one ordinary
        // owned maintenance pass before their opening/identity baseline.
        self.native
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .maintenance_tick(&self.local, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .expect("normal global maintenance warm-up")
            .outcome
            .expect("ordinary maintenance completes before the provider-only test");
    }

    fn require_unchanged_journal(&self) {
        evict_daemon_dfs_message_service_cache(&self.data_dir)
            .expect("close resident lane before durable journal assertion");
        let backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            default_dfs_metadata_root(&self.data_dir),
        )
        .unwrap();
        assert_eq!(
            backend.journal_last_sequence(),
            self.journal_sequence,
            "read retries cannot create namespace effects"
        );
    }

    fn hook(
        &self,
        expected: DfsMessageCommand,
        changed_attempts: usize,
        late_error: bool,
        block_retry_admission: bool,
    ) -> (Arc<RetryObservations>, Arc<DaemonDfsPreDispatchTestHook>) {
        let observations = Arc::new(RetryObservations::default());
        let state = Arc::clone(&observations);
        let paths = self.cluster_paths.clone();
        let local = self.local.clone();
        let scheduler = Arc::clone(&self.scheduler);
        let runtime = self.runtime.handle().clone();
        let hook = Arc::new(
            move |point: DaemonDfsPreDispatchTestPoint<'_>,
                  command: Option<&DfsMessageCommand>,
                  preflight: Option<&DaemonDfsMetadataAuthorityPreflight>| {
                if command != Some(&expected) {
                    return Ok(());
                }
                match point {
                    DaemonDfsPreDispatchTestPoint::PublishPlanningAuthorityCaptured { .. }
                    | DaemonDfsPreDispatchTestPoint::BeforeContentCopyStep { .. }
                    | DaemonDfsPreDispatchTestPoint::BeforeCompletedPrimaryRebind { .. }
                    | DaemonDfsPreDispatchTestPoint::BeforeReplicaContextComparison
                    | DaemonDfsPreDispatchTestPoint::PublishGenerationAdmissionRequested { .. }
                    | DaemonDfsPreDispatchTestPoint::PublishGenerationWorkerStarted { .. }
                    | DaemonDfsPreDispatchTestPoint::AfterReport { .. } => {}
                    DaemonDfsPreDispatchTestPoint::ContentStoreCacheAccess
                    | DaemonDfsPreDispatchTestPoint::ExactLaneCacheAccess
                    | DaemonDfsPreDispatchTestPoint::AfterPreflightFence
                    | DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight
                    | DaemonDfsPreDispatchTestPoint::BeforeResidentBackendOpen => {}
                    DaemonDfsPreDispatchTestPoint::BeforePreflightFence => {
                        let attempt = state.selections.fetch_add(1, Ordering::SeqCst);
                        if attempt < changed_attempts {
                            let preflight =
                                preflight.ok_or_else(|| test_error("missing real preflight"))?;
                            let before = preflight.authority.generation();
                            let mut advertisement =
                                observed_provider_advertisement(preflight, &local)?;
                            advertisement.zone = Some(format!("before-fence-zone-{attempt}"));
                            configure_native_provider_zone(&paths, &advertisement.zone)?;
                            commit_current_configuration_delta(
                                &paths,
                                &local,
                                sinomo_core::kv::KvCommand::Op(
                                    sinomo_core::kv::KvOp::put_serde(
                                        sinomo_dfs::content_service_key(&local),
                                        &advertisement,
                                        None,
                                    )
                                    .map_err(|error| test_error(error.to_string()))?,
                                ),
                            )
                            .map_err(sinomo_dfs::DfsError::from_network)?;
                            if preflight.authority.generation() <= before {
                                return Err(test_error(
                                    "actual committed configuration did not advance before the fence",
                                ));
                            }
                        }
                    }
                    DaemonDfsPreDispatchTestPoint::BeforeAuthorityRefresh {
                        owner: "message",
                        ..
                    } => {
                        require_original(command, &expected)?;
                        state.refreshes.fetch_add(1, Ordering::SeqCst);
                        if !state.last_admission_released.load(Ordering::SeqCst) {
                            return Err(test_error(
                                "original admission still held during authority refresh",
                            ));
                        }
                        state.released_before_refresh.fetch_add(1, Ordering::SeqCst);
                        if block_retry_admission {
                            let deadline = state
                                .reach_deadline
                                .lock()
                                .unwrap()
                                .expect("original reach deadline was installed before the RPC");
                            let admission = super::super::block_on_dfs_cluster_future(&runtime, async {
                                tokio::time::timeout_at(deadline, scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive))
                                    .await
                                    .map_err(|_| sinomo_core::service::ServiceError::Service("original reach deadline elapsed waiting for native test blocker".into()))?
                                    .map_err(|error| sinomo_core::service::ServiceError::Service(error.to_string()))
                            }).map_err(|error| test_error(error.to_string()))?;
                            *state.test_admission.lock().unwrap() = Some(Arc::new(admission));
                        }
                        state.refresh_entered.notify_one();
                    }
                    DaemonDfsPreDispatchTestPoint::AuthorityNativePrefixRead { .. }
                    | DaemonDfsPreDispatchTestPoint::PublishPlanningAttemptStarted { .. }
                    | DaemonDfsPreDispatchTestPoint::PublishPlanningAttemptFinished { .. }
                    | DaemonDfsPreDispatchTestPoint::BeforeReplicaTransferClaim { .. }
                    | DaemonDfsPreDispatchTestPoint::BeforeAuthorityRefresh { .. }
                    | DaemonDfsPreDispatchTestPoint::BeforeRetainedRepairQueueEnrollment {
                        ..
                    }
                    | DaemonDfsPreDispatchTestPoint::AfterRetainedRepairQueueEnrollment {
                        ..
                    } => {}
                    DaemonDfsPreDispatchTestPoint::ReplicaAdmissionReleased { released } => {
                        state
                            .last_admission_released
                            .store(released, Ordering::SeqCst);
                    }
                    DaemonDfsPreDispatchTestPoint::BeforeExecute { admission_held } => {
                        require_original(command, &expected)?;
                        state.executions.fetch_add(1, Ordering::SeqCst);
                        if !admission_held {
                            return Err(test_error(
                                "execution did not reacquire its original admission",
                            ));
                        }
                    }
                    DaemonDfsPreDispatchTestPoint::AfterExecute => {
                        require_original(command, &expected)?;
                        state.completed_executions.fetch_add(1, Ordering::SeqCst);
                        if late_error {
                            // Deliberately the same text as the pre-dispatch error:
                            // only the typed unexecuted outcome is safe to retry.
                            return Err(test_error(format!(
                                "DFS metadata authority advanced after async preflight of generation {}",
                                preflight.map(|p| p.generation).unwrap_or(0)
                            )));
                        }
                    }
                }
                Ok(())
            },
        );
        (observations, hook)
    }

    fn provider_change_hook(
        &self,
        expected: DfsMessageCommand,
        changed_attempts: usize,
    ) -> (Arc<RetryObservations>, Arc<DaemonDfsPreDispatchTestHook>) {
        let observations = Arc::new(RetryObservations::default());
        let state = Arc::clone(&observations);
        let paths = self.cluster_paths.clone();
        let local = self.local.clone();
        let hook = Arc::new(
            move |point: DaemonDfsPreDispatchTestPoint<'_>,
                  command: Option<&DfsMessageCommand>,
                  preflight: Option<&DaemonDfsMetadataAuthorityPreflight>| {
                if command != Some(&expected) {
                    return Ok(());
                }
                match point {
                    DaemonDfsPreDispatchTestPoint::AfterPreflightFence => {
                        let attempt = state.selections.fetch_add(1, Ordering::SeqCst);
                        if attempt < changed_attempts {
                            let preflight = preflight
                                .ok_or_else(|| test_error("missing provider preflight"))?;
                            let before = preflight.authority.generation();
                            let storage = preflight.authority.storage_generation();
                            let mut advertisement =
                                observed_provider_advertisement(preflight, &local)?;
                            advertisement.zone = Some(format!("pre-execution-zone-{attempt}"));
                            configure_native_provider_zone(&paths, &advertisement.zone)?;
                            // This is a real contiguous committed provider delta.
                            // Its shared subscription updates all three proven
                            // scopes; installing a DFS-only prefix would miss it.
                            commit_current_configuration_delta(
                                &paths,
                                &local,
                                sinomo_core::kv::KvCommand::Op(
                                    sinomo_core::kv::KvOp::put_serde(
                                        sinomo_dfs::content_service_key(&local),
                                        &advertisement,
                                        None,
                                    )
                                    .map_err(|error| test_error(error.to_string()))?,
                                ),
                            )
                            .map_err(sinomo_dfs::DfsError::from_network)?;
                            if preflight.authority.generation() <= before
                                || preflight.authority.storage_generation() != storage
                                || preflight
                                    .authority
                                    .write_configuration_projection()?
                                    .is_none()
                            {
                                return Err(test_error(
                                    "provider delta did not advance only the complete authority fence",
                                ));
                            }
                        }
                    }
                    DaemonDfsPreDispatchTestPoint::AuthorityNativePrefixRead { .. } => {
                        state.native_prefix_reads.fetch_add(1, Ordering::SeqCst);
                    }
                    DaemonDfsPreDispatchTestPoint::BeforeAuthorityRefresh {
                        owner: "message",
                        ..
                    } => {
                        require_original(command, &expected)?;
                        state.refreshes.fetch_add(1, Ordering::SeqCst);
                        if !state.last_admission_released.load(Ordering::SeqCst) {
                            return Err(test_error(
                                "provider replan retained its original admission",
                            ));
                        }
                        state.released_before_refresh.fetch_add(1, Ordering::SeqCst);
                    }
                    DaemonDfsPreDispatchTestPoint::ReplicaAdmissionReleased { released } => {
                        state
                            .last_admission_released
                            .store(released, Ordering::SeqCst);
                    }
                    DaemonDfsPreDispatchTestPoint::BeforeExecute { admission_held } => {
                        require_original(command, &expected)?;
                        state.executions.fetch_add(1, Ordering::SeqCst);
                        if !admission_held {
                            return Err(test_error(
                                "provider replan did not reacquire its original admission",
                            ));
                        }
                    }
                    DaemonDfsPreDispatchTestPoint::AfterExecute => {
                        require_original(command, &expected)?;
                        state.completed_executions.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => {}
                }
                Ok(())
            },
        );
        (observations, hook)
    }
}

fn commit_current_configuration_delta(
    paths: &ClusterPaths,
    node: &NodeId,
    command: sinomo_core::kv::KvCommand,
) -> Result<(), sinomo_core::network::NetworkError> {
    let data_dir = SinomoDataDir::new(paths.root.parent().expect("actual node root"));
    let host = registered_dfs_runtime_host(&data_dir)
        .map_err(|error| sinomo_core::network::NetworkError::Io {
            operation: "native retry configuration owner".into(),
            detail: error.to_string(),
        })?
        .ok_or_else(|| sinomo_core::network::NetworkError::Io {
            operation: "native retry configuration owner".into(),
            detail: "actual registered host is unavailable".into(),
        })?;
    let state = sinomo_core::cluster::read_cluster_state_without_kv(paths).map_err(|error| {
        sinomo_core::network::NetworkError::Io {
            operation: "native retry configuration identity".into(),
            detail: error.to_string(),
        }
    })?;
    if &state.local_node_id != node {
        return Err(sinomo_core::network::NetworkError::Io {
            operation: "native retry configuration identity".into(),
            detail: "wrong actual native node".into(),
        });
    }
    let txn = match command {
        sinomo_core::kv::KvCommand::Op(op) => sinomo_core::kv::KvTxn {
            compares: Vec::new(),
            success: vec![op],
            failure: Vec::new(),
        },
        sinomo_core::kv::KvCommand::Txn(txn) => txn,
        sinomo_core::kv::KvCommand::Barrier => {
            return Err(sinomo_core::network::NetworkError::Io {
                operation: "native retry configuration commit".into(),
                detail: "configuration fixture requires an actual typed operation".into(),
            });
        }
    };
    let request = sinomo_core::kv::KvTxnRequest::new(sinomo_core::kv::KvRequestId::generate(), txn);
    let result = block_on_dfs_cluster_future(crate::test_runtime::executor().handle(), async {
        host.cluster
            .kv_commit_compact_async(request)
            .await
            .map_err(|error| sinomo_core::service::ServiceError::Service(error.to_string()))
    })
    .map_err(|error| sinomo_core::network::NetworkError::Io {
        operation: "native retry configuration commit".into(),
        detail: error.to_string(),
    })?;
    if !result
        .txn_response
        .is_some_and(|response| response.succeeded)
    {
        return Err(sinomo_core::network::NetworkError::Io {
            operation: "native retry configuration commit".into(),
            detail: "actual configuration CAS rejected".into(),
        });
    }
    Ok(())
}

#[derive(Default)]
struct RetryObservations {
    selections: AtomicUsize,
    refreshes: AtomicUsize,
    native_prefix_reads: AtomicUsize,
    injected_provider_edges: AtomicUsize,
    planning_attempts: Mutex<Vec<usize>>,
    planning_outcomes: Mutex<Vec<(usize, DaemonDfsPublishPlanningAttemptOutcome)>>,
    released_before_refresh: AtomicUsize,
    executions: AtomicUsize,
    completed_executions: AtomicUsize,
    test_admission: Mutex<Option<Arc<DaemonDfsMutationAdmission>>>,
    last_admission_released: std::sync::atomic::AtomicBool,
    reach_deadline: Mutex<Option<tokio::time::Instant>>,
    refresh_entered: tokio::sync::Notify,
}

fn test_error(message: impl Into<String>) -> sinomo_dfs::DfsError {
    sinomo_dfs::DfsError::Network {
        message: message.into(),
    }
}

fn configure_native_provider_zone(
    paths: &ClusterPaths,
    zone: &Option<String>,
) -> Result<(), crate::DfsError> {
    let data_dir = SinomoDataDir::new(
        paths.root.parent().ok_or_else(|| test_error("native fixture DATA root"))?,
    );
    let host = registered_dfs_runtime_host(&data_dir)
        .map_err(|error| test_error(error.to_string()))?
        .ok_or_else(|| test_error("native fixture provider owner is unavailable"))?;
    // Change the owning node's input as well as its real committed projection.
    // Otherwise normal reconciliation correctly repairs the injected derived
    // row back to the old zone and introduces an unintended second actor.
    *host.fixture_local_zone.lock().unwrap() = zone.clone();
    assert_eq!(daemon_dfs_local_zone(&data_dir), *zone);
    Ok(())
}

fn observed_provider_advertisement(
    preflight: &DaemonDfsMetadataAuthorityPreflight,
    local: &NodeId,
) -> Result<crate::DfsNodeAdvertisement, crate::DfsError> {
    let view = preflight
        .authority
        .write_configuration_projection()?
        .ok_or_else(|| test_error("actual provider projection is unavailable"))?;
    let key = crate::content_service_key(local);
    view.entries(&key)
        .and_then(|entries| entries.iter().find(|entry| entry.key == key))
        .ok_or_else(|| test_error("actual sampled provider advertisement is absent"))?
        .decode()
        .map_err(|error| test_error(error.to_string()))
}
fn require_original(
    actual: Option<&DfsMessageCommand>,
    expected: &DfsMessageCommand,
) -> Result<(), sinomo_dfs::DfsError> {
    if actual == Some(expected) {
        Ok(())
    } else {
        Err(test_error(
            "retry changed the original request/source/target/frame/shard/epoch payload",
        ))
    }
}
fn namespace_result(response: &DfsDataPlaneResponse) -> &sinomo_dfs::DfsNamespaceResult {
    response
        .response
        .outbound_messages
        .iter()
        .find_map(|message| match &message.message {
            DfsNetworkMessage::NamespaceResponse(response) => Some(&response.result),
            _ => None,
        })
        .expect("namespace response from real service execution")
}

#[test]
fn predispatch_authority_edge_retries_once_with_released_and_reacquired_admission() {
    let f = RetryFixture::new("predispatch-retry-once");
    let command = f.command(50, "predispatch-once");
    let (seen, hook) = f.hook(command.clone(), 1, false, false);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let response = f
        .runtime
        .block_on(handle_daemon_dfs_message_async(
            &f.paths,
            command,
            None,
            None,
            f.runtime.handle(),
        ))
        .expect("retry the unchanged unexecuted read");
    assert_eq!(
        namespace_result(&response),
        &sinomo_dfs::DfsNamespaceResult::Applied
    );
    assert_eq!(seen.selections.load(Ordering::SeqCst), 2);
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(seen.released_before_refresh.load(Ordering::SeqCst), 1);
    assert_eq!(seen.executions.load(Ordering::SeqCst), 1);
    assert_eq!(seen.completed_executions.load(Ordering::SeqCst), 1);
    f.require_unchanged_journal();
}

fn provider_delta_after_first_fence(changed_attempts: usize) {
    let f = RetryFixture::new(if changed_attempts == 1 {
        "postfence-provider-once"
    } else {
        "postfence-provider-churn"
    });
    let command = f.command(90, "postfence-provider-original");
    f.warm_global_lane();
    let retained_cache_slot = daemon_dfs_message_service_cache()
        .root(&f.data_dir.root())
        .unwrap();
    let capture_admission = f
        .runtime
        .block_on(
            f.scheduler
                .admit(&DfsMessageExecutionScope::GlobalExclusive),
        )
        .unwrap();
    let (primary, exact, scan_owner, storage_generation) = {
        let cache = retained_cache_slot.lock().unwrap();
        let entry = cache.as_ref().unwrap();
        (
            entry.primary_service.clone(),
            entry.exact_shard_services.clone(),
            Arc::clone(&entry.write_intent_maintenance_scan),
            entry.signature.metadata_storage_generation,
        )
    };
    assert!(
        primary.is_some(),
        "the global lane is resident before the provider-only comparison"
    );
    drop(capture_admission);
    let opens = daemon_dfs_test_message_service_lane_open_count(&f.data_dir);
    let (seen, hook) = f.provider_change_hook(command.clone(), changed_attempts);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let result = f.runtime.block_on(handle_daemon_dfs_message_async(
        &f.paths,
        command,
        None,
        None,
        f.runtime.handle(),
    ));
    if changed_attempts == 1 {
        assert_eq!(
            namespace_result(
                &result.expect(
                    "retry the same command after the second fence detects a provider delta"
                )
            ),
            &sinomo_dfs::DfsNamespaceResult::Applied
        );
        assert_eq!(seen.selections.load(Ordering::SeqCst), 2);
        assert_eq!(seen.refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(seen.executions.load(Ordering::SeqCst), 1);
        assert_eq!(seen.completed_executions.load(Ordering::SeqCst), 1);
    } else {
        assert!(
            result.is_err(),
            "continuous provider churn must exhaust the existing replan bound"
        );
        if seen.selections.load(Ordering::SeqCst) != 3 {
            eprintln!(
                "native provider churn boundary: root={} result={result:?} selections={} refreshes={} executions={} completed={} prefix_reads={} generation={} storage_generation={}",
                f.data_dir.root().display(),
                seen.selections.load(Ordering::SeqCst),
                seen.refreshes.load(Ordering::SeqCst),
                seen.executions.load(Ordering::SeqCst),
                seen.completed_executions.load(Ordering::SeqCst),
                seen.native_prefix_reads.load(Ordering::SeqCst),
                f._authority.generation(),
                f._authority.storage_generation(),
            );
        }
        assert_eq!(seen.selections.load(Ordering::SeqCst), 3);
        assert_eq!(seen.refreshes.load(Ordering::SeqCst), 2);
        assert_eq!(seen.executions.load(Ordering::SeqCst), 0);
        assert_eq!(seen.completed_executions.load(Ordering::SeqCst), 0);
    }
    assert_eq!(
        seen.released_before_refresh.load(Ordering::SeqCst),
        seen.refreshes.load(Ordering::SeqCst)
    );
    assert!(seen.last_admission_released.load(Ordering::SeqCst));
    {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&f.data_dir.root())
            .expect("actual root cache slot");
        assert!(
            Arc::ptr_eq(&retained_cache_slot, &cache_slot),
            "the active observed root slot remains exact"
        );
        let cache = cache_slot.lock().unwrap();
        let entry = cache.as_ref().unwrap();
        assert_eq!(
            entry.signature.metadata_storage_generation,
            storage_generation
        );
        assert!(Arc::ptr_eq(
            &scan_owner,
            &entry.write_intent_maintenance_scan
        ));
        match (primary.as_ref(), entry.primary_service.as_ref()) {
            (Some(before), Some(after)) => assert!(Arc::ptr_eq(before, after)),
            (None, None) => {}
            _ => panic!("provider-only replan changed the resident global service"),
        }
        assert_eq!(exact.len(), entry.exact_shard_services.len());
        for (key, before) in &exact {
            assert!(Arc::ptr_eq(
                before,
                entry.exact_shard_services.get(key).unwrap()
            ));
        }
    }
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&f.data_dir),
        opens
    );
    assert_eq!(
        seen.native_prefix_reads.load(Ordering::SeqCst),
        0,
        "this request's authority refresh must use the retained contiguous view"
    );
    // Release cloned owners before proving that no durable read effects exist.
    drop(primary);
    drop(exact);
    drop(retained_cache_slot);
    f.require_unchanged_journal();
}

#[test]
fn provider_delta_after_first_preflight_fence_replans_without_reopening_stores() {
    provider_delta_after_first_fence(1);
}

#[test]
fn provider_churn_after_first_preflight_fence_is_bounded_without_execution() {
    provider_delta_after_first_fence(usize::MAX);
}

#[test]
fn valid_preflight_content_store_open_failure_is_terminal_without_authority_replan() {
    let f = RetryFixture::new("preflight-real-store-error");
    assert!(
        f._authority
            .write_configuration_projection()
            .unwrap()
            .is_some()
    );
    let generation = f._authority.generation();
    evict_daemon_dfs_message_service_cache(&f.data_dir).unwrap();
    let content_root = f.data_dir.dfs_dir().join("content");
    fs::remove_dir_all(&content_root).unwrap();
    fs::write(
        &content_root,
        b"file blocks the real content-store directory",
    )
    .unwrap();
    let store_error = LocalContentStore::new(&content_root)
        .err()
        .expect("the real store rejects the directory conflict");
    assert!(matches!(
        sinomo_dfs::DfsError::from(store_error),
        sinomo_dfs::DfsError::ContentStore { .. }
    ));
    let command = f.command(91, "valid-preflight-real-backend-error");
    let (seen, hook) = f.hook(command.clone(), 0, false, false);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let error = f
        .runtime
        .block_on(handle_daemon_dfs_message_async(
            &f.paths,
            command,
            None,
            None,
            f.runtime.handle(),
        ))
        .unwrap_err();
    assert!(
        matches!(&error, super::super::DfsRuntimeError::Parse { .. }),
        "the terminal store failure retains the runtime's typed backend-error wrapper: {error}"
    );
    assert_eq!(f._authority.generation(), generation);
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 0);
    assert_eq!(seen.executions.load(Ordering::SeqCst), 0);
    assert_eq!(seen.completed_executions.load(Ordering::SeqCst), 0);
    assert!(seen.last_admission_released.load(Ordering::SeqCst));
    f.require_unchanged_journal();
}

#[test]
fn predispatch_retry_does_not_rewrite_original_obsolete_shard_epoch() {
    let f = RetryFixture::new("predispatch-original-stamp");
    let command = f.command(51, "predispatch-original-epoch");
    let changed = f
        .native
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .reassign_metadata_shard(
            &f.shard.file_system_id,
            &f.shard.shard_id,
            f.remote.clone(),
            vec![f.local.clone()],
        )
        .unwrap();
    assert!(changed.epoch > f.shard.epoch);
    let before = LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new(&f.shard.shard_id),
        crate::default_dfs_metadata_shards_root(&f.data_dir).join(&f.shard.shard_id),
    )
    .unwrap()
    .journal_last_sequence();
    let (seen, hook) = f.hook(command.clone(), 0, false, false);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let response = f
        .runtime
        .block_on(handle_daemon_dfs_message_async(
            &f.paths,
            command,
            None,
            None,
            f.runtime.handle(),
        ))
        .expect("service-level structured stale-stamp rejection");
    assert!(matches!(
        namespace_result(&response),
        sinomo_dfs::DfsNamespaceResult::Rejected { .. }
    ));
    assert!(seen.executions.load(Ordering::SeqCst) <= 1);
    let after = LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new(&f.shard.shard_id),
        crate::default_dfs_metadata_shards_root(&f.data_dir).join(&f.shard.shard_id),
    )
    .unwrap()
    .journal_last_sequence();
    assert_eq!(
        after, before,
        "the original obsolete command cannot publish any journal effect"
    );
}

#[test]
fn predispatch_churn_stops_after_three_attempts_and_two_refreshes_without_execution() {
    let f = RetryFixture::new("predispatch-retry-bound");
    let command = f.command(52, "predispatch-bounded");
    let (seen, hook) = f.hook(command.clone(), usize::MAX, false, false);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    assert!(
        f.runtime
            .block_on(handle_daemon_dfs_message_async(
                &f.paths,
                command,
                None,
                None,
                f.runtime.handle()
            ))
            .is_err()
    );
    assert_eq!(seen.selections.load(Ordering::SeqCst), 3);
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 2);
    assert_eq!(seen.released_before_refresh.load(Ordering::SeqCst), 2);
    assert_eq!(seen.executions.load(Ordering::SeqCst), 0);
    assert_eq!(seen.completed_executions.load(Ordering::SeqCst), 0);
    assert!(seen.last_admission_released.load(Ordering::SeqCst));
    f.require_unchanged_journal();
}

#[test]
fn after_execution_error_matching_stale_text_is_terminal_and_never_replayed() {
    let f = RetryFixture::new("predispatch-late-error");
    let command = f.command(53, "predispatch-terminal-execute");
    let (seen, hook) = f.hook(command.clone(), 0, true, false);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let error = f
        .runtime
        .block_on(handle_daemon_dfs_message_async(
            &f.paths,
            command,
            None,
            None,
            f.runtime.handle(),
        ))
        .unwrap_err();
    assert!(error.to_string().contains("advanced after async preflight"));
    assert_eq!(seen.selections.load(Ordering::SeqCst), 1);
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 0);
    assert_eq!(seen.executions.load(Ordering::SeqCst), 1);
    assert_eq!(seen.completed_executions.load(Ordering::SeqCst), 1);
    f.require_unchanged_journal();
}

#[test]
fn outer_rpc_timeout_cancels_pending_readmission_without_reset_or_late_execution() {
    let f = RetryFixture::new("predispatch-outer-timeout");
    let command = f.command(54, "predispatch-timeout");
    let (seen, hook) = f.hook(command.clone(), 1, false, true);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let result = f.runtime.block_on(async {
        let reach_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        *seen.reach_deadline.lock().unwrap() = Some(reach_deadline);
        let request = handle_daemon_dfs_message_async(&f.paths, command, None, None, f.runtime.handle());
        tokio::pin!(request);
        tokio::time::timeout_at(reach_deadline, async {
            tokio::select! {
                _ = seen.refresh_entered.notified() => {},
                result = &mut request => panic!("request completed before retry admission blocker: {result:?}"),
            }
        }).await.expect("reach the forced stale edge without a timing race");
        // The SAME RPC future is now known to be blocked on native admission.
        // A caller deadline must cancel it; no internal retry gets a replacement
        // deadline or detached owner which resumes afterward.
        tokio::time::timeout(Duration::from_millis(25), &mut request).await
    });
    assert!(
        result.is_err(),
        "same outer RPC deadline must expire while retry admission is held"
    );
    assert_eq!(seen.selections.load(Ordering::SeqCst), 1);
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(seen.released_before_refresh.load(Ordering::SeqCst), 1);
    assert_eq!(seen.executions.load(Ordering::SeqCst), 0);
    let blocker = seen
        .test_admission
        .lock()
        .unwrap()
        .take()
        .expect("actual native test blocker");
    let blocker_released = Arc::downgrade(&blocker);
    drop(blocker);
    assert!(
        blocker_released.upgrade().is_none(),
        "the test's own blocker released"
    );
    assert!(seen.last_admission_released.load(Ordering::SeqCst));
    f.runtime.block_on(tokio::task::yield_now());
    assert_eq!(
        seen.executions.load(Ordering::SeqCst),
        0,
        "releasing the test blocker must not resurrect the canceled command"
    );
    f.require_unchanged_journal();
}

#[test]
fn cancelled_pre_dispatch_retry_drops_owned_quarantine_without_binding_or_execution() {
    let f = RetryFixture::new("predispatch-quarantine-cancel");
    let mut routed = test_dfs_write_routed_message(
        &f.remote,
        &f.local,
        55,
        "/services/cancel-before-dispatch.bin",
    );
    let DfsNetworkMessage::DistributedWriteRequest(write) = &mut routed.message else {
        unreachable!();
    };
    let bytes = std::mem::take(&mut write.bytes);
    let hash = sinomo_dfs::content_hash(&bytes);
    let object = write.request.object_id.clone();
    let version = write.request.target_version.clone();
    write.requester_blob_pre_staged = false;
    write.content_hash = Some(hash.clone());
    write.content_size_bytes = Some(bytes.len() as u64);
    write.authority_shard = Some(f.shard.clone());
    let command = test_dfs_message_command("predispatch-quarantine", routed);
    let (seen, hook) = f.hook(command.clone(), 1, false, true);
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(&f.data_dir, hook);
    let (sender, body) =
        sinomo_core::network::ObjectRpcIncomingBody::test_channel(Some(bytes.len() as u64), 1);
    let attachment = DfsInboundAttachment {
        descriptor: sinomo_dfs::DfsRpcAttachmentDescriptor {
            total_size_bytes: bytes.len() as u64,
            content_hash: Some(hash.clone()),
        },
        body,
    };
    f.runtime
        .block_on(sender.send(bytes))
        .expect("buffer bounded attachment");
    drop(sender);
    let temporary_root = f.data_dir.dfs_dir().join("content/blob-tmp");
    let result = f.runtime.block_on(async {
        let reach_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        *seen.reach_deadline.lock().unwrap() = Some(reach_deadline);
        let request = handle_daemon_dfs_message_async(&f.paths, command, Some(attachment), None, f.runtime.handle());
        tokio::pin!(request);
        tokio::time::timeout_at(reach_deadline, async {
            tokio::select! {
                _ = seen.refresh_entered.notified() => {},
                result = &mut request => panic!("quarantined request completed before retry blocker: {result:?}"),
            }
        }).await.expect("ingest once and reach retry with owned quarantine");
        assert!(fs::read_dir(&temporary_root).unwrap().next().is_some(), "unexecuted command still owns private bytes while waiting");
        tokio::time::timeout(Duration::from_millis(25), &mut request).await
    });
    assert!(result.is_err());
    assert_eq!(seen.selections.load(Ordering::SeqCst), 1);
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(seen.executions.load(Ordering::SeqCst), 0);
    let blocker = seen
        .test_admission
        .lock()
        .unwrap()
        .take()
        .expect("actual native test blocker");
    let blocker_released = Arc::downgrade(&blocker);
    drop(blocker);
    assert!(
        blocker_released.upgrade().is_none(),
        "the test's own blocker released"
    );
    assert!(seen.last_admission_released.load(Ordering::SeqCst));
    assert!(
        fs::read_dir(&temporary_root).unwrap().next().is_none(),
        "canceling the RPC drops its unchanged quarantine rather than orphaning or binding it"
    );
    let content = LocalContentStore::new(f.data_dir.dfs_dir().join("content")).unwrap();
    assert!(
        content
            .read_manifest(object.as_str(), version.as_str())
            .is_err()
    );
    assert!(
        content
            .content_reservation_roots()
            .unwrap()
            .reservations
            .is_empty()
    );
    assert!(!content.has_blob(&hash).unwrap());
    f.require_unchanged_journal();
}

struct ProgressTestTransport(Option<tokio::io::DuplexStream>);

impl sinomo_core::network::ClusterObjectRpcAsyncHost for ProgressTestTransport {
    fn open_object_rpc_stream_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
        _health: sinomo_core::network::NetworkHealthPolicy,
    ) -> sinomo_core::network::ClusterObjectRpcFuture<
        'a,
        Box<dyn sinomo_core::network::NetworkDuplexStream>,
    > {
        let stream = self.0.take().expect("one test carrier");
        Box::pin(async move {
            Ok(Box::new(stream) as Box<dyn sinomo_core::network::NetworkDuplexStream>)
        })
    }
}

fn rpc_progress_survives_dfs_cache_access(attachment_ingress: bool) {
    use sinomo_core::network::{
        ClusterObjectRpcClient, ClusterObjectRpcOptions, DaemonRpcTimeouts, ObjectFrameIoOptions,
        ObjectRpcBodySource, ObjectRpcCall, ObjectRpcOutgoingResponse, ObjectRpcRequest,
        RpcPeerContext, accept_local_object_rpc_stream_carrier,
        read_local_object_rpc_carrier_prelude, serve_service_rpc_peer_stream,
    };
    const IDLE_TIMEOUT: Duration = Duration::from_millis(80);
    const CACHE_DELAY: Duration = Duration::from_millis(320);
    let name = if attachment_ingress {
        "rpc-progress-content-cache"
    } else {
        "rpc-progress-exact-cache"
    };
    let fixture = RetryFixture::new(name);
    let entered = Arc::new(AtomicUsize::new(0));
    let hook_entered = Arc::clone(&entered);
    let point = if attachment_ingress {
        DaemonDfsPreDispatchTestPoint::ContentStoreCacheAccess
    } else {
        DaemonDfsPreDispatchTestPoint::ExactLaneCacheAccess
    };
    let (command, bytes) = if attachment_ingress {
        let mut routed = test_dfs_write_routed_message(
            &fixture.remote,
            &fixture.local,
            81,
            "/services/progress-cache.bin",
        );
        let DfsNetworkMessage::DistributedWriteRequest(write) = &mut routed.message else {
            unreachable!();
        };
        write.authority_shard = Some(fixture.shard.clone());
        let bytes = std::mem::take(&mut write.bytes);
        (test_dfs_message_command(name, routed), bytes)
    } else {
        (fixture.command(82, name), Vec::new())
    };
    let expected = command.clone();
    let _hook = install_daemon_dfs_pre_dispatch_test_hook(
        &fixture.data_dir,
        Arc::new(move |observed, command, _| {
            if observed == point && command == Some(&expected) {
                hook_entered.fetch_add(1, Ordering::SeqCst);
                // Stall only this root at the actual cache-access boundary.
                // Holding the process-wide mutex would delay unrelated tests.
                std::thread::sleep(CACHE_DELAY);
            }
            Ok(())
        }),
    );
    let size = bytes.len() as u64;
    let request = ObjectRpcRequest::encode_body("/test/dfs-cache-progress", "dfs", &command)
        .expect("typed DFS request");
    let call = if attachment_ingress {
        ObjectRpcCall::new(request, ObjectRpcBodySource::bytes(bytes))
    } else {
        ObjectRpcCall::value(request)
    };
    let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
    let client_data_dir = fixture.data_dir.clone();
    // The client clock runs independently. Blocking the server's current-thread
    // runtime therefore produces a genuine client idle timeout in the regression.
    let client = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut transport = ProgressTestTransport(Some(client_stream));
            let mut rpc = ClusterObjectRpcClient::endpoint_only(&mut transport);
            rpc.call_loopback(
                &client_data_dir,
                "127.0.0.1:9",
                call,
                ClusterObjectRpcOptions::new(DaemonRpcTimeouts {
                    connect_timeout: Some(Duration::from_secs(3)),
                    idle_timeout: Some(IDLE_TIMEOUT),
                })
                .with_overall_timeout(Duration::from_secs(5)),
            )
            .await?
            .decode_body::<String>()
            .await
        })
    });
    let server_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let server_result = server_runtime.block_on(async {
        let mut server_stream = server_stream;
        let context = read_local_object_rpc_carrier_prelude(&mut server_stream).await?;
        let mut acceptor =
            accept_local_object_rpc_stream_carrier(Box::new(server_stream), &context)?;
        let stream = acceptor.accept().await?.expect("test logical RPC stream");
        let context = RpcPeerContext::unauthenticated_local(
            stream.session_id.clone(),
            stream.channel_binding,
        );
        serve_service_rpc_peer_stream(
            stream,
            &context,
            ObjectFrameIoOptions::idle_timeout(IDLE_TIMEOUT),
            |inbound, _| async {
                let (command, attachment) = if attachment_ingress {
                    let command = inbound.request.decode_body::<DfsMessageCommand>()?;
                    let attachment = DfsInboundAttachment {
                        descriptor: sinomo_dfs::DfsRpcAttachmentDescriptor {
                            total_size_bytes: size,
                            content_hash: Some("deliberately-invalid-test-hash".to_string()),
                        },
                        body: inbound.body,
                    };
                    (command, Some(attachment))
                } else {
                    (inbound.decode_body::<DfsMessageCommand>().await?, None)
                };
                let result = handle_daemon_dfs_message_async(
                    &fixture.paths,
                    command,
                    attachment,
                    None,
                    server_runtime.handle(),
                )
                .await;
                if attachment_ingress {
                    assert!(
                        result.unwrap_err().to_string().contains("hash mismatch"),
                        "attachment must reach storage validation after the stalled lookup"
                    );
                } else {
                    assert_eq!(
                        namespace_result(&result.expect("real stamped namespace lookup")),
                        &sinomo_dfs::DfsNamespaceResult::Applied
                    );
                }
                ObjectRpcOutgoingResponse::encode_value(
                    "/test/dfs-cache-progress",
                    "completed-cache-access".to_string(),
                )
            },
        )
        .await
    });
    let client_result = client.join().expect("client thread");
    assert_eq!(
        client_result.expect("RPC progress must prevent an unknown idle-timeout outcome"),
        "completed-cache-access"
    );
    server_result.expect("server completed framed response");
    assert_eq!(
        entered.load(Ordering::SeqCst),
        1,
        "one stalled cache lookup"
    );
    fixture.require_unchanged_journal();
}

#[test]
fn streamed_dfs_cache_access_keeps_rpc_progress_live() {
    rpc_progress_survives_dfs_cache_access(true);
}

#[test]
fn exact_dfs_cache_access_keeps_rpc_progress_live() {
    rpc_progress_survives_dfs_cache_access(false);
}

struct BlockedIngestOwnerDrop {
    _owner: Arc<DaemonDataOwnerLock>,
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

impl Drop for BlockedIngestOwnerDrop {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        self.release
            .recv_timeout(Duration::from_secs(5))
            .expect("release completed ingest worker");
    }
}

#[test]
fn cancelled_attachment_ingest_retains_os_data_owner_until_worker_finishes() {
    let root = temp_root("cancelled-ingest-data-owner");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().unwrap();
    let paths = DaemonPaths::new(&data_dir);
    let owner = Arc::new(DaemonDataOwnerLock::acquire(&paths).unwrap());
    let (entered, worker_finished) = std::sync::mpsc::sync_channel(1);
    let (release, released) = std::sync::mpsc::sync_channel(1);
    let guard = BlockedIngestOwnerDrop {
        _owner: Arc::clone(&owner),
        entered,
        release: released,
    };
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content")).unwrap();
    let source = NodeId::from_legacy_named("cancelled-ingest-owner-source").unwrap();
    let target = NodeId::from_legacy_named("cancelled-ingest-owner-target").unwrap();
    let mut routed =
        test_dfs_write_routed_message(&source, &target, 83, "/services/cancelled-ingest-owner.bin");
    let DfsNetworkMessage::DistributedWriteRequest(write) = &mut routed.message else {
        unreachable!();
    };
    write.bytes.clear();
    let command = test_dfs_message_command("cancelled-ingest-owner", routed);
    let (sender, body) = sinomo_core::network::ObjectRpcIncomingBody::test_channel(Some(100), 1);
    let attachment = DfsInboundAttachment {
        descriptor: sinomo_dfs::DfsRpcAttachmentDescriptor {
            total_size_bytes: 100,
            content_hash: None,
        },
        body,
    };
    let runtime = daemon_test_runtime("cancelled-attachment-owner");
    let ingest = runtime.spawn(
        crate::runtime_message::ingest_dfs_message_attachment_with_owner(
            Some(content),
            None,
            command,
            Some(attachment),
            guard,
            None,
        ),
    );
    runtime.block_on(async {
        // Exceed the ingest channel's bounded depth without finishing the body.
        // Producer progress proves the worker began consuming input.
        for _ in 0..32 {
            sender.send(vec![7]).await.expect("partial attachment");
        }
    });
    assert!(
        fs::read_dir(data_dir.dfs_dir().join("content/blob-tmp"))
            .unwrap()
            .next()
            .is_some(),
        "ingest has begun durable staging"
    );
    drop(owner);
    ingest.abort();
    assert!(runtime.block_on(ingest).unwrap_err().is_cancelled());
    worker_finished
        .recv_timeout(Duration::from_secs(3))
        .expect("cancelled feed closed its worker input");
    assert!(
        matches!(
            DaemonDataOwnerLock::acquire(&paths),
            Err(DaemonError::AlreadyRunning { .. })
        ),
        "an abandoned RPC cannot release the OS owner while ingest cleanup runs"
    );
    release.send(()).unwrap();
    // Runtime shutdown waits for the owned blocking worker, proving guard drop.
    drop(runtime);
    DaemonDataOwnerLock::acquire(&paths).expect("worker released the OS owner after cleanup");
}

fn publisher_provider_delta_before_plan(changed_attempts: usize, expire_handoff: bool) {
    let f = RetryFixture::new(if expire_handoff {
        "publish-planning-original-deadline"
    } else if changed_attempts == 1 {
        "publish-planning-provider-once"
    } else {
        "publish-planning-provider-churn"
    });
    f.warm_global_lane();
    let retained_cache_slot = daemon_dfs_message_service_cache()
        .root(&f.data_dir.root())
        .unwrap();
    let capture_admission = f
        .runtime
        .block_on(
            f.scheduler
                .admit(&DfsMessageExecutionScope::GlobalExclusive),
        )
        .unwrap();
    let (primary, exact, scan_owner, storage_generation) = {
        let cache = retained_cache_slot.lock().unwrap();
        let entry = cache.as_ref().unwrap();
        (
            entry.primary_service.clone(),
            entry.exact_shard_services.clone(),
            Arc::clone(&entry.write_intent_maintenance_scan),
            entry.signature.metadata_storage_generation,
        )
    };
    assert!(
        primary.is_some(),
        "the global lane is resident before the provider-only comparison"
    );
    drop(capture_admission);
    let opens = daemon_dfs_test_message_service_lane_open_count(&f.data_dir);
    let seen = Arc::new(RetryObservations::default());
    let state = Arc::clone(&seen);
    let paths = f.cluster_paths.clone();
    let local = f.local.clone();
    let hook: Arc<DaemonDfsPreDispatchTestHook> = Arc::new(move |point, command, preflight| {
        match point {
            DaemonDfsPreDispatchTestPoint::AuthorityNativePrefixRead { .. } => {
                assert!(command.is_none());
                state.native_prefix_reads.fetch_add(1, Ordering::SeqCst);
            }
            DaemonDfsPreDispatchTestPoint::PublishPlanningAttemptStarted { replan } => {
                assert!(command.is_none());
                state.planning_attempts.lock().unwrap().push(replan);
            }
            DaemonDfsPreDispatchTestPoint::PublishPlanningAttemptFinished { replan, outcome } => {
                assert!(command.is_none());
                state
                    .planning_outcomes
                    .lock()
                    .unwrap()
                    .push((replan, outcome));
            }
            DaemonDfsPreDispatchTestPoint::PublishPlanningAuthorityCaptured { admission_held } => {
                assert!(
                    command.is_none(),
                    "publication planning owns no served request replay"
                );
                let attempt = state.selections.fetch_add(1, Ordering::SeqCst);
                if attempt < changed_attempts {
                    let preflight =
                        preflight.ok_or_else(|| test_error("missing publisher certificate"))?;
                    // Production preflight reattaches its lifecycle host. This
                    // fixture owns the injected edge, so keep unrelated
                    // discovery/publisher owners out of its hook counters while
                    // preserving the real native reads and configuration floor.
                    let generation = preflight.generation;
                    let storage = preflight.authority.storage_generation();
                    assert!(admission_held, "planning owns its actual exact admission");
                    let mut advertisement = observed_provider_advertisement(preflight, &local)?;
                    advertisement.zone = Some(format!("publication-planning-zone-{attempt}"));
                    configure_native_provider_zone(&paths, &advertisement.zone)?;
                    commit_current_configuration_delta(
                        &paths,
                        &local,
                        sinomo_core::kv::KvCommand::Op(
                            sinomo_core::kv::KvOp::put_serde(
                                sinomo_dfs::content_service_key(&local),
                                &advertisement,
                                None,
                            )
                            .map_err(|error| test_error(error.to_string()))?,
                        ),
                    )
                    .map_err(sinomo_dfs::DfsError::from_network)?;
                    assert!(preflight.authority.generation() > generation);
                    assert_eq!(preflight.authority.storage_generation(), storage);
                    assert!(
                        preflight
                            .authority
                            .write_configuration_projection()?
                            .is_some()
                    );
                    state.injected_provider_edges.fetch_add(1, Ordering::SeqCst);
                }
            }
            DaemonDfsPreDispatchTestPoint::BeforeAuthorityRefresh {
                owner: "publish-planning",
                ..
            } => {
                // The outer planning loop has its own replan bound. Resident,
                // idle-publication and maintenance retries have separate owners.
                assert!(command.is_none());
                state.refreshes.fetch_add(1, Ordering::SeqCst);
                if !state.last_admission_released.load(Ordering::SeqCst) {
                    return Err(test_error(
                        "publisher replan retained its original planning admission",
                    ));
                }
                state.released_before_refresh.fetch_add(1, Ordering::SeqCst);
                if expire_handoff {
                    // Stall this same invocation's handoff past its ORIGINAL
                    // planning budget; unrelated native publishers stay active.
                    std::thread::sleep(
                        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT + Duration::from_millis(25),
                    );
                }
            }
            DaemonDfsPreDispatchTestPoint::ReplicaAdmissionReleased { released } => {
                state
                    .last_admission_released
                    .store(released, Ordering::SeqCst);
            }
            _ => {}
        }
        Ok(())
    });
    let prepare = super::super::publish_host::prepare_daemon_dfs_metadata_publish_with_replanning(
        Arc::clone(&f.scheduler),
        f.data_dir.clone(),
        f.cluster_paths.clone(),
        DaemonDfsMutationLaneKey {
            file_system_id: f.shard.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(f.shard.shard_id.clone()),
        },
        f.runtime.handle().clone(),
        Arc::clone(&f._owner),
        daemon_dfs_metadata_replication_options(),
        Some(hook),
    );
    let result = f.runtime.block_on(async {
        if expire_handoff {
            tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT + Duration::from_secs(2),
                prepare,
            )
            .await
            .expect("the expired original budget must not grant the successor a new budget")
        } else {
            prepare.await
        }
    });
    let attempts = seen.planning_attempts.lock().unwrap();
    let outcomes = seen.planning_outcomes.lock().unwrap();
    assert!(!attempts.is_empty() && attempts.len() <= 3);
    assert_eq!(*attempts, (0..attempts.len()).collect::<Vec<_>>());
    assert_eq!(
        outcomes.len(),
        attempts.len(),
        "every actual attempt has a typed result"
    );
    assert_eq!(
        outcomes
            .iter()
            .map(|(replan, _)| *replan)
            .collect::<Vec<_>>(),
        *attempts
    );
    if expire_handoff {
        let error = result.expect_err("the original planning deadline expired during handoff");
        assert!(error.contains("deadline elapsed"), "{error}");
        assert_eq!(seen.selections.load(Ordering::SeqCst), 1);
        assert_eq!(seen.injected_provider_edges.load(Ordering::SeqCst), 1);
        assert_eq!(seen.refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(
            outcomes.last().unwrap().1,
            DaemonDfsPublishPlanningAttemptOutcome::AuthorityStale
        );
    } else if changed_attempts == 1 {
        let plan =
            result.expect("a provider-only rollover can rebind before preparing publication");
        assert!(
            plan.is_some(),
            "the real resident shard produced immutable journal work"
        );
        drop(plan);
        assert_eq!(seen.selections.load(Ordering::SeqCst), 2);
        assert_eq!(seen.injected_provider_edges.load(Ordering::SeqCst), 1);
        assert_eq!(seen.refreshes.load(Ordering::SeqCst), attempts.len() - 1);
        assert_eq!(
            outcomes.last().unwrap().1,
            DaemonDfsPublishPlanningAttemptOutcome::Complete
        );
    } else {
        assert!(
            result.is_err(),
            "continuous rollover must exhaust the existing two-replan bound"
        );
        // A stale retained guard before the capture hook consumes a real outer
        // attempt too. Require the original three-attempt bound, while every
        // capture actually reached still commits its configured provider edge.
        assert_eq!(attempts.len(), 3);
        let captured = seen.selections.load(Ordering::SeqCst);
        assert!(captured > 0 && captured <= attempts.len());
        assert_eq!(
            seen.injected_provider_edges.load(Ordering::SeqCst),
            captured
        );
        assert_eq!(seen.refreshes.load(Ordering::SeqCst), 2);
    }
    let stale_count = outcomes
        .iter()
        .filter(|(_, outcome)| *outcome == DaemonDfsPublishPlanningAttemptOutcome::AuthorityStale)
        .count();
    assert_eq!(
        stale_count,
        if expire_handoff {
            attempts.len()
        } else if changed_attempts == 1 {
            attempts.len() - 1
        } else {
            3
        }
    );
    assert!(seen.refreshes.load(Ordering::SeqCst) <= 2);
    drop(outcomes);
    drop(attempts);
    assert_eq!(
        seen.released_before_refresh.load(Ordering::SeqCst),
        seen.refreshes.load(Ordering::SeqCst)
    );
    assert!(seen.last_admission_released.load(Ordering::SeqCst));
    {
        let cache_slot = daemon_dfs_message_service_cache()
            .root(&f.data_dir.root())
            .expect("actual root cache slot");
        assert!(
            Arc::ptr_eq(&retained_cache_slot, &cache_slot),
            "the active observed root slot remains exact"
        );
        let cache = cache_slot.lock().unwrap();
        let entry = cache.as_ref().unwrap();
        assert_eq!(
            entry.signature.metadata_storage_generation,
            storage_generation
        );
        assert!(Arc::ptr_eq(
            &scan_owner,
            &entry.write_intent_maintenance_scan
        ));
        match (primary.as_ref(), entry.primary_service.as_ref()) {
            (Some(before), Some(after)) => assert!(Arc::ptr_eq(before, after)),
            (None, None) => {}
            _ => panic!("provider-only publication replan replaced the resident global service"),
        }
        assert_eq!(exact.len(), entry.exact_shard_services.len());
        for (key, before) in &exact {
            assert!(Arc::ptr_eq(
                before,
                entry.exact_shard_services.get(key).unwrap()
            ));
        }
    }
    assert_eq!(
        daemon_dfs_test_message_service_lane_open_count(&f.data_dir),
        opens
    );
    assert_eq!(
        seen.native_prefix_reads.load(Ordering::SeqCst),
        0,
        "this planner invocation must not issue a native authority refresh read"
    );
    drop(primary);
    drop(exact);
    drop(retained_cache_slot);
    f.require_unchanged_journal();
}

#[test]
fn publisher_provider_delta_before_plan_replans_without_reopening_stores() {
    publisher_provider_delta_before_plan(1, false);
}

#[test]
fn publisher_provider_churn_exhausts_existing_preeffect_replan_bound() {
    publisher_provider_delta_before_plan(3, false);
}

#[test]
fn publisher_handoff_preserves_the_original_planning_deadline() {
    publisher_provider_delta_before_plan(1, true);
}

#[test]
fn publisher_missing_backend_at_unchanged_generation_stays_terminal() {
    let f = RetryFixture::new("publish-planning-stable-missing");
    let seen = Arc::new(RetryObservations::default());
    let state = Arc::clone(&seen);
    let data_dir = f.data_dir.clone();
    let hook: Arc<DaemonDfsPreDispatchTestHook> = Arc::new(move |point, _, preflight| {
        match point {
            DaemonDfsPreDispatchTestPoint::PublishPlanningAuthorityCaptured { admission_held } => {
                assert!(admission_held, "original planning admission held");
                state.selections.fetch_add(1, Ordering::SeqCst);
                let preflight = preflight.unwrap();
                let cache_slot = daemon_dfs_message_service_cache()
                    .root(&data_dir.root())
                    .expect("actual root cache slot");
                let mut cache = cache_slot.lock().unwrap();
                let entry = cache.as_mut().unwrap();
                assert_eq!(
                    entry.signature.metadata_authority_generation,
                    preflight.generation
                );
                entry.resident_metadata_backend = None;
            }
            DaemonDfsPreDispatchTestPoint::BeforeAuthorityRefresh {
                owner: "publish-planning",
                ..
            } => {
                state.refreshes.fetch_add(1, Ordering::SeqCst);
            }
            DaemonDfsPreDispatchTestPoint::ReplicaAdmissionReleased { released } => {
                state
                    .last_admission_released
                    .store(released, Ordering::SeqCst);
            }
            _ => {}
        }
        Ok(())
    });
    let result = f.runtime.block_on(
        super::super::publish_host::prepare_daemon_dfs_metadata_publish_with_replanning(
            Arc::clone(&f.scheduler),
            f.data_dir.clone(),
            f.cluster_paths.clone(),
            DaemonDfsMutationLaneKey {
                file_system_id: f.shard.file_system_id.clone(),
                shard_id: DfsMetadataShardId::new(f.shard.shard_id.clone()),
            },
            f.runtime.handle().clone(),
            Arc::clone(&f._owner),
            daemon_dfs_metadata_replication_options(),
            Some(hook),
        ),
    );
    let error =
        result.expect_err("an unchanged-generation backend failure cannot become staleness");
    assert!(
        error.contains("resident backend does not match retained authority")
            || error.contains("metadata publish has no resident backend"),
        "the injected stable missing backend returned: {error}"
    );
    assert_eq!(seen.selections.load(Ordering::SeqCst), 1);
    assert_eq!(seen.refreshes.load(Ordering::SeqCst), 0);
    assert!(seen.last_admission_released.load(Ordering::SeqCst));
    f.require_unchanged_journal();
}

// Shared read-only receipt join for actual remote begin/ACK publications.
struct ActualHandoffPublicationObserver {
    _subscription: sinomo_core::kv::KvCommittedObservationSubscription,
    receiver: std::sync::mpsc::Receiver<(
        u64,
        std::result::Result<crate::DfsMetadataShardMembershipHandoff, String>,
    )>,
    deadline: Instant,
}
impl ActualHandoffPublicationObserver {
    fn new(
        paths: &ClusterPaths,
        file_system: &crate::DfsFileSystemId,
        shard: &DfsMetadataShardId,
        deadline: Instant,
    ) -> Self {
        let key = crate::metadata_shard_membership_handoff_key(file_system, &shard.0);
        let (sender, receiver) = std::sync::mpsc::channel();
        let subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
            paths,
            move |event| {
                let Some(index) = event.applied_index else {
                    return;
                };
                let sinomo_core::kv::KvCommittedObservationValue::Delta(delta) = &event.value
                else {
                    return;
                };
                for state in delta.final_key_states.iter() {
                    if state.key == key {
                        let actual = state
                            .entry
                            .as_ref()
                            .ok_or_else(|| "actual handoff publication deleted its key".to_string())
                            .and_then(|entry| {
                                entry
                                    .decode::<crate::DfsMetadataShardMembershipHandoff>()
                                    .map_err(|error| error.to_string())
                            });
                        let _ = sender.send((index, actual));
                    }
                }
            },
        );
        Self {
            _subscription: subscription,
            receiver,
            deadline,
        }
    }
    fn wait_exact(&self, expected: &crate::DfsMetadataShardMembershipHandoff) {
        loop {
            let remaining = self.deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "actual remote handoff publication reached its original deadline"
            );
            let (index, actual) = self
                .receiver
                .recv_timeout(remaining)
                .expect("actual remote committed handoff receipt");
            assert!(
                index > 0,
                "actual remote receipt has an applied native index"
            );
            let actual =
                actual.expect("actual remote handoff DATA decodes through the normal schema");
            if actual == *expected {
                return;
            }
        }
    }
}

fn current_direct_replica_context(
    fixture: &RetryFixture,
) -> (
    SinomoDataDir,
    DaemonPaths,
    crate::DfsMetadataReplicaShardContext,
) {
    let data_dir = {
        let native = fixture.native.lock().unwrap();
        native.as_ref().unwrap().data_dir(&fixture.remote).clone()
    };
    let paths = DaemonPaths::new(&data_dir);
    let host = super::super::data_plane::DaemonDfsDataPlaneHost::new_on_runtime(
        paths.clone(),
        fixture.runtime.handle(),
    );
    let context = fixture
        .runtime
        .block_on(
            crate::DfsDataPlaneHost::retained_metadata_replica_shard_context(
                &host,
                Arc::new(data_dir.clone()),
                fixture.shard.file_system_id.clone(),
                DfsMetadataShardId::new(&fixture.shard.shard_id),
            )
            .expect("actual mounted Main provides replica context"),
        )
        .unwrap()
        .expect("actual complete native replica assignment");
    assert_eq!(context.local_node_id, fixture.remote);
    assert_eq!(context.shard, fixture.shard);
    assert!(context.shard.replica_node_ids.contains(&fixture.remote));
    (data_dir, paths, context)
}

#[test]
fn direct_replica_preflight_provider_edge_replans_before_one_backend_invocation() {
    let fixture = RetryFixture::new("direct-replica-preflight-provider-once");
    let (data_dir, paths, context) = current_direct_replica_context(&fixture);
    let scheduler = daemon_dfs_mutation_scheduler(&data_dir).unwrap();
    let selections = Arc::new(AtomicUsize::new(0));
    let provider_edges = Arc::new(AtomicUsize::new(0));
    let hook_provider_edges = Arc::clone(&provider_edges);
    let refreshes = Arc::new(AtomicUsize::new(0));
    let invocations = Arc::new(AtomicUsize::new(0));
    let caller_thread = std::thread::current().id();
    let hook_selections = Arc::clone(&selections);
    let hook_refreshes = Arc::clone(&refreshes);
    let hook_invocations = Arc::clone(&invocations);
    let admission_releases = Arc::new(AtomicUsize::new(0));
    let hook_admission_releases = Arc::clone(&admission_releases);
    let eviction_data_dir = data_dir.clone();
    let cluster_paths = ClusterPaths::new(&data_dir);
    let local = fixture.remote.clone();
    let hook = Arc::new(
        move |point: DaemonDfsPreDispatchTestPoint<'_>,
              _command: Option<&DfsMessageCommand>,
              preflight: Option<&DaemonDfsMetadataAuthorityPreflight>| {
            // This existing hook also observes background native publishers. Fault
            // only this synchronous direct-selection call on its actual caller thread.
            if std::thread::current().id() != caller_thread {
                return Ok(());
            }
            match point {
                DaemonDfsPreDispatchTestPoint::BeforeReplicaContextComparison => {
                    if hook_selections.fetch_add(1, Ordering::SeqCst) == 0 {
                        // Remove only the rebuildable cache while this request
                        // owns actual Exact admission, before cache selection.
                        evict_daemon_dfs_message_service_cache(&eviction_data_dir)
                            .map_err(|error| test_error(error.to_string()))?;
                    }
                }
                DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight => {
                    if hook_provider_edges
                        .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        let preflight = preflight
                            .ok_or_else(|| test_error("missing actual direct preflight"))?;
                        let before = preflight.authority.generation();
                        let storage = preflight.authority.storage_generation();
                        let mut advertisement = observed_provider_advertisement(preflight, &local)?;
                        advertisement.zone = Some("direct-preflight-provider-successor".into());
                        configure_native_provider_zone(&cluster_paths, &advertisement.zone)?;
                        let operation = sinomo_core::kv::KvOp::put_serde(
                            crate::content_service_key(&local),
                            &advertisement,
                            None,
                        )
                        .map_err(|error| test_error(error.to_string()))?;
                        let sinomo_core::kv::KvOp::Put { key, value, .. } = &operation else {
                            unreachable!("actual provider producer is a Put");
                        };
                        let expected_key = key.clone();
                        let expected_value = value.clone();
                        let actual_authority = Arc::clone(&preflight.authority);
                        let (delivered_tx, delivered_rx) = std::sync::mpsc::channel();
                        let subscription =
                            sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
                                &cluster_paths,
                                move |event| {
                                    if event.applied_index.is_none() {
                                        return;
                                    }
                                    let sinomo_core::kv::KvCommittedObservationValue::Delta(delta) =
                                        &event.value
                                    else {
                                        return;
                                    };
                                    if delta.final_key_states.iter().any(|state| {
                                        state.key == expected_key
                                            && state
                                                .entry
                                                .as_ref()
                                                .is_some_and(|entry| entry.value == expected_value)
                                    }) {
                                        let _ = delivered_tx.send((
                                            actual_authority.generation(),
                                            actual_authority.storage_generation(),
                                        ));
                                    }
                                },
                            );
                        commit_current_configuration_delta(
                            &cluster_paths,
                            &local,
                            sinomo_core::kv::KvCommand::Op(operation),
                        )
                        .map_err(crate::DfsError::from_network)?;
                        // A remote native commit can return before this receiver's
                        // real ordered publication arrives. Join that exact key/value
                        // delivery; no authority I/O, polling or fabricated delta.
                        let (observed_generation, observed_storage) = delivered_rx
                            .recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
                            .map_err(|error| {
                                test_error(format!("actual native provider delivery: {error}"))
                            })?;
                        drop(subscription);
                        assert!(observed_generation > before);
                        assert_eq!(observed_storage, storage);
                        assert!(
                            preflight
                                .authority
                                .retained_complete_prefix_revision_for_generation(
                                    preflight.authority.generation(),
                                )
                                .is_some(),
                            "the actual native delta left a ready successor"
                        );
                    }
                }
                DaemonDfsPreDispatchTestPoint::ReplicaAdmissionReleased { released } => {
                    assert!(released, "direct retry retained old mutation admission");
                    hook_admission_releases.fetch_add(1, Ordering::SeqCst);
                }
                DaemonDfsPreDispatchTestPoint::BeforeAuthorityRefresh {
                    owner: "replica", ..
                } => {
                    assert_eq!(hook_invocations.load(Ordering::SeqCst), 0);
                    // Inspect this request's real guard, since a queued ordinary
                    // global writer may acquire the scheduler after its release.
                    assert_eq!(
                        hook_admission_releases.load(Ordering::SeqCst),
                        hook_refreshes.load(Ordering::SeqCst) + 1,
                        "direct retry must release its old admission before authority refresh"
                    );
                    hook_refreshes.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }
            Ok(())
        },
    );
    let hook_guard = install_daemon_dfs_pre_dispatch_test_hook(&data_dir, hook);
    let operation_invocations = Arc::clone(&invocations);
    let operation_scheduler = Arc::clone(&scheduler);
    let shard_id = DfsMetadataShardId::new(&context.shard.shard_id);
    let result = fixture
        .runtime
        .block_on(
            super::super::data_plane::handle_daemon_dfs_resident_replica_operation(
                &paths,
                Arc::new(data_dir.clone()),
                context,
                fixture.runtime.handle(),
                true,
                false,
                (
                    "actual-direct-preflight-provider-once",
                    "metadata-journal-batch",
                ),
                move |shard, _context| {
                    shard.with_backend(|backend| {
                        assert!(
                            operation_scheduler.try_admit_global().is_none(),
                            "actual admission is reacquired before the backend operation"
                        );
                        operation_invocations.fetch_add(1, Ordering::SeqCst);
                        Ok(crate::DfsMetadataBackend::metadata_prefix_proof(
                            backend, &shard_id,
                        ))
                    })
                },
            ),
        )
        .unwrap();
    match result {
        super::super::data_plane::DaemonDfsResidentReplicaOperation::Completed {
            result: proof,
        } => {
            assert!(
                proof.unwrap().is_some(),
                "inspect the real current receiver prefix"
            );
        }
        super::super::data_plane::DaemonDfsResidentReplicaOperation::Rejected { reason } => {
            panic!("unchanged actual replica context rejected: {reason}");
        }
    }
    assert_eq!(selections.load(Ordering::SeqCst), 2);
    assert_eq!(provider_edges.load(Ordering::SeqCst), 1);
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(admission_releases.load(Ordering::SeqCst), 1);
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    drop(hook_guard);
}

#[test]
fn direct_replica_reselects_actual_retirement_fence_before_one_backend_invocation() {
    let fixture = RetryFixture::new("direct-replica-actual-retirement-fence");
    let (data_dir, paths, initial) = current_direct_replica_context(&fixture);
    let publication_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let publication = ActualHandoffPublicationObserver::new(
        &ClusterPaths::new(&data_dir),
        &fixture.shard.file_system_id,
        &DfsMetadataShardId::new(&fixture.shard.shard_id),
        publication_deadline,
    );
    assert!(initial.pending_membership_handoff.is_none());
    // Normal constructors may join startup/configuration or rebind a frozen
    // shard. Complete them before retaining this same source admission.
    let (control, mut source) = {
        let native = fixture.native.lock().unwrap();
        let native = native.as_ref().unwrap();
        (
            native.shared_control_plane().unwrap(),
            native.metadata_primary_service("/").unwrap(),
        )
    };
    // Retain only this source shard's actual admission while capturing its
    // genuine unfenced context and durable freeze. Other nodes stay runnable.
    let source_admission = fixture
        .runtime
        .block_on(
            fixture
                .scheduler
                .admit(&DfsMessageExecutionScope::ExactShard {
                    file_system_id: fixture.shard.file_system_id.clone(),
                    shard_id: DfsMetadataShardId::new(&fixture.shard.shard_id),
                }),
        )
        .unwrap();
    let desired = DfsMetadataShardConfig {
        primary_node_id: fixture.remote.clone(),
        replica_node_ids: vec![fixture.local.clone()],
        epoch: fixture.shard.epoch + 1,
        ..fixture.shard.clone()
    };
    let handoff = control.with_client(|client| {
        DfsControlPlaneStore::new(client.as_mut())
            .begin_metadata_shard_membership_handoff(&fixture.shard, &desired)
            .unwrap()
            .unwrap()
    });
    publication.wait_exact(&handoff);
    let (_, _, unfenced) = current_direct_replica_context(&fixture);
    assert_ne!(data_dir.root(), fixture.data_dir.root());
    assert_eq!(unfenced.local_node_id, fixture.remote);
    assert!(!Arc::ptr_eq(
        &daemon_dfs_mutation_scheduler(&data_dir).unwrap(),
        &fixture.scheduler,
    ));
    assert_eq!(unfenced.pending_membership_handoff.as_ref(), Some(&handoff));
    assert!(handoff.retiring_primary_fence.is_none());
    let freeze = source
        .metadata_backend_mut()
        .unwrap()
        .freeze_metadata_shard_for_membership_handoff(
            &DfsMetadataShardId::new(&fixture.shard.shard_id),
            &handoff.handoff_id,
            fixture.shard.epoch,
        )
        .unwrap()
        .unwrap();
    let fence = crate::DfsMetadataShardMembershipHandoffFence {
        handoff_id: handoff.handoff_id.clone(),
        primary_node_id: fixture.local.clone(),
        journal_sequence: freeze.journal_sequence,
        state_fingerprint: freeze.state_fingerprint,
    };
    let acknowledged = control.with_client(|client| {
        DfsControlPlaneStore::new(client.as_mut())
            .acknowledge_metadata_shard_membership_handoff_fence(&handoff, &fence)
            .unwrap()
            .unwrap()
    });
    publication.wait_exact(&acknowledged);
    let (_, _, current) = current_direct_replica_context(&fixture);
    assert_eq!(
        current.pending_membership_handoff.as_ref(),
        Some(&acknowledged)
    );
    let can_reselect = super::super::data_plane::replica_context_can_reselect_committed_fence;
    assert!(can_reselect(&unfenced, &current));
    assert!(
        !can_reselect(&initial, &current),
        "a missing handoff cannot acquire this identity"
    );
    assert!(
        !can_reselect(&current, &initial),
        "a fence cannot disappear"
    );
    // Invalid DATA only: never installed or used as authority/context for IO.
    let mut replaced = current.clone();
    replaced
        .pending_membership_handoff
        .as_mut()
        .unwrap()
        .retiring_primary_fence
        .as_mut()
        .unwrap()
        .state_fingerprint
        .push('x');
    assert!(
        !can_reselect(&current, &replaced),
        "a retained fence cannot be replaced"
    );
    let releases = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&releases);
    let hook = install_daemon_dfs_pre_dispatch_test_hook(
        &data_dir,
        Arc::new(move |point, _, _| {
            if let DaemonDfsPreDispatchTestPoint::ReplicaAdmissionReleased { released } = point {
                assert!(released);
                observed.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }),
    );
    let invocations = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&invocations);
    let expected = current.clone();
    let result = fixture
        .runtime
        .block_on(
            super::super::data_plane::handle_daemon_dfs_resident_replica_operation(
                &paths,
                Arc::new(data_dir),
                unfenced,
                fixture.runtime.handle(),
                true,
                false,
                ("actual-direct-retirement-fence", "metadata-journal-batch"),
                move |shard, context| {
                    shard.with_backend(|backend| {
                        assert_eq!(
                            context, &expected,
                            "processor receives the SAME actual new fence context"
                        );
                        observed.fetch_add(1, Ordering::SeqCst);
                        Ok(backend.journal_last_sequence())
                    })
                },
            ),
        )
        .unwrap();
    assert!(matches!(
        result,
        super::super::data_plane::DaemonDfsResidentReplicaOperation::Completed { .. }
    ));
    assert_eq!(releases.load(Ordering::SeqCst), 1);
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    drop(hook);
    drop(publication);
    drop(source);
    drop(source_admission);
}

#[test]
fn direct_replica_preflight_rejects_a_ready_changed_assignment_before_backend_invocation() {
    let fixture = RetryFixture::new("direct-replica-preflight-changed-assignment");
    let (data_dir, paths, original_context) = current_direct_replica_context(&fixture);
    let successor = {
        let native = fixture.native.lock().unwrap();
        native
            .as_ref()
            .unwrap()
            .reassign_metadata_shard(
                &fixture.shard.file_system_id,
                &fixture.shard.shard_id,
                fixture.remote.clone(),
                vec![fixture.local.clone()],
            )
            .expect("actual normal handoff freeze, ACK and joint activation")
    };
    assert_ne!(successor, original_context.shard);
    let invocations = Arc::new(AtomicUsize::new(0));
    let operation_invocations = Arc::clone(&invocations);
    let result = fixture
        .runtime
        .block_on(
            super::super::data_plane::handle_daemon_dfs_resident_replica_operation(
                &paths,
                Arc::new(data_dir),
                original_context,
                fixture.runtime.handle(),
                true,
                false,
                (
                    "actual-direct-preflight-changed-assignment",
                    "metadata-journal-batch",
                ),
                move |shard, _context| {
                    shard.with_backend(|backend| {
                        operation_invocations.fetch_add(1, Ordering::SeqCst);
                        Ok(backend.journal_last_sequence())
                    })
                },
            ),
        )
        .unwrap();
    assert!(matches!(
        result,
        super::super::data_plane::DaemonDfsResidentReplicaOperation::Rejected { .. }
    ));
    assert_eq!(invocations.load(Ordering::SeqCst), 0);
}

#[test]
fn completed_rebind_rejects_a_moved_prefix_before_binding_then_accepts_current_ancestry() {
    let fixture = RetryFixture::new("completed-rebind-moved-prefix");
    let (data_dir, _paths, original_context) = current_direct_replica_context(&fixture);
    let shard_id = DfsMetadataShardId::new(&original_context.shard.shard_id);
    let physical_root = crate::default_dfs_metadata_shards_root(&data_dir).join(
        crate::metadata_keys::key_segment(&original_context.shard.shard_id),
    );
    // Keep this real storage instance across the independently served handoff
    // and writer append; its in-memory prefix is deliberately older afterward.
    let mut prior = LocalSobjMetadataBackend::new(shard_id.clone(), &physical_root).unwrap();
    let active = {
        let native = fixture.native.lock().unwrap();
        native
            .as_ref()
            .unwrap()
            .reassign_metadata_shard(
                &fixture.shard.file_system_id,
                &fixture.shard.shard_id,
                fixture.remote.clone(),
                vec![fixture.local.clone()],
            )
            .expect("actual normal freeze, acknowledgements and native joint activation")
    };
    prior
        .refresh_metadata_shard_durable_view_if_changed(&shard_id)
        .unwrap();
    let checked_prefix = DfsMetadataBackend::metadata_prefix_proof(&prior, &shard_id)
        .unwrap()
        .expect("actual completed handoff prefix");
    let original_freeze = prior
        .metadata_shard_write_freeze(&shard_id)
        .unwrap()
        .expect("actual promoted receiver freeze marker");
    let control = {
        let native = fixture.native.lock().unwrap();
        native.as_ref().unwrap().shared_control_plane().unwrap()
    };
    let proof = control
        .with_client(|kv| {
            DfsControlPlaneStore::new(kv.as_mut()).get_metadata_shard_membership_handoff_proof(
                &fixture.shard.file_system_id,
                &fixture.shard.shard_id,
            )
        })
        .unwrap()
        .expect("actual native completed handoff proof");
    assert_eq!(proof.desired_shard, active);
    assert_eq!(proof.handoff_id, original_freeze.handoff_id);
    drop(control);

    {
        let native = fixture.native.lock().unwrap();
        let mut writer = native.as_ref().unwrap().client(&fixture.remote).unwrap();
        writer
            .posix_mkdir("/completed-rebind-real-suffix")
            .expect("ordinary guarded successor appends a genuine journal suffix");
    }
    let before_epoch = prior
        .native_held_snapshot_for_scope(&active.file_system_id, &shard_id)
        .unwrap()
        .backend_instance_authority_epoch;
    let marker_before = prior.metadata_shard_write_freeze(&shard_id).unwrap();
    let error = prior
        .clear_metadata_shard_membership_handoff_freeze(
            &shard_id,
            &proof.handoff_id,
            &active,
            &checked_prefix,
        )
        .expect_err("old checked prefix must reject under the refreshed actual writer");
    assert!(matches!(
        error,
        crate::MetadataBackendError::Rejected {
            code: crate::DfsMetadataRejectionCode::PrefixMismatch,
            ..
        }
    ));
    assert_eq!(
        prior
            .native_held_snapshot_for_scope(&active.file_system_id, &shard_id,)
            .unwrap()
            .backend_instance_authority_epoch,
        before_epoch,
        "rejection cannot bind this old instance to the promoted epoch"
    );
    assert_eq!(
        prior.metadata_shard_write_freeze(&shard_id).unwrap(),
        marker_before,
        "rejection cannot checkpoint or change the actual freeze marker"
    );
    assert!(
        prior
            .metadata_shard_authoritative_writes_frozen(&shard_id)
            .unwrap(),
        "this independently opened instance remains fenced"
    );
    let current_prefix = DfsMetadataBackend::metadata_prefix_proof(&prior, &shard_id)
        .unwrap()
        .unwrap();
    assert!(
        current_prefix.journal_sequence > checked_prefix.journal_sequence,
        "the real writer suffix caused the stale-cut rejection"
    );
    let head_after_rejection = prior.journal_last_sequence();
    assert!(
        crate::service_sync::rebind_completed_metadata_shard_membership_handoff(
            &mut prior,
            &fixture.remote,
            &active,
            None,
            Some(&proof),
        )
        .unwrap(),
        "the same actual completion proof validates the current retained ancestry"
    );
    assert_eq!(
        DfsMetadataBackend::metadata_prefix_proof(&prior, &shard_id).unwrap(),
        Some(current_prefix)
    );
    assert_eq!(
        prior.journal_last_sequence(),
        head_after_rejection,
        "successful rebind must not author another terminal operation"
    );
    assert!(
        !prior
            .metadata_shard_authoritative_writes_frozen(&shard_id)
            .unwrap()
    );
    assert_eq!(
        prior
            .native_held_snapshot_for_scope(&active.file_system_id, &shard_id,)
            .unwrap()
            .backend_instance_authority_epoch,
        Some(active.epoch)
    );
    drop(prior);
}

#[test]
fn native_same_node_main_executes_byte_manifest_reader_exclusive_and_retained_writes() {
    use crate::test_runtime::{DfsTestRuntime, RpcPhase};
    let name = "native-same-node-main-writes";
    let nodes = DfsTestRuntime::ordered_node_ids(name, 2);
    let mut runtime = DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    runtime.define_node(nodes[1].clone()).unwrap();
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    let primary = shard.primary_node_id.clone();
    let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&seen);
    let expected_primary = primary.clone();
    let byte_exclusive_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_byte_exclusive = Arc::clone(&byte_exclusive_seen);
    let hook = Arc::new(move |point: DaemonDfsPreDispatchTestPoint<'_>, command: Option<&DfsMessageCommand>, _preflight: Option<&crate::runtime_host::DaemonDfsMetadataAuthorityPreflight>| {
        if !matches!(point, DaemonDfsPreDispatchTestPoint::BeforeExecute { .. }) { return Ok(()); }
        let Some(command) = command else { return Ok(()); };
        if command.routed_message.source_node_id != expected_primary
            || command.routed_message.target_node_id != expected_primary { return Ok(()); }
        if let DfsNetworkMessage::ExclusiveCreateRequest(write) = &command.routed_message.message
            && write.object_metadata.as_ref().is_some_and(|object| object.path == "/same-node-byte-exclusive")
        {
            assert!(write.exclusive_create);
            observed_byte_exclusive.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        let bit = match &command.routed_message.message {
            DfsNetworkMessage::DistributedWriteRequest(_) => 1,
            DfsNetworkMessage::ExclusiveCreateRequest(_) => 2,
            DfsNetworkMessage::RetainedWriteRequest(_) => 4,
            _ => 0,
        };
        observed.fetch_or(bit, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    });
    let hook_guard = install_daemon_dfs_pre_dispatch_test_hook(runtime.data_dir(&primary), hook);
    let mut client = runtime.client(&primary).unwrap();
    let options = DfsWriteOptions::replicated_eventual(vec![primary.clone()]);
    let path = "/same-node-main-write";
    let parent_before = client.posix_stat("/").unwrap();
    let base = client
        .posix_write_file(path, b"byte-source", options.clone())
        .unwrap();
    let parent_after = client.posix_stat("/").unwrap();
    assert_ne!(parent_before.version, parent_after.version);
    let durable_parent = client
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&parent_after.file_system_id, "/")
        .unwrap()
        .unwrap();
    assert_eq!(parent_after.version, durable_parent.version);
    assert_eq!(
        parent_after.modified_at_unix_nanos,
        durable_parent.modified_at_unix_nanos
    );
    let durable_base = client
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&base.file_system_id, path)
        .unwrap()
        .unwrap();
    assert_eq!(base.version, durable_base.version);
    assert_eq!(base.content_hash, durable_base.content_hash);
    assert_eq!(
        base.modified_at_unix_nanos,
        durable_base.modified_at_unix_nanos
    );
    let link_path = "/same-node-main-link";
    let link = client
        .posix_symlink(path, link_path, options.clone())
        .unwrap();
    assert_eq!(link.kind, crate::DfsObjectKind::Symlink);
    assert!(!link.current_locations.is_empty());
    let durable_link = client
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&link.file_system_id, link_path)
        .unwrap()
        .unwrap();
    assert_eq!(link.object_id, durable_link.object_id);
    assert_eq!(link.version, durable_link.version);
    assert_eq!(link.content_hash, durable_link.content_hash);
    assert_eq!(client.posix_readlink(link_path).unwrap(), path);
    assert_eq!(client.posix_read_file(link_path).unwrap(), b"byte-source");
    let manifest_bytes = vec![0x45; 32 * 1024 + 7];
    let manifest_version = DfsVersion::generate();
    let manifest = client
        .content()
        .put_object_version(
            base.object_id.as_str(),
            manifest_version.as_str(),
            &manifest_bytes,
        )
        .unwrap();
    let manifest_operation = sinomo_core::Id::generate().to_string();
    let outcome = client
        .write_distributed_manifest_with_candidates(
            DfsWriteRequest {
                file_system_id: base.file_system_id.clone(),
                object_id: base.object_id.clone(),
                target_version: manifest_version.clone(),
                requesting_node_id: primary.clone(),
            },
            Some(base.version),
            manifest.content_hash,
            manifest.size_bytes,
            vec![primary.clone()],
            DfsReplicationPolicy::new(1, 1).unwrap(),
            crate::service_write::DfsDistributedWriteOptions {
                operation_id: Some(manifest_operation),
                repair: crate::service_write::DfsWriteRepair::Background,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(
        matches!(client.settle_ready_write_outcome(outcome, false).unwrap(), crate::NetworkedDfsWriteOutcome::Committed { version, .. } if version == manifest_version)
    );
    assert_eq!(client.posix_read_file(path).unwrap(), manifest_bytes);
    let reader_bytes = vec![0x53; 32 * 1024 + 11];
    client
        .posix_write_file_from_reader(
            path,
            std::io::Cursor::new(reader_bytes.clone()),
            options.clone(),
        )
        .unwrap();
    assert_eq!(client.posix_read_file(path).unwrap(), reader_bytes);
    let byte_exclusive_operation = sinomo_core::Id::generate().to_string();
    let byte_exclusive = client
        .posix_create_file_with_contents_and_attrs_and_operation_id(
            "/same-node-byte-exclusive",
            b"byte-exclusive-source",
            options.clone(),
            crate::DfsPosixCreateAttrs::new(0o640, 6110, 6111),
            &byte_exclusive_operation,
        )
        .unwrap();
    assert!(
        byte_exclusive_seen.load(std::sync::atomic::Ordering::SeqCst),
        "the actual same-node Main must receive the public byte create as ExclusiveCreateRequest"
    );
    let byte_replay = client
        .posix_create_file_with_contents_and_attrs_and_operation_id(
            "/same-node-byte-exclusive",
            b"byte-exclusive-source",
            options.clone(),
            crate::DfsPosixCreateAttrs::new(0o640, 6110, 6111),
            &byte_exclusive_operation,
        )
        .unwrap();
    assert_eq!(byte_replay.object_id, byte_exclusive.object_id);
    assert_eq!(byte_replay.version, byte_exclusive.version);
    let byte_shard = crate::DfsMetadataShardId::new(shard.shard_id.clone());
    let before_rejection = client
        .metadata_backend()
        .unwrap()
        .metadata_prefix_proof(&byte_shard)
        .unwrap()
        .unwrap();
    let journal_reader = crate::LocalSobjMetadataBackend::open_for_inspection(
        byte_shard.clone(),
        crate::default_dfs_metadata_shards_root(runtime.data_dir(&primary))
            .join(crate::metadata_keys::key_segment(&byte_shard.0)),
    )
    .unwrap();
    let journal_bound = crate::DfsMetadataJournalReplicationOptions::default().batch_entry_limit;
    let before_journal = journal_reader
        .journal_since_limited(before_rejection.journal_sequence, journal_bound)
        .unwrap();
    assert!(
        before_journal.len() < journal_bound,
        "the bounded actual journal suffix must be complete before claiming no append"
    );
    let original = client
        .metadata_backend()
        .unwrap()
        .get_object(&byte_exclusive.object_id)
        .unwrap()
        .unwrap();
    let rejected = client
        .posix_create_file_with_contents_and_attrs_and_operation_id(
            "/same-node-byte-exclusive",
            b"another byte create",
            options.clone(),
            crate::DfsPosixCreateAttrs::new(0o600, 1, 2),
            &sinomo_core::Id::generate().to_string(),
        )
        .unwrap_err();
    assert!(matches!(
        rejected,
        crate::DfsPosixError::AlreadyExists { .. }
    ));
    assert_eq!(
        client
            .metadata_backend()
            .unwrap()
            .metadata_prefix_proof(&byte_shard)
            .unwrap()
            .unwrap(),
        before_rejection,
        "rejected exclusive byte creation must not append another native frame"
    );
    let after_journal = journal_reader
        .journal_since_limited(before_rejection.journal_sequence, journal_bound)
        .unwrap();
    assert!(
        after_journal.len() < journal_bound,
        "the bounded actual journal suffix must be complete after rejection"
    );
    assert_eq!(
        after_journal, before_journal,
        "the actual durable journal reader must observe no new frame after rejection"
    );
    drop(journal_reader);
    assert_eq!(
        client
            .metadata_backend()
            .unwrap()
            .get_object(&byte_exclusive.object_id)
            .unwrap()
            .unwrap(),
        original,
        "rejected exclusive byte creation must preserve the exact visible head and attributes"
    );
    assert_eq!(
        client.posix_read_file("/same-node-byte-exclusive").unwrap(),
        b"byte-exclusive-source"
    );
    let exclusive_operation = sinomo_core::Id::generate().to_string();
    let exclusive = client
        .posix_create_file_from_reader_with_operation_id(
            "/same-node-exclusive",
            std::io::Cursor::new(b"exclusive-source".to_vec()),
            options.clone(),
            &exclusive_operation,
        )
        .unwrap();
    let replay = client
        .posix_create_file_from_reader_with_operation_id(
            "/same-node-exclusive",
            std::io::Cursor::new(b"exclusive-source".to_vec()),
            options.clone(),
            &exclusive_operation,
        )
        .unwrap();
    assert_eq!(exclusive.object_id, replay.object_id);
    assert_eq!(exclusive.version, replay.version);
    let before_retained = client.posix_stat(path).unwrap();
    let retained = client
        .preserve_content_revision(
            crate::DfsPreservedContentRequest {
                path: path.into(),
                file_system_id: before_retained.file_system_id.clone(),
                object_id: before_retained.object_id.clone(),
                publication: crate::DfsRetainedContentPublication {
                    namespace: "sinomo.dfs.same-node-native".into(),
                    reference_id: sinomo_core::Id::generate().to_string(),
                    metadata: None,
                },
                operation_id: sinomo_core::Id::generate().to_string(),
            },
            std::io::Cursor::new(b"owned-retained-source".to_vec()),
            options,
        )
        .unwrap();
    assert!(matches!(
        retained,
        crate::DfsPreservedContentOutcome::Retained { .. }
    ));
    let after_retained = client.posix_stat(path).unwrap();
    assert_eq!(before_retained.version, after_retained.version);
    assert_eq!(client.posix_read_file(path).unwrap(), reader_bytes);
    assert_eq!(
        seen.load(std::sync::atomic::Ordering::SeqCst),
        7,
        "all three actual canonical variants execute in the real Main handler"
    );
    assert!(
        runtime
            .rpc_observations()
            .iter()
            .any(|call| call.source == primary
                && call.target == primary
                && call.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && call.phase == RpcPhase::AfterDispatch)
    );
    drop(client);
    drop(hook_guard);
    runtime.close().unwrap();
}
