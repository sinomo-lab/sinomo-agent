//! Real retained-configuration ownership around one routed placement command.
use super::*;
use crate::runtime_host::{DaemonDfsPreDispatchTestHook, DaemonDfsPreDispatchTestPoint};
use crate::test_runtime::{DfsTestNativeConfigurationFault, DfsTestRuntime};
use crate::*;
use sinomo_core::kv::{KvCompare, KvEntry, KvOp, KvTxn};
use sinomo_core::network::ClusterPaths;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;

#[path = "planning_route_native_tests/stale_authority.rs"]
mod stale_authority;

#[derive(Default)]
struct GateState {
    entered: Option<serde_json::Value>,
    released: bool,
}
#[derive(Default)]
struct StepGate {
    state: Mutex<GateState>,
    changed: Condvar,
}
impl StepGate {
    fn enter(&self, facts: serde_json::Value, deadline: Instant) -> Result<(), DfsError> {
        let mut state = self.state.lock().unwrap();
        if state.entered.is_some() {
            return Err(DfsError::Cancelled {
                operation: "duplicate native placement control boundary",
            });
        }
        state.entered = Some(facts);
        self.changed.notify_all();
        while !state.released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(DfsError::Cancelled {
                    operation: "native placement control boundary deadline",
                });
            }
            state = self.changed.wait_timeout(state, remaining).unwrap().0;
        }
        Ok(())
    }
    fn wait(&self, deadline: Instant) -> Result<serde_json::Value, String> {
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(facts) = &state.entered {
                return Ok(facts.clone());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err("actual native placement boundary was not reached".into());
            }
            state = self.changed.wait_timeout(state, remaining).unwrap().0;
        }
    }
    fn recorded(&self) -> Option<serde_json::Value> {
        self.state.lock().unwrap().entered.clone()
    }
    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }
}

struct RoutingPhases {
    expected: Mutex<Option<(String, String)>>,
    deadline: Mutex<Option<Instant>>,
    armed: AtomicBool,
    hold_automatic_placement: AtomicBool,
    extra: Mutex<Option<Arc<DaemonDfsPreDispatchTestHook>>>,
    acquired: StepGate,
    publication: StepGate,
    payload: Mutex<Option<DfsPreparedOperationPayload>>,
}
impl RoutingPhases {
    fn new() -> Self {
        Self {
            expected: Mutex::new(None),
            deadline: Mutex::new(None),
            armed: AtomicBool::new(false),
            hold_automatic_placement: AtomicBool::new(true),
            extra: Mutex::new(None),
            acquired: StepGate::default(),
            publication: StepGate::default(),
            payload: Mutex::new(None),
        }
    }
    fn set_extra_hook(&self, hook: Arc<DaemonDfsPreDispatchTestHook>) {
        *self.extra.lock().unwrap() = Some(hook);
    }
    fn set_hold_automatic_placement(&self, deny: bool) {
        self.hold_automatic_placement.store(deny, Ordering::SeqCst);
    }
    fn arm(&self, deadline: Instant) {
        *self.deadline.lock().unwrap() = Some(deadline);
        self.armed.store(true, Ordering::SeqCst);
    }
    fn release_all(&self) {
        self.acquired.release();
        self.publication.release();
    }
    fn payload(&self) -> Option<DfsPreparedOperationPayload> {
        self.payload.lock().unwrap().clone()
    }
}

#[derive(Debug, serde::Serialize)]
struct ActualCut {
    prefix: DfsMetadataPrefixProof,
    object: Option<DfsObjectMetadata>,
    intent: Option<DfsWriteIntentRecord>,
    prepared: Option<DfsPreparedOperationRecord>,
    committed: Option<DfsMetadataJournalEntry>,
    matching_prepared: Vec<DfsPreparedOperationRecord>,
    tables: serde_json::Value,
}

struct RoutingFixture {
    native: DfsTestRuntime,
    owner: NodeId,
    caller: NodeId,
    shard: DfsMetadataShardConfig,
    stat: DfsStat,
    object: DfsObjectMetadata,
    job: DfsPlacementReconciliationJob,
    replication: DfsReplicationPolicy,
    desired: Vec<NodeId>,
    phases: Arc<RoutingPhases>,
    caller_handle: Mutex<Option<NetworkedDfsService<DfsNodeTransport>>>,
    settings_publisher: Mutex<Option<Box<dyn DfsControlPlaneKvClient>>>,
    _hooks: Vec<crate::runtime_host::DaemonDfsPreDispatchTestHookGuard>,
}
impl RoutingFixture {
    fn new(name: &'static str) -> Self {
        let path = format!("/services/{name}.bin");
        let (native, initial, shard) =
            crate::test_support::current_native_metadata_service(name, 3, &path).unwrap();
        drop(initial);
        let owner = shard.primary_node_id.clone();
        let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
        let catalog = native.metadata_shards().unwrap();
        let coordinator = crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
            &shard.file_system_id,
            &catalog,
            "/",
        )
        .unwrap()
        .expect("actual native root placement coordinator")
        .primary_node_id;
        let caller = nodes
            .iter()
            .find(|node| **node != owner && **node != coordinator)
            .expect("three genuine native members include a distinct explicit caller")
            .clone();
        let mut seed_rule =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
        seed_rule.required_node_ids = vec![owner.clone()];
        let empty = native
            .apply_placement_policy(&owner, &path, DfsPlacementScope::SelfOnly, seed_rule)
            .unwrap();
        let control = native.shared_control_plane().unwrap();
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            let current = control
                .with_client(|kv| {
                    DfsControlPlaneStore::new(kv.as_mut())
                        .placement_reconciliation(&empty.file_system_id, &empty.job_id)
                })
                .unwrap()
                .unwrap();
            assert_eq!(current.scanned_object_count, 0);
            assert!(
                current.last_error.is_none(),
                "empty native seed policy: {current:?}"
            );
            if current.state == DfsPlacementReconciliationState::Completed {
                break;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero());
            let tick = native.maintenance_tick(&coordinator, remaining).unwrap();
            assert!(
                tick.outcome.is_ok(),
                "empty actual seed policy: {:?}",
                tick.outcome
            );
        }
        assert_eq!(
            control
                .with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&empty.file_system_id, &empty.job_id))
                .unwrap()
                .unwrap()
                .state,
            DfsPlacementReconciliationState::Completed
        );
        assert!(
            Instant::now() < deadline,
            "empty native seed policy completed after its original deadline"
        );
        let mut seed = native.client(&owner).unwrap();
        let mut seed_policy = DfsReplicationPolicy::new(1, 1).unwrap();
        seed_policy.required_node_ids = vec![owner.clone()];
        let stat = seed
            .posix_write_file(
                &path,
                b"native current placement routing under actual held configuration",
                DfsWriteOptions {
                    replica_candidates: vec![owner.clone()],
                    policy: seed_policy,
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        drop(seed);
        let shard_id = DfsMetadataShardId::new(&shard.shard_id);
        let mut inspection = LocalSobjMetadataBackend::open_for_inspection(
            shard_id.clone(),
            crate::local_dfs_metadata_shard_path(native.data_dir(&owner), &shard_id),
        )
        .unwrap();
        inspection.begin_inspection_snapshot().unwrap();
        let object = inspection.get_object(&stat.object_id).unwrap().unwrap();
        let seed_receipts = inspection
            .receipts_for_version(&stat.object_id, &stat.version)
            .unwrap();
        inspection.end_inspection_snapshot();
        drop(inspection);
        assert_eq!(object.version, stat.version);
        assert_eq!(object.current_locations, vec![owner.clone()]);
        assert_eq!(seed_receipts.len(), 1);
        assert_eq!(seed_receipts[0].replica_node_id, owner);
        // Acquire actual long-lived request/settings owners while this native
        // generation is healthy, before accepting another policy or holding
        // this object's automatic placement. A controlled gap must not require a fresh startup opener.
        let caller_handle = native.client(&caller).unwrap();
        let settings_publisher = native.configuration_publisher(&caller);
        let phases = Arc::new(RoutingPhases::new());
        let hooks = nodes.iter().map(|node| {
        let observed = Arc::clone(&phases);
        let controlled_path = path.clone();
        let controlled_object = stat.object_id.clone();
        let explicit_caller = caller.clone();
        crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(native.data_dir(node), Arc::new(move |point, command, context| {
            let extra = observed.extra.lock().unwrap().clone();
            if let Some(extra) = extra { extra(point, command, context)?; }
            if observed.hold_automatic_placement.load(Ordering::SeqCst) {
                if matches!(point, DaemonDfsPreDispatchTestPoint::BeforeExecute { .. })
                    && let Some(command) = command
                    && let DfsNetworkMessage::NamespaceRequest(request) = &command.routed_message.message
                    && let DfsNamespaceOperation::ReconcilePlacement { path, object_id, .. } = &request.operation
                    && path == &controlled_path && object_id == &controlled_object
                    && (command.routed_message.source_node_id != explicit_caller
                        || request.requesting_node_id != explicit_caller)
                {
                    return Err(DfsError::Cancelled { operation: "native placement control holds automatic remote reconciliation" });
                }
                if matches!(point, DaemonDfsPreDispatchTestPoint::LocalPlacementAuthorityAcquired {
                    path, namespace_shards: None, ..
                } if path == controlled_path)
                {
                    return Err(DfsError::Cancelled { operation: "native placement control holds automatic local reconciliation" });
                }
            }
            if !observed.armed.load(Ordering::SeqCst) { return Ok(()); }
            let expected = observed.expected.lock().unwrap().clone().unwrap();
            let deadline = observed.deadline.lock().unwrap().unwrap();
            match point {
                DaemonDfsPreDispatchTestPoint::LocalPlacementAuthorityAcquired { job_id, path, namespace_shards }
                    if job_id == expected.0 && namespace_shards.is_some() => {
                    observed.acquired.enter(serde_json::json!({
                        "actual_job_id": job_id, "actual_path": path, "actual_namespace_shards": namespace_shards,
                    }), deadline)?;
                }
                DaemonDfsPreDispatchTestPoint::BeforeLocalPlacementIntentPublication { job_id, intent_id, owner_epoch }
                    if job_id == expected.0 && intent_id == expected.1 => {
                    observed.publication.enter(serde_json::json!({
                        "actual_job_id": job_id, "actual_intent_id": intent_id,
                        "actual_owner_epoch": owner_epoch,
                    }), deadline)?;
                }
                DaemonDfsPreDispatchTestPoint::BeforeMetadataEffectPrepare { payload }
                    if payload.metadata_effects.iter().any(|effect| matches!(effect,
                        DfsPreparedMetadataEffect::PutIntent { intent } if intent.intent_id == expected.1)) => {
                    *observed.payload.lock().unwrap() = Some(payload.clone());
                }
                _ => {}
            }
            Ok(())
        }))
        }).collect();
        let mut rule = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 3).unwrap();
        rule.required_node_ids = nodes.clone();
        let job = native
            .apply_placement_policy(&owner, &path, DfsPlacementScope::SelfOnly, rule)
            .unwrap();
        let (_, replication, placement) =
            reconciliation_placement_for_object_with_pins(&job.accepted_config, &job, &object, &[])
                .unwrap();
        let desired = placement.plan.desired_replica_nodes;
        assert_eq!(replication.min_commit_replicas, 1);
        assert_eq!(replication.desired_replicas, 3);
        assert_eq!(
            desired.iter().cloned().collect::<BTreeSet<_>>(),
            nodes.into_iter().collect()
        );
        assert!(
            desired
                .iter()
                .any(|node| !object.current_locations.contains(node))
        );
        let intent_id = format!(
            "placement:{}:{}:{}",
            job.job_id, stat.object_id, stat.version
        );
        *phases.expected.lock().unwrap() = Some((job.job_id.clone(), intent_id));
        drop(control);
        Self {
            native,
            owner,
            caller,
            shard,
            stat,
            object,
            job,
            replication,
            desired,
            phases,
            caller_handle: Mutex::new(Some(caller_handle)),
            settings_publisher: Mutex::new(Some(settings_publisher)),
            _hooks: hooks,
        }
    }
    fn intent_id(&self) -> String {
        format!(
            "placement:{}:{}:{}",
            self.job.job_id, self.stat.object_id, self.stat.version
        )
    }
    fn operation(&self) -> DfsNamespaceOperation {
        DfsNamespaceOperation::ReconcilePlacement {
            job_id: self.job.job_id.clone(),
            path: self.stat.path.clone(),
            object_id: self.stat.object_id.clone(),
            expected_version: self.stat.version.clone(),
            object_policy: self.object.policy.clone(),
            replication_policy: self.replication.clone(),
            desired_replica_node_ids: self.desired.clone(),
        }
    }
    fn take_caller(&self) -> Result<NetworkedDfsService<DfsNodeTransport>, String> {
        self.caller_handle
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| "the one actual pre-opened request owner was already consumed".into())
    }
    fn take_settings_publisher(&self) -> Box<dyn DfsControlPlaneKvClient> {
        self.settings_publisher
            .lock()
            .unwrap()
            .take()
            .expect("one actual settings publisher was acquired before the controlled transition")
    }
    fn reconcile_once(&self, deadline: Instant) -> Result<(), String> {
        let mut caller = self.take_caller()?;
        caller.set_transport_operation_deadline(Some(deadline));
        caller
            .route_namespace_operation(self.owner.clone(), self.operation())
            .map_err(|error| error.to_string())
    }
    fn spawn_reconcile(&self, deadline: Instant) -> JoinHandle<Result<(), String>> {
        let mut caller = self.take_caller().unwrap();
        caller.set_transport_operation_deadline(Some(deadline));
        let owner = self.owner.clone();
        let operation = self.operation();
        std::thread::spawn(move || {
            caller
                .route_namespace_operation(owner, operation)
                .map_err(|error| error.to_string())
        })
    }
    fn inspect(&self, owner: &NodeId, operation: Option<&str>) -> ActualCut {
        let shard_id = DfsMetadataShardId::new(&self.shard.shard_id);
        let mut inspection = LocalSobjMetadataBackend::open_for_inspection(
            shard_id.clone(),
            crate::local_dfs_metadata_shard_path(self.native.data_dir(owner), &shard_id),
        )
        .unwrap();
        inspection.begin_inspection_snapshot().unwrap();
        let prepared = operation
            .and_then(|operation| inspection.prepared_operation_record(operation).unwrap());
        let committed = prepared.as_ref().and_then(|record| {
            inspection
                .committed_prepared_operation(&record.digest)
                .unwrap()
        });
        let snapshot = inspection
            .export_shard_table_snapshot(&shard_id)
            .unwrap()
            .unwrap();
        let raw_tables = serde_json::to_value(&snapshot).unwrap();
        let actual_tables = crate::metadata_table_store::metadata_tables_from_table_snapshot(
            snapshot,
            "actual native placement planning inspection",
        )
        .unwrap();
        let matching_prepared = actual_tables.prepared_operations_v7.values()
            .filter(|record| record.payload.metadata_effects.iter().any(|effect| matches!(effect,
                DfsPreparedMetadataEffect::PutIntent { intent } if intent.intent_id == self.intent_id())))
            .cloned().collect();
        let cut = ActualCut {
            prefix: DfsMetadataBackend::metadata_prefix_proof(&inspection, &shard_id)
                .unwrap()
                .expect("actual native inspection has a complete bound prefix"),
            object: inspection.get_object(&self.stat.object_id).unwrap(),
            intent: inspection
                .get_intent(&self.stat.object_id, &self.intent_id())
                .unwrap(),
            prepared,
            committed,
            matching_prepared,
            tables: raw_tables,
        };
        inspection.end_inspection_snapshot();
        cut
    }
    fn close(self) {
        let Self {
            native,
            phases,
            _hooks,
            caller_handle,
            settings_publisher,
            ..
        } = self;
        phases.set_hold_automatic_placement(false);
        phases.release_all();
        *phases.extra.lock().unwrap() = None;
        drop(_hooks);
        drop((caller_handle, settings_publisher));
        native.close().unwrap();
    }
    fn provider_diagnostics(&self, owner: &NodeId) -> serde_json::Value {
        let observed = (|| -> Result<serde_json::Value, String> {
            let paths = ClusterPaths::new(self.native.data_dir(owner));
            let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)
                .map_err(|error| error.to_string())?;
            let runtime = self.native.runtime_handle();
            let authority =
                crate::runtime_host::daemon_dfs_metadata_authority_generation_for_identity(
                    &paths,
                    &state.cluster_id,
                    owner,
                    Some(&runtime),
                )
                .map_err(|error| error.to_string())?;
            let readiness = match authority.write_configuration_readiness() {
                Ok(DfsWriteConfigurationReadiness::Ready(Some(view))) => {
                    serde_json::json!({"state":"ready", "revision":view.revision()})
                }
                Ok(DfsWriteConfigurationReadiness::Ready(None)) => {
                    serde_json::json!({"state":"ready-without-view"})
                }
                Ok(DfsWriteConfigurationReadiness::Pending) => {
                    serde_json::json!({"state":"pending"})
                }
                Err(error) => serde_json::json!({"error":error.to_string()}),
            };
            let configuration = authority
                .cached_current_publication_configuration(&self.shard.file_system_id)
                .map(|value| {
                    value.map(|(generation, shards, filesystem)| {
                        serde_json::json!({
                            "generation":generation, "shards":shards, "filesystem":filesystem,
                        })
                    })
                })
                .map_err(|error| error.to_string());
            Ok(
                serde_json::json!({"readiness":readiness,"actual_native_publication_configuration":configuration}),
            )
        })();
        serde_json::json!({"actual_owner":owner,"observation":observed})
    }
    fn evidence(&self, name: &str, value: serde_json::Value) {
        std::fs::write(
            self.native
                .data_dir(&self.owner)
                .root()
                .parent()
                .unwrap()
                .join(name),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .unwrap();
    }
}

struct HeldCall {
    call: Option<JoinHandle<Result<(), String>>>,
    phases: Arc<RoutingPhases>,
    fault: Arc<DfsTestNativeConfigurationFault>,
}
impl HeldCall {
    fn finish(mut self) -> Result<(), String> {
        self.fault.release();
        self.phases.release_all();
        self.call
            .take()
            .unwrap()
            .join()
            .map_err(|_| "actual routed placement caller panicked".to_owned())?
    }
}
impl Drop for HeldCall {
    fn drop(&mut self) {
        self.fault.release();
        self.phases.release_all();
        if let Some(call) = self.call.take() {
            let _ = call.join();
        }
    }
}

fn actual_same_byte_cas(
    publisher: &mut dyn crate::DfsControlPlaneKvClient,
    entry: &KvEntry,
    marker_key: &str,
    marker_value: &str,
) -> Result<sinomo_core::kv::KvRevision, String> {
    let companion =
        KvOp::put_serde(marker_key, marker_value, None).map_err(|error| error.to_string())?;
    let response = publisher
        .txn(KvTxn {
            compares: vec![
                KvCompare::ValueEquals {
                    key: entry.key.clone(),
                    value: entry.value.clone(),
                },
                KvCompare::Missing {
                    key: marker_key.to_string(),
                },
            ],
            success: vec![
                KvOp::Put {
                    key: entry.key.clone(),
                    value: entry.value.clone(),
                    lease_id: entry.lease_id.clone(),
                },
                companion,
            ],
            failure: Vec::new(),
        })
        .map_err(|error| error.to_string())?;
    if !response.succeeded || response.revision <= entry.modify_revision {
        return Err(
            "actual same-byte guarded KV publication did not advance its native revision".into(),
        );
    }
    Ok(response.revision)
}

#[test]
fn native_routed_placement_reaches_intent_publication_entry_during_real_configuration_gap_rebuild()
{
    let fixture = RoutingFixture::new("native-placement-real-held-configuration");
    let paths = ClusterPaths::new(fixture.native.data_dir(&fixture.owner));
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths).unwrap();
    let runtime = fixture.native.runtime_handle();
    let authority = crate::runtime_host::daemon_dfs_metadata_authority_generation_for_identity(
        &paths,
        &state.cluster_id,
        &fixture.owner,
        Some(&runtime),
    )
    .unwrap();
    // The settings producer is an actual different live node, so observing
    // its committed bytes does not join the receiver's deliberately held gap.
    let mut publisher = fixture.take_settings_publisher();
    let key = crate::file_system_config_key(&fixture.shard.file_system_id);
    let original_entry = publisher.get(&key).unwrap().unwrap();
    let original_config: DfsFileSystemConfig = original_entry.decode().unwrap();
    assert_eq!(original_config.file_system_id, fixture.shard.file_system_id);
    let setup_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let original_ready = crate::test_runtime::executor().block_on(async {
        tokio::time::timeout_at(
            setup_deadline.into(),
            authority.ready_write_configuration_projection_async(),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    });
    assert!(
        original_ready
            .entries(&key)
            .unwrap()
            .iter()
            .any(|entry| entry.key == key && entry.value == original_entry.value),
        "the actual receiver starts from the accepted native ready configuration"
    );
    let before = fixture.inspect(&fixture.owner, None);
    assert!(before.intent.is_none());
    let marker_prefix = format!(
        "/tests/sinomo-dfs/placement-routing/{}",
        fixture.stat.object_id
    );
    let first_marker = format!("{marker_prefix}/first");
    let second_marker = format!("{marker_prefix}/second");
    // Exact FS bytes stay unchanged. A unique persisted companion makes each
    // real transaction non-noop. Withhold its actual first delta: the native
    // authority receives every committed delta and must detect the next gap,
    // even if an unchanged FS put was elided by the normal commit owner.
    let faults = Arc::new(
        fixture
            .native
            .lose_configuration_delta_and_pause_rebuild(&fixture.owner, first_marker.clone())
            .unwrap(),
    );
    let capture = fixture
        .native
        .capture_placement_authority_calls(&fixture.owner)
        .unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    fixture.phases.arm(deadline);
    let call = HeldCall {
        call: Some(fixture.spawn_reconcile(deadline)),
        phases: Arc::clone(&fixture.phases),
        fault: Arc::clone(&faults),
    };
    let observed = (|| -> Result<serde_json::Value, String> {
        let acquired = fixture.phases.acquired.wait(deadline)?;
        fixture.evidence(
            "placement-held-outer-permit.json",
            serde_json::json!({
                "actual_outer_permit_boundary":acquired,"actual_job_id":fixture.job.job_id,
                "actual_intent_id":fixture.intent_id(),"actual_caller":fixture.caller,
            }),
        );
        let first_revision = actual_same_byte_cas(
            publisher.as_mut(),
            &original_entry,
            &first_marker,
            "first actual native companion",
        )?;
        let second_revision = actual_same_byte_cas(
            publisher.as_mut(),
            &original_entry,
            &second_marker,
            "second actual native companion",
        )?;
        if second_revision <= first_revision {
            return Err("two actual native KV commits did not have distinct revisions".into());
        }
        faults.wait_rebuild(deadline.saturating_duration_since(Instant::now()))?;
        let dropped = faults
            .dropped_delta()
            .ok_or("the actual first companion-bearing native delta was not withheld")?;
        if !matches!(
            authority
                .write_configuration_readiness()
                .map_err(|e| e.to_string())?,
            DfsWriteConfigurationReadiness::Pending
        ) {
            return Err("the actual shared configuration rebuild is not Pending while held".into());
        }
        let actual_first_marker = publisher
            .get(&first_marker)
            .map_err(|error| error.to_string())?
            .ok_or("first native companion is not persisted")?;
        let actual_second_marker = publisher
            .get(&second_marker)
            .map_err(|error| error.to_string())?
            .ok_or("second native companion is not persisted")?;
        if actual_first_marker
            .decode::<String>()
            .map_err(|error| error.to_string())?
            != "first actual native companion"
            || actual_second_marker
                .decode::<String>()
                .map_err(|error| error.to_string())?
                != "second actual native companion"
        {
            return Err("actual companion values differ from these two native transactions".into());
        }
        fixture.evidence("placement-held-gap-prerequisites.json", serde_json::json!({
            "actual_outer_permit_boundary":acquired,"first_revision":first_revision,
            "second_revision":second_revision,"dropped_actual_delta":dropped,
            "actual_first_marker":actual_first_marker,"actual_second_marker":actual_second_marker,
            "actual_refresh_pending":true,"actual_provider":fixture.provider_diagnostics(&fixture.owner),
            "qualification":"all fields observed before releasing the real admitted command into the held configuration gap",
        }));
        fixture.phases.acquired.release();
        let publication = fixture.phases.publication.wait(deadline)?;
        let held_cut = fixture.inspect(&fixture.owner, None);
        let held_payload_absent = fixture.phases.payload().is_none();
        let actual_native_publication_unavailable = authority
            .cached_current_publication_configuration(&fixture.shard.file_system_id)
            .map_err(|e| e.to_string())?
            .is_none();
        let still_pending = matches!(
            authority
                .write_configuration_readiness()
                .map_err(|e| e.to_string())?,
            DfsWriteConfigurationReadiness::Pending
        );
        let current_entry = publisher
            .get(&key)
            .map_err(|e| e.to_string())?
            .ok_or("actual filesystem entry disappeared")?;
        Ok(serde_json::json!({
            "acquired": acquired, "publication": publication, "dropped_actual_delta": dropped,
            "first_revision": first_revision, "second_revision": second_revision,
            "actual_entry_after": current_entry, "same_config_bytes": current_entry.value == original_entry.value,
            "held_refresh_still_pending": still_pending, "held_cut": held_cut,
            "held_payload_absent": held_payload_absent,
            "actual_native_publication_unavailable": actual_native_publication_unavailable,
            "authority_calls_while_held": format!("{:#?}", capture.reader().records()),
        }))
    })();
    // The routing cut is DATA only. Rejoin the ACTUAL required native ready
    // owner before letting this same invocation acquire publication permission.
    faults.release();
    let rejoined = if observed.is_ok() {
        crate::test_runtime::executor().block_on(async {
            match tokio::time::timeout_at(
                deadline.into(),
                authority.ready_write_configuration_projection_async(),
            )
            .await
            {
                Ok(Ok(Some(view))) => Ok(view.revision()),
                other => Err(format!(
                    "actual released native configuration did not become ready: {other:?}"
                )),
            }
        })
    } else {
        Err("entry was not reached while held; release all owned work for original failure".into())
    };
    fixture.phases.release_all();
    let outcome = call.finish();
    let payload = fixture.phases.payload();
    let after = fixture.inspect(
        &fixture.owner,
        payload
            .as_ref()
            .map(|payload| payload.operation_id.as_str()),
    );
    fixture.evidence("placement-held-configuration-state.json", serde_json::json!({
        "qualification": "actual encoded Main ReconcilePlacement; actual held native gap refresh; routing and intent-publication ENTRY only while held. Real provider configuration is still unavailable then; SAME-call native payload/commit required only after actual ready-owner rejoin",
        "actual_job": fixture.job, "actual_before": before,
        "original_fs_entry": original_entry, "original_native_ready_revision": original_ready.revision(), "observed": observed.as_ref().map_err(String::as_str),
        "actual_caller_outcome": outcome, "actual_ready_rejoin": rejoined,
        "recorded_outer_permit_boundary":fixture.phases.acquired.recorded(),
        "recorded_publication_entry_boundary":fixture.phases.publication.recorded(),
        "actual_provider_after_release":fixture.provider_diagnostics(&fixture.owner), "actual_payload": payload, "actual_after": after,
        "rpc_window": format!("{:#?}", fixture.native.rpc_observations()),
        "omitted_rpc_observations": fixture.native.observation_dropped_count(),
    }));
    fixture.native.checkpoint_rpc_observation_window().unwrap();
    let observed = observed
        .expect("the actual old nested sync cannot reach publication entry under this held owner");
    assert_eq!(observed["held_refresh_still_pending"], true);
    assert_eq!(observed["actual_native_publication_unavailable"], true);
    assert_eq!(observed["held_payload_absent"], true);
    assert_eq!(observed["same_config_bytes"], true);
    assert_eq!(observed["acquired"]["actual_job_id"], fixture.job.job_id);
    let shards: Vec<DfsMetadataShardConfig> =
        serde_json::from_value(observed["acquired"]["actual_namespace_shards"].clone()).unwrap();
    assert!(shards.contains(&fixture.shard));
    assert_eq!(
        observed["held_cut"]["matching_prepared"],
        serde_json::json!([])
    );
    assert_eq!(observed["held_cut"]["intent"], serde_json::Value::Null);
    assert!(rejoined.unwrap() > original_ready.revision());
    outcome.unwrap();
    let payload = payload.unwrap();
    assert_eq!(payload.file_system_id, fixture.shard.file_system_id);
    assert_eq!(
        payload.shard_id,
        DfsMetadataShardId::new(&fixture.shard.shard_id)
    );
    assert_eq!(payload.owner_epoch, fixture.shard.epoch);
    let record = after.prepared.unwrap();
    assert_eq!(record.status, DfsPreparedOperationStatus::Committed);
    assert_eq!(record.payload.as_ref(), &payload);
    DfsPreparedOperationCommitReference::from_journal_entry(&after.committed.unwrap())
        .unwrap()
        .validate(&record.digest)
        .unwrap();
    let intent = after.intent.unwrap();
    assert_eq!(intent.status, DfsWriteIntentStatus::Committed);
    assert_eq!(intent.intent_id, fixture.intent_id());
    assert_eq!(intent.desired_replica_node_ids, fixture.desired);
    assert_eq!(fixture.native.observation_dropped_count(), 0);
    drop((faults, capture, publisher, original_ready, authority));
    fixture.close();
}
