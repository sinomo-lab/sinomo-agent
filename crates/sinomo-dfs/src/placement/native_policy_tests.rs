use super::*;
use crate::DfsMetadataBackend;

fn native_current_policy_fixture(
    label: &str,
) -> (
    crate::test_support::DfsTestRuntime,
    NetworkedDfsService<crate::DfsNodeTransport>,
    DfsObjectMetadata,
    DfsPlacementReconciliationJob,
) {
    native_current_policy_fixture_with_nodes(label, 3)
}

fn native_current_policy_fixture_with_nodes(
    label: &str,
    node_count: usize,
) -> (
    crate::test_support::DfsTestRuntime,
    NetworkedDfsService<crate::DfsNodeTransport>,
    DfsObjectMetadata,
    DfsPlacementReconciliationJob,
) {
    let path = "/services/native-policy-preservation/file.bin";
    let (runtime, mut service, _) =
        crate::test_support::current_native_metadata_service(label, node_count, path).unwrap();
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(label, node_count);
    let expected_nodes = nodes.iter().cloned().collect::<BTreeSet<_>>();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let candidates = loop {
        let candidates = service.placement_content_candidates().unwrap();
        if candidates
            .iter()
            .map(|candidate| candidate.node_id.clone())
            .collect::<BTreeSet<_>>()
            == expected_nodes
        {
            break candidates;
        }
        assert!(
            Instant::now() < deadline,
            "actual native content roster incomplete"
        );
        std::thread::sleep(Duration::from_millis(1));
    };
    service
        .posix_mkdir_all("/services/native-policy-preservation")
        .unwrap();
    let written = service
        .posix_write_file(
            path,
            b"committed W2 N3 policy must survive ordinary reconciliation",
            crate::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 3).unwrap(),
                replica_candidates: nodes,
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    let object = service
        .current_read_object_metadata_distributed(&written.object_id)
        .unwrap();
    // Placement planning reads the written version's durable W/N from the
    // actual metadata owner; the delegating writer keeps no store.
    drop(service);
    let mut service = runtime.metadata_primary_service(path).unwrap();
    let version = service
        .metadata_backend()
        .unwrap()
        .get_version(&object.object_id, &object.version)
        .unwrap()
        .unwrap();
    assert_eq!(version.content_hash, object.content_hash);
    let durable = version.content_durability.unwrap();
    assert_eq!(
        (
            durable.policy.min_commit_replicas,
            durable.policy.desired_replicas
        ),
        (2, 3),
        "the BEFORE prerequisite is an actual committed W2/N3 version"
    );
    assert_eq!(durable.desired_replica_node_ids.len(), 3);
    let config = service.current_placement_file_system_config().unwrap();
    assert!(
        !config
            .subtree_policies
            .iter()
            .any(|rule| placement_rule_matches(rule, path))
    );
    // Build only ordinary planning DATA from the real config and roster. No
    // job, policy override, telemetry, receipt or authority is installed here.
    // R93 exercised the common planner with manual planning DATA. Adaptive
    // qualification now uses a genuine published native observation instead.
    let root_observer = runtime.client(&config.owner_node_id).unwrap();
    let snapshot = loop {
        if let Some(snapshot) = root_observer.placement_snapshot_cache.snapshot()
            && snapshot.primary_node_id == config.owner_node_id
            && !snapshot.nodes.is_empty()
            && time::OffsetDateTime::now_utc() <= snapshot.expires_at
        {
            break (*snapshot).clone();
        }
        assert!(
            Instant::now() < deadline,
            "actual native root placement observation was unavailable"
        );
        std::thread::sleep(Duration::from_millis(1));
    };
    drop(root_observer);
    let job = crate::control_plane::build_adaptive_placement_reconciliation_job(
        sinomo_core::Id::generate().to_string(),
        &config,
        "/".to_string(),
        DfsPlacementScope::Subtree,
        BTreeSet::new(),
        candidates,
        snapshot,
        crate::DfsAdaptivePlacementBudget::default(),
        0,
        dfs_now_unix_nanos(),
    )
    .unwrap();
    (runtime, service, object, job)
}

#[test]
fn native_no_override_reconciliation_preserves_current_w2_n3() {
    let (runtime, service, object, job) =
        native_current_policy_fixture("native-no-override-current-w2-n3");
    let planned = service.reconciliation_placement_for_object_with_pins(
        &job.accepted_config,
        &job,
        &object,
        &[],
    );
    drop(service);
    runtime.close().unwrap();
    let (_, replication, placement) = planned.unwrap();
    eprintln!(
        "NATIVE_NO_OVERRIDE_POLICY original_w=2 original_n=3 planned_w={} planned_n={} targets={}",
        replication.min_commit_replicas,
        replication.desired_replicas,
        placement.plan.desired_replica_nodes.len()
    );
    assert_eq!(
        (
            replication.min_commit_replicas,
            replication.desired_replicas
        ),
        (2, 3),
        "no managed override may reduce the actual current version's W/N"
    );
    assert_eq!(placement.plan.desired_replica_nodes.len(), 3);
}

#[test]
fn native_adaptive_candidate_shortage_cannot_lower_current_w2_n3() {
    let (runtime, service, object, mut job) =
        native_current_policy_fixture("native-adaptive-short-roster-w2-n3");
    // A smaller planning input uses only real advertisements. It is not
    // published as membership or claimed to be an actual node outage.
    job.candidate_snapshot.pop().unwrap();
    let resolved = service
        .resolved_current_object_placement_policy(&job.accepted_config, &object, 2, Some(&job))
        .unwrap();
    let planned = service.reconciliation_placement_for_object_with_pins(
        &job.accepted_config,
        &job,
        &object,
        &[],
    );
    drop(service);
    runtime.close().unwrap();
    assert_eq!(
        (
            resolved.policy.min_commit_replicas,
            resolved.policy.desired_replicas
        ),
        (2, 3)
    );
    assert!(matches!(
        planned,
        Err(DfsError::InvalidReplicationPolicy { .. })
    ));
}

#[test]
fn native_explicit_policy_still_overrides_current_durability() {
    let (runtime, service, object, observed) =
        native_current_policy_fixture("native-explicit-policy-current-durability");
    let mut policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    policy.required_node_ids.push(service.node_id().clone());
    let applied = runtime
        .apply_placement_policy(
            service.node_id(),
            &object.path,
            DfsPlacementScope::SelfOnly,
            policy,
        )
        .unwrap();
    let adaptive = crate::control_plane::build_adaptive_placement_reconciliation_job(
        sinomo_core::Id::generate().to_string(),
        &applied.accepted_config,
        object.path.clone(),
        DfsPlacementScope::SelfOnly,
        BTreeSet::new(),
        applied.candidate_snapshot.clone(),
        observed.placement_snapshot.unwrap(),
        crate::DfsAdaptivePlacementBudget::default(),
        0,
        dfs_now_unix_nanos(),
    )
    .unwrap();
    let planned = service.reconciliation_placement_for_object_with_pins(
        &adaptive.accepted_config,
        &adaptive,
        &object,
        &[],
    );
    let original = service
        .metadata_backend()
        .unwrap()
        .get_version(&object.object_id, &object.version)
        .unwrap()
        .unwrap()
        .content_durability
        .unwrap();
    drop(service);
    runtime.close().unwrap();
    let (_, replication, placement) = planned.unwrap();
    assert_eq!(
        (
            replication.min_commit_replicas,
            replication.desired_replicas
        ),
        (1, 1)
    );
    assert_eq!(placement.plan.desired_replica_nodes.len(), 1);
    assert_eq!(
        (
            original.policy.min_commit_replicas,
            original.policy.desired_replicas
        ),
        (2, 3)
    );
}

#[test]
fn native_manual_clear_keeps_its_reviewed_default_plan() {
    let (runtime, service, object, observed) =
        native_current_policy_fixture("native-manual-clear-reviewed-default");
    let policy = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    runtime
        .apply_placement_policy(
            service.node_id(),
            &object.path,
            DfsPlacementScope::SelfOnly,
            policy,
        )
        .unwrap();
    let control = runtime.shared_control_plane().unwrap();
    let expected = control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .get_file_system_config(&object.file_system_id)
            .unwrap()
            .unwrap()
    });
    let mut cleared = expected.clone();
    crate::browser_service::mutate_daemon_dfs_browser_placement_config(
        &mut cleared,
        &object.path,
        DfsPlacementScope::SelfOnly,
        None,
    )
    .unwrap();
    let preview = preview_placement_with_snapshot_and_unavailable_nodes(
        &cleared,
        &object.path,
        object.object_id.clone(),
        object.version.clone(),
        object.policy.clone(),
        service.node_id().clone(),
        object.current_locations.clone(),
        observed.candidate_snapshot.clone(),
        None,
        DfsPlacementScope::SelfOnly,
        observed.placement_snapshot.as_ref(),
        &[],
        &BTreeSet::new(),
    )
    .unwrap();
    let manual = crate::control_plane::build_placement_reconciliation_job(
        sinomo_core::Id::generate().to_string(),
        &cleared,
        object.path.clone(),
        DfsPlacementScope::SelfOnly,
        BTreeSet::new(),
        observed.candidate_snapshot,
        observed.placement_snapshot,
        dfs_now_unix_nanos(),
    )
    .unwrap();
    assert!(!manual.adaptive);
    assert!(control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_placement_update(&expected, &cleared, &manual)
            .unwrap()
            .is_some()
    }));
    let planned = service.reconciliation_placement_for_object_with_pins(
        &manual.accepted_config,
        &manual,
        &object,
        &[],
    );
    drop(control);
    drop(service);
    runtime.close().unwrap();
    let (resolved, replication, placement) = planned.unwrap();
    assert_eq!(resolved.policy, preview.resolved.policy);
    assert_eq!(
        (
            replication.min_commit_replicas,
            replication.desired_replicas
        ),
        (1, 2)
    );
    assert_eq!(
        placement.plan.desired_replica_nodes,
        preview.placement.plan.desired_replica_nodes
    );
}

struct PlacementBirthCut {
    object: DfsObjectMetadata,
    prior: crate::DfsWriteIntentRecord,
    current: crate::DfsWriteIntentRecord,
    exact: crate::DfsContentDurability,
    global_error: String,
}

struct PlacementBirthCapture {
    prior_intent_id: String,
    cut: std::sync::Mutex<Option<Result<PlacementBirthCut, String>>>,
}

static PLACEMENT_BIRTH_INTERRUPTS: std::sync::LazyLock<
    std::sync::Mutex<BTreeMap<String, std::sync::Arc<PlacementBirthCapture>>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(BTreeMap::new()));

pub(super) fn interrupt_after_placement_birth<T: DfsMessageTransport>(
    service: &NetworkedDfsService<T>,
    intent: &DistributedWriteIntent,
) -> Result<(), DfsError> {
    // Consume only this exact generated identity. Later ordinary continuation
    // cannot encounter a persistent fault or a second test-induced failure.
    let capture = PLACEMENT_BIRTH_INTERRUPTS
        .lock()
        .unwrap()
        .remove(&intent.intent_id);
    let Some(capture) = capture else {
        return Ok(());
    };
    let cut = (|| -> Result<PlacementBirthCut, DfsError> {
        let missing = || DfsError::Metadata {
            message: "native placement birth cut is incomplete".into(),
        };
        let backend = service.metadata_backend().ok_or_else(missing)?;
        let object = backend.get_object(&intent.object_id)?.ok_or_else(missing)?;
        let prior = backend
            .get_intent(&object.object_id, &capture.prior_intent_id)?
            .ok_or_else(missing)?;
        let current = backend
            .get_intent(&object.object_id, &intent.intent_id)?
            .ok_or_else(missing)?;
        if backend
            .get_intent_receipt(&object.object_id, &prior.intent_id)?
            .is_some()
            || backend
                .get_intent_receipt(&object.object_id, &current.intent_id)?
                .is_some()
        {
            return Err(missing());
        }
        let global_error = match crate::content_durability::current_content_durability_from_backend(
            backend, &object,
        ) {
            Err(error) => error.to_string(),
            Ok(_) => {
                return Err(DfsError::Metadata {
                    message: "native publication cut did not expose two current owners".into(),
                });
            }
        };
        let exact =
            crate::content_durability::current_placement_durability_for_intent_from_backend(
                backend,
                &object,
                &intent.intent_id,
            )?
            .ok_or_else(missing)?;
        Ok(PlacementBirthCut {
            object,
            prior,
            current,
            exact,
            global_error,
        })
    })()
    .map_err(|error| error.to_string());
    *capture.cut.lock().unwrap() = Some(cut);
    Err(DfsError::Metadata {
        message: "test interruption after committed placement birth".into(),
    })
}

#[derive(Clone)]
struct PlacementReadPruneSelection {
    node_id: NodeId,
    object_id: DfsObjectId,
    version: DfsVersion,
    content_hash: Option<String>,
}

static PLACEMENT_READ_PRUNE_CAPTURES: std::sync::LazyLock<
    std::sync::Mutex<BTreeMap<String, PlacementReadPruneSelection>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(BTreeMap::new()));

fn selected_read_prune_capture<T: DfsMessageTransport>(
    service: &NetworkedDfsService<T>,
    object: &DfsObjectMetadata,
) -> Option<(String, PlacementReadPruneSelection)> {
    PLACEMENT_READ_PRUNE_CAPTURES
        .lock()
        .unwrap()
        .iter()
        .find_map(|(intent, selected)| {
            (selected.node_id == *service.node_id()
                && selected.object_id == object.object_id
                && selected.version == object.version
                && selected.content_hash == object.content_hash)
                .then(|| (intent.clone(), selected.clone()))
        })
}

pub(super) fn read_prune_capture_selected<T: DfsMessageTransport>(
    service: &NetworkedDfsService<T>,
    object: &DfsObjectMetadata,
) -> bool {
    selected_read_prune_capture(service, object).is_some()
}

pub(super) fn capture_read_prune<T: DfsMessageTransport>(
    service: &NetworkedDfsService<T>,
    backend_object: Option<&DfsObjectMetadata>,
    before: &[NodeId],
    filtered: &DfsObjectMetadata,
) {
    let Some((intent_id, selected)) = selected_read_prune_capture(service, filtered) else {
        return;
    };
    let pruned = service
        .reads
        .transient_remote_read_pruned_locations
        .get(&(
            selected.object_id.clone(),
            selected.version.clone(),
            selected.content_hash.clone(),
        ))
        .cloned()
        .unwrap_or_default();
    if !pruned.contains(&selected.node_id)
        || !before.contains(&selected.node_id)
        || filtered.current_locations.contains(&selected.node_id)
    {
        return;
    }
    // Observe the existing read, not a second current_read/namespace operation.
    // The map lock is never held while the actual backend is inspected.
    let observed = (|| -> Result<Option<serde_json::Value>, DfsError> {
        let Some(raw) = backend_object else {
            return Ok(None);
        };
        if raw.object_id != selected.object_id
            || raw.version != selected.version
            || raw.content_hash != selected.content_hash
            || !raw.current_locations.contains(&selected.node_id)
        {
            return Ok(None);
        }
        let Some(backend) = service.metadata_backend() else {
            return Ok(None);
        };
        let Some(owner) = backend.get_intent(&selected.object_id, &intent_id)? else {
            return Ok(None);
        };
        if !owner.publication.is_current()
            || owner.status != crate::DfsWriteIntentStatus::Committed
            || owner.target_version != selected.version
            || owner.content_hash != selected.content_hash
            || !owner.desired_replica_node_ids.contains(&selected.node_id)
        {
            return Ok(None);
        }
        let receipts = backend.receipts_for_version(&selected.object_id, &selected.version)?;
        let positive = receipts
            .iter()
            .filter(|receipt| {
                receipt.receipt.file_system_id == raw.file_system_id
                    && receipt.receipt.object_id == selected.object_id
                    && receipt.receipt.committed_version == selected.version
                    && receipt.replica_node_id == selected.node_id
                    && receipt.content_hash == selected.content_hash
            })
            .collect::<Vec<_>>();
        if positive.is_empty() {
            return Ok(None);
        }
        Ok(Some(serde_json::json!({
            "status": "observed",
            "intent_id": intent_id,
            "node_id": selected.node_id,
            "object_id": selected.object_id,
            "version": selected.version,
            "content_hash": selected.content_hash,
            "raw_backend_locations": raw.current_locations,
            "locations_before_prune": before,
            "filtered_locations": filtered.current_locations,
            "exact_prune_set": pruned,
            "positive_receipts": positive,
            "desired_replica_nodes": owner.desired_replica_node_ids,
            "qualification": "actual read projection suppressed a raw/receipt-backed exact target; no invalidation or extra namespace read"
        })))
    })();
    let record = match observed {
        Ok(Some(record)) => record,
        Ok(None) => return,
        Err(error) => {
            serde_json::json!({"status":"unavailable", "intent_id":intent_id, "error":error.to_string()})
        }
    };
    if PLACEMENT_READ_PRUNE_CAPTURES
        .lock()
        .unwrap()
        .remove(&intent_id)
        .is_some()
    {
        eprintln!("NATIVE_RESTORED_TARGET_PRUNE {}", record);
    }
}

struct PlacementBirthInterrupt {
    intent_id: String,
    capture: std::sync::Arc<PlacementBirthCapture>,
}

impl PlacementBirthInterrupt {
    fn new(
        intent_id: String,
        prior_intent_id: String,
        restored_node: &NodeId,
        object: &DfsObjectMetadata,
    ) -> Self {
        assert!(
            PLACEMENT_READ_PRUNE_CAPTURES
                .lock()
                .unwrap()
                .insert(
                    intent_id.clone(),
                    PlacementReadPruneSelection {
                        node_id: restored_node.clone(),
                        object_id: object.object_id.clone(),
                        version: object.version.clone(),
                        content_hash: object.content_hash.clone(),
                    },
                )
                .is_none()
        );
        let capture = std::sync::Arc::new(PlacementBirthCapture {
            prior_intent_id,
            cut: std::sync::Mutex::new(None),
        });
        assert!(
            PLACEMENT_BIRTH_INTERRUPTS
                .lock()
                .unwrap()
                .insert(intent_id.clone(), capture.clone())
                .is_none()
        );
        Self { intent_id, capture }
    }
    fn reached(&self) -> bool {
        self.capture.cut.lock().unwrap().is_some()
    }
    fn take(&self) -> PlacementBirthCut {
        self.capture.cut.lock().unwrap().take().unwrap().unwrap()
    }
}

impl Drop for PlacementBirthInterrupt {
    fn drop(&mut self) {
        PLACEMENT_READ_PRUNE_CAPTURES
            .lock()
            .unwrap()
            .remove(&self.intent_id);
        PLACEMENT_BIRTH_INTERRUPTS
            .lock()
            .unwrap()
            .remove(&self.intent_id);
    }
}

#[test]
fn native_two_current_owners_resume_the_exact_adaptive_job_after_interrupted_birth() {
    let (runtime, service, object, observed) =
        native_current_policy_fixture_with_nodes("native-two-current-placement-owners", 4);
    let primary = service.node_id().clone();
    let config = observed.accepted_config.clone();
    let coordinator = config.owner_node_id.clone();
    let original_durability = service
        .metadata_backend()
        .unwrap()
        .get_version(&object.object_id, &object.version)
        .unwrap()
        .unwrap()
        .content_durability
        .unwrap();
    assert_eq!(original_durability.desired_replica_node_ids.len(), 3);
    let first_excluded = &original_durability.desired_replica_node_ids[0];
    let second_excluded = &original_durability.desired_replica_node_ids[1];
    let candidates_a = observed
        .candidate_snapshot
        .iter()
        .filter(|node| &node.node_id != first_excluded)
        .cloned()
        .collect::<Vec<_>>();
    let candidates_b = observed
        .candidate_snapshot
        .iter()
        .filter(|node| &node.node_id != second_excluded)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(candidates_a.len(), 3);
    assert_eq!(candidates_b.len(), 3);
    assert_ne!(candidates_a, candidates_b);
    let make_job = |candidates, path: &str, scope| {
        crate::control_plane::build_adaptive_placement_reconciliation_job(
            sinomo_core::Id::generate().to_string(),
            &config,
            path.to_owned(),
            scope,
            BTreeSet::new(),
            candidates,
            observed.placement_snapshot.clone().unwrap(),
            crate::DfsAdaptivePlacementBudget::default(),
            0,
            dfs_now_unix_nanos(),
        )
        .unwrap()
    };
    let prior = make_job(candidates_a, &object.path, DfsPlacementScope::SelfOnly);
    let prior_intent_id = format!(
        "placement:{}:{}:{}",
        prior.job_id, object.object_id, object.version
    );
    let control = runtime.shared_control_plane().unwrap();
    drop(service);
    assert!(control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_placement_update(&config, &config, &prior)
            .unwrap()
            .is_some()
    }));
    let repair_evidence = || {
        let attempts = crate::repair_executor::repair_executor_status_sample(
            &runtime.data_dir(&primary).root(),
            16,
        );
        let deliveries = runtime
            .rpc_observations()
            .into_iter()
            .rev()
            .filter(|call| call.route == crate::DFS_DATA_REPAIR_REPLICA_ROUTE)
            .take(12)
            .map(|call| (call.source, call.target, call.phase, call.outcome_error))
            .collect::<Vec<_>>();
        (attempts, deliveries)
    };
    let shard = crate::select_metadata_shard_config_for_path(
        &object.file_system_id,
        &runtime.metadata_shards().unwrap(),
        &object.path,
    )
    .unwrap()
    .unwrap();
    assert_eq!(shard.primary_node_id, primary);
    let shard_id = crate::DfsMetadataShardId::new(shard.shard_id);
    let objects = BTreeSet::from([object.object_id.clone()]);
    let finish_job = |job_id: &str, intent_id: &str, deadline: Instant| {
        loop {
            let current = control.with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&object.file_system_id, job_id)
                    .unwrap()
                    .unwrap()
            });
            assert_eq!(current.job_id, job_id);
            assert!(
                Instant::now() < deadline,
                "adaptive job missed its budget: {current:?}, repair={:?}",
                repair_evidence()
            );
            if current.state == DfsPlacementReconciliationState::Completed {
                assert_eq!(current.pending_repair_count, 0);
                break;
            }
            assert_ne!(
                current.state,
                DfsPlacementReconciliationState::Failed,
                "{current:?}"
            );
            if current.state == DfsPlacementReconciliationState::Repairing {
                // The executor now owns the outstanding copy. Observe its edge
                // before reading the index so a concurrent receipt cannot be
                // missed. Forcing another placement scan here competes with
                // that receipt for the same policy-mutation permit.
                let seen = runtime
                    .repair_executor_progress(&primary)
                    .unwrap_or_default();
                let index = runtime
                    .node_metadata(&primary)
                    .unwrap()
                    .repair_index_objects(&shard_id, &objects)
                    .unwrap();
                if index
                    .needs
                    .iter()
                    .any(|(_, need)| need.owner_intent_id.as_deref() == Some(intent_id))
                {
                    assert!(
                        runtime.wait_for_repair_executor_progress(&primary, seen, deadline),
                        "adaptive repair made no progress: {current:?}, repair={:?}",
                        repair_evidence()
                    );
                    continue;
                }
            }
            for owner in
                std::iter::once(&coordinator).chain((&primary != &coordinator).then_some(&primary))
            {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(
                    !remaining.is_zero(),
                    "adaptive job budget exhausted: {current:?}"
                );
                let tick = runtime.maintenance_tick(owner, remaining).unwrap();
                assert!(
                    tick.outcome.is_ok(),
                    "adaptive continuation failed on {owner}: {:?}, repair={:?}",
                    tick.outcome,
                    repair_evidence()
                );
            }
        }
    };
    finish_job(
        &prior.job_id,
        &prior_intent_id,
        Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    );
    let mut service = runtime.metadata_primary_service(&object.path).unwrap();
    assert_eq!(service.node_id(), &primary);
    let current = service
        .current_read_object_metadata(&object.object_id)
        .unwrap();
    assert_eq!(current.version, object.version);
    assert_eq!(current.content_hash, object.content_hash);
    let prior_owner = service
        .metadata_backend()
        .unwrap()
        .get_intent(&object.object_id, &prior_intent_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            prior_owner.policy.min_commit_replicas,
            prior_owner.policy.desired_replicas
        ),
        (2, 3)
    );
    assert!(
        service
            .metadata_backend()
            .unwrap()
            .get_intent_receipt(&object.object_id, &prior_intent_id)
            .unwrap()
            .is_none()
    );
    let job = make_job(
        candidates_b,
        "/services/native-policy-preservation",
        DfsPlacementScope::Subtree,
    );
    let (_, policy, placement) = service
        .reconciliation_placement_for_object_with_pins(&job.accepted_config, &job, &current, &[])
        .unwrap();
    assert_eq!(
        (policy.min_commit_replicas, policy.desired_replicas),
        (2, 3)
    );
    assert_ne!(
        placement.plan.desired_replica_nodes,
        prior_owner.desired_replica_node_ids
    );
    let intent_id = format!(
        "placement:{}:{}:{}",
        job.job_id, object.object_id, object.version
    );
    let interruption = PlacementBirthInterrupt::new(
        intent_id.clone(),
        prior_intent_id.clone(),
        &primary,
        &object,
    );
    drop(service);
    assert!(control.with_client(|kv| {
        DfsControlPlaneStore::new(kv.as_mut())
            .compare_and_put_placement_update(&config, &config, &job)
            .unwrap()
            .is_some()
    }));
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    while !interruption.reached() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "actual adaptive B publication cut was not reached"
        );
        let tick = runtime.maintenance_tick(&coordinator, remaining).unwrap();
        assert!(
            tick.outcome.is_ok() || interruption.reached(),
            "unexpected native B failure: {:?}",
            tick.outcome
        );
    }
    assert!(
        Instant::now() < deadline,
        "birth observed after the native deadline"
    );
    let cut = interruption.take();
    drop(interruption);
    assert_eq!(cut.object.version, object.version);
    assert_eq!(cut.object.content_hash, object.content_hash);
    assert_eq!(cut.prior, prior_owner);
    assert_eq!(cut.current.intent_id, intent_id);
    assert!(crate::placement::current_placement_repair_owner_matches_identity(&cut.prior));
    assert!(crate::placement::current_placement_repair_owner_matches_identity(&cut.current));
    assert!(
        cut.global_error
            .contains("multiple current placement owners")
    );
    assert_eq!(cut.exact.policy, policy);
    assert_eq!(
        cut.exact.desired_replica_node_ids,
        placement.plan.desired_replica_nodes
    );
    eprintln!(
        "NATIVE_TWO_OWNER_CUT job={} prior={} current={} object={} version={} global_error={}",
        job.job_id,
        cut.prior.intent_id,
        cut.current.intent_id,
        object.object_id,
        object.version,
        cut.global_error
    );
    // The captured rows came from the actual author at PutIntent, before its
    // old-owner retirement. Neither an observation nor this hook grants replay.
    let mut caller = runtime.client(&primary).unwrap();
    let wrong_job = caller.route_namespace_operation(
        primary.clone(),
        DfsNamespaceOperation::ReconcilePlacement {
            job_id: sinomo_core::Id::generate().to_string(),
            path: object.path.clone(),
            object_id: object.object_id.clone(),
            expected_version: object.version.clone(),
            object_policy: object.policy.clone(),
            replication_policy: policy.clone(),
            desired_replica_node_ids: cut.exact.desired_replica_node_ids.clone(),
        },
    );
    assert!(
        wrong_job.is_err(),
        "another job cannot borrow the committed owner"
    );
    drop(caller);
    service = runtime.metadata_primary_service(&object.path).unwrap();
    let mut wrong_config = job.accepted_config.clone();
    upsert_placement_rule(
        &mut wrong_config.subtree_policies,
        normalize_placement_rule(
            &object.path,
            DfsPlacementScope::SelfOnly,
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap(),
        )
        .unwrap(),
    );
    assert!(
        !service
            .managed_placement_intent_matches_job(
                &crate::intent::distributed_intent_from_record(&cut.current),
                &cut.object,
                &job,
                &wrong_config,
            )
            .unwrap(),
        "an uninstalled conflicting config cannot validate this job payload"
    );
    assert_eq!(
        service
            .metadata_backend()
            .unwrap()
            .get_intent(&object.object_id, &intent_id)
            .unwrap()
            .unwrap(),
        cut.current
    );
    drop(service);
    finish_job(&job.job_id, &intent_id, deadline);
    let mut service = runtime.client(&primary).unwrap();
    let current = service
        .current_read_object_metadata_distributed(&object.object_id)
        .unwrap();
    let store = runtime.node_metadata(&primary).unwrap();
    let backend = &store;
    assert!(
        backend
            .get_intent_receipt(&object.object_id, &prior_intent_id)
            .unwrap()
            .is_some()
    );
    let history = backend
        .object_write_intent_history(&object.object_id)
        .unwrap();
    assert!(
        history
            .intents
            .iter()
            .all(|owner| owner.intent_id != prior_intent_id)
    );
    assert!(history.receipts.iter().any(|receipt| {
        receipt.intent_id == prior_intent_id && receipt.matches_intent(&cut.prior)
    }));
    assert!(
        crate::content_durability::current_placement_durability_for_intent_from_backend(
            backend,
            &current,
            &prior_intent_id,
        )
        .unwrap()
        .is_none(),
        "a reconstructed terminal receipt is not an executable placement owner"
    );
    assert_eq!(
        backend
            .get_intent(&object.object_id, &intent_id)
            .unwrap()
            .unwrap(),
        cut.current
    );
    assert_eq!(
        crate::content_durability::current_content_durability_from_backend(backend, &current)
            .unwrap(),
        Some(cut.exact)
    );
    assert_eq!(
        backend
            .get_version(&object.object_id, &object.version)
            .unwrap()
            .unwrap()
            .content_durability,
        Some(original_durability.clone())
    );
    // A later sibling policy invalidates a subtree-wide mutation, but cannot
    // invalidate the completed owner's exact receipt on this unchanged file.
    runtime
        .apply_placement_policy(
            &primary,
            "/services/native-policy-preservation/other",
            DfsPlacementScope::Subtree,
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap(),
        )
        .unwrap();
    drop(service);
    let mut service = runtime.metadata_primary_service(&object.path).unwrap();
    assert!(
        service
            .acquire_authoritative_placement_reconciliation(
                &object.file_system_id,
                &job.job_id,
                Some(&object.path),
            )
            .unwrap()
            .is_none()
    );
    let target = cut
        .current
        .desired_replica_node_ids
        .iter()
        .find(|node| !original_durability.desired_replica_node_ids.contains(node))
        .expect("adaptive target outside the original write quorum");
    assert!(
        service
            .with_table_intent_placement_authority(
                &object.object_id,
                &prior_intent_id,
                &object.version,
                target,
                object.content_hash.as_deref().unwrap(),
                |_| Ok(()),
            )
            .unwrap()
            .is_none(),
        "compacted placement must not regain execution authority"
    );
    assert!(
        service
            .with_table_intent_placement_authority(
                &object.object_id,
                &intent_id,
                &object.version,
                target,
                "wrong hash",
                |_| Ok(()),
            )
            .unwrap()
            .is_none()
    );
    let previous = service
        .backend_receipt_for_replica(&object.object_id, &object.version, target)
        .unwrap()
        .expect("the completed job published this replica");
    // Recreate the late receipt deficit on the same bytes. Use the actual
    // withdrawal path and wait for the content worker to publish its new proof.
    service
        .mark_remote_read_location_missing(
            &object.object_id,
            &object.version,
            &object.content_hash,
            target,
        )
        .unwrap();
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let seen = runtime
            .repair_executor_progress(&primary)
            .unwrap_or_default();
        if let Some(receipt) = service
            .backend_receipt_for_replica(&object.object_id, &object.version, target)
            .unwrap()
            && receipt.generation > previous.generation
        {
            assert_eq!(receipt.content_hash, previous.content_hash);
            assert_ne!(receipt.receipt.event_id, previous.receipt.event_id);
            break;
        }
        assert!(
            runtime.wait_for_repair_executor_progress(&primary, seen, deadline),
            "completed placement's replica receipt did not settle: {:?}",
            repair_evidence()
        );
    }
    runtime
        .apply_placement_policy(
            &primary,
            &object.path,
            DfsPlacementScope::SelfOnly,
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap(),
        )
        .unwrap();
    assert_eq!(
        service
            .with_table_intent_placement_authority(
                &object.object_id,
                &intent_id,
                &object.version,
                target,
                object.content_hash.as_deref().unwrap(),
                |_| Ok(true)
            )
            .unwrap(),
        None,
        "a policy change applying to the file must still fence its old receipt",
    );
    drop(service);
    drop(control);
    runtime.close().unwrap();
}

#[test]
fn native_same_service_local_receipt_supersedes_its_negative_read_prune() {
    native_same_service_local_receipt_control(false);
}

#[test]
fn native_local_receipt_preserves_other_and_later_negative_read_evidence() {
    native_same_service_local_receipt_control(true);
}

fn native_same_service_local_receipt_control(check_negatives: bool) {
    let label = if check_negatives {
        "native-local-receipt-negative-read-fences"
    } else {
        "native-same-service-local-receipt-prune"
    };
    let path = "/services/same-service-local-receipt.bin";
    let (runtime, mut writer, assignment) =
        crate::test_support::current_native_metadata_service(label, 3, path).unwrap();
    let primary = assignment.primary_node_id.clone();
    let nodes = crate::test_support::DfsTestRuntime::ordered_node_ids(label, 3);
    let expected = nodes.iter().cloned().collect::<BTreeSet<_>>();
    let minimum = if check_negatives { 1 } else { 2 };
    let old_version = if check_negatives {
        Some(
            writer
                .posix_write_file(
                    path,
                    b"older real bytes for nonmatching read identities",
                    crate::DfsWriteOptions {
                        policy: DfsReplicationPolicy::new(minimum, 3).unwrap(),
                        replica_candidates: nodes.clone(),
                        wait_for_commit: true,
                        foreground_replica_repair: true,
                    },
                )
                .unwrap(),
        )
    } else {
        None
    };
    let written = writer
        .posix_write_file(
            path,
            b"real local bytes survive metadata withdrawal and are verified again",
            crate::DfsWriteOptions {
                policy: DfsReplicationPolicy::new(minimum, 3).unwrap(),
                replica_candidates: nodes,
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    drop(writer);
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let (intent, old_receipt) = loop {
        let inspect = runtime.metadata_primary_service(path).unwrap();
        let backend = inspect.metadata_backend().unwrap();
        let receipts = backend
            .receipts_for_version(&written.object_id, &written.version)
            .unwrap();
        let matched = receipts
            .iter()
            .filter(|receipt| {
                receipt.receipt.committed_version == written.version
                    && receipt.content_hash == written.content_hash
            })
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>();
        if matched == expected {
            let history = backend
                .object_write_intent_history(&written.object_id)
                .unwrap();
            let intent = history
                .intents
                .into_iter()
                .find(|intent| {
                    intent.target_version == written.version
                        && intent.content_hash == written.content_hash
                        && intent.status == crate::DfsWriteIntentStatus::Committed
                        && intent.publication.is_current()
                        && !intent.intent_id.starts_with("placement:")
                })
                .expect("actual current write retains its full repair owner");
            let receipt = receipts
                .into_iter()
                .find(|receipt| receipt.replica_node_id == primary)
                .unwrap();
            drop(inspect);
            break (intent, receipt);
        }
        drop(inspect);
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "normal N3 setup did not settle");
        let tick = runtime.maintenance_tick(&primary, remaining).unwrap();
        assert!(tick.outcome.is_ok(), "normal N3 setup: {:?}", tick.outcome);
    };
    // Select this one real Core producer while preserving its own read state.
    // The existing negative-only hook excludes competing autonomous recovery.
    let pause = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        runtime.data_dir(&primary),
        std::sync::Arc::new(|point, command, _context| {
            if matches!(
                point,
                crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight
            ) && command.is_none()
            {
                return Err(DfsError::Cancelled {
                    operation: "isolated same-service local receipt control",
                });
            }
            Ok(())
        }),
    );
    let mut service = runtime.metadata_primary_service(path).unwrap();
    let admission = service
        .acquire_prepared_execution(&crate::DfsPreparedOperationScope {
            file_system_id: assignment.file_system_id,
            shard_id: crate::DfsMetadataShardId::new(assignment.shard_id),
            owner_epoch: assignment.epoch,
            operation_id: format!("native-same-service-local-receipt:{}", written.object_id),
        })
        .unwrap()
        .expect("the actual owner grants ExactShard admission");
    let hash = written.content_hash.as_deref().unwrap();
    let other_node = expected
        .iter()
        .find(|node| **node != primary)
        .unwrap()
        .clone();
    if let Some(old) = old_version.as_ref() {
        assert_eq!(old.object_id, written.object_id);
        assert_ne!(old.version, written.version);
        assert_ne!(old.content_hash, written.content_hash);
        // The W1 negative variant leaves a real third holder while withdrawing
        // two locations. Nonmatching request identities never alter metadata.
        service
            .mark_remote_read_location_missing(
                &written.object_id,
                &written.version,
                &written.content_hash,
                &other_node,
            )
            .unwrap();
        service
            .mark_remote_read_location_missing(
                &written.object_id,
                &old.version,
                &written.content_hash,
                &primary,
            )
            .unwrap();
        service
            .mark_remote_read_location_missing(
                &written.object_id,
                &written.version,
                &old.content_hash,
                &primary,
            )
            .unwrap();
    }
    service
        .mark_remote_read_location_missing(
            &written.object_id,
            &written.version,
            &written.content_hash,
            &primary,
        )
        .unwrap();
    assert!(
        service
            .backend_receipt_for_replica(&written.object_id, &written.version, &primary)
            .unwrap()
            .is_none()
    );
    assert!(
        !service
            .current_read_object_metadata(&written.object_id)
            .unwrap()
            .current_locations
            .contains(&primary)
    );
    // Rebuild only from actual durable rows. Hydration preserves the reader's
    // negative cache and must not invent a replacement intent or receipt.
    service.hydrate_from_metadata_backend().unwrap();
    assert!(
        service
            .coordinator
            .receipts_for_intent(&intent.intent_id)
            .iter()
            .all(|receipt| receipt.replica_node_id != primary)
    );
    assert!(
        !service
            .current_read_object_metadata(&written.object_id)
            .unwrap()
            .current_locations
            .contains(&primary)
    );
    service
        .record_local_pending_write_receipt_if_desired(
            &intent.intent_id,
            &written.object_id,
            &written.version,
            hash,
        )
        .unwrap();
    let fresh = service
        .backend_receipt_for_replica(&written.object_id, &written.version, &primary)
        .unwrap()
        .expect("fresh local verification published its actual receipt");
    assert_ne!(fresh.receipt.event_id, old_receipt.receipt.event_id);
    let raw = service
        .metadata_backend()
        .unwrap()
        .get_object(&written.object_id)
        .unwrap()
        .unwrap();
    assert!(raw.current_locations.contains(&primary));
    let filtered = service
        .current_read_object_metadata(&written.object_id)
        .unwrap();
    let pruned = service
        .reads
        .transient_remote_read_pruned_locations
        .get(&(
            written.object_id.clone(),
            written.version.clone(),
            written.content_hash.clone(),
        ))
        .cloned()
        .unwrap_or_default();
    eprintln!(
        "NATIVE_SAME_SERVICE_LOCAL_RECEIPT {}",
        serde_json::json!({
            "intent_id": intent.intent_id, "object_id": written.object_id,
            "version": written.version, "content_hash": written.content_hash,
            "node_id": primary, "old_receipt": old_receipt, "fresh_receipt": fresh,
            "raw_locations": raw.current_locations, "filtered_locations": filtered.current_locations,
            "exact_prune_set": pruned,
        })
    );
    if let Some(old) = old_version.as_ref() {
        assert!(filtered.current_locations.contains(&primary));
        assert!(!filtered.current_locations.contains(&other_node));
        // Project requested reads through the real consumer. These deliberately
        // nonmatching identities are DATA only, never installed as metadata.
        for (version, hash) in [
            (old.version.clone(), written.content_hash.clone()),
            (written.version.clone(), old.content_hash.clone()),
        ] {
            let mut requested = raw.clone();
            requested.version = version;
            requested.content_hash = hash;
            requested.current_locations = vec![primary.clone()];
            service.apply_transient_remote_read_location_prunes(&mut requested);
            assert!(requested.current_locations.is_empty());
        }
        // A generic historical claim is not a fresh local verification. It
        // cannot unmask the other target, even when receipt replay deduplicates.
        service
            .put_metadata_receipt(
                &written.file_system_id,
                &written.object_id,
                &written.version,
                &other_node,
                written.content_hash.as_deref().unwrap(),
                service
                    .backend_receipt_slot_generation(
                        &written.object_id,
                        &written.version,
                        &other_node,
                    )
                    .unwrap(),
            )
            .unwrap();
        assert!(
            !service
                .put_metadata_receipt(
                    &written.file_system_id,
                    &written.object_id,
                    &written.version,
                    &other_node,
                    written.content_hash.as_deref().unwrap(),
                    service
                        .backend_receipt_slot_generation(
                            &written.object_id,
                            &written.version,
                            &other_node
                        )
                        .unwrap(),
                )
                .unwrap()
        );
        assert!(
            !service
                .current_read_object_metadata(&written.object_id)
                .unwrap()
                .current_locations
                .contains(&other_node)
        );
        service
            .mark_remote_read_location_missing(
                &written.object_id,
                &written.version,
                &written.content_hash,
                &primary,
            )
            .unwrap();
        assert!(
            service
                .coordinator
                .receipts_for_intent(&intent.intent_id)
                .iter()
                .any(|receipt| receipt.replica_node_id == primary)
        );
        // The deduplicated producer branch does not establish a new receipt.
        service
            .record_local_pending_write_receipt_if_desired(
                &intent.intent_id,
                &written.object_id,
                &written.version,
                written.content_hash.as_deref().unwrap(),
            )
            .unwrap();
        assert!(
            !service
                .current_read_object_metadata(&written.object_id)
                .unwrap()
                .current_locations
                .contains(&primary)
        );
        service
            .put_metadata_receipt(
                &written.file_system_id,
                &written.object_id,
                &written.version,
                &primary,
                written.content_hash.as_deref().unwrap(),
                service
                    .backend_receipt_slot_generation(&written.object_id, &written.version, &primary)
                    .unwrap(),
            )
            .unwrap();
        assert!(
            !service
                .put_metadata_receipt(
                    &written.file_system_id,
                    &written.object_id,
                    &written.version,
                    &primary,
                    written.content_hash.as_deref().unwrap(),
                    service
                        .backend_receipt_slot_generation(
                            &written.object_id,
                            &written.version,
                            &primary
                        )
                        .unwrap(),
                )
                .unwrap()
        );
        let later = service
            .current_read_object_metadata(&written.object_id)
            .unwrap();
        assert!(!later.current_locations.contains(&primary));
        assert!(!later.current_locations.contains(&other_node));
    }
    drop(admission);
    drop(service);
    drop(pause);
    assert!(
        filtered.current_locations.contains(&primary),
        "fresh admitted local verification/publication must supersede the same reader's negative"
    );
    runtime.close().unwrap();
}
