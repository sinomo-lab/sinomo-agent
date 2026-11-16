//! Genuine source DATA plus real current native import/publication.
use super::*;
use crate::authority::{shared_owner, storage_preparation_provider};
use sinomo_dfs::{DfsStoragePreparation, DfsStoragePreparationScope};

pub(in crate::authority) fn native_migration_cluster(
    name: &str,
    count: usize,
) -> (
    sinomo_dfs::test_support::DfsTestRuntime,
    Vec<sinomo_core::node::NodeId>,
) {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, count);
    let mut native = sinomo_dfs::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        native.define_node(node.clone()).unwrap();
    }
    native
        .configure_component_factories(vec![
            Arc::new(
                sinomo_dfs::component_factory_with_storage_preparation(Arc::new(
                    storage_preparation_provider,
                ))
                .unwrap(),
            ),
            Arc::new(staged_import_component_factory().unwrap()),
        ])
        .unwrap();
    native.start_nodes().unwrap();
    (native, nodes)
}

pub(in crate::authority) fn canonical_migration_runtime(
    context: &sinomo_module::ComponentContext,
) -> Arc<crate::authority::MigrationOwner> {
    let root = context.data_dir_root().canonicalize().unwrap();
    let lifecycle = sinomo_dfs::dfs_storage_preparation_lifecycle(&root)
        .expect("normal DFS factory registered its actual provider lifecycle");
    let runtime = shared_owner(context, lifecycle.clone()).unwrap();
    assert!(runtime.lifecycle.same_channel(&lifecycle));
    runtime
}

/// Continue only an unfinished actual source-writer admission. The caller owns
/// the immutable request/plan and one acceptance deadline across all stages.
/// An in-flight normal RPC keeps its existing transport limits.
pub(in crate::authority) fn continue_native_source_step<T>(
    source_root: &std::path::Path,
    deadline: std::time::Instant,
    mut step: impl FnMut() -> std::result::Result<T, MetadataBackendError>,
) -> std::result::Result<T, MetadataBackendError> {
    let source_root = source_root.canonicalize().map_err(error)?;
    loop {
        if std::time::Instant::now() >= deadline {
            return Err(error(
                "native source stage exceeded its single acceptance deadline",
            ));
        }
        match step() {
            Err(MetadataBackendError::StoragePreparationPending {
                root,
                processed_records: 0,
            }) if root == source_root => std::thread::yield_now(),
            Ok(value) if std::time::Instant::now() < deadline => return Ok(value),
            Ok(_) => {
                return Err(error(
                    "native source stage completed after its acceptance deadline",
                ));
            }
            Err(error) => return Err(error),
        }
    }
}

/// Await current authority through the SAME normal native reader and request ID.
/// Only structured unavailable-authority responses continue; no outcome is
/// synthesized and all other failures/invalid proofs remain terminal.
pub(in crate::authority) fn native_request_outcome_before(
    runtime: &tokio::runtime::Handle,
    cluster: &Arc<dyn sinomo_core::service::ClusterAccess>,
    request_id: &KvRequestId,
    deadline: std::time::Instant,
) -> std::result::Result<sinomo_core::kv::ClusterKvRequestOutcomeRead, MetadataBackendError> {
    use sinomo_core::{network::NetworkError, service::ServiceError};
    block_native(runtime, async {
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(ServiceError::Service(
                    "native current-authority read exceeded its original acceptance deadline"
                        .into(),
                ));
            }
            match cluster
                .kv_request_outcome_authoritative_async(request_id.clone())
                .await
            {
                Ok(read) => {
                    sinomo_core::kv::validate_cluster_kv_request_outcome_read(&read)?;
                    if matches!(&read.outcome, sinomo_core::kv::KvRequestOutcome::Committed(record) if &record.request_id != request_id)
                    {
                        return Err(ServiceError::Service(
                            "native outcome names another request".into(),
                        ));
                    }
                    if std::time::Instant::now() >= deadline {
                        return Err(ServiceError::Service("native current-authority read completed after its original acceptance deadline".into()));
                    }
                    return Ok(read);
                }
                Err(error @ ServiceError::Network(NetworkError::ClusterReadOnly { .. })) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(error);
                    }
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
    })
}

fn native_evaluation_before<T>(
    deadline: std::time::Instant,
    mut evaluate: impl FnMut() -> std::result::Result<T, crate::authority::NativeEvaluationError>,
) -> std::result::Result<T, MetadataBackendError> {
    use sinomo_core::{network::NetworkError, service::ServiceError};
    loop {
        if std::time::Instant::now() >= deadline {
            return Err(error(
                "native evaluation exceeded its original acceptance deadline",
            ));
        }
        match evaluate() {
            Err(crate::authority::NativeEvaluationError::Native(ServiceError::Network(
                NetworkError::ClusterReadOnly { .. },
            ))) => std::thread::yield_now(),
            Err(error) => return Err(error.into_metadata()),
            Ok(value) if std::time::Instant::now() < deadline => return Ok(value),
            Ok(_) => {
                return Err(error(
                    "native evaluation completed after its original acceptance deadline",
                ));
            }
        }
    }
}

#[test]
fn native_source_continuation_requires_actual_own_writer_release_and_keeps_other_errors_closed() {
    use fs2::FileExt as _;
    let directory = sinomo_testdir::TestDir::new("native-source-continuation-guards");
    let own = directory.path().join("own");
    let foreign = directory.path().join("foreign");
    for root in [&own, &foreign] {
        std::fs::create_dir_all(root.join("journal")).unwrap();
    }
    let writer = std::fs::File::create(foreign.join("journal/write.lock")).unwrap();
    writer.lock_exclusive().unwrap();
    let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let mut attempts = 0;
    let foreign_result = continue_native_source_step(&own, deadline, || {
        attempts += 1;
        crate::metadata::retain_source_fence(&foreign)
    });
    assert!(
        matches!(foreign_result, Err(MetadataBackendError::StoragePreparationPending {
        root, processed_records: 0
    }) if root == foreign.canonicalize().unwrap())
    );
    assert_eq!(attempts, 1, "foreign custody cannot enter the continuation");
    let mut own_writer = Some(std::fs::File::create(own.join("journal/write.lock")).unwrap());
    own_writer.as_ref().unwrap().lock_exclusive().unwrap();
    let mut own_attempts = 0;
    let fence = continue_native_source_step(&own, deadline, || {
        own_attempts += 1;
        let result = crate::metadata::retain_source_fence(&own);
        if own_attempts == 1 {
            assert!(
                matches!(&result, Err(MetadataBackendError::StoragePreparationPending {
                root, processed_records: 0
            }) if *root == own.canonicalize().unwrap())
            );
            drop(own_writer.take());
        }
        result
    })
    .unwrap();
    assert_eq!(
        own_attempts, 2,
        "the original writer must actually release custody"
    );
    drop(fence);
    std::fs::remove_file(own.join("journal/write.lock")).unwrap();
    std::fs::create_dir(own.join("journal/write.lock")).unwrap();
    assert!(matches!(
        continue_native_source_step(&own, deadline, || {
            crate::metadata::retain_source_fence(&own)
        }),
        Err(MetadataBackendError::Serde(_))
    ));
    let mut attempted_after_deadline = false;
    assert!(
        continue_native_source_step(&own, std::time::Instant::now(), || {
            attempted_after_deadline = true;
            crate::metadata::retain_source_fence(&own)
        })
        .is_err()
    );
    assert!(!attempted_after_deadline);
}

#[derive(Serialize)]
struct OriginalV0Job<'a> {
    job_id: &'a str,
    file_system_id: &'a str,
    path: &'a str,
    scope: &'a str,
    state: &'a str,
    total_object_count: usize,
    scanned_object_count: usize,
    matched_object_count: usize,
    updated_object_count: usize,
    scheduled_repair_count: usize,
    completed_repair_count: usize,
    pending_repair_count: usize,
    revision: u64,
    created_at_unix_nanos: u64,
    updated_at_unix_nanos: u64,
}

#[test]
fn pending_v0_goal_imports_actual_native_sources_and_publishes_current_job() {
    pending_goal_with_current_permit(false);
}
#[test]
fn pending_v1_goal_imports_actual_native_sources_behind_a_current_permit() {
    pending_goal_with_current_permit(true);
}
fn pending_goal_with_current_permit(v1: bool) {
    let name = if v1 { "migration-native-v1-pending-placement-data" } else { "migration-native8-pending-placement-data" };
    let (mut native, nodes) = native_migration_cluster(name, 1);
    let node = &nodes[0];
    let context = native.component_context(node).unwrap();
    let runtime = canonical_migration_runtime(&context);
    let fs = sinomo_dfs::sifs_file_system_id();
    let advertisement_key = sinomo_dfs::content_service_key(node);
    let publication = Arc::new(tokio::sync::Notify::new());
    let observed_publication = Arc::clone(&publication);
    let _subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
        &sinomo_core::network::ClusterPaths::new(context.data_dir()),
        move |_| observed_publication.notify_one(),
    );
    block_native(&runtime.authority.runtime, async {
        tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async {
            loop {
                let notified = publication.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let read = runtime
                    .authority
                    .cluster()
                    .unwrap()
                    .kv_get_authoritative_exact_set_async(vec![advertisement_key.clone()])
                    .await
                    .unwrap();
                current_authority(&read.authority).unwrap();
                if read.value[0].as_ref().is_some_and(|entry| {
                    entry
                        .decode::<sinomo_dfs::DfsNodeAdvertisement>()
                        .is_ok_and(|advertisement| {
                            advertisement.accepts_content
                                && advertisement.storage_freshness
                                    == sinomo_dfs::DfsObservationFreshness::Fresh
                        })
                }) {
                    return;
                }
                notified.await;
            }
        })
        .await
    })
    .expect("ordinary sampled content advertisement before source import");
    // A genuine current producer creates unrelated job B and retains its
    // ordinary named permit while old job A is restored. No holder row is made
    // by this fixture, and the closure utility retains no shared CP mutex.
    let mut current = native.client(node).unwrap();
    let candidates = current.placement_content_candidates().unwrap();
    let mut store = DfsControlPlaneStore::new(current.control_plane_client_mut().unwrap());
    let config = store.get_file_system_config(&fs).unwrap().unwrap();
    let held_job = sinomo_dfs::build_placement_reconciliation_job(
        sinomo_core::Id::generate().to_string(),
        &config,
        "/users".into(),
        sinomo_dfs::DfsPlacementScope::Subtree,
        BTreeSet::new(),
        candidates,
        None,
        u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp_nanos()).unwrap(),
    )
    .unwrap();
    assert!(
        store
            .compare_and_put_placement_update(&config, &config, &held_job)
            .unwrap()
    );
    drop(store);
    drop(current);
    let source = OriginalV0Job {
        job_id: "original-placement-pending",
        file_system_id: fs.as_str(),
        path: "/users",
        scope: "subtree",
        state: "pending",
        total_object_count: 0,
        scanned_object_count: 0,
        matched_object_count: 0,
        updated_object_count: 0,
        scheduled_repair_count: 0,
        completed_repair_count: 0,
        pending_repair_count: 0,
        revision: 1,
        created_at_unix_nanos: 1,
        updated_at_unix_nanos: 1,
    };
    let (old_key, original) = if v1 {
        let goal = seed_pending_v1_goal(&native, node, &runtime, source.job_id);
        (goal.job.entry.key, goal.job.entry.value)
    } else {
        let old_key = format!(
            "/dfs/filesystems/{fs}/placement-reconciliations/{}",
            source.job_id
        );
        let original =
            sinomo_core::object::serde_ext::to_bytes(&source, 1, sinomo_core::object::LengthMode::U32)
                .unwrap();
        // Converter fixture input only: faithful original wire enters actual native
        // KV. No old DFS writer or successful synthetic authority receipt exists.
        let write = runtime
            .authority
            .commit(KvTxnRequest::generated(KvTxn {
                compares: vec![KvCompare::Missing {
                    key: old_key.clone(),
                }],
                success: vec![KvOp::Put {
                    key: old_key.clone(),
                    value: original.clone(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            }))
            .unwrap();
        NativeImportAuthority::require_committed(&write).unwrap();
        (old_key, original)
    };
    let goals = runtime
        .authority
        .capture_original_placement_goals(4)
        .unwrap()
        .unwrap();
    assert_eq!(goals.len(), 1);
    assert_eq!(goals[0].job.entry.value, original);
    let required = runtime
        .authority
        .required_original_placement_sources(&goals)
        .unwrap();
    assert!(
        !required.is_empty(),
        "pending original DATA needs actual native retirement"
    );
    let configuration_ready = runtime.configuration_ready.load(std::sync::atomic::Ordering::Acquire);
    let prepare_original_data = || -> Result<DfsStoragePreparation> {
        let retained = runtime.authority.read_exact(std::slice::from_ref(&old_key))?.0;
        Ok(if retained[0].entry.is_none() && runtime.authority.required_original_placement_sources(&goals)?.is_empty() {
            DfsStoragePreparation::Ready
        } else { DfsStoragePreparation::Pending { processed_records: 0 } })
    };
    let key = goals[0].job.current_job_key().unwrap();
    native
        .with_current_placement_permit(node, &fs, &held_job.job_id, || {
            runtime.schedule_placement_work().unwrap();
            // The actual unrelated current permit may defer storage admission.
            // Observe the normal attempt without claiming the import completed.
            match prepare_original_data() {
                Ok(DfsStoragePreparation::Ready | DfsStoragePreparation::Pending { .. })
                | Err(MetadataBackendError::StoragePreparationPending { .. }) => {}
                Err(failed) => panic!("canonical source preparation rejected original DATA: {failed}"),
            }
            assert!(
                runtime.authority.read_exact(&[key.clone()]).unwrap().0[0]
                    .entry
                    .is_none(),
                "current planner must not publish through another real named permit"
            );
            let pass = native
                .maintenance_tick(node, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
                .unwrap();
            if !v1 {
                assert!(
                    pass.outcome.is_ok(),
                    "ordinary maintenance must complete or defer while the original goal remains unimported: {:?}",
                    pass.outcome
                );
            } else {
                // This queue itself still contains an original nested codec.
                // Current readers must reject it until migration retires the
                // row; they cannot treat it as executable current placement.
                let original_entry = runtime.authority.read_exact(std::slice::from_ref(&old_key)).unwrap().0[0].entry.clone().unwrap();
                assert_eq!(original_entry.value, original);
                assert!(original_entry.decode::<sinomo_dfs::DfsPlacementReconciliationJob>().is_err());
            }
            assert_eq!(pass.omitted_kv_event_count, 0);
            assert!(
                !pass.committed_kv_events.iter().any(|event| {
                    event.key == key
                        && matches!(
                            &event.kind,
                            sinomo_core::kv::KvEventKind::Put { .. }
                        )
                }),
                "ordinary maintenance must not publish the original goal through another real named permit"
            );
            assert!(
                runtime.authority.read_exact(&[key.clone()]).unwrap().0[0]
                    .entry
                    .is_none()
            );
        })
        .unwrap(); // The existing current producer releases the SAME exact handle.
    let mut changed = runtime.binding().readiness;
    block_native(&native.runtime_handle(), async {
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(error("managed original placement exceeded its original observation deadline"));
            }
            match prepare_original_data() {
                Ok(DfsStoragePreparation::Ready) => break,
                Ok(DfsStoragePreparation::Pending { .. }) => {},
                Err(MetadataBackendError::StoragePreparationPending { .. }) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(failure) => return Err(failure),
            }
            tokio::time::timeout_at(deadline, changed.changed()).await
                .map_err(|_| error("managed original placement did not complete within the native deadline"))?
                .map_err(error)?;
        }
        Ok::<_, MetadataBackendError>(())
    }).unwrap();
    // Ready above includes the authoritative empty required-source observation;
    // another independent read can be transiently unavailable after that proof.
    assert!(
        !configuration_ready
            || runtime
                .configuration_ready
                .load(std::sync::atomic::Ordering::Acquire),
        "captured DATA conversion cannot revoke the actual factory readiness fact"
    );
    let receipt_deadline =
        std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    for original_scope in required {
        let root = runtime
            .metadata_root
            .join(&original_scope.assignment.shard_id);
        let marker: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("metadata-import-v7.json")).unwrap())
                .unwrap();
        let cut: crate::metadata::MetadataSourceCut =
            serde_json::from_value(marker["source_cut"].clone()).unwrap();
        assert_eq!(
            cut.source_format_version,
            Some(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION)
        );
        let (plan, stage, activation, proof) = native_evaluation_before(receipt_deadline, || {
            runtime.authority.evaluate_scope_retirement(
                &fs,
                &sinomo_dfs::DfsMetadataShardId::new(&original_scope.assignment.shard_id),
            )
        })
        .unwrap()
        .expect("actual activated import plus persisted exact native receipts");
        NativeImportAuthority::require_committed(&stage).unwrap();
        NativeImportAuthority::require_committed(&activation).unwrap();
        assert!(
            stage.result.replayed_committed_request && activation.result.replayed_committed_request
        );
        assert_eq!(proof.desired_shard.file_system_id, fs);
        assert!(matches!(
            proof.cause,
            sinomo_dfs::DfsMetadataShardHandoffCause::StorageImport { .. }
        ));
        assert_eq!(
            proof.retiring_shard.epoch.checked_add(1),
            Some(proof.desired_shard.epoch)
        );
        assert!(cut.projection_assignments.contains(&proof.retiring_shard));
        assert_eq!(plan.source_cut.scope_shard, cut.scope_shard);
        let refs = crate::metadata::data_reservation_refs(&cut).unwrap();
        assert_eq!(refs.len(), 1);
        assert!(cut.pending_operations.is_empty(), "original placement DATA must not become executable pending operations");
        assert_eq!(cut.retained_placement_reservations.len(), 1);
        let cohort = cut.namespace_count_cohort.as_ref().unwrap();
        cohort.validate_bound_cut(&cut).unwrap();
        let mut omitted = cut.clone();
        omitted.retained_placement_reservations.clear();
        assert!(cohort.validate_bound_cut(&omitted).is_err());
        let mut repeated = cut.clone();
        repeated.retained_placement_reservations.push(cut.retained_placement_reservations[0].clone());
        assert!(crate::metadata::data_reservation_refs(&repeated).is_err());
        assert!(cohort.validate_bound_cut(&repeated).is_err());
        let mut changed = original_scope.capsules[0].clone();
        changed["original_source_scope"]["shard_id"] = serde_json::json!("foreign-source");
        assert!(crate::metadata::RetainedPlacementReservation::from_capsule(changed).is_err());
        let mut changed = original_scope.capsules[0].clone();
        changed["source_row"]["version"] = serde_json::json!(u64::MAX);
        assert!(crate::metadata::RetainedPlacementReservation::from_capsule(changed).is_err());
        assert_eq!(
            refs[0],
            OriginalPlacementJob::verify_capsule(&original_scope.capsules[0]).unwrap()
        );
    }
    assert!(
        runtime.authority.read_exact(&[old_key.clone()]).unwrap().0[0]
            .entry
            .is_none()
    );
    let mut published = false;
    for _ in 0..64 {
        let pass = native
            .maintenance_tick(node, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap();
        assert!(
            pass.outcome.is_ok(),
            "actual normal served maintenance: {:?}",
            pass.outcome
        );
        if let Some(entry) = runtime.authority.read_exact(&[key.clone()]).unwrap().0[0]
            .entry
            .as_ref()
        {
            let job: sinomo_dfs::DfsPlacementReconciliationJob = entry.decode().unwrap();
            assert_eq!(job.path, source.path);
            assert_eq!(job.scope, sinomo_dfs::DfsPlacementScope::Subtree);
            assert_eq!(job.file_system_id, fs);
            assert!(!job.candidate_snapshot.is_empty());
            published = true;
            break;
        }
    }
    assert!(
        published,
        "same normal current builder must durably reissue original pending DATA"
    );
    drop(changed);
    drop(runtime);
    drop(context);
    native.close().unwrap();
}

#[test]
fn received_uncommitted_v1_custody_data_never_becomes_its_first_native_effect() {
    let name = "migration-native-v1-unknown-custody-data";
    let (mut native, nodes) = native_migration_cluster(name, 1);
    let node = &nodes[0];
    let context = native.component_context(node).unwrap();
    let runtime = canonical_migration_runtime(&context);
    let fs = sinomo_dfs::sifs_file_system_id();
    let mut normal = native.client(node).unwrap();
    let mut store = DfsControlPlaneStore::new(normal.control_plane_client_mut().unwrap());
    let config = store.get_file_system_config(&fs).unwrap().unwrap();
    let candidates = store.cluster_view().unwrap().content_nodes;
    assert!(
        !candidates.is_empty(),
        "actual normal advertisements are required"
    );
    let now = u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp_nanos()).unwrap();
    let current = sinomo_dfs::build_placement_reconciliation_job(
        "source-v1-unknown-custody".into(),
        &config,
        "/users".into(),
        sinomo_dfs::DfsPlacementScope::Subtree,
        BTreeSet::new(),
        candidates,
        None,
        now,
    )
    .unwrap();
    drop(store);
    drop(normal);
    // Original source fixture DATA only. Identity, clocks and candidates come
    // from the actual normal native context. No old executor/authority exists.
    let mut original = serde_json::to_value(&current).unwrap();
    for field in ["retention_policies", "module_metadata", "metadata_format"] {
        original["accepted_config"]
            .as_object_mut()
            .unwrap()
            .remove(field);
    }
    if let Some(rules) = original["accepted_config"].get_mut("subtree_policies") {
        for rule in rules.as_array_mut().expect("actual accepted policy list") {
            rule.as_object_mut().unwrap().remove("min_commit_replicas");
            rule.as_object_mut().unwrap().remove("desired_replicas");
        }
    }
    let key = sinomo_dfs::placement_reconciliation_key(&fs, &current.job_id);
    let input = runtime
        .authority
        .commit(KvTxnRequest::generated(KvTxn {
            compares: vec![KvCompare::Missing { key: key.clone() }],
            success: vec![KvOp::Put {
                key: key.clone(),
                value: sinomo_core::kv::encode_kv_value(&original).unwrap(),
                lease_id: None,
            }],
            failure: Vec::new(),
        }))
        .unwrap();
    NativeImportAuthority::require_committed(&input).unwrap();
    let goals = runtime
        .authority
        .capture_original_placement_goals(4)
        .unwrap()
        .unwrap();
    let goal = goals.iter().find(|goal| goal.job.entry.key == key).unwrap();
    let coordinator = runtime
        .authority
        .placement_coordinator(goal)
        .unwrap()
        .unwrap();
    assert_eq!(coordinator.node_id, *node);
    let mut captured = vec![MetadataCapturedKvEntry {
        key: key.clone(),
        entry: Some(goal.job.entry.clone()),
    }];
    captured.extend(coordinator.guards.clone());
    let mut compares = OriginalPlacementJob::exact_retirement_compares(&captured);
    compares.push(KvCompare::Missing {
        key: sinomo_dfs::placement_authority_permit_key(&fs),
    });
    let proposal = KvTxnRequest::generated(KvTxn {
        compares,
        success: vec![KvOp::Delete { key: key.clone() }],
        failure: Vec::new(),
    });
    let envelope = PlacementCustodyEnvelope {
        goal: goal.clone(),
        phase: PlacementCustodyPhase::Retirement,
        coordinator,
        request: proposal.clone(),
        blueprints: Vec::new(),
    };
    let wrong_id = KvRequestId::generate();
    let proof_deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    for id in [&proposal.request_id, &wrong_id] {
        let outcome = native_request_outcome_before(
            &runtime.authority.runtime,
            runtime.authority.cluster().unwrap(),
            id,
            proof_deadline,
        )
        .unwrap();
        current_authority(&outcome.authority).unwrap();
        assert!(matches!(
            outcome.outcome,
            sinomo_core::kv::KvRequestOutcome::Unknown { .. }
        ));
    }
    let stored = runtime
        .authority
        .receive_placement_custody(
            node.clone(),
            PlacementCustodyRequest::Store {
                envelope: envelope.clone(),
            },
        )
        .unwrap();
    assert_eq!(stored.stored_request_id, Some(proposal.request_id.clone()));
    assert!(
        !native_evaluation_before(proof_deadline, || {
            runtime
                .authority
                .evaluate_received_placement_custody(goal, &envelope)
        })
        .unwrap(),
        "unknown received proposal DATA with missing actual retirements must not commit"
    );
    let after = runtime.authority.read_exact(&[key.clone()]).unwrap().0[0]
        .entry
        .clone()
        .unwrap();
    assert_eq!(
        after, goal.job.entry,
        "archive custody must leave the exact old row unchanged"
    );
    let mut wrong = envelope.clone();
    wrong.request.request_id = wrong_id.clone();
    assert!(
        !native_evaluation_before(proof_deadline, || {
            runtime
                .authority
                .evaluate_received_placement_custody(goal, &wrong)
        })
        .unwrap()
    );
    let mut changed = envelope.clone();
    changed.request.txn.success.push(KvOp::Delete {
        key: "/unrelated-received-data".into(),
    });
    assert!(
        !native_evaluation_before(proof_deadline, || {
            runtime
                .authority
                .evaluate_received_placement_custody(goal, &changed)
        })
        .unwrap()
    );
    assert_eq!(
        runtime.authority.read_exact(&[key]).unwrap().0[0]
            .entry
            .as_ref(),
        Some(&goal.job.entry)
    );
    for id in [&proposal.request_id, &wrong_id] {
        let outcome = native_request_outcome_before(
            &runtime.authority.runtime,
            runtime.authority.cluster().unwrap(),
            id,
            proof_deadline,
        )
        .unwrap();
        current_authority(&outcome.authority).unwrap();
        assert!(matches!(
            outcome.outcome,
            sinomo_core::kv::KvRequestOutcome::Unknown { .. }
        ));
    }
    drop(runtime);
    drop(context);
    native.close().unwrap();
}

#[test]
fn terminal_v1_without_routing_uses_actual_native_owner_and_shared_winning_receipt() {
    let name = "migration-native-terminal-v1-custody";
    let (mut native, nodes) = native_migration_cluster(name, 2);
    let mut contexts = Vec::new();
    let mut runtimes = Vec::new();
    for node in &nodes {
        let context = native.component_context(node).unwrap();
        let runtime = canonical_migration_runtime(&context);
        contexts.push(context);
        runtimes.push(runtime);
    }
    let owner = &nodes[0];
    let mut normal = native.client(owner).unwrap();
    let mut store = DfsControlPlaneStore::new(normal.control_plane_client_mut().unwrap());
    let mut config = store
        .get_file_system_config(&sinomo_dfs::sifs_file_system_id())
        .unwrap()
        .unwrap();
    // A normal current filesystem configuration with no metadata routing.
    // The source fixture is terminal DATA and has no active namespace work.
    config.file_system_id = sinomo_core::Id::generate();
    config.display_name = name.into();
    config.subtree_policies.clear();
    config.catalog_generation = 0;
    config.metadata_storage = None;
    store.put_file_system_config(&config).unwrap();
    assert!(
        store
            .list_metadata_shards(&config.file_system_id)
            .unwrap()
            .is_empty()
    );
    let candidates = store.cluster_view().unwrap().content_nodes;
    assert!(!candidates.is_empty());
    let now = u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp_nanos()).unwrap();
    let mut job = sinomo_dfs::build_placement_reconciliation_job(
        "terminal-original-v1".into(),
        &config,
        "/".into(),
        sinomo_dfs::DfsPlacementScope::Subtree,
        BTreeSet::new(),
        candidates,
        None,
        now,
    )
    .unwrap();
    job.state = sinomo_dfs::DfsPlacementReconciliationState::Completed;
    drop(store);
    drop(normal);
    let mut source = serde_json::to_value(&job).unwrap();
    for field in ["adaptive", "processed_content_bytes"] {
        source.as_object_mut().unwrap().remove(field);
    }
    for field in ["retention_policies", "module_metadata", "metadata_format"] {
        source["accepted_config"]
            .as_object_mut()
            .unwrap()
            .remove(field);
    }
    let key = sinomo_dfs::placement_reconciliation_key(&config.file_system_id, &job.job_id);
    let queue = format!(
        "/dfs/filesystems/{}/placement-reconciliation-queue-v1",
        config.file_system_id
    );
    let write = runtimes[0]
        .authority
        .commit(KvTxnRequest::generated(KvTxn {
            compares: vec![
                KvCompare::Missing { key: key.clone() },
                KvCompare::Missing { key: queue.clone() },
            ],
            success: vec![
                KvOp::Put {
                    key: key.clone(),
                    value: sinomo_core::kv::encode_kv_value(&source).unwrap(),
                    lease_id: None,
                },
                KvOp::Put {
                    key: queue,
                    value: sinomo_core::kv::encode_kv_value(
                        &serde_json::json!({"schema_version":1,"job_ids":[]}),
                    )
                    .unwrap(),
                    lease_id: None,
                },
            ],
            failure: Vec::new(),
        }))
        .unwrap();
    NativeImportAuthority::require_committed(&write).unwrap();
    let mut all_goals = Vec::new();
    for runtime in &runtimes {
        let goals = runtime
            .authority
            .capture_original_placement_goals(4)
            .unwrap()
            .unwrap();
        let goal = goals
            .into_iter()
            .find(|goal| goal.job.entry.key == key)
            .unwrap();
        assert!(goal.job.terminal);
        assert!(goal.assignments.is_empty());
        all_goals.push(goal);
    }
    let chosen = runtimes[0]
        .authority
        .placement_coordinator(&all_goals[0])
        .unwrap()
        .unwrap();
    let author = nodes
        .iter()
        .position(|node| node == &chosen.node_id)
        .unwrap();
    assert_eq!(
        chosen.guards[0].key,
        sinomo_core::kv::cluster_kv_raft_config_key()
    );
    let mut cursor = PlacementRetirementCursor::default();
    assert!(
        runtimes[author]
            .authority
            .retire_and_queue_placement_goals(&[all_goals[author].clone()], 1, &mut cursor)
            .unwrap()
    );
    assert!(
        runtimes[author]
            .authority
            .read_exact(&[key.clone()])
            .unwrap()
            .0[0]
            .entry
            .is_none()
    );
    let author_envelope = runtimes[author]
        .authority
        .saved_placement_custody(&all_goals[author], PlacementCustodyPhase::Retirement)
        .unwrap()
        .unwrap();
    let before = block_native(
        &runtimes[author].authority.runtime,
        runtimes[author]
            .authority
            .cluster()
            .unwrap()
            .kv_request_outcome_authoritative_async(author_envelope.request.request_id.clone()),
    )
    .unwrap();
    sinomo_core::kv::validate_cluster_kv_request_outcome_read(&before).unwrap();
    current_authority(&before.authority).unwrap();
    let sinomo_core::kv::KvRequestOutcome::Committed(original_receipt) = before.outcome else {
        panic!("actual administrative owner must have its original winning receipt")
    };
    NativeImportAuthority::validate_original_command_preimage(
        &author_envelope.request,
        &chosen.node_id,
        &chosen.guards,
        &original_receipt.command_digest,
    )
    .unwrap();
    let peer = (author + 1) % runtimes.len();
    // Native startup has one voter and a learner. Administrative custody is
    // retained on the actual voter cohort; the learner fetches its winning
    // envelope and verifies the committed native outcome before saving it.
    assert!(
        runtimes[peer]
            .authority
            .fetch_placement_custody(&all_goals[peer], PlacementCustodyPhase::Retirement, &chosen,)
            .unwrap()
    );
    assert!(
        native
            .rpc_observations()
            .iter()
            .any(|rpc| rpc.source == nodes[peer]
                && rpc.target == chosen.node_id
                && rpc.route == crate::authority::staged_service::PLACEMENT_CUSTODY_ROUTE
                && rpc.origin == sinomo_dfs::test_support::DfsTestRpcOrigin::RegisteredHost
                && rpc.phase == sinomo_dfs::test_support::DfsTestRpcPhase::Completed),
        "winning custody was fetched through actual registered signed ingress"
    );
    let envelope = runtimes[peer]
        .authority
        .saved_placement_custody(&all_goals[peer], PlacementCustodyPhase::Retirement)
        .unwrap()
        .unwrap();
    // Missing old key alone cannot advance: the SAME exact native winning
    // request must be authoritatively known and observed on this actual peer.
    assert!(
        runtimes[peer]
            .authority
            .replay_confirmed_placement_custody(&all_goals[peer], &envelope)
            .unwrap()
    );
    let mut peer_cursor = PlacementRetirementCursor::default();
    assert!(
        runtimes[peer]
            .authority
            .retire_and_queue_placement_goals(&[all_goals[peer].clone()], 1, &mut peer_cursor)
            .unwrap()
    );
    let new_key = all_goals[peer].job.current_job_key().unwrap();
    assert!(
        runtimes[peer].authority.read_exact(&[new_key]).unwrap().0[0]
            .entry
            .is_none(),
        "terminal cleanup never fabricates current DFS publication"
    );
    let after = block_native(
        &runtimes[peer].authority.runtime,
        runtimes[peer]
            .authority
            .cluster()
            .unwrap()
            .kv_request_outcome_authoritative_async(envelope.request.request_id.clone()),
    )
    .unwrap();
    sinomo_core::kv::validate_cluster_kv_request_outcome_read(&after).unwrap();
    current_authority(&after.authority).unwrap();
    let sinomo_core::kv::KvRequestOutcome::Committed(observed) = after.outcome else {
        panic!("peer custody must preserve the actual original receipt")
    };
    assert_eq!(
        observed, original_receipt,
        "pure observation preserves original body/term/index/revision/outcome"
    );
    let mut wrong_id = envelope.clone();
    wrong_id.request.request_id = KvRequestId::generate();
    assert!(
        !runtimes[peer]
            .authority
            .replay_confirmed_placement_custody(&all_goals[peer], &wrong_id)
            .unwrap()
    );
    let mut wrong_digest = envelope.clone();
    wrong_digest.request.txn.success.push(KvOp::Delete {
        key: "/unrelated-received-terminal-data".into(),
    });
    assert!(
        runtimes[peer]
            .authority
            .replay_confirmed_placement_custody(&all_goals[peer], &wrong_digest)
            .is_err()
    );
    drop(runtimes);
    drop(contexts);
    native.close().unwrap();
}

#[test]
fn owned_saved_attempts_repeat_custody_after_save_before_fanout_for_both_phases() {
    use sinomo_dfs::test_support::{DfsTestFaultAction, DfsTestFaultRule};
    let name = "migration-owned-attempt-save-before-fanout";
    let (mut native, nodes) = native_migration_cluster(name, 2);
    let fs = sinomo_dfs::sifs_file_system_id();
    let root_assignment = native.root_shard().unwrap();
    let local = root_assignment.primary_node_id.clone();
    let peer = root_assignment.replica_node_ids.first().unwrap().clone();
    let context = native.component_context(&local).unwrap();
    let runtime = canonical_migration_runtime(&context);
    let mut service = native.client(&local).unwrap();
    let mut store = DfsControlPlaneStore::new(service.control_plane_client_mut().unwrap());
    let config = store.get_file_system_config(&fs).unwrap().unwrap();
    let candidates = store.cluster_view().unwrap().content_nodes;
    let now = u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp_nanos()).unwrap();
    let current = sinomo_dfs::build_placement_reconciliation_job(
        "source-v1-save-before-fanout".into(),
        &config,
        "/users".into(),
        sinomo_dfs::DfsPlacementScope::Subtree,
        BTreeSet::new(),
        candidates.clone(),
        None,
        now,
    )
    .unwrap();
    drop(store);
    drop(service);
    let mut original = serde_json::to_value(&current).unwrap();
    for field in ["retention_policies", "module_metadata", "metadata_format"] {
        original["accepted_config"]
            .as_object_mut()
            .unwrap()
            .remove(field);
    }
    let key = sinomo_dfs::placement_reconciliation_key(&fs, &current.job_id);
    let input = runtime
        .authority
        .commit(KvTxnRequest::generated(KvTxn {
            compares: vec![KvCompare::Missing { key: key.clone() }],
            success: vec![KvOp::Put {
                key: key.clone(),
                value: sinomo_core::kv::encode_kv_value(&original).unwrap(),
                lease_id: None,
            }],
            failure: Vec::new(),
        }))
        .unwrap();
    NativeImportAuthority::require_committed(&input).unwrap();
    let goal = runtime
        .authority
        .capture_original_placement_goals(4)
        .unwrap()
        .unwrap()
        .into_iter()
        .find(|goal| goal.job.entry.key == key)
        .unwrap();
    let coordinator = runtime
        .authority
        .placement_coordinator(&goal)
        .unwrap()
        .unwrap();
    assert_eq!(coordinator.node_id, local);
    assert!(coordinator.peers.contains(&peer));
    let restored = goal
        .job
        .build_current_job(&config, candidates, None, now + 1)
        .unwrap();
    for phase in [
        PlacementCustodyPhase::Retirement,
        PlacementCustodyPhase::Publication,
    ] {
        let mut compares = OriginalPlacementJob::exact_retirement_compares(&coordinator.guards);
        let success = match phase {
            PlacementCustodyPhase::Retirement => {
                compares.extend(OriginalPlacementJob::exact_retirement_compares(&[
                    MetadataCapturedKvEntry {
                        key: key.clone(),
                        entry: Some(goal.job.entry.clone()),
                    },
                ]));
                vec![KvOp::Delete { key: key.clone() }]
            }
            PlacementCustodyPhase::Publication => {
                vec![KvOp::put_serde(goal.job.current_job_key().unwrap(), &restored, None).unwrap()]
            }
        };
        let request = KvTxnRequest::generated(KvTxn {
            compares,
            success,
            failure: Vec::new(),
        });
        let attempt_name = match phase {
            PlacementCustodyPhase::Retirement => format!(
                "placement-deletion-{}.json",
                goal.job.source_digest().unwrap()
            ),
            PlacementCustodyPhase::Publication => format!(
                "placement-publication-{}.json",
                goal.job.source_digest().unwrap()
            ),
        };
        let envelope = PlacementCustodyEnvelope {
            goal: goal.clone(),
            phase,
            coordinator: coordinator.clone(),
            request: request.clone(),
            blueprints: Vec::new(),
        };
        NativeImportAuthority::save_blueprint(
            &runtime.authority.placement_archive(),
            &attempt_name,
            &envelope,
        )
        .unwrap();
        assert!(
            runtime
                .authority
                .placement_outcome(&request, &coordinator.node_id, &coordinator.guards)
                .unwrap()
                .is_none()
        );
        // The real normal peer carrier loses custody delivery after the owned
        // request file exists. No synthetic Stored/Applied reply is supplied.
        native.install_fault(DfsTestFaultRule {
            source: Some(local.clone()),
            target: Some(peer.clone()),
            route: Some(crate::authority::staged_service::PLACEMENT_CUSTODY_ROUTE.into()),
            call_id: None,
            origin: None,
            remaining_hits: 2,
            action: DfsTestFaultAction::BudgetExhausted,
        });
        for _ in 0..2 {
            let envelope: PlacementCustodyEnvelope = serde_json::from_slice(
                &std::fs::read(runtime.authority.placement_archive().join(&attempt_name)).unwrap(),
            )
            .unwrap();
            assert!(
                runtime
                    .authority
                    .commit_owned_placement_attempt(&envelope)
                    .is_err()
            );
            assert!(
                runtime
                    .authority
                    .placement_outcome(&request, &coordinator.node_id, &coordinator.guards)
                    .unwrap()
                    .is_none()
            );
            let (old, _) = runtime.authority.read_exact(&[key.clone()]).unwrap();
            assert_eq!(old[0].entry.as_ref(), Some(&goal.job.entry));
            let (new, _) = runtime
                .authority
                .read_exact(&[goal.job.current_job_key().unwrap()])
                .unwrap();
            assert!(
                new[0].entry.is_none(),
                "a saved UNKNOWN request did not become its first effect"
            );
            assert_eq!(
                std::fs::read(runtime.authority.placement_archive().join(&attempt_name)).unwrap(),
                serde_json::to_vec(&envelope).unwrap()
            );
        }
        native.clear_faults();
    }
    let calls = native.rpc_observations();
    assert!(calls.iter().any(|call| call.source == local
        && call.target == peer
        && call.route == crate::authority::staged_service::PLACEMENT_CUSTODY_ROUTE));
    drop(runtime);
    drop(context);
    native.close().unwrap();
}

type GoalHook = Arc<dyn Fn(&str, Option<&DfsNativeTxnCommit>) -> bool + Send + Sync>;
static GOAL_HOOKS: std::sync::OnceLock<Mutex<BTreeMap<PathBuf, GoalHook>>> = std::sync::OnceLock::new();
pub(in crate::authority) struct GoalHookGuard(PathBuf);
impl Drop for GoalHookGuard {
    fn drop(&mut self) {
        GOAL_HOOKS.get().unwrap().lock().unwrap().remove(&self.0);
    }
}
pub(in crate::authority) fn install_goal_hook(root: &Path, hook: GoalHook) -> GoalHookGuard {
    assert!(GOAL_HOOKS.get_or_init(Default::default).lock().unwrap().insert(root.to_owned(), hook).is_none());
    GoalHookGuard(root.to_owned())
}
pub(super) fn observe_goal_step(authority: &NativeImportAuthority, point: &str, commit: Option<&DfsNativeTxnCommit>) -> bool {
    let hook = GOAL_HOOKS.get().and_then(|hooks| hooks.lock().unwrap().get(&authority.data_root).cloned());
    hook.is_none_or(|hook| hook(point, commit))
}

pub(in crate::authority) fn seed_pending_v0_goal(owner: &MigrationOwner, job_id: &str) -> OriginalPlacementGoal {
    let fs = sinomo_dfs::sifs_file_system_id();
    let source = OriginalV0Job {
        job_id, file_system_id: fs.as_str(), path: "/users", scope: "subtree", state: "pending",
        total_object_count: 0, scanned_object_count: 0, matched_object_count: 0, updated_object_count: 0,
        scheduled_repair_count: 0, completed_repair_count: 0, pending_repair_count: 0,
        revision: 1, created_at_unix_nanos: 1, updated_at_unix_nanos: 1,
    };
    let key = format!("/dfs/filesystems/{fs}/placement-reconciliations/{job_id}");
    let value = sinomo_core::object::serde_ext::to_bytes(&source, 1, sinomo_core::object::LengthMode::U32).unwrap();
    let commit = owner.authority.commit(KvTxnRequest::generated(KvTxn {
        compares: vec![KvCompare::Missing { key: key.clone() }],
        success: vec![KvOp::Put { key: key.clone(), value, lease_id: None }], failure: Vec::new(),
    })).unwrap();
    NativeImportAuthority::require_committed(&commit).unwrap();
    owner.authority.capture_original_placement_goals(256).unwrap().unwrap()
        .into_iter().find(|goal| goal.job.entry.key == key).unwrap()
}

pub(in crate::authority) fn seed_pending_v1_goal(
    native: &sinomo_dfs::test_support::DfsTestRuntime,
    node: &sinomo_core::node::NodeId,
    owner: &MigrationOwner,
    job_id: &str,
) -> OriginalPlacementGoal {
    let fs = sinomo_dfs::sifs_file_system_id();
    let mut client = native.client(node).unwrap();
    let candidates = client.placement_content_candidates().unwrap();
    let mut store = DfsControlPlaneStore::new(client.control_plane_client_mut().unwrap());
    let config = store.get_file_system_config(&fs).unwrap().unwrap();
    let current = sinomo_dfs::build_placement_reconciliation_job(
        job_id.into(), &config, "/users".into(), sinomo_dfs::DfsPlacementScope::Subtree,
        BTreeSet::new(), candidates, None,
        u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp_nanos()).unwrap(),
    ).unwrap();
    drop(store); drop(client);
    // Released initial-v1 nested configuration shape, using genuine native
    // identities/candidates. This is source DATA, never an old writer/receipt.
    let mut original = serde_json::to_value(&current).unwrap();
    for field in ["retention_policies", "module_metadata", "metadata_format", "catalog_generation", "metadata_storage"] {
        original["accepted_config"].as_object_mut().unwrap().remove(field);
    }
    if let Some(rules) = original["accepted_config"].get_mut("subtree_policies") {
        for rule in rules.as_array_mut().unwrap() {
            rule.as_object_mut().unwrap().remove("min_commit_replicas");
            rule.as_object_mut().unwrap().remove("desired_replicas");
        }
    }
    let key = sinomo_dfs::placement_reconciliation_key(&fs, job_id);
    // Keep the producer's atomic job/queue/activity shape. Only the original
    // nested job representation is source DATA; no partial queue is invented.
    let plan = sinomo_dfs::DfsPlacementUpdatePlan::new(&config, &config, &current).unwrap();
    let (captured, _) = owner.authority.read_exact(plan.keys()).unwrap();
    let entries = captured.into_iter().map(|row| row.entry).collect::<Vec<_>>();
    let queued_keys = plan.queued_job_keys(entries.clone()).unwrap().unwrap();
    let queued = if queued_keys.is_empty() { Vec::new() }
        else { owner.authority.read_exact(&queued_keys).unwrap().0.into_iter().map(|row| row.entry).collect() };
    let mut transaction = plan.transaction(entries, queued).unwrap().unwrap();
    let mut replaced = 0;
    for operation in &mut transaction.success {
        if let KvOp::Put { key: target, value, .. } = operation {
            if target == &key { *value = sinomo_core::kv::encode_kv_value(&original).unwrap(); replaced += 1; }
        }
    }
    assert_eq!(replaced, 1);
    let commit = owner.authority.commit(KvTxnRequest::generated(transaction)).unwrap();
    NativeImportAuthority::require_committed(&commit).unwrap();
    owner.authority.capture_original_placement_goals(256).unwrap().unwrap()
        .into_iter().find(|goal| goal.job.entry.key == key).unwrap()
}
