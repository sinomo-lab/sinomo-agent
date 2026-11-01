//! A real receiver-produced body is stopped before preparation, then offered
//! unchanged to the normal current publisher after an actual primary handoff.
//! The captured namespace cut is DATA; it never supplies writer permission.
use super::*;
use std::sync::atomic::AtomicUsize;

#[test]
fn native_captured_placement_payload_cannot_publish_after_real_primary_handoff() {
    let fixture = RoutingFixture::new("native-placement-stale-captured-authority");
    let captured = Arc::new(Mutex::new(None::<DfsPreparedOperationPayload>));
    let planning = Arc::new(Mutex::new(None::<serde_json::Value>));
    let publication_entry = Arc::new(Mutex::new(None::<serde_json::Value>));
    let allow_exact_replay = Arc::new(AtomicBool::new(false));
    let denied = Arc::new(AtomicUsize::new(0));
    let allowed = Arc::new(AtomicUsize::new(0));
    let expected_intent = fixture.intent_id();
    let expected_job = fixture.job.job_id.clone();
    let capture_body = Arc::clone(&captured);
    let capture_planning = Arc::clone(&planning);
    let capture_entry = Arc::clone(&publication_entry);
    let replay = Arc::clone(&allow_exact_replay);
    let rejected = Arc::clone(&denied);
    let passed = Arc::clone(&allowed);
    fixture.phases.set_extra_hook(Arc::new(move |point, _, _| {
        match point {
            DaemonDfsPreDispatchTestPoint::LocalPlacementAuthorityAcquired {
                job_id,
                path,
                namespace_shards,
            } if job_id == expected_job => {
                let mut first = capture_planning.lock().unwrap();
                if first.is_none() {
                    *first = Some(serde_json::json!({
                        "actual_job_id":job_id,"actual_path":path,
                        "actual_namespace_shards":namespace_shards,
                    }));
                }
            }
            DaemonDfsPreDispatchTestPoint::BeforeLocalPlacementIntentPublication { job_id, intent_id, owner_epoch }
                if job_id == expected_job => {
                *capture_entry.lock().unwrap() = Some(serde_json::json!({
                    "actual_job_id":job_id,"actual_intent_id":intent_id,"actual_owner_epoch":owner_epoch,
                }));
            }
            DaemonDfsPreDispatchTestPoint::BeforeMetadataEffectPrepare { payload }
                if payload.metadata_effects.iter().any(|effect| {
                    matches!(effect,
                    DfsPreparedMetadataEffect::PutIntent { intent }
                        if intent.intent_id == expected_intent)
                }) =>
            {
                let mut original = capture_body.lock().unwrap();
                if replay.load(Ordering::SeqCst) && original.as_ref() == Some(payload) {
                    // Release this negative fault only for the SAME actual
                    // input. The native provider still decides every authority.
                    passed.fetch_add(1, Ordering::SeqCst);
                    return Ok(());
                }
                if original.is_none() {
                    *original = Some(payload.clone());
                }
                rejected.fetch_add(1, Ordering::SeqCst);
                return Err(DfsError::Cancelled {
                    operation: "capture actual placement payload before native preparation",
                });
            }
            _ => {}
        }
        Ok(())
    }));
    let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let first_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let first_outcome = fixture.reconcile_once(first_deadline);
        let before_capture_assertion = fixture.inspect(&fixture.owner, None);
        let missing_before = before_capture_assertion.object.as_ref().map(|object| {
            fixture
                .desired
                .iter()
                .filter(|target| !object.current_locations.contains(target))
                .cloned()
                .collect::<Vec<_>>()
        });
        fixture.evidence("placement-stale-before-payload-assertion.json", serde_json::json!({
            "actual_caller_outcome":first_outcome,"actual_job":fixture.job,
            "actual_plan_policy":fixture.replication,"actual_desired_targets":fixture.desired,
            "actual_missing_targets":missing_before,"actual_current_cut":before_capture_assertion,
            "actual_outer_permit_boundary":*planning.lock().unwrap(),
            "actual_publication_entry_boundary":*publication_entry.lock().unwrap(),
            "actual_captured_payload":*captured.lock().unwrap(),
            "actual_provider":fixture.provider_diagnostics(&fixture.owner),
            "actual_denied_publications":denied.load(Ordering::SeqCst),
            "actual_rpc_window":format!("{:#?}",fixture.native.rpc_observations()),
            "omitted_rpc_observations":fixture.native.observation_dropped_count(),
            "qualification":"actual setup/caller facts exported before requiring any payload; absent capture leaves stage coverage and effect fate unestablished",
        }));
        fixture.native.checkpoint_rpc_observation_window().unwrap();
        let payload = captured
            .lock()
            .unwrap()
            .clone()
            .expect("real routed Main reconciliation reached its metadata factory");
        let actual_planning = planning
            .lock()
            .unwrap()
            .clone()
            .expect("real receiver already acquired the exact outer permit");
        let shards: Vec<DfsMetadataShardConfig> =
            serde_json::from_value(actual_planning["actual_namespace_shards"].clone()).unwrap();
        let original_digest = payload.canonical_digest().unwrap();
        let before_handoff = fixture.inspect(&fixture.owner, Some(&payload.operation_id));
        fixture.evidence("placement-stale-authority-captured.json", serde_json::json!({
            "qualification":"actual routed namespace context, actual outer permit and real metadata-effect factory; cancellation precedes all preparation of this metadata operation",
            "first_outcome":first_outcome,"actual_planning":actual_planning,
            "actual_payload":payload,"canonical_input_digest":original_digest,
            "before_handoff":before_handoff,"denied_publications":denied.load(Ordering::SeqCst),
        }));
        assert!(first_outcome.is_err());
        assert!(
            Instant::now() <= first_deadline,
            "original five-second action budget"
        );
        assert!(shards.contains(&fixture.shard));
        assert_eq!(actual_planning["actual_job_id"], fixture.job.job_id);
        assert_eq!(actual_planning["actual_path"], fixture.stat.path);
        assert_eq!(payload.file_system_id, fixture.shard.file_system_id);
        assert_eq!(
            payload.shard_id,
            DfsMetadataShardId::new(&fixture.shard.shard_id)
        );
        assert_eq!(payload.owner_epoch, fixture.shard.epoch);
        assert!(
            payload
                .expected_object_versions
                .iter()
                .any(|row| row.object_id == fixture.stat.object_id
                    && row.expected_version == Some(fixture.stat.version.clone()))
        );
        assert!(payload.expected_path_occupants.iter().any(|row| row.path
            == crate::namespace::metadata_path(&fixture.stat.path)
            && row.expected_object_id == Some(fixture.stat.object_id.clone())));
        assert!(before_handoff.prepared.is_none() && before_handoff.committed.is_none());
        assert!(before_handoff.intent.is_none());
        assert!(denied.load(Ordering::SeqCst) > 0);

        // The original invocation and its actual admission have UNWOUND.
        // Only the exact one-intent negative filter remains on all three roots.
        // Normal native handoff work must run; no fabricated shard row/freeze.
        fixture.phases.set_hold_automatic_placement(false);
        let target = fixture.shard.replica_node_ids.first().unwrap().clone();
        assert_ne!(target, fixture.owner);
        let old_ring = std::iter::once(fixture.owner.clone())
            .chain(fixture.shard.replica_node_ids.iter().cloned())
            .collect::<BTreeSet<_>>();
        let replicas = old_ring
            .iter()
            .filter(|node| **node != target)
            .cloned()
            .collect::<Vec<_>>();
        let handoff_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let active = fixture
            .native
            .reassign_metadata_shard(
                &fixture.shard.file_system_id,
                &fixture.shard.shard_id,
                target.clone(),
                replicas,
            )
            .unwrap();
        assert!(
            Instant::now() <= handoff_deadline,
            "normal role-only handoff keeps its five-second phase budget"
        );
        // Reuse the actual direct settings owner opened while healthy before
        // this fixture changed configuration; do not create a DFS startup
        // reader during the intentionally controlled transition.
        let mut publisher = fixture.take_settings_publisher();
        let proof = DfsControlPlaneStore::new(publisher.as_mut())
            .get_metadata_shard_membership_handoff_proof(&active.file_system_id, &active.shard_id)
            .unwrap()
            .expect("real native role transfer retains its completion proof");
        let pending = DfsControlPlaneStore::new(publisher.as_mut())
            .get_metadata_shard_membership_handoff(&active.file_system_id, &active.shard_id)
            .unwrap();
        assert!(pending.is_none());
        assert_eq!(active.primary_node_id, target);
        assert!(active.epoch > payload.owner_epoch);
        assert_eq!(proof.retiring_shard, fixture.shard);
        assert_eq!(proof.desired_shard, active);
        assert_eq!(
            std::iter::once(active.primary_node_id.clone())
                .chain(active.replica_node_ids.iter().cloned())
                .collect::<BTreeSet<_>>(),
            old_ring
        );
        drop(publisher);

        // Open the ordinary CURRENT owner before taking the comparison cuts.
        // No historical execution guard/permit is captured or installed here.
        let mut current = fixture
            .native
            .metadata_primary_service(&fixture.stat.path)
            .unwrap();
        assert_eq!(current.node_id(), &target);
        // Keep normal handoff/configuration completion enabled through the
        // current opener. The exact target-intent filter still prevents its
        // automatic publication; then hold only this object's automatic
        // reconciliation during the following comparison cut.
        fixture.phases.set_hold_automatic_placement(true);
        let before_current = fixture.inspect(&target, Some(&payload.operation_id));
        let before_old = fixture.inspect(&fixture.owner, Some(&payload.operation_id));
        assert!(before_current.prepared.is_none() && before_current.committed.is_none());
        assert!(before_current.intent.is_none());
        allow_exact_replay.store(true, Ordering::SeqCst);
        let action_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        current.set_transport_operation_deadline(Some(action_deadline));
        let outcome = current.publish_metadata_effect_payload(payload.clone());
        let elapsed_in_budget = Instant::now() <= action_deadline;
        let after_current = fixture.inspect(&target, Some(&payload.operation_id));
        let after_old = fixture.inspect(&fixture.owner, Some(&payload.operation_id));
        let same_captured_body = captured.lock().unwrap().as_ref() == Some(&payload)
            && payload.canonical_digest().unwrap() == original_digest;
        fixture.evidence("placement-stale-authority-state.json", serde_json::json!({
            "qualification":"same actual factory input/opid/body after old invocation unwound and real native role transfer completed; captured routing DATA never grants writer authority",
            "actual_job":fixture.job,"actual_planning":actual_planning,
            "actual_payload":payload,"canonical_input_digest":original_digest,
            "same_captured_body":same_captured_body,"active_assignment":active,
            "actual_handoff_proof":proof,"actual_outcome":format!("{outcome:?}"),
            "before_current":before_current,"after_current":after_current,
            "before_old":before_old,"after_old":after_old,
            "exact_input_fault_releases":allowed.load(Ordering::SeqCst),
            "denied_other_publications":denied.load(Ordering::SeqCst),
            "elapsed_in_original_action_budget":elapsed_in_budget,
            "rpc_window":format!("{:#?}",fixture.native.rpc_observations()),
            "omitted_rpc_observations":fixture.native.observation_dropped_count(),
        }));
        fixture.native.checkpoint_rpc_observation_window().unwrap();
        assert!(same_captured_body);
        assert_eq!(
            allowed.load(Ordering::SeqCst),
            1,
            "normal native guard was actually reached with the unchanged payload"
        );
        assert!(
            matches!(&outcome, Err(DfsError::StaleOwnerEpoch {
            file_system_id, local_node_id, primary_node_id, epoch,
        }) if file_system_id == &active.file_system_id && local_node_id == &target
            && primary_node_id == &target && *epoch == active.epoch),
            "actual current provider must reject the old captured authority: {outcome:?}"
        );
        assert!(elapsed_in_budget);
        assert_eq!(
            before_current.prefix, after_current.prefix,
            "no current-owner native prefix advance"
        );
        assert_eq!(
            before_current.tables, after_current.tables,
            "no new prepare/effect in the full native current-owner table image"
        );
        assert_eq!(
            before_old.prefix, after_old.prefix,
            "no old-owner native prefix advance"
        );
        assert_eq!(
            before_old.tables, after_old.tables,
            "no old-owner prepare/effect through stale input"
        );
        assert!(after_current.prepared.is_none() && after_current.committed.is_none());
        assert!(after_current.intent.is_none());
        assert_eq!(fixture.native.observation_dropped_count(), 0);
        drop(current);
    }));
    // Release negative hooks and drain the actual native runtime even when a
    // setup/authority assertion fails. Preserve that original failure.
    let cleaned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fixture.close()));
    match checked {
        Err(original) => {
            if cleaned.is_err() {
                eprintln!("normal native stale-authority cleanup also failed");
            }
            std::panic::resume_unwind(original);
        }
        Ok(()) => cleaned.unwrap(),
    }
}
