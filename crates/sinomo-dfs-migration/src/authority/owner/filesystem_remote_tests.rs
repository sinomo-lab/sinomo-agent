//! Real peer capture/enrollment while both ordinary providers remain Pending.
//! Hooks observe actual qualified native cuts; they never supply Ready/proofs.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[test]
fn native_two_owner_filesystem_stages_complete_catalog_before_activation() {
    two_owner_filesystem(false, false, false, false, false);
}
#[test]
fn native_two_owner_placement_evidence_requires_complete_activation() {
    two_owner_filesystem(true, false, false, false, false);
}
#[test]
fn native_two_owner_placement_retirement_observes_a_remote_issuer() {
    two_owner_filesystem(true, true, false, false, false);
}
#[test]
fn native_receiver_restart_completes_original_incomplete_source_binding() {
    two_owner_filesystem(false, false, true, false, false);
}
#[test]
fn native_staged_replica_catches_up_without_changing_original_activation_quorum() {
    two_owner_filesystem(false, false, false, true, false);
}
/// A takeover of the lagging receiver's shard after the import completed
/// leaves the receiver's staged copy activatable: the import proof binds the
/// shard's storage identity, not its serving configuration.
#[test]
fn native_staged_replica_activates_after_a_later_takeover() {
    two_owner_filesystem(false, false, false, true, true);
}
/// Switch `shard_id` to the write-set protocol and take it over by
/// `candidate` through the actual swap transaction, as a member whose primary
/// was lost does; `candidate_root` is the candidate's copy, whose head starts
/// the new epoch. Returns the shard's configuration after the swap.
fn take_over_for_test(
    native: &sinomo_dfs::test_support::DfsTestRuntime,
    fs: &sinomo_dfs::DfsFileSystemId,
    shard_id: &str,
    candidate: &sinomo_core::node::NodeId,
    nodes: &[sinomo_core::node::NodeId],
    candidate_root: &Path,
) -> DfsMetadataShardConfig {
    use sinomo_dfs::DfsMetadataBackend as _;
    for node in nodes {
        native.advertise_metadata_write_set_capability(node).unwrap();
    }
    let mut control = native.configuration_publisher(candidate);
    let mut store = sinomo_dfs::DfsControlPlaneStore::new(control.as_mut());
    let cut = store.read_metadata_shard_record_cut(fs, shard_id, nodes).unwrap().unwrap();
    let members = std::iter::once(cut.shard.primary_node_id.clone())
        .chain(cut.shard.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    let switch = sinomo_dfs::build_metadata_write_set_switch(&cut, &members).unwrap();
    assert_eq!(store.submit_metadata_write_set_change(&switch), sinomo_dfs::DfsMetadataWriteSetTxnOutcome::Applied);
    let cut = store.read_metadata_shard_record_cut(fs, shard_id, nodes).unwrap().unwrap();
    let shard = sinomo_dfs::DfsMetadataShardId::new(shard_id);
    let epoch_start = sinomo_dfs::LocalSobjMetadataBackend::open_for_inspection(shard.clone(), candidate_root)
        .unwrap().metadata_prefix_proof(&shard).unwrap().expect("the candidate's copy has a head");
    let swap = sinomo_dfs::build_metadata_takeover_swap(
        &cut, candidate, &epoch_start, &sinomo_core::kv::KvRequestId::generate(),
    ).unwrap();
    assert_eq!(store.submit_metadata_write_set_change(&swap), sinomo_dfs::DfsMetadataWriteSetTxnOutcome::Applied);
    store.read_metadata_shard_record_cut(fs, shard_id, nodes).unwrap().unwrap().shard
}

fn two_owner_filesystem(placement: bool, remote_issuer: bool, receiver_restart: bool, late_receiver: bool, later_takeover: bool) {
    use sinomo_dfs::test_support::DfsTestRuntime;
    let name = if later_takeover { "native-staged-receiver-later-takeover" }
        else if late_receiver { "native-staged-receiver-lost-prefix-reply" }
        else if receiver_restart { "native-receiver-incomplete-binding-restart" }
        else if remote_issuer { "native-two-owner-remote-placement-issuer" }
        else if placement { "native-two-owner-placement" } else { "native-two-owner-filesystem" };
    let nodes = DfsTestRuntime::ordered_node_ids(name, if late_receiver { 3 } else { 2 });
    let mut native = DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    for node in &nodes[1..] { native.define_node(node.clone()).unwrap(); }
    let held = Arc::new(AtomicBool::new(false));
    let factory_held = Arc::clone(&held);
    native
        .configure_component_factories(vec![
            Arc::new(
                sinomo_dfs::component_factory_with_storage_preparation(Arc::new(
                    move |context, scope, lifecycle| {
                        let binding = storage_preparation_provider(context, scope, lifecycle)?;
                        let provider = binding.provider;
                        let held = Arc::clone(&factory_held);
                        Ok(sinomo_dfs::DfsStoragePreparationProviderBinding {
                            readiness: binding.readiness,
                            provider: Arc::new(move |root, unit, budget| {
                                if held.load(Ordering::Acquire)
                                    && matches!(
                                        unit,
                                        DfsStoragePreparationScope::Metadata(_)
                                            | DfsStoragePreparationScope::MetadataGeneration { .. }
                                    )
                                {
                                    return Ok(sinomo_dfs::DfsStoragePreparation::Pending {
                                        processed_records: 0,
                                    });
                                }
                                provider(root, unit, budget)
                            }),
                        })
                    },
                ))
                .unwrap(),
            ),
            Arc::new(staged_import_component_factory().unwrap()),
        ])
        .unwrap();
    native.start_nodes().unwrap();
    let fs = native.file_system().file_system_id;
    for shard in ["services", "users"] {
        native
            .reassign_metadata_shard(&fs, shard, nodes[1].clone(), nodes.iter().filter(|node| **node != nodes[1]).cloned().collect())
            .unwrap();
    }
    if late_receiver {
        let root = native.root_shard().unwrap();
        if root.replica_node_ids.iter().collect::<BTreeSet<_>>() != nodes[1..].iter().collect::<BTreeSet<_>>() {
            native.reassign_metadata_shard(&fs, "root", nodes[0].clone(), nodes[1..].to_vec()).unwrap();
        }
    }
    if remote_issuer {
        native.reassign_metadata_shard(&fs, "root", nodes[1].clone(), vec![nodes[0].clone()]).unwrap();
        native.reassign_metadata_shard(&fs, "services", nodes[0].clone(), vec![nodes[1].clone()]).unwrap();
    }
    {
        let mut client = native.client(&nodes[0]).unwrap();
        client
            .posix_write_file(
                "/services/remote-owner-file",
                b"actual remote retained bytes",
                sinomo_dfs::DfsWriteOptions::replicated_eventual(nodes.clone()),
            )
            .unwrap();
        client
            .posix_hardlink(
                "/services/remote-owner-file",
                "/services/remote-owner-alias",
            )
            .unwrap();
        client.posix_mkdir("/services/remote-owner-child").unwrap();
        assert_eq!(
            client
                .posix_stat_distributed("/services/remote-owner-file")
                .unwrap()
                .link_count,
            2
        );
    }
    let catalog = native.metadata_shards().unwrap();
    assert_eq!(catalog.len(), 3);
    assert_eq!(
        catalog
            .iter()
            .map(|s| s.primary_node_id.clone())
            .collect::<BTreeSet<_>>()
            .len(),
        2
    );
    for node in nodes.iter().rev() {
        native.stop_node(node).unwrap();
    }
    held.store(true, Ordering::Release);
    for node in &nodes {
        let result = native.reopen_node_with_startup_action(
            node.clone(),
            Box::new(|_| Err("ordinary opens held while actual peer owners mount".into())),
        );
        assert!(
            result
                .unwrap_err()
                .contains("ordinary opens held while actual peer owners mount")
        );
    }
    let mut contexts = nodes
        .iter()
        .map(|node| native.component_context(node).unwrap())
        .collect::<Vec<_>>();
    let mut owners = contexts
        .iter()
        .map(placement_work::native_tests::canonical_migration_runtime)
        .collect::<Vec<_>>();
    let coordinator = owners
        .iter()
        .find(|owner| owner.authority.local_node().unwrap() == nodes[0])
        .unwrap();
    let (rows, _) = coordinator
        .authority
        .read_exact(&[sinomo_dfs::file_system_config_key(&fs)])
        .unwrap();
    let original: sinomo_dfs::DfsFileSystemConfig =
        rows[0].entry.as_ref().unwrap().decode().unwrap();
    let observed = Arc::new(Mutex::new(false));
    let observed_enrollment_status = Arc::new(AtomicBool::new(false));
    let waiting_predecessor = Arc::new(AtomicBool::new(false));
    let mut hooks = Vec::new();
    for owner in &owners {
        let observed = Arc::clone(&observed);
        let observed_enrollment_status = Arc::clone(&observed_enrollment_status);
        let waiting_predecessor = Arc::clone(&waiting_predecessor);
        let catalog = catalog.clone();
        let fs = fs.clone();
        let held = Arc::clone(&held);
        hooks.push(filesystem::install_scope_hook(
            &owner.authority.data_root,
            Arc::new(move |owner, shard, point| {
                if point == "enrollment-status" {
                    observed_enrollment_status.store(true, Ordering::Release);
                }
                if point == "qualified-enrollment-replayed" {
                    return Err(error("qualified peer must be observed without resending its full source enrollment"));
                }
                if point == "activation-waits-predecessor" {
                    assert_ne!(shard.shard_id, "root");
                    waiting_predecessor.store(true, Ordering::Release);
                    return Ok(());
                }
                if point != "cohort-qualified" {
                    return Ok(());
                }
                let mut seen = observed.lock().unwrap();
                if *seen {
                    if placement && shard.shard_id == "root" && !waiting_predecessor.load(Ordering::Acquire) {
                        return Err(MetadataBackendError::StoragePreparationPending { root: owner.metadata_root.clone(), processed_records: 0 });
                    }
                    return Ok(());
                }
                assert!(held.load(Ordering::Acquire));
                let mut keys = vec![sinomo_dfs::file_system_config_key(&fs)];
                for source in &catalog {
                    keys.push(sinomo_dfs::metadata_shard_key(&fs, &source.shard_id));
                    keys.push(sinomo_dfs::metadata_shard_membership_handoff_key(
                        &fs,
                        &source.shard_id,
                    ));
                }
                let (rows, _) = owner.authority.read_exact(&keys)?;
                let config: sinomo_dfs::DfsFileSystemConfig =
                    rows[0].entry.as_ref().unwrap().decode().map_err(error)?;
                assert!(matches!(
                    config.metadata_storage,
                    Some(sinomo_dfs::DfsMetadataStorageState::PendingTransition { .. })
                ));
                for (i, source) in catalog.iter().enumerate() {
                    let current: DfsMetadataShardConfig = rows[1 + i * 2]
                        .entry
                        .as_ref()
                        .unwrap()
                        .decode()
                        .map_err(error)?;
                    assert_eq!(
                        &current, source,
                        "first activation has not occurred at the qualified cut"
                    );
                    let handoff: sinomo_dfs::DfsMetadataShardMembershipHandoff = rows[2 + i * 2]
                        .entry
                        .as_ref()
                        .expect("all native stages exist")
                        .decode()
                        .map_err(error)?;
                    assert_eq!(&handoff.retiring_shard, source);
                    assert_eq!(handoff.desired_shard.epoch, source.epoch + 1);
                    let fence = handoff
                        .retiring_primary_fence
                        .as_ref()
                        .expect("every original primary is actually fenced");
                    assert_eq!(fence.primary_node_id, source.primary_node_id);
                    assert_eq!(fence.handoff_id, handoff.handoff_id);
                    assert!(matches!(
                        handoff.cause,
                        sinomo_dfs::DfsMetadataShardHandoffCause::StorageImport { .. }
                    ));
                }
                let scope = sinomo_dfs::DfsMetadataShardId::new(&shard.shard_id);
                let plan_root = crate::metadata::namespace_count_plan_root(&owner.metadata_root.join(&shard.shard_id), &scope)?;
                let plan = owner.authority.retained_plan_file.read(&plan_root.join("native-import-plan.json"))?;
                let capture = sinomo_dfs::test_support::capture_metadata_operation_boundaries(owner.authority.data_root.clone());
                let status = owner.authority.receive_count_probe(owner.authority.local_node()?, crate::authority::namespace_counts::Probe {
                    seal: plan.source_cut.namespace_count_cohort.as_deref().unwrap().clone(),
                    shard: scope, activated_only: false,
                })?;
                capture.checkpoint("cohort-status");
                let observation: serde_json::Value = serde_json::from_slice(&std::fs::read(
                    owner.authority.data_root.join("boundary-diagnostic-cohort-status.json")).map_err(error)?).map_err(error)?;
                assert_eq!(observation["dropped"], 0);
                let thread = format!("{:?}", std::thread::current());
                if observation["events"].as_array().unwrap().iter().any(|event|
                    event["thread"].as_str() == Some(&thread)
                        && event["kind"] == "migration-stage-participant-result") {
                    return Err(error("cohort status probe restaged native participants"));
                }
                assert!(matches!(status, crate::authority::namespace_counts::ProbeResult::Staged { .. }));
                let mut different = plan.as_ref().clone();
                different.facts.current_image_fingerprint.push_str("-changed");
                assert!(owner.authority.observed_count_stage(&different, &plan_root)?.is_none(),
                    "another plan cannot reuse a qualified staging observation");
                drop(capture);
                *seen = true;
                if placement && !waiting_predecessor.load(Ordering::Acquire) {
                    assert_eq!(shard.shard_id, "root");
                    return Err(MetadataBackendError::StoragePreparationPending { root: owner.metadata_root.clone(), processed_records: 0 });
                }
                Ok(())
            }),
        ));
    }
    let placement_goal = placement.then(|| placement_work::native_tests::seed_pending_v0_goal(coordinator, "two-owner-original-goal"));
    let publication = Arc::new(tokio::sync::Notify::new());
    let published = Arc::clone(&publication);
    let _publication = sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
        &sinomo_core::network::ClusterPaths::new(coordinator.authority.component_context.data_dir()),
        move |_| published.notify_one(),
    );
    if late_receiver {
        // Both peers receive the actual sealed image and original source ledger.
        // Withhold only the last prefix confirmation to one real participant;
        // the other two may activate, and the original ACK set must stay fixed.
        native.install_fault(sinomo_dfs::test_support::DfsTestFaultRule {
            source: Some(nodes[1].clone()), target: Some(nodes[2].clone()),
            route: Some(crate::authority::staged_service::PREFIX_ROUTE.into()),
            call_id: None, origin: None, remaining_hits: 32,
            action: sinomo_dfs::test_support::DfsTestFaultAction::BudgetExhausted,
        });
    } else if !placement {
        native.install_fault(sinomo_dfs::test_support::DfsTestFaultRule {
            source: Some(nodes[0].clone()), target: Some(nodes[1].clone()),
            route: Some(crate::authority::staged_service::SOURCE_FLOORS_ROUTE.into()),
            call_id: None, origin: None, remaining_hits: 1,
            action: sinomo_dfs::test_support::DfsTestFaultAction::BudgetExhaustedForFirstCall,
        });
    }
    let receiver_root = owners[0].metadata_root.join("services");
    let hold_receiver = Arc::new(AtomicBool::new(receiver_restart));
    let receiver_interrupted = Arc::new(AtomicBool::new(false));
    let receiver_wake = Arc::new(tokio::sync::Notify::new());
    let _receiver_hook = receiver_restart.then(|| {
        let root = receiver_root.clone();
        let hold = Arc::clone(&hold_receiver);
        let interrupted = Arc::clone(&receiver_interrupted);
        let wake = Arc::clone(&receiver_wake);
        crate::metadata::native_import_install_hook_for_test(&receiver_root, Arc::new(move |point, _, _, _, _| {
            if point != crate::metadata::NativeImportInstallTestPoint::TargetPublished
                || !hold.load(Ordering::Acquire) { return Ok(()); }
            if !interrupted.load(Ordering::Acquire) {
                let path = root.join("metadata-import-v7.json");
                let mut marker: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).map_err(error)?).map_err(error)?;
                assert_eq!(marker["installation_complete"], false);
                assert!(marker["receiver_local_source"].is_object());
                // Model the predecessor's missing optional receiver binding at
                // the real published-image cut. No source, intent, request or
                // native proof is changed; restart must re-earn the same seal.
                marker.as_object_mut().unwrap().remove("receiver_local_source");
                std::fs::write(&path, serde_json::to_vec(&marker).map_err(error)?).map_err(error)?;
                interrupted.store(true, Ordering::Release);
                wake.notify_one();
            }
            Err(MetadataBackendError::StoragePreparationPending { root: root.clone(), processed_records: 0 })
        }))
    });
    let unit = coordinator.enqueue_filesystem(&original).unwrap();
    let retained_receiver_plan = if receiver_restart {
        block_native(&native.runtime_handle(), async {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            while !receiver_interrupted.load(Ordering::Acquire) {
                tokio::time::timeout_at(deadline, receiver_wake.notified()).await.map_err(error)?;
            }
            Ok::<_, MetadataBackendError>(())
        }).unwrap();
        let path = receiver_root.join("native-import-plan.json");
        let plan = std::fs::read(&path).unwrap();
        hooks.clear();
        owners.clear();
        contexts.clear();
        native.stop_node(&nodes[0]).unwrap();
        hold_receiver.store(false, Ordering::Release);
        let result = native.reopen_node_with_startup_action(nodes[0].clone(),
            Box::new(|_| Err("ordinary opens held while actual peer owners mount".into())));
        assert!(result.unwrap_err().contains("ordinary opens held while actual peer owners mount"));
        contexts = nodes.iter().map(|node| native.component_context(node).unwrap()).collect();
        owners = contexts.iter().map(placement_work::native_tests::canonical_migration_runtime).collect();
        Some((path, plan))
    } else { None };
    let coordinator = owners.iter().find(|owner| owner.authority.local_node().unwrap() == nodes[0]).unwrap();
    let mut changes = owners
        .iter()
        .map(|owner| owner.binding().readiness)
        .collect::<Vec<_>>();
    block_native(&native.runtime_handle(), async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let current = coordinator.work.scheduler.lock().unwrap().state(&unit)?.cloned();
            match current {
                Some(inventory::ObservedState::VerifiedCurrent) => {
                    let reference = coordinator.work.inventory.read(&unit)?.ok_or_else(|| error("completed filesystem job vanished"))?;
                    let job = reference.job.ok_or_else(|| error("completed filesystem omitted its original job"))?;
                    let path = coordinator.authority.data_root.join(job.checkpoint);
                    let cohort: crate::namespace_count_cohort::NamespaceCountCohortSeal = serde_json::from_slice(
                        &std::fs::read(path.parent().unwrap().join("cohort.json")).map_err(error)?).map_err(error)?;
                    cohort.validate()?;
                    let mut replicas_ready = true;
                    for (owner, node) in owners.iter().zip(&nodes) {
                        for assignment in &catalog {
                            if assignment.primary_node_id != *node && !assignment.replica_node_ids.contains(node) { continue; }
                            let identity = &cohort.member(&sinomo_dfs::DfsMetadataShardId::new(&assignment.shard_id))?.initialized_generation;
                            let root = owner.metadata_root.join(&assignment.shard_id);
                            let replica_unit = inventory::UnitKey::from_scope(&owner.authority.data_root, &root,
                                &DfsStoragePreparationScope::MetadataGeneration { identity: identity.clone() })?;
                            if let Some(inventory::ObservedState::Blocked { evidence }) = owner.work.scheduler.lock().unwrap().state(&replica_unit)? {
                                return Err(error(format!("replica generation {node}/{} blocked: {evidence}", assignment.shard_id)));
                            }
                            replicas_ready &= sinomo_dfs::inspect_metadata_storage_generation(&root).is_ok_and(|header|
                                &header.identity == identity && matches!(header.state, sinomo_dfs::DfsMetadataGenerationState::Active { .. }));
                        }
                    }
                    if replicas_ready { break; }
                },
                Some(inventory::ObservedState::Blocked { evidence }) => {
                    return Err(error(evidence));
                }
                _ => {}
            }
            let (first, second) = changes.split_at_mut(1);
            tokio::time::timeout_at(deadline, async {
                if second.len() == 2 {
                    let (middle, last) = second.split_at_mut(1);
                    tokio::select! { changed = first[0].changed() => changed, changed = middle[0].changed() => changed, changed = last[0].changed() => changed }
                } else {
                    tokio::select! { changed = first[0].changed() => changed, changed = second[0].changed() => changed }
                }
            })
                .await
                .map_err(|_| {
                    for owner in &owners {
                        eprintln!("NATIVE_RECEIVER_TIMEOUT node={:?} scheduler={}", owner.authority.local_node(), owner.diagnostic_scheduler());
                        for shard in ["root", "users", "services"] {
                            let root = owner.metadata_root.join(shard);
                            eprintln!("NATIVE_RECEIVER_HEADER path={} header={:?}", root.display(), sinomo_dfs::inspect_metadata_storage_generation(&root));
                        }
                    }
                    error("two actual owners did not finish original filesystem migration")
                })?
                .map_err(error)?;
        }
        Ok::<_, MetadataBackendError>(())
    })
    .unwrap();
    assert!(
        *observed.lock().unwrap(),
        "actual complete-set qualification observed before first activation"
    );
    assert!(observed_enrollment_status.load(Ordering::Acquire),
        "coordinator must observe the peer's original qualified cohort through status");
    let mut keys = vec![sinomo_dfs::file_system_config_key(&fs)];
    for source in &catalog {
        keys.push(sinomo_dfs::metadata_shard_key(&fs, &source.shard_id));
        keys.push(sinomo_dfs::metadata_shard_membership_handoff_proof_key(
            &fs,
            &source.shard_id,
        ));
    }
    let (rows, _) = coordinator.authority.read_exact(&keys).unwrap();
    let active: sinomo_dfs::DfsFileSystemConfig = rows[0].entry.as_ref().unwrap().decode().unwrap();
    assert_eq!(active.catalog_generation, original.catalog_generation + 3);
    assert!(matches!(
        active.metadata_storage,
        Some(sinomo_dfs::DfsMetadataStorageState::Active {
            target_format: 12,
            ..
        })
    ));
    for (i, source) in catalog.iter().enumerate() {
        let current: DfsMetadataShardConfig =
            rows[1 + i * 2].entry.as_ref().unwrap().decode().unwrap();
        let proof: sinomo_dfs::DfsMetadataShardMembershipHandoffProof =
            rows[2 + i * 2].entry.as_ref().unwrap().decode().unwrap();
        assert_eq!(proof.retiring_shard, *source);
        assert_eq!(proof.desired_shard, current);
        assert_eq!(current.epoch, source.epoch + 1);
        assert!(matches!(
            proof.cause,
            sinomo_dfs::DfsMetadataShardHandoffCause::StorageImport { .. }
        ));
        if late_receiver && source.primary_node_id == nodes[1] {
            assert_eq!(proof.acknowledged_node_ids.len(), 2);
            assert!(!proof.acknowledged_node_ids.contains(&nodes[2]));
            let name = Path::new(&source.shard_id).join("native-activation-request.json");
            assert_eq!(std::fs::read(owners[2].metadata_root.join(&name)).unwrap(),
                std::fs::read(owners[1].metadata_root.join(&name)).unwrap(),
                "late receiver must retain the exact original native request");
            let root = owners[2].metadata_root.join(&source.shard_id);
            let plan = owners[2].authority.retained_plan_file.read(&root.join("native-import-plan.json")).unwrap();
            let lookup = crate::authority::activation_data::Request::for_plan(&plan, &proof).unwrap();
            let foreign = sinomo_core::node::NodeId::from_legacy_named("not-an-activation-participant").unwrap();
            assert!(owners[1].authority.activated_request_data(foreign, lookup.clone()).is_err());
            let original_request = std::fs::read(owners[1].metadata_root.join(&name)).unwrap();
            for (field, value) in [("plan_digest", "different-plan"), ("shard_id", "../services")] {
                let mut changed = serde_json::to_value(&lookup).unwrap();
                changed[field] = serde_json::json!(value);
                let denied = (|| -> Result<()> {
                    let call = sinomo_core::network::ObjectRpcCall::encode_borrowed_value(
                        crate::authority::activation_data::ROUTE, "dfs-migration", &changed).map_err(error)?;
                    let transport = owners[2].authority.transport.as_ref().unwrap();
                    let response = owners[2].authority.read_peer(transport.call_async(
                        &nodes[1], call, source_effect_rpc_options(), true))?;
                    let response = owners[2].authority.read_peer(response.into_materialized_value_response(64 * 1024 * 1024))?;
                    let _: Option<KvTxnRequest> = response.decode_body().map_err(error)?;
                    Ok(())
                })();
                assert!(denied.is_err(), "actual native lookup accepted changed {field}");
            }
            assert_eq!(std::fs::read(owners[1].metadata_root.join(&name)).unwrap(), original_request);
            let header = sinomo_dfs::inspect_metadata_storage_generation(&root).unwrap();
            let original_header = std::fs::read(root.join("metadata-generation.sobj")).unwrap();
            let cut = crate::metadata::installed_metadata_source_cut(&root, &header.identity.shard_id).unwrap().unwrap();
            let mut changed_seal = cut.namespace_count_cohort.as_ref().unwrap()
                .member(&header.identity.shard_id).unwrap().initialized_seal.clone();
            changed_seal.file_fingerprint.push('0');
            let rejected = block_native(&native.runtime_handle(), sinomo_dfs::activate_metadata_storage_generation(
                &owners[2].authority.component_context, &root, &header.identity, &changed_seal));
            assert!(rejected.unwrap_err().to_string().contains("installed candidate differs from native activation seal"));
            assert_eq!(std::fs::read(root.join("metadata-generation.sobj")).unwrap(), original_header);
        }
    }
    // An original primary may qualify only its own byte-exact retained cut.
    // A receiver with no ACK cannot borrow that exception from the sender.
    for owner in &owners {
        let local = owner.authority.local_node().unwrap();
        for assignment in catalog.iter().filter(|shard| shard.primary_node_id != local && shard.replica_node_ids.contains(&local)) {
            crate::metadata::assert_receiver_marker_path_scope_for_test(
                &owner.metadata_root.join(&assignment.shard_id),
                &sinomo_dfs::DfsMetadataShardId::new(&assignment.shard_id),
            );
        }
        for assignment in catalog.iter().filter(|shard| shard.primary_node_id == local) {
            let installed = owner.metadata_root.join(&assignment.shard_id);
            let marker: serde_json::Value = serde_json::from_slice(
                &std::fs::read(installed.join("metadata-import-v7.json")).unwrap()).unwrap();
            let retained = PathBuf::from(marker["retained_source_root"].as_str().unwrap());
            let original = std::fs::read(retained.join("records/tables/snapshot.sobj")).unwrap();
            let plan = owner.authority.retained_plan_file.read(&installed.join("native-import-plan.json")).unwrap();
            assert!(owner.authority.owns_exact_retiring_source(&retained, &plan).unwrap());
            let peer = owners.iter().find(|peer| peer.authority.local_node().unwrap() != local).unwrap();
            assert!(!peer.authority.owns_exact_retiring_source(&retained, &plan).unwrap());
            let mut changed = plan.as_ref().clone();
            changed.facts.source_cut.source_identity = "not-the-original-source".into();
            assert!(owner.authority.owns_exact_retiring_source(&retained, &changed).is_err());
            assert_eq!(std::fs::read(retained.join("records/tables/snapshot.sobj")).unwrap(), original);
        }
    }
    if let Some((path, bytes)) = retained_receiver_plan {
        assert_eq!(std::fs::read(path).unwrap(), bytes, "receiver recovery preserves its exact native plan");
        let marker: serde_json::Value = serde_json::from_slice(&std::fs::read(
            receiver_root.join("metadata-import-v7.json")).unwrap()).unwrap();
        assert_eq!(marker["installation_complete"], true);
        assert!(marker["receiver_local_source"].is_object());
    }
    // While the receiver's copies are still staged, a takeover moves the
    // services shard to another member. It rewrites the shard's serving
    // configuration, not its storage identity, so the receiver's staged copy
    // still activates under the original import proof.
    if later_takeover {
        let services = catalog.iter().find(|shard| shard.shard_id == "services").unwrap();
        let taken_over = take_over_for_test(&native, &fs, &services.shard_id, &nodes[0], &nodes,
            &owners[0].metadata_root.join(&services.shard_id));
        assert_ne!(taken_over.primary_node_id, services.primary_node_id);
        assert!(taken_over.replica_node_ids.contains(&nodes[2]));
    }
    held.store(false, Ordering::Release);
    for owner in &owners {
        owner.work.notify();
    }
    native.wait_startup_completion().unwrap();
    if later_takeover {
        for shard in ["services", "users"] {
            let root = owners[2].metadata_root.join(shard);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            while !matches!(sinomo_dfs::inspect_metadata_storage_generation(&root).map(|header| header.state),
                Ok(sinomo_dfs::DfsMetadataGenerationState::Active { .. }))
            {
                assert!(std::time::Instant::now() < deadline,
                    "the lagging receiver activates its staged {shard} copy after the services takeover");
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    }
    if let Some(goal) = placement_goal {
        assert!(waiting_predecessor.load(Ordering::Acquire), "a real later owner must wait for its actual catalog predecessor before constructing activation");
        let key = goal.current_job_key().unwrap();
        block_native(&native.runtime_handle(), async {
            let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            loop {
                let notified = publication.notified();
                tokio::pin!(notified); notified.as_mut().enable();
                let (rows, _) = coordinator.authority.read_exact(&[goal.original_entry().key.clone(), key.clone()])?;
                if rows[1].entry.is_some() { assert!(rows[0].entry.is_none()); break; }
                for owner in &owners {
                    let unit = inventory::UnitKey { relative_root: "dfs/metadata".into(), role: inventory::UnitRole::PlacementGoals };
                    if let Some(inventory::ObservedState::Blocked { evidence }) = owner.work.scheduler.lock().unwrap().state(&unit)? {
                        return Err(error(evidence));
                    }
                }
                tokio::time::timeout_at(deadline, notified).await
                    .map_err(|_| error("two source owners did not publish the original current placement goal"))?;
            }
            Ok::<_, MetadataBackendError>(())
        }).unwrap();
        for owner in &owners {
            for shard in &catalog {
                let cut = crate::metadata::installed_metadata_source_cut(&owner.metadata_root.join(&shard.shard_id),
                    &sinomo_dfs::DfsMetadataShardId::new(&shard.shard_id)).unwrap().unwrap();
                assert!(cut.pending_operations.is_empty());
                assert_eq!(cut.retained_placement_reservations.len(), 1);
                cut.namespace_count_cohort.as_ref().unwrap().validate_bound_cut(&cut).unwrap();
            }
        }
    }

    for node in &nodes {
        let mut client = native.client(node).unwrap();
        assert_eq!(
            client
                .posix_read_file_distributed("/services/remote-owner-alias")
                .unwrap(),
            b"actual remote retained bytes"
        );
        assert_eq!(
            client
                .posix_stat_distributed("/services/remote-owner-file")
                .unwrap()
                .link_count,
            2
        );
        assert_eq!(
            client
                .posix_stat_distributed("/services")
                .unwrap()
                .link_count,
            3
        );
        let path = format!("/users/remote-after-{node}");
        client
            .posix_write_file(
                &path,
                b"new native generation bytes",
                sinomo_dfs::DfsWriteOptions::replicated_eventual(nodes.clone()),
            )
            .unwrap();
        assert_eq!(
            client.posix_read_file_distributed(&path).unwrap(),
            b"new native generation bytes"
        );
    }
    if late_receiver {
        assert!(native.rpc_observations().iter().any(|call|
            call.source == nodes[1] && call.target == nodes[2]
                && call.route == crate::authority::staged_service::PREFIX_ROUTE
                && call.outcome_error.is_some()));
        assert!(native.rpc_observations().iter().any(|call|
            call.source == nodes[2] && call.route == "/dfs-migration/activated-request-data-v7"
                && call.phase == sinomo_dfs::test_support::DfsTestRpcPhase::Completed
                && call.outcome_error.is_none()), "the receiver uses the real native request-DATA lookup");
        native.clear_faults();
    } else if !placement { native.assert_fault_budgets().unwrap(); }
    drop(hooks);
    drop(changes);
    drop(owners);
    drop(contexts);
    native.close().unwrap();
}
