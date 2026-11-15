//! Real peer capture/enrollment while both ordinary providers remain Pending.
//! Hooks observe actual qualified native cuts; they never supply Ready/proofs.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[test]
fn native_two_owner_filesystem_stages_complete_catalog_before_activation() {
    two_owner_filesystem(false, false);
}
#[test]
fn native_two_owner_placement_evidence_requires_complete_activation() {
    two_owner_filesystem(true, false);
}
#[test]
fn native_two_owner_placement_retirement_observes_a_remote_issuer() {
    two_owner_filesystem(true, true);
}
fn two_owner_filesystem(placement: bool, remote_issuer: bool) {
    use sinomo_dfs::test_support::DfsTestRuntime;
    let name = if remote_issuer { "native-two-owner-remote-placement-issuer" }
        else if placement { "native-two-owner-placement" } else { "native-two-owner-filesystem" };
    let nodes = DfsTestRuntime::ordered_node_ids(name, 2);
    let mut native = DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    native.define_node(nodes[1].clone()).unwrap();
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
            .reassign_metadata_shard(&fs, shard, nodes[1].clone(), vec![nodes[0].clone()])
            .unwrap();
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
    let contexts = nodes
        .iter()
        .map(|node| native.component_context(node).unwrap())
        .collect::<Vec<_>>();
    let owners = contexts
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
    let waiting_predecessor = Arc::new(AtomicBool::new(false));
    let mut hooks = Vec::new();
    for owner in &owners {
        let observed = Arc::clone(&observed);
        let waiting_predecessor = Arc::clone(&waiting_predecessor);
        let catalog = catalog.clone();
        let fs = fs.clone();
        let held = Arc::clone(&held);
        hooks.push(filesystem::install_scope_hook(
            &owner.authority.data_root,
            Arc::new(move |owner, shard, point| {
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
    let unit = coordinator.enqueue_filesystem(&original).unwrap();
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
                tokio::select! { changed = first[0].changed() => changed, changed = second[0].changed() => changed }
            })
                .await
                .map_err(|_| {
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
    }
    held.store(false, Ordering::Release);
    for owner in &owners {
        owner.work.notify();
    }
    native.wait_startup_completion().unwrap();
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
    drop(hooks);
    drop(changes);
    drop(owners);
    drop(contexts);
    native.close().unwrap();
}
