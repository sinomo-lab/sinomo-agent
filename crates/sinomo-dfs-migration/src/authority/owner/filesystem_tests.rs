//! Native worker/control-plane continuation. Holding ordinary opens is a
//! negative fault; all source fences, handoffs and activation receipts are real.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) fn native_filesystem_owner_fixture(
    name: &str,
) -> (
    sinomo_dfs::test_support::DfsTestRuntime,
    sinomo_core::node::NodeId,
    Arc<AtomicBool>,
) {
    use sinomo_dfs::test_support::DfsTestRuntime;
    let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
    let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
    let hold = Arc::new(AtomicBool::new(false));
    let builder_hold = Arc::clone(&hold);
    native
        .configure_component_factories(vec![
            Arc::new(
                sinomo_dfs::component_factory_with_storage_preparation(Arc::new(
                    move |context, scope, lifecycle| {
                        let binding = storage_preparation_provider(context, scope, lifecycle)?;
                        let provider = binding.provider;
                        let held = Arc::clone(&builder_hold);
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
    {
        let mut client = native.client(&node).unwrap();
        client
            .posix_write_file(
                "/services/owner-retained-file",
                b"native retained source content",
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        client
            .posix_hardlink(
                "/services/owner-retained-file",
                "/services/owner-retained-alias",
            )
            .unwrap();
        client.posix_mkdir("/services/owner-child").unwrap();
        assert_eq!(
            client
                .posix_stat_distributed("/services/owner-retained-file")
                .unwrap()
                .link_count,
            2
        );
    }
    native.stop_node(&node).unwrap();
    hold.store(true, Ordering::Release);
    assert!(
        native.reopen_node(node.clone()).is_err(),
        "ordinary storage hold must not satisfy startup"
    );
    (native, node, hold)
}

#[test]
fn native_count_capability_publication_wakes_the_original_waiting_unit() {
    let (native, node, held) = native_filesystem_owner_fixture("native-count-capability-publication");
    let context = native.component_context(&node).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    let fs = native.file_system().file_system_id;
    let member_key = sinomo_core::node::cluster_member_key(&node);
    let policy_key = sinomo_core::node::cluster_capability_policy_key(&node);
    let (rows, _) = owner.authority.read_exact(&[member_key.clone(), policy_key.clone()]).unwrap();
    let member_entry = rows[0].entry.as_ref().unwrap();
    let policy_entry = rows[1].entry.as_ref().unwrap();
    let mut member: sinomo_core::ClusterMemberRecord = member_entry.decode().unwrap();
    let mut policy: sinomo_core::node::ClusterCapabilityPolicyRecord = policy_entry.decode().unwrap();
    let feature = sinomo_core::node::DFS_METADATA_STORAGE_GENERATION_V1_CAPABILITY;
    member.capabilities.retain(|capability| capability.as_str() != feature);
    policy.capabilities.retain(|capability| capability.as_str() != feature);
    let member_missing = sinomo_core::kv::encode_kv_value(&member).unwrap();
    let policy_missing = sinomo_core::kv::encode_kv_value(&policy).unwrap();
    let put = |key: &String, before: &[u8], value: &[u8]| {
        let committed = owner.authority.commit(KvTxnRequest::new(KvRequestId::generate(), KvTxn {
            compares: vec![KvCompare::ValueEquals { key: key.clone(), value: before.to_vec() }],
            success: vec![KvOp::Put { key: key.clone(), value: value.to_vec(),
                lease_id: if key == &member_key { member_entry.lease_id } else { policy_entry.lease_id } }],
            failure: Vec::new(),
        })).unwrap();
        NativeImportAuthority::require_committed(&committed).unwrap();
    };
    put(&member_key, &member_entry.value, &member_missing);
    put(&policy_key, &policy_entry.value, &policy_missing);
    let (rows, _) = owner.authority.read_exact(&[sinomo_dfs::file_system_config_key(&fs)]).unwrap();
    let original: sinomo_dfs::DfsFileSystemConfig = rows[0].entry.as_ref().unwrap().decode().unwrap();
    let catalog = owner.authority.assignments(std::slice::from_ref(&fs)).unwrap();
    let mut changes = owner.binding().readiness;
    let unit = owner.enqueue_filesystem(&original).unwrap();
    block_native(&native.runtime_handle(), async {
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            let state = owner.work.scheduler.lock().unwrap().state(&unit)?.cloned();
            match state {
                Some(inventory::ObservedState::Waiting { .. }) => break,
                Some(inventory::ObservedState::Blocked { evidence }) => return Err(error(format!("capability publication became a permanent block: {evidence}"))),
                _ => {}
            }
            // Discovered -> Waiting deliberately does not advertise readiness.
            // Observe the worker state without enqueueing or waking its work.
            if tokio::time::Instant::now() >= deadline { return Err(error("owner never parked for capability publication")); }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok::<_, MetadataBackendError>(())
    }).unwrap();
    assert!(owner.work.inventory.read(&unit).unwrap().unwrap().job.is_none());
    assert!(owner.work.scheduler.lock().unwrap().next_retry().is_none(), "an advertisement dependency needs its real publication edge, not polling");
    put(&member_key, &member_missing, &member_entry.value);
    assert!(owner.authority.require_count_capabilities(&catalog).is_err(), "binary support cannot grant the missing permission");
    assert!(owner.work.inventory.read(&unit).unwrap().unwrap().job.is_none());
    put(&policy_key, &policy_missing, &policy_entry.value);
    // Only actual native capability publications wake the existing unit.
    // No owner restart, extra enqueue, made-up readiness or new invocation.
    block_native(&native.runtime_handle(), async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match owner.work.scheduler.lock().unwrap().state(&unit)?.cloned() {
                Some(inventory::ObservedState::VerifiedCurrent) => break,
                Some(inventory::ObservedState::Blocked { evidence }) => return Err(error(evidence)),
                _ => {}
            }
            tokio::time::timeout_at(deadline, changes.changed()).await.map_err(error)?.map_err(error)?;
        }
        Ok::<_, MetadataBackendError>(())
    }).unwrap();
    held.store(false, Ordering::Release);
    owner.work.notify();
    native.wait_startup_completion().unwrap();
    let mut client = native.client(&node).unwrap();
    assert_eq!(client.posix_stat_distributed("/services/owner-retained-alias").unwrap().link_count, 2);
    drop(client);
    drop(owner);
    drop(context);
    native.close().unwrap();
}

pub(super) fn complete_filesystem_owner(
    native: &sinomo_dfs::test_support::DfsTestRuntime,
    node: &sinomo_core::node::NodeId,
    hold: &AtomicBool,
) {
    let fs = native.file_system().file_system_id;
    let context = native.component_context(node).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    let (entries, _) = owner
        .authority
        .read_exact(&[sinomo_dfs::file_system_config_key(&fs)])
        .unwrap();
    let original: sinomo_dfs::DfsFileSystemConfig =
        entries[0].entry.as_ref().unwrap().decode().unwrap();
    let source_generation = match &original.metadata_storage {
        None => original.catalog_generation,
        Some(sinomo_dfs::DfsMetadataStorageState::PendingTransition {
            source_catalog_generation,
            ..
        }) => *source_catalog_generation,
        _ => panic!("fixture completion requires its original new or pending transition"),
    };
    let unit = owner.enqueue_filesystem(&original).unwrap();
    let mut changed = owner.binding().readiness;
    // Explicit maintenance has its own bound. Normal startup below retains its
    // existing network deadline; no caller invokes conversion synchronously.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    block_native(&native.runtime_handle(), async {
        loop {
            let state = owner.work.scheduler.lock().unwrap().state(&unit)?.cloned();
            match state {
                Some(inventory::ObservedState::VerifiedCurrent) => {
                    return Ok::<_, MetadataBackendError>(());
                }
                Some(inventory::ObservedState::Blocked { evidence }) => {
                    return Err(error(evidence));
                }
                _ => {}
            }
            tokio::time::timeout_at(deadline, changed.changed())
                .await
                .map_err(|_| error("actual filesystem owner did not finish original maintenance"))?
                .map_err(error)?;
        }
    })
    .unwrap_or_else(|failure| {
        panic!(
            "{failure}; actual owner state: {:?}",
            owner.work.scheduler.lock().unwrap()
        )
    });
    let (entries, _) = owner
        .authority
        .read_exact(&[sinomo_dfs::file_system_config_key(&fs)])
        .unwrap();
    let active: sinomo_dfs::DfsFileSystemConfig =
        entries[0].entry.as_ref().unwrap().decode().unwrap();
    assert!(matches!(
        active.metadata_storage,
        Some(sinomo_dfs::DfsMetadataStorageState::Active {
            target_format: 12,
            ..
        })
    ));
    assert_eq!(active.catalog_generation, source_generation + 3);
    hold.store(false, Ordering::Release);
    // This removes only the test's negative hold. The actual owner's advisory
    // edge wakes another ordinary root/authority check; it cannot grant Ready.
    owner.work.notify();
    native.wait_startup_completion().unwrap();
    {
        let mut client = native.client(node).unwrap();
        assert_eq!(
            client
                .posix_read_file_distributed("/services/owner-retained-file")
                .unwrap(),
            b"native retained source content"
        );
        assert_eq!(
            client
                .posix_stat_distributed("/services/owner-retained-file")
                .unwrap()
                .link_count,
            2
        );
        assert_eq!(
            client
                .posix_stat_distributed("/services/owner-retained-alias")
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
        client
            .posix_write_file(
                "/services/owner-after-cutover",
                b"current generation write",
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        assert_eq!(
            client
                .posix_read_file_distributed("/services/owner-after-cutover")
                .unwrap(),
            b"current generation write"
        );
    }
    drop(changed);
    drop(owner);
    drop(context);
}

#[test]
fn native_filesystem_owner_converts_complete_quiesced_catalog() {
    let (native, node, hold) =
        native_filesystem_owner_fixture("single-migration-owner-native-filesystem");
    complete_filesystem_owner(&native, &node, &hold);
    native.close().unwrap();
}

#[test]
fn native_filesystem_owner_resumes_after_one_real_activation() {
    let (mut native, node, ordinary_hold) =
        native_filesystem_owner_fixture("single-migration-owner-forward-recovery");
    let context = native.component_context(&node).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    let root = owner.authority.data_root.clone();
    let fs = native.file_system().file_system_id;
    let (entries, _) = owner
        .authority
        .read_exact(&[sinomo_dfs::file_system_config_key(&fs)])
        .unwrap();
    let original: sinomo_dfs::DfsFileSystemConfig =
        entries[0].entry.as_ref().unwrap().decode().unwrap();
    let paused = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(Mutex::new(None::<sinomo_dfs::DfsMetadataShardConfig>));
    let fault_paused = Arc::clone(&paused);
    let fault_observed = Arc::clone(&observed);
    let first_activation = Arc::new(tokio::sync::Notify::new());
    let activation_observed = Arc::clone(&first_activation);
    let hook = filesystem::install_scope_hook(
        &root,
        Arc::new(move |owner, source, point| {
            if point == "before" && fault_paused.load(Ordering::Acquire) {
                return Err(MetadataBackendError::StoragePreparationPending {
                    root: owner.metadata_root.join(&source.shard_id),
                    processed_records: 0,
                });
            }
            if point == "after" && !fault_paused.load(Ordering::Acquire) {
                let (entries, _) = owner.authority.read_exact(&[
                    sinomo_dfs::metadata_shard_key(&source.file_system_id, &source.shard_id),
                    sinomo_dfs::metadata_shard_membership_handoff_proof_key(
                        &source.file_system_id,
                        &source.shard_id,
                    ),
                ])?;
                let current: sinomo_dfs::DfsMetadataShardConfig = entries[0]
                    .entry
                    .as_ref()
                    .ok_or_else(|| error("source assignment disappeared"))?
                    .decode()
                    .map_err(error)?;
                if current.epoch == source.epoch + 1 {
                    let proof: sinomo_dfs::DfsMetadataShardMembershipHandoffProof = entries[1]
                        .entry
                        .as_ref()
                        .ok_or_else(|| error("actual activation omitted its native proof"))?
                        .decode()
                        .map_err(error)?;
                    assert_eq!(proof.retiring_shard, *source);
                    assert_eq!(proof.desired_shard, current);
                    assert!(
                        matches!(proof.cause, sinomo_dfs::DfsMetadataShardHandoffCause::StorageImport { cut } if cut.storage_transition_id.is_some())
                    );
                    *fault_observed.lock().unwrap() = Some(current);
                    fault_paused.store(true, Ordering::Release);
                    activation_observed.notify_one();
                }
            }
            Ok(())
        }),
    );
    let unit = owner.enqueue_filesystem(&original).unwrap();
    let mut changed = owner.binding().readiness;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    block_native(&native.runtime_handle(), async {
        while observed.lock().unwrap().is_none() {
            if let Some(inventory::ObservedState::Blocked { evidence }) =
                owner.work.scheduler.lock().unwrap().state(&unit)?
            {
                return Err(error(evidence));
            }
            tokio::time::timeout_at(deadline, first_activation.notified())
                .await
                .map_err(|_| error("no actual first scope activation"))?;
        }
        Ok::<_, MetadataBackendError>(())
    })
    .unwrap();
    let first = observed.lock().unwrap().clone().unwrap();
    assert_eq!(first.shard_id, "root");
    let (entries, _) = owner
        .authority
        .read_exact(&[sinomo_dfs::file_system_config_key(&fs)])
        .unwrap();
    let partial: sinomo_dfs::DfsFileSystemConfig =
        entries[0].entry.as_ref().unwrap().decode().unwrap();
    assert_eq!(partial.catalog_generation, original.catalog_generation + 1);
    assert!(matches!(
        partial.metadata_storage,
        Some(sinomo_dfs::DfsMetadataStorageState::PendingTransition { .. })
    ));
    let reference = owner.work.inventory.read(&unit).unwrap().unwrap();
    let job = reference.job.clone().unwrap();
    let job_root = root.join(&job.checkpoint).parent().unwrap().to_owned();
    let invocation = std::fs::read(root.join(&job.checkpoint)).unwrap();
    let pending_request = std::fs::read(job_root.join("pending-request.json")).unwrap();
    let source_set = std::fs::read(job_root.join("captured.json")).unwrap();
    let cohort = std::fs::read(job_root.join("cohort.json")).unwrap();
    let physical = sinomo_dfs::DfsMetadataShardId::new(&first.shard_id);
    let activation_path = || {
        let live = root
            .join("dfs/metadata")
            .join(&first.shard_id)
            .join("native-activation-request.json");
        if live.is_file() {
            live
        } else {
            crate::metadata::namespace_count_plan_root(
                &root.join("dfs/metadata").join(&first.shard_id),
                &physical,
            )
            .unwrap()
            .join("native-activation-request.json")
        }
    };
    let activation_request = std::fs::read(activation_path()).unwrap();
    ordinary_hold.store(false, Ordering::Release);
    owner.work.notify();
    // Remove the synthetic ordinary-open hold: actual Pending/Staged authority
    // must still refuse an ordinary writer while only one scope is activated.
    match sinomo_dfs::runtime_host::open_dfs_runtime_service(
        native.data_dir(&node),
        Some(&native.runtime_handle()),
    ) {
        Ok(mut client) => assert!(
            client
                .posix_write_file(
                    "/must-not-publish-during-cutover",
                    b"not allowed",
                    sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()])
                )
                .is_err()
        ),
        Err(_) => {}
    }
    drop(changed);
    drop(owner);
    drop(context);
    native.stop_node(&node).unwrap();
    assert_eq!(
        std::fs::read(root.join(&job.checkpoint)).unwrap(),
        invocation
    );
    assert_eq!(
        std::fs::read(job_root.join("pending-request.json")).unwrap(),
        pending_request
    );
    assert_eq!(
        std::fs::read(job_root.join("captured.json")).unwrap(),
        source_set
    );
    assert_eq!(std::fs::read(job_root.join("cohort.json")).unwrap(), cohort);
    ordinary_hold.store(true, Ordering::Release);
    assert!(native.reopen_node(node.clone()).is_err());
    let context = native.component_context(&node).unwrap();
    let resumed = placement_work::native_tests::canonical_migration_runtime(&context);
    assert_eq!(
        resumed.work.inventory.read(&unit).unwrap().unwrap().job,
        Some(job.clone())
    );
    drop(hook);
    paused.store(false, Ordering::Release);
    resumed.work.notify();
    let mut changed = resumed.binding().readiness;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    block_native(&native.runtime_handle(), async {
        loop {
            match resumed
                .work
                .scheduler
                .lock()
                .unwrap()
                .state(&unit)?
                .cloned()
            {
                Some(inventory::ObservedState::VerifiedCurrent) => {
                    return Ok::<_, MetadataBackendError>(());
                }
                Some(inventory::ObservedState::Blocked { evidence }) => {
                    return Err(error(evidence));
                }
                _ => {}
            }
            tokio::time::timeout_at(deadline, changed.changed())
                .await
                .map_err(|_| error("original partial activation did not resume"))?
                .map_err(error)?;
        }
    })
    .unwrap_or_else(|failure| {
        panic!(
            "{failure}; actual resumed owner: {:?}",
            resumed.work.scheduler.lock().unwrap()
        )
    });
    assert_eq!(
        std::fs::read(root.join(&job.checkpoint)).unwrap(),
        invocation
    );
    assert_eq!(
        std::fs::read(job_root.join("pending-request.json")).unwrap(),
        pending_request
    );
    assert_eq!(std::fs::read(job_root.join("cohort.json")).unwrap(), cohort);
    assert_eq!(
        std::fs::read(activation_path()).unwrap(),
        activation_request
    );
    let (entries, _) = resumed
        .authority
        .read_exact(&[sinomo_dfs::file_system_config_key(&fs)])
        .unwrap();
    let active: sinomo_dfs::DfsFileSystemConfig =
        entries[0].entry.as_ref().unwrap().decode().unwrap();
    assert_eq!(active.catalog_generation, original.catalog_generation + 3);
    assert!(matches!(
        active.metadata_storage,
        Some(sinomo_dfs::DfsMetadataStorageState::Active { .. })
    ));
    ordinary_hold.store(false, Ordering::Release);
    resumed.work.notify();
    native.wait_startup_completion().unwrap();
    {
        let mut client = native.client(&node).unwrap();
        assert_eq!(
            client
                .posix_read_file_distributed("/services/owner-retained-file")
                .unwrap(),
            b"native retained source content"
        );
        assert_eq!(
            client
                .posix_stat_distributed("/services/owner-retained-file")
                .unwrap()
                .link_count,
            2
        );
        assert_eq!(
            client
                .posix_stat_distributed("/services/owner-retained-alias")
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
        assert!(
            client
                .posix_stat_distributed("/must-not-publish-during-cutover")
                .is_err()
        );
    }
    drop(changed);
    drop(resumed);
    drop(context);
    native.close().unwrap();
}

#[test]
fn native_owner_enrollment_restart_preserves_original_cut_and_source_authority() {
    let (mut native, node, held) =
        native_filesystem_owner_fixture("single-owner-enrollment-restart");
    let context = native.component_context(&node).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    let root = owner.authority.data_root.clone();
    let fs = native.file_system().file_system_id;
    let catalog = owner
        .authority
        .assignments(std::slice::from_ref(&fs))
        .unwrap();
    let original_freezes = catalog
        .iter()
        .map(|shard| {
            let path = owner
                .metadata_root
                .join(&shard.shard_id)
                .join("journal/membership-handoff-write-freeze.sobj");
            (shard.shard_id.clone(), optional_fixture_bytes(&path))
        })
        .collect::<BTreeMap<_, _>>();
    let captured = Arc::new(Mutex::new(None::<PathBuf>));
    let observed = Arc::clone(&captured);
    let enrolled = Arc::new(tokio::sync::Notify::new());
    let enrollment_observed = Arc::clone(&enrolled);
    let hook = filesystem::install_scope_hook(
        &root,
        Arc::new(move |owner, shard, point| {
            if point != "enrolled" {
                return Ok(());
            }
            let physical = sinomo_dfs::DfsMetadataShardId::new(&shard.shard_id);
            let stage = crate::metadata::namespace_count_plan_root(
                &owner.metadata_root.join(&shard.shard_id),
                &physical,
            )?;
            assert!(
                !stage
                    .join("journal/membership-handoff-write-freeze.sobj")
                    .exists(),
                "source authority copied into candidate"
            );
            let freeze = optional_fixture_bytes(
                &owner
                    .metadata_root
                    .join(&shard.shard_id)
                    .join("journal/membership-handoff-write-freeze.sobj"),
            );
            assert_eq!(freeze, original_freezes[&shard.shard_id]);
            *observed.lock().unwrap() = Some(
                stage
                    .parent()
                    .unwrap()
                    .join("namespace-count-source-cut.json"),
            );
            enrollment_observed.notify_one();
            Err(MetadataBackendError::StoragePreparationPending {
                root: stage,
                processed_records: 0,
            })
        }),
    );
    let (rows, _) = owner
        .authority
        .read_exact(&[sinomo_dfs::file_system_config_key(&fs)])
        .unwrap();
    let config: sinomo_dfs::DfsFileSystemConfig = rows[0].entry.as_ref().unwrap().decode().unwrap();
    let unit = owner.enqueue_filesystem(&config).unwrap();
    let mut changed = owner.binding().readiness;
    block_native(&native.runtime_handle(), async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        while captured.lock().unwrap().is_none() {
            if let Some(inventory::ObservedState::Blocked { evidence }) =
                owner.work.scheduler.lock().unwrap().state(&unit)?
            {
                return Err(error(evidence));
            }
            tokio::time::timeout_at(deadline, enrolled.notified())
                .await
                .map_err(|_| error("original enrollment did not reach actual checkpoint"))?;
        }
        Ok::<_, MetadataBackendError>(())
    })
    .unwrap();
    let cut_path = captured.lock().unwrap().clone().unwrap();
    let original_cut = std::fs::read(&cut_path).unwrap();
    let job = owner
        .work
        .inventory
        .read(&unit)
        .unwrap()
        .unwrap()
        .job
        .unwrap();
    let invocation = std::fs::read(root.join(&job.checkpoint)).unwrap();
    assert_eq!(
        owner
            .authority
            .assignments(std::slice::from_ref(&fs))
            .unwrap(),
        catalog
    );
    let cut: MetadataSourceCut = serde_json::from_slice(&original_cut).unwrap();
    assert!(!owner.authority.count_cohort_ready(&cut).unwrap());
    drop(changed);
    drop(owner);
    drop(context);
    native.stop_node(&node).unwrap();
    assert!(
        native.reopen_node(node.clone()).is_err(),
        "ordinary negative hold remains active"
    );
    drop(hook);
    complete_filesystem_owner(&native, &node, &held);
    assert_eq!(
        std::fs::read(&cut_path).unwrap(),
        original_cut,
        "no new cohort or initialization time after enrollment restart"
    );
    assert_eq!(
        std::fs::read(root.join(&job.checkpoint)).unwrap(),
        invocation
    );
    native.stop_node(&node).unwrap();
    let metadata_root = root.join("dfs/metadata/root");
    let marker = metadata_root.join("metadata-import-v7.json");
    let bytes = std::fs::read(&marker).unwrap();
    let mut malformed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    malformed["source_identity"] = serde_json::json!("changed-original-source");
    std::fs::write(&marker, serde_json::to_vec(&malformed).unwrap()).unwrap();
    assert!(
        crate::metadata::installed_metadata_source_cut(
            &metadata_root,
            &sinomo_dfs::DfsMetadataShardId::new("root")
        )
        .is_err()
    );
    std::fs::write(&marker, bytes).unwrap();
    native.reopen_node(node.clone()).unwrap();
    native.close().unwrap();
}

fn optional_fixture_bytes(path: &Path) -> Option<Vec<u8>> {
    match std::fs::read(path) {
        Ok(bytes) => Some(bytes),
        Err(failure) if failure.kind() == std::io::ErrorKind::NotFound => None,
        Err(failure) => panic!("{}: {failure}", path.display()),
    }
}

#[test]
fn native_pending_transition_loses_to_an_already_started_handoff() {
    let (native, nodes) = placement_work::native_tests::native_migration_cluster("pending-handoff-admission-race", 2);
    let node = nodes[0].clone();
    let context = native.component_context(&node).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    let fs = native.file_system().file_system_id;
    let key = sinomo_dfs::file_system_config_key(&fs);
    let original = owner.authority.read_exact(std::slice::from_ref(&key)).unwrap().0[0].entry.clone().unwrap();
    let config: sinomo_dfs::DfsFileSystemConfig = original.decode().unwrap();
    let shared = sinomo_dfs::SharedDfsControlPlaneKvClient::new(native.configuration_publisher(&node));
    let observed = Arc::new(Mutex::new(None));
    let retained = Arc::clone(&observed);
    let evaluated = Arc::new(tokio::sync::Notify::new());
    let attempted = Arc::clone(&evaluated);
    let hook = filesystem::install_scope_hook(&owner.authority.data_root, Arc::new(move |_, source, point| {
        if point == "before-pending" && retained.lock().unwrap().is_none() {
            let mut desired = source.clone();
            desired.epoch += 1;
            if desired.replica_node_ids.is_empty() { desired.replica_node_ids.push(nodes[1].clone()); }
            else { desired.replica_node_ids.pop(); }
            let handoff = shared.with_client(|kv| DfsControlPlaneStore::new(kv.as_mut())
                .begin_metadata_shard_membership_handoff(source, &desired)).map_err(error)?
                .ok_or_else(|| error("real membership handoff did not begin"))?;
            *retained.lock().unwrap() = Some(handoff);
        }
        if point == "pending-evaluated" { attempted.notify_one(); }
        Ok(())
    }));
    owner.enqueue_filesystem(&config).unwrap();
    block_native(&native.runtime_handle(), async {
        tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, evaluated.notified())
            .await.map_err(error)
    }).unwrap_or_else(|failure| panic!("{failure}; owner: {}", owner.diagnostic_scheduler()));
    let handoff = observed.lock().unwrap().clone().unwrap();
    let handoff_key = sinomo_dfs::metadata_shard_membership_handoff_key(&fs, &handoff.retiring_shard.shard_id);
    let (rows, proof) = owner.authority.read_exact(&[key, handoff_key]).unwrap();
    current_authority(&proof).unwrap();
    assert_eq!(rows[0].entry.as_ref().unwrap().value, original.value,
        "Pending must not reserve a filesystem with an already admitted membership handoff");
    assert_eq!(rows[1].entry.as_ref().unwrap().decode::<sinomo_dfs::DfsMetadataShardMembershipHandoff>().unwrap(), handoff);
    drop(hook); drop(owner); drop(context);
    native.close().unwrap();
}

#[test]
fn native_invocation_restart_recovers_before_inventory_linkage() {
    let (mut native, node, held) = native_filesystem_owner_fixture("native-invocation-linkage-restart");
    let context = native.component_context(&node).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    let root = owner.authority.data_root.clone();
    let fs = native.file_system().file_system_id;
    let config: sinomo_dfs::DfsFileSystemConfig = owner.authority.read_exact(&[
        sinomo_dfs::file_system_config_key(&fs)]).unwrap().0[0].entry.as_ref().unwrap().decode().unwrap();
    let retained = Arc::new(tokio::sync::Notify::new());
    let observed = Arc::clone(&retained);
    let once = AtomicBool::new(false);
    let hook = filesystem::install_scope_hook(&root, Arc::new(move |_, _, point| {
        if point == "invocation-retained" && !once.swap(true, Ordering::AcqRel) {
            observed.notify_one();
            return Err(error("interrupted after the real immutable invocation, before inventory linkage"));
        }
        Ok(())
    }));
    let unit = owner.enqueue_filesystem(&config).unwrap();
    block_native(&native.runtime_handle(), async {
        tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, retained.notified())
            .await.map_err(error)
    }).unwrap();
    let path = root.join("dfs/migration-owner/filesystems").join(unit.id().unwrap()).join("invocation.json");
    let original = std::fs::read(&path).unwrap();
    assert!(owner.work.inventory.read(&unit).unwrap().unwrap().job.is_none());
    drop(hook); drop(owner); drop(context);
    native.stop_node(&node).unwrap();
    assert!(native.reopen_node(node.clone()).is_err(), "ordinary-open hold remains in force");
    let context = native.component_context(&node).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    let mut changed = owner.binding().readiness;
    block_native(&native.runtime_handle(), async {
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            match owner.work.scheduler.lock().unwrap().state(&unit)?.cloned() {
                Some(inventory::ObservedState::VerifiedCurrent) => break,
                Some(inventory::ObservedState::Blocked { evidence }) => return Err(error(evidence)),
                _ => {},
            }
            tokio::time::timeout_at(deadline, changed.changed()).await.map_err(error)?.map_err(error)?;
        }
        Ok::<_, MetadataBackendError>(())
    }).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), original, "restart must not mint another invocation identity");
    assert!(owner.work.inventory.read(&unit).unwrap().unwrap().job.is_some());
    held.store(false, Ordering::Release);
    owner.work.notify();
    native.wait_startup_completion().unwrap();
    assert_eq!(native.client(&node).unwrap().posix_stat_distributed("/services/owner-retained-file").unwrap().link_count, 2);
    drop(changed); drop(owner); drop(context);
    native.close().unwrap();
}

#[test]
fn native_cohort_pending_retries_original_proposal_without_another_dependency_event() {
    let (native, node, ordinary_hold) = native_filesystem_owner_fixture("native-cohort-owned-pending-retry");
    let context = native.component_context(&node).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    let fs = native.file_system().file_system_id;
    let config: sinomo_dfs::DfsFileSystemConfig = owner.authority.read_exact(&[
        sinomo_dfs::file_system_config_key(&fs)]).unwrap().0[0].entry.as_ref().unwrap().decode().unwrap();
    let pending = Arc::new(AtomicBool::new(true));
    let injected = Arc::clone(&pending);
    let seen = Arc::new(tokio::sync::Notify::new());
    let observed = Arc::clone(&seen);
    let hook = filesystem::install_scope_hook(&owner.authority.data_root, Arc::new(move |owner, shard, point| {
        if point == "cohort-qualified" && shard.shard_id == "root" && injected.load(Ordering::Acquire) {
            observed.notify_one();
            return Err(MetadataBackendError::StoragePreparationPending {
                root: owner.metadata_root.join("root"), processed_records: 0,
            });
        }
        Ok(())
    }));
    let mut changed = owner.binding().readiness;
    let unit = owner.enqueue_filesystem(&config).unwrap();
    block_native(&native.runtime_handle(), async {
        tokio::time::timeout(std::time::Duration::from_secs(30), seen.notified()).await.map_err(error)?;
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            let scheduler = owner.work.scheduler.lock().unwrap();
            let waiting = matches!(scheduler.state(&unit)?, Some(inventory::ObservedState::Waiting { .. }));
            let retry = scheduler.next_retry();
            drop(scheduler);
            if waiting && retry.is_some() { break; }
            // A concurrently running turn can still expose its prior Waiting
            // state. Observe the actual parked retry under the existing bound.
            if tokio::time::Instant::now() >= deadline {
                return Err(error("native certification Pending lost its owned retry"));
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok::<_, MetadataBackendError>(())
    }).unwrap();
    let proposal = crate::metadata::finalization_checkpoint_for_test(&owner.metadata_root.join("root"),
        &sinomo_dfs::DfsMetadataShardId::new("root"), "import-proposal.json").unwrap().unwrap();
    let original = std::fs::read(&proposal).unwrap();
    pending.store(false, Ordering::Release);
    // No wake, KV write or replacement request: the same owner must retry.
    block_native(&native.runtime_handle(), async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match owner.work.scheduler.lock().unwrap().state(&unit)?.cloned() {
                Some(inventory::ObservedState::VerifiedCurrent) => break,
                Some(inventory::ObservedState::Blocked { evidence }) => return Err(error(evidence)),
                _ => {},
            }
            tokio::time::timeout_at(deadline, changed.changed()).await.map_err(error)?.map_err(error)?;
        }
        Ok::<_, MetadataBackendError>(())
    }).unwrap();
    assert_eq!(std::fs::read(&proposal).unwrap(), original);
    ordinary_hold.store(false, Ordering::Release);
    owner.work.notify();
    native.wait_startup_completion().unwrap();
    assert_eq!(native.client(&node).unwrap().posix_stat_distributed("/services/owner-retained-file").unwrap().link_count, 2);
    drop(hook); drop(changed); drop(owner); drop(context);
    native.close().unwrap();
}
