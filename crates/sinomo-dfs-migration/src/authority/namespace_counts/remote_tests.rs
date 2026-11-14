use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[test]
fn native_remote_count_owners_require_complete_stage_before_activation() {
    use sinomo_dfs::{DfsStoragePreparation, test_support::DfsTestRuntime};
    let name = "native-remote-count-cohort";
    let nodes = DfsTestRuntime::ordered_node_ids(name, 2);
    let mut native = DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    native.define_node(nodes[1].clone()).unwrap();
    let hold = Arc::new(AtomicBool::new(false));
    let mounted = Arc::new(Mutex::new(BTreeMap::<
        sinomo_core::node::NodeId,
        Arc<MigrationRuntime>,
    >::new()));
    let factory_hold = Arc::clone(&hold);
    let factory_mounted = Arc::clone(&mounted);
    native
        .configure_component_factories(vec![
            Arc::new(
                sinomo_dfs::component_factory_with_storage_preparation(Arc::new(
                    move |context, lifecycle| {
                        let provider = storage_preparation_provider(context, lifecycle.clone())?;
                        let runtime = shared_runtime(context, lifecycle)?;
                        factory_mounted
                            .lock()
                            .unwrap()
                            .insert(runtime.authority.local_node().unwrap(), runtime);
                        let hold = Arc::clone(&factory_hold);
                        Ok(Arc::new(move |root, scope, budget| {
                            if hold.load(Ordering::SeqCst)
                                && matches!(scope, DfsStoragePreparationScope::Metadata(_))
                            {
                                return Ok(DfsStoragePreparation::Pending {
                                    processed_records: 0,
                                });
                            }
                            provider(root, scope, budget)
                        }))
                    },
                ))
                .unwrap(),
            ),
            Arc::new(staged_import_component_factory().unwrap()),
        ])
        .unwrap();
    native.start_nodes().unwrap();
    let fs = native.file_system().file_system_id;
    native
        .reassign_metadata_shard(&fs, "services", nodes[1].clone(), vec![nodes[0].clone()])
        .unwrap();
    native
        .reassign_metadata_shard(&fs, "users", nodes[1].clone(), vec![nodes[0].clone()])
        .unwrap();
    {
        let mut client = native.client(&nodes[0]).unwrap();
        client
            .posix_write_file(
                "/services/remote-count-file",
                b"remote count source",
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![nodes[0].clone()]),
            )
            .unwrap();
    }
    mounted.lock().unwrap().clear();
    for node in nodes.iter().rev() {
        native.stop_node(node).unwrap();
    }
    hold.store(true, Ordering::SeqCst);
    let paused = native.reopen_node_with_startup_action(
        nodes[0].clone(),
        Box::new(|_| Err("hold first actual owner until remote mount".into())),
    );
    assert!(
        paused
            .unwrap_err()
            .contains("hold first actual owner until remote mount")
    );
    let action_mounted = Arc::clone(&mounted);
    let action_hold = Arc::clone(&hold);
    let action_fs = fs.clone();
    let paused_remote = native.reopen_node_with_startup_action(
        nodes[1].clone(),
        Box::new(|_| Err("explicit maintenance before ordinary Ready".into())),
    );
    assert!(
        paused_remote
            .unwrap_err()
            .contains("explicit maintenance before ordinary Ready")
    );
    // This is an explicit multi-owner maintenance task, not one startup RPC.
    // Bound its whole catalog work independently; every native RPC retains its
    // existing timeout, and normal startup is checked separately below using
    // its original five-second budget after maintenance publishes release.
    let maintenance = (move || -> std::result::Result<(), String> {
        let runtimes = action_mounted.lock().unwrap().clone();
        assert_eq!(runtimes.len(), 2);
        let coordinator = runtimes.values().next().unwrap();
        let catalog = coordinator
            .authority
            .assignments(std::slice::from_ref(&action_fs))
            .map_err(|e| e.to_string())?;
        assert_eq!(
            catalog
                .iter()
                .map(|row| row.primary_node_id.clone())
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );
        let started = std::time::Instant::now();
        let deadline = started + std::time::Duration::from_secs(30);
        let original_freezes = catalog
            .iter()
            .map(|assignment| {
                let path = runtimes[&assignment.primary_node_id]
                    .metadata_root
                    .join(&assignment.shard_id)
                    .join("journal/membership-handoff-write-freeze.sobj");
                (
                    assignment.shard_id.clone(),
                    if path.exists() {
                        Some(std::fs::read(path).unwrap())
                    } else {
                        None
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert!(
            original_freezes.values().any(Option::is_some),
            "actual prior handoff must own a freeze"
        );
        let mut sources = Vec::new();
        for assignment in &catalog {
            let owner = NamespaceCountMaintenance {
                runtime: Arc::clone(&runtimes[&assignment.primary_node_id]),
            };
            loop {
                if std::time::Instant::now() >= deadline {
                    return Err("remote count source capture deadline".into());
                }
                if let Some(source) = owner
                    .capture(
                        &DfsMetadataShardId::new(&assignment.shard_id),
                        &catalog,
                        256,
                    )
                    .map_err(|e| e.to_string())?
                {
                    sources.push(source);
                    break;
                }
            }
        }
        eprintln!(
            "COUNT_PHASE captured elapsed_ms={}",
            started.elapsed().as_millis()
        );
        let at = time::OffsetDateTime::now_utc();
        let mut seal = None;
        for assignment in &catalog {
            let owner = NamespaceCountMaintenance {
                runtime: Arc::clone(&runtimes[&assignment.primary_node_id]),
            };
            let actual = owner
                .enroll(
                    &DfsMetadataShardId::new(&assignment.shard_id),
                    &sources,
                    &catalog,
                    at,
                    crate::namespace_counts::NamespaceCountMaintenanceLimits {
                        max_objects: 4096,
                        max_encoded_bytes: 16 * 1024 * 1024,
                        deadline,
                    },
                )
                .map_err(|e| e.to_string())?;
            if let Some(expected) = &seal {
                assert_eq!(expected, &actual);
            }
            let source_freeze = owner
                .runtime
                .metadata_root
                .join(&assignment.shard_id)
                .join("journal/membership-handoff-write-freeze.sobj");
            assert_eq!(
                if source_freeze.exists() {
                    Some(std::fs::read(source_freeze).unwrap())
                } else {
                    None
                },
                original_freezes[&assignment.shard_id],
                "capture/enrollment must not mutate original native writer authority"
            );
            let bound = actual
                .bind_cut(
                    &actual
                        .member(&DfsMetadataShardId::new(&assignment.shard_id))
                        .unwrap()
                        .original_cut,
                )
                .unwrap();
            assert!(
                !bound
                    .staged_root
                    .join("journal/membership-handoff-write-freeze.sobj")
                    .exists(),
                "source authority must not be copied into the initialized candidate"
            );
            seal = Some(actual);
        }
        eprintln!(
            "COUNT_PHASE enrolled elapsed_ms={}",
            started.elapsed().as_millis()
        );
        let seal = seal.unwrap();
        let first = &seal.members[0];
        let first_owner = NamespaceCountMaintenance {
            runtime: Arc::clone(&runtimes[&first.assignment.primary_node_id]),
        };
        // Advance only one actual owner. Its native stage may exist, but no
        // member can activate while the other source plan is unstaged.
        loop {
            if std::time::Instant::now() >= deadline {
                return Err("remote count first stage deadline".into());
            }
            match first_owner.advance(&first.original_cut.scope_shard, 256) {
                Ok(DfsStoragePreparation::Pending { .. })
                | Err(MetadataBackendError::StoragePreparationPending { .. }) => {}
                Ok(DfsStoragePreparation::Ready) => {
                    return Err("one count source became Ready".into());
                }
                Err(e) => return Err(e.to_string()),
            }
            let key = sinomo_dfs::metadata_shard_membership_handoff_key(
                &action_fs,
                &first.assignment.shard_id,
            );
            let (rows, _) = coordinator
                .authority
                .read_exact(&[key])
                .map_err(|e| e.to_string())?;
            if rows[0].entry.is_some() {
                break;
            }
        }
        let current = coordinator
            .authority
            .assignments(std::slice::from_ref(&action_fs))
            .map_err(|e| e.to_string())?;
        assert_eq!(
            current, catalog,
            "one staged source cannot activate a partial cohort"
        );
        assert!(
            !coordinator
                .authority
                .published_count_cohort(&seal)
                .map_err(|e| e.to_string())?
        );
        // Invoke the actual unwrapped provider while other cold owners
        // remain held: this negative is the real complete-set gate.
        let actual_provider = storage_preparation_provider(
            &first_owner.runtime.authority.component_context,
            first_owner.runtime.lifecycle.clone(),
        )
        .map_err(|e| e.to_string())?;
        assert!(
            matches!(
                actual_provider(
                    &first_owner
                        .runtime
                        .metadata_root
                        .join(&first.assignment.shard_id),
                    &DfsStoragePreparationScope::Metadata(first.original_cut.scope_shard.clone()),
                    256,
                ),
                Ok(DfsStoragePreparation::Pending { .. })
                    | Err(MetadataBackendError::StoragePreparationPending { .. })
            ),
            "actual provider must refuse Ready with only one source staged"
        );
        // The negative fixture hold has proved partial-stage refusal.
        // Release it before real activation events so ordinary startup
        // observes those events, rather than parking on an artificial
        // hold whose AtomicBool transition has no production wake.
        action_hold.store(false, Ordering::SeqCst);
        loop {
            if std::time::Instant::now() >= deadline {
                return Err("remote complete count activation deadline".into());
            }
            for member in &seal.members {
                let owner = NamespaceCountMaintenance {
                    runtime: Arc::clone(&runtimes[&member.assignment.primary_node_id]),
                };
                match owner.advance(&member.original_cut.scope_shard, 256) {
                    Ok(_) | Err(MetadataBackendError::StoragePreparationPending { .. }) => {}
                    Err(e) => return Err(e.to_string()),
                }
            }
            if coordinator
                .authority
                .published_count_cohort(&seal)
                .map_err(|e| e.to_string())?
            {
                break;
            }
        }
        eprintln!(
            "COUNT_PHASE released elapsed_ms={}",
            started.elapsed().as_millis()
        );
        for (node, runtime) in &runtimes {
            let provider = storage_preparation_provider(
                &runtime.authority.component_context,
                runtime.lifecycle.clone(),
            )
            .map_err(|e| e.to_string())?;
            for member in &seal.members {
                if member.assignment.primary_node_id != *node
                    && !member.assignment.replica_node_ids.contains(node)
                {
                    continue;
                }
                let mut observed_pending = false;
                loop {
                    if std::time::Instant::now() >= deadline {
                        return Err(format!(
                            "normal counted receiver readiness deadline: node {node} shard {}",
                            member.assignment.shard_id
                        ));
                    }
                    let before_provider = std::time::Instant::now();
                    match provider(
                        &runtime.metadata_root.join(&member.assignment.shard_id),
                        &DfsStoragePreparationScope::Metadata(
                            member.original_cut.scope_shard.clone(),
                        ),
                        256,
                    ) {
                        Ok(DfsStoragePreparation::Ready) => {
                            eprintln!(
                                "COUNT_RECEIVER_READY node={node} shard={} provider_ms={} elapsed_ms={}",
                                member.assignment.shard_id,
                                before_provider.elapsed().as_millis(),
                                started.elapsed().as_millis()
                            );
                            break;
                        }
                        Ok(DfsStoragePreparation::Pending { .. })
                        | Err(MetadataBackendError::StoragePreparationPending { .. }) => {
                            if !observed_pending {
                                observed_pending = true;
                                let snapshot = sinomo_dfs::read_metadata_candidate(
                                    &runtime.metadata_root.join(&member.assignment.shard_id),
                                )
                                .map_err(|e| e.to_string())?;
                                let obligations = sinomo_dfs::namespace_count_cohort_obligations(
                                    &snapshot,
                                    &member.original_cut.scope_shard,
                                )
                                .map_err(|e| e.to_string())?;
                                let bound = seal
                                    .bind_cut(&member.original_cut)
                                    .map_err(|e| e.to_string())?;
                                eprintln!(
                                    "COUNT_RECEIVER_PENDING node={node} shard={} configuration_ready={} cohort_ready={} initialization_ready={} obligations={obligations:?} required_original_sources={:?}",
                                    member.assignment.shard_id,
                                    runtime.configuration_ready.load(Ordering::Acquire),
                                    runtime
                                        .authority
                                        .count_cohort_ready(&bound)
                                        .map_err(|e| e.to_string())?,
                                    runtime
                                        .authority
                                        .count_initializations_ready(
                                            &member.original_cut.scope_shard,
                                            &obligations
                                        )
                                        .map_err(|e| e.to_string())?,
                                    runtime.authority.required_original_metadata_sources().map(
                                        |rows| rows
                                            .into_iter()
                                            .map(|row| (
                                                row.retiring_shard.shard_id,
                                                row.retiring_shard.epoch
                                            ))
                                            .collect::<Vec<_>>()
                                    )
                                );
                            }
                            std::thread::yield_now()
                        }
                        Err(e) => {
                            return Err(format!(
                                "normal counted receiver node {node} shard {}: {e}",
                                member.assignment.shard_id
                            ));
                        }
                    }
                }
            }
        }
        for member in &seal.members {
            let owner = &runtimes[&member.assignment.primary_node_id];
            let shard = &member.original_cut.scope_shard;
            let key = sinomo_dfs::metadata_shard_membership_handoff_proof_key(&action_fs, &shard.0);
            let (rows, _) = owner
                .authority
                .read_exact(&[key])
                .map_err(|e| e.to_string())?;
            let proof: DfsMetadataShardMembershipHandoffProof = rows[0]
                .entry
                .as_ref()
                .unwrap()
                .decode()
                .map_err(|e| e.to_string())?;
            let backend = sinomo_dfs::LocalSobjMetadataBackend::open_for_inspection(
                shard.clone(),
                owner.metadata_root.join(&shard.0),
            )
            .map_err(|e| e.to_string())?;
            // The normal activated owner may already have consumed
            // its completed freeze. Any retained freeze must still be
            // this exact native operation, never copied source authority.
            if let Some(freeze) =
                sinomo_dfs::DfsMetadataBackend::metadata_shard_write_freeze(&backend, shard)
                    .map_err(|e| e.to_string())?
            {
                assert_eq!(freeze.handoff_id, proof.handoff_id);
                assert_eq!(freeze.retiring_epoch, member.assignment.epoch);
                assert_eq!(freeze.rebind_prefix_proof, member.initialized_prefix);
            }
        }
        action_hold.store(false, Ordering::SeqCst);
        Ok(())
    })();
    maintenance.unwrap();
    native.wait_startup_completion().unwrap();
    for node in &nodes {
        let mut client = native.client(node).unwrap();
        assert_eq!(
            client
                .posix_read_file_distributed("/services/remote-count-file")
                .unwrap(),
            b"remote count source"
        );
    }
    mounted.lock().unwrap().clear();
    native.close().unwrap();
}
