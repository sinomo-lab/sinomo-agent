//! Actual cold native owners, complete source sessions, and ordinary admission.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[test]
fn native_count_cohort_partial_activation_restarts_before_ready() {
    use sinomo_dfs::{DfsStoragePreparation, test_support::DfsTestRuntime};
    let name = "native-count-cohort-partial-activation";
    let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
    let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
    let hold = Arc::new(AtomicBool::new(false));
    let mounted = Arc::new(Mutex::new(None::<Arc<MigrationRuntime>>));
    let builder_hold = Arc::clone(&hold);
    let builder_mounted = Arc::clone(&mounted);
    native
        .configure_component_factories(vec![
            Arc::new(
                sinomo_dfs::component_factory_with_storage_preparation(Arc::new(
                    move |context, lifecycle| {
                        let provider = storage_preparation_provider(context, lifecycle.clone())?;
                        let runtime = shared_runtime(context, lifecycle)?;
                        *builder_mounted.lock().unwrap() = Some(runtime);
                        let hold = Arc::clone(&builder_hold);
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
    {
        let mut client = native.client(&node).unwrap();
        for path in ["/services/count-cohort-file", "/users/count-cohort-file"] {
            client
                .posix_write_file(
                    path,
                    b"retained count cohort bytes",
                    sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
                )
                .unwrap();
        }
    }
    mounted.lock().unwrap().take();
    native.stop_node(&node).unwrap();
    hold.store(true, Ordering::SeqCst);
    let captured = Arc::new(Mutex::new(None::<NamespaceCountCohortSeal>));
    for pause_enrollment in [true, false] {
        let first_mounted = Arc::clone(&mounted);
        let first_capture = Arc::clone(&captured);
        let first_fs = fs.clone();
        let first = native.reopen_node_with_startup_action(
            node.clone(),
            Box::new(move |remaining| {
                let runtime = first_mounted.lock().unwrap().as_ref().unwrap().clone();
                let owner = NamespaceCountMaintenance {
                    runtime: Arc::clone(&runtime),
                };
                let deadline = std::time::Instant::now() + remaining;
                let catalog = runtime
                    .authority
                    .assignments(std::slice::from_ref(&first_fs))
                    .map_err(|e| e.to_string())?;
                assert!(
                    catalog.len() >= 2,
                    "fixture must span independent physical scopes"
                );
                let mut sources = Vec::new();
                for assignment in &catalog {
                    let shard = DfsMetadataShardId::new(&assignment.shard_id);
                    loop {
                        if std::time::Instant::now() >= deadline {
                            return Err(
                                "count source capture exceeded original startup deadline".into()
                            );
                        }
                        if let Some(source) = owner
                            .capture(&shard, &catalog, 256)
                            .map_err(|e| e.to_string())?
                        {
                            sources.push(source);
                            break;
                        }
                    }
                }
                let retained = owner
                    .retained_cohort(&DfsMetadataShardId::new(&catalog[0].shard_id))
                    .map_err(|e| e.to_string())?;
                if !pause_enrollment {
                    assert!(
                        retained.is_some(),
                        "restart must retain the original invocation"
                    );
                }
                let at = retained
                    .as_ref()
                    .map_or_else(time::OffsetDateTime::now_utc, |seal| seal.initialized_at);
                let mut seal = None;
                for assignment in &catalog {
                    let current = owner
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
                        assert_eq!(expected, &current);
                    }
                    seal = Some(current.clone());
                    if pause_enrollment {
                        assert!(
                            runtime
                                .authority
                                .assignments(std::slice::from_ref(&first_fs))
                                .map_err(|e| e.to_string())?
                                == catalog
                        );
                        assert!(
                            !runtime
                                .authority
                                .published_count_cohort(&current)
                                .map_err(|e| e.to_string())?
                        );
                        *first_capture.lock().unwrap() = Some(current);
                        return Err("intentional native restart during enrollment".into());
                    }
                }
                let seal = seal.unwrap();
                assert_eq!(
                    first_capture.lock().unwrap().as_ref(),
                    Some(&seal),
                    "restart must not create another cohort/time"
                );
                *first_capture.lock().unwrap() = Some(seal.clone());
                loop {
                    if std::time::Instant::now() >= deadline {
                        return Err(
                            "count first activation exceeded original startup deadline".into()
                        );
                    }
                    for member in &seal.members {
                        match owner.advance(&member.original_cut.scope_shard, 256) {
                            Ok(DfsStoragePreparation::Pending { .. })
                            | Err(MetadataBackendError::StoragePreparationPending { .. }) => {}
                            Ok(DfsStoragePreparation::Ready) => {
                                panic!("ordinary Ready before complete cohort activation")
                            }
                            Err(e) => return Err(e.to_string()),
                        }
                        let current = runtime
                            .authority
                            .assignments(std::slice::from_ref(&first_fs))
                            .map_err(|e| e.to_string())?;
                        let activated = current
                            .iter()
                            .filter(|row| {
                                seal.members.iter().any(|member| {
                                    member.assignment.shard_id == row.shard_id
                                        && row.epoch > member.assignment.epoch
                                })
                            })
                            .count();
                        if activated > 0 {
                            assert!(activated < seal.members.len());
                            assert!(
                                !runtime
                                    .authority
                                    .published_count_cohort(&seal)
                                    .map_err(|e| e.to_string())?
                            );
                            return Err(
                                "intentional native restart after first real activation".into()
                            );
                        }
                    }
                }
            }),
        );
        let first_error = first.unwrap_err();
        let expected = if pause_enrollment {
            "intentional native restart during enrollment"
        } else {
            "intentional native restart after first real activation"
        };
        assert!(first_error.contains(expected), "{first_error}");
        mounted.lock().unwrap().take();
        native.stop_node(&node).unwrap();
    }
    let seal = captured.lock().unwrap().take().unwrap();
    let restart_mounted = Arc::clone(&mounted);
    let restart_hold = Arc::clone(&hold);
    native
        .reopen_node_with_startup_action(
            node.clone(),
            Box::new(move |remaining| {
                let runtime = restart_mounted.lock().unwrap().as_ref().unwrap().clone();
                let owner = NamespaceCountMaintenance {
                    runtime: Arc::clone(&runtime),
                };
                let deadline = std::time::Instant::now() + remaining;
                loop {
                    if std::time::Instant::now() >= deadline {
                        return Err(
                            "count cohort recovery exceeded original startup deadline".into()
                        );
                    }
                    for member in &seal.members {
                        match owner.advance(&member.original_cut.scope_shard, 256) {
                            Ok(_) | Err(MetadataBackendError::StoragePreparationPending { .. }) => {
                            }
                            Err(e) => return Err(e.to_string()),
                        }
                    }
                    if runtime
                        .authority
                        .published_count_cohort(&seal)
                        .map_err(|e| e.to_string())?
                    {
                        break;
                    }
                }
                restart_hold.store(false, Ordering::SeqCst);
                Ok(())
            }),
        )
        .unwrap();
    {
        let mut client = native.client(&node).unwrap();
        for path in ["/services/count-cohort-file", "/users/count-cohort-file"] {
            assert_eq!(
                client.posix_read_file_distributed(path).unwrap(),
                b"retained count cohort bytes"
            );
        }
    }
    // A completed release still needs its native provider to verify current
    // authority. A missing top-level marker cannot erase the journal binding.
    let metadata_root = native.data_dir(&node).dfs_dir().join("metadata/root");
    mounted.lock().unwrap().take();
    native.stop_node(&node).unwrap();
    let marker = metadata_root.join("metadata-import-v7.json");
    let original_marker = std::fs::read(&marker).unwrap();
    let runtime = native.runtime_handle();
    let entered = runtime.enter();
    assert!(matches!(
        sinomo_dfs::LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root),
        Err(MetadataBackendError::StoragePreparationPending { root, processed_records: 0 }) if root == metadata_root
    ));
    std::fs::remove_file(&marker).unwrap();
    assert!(
        matches!(
            sinomo_dfs::LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root),
            Err(MetadataBackendError::StoragePreparationPending { root, processed_records: 0 }) if root == metadata_root
        ),
        "missing marker must not erase the immutable cohort obligation"
    );
    std::fs::write(&marker, &original_marker).unwrap();
    let mut corrupt: serde_json::Value = serde_json::from_slice(&original_marker).unwrap();
    corrupt["source_identity"] = serde_json::json!("wrong-original-source");
    std::fs::write(&marker, serde_json::to_vec(&corrupt).unwrap()).unwrap();
    assert!(
        crate::metadata::published_count_source_for_test(
            &metadata_root,
            &DfsMetadataShardId::new("root")
        )
        .is_err()
    );
    std::fs::write(&marker, original_marker).unwrap();
    drop(entered);
    native.reopen_node(node.clone()).unwrap();
    mounted.lock().unwrap().take();
    native.close().unwrap();
}
