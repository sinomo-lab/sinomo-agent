//! Real completed namespace moves expose physical history rows which no longer
//! own their remembered path. GC must compare the actual namespace occupant.
use super::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const ORIGINAL: &[u8] = b"actual native cross-shard file retained at its target";
const REPLACEMENT: &[u8] = b"different new live source-path occupant";

#[derive(serde::Serialize)]
struct PhysicalCut {
    original: Option<DfsObjectMetadata>,
    occupant: Option<DfsObjectMetadata>,
    snapshot: DfsMetadataTableSnapshot,
}

fn physical_cut(
    native: &crate::test_runtime::DfsTestRuntime,
    config: &DfsMetadataShardConfig,
    original: &DfsObjectId,
    path: &str,
) -> PhysicalCut {
    let shard_id = DfsMetadataShardId::new(&config.shard_id);
    let mut reader = LocalSobjMetadataBackend::open_for_inspection(
        shard_id.clone(),
        crate::local_dfs_metadata_shard_path(native.data_dir(&config.primary_node_id), &shard_id),
    )
    .unwrap();
    reader.begin_inspection_snapshot().unwrap();
    let cut = PhysicalCut {
        original: reader.get_object_on_shard(&shard_id, original).unwrap(),
        occupant: reader
            .get_object_by_path_on_shard(&shard_id, &config.file_system_id, path)
            .unwrap(),
        snapshot: reader
            .export_shard_table_snapshot(&shard_id)
            .unwrap()
            .unwrap(),
    };
    reader.end_inspection_snapshot();
    cut
}

fn is_namespace_rewrite(effect: &DfsPreparedMetadataEffect) -> bool {
    matches!(
        effect,
        DfsPreparedMetadataEffect::PutObject { .. }
            | DfsPreparedMetadataEffect::FinalizeCrossShardSourceEviction { .. }
    )
}

fn completed_move_then_gc(directory: bool, recreate_source: bool) {
    let name = match (directory, recreate_source) {
        (false, false) => "native-history-gc-file-marker",
        (false, true) => "native-history-gc-file-replaced-path",
        (true, false) => "native-history-gc-directory-marker",
        _ => unreachable!(),
    };
    let from = format!("/{name}-source");
    let to = format!("/services/{name}-target");
    let (native, initial, target) =
        crate::test_support::current_native_metadata_service(name, 3, &to).unwrap();
    drop(initial);
    let catalog = native.metadata_shards().unwrap();
    let source = crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
        &target.file_system_id,
        &catalog,
        &from,
    )
    .unwrap()
    .unwrap();
    assert_ne!(source.shard_id, target.shard_id);
    // This is a physical cross-shard move, including when the normal initial
    // layout colocates both metadata primaries. No shard/config row is forged.
    assert_eq!(source.primary_node_id, target.primary_node_id);
    let owner = source.primary_node_id.clone();
    let mut client = native.client(&owner).unwrap();
    let original = if directory {
        client.posix_mkdir_all(&from).unwrap();
        client.posix_stat_distributed(&from).unwrap()
    } else {
        client
            .posix_write_file(
                &from,
                ORIGINAL,
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            )
            .unwrap()
    };
    let moved = client.posix_rename(&from, &to);
    let target_stat = client.posix_stat_distributed(&to);
    let source_stat_after_move = client.posix_stat_distributed(&from);
    let replacement = if recreate_source {
        Some(
            client
                .posix_write_file(
                    &from,
                    REPLACEMENT,
                    DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
                )
                .unwrap(),
        )
    } else {
        None
    };
    drop(client);
    let before_source = physical_cut(&native, &source, &original.object_id, &from);
    let before_target = physical_cut(&native, &target, &original.object_id, &to);
    let completed_full = before_source
        .snapshot
        .tables
        .namespace_jobs
        .iter()
        .find(|job| {
            crate::namespace::metadata_path(&job.root_path)
                == crate::namespace::metadata_path(&from)
                && job.target_path.as_ref().is_some_and(|path| {
                    crate::namespace::metadata_path(path) == crate::namespace::metadata_path(&to)
                })
                && job.status == DfsNamespaceJobStatus::Completed
                && job.pending_frames.is_empty()
        })
        .cloned();
    let completed_receipt = before_source
        .snapshot
        .tables
        .namespace_job_receipts
        .iter()
        .find(|job| {
            crate::namespace::metadata_path(&job.root_path)
                == crate::namespace::metadata_path(&from)
                && job.target_path.as_ref().is_some_and(|path| {
                    crate::namespace::metadata_path(path) == crate::namespace::metadata_path(&to)
                })
                && job.status == DfsNamespaceJobStatus::Completed
        })
        .cloned();
    // A rename is a metadata-only successor. Bind the physical read to its
    // actual recorded immutable source generation, then independently use the
    // normal per-object content-identity index. A new stat VID need not have a
    // separate physical content binding.
    let target_content = (!directory).then(|| {
        before_target
            .snapshot
            .tables
            .versions
            .iter()
            .find(|version| {
                version.object_id == original.object_id
                    && version.version == target_stat.as_ref().unwrap().version
            })
            .expect("actual target head version exists")
            .clone()
    });
    let before_physical =
        LocalContentStore::open_for_inspection(native.data_dir(&owner).dfs_dir().join("content"));
    let before_source_bytes = target_content.as_ref().map(|version| {
        let plan = version
            .content_durability
            .as_ref()
            .expect("actual write preserves its recorded content generation");
        before_physical
            .read_object_version(original.object_id.as_str(), plan.source_version.as_str())
    });
    let before_indexed_bytes = target_content.as_ref().map(|version| {
        before_physical.read_object_content_range(
            original.object_id.as_str(),
            version.content_hash.as_ref().unwrap(),
            version.size_bytes.unwrap(),
            0,
            usize::MAX,
        )
    });
    drop(before_physical);
    let before_logical_bytes = (!directory).then(|| {
        native
            .client(&owner)
            .unwrap()
            .posix_read_file_distributed(&to)
    });
    let diagnostics = native
        .data_dir(&owner)
        .root()
        .parent()
        .unwrap()
        .to_path_buf();
    std::fs::write(diagnostics.join("gc-move-before.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "qualification":"normal Main move return, full completed migration job or actual compact receipt, and fresh physical source/target native cuts before GC",
        "directory":directory,"recreated":recreate_source,"source_config":source,"target_config":target,
        "original":format!("{original:?}"),"actual_move_outcome":format!("{moved:?}"),
        "target_stat":format!("{target_stat:?}"),"source_stat_after_move":format!("{source_stat_after_move:?}"),
        "replacement":format!("{replacement:?}"),"completed_full_job":completed_full,"completed_job_receipt":completed_receipt,
        "physical_source":before_source,"physical_target":before_target,
        "actual_target_head_content_record":target_content,
        "before_exact_recorded_source_bytes":format!("{before_source_bytes:?}"),
        "before_object_content_index_bytes":format!("{before_indexed_bytes:?}"),
        "before_actual_logical_target_read":format!("{before_logical_bytes:?}"),
    })).unwrap()).unwrap();
    if let Some(result) = &before_source_bytes {
        let (manifest, bytes) = result.as_ref().unwrap();
        let version = target_content.as_ref().unwrap();
        assert_eq!(manifest.object_id, original.object_id.as_str());
        assert_eq!(
            manifest.version,
            version
                .content_durability
                .as_ref()
                .unwrap()
                .source_version
                .as_str()
        );
        assert_eq!(
            &manifest.content_hash,
            version.content_hash.as_ref().unwrap()
        );
        assert_eq!(manifest.size_bytes, version.size_bytes.unwrap());
        assert_eq!(bytes, ORIGINAL);
    }
    if let Some(result) = &before_indexed_bytes {
        let (manifest, bytes) = result.as_ref().unwrap().as_ref().unwrap();
        let version = target_content.as_ref().unwrap();
        assert_eq!(manifest.object_id, original.object_id.as_str());
        assert_eq!(
            &manifest.content_hash,
            version.content_hash.as_ref().unwrap()
        );
        assert_eq!(manifest.size_bytes, version.size_bytes.unwrap());
        assert_eq!(bytes, ORIGINAL);
    }
    if let Some(result) = &before_logical_bytes {
        assert_eq!(result.as_ref().unwrap(), ORIGINAL);
    }
    let target_stat = target_stat.unwrap();
    moved.unwrap();
    assert_eq!(target_stat.object_id, original.object_id);
    assert!(
        matches!(&source_stat_after_move, Err(DfsPosixError::NotFound { .. })),
        "the actual logical source must be absent, not unavailable: {source_stat_after_move:?}"
    );
    assert!(
        completed_full.is_some() || completed_receipt.is_some(),
        "successful move must have completed its entire actual namespace job, not only Adopt"
    );
    let retired = before_source
        .original
        .as_ref()
        .expect("actual source provenance row remains physical");
    assert_eq!(retired.kind, DfsObjectKind::Tombstone);
    assert!(matches!(&retired.tombstone_provenance,
        Some(DfsTombstoneProvenance::SourceEviction { source_shard_id, target_shard_id, .. })
            if *source_shard_id == DfsMetadataShardId::new(&source.shard_id)
                && *target_shard_id == DfsMetadataShardId::new(&target.shard_id)));
    let occupant = before_source
        .occupant
        .as_ref()
        .expect("normal cleanup/recreate left a physical namespace marker");
    if !directory {
        assert_ne!(
            occupant.object_id, original.object_id,
            "real file cleanup/new occupant is distinct from the physical source provenance"
        );
    }
    if let Some(replacement) = &replacement {
        assert_eq!(occupant.object_id, replacement.object_id);
        assert_eq!(occupant.version, replacement.version);
        assert_eq!(occupant.kind, DfsObjectKind::File);
    } else {
        assert_eq!(occupant.kind, DfsObjectKind::Tombstone);
    }
    assert_eq!(
        before_target.occupant.as_ref().unwrap().object_id,
        original.object_id
    );

    let observed = Arc::new(Mutex::new(Vec::<DfsPreparedOperationPayload>::new()));
    let capture = Arc::clone(&observed);
    let hook = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        native.data_dir(&owner),
        Arc::new(move |point, _, _| {
            if let crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeMetadataEffectPrepare {
                payload,
            } = point
                && payload.operation_id.starts_with("history-gc:")
            {
                capture.lock().unwrap().push(payload.clone());
            }
            Ok(())
        }),
    );
    // The normal current owning service selects its actual scopes and guards.
    // No supplied table row, receipt, completed job or historical effect is used.
    let mut outcomes = Vec::new();
    'gc_rounds: for _ in 0..2 {
        // Each opener deliberately owns exactly its selected physical shard.
        // Collect both actual move scopes; sharing one primary does not make
        // a source-only Core include the destination's history.
        for path in [&from, &to] {
            let result = native.metadata_primary_service(path).and_then(|mut owner| {
                owner
                    .gc_metadata_history(DfsMetadataHistoryGcOptions::delete_eligible(
                        Duration::ZERO,
                        1,
                    ))
                    .map_err(|error| error.to_string())
            });
            let failed = result.is_err();
            outcomes.push(result);
            if failed {
                break 'gc_rounds;
            }
        }
    }
    drop(hook);
    let after_source = physical_cut(&native, &source, &original.object_id, &from);
    let after_target = physical_cut(&native, &target, &original.object_id, &to);
    let payloads = observed.lock().unwrap().clone();
    let old_id_guarded = payloads.iter().any(|payload| {
        payload.shard_id == DfsMetadataShardId::new(&source.shard_id)
            && payload.expected_object_versions.iter().any(|guard| {
                guard.object_id == original.object_id
                    && guard.expected_version.as_ref() == Some(&retired.version)
            })
    });
    let actual_path_guarded = payloads.iter().any(|payload| {
        payload.shard_id == DfsMetadataShardId::new(&source.shard_id)
            && payload.expected_path_occupants.iter().any(|guard| {
                guard.path == crate::namespace::metadata_path(&from)
                    && guard.expected_object_id.as_ref() == Some(&occupant.object_id)
            })
    });
    let effects_do_not_rewrite_namespace = payloads.iter().all(|payload| {
        payload.transaction.objects.is_empty()
            && payload.transaction.versions.is_empty()
            && !payload.metadata_effects.iter().any(is_namespace_rewrite)
    });
    let target_compacted = before_target
        .snapshot
        .tables
        .prepared_operations_v7
        .iter()
        .filter(|record| {
            record.status == DfsPreparedOperationStatus::Committed
                && (record.payload.transaction.objects.iter().any(|object| {
                    object.object_id == original.object_id
                        && crate::namespace::metadata_path(&object.path)
                            == crate::namespace::metadata_path(&to)
                }) || record.payload.metadata_effects.iter().any(|effect| {
                    matches!(effect, DfsPreparedMetadataEffect::PutObject { object }
                        if object.object_id == original.object_id
                            && crate::namespace::metadata_path(&object.path)
                                == crate::namespace::metadata_path(&to))
                }))
        })
        .any(|record| {
            after_target
                .snapshot
                .tables
                .prepared_operation_evictions
                .iter()
                .any(|eviction| eviction.digest == record.digest && eviction.validate().is_ok())
        });
    let physical =
        LocalContentStore::open_for_inspection(native.data_dir(&owner).dfs_dir().join("content"));
    let target_bytes = (!directory).then(|| {
        physical.read_object_version(original.object_id.as_str(), target_stat.version.as_str())
    });
    let after_source_bytes = target_content.as_ref().map(|version| {
        physical.read_object_version(
            original.object_id.as_str(),
            version
                .content_durability
                .as_ref()
                .unwrap()
                .source_version
                .as_str(),
        )
    });
    let after_indexed_bytes = target_content.as_ref().map(|version| {
        physical.read_object_content_range(
            original.object_id.as_str(),
            version.content_hash.as_ref().unwrap(),
            version.size_bytes.unwrap(),
            0,
            usize::MAX,
        )
    });
    let after_logical_bytes = (!directory).then(|| {
        native
            .client(&owner)
            .unwrap()
            .posix_read_file_distributed(&to)
    });
    let replacement_bytes = replacement
        .as_ref()
        .map(|stat| physical.read_object_version(stat.object_id.as_str(), stat.version.as_str()));
    std::fs::write(diagnostics.join("gc-move-outcome.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "qualification":"actual GC producer payloads/outcomes and native after-cuts before assertions; GC read predicates never claim namespace mutation permission",
        "outcomes":outcomes,"actual_gc_payloads":payloads,"old_id_guarded":old_id_guarded,
        "actual_path_guarded":actual_path_guarded,"effects_do_not_rewrite_namespace":effects_do_not_rewrite_namespace,
        "target_compacted":target_compacted,"before_source":before_source,"after_source":after_source,
        "before_target":before_target,"after_target":after_target,
        "direct_metadata_vid_probe_only":format!("{target_bytes:?}"),
        "actual_target_head_content_record":target_content,
        "after_exact_recorded_source_bytes":format!("{after_source_bytes:?}"),
        "after_object_content_index_bytes":format!("{after_indexed_bytes:?}"),
        "after_actual_logical_target_read":format!("{after_logical_bytes:?}"),
        "actual_replacement_bytes":format!("{replacement_bytes:?}"),
        "rpc_window":format!("{:#?}",native.rpc_observations()),"omitted_rpc_observations":native.observation_dropped_count(),
    })).unwrap()).unwrap();
    native.checkpoint_rpc_observation_window().unwrap();
    assert!(
        outcomes.iter().all(Result::is_ok),
        "normal GC must complete: {outcomes:?}"
    );
    assert!(effects_do_not_rewrite_namespace && !payloads.is_empty());
    assert_eq!(
        after_source.occupant, before_source.occupant,
        "history collection cannot reclaim or redirect the new current source-path occupant"
    );
    assert_eq!(
        after_source.original, before_source.original,
        "the original physical source provenance/version is not rewritten by history compaction"
    );
    assert_eq!(after_target.occupant, before_target.occupant);
    if !directory {
        assert!(
            old_id_guarded && actual_path_guarded,
            "GC retains old physical version guard but binds the actual current path identity"
        );
    }
    assert!(
        target_compacted,
        "a genuine target terminal body must have a real validated compact outcome"
    );
    if let Some(bytes) = after_source_bytes {
        assert_eq!(
            bytes.unwrap().1,
            ORIGINAL,
            "GC must preserve the actual recorded immutable source binding proven live before collection"
        );
    }
    if let Some(bytes) = after_indexed_bytes {
        assert_eq!(
            bytes.unwrap().unwrap().1,
            ORIGINAL,
            "the ordinary per-object content-identity index must retain the current target content"
        );
    }
    if let Some(bytes) = after_logical_bytes {
        assert_eq!(bytes.unwrap(), ORIGINAL);
    }
    if let Some(bytes) = replacement_bytes {
        assert_eq!(bytes.unwrap().1, REPLACEMENT);
    }
    assert_eq!(native.observation_dropped_count(), 0);
    drop(physical);
    native.close().unwrap();
}

#[test]
fn native_completed_file_move_gc_preserves_distinct_source_marker_and_target() {
    completed_move_then_gc(false, false);
}

#[test]
fn native_completed_file_move_gc_cannot_reclaim_a_real_new_live_source_occupant() {
    completed_move_then_gc(false, true);
}

#[test]
fn native_completed_directory_move_gc_preserves_its_actual_source_marker() {
    completed_move_then_gc(true, false);
}
