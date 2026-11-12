use super::*;
use sinomo_core::kv::{KvEntry, KvError, KvRevision, KvTxn, KvTxnResponse};

use sinomo_core::node::NodeId;
use std::collections::BTreeSet;
use std::path::Path;
use std::time::Instant;

fn temp_root(name: &str) -> sinomo_testdir::TestDir {
    sinomo_testdir::TestDir::new(&format!("sinomo-dfs-attachments-{name}"))
}

fn native_attachment_service(
    name: &str,
    node: NodeId,
) -> (
    sinomo_dfs::test_support::DfsTestRuntime,
    NetworkedDfsService<sinomo_dfs::DfsNodeTransport>,
) {
    let mut runtime = sinomo_dfs::test_support::DfsTestRuntime::new(name, node.clone()).unwrap();
    runtime.start_nodes().unwrap();
    let service = runtime.client(&node).unwrap();
    (runtime, service)
}

fn options(node: &NodeId) -> DfsWriteOptions {
    DfsWriteOptions {
        policy: DfsReplicationPolicy::new(1, 1).unwrap(),
        replica_candidates: vec![node.clone()],
        wait_for_commit: true,
        foreground_replica_repair: false,
    }
}

fn source(stat: &DfsStat) -> DfsAttachmentSource {
    DfsAttachmentSource {
        path: stat.path.clone(),
        file_system_id: stat.file_system_id.clone(),
        object_id: stat.object_id.clone(),
    }
}

fn metadata(version: Option<DfsVersion>, name: &str) -> DfsAttachmentMetadata {
    DfsAttachmentMetadata {
        key: DfsAttachmentKey {
            kind: "org.example.test".into(),
            name: name.into(),
        },
        schema: "org.example.test/v1".into(),
        media_type: "application/octet-stream".into(),
        source_version: version,
        producer: "test/v1".into(),
    }
}

fn stored(response: DfsAttachmentResponse) -> DfsAttachmentDescriptor {
    match response {
        DfsAttachmentResponse::Stored { attachment } => attachment,
        other => panic!("{other:?}"),
    }
}

/// Opt-in density benchmark. Run the same compiled test with different counts;
/// output is JSON so baseline and candidate runs can be compared without CI.
#[test]
#[ignore = "attachment density benchmark; set SINOMO_ATTACHMENT_BENCH_OUTPUT"]
fn attachment_density_benchmark() {
    fn parameter(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .map(|v| v.parse().unwrap())
            .unwrap_or(default)
    }
    fn disk_bytes(path: &Path) -> u64 {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let metadata = entry.metadata().unwrap();
                if metadata.is_dir() {
                    disk_bytes(&entry.path())
                } else {
                    metadata.len()
                }
            })
            .sum()
    }
    fn percentile(values: &mut [u128], percent: usize) -> u128 {
        values.sort_unstable();
        values[(values.len() - 1) * percent / 100]
    }
    let files = parameter("SINOMO_ATTACHMENT_BENCH_FILES", 32);
    let kinds = parameter("SINOMO_ATTACHMENT_BENCH_KINDS", 20);
    let size = parameter("SINOMO_ATTACHMENT_BENCH_BYTES", 128);
    assert!(files > 0 && kinds > 0 && size <= DFS_ATTACHMENT_MAX_PAYLOAD_BYTES);
    let root = temp_root("attachment-density-benchmark");
    let node = NodeId::from_legacy_named("attachment-density-benchmark").unwrap();
    let (mut runtime, mut dfs) =
        native_attachment_service("attachment_density_benchmark", node.clone());
    let mut sources = Vec::new();
    for i in 0..files {
        sources.push(source(
            &dfs.posix_write_file(&format!("/users/bench-{i}"), b"source", options(&node))
                .unwrap(),
        ));
    }
    let before_bytes = disk_bytes(&runtime.data_dir(&node).dfs_dir());
    let started = Instant::now();
    let mut write_micros = Vec::new();
    for (i, source) in sources.iter().enumerate() {
        for j in 0..kinds {
            let start = Instant::now();
            dfs.file_attachment(
                DfsAttachmentRequest::Put {
                    source: source.clone(),
                    metadata: metadata(None, &format!("kind-{j}")),
                    expected: None,
                    operation_id: format!("bench-{i}-{j}"),
                    bytes: vec![42; size],
                },
                Some(options(&node)),
            )
            .unwrap();
            write_micros.push(start.elapsed().as_micros());
        }
    }
    let elapsed = started.elapsed();
    let mut get_micros = Vec::new();
    for i in 0..256 {
        let start = Instant::now();
        let response = dfs
            .file_attachment(
                DfsAttachmentRequest::Get {
                    source: sources[i % files].clone(),
                    key: metadata(None, &format!("kind-{}", i % kinds)).key,
                },
                None,
            )
            .unwrap();
        assert!(matches!(
            response,
            DfsAttachmentResponse::Descriptor {
                attachment: Some(_)
            }
        ));
        get_micros.push(start.elapsed().as_micros());
    }
    let live_catalog_bytes: u64 = sources
        .iter()
        .map(|source| {
            dfs.posix_stat_distributed(&format!(
                "{}/catalog.sobj",
                dfs_attachment_catalog_directory(source)
            ))
            .unwrap()
            .size_bytes
            .unwrap()
        })
        .sum();
    let live_attachment_bytes = live_catalog_bytes
        + if size > DFS_ATTACHMENT_INLINE_BYTES {
            (files * kinds * size) as u64
        } else {
            0
        };
    let after_bytes = disk_bytes(&runtime.data_dir(&node).dfs_dir());
    let object_count = runtime
        .node_metadata(&node)
        .unwrap()
        .metadata_summary()
        .unwrap()
        .object_count;
    drop(dfs);
    let opened = Instant::now();
    runtime.stop_node(&node).unwrap();
    runtime.reopen_node(node.clone()).unwrap();
    let mut dfs = runtime.client(&node).unwrap();
    dfs.file_attachment(
        DfsAttachmentRequest::Get {
            source: sources[0].clone(),
            key: metadata(None, "kind-0").key,
        },
        None,
    )
    .unwrap();
    let report = serde_json::json!({
        "files": files, "kinds_per_file": kinds, "payload_bytes": size,
        "attachment_count": files * kinds, "objects": object_count,
        "added_disk_bytes": after_bytes - before_bytes,
        "live_attachment_bytes": live_attachment_bytes,
        "write_elapsed_ms": elapsed.as_millis(),
        "writes_per_second": (files * kinds) as f64 / elapsed.as_secs_f64(),
        "write_p50_us": percentile(&mut write_micros, 50),
        "write_p95_us": percentile(&mut write_micros, 95),
        "get_p50_us": percentile(&mut get_micros, 50),
        "get_p95_us": percentile(&mut get_micros, 95),
        "reopen_and_get_ms": opened.elapsed().as_millis(),
    });
    let output = std::env::var("SINOMO_ATTACHMENT_BENCH_OUTPUT").expect("benchmark output path");
    std::fs::write(output, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    eprintln!("{report}");
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn attachments_persist_ranges_cas_and_source_fences() {
    let root = temp_root("attachments");
    let node = NodeId::from_legacy_named("attachments-node").unwrap();
    let (mut runtime, mut dfs) = native_attachment_service(
        "attachments_persist_ranges_cas_and_source_fences",
        node.clone(),
    );
    let stat = dfs
        .posix_write_file("/users/source", b"original", options(&node))
        .unwrap();
    let source = source(&stat);
    let metadata = metadata(Some(stat.version.clone()), "large");
    let bytes = (0..2 * 1024 * 1024 + 23)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<_>>();
    let request = DfsAttachmentRequest::Put {
        source: source.clone(),
        metadata: metadata.clone(),
        expected: None,
        operation_id: "attachment-create".into(),
        bytes: bytes.clone(),
    };
    let first = stored(
        dfs.file_attachment(request.clone(), Some(options(&node)))
            .unwrap(),
    );
    let retry = stored(dfs.file_attachment(request, Some(options(&node))).unwrap());
    assert_eq!(first.revision, retry.revision);
    assert_eq!(
        dfs.posix_stat_distributed(&source.path).unwrap().version,
        stat.version
    );
    drop(dfs);
    runtime.stop_node(&node).unwrap();
    runtime.reopen_node(node.clone()).unwrap();
    let mut dfs = runtime.client(&node).unwrap();
    let get = dfs
        .file_attachment(
            DfsAttachmentRequest::Get {
                source: source.clone(),
                key: metadata.key.clone(),
            },
            None,
        )
        .unwrap();
    assert!(
        matches!(get, DfsAttachmentResponse::Descriptor { attachment: Some(ref a) } if a == &first)
    );
    for offset in [0, 1024 * 1024, 2 * 1024 * 1024] {
        let response = dfs
            .file_attachment(
                DfsAttachmentRequest::Read {
                    source: source.clone(),
                    key: metadata.key.clone(),
                    revision: first.revision.clone(),
                    offset: offset as u64,
                    max_bytes: 1024 * 1024,
                },
                None,
            )
            .unwrap();
        let DfsAttachmentResponse::Data { bytes: range, eof } = response else {
            panic!("range response");
        };
        assert_eq!(
            range,
            bytes[offset..(offset + 1024 * 1024).min(bytes.len())]
        );
        assert_eq!(eof, offset + range.len() == bytes.len());
    }
    let replacement = DfsAttachmentRequest::Put {
        source: source.clone(),
        metadata: metadata.clone(),
        expected: Some(first.revision.clone()),
        operation_id: "attachment-replace".into(),
        bytes: b"replacement".to_vec(),
    };
    let second = stored(
        dfs.file_attachment(replacement.clone(), Some(options(&node)))
            .unwrap(),
    );
    assert_eq!(
        stored(
            dfs.file_attachment(replacement, Some(options(&node)))
                .unwrap()
        )
        .revision,
        second.revision
    );
    assert_ne!(first.revision, second.revision);
    assert!(
        dfs.file_attachment(
            DfsAttachmentRequest::Delete {
                source: source.clone(),
                key: metadata.key.clone(),
                expected: first.revision,
                operation_id: "stale-delete".into()
            },
            None
        )
        .is_err()
    );
    dfs.posix_write_file(&source.path, b"new source", options(&node))
        .unwrap();
    let response = dfs
        .file_attachment(
            DfsAttachmentRequest::Get {
                source: source.clone(),
                key: metadata.key.clone(),
            },
            None,
        )
        .unwrap();
    assert!(
        matches!(response, DfsAttachmentResponse::Descriptor { attachment: Some(a) } if !a.current)
    );
    assert!(
        dfs.file_attachment(
            DfsAttachmentRequest::Read {
                source: source.clone(),
                key: metadata.key.clone(),
                revision: second.revision.clone(),
                offset: 0,
                max_bytes: 100
            },
            None
        )
        .is_err()
    );
    assert!(
        dfs.file_attachment(
            DfsAttachmentRequest::Put {
                source: source.clone(),
                metadata,
                expected: Some(second.revision),
                operation_id: "stale-worker".into(),
                bytes: vec![]
            },
            Some(options(&node))
        )
        .is_err()
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn attachment_pages_follow_identity_without_full_directory_collection() {
    let root = temp_root("attachment-pages");
    let node = NodeId::from_legacy_named("attachment-pages-node").unwrap();
    let (mut runtime, mut dfs) = native_attachment_service(
        "attachment_pages_follow_identity_without_full_directory_collection",
        node.clone(),
    );
    let stat = dfs
        .posix_write_file("/users/source", b"source", options(&node))
        .unwrap();
    let mut source = source(&stat);
    for i in 0..17 {
        dfs.file_attachment(
            DfsAttachmentRequest::Put {
                source: source.clone(),
                metadata: metadata(None, &format!("item-{i}")),
                expected: None,
                operation_id: format!("create-{i}"),
                bytes: vec![i],
            },
            Some(options(&node)),
        )
        .unwrap();
    }
    // Populate unrelated objects through the normal batched write path. The
    // queried source still has only 17 attachments across this larger namespace.
    let unrelated = (0..10_000)
        .map(|i| format!("/users/unrelated-{i}"))
        .collect::<Vec<_>>();
    dfs.posix_write_files_local_eventual_with_known_parents(
        unrelated.iter().map(|path| (path.as_str(), b"".as_slice())),
    )
    .unwrap();
    dfs.posix_rename(&source.path, "/users/renamed").unwrap();
    source.path = "/users/renamed".into();
    sinomo_dfs::reset_posix_metadata_full_directory_collect_count();
    let mut cursor = None;
    let mut names = BTreeSet::new();
    loop {
        let response = dfs
            .file_attachment(
                DfsAttachmentRequest::List {
                    source: source.clone(),
                    cursor,
                    limit: 3,
                },
                None,
            )
            .unwrap();
        let DfsAttachmentResponse::Page {
            attachments,
            next_cursor,
        } = response
        else {
            panic!("page");
        };
        assert!(attachments.len() <= 3);
        for a in attachments {
            assert!(a.current);
            assert!(names.insert(a.metadata.key.name));
        }
        cursor = next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(names.len(), 17);
    assert_eq!(sinomo_dfs::posix_metadata_full_directory_collect_count(), 0);
    dfs.posix_unlink(&source.path).unwrap();
    dfs.posix_write_file(&source.path, b"different file", options(&node))
        .unwrap();
    assert!(
        dfs.file_attachment(
            DfsAttachmentRequest::List {
                source,
                cursor: None,
                limit: 3
            },
            None
        )
        .is_err()
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn attachment_builtin_validation_and_custom_schema_roundtrip() {
    let mut tags = DfsFileTags {
        tags: vec![" work ".into(), "work".into(), "project".into()],
    };
    tags.canonicalize().unwrap();
    assert_eq!(tags.tags, ["project", "work"]);
    let version = DfsVersion::from_legacy_named("transcript-version").unwrap();
    let metadata = DfsAttachmentMetadata {
        key: DfsAttachmentKey::transcript("en"),
        schema: "sinomo.transcript/v1".into(),
        media_type: "application/x-ndjson".into(),
        source_version: Some(version),
        producer: "test".into(),
    };
    assert!(
        validate_dfs_attachment_payload(
            &metadata,
            b"{\"start_millis\":0,\"end_millis\":100,\"speaker\":null,\"text\":\"hello\"}\n"
        )
        .is_ok()
    );
    assert!(
        validate_dfs_attachment_payload(
            &metadata,
            b"{\"start_millis\":100,\"end_millis\":0,\"speaker\":null,\"text\":\"hello\"}\n"
        )
        .is_err()
    );
    assert!(
        DfsAttachmentKey {
            kind: "org.example.data".into(),
            name: "../escape".into()
        }
        .validate()
        .is_err()
    );
}

#[test]
fn attachments_compact_batch_and_enforce_producer_ownership() {
    let root = temp_root("attachment-producers");
    let node = NodeId::from_legacy_named("attachment-producers").unwrap();
    let (mut runtime, mut dfs) = native_attachment_service(
        "attachments_compact_batch_and_enforce_producer_ownership",
        node.clone(),
    );
    let stat = dfs
        .posix_write_file("/users/source", b"source", options(&node))
        .unwrap();
    let source = source(&stat);
    let before = runtime
        .node_metadata(&node)
        .unwrap()
        .metadata_summary()
        .unwrap()
        .object_count;
    let mut first = None;
    let mut checkpointed = 0;
    for i in 0..128 {
        let result = stored(
            dfs.file_attachment_as(
                "component:one",
                DfsAttachmentRequest::Put {
                    source: source.clone(),
                    metadata: metadata(None, &format!("item-{i}")),
                    expected: None,
                    operation_id: format!("create-{i}"),
                    bytes: vec![i as u8],
                },
                Some(options(&node)),
            )
            .unwrap(),
        );
        if i == 0 {
            first = Some(result);
        }
        if (i + 1) % 16 == 0 {
            // Retain every native RPC in verified windows while preserving
            // the fixed 4096-record live buffer and all 128 catalog writes.
            checkpointed += runtime.checkpoint_rpc_observation_window().unwrap();
        }
    }
    // One catalog and a bounded number of bucket directories, not 128 files.
    assert!(
        runtime
            .node_metadata(&node)
            .unwrap()
            .metadata_summary()
            .unwrap()
            .object_count
            - before
            < 10
    );
    let first = first.unwrap();
    let response = dfs
        .file_attachment(
            DfsAttachmentRequest::GetMany {
                source: source.clone(),
                keys: vec![first.metadata.key.clone(), metadata(None, "missing").key],
            },
            None,
        )
        .unwrap();
    assert!(
        matches!(response, DfsAttachmentResponse::Descriptors { attachments } if attachments == vec![Some(first.clone()), None])
    );
    let mut spoofed_metadata = first.metadata.clone();
    // Producer is an extractor fingerprint, never an authenticated identity.
    spoofed_metadata.producer = "component:one".into();
    let spoof = DfsAttachmentRequest::Put {
        source: source.clone(),
        metadata: spoofed_metadata,
        expected: Some(first.revision.clone()),
        operation_id: "spoof".into(),
        bytes: vec![9],
    };
    assert!(
        dfs.file_attachment_as("component:two", spoof, Some(options(&node)))
            .is_err()
    );
    // Deletion does not let a producer evade its retained catalog-slot budget.
    dfs.file_attachment_as(
        "component:one",
        DfsAttachmentRequest::Delete {
            source: source.clone(),
            key: first.metadata.key,
            expected: first.revision,
            operation_id: "delete".into(),
        },
        Some(options(&node)),
    )
    .unwrap();
    let extra = DfsAttachmentRequest::Put {
        source: source.clone(),
        metadata: metadata(None, "extra"),
        expected: None,
        operation_id: "extra".into(),
        bytes: vec![0],
    };
    assert!(
        dfs.file_attachment_as("component:one", extra.clone(), Some(options(&node)))
            .is_err()
    );
    dfs.file_attachment_as("component:two", extra, Some(options(&node)))
        .unwrap();
    checkpointed += runtime.checkpoint_rpc_observation_window().unwrap();
    assert!(
        checkpointed > 4096,
        "the full workload exceeds one capture window"
    );
    assert_eq!(runtime.observation_dropped_count(), 0);
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn attachments_cleanup_preserves_live_payloads_and_pinned_sources() {
    let root = temp_root("attachment-cleanup");
    let node = NodeId::from_legacy_named("attachment-cleanup").unwrap();
    let (mut runtime, mut dfs) = native_attachment_service(
        "attachments_cleanup_preserves_live_payloads_and_pinned_sources",
        node.clone(),
    );
    for pinned in [false, true] {
        let stat = dfs
            .posix_write_file(
                &format!("/users/source-{pinned}"),
                b"source",
                options(&node),
            )
            .unwrap();
        let source = source(&stat);
        let bytes = vec![42; DFS_ATTACHMENT_INLINE_BYTES + 1024];
        let attachment = stored(
            dfs.file_attachment(
                DfsAttachmentRequest::Put {
                    source: source.clone(),
                    metadata: metadata(None, "large"),
                    expected: None,
                    operation_id: format!("put-{pinned}"),
                    bytes: bytes.clone(),
                },
                Some(options(&node)),
            )
            .unwrap(),
        );
        let directory = dfs_attachment_catalog_directory(&source);
        let orphan = format!("{directory}/payload-{}", sinomo_core::Id::generate());
        dfs.posix_write_file(&orphan, b"abandoned upload", options(&node))
            .unwrap();
        let (_, _, reclaimed) = dfs.maintain_attachment_source(&source, None, 64).unwrap();
        assert_eq!(reclaimed, 1);
        assert!(dfs.posix_stat_distributed(&orphan).is_err());
        assert!(
            matches!(dfs.file_attachment(DfsAttachmentRequest::Read { source: source.clone(), key: attachment.metadata.key, revision: attachment.revision, offset: 0, max_bytes: DFS_ATTACHMENT_MAX_RANGE_BYTES }, None).unwrap(), DfsAttachmentResponse::Data { bytes: read, eof: true } if read == bytes)
        );
        if pinned {
            dfs.retain_content_revision(DfsContentReferenceRequest {
                namespace: "org.example.attachment-retention".into(),
                path: source.path.clone(),
                identity: DfsFileVersionIdentity {
                    file_system_id: source.file_system_id.clone(),
                    object_id: source.object_id.clone(),
                    version: stat.version,
                },
                operation: DfsContentReferenceOperation::CaptureCurrent {
                    reference_id: "keep".into(),
                    metadata: None,
                },
                operation_id: "pin-source".into(),
            })
            .unwrap();
        }
        dfs.posix_unlink(&source.path).unwrap();
        let (_, _, reclaimed) = dfs.maintain_attachment_source(&source, None, 64).unwrap();
        if pinned {
            assert_eq!(reclaimed, 0);
            assert!(
                dfs.posix_stat_distributed(&format!("{directory}/catalog.sobj"))
                    .is_ok()
            );
        } else {
            assert!(reclaimed >= 2);
            assert!(dfs.posix_stat_distributed(&directory).is_err());
        }
    }
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn attachment_cleanup_parks_until_committed_namespace_notification() {
    let root = temp_root("attachment-cleanup-missed-notification");
    let node = NodeId::from_legacy_named("attachment-cleanup-missed-notification").unwrap();
    let (mut runtime, mut dfs) = native_attachment_service(
        "attachment_cleanup_parks_until_committed_namespace_notification",
        node.clone(),
    );
    let stat = dfs
        .posix_write_file("/users/source", b"source", options(&node))
        .unwrap();
    let source = source(&stat);
    dfs.file_attachment(
        DfsAttachmentRequest::Put {
            source: source.clone(),
            metadata: metadata(None, "small"),
            expected: None,
            operation_id: "put-before-missed-notification".into(),
            bytes: vec![42],
        },
        Some(options(&node)),
    )
    .unwrap();
    let data_dir = runtime.data_dir(&node).clone();
    let shards = runtime.metadata_shards().unwrap();
    let checkpoint = crate::attachment_maintenance::checkpoint_path(&data_dir);
    let catalog = format!("{}/catalog.sobj", dfs_attachment_catalog_directory(&source));
    let start = crate::dfs_now_unix_nanos();
    // Establish the feed cursor and finish the initial managed-root scan.
    for step in 0..4 {
        runtime
            .with_main_maintenance_service(&node, |main| {
                main.maintain_file_attachments_bounded_at(
                    &checkpoint,
                    &shards,
                    8,
                    start + step * 30_000_000_000,
                )
            })
            .unwrap()
            .unwrap();
    }
    let before = std::fs::read(&checkpoint).unwrap();
    runtime
        .with_main_maintenance_service(&node, |main| {
            main.maintain_file_attachments_bounded_at(
                &checkpoint,
                &shards,
                8,
                start + 120_000_000_000,
            )
        })
        .unwrap()
        .unwrap();
    assert_eq!(
        std::fs::read(&checkpoint).unwrap(),
        before,
        "an idle worker has no scheduled job or checkpoint write"
    );

    // A sibling handle commits the deletion, leaving this worker's cached
    // prefix proof stale, just like the daemon's separate message lane.
    assert_eq!(
        crate::attachment_maintenance::deadline(&data_dir).unwrap(),
        None
    );
    let mut writer = runtime.client(&node).unwrap();
    let tracker = crate::DfsCommittedWorkTracker::new();
    writer.set_committed_work_tracker(tracker.clone());
    writer.posix_unlink(&source.path).unwrap();
    drop(writer); // The committed edge outlives the response and service owner.
    assert!(tracker.take().semantic_changes);
    crate::attachment_maintenance::notify(&data_dir);
    runtime
        .with_main_maintenance_service(&node, |main| {
            main.maintain_file_attachments_bounded_at(
                &checkpoint,
                &shards,
                8,
                start + 150_000_000_000,
            )
        })
        .unwrap()
        .unwrap();
    assert!(matches!(
        dfs.posix_stat_distributed(&catalog),
        Err(DfsPosixError::NotFound { .. })
    ));
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn attachments_stream_validation_rejects_bad_lengths_and_transcripts() {
    let root = temp_root("attachment-stream-validation");
    let node = NodeId::from_legacy_named("attachment-stream-validation").unwrap();
    let (mut runtime, mut dfs) = native_attachment_service(
        "attachments_stream_validation_rejects_bad_lengths_and_transcripts",
        node.clone(),
    );
    let stat = dfs
        .posix_write_file("/users/source", b"source", options(&node))
        .unwrap();
    let source = source(&stat);
    let upload = DfsAttachmentUploadRequest {
        generation: false,
        source: source.clone(),
        metadata: metadata(None, "data"),
        expected: None,
        operation_id: "stream".into(),
        payload_bytes: 4,
    };
    assert!(
        dfs.file_attachment_from_reader(upload, std::io::Cursor::new(b"too long"), options(&node))
            .is_err()
    );
    let meta = DfsAttachmentMetadata {
        key: DfsAttachmentKey::transcript("en"),
        schema: "sinomo.transcript/v1".into(),
        media_type: "application/x-ndjson".into(),
        source_version: Some(stat.version),
        producer: "test".into(),
    };
    let bad = b"{\"start_millis\":100,\"end_millis\":0,\"text\":\"bad\"}\n";
    assert!(
        dfs.file_attachment_from_reader(
            DfsAttachmentUploadRequest {
                generation: false,
                source,
                metadata: meta,
                expected: None,
                operation_id: "transcript".into(),
                payload_bytes: bad.len() as u64
            },
            std::io::Cursor::new(bad),
            options(&node)
        )
        .is_err()
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn attachments_generation_claims_deduplicate_and_fence_publication() {
    let root = temp_root("attachment-generation");
    let node = NodeId::from_legacy_named("attachment-generation").unwrap();
    let (mut runtime, mut dfs) = native_attachment_service(
        "attachments_generation_claims_deduplicate_and_fence_publication",
        node.clone(),
    );
    let stat = dfs
        .posix_write_file("/users/source", b"source", options(&node))
        .unwrap();
    let source = source(&stat);
    let mut upload = DfsAttachmentUploadRequest {
        source: source.clone(),
        metadata: metadata(Some(stat.version), "derived"),
        expected: None,
        operation_id: "generation-one".into(),
        payload_bytes: 4096,
        generation: true,
    };
    let claim = DfsAttachmentRequest::ClaimGeneration {
        upload: upload.clone(),
    };
    let granted = dfs
        .file_attachment_as("component:one", claim.clone(), Some(options(&node)))
        .unwrap();
    assert!(matches!(
        granted,
        DfsAttachmentResponse::Generation { granted: true, .. }
    ));
    drop(dfs);
    runtime.stop_node(&node).unwrap();
    runtime.reopen_node(node.clone()).unwrap();
    let mut dfs = runtime.client(&node).unwrap();
    assert_eq!(
        dfs.file_attachment_as("component:one", claim.clone(), Some(options(&node)))
            .unwrap(),
        granted
    );
    assert!(matches!(
        dfs.file_attachment_as("component:two", claim, Some(options(&node)))
            .unwrap(),
        DfsAttachmentResponse::Generation { granted: false, .. }
    ));
    let mut competing = upload.clone();
    competing.operation_id = "generation-two".into();
    assert!(matches!(
        dfs.file_attachment_as(
            "component:one",
            DfsAttachmentRequest::ClaimGeneration { upload: competing },
            Some(options(&node))
        )
        .unwrap(),
        DfsAttachmentResponse::Generation { granted: false, .. }
    ));
    let mut oversized = upload.clone();
    oversized.payload_bytes = 4097;
    assert!(
        dfs.file_attachment_from_reader_as(
            "component:one",
            oversized,
            std::io::Cursor::new(vec![0; 4097]),
            options(&node)
        )
        .is_err()
    );
    upload.payload_bytes = 2048;
    assert!(
        dfs.file_attachment_from_reader_as(
            "component:two",
            upload.clone(),
            std::io::Cursor::new(vec![0; 2048]),
            options(&node)
        )
        .is_err()
    );
    let first = stored(
        dfs.file_attachment_from_reader_as(
            "component:one",
            upload.clone(),
            std::io::Cursor::new(vec![0; 2048]),
            options(&node),
        )
        .unwrap(),
    );
    assert_eq!(
        stored(
            dfs.file_attachment_from_reader_as(
                "component:one",
                upload.clone(),
                std::io::Cursor::new(vec![0; 2048]),
                options(&node)
            )
            .unwrap()
        ),
        first
    );
    assert!(matches!(
        dfs.file_attachment_as(
            "component:one",
            DfsAttachmentRequest::ClaimGeneration {
                upload: upload.clone()
            },
            Some(options(&node))
        )
        .unwrap(),
        DfsAttachmentResponse::Descriptor {
            attachment: Some(_)
        }
    ));
    // A claim invalidated by a source edit cannot subsequently publish.
    upload.metadata.key.name = "other".into();
    upload.operation_id = "generation-stale".into();
    dfs.file_attachment_as(
        "component:one",
        DfsAttachmentRequest::ClaimGeneration {
            upload: upload.clone(),
        },
        Some(options(&node)),
    )
    .unwrap();
    dfs.posix_write_file(&source.path, b"changed", options(&node))
        .unwrap();
    dfs.maintain_attachment_source(&source, None, 64).unwrap();
    assert!(
        dfs.file_attachment_from_reader_as(
            "component:one",
            upload,
            std::io::Cursor::new(vec![0; 2048]),
            options(&node)
        )
        .is_err()
    );
    // Another authenticated node can regenerate the stale preview without
    // transferring its storage ownership or requiring the original node.
    let changed = dfs.posix_stat_distributed(&source.path).unwrap();
    let refresh = DfsAttachmentUploadRequest {
        source: source.clone(),
        metadata: metadata(Some(changed.version), "derived"),
        expected: Some(first.revision),
        operation_id: "other-node-generation".into(),
        payload_bytes: 128,
        generation: true,
    };
    assert!(matches!(
        dfs.file_attachment_as(
            "node:other",
            DfsAttachmentRequest::ClaimGeneration {
                upload: refresh.clone()
            },
            Some(options(&node))
        )
        .unwrap(),
        DfsAttachmentResponse::Generation { granted: true, .. }
    ));
    dfs.file_attachment_from_reader_as(
        "node:other",
        refresh,
        std::io::Cursor::new(vec![1; 128]),
        options(&node),
    )
    .unwrap();
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn small_attachment_spill_keeps_catalog_payload_bounded() {
    let root = temp_root("attachment-inline-budget");
    let node = NodeId::from_legacy_named("attachment-inline-budget").unwrap();
    let (mut runtime, mut dfs) = native_attachment_service(
        "small_attachment_spill_keeps_catalog_payload_bounded",
        node.clone(),
    );
    let stat = dfs
        .posix_write_file("/users/source", b"source", options(&node))
        .unwrap();
    let source = source(&stat);
    let inline_slots = DFS_ATTACHMENT_INLINE_BUDGET_BYTES / DFS_ATTACHMENT_INLINE_BYTES;
    let mut last = None;
    let mut checkpointed = 0;
    for i in 0..inline_slots + 3 {
        last = Some(stored(
            dfs.file_attachment(
                DfsAttachmentRequest::Put {
                    source: source.clone(),
                    metadata: metadata(None, &format!("item-{i}")),
                    expected: None,
                    operation_id: format!("spill-{i}"),
                    bytes: vec![i as u8; DFS_ATTACHMENT_INLINE_BYTES],
                },
                Some(options(&node)),
            )
            .unwrap(),
        ));
        // Preserve each completed catalog mutation before the next bounded capture window.
        checkpointed += runtime.checkpoint_rpc_observation_window().unwrap();
    }
    let entries = dfs
        .posix_list_dir_distributed(&dfs_attachment_catalog_directory(&source))
        .unwrap();
    assert_eq!(
        entries
            .iter()
            .filter(|e| e.name.starts_with("payload-"))
            .count(),
        3
    );
    let last = last.unwrap();
    assert!(
        matches!(dfs.file_attachment(DfsAttachmentRequest::Read { source, key: last.metadata.key, revision: last.revision, offset: 0, max_bytes: DFS_ATTACHMENT_MAX_RANGE_BYTES }, None).unwrap(), DfsAttachmentResponse::Data { bytes, eof: true } if bytes == vec![(inline_slots+2) as u8; DFS_ATTACHMENT_INLINE_BYTES])
    );
    checkpointed += runtime.checkpoint_rpc_observation_window().unwrap();
    assert!(
        checkpointed > 4096,
        "the full spill workload spans capture windows"
    );
    assert_eq!(runtime.observation_dropped_count(), 0);
    drop(dfs);
    runtime.close().unwrap();
}

#[derive(Debug)]
struct UnavailableControlPlane;

impl DfsControlPlaneKvClient for UnavailableControlPlane {
    fn get(&self, _key: &str) -> Result<Option<KvEntry>, KvError> {
        Err(KvError::AuthorityUnavailable {
            detail: "test authority unavailable".into(),
        })
    }

    fn range_prefix(&self, _prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        Err(KvError::AuthorityUnavailable {
            detail: "test authority unavailable".into(),
        })
    }

    fn put(&mut self, _key: String, _value: Vec<u8>) -> Result<KvRevision, KvError> {
        panic!("attachment lifecycle inspection must not mutate cluster configuration")
    }

    fn txn(&mut self, _txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        panic!("attachment lifecycle inspection must not mutate cluster configuration")
    }
}

#[test]
fn unavailable_lifecycle_authority_defers_attachment_reclamation() {
    let root = temp_root("unavailable-lifecycle-authority");
    let node = NodeId::from_legacy_named("unavailable-lifecycle-authority").unwrap();
    let (mut runtime, mut dfs) = native_attachment_service(
        "unavailable_lifecycle_authority_defers_attachment_reclamation",
        node.clone(),
    );
    let stat = dfs
        .posix_write_file("/users/source", b"source", options(&node))
        .unwrap();
    let source = source(&stat);
    dfs.file_attachment(
        DfsAttachmentRequest::Put {
            source: source.clone(),
            metadata: metadata(None, "large"),
            expected: None,
            operation_id: "unavailable-authority-payload".into(),
            bytes: vec![42; DFS_ATTACHMENT_INLINE_BYTES + 1024],
        },
        Some(options(&node)),
    )
    .unwrap();
    let directory = dfs_attachment_catalog_directory(&source);
    let catalog = format!("{directory}/catalog.sobj");
    let before = dfs
        .posix_list_dir_beneath_no_links_distributed_page("/", &directory, None, 64)
        .unwrap()
        .1;
    dfs.posix_unlink(&source.path).unwrap();
    let mut dfs = dfs.with_control_plane(UnavailableControlPlane);
    assert!(dfs.maintain_attachment_source(&source, None, 64).is_err());
    drop(dfs);
    runtime.stop_node(&node).unwrap();
    runtime.reopen_node(node.clone()).unwrap();
    let mut dfs = runtime.client(&node).unwrap();
    assert!(dfs.posix_stat_distributed(&catalog).is_ok());
    let after = dfs
        .posix_list_dir_beneath_no_links_distributed_page("/", &directory, None, 64)
        .unwrap()
        .1;
    assert_eq!(
        before.entries, after.entries,
        "missing authority cannot erase payloads or catalog generations"
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn absent_object_identity_does_not_authorize_attachment_reclamation() {
    let root = temp_root("absent-lifecycle-identity");
    let node = NodeId::from_legacy_named("absent-lifecycle-identity").unwrap();
    let (mut runtime, mut dfs) = native_attachment_service(
        "absent_object_identity_does_not_authorize_attachment_reclamation",
        node.clone(),
    );
    let unknown = DfsObjectId::from_legacy_named("absent-source-object").unwrap();
    let lifecycle = dfs
        .object_lifecycle(&sifs_file_system_id(), &unknown)
        .unwrap();
    assert_eq!(lifecycle.object, None);
    assert!(
        !lifecycle.reclaimable,
        "missing metadata is not source deletion evidence"
    );
    drop(dfs);
    runtime.close().unwrap();
}

#[test]
fn last_generic_source_reference_release_wakes_catalog_cleanup_from_durable_feed() {
    let root = temp_root("last-reference-release");
    let node = NodeId::from_legacy_named("last-reference-release").unwrap();
    let (mut runtime, mut dfs) = native_attachment_service(
        "last_generic_source_reference_release_wakes_catalog_cleanup_from_durable_feed",
        node.clone(),
    );
    let stat = dfs
        .posix_write_file("/users/source", b"source", options(&node))
        .unwrap();
    let source = source(&stat);
    let reference = DfsContentReferenceRequest {
        namespace: "org.example.attachment-retention".into(),
        path: source.path.clone(),
        identity: DfsRevisionReference {
            file_system_id: source.file_system_id.clone(),
            object_id: source.object_id.clone(),
            version: stat.version,
        },
        operation: DfsContentReferenceOperation::CaptureCurrent {
            reference_id: "keep-source".into(),
            metadata: None,
        },
        operation_id: "capture-before-deletion".into(),
    };
    dfs.retain_content_revision(reference.clone()).unwrap();
    dfs.file_attachment(
        DfsAttachmentRequest::Put {
            source: source.clone(),
            metadata: metadata(None, "small"),
            expected: None,
            operation_id: "attachment-before-deletion".into(),
            bytes: vec![42],
        },
        Some(options(&node)),
    )
    .unwrap();
    let data = runtime.data_dir(&node).clone();
    let shards = runtime.metadata_shards().unwrap();
    let checkpoint = crate::attachment_maintenance::checkpoint_path(&data);
    let catalog = format!("{}/catalog.sobj", dfs_attachment_catalog_directory(&source));
    let start = crate::dfs_now_unix_nanos();
    for step in 0..4 {
        runtime
            .with_main_maintenance_service(&node, |main| {
                main.maintain_file_attachments_bounded_at(
                    &checkpoint,
                    &shards,
                    8,
                    start + step * 30_000_000_000,
                )
            })
            .unwrap()
            .unwrap();
    }
    dfs.posix_unlink(&source.path).unwrap();
    crate::attachment_maintenance::notify(&data);
    runtime
        .with_main_maintenance_service(&node, |main| {
            main.maintain_file_attachments_bounded_at(
                &checkpoint,
                &shards,
                8,
                start + 150_000_000_000,
            )
        })
        .unwrap()
        .unwrap();
    assert!(dfs.posix_stat_distributed(&catalog).is_ok());
    assert_eq!(
        crate::attachment_maintenance::deadline(&data).unwrap(),
        None
    );

    dfs.retain_content_revision(DfsContentReferenceRequest {
        operation: DfsContentReferenceOperation::Release {
            reference_id: "keep-source".into(),
        },
        operation_id: "release-last-source-reference".into(),
        ..reference
    })
    .unwrap();
    // The event is only a wake: the durable V6 revision change supplies the
    // source identity, and the maintenance task rechecks canonical lifecycle.
    crate::attachment_maintenance::notify(&data);
    runtime
        .with_main_maintenance_service(&node, |main| {
            main.maintain_file_attachments_bounded_at(
                &checkpoint,
                &shards,
                8,
                start + 180_000_000_000,
            )
        })
        .unwrap()
        .unwrap();
    assert!(matches!(
        dfs.posix_stat_distributed(&catalog),
        Err(DfsPosixError::NotFound { .. })
    ));
    drop(dfs);
    runtime.close().unwrap();
}
