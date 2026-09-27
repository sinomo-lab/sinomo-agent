use super::*;

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
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
    let mut sources = Vec::new();
    for i in 0..files {
        sources.push(source(
            &dfs.posix_write_file(&format!("/users/bench-{i}"), b"source", options(&node))
                .unwrap(),
        ));
    }
    let before_bytes = disk_bytes(&root);
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
    let after_bytes = disk_bytes(&root);
    let object_count = dfs
        .metadata_backend()
        .unwrap()
        .metadata_summary()
        .unwrap()
        .object_count;
    drop(dfs);
    let opened = Instant::now();
    let mut dfs = networked_sifs_with_backend(&root, node);
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
}

#[test]
fn attachments_persist_ranges_cas_and_source_fences() {
    let root = temp_root("attachments");
    let node = NodeId::from_legacy_named("attachments-node").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
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
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
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
}

#[test]
fn attachment_pages_follow_identity_without_full_directory_collection() {
    let root = temp_root("attachment-pages");
    let node = NodeId::from_legacy_named("attachment-pages-node").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
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
    // Populate a large unrelated namespace directly as a test fixture. The
    // queried file has only 17 attachments regardless of global object count.
    let template = dfs
        .metadata_backend()
        .unwrap()
        .get_object(&stat.object_id)
        .unwrap()
        .unwrap();
    dfs.add_object_metadata_batch((0..10_000).map(|i| {
        let mut object = template.clone();
        object.object_id = DfsObjectId::from_legacy_named(&format!("unrelated-{i}")).unwrap();
        object.path = format!("users/unrelated-{i}");
        object
    }))
    .unwrap();
    dfs.posix_rename(&source.path, "/users/renamed").unwrap();
    source.path = "/users/renamed".into();
    crate::posix::reset_posix_metadata_full_directory_collect_count();
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
    assert_eq!(
        crate::posix::posix_metadata_full_directory_collect_count(),
        0
    );
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
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
    let stat = dfs
        .posix_write_file("/users/source", b"source", options(&node))
        .unwrap();
    let source = source(&stat);
    let before = dfs
        .metadata_backend()
        .unwrap()
        .metadata_summary()
        .unwrap()
        .object_count;
    let mut first = None;
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
    }
    // One catalog and a bounded number of bucket directories, not 128 files.
    assert!(
        dfs.metadata_backend()
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
    let spoof = DfsAttachmentRequest::Put {
        source: source.clone(),
        metadata: first.metadata.clone(),
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
}

#[test]
fn attachments_cleanup_preserves_live_payloads_and_pinned_sources() {
    let root = temp_root("attachment-cleanup");
    let node = NodeId::from_legacy_named("attachment-cleanup").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
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
            dfs.file_history_pin(DfsFileHistoryPinRequest {
                path: source.path.clone(),
                identity: DfsFileVersionIdentity {
                    file_system_id: source.file_system_id.clone(),
                    object_id: source.object_id.clone(),
                    version: stat.version,
                },
                operation: DfsFileHistoryPinOperation::Capture {
                    pin_id: "keep".into(),
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
}

#[test]
fn attachment_cleanup_polls_missed_changes_without_idle_checkpoint_writes() {
    let root = temp_root("attachment-cleanup-missed-notification");
    let node = NodeId::from_legacy_named("attachment-cleanup-missed-notification").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
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
    let checkpoint = root.join("attachment-cleanup.sobj");
    let catalog = format!("{}/catalog.sobj", dfs_attachment_catalog_directory(&source));
    let start = crate::dfs_now_unix_nanos();
    // Establish the feed cursor and finish the initial managed-root scan.
    for step in 0..4 {
        dfs.maintain_file_attachments_bounded_at(
            &checkpoint,
            &[],
            8,
            start + step * 30_000_000_000,
        )
        .unwrap();
    }
    let before = std::fs::read(&checkpoint).unwrap();
    dfs.maintain_file_attachments_bounded_at(&checkpoint, &[], 8, start + 120_000_000_000)
        .unwrap();
    assert_eq!(
        std::fs::read(&checkpoint).unwrap(),
        before,
        "an idle journal check must not rewrite the checkpoint just to move its timer"
    );

    // A sibling handle commits the deletion, leaving this worker's cached
    // prefix proof stale, just like the daemon's separate message lane.
    // The core mutation emits no browser/data-plane wake hint in this fixture.
    // The next timer must consume its durable feed entry, without an hourly
    // directory rescan or a foreground attachment read to drive cleanup.
    let mut writer = networked_sifs_with_backend(&root, node);
    writer.posix_unlink(&source.path).unwrap();
    dfs.maintain_file_attachments_bounded_at(&checkpoint, &[], 8, start + 150_000_000_000)
        .unwrap();
    assert!(matches!(
        dfs.posix_stat_distributed(&catalog),
        Err(DfsPosixError::NotFound { .. })
    ));
}

#[test]
fn attachments_stream_validation_rejects_bad_lengths_and_transcripts() {
    let root = temp_root("attachment-stream-validation");
    let node = NodeId::from_legacy_named("attachment-stream-validation").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
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
}

#[test]
fn attachments_generation_claims_deduplicate_and_fence_publication() {
    let root = temp_root("attachment-generation");
    let node = NodeId::from_legacy_named("attachment-generation").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
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
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
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
}

#[test]
fn small_attachment_spill_keeps_catalog_payload_bounded() {
    let root = temp_root("attachment-inline-budget");
    let node = NodeId::from_legacy_named("attachment-inline-budget").unwrap();
    let mut dfs = networked_sifs_with_backend(&root, node.clone());
    let stat = dfs
        .posix_write_file("/users/source", b"source", options(&node))
        .unwrap();
    let source = source(&stat);
    let inline_slots = DFS_ATTACHMENT_INLINE_BUDGET_BYTES / DFS_ATTACHMENT_INLINE_BYTES;
    let mut last = None;
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
    }
    let entries = dfs
        .posix_list_dir(&dfs_attachment_catalog_directory(&source))
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
}
