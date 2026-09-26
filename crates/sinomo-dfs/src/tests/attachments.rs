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
