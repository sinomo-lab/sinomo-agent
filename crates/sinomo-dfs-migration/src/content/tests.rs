use super::incremental::ContentConverter;
use super::*;
use std::collections::BTreeMap;

#[derive(Serialize)]
struct PackedSnapshot {
    version: u32,
    next_segment_id: u64,
    entries: BTreeMap<String, PackedLocation>,
}
#[derive(Serialize)]
struct PackedLocation {
    segment_id: String,
    offset: u64,
    size_bytes: u64,
}
#[derive(Serialize)]
struct PackedLog {
    version: u32,
    content_hash: String,
    location: Option<PackedLocation>,
    next_segment_id: u64,
}
fn source(root: &Path, bytes: &[u8], packed: bool) -> ContentManifest {
    let hash = format!("blake3:{}", blake3::hash(bytes).to_hex());
    let manifest = ContentManifest {
        object_id: "object".into(),
        version: "v1".into(),
        content_hash: hash.clone(),
        size_bytes: bytes.len() as u64,
        extents: Vec::new(),
    };
    if packed {
        let path = root.join("blob-packs/segments/0000000000000000.pack");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            [b"unused".as_slice(), bytes, b"physical unused tail"].concat(),
        )
        .unwrap();
        write(
            &root.join("blob-packs/index.sobj"),
            &PackedSnapshot {
                version: 1,
                next_segment_id: 1,
                entries: BTreeMap::from([(
                    hash,
                    PackedLocation {
                        segment_id: "0000000000000000.pack".into(),
                        offset: 6,
                        size_bytes: bytes.len() as u64,
                    },
                )]),
            },
        )
        .unwrap();
    } else {
        let hex = hash.strip_prefix("blake3:").unwrap();
        let path = root.join("blobs").join(&hex[..2]).join(hex);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }
    write(&root.join("objects/object/v1/manifest.sobj"), &manifest).unwrap();
    manifest
}
fn finish(converter: &ContentConverter, root: &Path, budget: usize) {
    for _ in 0..1000 {
        match converter.prepare(root, budget).unwrap() {
            DfsStoragePreparation::Ready => return,
            DfsStoragePreparation::Pending { processed_records } => {
                assert!(processed_records <= budget)
            }
        }
    }
    panic!("converter failed to terminate");
}
#[test]
fn real_loose_and_packed_bytes_convert_with_one_record_budget_and_native_reopen() {
    for packed in [false, true] {
        let root = sinomo_testdir::TestDir::new("content-conversion-bounded");
        let bytes = b"retained actual bytes";
        let manifest = source(root.path(), bytes, packed);
        assert!(LocalContentStore::new(root.path()).is_err());
        let converter = ContentConverter::default();
        finish(&converter, root.path(), 1);
        let store = LocalContentStore::new(root.path()).unwrap();
        assert_eq!(store.read_manifest("object", "v1").unwrap(), manifest);
        assert_eq!(store.read_object_version("object", "v1").unwrap().1, bytes);
        assert!(!root.join("objects/object/v1/manifest.sobj").exists());
        assert!(store.scrub().unwrap().is_healthy());
        drop(store);
        drop(converter);
        finish(&ContentConverter::default(), root.path(), 1);
        assert_eq!(
            LocalContentStore::new(root.path())
                .unwrap()
                .read_object_version("object", "v1")
                .unwrap()
                .1,
            bytes
        );
    }
}
#[test]
fn retained_packed_partial_index_tail_does_not_import_a_missing_record() {
    let root = sinomo_testdir::TestDir::new("content-conversion-packed-tail");
    let bytes = b"committed packed prefix";
    source(root.path(), bytes, true);
    let record = PackedLog {
        version: 1,
        content_hash: format!("blake3:{}", blake3::hash(b"missing tail").to_hex()),
        location: None,
        next_segment_id: 1,
    };
    let encoded = serde_ext::to_bytes(&record, 1, LengthMode::U32).unwrap();
    let mut tail = (encoded.len() as u32).to_le_bytes().to_vec();
    tail.extend_from_slice(&encoded[..encoded.len() / 2]);
    fs::write(root.join("blob-packs/index.slog"), tail).unwrap();
    finish(&ContentConverter::default(), root.path(), 1);
    assert_eq!(
        LocalContentStore::new(root.path())
            .unwrap()
            .read_object_version("object", "v1")
            .unwrap()
            .1,
        bytes
    );
}
#[test]
fn candidate_publication_resumes_each_real_filesystem_prefix_before_current_marker() {
    use super::incremental::{Phase, Progress};
    for publication_step in [0, 1, 2, 3] {
        let root = sinomo_testdir::TestDir::new("content-conversion-publication-prefix");
        let bytes = b"verified source remains reachable";
        source(root.path(), bytes, false);
        let converter = ContentConverter::default();
        // Park naturally after the bounded payload install phase, then model a
        // process crash at the actual durable rename/marker boundaries.
        for _ in 0..1000 {
            let _ = converter.prepare(root.path(), 1).unwrap();
            let path = root.join(CONVERSION).join("progress.sobj");
            let progress: Progress = read(&path).unwrap();
            if progress.phase == Phase::Install {
                break;
            }
        }
        drop(converter);
        let work = root.join(CONVERSION);
        let target = work.join("store");
        let hex = blake3::hash(bytes).to_hex().to_string();
        let destination = root.join("blobs").join(&hex[..2]).join(&hex);
        assert!(destination.exists()); // original loose source is durable.
        let progress_path = work.join("progress.sobj");
        let mut progress: Progress = read(&progress_path).unwrap();
        progress.phase = Phase::Publish;
        progress.publication_step = publication_step;
        // A v1 inline directory is optional for this real file-layout source.
        if publication_step >= 2 {
            fs::rename(target.join("inline-bindings"), root.join("inline-bindings")).unwrap();
        }
        if publication_step == 3 {
            let marker = fs::read(target.join(MARKER)).unwrap();
            durable_file::write_file_atomic(
                &root.join(MARKER),
                &marker,
                durable_file::DurableAtomicFileOptions::new("test", "tmp").sync_all(),
            )
            .unwrap();
        }
        write(&progress_path, &progress).unwrap();
        if publication_step != 3 {
            assert!(LocalContentStore::new(root.path()).is_err());
        }
        finish(&ContentConverter::default(), root.path(), 1);
        assert_eq!(
            LocalContentStore::new(root.path())
                .unwrap()
                .read_object_version("object", "v1")
                .unwrap()
                .1,
            bytes
        );
    }
}

#[test]
fn current_content_retires_old_gc_hints_with_bounded_restart_and_exact_archive() {
    let root = sinomo_testdir::TestDir::new("content-old-gc-hints-current-root");
    let bytes = b"current content and retention remain unchanged";
    let store = LocalContentStore::new(root.path()).unwrap();
    let mut ingest = store
        .begin_object_version_ingest("object", "v1", bytes.len() as u64, None)
        .unwrap();
    ingest.write_chunk(bytes).unwrap();
    let manifest = ingest.finish().unwrap();
    let retention = store.content_reservation_roots().unwrap();
    drop(store);
    let old_orphan = root.join("orphan-observations/aa/old.sobj");
    let old_absence = root.join("manifest-absence-observations/bb/old.sobj");
    let current_hint = root.join("orphan-observations/cc/current.sobj");
    let torn_hint = root.join("orphan-observations/dd/torn.sobj");
    write(
        &old_orphan,
        &serde_json::json!({
            "format_version": 1, "content_hash": manifest.content_hash,
            "first_observed_at_unix_nanos": 1
        }),
    )
    .unwrap();
    write(
        &old_absence,
        &serde_json::json!({
            "format_version": 1, "object_id": "object", "version": "v1",
            "content_hash": manifest.content_hash, "first_observed_at_unix_nanos": 1
        }),
    )
    .unwrap();
    write(
        &current_hint,
        &serde_json::json!({
            "format_version": 2, "content_hash": manifest.content_hash,
            "first_observed_at_unix_nanos": 2, "catalog_generation": "current-cut",
            "catalog_observed_at_unix_nanos": 3
        }),
    )
    .unwrap();
    fs::create_dir_all(torn_hint.parent().unwrap()).unwrap();
    fs::write(&torn_hint, b"incomplete-current-observation").unwrap();
    let originals: Vec<_> = [&old_orphan, &old_absence]
        .into_iter()
        .map(|path| (path.to_owned(), fs::read(path).unwrap()))
        .collect();
    let current_bytes = fs::read(&current_hint).unwrap();
    let torn_bytes = fs::read(&torn_hint).unwrap();
    // Recreate the component-owned converter after every bounded durable step.
    for _ in 0..100 {
        match ContentConverter::default().prepare(root.path(), 1).unwrap() {
            DfsStoragePreparation::Ready => break,
            DfsStoragePreparation::Pending { processed_records } => assert!(processed_records <= 1),
        }
    }
    assert!(matches!(
        ContentConverter::default().prepare(root.path(), 1).unwrap(),
        DfsStoragePreparation::Ready
    ));
    for (path, original) in originals {
        assert!(!path.exists());
        assert_eq!(
            fs::read(
                root.join(CONVERSION)
                    .join("original-gc-hints")
                    .join(path.strip_prefix(root.path()).unwrap())
            )
            .unwrap(),
            original
        );
    }
    assert_eq!(fs::read(current_hint).unwrap(), current_bytes);
    assert_eq!(fs::read(torn_hint).unwrap(), torn_bytes);
    let reopened = LocalContentStore::new(root.path()).unwrap();
    assert_eq!(
        reopened.read_object_version("object", "v1").unwrap(),
        (manifest, bytes.to_vec())
    );
    assert_eq!(reopened.content_reservation_roots().unwrap(), retention);
}

#[test]
fn converted_content_archives_old_gc_hint_before_current_marker_publication() {
    let root = sinomo_testdir::TestDir::new("content-old-gc-hints-conversion");
    let bytes = b"retained source bytes";
    source(root.path(), bytes, false);
    let hint = root.join("orphan-observations/aa/old.sobj");
    write(&hint, &serde_json::json!({"format_version": 1, "content_hash": "old-hint", "first_observed_at_unix_nanos": 1})).unwrap();
    let original = fs::read(&hint).unwrap();
    finish(&ContentConverter::default(), root.path(), 1);
    assert!(!hint.exists());
    assert_eq!(
        fs::read(
            root.join(CONVERSION)
                .join("original-gc-hints/orphan-observations/aa/old.sobj")
        )
        .unwrap(),
        original
    );
    assert_eq!(
        LocalContentStore::new(root.path())
            .unwrap()
            .read_object_version("object", "v1")
            .unwrap()
            .1,
        bytes
    );
}

#[test]
fn current_content_retires_direct_and_old_proof_catalog_hints_without_accepting_torn_current() {
    for format in [
        "direct",
        "old-envelope-proof",
        "torn-current",
        "current-envelope",
    ] {
        let root = sinomo_testdir::TestDir::new(&format!("content-source-root-catalog-{format}"));
        let store = LocalContentStore::new(root.path()).unwrap();
        let manifest = store
            .put_object_version("object", "version", b"actual unchanged current bytes")
            .unwrap();
        drop(store);
        let path = root.join("catalog/metadata-content-roots.sobj");
        let catalog = serde_json::json!({"format_version":2,"topology_digest":"0".repeat(64),"generated_at_unix_nanos":1,"fragments":[],"incomplete_reasons":[]});
        match format {
            "direct" => write(&path, &catalog).unwrap(),
            "old-envelope-proof" => {
                let mut old = catalog.clone();
                old["fragments"] = serde_json::json!([{"coverage":{"complete":{"prefix_proof_version":1,"journal_sequence":1,"state_fingerprint":"retained old hint"}}}]);
                write(&path, &serde_json::json!({"storage_format_version":1,"catalog_json":serde_json::to_string(&old).unwrap()})).unwrap();
            }
            "current-envelope" => write(&path, &serde_json::json!({"storage_format_version":1,"catalog_json":serde_json::to_string(&catalog).unwrap()})).unwrap(),
            "torn-current" => {
                fs::create_dir_all(path.parent().unwrap()).unwrap();
                fs::write(&path, b"torn current catalog envelope").unwrap();
            }
            _ => unreachable!(),
        }
        let original = fs::read(&path).unwrap();
        finish(&ContentConverter::default(), root.path(), 1);
        let archive = root
            .join(CONVERSION)
            .join("original-gc-hints/catalog/metadata-content-roots.sobj");
        if matches!(format, "direct" | "old-envelope-proof") {
            assert!(!path.exists());
            assert_eq!(fs::read(archive).unwrap(), original);
        } else {
            assert_eq!(fs::read(path).unwrap(), original);
            assert!(!archive.exists());
        }
        let store = LocalContentStore::new(root.path()).unwrap();
        assert_eq!(store.read_manifest("object", "version").unwrap(), manifest);
        assert_eq!(
            store.read_object_version("object", "version").unwrap().1,
            b"actual unchanged current bytes"
        );
    }
}

#[test]
fn actual_native_provider_converts_legacy_content_without_recursive_metadata_preparation() {
    use sinomo_dfs::test_support::DfsTestRuntime;
    let name = "native-provider-content-dependency";
    let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
    let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
    let root = native.data_dir(&node).dfs_dir().join("content");
    let bytes = b"actual retained packed bytes before native mounting";
    let manifest = source(&root, bytes, true);
    let observed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = observed.clone();
    let builder: sinomo_dfs::DfsStoragePreparationProviderBuilder = std::sync::Arc::new(
        move |context, lifecycle| {
            let provider = crate::authority::storage_preparation_provider(context, lifecycle)?;
            let content = context
                .data_dir()
                .dfs_dir()
                .join("content")
                .canonicalize()
                .unwrap();
            assert!(
                matches!(
                    provider(&content, &sinomo_dfs::DfsStoragePreparationScope::Content, 1).unwrap(),
                    DfsStoragePreparation::Pending { processed_records } if processed_records <= 1
                ),
                "unfinished actual old content remains bounded Pending"
            );
            assert!(
                !context
                    .data_dir()
                    .dfs_dir()
                    .join("metadata/root/records/tables/snapshot.sobj")
                    .exists(),
                "Content preparation does not certify or initialize metadata"
            );
            let foreign = context.data_dir().dfs_dir().join("foreign-content");
            assert!(
                provider(
                    &foreign,
                    &sinomo_dfs::DfsStoragePreparationScope::Content,
                    1
                )
                .is_err(),
                "same provider cannot prepare a different content owner root"
            );
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(provider)
        },
    );
    native
        .configure_component_factories(vec![
            std::sync::Arc::new(
                sinomo_dfs::component_factory_with_storage_preparation(builder).unwrap(),
            ),
            std::sync::Arc::new(crate::authority::staged_import_component_factory().unwrap()),
        ])
        .unwrap();
    native.start_nodes().unwrap();
    assert_eq!(observed.load(std::sync::atomic::Ordering::SeqCst), 1);
    let client = native.client(&node).unwrap();
    assert_eq!(
        client.content().read_manifest("object", "v1").unwrap(),
        manifest
    );
    assert_eq!(
        client
            .content()
            .read_object_version("object", "v1")
            .unwrap()
            .1,
        bytes
    );
    assert_eq!(
        client.content().read_blob(&manifest.content_hash).unwrap(),
        bytes
    );
    assert!(current(&root).unwrap());
    assert_eq!(native.metadata_shards().unwrap().len(), 3);
    drop(client);
    native.close().unwrap();
}
