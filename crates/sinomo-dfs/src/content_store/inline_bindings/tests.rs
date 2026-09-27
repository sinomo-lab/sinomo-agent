use super::*;
use crate::runtime_host::{DfsDataOwnerLock, DfsRuntimePaths};
use sinomo_core::local::SinomoDataDir;

pub(super) fn active_store(
    name: &str,
) -> (sinomo_testdir::TestDir, DfsDataOwnerLock, LocalContentStore) {
    let root = sinomo_testdir::TestDir::new(name);
    let data = SinomoDataDir::new(root.path());
    let mut owner = DfsDataOwnerLock::acquire(&DfsRuntimePaths::new(&data)).unwrap();
    owner.activate_content_bindings_before_routes().unwrap();
    let content = LocalContentStore::new(data.dfs_dir().join("content")).unwrap();
    (root, owner, content)
}

#[test]
fn inline_binding_recovers_exact_manifest_payload_and_reservation_without_legacy_indexes() {
    let (_root, _owner, store) = active_store("dfs-inline-exact-reopen");
    let manifest = store
        .put_object_version("object", "v1", b"complete durable binding")
        .unwrap();
    assert!(!store.manifest_path("object", "v1").exists());
    assert!(!store.blob_path(&manifest.content_hash).unwrap().exists());
    assert!(
        store
            .read_packed_blob_index_unlocked()
            .unwrap()
            .entries
            .is_empty()
    );
    let root = store.root.clone();
    drop(store); // lost reply/weak resident expiry; fresh owner must resync.
    let reopened = LocalContentStore::new(root).unwrap();
    assert_eq!(reopened.read_manifest("object", "v1").unwrap(), manifest);
    assert_eq!(
        reopened.read_object_version("object", "v1").unwrap().1,
        b"complete durable binding"
    );
    let roots = reopened.content_reservation_roots().unwrap();
    assert!(roots.content_hashes.contains(&manifest.content_hash));
    assert_eq!(
        reopened.manifests_for_object("object").unwrap(),
        vec![manifest]
    );
    assert!(reopened.scrub().unwrap().is_healthy());
    reopened
        .put_object_version("object", "v2", b"after recovery")
        .unwrap();
}

#[test]
fn cold_activation_rejects_live_legacy_reader_and_retries_on_existing_root() {
    let root = sinomo_testdir::TestDir::new("dfs-inline-cold-drain");
    let data = SinomoDataDir::new(root.path());
    let content_root = data.dfs_dir().join("content");
    let legacy = LocalContentStore::new(&content_root).unwrap();
    let old = legacy
        .put_object_version("old", "v1", b"legacy bytes")
        .unwrap();
    let mut reader = legacy.open_blob_stream(&old.content_hash).unwrap();
    drop(legacy);
    let mut owner = DfsDataOwnerLock::acquire(&DfsRuntimePaths::new(&data)).unwrap();
    let error = owner
        .activate_content_bindings_before_routes()
        .expect_err("a retained legacy lease must block activation");
    let expected = ContentStoreError::InlineActivationBusy {
        root: fs::canonicalize(&content_root).expect("created canonical content root"),
        live_leases: 1,
    };
    assert!(
        matches!(&error, crate::runtime_host::DfsRuntimeError::Parse { detail }
            if detail == &expected.to_string()),
        "unexpected activation refusal: {error:?}"
    );
    assert!(!content_root.join("inline-bindings/current.sobj").exists());
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"legacy bytes");
    drop(reader);
    owner.activate_content_bindings_before_routes().unwrap();
    assert!(content_root.join("inline-bindings/current.sobj").is_file());
    let store = LocalContentStore::new(&content_root).unwrap();
    assert_eq!(
        store.read_object_version("old", "v1").unwrap().1,
        b"legacy bytes"
    );
    store
        .put_object_version("new", "v1", b"inline bytes")
        .unwrap();
    assert_eq!(store.stats().unwrap().manifest_count, 2);
    assert!(store.scrub().unwrap().is_healthy());
}

#[test]
fn partial_activation_catalog_parent_failure_is_retryable_before_routes() {
    let root = sinomo_testdir::TestDir::new("dfs-inline-partial-activation");
    let data = SinomoDataDir::new(root.path());
    let content_root = data.dfs_dir().join("content");
    drop(LocalContentStore::new(&content_root).unwrap());
    let canonical = fs::canonicalize(&content_root).unwrap();
    let mut owner = DfsDataOwnerLock::acquire(&DfsRuntimePaths::new(&data)).unwrap();
    INLINE_FAULT
        .with(|fault| *fault.borrow_mut() = Some((canonical, InlineFault::CatalogParentSync)));
    assert!(owner.activate_content_bindings_before_routes().is_err());
    let inspected = LocalContentStore::open_for_inspection(&content_root);
    assert!(inspected.inline_sentinel_unlocked().unwrap().is_none());
    drop(inspected);
    drop(owner);
    let mut owner = DfsDataOwnerLock::acquire(&DfsRuntimePaths::new(&data)).unwrap();
    owner.activate_content_bindings_before_routes().unwrap();
    let store = LocalContentStore::new(content_root).unwrap();
    store
        .put_object_version("after-retry", "v1", b"bytes")
        .unwrap();
}

#[test]
fn inline_generic_retry_renews_grace_and_clears_absence_even_after_clock_rollback() {
    let (_root, _owner, store) = active_store("dfs-inline-renewal-grace");
    let manifest = store
        .put_object_version("object", "v1", b"same payload")
        .unwrap();
    let identity = ContentStoreReservationIdentity {
        object_id: "object".into(),
        version: "v1".into(),
        content_hash: manifest.content_hash.clone(),
    };
    let future = u64::MAX - 100;
    {
        let _owner = store.content_mutation_guard().unwrap();
        let reservation = store.lock_content_reservations_for_write().unwrap();
        let mut state = store.inline_store.owner.state.lock().unwrap();
        append_operation(
            &store.root,
            &reservation.sentinel,
            &mut state,
            InlineOperation::Reserve(ContentStoreReservationRecord {
                identity: identity.clone(),
                reserved_at_unix_nanos: future,
                causal_stamp: Some(reservation.sentinel.stamp()),
            }),
            &[],
        )
        .unwrap();
    }
    let marker = store.manifest_absence_observation_path(&identity);
    if let Some(parent) = marker.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(&marker, b"old observation must be removed before renewal").unwrap();
    store
        .put_object_version("object", "v1", b"same payload")
        .unwrap();
    assert!(!marker.exists());
    let Some(InlineReservationState::Live(record)) =
        store.inline_reservation_state(&identity).unwrap()
    else {
        panic!("missing renewed root");
    };
    assert!(record.reserved_at_unix_nanos >= future);
    store
        .ensure_content_reservations_batch(std::slice::from_ref(&identity))
        .unwrap();
    let Some(InlineReservationState::Live(batch_renewed)) =
        store.inline_reservation_state(&identity).unwrap()
    else {
        panic!("missing batch renewal");
    };
    assert!(batch_renewed.reserved_at_unix_nanos >= record.reserved_at_unix_nanos);
}

#[test]
fn inline_quota_deduplicates_payload_and_does_not_admit_failed_tuple() {
    let (_root, _owner, store) = active_store("dfs-inline-quota-union");
    let store = store.with_quota(ContentStoreQuota {
        max_blob_bytes: Some(4),
        max_logical_object_bytes: Some(8),
        ..ContentStoreQuota::default()
    });
    store.put_object_version("a", "v1", b"same").unwrap();
    store.put_object_version("b", "v1", b"same").unwrap();
    assert!(store.put_object_version("c", "v1", b"same").is_err());
    assert!(content_error_is_not_found(
        &store.read_manifest("c", "v1").unwrap_err()
    ));
    let stats = store.stats().unwrap();
    assert_eq!(
        (
            stats.blob_count,
            stats.blob_bytes,
            stats.manifest_count,
            stats.logical_object_bytes
        ),
        (1, 4, 2, 8)
    );
}

#[test]
fn inline_torn_tail_recovery_never_accepts_complete_corruption_or_cached_rejected_view() {
    let (_root, _owner, store) = active_store("dfs-inline-tail-recovery");
    store.put_object_version("object", "v1", b"first").unwrap();
    let path = segment_path(&store.root, 1, 0);
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&[1, 2, 3])
        .unwrap();
    store.put_object_version("object", "v2", b"second").unwrap();
    assert_eq!(
        store.read_object_version("object", "v1").unwrap().1,
        b"first"
    );
    let mut bytes = fs::read(&path).unwrap();
    let end = bytes.len() - 1;
    bytes[end] ^= 0x40;
    fs::write(&path, &bytes).unwrap();
    assert!(store.inline_binding_view_verified().is_err());
    assert!(store.read_manifest("object", "v1").is_err());
    assert!(store.content_reservation_roots().is_err());
    assert!(
        store
            .put_object_version("object", "v3", b"forbidden")
            .is_err()
    );
}

#[test]
fn inline_warm_writer_rejects_same_size_active_path_replacement() {
    let (_root, _owner, store) = active_store("dfs-inline-inode-replacement");
    store.put_object_version("object", "v1", b"first").unwrap();
    let path = segment_path(&store.root, 1, 0);
    let replacement = path.with_extension("replacement");
    fs::write(&replacement, fs::read(&path).unwrap()).unwrap();
    fs::rename(&replacement, &path).unwrap();
    let before = fs::read(&path).unwrap();
    assert!(store.put_object_version("object", "v2", b"second").is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn visible_rotated_catalog_cannot_ack_before_recertification_even_after_owner_expiry() {
    let (_root, _owner, store) = active_store("dfs-inline-rotation-certification");
    store.put_object_version("object", "v1", b"first").unwrap();
    let root = store.root.clone();
    let sentinel = store.inline_sentinel_unlocked().unwrap().unwrap();
    let mut catalog = read_catalog(&root, &sentinel.store_incarnation).unwrap();
    let path = segment_path(&root, 1, 0);
    let (hash, length) = durable_file::file_blake3_hash_and_size(&path).unwrap();
    catalog.segments[0].sealed_end = Some(length);
    catalog.segments[0].sealed_digest = Some(hash.to_hex().to_string());
    let next = segment_path(&root, 1, 1);
    drop(FrameAppender::open(&next, frame_limits()).unwrap());
    catalog.segments.push(InlineSegment {
        id: 1,
        sealed_end: None,
        sealed_digest: None,
    });
    INLINE_FAULT
        .with(|fault| *fault.borrow_mut() = Some((root.clone(), InlineFault::CatalogParentSync)));
    assert!(publish_catalog(&root, &catalog).is_err());
    drop(store);
    let store = LocalContentStore::new(&root).unwrap();
    INLINE_FAULT.with(|fault| {
        *fault.borrow_mut() = Some((root.clone(), InlineFault::CatalogCertification))
    });
    assert!(store.put_object_version("object", "v2", b"second").is_err());
    assert_eq!(
        fs::metadata(&next).unwrap().len(),
        0,
        "no receipt bytes before namespace certification"
    );
    store.put_object_version("object", "v2", b"second").unwrap();
    drop(store);
    assert_eq!(
        LocalContentStore::new(&root)
            .unwrap()
            .read_object_version("object", "v2")
            .unwrap()
            .1,
        b"second"
    );
}

#[test]
fn inline_delete_shadows_legacy_copies_and_blob_only_republish_has_no_manifest() {
    let (_root, _owner, store) = active_store("dfs-inline-shadowed-delete");
    let manifest = store
        .put_object_version("object", "v1", b"payload")
        .unwrap();
    let legacy_manifest = store.manifest_path("object", "v1");
    write_file_atomic(&legacy_manifest, &encode_manifest(&manifest).unwrap()).unwrap();
    fs::write(store.blob_path(&manifest.content_hash).unwrap(), b"payload").unwrap();
    {
        let _owner = store.content_mutation_guard().unwrap();
        store
            .inline_delete_version_unlocked("object", "v1", &manifest.content_hash)
            .unwrap();
        let _reservation = store.lock_content_reservations_for_write().unwrap();
        store
            .inline_release_reservations_unlocked(&BTreeSet::from([
                ContentStoreReservationIdentity {
                    object_id: "object".into(),
                    version: "v1".into(),
                    content_hash: manifest.content_hash.clone(),
                },
            ]))
            .unwrap();
    }
    {
        let _owner = store.content_mutation_guard().unwrap();
        store
            .inline_remove_blob_unlocked(&manifest.content_hash)
            .unwrap();
    }
    assert!(store.manifests_for_object("object").unwrap().is_empty());
    assert!(!store.has_blob(&manifest.content_hash).unwrap());
    assert_eq!(store.stats().unwrap().manifest_count, 0);
    assert!(
        !store
            .object_version_storage_candidate_exists("object", "v1", &manifest.content_hash)
            .unwrap()
    );
    // A stale physical manifest must not make blob-only staging return a
    // receipt before its retired payload has actually been restored.
    store
        .put_object_version_blob_only("object", "v1", b"payload")
        .unwrap();
    assert!(store.has_blob(&manifest.content_hash).unwrap());
    assert!(content_error_is_not_found(
        &store.read_manifest("object", "v1").unwrap_err()
    ));
    store
        .put_object_version_blob_only("other", "v1", b"payload")
        .unwrap();
    assert!(content_error_is_not_found(
        &store.read_manifest("other", "v1").unwrap_err()
    ));
    assert_eq!(store.read_blob(&manifest.content_hash).unwrap(), b"payload");
    assert!(content_error_is_not_found(
        &store.read_manifest("object", "v1").unwrap_err()
    ));
}

#[cfg(unix)]
#[test]
fn precreation_alias_inspection_lease_cannot_evade_activation_drain() {
    let root = sinomo_testdir::TestDir::new("dfs-inline-alias-drain");
    let real = root.join("real");
    fs::create_dir(&real).unwrap();
    let alias = root.join("alias");
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    let inspected = LocalContentStore::open_for_inspection(alias.join("dfs/content"));
    assert!(!real.join("dfs/content").exists());
    let data = SinomoDataDir::new(&real);
    let content_root = real.join("dfs/content");
    let mut owner = DfsDataOwnerLock::acquire(&DfsRuntimePaths::new(&data)).unwrap();
    let error = owner
        .activate_content_bindings_before_routes()
        .expect_err("a retained legacy lease must block activation");
    let expected = ContentStoreError::InlineActivationBusy {
        root: fs::canonicalize(&content_root).expect("created canonical content root"),
        live_leases: 1,
    };
    assert!(
        matches!(&error, crate::runtime_host::DfsRuntimeError::Parse { detail }
            if detail == &expected.to_string()),
        "unexpected activation refusal: {error:?}"
    );
    assert!(!content_root.join("inline-bindings/current.sobj").exists());
    drop(inspected);
    owner.activate_content_bindings_before_routes().unwrap();
    assert!(content_root.join("inline-bindings/current.sobj").is_file());
    let store = LocalContentStore::new(alias.join("dfs/../dfs/content")).unwrap();
    store
        .put_object_version("object", "v1", b"alias safe")
        .unwrap();
    assert_eq!(
        store.read_object_version("object", "v1").unwrap().1,
        b"alias safe"
    );
}

#[test]
fn successful_scrub_preserves_exact_resident_durability_capability() {
    let (_root, _owner, store) = active_store("dfs-inline-scrub-keeps-certificate");
    store.put_object_version("object", "v1", b"first").unwrap();
    assert!(store.scrub().unwrap().is_healthy());
    INLINE_FAULT.with(|fault| {
        *fault.borrow_mut() = Some((store.root.clone(), InlineFault::CatalogCertification))
    });
    store
        .put_object_version("object", "v2", b"second")
        .expect("unchanged certified generation does not need fresh namespace publication");
    INLINE_FAULT.with(|fault| {
        assert!(
            fault.borrow_mut().take().is_some(),
            "scrub should preserve the exact resident certificate"
        );
    });
    assert_eq!(
        store.read_object_version("object", "v2").unwrap().1,
        b"second"
    );
}

#[cfg(unix)]
#[test]
fn small_verified_quarantine_adopts_one_public_binding_and_large_bytes_keep_legacy_layout() {
    let (_root, _owner, store) = active_store("dfs-inline-stream-and-large");
    let bytes = vec![17u8; PACKED_BLOB_SMALL_LIMIT_BYTES as usize];
    let hash = content_hash(&bytes);
    let mut ingest = store
        .begin_quarantined_blob_ingest(bytes.len() as u64, Some(&hash))
        .unwrap();
    ingest.write_chunk(&bytes[..100]).unwrap();
    ingest.write_chunk(&bytes[100..]).unwrap();
    let blob = ingest.finish_quarantined().unwrap();
    store.bind_quarantined_blob("streamed", "v1", blob).unwrap();
    assert!(matches!(
        store.inline_version_state("streamed", "v1").unwrap(),
        Some(InlineVersionState::Live(_))
    ));
    assert!(!store.manifest_path("streamed", "v1").exists());
    assert_eq!(
        store.read_object_version("streamed", "v1").unwrap().1,
        bytes
    );
    let large = vec![23u8; PACKED_BLOB_SMALL_LIMIT_BYTES as usize + 1];
    store.put_object_version("large", "v1", &large).unwrap();
    assert!(store.manifest_path("large", "v1").is_file());
    assert!(store.inline_version_state("large", "v1").unwrap().is_none());
    assert_eq!(store.read_object_version("large", "v1").unwrap().1, large);
    assert!(store.scrub().unwrap().is_healthy());
}

#[test]
fn mixed_extent_manifest_protects_inline_base_hash_after_original_version_retirement() {
    let (_root, _owner, store) = active_store("dfs-inline-mixed-extents");
    let base = store
        .put_object_version("object", "v1", b"0123456789abcdef")
        .unwrap();
    let patch = store
        .put_object_version_patch("object", "v1", "v2", 6, b"WXYZ")
        .unwrap();
    assert!(patch.extents.len() >= 3);
    assert_eq!(
        store.read_object_version("object", "v2").unwrap().1,
        b"012345WXYZabcdef"
    );
    {
        let _owner = store.content_mutation_guard().unwrap();
        store
            .inline_delete_version_unlocked("object", "v1", &base.content_hash)
            .unwrap();
        let _reservation = store.lock_content_reservations_for_write().unwrap();
        store
            .inline_release_reservations_unlocked(&BTreeSet::from([
                ContentStoreReservationIdentity {
                    object_id: "object".into(),
                    version: "v1".into(),
                    content_hash: base.content_hash.clone(),
                },
            ]))
            .unwrap();
    }
    let report = store.scrub().unwrap();
    assert!(
        report.is_healthy(),
        "mixed manifest owns its inline base extent: {report:?}"
    );
    assert_eq!(report.stats.manifest_count, 1);
    let root = store.root.clone();
    drop(store);
    let reopened = LocalContentStore::new(root).unwrap();
    assert_eq!(
        reopened.read_object_version("object", "v2").unwrap().1,
        b"012345WXYZabcdef"
    );
    assert!(reopened.has_blob(&base.content_hash).unwrap());
}
