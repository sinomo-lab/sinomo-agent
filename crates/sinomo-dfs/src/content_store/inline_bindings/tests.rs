use super::*;
use crate::runtime_host::{DfsDataOwnerLock, DfsRuntimePaths};
use sinomo_core::local::SinomoDataDir;

pub(super) fn active_store(
    name: &str,
) -> (sinomo_testdir::TestDir, DfsDataOwnerLock, LocalContentStore) {
    let root = sinomo_testdir::TestDir::new(name);
    let data = SinomoDataDir::new(root.path());
    let owner = DfsDataOwnerLock::acquire(&DfsRuntimePaths::new(&data)).unwrap();
    let content = LocalContentStore::new(data.dfs_dir().join("content")).unwrap();
    (root, owner, content)
}

#[test]
fn inline_binding_recovers_exact_manifest_payload_and_reservation_without_files_indexes() {
    let (_root, _owner, store) = active_store("dfs-inline-exact-reopen");
    let manifest = store
        .put_object_version("object", "v1", b"complete durable binding")
        .unwrap();
    assert!(!store.manifest_path("object", "v1").exists());
    assert!(!store.blob_path(&manifest.content_hash).unwrap().exists());
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
                causal_stamp: reservation.sentinel.stamp(),
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
        .ensure_content_reservation(
            &identity.object_id,
            &identity.version,
            &identity.content_hash,
        )
        .unwrap();
    let Some(InlineReservationState::Live(renewed_again)) =
        store.inline_reservation_state(&identity).unwrap()
    else {
        panic!("missing normal renewal");
    };
    assert!(renewed_again.reserved_at_unix_nanos >= record.reserved_at_unix_nanos);
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
    let second = store.put_object_version("object", "v2", b"second").unwrap();
    assert_eq!(
        store.read_object_version("object", "v1").unwrap().1,
        b"first"
    );
    assert_eq!(store.read_manifest("object", "v2").unwrap(), second);
    store.corrupt_current_manifest_for_test(&second).unwrap();
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
fn inline_delete_shadows_files_copies_and_blob_only_republish_has_no_manifest() {
    let (_root, _owner, store) = active_store("dfs-inline-shadowed-delete");
    let manifest = store
        .put_object_version("object", "v1", b"payload")
        .unwrap();
    let legacy_manifest = store.manifest_path("object", "v1");
    write_file_atomic(&legacy_manifest, &encode_manifest(&manifest).unwrap()).unwrap();
    let loose_copy = store.blob_path(&manifest.content_hash).unwrap();
    fs::create_dir_all(loose_copy.parent().unwrap()).unwrap();
    fs::write(loose_copy, b"payload").unwrap();
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
fn small_verified_quarantine_adopts_one_public_binding_and_large_bytes_keep_files_layout() {
    let (_root, _owner, store) = active_store("dfs-inline-stream-and-large");
    let bytes = vec![17u8; INLINE_BLOB_SMALL_LIMIT_BYTES as usize];
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
    let large = vec![23u8; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1];
    store.put_object_version("large", "v1", &large).unwrap();
    assert!(!store.manifest_path("large", "v1").exists());
    assert!(matches!(
        store.inline_version_state("large", "v1").unwrap(),
        Some(InlineVersionState::Live(_))
    ));
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

#[test]
fn expected_hash_inline_bytes_bypass_quarantine_and_reopen_exact_binding() {
    let (_root, _owner, store) = active_store("dfs-inline-expected-hash-no-quarantine");
    let bytes = vec![31; INLINE_BLOB_SMALL_LIMIT_BYTES as usize];
    let hash = content_hash(&bytes);
    // The old expected-hash path cannot even create its private temporary file.
    // Successful publication therefore cannot have issued a quarantine sync.
    let temporary_directory = store.root.join("blob-tmp");
    fs::write(&temporary_directory, b"quarantine unavailable").unwrap();
    let manifest = store
        .put_object_version_with_expected_hash("object", "v1", &hash, &bytes)
        .expect("eligible owned bytes do not need a temporary file");
    assert_eq!(manifest.content_hash, hash);
    assert_eq!(manifest.size_bytes, bytes.len() as u64);
    assert!(manifest.extents.is_empty());
    assert!(matches!(
        store.inline_version_state("object", "v1").unwrap(),
        Some(InlineVersionState::Live(_))
    ));
    assert!(!store.manifest_path("object", "v1").exists());
    assert!(!store.blob_path(&hash).unwrap().exists());
    assert_eq!(
        fs::read(&temporary_directory).unwrap(),
        b"quarantine unavailable"
    );
    let identity = ContentStoreReservationIdentity {
        object_id: "object".into(),
        version: "v1".into(),
        content_hash: hash.clone(),
    };
    let Some(InlineReservationState::Live(reservation)) =
        store.inline_reservation_state(&identity).unwrap()
    else {
        panic!("binding must retain its exact content root");
    };
    assert_eq!(
        reservation.causal_stamp,
        store.inline_sentinel_unlocked().unwrap().unwrap().stamp()
    );
    let empty = store
        .put_object_version_with_expected_hash("empty", "v1", intrinsic_empty_content_hash(), &[])
        .unwrap();
    assert_eq!(empty.size_bytes, 0);
    let root = store.root.clone();
    drop(store);
    let reopened = LocalContentStore::new(root).unwrap();
    assert_eq!(reopened.read_manifest("object", "v1").unwrap(), manifest);
    assert_eq!(
        reopened.read_object_version("object", "v1").unwrap().1,
        bytes
    );
    assert_eq!(reopened.read_manifest("empty", "v1").unwrap(), empty);
    assert!(
        reopened
            .read_object_version("empty", "v1")
            .unwrap()
            .1
            .is_empty()
    );
    assert!(
        reopened
            .content_reservation_roots()
            .unwrap()
            .content_hashes
            .contains(&hash)
    );
    assert!(reopened.scrub().unwrap().is_healthy());
}

#[test]
fn expected_hash_inline_mismatch_cannot_publish_or_renew_existing_roots() {
    let (_root, _owner, store) = active_store("dfs-inline-expected-hash-mismatch");
    let bytes = b"canonical bytes";
    let hash = content_hash(bytes);
    let manifest = store
        .put_object_version_with_expected_hash("existing", "v1", &hash, bytes)
        .unwrap();
    let identity = ContentStoreReservationIdentity {
        object_id: "existing".into(),
        version: "v1".into(),
        content_hash: hash.clone(),
    };
    let Some(InlineReservationState::Live(before_reservation)) =
        store.inline_reservation_state(&identity).unwrap()
    else {
        panic!("existing exact reservation");
    };
    let before_log = fs::read(segment_path(&store.root, 1, 0)).unwrap();
    let absence = store.manifest_absence_observation_path(&identity);
    fs::create_dir_all(absence.parent().unwrap()).unwrap();
    fs::write(&absence, b"must survive rejected content").unwrap();
    let wrong_hash = content_hash(b"other bytes");
    for object in ["new", "existing"] {
        let error = store
            .put_object_version_with_expected_hash(object, "v1", &wrong_hash, bytes)
            .unwrap_err();
        assert!(
            matches!(error, ContentStoreError::BlobHashMismatch { expected, actual }
            if expected == wrong_hash && actual == hash)
        );
    }
    assert_eq!(
        fs::read(segment_path(&store.root, 1, 0)).unwrap(),
        before_log
    );
    let Some(InlineReservationState::Live(after_reservation)) =
        store.inline_reservation_state(&identity).unwrap()
    else {
        panic!("rejected content must retain the exact reservation");
    };
    assert_eq!(after_reservation, before_reservation);
    assert_eq!(store.read_manifest("existing", "v1").unwrap(), manifest);
    assert!(content_error_is_not_found(
        &store.read_manifest("new", "v1").unwrap_err()
    ));
    assert_eq!(fs::read(absence).unwrap(), b"must survive rejected content");
    assert!(!store.root.join("blob-tmp").exists());
}

#[test]
fn expected_hash_inline_bytes_preserve_quota_and_exact_tuple_conflicts() {
    let (_root, _owner, store) = active_store("dfs-inline-expected-hash-admission");
    let store = store.with_quota(ContentStoreQuota {
        max_blob_bytes: Some(4),
        max_logical_object_bytes: Some(4),
        ..ContentStoreQuota::default()
    });
    let first = store
        .put_object_version_with_expected_hash("object", "v1", &content_hash(b"same"), b"same")
        .unwrap();
    let before = store.inline_binding_view().unwrap().unwrap().sequence;
    assert!(matches!(
        store.put_object_version_with_expected_hash(
            "object",
            "v1",
            &content_hash(b"diff"),
            b"diff"
        ),
        Err(ContentStoreError::VersionHashConflict { .. })
    ));
    assert!(matches!(
        store.put_object_version_with_expected_hash("new", "v1", &content_hash(b"same"), b"same"),
        Err(ContentStoreError::QuotaExceeded { .. })
    ));
    assert_eq!(
        store.inline_binding_view().unwrap().unwrap().sequence,
        before
    );
    assert_eq!(store.read_manifest("object", "v1").unwrap(), first);
    assert!(content_error_is_not_found(
        &store.read_manifest("new", "v1").unwrap_err()
    ));
    assert_eq!(
        store
            .content_reservation_roots()
            .unwrap()
            .content_hashes
            .len(),
        1
    );
}

#[test]
fn expected_hash_inline_lost_completion_requires_same_or_fresh_owner_certification() {
    for fresh_owner in [false, true] {
        let (_root, _owner, store) =
            active_store(&format!("dfs-inline-expected-hash-uncertain-{fresh_owner}"));
        let bytes = b"complete but not acknowledged";
        let hash = content_hash(bytes);
        let root = store.root.clone();
        INLINE_FAULT.with(|fault| {
            *fault.borrow_mut() = Some((root.clone(), InlineFault::LostAppendCompletion));
        });
        assert!(
            store
                .put_object_version_with_expected_hash("object", "v1", &hash, bytes)
                .unwrap_err()
                .to_string()
                .contains("lost append completion")
        );
        let path = segment_path(&root, 1, 0);
        let persisted = fs::read(&path).unwrap();
        assert!(!persisted.is_empty());
        let store = if fresh_owner {
            drop(store);
            LocalContentStore::new(&root).unwrap()
        } else {
            store
        };
        INLINE_FAULT.with(|fault| {
            *fault.borrow_mut() = Some((root.clone(), InlineFault::CatalogCertification));
        });
        assert!(
            store
                .put_object_version_with_expected_hash("object", "v1", &hash, bytes)
                .is_err(),
            "readable complete frames cannot certify an equal retry"
        );
        assert_eq!(fs::read(&path).unwrap(), persisted);
        let manifest = store
            .put_object_version_with_expected_hash("object", "v1", &hash, bytes)
            .unwrap();
        assert_eq!(store.read_object_version("object", "v1").unwrap().1, bytes);
        assert_eq!(
            store
                .content_reservation_roots()
                .unwrap()
                .content_hashes
                .len(),
            1
        );
        drop(store);
        let reopened = LocalContentStore::new(root).unwrap();
        assert_eq!(reopened.read_manifest("object", "v1").unwrap(), manifest);
        assert_eq!(
            reopened.read_object_version("object", "v1").unwrap().1,
            bytes
        );
        assert!(reopened.scrub().unwrap().is_healthy());
    }
}

#[test]
fn expected_hash_large_files_and_public_quarantine_keep_file_ingest() {
    let (_root, _owner, store) = active_store("dfs-inline-expected-hash-file-fallback");
    let blocker = store.root.join("blob-tmp");
    fs::write(&blocker, b"file ingest blocked").unwrap();
    let large = vec![41; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1];
    let large_hash = content_hash(&large);
    assert!(
        store
            .put_object_version_with_expected_hash("large", "v1", &large_hash, &large)
            .is_err()
    );
    assert!(store.begin_quarantined_blob_ingest(1, None).is_err());
    assert!(content_error_is_not_found(
        &store.read_manifest("large", "v1").unwrap_err()
    ));
    fs::remove_file(&blocker).unwrap();
    let manifest = store
        .put_object_version_with_expected_hash("large", "v1", &large_hash, &large)
        .unwrap();
    assert_eq!(store.read_object_version("large", "v1").unwrap().1, large);
    assert!(matches!(
        store.inline_version_state("large", "v1").unwrap(),
        Some(InlineVersionState::Live(_))
    ));
    assert!(!store.manifest_path("large", "v1").exists());
    assert_eq!(manifest.content_hash, large_hash);
    let mut quarantine = store
        .begin_quarantined_blob_ingest(1, Some(&content_hash(b"q")))
        .unwrap();
    quarantine.write_chunk(b"q").unwrap();
    let quarantine = quarantine.finish_quarantined().unwrap();
    assert!(quarantine.synced_file.path().is_file());
    assert!(content_error_is_not_found(
        &store.read_manifest("anonymous", "v1").unwrap_err()
    ));
    drop(quarantine);
}

#[test]
fn a_retained_repair_payload_record_retains_nothing_and_its_content_is_reclaimable() {
    let (_root, _owner, store) = active_store("dfs-inline-legacy-repair-payload");
    let pinned_hash = crate::content_hash(b"bytes only a removed repair custody pinned");
    let incarnation = {
        let reservation = store.lock_content_reservations_for_write().unwrap();
        reservation.sentinel.store_incarnation.clone()
    };
    let root = RepairPayloadRoot {
        subject: RepairPayloadSubject {
            queue_incarnation: "queue".into(),
            producer: "producer".into(),
            target: "target".into(),
            intent_id: "intent".into(),
            request_id: "request".into(),
        },
        original_command_digest: "0".repeat(64),
        store_incarnation: incarnation,
        source: RepairPayloadSource::Blob {
            content_hash: pinned_hash.clone(),
            size_bytes: 42,
        },
    };
    {
        let _owner = store.content_mutation_guard().unwrap();
        let reservation = store.lock_content_reservations_for_write().unwrap();
        let mut state = store.inline_store.owner.state.lock().unwrap();
        append_operation(
            &store.inline_store.owner.root,
            &reservation.sentinel,
            &mut state,
            InlineOperation::RetainRepairPayload(root),
            &[],
        )
        .unwrap();
    }
    let content_root = store.root.clone();
    drop(store);
    let reopened = LocalContentStore::new(content_root).unwrap();
    let roots = reopened.content_reservation_roots().unwrap();
    assert!(
        !roots.content_hashes.contains(&pinned_hash),
        "a retained repair payload root of the removed custody protects nothing"
    );
    assert!(reopened.scrub().unwrap().is_healthy());
}
