//! Small blob-only staging keeps its public no-new-manifest contract. The exact
//! payload and reservation become durable together; only a later explicit
//! manifest publication inserts a visible version entry.

use super::*;

impl LocalContentStore {
    pub(in crate::content_store) fn try_stage_inline_blob_only(
        &self,
        object_id: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<Option<ContentManifest>> {
        if bytes.len() as u64 > PACKED_BLOB_SMALL_LIMIT_BYTES {
            return Ok(None);
        }
        let _owner = self.content_mutation_guard()?;
        if self.inline_sentinel_unlocked()?.is_none() {
            return Ok(None);
        }
        validate_key(object_id, version)?;
        let proposed = ContentManifest {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            content_hash: content_hash(bytes),
            size_bytes: bytes.len() as u64,
            extents: Vec::new(),
        };
        self.inline_check_manifest_conflict(&proposed)?;
        let existing = match self.read_manifest(object_id, version) {
            Ok(manifest) => {
                // Existing patch layouts retain their original blob-only
                // behavior; staging must not flatten or duplicate extents.
                if !manifest.extents.is_empty() {
                    return Ok(None);
                }
                if manifest.content_hash != proposed.content_hash
                    || manifest.size_bytes != proposed.size_bytes
                {
                    return Err(ContentStoreError::VersionHashConflict {
                        object_id: object_id.to_owned(),
                        version: version.to_owned(),
                        existing_hash: manifest.content_hash,
                        new_hash: proposed.content_hash,
                    });
                }
                Some(manifest)
            }
            Err(error) if content_error_is_not_found(&error) => None,
            Err(error) => return Err(error),
        };
        if bytes.is_empty() {
            return Ok(Some(existing.unwrap_or(proposed)));
        }
        let reservation_guard = self.lock_content_reservations_for_write()?;
        let identity = ContentStoreReservationIdentity {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            content_hash: proposed.content_hash.clone(),
        };
        let prior_time = self
            .reservation_copies_for_identity_unlocked(&identity)?
            .iter()
            .map(|record| record.reserved_at_unix_nanos)
            .max();
        let reservation = ContentStoreReservationRecord {
            identity: identity.clone(),
            reserved_at_unix_nanos: refreshed_reservation_timestamp(prior_time),
            causal_stamp: Some(reservation_guard.sentinel.stamp()),
        };
        let reuse_payload = self.inline_blob_entry(&proposed.content_hash)?.is_some();
        if reuse_payload && !self.has_blob(&proposed.content_hash)? {
            return Err(inline_error(
                "existing inline payload is unavailable for blob-only staging",
            ));
        }
        let prior_bytes = self
            .readable_blob_size(&proposed.content_hash)?
            .unwrap_or(0);
        self.quota_stats_initialized.store(false, Ordering::Release);
        self.ensure_quota_delta(ContentStoreQuotaDelta {
            blob_bytes: proposed.size_bytes.saturating_sub(prior_bytes),
            ..ContentStoreQuotaDelta::default()
        })?;
        // Staging/retry renews grace even when the same payload is reused. It
        // never borrows the canonical-completion no-renewal exception.
        self.remove_manifest_absence_observation(&identity)?;
        self.clear_orphan_observations_before_manifest_publish(std::iter::once(&proposed))?;
        let operation = if reuse_payload {
            InlineOperation::Reserve(reservation)
        } else {
            InlineOperation::StageBlob {
                size_bytes: proposed.size_bytes,
                published_at_unix_nanos: system_time_nanos_since_epoch_saturating(SystemTime::now()),
                reservation,
            }
        };
        {
            let mut state = self
                .inline_store
                .owner
                .state
                .lock()
                .map_err(|_| inline_error("resident binding mutex poisoned"))?;
            append_operation(
                &self.inline_store.owner.root,
                &reservation_guard.sentinel,
                &mut state,
                operation,
                if reuse_payload { &[] } else { bytes },
            )?;
        }
        self.quota_stats_initialized.store(false, Ordering::Release);
        if self.quota.is_some() {
            self.replace_stats_snapshot(&self.scan_stats()?)?;
        }
        Ok(Some(existing.unwrap_or(proposed)))
    }

    pub(in crate::content_store) fn try_publish_inline_staged_manifest(
        &self,
        object_id: &str,
        version: &str,
        hash: &str,
        size_bytes: u64,
    ) -> Result<Option<ContentManifest>> {
        if size_bytes > PACKED_BLOB_SMALL_LIMIT_BYTES {
            return Ok(None);
        }
        let _owner = self.content_mutation_guard()?;
        if self.inline_sentinel_unlocked()?.is_none() {
            return Ok(None);
        }
        // Already-public manifests keep their existing renewal path (including
        // its Reserve-only inline update). Optimize only the missing public
        // manifest edge; never flatten an existing extent representation.
        match self.read_manifest(object_id, version) {
            Ok(_) => return Ok(None),
            Err(error) if content_error_is_not_found(&error) => {}
            Err(error) => return Err(error),
        }
        if size_bytes == 0 && hash == intrinsic_empty_content_hash() {
            return self.try_write_inline_binding(object_id, version, &[]);
        }
        let Some(blob) = self.inline_blob_entry(hash)? else {
            return Ok(None);
        };
        if blob.size_bytes != size_bytes {
            return Err(inline_error(
                "explicit manifest size differs from its staged payload",
            ));
        }
        let mut file =
            open_verified_inline_payload(&blob.path, blob.payload_offset, blob.size_bytes, hash)?;
        let mut bytes = vec![0; blob.size_bytes as usize];
        file.read_exact(&mut bytes)
            .map_err(|source| ContentStoreError::Io {
                path: blob.path,
                source,
            })?;
        // The shared full-binding path publishes a Link to this certified
        // payload and renews the exact reservation in the same one-sync frame.
        self.try_write_inline_binding(object_id, version, &bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::active_store;
    use super::*;

    fn sequence(store: &LocalContentStore) -> u64 {
        store.inline_binding_view().unwrap().unwrap().sequence
    }
    fn identity(manifest: &ContentManifest) -> ContentStoreReservationIdentity {
        ContentStoreReservationIdentity {
            object_id: manifest.object_id.clone(),
            version: manifest.version.clone(),
            content_hash: manifest.content_hash.clone(),
        }
    }
    fn require_absent(store: &LocalContentStore, object: &str, version: &str) {
        assert!(content_error_is_not_found(
            &store.read_manifest(object, version).unwrap_err()
        ));
        assert!(store.manifests_for_object(object).unwrap().is_empty());
        assert!(!store.manifest_path(object, version).exists());
        assert!(
            store
                .inline_version_state(object, version)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn small_blob_only_stage_is_one_frame_and_recovers_without_a_public_manifest() {
        let (_root, _owner, store) = active_store("dfs-inline-blob-stage-reopen");
        let cut = store.begin_content_root_catalog_scan().unwrap();
        let staged = store
            .put_object_version_blob_only("object", "v1", b"exact staged bytes")
            .unwrap();
        assert_eq!(sequence(&store), 1);
        require_absent(&store, "object", "v1");
        let path = segment_path(&store.root, 1, 0);
        let mut file = fs::File::open(&path).unwrap();
        let FrameRead::Complete(frame) = read_frame(&mut file, &path, frame_limits()).unwrap()
        else {
            panic!("complete frame");
        };
        let record: InlineRecord = sinomo_object_serde::from_bytes(frame.header()).unwrap();
        let InlineOperation::StageBlob {
            reservation,
            size_bytes,
            ..
        } = record.operation
        else {
            panic!("one stage record");
        };
        assert_eq!(reservation.identity, identity(&staged));
        assert_eq!(reservation.causal_stamp, Some(cut.stamp));
        assert_eq!(size_bytes, b"exact staged bytes".len() as u64);
        assert!(matches!(
            read_frame(&mut file, &path, frame_limits()).unwrap(),
            FrameRead::CleanEof { .. }
        ));
        let stats = store.stats().unwrap();
        assert_eq!(
            (
                stats.manifest_count,
                stats.logical_object_bytes,
                stats.blob_count
            ),
            (0, 0, 1)
        );
        let root = store.root.clone();
        drop(file);
        drop(store);
        let reopened = LocalContentStore::new(root).unwrap();
        require_absent(&reopened, "object", "v1");
        assert_eq!(
            reopened.read_blob(&staged.content_hash).unwrap(),
            b"exact staged bytes"
        );
        assert!(
            reopened
                .content_reservation_roots()
                .unwrap()
                .reservations
                .iter()
                .any(|r| r.identity == identity(&staged))
        );
        assert!(
            reopened
                .object_version_storage_candidate_exists("object", "v1", &staged.content_hash)
                .unwrap()
        );
    }

    #[test]
    fn staged_hashes_coexist_until_explicit_manifest_publication_selects_one() {
        let (_root, _owner, store) = active_store("dfs-inline-blob-stage-alternatives");
        let a = store
            .put_object_version_blob_only("object", "v1", b"first candidate")
            .unwrap();
        let b = store
            .put_object_version_blob_only("object", "v1", b"other candidate")
            .unwrap();
        require_absent(&store, "object", "v1");
        assert_eq!(
            store
                .content_reservation_roots()
                .unwrap()
                .reservations
                .len(),
            2
        );
        assert_eq!(store.stats().unwrap().blob_count, 2);
        let original_locator = store.inline_blob_entry(&a.content_hash).unwrap().unwrap();
        let before = sequence(&store);
        store
            .put_blob_only_manifest("object", "v1", &a.content_hash, a.size_bytes)
            .unwrap();
        assert_eq!(sequence(&store), before + 1);
        assert_eq!(store.read_manifest("object", "v1").unwrap(), a);
        assert!(
            !store.manifest_path("object", "v1").exists(),
            "explicit publication uses an inline Link"
        );
        assert_eq!(
            store
                .inline_blob_entry(&a.content_hash)
                .unwrap()
                .unwrap()
                .payload_offset,
            original_locator.payload_offset
        );
        assert_eq!(
            store.read_blob(&b.content_hash).unwrap(),
            b"other candidate"
        );
        let before = sequence(&store);
        assert!(matches!(
            store.put_object_version_blob_only("object", "v1", b"other candidate"),
            Err(ContentStoreError::VersionHashConflict { .. })
        ));
        assert_eq!(sequence(&store), before);
        assert_eq!(
            store
                .content_reservation_roots()
                .unwrap()
                .reservations
                .len(),
            2
        );
    }

    #[test]
    fn blob_only_retry_recovers_torn_tail_and_requires_uncertain_prefix_certification() {
        let (_root, _owner, store) = active_store("dfs-inline-blob-stage-uncertain");
        let staged = store
            .put_object_version_blob_only("object", "v1", b"payload")
            .unwrap();
        let path = segment_path(&store.root, 1, 0);
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[1, 2, 3])
            .unwrap();
        let root = store.root.clone();
        drop(store);
        let reopened = LocalContentStore::new(&root).unwrap();
        INLINE_FAULT
            .with(|fault| *fault.borrow_mut() = Some((root, InlineFault::CatalogCertification)));
        assert!(
            reopened
                .put_object_version_blob_only("object", "v1", b"payload")
                .is_err()
        );
        assert_eq!(sequence(&reopened), 1);
        require_absent(&reopened, "object", "v1");
        reopened
            .put_object_version_blob_only("object", "v1", b"payload")
            .unwrap();
        assert_eq!(sequence(&reopened), 2);
        assert_eq!(
            reopened.read_blob(&staged.content_hash).unwrap(),
            b"payload"
        );
        require_absent(&reopened, "object", "v1");
    }

    #[test]
    fn blob_only_stage_failure_does_not_publish_payload_root_or_manifest() {
        let (_root, _owner, store) = active_store("dfs-inline-blob-stage-quota");
        let store = store.with_quota(ContentStoreQuota {
            max_blob_bytes: Some(3),
            ..ContentStoreQuota::default()
        });
        assert!(matches!(
            store.put_object_version_blob_only("object", "v1", b"too large"),
            Err(ContentStoreError::QuotaExceeded { .. })
        ));
        assert_eq!(sequence(&store), 0);
        require_absent(&store, "object", "v1");
        assert!(
            store
                .content_reservation_roots()
                .unwrap()
                .reservations
                .is_empty()
        );
        assert_eq!(store.stats().unwrap().blob_bytes, 0);
        let staged = store
            .put_object_version_blob_only("object", "v1", b"yes")
            .unwrap();
        let before = sequence(&store);
        assert!(
            store
                .put_blob_only_manifest("object", "v1", &staged.content_hash, 2)
                .is_err()
        );
        assert_eq!(sequence(&store), before);
        require_absent(&store, "object", "v1");
    }

    #[test]
    fn reservation_protects_blob_only_stage_then_gc_and_restage_keep_manifest_absent() {
        let (_root, _owner, store) = active_store("dfs-inline-blob-stage-gc");
        let staged = store
            .put_object_version_blob_only("object", "v1", b"payload")
            .unwrap();
        let protected = store
            .collect_garbage(ContentStoreGcOptions::dry_run(Duration::ZERO))
            .unwrap();
        assert_eq!(protected.retained_protected_orphan_count, 1);
        assert_eq!(protected.deleted_orphan_count, 0);
        {
            let _owner = store.content_mutation_guard().unwrap();
            let _reservation = store.lock_content_reservations_for_write().unwrap();
            store
                .inline_release_reservations_unlocked(&BTreeSet::from([identity(&staged)]))
                .unwrap();
        }
        store
            .collect_garbage(
                ContentStoreGcOptions::delete_eligible(Duration::ZERO)
                    .with_catalog_observation("first", u64::MAX - 1),
            )
            .unwrap();
        let deleted = store
            .collect_garbage(
                ContentStoreGcOptions::delete_eligible(Duration::ZERO)
                    .with_catalog_observation("second", u64::MAX),
            )
            .unwrap();
        assert_eq!(deleted.deleted_orphan_count, 1);
        assert!(!store.has_blob(&staged.content_hash).unwrap());
        assert_eq!(store.stats().unwrap().blob_count, 0);
        store
            .put_object_version_blob_only("object", "v1", b"payload")
            .unwrap();
        assert_eq!(store.read_blob(&staged.content_hash).unwrap(), b"payload");
        require_absent(&store, "object", "v1");
    }

    #[test]
    fn blob_only_tombstoned_tuple_stays_deleted_until_explicit_publication() {
        let (_root, _owner, store) = active_store("dfs-inline-blob-stage-version-tombstone");
        let original = store
            .put_object_version("object", "v1", b"payload")
            .unwrap();
        {
            let _owner = store.content_mutation_guard().unwrap();
            store
                .inline_delete_version_unlocked("object", "v1", &original.content_hash)
                .unwrap();
        }
        {
            let _owner = store.content_mutation_guard().unwrap();
            {
                let _reservation = store.lock_content_reservations_for_write().unwrap();
                store
                    .inline_release_reservations_unlocked(&BTreeSet::from([identity(&original)]))
                    .unwrap();
            }
            store
                .inline_remove_blob_unlocked(&original.content_hash)
                .unwrap();
        }
        store
            .put_object_version_blob_only("object", "v1", b"payload")
            .unwrap();
        assert!(matches!(
            store.inline_version_state("object", "v1").unwrap(),
            Some(InlineVersionState::Deleted { .. })
        ));
        assert!(content_error_is_not_found(
            &store.read_manifest("object", "v1").unwrap_err()
        ));
        store
            .put_blob_only_manifest("object", "v1", &original.content_hash, original.size_bytes)
            .unwrap();
        assert_eq!(store.read_manifest("object", "v1").unwrap(), original);
    }

    #[test]
    fn blob_only_reuse_preserves_existing_extent_layout_and_intrinsic_empty_contract() {
        let (_root, _owner, store) = active_store("dfs-inline-blob-stage-extent-empty");
        store.put_object_version("object", "v1", b"abcdef").unwrap();
        let patch = store
            .put_object_version_patch("object", "v1", "v2", 1, b"B")
            .unwrap();
        assert!(!patch.extents.is_empty());
        assert_eq!(
            store
                .put_object_version_blob_only("object", "v2", b"aBcdef")
                .unwrap(),
            patch
        );
        assert!(
            store
                .inline_blob_entry(&patch.content_hash)
                .unwrap()
                .is_none()
        );
        let before = sequence(&store);
        store
            .put_object_version_blob_only("empty", "v1", &[])
            .unwrap();
        assert_eq!(sequence(&store), before);
        require_absent(&store, "empty", "v1");
        store
            .put_blob_only_manifest("empty", "v1", intrinsic_empty_content_hash(), 0)
            .unwrap();
        assert_eq!(store.read_object_version("empty", "v1").unwrap().1, b"");
    }
    #[test]
    fn complete_unconfirmed_blob_stage_requires_recertification_before_retry_ack() {
        for fresh_owner in [false, true] {
            let (_root, _owner, store) = active_store(&format!(
                "dfs-inline-blob-stage-lost-completion-{fresh_owner}"
            ));
            let root = store.root.clone();
            INLINE_FAULT.with(|fault| {
                *fault.borrow_mut() = Some((root.clone(), InlineFault::LostAppendCompletion))
            });
            assert!(
                store
                    .put_object_version_blob_only("object", "v1", b"complete but unconfirmed")
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
            require_absent(&store, "object", "v1");
            INLINE_FAULT.with(|fault| {
                *fault.borrow_mut() = Some((root, InlineFault::CatalogCertification))
            });
            assert!(
                store
                    .put_object_version_blob_only("object", "v1", b"complete but unconfirmed")
                    .is_err()
            );
            assert_eq!(
                fs::read(&path).unwrap(),
                persisted,
                "failed recertification must not append a retry receipt"
            );
            let staged = store
                .put_object_version_blob_only("object", "v1", b"complete but unconfirmed")
                .unwrap();
            assert_eq!(
                store.read_blob(&staged.content_hash).unwrap(),
                b"complete but unconfirmed"
            );
            assert_eq!(
                sequence(&store),
                2,
                "one surviving StageBlob plus one certified renewal"
            );
            require_absent(&store, "object", "v1");
            assert!(
                store
                    .content_reservation_roots()
                    .unwrap()
                    .reservations
                    .iter()
                    .any(|record| record.identity == identity(&staged))
            );
        }
    }
}
