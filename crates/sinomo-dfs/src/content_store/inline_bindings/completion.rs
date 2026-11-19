//! Reuse a durable binding only at canonical current-write completion.
//!
//! Generic staging remains a renewal. This seam additionally requires the
//! response handler's current-publication proof and the live reservation's
//! exact current catalog cut. An old scan must not consume a reservation whose
//! causal stamp has advanced.

use super::*;
use crate::service_routed_write::CanonicalCurrentWriteCompletion;

impl LocalContentStore {
    pub(crate) fn try_complete_canonical_current_inline_manifest(
        &self,
        proof: &CanonicalCurrentWriteCompletion,
    ) -> Result<Option<ContentManifest>> {
        self.try_complete_current_inline_manifest(proof.manifest())
    }

    fn try_complete_current_inline_manifest(
        &self,
        manifest: &ContentManifest,
    ) -> Result<Option<ContentManifest>> {
        let _owner = self.content_mutation_guard()?;
        if self.inline_sentinel_unlocked()?.is_none()
            || !manifest.extents.is_empty()
            || manifest.size_bytes > INLINE_BLOB_SMALL_LIMIT_BYTES
            || manifest.content_hash == intrinsic_empty_content_hash()
        {
            return Ok(None);
        }
        let reservation = self.lock_content_reservations_for_write()?;
        let stamp = reservation.sentinel.stamp();
        let identity = ContentStoreReservationIdentity {
            object_id: manifest.object_id.clone(),
            version: manifest.version.clone(),
            content_hash: manifest.content_hash.clone(),
        };
        let blob = {
            let mut state = self
                .inline_store
                .owner
                .state
                .lock()
                .map_err(|_| inline_error("resident binding mutex poisoned"))?;
            // Decoding a surviving uncertain tail is not durable evidence.
            // This also recertifies catalog publication after owner replacement.
            ensure_writer(
                &self.inline_store.owner.root,
                &reservation.sentinel.store_incarnation,
                &mut state,
            )?;
            if state
                .view
                .live_manifest(&manifest.object_id, &manifest.version)
                != Some(manifest)
                || !matches!(state.view.reservations.get(&identity),
                    Some(InlineReservationState::Live(record)) if record.causal_stamp == stamp)
                || state.view.deleted_blobs.contains(&manifest.content_hash)
            {
                return Ok(None);
            }
            let Some(blob) = state.view.blobs.get(&manifest.content_hash) else {
                return Ok(None);
            };
            if blob.size_bytes != manifest.size_bytes {
                return Ok(None);
            }
            blob.clone()
        };
        // Keep both owner and reservation fence through completion. The live
        // reservation was checked against this exact stamp under the resident
        // mutex above; these locks prevent a renewal or release in between.
        // The certified frame owns this exact payload. Check the bounded bytes
        // as well: a missing or damaged source must not turn completion into a
        // successful local publication. The mutation owner pins the generation.
        let mut file = match fs::File::open(&blob.path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(ContentStoreError::Io {
                    path: blob.path,
                    source,
                });
            }
        };
        let mut bytes = vec![0; manifest.size_bytes as usize];
        file.seek(SeekFrom::Start(blob.payload_offset))
            .and_then(|_| file.read_exact(&mut bytes))
            .map_err(|source| ContentStoreError::Io {
                path: blob.path,
                source,
            })?;
        ensure_bytes_match_hash(&manifest.content_hash, &bytes)?;
        self.remove_manifest_absence_observation(&identity)?;
        self.clear_orphan_observations_before_manifest_publish(std::iter::once(manifest))?;
        Ok(Some(manifest.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::active_store;
    use super::*;

    fn identity(manifest: &ContentManifest) -> ContentStoreReservationIdentity {
        ContentStoreReservationIdentity {
            object_id: manifest.object_id.clone(),
            version: manifest.version.clone(),
            content_hash: manifest.content_hash.clone(),
        }
    }

    fn sequence(store: &LocalContentStore) -> u64 {
        store.inline_binding_view().unwrap().unwrap().sequence
    }

    #[test]
    fn canonical_completion_reuses_exact_binding_and_clears_observations() {
        let (_root, _owner, store) = active_store("dfs-inline-completion-exact");
        let manifest = store
            .put_owner_write_content("object", "v1", b"completed content")
            .unwrap();
        assert_eq!(
            sequence(&store),
            1,
            "one frame binds the owner version, reservation and bytes"
        );
        let original = store.content_reservation_roots().unwrap().reservations;
        let absence = store.manifest_absence_observation_path(&identity(&manifest));
        let orphan = store
            .orphan_observation_path(&manifest.content_hash)
            .unwrap();
        for path in [&absence, &orphan] {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"old observation").unwrap();
        }
        let before = sequence(&store);
        assert_eq!(
            store
                .try_complete_current_inline_manifest(&manifest)
                .unwrap(),
            Some(manifest.clone())
        );
        assert_eq!(
            sequence(&store),
            before,
            "completion must not append another reservation"
        );
        assert_eq!(
            store.content_reservation_roots().unwrap().reservations,
            original
        );
        assert!(!absence.exists());
        assert!(!orphan.exists());
        assert_eq!(
            store.read_object_version("object", "v1").unwrap().1,
            b"completed content"
        );
        store
            .put_blob_only_manifest("object", "v1", &manifest.content_hash, manifest.size_bytes)
            .unwrap();
        assert!(
            sequence(&store) > before,
            "generic publication remains a renewal"
        );
    }

    #[test]
    fn canonical_completion_recertifies_uncertain_and_reopened_writers_before_success() {
        let (_root, _owner, store) = active_store("dfs-inline-completion-recertify");
        let manifest = store
            .put_object_version("object", "v1", b"durable bytes")
            .unwrap();
        let before = sequence(&store);
        let canonical_root = store.inline_store.owner.root.clone();
        store.inline_store.owner.state.lock().unwrap().uncertain = true;
        INLINE_FAULT.with(|fault| {
            *fault.borrow_mut() = Some((canonical_root, InlineFault::CatalogCertification))
        });
        assert!(
            store
                .try_complete_current_inline_manifest(&manifest)
                .unwrap_err()
                .to_string()
                .contains("recertification")
        );
        assert!(store.inline_store.owner.state.lock().unwrap().uncertain);
        assert_eq!(
            store
                .try_complete_current_inline_manifest(&manifest)
                .unwrap(),
            Some(manifest.clone())
        );
        assert!(!store.inline_store.owner.state.lock().unwrap().uncertain);
        assert_eq!(sequence(&store), before);
        let root = store.root.clone();
        drop(store);
        let reopened = LocalContentStore::new(root).unwrap();
        assert_eq!(
            reopened
                .try_complete_current_inline_manifest(&manifest)
                .unwrap(),
            Some(manifest)
        );
        assert_eq!(sequence(&reopened), before);
        assert!(!reopened.inline_store.owner.state.lock().unwrap().uncertain);
    }

    #[test]
    fn canonical_completion_after_catalog_cut_requires_generic_renewal() {
        let (_root, _owner, store) = active_store("dfs-inline-completion-post-cut");
        let manifest = store
            .put_object_version("object", "v1", b"newly committed bytes")
            .unwrap();
        let before = sequence(&store);
        store.begin_content_root_catalog_scan().unwrap();
        assert_eq!(
            store
                .try_complete_current_inline_manifest(&manifest)
                .unwrap(),
            None
        );
        assert_eq!(sequence(&store), before);
        store
            .put_blob_only_manifest("object", "v1", &manifest.content_hash, manifest.size_bytes)
            .unwrap();
        let renewed = sequence(&store);
        assert!(renewed > before);
        assert_eq!(
            store
                .try_complete_current_inline_manifest(&manifest)
                .unwrap(),
            Some(manifest)
        );
        assert_eq!(sequence(&store), renewed);
    }

    #[test]
    fn canonical_completion_rejects_invalid_provenance_and_requires_current_cut() {
        let (_root, _owner, store) = active_store("dfs-inline-completion-provenance");
        store.begin_content_root_catalog_scan().unwrap();
        let manifest = store
            .put_object_version("object", "v1", b"same tuple")
            .unwrap();
        let original = store.content_reservation_roots().unwrap().reservations[0].clone();
        for stamp in [ContentStoreCausalStamp {
            store_incarnation: sinomo_core::Id::generate().to_string(),
            catalog_scan_epoch: 0,
        }] {
            let before = sequence(&store);
            {
                let _owner = store.content_mutation_guard().unwrap();
                let reservation = store.lock_content_reservations_for_write().unwrap();
                let mut invalid = original.clone();
                invalid.causal_stamp = stamp;
                assert!(
                    store
                        .inline_reserve_owned_identity_unlocked(&invalid, &reservation.sentinel)
                        .is_err()
                );
            }
            assert_eq!(sequence(&store), before);
            assert_eq!(
                store
                    .try_complete_current_inline_manifest(&manifest)
                    .unwrap(),
                Some(manifest.clone())
            );
        }
        store.begin_content_root_catalog_scan().unwrap();
        let before = sequence(&store);
        assert_eq!(
            store
                .try_complete_current_inline_manifest(&manifest)
                .unwrap(),
            None
        );
        assert_eq!(sequence(&store), before);
        store
            .put_blob_only_manifest("object", "v1", &manifest.content_hash, manifest.size_bytes)
            .unwrap();
        assert!(sequence(&store) > before);
        assert_eq!(
            store
                .try_complete_current_inline_manifest(&manifest)
                .unwrap(),
            Some(manifest)
        );
    }

    #[test]
    fn canonical_completion_requires_live_inline_reservation_and_manifest() {
        let (_root, _owner, store) = active_store("dfs-inline-completion-released");
        let manifest = store
            .put_object_version("object", "v1", b"retained payload")
            .unwrap();
        assert!(
            store
                .release_content_reservation_for_test("object", "v1", &manifest.content_hash)
                .unwrap()
        );
        let before = sequence(&store);
        assert_eq!(
            store
                .try_complete_current_inline_manifest(&manifest)
                .unwrap(),
            None
        );
        assert_eq!(sequence(&store), before);
        store
            .put_blob_only_manifest("object", "v1", &manifest.content_hash, manifest.size_bytes)
            .unwrap();
        assert_eq!(
            store
                .try_complete_current_inline_manifest(&manifest)
                .unwrap(),
            Some(manifest.clone())
        );
        {
            let _owner = store.content_mutation_guard().unwrap();
            store
                .inline_delete_version_unlocked("object", "v1", &manifest.content_hash)
                .unwrap();
        }
        let deleted = sequence(&store);
        assert_eq!(
            store
                .try_complete_current_inline_manifest(&manifest)
                .unwrap(),
            None
        );
        assert_eq!(sequence(&store), deleted);
    }

    #[test]
    fn canonical_completion_external_binding_takes_generic_renewal() {
        let root = sinomo_testdir::TestDir::new("dfs-inline-completion-legacy");
        let store = LocalContentStore::new(root.path()).unwrap();
        let manifest = store
            .put_object_version(
                "object",
                "v1",
                &vec![b'x'; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1],
            )
            .unwrap();
        assert_eq!(
            store
                .try_complete_current_inline_manifest(&manifest)
                .unwrap(),
            None
        );
        store
            .put_blob_only_manifest("object", "v1", &manifest.content_hash, manifest.size_bytes)
            .unwrap();
        assert_eq!(
            store.read_object_version("object", "v1").unwrap().1,
            vec![b'x'; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1]
        );
    }
}
