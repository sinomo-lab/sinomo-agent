//! Generic current-format import coverage. No legacy filenames or codecs live here.
use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "state", rename_all = "kebab-case", deny_unknown_fields)]
pub(super) enum ContentInventoryState {
    Pending { identity: String },
    Complete { identity: String },
}

impl ContentInventoryState {
    pub(super) fn valid_identity(&self) -> bool {
        let identity = match self {
            Self::Pending { identity } | Self::Complete { identity } => identity,
        };
        !identity.is_empty() && identity.len() <= 1024 && !identity.contains('\0')
    }
}

pub(super) fn require_pending_inventory(
    sentinel: &ContentStoreReservationBatchSentinel,
    identity: &str,
) -> Result<()> {
    if !matches!(&sentinel.inventory, Some(ContentInventoryState::Pending { identity: actual }) if actual == identity)
    {
        return Err(ContentStoreError::InvalidContentLayout {
            message: "current content import no longer owns its exact pending inventory".into(),
        });
    }
    Ok(())
}

fn pending(root: &Path) -> ContentStoreError {
    ContentStoreError::StoragePreparationPending {
        root: root.to_owned(),
        processed_records: 0,
    }
}

fn preparation_error(error: crate::MetadataBackendError) -> ContentStoreError {
    match error {
        crate::MetadataBackendError::StoragePreparationPending {
            root,
            processed_records,
        } => ContentStoreError::StoragePreparationPending {
            root,
            processed_records,
        },
        crate::MetadataBackendError::Io { path, source } => ContentStoreError::Io { path, source },
        error => ContentStoreError::InvalidContentLayout {
            message: error.to_string(),
        },
    }
}

impl LocalContentStore {
    /// Opaque current-root identity. It grants no authority and does not imply
    /// complete inventory; import checkpoints bind it to detect root replacement.
    pub fn store_incarnation(&self) -> Result<String> {
        let _owner = self.content_mutation_guard()?;
        self.inline_sentinel_unlocked()?
            .map(|sentinel| sentinel.store_incarnation)
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: "current content store has no incarnation header".into(),
            })
    }

    /// Current coverage DATA, not authority to release references or collect bytes.
    pub fn inventory_import_pending(&self) -> Result<Option<String>> {
        let _owner = self.content_mutation_guard()?;
        Ok(self
            .inline_sentinel_unlocked()?
            .and_then(|sentinel| match sentinel.inventory {
                Some(ContentInventoryState::Pending { identity }) => Some(identity),
                _ => None,
            }))
    }

    /// Observe exact completed coverage without changing it. Fresh current
    /// storage with no import lineage does not prove any requested import.
    pub fn inventory_import_completed(&self, identity: &str) -> Result<bool> {
        let _owner = self.content_mutation_guard()?;
        Ok(self.inline_sentinel_unlocked()?.is_some_and(|sentinel|
            matches!(sentinel.inventory, Some(ContentInventoryState::Complete { identity: actual }) if actual == identity)))
    }

    /// Mark the exact current target before exposing a partial import. The
    /// migration owner retains its original source and durable inventory job.
    #[cfg(feature = "storage-conversion")]
    pub fn begin_inventory_import(&self, identity: &str) -> Result<()> {
        if identity.is_empty() || identity.len() > 1024 || identity.contains('\0') {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "invalid current content inventory identity".into(),
            });
        }
        let _owner = self.content_mutation_guard()?;
        let mut guard = self.lock_content_reservations_for_write()?;
        match &guard.sentinel.inventory {
            Some(ContentInventoryState::Pending { identity: actual }) if actual == identity => {
                return Ok(());
            }
            Some(ContentInventoryState::Pending { .. }) => {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: "another content inventory owns this current generation".into(),
                });
            }
            Some(ContentInventoryState::Complete { identity: actual }) if actual == identity => {
                return Ok(());
            }
            Some(ContentInventoryState::Complete { .. }) => {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: "a completed content generation cannot adopt another import lineage"
                        .into(),
                });
            }
            None => {}
        }
        guard.sentinel.inventory = Some(ContentInventoryState::Pending {
            identity: identity.to_owned(),
        });
        self.publish_inventory_state(&mut guard)
    }

    /// The owning migration must first verify its entire original inventory.
    /// This updates representation coverage only; destructive GC still consumes
    /// its separate native complete-catalog seal and retention evidence.
    #[cfg(feature = "storage-conversion")]
    pub fn complete_inventory_import(&self, identity: &str) -> Result<()> {
        let _owner = self.content_mutation_guard()?;
        let mut guard = self.lock_content_reservations_for_write()?;
        match &guard.sentinel.inventory {
            Some(ContentInventoryState::Complete { identity: actual }) if actual == identity => {
                return Ok(());
            }
            Some(ContentInventoryState::Pending { identity: actual }) if actual == identity => {}
            _ => {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: "content inventory completion changed its original identity".into(),
                });
            }
        }
        guard.sentinel.inventory = Some(ContentInventoryState::Complete {
            identity: identity.to_owned(),
        });
        self.publish_inventory_state(&mut guard)
    }

    #[cfg(feature = "storage-conversion")]
    fn publish_inventory_state(&self, guard: &mut ContentReservationWriteGuard) -> Result<()> {
        guard.sentinel.catalog_scan_epoch = guard
            .sentinel
            .catalog_scan_epoch
            .checked_add(1)
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: "content inventory causal epoch exhausted".into(),
            })?;
        write_file_atomic(
            &self.content_reservation_batch_sentinel_path(),
            &encode_content_reservation_batch_sentinel(&guard.sentinel)?,
        )
    }

    fn request_content_preparation(&self, scope: crate::DfsStoragePreparationScope) -> Result<()> {
        // No content mutation/reservation guard is retained across this boundary.
        // The callback schedules/observes the existing owner; it never converts.
        crate::storage_preparation::prepare_storage_root_before_open(&self.root, &scope)
            .map_err(preparation_error)
    }

    pub(super) fn prepare_version_if_missing(&self, object_id: &str, version: &str) -> Result<()> {
        let needed = {
            let _owner = self.content_mutation_guard()?;
            if self.inline_version_state(object_id, version)?.is_some() {
                return Ok(());
            }
            self.inventory_import_pending()?.is_some()
        };
        if needed {
            self.request_content_preparation(crate::DfsStoragePreparationScope::ContentVersion {
                object_id: object_id.to_owned(),
                version: version.to_owned(),
            })?;
        }
        Ok(())
    }

    pub(super) fn prepare_blob_if_missing(&self, hash: &str) -> Result<()> {
        if hash == intrinsic_empty_content_hash() {
            return Ok(());
        }
        let needed = {
            let _owner = self.content_mutation_guard()?;
            if self.inline_blob_entry(hash)?.is_some()
                || self.inline_blob_deleted(hash)?
                || self.blob_path(hash)?.is_file()
            {
                return Ok(());
            }
            self.inventory_import_pending()?.is_some()
        };
        if needed {
            self.request_content_preparation(crate::DfsStoragePreparationScope::ContentBlob {
                hash: hash.to_owned(),
            })?;
        }
        Ok(())
    }

    /// Recheck after acquiring the existing content owner. Scheduling is
    /// outside that lock; a new import must not race destructive work after
    /// its earlier readiness observation.
    pub(super) fn require_complete_inventory_under_owner(&self) -> Result<()> {
        if self.inline_sentinel_unlocked()?.is_some_and(|sentinel| {
            matches!(
                sentinel.inventory,
                Some(ContentInventoryState::Pending { .. })
            )
        }) {
            return Err(pending(&self.root));
        }
        Ok(())
    }

    pub(super) fn require_complete_content_inventory(&self) -> Result<()> {
        if self.inventory_import_pending()?.is_none() {
            return Ok(());
        }
        self.request_content_preparation(crate::DfsStoragePreparationScope::ContentInventory)?;
        // Callback Ready alone cannot erase persisted incomplete coverage.
        if self.inventory_import_pending()?.is_some() {
            return Err(pending(&self.root));
        }
        Ok(())
    }

    pub(super) fn prepare_content_object_inventory(&self, object_id: &str) -> Result<()> {
        if self.inventory_import_pending()?.is_some() {
            self.request_content_preparation(crate::DfsStoragePreparationScope::ContentObject {
                object_id: object_id.to_owned(),
            })?;
        }
        Ok(())
    }

    pub(super) fn prepare_patch_units(
        &self,
        object_id: &str,
        base: &str,
        target: &str,
        identity: Option<&ContentVersionIdentity>,
    ) -> Result<()> {
        self.prepare_version_if_missing(object_id, base)?;
        self.prepare_version_if_missing(object_id, target)?;
        if let Some(identity) = identity {
            let current = {
                let _owner = self.content_mutation_guard()?;
                matches!(
                    self.inline_version_state(object_id, base)?,
                    Some(InlineVersionState::Live(_))
                )
            };
            if !current {
                self.prepare_blob_if_missing(&identity.content_hash)?;
            }
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "storage-conversion"))]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn ordinary_current_open_rejects_old_or_malformed_inventory_headers_without_rewriting() {
        for old in [true, false] {
            let root = sinomo_testdir::TestDir::new("content-current-header-only");
            let store = LocalContentStore::new(&root).unwrap();
            let path = store.content_reservation_batch_sentinel_path();
            let mut sentinel = store.inline_sentinel_unlocked().unwrap().unwrap();
            if old {
                sentinel.format_version = 3;
            } else {
                sentinel.inventory = Some(ContentInventoryState::Pending {
                    identity: String::new(),
                });
            }
            let bytes = encode_content_reservation_batch_sentinel(&sentinel).unwrap();
            fs::write(&path, &bytes).unwrap();
            drop(store);
            assert!(LocalContentStore::new(&root).is_err());
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }

    #[test]
    fn partial_inventory_keeps_current_reads_local_and_unknown_versions_pending_after_restart() {
        let root = sinomo_testdir::TestDir::new("partial-content-current-read");
        let store = LocalContentStore::new(&root).unwrap();
        store
            .put_object_version("object", "current", b"current bytes")
            .unwrap();
        store.begin_inventory_import("original-cut").unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let _registration = crate::register_dfs_storage_preparation_provider(
            root.to_path_buf(),
            Arc::new(move |_, scope, _| {
                if matches!(scope, crate::DfsStoragePreparationScope::Content) {
                    return Ok(crate::DfsStoragePreparation::Ready);
                }
                seen.fetch_add(1, Ordering::Relaxed);
                Ok(crate::DfsStoragePreparation::Pending {
                    processed_records: 0,
                })
            }),
            None,
        )
        .unwrap();
        assert_eq!(
            store.read_object_version("object", "current").unwrap().1,
            b"current bytes"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert!(matches!(
            store.read_manifest("object", "unknown"),
            Err(ContentStoreError::StoragePreparationPending { .. })
        ));
        drop(store);
        let reopened = LocalContentStore::new(&root).unwrap();
        assert_eq!(
            reopened.inventory_import_pending().unwrap().as_deref(),
            Some("original-cut")
        );
        assert!(matches!(
            reopened.read_manifest("object", "unknown"),
            Err(ContentStoreError::StoragePreparationPending { .. })
        ));
        assert_eq!(
            reopened.read_object_version("object", "current").unwrap().1,
            b"current bytes"
        );
    }

    #[test]
    fn scheduling_callback_holds_no_content_or_reservation_lock_and_cannot_clear_inventory() {
        let root = sinomo_testdir::TestDir::new("content-demand-outside-owner");
        let store = LocalContentStore::new(&root).unwrap();
        store.begin_inventory_import("original-cut").unwrap();
        let callback_store = store.clone();
        let _registration = crate::register_dfs_storage_preparation_provider(
            root.to_path_buf(),
            Arc::new(move |_, _, _| {
                // A different thread must acquire both locks while this callback is
                // on the caller stack; a recursive mutex would conceal a violation.
                let store = callback_store.clone();
                let (tx, rx) = std::sync::mpsc::channel();
                let worker = std::thread::spawn(move || {
                    let _owner = store.content_mutation_guard().unwrap();
                    let _reservation = store.lock_content_reservations_for_write().unwrap();
                    tx.send(()).unwrap();
                });
                rx.recv_timeout(Duration::from_secs(5))
                    .expect("demand callback retained a content lock");
                worker.join().unwrap();
                Ok(crate::DfsStoragePreparation::Ready)
            }),
            None,
        )
        .unwrap();
        assert!(matches!(
            store.require_complete_content_inventory(),
            Err(ContentStoreError::StoragePreparationPending { .. })
        ));
        assert!(matches!(
            store.begin_content_root_catalog_scan_at(1),
            Err(ContentStoreError::StoragePreparationPending { .. })
        ));
        assert!(store.complete_inventory_import("other-cut").is_err());
        assert_eq!(
            store.inventory_import_pending().unwrap().as_deref(),
            Some("original-cut")
        );
        assert!(!store.inventory_import_completed("original-cut").unwrap());
        store.complete_inventory_import("original-cut").unwrap();
        assert!(store.inventory_import_completed("original-cut").unwrap());
        assert!(!store.inventory_import_completed("other-cut").unwrap());
        store.require_complete_content_inventory().unwrap();
        assert!(store.begin_inventory_import("next-cut").is_err());
        assert!(store.complete_inventory_import("next-cut").is_err());
        assert_eq!(store.inventory_import_pending().unwrap(), None);
    }

    #[test]
    fn import_merge_preserves_current_tombstones_and_rejects_immutable_conflicts() {
        let root = sinomo_testdir::TestDir::new("content-import-current-wins");
        let store = LocalContentStore::new(&root).unwrap();
        let manifest = store
            .put_object_version("object", "version", b"current")
            .unwrap();
        store.begin_inventory_import("original-cut").unwrap();
        let writer = ContentStoreStagingWriter::open_import_target(&root, "original-cut").unwrap();
        writer
            .apply_storage_staging_row(ContentStoreStagingRow::Manifest {
                manifest: manifest.clone(),
                published_at_unix_nanos: 1,
            })
            .unwrap();
        let mut conflicting = manifest.clone();
        conflicting.size_bytes += 1;
        assert!(
            writer
                .apply_storage_staging_row(ContentStoreStagingRow::Manifest {
                    manifest: conflicting,
                    published_at_unix_nanos: 1
                })
                .is_err()
        );
        writer
            .apply_storage_staging_row(ContentStoreStagingRow::VersionTombstone {
                object_id: manifest.object_id.clone(),
                version: manifest.version.clone(),
                content_hash: manifest.content_hash.clone(),
            })
            .unwrap();
        assert_eq!(store.read_manifest("object", "version").unwrap(), manifest);
        writer
            .apply_storage_staging_row(ContentStoreStagingRow::VersionTombstone {
                object_id: "deleted".into(),
                version: "old".into(),
                content_hash: manifest.content_hash.clone(),
            })
            .unwrap();
        let mut deleted = manifest.clone();
        deleted.object_id = "deleted".into();
        deleted.version = "old".into();
        writer
            .apply_storage_staging_row(ContentStoreStagingRow::Manifest {
                manifest: deleted,
                published_at_unix_nanos: 1,
            })
            .unwrap();
        assert!(store.object_version_is_deleted("deleted", "old").unwrap());
        store.complete_inventory_import("original-cut").unwrap();
        assert!(
            writer
                .apply_storage_staging_row(ContentStoreStagingRow::Manifest {
                    manifest,
                    published_at_unix_nanos: 1
                })
                .is_err()
        );
    }
}
