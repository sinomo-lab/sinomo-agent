use super::*;

/// Open and verify one bounded authoritative inline payload. Capturing a lazy
/// read snapshot does not call this; the payload is checked when its file is
/// actually opened. The returned descriptor is positioned at payload start.
pub(in crate::content_store) fn open_verified_inline_payload(
    path: &Path,
    payload_offset: u64,
    size_bytes: u64,
    content_hash: &str,
) -> Result<fs::File> {
    if size_bytes > INLINE_BLOB_SMALL_LIMIT_BYTES {
        return Err(inline_error(
            "inline payload exceeds its bounded representation",
        ));
    }
    let mut file = fs::File::open(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    let metadata = file.metadata().map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    if !metadata.is_file()
        || payload_offset
            .checked_add(size_bytes)
            .is_none_or(|end| end > metadata.len())
    {
        return Err(inline_error(
            "inline payload locator exceeds its retained segment",
        ));
    }
    let mut bytes = vec![0; size_bytes as usize];
    file.seek(SeekFrom::Start(payload_offset))
        .and_then(|_| file.read_exact(&mut bytes))
        .map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?;
    ensure_bytes_match_hash(content_hash, &bytes)?;
    file.seek(SeekFrom::Start(payload_offset))
        .map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?;
    Ok(file)
}

impl LocalContentStore {
    pub(in crate::content_store) fn inline_sentinel_unlocked(
        &self,
    ) -> Result<Option<ContentStoreReservationBatchSentinel>> {
        let path = self.content_reservation_batch_sentinel_path();
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source)
                if source.kind() == io::ErrorKind::NotFound
                    && !self.content_reservation_writer_initialized =>
            {
                if self.root.exists()
                    && fs::read_dir(&self.root)
                        .map_err(|source| ContentStoreError::Io {
                            path: self.root.clone(),
                            source,
                        })?
                        .next()
                        .is_some()
                {
                    return Err(ContentStoreError::UnsupportedStorageFormat {
                        root: self.root.clone(),
                    });
                }
                return Ok(None);
            }
            Err(source) => return Err(ContentStoreError::Io { path, source }),
        };
        let sentinel = decode_content_reservation_batch_sentinel(&bytes)?;
        validate_content_reservation_batch_sentinel(&sentinel)?;
        Ok((sentinel.content_binding_layout == INLINE_BINDING_LAYOUT).then_some(sentinel))
    }

    pub(in crate::content_store) fn inline_binding_view(
        &self,
    ) -> Result<Option<InlineBindingView>> {
        self.inline_binding_view_with_validation(false)
    }

    pub(in crate::content_store) fn inline_binding_view_verified(
        &self,
    ) -> Result<Option<InlineBindingView>> {
        self.inline_binding_view_with_validation(true)
    }

    fn inline_binding_view_with_validation(
        &self,
        force: bool,
    ) -> Result<Option<InlineBindingView>> {
        let _owner = self.content_mutation_guard()?;
        let Some(sentinel) = self.inline_sentinel_unlocked()? else {
            return Ok(None);
        };
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        refresh_resident(
            &self.inline_store.owner.root,
            &sentinel.store_incarnation,
            &mut state,
            force,
        )?;
        Ok(Some(state.view.clone()))
    }

    pub(in crate::content_store) fn inline_manifest_for_content(
        &self,
        object_id: &str,
        hash: &str,
        size: u64,
    ) -> Result<Option<ContentManifest>> {
        let _owner = self.content_mutation_guard()?;
        let Some(sentinel) = self.inline_sentinel_unlocked()? else {
            return Ok(None);
        };
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        refresh_resident(
            &self.inline_store.owner.root,
            &sentinel.store_incarnation,
            &mut state,
            false,
        )?;
        let manifest = state
            .view
            .content_versions
            .get(&(object_id.into(), hash.into(), size))
            .and_then(|versions| versions.last())
            .and_then(|version| state.view.live_manifest(object_id, version))
            .cloned();
        Ok(manifest)
    }

    pub(in crate::content_store) fn inline_version_state(
        &self,
        object_id: &str,
        version: &str,
    ) -> Result<Option<InlineVersionState>> {
        let _owner = self.content_mutation_guard()?;
        let Some(sentinel) = self.inline_sentinel_unlocked()? else {
            return Ok(None);
        };
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        refresh_resident(
            &self.inline_store.owner.root,
            &sentinel.store_incarnation,
            &mut state,
            false,
        )?;
        Ok(state
            .view
            .versions
            .get(&(object_id.to_owned(), version.to_owned()))
            .cloned())
    }

    pub(in crate::content_store) fn inline_blob_entry(
        &self,
        hash: &str,
    ) -> Result<Option<InlineBlobEntry>> {
        let _owner = self.content_mutation_guard()?;
        let Some(sentinel) = self.inline_sentinel_unlocked()? else {
            return Ok(None);
        };
        hash_hex(hash)?;
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        refresh_resident(
            &self.inline_store.owner.root,
            &sentinel.store_incarnation,
            &mut state,
            false,
        )?;
        Ok(state.view.blobs.get(hash).cloned())
    }

    pub(in crate::content_store) fn inline_blob_deleted(&self, hash: &str) -> Result<bool> {
        let _owner = self.content_mutation_guard()?;
        let Some(sentinel) = self.inline_sentinel_unlocked()? else {
            return Ok(false);
        };
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        refresh_resident(
            &self.inline_store.owner.root,
            &sentinel.store_incarnation,
            &mut state,
            false,
        )?;
        Ok(state.view.deleted_blobs.contains(hash))
    }

    pub(in crate::content_store) fn inline_remove_blob_unlocked(&self, hash: &str) -> Result<bool> {
        let Some(sentinel) = self.inline_sentinel_unlocked()? else {
            return Ok(false);
        };
        let _reservation = self.lock_content_reservations_for_write()?;
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        ensure_writer(
            &self.inline_store.owner.root,
            &sentinel.store_incarnation,
            &mut state,
        )?;
        if !state.view.blobs.contains_key(hash) {
            return Ok(false);
        }
        append_operation(
            &self.inline_store.owner.root,
            &sentinel,
            &mut state,
            InlineOperation::RemoveBlob {
                content_hash: hash.to_owned(),
            },
            &[],
        )?;
        Ok(true)
    }

    /// Re-adopt a hash that the authoritative overlay previously retired.
    /// This record intentionally has no version manifest: blob-only APIs keep
    /// their public contract while a stale stored copy cannot clear a tombstone.
    pub(in crate::content_store) fn try_restore_inline_blob(
        &self,
        hash: &str,
        bytes: &[u8],
    ) -> Result<bool> {
        let _owner = self.content_mutation_guard()?;
        if bytes.len() as u64 > INLINE_BLOB_SMALL_LIMIT_BYTES {
            return Ok(false);
        }
        ensure_bytes_match_hash(hash, bytes)?;
        if self.inline_blob_entry(hash)?.is_some() && self.open_inline_blob(hash, 0, 0)?.is_some() {
            return Ok(true);
        }
        self.ensure_quota_delta(ContentStoreQuotaDelta {
            blob_bytes: bytes.len() as u64,
            ..ContentStoreQuotaDelta::default()
        })?;
        let reservation = self.lock_content_reservations_for_write()?;
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        append_operation(
            &self.inline_store.owner.root,
            &reservation.sentinel,
            &mut state,
            InlineOperation::Blob {
                content_hash: hash.to_owned(),
                size_bytes: bytes.len() as u64,
                published_at_unix_nanos: system_time_nanos_since_epoch_saturating(SystemTime::now()),
            },
            bytes,
        )?;
        drop(state);
        self.apply_stats_delta(ContentStoreStatsDelta {
            blob_count: 1,
            blob_bytes: bytes.len() as i128,
            ..ContentStoreStatsDelta::default()
        })?;
        Ok(true)
    }

    pub(in crate::content_store) fn inline_reservation_state(
        &self,
        identity: &ContentStoreReservationIdentity,
    ) -> Result<Option<InlineReservationState>> {
        let _owner = self.content_mutation_guard()?;
        let Some(sentinel) = self.inline_sentinel_unlocked()? else {
            return Ok(None);
        };
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        refresh_resident(
            &self.inline_store.owner.root,
            &sentinel.store_incarnation,
            &mut state,
            false,
        )?;
        Ok(state.view.reservations.get(identity).cloned())
    }

    pub(in crate::content_store) fn open_inline_blob(
        &self,
        hash: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Option<(fs::File, u64, u64, InlineReaderLease)>> {
        let _owner = self.content_mutation_guard()?;
        let Some(entry) = self.inline_blob_entry(hash)? else {
            return Ok(None);
        };
        let lock_path =
            generation_root(&self.inline_store.owner.root, entry.generation).join("readers.lock");
        let generation_lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|source| ContentStoreError::Io {
                path: lock_path.clone(),
                source,
            })?;
        FileExt::lock_shared(&generation_lock).map_err(|source| ContentStoreError::Io {
            path: lock_path,
            source,
        })?;
        let mut file = open_verified_inline_payload(
            &entry.path,
            entry.payload_offset,
            entry.size_bytes,
            hash,
        )?;
        let start = offset.min(entry.size_bytes);
        file.seek(SeekFrom::Start(entry.payload_offset + start))
            .map_err(|source| ContentStoreError::Io {
                path: entry.path,
                source,
            })?;
        let remaining = entry.size_bytes.saturating_sub(start).min(max_bytes as u64);
        Ok(Some((
            file,
            entry.size_bytes,
            remaining,
            InlineReaderLease {
                _store: self.inline_store.clone(),
                generation_lock,
            },
        )))
    }

    pub(in crate::content_store) fn inline_check_manifest_conflict(
        &self,
        manifest: &ContentManifest,
    ) -> Result<()> {
        if let Some(prior) = self.inline_version_state(&manifest.object_id, &manifest.version)? {
            let existing_hash = version_hash(&prior);
            if existing_hash != manifest.content_hash {
                return Err(ContentStoreError::VersionHashConflict {
                    object_id: manifest.object_id.clone(),
                    version: manifest.version.clone(),
                    existing_hash: existing_hash.to_owned(),
                    new_hash: manifest.content_hash.clone(),
                });
            }
            if let InlineVersionState::Live(entry) = prior
                && entry.manifest.size_bytes != manifest.size_bytes
            {
                return Err(inline_error(
                    "same tuple/hash has a different declared size",
                ));
            }
        }
        Ok(())
    }

    pub(in crate::content_store) fn try_write_inline_binding(
        &self,
        object_id: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<Option<ContentManifest>> {
        if bytes.len() as u64 > INLINE_BLOB_SMALL_LIMIT_BYTES {
            return Ok(None);
        }
        let _owner = self.content_mutation_guard()?;
        let Some(_) = self.inline_sentinel_unlocked()? else {
            return Ok(None);
        };
        let reservation_guard = self.lock_content_reservations_for_write()?;
        let manifest = ContentManifest {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            content_hash: content_hash(bytes),
            size_bytes: bytes.len() as u64,
            extents: Vec::new(),
        };
        self.inline_check_manifest_conflict(&manifest)?;
        let existing = match self.read_manifest(object_id, version) {
            Ok(existing) => {
                if existing.content_hash != manifest.content_hash
                    || existing.size_bytes != manifest.size_bytes
                {
                    return Err(ContentStoreError::VersionHashConflict {
                        object_id: object_id.to_owned(),
                        version: version.to_owned(),
                        existing_hash: existing.content_hash,
                        new_hash: manifest.content_hash.clone(),
                    });
                }
                Some(existing)
            }
            Err(error) if content_error_is_not_found(&error) => None,
            Err(error) => return Err(error),
        };
        let published_at = match self.inline_version_state(object_id, version)? {
            Some(InlineVersionState::Live(entry)) => entry.published_at_unix_nanos,
            _ => system_time_nanos_since_epoch_saturating(SystemTime::now()),
        };
        let encoded_manifest_bytes = encode_manifest(&manifest)?.len() as u64;
        // Quota admission rebuilds a coherent union under the mutation owner.
        // A durable append followed by a stats error is rebuilt, never counted
        // twice by replaying an old delta on a newly reconstructed snapshot.
        self.quota_stats_initialized.store(false, Ordering::Release);
        let existing_blob_bytes = self
            .readable_blob_size(&manifest.content_hash)?
            .unwrap_or(0);
        self.ensure_quota_delta(ContentStoreQuotaDelta {
            blob_bytes: manifest.size_bytes.saturating_sub(existing_blob_bytes),
            logical_object_bytes: if existing.is_some() {
                0
            } else {
                manifest.size_bytes
            },
            manifest_bytes: if existing.is_some() {
                0
            } else {
                encoded_manifest_bytes
            },
            ..ContentStoreQuotaDelta::default()
        })?;
        self.clear_orphan_observations_before_manifest_publish(std::iter::once(&manifest))?;
        let identity = ContentStoreReservationIdentity {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            content_hash: manifest.content_hash.clone(),
        };
        // Generic staging/retry is a renewal even for an identical tuple. Clear
        // its old absence epoch and keep grace monotonic across clock rollback.
        self.remove_manifest_absence_observation(&identity)?;
        let previous_reservation_time =
            self.inline_reservation_state(&identity)?
                .and_then(|state| match state {
                    InlineReservationState::Live(record) => Some(record.reserved_at_unix_nanos),
                    InlineReservationState::Released => None,
                });
        let reservation = if manifest.content_hash == intrinsic_empty_content_hash() {
            None
        } else {
            Some(ContentStoreReservationRecord {
                identity,
                reserved_at_unix_nanos: refreshed_reservation_timestamp(previous_reservation_time),
                causal_stamp: reservation_guard.sentinel.stamp(),
            })
        };
        let reuse_payload = self.inline_blob_entry(&manifest.content_hash)?.is_some()
            && self.has_blob(&manifest.content_hash)?;
        let operation = if reuse_payload {
            InlineOperation::Link {
                manifest: manifest.clone(),
                published_at_unix_nanos: published_at,
                reservation,
            }
        } else {
            InlineOperation::Bind {
                manifest: manifest.clone(),
                published_at_unix_nanos: published_at,
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
        self.evict_stream_manifest(&self.manifest_path(object_id, version));
        Ok(Some(manifest))
    }

    /// Caller holds the mutation owner and reservation write flock.
    pub(in crate::content_store) fn inline_reserve_owned_identity_unlocked(
        &self,
        record: &ContentStoreReservationRecord,
        sentinel: &ContentStoreReservationBatchSentinel,
    ) -> Result<bool> {
        if sentinel.content_binding_layout != INLINE_BINDING_LAYOUT {
            return Ok(false);
        }
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        ensure_writer(
            &self.inline_store.owner.root,
            &sentinel.store_incarnation,
            &mut state,
        )?;
        let mut renewed = record.clone();
        if let Some(InlineReservationState::Live(previous)) =
            state.view.reservations.get(&record.identity)
        {
            renewed.reserved_at_unix_nanos = refreshed_reservation_timestamp(Some(
                previous
                    .reserved_at_unix_nanos
                    .max(record.reserved_at_unix_nanos),
            ));
        }
        append_operation(
            &self.inline_store.owner.root,
            sentinel,
            &mut state,
            InlineOperation::Reserve(renewed),
            &[],
        )?;
        Ok(true)
    }

    /// Caller proved catalog coverage/absence under both ownership locks.
    pub(in crate::content_store) fn inline_release_reservations_unlocked(
        &self,
        identities: &BTreeSet<ContentStoreReservationIdentity>,
    ) -> Result<()> {
        let Some(sentinel) = self.inline_sentinel_unlocked()? else {
            return Ok(());
        };
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        for identity in identities {
            append_operation(
                &self.inline_store.owner.root,
                &sentinel,
                &mut state,
                InlineOperation::Release(identity.clone()),
                &[],
            )?;
        }
        Ok(())
    }

    pub(in crate::content_store) fn inline_delete_version_unlocked(
        &self,
        object_id: &str,
        version: &str,
        hash: &str,
    ) -> Result<bool> {
        let Some(sentinel) = self.inline_sentinel_unlocked()? else {
            return Ok(false);
        };
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        append_operation(
            &self.inline_store.owner.root,
            &sentinel,
            &mut state,
            InlineOperation::DeleteVersion {
                object_id: object_id.to_owned(),
                version: version.to_owned(),
                content_hash: hash.to_owned(),
            },
            &[],
        )?;
        drop(state);
        self.evict_stream_manifest(&self.manifest_path(object_id, version));
        Ok(true)
    }

    pub(in crate::content_store) fn inline_manifest_published(
        &self,
        manifest: &ContentManifest,
    ) -> Result<()> {
        let _owner = self.content_mutation_guard()?;
        let reservation = self.lock_content_reservations_for_write()?;
        let identity = ContentStoreReservationIdentity {
            object_id: manifest.object_id.clone(),
            version: manifest.version.clone(),
            content_hash: manifest.content_hash.clone(),
        };
        let record = if manifest.content_hash == intrinsic_empty_content_hash() {
            None
        } else {
            Some(ContentStoreReservationRecord {
                identity: identity.clone(),
                reserved_at_unix_nanos: refreshed_reservation_timestamp(
                    self.inline_reservation_state(&identity)?
                        .and_then(|state| match state {
                            InlineReservationState::Live(record) => {
                                Some(record.reserved_at_unix_nanos)
                            }
                            InlineReservationState::Released => None,
                        }),
                ),
                causal_stamp: reservation.sentinel.stamp(),
            })
        };
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        append_operation(
            &self.inline_store.owner.root,
            &reservation.sentinel,
            &mut state,
            InlineOperation::ExternalBinding {
                manifest: manifest.clone(),
                published_at_unix_nanos: system_time_nanos_since_epoch_saturating(SystemTime::now()),
                reservation: record,
            },
            &[],
        )?;
        drop(state);
        self.evict_stream_manifest(&self.manifest_path(&manifest.object_id, &manifest.version));
        Ok(())
    }
}

#[cfg(feature = "storage-conversion")]
pub struct ContentStoreStagingWriter {
    store: LocalContentStore,
    _owner: fs::File,
}

#[cfg(feature = "storage-conversion")]
impl std::ops::Deref for ContentStoreStagingWriter {
    type Target = LocalContentStore;
    fn deref(&self) -> &LocalContentStore {
        &self.store
    }
}

#[cfg(feature = "storage-conversion")]
impl ContentStoreStagingWriter {
    /// Opens an existing candidate or begins from an empty current catalog.
    /// A populated ordinary store cannot acquire this staging capability.
    pub fn open_candidate(root: &Path) -> Result<Self> {
        // This capability constructs only current output. Invoking the source
        // preparation provider here recursively enters its own converter.
        // The shared initializer still rejects every unmarked nonempty/legacy
        // root and validates any retained current initialization identity.
        let store = LocalContentStore::initialize_current(root.to_owned())?;
        let path = store.root.join("catalog-staging-owner.lock");
        let existed = path.is_file();
        if !existed
            && (store
                .inline_binding_view()?
                .is_some_and(|view| view.sequence != 0)
                || store.scan_blob_stats()?.0 != 0)
        {
            return Err(inline_error(
                "an ordinary populated catalog cannot become a staging candidate",
            ));
        }
        let owner = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(!existed)
            .truncate(false)
            .open(&path)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        owner
            .try_lock_exclusive()
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        owner.sync_all().map_err(|source| ContentStoreError::Io {
            path: path.clone(),
            source,
        })?;
        sync_dir(&store.root)?;
        Ok(Self {
            store,
            _owner: owner,
        })
    }
}

/// Current catalog rows for constructing a private conversion candidate.
/// Every row is appended through the normal owner, stamp, and framed writer.
#[cfg(feature = "storage-conversion")]
#[derive(Clone, Debug)]
pub enum ContentStoreStagingRow {
    Manifest {
        manifest: ContentManifest,
        published_at_unix_nanos: u64,
    },
    VersionTombstone {
        object_id: String,
        version: String,
        content_hash: String,
    },
    Blob {
        content_hash: String,
        bytes: Vec<u8>,
        published_at_unix_nanos: u64,
    },
    BlobTombstone {
        content_hash: String,
    },
    RetentionRoot {
        identity: ContentStoreReservationIdentity,
        not_before: u64,
    },
    RetentionRelease(ContentStoreReservationIdentity),
}

#[cfg(feature = "storage-conversion")]
impl ContentStoreStagingWriter {
    pub fn apply_storage_staging_row(&self, row: ContentStoreStagingRow) -> Result<()> {
        let _owner = self.content_mutation_guard()?;
        if let ContentStoreStagingRow::RetentionRoot {
            identity,
            not_before,
        } = &row
        {
            return self.ensure_content_reservation_not_before(identity, *not_before);
        }
        let reservation = self.lock_content_reservations_for_write()?;
        let cache_key = match &row {
            ContentStoreStagingRow::Manifest { manifest, .. } => {
                Some(self.manifest_path(&manifest.object_id, &manifest.version))
            }
            ContentStoreStagingRow::VersionTombstone {
                object_id, version, ..
            } => Some(self.manifest_path(object_id, version)),
            _ => None,
        };
        let (operation, payload) = match row {
            ContentStoreStagingRow::Manifest {
                manifest,
                published_at_unix_nanos,
            } => {
                if !self.manifest_content_available(&manifest)? {
                    return Err(inline_error("staged manifest has no exact durable content"));
                }
                (
                    InlineOperation::RestoreVersion {
                        manifest,
                        published_at_unix_nanos,
                    },
                    Vec::new(),
                )
            }
            ContentStoreStagingRow::VersionTombstone {
                object_id,
                version,
                content_hash,
            } => (
                InlineOperation::DeleteVersion {
                    object_id,
                    version,
                    content_hash,
                },
                Vec::new(),
            ),
            ContentStoreStagingRow::Blob {
                content_hash,
                bytes,
                published_at_unix_nanos,
            } => {
                ensure_bytes_match_hash(&content_hash, &bytes)?;
                (
                    InlineOperation::Blob {
                        content_hash,
                        size_bytes: bytes.len() as u64,
                        published_at_unix_nanos,
                    },
                    bytes,
                )
            }
            ContentStoreStagingRow::BlobTombstone { content_hash } => {
                (InlineOperation::RemoveBlob { content_hash }, Vec::new())
            }
            ContentStoreStagingRow::RetentionRelease(identity) => {
                (InlineOperation::Release(identity), Vec::new())
            }
            ContentStoreStagingRow::RetentionRoot { .. } => unreachable!(),
        };
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        append_operation(
            &self.inline_store.owner.root,
            &reservation.sentinel,
            &mut state,
            operation,
            &payload,
        )?;
        drop(state);
        if let Some(path) = cache_key {
            self.evict_stream_manifest(&path);
        }
        self.quota_stats_initialized.store(false, Ordering::Release);
        Ok(())
    }

    pub fn storage_staging_version_owned(&self, object_id: &str, version: &str) -> Result<bool> {
        Ok(self.inline_version_state(object_id, version)?.is_some())
    }
}

#[cfg(feature = "storage-conversion")]
impl ContentStoreStagingWriter {
    pub fn storage_staging_manifest_page(
        &self,
        after: Option<&(String, String)>,
        limit: usize,
    ) -> Result<Vec<ContentManifest>> {
        use std::ops::Bound::{Excluded, Unbounded};
        let _owner = self.content_mutation_guard()?;
        let sentinel = self
            .inline_sentinel_unlocked()?
            .ok_or_else(|| inline_error("staging catalog is absent"))?;
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        ensure_writer(
            &self.inline_store.owner.root,
            &sentinel.store_incarnation,
            &mut state,
        )?;
        let bounds = (after.map_or(Unbounded, Excluded), Unbounded);
        Ok(state
            .view
            .versions
            .range(bounds)
            .filter_map(|(_, row)| match row {
                InlineVersionState::Live(entry) => Some(entry.manifest.clone()),
                _ => None,
            })
            .take(limit)
            .collect())
    }
}
