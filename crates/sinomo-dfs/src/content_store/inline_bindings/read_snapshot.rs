//! Immutable locators and reclamation pins for readers that open extents lazily.

use super::*;

const LEGACY_READERS_LOCK: &str = "legacy-readers.lock";

#[derive(Clone, Debug)]
enum CapturedBlobLocator {
    Empty,
    File {
        path: PathBuf,
        payload_offset: u64,
        size_bytes: u64,
        inline: bool,
    },
}

impl CapturedBlobLocator {
    fn size_bytes(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::File { size_bytes, .. } => *size_bytes,
        }
    }
}

#[derive(Debug)]
struct LegacyReaderLease(fs::File);

impl Drop for LegacyReaderLease {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

/// An exclusive legacy-reclamation window. Try acquisition never waits for a
/// live reader. Retaining the owner guard enforces owner -> reclamation -> lane
/// ordering even when a destructive caller did not already enter the owner.
pub(in crate::content_store) struct LegacyReclamationGuard {
    file: fs::File,
    _owner: ContentMutationGuard,
}

impl Drop for LegacyReclamationGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

/// One locator per distinct hash, one descriptor per inline generation, and at
/// most one legacy reclamation descriptor. Payload descriptors are opened only
/// for the current extent. No common owner guard survives snapshot construction.
#[derive(Debug)]
pub(in crate::content_store) struct ContentReadSnapshot {
    locators: BTreeMap<String, CapturedBlobLocator>,
    _generations: Vec<InlineReaderLease>,
    _legacy: Option<LegacyReaderLease>,
    _store: InlineStoreLease,
    #[cfg(test)]
    blob_stream_open_count: Arc<AtomicU64>,
}

impl ContentReadSnapshot {
    pub(in crate::content_store) fn require_range(
        &self,
        hash: &str,
        offset: u64,
        size_bytes: u64,
    ) -> Result<()> {
        let locator = self.locators.get(hash).ok_or_else(|| {
            inline_error("extent hash was not included in the captured read snapshot")
        })?;
        if offset
            .checked_add(size_bytes)
            .is_none_or(|end| end > locator.size_bytes())
        {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "manifest references missing or truncated content extents".into(),
            });
        }
        Ok(())
    }

    pub(in crate::content_store) fn open_range(
        self: &Arc<Self>,
        hash: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<BlobRangeReader> {
        #[cfg(test)]
        self.blob_stream_open_count.fetch_add(1, Ordering::Relaxed);
        let locator = self.locators.get(hash).ok_or_else(|| {
            inline_error("extent hash was not included in the captured read snapshot")
        })?;
        let CapturedBlobLocator::File {
            path,
            payload_offset,
            size_bytes,
            inline,
        } = locator
        else {
            return Ok(BlobRangeReader::empty().retain_snapshot(Arc::clone(self)));
        };
        let mut file = if *inline {
            // Verify the complete bounded inline payload even for a subrange.
            // A retained sealed-generation locator must not serve corruption
            // merely because its offsets still fit the file. Capture stays
            // lazy: this is the first payload descriptor for this extent.
            open_verified_inline_payload(path, *payload_offset, *size_bytes, hash)?
        } else {
            fs::File::open(path).map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?
        };
        validate_locator(
            path,
            *payload_offset,
            *size_bytes,
            &file.metadata().map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?,
        )?;
        let start = offset.min(*size_bytes);
        let physical_start = payload_offset
            .checked_add(start)
            .ok_or_else(|| inline_error("captured extent physical offset overflows"))?;
        file.seek(SeekFrom::Start(physical_start))
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        let remaining = size_bytes
            .saturating_sub(start)
            .min(u64::try_from(max_bytes).unwrap_or(u64::MAX));
        Ok(BlobRangeReader::new(file, start, *size_bytes, remaining)
            .retain_snapshot(Arc::clone(self)))
    }
}

fn validate_locator(path: &Path, offset: u64, size: u64, metadata: &fs::Metadata) -> Result<()> {
    if !metadata.is_file()
        || offset
            .checked_add(size)
            .is_none_or(|end| end > metadata.len())
    {
        return Err(inline_error(format!(
            "captured payload range exceeds retained file {}",
            path.display()
        )));
    }
    Ok(())
}

fn pin_file_shared(path: &Path) -> Result<fs::File> {
    let file = fs::OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?;
    FileExt::lock_shared(&file).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    Ok(file)
}

/// Called only by writable construction/activation. Read-only snapshots never
/// create this path; an older inspection-only root without it fails closed.
pub(in crate::content_store) fn initialize_legacy_reclamation_lock(root: &Path) -> Result<()> {
    let root =
        root_identity::canonical_existing_root(root).map_err(|source| ContentStoreError::Io {
            path: root.to_owned(),
            source,
        })?;
    let path = root.join(LEGACY_READERS_LOCK);
    let file = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
    {
        Ok(file) => {
            file.sync_all().map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
            sync_dir(&root)?;
            file
        }
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?,
        Err(source) => return Err(ContentStoreError::Io { path, source }),
    };
    if !file
        .metadata()
        .map_err(|source| ContentStoreError::Io { path, source })?
        .is_file()
    {
        return Err(inline_error(
            "legacy reclamation lock is not a regular file",
        ));
    }
    Ok(())
}

impl LocalContentStore {
    pub(in crate::content_store) fn try_lock_legacy_reclamation(
        &self,
    ) -> Result<Option<LegacyReclamationGuard>> {
        let owner = self.content_mutation_guard()?;
        let path = self.inline_store.owner.root.join(LEGACY_READERS_LOCK);
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(Some(LegacyReclamationGuard {
                file,
                _owner: owner,
            })),
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(source) => Err(ContentStoreError::Io { path, source }),
        }
    }

    pub(in crate::content_store) fn capture_read_snapshot<'a>(
        &self,
        hashes: impl IntoIterator<Item = &'a str>,
    ) -> Result<Arc<ContentReadSnapshot>> {
        let _owner = self.content_mutation_guard()?;
        let root = root_identity::canonical_existing_root(&self.root).map_err(|source| {
            ContentStoreError::Io {
                path: self.root.clone(),
                source,
            }
        })?;
        if root != self.inline_store.owner.root {
            return Err(inline_error(
                "content root changed after its store lease was acquired",
            ));
        }
        let hashes = hashes
            .into_iter()
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<_>>();
        for hash in &hashes {
            hash_hex(hash)?;
        }
        let mut inline = BTreeMap::new();
        if let Some(sentinel) = self.inline_sentinel_unlocked()? {
            let mut state = self
                .inline_store
                .owner
                .state
                .lock()
                .map_err(|_| inline_error("resident binding mutex poisoned"))?;
            refresh_resident(&root, &sentinel.store_incarnation, &mut state, false)?;
            for hash in &hashes {
                if state.view.deleted_blobs.contains(hash) {
                    return Err(ContentStoreError::Io {
                        path: self.blob_path(hash)?,
                        source: io::Error::new(
                            io::ErrorKind::NotFound,
                            "content blob was durably collected",
                        ),
                    });
                }
                if let Some(entry) = state.view.blobs.get(hash) {
                    inline.insert(hash.clone(), entry.clone());
                }
            }
        }
        let generations = inline
            .values()
            .map(|entry| entry.generation)
            .collect::<BTreeSet<_>>();
        let mut generation_pins = Vec::with_capacity(generations.len());
        for generation in generations {
            generation_pins.push(InlineReaderLease {
                _store: self.inline_store.clone(),
                generation_lock: pin_file_shared(
                    &generation_root(&root, generation).join("readers.lock"),
                )?,
            });
        }
        let has_legacy = hashes.iter().any(|hash| {
            hash.as_str() != intrinsic_empty_content_hash() && !inline.contains_key(hash)
        });
        // Pin before resolving any loose path or packed index. In an active
        // store the common owner also prevents catalog/locator changes during
        // capture; in a legacy store the reclamation pin protects the lookup.
        let legacy_pin = if has_legacy {
            Some(LegacyReaderLease(pin_file_shared(
                &root.join(LEGACY_READERS_LOCK),
            )?))
        } else {
            None
        };
        let mut locators = BTreeMap::new();
        for hash in hashes {
            let locator = if hash == intrinsic_empty_content_hash() {
                CapturedBlobLocator::Empty
            } else if let Some(entry) = inline.remove(&hash) {
                let metadata =
                    fs::metadata(&entry.path).map_err(|source| ContentStoreError::Io {
                        path: entry.path.clone(),
                        source,
                    })?;
                validate_locator(
                    &entry.path,
                    entry.payload_offset,
                    entry.size_bytes,
                    &metadata,
                )?;
                CapturedBlobLocator::File {
                    path: entry.path,
                    payload_offset: entry.payload_offset,
                    size_bytes: entry.size_bytes,
                    inline: true,
                }
            } else {
                let candidate = self.blob_path(&hash)?;
                let path = root.join(
                    candidate
                        .strip_prefix(&self.root)
                        .map_err(|_| inline_error("loose locator escaped content root"))?,
                );
                match fs::metadata(&path) {
                    Ok(metadata) => {
                        validate_locator(&path, 0, metadata.len(), &metadata)?;
                        CapturedBlobLocator::File {
                            path,
                            payload_offset: 0,
                            size_bytes: metadata.len(),
                            inline: false,
                        }
                    }
                    Err(source) if source.kind() == io::ErrorKind::NotFound => {
                        let Some((location, _lane)) =
                            self.packed_blob_location_and_read_lock(&hash)?
                        else {
                            return Err(ContentStoreError::Io {
                                path,
                                source: io::Error::new(
                                    io::ErrorKind::NotFound,
                                    "blob not found while capturing extent",
                                ),
                            });
                        };
                        let candidate = self.packed_blob_location_path(&location);
                        let path =
                            root.join(candidate.strip_prefix(&self.root).map_err(|_| {
                                inline_error("packed locator escaped content root")
                            })?);
                        let metadata =
                            fs::metadata(&path).map_err(|source| ContentStoreError::Io {
                                path: path.clone(),
                                source,
                            })?;
                        validate_locator(&path, location.offset, location.size_bytes, &metadata)?;
                        CapturedBlobLocator::File {
                            path,
                            payload_offset: location.offset,
                            size_bytes: location.size_bytes,
                            inline: false,
                        }
                    }
                    Err(source) => return Err(ContentStoreError::Io { path, source }),
                }
            };
            locators.insert(hash, locator);
        }
        Ok(Arc::new(ContentReadSnapshot {
            locators,
            _generations: generation_pins,
            _legacy: legacy_pin,
            _store: self.inline_store.clone(),
            #[cfg(test)]
            blob_stream_open_count: Arc::clone(&self.blob_stream_open_count),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_host::{DfsDataOwnerLock, DfsRuntimePaths};
    use sinomo_core::local::SinomoDataDir;

    fn legacy_store(name: &str) -> (sinomo_testdir::TestDir, LocalContentStore) {
        let root = sinomo_testdir::TestDir::new(name);
        let store = LocalContentStore::new(root.path()).expect("legacy store");
        initialize_legacy_reclamation_lock(root.path()).expect("legacy reader lock");
        (root, store)
    }

    fn inline_store(name: &str) -> (sinomo_testdir::TestDir, DfsDataOwnerLock, LocalContentStore) {
        let root = sinomo_testdir::TestDir::new(name);
        let data_dir = SinomoDataDir::new(root.path());
        let mut owner =
            DfsDataOwnerLock::acquire(&DfsRuntimePaths::new(&data_dir)).expect("main owner");
        owner
            .activate_content_bindings_before_routes()
            .expect("cold activation");
        let store =
            LocalContentStore::new(data_dir.dfs_dir().join("content")).expect("active store");
        (root, owner, store)
    }

    #[test]
    fn lazy_legacy_extent_stream_pins_reclamation_until_reader_drop() {
        let (_root, store) = legacy_store("dfs-lazy-legacy-reader-pin");
        store
            .put_object_version("extent-object", "v1", b"0123456789abcdef")
            .unwrap();
        let manifest = store
            .put_object_version_patch("extent-object", "v1", "v2", 6, b"WXYZ")
            .unwrap();
        assert!(manifest.extents.len() >= 3);
        let before = store.blob_stream_open_count.load(Ordering::Relaxed);
        let (_, mut reader) = store
            .open_object_version_stream("extent-object", "v2")
            .unwrap();
        assert_eq!(
            store.blob_stream_open_count.load(Ordering::Relaxed),
            before,
            "capturing locators must not eagerly open payload files"
        );
        assert!(
            store.try_lock_legacy_reclamation().unwrap().is_none(),
            "future unopened legacy extents need their shared reclamation pin"
        );
        // The returned reader owns manifest/locator data rather than its name.
        fs::remove_file(store.manifest_path("extent-object", "v2")).unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"012345WXYZabcdef");
        assert_eq!(
            store.blob_stream_open_count.load(Ordering::Relaxed) - before,
            manifest.extents.len() as u64
        );
        assert!(store.try_lock_legacy_reclamation().unwrap().is_none());
        drop(reader);
        assert!(store.try_lock_legacy_reclamation().unwrap().is_some());
    }

    #[test]
    fn lazy_range_stream_retains_only_captured_selected_sources() {
        let (_root, store) = legacy_store("dfs-lazy-range-reader-pin");
        store
            .put_object_version("range-object", "v1", b"0123456789abcdef")
            .unwrap();
        store
            .put_object_version_patch("range-object", "v1", "v2", 6, b"WXYZ")
            .unwrap();
        let (_, mut reader) = store
            .open_object_version_range_stream("range-object", "v2", 5, 6)
            .unwrap();
        fs::remove_file(store.manifest_path("range-object", "v2")).unwrap();
        assert!(store.try_lock_legacy_reclamation().unwrap().is_none());
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"5WXYZa");
        drop(reader);
        assert!(store.try_lock_legacy_reclamation().unwrap().is_some());
    }

    #[test]
    fn captured_range_keeps_shared_legacy_pin_after_snapshot_handle_drops() {
        let (_root, store) = legacy_store("dfs-captured-range-retains-snapshot");
        let manifest = store
            .put_object_version("object", "v1", b"retained bytes")
            .unwrap();
        let snapshot = store
            .capture_read_snapshot([manifest.content_hash.as_str()])
            .unwrap();
        let mut range = snapshot.open_range(&manifest.content_hash, 2, 6).unwrap();
        drop(snapshot);
        assert!(store.try_lock_legacy_reclamation().unwrap().is_none());
        let mut bytes = Vec::new();
        range.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"tained");
        drop(range);
        assert!(store.try_lock_legacy_reclamation().unwrap().is_some());
    }

    #[test]
    fn inspection_cannot_create_a_missing_legacy_reader_lock() {
        let (root, store) = legacy_store("dfs-inspection-lazy-pin-missing");
        store
            .put_object_version("object", "v1", b"0123456789abcdef")
            .unwrap();
        store
            .put_object_version_patch("object", "v1", "v2", 6, b"WXYZ")
            .unwrap();
        let lock = root.join(LEGACY_READERS_LOCK);
        fs::remove_file(&lock).unwrap();
        drop(store);
        let inspected = LocalContentStore::open_for_inspection(root.path());
        assert!(
            matches!(inspected.open_object_version_stream("object", "v2"),
            Err(ContentStoreError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound)
        );
        assert!(
            !lock.exists(),
            "inspection must not create reclamation coordination state"
        );
    }

    #[test]
    fn lazy_inline_reader_uses_retained_locators_after_logical_blob_removal() {
        let (_root, _data_owner, store) = inline_store("dfs-lazy-inline-read-snapshot");
        let a = store
            .put_object_version("inline-a", "v1", b"first")
            .unwrap();
        let b = store
            .put_object_version("inline-b", "v1", b"second")
            .unwrap();
        let snapshot = store
            .capture_read_snapshot([a.content_hash.as_str(), b.content_hash.as_str()])
            .unwrap();
        assert_eq!(snapshot.locators.len(), 2);
        assert_eq!(snapshot._generations.len(), 1);
        assert!(snapshot._legacy.is_none());
        let extents = vec![
            ContentExtent {
                logical_offset: 0,
                size_bytes: 5,
                content_hash: a.content_hash.clone(),
                blob_offset: 0,
            },
            ContentExtent {
                logical_offset: 5,
                size_bytes: 6,
                content_hash: b.content_hash.clone(),
                blob_offset: 0,
            },
        ];
        let mut reader = ObjectVersionReader::from_extents(Arc::clone(&snapshot), 11, extents);
        // A read snapshot is Send precisely because it does not retain the
        // thread-bound common owner guard during payload streaming.
        fn assert_send<T: Send>() {}
        assert_send::<ObjectVersionReader>();
        {
            let _owner = store.content_mutation_guard().unwrap();
            let sentinel = store.inline_sentinel_unlocked().unwrap().unwrap();
            let mut state = store.inline_store.owner.state.lock().unwrap();
            for (object, hash) in [("inline-a", &a.content_hash), ("inline-b", &b.content_hash)] {
                append_operation(
                    &store.inline_store.owner.root,
                    &sentinel,
                    &mut state,
                    InlineOperation::DeleteVersion {
                        object_id: object.into(),
                        version: "v1".into(),
                        content_hash: hash.clone(),
                    },
                    &[],
                )
                .unwrap();
                append_operation(
                    &store.inline_store.owner.root,
                    &sentinel,
                    &mut state,
                    InlineOperation::Release(ContentStoreReservationIdentity {
                        object_id: object.into(),
                        version: "v1".into(),
                        content_hash: hash.clone(),
                    }),
                    &[],
                )
                .unwrap();
                append_operation(
                    &store.inline_store.owner.root,
                    &sentinel,
                    &mut state,
                    InlineOperation::RemoveBlob {
                        content_hash: hash.clone(),
                    },
                    &[],
                )
                .unwrap();
            }
        }
        assert!(store.inline_blob_entry(&b.content_hash).unwrap().is_none());
        assert!(store.open_blob_stream(&b.content_hash).is_err());
        // An authoritative Drop must also shadow a surviving legacy copy.
        // Existing readers retain their captured locator; new readers cannot
        // resurrect the hash by falling through to this old representation.
        let legacy_copy = store.blob_path(&b.content_hash).unwrap();
        fs::write(&legacy_copy, b"second").unwrap();
        assert!(matches!(
            store.capture_read_snapshot([b.content_hash.as_str()]),
            Err(ContentStoreError::Io { source, .. })
                if source.kind() == io::ErrorKind::NotFound
        ));
        let lock_path = generation_root(&store.inline_store.owner.root, 1).join("readers.lock");
        let collector = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(lock_path)
            .unwrap();
        assert_eq!(
            FileExt::try_lock_exclusive(&collector).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"firstsecond");
        drop(snapshot);
        assert!(
            FileExt::try_lock_exclusive(&collector).is_err(),
            "lazy reader retains generation after original snapshot handle drops"
        );
        drop(reader);
        FileExt::try_lock_exclusive(&collector).unwrap();
        FileExt::unlock(&collector).unwrap();
    }

    #[test]
    fn repeated_extents_share_one_locator_and_generation_pin() {
        let (_root, _owner, store) = inline_store("dfs-reader-pin-deduplicated");
        let manifest = store.put_object_version("repeated", "v1", b"abcd").unwrap();
        let before = store.blob_stream_open_count.load(Ordering::Relaxed);
        let snapshot = store
            .capture_read_snapshot(std::iter::repeat_n(manifest.content_hash.as_str(), 2048))
            .unwrap();
        assert_eq!(snapshot.locators.len(), 1);
        assert_eq!(snapshot._generations.len(), 1);
        assert_eq!(store.blob_stream_open_count.load(Ordering::Relaxed), before);
        assert!(
            snapshot
                .require_range(&manifest.content_hash, 2, 3)
                .is_err()
        );
        let mut range = snapshot.open_range(&manifest.content_hash, 1, 2).unwrap();
        let mut bytes = Vec::new();
        range.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"bc");
    }

    #[test]
    fn lazy_inline_range_verifies_whole_sealed_payload_at_actual_open() {
        let (_root, _owner, store) = inline_store("dfs-inline-lazy-sealed-corruption");
        let payload = b"bounded inline payload";
        let manifest = store.put_object_version("object", "v1", payload).unwrap();
        // Seal this small test segment explicitly, without hundreds of writes
        // just to cross the production rotation threshold.
        {
            let _owner = store.content_mutation_guard().unwrap();
            let sentinel = store.inline_sentinel_unlocked().unwrap().unwrap();
            let mut state = store.inline_store.owner.state.lock().unwrap();
            let mut catalog = state.catalog.clone().unwrap();
            let path = segment_path(&store.root, catalog.generation, 0);
            let (hash, length) = durable_file::file_blake3_hash_and_size(&path).unwrap();
            catalog.segments[0].sealed_end = Some(length);
            catalog.segments[0].sealed_digest = Some(hash.to_hex().to_string());
            let next = segment_path(&store.root, catalog.generation, 1);
            drop(FrameAppender::open(&next, frame_limits()).unwrap());
            catalog.segments.push(InlineSegment {
                id: 1,
                sealed_end: None,
                sealed_digest: None,
            });
            publish_catalog(&store.root, &catalog).unwrap();
            ensure_writer(&store.root, &sentinel.store_incarnation, &mut state).unwrap();
        }
        let before = store.blob_stream_open_count.load(Ordering::Relaxed);
        let snapshot = store
            .capture_read_snapshot([manifest.content_hash.as_str()])
            .unwrap();
        assert_eq!(store.blob_stream_open_count.load(Ordering::Relaxed), before);
        let CapturedBlobLocator::File {
            path,
            payload_offset,
            inline,
            ..
        } = snapshot.locators.get(&manifest.content_hash).unwrap()
        else {
            panic!("expected an inline payload locator");
        };
        assert!(*inline);
        // Corrupt outside the requested range after locator capture. Bounds
        // remain unchanged, and the active segment remains untouched, so only
        // actual-open validation of the entire old payload detects this.
        let mut file = fs::OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(*payload_offset + payload.len() as u64 - 1))
            .unwrap();
        file.write_all(b"!").unwrap();
        file.sync_all().unwrap();
        let result = snapshot.open_range(&manifest.content_hash, 0, 4);
        assert!(
            result.is_err(),
            "captured sealed payload must be verified at open"
        );
        // The retained reader is still pinned to its original source and may
        // read after that source is restored; it never re-resolves current maps.
        file.seek(SeekFrom::Start(*payload_offset + payload.len() as u64 - 1))
            .unwrap();
        file.write_all(&payload[payload.len() - 1..]).unwrap();
        file.sync_all().unwrap();
        let mut range = snapshot.open_range(&manifest.content_hash, 1, 6).unwrap();
        let mut bytes = Vec::new();
        range.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, &payload[1..7]);
    }
}
