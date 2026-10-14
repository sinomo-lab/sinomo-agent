//! Durable identity of a fresh CURRENT content initialization. This is not a
//! storage converter or publication grant; the normal owner still fences writes.
use super::*;

const INTENT: &str = "content-initialization-v1.sobj";
const PURPOSE: &str = "sinomo.dfs.current-content-initialization.v1";
const MAX_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "platform", rename_all = "kebab-case", deny_unknown_fields)]
enum RootIdentity {
    Unix { device: u64, inode: u64 },
    Windows { volume: u32, file_index: u64 },
    Other { created_unix_nanos: String },
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    purpose: String,
    canonical_root: PathBuf,
    root_identity: RootIdentity,
    expected_marker: ContentStoreReservationBatchSentinel,
}
fn root_identity(root: &Path) -> Result<RootIdentity> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = fs::metadata(root).map_err(|source| ContentStoreError::Io {
            path: root.to_owned(),
            source,
        })?;
        if !metadata.is_dir() {
            return Err(invalid("initialization root is not a directory"));
        }
        Ok(RootIdentity::Unix {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(windows)]
    {
        use std::os::windows::{fs::OpenOptionsExt, io::AsRawHandle};
        use windows_sys::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, FILE_FLAG_BACKUP_SEMANTICS, GetFileInformationByHandle,
        };
        let directory = fs::OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(root)
            .map_err(|source| ContentStoreError::Io {
                path: root.to_owned(),
                source,
            })?;
        let mut information = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
        if unsafe {
            GetFileInformationByHandle(directory.as_raw_handle().cast(), information.as_mut_ptr())
        } == 0
        {
            return Err(ContentStoreError::Io {
                path: root.to_owned(),
                source: io::Error::last_os_error(),
            });
        }
        let information = unsafe { information.assume_init() };
        Ok(RootIdentity::Windows {
            volume: information.dwVolumeSerialNumber,
            file_index: (u64::from(information.nFileIndexHigh) << 32)
                | u64::from(information.nFileIndexLow),
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let created = fs::metadata(root)
            .and_then(|metadata| metadata.created())
            .map_err(|source| ContentStoreError::Io {
                path: root.to_owned(),
                source,
            })?;
        let nanos = match created.duration_since(std::time::UNIX_EPOCH) {
            Ok(duration) => duration.as_nanos().to_string(),
            Err(error) => format!("-{}", error.duration().as_nanos()),
        };
        Ok(RootIdentity::Other {
            created_unix_nanos: nanos,
        })
    }
}
/// Serializes only cold current initialization, including intent retirement.
/// Keep this gate outside the empty root so a crash cannot leave an owner file
/// inside an otherwise unidentifiable, uninitialized content layout. Never
/// unlink the gate: another opener may already hold the same file identity.
pub(super) struct InitializationGuard {
    _file: fs::File,
}
impl InitializationGuard {
    pub(super) fn acquire(root: &Path) -> Result<Self> {
        let canonical = fs::canonicalize(root).map_err(|source| ContentStoreError::Io {
            path: root.to_owned(),
            source,
        })?;
        let parent = canonical
            .parent()
            .ok_or_else(|| invalid("root has no owned parent"))?;
        let identity =
            sinomo_object_serde::to_bytes(&root_identity(&canonical)?, 1, LengthMode::U32)
                .map_err(|error| ContentStoreError::Serde(error.to_string()))?;
        let path = parent.join(format!(
            ".dfs-content-initialization-{}.lock",
            blake3::hash(&identity).to_hex()
        ));
        let mut options = fs::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&path)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        FileExt::lock_exclusive(&file).map_err(|source| ContentStoreError::Io { path, source })?;
        Ok(Self { _file: file })
    }
}

fn invalid(message: &str) -> ContentStoreError {
    ContentStoreError::InvalidContentLayout {
        message: format!("current content initialization: {message}"),
    }
}
fn load(root: &Path) -> Result<Option<Intent>> {
    let path = root.join(INTENT);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(ContentStoreError::Io { path, source }),
    };
    if !metadata.is_file() || metadata.len() > MAX_BYTES {
        return Err(invalid("intent is not a bounded physical regular file"));
    }
    let intent: Intent = sinomo_object_serde::from_bytes(&read_file(&path)?)
        .map_err(|error| ContentStoreError::Serde(error.to_string()))?;
    if intent.purpose != PURPOSE
        || intent.canonical_root
            != fs::canonicalize(root).map_err(|source| ContentStoreError::Io {
                path: root.to_owned(),
                source,
            })?
        || intent.root_identity != root_identity(root)?
    {
        return Err(invalid("intent belongs to a different current root"));
    }
    validate_content_reservation_batch_sentinel(&intent.expected_marker)?;
    let marker = root.join("content-reservations/00/batch-format-v1.sobj");
    match fs::read(&marker) {
        Ok(bytes) => {
            let actual = decode_content_reservation_batch_sentinel(&bytes)?;
            if actual != intent.expected_marker {
                return Err(invalid("current marker differs from initialization intent"));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(ContentStoreError::Io {
                path: marker,
                source,
            });
        }
    }
    Ok(Some(intent))
}
/// Read-only current-schema recognition for the existing preparation provider.
pub(super) fn pending(root: &Path) -> Result<bool> {
    Ok(load(root)?.is_some())
}
pub(super) fn marker(root: &Path) -> Result<Option<ContentStoreReservationBatchSentinel>> {
    Ok(load(root)?.map(|intent| intent.expected_marker))
}
pub(super) fn begin(root: &Path) -> Result<()> {
    if load(root)?.is_some() {
        return Ok(());
    }
    if fs::read_dir(root)
        .map_err(|source| ContentStoreError::Io {
            path: root.to_owned(),
            source,
        })?
        .next()
        .is_some()
    {
        return Err(ContentStoreError::UnsupportedStorageFormat {
            root: root.to_owned(),
        });
    }
    let canonical_root = fs::canonicalize(root).map_err(|source| ContentStoreError::Io {
        path: root.to_owned(),
        source,
    })?;
    let intent = Intent {
        purpose: PURPOSE.into(),
        root_identity: root_identity(&canonical_root)?,
        canonical_root: canonical_root.clone(),
        expected_marker: ContentStoreReservationBatchSentinel::new(),
    };
    let bytes = sinomo_object_serde::to_bytes(&intent, 1, LengthMode::U32)
        .map_err(|error| ContentStoreError::Serde(error.to_string()))?;
    let parent = canonical_root
        .parent()
        .ok_or_else(|| invalid("root has no owned parent"))?;
    // Stage OUTSIDE the empty root. A crash before publication leaves an empty
    // root, rather than an unidentifiable half-written file inside it.
    let stage = parent.join(format!(
        ".dfs-content-initialization-{}.tmp",
        sinomo_core::Id::generate()
    ));
    let _cleanup = sinomo_core::storage::scratch::ScratchPath::new(stage.clone());
    let mut file =
        durable_file::create_private_staged_file(&stage).map_err(content_durable_file_error)?;
    io_trace::write_all(&mut file, &bytes).map_err(|source| ContentStoreError::Io {
        path: stage.clone(),
        source,
    })?;
    let synced =
        durable_file::sync_private_staged_file(&stage, file).map_err(content_durable_file_error)?;
    match durable_file::install_synced_private_staged_file(&synced, &canonical_root.join(INTENT)) {
        Ok(()) => {}
        Err(durable_file::DurableFileError::Io {
            kind: io::ErrorKind::AlreadyExists,
            ..
        }) => {
            if load(root)?.is_none() {
                return Err(invalid(
                    "concurrent initialization did not publish a valid intent",
                ));
            }
        }
        Err(error) => return Err(content_durable_file_error(error)),
    }
    Ok(())
}
pub(super) fn complete(root: &Path) -> Result<()> {
    if load(root)?.is_some() {
        let marker = root.join("content-reservations/00/batch-format-v1.sobj");
        // Completion cannot erase the only identity of an unmarked root.
        validate_content_reservation_batch_sentinel(&decode_content_reservation_batch_sentinel(
            &read_file(&marker)?,
        )?)?;
        remove_file_if_exists_and_sync_parent(&root.join(INTENT))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_initialization_resumes_each_durable_phase_with_the_same_marker() {
        for phase in 0..=5 {
            let fixture = sinomo_testdir::TestDir::new("content-current-initialization-phases");
            let root = fixture.join("content");
            fs::create_dir(&root).unwrap();
            if phase > 0 {
                begin(&root).unwrap();
            }
            let expected = marker(&root).unwrap();
            if phase >= 2 {
                let mut partial = LocalContentStore::open_for_inspection(&root);
                let _owner = inline_bindings::ContentMutationGuard::acquire(&root, true).unwrap();
                fs::create_dir_all(root.join("blobs/00")).unwrap();
                if phase >= 3 {
                    partial
                        .initialize_content_reservation_writer_state()
                        .unwrap();
                }
                if phase >= 4 {
                    partial.initialize_inline_bindings().unwrap();
                }
                if phase >= 5 {
                    inline_bindings::initialize_file_reclamation_lock(&root).unwrap();
                }
                // Dropping this real owner models process termination at the
                // actual durable phase; no alternate initialization is used.
            }
            let store = LocalContentStore::new(&root).unwrap();
            let actual = decode_content_reservation_batch_sentinel(
                &read_file(&store.content_reservation_batch_sentinel_path()).unwrap(),
            )
            .unwrap();
            if let Some(expected) = expected {
                assert_eq!(actual, expected);
            }
            assert!(!root.join(INTENT).exists());
            assert!(root.join("content-owner.lock").is_file());
            let object = sinomo_core::Id::generate().to_string();
            let version = sinomo_core::Id::generate().to_string();
            store
                .put_object_version(&object, &version, b"current durable bytes")
                .unwrap();
            drop(store);
            assert_eq!(
                LocalContentStore::new(&root)
                    .unwrap()
                    .read_object_version(&object, &version)
                    .unwrap()
                    .1,
                b"current durable bytes"
            );
        }
    }
    #[test]
    fn concurrent_current_openers_keep_one_initialization_and_marker() {
        let fixture = sinomo_testdir::TestDir::new("content-current-concurrent-open");
        for case in 0..32 {
            let root = fixture.join(format!("content-{case}"));
            let start = std::sync::Arc::new(std::sync::Barrier::new(8));
            let attempts = std::thread::scope(|scope| {
                let threads = (0..8)
                    .map(|_| {
                        let root = root.clone();
                        let start = std::sync::Arc::clone(&start);
                        scope.spawn(move || {
                            start.wait();
                            let store = LocalContentStore::new(root)?;
                            decode_content_reservation_batch_sentinel(&read_file(
                                &store.content_reservation_batch_sentinel_path(),
                            )?)
                        })
                    })
                    .collect::<Vec<_>>();
                threads
                    .into_iter()
                    .map(|thread| thread.join().unwrap())
                    .collect::<Vec<_>>()
            });
            let errors = attempts
                .iter()
                .filter_map(|result| result.as_ref().err())
                .collect::<Vec<_>>();
            assert!(
                errors.is_empty(),
                "concurrent normal open failed in case {case}: {errors:?}"
            );
            let marker = attempts[0].as_ref().unwrap();
            for actual in &attempts {
                assert_eq!(actual.as_ref().unwrap(), marker);
            }
            assert!(!root.join(INTENT).exists());
            let store = LocalContentStore::new(&root).unwrap();
            assert_eq!(
                decode_content_reservation_batch_sentinel(
                    &read_file(&store.content_reservation_batch_sentinel_path()).unwrap(),
                )
                .unwrap(),
                *marker
            );
        }
    }

    #[test]
    fn copied_or_restamped_intent_cannot_admit_a_different_root_or_marker() {
        let fixture = sinomo_testdir::TestDir::new("content-current-initialization-binding");
        let original = fixture.join("original");
        let other = fixture.join("other");
        fs::create_dir(&original).unwrap();
        fs::create_dir(&other).unwrap();
        begin(&original).unwrap();
        fs::copy(original.join(INTENT), other.join(INTENT)).unwrap();
        assert!(LocalContentStore::new(&other).is_err());
        let mut partial = LocalContentStore::open_for_inspection(&original);
        let owner = inline_bindings::ContentMutationGuard::acquire(&original, true).unwrap();
        partial
            .initialize_content_reservation_writer_state()
            .unwrap();
        let mut changed = decode_content_reservation_batch_sentinel(
            &read_file(&partial.content_reservation_batch_sentinel_path()).unwrap(),
        )
        .unwrap();
        changed.store_incarnation = sinomo_core::Id::generate().to_string();
        write_file_atomic(
            &partial.content_reservation_batch_sentinel_path(),
            &encode_content_reservation_batch_sentinel(&changed).unwrap(),
        )
        .unwrap();
        drop(owner);
        drop(partial);
        assert!(LocalContentStore::new(&original).is_err());
        assert!(original.join(INTENT).exists());
    }
    #[test]
    fn existing_current_store_cannot_recreate_a_missing_owner() {
        let fixture = sinomo_testdir::TestDir::new("content-current-owner-required");
        let root = fixture.join("content");
        let store = LocalContentStore::new(&root).unwrap();
        let marker_path = store.content_reservation_batch_sentinel_path();
        let bytes = read_file(&marker_path).unwrap();
        drop(store);
        fs::remove_file(root.join("content-owner.lock")).unwrap();
        assert!(matches!(
            LocalContentStore::new(&root),
            Err(ContentStoreError::Io { .. })
        ));
        assert_eq!(read_file(&marker_path).unwrap(), bytes);
        assert!(!root.join("content-owner.lock").exists());
    }
    #[test]
    fn current_initialization_still_obeys_the_registered_preparation_provider() {
        let fixture = sinomo_testdir::TestDir::new("content-current-provider-boundary");
        let root = fixture.join("content");
        fs::create_dir(&root).unwrap();
        begin(&root).unwrap();
        let _registration = crate::register_dfs_storage_preparation_provider(
            fixture.path().to_owned(),
            Arc::new(|_, scope, _| {
                assert_eq!(*scope, crate::DfsStoragePreparationScope::Content);
                Ok(crate::DfsStoragePreparation::Pending {
                    processed_records: 0,
                })
            }),
            None,
        )
        .unwrap();
        assert!(matches!(
            LocalContentStore::new(&root),
            Err(ContentStoreError::StoragePreparationPending { .. })
        ));
        assert!(!root.join("content-owner.lock").exists());
        assert!(root.join(INTENT).is_file());
    }
    #[test]
    fn nonempty_unmarked_data_cannot_be_labeled_as_current_initialization() {
        let fixture = sinomo_testdir::TestDir::new("content-current-initialization-not-inferred");
        let root = fixture.join("content");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("payload"), b"retained input").unwrap();
        assert!(matches!(
            LocalContentStore::new(&root),
            Err(ContentStoreError::UnsupportedStorageFormat { .. })
        ));
        assert!(!root.join(INTENT).exists());
        assert_eq!(fs::read(root.join("payload")).unwrap(), b"retained input");
    }
    #[cfg(feature = "storage-conversion")]
    #[test]
    fn explicit_current_staging_does_not_invoke_a_source_preparation_provider() {
        let fixture = sinomo_testdir::TestDir::new("current-staging-provider-negative");
        let root = fixture.join("candidate");
        fs::create_dir(&root).unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = calls.clone();
        let _registration = crate::register_dfs_storage_preparation_provider(
            fixture.path().to_owned(),
            Arc::new(move |_, scope, _| {
                assert_eq!(*scope, crate::DfsStoragePreparationScope::Content);
                count.fetch_add(1, Ordering::SeqCst);
                Ok(crate::DfsStoragePreparation::Pending {
                    processed_records: 0,
                })
            }),
            None,
        )
        .unwrap();
        assert!(matches!(
            LocalContentStore::new(&root),
            Err(ContentStoreError::StoragePreparationPending { .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let candidate = ContentStoreStagingWriter::open_candidate(&root).unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "explicit current output must not call the source converter again"
        );
        assert!(root.join("catalog-staging-owner.lock").is_file());
        assert!(
            matches!(ContentStoreStagingWriter::open_candidate(&root),
            Err(ContentStoreError::Io { source, .. }) if source.kind() == io::ErrorKind::WouldBlock),
            "actual current staging writer custody remains exclusive"
        );
        drop(candidate);
        assert!(ContentStoreStagingWriter::open_candidate(&root).is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "storage-conversion")]
    #[test]
    fn current_staging_rejects_original_nonempty_output_without_relabeling_it() {
        let fixture = sinomo_testdir::TestDir::new("current-staging-original-output");
        let root = fixture.join("candidate");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("original-input"), b"retained original bytes").unwrap();
        assert!(matches!(
            ContentStoreStagingWriter::open_candidate(&root),
            Err(ContentStoreError::UnsupportedStorageFormat { .. })
        ));
        assert_eq!(
            fs::read(root.join("original-input")).unwrap(),
            b"retained original bytes"
        );
        assert!(!root.join(INTENT).exists());
        assert!(!root.join("catalog-staging-owner.lock").exists());
    }
}
