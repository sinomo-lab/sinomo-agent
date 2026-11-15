//! Generic cold-open preparation. Conversion belongs to an external provider.
use crate::DfsMetadataShardId;
use crate::metadata_error::{MetadataBackendError, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

pub const DFS_STORAGE_PREPARATION_RECORD_BUDGET: usize = 256;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DfsStoragePreparation {
    Ready,
    Pending { processed_records: usize },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsStoragePreparationScope {
    Metadata(DfsMetadataShardId),
    /// Current immutable metadata generation selected by the native owner.
    MetadataGeneration {
        identity: crate::DfsMetadataGenerationIdentity,
    },
    /// Derived metadata-routing representation under the same data owner.
    MetadataRouting,
    /// Admission of the current root representation, not completeness of all
    /// immutable versions retained beneath it.
    Content,
    /// Admission of one immutable version. Undiscovered retained data remains
    /// pending rather than becoming a false missing-version result.
    ContentVersion {
        object_id: String,
        version: String,
    },
    ContentBlob { hash: String },
    ContentObject { object_id: String },
    /// Completeness required by retention and garbage collection; ordinary
    /// reads of an admitted version do not wait for this coverage.
    ContentInventory,
    /// Node-local representation preparation under the SAME component owner.
    LocalState,
}

pub type DfsStoragePreparationProviderBuilder = Arc<
    dyn Fn(
            &sinomo_module::ComponentContext,
            &mut sinomo_module::ComponentScope,
            tokio::sync::watch::Receiver<bool>,
        )
            -> std::result::Result<DfsStoragePreparationProviderBinding, sinomo_module::ComponentError>
        + Send
        + Sync,
>;

pub type DfsStoragePreparationProvider = Arc<
    dyn Fn(&Path, &DfsStoragePreparationScope, usize) -> Result<DfsStoragePreparation>
        + Send
        + Sync,
>;
/// The worker belongs to the component scope. This watch only wakes consumers;
/// each consumer must recheck its exact unit before using storage.
pub struct DfsStoragePreparationProviderBinding {
    pub provider: DfsStoragePreparationProvider,
    pub readiness: tokio::sync::watch::Receiver<u64>,
}
struct Entry {
    id: u64,
    provider: DfsStoragePreparationProvider,
    lifecycle: Option<tokio::sync::watch::Receiver<bool>>,
    readiness: Option<tokio::sync::watch::Receiver<u64>>,
}
#[derive(Default)]
struct Registry {
    next_id: u64,
    roots: BTreeMap<PathBuf, Entry>,
}
static PROVIDERS: OnceLock<Mutex<Registry>> = OnceLock::new();
/// Drop unregisters new admission; an in-flight callback retains its provider
/// and therefore the component's actual data owner until the call completes.
pub struct DfsStoragePreparationRegistration {
    root: PathBuf,
    id: u64,
}
impl Drop for DfsStoragePreparationRegistration {
    fn drop(&mut self) {
        if let Some(registry) = PROVIDERS.get() {
            let mut registry = registry.lock().unwrap_or_else(|e| e.into_inner());
            if registry
                .roots
                .get(&self.root)
                .is_some_and(|entry| entry.id == self.id)
            {
                registry.roots.remove(&self.root);
            }
        }
    }
}
pub fn register_dfs_storage_preparation_provider(
    data_root: PathBuf,
    provider: DfsStoragePreparationProvider,
    lifecycle: Option<tokio::sync::watch::Receiver<bool>>,
) -> Result<DfsStoragePreparationRegistration> {
    register_provider(data_root, provider, lifecycle, None)
}
pub(crate) fn register_provider(
    data_root: PathBuf,
    provider: DfsStoragePreparationProvider,
    lifecycle: Option<tokio::sync::watch::Receiver<bool>>,
    readiness: Option<tokio::sync::watch::Receiver<u64>>,
) -> Result<DfsStoragePreparationRegistration> {
    let root = std::fs::canonicalize(&data_root).map_err(|source| MetadataBackendError::Io {
        path: data_root,
        source,
    })?;
    let mut registry = PROVIDERS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if registry.roots.contains_key(&root) {
        return Err(MetadataBackendError::Serde(
            "storage preparation already has a live owner for this data root".into(),
        ));
    }
    registry.next_id = registry.next_id.checked_add(1).ok_or_else(|| {
        MetadataBackendError::Serde("storage preparation registration exhausted".into())
    })?;
    let id = registry.next_id;
    registry.roots.insert(
        root.clone(),
        Entry {
            id,
            provider,
            lifecycle,
            readiness,
        },
    );
    Ok(DfsStoragePreparationRegistration { root, id })
}

/// Subscribe to the already-owned preparation worker without starting work.
pub fn dfs_storage_preparation_readiness(
    data_root: &Path,
) -> Option<tokio::sync::watch::Receiver<u64>> {
    let root = std::fs::canonicalize(data_root).ok()?;
    PROVIDERS.get()?.lock().ok()?.roots.get(&root)?.readiness.clone()
}

/// Retain the actual provider component's watch, without creating an owner.
/// Migration consumers use channel identity solely to share that live lifecycle.
pub fn dfs_storage_preparation_lifecycle(
    data_root: &Path,
) -> Option<tokio::sync::watch::Receiver<bool>> {
    PROVIDERS
        .get()?
        .lock()
        .ok()?
        .roots
        .get(data_root)?
        .lifecycle
        .clone()
}
pub(crate) fn prepare_storage_root_before_open(
    root: &Path,
    scope: &DfsStoragePreparationScope,
) -> Result<()> {
    // A missing destination can still have retained data in an older parent
    // layout. Resolve the existing physical ancestor before choosing a provider.
    let mut existing = root.to_owned();
    let mut suffix = Vec::new();
    while !existing.exists() {
        let name = existing
            .file_name()
            .ok_or_else(|| {
                MetadataBackendError::Serde("storage root has no existing physical ancestor".into())
            })?
            .to_owned();
        suffix.push(name);
        if !existing.pop() {
            return Err(MetadataBackendError::Serde(
                "storage root has no existing physical ancestor".into(),
            ));
        }
    }
    let mut physical =
        std::fs::canonicalize(&existing).map_err(|source| MetadataBackendError::Io {
            path: existing,
            source,
        })?;
    for name in suffix.into_iter().rev() {
        physical.push(name);
    }
    let provider = PROVIDERS.get().and_then(|registry| {
        registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .roots
            .iter()
            .filter(|(prefix, _)| physical.starts_with(prefix))
            .max_by_key(|(prefix, _)| prefix.components().count())
            .map(|(_, entry)| Arc::clone(&entry.provider))
    });
    if let Some(provider) = provider {
        match provider(&physical, scope, DFS_STORAGE_PREPARATION_RECORD_BUDGET)? {
            DfsStoragePreparation::Ready => {}
            DfsStoragePreparation::Pending { processed_records } => {
                return Err(MetadataBackendError::StoragePreparationPending {
                    root: physical,
                    processed_records,
                });
            }
        }
    } else if matches!(scope,
        DfsStoragePreparationScope::ContentVersion { .. }
        | DfsStoragePreparationScope::ContentBlob { .. }
        | DfsStoragePreparationScope::ContentObject { .. }
        | DfsStoragePreparationScope::ContentInventory
        | DfsStoragePreparationScope::MetadataGeneration { .. }) {
        return Err(MetadataBackendError::StoragePreparationPending {
            root: physical, processed_records: 0,
        });
    }
    Ok(())
}

#[cfg(all(test, feature = "storage-conversion"))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn undiscovered_content_units_without_an_owner_stay_pending() {
        let root = sinomo_testdir::TestDir::new("unowned-content-preparation");
        for unit in [
            DfsStoragePreparationScope::ContentVersion { object_id: "opaque object".into(), version: "opaque version".into() },
            DfsStoragePreparationScope::ContentObject { object_id: "opaque object".into() },
            DfsStoragePreparationScope::ContentBlob { hash: "immutable hash".into() },
            DfsStoragePreparationScope::ContentInventory,
        ] {
            assert!(matches!(prepare_storage_root_before_open(root.path(), &unit),
                Err(MetadataBackendError::StoragePreparationPending { processed_records: 0, .. })));
        }
    }

    #[test]
    fn readiness_watch_is_advisory_and_keeps_exact_unit_admission() {
        let root = sinomo_testdir::TestDir::new("storage-owner-readiness-watch");
        let (signal, change) = tokio::sync::watch::channel(0);
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let registration = register_provider(root.path().to_owned(), Arc::new(move |_, unit, _| {
            assert_eq!(unit, &DfsStoragePreparationScope::ContentObject { object_id: "a".into() });
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(DfsStoragePreparation::Pending { processed_records: 0 })
        }), None, Some(change)).unwrap();
        let receiver = dfs_storage_preparation_readiness(root.path()).unwrap();
        signal.send(1).unwrap();
        assert!(receiver.has_changed().unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 0, "a wake never executes conversion or admission");
        assert!(matches!(prepare_storage_root_before_open(root.path(), &DfsStoragePreparationScope::ContentObject { object_id: "a".into() }),
            Err(MetadataBackendError::StoragePreparationPending { .. })));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(registration);
        assert!(dfs_storage_preparation_readiness(root.path()).is_none());
    }

    #[test]
    fn provider_ready_cannot_activate_a_staged_current_generation() {
        let root = sinomo_testdir::TestDir::new("storage-generation-ready");
        let shard = DfsMetadataShardId::new("root");
        let writer = crate::DfsMetadataCandidateWriter::open_generation(root.path(), crate::DfsMetadataGenerationIdentity {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: shard.clone(),
            generation: "current-data-only".into(),
            origin: crate::DfsMetadataGenerationOrigin::StorageTransition { transition_id: "data-only-transition".into() },
        }).unwrap();
        let proof = crate::DfsMetadataPrefixProof {
            version: 2,
            journal_sequence: 0,
            digest: format!("dfs-metadata-prefix-v2:blake3:{}", "0".repeat(64)),
        };
        let identity = writer.write(Default::default(), proof).unwrap();
        drop(writer);
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let registration = register_dfs_storage_preparation_provider(
            root.path().to_owned(),
            Arc::new(move |_, scope, budget| {
                assert_eq!(
                    scope,
                    &DfsStoragePreparationScope::Metadata(DfsMetadataShardId::new("root"))
                );
                assert_eq!(budget, DFS_STORAGE_PREPARATION_RECORD_BUDGET);
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(DfsStoragePreparation::Pending {
                    processed_records: 0,
                })
            }),
            None,
        )
        .unwrap();
        assert!(matches!(
            crate::LocalSobjMetadataBackend::new(shard.clone(), root.path()),
            Err(MetadataBackendError::StoragePreparationPending { .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // The hidden-image decoder carries no ordinary author admission and
        // remains usable by the original converter while Ready is withheld.
        assert_eq!(
            crate::inspect_metadata_candidate(root.path()).unwrap(),
            identity
        );
        drop(registration);
        let ready = register_dfs_storage_preparation_provider(
            root.path().to_owned(),
            Arc::new(|_, _, _| Ok(DfsStoragePreparation::Ready)),
            None,
        )
        .unwrap();
        assert!(matches!(crate::LocalSobjMetadataBackend::new(shard, root.path()),
            Err(MetadataBackendError::StoragePreparationPending { .. })));
        drop(ready);
    }
}
