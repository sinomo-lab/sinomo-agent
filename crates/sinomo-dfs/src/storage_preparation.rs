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
    /// Derived metadata-routing representation under the same data owner.
    MetadataRouting,
    Content,
    /// Node-local representation preparation under the SAME component owner.
    LocalState,
}

pub type DfsStoragePreparationProviderBuilder = Arc<
    dyn Fn(
            &sinomo_module::ComponentContext,
            tokio::sync::watch::Receiver<bool>,
        )
            -> std::result::Result<DfsStoragePreparationProvider, sinomo_module::ComponentError>
        + Send
        + Sync,
>;

pub type DfsStoragePreparationProvider = Arc<
    dyn Fn(&Path, &DfsStoragePreparationScope, usize) -> Result<DfsStoragePreparation>
        + Send
        + Sync,
>;
struct Entry {
    id: u64,
    provider: DfsStoragePreparationProvider,
    lifecycle: Option<tokio::sync::watch::Receiver<bool>>,
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
        },
    );
    Ok(DfsStoragePreparationRegistration { root, id })
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
    }
    Ok(())
}
