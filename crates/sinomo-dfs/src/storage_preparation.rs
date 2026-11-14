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
    /// Post-decode readiness for journal/table-log-only roots. The provider must
    /// validate current activation directly, without reopening this backend.
    MetadataCountCohort {
        shard: DfsMetadataShardId,
        obligations: std::collections::BTreeSet<(crate::DfsFileSystemId, String)>,
    },
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
    } else if let DfsStoragePreparationScope::Metadata(shard) = scope
        && imported_metadata_needs_preparation_owner(&physical, shard)?
    {
        return Err(MetadataBackendError::StoragePreparationPending {
            root: physical,
            processed_records: 0,
        });
    }
    Ok(())
}

// This is a refusal-only inspection of the existing converter marker. It
// cannot certify a cohort from saved flags; the registered owner must reobserve
// native outcomes. Completed non-cohort imports retain their ordinary open.
fn imported_metadata_needs_preparation_owner(
    root: &Path,
    shard: &DfsMetadataShardId,
) -> Result<bool> {
    use std::io::Read;
    let path = root.join("metadata-import-v7.json");
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => return Err(MetadataBackendError::Io { path, source }),
    }
    let file = std::fs::File::open(&path).map_err(|source| MetadataBackendError::Io {
        path: path.clone(),
        source,
    })?;
    const MAX_MARKER_BYTES: usize = 8 * 1024 * 1024;
    let mut bytes = Vec::new();
    file.take((MAX_MARKER_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
    if bytes.len() > MAX_MARKER_BYTES {
        return Ok(true);
    }
    let marker: serde_json::Value = serde_json::from_slice(&bytes).map_err(|error| {
        MetadataBackendError::Serde(format!("unreadable import readiness marker: {error}"))
    })?;
    let invalid = || {
        MetadataBackendError::Serde(
            "import readiness marker has invalid source/image binding".into(),
        )
    };
    if marker
        .get("format_version")
        .and_then(serde_json::Value::as_u64)
        != Some(7)
    {
        return Err(invalid());
    }
    let cut = marker
        .get("source_cut")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(invalid)?;
    let scope: DfsMetadataShardId =
        serde_json::from_value(cut.get("scope_shard").cloned().ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
    if scope != *shard {
        return Err(invalid());
    }
    if cut
        .get("namespace_count_cohort")
        .is_some_and(|cohort| !cohort.is_null())
    {
        return Ok(true);
    }
    if marker
        .get("installation_complete")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        return Ok(true);
    }
    let image = marker
        .get("native_image")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(invalid)?;
    let prefix: crate::DfsMetadataPrefixProof =
        serde_json::from_value(image.get("prefix_proof").cloned().ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
    if !prefix.is_well_formed()
        || image.get("prefix_proof") != cut.get("target_exact_prefix")
        || image.get("state_fingerprint") != cut.get("current_image_fingerprint")
        || [
            "authority_binding",
            "source_identity",
            "source_snapshot_fingerprint",
            "retained_source_root",
        ]
        .iter()
        .any(|field| {
            marker
                .get(*field)
                .and_then(serde_json::Value::as_str)
                .is_none_or(str::is_empty)
        })
        || ["state_fingerprint", "file_fingerprint"]
            .iter()
            .any(|field| {
                image
                    .get(*field)
                    .and_then(serde_json::Value::as_str)
                    .is_none_or(str::is_empty)
            })
    {
        return Err(invalid());
    }
    Ok(false)
}

/// A validated journal obligation survives loss of the convenience import
/// marker and compaction of the original body. Only the existing preparation
/// owner can settle it; finding it never certifies successful activation.
pub(crate) fn require_count_cohort_preparation_owner(
    root: &Path,
    shard: &DfsMetadataShardId,
    tables: &crate::metadata_tables::DfsMetadataTables,
) -> Result<()> {
    let obligations =
        crate::namespace_counts::namespace_count_cohort_obligations_from_tables(tables, shard)?;
    if obligations.is_empty() {
        return Ok(());
    }
    let physical = std::fs::canonicalize(root).map_err(|source| MetadataBackendError::Io {
        path: root.to_owned(),
        source,
    })?;
    let provider = PROVIDERS.get().and_then(|registry| {
        registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .roots
            .iter()
            .filter(|(prefix, _)| physical.starts_with(prefix))
            .max_by_key(|(prefix, _)| prefix.components().count())
            .map(|(_, entry)| Arc::clone(&entry.provider))
    });
    let result = match provider {
        Some(provider) => provider(
            &physical,
            &DfsStoragePreparationScope::MetadataCountCohort {
                shard: shard.clone(),
                obligations,
            },
            DFS_STORAGE_PREPARATION_RECORD_BUDGET,
        )?,
        None => DfsStoragePreparation::Pending {
            processed_records: 0,
        },
    };
    if let DfsStoragePreparation::Pending { processed_records } = result {
        return Err(MetadataBackendError::StoragePreparationPending {
            root: physical,
            processed_records,
        });
    }
    Ok(())
}

#[cfg(all(test, feature = "storage-conversion"))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn installed_current_metadata_stays_pending_while_provider_finishes_its_cohort() {
        let root = sinomo_testdir::TestDir::new("storage-count-cohort-ready");
        let shard = DfsMetadataShardId::new("root");
        let writer = crate::DfsMetadataCandidateWriter::open(root.path()).unwrap();
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
        crate::LocalSobjMetadataBackend::new(shard, root.path()).unwrap();
        drop(ready);
    }
}
