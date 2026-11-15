//! Owned capabilities for one prepared metadata publication.
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_lock::MetadataWriteLock;
use crate::runtime_admission::DfsMutationAdmission;
use crate::runtime_host::DfsDataOwnerLock;
use crate::{
    DfsFileSystemId, DfsMessageExecutionScope, DfsMetadataShardId, DfsPreparedOperationDigest,
};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

#[cfg(test)]
thread_local! {
    pub(crate) static BEFORE_STORED_PREPARE_EXECUTION: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    /// Pause only after the real author guard and complete participant evidence
    /// exist. Tests may interrupt delivery; they cannot supply acknowledgement claims.
    pub(crate) static AFTER_PREPARED_AUTHORIZATION: std::cell::RefCell<Option<Box<dyn FnOnce(&DfsPreparedOperationDigest, &crate::DfsPreparedOperationPayload)>>> = const { std::cell::RefCell::new(None) };
    /// Observe a genuine configuration update after the initial binding cut.
    /// The hook supplies no configuration, execution owner or publication proof.
    pub(crate) static INITIAL_BINDING_AFTER_CUT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    /// Observe a genuine configuration update after an effect operation took
    /// its publication snapshot and before its admission completes.
    pub(crate) static EFFECT_ADMISSION_AFTER_CUT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

/// Actual backend identity. This is a lock index, never author permission.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct DfsPublicationOwnerScope {
    root: PathBuf,
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    #[cfg(unix)]
    root_identity: (u64, u64),
}
impl DfsPublicationOwnerScope {
    pub(crate) fn for_backend(
        root: &Path,
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
    ) -> Result<Self> {
        let root = std::fs::canonicalize(root).map_err(|source| MetadataBackendError::Io {
            path: root.to_path_buf(),
            source,
        })?;
        #[cfg(unix)]
        let root_identity = {
            use std::os::unix::fs::MetadataExt;
            let metadata = std::fs::metadata(&root).map_err(|source| MetadataBackendError::Io {
                path: root.clone(),
                source,
            })?;
            (metadata.dev(), metadata.ino())
        };
        Ok(Self {
            root,
            file_system_id,
            shard_id,
            #[cfg(unix)]
            root_identity,
        })
    }
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }
    pub(crate) fn file_system_id(&self) -> &DfsFileSystemId {
        &self.file_system_id
    }
    pub(crate) fn shard_id(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }

    fn matches(&self, digest: &DfsPreparedOperationDigest) -> bool {
        self.file_system_id == digest.file_system_id && self.shard_id == digest.shard_id
    }
}

static FENCES: OnceLock<(Mutex<BTreeSet<DfsPublicationOwnerScope>>, Condvar)> = OnceLock::new();
/// Handoff exclusion grants no permission to author history.
#[derive(Debug)]
pub(crate) struct ShardFenceGuard {
    scope: DfsPublicationOwnerScope,
}
pub(crate) fn shard_fence_guard(scope: &DfsPublicationOwnerScope) -> ShardFenceGuard {
    let (mutex, changed) = FENCES.get_or_init(|| (Mutex::new(BTreeSet::new()), Condvar::new()));
    let mut held = mutex.lock().unwrap_or_else(|error| error.into_inner());
    while !held.insert(scope.clone()) {
        held = changed
            .wait(held)
            .unwrap_or_else(|error| error.into_inner());
    }
    ShardFenceGuard {
        scope: scope.clone(),
    }
}
impl Drop for ShardFenceGuard {
    fn drop(&mut self) {
        if let Some((mutex, changed)) = FENCES.get() {
            mutex
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&self.scope);
            changed.notify_all();
        }
    }
}

/// The physical writer is held from final validation through durable effect.
#[derive(Debug)]
pub struct DfsPublicationWriterSession {
    scope: DfsPublicationOwnerScope,
    digest: DfsPreparedOperationDigest,
    _write_lock: Arc<MetadataWriteLock>,
}
impl DfsPublicationWriterSession {
    pub(crate) fn for_shared_backend(
        lock: Arc<MetadataWriteLock>,
        scope: DfsPublicationOwnerScope,
        digest: DfsPreparedOperationDigest,
    ) -> Self {
        Self {
            scope,
            digest,
            _write_lock: lock,
        }
    }
    pub(crate) fn owner_scope(&self) -> &DfsPublicationOwnerScope {
        &self.scope
    }
}

/// Requester custody is not publication permission. Only local kernel work
/// can acquire the real effect-shard permit from this normal host provider.
pub struct DfsPublicationExecutionProvider {
    data_root: PathBuf,
    data_owner: Arc<DfsDataOwnerLock>,
    scheduler: Arc<crate::runtime_admission::DfsMutationScheduler>,
    authority: Arc<crate::DfsMetadataAuthorityRuntime>,
    storage_generation: u64,
    runtime: tokio::runtime::Handle,
}
impl std::fmt::Debug for DfsPublicationExecutionProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DfsPublicationExecutionProvider")
            .field("data_root", &self.data_root)
            .finish_non_exhaustive()
    }
}
impl DfsPublicationExecutionProvider {
    pub(crate) fn from_runtime(
        data_root: PathBuf,
        data_owner: Arc<DfsDataOwnerLock>,
        scheduler: Arc<crate::runtime_admission::DfsMutationScheduler>,
        authority: Arc<crate::DfsMetadataAuthorityRuntime>,
        runtime: tokio::runtime::Handle,
    ) -> Arc<Self> {
        let storage_generation = authority.storage_generation();
        Arc::new(Self {
            data_root,
            data_owner,
            scheduler,
            authority,
            storage_generation,
            runtime,
        })
    }
    /// Reuse the producer's existing admission and OS owner for bootstrap.
    /// This does not acquire another permit or bind a metadata instance.
    pub(crate) fn from_execution_owners(
        execution: &DfsPublicationExecutionOwners,
        authority: Arc<crate::DfsMetadataAuthorityRuntime>,
        runtime: tokio::runtime::Handle,
    ) -> Result<Arc<Self>> {
        let scheduler = {
            let admission = execution.admission.lock().map_err(|_| {
                MetadataBackendError::Serde("publication admission is poisoned".into())
            })?;
            let admission = admission.as_ref().ok_or_else(|| {
                MetadataBackendError::Serde(
                    "bootstrap publication admission was transferred".into(),
                )
            })?;
            if !admission.is_global() && !admission.is_exact() {
                return Err(MetadataBackendError::Serde(
                    "bootstrap publication requires actual mutation admission".into(),
                ));
            }
            admission.scheduler()
        };
        Ok(Self::from_runtime(
            execution.data_root.clone(),
            Arc::clone(&execution._data_owner),
            scheduler,
            authority,
            runtime,
        ))
    }

    /// Qualify the existing initial instance field from the normal host owner.
    /// This retained-only check grants no publication or handoff permission.
    pub(crate) fn validate_initial_epoch_binding(
        &self,
        scope: &DfsPublicationOwnerScope,
        digest: &DfsPreparedOperationDigest,
        execution: &DfsPublicationExecutionOwners,
        expected: &crate::DfsMetadataShardConfig,
    ) -> Result<()> {
        let owner = crate::runtime_host::publication_runtime_owner(scope.root())
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?
            .ok_or_else(|| {
                MetadataBackendError::Serde("initial epoch has no actual data owner".into())
            })?;
        if !Arc::ptr_eq(&owner, &self.data_owner)
            || !Arc::ptr_eq(&execution._data_owner, &self.data_owner)
            || !execution.admits(scope, &digest.file_system_id, &digest.shard_id)
            || !self.data_owner.owns_backend_root(scope.root())
            || self.authority.storage_generation() != self.storage_generation
        {
            return Err(MetadataBackendError::Serde(
                "initial epoch owner/storage generation changed".into(),
            ));
        }
        // One publication supplies the filesystem and shard rows checked here.
        let snapshot = self.authority.configuration_snapshot();
        let (shards, filesystem) = snapshot
            .publication_configuration(&digest.file_system_id)
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?
            .ok_or_else(|| {
                MetadataBackendError::Serde("initial epoch configuration is unavailable".into())
            })?;
        let mismatch = if filesystem
            .as_ref()
            .is_none_or(|filesystem| filesystem.file_system_id != digest.file_system_id)
        {
            Some("filesystem")
        } else if !scope.matches(digest) {
            Some("physical-scope")
        } else if expected.file_system_id != digest.file_system_id {
            Some("filesystem-id")
        } else if expected.shard_id != digest.shard_id.0 {
            Some("shard-id")
        } else if expected.epoch != digest.owner_epoch {
            Some("owner-epoch")
        } else if !shards.iter().any(|shard| shard == expected) {
            Some("selected-shard")
        } else if snapshot.storage_generation() != self.storage_generation {
            Some("storage-generation")
        } else {
            None
        };
        let binding = match mismatch {
            Some(reason) => {
                #[cfg(any(test, feature = "test-support"))]
                eprintln!(
                    "NATIVE_INITIAL_EPOCH_BINDING_REJECTED branch={reason} root={:?} digest={digest:?} expected={expected:?} cached_shards={shards:?} snapshot_projection={} expected_storage={} snapshot_storage={}",
                    scope.root(), snapshot.projection_generation(),
                    self.storage_generation, snapshot.storage_generation(),
                );
                #[cfg(not(any(test, feature = "test-support")))]
                let _ = reason;
                Err(MetadataBackendError::Serde(
                    "initial epoch differs from the coherent configuration".into(),
                ))
            }
            None => Ok(()),
        };
        #[cfg(test)]
        {
            let hook = INITIAL_BINDING_AFTER_CUT.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook();
            }
        }
        binding
    }

    fn acquire(
        &self,
        scope: &crate::DfsPreparedOperationScope,
        node: &sinomo_core::node::NodeId,
    ) -> std::result::Result<Arc<DfsPublicationExecutionOwners>, crate::DfsError> {
        self.acquire_before(scope, node, None)
    }

    fn acquire_before(
        &self,
        scope: &crate::DfsPreparedOperationScope,
        node: &sinomo_core::node::NodeId,
        deadline: Option<std::time::Instant>,
    ) -> std::result::Result<Arc<DfsPublicationExecutionOwners>, crate::DfsError> {
        // The provider retains this physical backend generation. Ordinary
        // proof updates do not replace that backend; each operation instead
        // takes one current publication and revalidates it after admission.
        let snapshot = self.authority.configuration_snapshot();
        if snapshot.storage_generation() != self.storage_generation {
            return Err(crate::DfsError::Metadata {
                message:
                    "publication storage configuration advanced; release the service before reopening"
                        .into(),
            });
        }
        let (shards, filesystem_config) = snapshot
            .publication_configuration(&scope.file_system_id)?
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "publication configuration is not current".into(),
            })?;
        #[cfg(test)]
        {
            let hook = EFFECT_ADMISSION_AFTER_CUT.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook();
            }
        }
        let filesystem_config = filesystem_config.ok_or_else(|| crate::DfsError::Metadata {
            message: "publication filesystem configuration is unavailable".into(),
        })?;
        let shard = shards
            .iter()
            .find(|shard| shard.shard_id == scope.shard_id.0)
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "publication has no exact shard route".into(),
            })?;
        if shard.primary_node_id != *node || shard.epoch != scope.owner_epoch {
            return Err(crate::DfsError::StaleOwnerEpoch {
                file_system_id: scope.file_system_id.clone(),
                local_node_id: node.clone(),
                primary_node_id: shard.primary_node_id.clone(),
                epoch: shard.epoch,
            });
        }
        let execution_scope = DfsMessageExecutionScope::ExactShard {
            file_system_id: scope.file_system_id.clone(),
            shard_id: scope.shard_id.clone(),
        };
        let future = async {
            if let Some(deadline) = deadline {
                tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    self.scheduler.admit(&execution_scope),
                )
                .await
                .map_err(|_| crate::DfsError::Metadata {
                    message: "historical prefix intake admission reached its original deadline"
                        .into(),
                })?
            } else {
                self.scheduler.admit(&execution_scope).await
            }
        };
        let admission = if let Ok(current) = tokio::runtime::Handle::try_current() {
            if current.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
                return Err(crate::DfsError::Metadata {
                    message: "synchronous publication cannot queue inside a current-thread runtime"
                        .into(),
                });
            }
            tokio::task::block_in_place(|| self.runtime.block_on(future))?
        } else {
            self.runtime.block_on(future)?
        };
        // Admission can wait across ordinary watermark/proof publication.
        // Rebind to the publication current after that wait, preserving every
        // semantic configuration row selected before it. A withdrawn scope
        // cannot authorize work, and structural changes still require reopening.
        let current = self.authority.configuration_snapshot();
        if current.storage_generation() != self.storage_generation
            || current
                .publication_configuration(&scope.file_system_id)?
                .as_ref()
                .is_none_or(|(current_shards, current_filesystem)| {
                    current_shards
                        .iter()
                        .find(|current| current.shard_id == scope.shard_id.0)
                        != Some(shard)
                        || current_filesystem.as_ref() != Some(&filesystem_config)
                })
        {
            return Err(crate::DfsError::Metadata {
                message: "publication configuration advanced while effect admission queued".into(),
            });
        }
        Ok(DfsPublicationExecutionOwners::from_runtime(
            self.data_root.clone(),
            execution_scope,
            admission,
            Arc::clone(&self.data_owner),
        ))
    }

    #[cfg(test)]
    pub(crate) fn acquire_prefix_history_execution(
        &self,
        assignment: &crate::DfsMetadataShardConfig,
        node: &sinomo_core::node::NodeId,
        round_id: &str,
        deadline: std::time::Instant,
    ) -> std::result::Result<Arc<DfsPublicationExecutionOwners>, crate::DfsError> {
        self.acquire_before(
            &crate::DfsPreparedOperationScope {
                file_system_id: assignment.file_system_id.clone(),
                shard_id: crate::DfsMetadataShardId::new(&assignment.shard_id),
                owner_epoch: assignment.epoch,
                // Existing correlation label for admission/trace only. No V7
                // prepare/digest/session or new logical operation is constructed.
                operation_id: round_id.to_owned(),
            },
            node,
            Some(deadline),
        )
    }

    #[cfg(test)]
    pub(crate) fn validate_prefix_history_writer(
        &self,
        execution: &DfsPublicationExecutionOwners,
        scope: &DfsPublicationOwnerScope,
        expected: &crate::DfsMetadataShardConfig,
        local: &sinomo_core::node::NodeId,
        deadline: std::time::Instant,
    ) -> Result<()> {
        let invalid = || {
            MetadataBackendError::Serde(
                "historical prefix DATA lacks its independent current native writer permission"
                    .into(),
            )
        };
        let actual = crate::runtime_host::publication_runtime_owner(scope.root())
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?
            .ok_or_else(invalid)?;
        if std::time::Instant::now() >= deadline
            || !Arc::ptr_eq(&actual, &self.data_owner)
            || !Arc::ptr_eq(&actual, &execution._data_owner)
            || self.data_root != execution.data_root
            || self.authority.storage_generation() != self.storage_generation
            || !execution.admits(
                scope,
                &expected.file_system_id,
                &crate::DfsMetadataShardId::new(&expected.shard_id),
            )
            || scope.file_system_id() != &expected.file_system_id
            || scope.shard_id().0 != expected.shard_id
            || expected.primary_node_id != *local
        {
            return Err(invalid());
        }
        let snapshot = self.authority.configuration_snapshot();
        let (shards, filesystem) = snapshot
            .publication_configuration(&expected.file_system_id)
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?
            .ok_or_else(invalid)?;
        if filesystem
            .as_ref()
            .is_none_or(|filesystem| filesystem.file_system_id != expected.file_system_id)
            || !shards.iter().any(|shard| shard == expected)
            || snapshot.storage_generation() != self.storage_generation
        {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Default)]
pub(crate) struct DfsPublicationProviderSlot(
    Arc<Mutex<Option<Arc<DfsPublicationExecutionProvider>>>>,
);
pub(crate) struct DfsPublicationProviderRestore {
    slot: DfsPublicationProviderSlot,
    previous: Option<Arc<DfsPublicationExecutionProvider>>,
}
impl DfsPublicationProviderSlot {
    #[cfg(test)]
    pub(crate) fn validate_initial_binding_from_retained_provider(
        &self,
        scope: &DfsPublicationOwnerScope,
        digest: &DfsPreparedOperationDigest,
        execution: &DfsPublicationExecutionOwners,
        expected: &crate::DfsMetadataShardConfig,
    ) -> Result<()> {
        self.current()
            .ok_or_else(|| MetadataBackendError::Serde("test has no retained native provider".into()))?
            .validate_initial_epoch_binding(scope, digest, execution, expected)
    }

    pub(crate) fn retain_runtime_provider(&self, provider: Arc<DfsPublicationExecutionProvider>) {
        *self.0.lock().unwrap_or_else(|error| error.into_inner()) = Some(provider);
    }
    pub(crate) fn install(
        &self,
        provider: Arc<DfsPublicationExecutionProvider>,
    ) -> DfsPublicationProviderRestore {
        let previous = self
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .replace(provider);
        DfsPublicationProviderRestore {
            slot: self.clone(),
            previous,
        }
    }
    /// Borrow the actual installed provider, not publication permission. Every
    /// consumer must still enter its native owner/admission validation path.
    pub(crate) fn current(&self) -> Option<Arc<DfsPublicationExecutionProvider>> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}
impl Drop for DfsPublicationProviderRestore {
    fn drop(&mut self) {
        *self
            .slot
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = self.previous.take();
    }
}

/// Existing runtime admission and OS ownership, retained by every live guard.
#[derive(Debug)]
pub struct DfsPublicationExecutionOwners {
    data_root: PathBuf,
    scope: DfsMessageExecutionScope,
    admission: Mutex<Option<DfsMutationAdmission>>,
    _data_owner: Arc<DfsDataOwnerLock>,
}

impl DfsPublicationExecutionOwners {
    /// Representation context from the retained physical owner only. This
    /// does not grant genesis, publication or successor execution authority.
    pub(crate) fn selected_original_data_layout(
        &self,
    ) -> Result<Option<Arc<crate::DfsSelectedOriginalDataLayout>>> {
        self._data_owner.selected_original_data_layout().map_err(|error| {
            MetadataBackendError::Rejected {
                code: crate::DfsMetadataRejectionCode::Other,
                message: error.to_string(),
            }
        })
    }
    pub(crate) fn data_root(&self) -> &Path {
        &self.data_root
    }
    /// Open through the same physical owner retained by this admission.
    /// Representation admission does not grant metadata publication authority.
    pub(crate) fn open_native_content(&self) -> Result<crate::LocalContentStore> {
        self._data_owner.open_native_content().map_err(|error| match error {
            crate::runtime_host::DfsRuntimeError::StoragePreparationPending { root, processed_records } =>
                MetadataBackendError::StoragePreparationPending { root, processed_records },
            error => MetadataBackendError::Rejected {
                code: crate::DfsMetadataRejectionCode::Other,
                message: error.to_string(),
            },
        })
    }
    pub(crate) fn from_runtime(
        data_root: PathBuf,
        scope: DfsMessageExecutionScope,
        admission: DfsMutationAdmission,
        data_owner: Arc<DfsDataOwnerLock>,
    ) -> Arc<Self> {
        Arc::new(Self {
            data_root,
            scope,
            admission: Mutex::new(Some(admission)),
            _data_owner: data_owner,
        })
    }
    pub(crate) fn admits(
        &self,
        owner: &DfsPublicationOwnerScope,
        admitted_file_system_id: &DfsFileSystemId,
        admitted_shard_id: &DfsMetadataShardId,
    ) -> bool {
        let admission = self
            .admission
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(admission) = admission.as_ref() else {
            return false;
        };
        self._data_owner.owns_backend_root(owner.root())
            && (admission.is_exact() || admission.is_global())
            && match &self.scope {
                DfsMessageExecutionScope::ExactShard {
                    file_system_id,
                    shard_id,
                } => file_system_id == admitted_file_system_id && shard_id == admitted_shard_id,
                DfsMessageExecutionScope::NamespacePaths { file_system_id, .. } => {
                    file_system_id == admitted_file_system_id && admission.is_global()
                }
                DfsMessageExecutionScope::GlobalExclusive => true,
                DfsMessageExecutionScope::ReadOnly => false,
            }
    }
    pub(crate) fn take_admission(self: &Arc<Self>) -> Result<DfsMutationAdmission> {
        if Arc::strong_count(self) != 1 {
            return Err(MetadataBackendError::Serde(
                "publication ownership still retained at materialization transfer".into(),
            ));
        }
        self.admission
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .ok_or_else(|| {
                MetadataBackendError::Serde("publication admission already transferred".into())
            })
    }
}

/// Restores producer custody only after every author capability is consumed.
/// The owner remains inside synchronous work even if its outer waiter leaves.
pub(crate) struct DfsPublicationExecutionScope<'a> {
    owners: Arc<DfsPublicationExecutionOwners>,
    producer: &'a mut Option<DfsMutationAdmission>,
}
impl<'a> DfsPublicationExecutionScope<'a> {
    pub(crate) fn enter(
        data_root: PathBuf,
        scope: DfsMessageExecutionScope,
        producer: &'a mut Option<DfsMutationAdmission>,
        data_owner: Arc<DfsDataOwnerLock>,
    ) -> Result<Self> {
        let admission = producer.take().ok_or_else(|| {
            MetadataBackendError::Serde(
                "publication execution requires actual retained admission".into(),
            )
        })?;
        Ok(Self {
            owners: DfsPublicationExecutionOwners::from_runtime(
                data_root, scope, admission, data_owner,
            ),
            producer,
        })
    }
    pub(crate) fn owners(&self) -> Arc<DfsPublicationExecutionOwners> {
        Arc::clone(&self.owners)
    }
    pub(crate) fn finish(self) -> Result<()> {
        *self.producer = Some(self.owners.take_admission()?);
        Ok(())
    }
}
impl Drop for DfsPublicationExecutionScope<'_> {
    fn drop(&mut self) {
        if self.producer.is_none() {
            // On unwind the service scope has already removed its reference.
            // If an effect still retains ownership, do not detach its permit.
            if let Ok(admission) = self.owners.take_admission() {
                *self.producer = Some(admission);
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DfsPublicationContextSlot(Arc<Mutex<Option<Arc<DfsPublicationExecutionOwners>>>>);
pub(crate) struct DfsPublicationContextRestore {
    slot: DfsPublicationContextSlot,
    previous: Option<Arc<DfsPublicationExecutionOwners>>,
}
impl DfsPublicationContextSlot {
    pub(crate) fn install(
        &self,
        owners: Arc<DfsPublicationExecutionOwners>,
    ) -> DfsPublicationContextRestore {
        let previous = self
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .replace(owners);
        DfsPublicationContextRestore {
            slot: self.clone(),
            previous,
        }
    }
    pub(crate) fn current(&self) -> Option<Arc<DfsPublicationExecutionOwners>> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}
impl Drop for DfsPublicationContextRestore {
    fn drop(&mut self) {
        *self
            .slot
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = self.previous.take();
    }
}

#[derive(Clone, Debug, Default)]
struct SourceAdoptionContext {
    offer: Option<crate::DfsSourceAdoptionOffer>,
    witness: Option<Arc<crate::DfsSourceAdoptionWitness>>,
    released_session: Option<String>,
}
#[derive(Clone, Debug, Default)]
pub(crate) struct DfsSourceAdoptionContextSlot(Arc<Mutex<SourceAdoptionContext>>);
pub(crate) struct DfsSourceAdoptionContextRestore {
    slot: DfsSourceAdoptionContextSlot,
    previous: Option<SourceAdoptionContext>,
}
impl DfsSourceAdoptionContextSlot {
    #[cfg(test)]
    pub(crate) fn has_retained_custody_for_prefix_history(&self) -> bool {
        let current = self.0.lock().unwrap_or_else(|error| error.into_inner());
        current.offer.is_some() || current.witness.is_some()
    }

    pub(crate) fn install(
        &self,
        offer: crate::DfsSourceAdoptionOffer,
    ) -> DfsSourceAdoptionContextRestore {
        let mut current = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let previous = std::mem::replace(
            &mut *current,
            SourceAdoptionContext {
                offer: Some(offer),
                witness: None,
                released_session: None,
            },
        );
        DfsSourceAdoptionContextRestore {
            slot: self.clone(),
            previous: Some(previous),
        }
    }
    pub(crate) fn offer(&self) -> Option<crate::DfsSourceAdoptionOffer> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .offer
            .clone()
    }
    fn witness(&self) -> Option<Arc<crate::DfsSourceAdoptionWitness>> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .witness
            .clone()
    }
    fn clear_released(&self, witness: &Arc<crate::DfsSourceAdoptionWitness>) {
        let mut current = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if current
            .witness
            .as_ref()
            .is_some_and(|actual| Arc::ptr_eq(actual, witness))
        {
            current.witness = None;
            if witness.supplies_source_family() {
                current.released_session = Some(witness.offer_session_id().to_owned());
                current.offer = None;
            }
        }
    }
    fn clear_body_witness(&self, offer: &crate::DfsSourceAdoptionOffer) {
        let mut current = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if current.offer.as_ref() == Some(offer) {
            current.witness = None;
        }
    }
    fn bind(
        &self,
        offer: &crate::DfsSourceAdoptionOffer,
        witness: Arc<crate::DfsSourceAdoptionWitness>,
    ) -> Result<()> {
        let mut current = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if current.offer.as_ref() != Some(offer) {
            return Err(MetadataBackendError::Serde(
                "source adoption execution changed before witness binding".into(),
            ));
        }
        current.witness = Some(witness);
        Ok(())
    }
}
impl Drop for DfsSourceAdoptionContextRestore {
    fn drop(&mut self) {
        if let Some(mut previous) = self.previous.take() {
            let mut current = self
                .slot
                .0
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if current.released_session.as_ref().is_some_and(|released| {
                previous
                    .offer
                    .as_ref()
                    .is_some_and(|offer| &offer.session_id == released)
            }) {
                previous.offer = None;
                previous.witness = None;
            }
            previous.released_session = current
                .released_session
                .take()
                .or(previous.released_session);
            *current = previous;
        }
    }
}

/// Private proof storage; only the real authorization mint constructs a guard.
enum DfsPublicationAuthority {
    Local {
        node_id: sinomo_core::node::NodeId,
    },
    FullShard {
        expected: crate::DfsMetadataShardConfig,
        acknowledged: BTreeSet<sinomo_core::node::NodeId>,
    },
}

pub struct DfsPublicationGuard {
    source_witness: Option<Arc<crate::DfsSourceAdoptionWitness>>,
    digest: DfsPreparedOperationDigest,
    writer: DfsPublicationWriterSession,
    fence: Arc<ShardFenceGuard>,
    authority: DfsPublicationAuthority,
    execution: Option<Arc<DfsPublicationExecutionOwners>>,
    // Last: observe this guard's leases dropping. Source custody can still
    // retain the same writer/fence Arcs; this is not final OS-lock release.
    #[cfg(any(test, feature = "test-support"))]
    _native_trace: Option<crate::native_shard_trace::NativeGuardTrace>,
}
pub(crate) struct DfsPublicationCommitGuard {
    _guard: DfsPublicationGuard,
}
impl DfsPublicationCommitGuard {
    pub(crate) fn source_witness(&self) -> Option<&crate::DfsSourceAdoptionWitness> {
        self._guard.source_witness.as_deref()
    }
    pub(crate) fn record_source_terminal(
        &self,
        commit: &crate::DfsPreparedOperationCommitReference,
        prefix: &crate::DfsMetadataPrefixProof,
        storage: crate::metadata_table_store::MetadataTableStorageGeneration,
    ) -> Result<()> {
        let Some(witness) = self
            .source_witness()
            .filter(|source| source.supplies_source_family())
        else {
            return Ok(());
        };
        let DfsPublicationAuthority::FullShard { expected, .. } = &self._guard.authority else {
            return Err(MetadataBackendError::Serde(
                "native source terminal requires its full assigned author proof".into(),
            ));
        };
        let execution = self._guard.execution.as_ref().ok_or_else(|| {
            MetadataBackendError::Serde(
                "native source terminal lost actual effect execution".into(),
            )
        })?;
        witness.record_kernel_terminal(
            &self._guard.writer.scope,
            expected,
            &execution._data_owner,
            &self._guard.digest,
            commit,
            prefix,
            storage,
        )
    }
}

pub(crate) struct DfsPreparedPublication {
    pub(crate) payload: Arc<crate::DfsPreparedOperationPayload>,
    pub(crate) published: bool,
}
impl std::ops::Deref for DfsPreparedPublication {
    type Target = crate::DfsPreparedOperationPayload;
    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl<T: crate::DfsMessageTransport> crate::NetworkedDfsService<T> {

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn native_publication_owner_for_observation(
        &self,
    ) -> Option<(PathBuf, Arc<DfsDataOwnerLock>)> {
        if let Some(owners) = self.publication_execution.current() {
            return Some((owners.data_root.clone(), Arc::clone(&owners._data_owner)));
        }
        self.publication_provider
            .current()
            .map(|provider| (provider.data_root.clone(), Arc::clone(&provider.data_owner)))
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn observe_native_content_persistence(
        &self,
        shard: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        manifest: &crate::ContentManifest,
        persistence: crate::native_shard_trace::NativeContentPersistenceKindV1,
    ) {
        let Some((root, owner)) = self.native_publication_owner_for_observation() else {
            return;
        };
        if !crate::native_shard_trace::has_interest(&root, shard) {
            return;
        }
        crate::native_shard_trace::record_content_receipt(
            &root,
            &owner,
            shard,
            crate::native_shard_trace::NativeContentReceiptFactsV1::LocalPersisted {
                file_system_id: file_system_id.clone(),
                manifest: manifest.clone(),
                persistence,
            },
        );
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn observe_native_coordinator_receipt(
        &self,
        shard: Option<&DfsMetadataShardId>,
        intent: &crate::DistributedWriteIntent,
        node: &sinomo_core::node::NodeId,
    ) {
        let execution = self.publication_execution.current();
        let actual_shard = execution
            .as_ref()
            .and_then(|execution| match &execution.scope {
                DfsMessageExecutionScope::ExactShard {
                    file_system_id,
                    shard_id,
                } if file_system_id == &intent.file_system_id => Some(shard_id),
                _ => None,
            });
        let Some(shard) = shard.or(actual_shard) else {
            return;
        };
        let Some((root, owner)) = self.native_publication_owner_for_observation() else {
            return;
        };
        if !crate::native_shard_trace::has_interest(&root, shard) {
            return;
        }
        let Some(receipt) = self
            .coordinator
            .receipts_for_intent(&intent.intent_id)
            .into_iter()
            .find(|receipt| receipt.replica_node_id == *node)
        else {
            return;
        };
        crate::native_shard_trace::record_content_receipt(
            &root,
            &owner,
            shard,
            crate::native_shard_trace::NativeContentReceiptFactsV1::CoordinatorDurable {
                intent: intent.clone(),
                receipt: receipt.clone(),
            },
        );
    }

    pub(crate) fn acquire_prepared_execution(
        &self,
        scope: &crate::DfsPreparedOperationScope,
    ) -> std::result::Result<Option<DfsPublicationContextRestore>, crate::DfsError> {
        if self.publication_execution.current().is_some() {
            return Ok(None);
        }
        let Some(provider) = self.publication_provider.current() else {
            return Ok(None);
        };
        let owners = provider.acquire(scope, &self.node_id)?;
        Ok(Some(self.publication_execution.install(owners)))
    }
    pub(crate) fn local_prepared_operation_shard(
        &self,
    ) -> std::result::Result<DfsMetadataShardId, crate::DfsError> {
        let statuses = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "local publication has no native backend".into(),
            })?
            .metadata_shard_statuses()?;
        let [status] = statuses.as_slice() else {
            return Err(crate::DfsError::Metadata {
                message: "local publication requires an exact native shard route".into(),
            });
        };
        Ok(status.shard_id.clone())
    }

    /// Every V7 author publishes the same complete body through one kernel.
    pub(crate) fn publish_prepared_operation(
        &mut self,
        payload: crate::DfsPreparedOperationPayload,
    ) -> std::result::Result<DfsPreparedPublication, crate::DfsError> {
        self.publish_prepared_operation_with_file_system(payload, None)
    }

    pub(crate) fn publish_prepared_operation_with_file_system(
        &mut self,
        payload: crate::DfsPreparedOperationPayload,
        configured_file_system: Option<&crate::DfsFileSystem>,
    ) -> std::result::Result<DfsPreparedPublication, crate::DfsError> {
        let _execution = self.acquire_prepared_execution(&crate::DfsPreparedOperationScope {
            file_system_id: payload.file_system_id.clone(),
            shard_id: payload.shard_id.clone(),
            owner_epoch: payload.owner_epoch,
            operation_id: payload.operation_id.clone(),
        })?;
        let digest = self.prepare_payload_for_publication_with_file_system(
            payload,
            configured_file_system,
        )?;
        self.commit_stored_prepared_operation(&digest)
    }

    /// Seal an explicitly selected policy under the same existing publication
    /// admission. A service cache alone cannot replace a native definition.
    fn bind_admitted_file_system(
        &self,
        payload: &mut crate::DfsPreparedOperationPayload,
        file_system: &crate::DfsFileSystem,
        stored_file_system: Option<&crate::DfsFileSystem>,
    ) -> Result<()> {
        let invalid = || MetadataBackendError::Serde(
            "fresh filesystem policy differs from its current publication admission".into(),
        );
        if file_system.file_system_id != payload.file_system_id
            || payload.metadata_effects.iter().any(|effect| matches!(effect,
                crate::DfsPreparedMetadataEffect::PutFileSystem { .. }))
        {
            return Err(invalid());
        }
        if self.control_plane.is_some() {
            let provider = self.publication_provider.current().ok_or_else(invalid)?;
            let execution = self.publication_execution.current().ok_or_else(invalid)?;
            let scope = self.metadata_backend.as_ref().ok_or_else(invalid)?
                .publication_owner_scope(&payload.file_system_id, &payload.shard_id)?;
            if provider.authority.storage_generation() != provider.storage_generation
                || provider.data_root != execution.data_root
                || !Arc::ptr_eq(&provider.data_owner, &execution._data_owner)
                || !execution.admits(&scope, &payload.file_system_id, &payload.shard_id)
            {
                return Err(invalid());
            }
            // One floor-checked publication supplies the filesystem policy and
            // the shard route.
            let snapshot = provider.authority.write_configuration_snapshot()
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))?
                .ok_or_else(invalid)?;
            let expected = crate::DfsFileSystemConfig::from(file_system);
            let (shards, actual) = snapshot.publication_configuration(&payload.file_system_id)
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))?
                .ok_or_else(invalid)?;
            // Every row comes from this one snapshot, so no generation
            // re-check is needed; the commit point keeps its own fences.
            if actual.as_ref() != Some(&expected)
                || !shards.iter().any(|shard| shard.shard_id == payload.shard_id.0
                    && shard.primary_node_id == self.node_id
                    && shard.epoch == payload.owner_epoch)
                || snapshot.storage_generation() != provider.storage_generation
            {
                return Err(invalid());
            }
        }
        if stored_file_system != Some(file_system) {
            payload.metadata_effects.push(crate::DfsPreparedMetadataEffect::PutFileSystem {
                file_system: file_system.clone(),
            });
        }
        Ok(())
    }

    fn rebind_prepared_publication_instance_if_frozen(
        &mut self,
        payload: &crate::DfsPreparedOperationPayload,
    ) -> std::result::Result<(), crate::DfsError> {
        let Some(control) = self.control_plane.as_mut() else {
            return Ok(());
        };
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "prepare requires a current durable backend".into(),
            })?;
        if !backend.metadata_shard_instance_writes_frozen(&payload.shard_id)? {
            return Ok(());
        }
        // An offer can retain its original job writer even without a body
        // witness. Keep that custody intact and never reacquire the same writer.
        if self.source_adoption.offer().is_some() || self.source_adoption.witness().is_some() {
            return Err(crate::DfsError::Metadata {
                message: "frozen source-adoption custody cannot rebind its held writer".into(),
            });
        }
        let owner = backend.publication_owner_scope(&payload.file_system_id, &payload.shard_id)?;
        let execution =
            self.publication_execution
                .current()
                .ok_or_else(|| crate::DfsError::Metadata {
                    message: "completed-handoff binding requires actual execution custody".into(),
                })?;
        let runtime_owner = crate::runtime_host::publication_runtime_owner(owner.root())
            .map_err(|error| crate::DfsError::Metadata {
                message: error.to_string(),
            })?
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "completed-handoff binding requires the actual data owner".into(),
            })?;
        if !Arc::ptr_eq(&runtime_owner, &execution._data_owner)
            || !execution.admits(&owner, &payload.file_system_id, &payload.shard_id)
        {
            return Err(crate::DfsError::Metadata {
                message: "completed-handoff binding has different execution custody".into(),
            });
        }
        let active = crate::DfsControlPlaneStore::new(control.as_mut())
            .get_metadata_shard(&payload.file_system_id, &payload.shard_id.0)?
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "completed-handoff binding has no current shard".into(),
            })?;
        if active.primary_node_id != self.node_id || active.epoch != payload.owner_epoch {
            return Err(crate::DfsError::StaleOwnerEpoch {
                file_system_id: payload.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: active.primary_node_id,
                epoch: active.epoch,
            });
        }
        // The SAME admission stays held. Only the exact physical instance is
        // rebound; current pending/proof/prefix checks remain in the existing kernel.
        self.rebind_completed_local_metadata_shard_instance_if_frozen(&active)?;
        Ok(())
    }

    pub(crate) fn prepare_payload_for_publication(
        &mut self,
        payload: crate::DfsPreparedOperationPayload,
    ) -> Result<DfsPreparedOperationDigest> {
        self.prepare_payload_for_publication_with_file_system(payload, None)
    }

    pub(crate) fn prepare_payload_for_publication_with_file_system(
        &mut self,
        mut payload: crate::DfsPreparedOperationPayload,
        configured_file_system: Option<&crate::DfsFileSystem>,
    ) -> Result<DfsPreparedOperationDigest> {
        let mut reservations = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| {
                MetadataBackendError::Serde("prepare requires a current durable backend".into())
            })?
            .prepared_operation_reservations(&payload)?;
        reservations.sort_by_key(|record| record.digest.prepare_sequence);
        for record in reservations {
            if record.owner_epoch > payload.owner_epoch || payload.metadata_effects.iter().any(|effect|
                matches!(effect, crate::DfsPreparedMetadataEffect::ResolvePreparedOperation {digest,..} if digest == &record.digest)) {
                continue;
            }
            // Admission serializes this producer with the previous execution,
            // but its durable reservation survives a failed quorum or reply.
            // Finish that exact body before preparing an overlapping successor,
            // including when the authority epoch has not changed. The existing
            // recovery path validates the current primary, original readset and
            // full shard proof; it never rebases either operation's payload.
            let original_operation_id = record.operation_id.clone();
            self.recover_one_prepared_operation(record)
                .map_err(|error| {
                    MetadataBackendError::Serde(format!(
                        "namespace prepare {} overlaps active prepare {}: original recovery failed: {error}",
                        payload.operation_id, original_operation_id
                    ))
                })?;
        }
        self.rebind_prepared_publication_instance_if_frozen(&payload)
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        let (stored_file_system, existing_prepare) = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| {
                MetadataBackendError::Serde("prepare requires a current durable backend".into())
            })?
            .prepared_operation_read_context(
                &payload.shard_id,
                &payload.file_system_id,
                &payload.operation_id,
            )?;
        // A recorded original always keeps its exact filesystem effect (or
        // absence). Only a fresh content/namespace producer supplies a policy.
        if existing_prepare.is_none()
            && let Some(file_system) = configured_file_system
        {
            self.bind_admitted_file_system(
                &mut payload,
                file_system,
                stored_file_system.as_ref(),
            )?;
        }
        let supersedes_original = payload.metadata_effects.iter().any(|effect| {
            matches!(
                effect,
                crate::DfsPreparedMetadataEffect::ResolvePreparedOperation {
                    status: crate::DfsPreparedOperationStatus::Superseded,
                    ..
                }
            )
        });
        // Recovery re-certifies the original complete body. A refreshed local
        // filesystem projection must not append an unrelated effect to it;
        // the kernel still checks the exact retiring body and current authority.
        let captured_negative_outcome = payload.metadata_effects.iter().any(|effect| matches!(effect,
            crate::DfsPreparedMetadataEffect::PutIntent { intent }
                if matches!(intent.status, crate::DfsWriteIntentStatus::PreconditionFailed { .. })));
        // An ordinary publication may bootstrap a missing definition. Updating
        // an existing definition belongs to its explicit filesystem producer,
        // not whichever service cache happened to prepare this operation.
        if stored_file_system.is_none() && !supersedes_original && !captured_negative_outcome && !payload.metadata_effects.iter().any(|effect| matches!(effect,
            crate::DfsPreparedMetadataEffect::PutFileSystem { file_system } if file_system.file_system_id == payload.file_system_id)) {
            let file_system = self.metadata.filesystems.get(&payload.file_system_id).cloned()
                .or_else(|| stored_file_system.clone())
                .ok_or_else(|| MetadataBackendError::Serde("prepare has no actual filesystem definition".into()))?;
            if stored_file_system.as_ref() != Some(&file_system) {
                payload.metadata_effects.push(crate::DfsPreparedMetadataEffect::PutFileSystem { file_system });
                payload.validate()?;
            }
        }
        let completes_request =
            payload.completed_request.is_some() || payload.completed_request_completion.is_some();
        #[cfg(test)]
        if payload.prepared_intent.as_ref().is_some_and(|intent| {
            intent.touch_parent_on_create
                && payload.terminal_intent.is_some()
                && payload.transaction.objects.iter().any(|object| {
                    object.object_id == intent.object_id && object.version == intent.target_version
                })
        }) {
            let before_prepare =
                crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| hook.borrow_mut().take());
            if let Some(before_prepare) = before_prepare {
                before_prepare(&payload);
            }
        }
        if let Some(existing) = existing_prepare {
            if existing.payload.as_ref() == &payload
                && existing.digest.payload_digest == payload.canonical_digest()?
                && existing.status == crate::DfsPreparedOperationStatus::Committed
            {
                return Ok(existing.digest);
            }
        }
        let offer = self.source_adoption.offer();
        let source_witness = if let Some(offer) = offer {
            self.source_adoption.clear_body_witness(&offer);
            let execution = self.publication_execution.current().ok_or_else(|| {
                MetadataBackendError::Serde(
                    "source adoption requires the actual target execution owner".into(),
                )
            })?;
            let actor = crate::source_adoption::actor_for_root(&execution.data_root)?;
            let adopts = payload.transaction.objects.iter().any(|object| object == &offer.destination)
                || payload.metadata_effects.iter().any(|effect| matches!(effect,
                    crate::DfsPreparedMetadataEffect::PutObject { object } if object == &offer.destination));
            let witness = if adopts {
                Some(actor.claim(&offer, &payload)?)
            } else {
                actor.job_writer_lease(&offer, &payload)?
            };
            if let Some(witness) = witness.as_ref() {
                self.source_adoption.bind(&offer, Arc::clone(witness))?;
            }
            witness
        } else {
            None
        };
        let digest = self
            .metadata_backend
            .as_mut()
            .ok_or_else(|| {
                MetadataBackendError::Serde(
                    "prepared publication requires a durable backend".into(),
                )
            })?
            .prepare_operation(payload, source_witness.as_deref())?;
        // Record actual durable custody BEFORE authorization can fail or lose
        // a response. A retained request-owned prepare is not DefinitelyNotSent.
        if completes_request {
            self.namespace_execution.record_prepare();
        }
        Ok(digest)
    }

    /// Exact retry names the captured durable body; it introduces no new effect.
    pub(crate) fn commit_stored_prepared_operation(
        &mut self,
        digest: &DfsPreparedOperationDigest,
    ) -> std::result::Result<DfsPreparedPublication, crate::DfsError> {
        let record = self
            .metadata_backend
            .as_ref()
            .expect("prepared backend")
            .prepared_operation_record(&digest.operation_id)?
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "durable prepare lost its immutable body".into(),
            })?;
        record.validate()?;
        if record.digest != *digest {
            return Err(crate::DfsError::Metadata {
                message: "publication names no exact durable prepare".into(),
            });
        }
        let payload = Arc::clone(&record.payload);
        if record.status == crate::DfsPreparedOperationStatus::Committed {
            self.refresh_recorded_prepared_rows(&payload)?;
            return Ok(DfsPreparedPublication {
                payload,
                published: false,
            });
        }
        #[cfg(test)]
        if let Some(before_execution) =
            BEFORE_STORED_PREPARE_EXECUTION.with(|hook| hook.borrow_mut().take())
        {
            before_execution();
        }
        let _execution = self.acquire_prepared_execution(&crate::DfsPreparedOperationScope {
            file_system_id: digest.file_system_id.clone(),
            shard_id: digest.shard_id.clone(),
            owner_epoch: digest.owner_epoch,
            operation_id: digest.operation_id.clone(),
        })?;
        // Admission can wait behind another normal recovery owner. A shared
        // read snapshot may already see its new record while this projection's
        // resident exact-prefix proof still names the older cut. Refresh only
        // this shard after acquiring admission, then observe the terminal state
        // again before attempting any new certification. Do not reopen a writer
        // already retained by an adoption offer/body witness across yields.
        let record = if _execution.is_some()
            && self.source_adoption.offer().is_none()
            && self.source_adoption.witness().is_none()
        {
            let backend = self.metadata_backend.as_mut().expect("prepared backend");
            backend.refresh_metadata_shard_attestation_view(&digest.shard_id)?;
            let current = backend
                .prepared_operation_record(&digest.operation_id)?
                .ok_or_else(|| crate::DfsError::Metadata {
                    message: "admitted retry lost its original durable prepare".into(),
                })?;
            current.validate()?;
            if current.digest != *digest || current.payload != payload {
                return Err(crate::DfsError::Metadata {
                    message: "admitted retry differs from its original durable prepare".into(),
                });
            }
            if current.status == crate::DfsPreparedOperationStatus::Committed {
                self.refresh_recorded_prepared_rows(&payload)?;
                return Ok(DfsPreparedPublication {
                    payload,
                    published: false,
                });
            }
            current
        } else {
            record
        };
        let guard = self.authorize_prepared_publication_from_record(digest, record)?;
        #[cfg(test)]
        if let Some(after_authorization) =
            AFTER_PREPARED_AUTHORIZATION.with(|hook| hook.borrow_mut().take())
        {
            after_authorization(digest, &payload);
        }
        let source_witness = guard.source_witness.clone();
        let committed = self
            .metadata_backend
            .as_mut()
            .expect("prepared backend")
            .commit_prepared_operation(digest, guard)?;
        if !committed.published {
            self.refresh_recorded_prepared_rows(&payload)?;
        } else if let Some(witness) = source_witness {
            // This follows the actual durable kernel result. Errors and
            // ambiguous outcomes retain source custody for reconciliation.
            witness.release_after_effect(digest)?;
            self.source_adoption.clear_released(&witness);
        }
        Ok(DfsPreparedPublication {
            payload,
            published: committed.published,
        })
    }

    /// The sole V7 author mint. It reads the actual prepared record and
    /// obtains actual operation-bound evidence; callers supply no ACK claims.
    #[cfg(test)]
    pub(crate) fn authorize_prepared_publication(
        &mut self,
        digest: &DfsPreparedOperationDigest,
    ) -> std::result::Result<DfsPublicationGuard, crate::DfsError> {
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "prepared publication requires an actual durable backend".into(),
            })?;
        let record = backend
            .prepared_operation_record(&digest.operation_id)?
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "publication names no exact durable prepare".into(),
            })?;
        self.authorize_prepared_publication_from_record(digest, record)
    }

    // Both callers obtained this immutable body from the durable backend.
    // Reusing the observation avoids a second snapshot; the writer session
    // below still validates the current canonical record under its own gate.
    fn authorize_prepared_publication_from_record(
        &mut self,
        digest: &DfsPreparedOperationDigest,
        record: crate::DfsPreparedOperationRecord,
    ) -> std::result::Result<DfsPublicationGuard, crate::DfsError> {
        let backend = self.metadata_backend.as_ref().expect("prepared backend");
        record.validate()?;
        if record.digest != *digest {
            return Err(crate::DfsError::Metadata {
                message: "publication names no exact durable prepare".into(),
            });
        }
        let payload = Arc::clone(&record.payload);
        let scope = backend.publication_owner_scope(&digest.file_system_id, &digest.shard_id)?;
        let execution = self.publication_execution.current();
        let runtime_owner =
            crate::runtime_host::publication_runtime_owner(scope.root()).map_err(|error| {
                crate::DfsError::Metadata {
                    message: error.to_string(),
                }
            })?;
        if self.control_plane.is_some() && (execution.is_none() || runtime_owner.is_none()) {
            return Err(crate::DfsError::Metadata {
                message:
                    "clustered publication requires actual runtime admission and data-owner custody"
                        .into(),
            });
        }
        if let Some(owner) = runtime_owner.as_ref() {
            let Some(execution) = execution.as_ref() else {
                return Err(crate::DfsError::Metadata {
                    message: "runtime publication has no owned execution admission".into(),
                });
            };
            if !Arc::ptr_eq(owner, &execution._data_owner)
                || !execution.admits(&scope, &digest.file_system_id, &digest.shard_id)
            {
                return Err(crate::DfsError::Metadata {
                    message: "runtime publication ownership changed".into(),
                });
            }
        }
        let operation = crate::DfsMetadataAuthorityOperation::PreparedOperation {
            operation_id: digest.operation_id.clone(),
            file_system_id: digest.file_system_id.clone(),
            shard_id: digest.shard_id.clone(),
            owner_epoch: digest.owner_epoch,
            prepare_sequence: digest.prepare_sequence,
            payload_digest: digest.payload_digest.clone(),
        };
        let authorization = if self.control_plane.is_some() {
            let context = self
                .retained_metadata_replica_context(&digest.file_system_id, &digest.shard_id)?
                .ok_or_else(|| crate::DfsError::Metadata {
                    message: "publication requires the host's coherent shard configuration".into(),
                })?;
            if digest.owner_epoch == 1 && digest.operation_id == crate::default_cluster::genesis::genesis_operation_id(&context.shard) {
                context.namespace_catalog.as_ref().ok_or_else(|| crate::DfsError::Metadata {
                    message: "native genesis requires its actual complete configuration catalog".into(),
                })?.validate_native_genesis_preparation(&record)?;
                let generation = crate::storage_generation::read_generation(scope.root())?;
                let original = crate::default_cluster::genesis::original_genesis_file_system(&record, &context.shard)?;
                if generation.identity.file_system_id != digest.file_system_id
                    || generation.identity.shard_id != digest.shard_id
                    || !generation.identity.matches_native_file_system_storage(original.metadata_storage.as_ref()) {
                    return Err(crate::DfsError::Metadata { message: "native genesis header differs from its original storage binding".into() });
                }
            }
            let shard = context.shard;
            let batch = self
                .metadata_backend
                .as_ref()
                .expect("checked durable backend")
                .journal_batch_for_shard_since_limited(
                    &digest.shard_id,
                    digest.prepare_sequence.saturating_sub(1),
                    1,
                    usize::MAX,
                )?
                .ok_or_else(|| crate::DfsError::Metadata {
                    message: "publication prepare frame is unavailable".into(),
                })?;
            Some(self.authorize_metadata_shard_operation(&shard, &operation, &batch)?)
        } else {
            None
        };
        let source_witness = self.source_adoption.witness();
        let custody = source_witness
            .as_deref()
            .map(|source| source.target_writer_custody(&scope))
            .transpose()?
            .flatten();
        let fence = if let Some(custody) = &custody {
            custody.validate_scope(&scope)?;
            custody.fence()
        } else {
            Arc::new(shard_fence_guard(&scope))
        };
        let provider = self.publication_provider.current();
        let clustered = authorization.as_ref().map(|authorization| {
            let crate::DfsCanonicalAuthorization::FullShard { expected_shard, .. } = authorization;
            expected_shard
        });
        let writer = self
            .metadata_backend
            .as_mut()
            .expect("checked durable backend")
            .publication_writer_session(
                &digest.file_system_id,
                &digest.shard_id,
                digest,
                source_witness.as_deref(),
                provider.as_deref(),
                execution.as_deref(),
                clustered,
            )?;
        #[cfg(any(test, feature = "test-support"))]
        let mut native_trace =
            crate::native_shard_trace::begin_guard_trace(scope.root(), &digest.shard_id, digest);
        let authority = match authorization {
            Some(crate::DfsCanonicalAuthorization::FullShard {
                expected_shard,
                acknowledged_node_ids,
                operation_digest,
                prepare_sequence,
            }) => {
                let configuration_matches = operation_digest == digest.payload_digest
                    && prepare_sequence == digest.prepare_sequence
                    && {
                        #[cfg(any(test, feature = "test-support"))]
                        let _native_configuration_scope = native_trace
                            .as_ref()
                            .map(|trace| trace.enter_retained_configuration());
                        // Observe this existing verified view, not another read.
                        self.retained_metadata_replica_context(
                            &digest.file_system_id,
                            &digest.shard_id,
                        )?
                        .as_ref()
                        .is_some_and(|context| context.shard == expected_shard)
                    };
                if !configuration_matches {
                    return Err(crate::DfsError::StaleOwnerEpoch {
                        file_system_id: digest.file_system_id.clone(),
                        local_node_id: self.node_id.clone(),
                        primary_node_id: expected_shard.primary_node_id,
                        epoch: expected_shard.epoch,
                    });
                }
                DfsPublicationAuthority::FullShard {
                    expected: expected_shard,
                    acknowledged: acknowledged_node_ids.into_iter().collect(),
                }
            }
            None => {
                // Local mode is positively established from the fresh native
                // writer view. No control-plane absence or cached owner grants it.
                let filesystem = self
                    .metadata_backend
                    .as_ref()
                    .expect("checked durable backend")
                    .publication_writer_file_system(&digest.file_system_id, &writer)?
                    .or_else(|| {
                        payload
                            .metadata_effects
                            .iter()
                            .find_map(|effect| match effect {
                                crate::DfsPreparedMetadataEffect::PutFileSystem { file_system }
                                    if file_system.file_system_id == digest.file_system_id
                                        && file_system.owner_node_id == self.node_id
                                        && digest.owner_epoch == 0 =>
                                {
                                    Some(file_system.clone())
                                }
                                _ => None,
                            })
                    })
                    .ok_or_else(|| crate::DfsError::UnknownFileSystem {
                        file_system_id: digest.file_system_id.clone(),
                    })?;
                if digest.owner_epoch != 0
                    || filesystem.owner_node_id != self.node_id
                    || payload
                        .transaction
                        .objects
                        .iter()
                        .any(|object| object.owner_node_id != self.node_id)
                {
                    return Err(crate::DfsError::Metadata {
                        message: "local publication has no actual local author owner".into(),
                    });
                }
                DfsPublicationAuthority::Local {
                    node_id: self.node_id.clone(),
                }
            }
        };
        if let Some(witness) = source_witness.as_deref() {
            witness.validate_for(&payload, digest)?;
        }
        let guard = DfsPublicationGuard::from_authorized(
            digest.clone(),
            writer,
            fence,
            authority,
            execution,
            source_witness,
        )
        .map_err(crate::DfsError::from)?;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(trace) = native_trace.as_mut() {
            use crate::native_shard_trace::{
                NativeGuardAuthorityV1, NativeGuardExecutionV1, NativeGuardFactsV1,
                NativeGuardScopeV1,
            };
            let authority = match &guard.authority {
                DfsPublicationAuthority::Local { node_id } => NativeGuardAuthorityV1::Local {
                    node_id: node_id.clone(),
                },
                DfsPublicationAuthority::FullShard {
                    expected,
                    acknowledged,
                } => NativeGuardAuthorityV1::FullShard {
                    expected: expected.clone(),
                    authenticated_acknowledged_node_ids: acknowledged.iter().cloned().collect(),
                },
            };
            let execution = guard.execution.as_ref().map(|owners| {
                let scheduler = owners
                    .admission
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .map(|admission| admission.scheduler());
                NativeGuardExecutionV1 {
                    execution_owner_handle: format!("{:p}", Arc::as_ptr(owners)),
                    admission_scheduler_handle: scheduler
                        .as_ref()
                        .map(|scheduler| format!("{:p}", Arc::as_ptr(scheduler)))
                        .unwrap_or_default(),
                    execution_scope_debug: format!("{:?}", owners.scope),
                }
            });
            #[cfg(unix)]
            let cached_root_identity = Some((
                scope.root_identity.0.to_string(),
                scope.root_identity.1.to_string(),
            ));
            #[cfg(not(unix))]
            let cached_root_identity = None;
            trace.admitted(
                NativeGuardFactsV1 {
                    actor_node_id: self.node_id.clone(),
                    authority,
                    physical_scope: NativeGuardScopeV1 {
                        canonical_root: scope.root().to_owned(),
                        file_system_id: scope.file_system_id().clone(),
                        shard_id: scope.shard_id().clone(),
                        cached_root_identity,
                    },
                    execution,
                },
                self.metadata_backend.as_ref().and_then(|backend| {
                    backend.native_held_snapshot_for_scope(&digest.file_system_id, &digest.shard_id)
                }),
                runtime_owner.as_ref(),
            );
        }
        #[cfg(any(test, feature = "test-support"))]
        let guard = {
            let mut guard = guard;
            guard._native_trace = native_trace;
            guard
        };
        Ok(guard)
    }
}
impl std::fmt::Debug for DfsPublicationGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsPublicationGuard")
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}
impl DfsPublicationGuard {
    fn from_authorized(
        digest: DfsPreparedOperationDigest,
        writer: DfsPublicationWriterSession,
        fence: Arc<ShardFenceGuard>,
        authority: DfsPublicationAuthority,
        execution: Option<Arc<DfsPublicationExecutionOwners>>,
        source_witness: Option<Arc<crate::DfsSourceAdoptionWitness>>,
    ) -> Result<Self> {
        if !writer.scope.matches(&digest) || writer.digest != digest || fence.scope != writer.scope
        {
            return Err(MetadataBackendError::Serde(
                "publication capability binding mismatch".into(),
            ));
        }
        let valid_authority = match &authority {
            DfsPublicationAuthority::Local { node_id } => {
                digest.owner_epoch == 0 && !node_id.to_string().is_empty()
            }
            DfsPublicationAuthority::FullShard {
                expected,
                acknowledged,
            } => {
                let required = expected
                    .replica_node_ids
                    .iter()
                    .chain(std::iter::once(&expected.primary_node_id))
                    .cloned()
                    .collect::<BTreeSet<_>>();
                expected.file_system_id == digest.file_system_id
                    && expected.shard_id == digest.shard_id.0
                    && expected.epoch == digest.owner_epoch
                    && &required == acknowledged
            }
        };
        if !valid_authority {
            return Err(MetadataBackendError::Serde(
                "publication has no valid bound author certificate".into(),
            ));
        }
        if execution.as_ref().is_some_and(|owners| {
            !owners.admits(&writer.scope, &digest.file_system_id, &digest.shard_id)
        }) {
            return Err(MetadataBackendError::Serde(
                "publication execution ownership mismatch".into(),
            ));
        }
        Ok(Self {
            source_witness,
            digest,
            writer,
            fence,
            authority,
            execution,
            #[cfg(any(test, feature = "test-support"))]
            _native_trace: None,
        })
    }
    pub(crate) fn consume_for(
        self,
        scope: &DfsPublicationOwnerScope,
        digest: &DfsPreparedOperationDigest,
    ) -> Result<DfsPublicationCommitGuard> {
        if &self.digest != digest || &self.writer.scope != scope || !scope.matches(digest) {
            return Err(MetadataBackendError::Serde(
                "publication digest or owner scope changed".into(),
            ));
        }
        Ok(DfsPublicationCommitGuard { _guard: self })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_admission::DfsMutationScheduler;
    use sinomo_core::local::SinomoDataDir;
    use std::time::Duration;

    #[test]
    fn shard_fence_excludes_one_actual_backend_and_allows_another_owner() {
        let first = sinomo_testdir::TestDir::new("publication-fence-first-owner");
        let second = sinomo_testdir::TestDir::new("publication-fence-second-owner");
        let filesystem = sinomo_core::Id::generate();
        let shard = DfsMetadataShardId::new("root");
        let first_scope =
            DfsPublicationOwnerScope::for_backend(first.path(), filesystem.clone(), shard.clone())
                .unwrap();
        let second_scope =
            DfsPublicationOwnerScope::for_backend(second.path(), filesystem, shard).unwrap();
        let held = shard_fence_guard(&first_scope);
        let (tx, rx) = std::sync::mpsc::channel();
        let other = std::thread::spawn(move || {
            let _other_owner = shard_fence_guard(&second_scope);
            tx.send(()).unwrap();
        });
        let independent = rx.recv_timeout(Duration::from_secs(1));
        // Always release and join, including a failed isolation assertion.
        drop(held);
        other.join().unwrap();
        assert!(
            independent.is_ok(),
            "different native roots must not share a publication fence"
        );

        let held = shard_fence_guard(&first_scope);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (tx, rx) = std::sync::mpsc::channel();
        let same = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let _same_owner = shard_fence_guard(&first_scope);
            tx.send(()).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(25)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        drop(held);
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        same.join().unwrap();
    }

    #[tokio::test]
    async fn publication_scope_restores_custody_before_materialization_transfer() {
        let root = sinomo_testdir::TestDir::new("publication-execution-custody");
        let data_dir = SinomoDataDir::new(root.path());
        let owner = Arc::new(
            DfsDataOwnerLock::acquire(&crate::runtime_host::DfsRuntimePaths::new(&data_dir))
                .unwrap(),
        );
        let scheduler = Arc::new(DfsMutationScheduler::new());
        let mut producer = Some(
            scheduler
                .admit(&DfsMessageExecutionScope::GlobalExclusive)
                .await
                .unwrap(),
        );
        let scope = DfsPublicationExecutionScope::enter(
            data_dir.root(),
            DfsMessageExecutionScope::GlobalExclusive,
            &mut producer,
            Arc::clone(&owner),
        )
        .unwrap();
        let slot = DfsPublicationContextSlot::default();
        let restore = slot.install(scope.owners());
        assert!(
            scope.owners.take_admission().is_err(),
            "a live author scope must retain real admission"
        );
        assert!(scheduler.try_admit_global().is_none());
        drop(restore);
        assert!(slot.current().is_none());
        scope.finish().unwrap();
        assert!(
            producer
                .as_ref()
                .is_some_and(DfsMutationAdmission::is_global)
        );
        assert!(
            scheduler.try_admit_global().is_none(),
            "producer still owns admission until its real follow-up takes it"
        );
        assert!(
            DfsDataOwnerLock::acquire(&crate::runtime_host::DfsRuntimePaths::new(&data_dir))
                .is_err()
        );
        drop(producer);
        assert!(scheduler.try_admit_global().is_some());
        drop(owner);
        DfsDataOwnerLock::acquire(&crate::runtime_host::DfsRuntimePaths::new(&data_dir)).unwrap();
    }

    #[tokio::test]
    async fn publication_context_is_restored_when_service_work_panics() {
        let root = sinomo_testdir::TestDir::new("publication-context-unwind");
        let data_dir = SinomoDataDir::new(root.path());
        let owner = Arc::new(
            DfsDataOwnerLock::acquire(&crate::runtime_host::DfsRuntimePaths::new(&data_dir))
                .unwrap(),
        );
        let scheduler = Arc::new(DfsMutationScheduler::new());
        let mut producer = Some(
            scheduler
                .admit(&DfsMessageExecutionScope::GlobalExclusive)
                .await
                .unwrap(),
        );
        let scope = DfsPublicationExecutionScope::enter(
            data_dir.root(),
            DfsMessageExecutionScope::GlobalExclusive,
            &mut producer,
            owner,
        )
        .unwrap();
        let slot = DfsPublicationContextSlot::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _restore = slot.install(scope.owners());
            panic!("publication service unwind");
        }));
        assert!(result.is_err());
        assert!(slot.current().is_none());
        scope.finish().unwrap();
        drop(producer);
        assert!(
            scheduler.try_admit_global().is_some(),
            "unwind must not leave detached execution custody"
        );
    }
}
