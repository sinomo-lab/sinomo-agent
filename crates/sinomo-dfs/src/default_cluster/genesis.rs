use super::*;
use crate::{
    DfsPreparedMetadataEffect, DfsPreparedOperationPayload, DfsPreparedOperationScope,
    DfsPreparedOperationStatus,
};

#[cfg(test)]
pub(crate) const NATIVE_GENESIS_COMPLETION_DEFERRED: &str =
    "injected exact native genesis completion deferred before commit";

#[cfg(test)]
#[derive(Debug)]
struct NativeGenesisDeferralState {
    data_root: PathBuf,
    root_identity: (u64, u64),
    owner_identity: (u64, u64),
    shard: DfsMetadataShardConfig,
    active: std::sync::atomic::AtomicBool,
    hits: std::sync::atomic::AtomicUsize,
    original: Mutex<Option<crate::DfsPreparedOperationRecord>>,
}

#[cfg(test)]
type NativeGenesisDeferralKey = (PathBuf, DfsFileSystemId, String);

#[cfg(test)]
static NATIVE_GENESIS_DEFERRALS: OnceLock<
    Mutex<BTreeMap<NativeGenesisDeferralKey, std::sync::Weak<NativeGenesisDeferralState>>>,
> = OnceLock::new();

#[cfg(test)]
fn native_genesis_physical_identity(
    data_dir: &SinomoDataDir,
) -> Result<((u64, u64), (u64, u64)), DfsError> {
    let root = data_dir.root();
    let named = std::fs::symlink_metadata(&root).map_err(|e| DfsError::Metadata {
        message: e.to_string(),
    })?;
    if !named.is_dir() || named.file_type().is_symlink() {
        return Err(DfsError::Metadata {
            message: "native genesis deferral requires its actual physical DATA directory".into(),
        });
    }
    let directory = std::fs::File::open(&root).map_err(|e| DfsError::Metadata {
        message: e.to_string(),
    })?;
    let root_identity =
        crate::runtime_host::storage_file_identity(&directory).map_err(|e| DfsError::Metadata {
            message: e.to_string(),
        })?;
    let owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
        &crate::runtime_host::DfsRuntimePaths::new(data_dir),
    )
    .map_err(|e| DfsError::Metadata {
        message: e.to_string(),
    })?;
    let owner_identity = owner
        .storage_owner_file_identity()
        .map_err(|e| DfsError::Metadata {
            message: e.to_string(),
        })?;
    Ok((root_identity, owner_identity))
}

/// Negative-only completion selection. It retains no native owner, admission,
/// metadata lock or serving capability; normal return releases every owner.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct NativeGenesisCompletionDeferral {
    state: Arc<NativeGenesisDeferralState>,
}

#[cfg(test)]
impl NativeGenesisCompletionDeferral {
    /// Restore only a negative test selection before actual component mounting.
    /// Expected physical identities/body are DATA, never an owner or authority.
    /// The ordinary hook still requires the newly acquired real native owner.
    #[cfg(unix)]
    pub(crate) fn restore_before_mount(
        data_dir: &SinomoDataDir,
        shard: &DfsMetadataShardConfig,
        original: &crate::DfsPreparedOperationRecord,
        root_identity: (u64, u64),
        owner_identity: (u64, u64),
    ) -> Result<Self, DfsError> {
        original.validate()?;
        if shard.epoch != 1
            || !shard.replica_node_ids.is_empty()
            || original.digest.operation_id != genesis_operation_id(shard)
            || original.digest.file_system_id != shard.file_system_id
            || original.digest.shard_id.0 != shard.shard_id
            || original.digest.owner_epoch != 1
            || original.status != DfsPreparedOperationStatus::Prepared
            || original.committed_entry.is_some()
        {
            return Err(DfsError::Metadata {
                message:
                    "retained genesis negative selection differs from its original singleton body"
                        .into(),
            });
        }
        for (path, identity, directory) in [
            (data_dir.root(), root_identity, true),
            (
                data_dir.root().join("daemon/data-owner.lock"),
                owner_identity,
                false,
            ),
        ] {
            let named = std::fs::symlink_metadata(&path).map_err(|e| DfsError::Metadata {
                message: e.to_string(),
            })?;
            if named.file_type().is_symlink()
                || named.is_dir() != directory
                || (!directory && !named.is_file())
            {
                return Err(DfsError::Metadata {
                    message: "retained genesis negative selection has a foreign physical path"
                        .into(),
                });
            }
            let file = std::fs::File::open(&path).map_err(|e| DfsError::Metadata {
                message: e.to_string(),
            })?;
            if crate::runtime_host::storage_file_identity(&file).map_err(|e| {
                DfsError::Metadata {
                    message: e.to_string(),
                }
            })? != identity
            {
                return Err(DfsError::Metadata {
                    message: "retained genesis negative selection physical identity changed".into(),
                });
            }
        }
        let state = Arc::new(NativeGenesisDeferralState {
            data_root: data_dir.root(),
            root_identity,
            owner_identity,
            shard: shard.clone(),
            active: std::sync::atomic::AtomicBool::new(true),
            hits: std::sync::atomic::AtomicUsize::new(0),
            original: Mutex::new(Some(original.clone())),
        });
        let mut registry = NATIVE_GENESIS_DEFERRALS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .map_err(|_| DfsError::Metadata {
                message: "native genesis deferral registry poisoned".into(),
            })?;
        let key = (
            state.data_root.clone(),
            shard.file_system_id.clone(),
            shard.shard_id.clone(),
        );
        if registry
            .get(&key)
            .and_then(std::sync::Weak::upgrade)
            .is_some()
        {
            return Err(DfsError::Metadata {
                message: "native genesis completion already has a scoped owner".into(),
            });
        }
        registry.insert(key, Arc::downgrade(&state));
        Ok(Self { state })
    }

    pub(crate) fn install(
        data_dir: &SinomoDataDir,
        shard: &DfsMetadataShardConfig,
    ) -> Result<Self, DfsError> {
        if shard.epoch != 1 || !shard.replica_node_ids.is_empty() {
            return Err(DfsError::Metadata {
                message: "genesis deferral accepts only the supported initial singleton".into(),
            });
        }
        let (root_identity, owner_identity) = native_genesis_physical_identity(data_dir)?;
        let state = Arc::new(NativeGenesisDeferralState {
            data_root: data_dir.root(),
            root_identity,
            owner_identity,
            shard: shard.clone(),
            active: std::sync::atomic::AtomicBool::new(true),
            hits: std::sync::atomic::AtomicUsize::new(0),
            original: Mutex::new(None),
        });
        let mut registry = NATIVE_GENESIS_DEFERRALS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .map_err(|_| DfsError::Metadata {
                message: "native genesis deferral registry poisoned".into(),
            })?;
        let key = (
            state.data_root.clone(),
            shard.file_system_id.clone(),
            shard.shard_id.clone(),
        );
        if registry
            .get(&key)
            .and_then(std::sync::Weak::upgrade)
            .is_some()
        {
            return Err(DfsError::Metadata {
                message: "native genesis completion already has a scoped owner".into(),
            });
        }
        registry.insert(key, Arc::downgrade(&state));
        Ok(Self { state })
    }
    pub(crate) fn original_record(
        &self,
    ) -> Result<Option<crate::DfsPreparedOperationRecord>, DfsError> {
        self.state
            .original
            .lock()
            .map(|record| record.clone())
            .map_err(|_| DfsError::Metadata {
                message: "native genesis deferral record poisoned".into(),
            })
    }
    pub(crate) fn hits(&self) -> usize {
        self.state.hits.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(crate) fn release(&self) {
        self.state
            .active
            .store(false, std::sync::atomic::Ordering::Release);
        if let Some(registry) = NATIVE_GENESIS_DEFERRALS.get()
            && let Ok(mut registry) = registry.lock()
        {
            let key = (
                self.state.data_root.clone(),
                self.state.shard.file_system_id.clone(),
                self.state.shard.shard_id.clone(),
            );
            if registry
                .get(&key)
                .and_then(std::sync::Weak::upgrade)
                .is_some_and(|current| Arc::ptr_eq(&current, &self.state))
            {
                registry.remove(&key);
            }
        }
    }
}

#[cfg(test)]
impl Drop for NativeGenesisCompletionDeferral {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
fn defer_native_genesis_completion(
    data_dir: &SinomoDataDir,
    shard: &DfsMetadataShardConfig,
    record: &crate::DfsPreparedOperationRecord,
) -> Result<(), DfsError> {
    let Some(registry) = NATIVE_GENESIS_DEFERRALS.get() else {
        return Ok(());
    };
    let state = registry
        .lock()
        .map_err(|_| DfsError::Metadata {
            message: "native genesis deferral registry poisoned".into(),
        })?
        .get(&(
            data_dir.root(),
            shard.file_system_id.clone(),
            shard.shard_id.clone(),
        ))
        .and_then(std::sync::Weak::upgrade);
    let Some(state) = state else {
        return Ok(());
    };
    if !state.active.load(std::sync::atomic::Ordering::Acquire) {
        return Ok(());
    }
    record.validate()?;
    if state.shard != *shard
        || record.digest.operation_id != genesis_operation_id(shard)
        || record.digest.file_system_id != shard.file_system_id
        || record.digest.shard_id.0 != shard.shard_id
        || record.digest.owner_epoch != 1
        || record.status != DfsPreparedOperationStatus::Prepared
        || record.committed_entry.is_some()
        || native_genesis_physical_identity(data_dir)?
            != (state.root_identity, state.owner_identity)
    {
        return Err(DfsError::Metadata {
            message: "native genesis completion deferral lost its exact physical scope or original"
                .into(),
        });
    }
    let mut original = state.original.lock().map_err(|_| DfsError::Metadata {
        message: "native genesis deferral record poisoned".into(),
    })?;
    if original.as_ref().is_some_and(|old| old != record) {
        return Err(DfsError::Metadata {
            message: "deferred native genesis original body or digest changed".into(),
        });
    }
    original.get_or_insert_with(|| record.clone());
    state.hits.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    Err(DfsError::Metadata {
        message: NATIVE_GENESIS_COMPLETION_DEFERRED.into(),
    })
}

/// The same negative selection at the shared first-commit kernel. Hydration
/// and ordinary recovery can reach that kernel without bootstrap completion.
/// The caller already owns its real writer/guard; do not acquire another one.
#[cfg(test)]
pub(crate) fn defer_native_genesis_commit_at_writer(
    backend_root: &Path,
    record: &crate::DfsPreparedOperationRecord,
) -> Result<(), DfsError> {
    let Some(registry) = NATIVE_GENESIS_DEFERRALS.get() else {
        return Ok(());
    };
    let state = registry
        .lock()
        .map_err(|_| DfsError::Metadata {
            message: "native genesis deferral registry poisoned".into(),
        })?
        .values()
        .filter_map(std::sync::Weak::upgrade)
        .find(|state| {
            state.shard.file_system_id == record.digest.file_system_id
                && state.shard.shard_id == record.digest.shard_id.0
                && genesis_operation_id(&state.shard) == record.digest.operation_id
                && crate::local_dfs_metadata_shard_path(
                    &SinomoDataDir::new(&state.data_root),
                    &record.digest.shard_id,
                )
                .as_path()
                    == backend_root
        });
    let Some(state) = state else {
        return Ok(());
    };
    // Release the registry before physical-owner and full-record validation.
    // These are the SAME checks/record capture used by the bootstrap hook.
    defer_native_genesis_completion(&SinomoDataDir::new(&state.data_root), &state.shard, record)
}

pub(crate) fn genesis_operation_id(shard: &DfsMetadataShardConfig) -> String {
    format!("genesis:{}:{}:1", shard.file_system_id, shard.shard_id)
}

pub(crate) fn original_genesis_file_system<'a>(record: &'a crate::DfsPreparedOperationRecord,
    shard: &DfsMetadataShardConfig) -> Result<&'a DfsFileSystem, DfsError> {
    record.validate()?;
    if shard.epoch != 1 || !shard.replica_node_ids.is_empty()
        || record.digest.owner_epoch != 1 || record.digest.file_system_id != shard.file_system_id
        || record.digest.shard_id.0 != shard.shard_id || record.digest.operation_id != genesis_operation_id(shard) {
        return Err(DfsError::Metadata { message: "native birth binding has a different original scope".into() });
    }
    let rows = record.payload.metadata_effects.iter().filter_map(|effect|
        if let DfsPreparedMetadataEffect::PutFileSystem { file_system } = effect { Some(file_system) } else { None })
        .collect::<Vec<_>>();
    let [original] = rows.as_slice() else {
        return Err(DfsError::Metadata { message: "native birth has no unique original filesystem".into() });
    };
    if original.file_system_id != shard.file_system_id {
        return Err(DfsError::Metadata { message: "native birth original filesystem differs".into() });
    }
    Ok(original)
}

// GC collects the immutable body but retains the same unified operation's
// exact terminal digest and commit reference. Neither representation authorizes
// publication; cold bootstrap only verifies the original genesis outcome.
pub(super) fn native_genesis_committed(
    backend: &crate::LocalSobjMetadataBackend,
    shard: &DfsMetadataShardConfig,
) -> Result<bool, DfsError> {
    let operation_id = genesis_operation_id(shard);
    let tables = backend.current_tables_snapshot()?;
    let (digest, status) = if let Some(record) = tables.prepared_operations_v7.get(&operation_id) {
        record.validate()?;
        (&record.digest, record.status)
    } else if let Some(witness) = tables.prepared_operation_evictions.get(&operation_id) {
        witness.validate()?;
        (&witness.digest, witness.status)
    } else {
        return Err(DfsError::Metadata {
            message: "initial native assignment has no retained genesis; certified import/repair is required".into(),
        });
    };
    if shard.epoch != 1
        || !shard.replica_node_ids.is_empty()
        || digest.file_system_id != shard.file_system_id
        || digest.shard_id != DfsMetadataShardId::new(shard.shard_id.clone())
        || backend.shard_id() != &digest.shard_id
        || digest.owner_epoch != shard.epoch
        || digest.operation_id != operation_id
    {
        return Err(DfsError::Metadata {
            message: "retained genesis outcome differs from the initial native assignment".into(),
        });
    }
    Ok(status == DfsPreparedOperationStatus::Committed
        && backend.committed_prepared_operation(digest)?.is_some())
}

impl DfsControlPlaneDefaultClusterAsyncBootstrapHost {
    fn staged_singleton_genesis(
        &self,
        shards: &[DfsMetadataShardConfig],
    ) -> Result<BTreeSet<DfsMetadataShardId>, DfsError> {
        let mut staged = BTreeSet::new();
        for shard in shards {
            if shard.primary_node_id != self.local_node_id
                || shard.epoch != 1
                || !shard.replica_node_ids.is_empty()
            {
                continue;
            }
            let id = DfsMetadataShardId::new(&shard.shard_id);
            let physical = crate::local_dfs_metadata_shard_path(&self.data_dir, &id);
            if physical.join("metadata-generation.sobj").exists() {
                let generation = crate::storage_generation::read_generation(&physical)?;
                if generation.identity.file_system_id == shard.file_system_id
                    && generation.identity.shard_id == id
                    && generation.identity.is_native_genesis()
                    && matches!(
                        generation.state,
                        crate::storage_generation::GenerationState::Staged { .. }
                    )
                {
                    staged.insert(id);
                }
            }
        }
        Ok(staged)
    }

    fn open_owned_bootstrap_metadata(
        &self,
        shards: Vec<DfsMetadataShardConfig>,
        staged: &BTreeSet<DfsMetadataShardId>,
    ) -> Result<crate::LocalShardedMetadataBackend, DfsError> {
        let root = default_dfs_metadata_shards_root(&self.data_dir);
        Ok(crate::LocalShardedMetadataBackend::open_for_native_genesis(
            root,
            shards,
            &self.local_node_id,
            staged,
            self.publication_owners.selected_original_data_layout()?,
        )?)
    }

    pub(super) async fn prepare_native_genesis(
        &mut self,
        file_system: &DfsFileSystem,
        shards: &[DfsMetadataShardConfig],
        bootstrap_nodes: Vec<NodeId>,
        fallback_node: NodeId,
    ) -> Result<(), DfsError> {
        let mut missing = Vec::new();
        for shard in shards {
            if self
                .control_plane
                .get_async(&metadata_shard_key(&shard.file_system_id, &shard.shard_id))
                .await?
                .is_none()
            {
                if shard.primary_node_id != self.local_node_id
                    || shard.epoch != 1
                    || !shard.replica_node_ids.is_empty()
                {
                    return Err(DfsError::Metadata {
                        message:
                            "only the deterministic native singleton owner may prepare genesis"
                                .into(),
                    });
                }
                missing.push(shard.clone());
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        let original_config: crate::DfsFileSystemConfig = self.control_plane
            .get_async(&crate::file_system_config_key(&file_system.file_system_id)).await?
            .ok_or_else(|| DfsError::Metadata { message: "native genesis requires its actual filesystem configuration".into() })?
            .decode()?;
        if original_config.file_system_id != file_system.file_system_id {
            return Err(DfsError::Metadata { message: "native genesis filesystem configuration is misbound".into() });
        }
        let original_file_system: DfsFileSystem = original_config.into();
        let selected_layout = self.publication_owners.selected_original_data_layout()?;
        for shard in &missing {
            crate::storage_generation::stage_native_genesis_storage_generation(
                &crate::local_dfs_metadata_shard_path(&self.data_dir, &DfsMetadataShardId::new(&shard.shard_id)),
                shard,
                selected_layout.as_deref(),
            )?;
        }
        let mut backend = self.open_owned_bootstrap_metadata(shards.to_vec(), &self.staged_singleton_genesis(shards)?)?;
        for shard in missing {
            let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
            let physical = backend.shard_backend_mut(&shard_id)?;
            let operation_id = genesis_operation_id(&shard);
            if let Some(record) = physical.prepared_operation_record(&operation_id)? {
                record.validate()?;
                if record.digest.file_system_id != shard.file_system_id
                    || record.digest.shard_id != shard_id
                    || record.digest.owner_epoch != 1
                    || record.status != DfsPreparedOperationStatus::Prepared
                {
                    return Err(DfsError::Metadata {
                        message: "retained genesis differs from the missing native assignment"
                            .into(),
                    });
                }
                continue;
            }
            let status = physical.metadata_shard_statuses()?;
            let [status] = status.as_slice() else {
                return Err(DfsError::Metadata {
                    message: "genesis requires one actual physical shard".into(),
                });
            };
            if status.journal_last_sequence != 0
                || !physical.list_file_systems()?.is_empty()
                || !physical.list_prepared_operations()?.is_empty()
                || physical.metadata_authority_ack_floor(&shard_id)?.is_some()
                || physical.metadata_prefix_ack_floor(&shard_id)?.is_some()
            {
                return Err(DfsError::Metadata { message: "retained metadata cannot be re-seeded as native genesis; import or repair its certified history".into() });
            }
            let objects = crate::distributed_sifs_bootstrap_objects_for_nodes(bootstrap_nodes.clone(), fallback_node.clone())
                .into_iter().filter_map(|object| {
                    match crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(&object.file_system_id, shards, &object.path) {
                        Ok(Some(selected)) if selected.shard_id == shard.shard_id => Some(Ok(object)),
                        Ok(_) => None,
                        Err(error) => Some(Err(error)),
                    }
                }).collect::<Result<Vec<_>, sinomo_core::kv::KvError>>()?;
            let versions = objects
                .iter()
                .map(|object| crate::object_version_record(object, None))
                .collect();
            let expected_objects = objects
                .iter()
                .map(|object| crate::DfsNamespaceObjectVersionExpectation {
                    object_id: object.object_id.clone(),
                    expected_version: None,
                })
                .collect();
            let expected_paths = objects
                .iter()
                .map(|object| crate::DfsNamespacePathOccupantExpectation {
                    path: object.path.clone(),
                    expected_object_id: None,
                })
                .collect();
            let mut payload = DfsPreparedOperationPayload::for_namespace_transaction(
                DfsPreparedOperationScope {
                    file_system_id: shard.file_system_id.clone(),
                    shard_id,
                    owner_epoch: 1,
                    operation_id: operation_id.clone(),
                },
                expected_objects,
                expected_paths,
                crate::DfsNamespaceTransaction {
                    transaction_id: operation_id,
                    file_system_id: shard.file_system_id.clone(),
                    objects,
                    versions,
                    advisory_lock_states: Vec::new(),
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    content_reference_identity: None,
                    extension_expectations: Vec::new(),
                    contribution_digest: None,
                    file_version_pins: None,
                },
                Vec::new(),
                None,
                None,
                Vec::new(),
                None,
                None,
            )?;
            payload
                .metadata_effects
                .push(DfsPreparedMetadataEffect::PutFileSystem {
                    file_system: original_file_system.clone(),
                });
            // This hidden body precedes the native assignment CAS. No guard is
            // minted and no namespace row becomes visible at this boundary.
            physical.prepare_operation(payload, None)?;
        }
        self.genesis_backend = Some(backend);
        Ok(())
    }

    pub(super) fn publish_current_bootstrap(
        &mut self,
        publication_provider: Arc<crate::DfsPublicationExecutionProvider>,
        file_system: &DfsFileSystem,
        shards: Vec<DfsMetadataShardConfig>,
        bootstrap_nodes: Vec<NodeId>,
        fallback_node: NodeId,
        evidence: Vec<(
            Option<DfsMetadataShardMembershipHandoff>,
            Option<DfsMetadataShardMembershipHandoffProof>,
        )>,
        confirmed_new_genesis_commit: &mut bool,
    ) -> Result<(), DfsError> {
        let runtime = tokio::runtime::Handle::current();
        let transport = crate::runtime_host::daemon_dfs_node_transport_on_runtime(
            self.local_node_id.clone(),
            shards
                .iter()
                .flat_map(|shard| shard.replica_node_ids.iter().cloned()),
            &self.data_dir,
            &runtime,
        );
        let content = self.publication_owners.open_native_content()?;
        let service = crate::NetworkedDfsService::new(transport, content);
        // After an interrupted initial publication, the assignment can already
        // exist while its storage generation is staged. Only the genesis
        // opener can recover that original operation; a serving checkout must
        // wait for activation.
        let staged = self.staged_singleton_genesis(&shards)?;
        let mut service = if let Some(backend) = self.genesis_backend.take() {
            service.with_metadata_backend_lazy(backend)?
        } else if let Some(resident) = self.resident_metadata_backend.as_ref().filter(|resident| {
            staged.is_empty() && resident
                .matches_topology(&default_dfs_metadata_shards_root(&self.data_dir), &shards)
                .unwrap_or(false)
        }) {
            service.with_resident_sharded_metadata_backend_lazy(resident.checkout_global()?)?
        } else {
            service.with_metadata_backend_lazy(
                self.open_owned_bootstrap_metadata(shards.clone(), &staged)?,
            )?
        };
        service = service.with_boxed_control_plane(
            crate::runtime_host::daemon_dfs_control_plane_client_on_runtime(
                &self.cluster_paths,
                Some(&runtime),
            )
            .ok_or_else(|| DfsError::Metadata {
                message: "native bootstrap has no actual cluster authority client".into(),
            })?,
        );
        service
            .publication_provider
            .retain_runtime_provider(publication_provider);
        let _execution = service
            .publication_execution
            .install(Arc::clone(&self.publication_owners));
        service.metadata.add_file_system(file_system.clone());
        for (shard, (pending, proof)) in shards.iter().zip(&evidence) {
            if shard.primary_node_id != self.local_node_id || pending.is_some() {
                continue;
            }
            #[cfg(test)]
            if proof.is_some()
                && let Some(hook) =
                    crate::runtime_host::daemon_dfs_pre_dispatch_test_hook(&self.data_dir)
            {
                hook(crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeCompletedPrimaryRebind {
                    shard_id: &crate::DfsMetadataShardId::new(&shard.shard_id),
                }, None, None)?;
            }
            rebind_completed_metadata_shard_membership_handoff(
                service
                    .metadata_backend
                    .as_mut()
                    .expect("bootstrap backend")
                    .as_mut(),
                &self.local_node_id,
                shard,
                pending.as_ref(),
                proof.as_ref(),
            )?;
            let operation_id = genesis_operation_id(shard);
            let retained = service
                .metadata_backend
                .as_ref()
                .expect("bootstrap backend")
                .prepared_operation_record(&operation_id)?;
            if let Some(record) = retained {
                if record.status == DfsPreparedOperationStatus::Prepared {
                    // Only the exact initial singleton can finish its retained
                    // genesis. New owners recover through normal certified handoff.
                    if shard.epoch != record.digest.owner_epoch
                        || !shard.replica_node_ids.is_empty()
                    {
                        return Err(DfsError::Metadata {
                            message: "unfinished genesis cannot be expanded or rebound".into(),
                        });
                    }
                    #[cfg(test)]
                    defer_native_genesis_completion(&self.data_dir, shard, &record)?;
                    let publication = service.commit_stored_prepared_operation(&record.digest)?;
                    // Keep only confirmed semantic progress across a later
                    // shard/object error. Unknown outcomes never set this bit.
                    *confirmed_new_genesis_commit |= publication.published;
                }
            } else if service
                .metadata_backend
                .as_ref()
                .expect("bootstrap backend")
                .get_file_system(&shard.file_system_id)?
                .is_none()
            {
                return Err(DfsError::Metadata { message: "existing assignment has no retained genesis or certified metadata image; import/catch-up required".into() });
            }
            if shard.epoch == 1 && shard.replica_node_ids.is_empty() {
                service.metadata_backend.as_mut().expect("bootstrap backend")
                    .activate_native_genesis_storage_generation(shard)?;
            }
        }
        let mut missing_objects = Vec::new();
        for object in
            crate::distributed_sifs_bootstrap_objects_for_nodes(bootstrap_nodes, fallback_node)
        {
            let selected = crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(&object.file_system_id, &shards, &object.path)?.ok_or_else(|| DfsError::Metadata { message: "current bootstrap object has no exact assignment".into() })?;
            if selected.primary_node_id != self.local_node_id
                || evidence.iter().zip(&shards).any(|((pending, _), shard)| {
                    shard.shard_id == selected.shard_id && pending.is_some()
                })
            {
                continue;
            }
            if service
                .metadata_backend
                .as_ref()
                .expect("bootstrap backend")
                .get_object_by_path(&object.file_system_id, &object.path)?
                .is_none()
            {
                missing_objects.push(object);
            }
        }
        service.publish_missing_bootstrap_objects(missing_objects)?;
        Ok(())
    }
}
