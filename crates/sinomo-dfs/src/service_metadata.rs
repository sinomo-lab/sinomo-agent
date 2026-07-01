//! Service-local metadata mutations and tombstone index maintenance.
//!
//! Public methods here update DFS metadata state and keep the in-memory service
//! view synchronized with the durable metadata backend when one is configured.

use std::collections::BTreeMap;
use std::time::Instant;

use time::OffsetDateTime;

use crate::{
    DfsControlPlaneStore, DfsError, DfsFileSystem, DfsFileSystemConfig, DfsFileSystemId,
    DfsMessageTransport, DfsNamespaceTransaction, DfsObjectId, DfsObjectKind, DfsObjectMetadata,
    DfsObjectVersionRecord, NetworkedDfsService, dfs_perf_trace, metadata_path,
    namespace_transaction_id_for_batch, normalize_current_locations, object_version_record,
    path_index_key,
};

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn add_file_system(&mut self, file_system: DfsFileSystem) -> Result<(), DfsError> {
        if let Some(kv) = self.control_plane.as_mut() {
            let file_system_config = DfsFileSystemConfig::from(&file_system);
            let mut store = DfsControlPlaneStore::new(kv.as_mut());
            if store.get_file_system_config(&file_system.file_system_id)?
                != Some(file_system_config.clone())
            {
                store.put_file_system_config(&file_system_config)?;
            }
        }
        if let Some(backend) = self.metadata_backend.as_mut() {
            backend.ensure_file_system(&file_system)?;
        }
        self.metadata.add_file_system(file_system);
        Ok(())
    }

    pub fn add_object_metadata(&mut self, object: DfsObjectMetadata) -> Result<(), DfsError> {
        self.put_visible_object_in_metadata_backend(&object)?;
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        self.track_metadata_tombstone(&object);
        self.metadata.add_object(object)
    }

    pub fn add_object_metadata_batch(
        &mut self,
        objects: impl IntoIterator<Item = DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let mut grouped =
            BTreeMap::<DfsFileSystemId, Vec<(DfsObjectMetadata, DfsObjectVersionRecord)>>::new();
        for mut object in objects {
            normalize_current_locations(&mut object.current_locations);
            let size_bytes = if matches!(object.kind, DfsObjectKind::File | DfsObjectKind::Symlink)
                && object.content_size_bytes.is_some()
            {
                object.content_size_bytes
            } else {
                self.object_content_size_hint(&object)?
            };
            let version = object_version_record(&object, size_bytes);
            grouped
                .entry(object.file_system_id.clone())
                .or_default()
                .push((object, version));
        }
        if let Some(backend) = self.metadata_backend.as_mut() {
            for (file_system_id, records) in &grouped {
                let Some((object, _)) = records.first() else {
                    continue;
                };
                let file_system =
                    backend
                        .get_file_system(file_system_id)?
                        .unwrap_or_else(|| DfsFileSystem {
                            file_system_id: file_system_id.clone(),
                            display_name: file_system_id.to_string(),
                            owner_node_id: object.owner_node_id.clone(),
                            subtree_policies: Vec::new(),
                        });
                backend.ensure_file_system(&file_system)?;

                let mut transaction_objects = Vec::new();
                let mut transaction_versions = Vec::new();
                let lookup_keys = records
                    .iter()
                    .map(|(object, version)| (object.object_id.clone(), version.version.clone()))
                    .collect::<Vec<_>>();
                let existing_pairs = backend.get_object_version_pairs(&lookup_keys)?;
                for ((object, version), (existing_object, existing_version)) in
                    records.iter().zip(existing_pairs.into_iter())
                {
                    if existing_object.as_ref() != Some(object) {
                        transaction_objects.push(object.clone());
                    }
                    let mut version = version.clone();
                    if let Some(existing) = existing_version {
                        version.created_at = existing.created_at;
                        if existing == version {
                            continue;
                        }
                    }
                    transaction_versions.push(version);
                }
                if transaction_objects.is_empty() && transaction_versions.is_empty() {
                    continue;
                }
                backend.apply_namespace_transaction(&DfsNamespaceTransaction {
                    transaction_id: namespace_transaction_id_for_batch(
                        "visible-batch",
                        file_system_id,
                        &transaction_objects,
                        &transaction_versions,
                    ),
                    file_system_id: file_system_id.clone(),
                    objects: transaction_objects,
                    versions: transaction_versions,
                    created_at: OffsetDateTime::now_utc(),
                })?;
            }
        }
        for (_, records) in grouped {
            for (object, _) in records {
                self.coordinator.set_committed_version(
                    object.file_system_id.clone(),
                    object.object_id.clone(),
                    object.version.clone(),
                );
                self.track_metadata_tombstone(&object);
                self.metadata.add_object(object)?;
            }
        }
        Ok(())
    }

    pub fn add_transient_object_metadata(
        &mut self,
        object: DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        self.track_metadata_tombstone(&object);
        self.metadata.add_object(object)
    }

    pub(crate) fn track_metadata_tombstone(&mut self, object: &DfsObjectMetadata) {
        let key = (object.file_system_id.clone(), metadata_path(&object.path));
        if object.kind == DfsObjectKind::Tombstone {
            let replace = self
                .tombstones_by_metadata_path
                .get(&key)
                .is_none_or(|existing| existing.version < object.version);
            if replace {
                self.tombstones_by_metadata_path.insert(key, object.clone());
            }
            return;
        }
        self.tombstones_by_metadata_path.remove(&key);
    }

    pub(crate) fn track_hydrated_metadata_tombstone(&mut self, object: &DfsObjectMetadata) {
        if object.kind != DfsObjectKind::Tombstone {
            let key = (object.file_system_id.clone(), metadata_path(&object.path));
            if self
                .tombstones_by_metadata_path
                .get(&key)
                .is_some_and(|existing| existing.version > object.version)
            {
                return;
            }
            self.track_metadata_tombstone(object);
            return;
        }
        let live_path = path_index_key(&object.file_system_id, &object.path).map(|(_, path)| path);
        let visible_object = self.metadata.object_by_normalized_posix_path(
            &object.file_system_id,
            live_path.as_deref().unwrap_or(&object.path),
        );
        if visible_object.is_none_or(|visible| visible.version < object.version) {
            self.track_metadata_tombstone(object);
        }
    }

    pub fn remove_transient_object_metadata(&mut self, object_id: &DfsObjectId) {
        self.metadata.remove_object(object_id);
    }

    pub fn apply_namespace_transaction(
        &mut self,
        transaction: DfsNamespaceTransaction,
    ) -> Result<(), DfsError> {
        let trace_started = Instant::now();
        let trace_object_count = transaction.objects.len();
        let trace_version_count = transaction.versions.len();
        let mut trace_ensure_fs_ms = 0;
        let mut trace_backend_apply_ms = 0;
        let mut trace_memory_coordinator_ms = 0;
        let mut trace_memory_tombstone_ms = 0;
        let mut trace_memory_add_ms = 0;
        if let Some(backend) = self.metadata_backend.as_mut() {
            let ensure_started = Instant::now();
            let file_system = backend
                .get_file_system(&transaction.file_system_id)?
                .unwrap_or_else(|| DfsFileSystem {
                    file_system_id: transaction.file_system_id.clone(),
                    display_name: transaction.file_system_id.to_string(),
                    owner_node_id: self.node_id.clone(),
                    subtree_policies: Vec::new(),
                });
            backend.ensure_file_system(&file_system)?;
            trace_ensure_fs_ms = ensure_started.elapsed().as_millis();
            let backend_apply_started = Instant::now();
            backend.apply_namespace_transaction(&transaction)?;
            trace_backend_apply_ms = backend_apply_started.elapsed().as_millis();
        }
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind == DfsObjectKind::Tombstone)
            .chain(
                transaction
                    .objects
                    .iter()
                    .filter(|object| object.kind != DfsObjectKind::Tombstone),
            )
            .cloned()
        {
            let coordinator_started = Instant::now();
            self.coordinator.set_committed_version(
                object.file_system_id.clone(),
                object.object_id.clone(),
                object.version.clone(),
            );
            trace_memory_coordinator_ms += coordinator_started.elapsed().as_millis();
            let tombstone_started = Instant::now();
            self.track_metadata_tombstone(&object);
            trace_memory_tombstone_ms += tombstone_started.elapsed().as_millis();
            let add_started = Instant::now();
            self.metadata.add_object(object)?;
            trace_memory_add_ms += add_started.elapsed().as_millis();
        }
        dfs_perf_trace(format_args!(
            "apply-namespace-transaction node={} objects={} versions={} ensure_fs_ms={} backend_apply_ms={} memory_coordinator_ms={} memory_tombstone_ms={} memory_add_ms={} total_ms={}",
            self.node_id,
            trace_object_count,
            trace_version_count,
            trace_ensure_fs_ms,
            trace_backend_apply_ms,
            trace_memory_coordinator_ms,
            trace_memory_tombstone_ms,
            trace_memory_add_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(())
    }
}
