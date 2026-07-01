//! POSIX-like DFS namespace facade.
//!
//! The facade provides path-based operations without Unix user/group permission
//! semantics. It is intended for services such as static web servers and
//! rollout and higher-level automation tools that need stable mkdir/list/stat/read/write
//! behavior over SIFS while the lower DFS layers handle placement, receipts,
//! repair, and metadata journaling.

mod cross_shard;
mod jobs;
mod read;
mod routing;

use crate::dfs_now_unix_nanos as now_unix_nanos;
use crate::metadata_pages::try_walk_metadata_pages;
use crate::namespace::{
    initial_version, metadata_path, namespace_transaction_id_for_path, next_version,
    object_id_for_path,
};
use crate::posix_model::{
    DfsDirEntry, DfsDirEntryPage, DfsPosixError, DfsStat, DfsWriteOptions, Result,
    read_conflict_error,
};
use crate::posix_object::{
    new_posix_metadata_object, object_mode_or_default, object_timestamp_or_default,
    recreated_posix_metadata_object, set_object_create_timestamps,
};
use crate::posix_path::{
    append_posix_components, hidden_alias_tombstone_path, join_posix_path, normalize_posix_path,
    normalize_posix_path_with_trailing_directory, parent_posix_path,
    posix_path_is_at_or_under_prefix, resolve_posix_symlink_target, validate_posix_symlink_target,
};
use crate::{
    DfsControlPlaneStore, DfsDirectoryPage, DfsDirectoryPageCursor, DfsError, DfsFileSystem,
    DfsMessageTransport, DfsNamespaceJobStatus, DfsNamespaceOperation, DfsNamespaceTransaction,
    DfsObjectKind, DfsObjectMetadata, DfsReadData, DfsReplicationPolicy, NetworkedDfsReadOutcome,
    NetworkedDfsService, NetworkedDfsWriteOutcome, sifs_file_system_id,
};
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};
use sinomo_events::EventId;
use sinomo_node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read};
use std::time::{Duration, Instant};

const POSIX_WAIT_FOR_COMMIT_READY_DRAIN_LIMIT: usize = 8;
const POSIX_WAIT_FOR_COMMIT_IDLE_SLEEP: Duration = Duration::from_millis(10);
const POSIX_WAIT_FOR_COMMIT_TIMEOUT: Duration = Duration::from_secs(45);
const POSIX_LIST_DIR_COLLECT_PAGE_SIZE: usize = 4096;
const POSIX_LINK_COUNT_PAGE_SIZE: usize = 4096;
const POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS: usize = 512;
const POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE: usize = 128;
const POSIX_NAMESPACE_JOB_MAINTENANCE_PAGE_SIZE: usize = 1024;
const POSIX_REMOTE_READ_TIMEOUT: Duration = Duration::from_secs(30);
const POSIX_REMOTE_READ_IDLE_WAIT_INITIAL: Duration = Duration::from_millis(2);
/// Version-history records shipped with a cross-shard adoption; older
/// history stays in the source shard until lifecycle GC collects it.
const CROSS_SHARD_MOVE_VERSION_HISTORY_LIMIT: usize = 32;
/// Follow-up sync rounds a routed-operation wrapper performs when bounded
/// response replay has not yet surfaced the mutation locally.
const POSIX_ROUTED_SYNC_RETRY_ROUNDS: usize = 4;
/// Budget for the best-effort freshness pull a distributed read performs on
/// foreign-shard paths; reads fall back to the local mirror when it elapses.
const POSIX_READ_FRESHNESS_SYNC_TIMEOUT: Duration = Duration::from_secs(3);
const POSIX_REMOTE_READ_IDLE_WAIT_MAX: Duration = Duration::from_millis(50);

struct PosixBulkLocalWrite<'a> {
    path: String,
    object: DfsObjectMetadata,
    target_version: DfsVersion,
    bytes: &'a [u8],
    metadata_primary: Option<NodeId>,
    parent_touch: Option<PosixBulkParentTouch>,
    link_count_hint: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PosixBulkParentTouch {
    path: String,
    metadata_primary: Option<NodeId>,
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn settle_ready_write_outcome(
        &mut self,
        outcome: NetworkedDfsWriteOutcome,
    ) -> std::result::Result<NetworkedDfsWriteOutcome, DfsError> {
        let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = &outcome else {
            return Ok(outcome);
        };
        let mut intent_id = intent_id.clone();
        let started_at = Instant::now();
        let mut ready_drains = 0;
        loop {
            let mut events = self.process_incoming()?;
            let outcome = self.write_outcome(&intent_id)?;
            if let NetworkedDfsWriteOutcome::Pending {
                intent_id: next_intent_id,
                ..
            } = &outcome
            {
                intent_id = next_intent_id.clone();
            }
            if !matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }) {
                let (_, repair_events) = self.retry_pending_repairs_best_effort();
                if !repair_events.is_empty() {
                    events.extend(repair_events);
                    self.last_events = events;
                }
                return Ok(outcome);
            }
            let (scheduled, repair_events) = self.retry_pending_repairs_best_effort();
            if !repair_events.is_empty() {
                events.extend(repair_events);
                self.last_events = events.clone();
            }
            if (events.is_empty() && scheduled.is_empty())
                || ready_drains >= POSIX_WAIT_FOR_COMMIT_READY_DRAIN_LIMIT
            {
                let elapsed = started_at.elapsed();
                if elapsed >= POSIX_WAIT_FOR_COMMIT_TIMEOUT {
                    return Ok(outcome);
                }
                std::thread::sleep(
                    POSIX_WAIT_FOR_COMMIT_IDLE_SLEEP
                        .min(POSIX_WAIT_FOR_COMMIT_TIMEOUT.saturating_sub(elapsed)),
                );
                ready_drains = 0;
            } else {
                ready_drains += 1;
            }
        }
    }

    fn posix_replica_candidates_for_write(
        &mut self,
        object: &DfsObjectMetadata,
        options: &DfsWriteOptions,
    ) -> std::result::Result<Vec<NodeId>, DfsError> {
        let mut candidates = options.replica_candidates.clone();
        if !candidates.contains(&self.node_id) {
            candidates.insert(0, self.node_id.clone());
        }

        if options.policy.is_strict_confirmed()
            && options.policy.desired_replicas >= 2
            && candidates.len() == options.policy.desired_replicas
            && candidates.contains(&self.node_id)
            && let Some(shard) = self.select_metadata_shard_for_object(object)?
            && shard.primary_node_id != self.node_id
            && !candidates.contains(&shard.primary_node_id)
            && let Some(replica_slot) = candidates
                .iter_mut()
                .rev()
                .find(|node_id| **node_id != self.node_id)
        {
            *replica_slot = shard.primary_node_id;
        }

        Ok(candidates)
    }

    pub fn bootstrap_distributed_sifs(&mut self) -> Result<()> {
        let fallback_file_system = crate::namespace::distributed_sifs(self.node_id.clone());
        let file_system_id = fallback_file_system.file_system_id.clone();
        let file_system = if let Some(kv) = self.control_plane.as_mut() {
            DfsControlPlaneStore::new(kv.as_mut())
                .get_file_system_config(&file_system_id)
                .map_err(DfsError::from)?
                .map(DfsFileSystem::from)
                .unwrap_or(fallback_file_system)
        } else {
            fallback_file_system
        };
        let bootstrap_owner_node_id = file_system.owner_node_id.clone();
        self.add_file_system(file_system)?;
        for object in
            crate::namespace::distributed_sifs_bootstrap_objects(bootstrap_owner_node_id.clone())
        {
            if self
                .lookup_metadata_path(&object.file_system_id, &object.path)?
                .is_none()
            {
                self.add_object_metadata(object)?;
            }
        }
        Ok(())
    }

    pub fn posix_mkdir(&mut self, path: &str) -> Result<DfsStat> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::Mkdir {
                    path: path.to_string(),
                },
            )?;
            return self.stat_routed_namespace_result(&normalized, false);
        }
        self.posix_mkdir_local(path)
    }

    pub(crate) fn posix_mkdir_local(&mut self, path: &str) -> Result<DfsStat> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&path)?;
        if let Some(mut object) = self.lookup_sifs_path(&path)? {
            if object.kind != DfsObjectKind::Tombstone {
                return Err(DfsPosixError::AlreadyExists { path });
            }
            object.kind = DfsObjectKind::Directory;
            object.content_hash = None;
            object.content_size_bytes = None;
            object.link_target_object_id = None;
            object.current_locations = vec![self.node_id.clone()];
            set_object_create_timestamps(&mut object);
            object.version = next_version(&object.version);
            self.add_object_metadata(object.clone())?;
            self.touch_parent_directory_modified(&path)?;
            return self.stat_object(&object);
        }
        self.ensure_parent_directory(&path)?;
        let object = new_posix_metadata_object(&self.node_id, &path, DfsObjectKind::Directory);
        self.add_object_metadata(object.clone())?;
        self.touch_parent_directory_modified(&path)?;
        self.stat_object(&object)
    }

    pub fn posix_mkdir_all(&mut self, path: &str) -> Result<()> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::MkdirAll {
                    path: path.to_string(),
                },
            )?;
            self.stat_routed_namespace_result(&normalized, false)
                .map(drop)?;
            return Ok(());
        }
        self.posix_mkdir_all_local(path)
    }

    pub fn posix_mkdir_all_without_local_replay(&mut self, path: &str) -> Result<()> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            return self.route_namespace_operation_without_local_replay(
                primary,
                DfsNamespaceOperation::MkdirAll {
                    path: path.to_string(),
                },
            );
        }
        self.posix_mkdir_all_local(path)
    }

    pub(crate) fn posix_mkdir_all_local(&mut self, path: &str) -> Result<()> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if path == "/" {
            return Ok(());
        }
        let mut current = String::new();
        for part in path.trim_matches('/').split('/') {
            current.push('/');
            current.push_str(part);
            match self.lookup_sifs_path(&current)? {
                Some(object) if object.kind == DfsObjectKind::Directory => {}
                Some(object) if object.kind == DfsObjectKind::Tombstone => {
                    let mut object = object;
                    object.kind = DfsObjectKind::Directory;
                    object.content_hash = None;
                    object.content_size_bytes = None;
                    object.link_target_object_id = None;
                    object.current_locations = vec![self.node_id.clone()];
                    set_object_create_timestamps(&mut object);
                    object.version = next_version(&object.version);
                    self.add_object_metadata(object)?;
                    self.touch_parent_directory_modified(&current)?;
                }
                Some(_) => return Err(DfsPosixError::NotDirectory { path: current }),
                None => {
                    self.posix_mkdir_local(&current)?;
                }
            }
        }
        Ok(())
    }

    pub fn posix_write_file(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        self.posix_write_file_bytes_checked_parent(path, bytes, options, true, true, true)
    }

    pub fn posix_write_file_with_known_parent(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        self.posix_write_file_bytes_checked_parent(path, bytes, options, false, false, true)
    }

    pub fn posix_write_file_with_known_parent_deferred_local_replay(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        self.posix_write_file_bytes_checked_parent(path, bytes, options, false, false, false)
    }

    pub fn posix_write_files_local_eventual_with_known_parents<'a, I>(
        &mut self,
        files: I,
    ) -> Result<Vec<DfsStat>>
    where
        I: IntoIterator<Item = (&'a str, &'a [u8])>,
    {
        let files = files.into_iter().collect::<Vec<_>>();
        if files.is_empty() {
            return Ok(Vec::new());
        }

        let mut prepared = Vec::with_capacity(files.len());
        let mut seen_objects = BTreeSet::new();
        let mut resolved_parents = BTreeMap::<String, String>::new();
        let metadata_shard_snapshot = self.refresh_sifs_metadata_shards()?;
        for (path, bytes) in &files {
            let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
            if requires_directory {
                return match self.ensure_resolved_path_is_directory(&path) {
                    Ok(()) => Err(DfsPosixError::IsDirectory { path }),
                    Err(DfsPosixError::NotDirectory { path }) => {
                        Err(DfsPosixError::NotDirectory { path })
                    }
                    Err(DfsPosixError::NotFound { path }) => {
                        Err(DfsPosixError::NotDirectory { path })
                    }
                    Err(error) => Err(error),
                };
            }
            let Some(parent) = parent_posix_path(&path) else {
                return Err(DfsPosixError::IsDirectory { path });
            };
            let resolved_parent = match resolved_parents.get(&parent) {
                Some(resolved) => resolved.clone(),
                None => {
                    let resolved = self.resolve_posix_path_distributed(&parent, true)?;
                    resolved_parents.insert(parent.clone(), resolved.clone());
                    resolved
                }
            };
            let Some((_, child_name)) = path.rsplit_once('/') else {
                return Err(DfsPosixError::InvalidPath { path });
            };
            let path = join_posix_path(&resolved_parent, child_name);
            let mut link_count_hint = None;
            let object = match self.lookup_sifs_path(&path)? {
                Some(object) if object.kind == DfsObjectKind::Directory => {
                    return Err(DfsPosixError::IsDirectory { path });
                }
                Some(object) if object.kind == DfsObjectKind::Symlink => {
                    return self.posix_write_files_local_eventual_fallback(&files);
                }
                Some(object) if object.kind == DfsObjectKind::Hardlink => {
                    self.resolve_hardlink_target(&object)?
                }
                Some(object) if object.kind == DfsObjectKind::Tombstone => {
                    let object =
                        recreated_posix_metadata_object(&self.node_id, &path, DfsObjectKind::File);
                    link_count_hint = Some(1);
                    object
                }
                Some(object) => object,
                None => {
                    link_count_hint = Some(1);
                    new_posix_metadata_object(&self.node_id, &path, DfsObjectKind::File)
                }
            };
            if !seen_objects.insert(object.object_id.clone()) {
                return self.posix_write_files_local_eventual_fallback(&files);
            }
            if object.policy != DfsObjectPolicy::ReplicatedEventual
                || object.owner_node_id != self.node_id
            {
                return self.posix_write_files_local_eventual_fallback(&files);
            }
            let metadata_primary = match metadata_shard_snapshot.as_deref() {
                Some(shards) => {
                    self.routed_namespace_primary_for_path_from_snapshot(&path, shards)?
                }
                None => None,
            };
            let target_version = next_version(&object.version);
            prepared.push(PosixBulkLocalWrite {
                path,
                object,
                target_version,
                bytes: *bytes,
                metadata_primary,
                parent_touch: None,
                link_count_hint,
            });
        }

        self.commit_posix_bulk_local_writes(prepared)
    }

    pub fn posix_create_files_local_eventual_with_known_parents<'a, I>(
        &mut self,
        files: I,
    ) -> Result<Vec<DfsStat>>
    where
        I: IntoIterator<Item = (&'a str, &'a [u8])>,
    {
        let files = files.into_iter().collect::<Vec<_>>();
        if files.is_empty() {
            return Ok(Vec::new());
        }
        let mut prepared = Vec::with_capacity(files.len());
        let mut seen_objects = BTreeSet::new();
        let mut existing_children_by_parent = BTreeMap::<String, BTreeSet<String>>::new();
        let mut resolved_parents = BTreeMap::<String, String>::new();
        let metadata_shard_snapshot = self.refresh_sifs_metadata_shards()?;
        for (path, bytes) in files {
            let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
            if requires_directory {
                return Err(DfsPosixError::IsDirectory { path });
            }
            let Some(parent) = parent_posix_path(&path) else {
                return Err(DfsPosixError::IsDirectory { path });
            };
            let resolved_parent = match resolved_parents.get(&parent) {
                Some(resolved) => resolved.clone(),
                None => {
                    let resolved = self.resolve_posix_path_distributed(&parent, true)?;
                    resolved_parents.insert(parent.clone(), resolved.clone());
                    resolved
                }
            };
            let Some((_, child_name)) = path.rsplit_once('/') else {
                return Err(DfsPosixError::InvalidPath { path });
            };
            let path = join_posix_path(&resolved_parent, child_name);
            if !existing_children_by_parent.contains_key(&resolved_parent) {
                let existing_children = self
                    .child_objects_for_sifs_dir(&resolved_parent)?
                    .into_iter()
                    .filter_map(|child| {
                        normalize_posix_path(&child.path).ok().and_then(|path| {
                            path.rsplit_once('/').map(|(_, name)| name.to_string())
                        })
                    })
                    .collect::<BTreeSet<_>>();
                existing_children_by_parent.insert(resolved_parent.clone(), existing_children);
            }
            let existing_children = existing_children_by_parent
                .get_mut(&resolved_parent)
                .expect("parent children inserted");
            if !existing_children.insert(child_name.to_string()) {
                return Err(DfsPosixError::AlreadyExists { path });
            }
            let object = new_posix_metadata_object(&self.node_id, &path, DfsObjectKind::File);
            if !seen_objects.insert(object.object_id.clone()) {
                return Err(DfsPosixError::AlreadyExists { path });
            }
            let metadata_primary = match metadata_shard_snapshot.as_deref() {
                Some(shards) => {
                    self.routed_namespace_primary_for_path_from_snapshot(&path, shards)?
                }
                None => None,
            };
            let parent_touch = Some(PosixBulkParentTouch {
                path: resolved_parent.clone(),
                metadata_primary: match metadata_shard_snapshot.as_deref() {
                    Some(shards) => self.routed_namespace_primary_for_path_from_snapshot(
                        &resolved_parent,
                        shards,
                    )?,
                    None => None,
                },
            });
            let target_version = next_version(&object.version);
            prepared.push(PosixBulkLocalWrite {
                path,
                object,
                target_version,
                bytes,
                metadata_primary,
                parent_touch,
                link_count_hint: Some(1),
            });
        }
        self.commit_posix_bulk_local_writes(prepared)
    }

    fn commit_posix_bulk_local_writes<'a>(
        &mut self,
        prepared: Vec<PosixBulkLocalWrite<'a>>,
    ) -> Result<Vec<DfsStat>> {
        let content_versions = prepared.iter().map(|write| {
            (
                write.object.object_id.as_str(),
                write.target_version.as_str(),
                write.bytes,
            )
        });
        let manifests = self
            .content
            .put_object_version_blobs_only(content_versions)
            .map_err(DfsError::from)?;
        let write_time = now_unix_nanos();
        let mut committed_objects = Vec::with_capacity(prepared.len());
        for (write, manifest) in prepared.iter().zip(manifests.iter()) {
            let mut object = write.object.clone();
            object.version = write.target_version.clone();
            object.content_hash = Some(manifest.content_hash.clone());
            object.content_size_bytes = Some(manifest.size_bytes);
            object.current_locations = vec![self.node_id.clone()];
            object.modified_at_unix_nanos = write_time;
            object.changed_at_unix_nanos = write_time;
            object.last_modified_by_event_id = Some(EventId::generate());
            committed_objects.push(object);
        }
        let mut objects_by_metadata_primary =
            BTreeMap::<Option<NodeId>, Vec<DfsObjectMetadata>>::new();
        for (write, object) in prepared.iter().zip(committed_objects.iter().cloned()) {
            objects_by_metadata_primary
                .entry(write.metadata_primary.clone())
                .or_default()
                .push(object);
        }
        let mut touched_parent_objects =
            BTreeMap::<(Option<NodeId>, DfsObjectId), DfsObjectMetadata>::new();
        let mut resolved_parent_objects =
            BTreeMap::<(Option<NodeId>, String), DfsObjectMetadata>::new();
        for write in &prepared {
            let Some(parent_touch) = write.parent_touch.as_ref() else {
                continue;
            };
            let parent_cache_key = (
                parent_touch.metadata_primary.clone(),
                parent_touch.path.clone(),
            );
            let parent = if let Some(parent) = resolved_parent_objects.get(&parent_cache_key) {
                parent.clone()
            } else {
                let parent = match self.lookup_sifs_path(&parent_touch.path)? {
                    Some(parent) if parent.kind != DfsObjectKind::Tombstone => parent,
                    _ => {
                        let (_, parent) =
                            self.lookup_sifs_path_distributed_synced(&parent_touch.path, false)?;
                        parent.ok_or_else(|| DfsPosixError::MissingParent {
                            path: parent_touch.path.clone(),
                        })?
                    }
                };
                resolved_parent_objects.insert(parent_cache_key, parent.clone());
                parent
            };
            if parent.kind == DfsObjectKind::Tombstone {
                return Err(DfsPosixError::MissingParent {
                    path: parent_touch.path.clone(),
                });
            }
            if parent.kind != DfsObjectKind::Directory {
                return Err(DfsPosixError::NotDirectory {
                    path: parent_touch.path.clone(),
                });
            }
            let key = (
                parent_touch.metadata_primary.clone(),
                parent.object_id.clone(),
            );
            touched_parent_objects.entry(key).or_insert_with(|| {
                let mut parent = parent.clone();
                parent.version = next_version(&parent.version);
                parent.modified_at_unix_nanos = write_time;
                parent.changed_at_unix_nanos = write_time;
                parent.last_modified_by_event_id = Some(EventId::generate());
                parent
            });
        }
        for ((metadata_primary, _), object) in touched_parent_objects {
            objects_by_metadata_primary
                .entry(metadata_primary)
                .or_default()
                .push(object);
        }
        for (metadata_primary, objects) in objects_by_metadata_primary {
            let versions = objects
                .iter()
                .map(|object| crate::object_version_record(object, object.content_size_bytes))
                .collect::<Vec<_>>();
            let transaction = DfsNamespaceTransaction {
                transaction_id: crate::namespace_transaction_id_for_batch(
                    "visible-batch",
                    &sifs_file_system_id(),
                    &objects,
                    &versions,
                ),
                file_system_id: sifs_file_system_id(),
                objects: objects.clone(),
                versions,
                created_at: time::OffsetDateTime::now_utc(),
            };
            if let Some(primary) = metadata_primary {
                self.route_namespace_operation_without_local_replay(
                    primary,
                    DfsNamespaceOperation::ApplyTransaction { transaction },
                )?;
            } else {
                self.add_object_metadata_batch(objects)?;
            }
        }
        committed_objects
            .iter()
            .zip(prepared.iter())
            .map(|(object, write)| {
                self.stat_object_for_path_with_link_count_hint(
                    object,
                    &write.path,
                    write.link_count_hint,
                )
            })
            .collect()
    }

    fn posix_write_files_local_eventual_fallback<'a>(
        &mut self,
        files: &[(&'a str, &'a [u8])],
    ) -> Result<Vec<DfsStat>> {
        let options = DfsWriteOptions {
            policy: DfsReplicationPolicy::new(1, 1).expect("valid local eventual policy"),
            replica_candidates: vec![self.node_id.clone()],
            wait_for_commit: true,
            foreground_replica_repair: false,
        };
        files
            .iter()
            .map(|(path, bytes)| {
                self.posix_write_file_with_known_parent(path, bytes, options.clone())
            })
            .collect()
    }

    pub fn posix_write_file_from_reader<R: Read>(
        &mut self,
        path: &str,
        reader: R,
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        self.posix_write_file_from_reader_checked_parent(path, reader, options, true, true)
    }

    fn posix_write_file_bytes_checked_parent(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
        check_parent: bool,
        touch_parent_on_create: bool,
        replay_response_journal: bool,
    ) -> Result<DfsStat> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if requires_directory {
            return match self.ensure_resolved_path_is_directory(&path) {
                Ok(()) => Err(DfsPosixError::IsDirectory { path }),
                Err(DfsPosixError::NotDirectory { path }) => {
                    Err(DfsPosixError::NotDirectory { path })
                }
                Err(DfsPosixError::NotFound { path }) => Err(DfsPosixError::NotDirectory { path }),
                Err(error) => Err(error),
            };
        }
        if check_parent {
            self.ensure_parent_directory(&path)?;
        }
        let trace_started = Instant::now();
        let trace_path = path.clone();
        let trace_bytes = bytes.len();
        let mut transient_create = false;
        let object = match self.lookup_sifs_path(&path)? {
            Some(object) if object.kind == DfsObjectKind::Directory => {
                return Err(DfsPosixError::IsDirectory { path });
            }
            Some(object) if object.kind == DfsObjectKind::Hardlink => {
                self.resolve_hardlink_target(&object)?
            }
            Some(object) if object.kind == DfsObjectKind::Tombstone => {
                let object =
                    recreated_posix_metadata_object(&self.node_id, &path, DfsObjectKind::File);
                self.add_transient_object_metadata(object.clone())?;
                transient_create = true;
                object
            }
            Some(object) => object,
            None => {
                let object = new_posix_metadata_object(&self.node_id, &path, DfsObjectKind::File);
                self.add_transient_object_metadata(object.clone())?;
                transient_create = true;
                object
            }
        };
        let target_version = next_version(&object.version);
        let candidates = self.posix_replica_candidates_for_write(&object, &options)?;
        let committed_locations = candidates.clone();
        let content_started = Instant::now();
        let manifest_result = if options.wait_for_commit {
            self.content.put_object_version_blob_only_durable(
                object.object_id.as_str(),
                target_version.as_str(),
                bytes,
            )
        } else {
            self.content.put_object_version_blob_only(
                object.object_id.as_str(),
                target_version.as_str(),
                bytes,
            )
        };
        let manifest = match manifest_result {
            Ok(manifest) => manifest,
            Err(error) => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                return Err(DfsPosixError::Dfs(error.into()));
            }
        };
        let content_elapsed = content_started.elapsed();
        let distributed_started = Instant::now();
        let detach_request = !options.wait_for_commit && !options.foreground_replica_repair;
        let outcome = self
            .write_distributed_eventual_from_local_manifest_with_candidates_and_parent_touch(
                DfsWriteRequest {
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    target_version: target_version.clone(),
                    requesting_node_id: self.node_id.clone(),
                },
                Some(object.version.clone()),
                manifest.content_hash.clone(),
                manifest.size_bytes,
                candidates,
                options.policy,
                touch_parent_on_create,
                options.foreground_replica_repair,
                detach_request,
                replay_response_journal,
            );
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                return Err(error.into());
            }
        };
        let outcome = if options.wait_for_commit {
            match self.settle_ready_write_outcome(outcome) {
                Ok(outcome) => outcome,
                Err(error) => {
                    if transient_create {
                        self.remove_transient_object_metadata(&object.object_id);
                    }
                    return Err(error.into());
                }
            }
        } else {
            outcome
        };
        let distributed_elapsed = distributed_started.elapsed();
        crate::dfs_perf_trace(format_args!(
            "posix-byte-write node={} path={} object={} version={} bytes={} content_ms={} distributed_ms={} total_ms={}",
            self.node_id,
            trace_path,
            object.object_id,
            target_version,
            trace_bytes,
            content_elapsed.as_millis(),
            distributed_elapsed.as_millis(),
            trace_started.elapsed().as_millis()
        ));
        match outcome {
            NetworkedDfsWriteOutcome::Committed {
                version,
                content_hash,
                repair_jobs,
                ..
            } => {
                let mut committed_object = object.clone();
                committed_object.version = version;
                committed_object.content_hash = Some(content_hash);
                committed_object.current_locations = committed_locations
                    .into_iter()
                    .filter(|node_id| {
                        !repair_jobs
                            .iter()
                            .any(|job| job.missing_replica_node_id == *node_id)
                    })
                    .collect();
                committed_object.content_size_bytes = Some(manifest.size_bytes);
                let now = now_unix_nanos();
                committed_object.modified_at_unix_nanos = now;
                committed_object.changed_at_unix_nanos = now;
                committed_object.last_modified_by_event_id = Some(EventId::generate());
                self.add_transient_object_metadata(committed_object.clone())?;
                if transient_create
                    && touch_parent_on_create
                    && self.is_local_metadata_primary_for_object(&object)?
                {
                    self.touch_parent_directory_modified(&path)?;
                }
                if !options.foreground_replica_repair {
                    return self.stat_object_for_path(&committed_object, &path);
                }
                let object = match self.stat_target_for_path(&path) {
                    Err(DfsPosixError::NotFound { .. }) => {
                        let mut refreshed = None;
                        for _ in 0..POSIX_ROUTED_SYNC_RETRY_ROUNDS {
                            self.sync_remote_namespace_path(&path)?;
                            match self.stat_target_for_path(&path) {
                                Err(DfsPosixError::NotFound { .. }) => continue,
                                result => {
                                    refreshed = Some(result);
                                    break;
                                }
                            }
                        }
                        refreshed.unwrap_or_else(|| self.stat_target_for_path(&path))?
                    }
                    result => result?,
                };
                self.stat_object_for_path(&object, &path)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } if !options.wait_for_commit && !options.foreground_replica_repair => {
                let mut pending_object = object.clone();
                pending_object.version = target_version;
                pending_object.content_hash = Some(manifest.content_hash);
                pending_object.current_locations = vec![self.node_id.clone()];
                pending_object.content_size_bytes = Some(manifest.size_bytes);
                let now = now_unix_nanos();
                pending_object.modified_at_unix_nanos = now;
                pending_object.changed_at_unix_nanos = now;
                pending_object.last_modified_by_event_id = Some(EventId::generate());
                self.add_transient_object_metadata(pending_object.clone())?;
                if transient_create
                    && touch_parent_on_create
                    && self.is_local_metadata_primary_for_object(&pending_object)?
                {
                    self.touch_parent_directory_modified(&path)?;
                }
                let _ = (intent_id, durable_receipts, required_receipts);
                self.stat_object_for_path(&pending_object, &path)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } if !options.wait_for_commit => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                    return Err(DfsPosixError::PendingWrite {
                        intent_id,
                        durable_receipts,
                        required_receipts,
                    });
                }
                Ok(DfsStat {
                    file_system_id: object.file_system_id,
                    path,
                    object_id: object.object_id,
                    kind: object.kind,
                    mode: object.mode,
                    uid: object.uid,
                    gid: object.gid,
                    size_bytes: Some(manifest.size_bytes),
                    link_count: 1,
                    version: target_version,
                    modified_at_unix_nanos: object.modified_at_unix_nanos,
                    accessed_at_unix_nanos: object.accessed_at_unix_nanos,
                    created_at_unix_nanos: object.created_at_unix_nanos,
                    changed_at_unix_nanos: object.changed_at_unix_nanos,
                    content_hash: Some(manifest.content_hash),
                    policy: object.policy,
                    current_locations: object.current_locations,
                })
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                Err(DfsPosixError::PendingWrite {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                })
            }
            NetworkedDfsWriteOutcome::Conflict { conflict } => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                Err(DfsPosixError::Conflict {
                    path,
                    reason: conflict.reason,
                })
            }
        }
    }

    fn posix_write_file_from_reader_checked_parent<R: Read>(
        &mut self,
        path: &str,
        reader: R,
        options: DfsWriteOptions,
        check_parent: bool,
        touch_parent_on_create: bool,
    ) -> Result<DfsStat> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if requires_directory {
            return match self.ensure_resolved_path_is_directory(&path) {
                Ok(()) => Err(DfsPosixError::IsDirectory { path }),
                Err(DfsPosixError::NotDirectory { path }) => {
                    Err(DfsPosixError::NotDirectory { path })
                }
                Err(DfsPosixError::NotFound { path }) => Err(DfsPosixError::NotDirectory { path }),
                Err(error) => Err(error),
            };
        }
        if check_parent {
            self.ensure_parent_directory(&path)?;
        }
        let mut transient_create = false;
        let object = match self.lookup_sifs_path(&path)? {
            Some(object) if object.kind == DfsObjectKind::Directory => {
                return Err(DfsPosixError::IsDirectory { path });
            }
            Some(object) if object.kind == DfsObjectKind::Hardlink => {
                self.resolve_hardlink_target(&object)?
            }
            Some(object) if object.kind == DfsObjectKind::Tombstone => {
                let object =
                    recreated_posix_metadata_object(&self.node_id, &path, DfsObjectKind::File);
                self.add_transient_object_metadata(object.clone())?;
                transient_create = true;
                object
            }
            Some(object) => object,
            None => {
                let object = new_posix_metadata_object(&self.node_id, &path, DfsObjectKind::File);
                self.add_transient_object_metadata(object.clone())?;
                transient_create = true;
                object
            }
        };
        let target_version = next_version(&object.version);
        let target_version_for_stat = target_version.clone();
        let candidates = self.posix_replica_candidates_for_write(&object, &options)?;
        let committed_locations = candidates.clone();
        let outcome = self.write_distributed_eventual_from_reader_with_candidates_and_parent_touch(
            DfsWriteRequest {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                target_version,
                requesting_node_id: self.node_id.clone(),
            },
            reader,
            candidates,
            options.policy,
            touch_parent_on_create,
            options.foreground_replica_repair,
        );
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                return Err(error.into());
            }
        };
        let outcome = if options.wait_for_commit {
            match self.settle_ready_write_outcome(outcome) {
                Ok(outcome) => outcome,
                Err(error) => {
                    if transient_create {
                        self.remove_transient_object_metadata(&object.object_id);
                    }
                    return Err(error.into());
                }
            }
        } else {
            outcome
        };
        match outcome {
            NetworkedDfsWriteOutcome::Committed {
                version,
                content_hash,
                repair_jobs,
                ..
            } => {
                let mut committed_object = object.clone();
                committed_object.version = version;
                committed_object.content_hash = Some(content_hash);
                committed_object.current_locations = committed_locations
                    .into_iter()
                    .filter(|node_id| {
                        !repair_jobs
                            .iter()
                            .any(|job| job.missing_replica_node_id == *node_id)
                    })
                    .collect();
                committed_object.content_size_bytes =
                    self.object_content_size_hint(&committed_object)?;
                let now = now_unix_nanos();
                committed_object.modified_at_unix_nanos = now;
                committed_object.changed_at_unix_nanos = now;
                committed_object.last_modified_by_event_id = Some(EventId::generate());
                self.add_transient_object_metadata(committed_object.clone())?;
                // A routed create's parent-mtime update is journaled by the
                // metadata shard primary and arrives here through journal
                // replay; appending it locally as well would fork this
                // replica's journal sequence space from the primary's stream.
                if transient_create
                    && touch_parent_on_create
                    && self.is_local_metadata_primary_for_object(&object)?
                {
                    self.touch_parent_directory_modified(&path)?;
                }
                if !options.foreground_replica_repair {
                    return self.stat_object_for_path(&committed_object, &path);
                }
                // Bounded response replay may not have covered a deep
                // catch-up; pull further batches before surfacing NotFound
                // for a write that committed.
                let object = match self.stat_target_for_path(&path) {
                    Err(DfsPosixError::NotFound { .. }) => {
                        let mut refreshed = None;
                        for _ in 0..POSIX_ROUTED_SYNC_RETRY_ROUNDS {
                            self.sync_remote_namespace_path(&path)?;
                            match self.stat_target_for_path(&path) {
                                Err(DfsPosixError::NotFound { .. }) => continue,
                                result => {
                                    refreshed = Some(result);
                                    break;
                                }
                            }
                        }
                        refreshed.unwrap_or_else(|| self.stat_target_for_path(&path))?
                    }
                    result => result?,
                };
                self.stat_object_for_path(&object, &path)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } if !options.wait_for_commit => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                    return Err(DfsPosixError::PendingWrite {
                        intent_id,
                        durable_receipts,
                        required_receipts,
                    });
                }
                let object = self.stat_target_for_path(&path)?;
                let mut stat = self.stat_object(&object)?;
                stat.path = path.clone();
                if let Ok(manifest) = self
                    .content
                    .read_manifest(object.object_id.as_str(), target_version_for_stat.as_str())
                {
                    stat.size_bytes = Some(manifest.size_bytes);
                }
                let _ = intent_id;
                let _ = durable_receipts;
                let _ = required_receipts;
                Ok(stat)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                Err(DfsPosixError::PendingWrite {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                })
            }
            NetworkedDfsWriteOutcome::Conflict { conflict } => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                Err(DfsPosixError::Conflict {
                    path,
                    reason: conflict.reason,
                })
            }
        }
    }

    pub fn posix_write_file_at_from_reader<R: Read>(
        &mut self,
        path: &str,
        offset: u64,
        mut reader: R,
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let mut first_byte = [0_u8; 1];
        let first_len = reader
            .read(&mut first_byte)
            .map_err(|source| DfsError::ContentStore {
                message: format!("failed to read POSIX write_at stream for {path}: {source}"),
            })?;
        if first_len == 0 {
            return self.posix_write_file_at(path, offset, &[], options);
        }
        let mut reader = Cursor::new(first_byte[..first_len].to_vec()).chain(reader);
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if requires_directory {
            return match self.ensure_resolved_path_is_directory(&path) {
                Ok(()) => Err(DfsPosixError::IsDirectory { path }),
                Err(DfsPosixError::NotDirectory { path }) => {
                    Err(DfsPosixError::NotDirectory { path })
                }
                Err(DfsPosixError::NotFound { path }) => Err(DfsPosixError::NotDirectory { path }),
                Err(error) => Err(error),
            };
        }
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        let base_version = object.version.clone();
        let target_version = next_version(&object.version);
        let candidates = self.posix_replica_candidates_for_write(&object, &options)?;
        let request = DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version,
            requesting_node_id: self.node_id.clone(),
        };
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidates, options.policy.clone())?;
        if object
            .content_size_bytes
            .is_some_and(|current_size| offset <= current_size)
            && let Some(outcome) = self.write_distributed_eventual_patch_from_reader_with_base(
                request,
                base_version,
                offset,
                &mut reader,
                plan.desired_replica_nodes,
                options.policy.clone(),
            )?
        {
            let outcome = if options.wait_for_commit {
                self.settle_ready_write_outcome(outcome)?
            } else {
                outcome
            };
            return match outcome {
                NetworkedDfsWriteOutcome::Committed { .. } => {
                    let object = self.stat_target_for_path(&path)?;
                    self.stat_object_for_path(&object, &path)
                }
                NetworkedDfsWriteOutcome::Pending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                } if !options.wait_for_commit => {
                    let object = self.stat_target_for_path(&path)?;
                    let mut stat = self.stat_object_for_path(&object, &path)?;
                    stat.size_bytes = object.content_size_bytes;
                    let _ = intent_id;
                    let _ = durable_receipts;
                    let _ = required_receipts;
                    Ok(stat)
                }
                NetworkedDfsWriteOutcome::Pending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                } => Err(DfsPosixError::PendingWrite {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                }),
                NetworkedDfsWriteOutcome::Conflict { conflict } => Err(DfsPosixError::Conflict {
                    path,
                    reason: conflict.reason,
                }),
            };
        }

        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(|source| DfsError::ContentStore {
                message: format!("failed to buffer POSIX write_at fallback for {path}: {source}"),
            })?;
        self.posix_write_file_at(&path, offset, &bytes, options)
    }

    pub fn posix_write_file_at(
        &mut self,
        path: &str,
        offset: u64,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if requires_directory {
            return match self.ensure_resolved_path_is_directory(&path) {
                Ok(()) => Err(DfsPosixError::IsDirectory { path }),
                Err(DfsPosixError::NotDirectory { path }) => {
                    Err(DfsPosixError::NotDirectory { path })
                }
                Err(DfsPosixError::NotFound { path }) => Err(DfsPosixError::NotDirectory { path }),
                Err(error) => Err(error),
            };
        }
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        let base_version = object.version.clone();
        let target_version = next_version(&object.version);
        let candidates = self.posix_replica_candidates_for_write(&object, &options)?;
        let request = DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version,
            requesting_node_id: self.node_id.clone(),
        };
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidates, options.policy.clone())?;
        let patch_end =
            offset
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| DfsPosixError::InvalidPath {
                    path: format!("{path}@{offset}+{}", bytes.len()),
                })?;
        if object
            .content_size_bytes
            .is_some_and(|current_size| offset <= current_size)
            && let Some(outcome) = self.write_distributed_eventual_patch_with_base(
                request.clone(),
                base_version.clone(),
                offset,
                bytes,
                plan.desired_replica_nodes.clone(),
                options.policy.clone(),
            )?
        {
            let outcome = if options.wait_for_commit {
                self.settle_ready_write_outcome(outcome)?
            } else {
                outcome
            };
            match outcome {
                NetworkedDfsWriteOutcome::Committed { .. } => {
                    let object = self.stat_target_for_path(&path)?;
                    return self.stat_object_for_path(&object, &path);
                }
                NetworkedDfsWriteOutcome::Pending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                } if !options.wait_for_commit => {
                    let object = self.stat_target_for_path(&path)?;
                    let mut stat = self.stat_object_for_path(&object, &path)?;
                    stat.size_bytes = object.content_size_bytes.map(|size| size.max(patch_end));
                    let _ = intent_id;
                    let _ = durable_receipts;
                    let _ = required_receipts;
                    return Ok(stat);
                }
                NetworkedDfsWriteOutcome::Pending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                } => {
                    return Err(DfsPosixError::PendingWrite {
                        intent_id,
                        durable_receipts,
                        required_receipts,
                    });
                }
                NetworkedDfsWriteOutcome::Conflict { conflict } => {
                    return Err(DfsPosixError::Conflict {
                        path,
                        reason: conflict.reason,
                    });
                }
            }
        }
        let mut current = match self.read_current_object_or_fetch_remote(&object.object_id)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => bytes,
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. } => {
                return Err(DfsPosixError::Conflict {
                    path,
                    reason: "current file content is not locally available for ranged write; retry"
                        .to_string(),
                });
            }
            NetworkedDfsReadOutcome::NotFound => return Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                return Err(read_conflict_error(path, conflicts));
            }
        };
        let start = usize::try_from(offset).map_err(|_| DfsPosixError::InvalidPath {
            path: format!("{path}@{offset}"),
        })?;
        let end = start
            .checked_add(bytes.len())
            .ok_or_else(|| DfsPosixError::InvalidPath {
                path: format!("{path}@{offset}+{}", bytes.len()),
            })?;
        if current.len() < end {
            current.resize(end, 0);
        }
        current[start..end].copy_from_slice(bytes);
        let result_size = current.len() as u64;
        let outcome = self.write_distributed_eventual_with_base(
            request,
            Some(base_version),
            current,
            plan.desired_replica_nodes,
            options.policy,
        )?;
        let outcome = if options.wait_for_commit {
            self.settle_ready_write_outcome(outcome)?
        } else {
            outcome
        };
        match outcome {
            NetworkedDfsWriteOutcome::Committed { .. } => {
                let object = self.stat_target_for_path(&path)?;
                self.stat_object_for_path(&object, &path)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } if !options.wait_for_commit => {
                let object = self.stat_target_for_path(&path)?;
                let mut stat = self.stat_object_for_path(&object, &path)?;
                stat.size_bytes = Some(result_size);
                let _ = intent_id;
                let _ = durable_receipts;
                let _ = required_receipts;
                Ok(stat)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } => Err(DfsPosixError::PendingWrite {
                intent_id,
                durable_receipts,
                required_receipts,
            }),
            NetworkedDfsWriteOutcome::Conflict { conflict } => Err(DfsPosixError::Conflict {
                path,
                reason: conflict.reason,
            }),
        }
    }

    pub fn posix_truncate_file(
        &mut self,
        path: &str,
        len: u64,
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if requires_directory {
            return match self.ensure_resolved_path_is_directory(&path) {
                Ok(()) => Err(DfsPosixError::IsDirectory { path }),
                Err(DfsPosixError::NotDirectory { path }) => {
                    Err(DfsPosixError::NotDirectory { path })
                }
                Err(DfsPosixError::NotFound { path }) => Err(DfsPosixError::NotDirectory { path }),
                Err(error) => Err(error),
            };
        }
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        let base_version = object.version.clone();
        let mut current = match self.read_current_object_or_fetch_remote(&object.object_id)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => bytes,
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. } => {
                return Err(DfsPosixError::Conflict {
                    path,
                    reason: "current file content is not locally available for truncate; retry"
                        .to_string(),
                });
            }
            NetworkedDfsReadOutcome::NotFound => return Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                return Err(read_conflict_error(path, conflicts));
            }
        };
        let len = usize::try_from(len).map_err(|_| DfsPosixError::InvalidPath {
            path: format!("{path}@len={len}"),
        })?;
        current.resize(len, 0);
        let target_version = next_version(&object.version);
        let candidates = self.posix_replica_candidates_for_write(&object, &options)?;
        let request = DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version,
            requesting_node_id: self.node_id.clone(),
        };
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidates, options.policy.clone())?;
        let outcome = self.write_distributed_eventual_with_base(
            request,
            Some(base_version),
            current,
            plan.desired_replica_nodes,
            options.policy,
        )?;
        let outcome = if options.wait_for_commit {
            self.settle_ready_write_outcome(outcome)?
        } else {
            outcome
        };
        match outcome {
            NetworkedDfsWriteOutcome::Committed { .. } => {
                let object = self.stat_target_for_path(&path)?;
                self.stat_object_for_path(&object, &path)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } if !options.wait_for_commit => {
                let object = self.stat_target_for_path(&path)?;
                let mut stat = self.stat_object_for_path(&object, &path)?;
                stat.size_bytes = Some(len as u64);
                let _ = intent_id;
                let _ = durable_receipts;
                let _ = required_receipts;
                Ok(stat)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } => Err(DfsPosixError::PendingWrite {
                intent_id,
                durable_receipts,
                required_receipts,
            }),
            NetworkedDfsWriteOutcome::Conflict { conflict } => Err(DfsPosixError::Conflict {
                path,
                reason: conflict.reason,
            }),
        }
    }

    pub fn posix_symlink(
        &mut self,
        target: &str,
        link_path: &str,
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        validate_posix_symlink_target(target)?;
        let (link_path, requires_directory) =
            normalize_posix_path_with_trailing_directory(link_path)?;
        let link_path = self.resolve_posix_path(&link_path, false)?;
        if requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&link_path)?;
            return Err(DfsPosixError::AlreadyExists { path: link_path });
        }
        if link_path == "/" {
            return Err(DfsPosixError::InvalidPath { path: link_path });
        }
        self.ensure_parent_directory(&link_path)?;
        let object = match self.lookup_sifs_path(&link_path)? {
            Some(object) if object.kind != DfsObjectKind::Tombstone => {
                return Err(DfsPosixError::AlreadyExists { path: link_path });
            }
            Some(_) => {
                let object = recreated_posix_metadata_object(
                    &self.node_id,
                    &link_path,
                    DfsObjectKind::Symlink,
                );
                self.add_transient_object_metadata(object.clone())?;
                object
            }
            None => {
                let object =
                    new_posix_metadata_object(&self.node_id, &link_path, DfsObjectKind::Symlink);
                self.add_transient_object_metadata(object.clone())?;
                object
            }
        };
        let target_version = next_version(&object.version);
        let candidates = self.posix_replica_candidates_for_write(&object, &options)?;
        let outcome = self.write_distributed_eventual_with_candidates(
            DfsWriteRequest {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                target_version,
                requesting_node_id: self.node_id.clone(),
            },
            target.as_bytes().to_vec(),
            candidates,
            options.policy,
        );
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                self.remove_transient_object_metadata(&object.object_id);
                return Err(error.into());
            }
        };
        let outcome = if options.wait_for_commit {
            match self.settle_ready_write_outcome(outcome) {
                Ok(outcome) => outcome,
                Err(error) => {
                    self.remove_transient_object_metadata(&object.object_id);
                    return Err(error.into());
                }
            }
        } else {
            outcome
        };
        match outcome {
            NetworkedDfsWriteOutcome::Committed { .. } => {
                // Same as posix_write_file: the shard primary journals the
                // parent-mtime update for routed symlink creates.
                if self.is_local_metadata_primary_for_object(&object)? {
                    self.touch_parent_directory_modified(&link_path)?;
                }
                let object =
                    self.lookup_sifs_path(&link_path)?
                        .ok_or_else(|| DfsPosixError::NotFound {
                            path: link_path.clone(),
                        })?;
                self.stat_object(&object)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } if !options.wait_for_commit => {
                self.remove_transient_object_metadata(&object.object_id);
                Err(DfsPosixError::PendingWrite {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                })
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } => {
                self.remove_transient_object_metadata(&object.object_id);
                Err(DfsPosixError::PendingWrite {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                })
            }
            NetworkedDfsWriteOutcome::Conflict { conflict } => {
                self.remove_transient_object_metadata(&object.object_id);
                Err(DfsPosixError::Conflict {
                    path: link_path,
                    reason: conflict.reason,
                })
            }
        }
    }

    pub fn posix_hardlink(&mut self, existing_path: &str, link_path: &str) -> Result<DfsStat> {
        let normalized_existing = normalize_posix_path(existing_path)?;
        let normalized_link = normalize_posix_path(link_path)?;
        let existing_primary = self.routed_namespace_primary_for_path(&normalized_existing)?;
        let link_primary = self.routed_namespace_primary_for_path(&normalized_link)?;
        match (existing_primary, link_primary) {
            (None, None) => self.posix_hardlink_local(existing_path, link_path),
            (None, Some(_)) => {
                // This node owns the target's shard and coordinates the
                // cross-shard alias adoption itself.
                self.namespace_coordinator_dispatch_hardlink(
                    &sifs_file_system_id(),
                    existing_path,
                    link_path,
                )?;
                self.stat_routed_namespace_result(&normalized_link, false)
            }
            (Some(target_primary), link_primary) => {
                // The link target's shard primary coordinates; it adopts the
                // alias into the link path's shard when that lives elsewhere.
                self.route_namespace_operation(
                    target_primary.clone(),
                    DfsNamespaceOperation::Hardlink {
                        existing_path: existing_path.to_string(),
                        link_path: link_path.to_string(),
                    },
                )?;
                if link_primary.is_some_and(|link_primary| link_primary != target_primary) {
                    self.sync_remote_namespace_path(&normalized_link)?;
                }
                self.stat_routed_namespace_result(&normalized_link, false)
            }
        }
    }

    pub(crate) fn posix_hardlink_local(
        &mut self,
        existing_path: &str,
        link_path: &str,
    ) -> Result<DfsStat> {
        let (existing_path, existing_requires_directory) =
            normalize_posix_path_with_trailing_directory(existing_path)?;
        let existing_path = self.resolve_posix_path_distributed(&existing_path, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&existing_path)?;
        if existing_requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&existing_path)?;
        }
        let (link_path, link_requires_directory) =
            normalize_posix_path_with_trailing_directory(link_path)?;
        let link_path = self.resolve_posix_path_distributed(&link_path, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&link_path)?;
        if link_requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&link_path)?;
            return Err(DfsPosixError::AlreadyExists { path: link_path });
        }
        if link_path == "/" {
            return Err(DfsPosixError::InvalidPath { path: link_path });
        }
        self.ensure_parent_directory(&link_path)?;
        match self.lookup_sifs_path(&link_path)? {
            Some(object) if object.kind != DfsObjectKind::Tombstone => {
                return Err(DfsPosixError::AlreadyExists { path: link_path });
            }
            _ => {}
        }
        let existing =
            self.lookup_sifs_path(&existing_path)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: existing_path.clone(),
                })?;
        if existing.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: existing_path,
            });
        }
        let target = self.resolve_hardlink_target(&existing)?;
        match target.kind {
            DfsObjectKind::Directory => {
                return Err(DfsPosixError::IsDirectory {
                    path: existing_path,
                });
            }
            DfsObjectKind::Tombstone => {
                return Err(DfsPosixError::NotFound {
                    path: existing_path,
                });
            }
            _ => {}
        }
        let mut target = target;
        target.changed_at_unix_nanos = now_unix_nanos();
        target.version = next_version(&target.version);
        self.add_object_metadata(target.clone())?;
        let alias_object_id = {
            let candidate = object_id_for_path(&link_path);
            if candidate == target.object_id {
                DfsObjectId::generate()
            } else {
                candidate
            }
        };
        let alias = DfsObjectMetadata {
            object_id: alias_object_id,
            file_system_id: target.file_system_id.clone(),
            path: link_path.clone(),
            kind: DfsObjectKind::Hardlink,
            owner_node_id: self.node_id.clone(),
            current_locations: target.current_locations.clone(),
            content_hash: target.content_hash.clone(),
            content_size_bytes: target.content_size_bytes,
            link_target_object_id: Some(target.object_id.clone()),
            mode: target.mode,
            uid: target.uid,
            gid: target.gid,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: initial_version(),
            policy: target.policy.clone(),
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        self.add_object_metadata(alias.clone())?;
        self.touch_parent_directory_modified(&link_path)?;
        self.stat_object_for_path(&target, &link_path)
    }

    pub fn posix_set_mode(&mut self, path: &str, mode: u32) -> Result<DfsStat> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::SetMode {
                    path: path.to_string(),
                    mode,
                },
            )?;
            return self.posix_stat(&normalized);
        }
        self.posix_set_mode_local(path, mode)
    }

    pub(crate) fn posix_set_mode_local(&mut self, path: &str, mode: u32) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&path)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let mut target = self.resolve_hardlink_target(&object)?;
        if target.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        target.mode = mode & 0o7777;
        target.changed_at_unix_nanos = now_unix_nanos();
        target.version = next_version(&target.version);
        self.add_object_metadata(target.clone())?;
        self.stat_object_for_path(&target, &path)
    }

    pub fn posix_set_owner(&mut self, path: &str, uid: u32, gid: u32) -> Result<DfsStat> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::SetOwner {
                    path: path.to_string(),
                    uid,
                    gid,
                },
            )?;
            return self.posix_stat(&normalized);
        }
        self.posix_set_owner_local(path, uid, gid)
    }

    pub(crate) fn posix_set_owner_local(
        &mut self,
        path: &str,
        uid: u32,
        gid: u32,
    ) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&path)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let mut target = self.resolve_hardlink_target(&object)?;
        if target.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        target.uid = uid;
        target.gid = gid;
        target.changed_at_unix_nanos = now_unix_nanos();
        target.version = next_version(&target.version);
        self.add_object_metadata(target.clone())?;
        self.stat_object_for_path(&target, &path)
    }

    pub fn posix_set_times(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::SetTimes {
                    path: path.to_string(),
                    accessed_at_unix_nanos,
                    modified_at_unix_nanos,
                    follow_symlink: true,
                },
            )?;
            return self.posix_stat(&normalized);
        }
        self.posix_set_times_local(path, accessed_at_unix_nanos, modified_at_unix_nanos)
    }

    pub(crate) fn posix_set_times_local(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        self.posix_set_times_at_path(&path, accessed_at_unix_nanos, modified_at_unix_nanos)
    }

    pub fn posix_set_symlink_times(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::SetTimes {
                    path: path.to_string(),
                    accessed_at_unix_nanos,
                    modified_at_unix_nanos,
                    follow_symlink: false,
                },
            )?;
            return self.posix_lstat(&normalized);
        }
        self.posix_set_symlink_times_local(path, accessed_at_unix_nanos, modified_at_unix_nanos)
    }

    pub(crate) fn posix_set_symlink_times_local(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path_distributed(path, false)?;
        self.posix_set_times_at_path(&path, accessed_at_unix_nanos, modified_at_unix_nanos)
    }

    fn posix_set_times_at_path(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        self.ensure_resolved_path_not_in_cross_shard_flight(path)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: path.to_string(),
            })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        let mut target = self.resolve_hardlink_target(&object)?;
        if target.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        if accessed_at_unix_nanos.is_none() && modified_at_unix_nanos.is_none() {
            return self.stat_object_for_path(&target, path);
        }
        if let Some(accessed_at_unix_nanos) = accessed_at_unix_nanos {
            target.accessed_at_unix_nanos = accessed_at_unix_nanos;
        }
        if let Some(modified_at_unix_nanos) = modified_at_unix_nanos {
            target.modified_at_unix_nanos = modified_at_unix_nanos;
        }
        target.changed_at_unix_nanos = now_unix_nanos();
        target.version = next_version(&target.version);
        self.add_object_metadata(target.clone())?;
        self.stat_object_for_path(&target, path)
    }

    pub fn posix_readlink(&self, path: &str) -> Result<String> {
        let path = self.resolve_existing_posix_path(path, false)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind != DfsObjectKind::Symlink {
            return Err(DfsPosixError::NotSymlink { path });
        }
        match self.read_local_object_with_state(&object.object_id, false)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => String::from_utf8(bytes).map_err(|_| DfsPosixError::InvalidPath { path }),
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    pub fn posix_readlink_distributed(&mut self, path: &str) -> Result<String> {
        let path = self.resolve_existing_posix_path_distributed(path, false)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind != DfsObjectKind::Symlink {
            return Err(DfsPosixError::NotSymlink { path });
        }
        match self.read_object_metadata_or_fetch_remote_without_cache(&object)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => String::from_utf8(bytes).map_err(|_| DfsPosixError::InvalidPath { path }),
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    pub fn posix_readlink_update_access(&mut self, path: &str) -> Result<String> {
        let resolved_path = self.resolve_existing_posix_path_distributed(path, false)?;
        let target = self.posix_readlink_distributed(path)?;
        let object =
            self.lookup_sifs_path(&resolved_path)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: resolved_path.clone(),
                })?;
        let mut object = self.resolve_hardlink_target(&object)?;
        if object.kind != DfsObjectKind::Symlink {
            return Err(DfsPosixError::NotSymlink {
                path: resolved_path,
            });
        }
        if !self.is_local_metadata_primary_for_object(&object)? {
            return Ok(target);
        }
        object.accessed_at_unix_nanos = now_unix_nanos();
        object.version = next_version(&object.version);
        self.add_object_metadata(object)?;
        Ok(target)
    }

    pub fn posix_unlink(&mut self, path: &str) -> Result<DfsStat> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::Unlink {
                    path: path.to_string(),
                },
            )?;
            return self.stat_replayed_tombstone_synced(&normalized);
        }
        self.posix_unlink_local(path)
    }

    pub(crate) fn posix_unlink_local(&mut self, path: &str) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path_distributed(path, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&path)?;
        let mut object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        if object.kind == DfsObjectKind::Hardlink {
            if let Some(target_object_id) = object.link_target_object_id.clone()
                && let Some(mut target) = self.object_by_id(&target_object_id)?
            {
                target.changed_at_unix_nanos = now_unix_nanos();
                target.version = next_version(&target.version);
                self.add_object_metadata(target)?;
            }
            object.kind = DfsObjectKind::Tombstone;
            object.version = next_version(&object.version);
            object.content_hash = None;
            object.content_size_bytes = None;
            object.link_target_object_id = None;
            object.current_locations = vec![self.node_id.clone()];
            self.add_object_metadata(object.clone())?;
            self.touch_parent_directory_modified(&path)?;
            return self.stat_object(&object);
        }
        if self.link_count_for_object(&object)? > 1 {
            // Unlinking a canonical record promotes one of its hardlink
            // aliases. When the chosen alias lives in a shard owned by
            // another primary, the promotion is a cross-shard move of the
            // canonical record onto the alias path.
            if let Some(alias) = self.first_hardlink_to_object(&object.object_id)? {
                let alias_path = normalize_posix_path(&alias.path)?;
                if let Some(destination_primary) =
                    self.routed_namespace_primary_for_path(&alias_path)?
                {
                    self.coordinate_cross_shard_rename(&path, &alias_path, destination_primary)?;
                    return self.stat_replayed_tombstone(&path);
                }
            }
            return self.unlink_canonical_with_hardlinks(object, &path);
        }
        object.kind = DfsObjectKind::Tombstone;
        object.version = next_version(&object.version);
        object.content_hash = None;
        object.content_size_bytes = None;
        object.link_target_object_id = None;
        object.current_locations = vec![self.node_id.clone()];
        self.add_object_metadata(object.clone())?;
        self.touch_parent_directory_modified(&path)?;
        self.stat_object(&object)
    }

    pub fn posix_rmdir(&mut self, path: &str) -> Result<DfsStat> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::Rmdir {
                    path: path.to_string(),
                },
            )?;
            return self.stat_replayed_tombstone_synced(&normalized);
        }
        self.posix_rmdir_local(path)
    }

    pub(crate) fn posix_rmdir_local(&mut self, path: &str) -> Result<DfsStat> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&path)?;
        if path == "/" {
            return Err(DfsPosixError::InvalidPath { path });
        }
        let mut object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        if object.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory { path });
        }
        if self.sifs_dir_has_children(&path)? {
            return Err(DfsPosixError::DirectoryNotEmpty { path });
        }
        object.kind = DfsObjectKind::Tombstone;
        object.version = next_version(&object.version);
        object.current_locations = vec![self.node_id.clone()];
        self.add_object_metadata(object.clone())?;
        self.touch_parent_directory_modified(&path)?;
        self.stat_object(&object)
    }

    pub fn posix_rename(&mut self, from: &str, to: &str) -> Result<DfsStat> {
        let normalized_from = normalize_posix_path(from)?;
        let normalized_to = normalize_posix_path(to)?;
        let from_primary = self.routed_namespace_primary_for_path(&normalized_from)?;
        let to_primary = self.routed_namespace_primary_for_path(&normalized_to)?;
        match (from_primary, to_primary) {
            (None, None) => self.posix_rename_local(from, to),
            (None, Some(_)) => {
                // This node owns the source side and coordinates the
                // cross-shard move itself.
                self.namespace_coordinator_dispatch_rename(&sifs_file_system_id(), from, to)?;
                self.stat_routed_namespace_result(&normalized_to, true)
            }
            (Some(source_primary), to_primary) => {
                // The source-side shard primary coordinates; it adopts the
                // record into the destination shard when that lives
                // elsewhere.
                self.route_namespace_operation(
                    source_primary.clone(),
                    DfsNamespaceOperation::Rename {
                        from: from.to_string(),
                        to: to.to_string(),
                    },
                )?;
                if to_primary.is_some_and(|to_primary| to_primary != source_primary) {
                    self.sync_remote_namespace_path(&normalized_to)?;
                }
                self.stat_routed_namespace_result(&normalized_to, true)
            }
        }
    }

    pub(crate) fn posix_rename_local(&mut self, from: &str, to: &str) -> Result<DfsStat> {
        let (from, from_requires_directory) = normalize_posix_path_with_trailing_directory(from)?;
        let (to, to_requires_directory) = normalize_posix_path_with_trailing_directory(to)?;
        let from = self.resolve_posix_path_distributed(&from, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&from)?;
        if from_requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&from)?;
        }
        let to = self.resolve_posix_path_distributed(&to, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&to)?;
        if from == "/" || to == "/" {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{from} -> {to}"),
            });
        }
        if from == to {
            return self.posix_stat(&from);
        }
        let mut source = self
            .lookup_sifs_path(&from)?
            .ok_or_else(|| DfsPosixError::NotFound { path: from.clone() })?;
        if source.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path: from });
        }
        if to_requires_directory && source.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory { path: to });
        }
        self.ensure_parent_directory(&to)?;
        if source.kind == DfsObjectKind::Directory {
            let child_prefix = format!("{}/", from.trim_end_matches('/'));
            if to.starts_with(&child_prefix) {
                return Err(DfsPosixError::InvalidPath {
                    path: format!("{from} -> {to}"),
                });
            }
            let mut job = self.posix_rename_directory_bounded_resolved(
                &from,
                &to,
                source.clone(),
                POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS,
            )?;
            while job.status != DfsNamespaceJobStatus::Completed {
                job = self.posix_resume_namespace_job_bounded(
                    &job.job_id,
                    POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS,
                )?;
            }
            return self.posix_stat(&to);
        }

        let old_source_path = source.path.clone();
        let mut moved_children = Vec::new();
        let mut hardlink_target_updates = Vec::new();

        if let Some(mut destination) = self.lookup_sifs_path(&to)? {
            if destination.kind != DfsObjectKind::Tombstone {
                let source_target = self.resolve_hardlink_target(&source)?;
                let mut destination_target = self.resolve_hardlink_target(&destination)?;
                if source_target.object_id == destination_target.object_id {
                    return self.posix_stat(&to);
                }
                match (&source.kind, &destination.kind) {
                    (DfsObjectKind::Directory, DfsObjectKind::Directory) => {
                        if self.sifs_dir_has_children(&to)? {
                            return Err(DfsPosixError::DirectoryNotEmpty { path: to });
                        }
                    }
                    (DfsObjectKind::Directory, _) => {
                        return Err(DfsPosixError::NotDirectory { path: to });
                    }
                    (_, DfsObjectKind::Directory) => {
                        return Err(DfsPosixError::IsDirectory { path: to });
                    }
                    _ => {}
                }
                let destination_target_changed_at = now_unix_nanos();
                if !matches!(
                    destination.kind,
                    DfsObjectKind::Hardlink | DfsObjectKind::Directory
                ) && self.link_count_for_object(&destination_target)? > 1
                {
                    let alias = self
                        .first_hardlink_to_object(&destination.object_id)?
                        .ok_or_else(|| DfsPosixError::NotFound { path: to.clone() })?;
                    // Demoting the replaced destination onto its alias would
                    // write into the alias's shard; that shard's primary must
                    // be this node, or the promotion would fork its journal.
                    let alias_path = normalize_posix_path(&alias.path)?;
                    if self
                        .routed_namespace_primary_for_path(&alias_path)?
                        .is_some()
                    {
                        return Err(DfsPosixError::InvalidPath {
                            path: format!(
                                "{to} has a hardlink alias at {alias_path} owned by another metadata shard primary"
                            ),
                        });
                    }
                    let mut alias_tombstone = alias.clone();
                    alias_tombstone.path = hidden_alias_tombstone_path(&alias.object_id);
                    alias_tombstone.kind = DfsObjectKind::Tombstone;
                    alias_tombstone.version = next_version(&alias.version);
                    alias_tombstone.content_hash = None;
                    alias_tombstone.content_size_bytes = None;
                    alias_tombstone.link_target_object_id = None;
                    alias_tombstone.current_locations = vec![self.node_id.clone()];

                    destination.path = alias.path.clone();
                    destination.changed_at_unix_nanos = destination_target_changed_at;
                    destination.version = next_version(&destination.version);
                    moved_children.push(alias_tombstone);
                    moved_children.push(destination);
                } else {
                    if destination.kind == DfsObjectKind::Hardlink {
                        destination_target.changed_at_unix_nanos = destination_target_changed_at;
                        destination_target.version = next_version(&destination_target.version);
                        hardlink_target_updates.push(destination_target);
                    }
                    destination.kind = DfsObjectKind::Tombstone;
                    destination.version = next_version(&destination.version);
                    destination.content_hash = None;
                    destination.content_size_bytes = None;
                    destination.link_target_object_id = None;
                    destination.current_locations = vec![self.node_id.clone()];
                    moved_children.push(destination);
                }
            }
        }

        if source.kind == DfsObjectKind::Hardlink
            && let Some(target_object_id) = source.link_target_object_id.as_ref()
            && let Some(mut source_target) = self.object_by_id(target_object_id)?
        {
            source_target.changed_at_unix_nanos = now_unix_nanos();
            source_target.version = next_version(&source_target.version);
            if !hardlink_target_updates
                .iter()
                .any(|object: &DfsObjectMetadata| object.object_id == source_target.object_id)
            {
                hardlink_target_updates.push(source_target);
            }
        }

        let mut old_path_tombstone = source.clone();
        old_path_tombstone.object_id = DfsObjectId::generate();
        old_path_tombstone.path = old_source_path;
        old_path_tombstone.kind = DfsObjectKind::Tombstone;
        old_path_tombstone.version = next_version(&source.version);
        old_path_tombstone.content_hash = None;
        old_path_tombstone.content_size_bytes = None;
        old_path_tombstone.link_target_object_id = None;
        old_path_tombstone.current_locations = vec![self.node_id.clone()];

        source.path = to;
        source.changed_at_unix_nanos = now_unix_nanos();
        source.version = next_version(&source.version);
        let mut transaction_objects = vec![old_path_tombstone, source.clone()];
        transaction_objects.extend(moved_children);
        transaction_objects.extend(hardlink_target_updates);
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id_for_path(
                "rename",
                &source.object_id,
                &source.path,
            ),
            file_system_id: source.file_system_id.clone(),
            objects: transaction_objects,
            versions: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        self.touch_parent_directory_modified(&from)?;
        if parent_posix_path(&from) != parent_posix_path(&source.path) {
            self.touch_parent_directory_modified(&source.path)?;
        }
        self.stat_object(&source)
    }

    pub fn posix_stat(&self, path: &str) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path(path, true)?;
        self.posix_lstat(&path)
    }

    pub fn posix_stat_distributed(&mut self, path: &str) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        self.posix_lstat_distributed(&path)
    }

    pub fn posix_lstat(&self, path: &str) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path(path, false)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        self.stat_object_for_path(&object, &path)
    }

    pub fn posix_lstat_distributed(&mut self, path: &str) -> Result<DfsStat> {
        let (path, object) = self.lookup_sifs_path_distributed_synced(path, false)?;
        let object = object.ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        self.stat_object_for_path(&object, &path)
    }

    pub fn posix_stat_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsStat>> {
        let Some(object) = self.object_by_id(object_id)? else {
            return Ok(None);
        };
        if object.kind == DfsObjectKind::Tombstone {
            return Ok(None);
        }
        Ok(Some(self.stat_object(&object)?))
    }

    pub fn posix_list_dir(&self, path: &str) -> Result<Vec<DfsDirEntry>> {
        let path = self.resolve_existing_posix_path(path, true)?;
        self.posix_collect_resolved_dir(&path)
    }

    pub fn posix_list_dir_distributed(&mut self, path: &str) -> Result<Vec<DfsDirEntry>> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        self.posix_collect_resolved_dir(&path)
    }

    pub fn posix_list_dir_page(
        &self,
        path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirEntryPage> {
        let path = self.resolve_existing_posix_path(path, true)?;
        self.posix_list_resolved_dir_page(&path, after, max_entries)
    }

    pub fn posix_list_dir_distributed_page(
        &mut self,
        path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirEntryPage> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        self.posix_list_resolved_dir_page(&path, after, max_entries)
    }

    fn posix_collect_resolved_dir(&self, path: &str) -> Result<Vec<DfsDirEntry>> {
        let mut entries = Vec::new();
        try_walk_metadata_pages(|cursor| {
            let page =
                self.posix_list_resolved_dir_page(path, cursor, POSIX_LIST_DIR_COLLECT_PAGE_SIZE)?;
            entries.extend(page.entries);
            Ok::<_, DfsPosixError>(page.next_cursor)
        })?;
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(entries)
    }

    fn posix_list_resolved_dir_page(
        &self,
        path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirEntryPage> {
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: path.to_string(),
            })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        if object.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: path.to_string(),
            });
        }
        let mut entries = Vec::new();
        let page = self.child_objects_for_sifs_dir_page(path, after, max_entries)?;
        for object in page.objects {
            let object_path = normalize_posix_path(&object.path)?;
            let listed_object_id = object.object_id.clone();
            let Some(object) = self.lookup_sifs_path(&object_path)? else {
                continue;
            };
            if object.kind == DfsObjectKind::Tombstone || object.object_id != listed_object_id {
                continue;
            }
            let target = self.resolve_hardlink_target(&object)?;
            entries.push(DfsDirEntry {
                name: object_path
                    .rsplit('/')
                    .next()
                    .filter(|name| !name.is_empty())
                    .unwrap_or("/")
                    .to_string(),
                path: object_path,
                kind: target.kind,
                object_id: target.object_id,
            });
        }
        Ok(DfsDirEntryPage {
            entries,
            next_cursor: page.next_cursor,
        })
    }

    pub fn posix_list_dir_update_access(&mut self, path: &str) -> Result<Vec<DfsDirEntry>> {
        let resolved_path = self.resolve_existing_posix_path_distributed(path, true)?;
        let entries = self.posix_list_dir(&resolved_path)?;
        let object =
            self.lookup_sifs_path(&resolved_path)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: resolved_path.clone(),
                })?;
        let mut object = self.resolve_hardlink_target(&object)?;
        if object.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: resolved_path,
            });
        }
        if !self.is_local_metadata_primary_for_object(&object)? {
            return Ok(entries);
        }
        object.accessed_at_unix_nanos = now_unix_nanos();
        object.version = next_version(&object.version);
        self.add_object_metadata(object)?;
        Ok(entries)
    }

    fn lookup_sifs_path(&self, path: &str) -> Result<Option<DfsObjectMetadata>> {
        self.lookup_metadata_path(&sifs_file_system_id(), path)
    }

    fn resolve_posix_path(&self, path: &str, follow_final: bool) -> Result<String> {
        let mut current = normalize_posix_path(path)?;
        for _ in 0..40 {
            let components = current
                .trim_start_matches('/')
                .split('/')
                .filter(|component| !component.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            let mut resolved = "/".to_string();
            let mut restarted = false;
            for (index, component) in components.iter().enumerate() {
                let candidate = join_posix_path(&resolved, component);
                let final_component = index + 1 == components.len();
                if final_component && !follow_final {
                    resolved = candidate;
                    continue;
                }
                let Some(object) = self.lookup_sifs_path(&candidate)? else {
                    resolved = append_posix_components(candidate, &components[index + 1..])?;
                    return Ok(resolved);
                };
                if object.kind == DfsObjectKind::Tombstone {
                    resolved = append_posix_components(candidate, &components[index + 1..])?;
                    return Ok(resolved);
                }
                let object = self.resolve_hardlink_target(&object)?;
                if object.kind == DfsObjectKind::Symlink {
                    let target = self.symlink_target_for_object(&candidate, &object)?;
                    current = resolve_posix_symlink_target(&candidate, &target)?;
                    for rest in &components[index + 1..] {
                        current = join_posix_path(&current, rest);
                    }
                    current = normalize_posix_path(&current)?;
                    restarted = true;
                    break;
                }
                resolved = candidate;
            }
            if !restarted {
                return Ok(resolved);
            }
        }
        Err(DfsPosixError::InvalidPath {
            path: path.to_string(),
        })
    }

    fn resolve_posix_path_distributed(&mut self, path: &str, follow_final: bool) -> Result<String> {
        let mut current = normalize_posix_path(path)?;
        for _ in 0..40 {
            let components = current
                .trim_start_matches('/')
                .split('/')
                .filter(|component| !component.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            let mut resolved = "/".to_string();
            let mut restarted = false;
            for (index, component) in components.iter().enumerate() {
                let candidate = join_posix_path(&resolved, component);
                let final_component = index + 1 == components.len();
                if final_component && !follow_final {
                    resolved = candidate;
                    continue;
                }
                let Some(object) = self.lookup_sifs_path(&candidate)? else {
                    resolved = append_posix_components(candidate, &components[index + 1..])?;
                    return Ok(resolved);
                };
                if object.kind == DfsObjectKind::Tombstone {
                    resolved = append_posix_components(candidate, &components[index + 1..])?;
                    return Ok(resolved);
                }
                let object = self.resolve_hardlink_target(&object)?;
                if object.kind == DfsObjectKind::Symlink {
                    let target = self.symlink_target_for_object_distributed(&candidate, &object)?;
                    current = resolve_posix_symlink_target(&candidate, &target)?;
                    for rest in &components[index + 1..] {
                        current = join_posix_path(&current, rest);
                    }
                    current = normalize_posix_path(&current)?;
                    restarted = true;
                    break;
                }
                resolved = candidate;
            }
            if !restarted {
                return Ok(resolved);
            }
        }
        Err(DfsPosixError::InvalidPath {
            path: path.to_string(),
        })
    }

    fn resolve_existing_posix_path(&self, path: &str, follow_final: bool) -> Result<String> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path(&path, follow_final || requires_directory)?;
        if requires_directory {
            self.ensure_resolved_path_is_directory(&path)?;
        }
        Ok(path)
    }

    fn resolve_existing_posix_path_distributed(
        &mut self,
        path: &str,
        follow_final: bool,
    ) -> Result<String> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path =
            self.resolve_posix_path_distributed(&path, follow_final || requires_directory)?;
        if requires_directory {
            self.ensure_resolved_path_is_directory(&path)?;
        }
        Ok(path)
    }

    fn symlink_target_for_object(&self, path: &str, object: &DfsObjectMetadata) -> Result<String> {
        if object.kind != DfsObjectKind::Symlink {
            return Err(DfsPosixError::NotSymlink {
                path: path.to_string(),
            });
        }
        match self.read_local_object_with_state(&object.object_id, false)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => String::from_utf8(bytes).map_err(|_| DfsPosixError::InvalidPath {
                path: path.to_string(),
            }),
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound {
                path: path.to_string(),
            }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path.to_string(), conflicts))
            }
        }
    }

    fn symlink_target_for_object_distributed(
        &mut self,
        path: &str,
        object: &DfsObjectMetadata,
    ) -> Result<String> {
        if object.kind != DfsObjectKind::Symlink {
            return Err(DfsPosixError::NotSymlink {
                path: path.to_string(),
            });
        }
        match self.read_object_metadata_or_fetch_remote_without_cache(object)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => String::from_utf8(bytes).map_err(|_| DfsPosixError::InvalidPath {
                path: path.to_string(),
            }),
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound {
                path: path.to_string(),
            }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path.to_string(), conflicts))
            }
        }
    }

    fn lookup_metadata_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        if self.has_tombstone_ancestor(file_system_id, path)? {
            return Ok(None);
        }
        self.lookup_metadata_path_raw(file_system_id, path)
    }

    fn lookup_metadata_path_raw(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        if let Some(tombstone) = self
            .tombstones_by_metadata_path
            .get(&(file_system_id.clone(), metadata_path(path)))
        {
            return Ok(Some(tombstone.clone()));
        }
        let memory_object = self
            .metadata
            .object_by_normalized_posix_path(file_system_id, path)
            .cloned();
        if let Some(backend) = self.metadata_backend.as_ref() {
            let backend_object =
                backend.get_object_by_path(file_system_id, &metadata_path(path))?;
            let object = match (memory_object.as_ref(), backend_object.as_ref()) {
                (Some(memory_object), Some(backend_object)) => {
                    Some(self.resolve_authoritative_object_metadata(
                        &backend_object.object_id,
                        Some(memory_object),
                        Some(backend_object),
                    )?)
                }
                (Some(memory_object), None) => Some(memory_object.clone()),
                (_, Some(backend_object)) => Some(backend_object.clone()),
                (None, None) => None,
            };
            return object
                .map(|object| self.object_metadata_with_receipt_locations(object))
                .transpose()
                .map_err(Into::into);
        }
        Ok(memory_object)
    }

    fn has_tombstone_ancestor(&self, file_system_id: &DfsFileSystemId, path: &str) -> Result<bool> {
        let path = normalize_posix_path(path)?;
        if path == "/" {
            return Ok(false);
        }
        let mut parent = path
            .rsplit_once('/')
            .map(|(parent, _)| parent)
            .unwrap_or("/");
        if parent.is_empty() {
            parent = "/";
        }
        loop {
            if let Some(object) = self.lookup_metadata_path_raw(file_system_id, parent)? {
                if object.kind == DfsObjectKind::Tombstone {
                    return Ok(true);
                }
            }
            if parent == "/" {
                return Ok(false);
            }
            parent = parent
                .rsplit_once('/')
                .map(|(ancestor, _)| if ancestor.is_empty() { "/" } else { ancestor })
                .unwrap_or("/");
        }
    }

    fn child_objects_for_sifs_dir(&self, path: &str) -> Result<Vec<DfsObjectMetadata>> {
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            let mut children = Vec::new();
            try_walk_metadata_pages(|cursor| {
                let page = backend.child_objects_for_directory_page(
                    &file_system_id,
                    path,
                    cursor,
                    POSIX_LIST_DIR_COLLECT_PAGE_SIZE,
                )?;
                children.extend(page.objects);
                Ok::<_, DfsPosixError>(page.next_cursor)
            })?;
            return Ok(children);
        }
        let prefix = if path == "/" {
            "/".to_string()
        } else {
            format!("{}/", path.trim_end_matches('/'))
        };
        Ok(self
            .metadata
            .objects
            .values()
            .filter(|object| {
                if object.file_system_id != file_system_id
                    || object.kind == DfsObjectKind::Tombstone
                {
                    return false;
                }
                let Ok(object_path) = normalize_posix_path(&object.path) else {
                    return false;
                };
                if object_path == path || !object_path.starts_with(&prefix) {
                    return false;
                }
                let rest = object_path.trim_start_matches(&prefix);
                !rest.is_empty() && !rest.contains('/')
            })
            .cloned()
            .collect())
    }

    fn child_objects_for_sifs_dir_page(
        &self,
        path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        if max_entries == 0 {
            return Ok(DfsDirectoryPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.child_objects_for_directory_page(
                &file_system_id,
                path,
                after,
                max_entries,
            )?);
        }
        let mut objects = self.child_objects_for_sifs_dir(path)?;
        objects.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        if let Some(after) = after {
            objects.retain(|object| {
                (&object.path, &object.object_id) > (&after.path, &after.object_id)
            });
        }
        let has_more = objects.len() > max_entries;
        objects.truncate(max_entries);
        let next_cursor = if has_more {
            objects.last().map(|object| DfsDirectoryPageCursor {
                path: object.path.clone(),
                object_id: object.object_id.clone(),
                dentry: None,
                shard_id: None,
            })
        } else {
            None
        };
        Ok(DfsDirectoryPage {
            objects,
            next_cursor,
        })
    }

    fn sifs_dir_has_children(&self, path: &str) -> Result<bool> {
        Ok(!self
            .child_objects_for_sifs_dir_page(path, None, 1)?
            .objects
            .is_empty())
    }

    fn hardlink_count_to_object(&self, target_object_id: &DfsObjectId) -> Result<usize> {
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.hardlink_count_to_object(&file_system_id, target_object_id)?);
        }
        Ok(self
            .metadata
            .objects
            .values()
            .filter(|candidate| {
                candidate.file_system_id == file_system_id
                    && candidate.kind == DfsObjectKind::Hardlink
                    && candidate.link_target_object_id.as_ref() == Some(target_object_id)
            })
            .count())
    }

    fn first_hardlink_to_object(
        &self,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.first_hardlink_to_object(&file_system_id, target_object_id)?);
        }
        let mut first = None::<DfsObjectMetadata>;
        for candidate in self.metadata.objects.values() {
            if candidate.file_system_id != file_system_id
                || candidate.kind != DfsObjectKind::Hardlink
                || candidate.link_target_object_id.as_ref() != Some(target_object_id)
            {
                continue;
            }
            if first.as_ref().is_none_or(|current| {
                candidate
                    .path
                    .cmp(&current.path)
                    .then_with(|| candidate.object_id.cmp(&current.object_id))
                    .is_lt()
            }) {
                first = Some(candidate.clone());
            }
        }
        Ok(first)
    }

    fn child_directory_count_for_sifs_dir(&self, path: &str) -> Result<u64> {
        let mut count = 0u64;
        try_walk_metadata_pages(|cursor| {
            let page =
                self.child_objects_for_sifs_dir_page(path, cursor, POSIX_LINK_COUNT_PAGE_SIZE)?;
            count = count.saturating_add(
                page.objects
                    .iter()
                    .filter(|child| child.kind == DfsObjectKind::Directory)
                    .count() as u64,
            );
            Ok::<_, DfsPosixError>(page.next_cursor)
        })?;
        Ok(count)
    }

    fn object_by_id(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>> {
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(object) = backend.get_object(object_id)?
        {
            return self
                .object_metadata_with_receipt_locations(object)
                .map(Some)
                .map_err(Into::into);
        }
        self.metadata
            .object(object_id)
            .cloned()
            .map(|object| self.object_metadata_with_receipt_locations(object))
            .transpose()
            .map_err(Into::into)
    }

    pub(crate) fn resolve_hardlink_target(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsObjectMetadata> {
        let mut current = object.clone();
        for _ in 0..16 {
            if current.kind != DfsObjectKind::Hardlink {
                return Ok(current);
            }
            let Some(target_id) = current.link_target_object_id.clone() else {
                return Err(DfsPosixError::NotFound { path: current.path });
            };
            current = self
                .object_by_id(&target_id)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: current.path.clone(),
                })?;
        }
        Err(DfsPosixError::InvalidPath {
            path: object.path.clone(),
        })
    }

    /// Stat for a path that a routed unlink/rmdir just tombstoned; the
    /// response journal batches were replayed before this runs, so the local
    /// lookup observes the authoritative tombstone like a local removal does.
    fn stat_replayed_tombstone(&self, path: &str) -> Result<DfsStat> {
        let resolved = self.resolve_posix_path(path, false)?;
        let object = self
            .lookup_sifs_path(&resolved)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: resolved.clone(),
            })?;
        self.stat_object(&object)
    }

    fn stat_target_for_path(&self, path: &str) -> Result<DfsObjectMetadata> {
        let object = self
            .lookup_sifs_path(path)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: path.to_string(),
            })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        self.resolve_hardlink_target(&object)
    }

    fn ensure_parent_directory(&self, path: &str) -> Result<()> {
        let Some(parent) = path.rsplit_once('/').map(|(parent, _)| parent) else {
            return Ok(());
        };
        let parent = if parent.is_empty() { "/" } else { parent };
        match self.lookup_sifs_path(parent)? {
            Some(object) if object.kind == DfsObjectKind::Directory => Ok(()),
            Some(object) if object.kind == DfsObjectKind::Tombstone => {
                Err(DfsPosixError::MissingParent {
                    path: parent.to_string(),
                })
            }
            Some(_) => Err(DfsPosixError::NotDirectory {
                path: parent.to_string(),
            }),
            None => Err(DfsPosixError::MissingParent {
                path: parent.to_string(),
            }),
        }
    }

    fn ensure_resolved_path_is_directory(&self, path: &str) -> Result<()> {
        let object = self
            .lookup_sifs_path(path)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: path.to_string(),
            })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            Ok(())
        } else {
            Err(DfsPosixError::NotDirectory {
                path: path.to_string(),
            })
        }
    }

    fn ensure_resolved_path_is_directory_no_follow(&self, path: &str) -> Result<()> {
        let object = self
            .lookup_sifs_path(path)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: path.to_string(),
            })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            Ok(())
        } else {
            Err(DfsPosixError::NotDirectory {
                path: path.to_string(),
            })
        }
    }

    pub(crate) fn touch_parent_directory_modified(&mut self, path: &str) -> Result<()> {
        let Some(parent) = parent_posix_path(path) else {
            return Ok(());
        };
        self.touch_directory_modified(&parent)
    }

    pub fn posix_touch_directory_modified(&mut self, path: &str) -> Result<()> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&path)? {
            return self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::TouchDirectoryModified { path },
            );
        }
        self.touch_directory_modified(&path)
    }

    pub fn posix_touch_directory_modified_without_local_replay(
        &mut self,
        path: &str,
    ) -> Result<()> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&path)? {
            return self.route_namespace_operation_without_local_replay(
                primary,
                DfsNamespaceOperation::TouchDirectoryModified { path },
            );
        }
        self.touch_directory_modified(&path)
    }

    fn touch_directory_modified(&mut self, path: &str) -> Result<()> {
        let mut object =
            self.lookup_sifs_path(path)?
                .ok_or_else(|| DfsPosixError::MissingParent {
                    path: path.to_string(),
                })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::MissingParent {
                path: path.to_string(),
            });
        }
        if object.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: path.to_string(),
            });
        }
        let now = now_unix_nanos()
            .max(object.modified_at_unix_nanos.saturating_add(1))
            .max(object.changed_at_unix_nanos.saturating_add(1));
        object.modified_at_unix_nanos = now;
        object.changed_at_unix_nanos = now;
        object.version = next_version(&object.version);
        self.add_object_metadata(object)?;
        Ok(())
    }

    fn stat_object(&self, object: &DfsObjectMetadata) -> Result<DfsStat> {
        self.stat_object_for_path(object, &object.path)
    }

    fn stat_object_for_path(&self, object: &DfsObjectMetadata, path: &str) -> Result<DfsStat> {
        self.stat_object_for_path_with_link_count_hint(object, path, None)
    }

    fn stat_object_for_path_with_link_count_hint(
        &self,
        object: &DfsObjectMetadata,
        path: &str,
        link_count_hint: Option<u64>,
    ) -> Result<DfsStat> {
        let object = self.resolve_hardlink_target(object)?;
        let size_bytes = self.object_content_size_hint(&object)?;
        let link_count = match link_count_hint {
            Some(count)
                if object.kind != DfsObjectKind::Directory
                    && object.kind != DfsObjectKind::Hardlink =>
            {
                count
            }
            _ => self.link_count_for_object(&object)?,
        };
        Ok(DfsStat {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: normalize_posix_path(path)?,
            kind: object.kind.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes,
            link_count,
            mode: object_mode_or_default(&object),
            uid: object.uid,
            gid: object.gid,
            accessed_at_unix_nanos: object_timestamp_or_default(object.accessed_at_unix_nanos),
            modified_at_unix_nanos: object_timestamp_or_default(object.modified_at_unix_nanos),
            created_at_unix_nanos: object_timestamp_or_default(object.created_at_unix_nanos),
            changed_at_unix_nanos: object_timestamp_or_default(object.changed_at_unix_nanos),
            policy: object.policy.clone(),
            current_locations: object.current_locations.clone(),
        })
    }

    fn link_count_for_object(&self, object: &DfsObjectMetadata) -> Result<u64> {
        if object.kind == DfsObjectKind::Hardlink {
            let target = self.resolve_hardlink_target(object)?;
            return self.link_count_for_object(&target);
        }
        if object.kind != DfsObjectKind::Directory {
            let count = self.hardlink_count_to_object(&object.object_id)?;
            return Ok(1u64.saturating_add(count as u64));
        }
        let path = normalize_posix_path(&object.path)?;
        let child_directory_count = self.child_directory_count_for_sifs_dir(&path)?;
        Ok(2u64.saturating_add(child_directory_count))
    }

    fn unlink_canonical_with_hardlinks(
        &mut self,
        mut object: DfsObjectMetadata,
        old_path: &str,
    ) -> Result<DfsStat> {
        let alias = self
            .first_hardlink_to_object(&object.object_id)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: old_path.to_string(),
            })?;
        let mut alias_tombstone = alias.clone();
        alias_tombstone.path = hidden_alias_tombstone_path(&alias.object_id);
        alias_tombstone.kind = DfsObjectKind::Tombstone;
        alias_tombstone.version = next_version(&alias.version);
        alias_tombstone.content_hash = None;
        alias_tombstone.content_size_bytes = None;
        alias_tombstone.link_target_object_id = None;
        alias_tombstone.current_locations = vec![self.node_id.clone()];

        let mut old_path_tombstone = object.clone();
        old_path_tombstone.object_id = DfsObjectId::generate();
        old_path_tombstone.path = old_path.to_string();
        old_path_tombstone.kind = DfsObjectKind::Tombstone;
        old_path_tombstone.version = next_version(&object.version);
        old_path_tombstone.content_hash = None;
        old_path_tombstone.content_size_bytes = None;
        old_path_tombstone.link_target_object_id = None;
        old_path_tombstone.current_locations = vec![self.node_id.clone()];

        object.path = alias.path.clone();
        object.changed_at_unix_nanos = now_unix_nanos();
        object.version = next_version(&object.version);

        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id_for_path(
                "unlink-hardlink",
                &object.object_id,
                old_path,
            ),
            file_system_id: object.file_system_id.clone(),
            objects: vec![old_path_tombstone.clone(), alias_tombstone, object],
            versions: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        self.touch_parent_directory_modified(old_path)?;
        self.stat_object(&old_path_tombstone)
    }

    fn first_hardlink_alias_outside_prefix(
        &self,
        object: &DfsObjectMetadata,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let prefix = normalize_posix_path(prefix)?;
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.first_hardlink_to_object_outside_prefix(
                &file_system_id,
                &object.object_id,
                &prefix,
            )?);
        }
        let mut first = None::<DfsObjectMetadata>;
        for candidate in self.metadata.objects.values() {
            if candidate.file_system_id != file_system_id
                || candidate.kind != DfsObjectKind::Hardlink
                || candidate.link_target_object_id.as_ref() != Some(&object.object_id)
            {
                continue;
            }
            let Ok(path) = normalize_posix_path(&candidate.path) else {
                continue;
            };
            if posix_path_is_at_or_under_prefix(&path, &prefix) {
                continue;
            }
            if first.as_ref().is_none_or(|current| {
                candidate
                    .path
                    .cmp(&current.path)
                    .then_with(|| candidate.object_id.cmp(&current.object_id))
                    .is_lt()
            }) {
                first = Some(candidate.clone());
            }
        }
        Ok(first)
    }
}

fn namespace_job_id(
    kind: &str,
    file_system_id: &DfsFileSystemId,
    path: &str,
    object_id: &DfsObjectId,
    version: &DfsVersion,
) -> String {
    format!(
        "namespace_job_{}_{}",
        kind,
        blake3::hash(format!("{file_system_id}\0{path}\0{object_id}\0{version}").as_bytes())
            .to_hex()
    )
}

#[allow(dead_code)]
fn _assert_filesystem_send_sync<T: Send + Sync>(_: &DfsFileSystem) {}
