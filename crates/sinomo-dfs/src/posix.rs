//! POSIX-like DFS namespace facade.
//!
//! The facade provides path-based operations without Unix user/group permission
//! semantics. It is intended for services such as static web servers and
//! rollout and higher-level automation tools that need stable mkdir/list/stat/read/write
//! behavior over SIFS while the lower DFS layers handle placement, receipts,
//! repair, and metadata journaling.

mod attrs;
mod cross_shard;
mod jobs;
mod links;
mod metadata;
mod namespace_ops;
mod query;
mod read;
mod routing;

use crate::dfs_now_unix_nanos as now_unix_nanos;
use crate::namespace::next_version;
use crate::posix_model::{DfsPosixError, DfsStat, DfsWriteOptions, Result, read_conflict_error};
use crate::posix_object::{
    new_posix_metadata_object, recreated_posix_metadata_object, set_object_create_timestamps,
};
use crate::posix_path::{
    join_posix_path, normalize_posix_path, normalize_posix_path_with_trailing_directory,
    parent_posix_path,
};
use crate::{
    DfsControlPlaneStore, DfsError, DfsFileSystem, DfsMessageTransport, DfsNamespaceOperation,
    DfsNamespaceTransaction, DfsObjectKind, DfsObjectMetadata, DfsReadData, DfsReplicationPolicy,
    NetworkedDfsReadOutcome, NetworkedDfsService, NetworkedDfsWriteOutcome, sifs_file_system_id,
};
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};
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
/// Whole-file POSIX writes are blind overwrites, not user-supplied CAS writes.
/// If another local service instance advanced the metadata between lookup and
/// commit, refresh from the backend and retry against the current version.
const POSIX_WRITE_STALE_BASE_RETRY_ROUNDS: usize = 4;
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

fn posix_error_is_stale_write_base(error: &DfsPosixError) -> bool {
    matches!(
        error,
        DfsPosixError::Dfs(DfsError::Distributed { message })
            if message.contains("stale DFS write base")
    )
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn settle_ready_write_outcome(
        &mut self,
        outcome: NetworkedDfsWriteOutcome,
        foreground_replica_repair: bool,
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
                if foreground_replica_repair {
                    let (_, repair_events) = self.retry_pending_repairs_best_effort();
                    if !repair_events.is_empty() {
                        events.extend(repair_events);
                        self.last_events = events;
                    }
                }
                return Ok(outcome);
            }
            let (scheduled, repair_events) = if foreground_replica_repair {
                self.retry_pending_repairs_best_effort()
            } else {
                (Vec::new(), Vec::new())
            };
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

    fn sync_blocking_routed_write_namespace(
        &mut self,
        path: &str,
        wait_for_commit: bool,
        foreground_replica_repair: bool,
        replay_response_journal: bool,
    ) -> Result<()> {
        if !wait_for_commit || !foreground_replica_repair || !replay_response_journal {
            return Ok(());
        }
        self.sync_remote_namespace_path_until_quiet(path)
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
            if self.posix_mkdir_all_chain_exists_locally(&normalized)? {
                return Ok(());
            }
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
            if self.posix_mkdir_all_chain_exists_locally(&normalized)? {
                return Ok(());
            }
            return self.route_namespace_operation_without_local_replay(
                primary,
                DfsNamespaceOperation::MkdirAll {
                    path: path.to_string(),
                },
            );
        }
        self.posix_mkdir_all_local(path)
    }

    fn posix_mkdir_all_chain_exists_locally(&self, path: &str) -> Result<bool> {
        let path = normalize_posix_path(path)?;
        if path == "/" {
            return Ok(true);
        }
        let mut current = String::new();
        for part in path.trim_matches('/').split('/') {
            current.push('/');
            current.push_str(part);
            match self.lookup_sifs_path(&current)? {
                Some(object) if object.kind == DfsObjectKind::Directory => {}
                _ => return Ok(false),
            }
        }
        Ok(true)
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
        let mut last_stale_base = None;
        for attempt in 0..=POSIX_WRITE_STALE_BASE_RETRY_ROUNDS {
            match self.posix_write_file_bytes_checked_parent_once(
                path,
                bytes,
                options.clone(),
                check_parent,
                touch_parent_on_create,
                replay_response_journal,
            ) {
                Ok(stat) => return Ok(stat),
                Err(error)
                    if attempt < POSIX_WRITE_STALE_BASE_RETRY_ROUNDS
                        && posix_error_is_stale_write_base(&error) =>
                {
                    last_stale_base = Some(error);
                    self.hydrate_from_metadata_backend()?;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_stale_base.unwrap_or_else(|| {
            DfsPosixError::Dfs(DfsError::Distributed {
                message: "stale DFS write base retry exhausted".to_string(),
            })
        }))
    }

    fn posix_write_file_bytes_checked_parent_once(
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
        let detach_request = !options.wait_for_commit && !options.foreground_replica_repair;
        let inline_remote_primary_write = match self.select_metadata_shard_for_object(&object) {
            Ok(Some(shard)) => {
                !detach_request
                    && shard.primary_node_id != self.node_id
                    && (bytes.len() as u64) <= crate::DFS_ROUTED_WRITE_INLINE_BYTES_LIMIT
            }
            Ok(None) => false,
            Err(error) => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                return Err(error.into());
            }
        };
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
        let request = DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version: target_version.clone(),
            requesting_node_id: self.node_id.clone(),
        };
        let outcome = if inline_remote_primary_write {
            self.write_distributed_eventual_with_base_and_parent_touch_and_repair_mode(
                request,
                Some(object.version.clone()),
                bytes.to_vec(),
                candidates,
                options.policy,
                touch_parent_on_create,
                options.foreground_replica_repair,
            )
        } else {
            self.write_distributed_eventual_from_local_manifest_with_candidates_and_parent_touch(
                request,
                Some(object.version.clone()),
                manifest.content_hash.clone(),
                manifest.size_bytes,
                candidates,
                options.policy,
                touch_parent_on_create,
                options.foreground_replica_repair,
                detach_request,
                replay_response_journal,
            )
        };
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
            match self.settle_ready_write_outcome(outcome, options.foreground_replica_repair) {
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
            "posix-byte-write node={} path={} object={} version={} bytes={} inline_routed={} content_ms={} distributed_ms={} total_ms={}",
            self.node_id,
            trace_path,
            object.object_id,
            target_version,
            trace_bytes,
            inline_remote_primary_write,
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
                self.sync_blocking_routed_write_namespace(
                    &path,
                    options.wait_for_commit,
                    options.foreground_replica_repair,
                    replay_response_journal,
                )?;
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
            match self.settle_ready_write_outcome(outcome, options.foreground_replica_repair) {
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
                self.sync_blocking_routed_write_namespace(
                    &path,
                    options.wait_for_commit,
                    options.foreground_replica_repair,
                    true,
                )?;
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
                options.foreground_replica_repair,
            )?
        {
            let outcome = if options.wait_for_commit {
                self.settle_ready_write_outcome(outcome, options.foreground_replica_repair)?
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
                options.foreground_replica_repair,
            )?
        {
            let outcome = if options.wait_for_commit {
                self.settle_ready_write_outcome(outcome, options.foreground_replica_repair)?
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
                return Err(DfsError::Busy { path }.into());
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
        let outcome = self.write_distributed_eventual_with_base_and_repair_mode(
            request,
            Some(base_version),
            current,
            plan.desired_replica_nodes,
            options.policy,
            options.foreground_replica_repair,
        )?;
        let outcome = if options.wait_for_commit {
            self.settle_ready_write_outcome(outcome, options.foreground_replica_repair)?
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
                return Err(DfsError::Busy { path }.into());
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
        let outcome = self.write_distributed_eventual_with_base_and_repair_mode(
            request,
            Some(base_version),
            current,
            plan.desired_replica_nodes,
            options.policy,
            options.foreground_replica_repair,
        )?;
        let outcome = if options.wait_for_commit {
            self.settle_ready_write_outcome(outcome, options.foreground_replica_repair)?
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
