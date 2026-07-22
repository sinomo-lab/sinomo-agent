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
pub(crate) mod locks;
mod metadata;
mod namespace_ops;
mod query;
mod read;
mod routing;
pub(crate) mod sync;

use crate::dfs_now_unix_nanos as now_unix_nanos;
use crate::metadata_records::object_version_record;
use crate::namespace::{initial_version, namespace_transaction_id_for_object, next_version};
use crate::posix_model::{
    DfsPosixCreateAttrs, DfsPosixError, DfsStat, DfsWriteOptions, Result, read_conflict_error,
};
use crate::posix_object::{
    apply_posix_create_attrs, inherit_posix_create_attrs, new_posix_metadata_object,
    recreated_posix_metadata_object, set_object_create_timestamps,
};
use crate::posix_path::{
    join_posix_path, normalize_posix_path, normalize_posix_path_with_trailing_directory,
    parent_posix_path, posix_path_is_at_or_under_prefix,
};
use crate::{
    DfsControlPlaneStore, DfsError, DfsFileSystem, DfsLocalEventualBatchWriteProof,
    DfsMessageTransport, DfsMetadataShardConfig, DfsNamespaceOperation, DfsNamespaceTransaction,
    DfsObjectKind, DfsObjectMetadata, DfsReadData, DfsReplicaReceiptRecord, DfsReplicationPolicy,
    NetworkedDfsReadOutcome, NetworkedDfsService, NetworkedDfsWriteOutcome, sifs_file_system_id,
};
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;
use sinomo_core::{DfsObjectPolicy, DfsWriteReceipt, DfsWriteRequest};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read};
use std::time::{Duration, Instant};

const POSIX_WAIT_FOR_COMMIT_READY_DRAIN_LIMIT: usize = 8;
const POSIX_WAIT_FOR_COMMIT_REPAIR_MAX_JOBS_PER_PASS: usize = 1;
const POSIX_WAIT_FOR_COMMIT_IDLE_SLEEP: Duration = Duration::from_millis(10);
const POSIX_WAIT_FOR_COMMIT_TIMEOUT: Duration = Duration::from_secs(45);
const POSIX_LIST_DIR_COLLECT_PAGE_SIZE: usize = 4096;
const POSIX_LINK_COUNT_PAGE_SIZE: usize = 4096;
const POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS: usize = 512;
const POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE: usize = 128;
pub(crate) const POSIX_RENAME_MOVE_TRANSACTION_MAX_OBJECTS: usize =
    crate::DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT;
pub(crate) const POSIX_RENAME_MOVE_TRANSACTION_MAX_PATH_BYTES: usize =
    crate::DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT / 2;
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
/// If another service instance is already committing, wait for that intent to
/// settle, refresh from the backend, and retry against the new current version.
/// The caller's transport deadline remains authoritative; callers without one
/// use the same bounded wait as foreground commit settlement.
const POSIX_WRITE_SERIALIZATION_RETRY_IDLE_SLEEP: Duration = Duration::from_millis(10);
/// Budget for an authoritative path lookup or the best-effort freshness pull
/// used after that lookup cannot answer. These requests share the serialized
/// namespace lane with durable mutations, so the budget must tolerate bounded
/// WAN metadata work already ahead of them.
const POSIX_READ_FRESHNESS_SYNC_TIMEOUT: Duration = Duration::from_secs(15);
const POSIX_REMOTE_READ_IDLE_WAIT_MAX: Duration = Duration::from_millis(50);

struct PosixBulkLocalWrite<'a> {
    path: String,
    object: DfsObjectMetadata,
    target_version: DfsVersion,
    expected_base_version: Option<DfsVersion>,
    bytes: &'a [u8],
    metadata_shard: Option<DfsMetadataShardConfig>,
    parent_touch: Option<PosixBulkParentTouch>,
    link_count_hint: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PosixBulkParentTouch {
    path: String,
    metadata_shard: Option<DfsMetadataShardConfig>,
}

fn posix_error_is_retryable_write_serialization(error: &DfsPosixError) -> bool {
    let DfsPosixError::Dfs(DfsError::Distributed { message }) = error else {
        return false;
    };
    // Local authority errors and routed rejection envelopes currently meet at
    // this string-typed DFS boundary. Keep this list aligned with
    // `distributed_error_is_stale_or_concurrent_write`: all four messages mean
    // that this blind overwrite was not accepted and may be retried safely.
    [
        "stale DFS write base",
        "base version conflict",
        "duplicate DFS write intent for",
        "pending DFS write intent",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}

fn posix_error_is_routed_namespace_unavailable(error: &DfsPosixError) -> bool {
    matches!(error, DfsPosixError::Dfs(DfsError::Network { .. }))
}

#[cfg(test)]
fn select_posix_write_replica_candidates(
    local_node_id: &NodeId,
    replica_candidates: &[NodeId],
    desired_replicas: usize,
) -> Vec<NodeId> {
    std::iter::once(local_node_id.clone())
        .chain(replica_candidates.iter().cloned())
        .fold(Vec::new(), |mut selected, candidate| {
            if selected.len() < desired_replicas && !selected.contains(&candidate) {
                selected.push(candidate);
            }
            selected
        })
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn with_posix_commit_operation_deadline<R>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<R>,
    ) -> Result<R> {
        let requested_deadline = Instant::now() + POSIX_WAIT_FOR_COMMIT_TIMEOUT;
        let observed_previous_deadline = self.transport.operation_deadline();
        let replaced_previous_deadline = self
            .transport
            .set_operation_deadline(Some(requested_deadline));
        let previous_deadline = replaced_previous_deadline.or(observed_previous_deadline);
        let effective_deadline = previous_deadline
            .map(|deadline| deadline.min(requested_deadline))
            .unwrap_or(requested_deadline);
        if effective_deadline != requested_deadline {
            self.transport
                .set_operation_deadline(Some(effective_deadline));
        }
        let result = operation(self);
        self.transport.set_operation_deadline(previous_deadline);
        result
    }

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
        let caller_deadline = self.transport.operation_deadline();
        let settle_deadline = caller_deadline
            .map(|deadline| deadline.min(started_at + POSIX_WAIT_FOR_COMMIT_TIMEOUT))
            .unwrap_or(started_at + POSIX_WAIT_FOR_COMMIT_TIMEOUT);
        let mut outcome = outcome;
        let mut ready_drains = 0;
        loop {
            if Instant::now() >= settle_deadline {
                return Ok(outcome);
            }
            self.check_cancelled("write settle")?;
            let mut events = self.process_incoming()?;
            outcome = self.write_outcome(&intent_id)?;
            if let NetworkedDfsWriteOutcome::Pending {
                intent_id: next_intent_id,
                ..
            } = &outcome
            {
                intent_id = next_intent_id.clone();
            }
            if !matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }) {
                // Preserve the foreground repair pass after commit, but keep it
                // bounded when the caller (or the POSIX wrapper) installed an
                // operation deadline. This retains repair-failure evidence
                // without allowing recovery scans to outlive the write budget.
                if foreground_replica_repair && Instant::now() < settle_deadline {
                    let (_, repair_events) = if caller_deadline.is_some() {
                        self.retry_pending_repairs_best_effort_bounded(
                            POSIX_WAIT_FOR_COMMIT_REPAIR_MAX_JOBS_PER_PASS,
                        )
                    } else {
                        self.retry_pending_repairs_best_effort()
                    };
                    if !repair_events.is_empty() {
                        events.extend(repair_events);
                        self.last_events = events;
                    }
                }
                return Ok(outcome);
            }
            let (_scheduled, repair_events) =
                if foreground_replica_repair && Instant::now() < settle_deadline {
                    if caller_deadline.is_some() {
                        self.retry_pending_repairs_best_effort_bounded(
                            POSIX_WAIT_FOR_COMMIT_REPAIR_MAX_JOBS_PER_PASS,
                        )
                    } else {
                        self.retry_pending_repairs_best_effort()
                    }
                } else {
                    (Vec::new(), Vec::new())
                };
            if !repair_events.is_empty() {
                events.extend(repair_events);
                self.last_events = events.clone();
            }
            // Re-scheduled repairs alone are not progress: on a degraded cluster
            // every retry fails fast and reschedules, and treating that churn as
            // activity turned this wait into a hot spin (a full core per pending
            // write). Only incoming events skip the pacing wait — which wakes
            // early on arrival, so healthy-path settle latency is unchanged.
            if events.is_empty() || ready_drains >= POSIX_WAIT_FOR_COMMIT_READY_DRAIN_LIMIT {
                let now = Instant::now();
                if now >= settle_deadline {
                    return Ok(outcome);
                }
                let wait = POSIX_WAIT_FOR_COMMIT_IDLE_SLEEP
                    .min(settle_deadline.saturating_duration_since(now));
                self.transport
                    .wait_for_incoming(wait)
                    .map_err(DfsError::from_network)?;
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
        if !options.policy.forbidden_node_ids.contains(&self.node_id)
            && !candidates.contains(&self.node_id)
        {
            candidates.insert(0, self.node_id.clone());
        }
        for required in &options.policy.required_node_ids {
            if !candidates.contains(required) {
                candidates.push(required.clone());
            }
        }
        let request = DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version: next_version(&object.version),
            requesting_node_id: self.node_id.clone(),
        };
        let mut plan =
            self.plan_distributed_eventual_replicas(&request, candidates, options.policy.clone())?;

        // Strict confirmed writes keep the metadata primary in the synchronous
        // set when there is an unconstrained slot. Managed required/forbidden
        // placement remains authoritative over this locality preference.
        if options.policy.is_strict_confirmed()
            && options.policy.desired_replicas >= 2
            && let Some(shard) = self.select_metadata_shard_for_object(object)?
            && shard.primary_node_id != self.node_id
            && !options
                .policy
                .forbidden_node_ids
                .contains(&shard.primary_node_id)
            && !plan.desired_replica_nodes.contains(&shard.primary_node_id)
            && let Some(replica_slot) =
                plan.desired_replica_nodes.iter_mut().rev().find(|node_id| {
                    **node_id != self.node_id
                        && !options.policy.required_node_ids.contains(*node_id)
                })
        {
            *replica_slot = shard.primary_node_id;
            crate::validate_replication_plan(&plan.desired_replica_nodes, &options.policy)?;
        }

        Ok(plan.desired_replica_nodes)
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
        // A durable completed handoff does not mutate process-local authority
        // bindings in service handles opened before or after activation. Every
        // bootstrap path must therefore verify the exact active assignment,
        // completion proof, and local marker before its first authoritative
        // namespace mutation. Keeping this transition here makes it impossible
        // for a caller to accidentally open a fresh lane and skip the rebind.
        self.rebind_completed_local_metadata_shard_membership_handoffs(&sifs_file_system_id())?;
        let fallback_file_system = crate::namespace::distributed_sifs(self.node_id.clone());
        let file_system_id = fallback_file_system.file_system_id.clone();
        let (file_system, configured_shards) = if let Some(kv) = self.control_plane.as_mut() {
            let store = DfsControlPlaneStore::new(kv.as_mut());
            let file_system = store
                .get_file_system_config(&file_system_id)
                .map_err(DfsError::from)?
                .map(DfsFileSystem::from)
                .unwrap_or(fallback_file_system);
            let shards = store
                .list_metadata_shards(&file_system_id)
                .map_err(DfsError::from)?;
            (file_system, Some(shards))
        } else {
            (fallback_file_system, None)
        };
        let bootstrap_owner_node_id = file_system.owner_node_id.clone();
        if configured_shards.is_some() {
            // Cluster KV is the source of filesystem configuration. Register
            // it in memory for path resolution, but do not journal it across
            // every locally opened shard. Each exact shard primary persists
            // the record on that shard when it prepares its first canonical
            // namespace mutation; replicas learn it through that journal.
            self.clear_metadata_lookup_cache();
            self.metadata.add_file_system(file_system);
            self.clear_metadata_lookup_cache();
        } else {
            self.add_file_system(file_system)?;
        }
        for object in
            crate::namespace::distributed_sifs_bootstrap_objects(bootstrap_owner_node_id.clone())
        {
            if let Some(shards) = configured_shards.as_deref() {
                // Bootstrap is an authority-local maintenance action. A
                // client/replica must not manufacture root metadata when the
                // configured primary is elsewhere, and an incomplete default
                // split must fail closed for that anchor without preventing
                // the service from opening. The responsible primary will
                // create or repair its own fixed bootstrap objects.
                let Ok(Some(shard)) =
                    crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                        &file_system_id,
                        shards,
                        &object.path,
                    )
                else {
                    continue;
                };
                if shard.primary_node_id != self.node_id {
                    continue;
                }
                // A shard-primary bootstrap can start from journal genesis.
                // Seed the configured filesystem on this exact shard before
                // the first lookup: backend lookup intentionally rejects an
                // unknown filesystem, and using the broad sharded ensure here
                // would author the same entry on sibling replica shards.
                let backend = self.metadata_backend.as_mut().ok_or_else(|| {
                    DfsPosixError::Dfs(DfsError::Metadata {
                        message: format!(
                            "clustered SIFS bootstrap for shard {} requires a durable metadata backend",
                            shard.shard_id
                        ),
                    })
                })?;
                backend.ensure_file_system_on_shard(
                    &crate::DfsMetadataShardId::new(shard.shard_id),
                    self.metadata
                        .filesystems
                        .get(&file_system_id)
                        .ok_or_else(|| {
                            DfsPosixError::Dfs(DfsError::Metadata {
                                message: "clustered SIFS bootstrap lost its configured filesystem"
                                    .to_string(),
                            })
                        })?,
                )?;
            }
            // Clustered bootstrap objects are shard anchors. Their parent can
            // legitimately live on a shard that is not opened locally (or,
            // during a partial rollout, is not configured yet), so the
            // ordinary tombstone-ancestor walk would route `/_anchor` through
            // `/` and turn an exact local-shard bootstrap into
            // `UnknownFileSystem`. The anchor itself is still checked on its
            // selected shard; a tombstoned anchor is therefore not
            // resurrected. Standalone bootstrap retains the full ancestor
            // check.
            let existing = if configured_shards.is_some() {
                self.lookup_metadata_path_raw(&object.file_system_id, &object.path)?
            } else {
                self.lookup_metadata_path(&object.file_system_id, &object.path)?
            };
            match existing {
                None => self.add_object_metadata(object)?,
                // Early SIFS metadata predates persisted POSIX modes. A zero
                // mode on a runtime-owned bootstrap directory makes the
                // entire FUSE namespace untraversable under
                // `DefaultPermissions`. Repair only the fixed bootstrap set;
                // ordinary directories retain a deliberate chmod(000).
                Some(existing)
                    if existing.kind == DfsObjectKind::Directory
                        && existing.mode == 0
                        && existing.version == initial_version()
                        && existing.accessed_at_unix_nanos == 0
                        && existing.modified_at_unix_nanos == 0
                        && existing.created_at_unix_nanos == 0
                        && existing.changed_at_unix_nanos == 0 =>
                {
                    self.posix_set_mode(&object.path, object.mode)?;
                }
                Some(_) => {}
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

    pub fn posix_mkdir_with_create_attrs(
        &mut self,
        path: &str,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::MkdirWithAttrs {
                    path: path.to_string(),
                    attrs,
                },
            )?;
            return self.stat_routed_namespace_result(&normalized, false);
        }
        self.posix_mkdir_local_with_create_attrs(path, attrs)
    }

    pub(crate) fn posix_inherit_create_attrs_from_parent_local(
        &self,
        path: &str,
        attrs: DfsPosixCreateAttrs,
        directory: bool,
    ) -> Result<DfsPosixCreateAttrs> {
        let parent_path = parent_posix_path(path)
            .ok_or_else(|| DfsPosixError::InvalidPath { path: path.into() })?;
        let parent =
            self.lookup_sifs_path(&parent_path)?
                .ok_or_else(|| DfsPosixError::MissingParent {
                    path: parent_path.clone(),
                })?;
        if parent.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory { path: parent_path });
        }
        Ok(inherit_posix_create_attrs(
            attrs,
            directory,
            parent.mode,
            parent.gid,
        ))
    }

    fn posix_create_attrs_for_object_authority(
        &mut self,
        object: &DfsObjectMetadata,
        attrs: DfsPosixCreateAttrs,
        directory: bool,
    ) -> Result<DfsPosixCreateAttrs> {
        if self.is_local_metadata_primary_for_object(object)? {
            self.posix_inherit_create_attrs_from_parent_local(&object.path, attrs, directory)
        } else {
            Ok(attrs)
        }
    }

    fn commit_posix_created_directory(&mut self, object: DfsObjectMetadata) -> Result<DfsStat> {
        let parent = self.parent_directory_touch_metadata_for_create(&object)?;
        let mut objects = vec![object.clone()];
        let mut versions = vec![object_version_record(&object, object.content_size_bytes)];
        if let Some(parent) = parent {
            versions.push(object_version_record(&parent, parent.content_size_bytes));
            objects.push(parent);
        }
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id_for_object(
                "mkdir-create",
                &object.object_id,
                &object.version,
            ),
            file_system_id: object.file_system_id.clone(),
            objects,
            versions,
            advisory_lock_states: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        self.stat_object(&object)
    }

    pub(crate) fn posix_mkdir_local(&mut self, path: &str) -> Result<DfsStat> {
        self.posix_mkdir_local_with_create_attrs(path, DfsPosixCreateAttrs::directory_default())
    }

    pub(crate) fn posix_mkdir_local_with_create_attrs(
        &mut self,
        path: &str,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, false)?;
        self.posix_mkdir_local_at_normalized_path(&path, attrs)
    }

    pub(crate) fn posix_mkdir_at_no_links_local(
        &mut self,
        path: &str,
        expected_parent_object_id: &DfsObjectId,
        expected_parent_version: &DfsVersion,
    ) -> Result<DfsStat> {
        let path = normalize_posix_path(path)?;
        let parent = parent_posix_path(&path)
            .ok_or_else(|| DfsPosixError::InvalidPath { path: path.clone() })?;
        let current_parent =
            self.lookup_sifs_path(&parent)?
                .ok_or_else(|| DfsPosixError::MissingParent {
                    path: parent.clone(),
                })?;
        if current_parent.kind != DfsObjectKind::Directory {
            return Err(
                if matches!(
                    &current_parent.kind,
                    DfsObjectKind::Symlink | DfsObjectKind::Hardlink
                ) {
                    DfsPosixError::InvalidPath { path: parent }
                } else {
                    DfsPosixError::NotDirectory { path: parent }
                },
            );
        }
        if &current_parent.object_id != expected_parent_object_id
            || &current_parent.version != expected_parent_version
        {
            return Err(DfsPosixError::Conflict {
                path: parent,
                reason: "opened parent directory changed before no-link mkdir".to_string(),
            });
        }
        self.posix_mkdir_local_at_normalized_path(&path, DfsPosixCreateAttrs::directory_default())
    }

    fn posix_mkdir_local_at_normalized_path(
        &mut self,
        path: &str,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        let path = normalize_posix_path(path)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&path)?;
        self.ensure_parent_directory(&path)?;
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
            let attrs = self.posix_inherit_create_attrs_from_parent_local(&path, attrs, true)?;
            apply_posix_create_attrs(&mut object, attrs);
            object.version = next_version(&object.version);
            return self.commit_posix_created_directory(object);
        }
        let mut object = new_posix_metadata_object(&self.node_id, &path, DfsObjectKind::Directory);
        self.apply_managed_policy_to_new_object(&mut object, 2)?;
        let attrs = self.posix_inherit_create_attrs_from_parent_local(&path, attrs, true)?;
        apply_posix_create_attrs(&mut object, attrs);
        self.commit_posix_created_directory(object)
    }

    pub fn posix_mkdir_all(&mut self, path: &str) -> Result<()> {
        self.with_posix_commit_operation_deadline(|service| {
            service.posix_mkdir_all_with_current_deadline(path)
        })
    }

    fn posix_mkdir_all_with_current_deadline(&mut self, path: &str) -> Result<()> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            let local_chain_exists = self.posix_mkdir_all_chain_exists_locally(&normalized)?;
            match self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::MkdirAll {
                    path: path.to_string(),
                },
            ) {
                Ok(()) => {
                    self.stat_routed_namespace_result(&normalized, false)
                        .map(drop)?;
                    return Ok(());
                }
                Err(error)
                    if local_chain_exists
                        && posix_error_is_routed_namespace_unavailable(&error) =>
                {
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
        self.posix_mkdir_all_local(path)
    }

    pub fn posix_mkdir_all_without_local_replay(&mut self, path: &str) -> Result<()> {
        self.with_posix_commit_operation_deadline(|service| {
            service.posix_mkdir_all_without_local_replay_with_current_deadline(path)
        })
    }

    fn posix_mkdir_all_without_local_replay_with_current_deadline(
        &mut self,
        path: &str,
    ) -> Result<()> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            let local_chain_exists = self.posix_mkdir_all_chain_exists_locally(&normalized)?;
            match self.route_namespace_operation_without_local_replay(
                primary,
                DfsNamespaceOperation::MkdirAll {
                    path: path.to_string(),
                },
            ) {
                Ok(()) => return Ok(()),
                Err(error)
                    if local_chain_exists
                        && posix_error_is_routed_namespace_unavailable(&error) =>
                {
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
        self.posix_mkdir_all_local(path)
    }

    /// Ensure a directory chain exists beneath `root` without following any
    /// link component, including while creating a missing scoped root.
    ///
    /// Each component is opened from the filesystem root and each missing
    /// directory is created relative to an exact opened parent. A concurrent
    /// replacement therefore yields a bounded retry or conflict rather than a
    /// pathname traversal through the replacement.
    pub fn posix_mkdir_all_beneath_no_links(&mut self, root: &str, path: &str) -> Result<()> {
        const MAX_ATTEMPTS_PER_COMPONENT: usize = 3;

        let root = normalize_posix_path(root)?;
        let path = normalize_posix_path(path)?;
        if !posix_path_is_at_or_under_prefix(&path, &root) {
            return Err(DfsPosixError::InvalidPath { path });
        }
        self.posix_open_beneath_no_links_distributed("/", "/")?;
        if path == "/" {
            return Ok(());
        }

        let mut current = "/".to_string();
        for child in path
            .trim_start_matches('/')
            .split('/')
            .filter(|component| !component.is_empty())
        {
            let candidate = join_posix_path(&current, child);
            let mut completed = false;
            for attempt in 0..MAX_ATTEMPTS_PER_COMPONENT {
                let opened_parent = self.posix_open_beneath_no_links_distributed("/", &current)?;
                match self.posix_open_beneath_no_links_distributed("/", &candidate) {
                    Ok(opened) if opened.kind == DfsObjectKind::Directory => {
                        completed = true;
                        break;
                    }
                    Ok(_) => {
                        return Err(DfsPosixError::NotDirectory {
                            path: candidate.clone(),
                        });
                    }
                    Err(DfsPosixError::NotFound { .. }) => {
                        match self.posix_mkdir_at_opened_directory_no_links(&opened_parent, child) {
                            Ok(_) => {
                                completed = true;
                                break;
                            }
                            Err(DfsPosixError::AlreadyExists { .. })
                            | Err(DfsPosixError::Conflict { .. })
                                if attempt + 1 < MAX_ATTEMPTS_PER_COMPONENT =>
                            {
                                self.check_cancelled("no-link mkdir-all retry")?;
                                self.hydrate_from_metadata_backend()?;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
            if !completed {
                return Err(DfsPosixError::Conflict {
                    path: candidate,
                    reason: "directory component kept changing during no-link mkdir-all"
                        .to_string(),
                });
            }
            current = candidate;
        }
        Ok(())
    }

    /// Create one direct child directory relative to an exact opened parent.
    pub fn posix_mkdir_at_opened_directory_no_links(
        &mut self,
        opened_parent: &DfsStat,
        child: &str,
    ) -> Result<DfsStat> {
        if child.is_empty()
            || matches!(child, "." | "..")
            || child.contains('/')
            || child.bytes().any(|byte| byte == 0)
        {
            return Err(DfsPosixError::InvalidPath {
                path: child.to_string(),
            });
        }
        if opened_parent.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: opened_parent.path.clone(),
            });
        }
        let parent = normalize_posix_path(&opened_parent.path)?;
        let current_parent = self
            .lookup_sifs_path_direct_distributed_synced(&parent)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: parent.clone(),
            })?;
        if current_parent.kind != DfsObjectKind::Directory {
            return Err(
                if matches!(
                    &current_parent.kind,
                    DfsObjectKind::Symlink | DfsObjectKind::Hardlink
                ) {
                    DfsPosixError::InvalidPath {
                        path: parent.clone(),
                    }
                } else {
                    DfsPosixError::NotDirectory {
                        path: parent.clone(),
                    }
                },
            );
        }
        if current_parent.object_id != opened_parent.object_id
            || current_parent.version != opened_parent.version
        {
            return Err(DfsPosixError::Conflict {
                path: parent.clone(),
                reason: "opened parent directory changed before no-link mkdir".to_string(),
            });
        }
        let path = join_posix_path(&parent, child);
        if let Some(primary) = self.routed_namespace_primary_for_path(&path)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::MkdirAtNoLinks {
                    path: path.clone(),
                    expected_parent_object_id: opened_parent.object_id.clone(),
                    expected_parent_version: opened_parent.version.clone(),
                },
            )?;
            return self.stat_routed_namespace_result(&path, false);
        }
        self.posix_mkdir_at_no_links_local(&path, &opened_parent.object_id, &opened_parent.version)
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
        self.posix_write_file_bytes_checked_parent(
            path, bytes, options, true, true, true, None, false, false,
        )
    }

    /// Replace an existing file only while its stable identity and visible
    /// version still match the caller's snapshot.
    ///
    /// Unlike [`Self::posix_write_file`], this operation deliberately performs
    /// a single write attempt.  A stale base is returned as a conflict instead
    /// of being refreshed and retried against a newer version, which makes it
    /// suitable for editors and other compare-and-swap callers.
    pub fn posix_replace_file_if_version(
        &mut self,
        path: &str,
        expected_object_id: &DfsObjectId,
        expected_version: &DfsVersion,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        self.posix_write_file_bytes_checked_parent_once(
            path,
            bytes,
            options,
            true,
            false,
            true,
            None,
            false,
            false,
            Some((expected_object_id, expected_version)),
            true,
        )
    }

    pub fn posix_write_file_with_create_attrs(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        self.posix_write_file_bytes_checked_parent(
            path,
            bytes,
            options,
            true,
            true,
            true,
            Some(attrs),
            false,
            false,
        )
    }

    /// Create a new regular file with its final POSIX attributes.
    ///
    /// Unlike [`Self::posix_write_file_with_create_attrs`], this operation is
    /// exclusive: an existing non-tombstoned namespace object is never opened
    /// or overwritten. A create race is retried after refreshing authoritative
    /// metadata and resolves to `AlreadyExists` once the winner is visible.
    pub fn posix_create_file_with_attrs(
        &mut self,
        path: &str,
        options: DfsWriteOptions,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        self.posix_write_file_bytes_checked_parent(
            path,
            &[],
            options,
            true,
            true,
            true,
            Some(attrs),
            true,
            false,
        )
    }

    /// Create a new regular file with content and final POSIX attributes in
    /// one exclusive namespace operation.
    ///
    /// This is the content-bearing counterpart to
    /// [`Self::posix_create_file_with_attrs`]. It never opens or overwrites an
    /// existing object, including when two writers race to create the same
    /// path.
    pub fn posix_create_file_with_contents_and_attrs(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        self.posix_write_file_bytes_checked_parent(
            path,
            bytes,
            options,
            true,
            true,
            true,
            Some(attrs),
            true,
            false,
        )
    }

    /// Create a new regular file with content, inheriting the ordinary
    /// filesystem create attributes. Existing paths are rejected atomically.
    pub fn posix_create_file(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        self.posix_write_file_bytes_checked_parent(
            path, bytes, options, true, true, true, None, true, false,
        )
    }

    /// Exclusively create one regular file beneath a no-link scoped root.
    ///
    /// The parent is opened component-by-component without following links,
    /// then the child mutation is issued directly against that opened
    /// directory path. Retrying restarts the complete open so a concurrent
    /// namespace change can never turn a stale validation into a followed
    /// link.
    pub fn posix_create_file_beneath_no_links(
        &mut self,
        root: &str,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        const MAX_ATTEMPTS: usize = 3;

        let path = normalize_posix_path(path)?;
        let parent = parent_posix_path(&path)
            .ok_or_else(|| DfsPosixError::InvalidPath { path: path.clone() })?;
        let child = path
            .rsplit_once('/')
            .map(|(_, child)| child)
            .filter(|child| !child.is_empty())
            .ok_or_else(|| DfsPosixError::InvalidPath { path: path.clone() })?;

        for attempt in 0..MAX_ATTEMPTS {
            let opened_parent = self.posix_open_beneath_no_links_distributed(root, &parent)?;
            match self.posix_create_file_at_opened_directory_no_links(
                &opened_parent,
                child,
                bytes,
                options.clone(),
            ) {
                Ok(stat) => return Ok(stat),
                Err(error)
                    if attempt + 1 < MAX_ATTEMPTS
                        && (posix_error_is_retryable_write_serialization(&error)
                            || matches!(&error, DfsPosixError::Conflict { .. })) =>
                {
                    self.check_cancelled("no-link beneath create retry")?;
                    self.hydrate_from_metadata_backend()?;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("bounded no-link create loop returns on its final attempt")
    }

    /// Exclusively create a direct child of an exact opened directory without
    /// performing path resolution after the open.
    ///
    /// This is intentionally `openat`-shaped: `child` must be one basename,
    /// and the current directory record must still match the opened identity.
    pub fn posix_create_file_at_opened_directory_no_links(
        &mut self,
        opened_parent: &DfsStat,
        child: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        if child.is_empty()
            || matches!(child, "." | "..")
            || child.contains('/')
            || child.bytes().any(|byte| byte == 0)
        {
            return Err(DfsPosixError::InvalidPath {
                path: child.to_string(),
            });
        }
        if opened_parent.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: opened_parent.path.clone(),
            });
        }
        let parent = normalize_posix_path(&opened_parent.path)?;
        let current_parent = self
            .lookup_sifs_path_direct_distributed_synced(&parent)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: parent.clone(),
            })?;
        if current_parent.kind != DfsObjectKind::Directory {
            return Err(
                if matches!(
                    &current_parent.kind,
                    DfsObjectKind::Symlink | DfsObjectKind::Hardlink
                ) {
                    DfsPosixError::InvalidPath {
                        path: parent.clone(),
                    }
                } else {
                    DfsPosixError::NotDirectory {
                        path: parent.clone(),
                    }
                },
            );
        }
        if current_parent.object_id != opened_parent.object_id
            || current_parent.version != opened_parent.version
        {
            return Err(DfsPosixError::Conflict {
                path: parent.clone(),
                reason: "opened parent directory changed before exclusive create".to_string(),
            });
        }
        let path = join_posix_path(&parent, child);
        self.posix_write_file_bytes_checked_parent_once(
            &path, bytes, options, true, true, true, None, true, false, None, false,
        )
    }

    /// Compare-and-replace a file beneath `root` without following any link
    /// record. The caller's identity/version must match the exact no-link open
    /// before the content write begins.
    pub fn posix_replace_file_if_version_beneath_no_links(
        &mut self,
        root: &str,
        path: &str,
        expected_object_id: &DfsObjectId,
        expected_version: &DfsVersion,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let opened = self.posix_open_beneath_no_links_distributed(root, path)?;
        if &opened.object_id != expected_object_id || &opened.version != expected_version {
            return Err(DfsPosixError::Conflict {
                path: opened.path.clone(),
                reason: "file identity or version changed before no-link replace".to_string(),
            });
        }
        self.posix_replace_opened_file_if_version_no_links(&opened, bytes, options)
    }

    /// Replace the exact file/version captured by a no-link open without
    /// resolving its pathname again.
    pub fn posix_replace_opened_file_if_version_no_links(
        &mut self,
        opened: &DfsStat,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        match &opened.kind {
            DfsObjectKind::Directory => {
                return Err(DfsPosixError::IsDirectory {
                    path: opened.path.clone(),
                });
            }
            DfsObjectKind::Symlink | DfsObjectKind::Hardlink | DfsObjectKind::Mount => {
                return Err(DfsPosixError::InvalidPath {
                    path: opened.path.clone(),
                });
            }
            DfsObjectKind::Tombstone => {
                return Err(DfsPosixError::NotFound {
                    path: opened.path.clone(),
                });
            }
            DfsObjectKind::File | DfsObjectKind::Artifact => {}
        }
        self.posix_write_file_bytes_checked_parent_once(
            &opened.path,
            bytes,
            options,
            true,
            false,
            true,
            None,
            false,
            false,
            Some((&opened.object_id, &opened.version)),
            false,
        )
    }

    /// Exclusive creation after resolving the final symlink. This models
    /// `open(O_CREAT)` on a dangling symlink: the link remains intact and its
    /// missing target is created, while a target that already exists wins the
    /// race without being truncated.
    pub fn posix_create_file_following_symlink_with_attrs(
        &mut self,
        path: &str,
        options: DfsWriteOptions,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        self.posix_write_file_bytes_checked_parent(
            path,
            &[],
            options,
            true,
            true,
            true,
            Some(attrs),
            true,
            true,
        )
    }

    pub fn posix_write_file_with_known_parent(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        self.posix_write_file_bytes_checked_parent(
            path, bytes, options, false, false, true, None, false, false,
        )
    }

    pub fn posix_write_file_with_known_parent_deferred_local_replay(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        self.posix_write_file_bytes_checked_parent(
            path, bytes, options, false, false, false, None, false, false,
        )
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
            let (object, expected_base_version, touch_parent_on_create) = match self
                .lookup_sifs_path(&path)?
            {
                Some(object) if object.kind == DfsObjectKind::Directory => {
                    return Err(DfsPosixError::IsDirectory { path });
                }
                Some(object) if object.kind == DfsObjectKind::Symlink => {
                    return self.posix_write_files_local_eventual_fallback(&files);
                }
                Some(object) if object.kind == DfsObjectKind::Hardlink => {
                    let object = self.resolve_hardlink_target(&object)?;
                    let base = Some(object.version.clone());
                    (object, base, false)
                }
                Some(object) if object.kind == DfsObjectKind::Tombstone => {
                    let object =
                        recreated_posix_metadata_object(&self.node_id, &path, DfsObjectKind::File);
                    link_count_hint = Some(1);
                    (object, None, true)
                }
                Some(object) => {
                    let base = Some(object.version.clone());
                    (object, base, false)
                }
                None => {
                    link_count_hint = Some(1);
                    (
                        new_posix_metadata_object(&self.node_id, &path, DfsObjectKind::File),
                        None,
                        true,
                    )
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
            let metadata_shard = match metadata_shard_snapshot.as_deref() {
                Some(shards) => {
                    self.select_sifs_metadata_shard_for_path_from_snapshot(&path, shards)?
                }
                None => None,
            };
            let parent_touch = if touch_parent_on_create {
                Some(PosixBulkParentTouch {
                    path: resolved_parent.clone(),
                    metadata_shard: match metadata_shard_snapshot.as_deref() {
                        Some(shards) => self.select_sifs_metadata_shard_for_path_from_snapshot(
                            &resolved_parent,
                            shards,
                        )?,
                        None => None,
                    },
                })
            } else {
                None
            };
            let target_version = next_version(&object.version);
            prepared.push(PosixBulkLocalWrite {
                path,
                object,
                target_version,
                expected_base_version,
                bytes: *bytes,
                metadata_shard,
                parent_touch,
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
        let mut seen_paths = BTreeSet::new();
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
            if !seen_paths.insert(path.clone())
                || self
                    .lookup_sifs_path(&path)?
                    .is_some_and(|object| object.kind != DfsObjectKind::Tombstone)
            {
                return Err(DfsPosixError::AlreadyExists { path });
            }
            let object = new_posix_metadata_object(&self.node_id, &path, DfsObjectKind::File);
            if !seen_objects.insert(object.object_id.clone()) {
                return Err(DfsPosixError::AlreadyExists { path });
            }
            let metadata_shard = match metadata_shard_snapshot.as_deref() {
                Some(shards) => {
                    self.select_sifs_metadata_shard_for_path_from_snapshot(&path, shards)?
                }
                None => None,
            };
            let parent_touch = Some(PosixBulkParentTouch {
                path: resolved_parent.clone(),
                metadata_shard: match metadata_shard_snapshot.as_deref() {
                    Some(shards) => self.select_sifs_metadata_shard_for_path_from_snapshot(
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
                expected_base_version: None,
                bytes,
                metadata_shard,
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
        type MetadataAuthorityKey = Option<(NodeId, String, u64)>;
        let authority_key = |shard: Option<&DfsMetadataShardConfig>| {
            shard.map(|shard| {
                (
                    shard.primary_node_id.clone(),
                    shard.shard_id.clone(),
                    shard.epoch,
                )
            })
        };
        let mut metadata_shards =
            BTreeMap::<MetadataAuthorityKey, Option<DfsMetadataShardConfig>>::new();
        let mut objects_by_metadata_authority =
            BTreeMap::<MetadataAuthorityKey, Vec<DfsObjectMetadata>>::new();
        for (write, object) in prepared.iter().zip(committed_objects.iter().cloned()) {
            let key = authority_key(write.metadata_shard.as_ref());
            metadata_shards
                .entry(key.clone())
                .or_insert_with(|| write.metadata_shard.clone());
            objects_by_metadata_authority
                .entry(key)
                .or_default()
                .push(object);
        }
        let mut touched_parent_objects =
            BTreeMap::<(MetadataAuthorityKey, DfsObjectId), DfsObjectMetadata>::new();
        let mut resolved_parent_objects =
            BTreeMap::<(MetadataAuthorityKey, String), DfsObjectMetadata>::new();
        for write in &prepared {
            let Some(parent_touch) = write.parent_touch.as_ref() else {
                continue;
            };
            let metadata_authority = authority_key(parent_touch.metadata_shard.as_ref());
            metadata_shards
                .entry(metadata_authority.clone())
                .or_insert_with(|| parent_touch.metadata_shard.clone());
            let parent_cache_key = (metadata_authority.clone(), parent_touch.path.clone());
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
            let key = (metadata_authority, parent.object_id.clone());
            touched_parent_objects.entry(key).or_insert_with(|| {
                let mut parent = parent.clone();
                parent.version = next_version(&parent.version);
                parent.modified_at_unix_nanos = write_time;
                parent.changed_at_unix_nanos = write_time;
                parent.last_modified_by_event_id = Some(EventId::generate());
                parent
            });
        }
        for ((metadata_authority, _), object) in touched_parent_objects {
            objects_by_metadata_authority
                .entry(metadata_authority)
                .or_default()
                .push(object);
        }
        let all_metadata_paths = objects_by_metadata_authority
            .values()
            .flatten()
            .map(|object| object.path.clone())
            .collect::<BTreeSet<_>>();
        let mut committed_metadata_paths = BTreeSet::new();
        for (metadata_authority, objects) in objects_by_metadata_authority {
            let group_metadata_paths = objects
                .iter()
                .map(|object| object.path.clone())
                .collect::<BTreeSet<_>>();
            let selected_shard = metadata_shards.remove(&metadata_authority).unwrap_or(None);
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
                advisory_lock_states: Vec::new(),
                created_at: time::OffsetDateTime::now_utc(),
            };
            let authority_node_id = selected_shard
                .as_ref()
                .map(|shard| shard.primary_node_id.clone())
                .unwrap_or_else(|| self.node_id.clone());
            let writes = prepared
                .iter()
                .zip(committed_objects.iter())
                .filter(|(write, _)| {
                    authority_key(write.metadata_shard.as_ref()) == metadata_authority
                })
                .map(|(write, object)| DfsLocalEventualBatchWriteProof {
                    expected_base_version: write.expected_base_version.clone(),
                    receipt: DfsReplicaReceiptRecord {
                        receipt: DfsWriteReceipt {
                            file_system_id: object.file_system_id.clone(),
                            object_id: object.object_id.clone(),
                            committed_version: object.version.clone(),
                            owner_node_id: authority_node_id.clone(),
                            event_id: object
                                .last_modified_by_event_id
                                .clone()
                                .expect("bulk committed objects carry a write event"),
                        },
                        replica_node_id: self.node_id.clone(),
                        content_hash: object.content_hash.clone(),
                        received_at: time::OffsetDateTime::now_utc(),
                    },
                })
                .collect::<Vec<_>>();
            let result = if authority_node_id != self.node_id {
                self.route_namespace_operation_without_local_replay(
                    authority_node_id,
                    DfsNamespaceOperation::CommitLocalEventualBatch {
                        transaction,
                        writes,
                    },
                )
            } else {
                let requester_node_id = self.node_id.clone();
                self.apply_local_eventual_batch_commit(
                    &requester_node_id,
                    transaction,
                    &writes,
                    selected_shard,
                )
                .map(drop)
                .map_err(DfsPosixError::from)
            };
            if let Err(source) = result {
                let unattempted_metadata_paths = all_metadata_paths
                    .difference(&committed_metadata_paths)
                    .filter(|path| !group_metadata_paths.contains(*path))
                    .cloned()
                    .collect();
                return Err(DfsPosixError::PartialBulkCommit {
                    committed_metadata_paths: committed_metadata_paths.iter().cloned().collect(),
                    indeterminate_metadata_paths: group_metadata_paths.iter().cloned().collect(),
                    unattempted_metadata_paths,
                    source: Box::new(source),
                });
            }
            committed_metadata_paths.extend(group_metadata_paths);
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
        self.posix_write_file_from_reader_checked_parent(path, reader, options, true, true, None)
    }

    pub fn posix_write_file_from_reader_with_create_attrs<R: Read>(
        &mut self,
        path: &str,
        reader: R,
        options: DfsWriteOptions,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        self.posix_write_file_from_reader_checked_parent(
            path,
            reader,
            options,
            true,
            true,
            Some(attrs),
        )
    }

    fn posix_write_file_bytes_checked_parent(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
        check_parent: bool,
        touch_parent_on_create: bool,
        replay_response_journal: bool,
        create_attrs: Option<DfsPosixCreateAttrs>,
        exclusive_create: bool,
        follow_final_for_exclusive_create: bool,
    ) -> Result<DfsStat> {
        let started_at = Instant::now();
        let requested_deadline = started_at + POSIX_WAIT_FOR_COMMIT_TIMEOUT;
        let observed_previous_deadline = self.transport.operation_deadline();
        let replaced_previous_deadline = self
            .transport
            .set_operation_deadline(Some(requested_deadline));
        let previous_deadline = replaced_previous_deadline.or(observed_previous_deadline);
        let write_deadline = previous_deadline
            .map(|deadline| deadline.min(requested_deadline))
            .unwrap_or(requested_deadline);
        if write_deadline != requested_deadline {
            self.transport.set_operation_deadline(Some(write_deadline));
        }
        let result = (|| {
            loop {
                match self.posix_write_file_bytes_checked_parent_once(
                    path,
                    bytes,
                    options.clone(),
                    check_parent,
                    touch_parent_on_create,
                    replay_response_journal,
                    create_attrs,
                    exclusive_create,
                    follow_final_for_exclusive_create,
                    None,
                    true,
                ) {
                    Ok(stat) => return Ok(stat),
                    Err(error) if posix_error_is_retryable_write_serialization(&error) => {
                        self.check_cancelled("POSIX blind-write serialization retry")?;
                        let now = Instant::now();
                        if now >= write_deadline {
                            return Err(error);
                        }
                        std::thread::sleep(
                            POSIX_WRITE_SERIALIZATION_RETRY_IDLE_SLEEP
                                .min(write_deadline.saturating_duration_since(now)),
                        );
                        self.hydrate_from_metadata_backend()?;
                    }
                    Err(error) => return Err(error),
                }
            }
        })();
        self.transport.set_operation_deadline(previous_deadline);
        result
    }

    fn posix_write_file_bytes_checked_parent_once(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
        check_parent: bool,
        touch_parent_on_create: bool,
        replay_response_journal: bool,
        create_attrs: Option<DfsPosixCreateAttrs>,
        exclusive_create: bool,
        follow_final_for_exclusive_create: bool,
        expected_existing: Option<(&DfsObjectId, &DfsVersion)>,
        resolve_links: bool,
    ) -> Result<DfsStat> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = if resolve_links {
            self.resolve_posix_path_distributed(
                &path,
                !exclusive_create || follow_final_for_exclusive_create,
            )?
        } else {
            path
        };
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
        let mut object = match self.lookup_sifs_path(&path)? {
            Some(object) if exclusive_create && object.kind != DfsObjectKind::Tombstone => {
                return Err(DfsPosixError::AlreadyExists { path });
            }
            Some(object)
                if !resolve_links
                    && matches!(
                        &object.kind,
                        DfsObjectKind::Symlink | DfsObjectKind::Hardlink
                    ) =>
            {
                return Err(DfsPosixError::InvalidPath { path });
            }
            Some(object) if object.kind == DfsObjectKind::Directory => {
                return Err(DfsPosixError::IsDirectory { path });
            }
            Some(object) if object.kind == DfsObjectKind::Hardlink => {
                self.resolve_hardlink_target(&object)?
            }
            Some(object)
                if object.kind == DfsObjectKind::Tombstone && expected_existing.is_some() =>
            {
                return Err(DfsPosixError::NotFound { path });
            }
            Some(object) if object.kind == DfsObjectKind::Tombstone => {
                let mut object =
                    recreated_posix_metadata_object(&self.node_id, &path, DfsObjectKind::File);
                let attrs = create_attrs.unwrap_or_else(DfsPosixCreateAttrs::file_default);
                let attrs = self.posix_create_attrs_for_object_authority(&object, attrs, false)?;
                apply_posix_create_attrs(&mut object, attrs);
                self.add_transient_object_metadata(object.clone())?;
                transient_create = true;
                object
            }
            Some(object) => object,
            None if expected_existing.is_some() => {
                return Err(DfsPosixError::NotFound { path });
            }
            None => {
                let mut object =
                    new_posix_metadata_object(&self.node_id, &path, DfsObjectKind::File);
                let attrs = create_attrs.unwrap_or_else(DfsPosixCreateAttrs::file_default);
                let attrs = self.posix_create_attrs_for_object_authority(&object, attrs, false)?;
                apply_posix_create_attrs(&mut object, attrs);
                self.add_transient_object_metadata(object.clone())?;
                transient_create = true;
                object
            }
        };
        self.apply_managed_policy_to_new_object(&mut object, options.replica_candidates.len())?;
        if let Some((expected_object_id, expected_version)) = expected_existing {
            if &object.object_id != expected_object_id {
                return Err(DfsPosixError::Conflict {
                    path,
                    reason: format!(
                        "file identity changed: expected {expected_object_id}, found {}",
                        object.object_id
                    ),
                });
            }
            if &object.version != expected_version {
                return Err(DfsPosixError::Conflict {
                    path,
                    reason: format!(
                        "file changed since it was opened: expected version {expected_version}, found {}",
                        object.version
                    ),
                });
            }
        }
        let target_version = next_version(&object.version);
        let candidates = self.posix_replica_candidates_for_write(&object, &options)?;
        let committed_locations = candidates.clone();
        let remote_metadata_primary = match self.select_metadata_shard_for_object(&object) {
            Ok(Some(shard)) => shard.primary_node_id != self.node_id,
            Ok(None) => false,
            Err(error) => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                return Err(error.into());
            }
        };
        let detach_request = !options.wait_for_commit
            && !options.foreground_replica_repair
            && !remote_metadata_primary;
        let materialized_remote_primary_write = remote_metadata_primary;
        let content_started = Instant::now();
        let manifest_result = self.content.put_object_version_blob_only(
            object.object_id.as_str(),
            target_version.as_str(),
            bytes,
        );
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
        let outcome = if materialized_remote_primary_write {
            self.write_distributed_eventual_with_base_and_parent_touch_and_repair_mode(
                request,
                Some(object.version.clone()),
                bytes.to_vec(),
                candidates,
                options.policy,
                touch_parent_on_create,
                exclusive_create,
                options.foreground_replica_repair,
                detach_request,
                replay_response_journal,
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
            "posix-byte-write node={} path={} object={} version={} bytes={} materialized_routed={} content_ms={} distributed_ms={} total_ms={}",
            self.node_id,
            trace_path,
            object.object_id,
            target_version,
            trace_bytes,
            materialized_remote_primary_write,
            content_elapsed.as_millis(),
            distributed_elapsed.as_millis(),
            trace_started.elapsed().as_millis()
        ));
        match outcome {
            NetworkedDfsWriteOutcome::Committed {
                intent_id,
                version,
                content_hash,
                repair_jobs,
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
                    && !self
                        .coordinator
                        .intent(&intent_id)
                        .is_some_and(|intent| intent.touch_parent_on_create)
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
                    if transient_create && remote_metadata_primary {
                        let authoritative = self.stat_target_for_path(&path)?;
                        return self.stat_object_for_path(&authoritative, &path);
                    }
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
                    && !self
                        .coordinator
                        .intent(&intent_id)
                        .is_some_and(|intent| intent.touch_parent_on_create)
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
        create_attrs: Option<DfsPosixCreateAttrs>,
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
        let mut object = match self.lookup_sifs_path(&path)? {
            Some(object) if object.kind == DfsObjectKind::Directory => {
                return Err(DfsPosixError::IsDirectory { path });
            }
            Some(object) if object.kind == DfsObjectKind::Hardlink => {
                self.resolve_hardlink_target(&object)?
            }
            Some(object) if object.kind == DfsObjectKind::Tombstone => {
                let mut object =
                    recreated_posix_metadata_object(&self.node_id, &path, DfsObjectKind::File);
                let attrs = create_attrs.unwrap_or_else(DfsPosixCreateAttrs::file_default);
                let attrs = self.posix_create_attrs_for_object_authority(&object, attrs, false)?;
                apply_posix_create_attrs(&mut object, attrs);
                self.add_transient_object_metadata(object.clone())?;
                transient_create = true;
                object
            }
            Some(object) => object,
            None => {
                let mut object =
                    new_posix_metadata_object(&self.node_id, &path, DfsObjectKind::File);
                let attrs = create_attrs.unwrap_or_else(DfsPosixCreateAttrs::file_default);
                let attrs = self.posix_create_attrs_for_object_authority(&object, attrs, false)?;
                apply_posix_create_attrs(&mut object, attrs);
                self.add_transient_object_metadata(object.clone())?;
                transient_create = true;
                object
            }
        };
        self.apply_managed_policy_to_new_object(&mut object, options.replica_candidates.len())?;
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
                intent_id,
                version,
                content_hash,
                repair_jobs,
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
                    && !self
                        .coordinator
                        .intent(&intent_id)
                        .is_some_and(|intent| intent.touch_parent_on_create)
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
                    if transient_create {
                        let authoritative = self.stat_target_for_path(&path)?;
                        return self.stat_object_for_path(&authoritative, &path);
                    }
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
        self.posix_write_file_at_expected_object(path, None, offset, bytes, options)
    }

    /// Write through a stable opened-object identity. The expected id is
    /// checked after path resolution and before any intent or content side
    /// effect, so unlink/recreate cannot redirect an old descriptor's write
    /// into the replacement file.
    pub fn posix_write_file_at_for_object(
        &mut self,
        object_id: &DfsObjectId,
        offset: u64,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let object = self
            .current_read_object_metadata(object_id)
            .map_err(DfsPosixError::from)?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path: object.path });
        }
        self.posix_write_file_at_expected_object(
            &object.path,
            Some(object_id),
            offset,
            bytes,
            options,
        )
    }

    fn posix_write_file_at_expected_object(
        &mut self,
        path: &str,
        expected_object_id: Option<&DfsObjectId>,
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
        if expected_object_id.is_some_and(|expected| expected != &object.object_id) {
            return Err(DfsPosixError::NotFound { path });
        }
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
        self.posix_truncate_file_expected_object(path, None, len, options)
    }

    /// Truncate through a stable opened-object identity, rejecting a path
    /// replacement before any write intent is created.
    pub fn posix_truncate_file_for_object(
        &mut self,
        object_id: &DfsObjectId,
        len: u64,
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let object = self
            .current_read_object_metadata(object_id)
            .map_err(DfsPosixError::from)?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path: object.path });
        }
        self.posix_truncate_file_expected_object(&object.path, Some(object_id), len, options)
    }

    fn posix_truncate_file_expected_object(
        &mut self,
        path: &str,
        expected_object_id: Option<&DfsObjectId>,
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
        if expected_object_id.is_some_and(|expected| expected != &object.object_id) {
            return Err(DfsPosixError::NotFound { path });
        }
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

#[cfg(test)]
mod replica_candidate_tests {
    use super::*;

    fn distributed_posix_error(message: &str) -> DfsPosixError {
        DfsPosixError::Dfs(DfsError::Distributed {
            message: message.to_string(),
        })
    }

    #[test]
    fn blind_write_serialization_retry_classifies_every_unaccepted_write_race() {
        for message in [
            "stale DFS write base for object",
            "base version conflict for object",
            "duplicate DFS write intent for object conflicts with requested target",
            "pending DFS write intent intent-a for object conflicts with requested target",
            "distributed DFS error: pending DFS write intent intent-a for object conflicts",
        ] {
            assert!(
                posix_error_is_retryable_write_serialization(&distributed_posix_error(message)),
                "expected retryable blind-write serialization error: {message}"
            );
        }
    }

    #[test]
    fn accepted_pending_write_is_not_restarted_as_a_blind_overwrite() {
        assert!(!posix_error_is_retryable_write_serialization(
            &DfsPosixError::PendingWrite {
                intent_id: "accepted-intent".to_string(),
                durable_receipts: 1,
                required_receipts: 2,
            }
        ));
        assert!(!posix_error_is_retryable_write_serialization(
            &distributed_posix_error("invalid DFS replication policy")
        ));
        assert!(!posix_error_is_retryable_write_serialization(
            &distributed_posix_error("duplicate DFS write intent id intent-a")
        ));
    }

    #[test]
    fn posix_write_selects_local_plus_policy_sized_remote_set() {
        let local = NodeId::from_legacy_named("node_posix_candidate_local").expect("local");
        let remote_a =
            NodeId::from_legacy_named("node_posix_candidate_remote_a").expect("remote A");
        let remote_b =
            NodeId::from_legacy_named("node_posix_candidate_remote_b").expect("remote B");

        let selected = select_posix_write_replica_candidates(
            &local,
            &[remote_a.clone(), local.clone(), remote_b],
            2,
        );

        assert_eq!(selected, vec![local, remote_a]);
    }
}
