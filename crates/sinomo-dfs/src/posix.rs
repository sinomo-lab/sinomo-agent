//! POSIX-like DFS namespace facade.
//!
//! The facade provides path-based operations without Unix user/group permission
//! semantics. It is intended for services such as static web servers and
//! rollout and higher-level automation tools that need stable mkdir/list/stat/read/write
//! behavior over SIFS while the lower DFS layers handle placement, receipts,
//! repair, and metadata journaling.

use crate::service_write::{
    DfsDistributedWriteOptions, DfsWriteCreation, DfsWriteDispatch, DfsWriteRepair,
};
mod attrs;
#[cfg(test)]
mod attributed_mkdir_native_tests;
mod cross_shard;
#[cfg(test)]
mod directory_identity_native_tests;
#[cfg(test)]
pub(crate) use cross_shard::{hold_parent_apply_commit, parent_apply_commit_is_held, ParentCountTestPoint, install_parent_count_test_hook, install_hardlink_alias_after_commit_test_hook, install_hardlink_count_after_commit_test_hook};
#[cfg(test)]
pub(crate) use cross_shard::fail_directory_move_scan_of;
#[cfg(any(test, feature = "test-support"))]
pub use cross_shard::DfsDirectoryMoveScanRefusal;
mod job_events;
mod jobs;
mod links;
pub(crate) mod locks;
mod metadata;
pub(crate) use metadata::updated_namespace_link_count;
mod namespace_ops;
pub(crate) mod owner_write;
mod query;
mod read;
mod range_write;
mod routing;
mod stat;
pub(crate) mod sync;

pub(crate) use routing::directory_child_authority_shards;

#[cfg(test)]
pub(crate) use metadata::{
    posix_directory_link_count_calls, posix_metadata_raw_lookup_count,
    posix_transient_child_candidate_visit_count, reset_posix_directory_link_count_calls,
    reset_posix_metadata_raw_lookup_count, reset_posix_transient_child_candidate_visit_count,
};

#[cfg(test)]
thread_local! {
    static POSIX_BULK_PREP_DURABLE_TABLE_PROBES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static POSIX_BULK_PREP_TABLE_SNAPSHOT_CALLS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static POSIX_BULK_PARENT_TOUCH_CONSTRUCTIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_posix_bulk_prep_durable_table_probes() {
    POSIX_BULK_PREP_DURABLE_TABLE_PROBES.set(0);
    POSIX_BULK_PREP_TABLE_SNAPSHOT_CALLS.set(0);
}

#[cfg(test)]
pub(crate) fn posix_bulk_prep_durable_table_probes() -> usize {
    POSIX_BULK_PREP_DURABLE_TABLE_PROBES.get()
}

#[cfg(test)]
pub(crate) fn posix_bulk_prep_table_snapshot_calls() -> usize {
    POSIX_BULK_PREP_TABLE_SNAPSHOT_CALLS.get()
}

#[cfg(test)]
pub(crate) fn posix_bulk_parent_touch_constructions() -> usize {
    POSIX_BULK_PARENT_TOUCH_CONSTRUCTIONS.get()
}

#[cfg(test)]
pub(crate) fn reset_posix_bulk_parent_touch_constructions() {
    POSIX_BULK_PARENT_TOUCH_CONSTRUCTIONS.set(0);
}

use crate::dfs_now_unix_nanos as now_unix_nanos;
use crate::intent::{stable_create_object_id, stable_write_target_version};
use crate::metadata_records::object_version_record;
use crate::namespace::{
    namespace_transaction_id_for_batch, namespace_transaction_id_for_object, next_version,
};
use crate::posix_model::{DfsPosixCreateAttrs, DfsPosixError, DfsStat, DfsWriteOptions, Result};
use crate::posix_object::{
    apply_posix_create_attrs, inherit_posix_create_attrs, new_posix_metadata_object,
    recreated_posix_metadata_object, set_object_create_timestamps,
};
use crate::posix_path::{
    join_posix_path, normalize_posix_path, normalize_posix_path_with_trailing_directory,
    parent_posix_path, posix_path_is_at_or_under_prefix, resolve_posix_symlink_target,
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
use std::rc::Rc;
use std::time::{Duration, Instant};

struct RangedWriteOverlayReader<R> {
    current: Vec<u8>,
    patch: R,
    write_offset: u64,
    prefix_end: usize,
    prefix_offset: usize,
    gap_remaining: u64,
    patch_size_bytes: u64,
    patch_complete: bool,
    suffix_offset: usize,
}

impl<R> RangedWriteOverlayReader<R> {
    fn new(current: Vec<u8>, write_offset: u64, patch: R) -> Self {
        let current_size = current.len() as u64;
        let prefix_end = usize::try_from(write_offset.min(current_size))
            .expect("bounded by the resident current buffer length");
        Self {
            current,
            patch,
            write_offset,
            prefix_end,
            prefix_offset: 0,
            gap_remaining: write_offset.saturating_sub(current_size),
            patch_size_bytes: 0,
            patch_complete: false,
            suffix_offset: prefix_end,
        }
    }

    fn result_size_bytes(&self) -> Option<u64> {
        self.patch_complete.then(|| {
            (self.current.len() as u64).max(
                self.write_offset
                    .checked_add(self.patch_size_bytes)
                    .expect("overflow is rejected while reading the patch"),
            )
        })
    }
}

impl<R: Read> Read for RangedWriteOverlayReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.prefix_offset < self.prefix_end {
            let len = buffer.len().min(self.prefix_end - self.prefix_offset);
            buffer[..len].copy_from_slice(
                &self.current[self.prefix_offset..self.prefix_offset.saturating_add(len)],
            );
            self.prefix_offset += len;
            return Ok(len);
        }
        if self.gap_remaining > 0 {
            let len = usize::try_from(self.gap_remaining.min(buffer.len() as u64))
                .expect("bounded by the destination buffer length");
            buffer[..len].fill(0);
            self.gap_remaining -= len as u64;
            return Ok(len);
        }
        if !self.patch_complete {
            let len = self.patch.read(buffer)?;
            if len > 0 {
                self.patch_size_bytes =
                    self.patch_size_bytes
                        .checked_add(len as u64)
                        .ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "ranged write patch length exceeds u64",
                            )
                        })?;
                return Ok(len);
            }
            self.patch_complete = true;
            let patch_end = self
                .write_offset
                .checked_add(self.patch_size_bytes)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "ranged write result length exceeds u64",
                    )
                })?;
            self.suffix_offset = usize::try_from(patch_end.min(self.current.len() as u64))
                .expect("bounded by the resident current buffer length");
        }
        if self.suffix_offset < self.current.len() {
            let len = buffer.len().min(self.current.len() - self.suffix_offset);
            buffer[..len].copy_from_slice(
                &self.current[self.suffix_offset..self.suffix_offset.saturating_add(len)],
            );
            self.suffix_offset += len;
            return Ok(len);
        }
        Ok(0)
    }
}

fn touch_directory_for_batched_child(directory: &mut DfsObjectMetadata, advance_version: bool) {
    let touch_time = now_unix_nanos()
        .max(directory.modified_at_unix_nanos.saturating_add(1))
        .max(directory.changed_at_unix_nanos.saturating_add(1));
    directory.modified_at_unix_nanos = touch_time;
    directory.changed_at_unix_nanos = touch_time;
    if advance_version {
        directory.version = next_version(&directory.version);
    }
    directory.last_modified_by_event_id = Some(EventId::generate());
}

const POSIX_WAIT_FOR_COMMIT_READY_DRAIN_LIMIT: usize = 8;
const POSIX_WAIT_FOR_COMMIT_STAGE_MAX_TARGETS_PER_PASS: usize = 1;
/// A waiting write re-drives its own unfinished stages on this timer, doubling
/// to the network layer's liveness interval. The timer only re-sends: the wait
/// itself ends on the write's outcome, the caller's deadline or cancellation.
const POSIX_WAIT_FOR_COMMIT_STAGE_RESEND_FIRST: Duration = Duration::from_millis(250);
const POSIX_WAIT_FOR_COMMIT_STAGE_RESEND_MAX: Duration =
    sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
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
const POSIX_BULK_PARENT_RETRY_ATTEMPTS: usize = 3;
/// Follow-up sync rounds a routed-operation wrapper performs when bounded
/// response replay has not yet surfaced the mutation locally.
pub(crate) const POSIX_ROUTED_SYNC_RETRY_ROUNDS: usize = 4;
/// Whole-file POSIX writes are blind overwrites, not user-supplied CAS writes.
/// If another writer is already committing, the write is re-sent against the
/// new current version once that intent has settled. Nothing announces a
/// remote intent settling to this caller, so the re-send runs on a timer,
/// doubling to the network layer's liveness interval. Only the caller's
/// deadline or cancellation ends the retries.
const POSIX_WRITE_SERIALIZATION_RESEND_FIRST: Duration = Duration::from_millis(10);
const POSIX_WRITE_SERIALIZATION_RESEND_MAX: Duration =
    sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
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
    parent_touch: Option<Rc<PosixBulkParentTouch>>,
    link_count_hint: Option<u64>,
}

#[derive(Clone, Debug, Default)]
struct PosixBulkAuthoritativeLookup {
    exact_objects_by_path: BTreeMap<String, DfsObjectMetadata>,
    /// Authority-derived object-id-to-path hints. These are never consumed as
    /// target metadata until an exact lookup at the hinted path's current
    /// shard authority returns the same object id.
    hardlink_target_hints: BTreeMap<DfsObjectId, DfsObjectMetadata>,
}

#[derive(Clone, Debug, Default)]
struct PosixBulkAuthoritativePrefetch {
    objects_by_path: BTreeMap<String, DfsObjectMetadata>,
    /// Lexical link path to the exact-shard-revalidated target path/object.
    resolved_link_targets: BTreeMap<String, (String, DfsObjectMetadata)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PosixBulkParentTouch {
    path: String,
    object: DfsObjectMetadata,
    metadata_shard: Option<DfsMetadataShardConfig>,
}

impl PosixBulkParentTouch {
    fn shared(
        path: String,
        object: DfsObjectMetadata,
        metadata_shard: Option<DfsMetadataShardConfig>,
    ) -> Rc<Self> {
        #[cfg(test)]
        POSIX_BULK_PARENT_TOUCH_CONSTRUCTIONS.set(
            POSIX_BULK_PARENT_TOUCH_CONSTRUCTIONS
                .get()
                .saturating_add(1),
        );
        Rc::new(Self {
            path,
            object,
            metadata_shard,
        })
    }
}

pub fn posix_error_is_retryable_write_serialization(error: &DfsPosixError) -> bool {
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

fn posix_error_is_concurrent_create_winner(error: &DfsPosixError) -> bool {
    matches!(
        error,
        DfsPosixError::AlreadyExists { .. } | DfsPosixError::Dfs(DfsError::AlreadyExists { .. })
    )
}

fn posix_error_is_routed_namespace_unavailable(error: &DfsPosixError) -> bool {
    matches!(error, DfsPosixError::Dfs(DfsError::Network { .. }))
        || matches!(
            error,
            DfsPosixError::Dfs(DfsError::NetworkDelivery {
                delivery_state: sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                ..
            })
        )
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

fn no_link_write_condition_error(error: DfsPosixError) -> DfsPosixError {
    match error {
        DfsPosixError::Dfs(DfsError::WritePreconditionFailed { failure }) => DfsPosixError::Conflict {
            path: failure.path().to_string(), reason: failure.reason(),
        },
        other => other,
    }
}

/// What a committed write hands back before its stat is reported. A caller
/// without a metadata store holds no committed record, so its stat is a
/// separate read at the authorities which a caller discarding it never pays.
enum PosixWriteCommit {
    Stat(DfsStat),
    AuthorityHead {
        file_system_id: crate::DfsFileSystemId,
        object_id: DfsObjectId,
        path: String,
    },
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Run one POSIX commit. A commit adds no deadline of its own: the
    /// operation deadline its caller installed, if any, is the only bound in
    /// time, and it travels with every request the commit sends.
    fn with_posix_commit_operation_deadline<R>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<R>,
    ) -> Result<R> {
        operation(self)
    }

    /// Retry the waiting write's own unfinished stages, bounded when the
    /// caller set a deadline. A failed retry is reported among the events,
    /// never as the waiting write's own failure.
    fn retry_own_stages_reporting_failure(
        &mut self,
        intent_id: &str,
        bounded: bool,
        events: &mut Vec<crate::NetworkedDfsServiceEvent>,
    ) {
        let max_targets = if bounded {
            POSIX_WAIT_FOR_COMMIT_STAGE_MAX_TARGETS_PER_PASS
        } else {
            usize::MAX
        };
        let intent_ids = std::collections::BTreeSet::from([intent_id.to_owned()]);
        if let Err(error) = self.retry_pending_stages_for_intents(&intent_ids, max_targets) {
            events.push(crate::NetworkedDfsServiceEvent::RepairRetryFailed {
                reason: error.to_string(),
            });
            self.last_events = events.clone();
        }
    }

    pub(crate) fn settle_ready_write_outcome(
        &mut self,
        outcome: NetworkedDfsWriteOutcome,
        foreground_replica_repair: bool,
    ) -> std::result::Result<NetworkedDfsWriteOutcome, DfsError> {
        let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = &outcome else {
            return Ok(outcome);
        };
        let mut intent_id = intent_id.clone();
        // Only the caller's deadline bounds the settle in time. Without one it
        // ends on the write's outcome, a definite failure or cancellation.
        let caller_deadline = self.transport.operation_deadline();
        let deadline_passed = || caller_deadline.is_some_and(|deadline| Instant::now() >= deadline);
        // Cancellation wakes the wait below at once.
        let incoming_waker = self.transport.incoming_wait_waker();
        let cancellation_waker: crate::service_state::DfsCancellationWaker =
            std::sync::Arc::new(move || {
                if let Some(waker) = &incoming_waker {
                    waker();
                }
            });
        let _cancellation_wake = self.register_cancellation_waker(&cancellation_waker);
        let mut stage_resend = POSIX_WAIT_FOR_COMMIT_STAGE_RESEND_FIRST;
        let mut next_stage_resend = Instant::now() + stage_resend;
        let mut outcome = outcome;
        let mut ready_drains = 0;
        loop {
            if deadline_passed() {
                self.compact_pending_routed_write(&intent_id);
                return Ok(outcome);
            }
            if let Err(error) = self.check_cancelled("write settle") {
                self.compact_pending_routed_write(&intent_id);
                return Err(error);
            }
            let mut events = match self.process_incoming() {
                Ok(events) => events,
                Err(error) => {
                    self.compact_pending_routed_write(&intent_id);
                    return Err(error);
                }
            };
            outcome = match self.take_write_outcome(&intent_id) {
                Ok(outcome) => outcome,
                // An unknown outcome keeps the compact identity that
                // `take_write_outcome` retained for a late response.
                Err(error) if error.is_commit_outcome_unknown() => return Err(error),
                Err(error) => {
                    self.abandon_pending_routed_write(&intent_id);
                    return Err(error);
                }
            };
            if let NetworkedDfsWriteOutcome::Pending {
                intent_id: next_intent_id,
                ..
            } = &outcome
            {
                intent_id = next_intent_id.clone();
            }
            if !matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }) {
                // Replicas left below N are needs in the shard's repair index;
                // the primary's executor sends them. The write drives none.
                return Ok(outcome);
            }
            // Before W the write is not committed and has no repair need: it
            // re-sends only its own stages, as part of the write protocol, on
            // the re-send timer.
            if foreground_replica_repair
                && !deadline_passed()
                && Instant::now() >= next_stage_resend
            {
                self.retry_own_stages_reporting_failure(
                    &intent_id,
                    caller_deadline.is_some(),
                    &mut events,
                );
                stage_resend = stage_resend
                    .saturating_mul(2)
                    .min(POSIX_WAIT_FOR_COMMIT_STAGE_RESEND_MAX);
                next_stage_resend = Instant::now() + stage_resend;
            }
            // Re-scheduled repairs alone are not progress: on a degraded cluster
            // every retry fails fast and reschedules, and treating that churn as
            // activity turned this wait into a hot spin (a full core per pending
            // write). Only incoming events skip the pacing wait — which wakes
            // early on arrival, so healthy-path settle latency is unchanged.
            if events.is_empty() || ready_drains >= POSIX_WAIT_FOR_COMMIT_READY_DRAIN_LIMIT {
                if deadline_passed() {
                    self.compact_pending_routed_write(&intent_id);
                    return Ok(outcome);
                }
                // An arrival or cancellation wakes this wait. Its length is
                // only when the next re-send is due, or the caller's deadline.
                let now = Instant::now();
                let mut wait = if foreground_replica_repair {
                    next_stage_resend.saturating_duration_since(now)
                } else {
                    POSIX_WAIT_FOR_COMMIT_STAGE_RESEND_MAX
                };
                if let Some(deadline) = caller_deadline {
                    wait = wait.min(deadline.saturating_duration_since(now));
                }
                if let Err(error) = self.transport.wait_for_incoming(wait) {
                    self.compact_pending_routed_write(&intent_id);
                    return Err(DfsError::from_network(error));
                }
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
        self.posix_replica_candidates_for_write_origin(object, options, false)
    }

    fn posix_replica_candidates_for_write_origin(
        &mut self,
        object: &DfsObjectMetadata,
        options: &DfsWriteOptions,
        local_content_staged: bool,
    ) -> std::result::Result<Vec<NodeId>, DfsError> {
        self.posix_replica_candidates_for_write_creation(
            object,
            options,
            local_content_staged,
            DfsWriteCreation::Upsert,
            None,
        )
    }

    fn posix_replica_candidates_for_write_creation(
        &mut self,
        object: &DfsObjectMetadata,
        options: &DfsWriteOptions,
        local_content_staged: bool,
        creation: DfsWriteCreation,
        target_version: Option<&DfsVersion>,
    ) -> std::result::Result<Vec<NodeId>, DfsError> {
        let _namespace_plan = self.namespace_execution.begin_planning();
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
            target_version: target_version
                .cloned()
                .unwrap_or_else(|| next_version(&object.version)),
            requesting_node_id: self.node_id.clone(),
        };
        let mut plan = self.plan_namespace_creation_replicas(
            &request,
            object,
            candidates,
            options.policy.clone(),
            creation,
            if options.foreground_replica_repair {
                DfsWriteRepair::Foreground
            } else {
                DfsWriteRepair::Background
            },
        )?;

        // A remote metadata primary now ingests the only sender-owned payload
        // directly. Represent that durable copy honestly by replacing an
        // unconstrained requester slot with the primary; the sender is no
        // longer a replica merely because it originated the bytes.
        // Stable reader/CAS operations capture bytes durably on the requester.
        // Do not replace that policy-approved replica with a metadata primary
        // that receives only a manifest; that discards the available receipt
        // and forces a redundant network fetch before the write can commit.
        // A metadata ingress copy does not change LocalOwned content custody.
        if !local_content_staged
            && object.policy != DfsObjectPolicy::LocalOwned
            && let Some(shard) = self.select_metadata_shard_for_object(object)?
            && shard.primary_node_id != self.node_id
            && !options
                .policy
                .forbidden_node_ids
                .contains(&shard.primary_node_id)
            && !plan.desired_replica_nodes.contains(&shard.primary_node_id)
        {
            let replacement_index = if options.policy.is_strict_confirmed()
                && options.policy.desired_replicas >= 2
            {
                plan.desired_replica_nodes.iter().rposition(|node_id| {
                    *node_id != self.node_id && !options.policy.required_node_ids.contains(node_id)
                })
            } else {
                plan.desired_replica_nodes
                    .iter()
                    .position(|node_id| {
                        *node_id == self.node_id
                            && !options.policy.required_node_ids.contains(node_id)
                    })
                    .or_else(|| {
                        plan.desired_replica_nodes.iter().rposition(|node_id| {
                            !options.policy.required_node_ids.contains(node_id)
                        })
                    })
            };
            if let Some(index) = replacement_index {
                plan.desired_replica_nodes[index] = shard.primary_node_id;
            }
            crate::validate_replication_plan(&plan.desired_replica_nodes, &options.policy)?;
        }

        Ok(plan.desired_replica_nodes)
    }

    pub fn bootstrap_distributed_sifs(&mut self) -> Result<()> {
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        if self.transport.delegates_local_namespace_authority() {
            // The resident Main owns bootstrap completion, rebind and retry.
            // A client needs the filesystem projection to route its first
            // request, but opening it grants no local mutation admission.
            self.clear_metadata_lookup_cache();
            self.metadata.add_file_system(file_system);
            self.clear_metadata_lookup_cache();
            return Ok(());
        }
        // A durable completed handoff does not mutate process-local authority
        // bindings in service handles opened before or after activation. The
        // owning bootstrap lane verifies the exact assignment, completion proof
        // and local marker before its first authoritative namespace mutation.
        self.rebind_completed_local_metadata_shard_membership_handoffs(&sifs_file_system_id())?;
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
        let mut missing_objects = Vec::new();
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
                // The common prepared body initializes this exact shard's
                // filesystem together with its first namespace anchors.
            }
            // Clustered bootstrap objects are shard anchors. Their parent can
            // legitimately live on a shard that is not opened locally (or,
            // while a current shard is initializing), so the
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
                None => missing_objects.push(object),
                Some(_) => {}
            }
        }
        self.publish_missing_bootstrap_objects(missing_objects)?;
        Ok(())
    }

    pub fn posix_mkdir(&mut self, path: &str) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized = self.resolve_posix_dot_components_distributed(path)?;
        let dispatch_path = crate::posix_path::path_after_dot_resolution(path, &normalized);
        let path = dispatch_path.as_ref();
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

    /// Create one directory under a caller-owned durable operation identity.
    ///
    /// Clustered local-primary execution and routed execution both enter the
    /// same durable namespace admission table before touching the directory.
    /// An exact retry therefore replays the canonical response, while reuse of
    /// the id for another path or operation is rejected as a digest conflict.
    pub fn posix_mkdir_with_operation_id(
        &mut self,
        path: &str,
        operation_id: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        crate::service_routed_namespace::validate_namespace_operation_id(operation_id)?;
        let normalized = self.resolve_posix_dot_components_distributed(path)?;
        let dispatch_path = crate::posix_path::path_after_dot_resolution(path, &normalized);
        let path = dispatch_path.as_ref();
        let operation = DfsNamespaceOperation::Mkdir {
            path: path.to_string(),
        };
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation_with_request_id(
                operation_id.to_string(),
                primary,
                operation,
            )?;
            return self.stat_routed_namespace_result(&normalized, false);
        }
        if self.control_plane.is_some() {
            self.execute_local_primary_namespace_operation_with_request_id(
                operation_id.to_string(),
                operation,
            )?;
            return self.stat_object(&self.lookup_sifs_path(&normalized)?.ok_or_else(|| {
                DfsPosixError::NotFound {
                    path: normalized.clone(),
                }
            })?);
        }
        Err(DfsPosixError::Dfs(DfsError::Distributed {
            message: "stable mkdir requires a committed control-plane shard view for durable request admission"
                .to_string(),
        }))
    }

    pub fn posix_mkdir_with_create_attrs(
        &mut self,
        path: &str,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized = self.resolve_posix_dot_components_distributed(path)?;
        let dispatch_path = crate::posix_path::path_after_dot_resolution(path, &normalized);
        let path = dispatch_path.as_ref();
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

    /// Create one attributed directory under a caller-owned durable operation
    /// identity. The path, attributes, requester, and operation kind are bound
    /// into the durable command digest before the destination-absent mutation.
    pub fn posix_mkdir_with_create_attrs_and_operation_id(
        &mut self,
        path: &str,
        attrs: DfsPosixCreateAttrs,
        operation_id: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        crate::service_routed_namespace::validate_namespace_operation_id(operation_id)?;
        let normalized = self.resolve_posix_dot_components_distributed(path)?;
        let dispatch_path = crate::posix_path::path_after_dot_resolution(path, &normalized);
        let path = dispatch_path.as_ref();
        let operation = DfsNamespaceOperation::MkdirWithAttrs {
            path: path.to_string(),
            attrs,
        };
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation_with_request_id(
                operation_id.to_string(),
                primary,
                operation,
            )?;
            return self.stat_routed_namespace_result(&normalized, false);
        }
        if self.control_plane.is_some() {
            self.execute_local_primary_namespace_operation_with_request_id(
                operation_id.to_string(),
                operation,
            )?;
            return self.stat_object(&self.lookup_sifs_path(&normalized)?.ok_or_else(|| {
                DfsPosixError::NotFound {
                    path: normalized.clone(),
                }
            })?);
        }
        Err(DfsPosixError::Dfs(DfsError::Distributed {
            message: "stable mkdir requires a committed control-plane shard view for durable request admission"
                .to_string(),
        }))
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        if self.is_local_metadata_primary_for_object(object)? {
            self.posix_inherit_create_attrs_from_parent_local(&object.path, attrs, directory)
        } else {
            Ok(attrs)
        }
    }

    fn new_posix_directory_for_creation(&self, path: &str) -> DfsObjectMetadata {
        // A pathname can be reused after its old directory moved elsewhere.
        // The admitted request owns retry identity; an unrequested direct
        // create mints its identity only during this first planning attempt.
        // Existing preparations and completions are resolved before planning.
        let mut object =
            recreated_posix_metadata_object(&self.node_id, path, DfsObjectKind::Directory);
        if let Some(request) = self.namespace_execution.request() {
            object.object_id = stable_create_object_id(
                &request.requester_node_id,
                path,
                &request.operation_id,
            );
        }
        object
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
            content_reference_identity: None,
            extension_expectations: Vec::new(),
            contribution_digest: None,
            file_version_pins: None,
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_mkdir_local_with_create_attrs(path, DfsPosixCreateAttrs::directory_default())
    }

    pub(crate) fn posix_mkdir_local_with_create_attrs(
        &mut self,
        path: &str,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let path = self.resolve_posix_dot_components_distributed(path)?;
        let path = self.resolve_posix_path_distributed(&path, false)?;
        self.posix_mkdir_local_at_normalized_path(&path, attrs)
    }

    pub(crate) fn posix_mkdir_at_no_links_local(
        &mut self,
        path: &str,
        expected_parent_object_id: &DfsObjectId,
        expected_parent_version: &DfsVersion,
    ) -> Result<DfsStat> {
        self.posix_mkdir_at_no_links_local_with_attrs(
            path,
            expected_parent_object_id,
            expected_parent_version,
            DfsPosixCreateAttrs::directory_default(),
        )
    }

    pub(crate) fn posix_mkdir_at_no_links_local_with_attrs(
        &mut self,
        path: &str,
        expected_parent_object_id: &DfsObjectId,
        expected_parent_version: &DfsVersion,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        self.posix_mkdir_local_at_normalized_path(&path, attrs)
    }

    fn posix_mkdir_local_at_normalized_path(
        &mut self,
        path: &str,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let path = self.resolve_posix_dot_components_distributed(path)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&path)?;
        // Creating the child also publishes its parent's generation. Check
        // that existing job reservation before either durable mutation;
        // discovering it during the result lookup would hide a committed mkdir.
        if let Some(parent) = parent_posix_path(&path) {
            self.ensure_resolved_path_not_in_cross_shard_flight(&parent)?;
        }
        self.ensure_parent_directory(&path)?;
        if let Some(mut object) = self.lookup_sifs_path(&path)? {
            if object.kind != DfsObjectKind::Tombstone {
                return Err(DfsPosixError::AlreadyExists { path });
            }
            object.kind = DfsObjectKind::Directory;
                    object.namespace_link_count = Some(2);
            object.content_hash = None;
            object.content_size_bytes = None;
            object.link_target_object_id = None;
            object.current_locations = vec![self.node_id.clone()];
            set_object_create_timestamps(&mut object);
            let attrs = self.posix_inherit_create_attrs_from_parent_local(&path, attrs, true)?;
            self.apply_posix_new_directory_metadata(&mut object, attrs)?;
            object.version = next_version(&object.version);
            return self.commit_posix_created_directory(object);
        }
        let mut object = self.new_posix_directory_for_creation(&path);
        let attrs = self.posix_inherit_create_attrs_from_parent_local(&path, attrs, true)?;
        self.apply_posix_new_directory_metadata(&mut object, attrs)?;
        self.commit_posix_created_directory(object)
    }

    fn apply_posix_new_directory_metadata(
        &mut self,
        object: &mut DfsObjectMetadata,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<()> {
        // A tombstone deliberately retains the deleted object's metadata. Once
        // the path is created as a new directory, start from the same durable
        // policy as a fresh directory before resolving current managed rules.
        // Otherwise an unmanaged resurrection inherits the dead object's
        // policy, and a managed resurrection can use it as its fallback.
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        self.apply_managed_policy_to_new_object(object, 2)?;
        apply_posix_create_attrs(object, attrs);
        Ok(())
    }

    pub fn posix_mkdir_all(&mut self, path: &str) -> Result<()> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.with_posix_commit_operation_deadline(|service| {
            service.posix_mkdir_all_with_current_deadline(path)
        })
    }

    fn posix_mkdir_all_with_current_deadline(&mut self, path: &str) -> Result<()> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized = self.resolve_posix_dot_components_distributed(path)?;
        let dispatch_path = crate::posix_path::path_after_dot_resolution(path, &normalized);
        let path = dispatch_path.as_ref();
        if let Some(shard) = self.routed_namespace_shard_for_path(&normalized)? {
            let local_chain_exists =
                match self.routed_mkdir_boundary_directory_exists(&normalized, &shard)? {
                    Some(true) => return Ok(()),
                    Some(false) => false,
                    None => self.posix_mkdir_all_chain_exists_locally(&normalized)?,
                };
            match self.route_namespace_operation(
                shard.primary_node_id,
                DfsNamespaceOperation::MkdirAll {
                    path: path.to_string(),
                },
            ) {
                Ok(()) => {
                    // Main has already authenticated and applied this unit
                    // mutation. A projection-only caller has no returned stat
                    // fields to validate: a subsequent, discarded stat can
                    // fail on an unrelated publication after mkdir committed.
                    // Keep bounded catch-up for legacy local replay clients;
                    // explicit stat calls retain their full current-cut checks.
                    if !self.transport.delegates_local_namespace_authority() {
                        self.stat_routed_namespace_result(&normalized, false)
                            .map(drop)?;
                    }
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.with_posix_commit_operation_deadline(|service| {
            service.posix_mkdir_all_without_local_replay_with_current_deadline(path)
        })
    }

    fn posix_mkdir_all_without_local_replay_with_current_deadline(
        &mut self,
        path: &str,
    ) -> Result<()> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized = self.resolve_posix_dot_components_distributed(path)?;
        let dispatch_path = crate::posix_path::path_after_dot_resolution(path, &normalized);
        let path = dispatch_path.as_ref();
        if let Some(shard) = self.routed_namespace_shard_for_path(&normalized)? {
            let local_chain_exists =
                match self.routed_mkdir_boundary_directory_exists(&normalized, &shard)? {
                    Some(true) => return Ok(()),
                    Some(false) => false,
                    None => self.posix_mkdir_all_chain_exists_locally(&normalized)?,
                };
            match self.route_namespace_operation_without_local_replay(
                shard.primary_node_id,
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        self.posix_mkdir_at_opened_directory_no_links_impl(opened_parent, child, None)
    }

    /// Create one direct child with attributes installed in its initial
    /// publication, while preserving the actual opened parent's generation.
    pub fn posix_mkdir_at_opened_directory_no_links_with_attrs(
        &mut self,
        opened_parent: &DfsStat,
        child: &str,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        self.posix_mkdir_at_opened_directory_no_links_impl(opened_parent, child, Some(attrs))
    }

    fn posix_mkdir_at_opened_directory_no_links_impl(
        &mut self,
        opened_parent: &DfsStat,
        child: &str,
        attrs: Option<DfsPosixCreateAttrs>,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
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
                match attrs {
                    Some(attrs) => DfsNamespaceOperation::MkdirAtNoLinksWithAttrs(
                        crate::DfsMkdirAtNoLinksWithAttrs {
                            path: path.clone(),
                            expected_parent_object_id: opened_parent.object_id.clone(),
                            expected_parent_version: opened_parent.version.clone(),
                            attrs,
                        },
                    ),
                    None => DfsNamespaceOperation::MkdirAtNoLinks {
                        path: path.clone(),
                        expected_parent_object_id: opened_parent.object_id.clone(),
                        expected_parent_version: opened_parent.version.clone(),
                    },
                },
            )?;
            return self.stat_routed_namespace_result(&path, false);
        }
        self.posix_mkdir_at_no_links_local_with_attrs(
            &path,
            &opened_parent.object_id,
            &opened_parent.version,
            attrs.unwrap_or_else(DfsPosixCreateAttrs::directory_default),
        )
    }

    fn posix_mkdir_all_chain_exists_locally(&self, path: &str) -> Result<bool> {
        let path = self.resolve_posix_dot_components(path)?;
        if path == "/" {
            return Ok(true);
        }
        let file_system_id = sifs_file_system_id();
        let mut known_live_ancestors = false;
        let mut current = String::new();
        for part in path.trim_matches('/').split('/') {
            current.push('/');
            current.push_str(part);
            match self.lookup_metadata_path_with_known_live_ancestors(
                &file_system_id,
                &current,
                known_live_ancestors,
            )? {
                Some(object) if object.kind == DfsObjectKind::Directory => {
                    known_live_ancestors = true;
                }
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    pub(crate) fn posix_mkdir_all_local(&mut self, path: &str) -> Result<()> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let path = self.resolve_posix_dot_components_distributed(path)?;
        let resolved = self.resolve_posix_path_distributed_for_mkdir_all(&path)?;
        let path = resolved.path;
        if path == "/" {
            return Ok(());
        }
        debug_assert_eq!(
            resolved
                .components
                .last()
                .map(|component| component.path.as_str()),
            Some(path.as_str()),
            "a fully followed path walk must describe its final component"
        );

        let mut parent = None;
        let mut planned = BTreeMap::<String, DfsObjectMetadata>::new();
        let mut durable_parents_to_touch = BTreeMap::<String, DfsObjectMetadata>::new();
        let mut planned_parent_paths = Vec::<(String, String)>::new();
        for component in resolved.components {
            let current = component.path;
            match component.object {
                Some(object) if object.kind == DfsObjectKind::Directory => {
                    parent = Some(object);
                }
                Some(object) if object.kind == DfsObjectKind::Tombstone => {
                    self.ensure_resolved_path_not_in_cross_shard_flight(&current)?;
                    if parent.is_none() {
                        let parent_path = parent_posix_path(&current).unwrap_or_else(|| "/".into());
                        parent = self.lookup_sifs_path(&parent_path)?;
                    }
                    let parent_for_child =
                        parent
                            .as_ref()
                            .cloned()
                            .ok_or_else(|| DfsPosixError::MissingParent {
                                path: parent_posix_path(&current).unwrap_or_else(|| "/".into()),
                            })?;
                    if parent_for_child.kind != DfsObjectKind::Directory {
                        return Err(DfsPosixError::NotDirectory {
                            path: parent_for_child.path,
                        });
                    }
                    let mut object = object;
                    object.kind = DfsObjectKind::Directory;
                    object.namespace_link_count = Some(2);
                    object.content_hash = None;
                    object.content_size_bytes = None;
                    object.link_target_object_id = None;
                    object.current_locations = vec![self.node_id.clone()];
                    set_object_create_timestamps(&mut object);
                    let attrs = inherit_posix_create_attrs(
                        DfsPosixCreateAttrs::directory_default(),
                        true,
                        parent_for_child.mode,
                        parent_for_child.gid,
                    );
                    self.apply_posix_new_directory_metadata(&mut object, attrs)?;
                    object.version = next_version(&object.version);
                    if let Some(planned_parent) = planned.get_mut(&parent_for_child.path) {
                        touch_directory_for_batched_child(planned_parent, false);
                        planned_parent.namespace_link_count = Some(updated_namespace_link_count(planned_parent, 1)?);
                    } else {
                        let parent = durable_parents_to_touch
                            .entry(parent_for_child.path.clone())
                            .or_insert(parent_for_child.clone());
                        parent.namespace_link_count = Some(updated_namespace_link_count(parent, 1)?);
                    }
                    planned_parent_paths.push((current.clone(), parent_for_child.path.clone()));
                    planned.insert(current.clone(), object.clone());
                    parent = Some(object);
                }
                Some(_) => return Err(DfsPosixError::NotDirectory { path: current }),
                None => {
                    self.ensure_resolved_path_not_in_cross_shard_flight(&current)?;
                    if parent.is_none() {
                        let parent_path = parent_posix_path(&current).unwrap_or_else(|| "/".into());
                        parent = self.lookup_sifs_path(&parent_path)?;
                    }
                    let parent_for_child =
                        parent
                            .as_ref()
                            .cloned()
                            .ok_or_else(|| DfsPosixError::MissingParent {
                                path: parent_posix_path(&current).unwrap_or_else(|| "/".into()),
                            })?;
                    if parent_for_child.kind != DfsObjectKind::Directory {
                        return Err(DfsPosixError::NotDirectory {
                            path: parent_for_child.path,
                        });
                    }
                    let mut object = self.new_posix_directory_for_creation(&current);
                    let attrs = inherit_posix_create_attrs(
                        DfsPosixCreateAttrs::directory_default(),
                        true,
                        parent_for_child.mode,
                        parent_for_child.gid,
                    );
                    self.apply_posix_new_directory_metadata(&mut object, attrs)?;
                    if let Some(planned_parent) = planned.get_mut(&parent_for_child.path) {
                        touch_directory_for_batched_child(planned_parent, false);
                        planned_parent.namespace_link_count = Some(updated_namespace_link_count(planned_parent, 1)?);
                    } else {
                        let parent = durable_parents_to_touch
                            .entry(parent_for_child.path.clone())
                            .or_insert(parent_for_child.clone());
                        parent.namespace_link_count = Some(updated_namespace_link_count(parent, 1)?);
                    }
                    planned_parent_paths.push((current.clone(), parent_for_child.path.clone()));
                    planned.insert(current.clone(), object.clone());
                    parent = Some(object);
                }
            }
        }

        if planned.is_empty() {
            return Ok(());
        }

        let file_system_id = sifs_file_system_id();
        // Preserve the same atomic child-plus-parent boundary as repeated
        // `mkdir` while retaining one transaction for the common same-shard
        // chain. Validate against one placement snapshot so a deep mkdir does
        // not reread cluster KV for every component, and report the exact pair
        // that crosses a shard boundary.
        let shard_snapshot = self.metadata_shard_config_snapshot(&file_system_id)?;
        for (child_path, parent_path) in &planned_parent_paths {
            let shards = match shard_snapshot.as_deref() {
                Some(shards) => Self::namespace_shards_for_normalized_paths_from_snapshot(
                    &file_system_id,
                    shards,
                    [child_path.as_str(), parent_path.as_str()],
                )?,
                None => Vec::new(),
            };
            crate::service_commit::ensure_atomic_create_parent_touch_shard(
                &file_system_id,
                &self.node_id,
                child_path,
                parent_path,
                &shards,
            )?;
        }

        for parent in durable_parents_to_touch.values_mut() {
            touch_directory_for_batched_child(parent, true);
        }
        let objects = durable_parents_to_touch
            .into_values()
            .chain(planned.into_values())
            .collect::<Vec<_>>();
        let versions = objects
            .iter()
            .map(|object| object_version_record(object, object.content_size_bytes))
            .collect::<Vec<_>>();
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            content_reference_identity: None,
            extension_expectations: Vec::new(),
            contribution_digest: None,
            file_version_pins: None,
            transaction_id: namespace_transaction_id_for_batch(
                "mkdir-all",
                &file_system_id,
                &objects,
                &versions,
            ),
            file_system_id,
            objects,
            versions,
            advisory_lock_states: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        Ok(())
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.posix.write",
        skip_all,
        fields(stage = "dfs.posix.write")
    )]
    pub fn posix_write_file(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_bytes_checked_parent(
            path, bytes, options, true, true, true, None, false, false,
        )
    }

    /// Write or create a file under a caller-owned idempotency key.
    ///
    /// Reusing `operation_id` with the same payload replays the exact durable
    /// outcome across response loss and service restart. Reusing it for a
    /// different payload is rejected.
    pub fn posix_write_file_with_operation_id(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
        operation_id: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_bytes_checked_parent_with_operation_id(
            path,
            bytes,
            options,
            true,
            true,
            true,
            None,
            false,
            false,
            Some(operation_id.to_string()),
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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

    /// Compare-and-replace with a durable caller-owned operation identity.
    pub fn posix_replace_file_if_version_with_operation_id(
        &mut self,
        path: &str,
        expected_object_id: &DfsObjectId,
        expected_version: &DfsVersion,
        bytes: &[u8],
        options: DfsWriteOptions,
        operation_id: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        crate::service_routed_write::validate_write_operation_id(operation_id)?;
        self.posix_write_file_bytes_checked_parent_once_with_operation_id(
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
            Some(operation_id),
            None,
            Default::default(),
        )
    }

    pub fn posix_write_file_with_create_attrs(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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

    /// Exclusively create an empty file under a durable operation identity.
    pub fn posix_create_file_with_attrs_and_operation_id(
        &mut self,
        path: &str,
        options: DfsWriteOptions,
        attrs: DfsPosixCreateAttrs,
        operation_id: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_bytes_checked_parent_with_operation_id(
            path,
            &[],
            options,
            true,
            true,
            true,
            Some(attrs),
            true,
            false,
            Some(operation_id.to_string()),
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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

    /// Exclusively create a content-bearing file under a durable operation
    /// identity.
    pub fn posix_create_file_with_contents_and_attrs_and_operation_id(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
        attrs: DfsPosixCreateAttrs,
        operation_id: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_bytes_checked_parent_with_operation_id(
            path,
            bytes,
            options,
            true,
            true,
            true,
            Some(attrs),
            true,
            false,
            Some(operation_id.to_string()),
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_bytes_checked_parent(
            path, bytes, options, true, true, true, None, true, false,
        )
    }

    /// Exclusively create a file under a durable operation identity.
    pub fn posix_create_file_with_operation_id(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
        operation_id: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_bytes_checked_parent_with_operation_id(
            path,
            bytes,
            options,
            true,
            true,
            true,
            None,
            true,
            false,
            Some(operation_id.to_string()),
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
            let condition = self.no_link_scoped_write_condition(
                root,
                &path,
                crate::DfsWriteTargetPrecondition::Absent {},
            )?;
            match self.posix_create_file_at_opened_directory_with_condition(
                &opened_parent,
                child,
                bytes,
                options.clone(),
                condition,
            ) {
                Ok(stat) => return Ok(stat),
                Err(error @ DfsPosixError::Dfs(DfsError::WritePreconditionFailed { .. })) => {
                    return Err(no_link_write_condition_error(error));
                }
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

    fn no_link_scoped_write_condition(
        &self,
        root: &str,
        path: &str,
        target: crate::DfsWriteTargetPrecondition,
    ) -> Result<crate::DfsCurrentWritePrecondition> {
        let root = normalize_posix_path(root)?;
        let reads =
            self.namespace_execution
                .captured_reads()
                .ok_or_else(|| DfsError::Metadata {
                    message: "no-link write lost its original namespace planning scope".into(),
                })?;
        crate::write_precondition::opened_scope_from_reads(
            &root,
            path,
            &reads,
            &sifs_file_system_id(),
            target,
        )
        .map_err(|message| DfsError::Metadata { message }.into())
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
        let parent = normalize_posix_path(&opened_parent.path)?;
        let path = join_posix_path(&parent, child);
        let condition = crate::DfsCurrentWritePrecondition::ParentOnly {
            path,
            parent: crate::DfsNamespaceObjectPrecondition {
                object_id: opened_parent.object_id.clone(),
                version: opened_parent.version.clone(),
            },
        };
        self.posix_create_file_at_opened_directory_with_condition(
            opened_parent,
            child,
            bytes,
            options,
            condition,
        )
        .map_err(no_link_write_condition_error)
    }

    fn posix_create_file_at_opened_directory_with_condition(
        &mut self,
        opened_parent: &DfsStat,
        child: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
        condition: crate::DfsCurrentWritePrecondition,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        self.posix_write_file_bytes_checked_parent_once_with_operation_id(
            &path,
            bytes,
            options,
            true,
            true,
            true,
            None,
            true,
            false,
            None,
            false,
            None,
            Some(&current_parent),
            crate::DfsWritePublication::ConditionalCurrent {
                precondition: Box::new(condition),
            },
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        let opened = self.posix_open_beneath_no_links_distributed(root, path)?;
        if &opened.object_id != expected_object_id || &opened.version != expected_version {
            return Err(DfsPosixError::Conflict {
                path: opened.path.clone(),
                reason: "file identity or version changed before no-link replace".to_string(),
            });
        }
        let condition = self.no_link_scoped_write_condition(
            root,
            &opened.path,
            crate::DfsWriteTargetPrecondition::Match {
                expected: crate::DfsNamespaceObjectPrecondition {
                    object_id: opened.object_id.clone(),
                    version: opened.version.clone(),
                },
            },
        )?;
        self.posix_replace_opened_file_with_condition(&opened, bytes, options, condition)
            .map_err(no_link_write_condition_error)
    }

    /// Replace the exact file/version captured by a no-link open without
    /// resolving its pathname again.
    pub fn posix_replace_opened_file_if_version_no_links(
        &mut self,
        opened: &DfsStat,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let condition = crate::DfsCurrentWritePrecondition::CompareAndSwap {
            path: opened.path.clone(),
            expected: crate::DfsNamespaceObjectPrecondition {
                object_id: opened.object_id.clone(),
                version: opened.version.clone(),
            },
        };
        self.posix_replace_opened_file_with_condition(opened, bytes, options, condition)
            .map_err(no_link_write_condition_error)
    }

    fn posix_replace_opened_file_with_condition(
        &mut self,
        opened: &DfsStat,
        bytes: &[u8],
        options: DfsWriteOptions,
        condition: crate::DfsCurrentWritePrecondition,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        self.posix_write_file_bytes_checked_parent_once_with_operation_id(
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
            None,
            None,
            crate::DfsWritePublication::ConditionalCurrent {
                precondition: Box::new(condition),
            },
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_bytes_checked_parent(
            path, bytes, options, false, false, false, None, false, false,
        )
    }

    fn resolve_posix_bulk_parent(
        &mut self,
        path: &str,
        require_authoritative: bool,
    ) -> Result<(String, DfsObjectMetadata)> {
        let (mut path, mut parent) =
            self.resolve_posix_path_distributed_with_terminal_object(path)?;
        if require_authoritative
            || parent
                .as_ref()
                .is_none_or(|parent| parent.kind == DfsObjectKind::Tombstone)
        {
            // The first attempt remains local-first. A stale-parent conflict
            // forces one authoritative refresh per distinct parent before the
            // complete same-shard batch is rebuilt and retried.
            (path, parent) = self.lookup_sifs_path_distributed_synced(&path, false)?;
        }
        let parent = parent.ok_or_else(|| DfsPosixError::MissingParent { path: path.clone() })?;
        if parent.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::MissingParent { path });
        }
        let parent = self.resolve_hardlink_target(&parent)?;
        if parent.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory { path });
        }
        Ok((path, parent))
    }

    /// Fetch the exact current parent and child records needed by a
    /// metadata-less bulk overwrite requester.
    ///
    /// The ordinary service prepares against its pinned local metadata
    /// backend. A one-shot routed requester deliberately has no such backend,
    /// so treating an empty memory overlay as authoritative would turn every
    /// overwrite into an occupied create. Group exact-path lookups by current
    /// metadata primary and retain the returned records only for this prepare
    /// attempt. A conflict retries this whole function and therefore refreshes
    /// the child versions without materializing a durable local mirror.
    fn route_authoritative_bulk_lookup_paths(
        &mut self,
        file_system_id: &DfsFileSystemId,
        primary_node_id: NodeId,
        requested_paths: BTreeSet<String>,
    ) -> Result<PosixBulkAuthoritativeLookup> {
        let objects = self
            .route_namespace_lookup_paths_to_metadata_primary(
                file_system_id.clone(),
                primary_node_id,
                requested_paths.iter().cloned().collect(),
                POSIX_READ_FRESHNESS_SYNC_TIMEOUT,
            )
            .map_err(DfsPosixError::from)?;
        let mut lookup = PosixBulkAuthoritativeLookup::default();
        let mut extras = Vec::new();
        for object in objects {
            if object.file_system_id != *file_system_id {
                return Err(DfsPosixError::Dfs(DfsError::Distributed {
                    message: format!(
                        "bulk overwrite authority returned object {} for unexpected filesystem {}",
                        object.object_id, object.file_system_id
                    ),
                }));
            }
            if requested_paths.contains(&object.path) {
                let path = object.path.clone();
                if lookup
                    .exact_objects_by_path
                    .insert(path.clone(), object)
                    .is_some()
                {
                    return Err(DfsPosixError::Dfs(DfsError::Distributed {
                        message: format!(
                            "bulk overwrite authority returned duplicate metadata for {path}"
                        ),
                    }));
                }
            } else {
                extras.push(object);
            }
        }

        let allowed_hint_ids = lookup
            .exact_objects_by_path
            .values()
            .filter(|object| object.kind == DfsObjectKind::Hardlink)
            .filter_map(|object| object.link_target_object_id.clone())
            .collect::<BTreeSet<_>>();
        for hint in extras {
            if !allowed_hint_ids.contains(&hint.object_id) {
                return Err(DfsPosixError::Dfs(DfsError::Distributed {
                    message: format!(
                        "bulk overwrite authority returned unreferenced object {} at {}",
                        hint.object_id, hint.path
                    ),
                }));
            }
            if lookup
                .hardlink_target_hints
                .insert(hint.object_id.clone(), hint)
                .is_some()
            {
                return Err(DfsPosixError::Dfs(DfsError::Distributed {
                    message: "bulk overwrite authority returned a duplicate hardlink target hint"
                        .to_string(),
                }));
            }
        }
        Ok(lookup)
    }

    fn exact_authoritative_bulk_lookup(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
        metadata_shards: &[DfsMetadataShardConfig],
    ) -> Result<PosixBulkAuthoritativeLookup> {
        let shard = self
            .select_sifs_metadata_shard_for_path_from_snapshot(path, metadata_shards)?
            .ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Distributed {
                    message: format!(
                        "clustered bulk overwrite target {path} has no metadata shard assignment"
                    ),
                })
            })?;
        self.route_authoritative_bulk_lookup_paths(
            file_system_id,
            shard.primary_node_id,
            BTreeSet::from([path.to_string()]),
        )
    }

    fn resolve_authoritative_bulk_hardlink_target(
        &mut self,
        file_system_id: &DfsFileSystemId,
        link_path: &str,
        mut object: DfsObjectMetadata,
        mut hints: BTreeMap<DfsObjectId, DfsObjectMetadata>,
        metadata_shards: &[DfsMetadataShardConfig],
    ) -> Result<(String, DfsObjectMetadata)> {
        let mut target_path = link_path.to_string();
        for _ in 0..16 {
            if object.kind != DfsObjectKind::Hardlink {
                return Ok((target_path, object));
            }
            let target_object_id = object.link_target_object_id.clone().ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Distributed {
                    message: format!(
                        "authoritative hardlink at {link_path} has no target object id"
                    ),
                })
            })?;
            let hint = hints.remove(&target_object_id).ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Distributed {
                    message: format!(
                        "authoritative lookup for hardlink {link_path} did not include a target routing hint for {target_object_id}"
                    ),
                })
            })?;
            target_path = normalize_posix_path(&hint.path)?;
            let mut target_lookup = self.exact_authoritative_bulk_lookup(
                file_system_id,
                &target_path,
                metadata_shards,
            )?;
            for (object_id, target_hint) in target_lookup.hardlink_target_hints {
                if hints.insert(object_id.clone(), target_hint).is_some() {
                    return Err(DfsPosixError::Dfs(DfsError::Distributed {
                        message: format!(
                            "authoritative lookup returned duplicate routing hint for {object_id}"
                        ),
                    }));
                }
            }
            object = target_lookup
                .exact_objects_by_path
                .remove(&target_path)
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: target_path.clone(),
                })?;
            if object.object_id != target_object_id {
                return Err(DfsPosixError::Conflict {
                    path: link_path.to_string(),
                    reason: format!(
                        "hardlink target hint named {target_object_id}, but current path {} names {}",
                        target_path, object.object_id
                    ),
                });
            }
            if object.kind == DfsObjectKind::Tombstone {
                return Err(DfsPosixError::NotFound {
                    path: link_path.to_string(),
                });
            }
        }
        Err(DfsPosixError::InvalidPath {
            path: link_path.to_string(),
        })
    }

    fn resolve_authoritative_bulk_link_target(
        &mut self,
        file_system_id: &DfsFileSystemId,
        link_path: &str,
        object: DfsObjectMetadata,
        hints: BTreeMap<DfsObjectId, DfsObjectMetadata>,
        metadata_shards: &[DfsMetadataShardConfig],
    ) -> Result<(String, DfsObjectMetadata)> {
        let mut resolution_path = link_path.to_string();
        let (mut metadata_path, mut object) = self.resolve_authoritative_bulk_hardlink_target(
            file_system_id,
            &resolution_path,
            object,
            hints,
            metadata_shards,
        )?;
        for _ in 0..40 {
            if object.kind != DfsObjectKind::Symlink {
                return Ok((metadata_path, object));
            }
            let outcome = self.read_object_metadata_or_fetch_remote_without_cache(&object)?;
            let target =
                metadata::symlink_target_from_distributed_read_outcome(&resolution_path, outcome)?;
            resolution_path = resolve_posix_symlink_target(&resolution_path, &target)?;
            let mut target_lookup = self.exact_authoritative_bulk_lookup(
                file_system_id,
                &resolution_path,
                metadata_shards,
            )?;
            object = target_lookup
                .exact_objects_by_path
                .remove(&resolution_path)
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: resolution_path.clone(),
                })?;
            if object.kind == DfsObjectKind::Tombstone {
                return Err(DfsPosixError::NotFound {
                    path: resolution_path,
                });
            }
            (metadata_path, object) = self.resolve_authoritative_bulk_hardlink_target(
                file_system_id,
                &resolution_path,
                object,
                target_lookup.hardlink_target_hints,
                metadata_shards,
            )?;
        }
        Err(DfsPosixError::InvalidPath {
            path: link_path.to_string(),
        })
    }

    fn prefetch_authoritative_bulk_overwrite_objects(
        &mut self,
        file_system_id: &DfsFileSystemId,
        normalized_files: &[(String, String, &[u8])],
        metadata_shards: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<Option<PosixBulkAuthoritativePrefetch>> {
        if self.metadata_backend.is_some() {
            return Ok(None);
        }
        let Some(metadata_shards) = metadata_shards else {
            // A standalone in-memory service has no routed authority. Its
            // memory overlay remains the complete namespace view.
            return Ok(None);
        };

        let trace_started = Instant::now();
        let mut paths_by_primary = BTreeMap::<NodeId, BTreeSet<String>>::new();
        let mut requested_paths = BTreeSet::new();
        for (parent, child_name, _) in normalized_files {
            for path in [parent.clone(), join_posix_path(parent, child_name)] {
                if !requested_paths.insert(path.clone()) {
                    continue;
                }
                let shard = self
                    .select_sifs_metadata_shard_for_path_from_snapshot(&path, metadata_shards)?
                    .ok_or_else(|| {
                        DfsPosixError::Dfs(DfsError::Distributed {
                            message: format!(
                                "clustered bulk overwrite path {path} has no metadata shard assignment"
                            ),
                        })
                    })?;
                paths_by_primary
                    .entry(shard.primary_node_id)
                    .or_default()
                    .insert(path);
            }
        }

        let mut objects_by_path = BTreeMap::new();
        let mut hardlink_target_hints = BTreeMap::new();
        for (primary_node_id, paths) in paths_by_primary {
            let lookup =
                self.route_authoritative_bulk_lookup_paths(file_system_id, primary_node_id, paths)?;
            for (path, object) in lookup.exact_objects_by_path {
                if objects_by_path.insert(path.clone(), object).is_some() {
                    return Err(DfsPosixError::Dfs(DfsError::Distributed {
                        message: format!(
                            "bulk overwrite authority returned duplicate metadata for {path}"
                        ),
                    }));
                }
            }
            for (object_id, hint) in lookup.hardlink_target_hints {
                if hardlink_target_hints
                    .insert(object_id.clone(), hint)
                    .is_some()
                {
                    return Err(DfsPosixError::Dfs(DfsError::Distributed {
                        message: format!(
                            "bulk overwrite authorities returned duplicate routing hints for {object_id}"
                        ),
                    }));
                }
            }
        }
        let link_objects = objects_by_path
            .iter()
            .filter(|(_, object)| {
                matches!(
                    object.kind,
                    DfsObjectKind::Symlink | DfsObjectKind::Hardlink
                )
            })
            .map(|(path, object)| (path.clone(), object.clone()))
            .collect::<Vec<_>>();
        let mut resolved_link_targets = BTreeMap::new();
        for (path, object) in link_objects {
            let resolved = self.resolve_authoritative_bulk_link_target(
                file_system_id,
                &path,
                object,
                hardlink_target_hints.clone(),
                metadata_shards,
            )?;
            resolved_link_targets.insert(path, resolved);
        }
        crate::dfs_perf_trace(format_args!(
            "bulk-overwrite-prefetch paths={} objects={} resolved_links={} elapsed_ms={}",
            requested_paths.len(),
            objects_by_path.len(),
            resolved_link_targets.len(),
            trace_started.elapsed().as_millis()
        ));
        Ok(Some(PosixBulkAuthoritativePrefetch {
            objects_by_path,
            resolved_link_targets,
        }))
    }

    pub fn posix_write_files_local_eventual_with_known_parents<'a, I>(
        &mut self,
        files: I,
    ) -> Result<Vec<DfsStat>>
    where
        I: IntoIterator<Item = (&'a str, &'a [u8])>,
    {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let files = files.into_iter().collect::<Vec<_>>();
        for attempt in 0..POSIX_BULK_PARENT_RETRY_ATTEMPTS {
            match self.posix_write_files_local_eventual_with_known_parents_once(&files, attempt > 0)
            {
                Err(error)
                    if attempt + 1 < POSIX_BULK_PARENT_RETRY_ATTEMPTS
                        && (matches!(&error, DfsPosixError::Conflict { .. })
                            || posix_error_is_concurrent_create_winner(&error)) => {}
                result => return result,
            }
        }
        unreachable!("bounded bulk-parent retry loop always returns")
    }

    fn posix_write_files_local_eventual_with_known_parents_once<'a>(
        &mut self,
        files: &[(&'a str, &'a [u8])],
        require_authoritative_parents: bool,
    ) -> Result<Vec<DfsStat>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        if files.is_empty() {
            return Ok(Vec::new());
        }

        // Normalize the complete request before fetching one authoritative
        // parent/child projection for a metadata-less requester.
        let mut normalized_files = Vec::with_capacity(files.len());
        for &(path, bytes) in files {
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
            let Some((_, child_name)) = path.rsplit_once('/') else {
                return Err(DfsPosixError::InvalidPath { path });
            };
            normalized_files.push((parent, child_name.to_string(), bytes));
        }

        let metadata_shard_snapshot = self.refresh_sifs_metadata_shards()?;
        let file_system_id = sifs_file_system_id();
        let authoritative_prefetched_objects = self.prefetch_authoritative_bulk_overwrite_objects(
            &file_system_id,
            &normalized_files,
            metadata_shard_snapshot.as_deref(),
        )?;
        // Resolve parents before pinning the local metadata generation. Parent
        // resolution may need a remote freshness pull, which can ingest newer
        // metadata locally; no backend mutation may happen inside the bounded
        // consistent-read scope below.
        let mut resolved_parents = BTreeMap::<String, String>::new();
        for (parent, _, _) in &normalized_files {
            if resolved_parents.contains_key(parent) {
                continue;
            }
            let resolved_parent = match authoritative_prefetched_objects.as_ref() {
                Some(prefetch) => {
                    let Some(parent_object) = prefetch.objects_by_path.get(parent) else {
                        return Err(DfsPosixError::MissingParent {
                            path: parent.clone(),
                        });
                    };
                    if parent_object.kind == DfsObjectKind::Tombstone {
                        return Err(DfsPosixError::MissingParent {
                            path: parent.clone(),
                        });
                    }
                    if parent_object.kind != DfsObjectKind::Directory {
                        return Err(DfsPosixError::NotDirectory {
                            path: parent.clone(),
                        });
                    }
                    parent.clone()
                }
                None => {
                    let (resolved_parent, _) =
                        self.resolve_posix_bulk_parent(parent, require_authoritative_parents)?;
                    resolved_parent
                }
            };
            resolved_parents.insert(parent.clone(), resolved_parent);
        }
        let pinned_metadata_snapshot = if let Some(backend) = self.metadata_backend.as_mut() {
            backend
                .begin_consistent_read_snapshot()
                .map_err(DfsPosixError::from)?;
            true
        } else {
            false
        };
        #[cfg(test)]
        let durable_table_probes_before =
            crate::metadata_read_snapshot::metadata_durable_table_probes();
        #[cfg(test)]
        let table_snapshot_calls_before =
            crate::metadata_backend::metadata_current_tables_snapshot_calls();
        let preparation = (|| {
            // The pre-pin walk may have synchronized a remote parent, but it
            // cannot prove that parent is still live in the generation used
            // for child discovery. Revalidate every unique resolved parent
            // once, then use that exact pinned object for parent touches.
            let mut pinned_parents = BTreeMap::<String, Rc<PosixBulkParentTouch>>::new();
            for (parent, resolved_parent) in &resolved_parents {
                let parent_object = match authoritative_prefetched_objects.as_ref() {
                    Some(prefetch) => prefetch.objects_by_path.get(resolved_parent).cloned(),
                    None => self.lookup_metadata_path_for_local_overwrite(
                        &file_system_id,
                        resolved_parent,
                        false,
                    )?,
                };
                let Some(parent_object) = parent_object else {
                    return Err(DfsPosixError::MissingParent {
                        path: resolved_parent.clone(),
                    });
                };
                if parent_object.kind == DfsObjectKind::Tombstone {
                    return Err(DfsPosixError::MissingParent {
                        path: resolved_parent.clone(),
                    });
                }
                let parent_object = self.resolve_hardlink_target(&parent_object)?;
                if parent_object.kind != DfsObjectKind::Directory {
                    return Err(DfsPosixError::NotDirectory {
                        path: resolved_parent.clone(),
                    });
                }
                let metadata_shard = match metadata_shard_snapshot.as_deref() {
                    Some(shards) => self.select_sifs_metadata_shard_for_path_from_snapshot(
                        resolved_parent,
                        shards,
                    )?,
                    None => None,
                };
                pinned_parents.insert(
                    parent.clone(),
                    PosixBulkParentTouch::shared(
                        resolved_parent.clone(),
                        parent_object,
                        metadata_shard,
                    ),
                );
            }

            let mut prepared = Vec::with_capacity(files.len());
            let mut seen_objects = BTreeSet::new();
            for (parent, child_name, bytes) in &normalized_files {
                let parent_touch = pinned_parents
                    .get(parent)
                    .expect("bulk parent was revalidated in pinned metadata generation");
                let resolved_parent = &parent_touch.path;
                let path = join_posix_path(resolved_parent, child_name);
                let mut metadata_path = path.clone();
                let mut link_count_hint = None;
                let observed_object = match authoritative_prefetched_objects.as_ref() {
                    Some(prefetch) => prefetch.objects_by_path.get(&path).cloned(),
                    None => {
                        self.lookup_metadata_path_for_local_overwrite(&file_system_id, &path, true)?
                    }
                };
                let (object, expected_base_version, touch_parent_on_create) = match observed_object
                {
                    Some(object) if object.kind == DfsObjectKind::Directory => {
                        return Err(DfsPosixError::IsDirectory { path });
                    }
                    Some(object) if object.kind == DfsObjectKind::Symlink => {
                        let Some(prefetch) = authoritative_prefetched_objects.as_ref() else {
                            return Ok(None);
                        };
                        let (target_path, object) = prefetch
                            .resolved_link_targets
                            .get(&path)
                            .cloned()
                            .ok_or_else(|| {
                                DfsPosixError::Dfs(DfsError::Distributed {
                                    message: format!(
                                        "authoritative bulk prefetch did not resolve symlink {path}"
                                    ),
                                })
                            })?;
                        metadata_path = target_path;
                        let base = Some(object.version.clone());
                        (object, base, false)
                    }
                    Some(object) if object.kind == DfsObjectKind::Hardlink => {
                        let object = if let Some(prefetch) =
                            authoritative_prefetched_objects.as_ref()
                        {
                            let (target_path, object) = prefetch
                                .resolved_link_targets
                                .get(&path)
                                .cloned()
                                .ok_or_else(|| {
                                    DfsPosixError::Dfs(DfsError::Distributed {
                                        message: format!(
                                            "authoritative bulk prefetch did not resolve hardlink {path}"
                                        ),
                                    })
                                })?;
                            metadata_path = target_path;
                            object
                        } else {
                            let object = self.resolve_hardlink_target(&object)?;
                            metadata_path = object.path.clone();
                            object
                        };
                        let base = Some(object.version.clone());
                        (object, base, false)
                    }
                    Some(object) if object.kind == DfsObjectKind::Tombstone => {
                        let object = recreated_posix_metadata_object(
                            &self.node_id,
                            &path,
                            DfsObjectKind::File,
                        );
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
                    return Ok(None);
                }
                if object.policy != DfsObjectPolicy::ReplicatedEventual
                    || object.owner_node_id != self.node_id
                {
                    return Ok(None);
                }
                let metadata_shard = match metadata_shard_snapshot.as_deref() {
                    Some(shards) => self.select_sifs_metadata_shard_for_path_from_snapshot(
                        &metadata_path,
                        shards,
                    )?,
                    None => None,
                };
                let parent_touch = touch_parent_on_create.then(|| Rc::clone(parent_touch));
                let target_version = next_version(&object.version);
                prepared.push(PosixBulkLocalWrite {
                    path,
                    object,
                    target_version,
                    expected_base_version,
                    bytes,
                    metadata_shard,
                    parent_touch,
                    link_count_hint,
                });
            }
            Ok(Some(prepared))
        })();
        #[cfg(test)]
        POSIX_BULK_PREP_DURABLE_TABLE_PROBES.set(
            crate::metadata_read_snapshot::metadata_durable_table_probes()
                .saturating_sub(durable_table_probes_before),
        );
        #[cfg(test)]
        POSIX_BULK_PREP_TABLE_SNAPSHOT_CALLS.set(
            crate::metadata_backend::metadata_current_tables_snapshot_calls()
                .saturating_sub(table_snapshot_calls_before),
        );
        if pinned_metadata_snapshot {
            self.metadata_backend
                .as_mut()
                .expect("pinned metadata backend remains installed")
                .end_consistent_read_snapshot();
        }

        match preparation? {
            Some(prepared) => self.commit_posix_bulk_local_writes(prepared),
            None => self.posix_write_files_local_eventual_fallback(files),
        }
    }

    pub fn posix_create_files_local_eventual_with_known_parents<'a, I>(
        &mut self,
        files: I,
    ) -> Result<Vec<DfsStat>>
    where
        I: IntoIterator<Item = (&'a str, &'a [u8])>,
    {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let files = files.into_iter().collect::<Vec<_>>();
        for attempt in 0..POSIX_BULK_PARENT_RETRY_ATTEMPTS {
            match self
                .posix_create_files_local_eventual_with_known_parents_once(&files, attempt > 0)
            {
                Err(DfsPosixError::Conflict { .. })
                    if attempt + 1 < POSIX_BULK_PARENT_RETRY_ATTEMPTS => {}
                result => return result,
            }
        }
        unreachable!("bounded bulk-parent retry loop always returns")
    }

    fn posix_create_files_local_eventual_with_known_parents_once<'a>(
        &mut self,
        files: &[(&'a str, &'a [u8])],
        require_authoritative_parents: bool,
    ) -> Result<Vec<DfsStat>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        if files.is_empty() {
            return Ok(Vec::new());
        }
        let mut prepared = Vec::with_capacity(files.len());
        let mut seen_objects = BTreeSet::new();
        let mut seen_paths = BTreeSet::new();
        let mut resolved_parents = BTreeMap::<String, Rc<PosixBulkParentTouch>>::new();
        let metadata_shard_snapshot = self.refresh_sifs_metadata_shards()?;
        for &(path, bytes) in files {
            let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
            if requires_directory {
                return Err(DfsPosixError::IsDirectory { path });
            }
            let Some(parent) = parent_posix_path(&path) else {
                return Err(DfsPosixError::IsDirectory { path });
            };
            let parent_touch = match resolved_parents.get(&parent) {
                Some(resolved) => Rc::clone(resolved),
                None => {
                    let (resolved_parent, parent_object) =
                        self.resolve_posix_bulk_parent(&parent, require_authoritative_parents)?;
                    let metadata_shard = match metadata_shard_snapshot.as_deref() {
                        Some(shards) => self.select_sifs_metadata_shard_for_path_from_snapshot(
                            &resolved_parent,
                            shards,
                        )?,
                        None => None,
                    };
                    let resolved = PosixBulkParentTouch::shared(
                        resolved_parent,
                        parent_object,
                        metadata_shard,
                    );
                    resolved_parents.insert(parent.clone(), Rc::clone(&resolved));
                    resolved
                }
            };
            let resolved_parent = &parent_touch.path;
            let Some((_, child_name)) = path.rsplit_once('/') else {
                return Err(DfsPosixError::InvalidPath { path });
            };
            let path = join_posix_path(resolved_parent, child_name);
            // Duplicate paths can be rejected locally. Existing-path
            // exclusivity is checked from one coherent shard snapshot by the
            // authoritative CommitLocalEventualBatch handler and fenced again
            // by namespace prepare/commit, so the requester must not reopen
            // metadata once per file.
            if !seen_paths.insert(path.clone()) {
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
            let target_version = next_version(&object.version);
            prepared.push(PosixBulkLocalWrite {
                path,
                object,
                target_version,
                expected_base_version: None,
                bytes,
                metadata_shard,
                parent_touch: Some(parent_touch),
                link_count_hint: Some(1),
            });
        }
        self.commit_posix_bulk_local_writes(prepared)
    }

    fn commit_posix_bulk_local_writes<'a>(
        &mut self,
        prepared: Vec<PosixBulkLocalWrite<'a>>,
    ) -> Result<Vec<DfsStat>> {
        let trace_started = Instant::now();
        let trace_object_count = prepared.len();
        let trace_content_bytes = prepared.iter().fold(0usize, |bytes, write| {
            bytes.saturating_add(write.bytes.len())
        });
        let content_started = Instant::now();
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
        let content_elapsed = content_started.elapsed();
        let prepare_setup_started = Instant::now();
        let write_time = now_unix_nanos();
        type MetadataAuthorityKey = Option<(NodeId, String, u64)>;
        struct PosixBulkAuthorityGroup {
            selected_shard: Option<DfsMetadataShardConfig>,
            objects: Vec<DfsObjectMetadata>,
            writes: Vec<DfsLocalEventualBatchWriteProof>,
        }

        let authority_key = |shard: Option<&DfsMetadataShardConfig>| {
            shard.map(|shard| {
                (
                    shard.primary_node_id.clone(),
                    shard.shard_id.clone(),
                    shard.epoch,
                )
            })
        };
        let mut groups = BTreeMap::<MetadataAuthorityKey, PosixBulkAuthorityGroup>::new();
        let mut unique_parent_touches = BTreeMap::<DfsObjectId, Rc<PosixBulkParentTouch>>::new();
        let mut stats = Vec::with_capacity(prepared.len());
        let mut stat_elapsed = Duration::ZERO;
        for (write, manifest) in prepared.into_iter().zip(manifests) {
            let PosixBulkLocalWrite {
                path,
                mut object,
                target_version,
                expected_base_version,
                bytes: _,
                metadata_shard,
                parent_touch,
                link_count_hint,
            } = write;
            object.version = target_version;
            object.content_hash = Some(manifest.content_hash);
            object.content_size_bytes = Some(manifest.size_bytes);
            object.current_locations = vec![self.node_id.clone()];
            object.modified_at_unix_nanos = write_time;
            object.changed_at_unix_nanos = write_time;
            object.last_modified_by_event_id = Some(EventId::generate());

            let stat_started = Instant::now();
            stats.push(self.stat_object_for_path_with_link_count_hint(
                &object,
                &path,
                link_count_hint,
            ));
            stat_elapsed = stat_elapsed.saturating_add(stat_started.elapsed());

            let metadata_authority = authority_key(metadata_shard.as_ref());
            let authority_node_id = metadata_shard
                .as_ref()
                .map(|shard| shard.primary_node_id.clone())
                .unwrap_or_else(|| self.node_id.clone());
            if let Some(parent) = parent_touch.as_ref()
                && !unique_parent_touches.contains_key(&parent.object.object_id)
            {
                unique_parent_touches.insert(parent.object.object_id.clone(), Rc::clone(parent));
            }
            let expected_parent_object_id = parent_touch
                .as_ref()
                .map(|parent| parent.object.object_id.clone());
            let expected_parent_version = parent_touch
                .as_ref()
                .map(|parent| parent.object.version.clone());
            let proof = DfsLocalEventualBatchWriteProof {
                expected_base_version,
                expected_parent_object_id,
                expected_parent_version,
                receipt: DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: object.file_system_id.clone(),
                        object_id: object.object_id.clone(),
                        committed_version: object.version.clone(),
                        owner_node_id: authority_node_id,
                        event_id: object
                            .last_modified_by_event_id
                            .clone()
                            .expect("bulk committed objects carry a write event"),
                    },
                    replica_node_id: self.node_id.clone(),
                    content_hash: object.content_hash.clone(),
                    received_at: time::OffsetDateTime::now_utc(),
                    // Each bulk write names a new version, whose slots have
                    // never been withdrawn.
                    generation: 0,
                },
            };
            let group =
                groups
                    .entry(metadata_authority)
                    .or_insert_with(|| PosixBulkAuthorityGroup {
                        selected_shard: metadata_shard,
                        objects: Vec::new(),
                        writes: Vec::new(),
                    });
            group.objects.push(object);
            group.writes.push(proof);
        }
        for (_, parent_touch) in unique_parent_touches {
            let parent_authority = authority_key(parent_touch.metadata_shard.as_ref());
            groups
                .entry(parent_authority.clone())
                .or_insert_with(|| PosixBulkAuthorityGroup {
                    selected_shard: parent_touch.metadata_shard.clone(),
                    objects: Vec::new(),
                    writes: Vec::new(),
                });
            let parent = &parent_touch.object;
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
            // This is the version observed while resolving the parent. The
            // write proof carries that exact predecessor to the metadata
            // authority; a mismatch rejects the whole shard group and
            // triggers an authoritative parent refresh plus bounded retry.
            let mut parent = parent.clone();
            parent.version = next_version(&parent.version);
            parent.modified_at_unix_nanos = write_time;
            parent.changed_at_unix_nanos = write_time;
            parent.last_modified_by_event_id = Some(EventId::generate());
            groups
                .get_mut(&parent_authority)
                .expect("parent authority group was installed with its write proof")
                .objects
                .push(parent);
        }
        let prepare_setup_elapsed = prepare_setup_started.elapsed().saturating_sub(stat_elapsed);
        let authority_group_count = groups.len();
        let mut group_prepare_elapsed = Duration::ZERO;
        let mut authority_commit_elapsed = Duration::ZERO;
        let mut committed_metadata_paths = BTreeSet::new();
        let mut remaining_groups = groups.into_iter();
        while let Some((_metadata_authority, group)) = remaining_groups.next() {
            let group_prepare_started = Instant::now();
            let PosixBulkAuthorityGroup {
                selected_shard,
                objects,
                writes,
            } = group;
            let trace_object_count = objects.len();
            let group_metadata_paths = objects
                .iter()
                .map(|object| object.path.clone())
                .collect::<BTreeSet<_>>();
            let proofs_by_object = writes
                .iter()
                .map(|write| (&write.receipt.receipt.object_id, write))
                .collect::<BTreeMap<_, _>>();
            let versions = objects
                .iter()
                .map(|object| {
                    let mut version =
                        crate::object_version_record(object, object.content_size_bytes);
                    if let Some(proof) = proofs_by_object.get(&object.object_id) {
                        version.content_durability = Some(
                            crate::content_durability::from_local_receipt(&proof.receipt),
                        );
                    }
                    version
                })
                .collect::<Vec<_>>();
            let transaction = DfsNamespaceTransaction {
                content_reference_identity: None,
                extension_expectations: Vec::new(),
                contribution_digest: None,
                file_version_pins: None,
                transaction_id: crate::namespace_transaction_id_for_batch(
                    "visible-batch",
                    &sifs_file_system_id(),
                    &objects,
                    &versions,
                ),
                file_system_id: sifs_file_system_id(),
                objects,
                versions,
                advisory_lock_states: Vec::new(),
                created_at: time::OffsetDateTime::now_utc(),
            };
            let authority_node_id = selected_shard
                .as_ref()
                .map(|shard| shard.primary_node_id.clone())
                .unwrap_or_else(|| self.node_id.clone());
            let trace_write_count = writes.len();
            let group_prepare_duration = group_prepare_started.elapsed();
            group_prepare_elapsed = group_prepare_elapsed.saturating_add(group_prepare_duration);
            let trace_shard_id = selected_shard
                .as_ref()
                .map(|shard| shard.shard_id.clone())
                .unwrap_or_else(|| "local-unsharded".to_string());
            let trace_remote = authority_node_id != self.node_id;
            let authority_commit_started = Instant::now();
            let result = if authority_node_id != self.node_id
                || self.transport.delegates_local_namespace_authority()
            {
                // A client shell delegates the same-node group to Main too.
                // Its private committed-work bits cannot wake Main's durable
                // metadata publication and repair owners.
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
                    writes,
                    selected_shard,
                )
                .map(drop)
            };
            let authority_commit_duration = authority_commit_started.elapsed();
            authority_commit_elapsed =
                authority_commit_elapsed.saturating_add(authority_commit_duration);
            crate::dfs_perf_trace(format_args!(
                "posix-bulk-authority-commit node={} shard={} remote={} objects={} writes={} prepare_us={} commit_ms={} success={}",
                self.node_id,
                trace_shard_id,
                trace_remote,
                trace_object_count,
                trace_write_count,
                group_prepare_duration.as_micros(),
                authority_commit_duration.as_millis(),
                result.is_ok()
            ));
            if let Err(source) = result {
                // An authoritative create-only rejection proves that this
                // shard group committed nothing. Preserve O_EXCL/EEXIST when
                // no earlier shard group was committed; once earlier groups
                // are visible the caller still needs the partial-commit
                // boundary.
                if committed_metadata_paths.is_empty()
                    && matches!(
                        &source,
                        DfsPosixError::AlreadyExists { .. } | DfsPosixError::Conflict { .. }
                    )
                {
                    return Err(source);
                }
                let unattempted_metadata_paths = remaining_groups
                    .flat_map(|(_, group)| group.objects.into_iter().map(|object| object.path))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                return Err(DfsPosixError::PartialBulkCommit {
                    committed_metadata_paths: committed_metadata_paths.into_iter().collect(),
                    indeterminate_metadata_paths: group_metadata_paths.into_iter().collect(),
                    unattempted_metadata_paths,
                    source: Box::new(source),
                });
            }
            committed_metadata_paths.extend(group_metadata_paths);
        }
        let stats = stats.into_iter().collect::<Result<Vec<_>>>()?;
        crate::dfs_perf_trace(format_args!(
            "posix-bulk-local-write node={} objects={} content_bytes={} authority_groups={} content_ms={} prepare_setup_ms={} group_prepare_ms={} authority_commit_ms={} stat_ms={} total_ms={}",
            self.node_id,
            trace_object_count,
            trace_content_bytes,
            authority_group_count,
            content_elapsed.as_millis(),
            prepare_setup_elapsed.as_millis(),
            group_prepare_elapsed.as_millis(),
            authority_commit_elapsed.as_millis(),
            stat_elapsed.as_millis(),
            trace_started.elapsed().as_millis()
        ));
        Ok(stats)
    }

    fn posix_write_files_local_eventual_fallback<'a>(
        &mut self,
        files: &[(&'a str, &'a [u8])],
    ) -> Result<Vec<DfsStat>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_from_reader_checked_parent(path, reader, options, true, true, None)
    }

    /// Reader-backed write under a durable caller-owned operation identity.
    /// The reader is captured once into immutable content before any routed
    /// retry can occur.
    pub fn posix_write_file_from_reader_with_operation_id<R: Read>(
        &mut self,
        path: &str,
        reader: R,
        options: DfsWriteOptions,
        operation_id: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_from_reader_checked_parent_with_operation_id(
            path,
            reader,
            options,
            true,
            true,
            None,
            Some(operation_id.to_string()),
        )
    }

    /// Exclusively create a file from a reader under a durable caller-owned
    /// operation identity. The reader is captured once; response recovery and
    /// authority redirects reuse immutable DFS content.
    pub fn posix_create_file_from_reader_with_operation_id<R: Read>(
        &mut self,
        path: &str,
        reader: R,
        options: DfsWriteOptions,
        operation_id: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_from_reader_checked_parent_with_operation_id_and_mode(
            path,
            reader,
            options,
            true,
            true,
            None,
            true,
            None,
            Some(operation_id.to_string()),
        )
    }

    /// Replace an exact object/version snapshot from a reader under a durable
    /// operation identity. A recreated path or stale visible version is a
    /// conflict and is never refreshed into a blind overwrite.
    pub fn posix_replace_file_if_version_from_reader_with_operation_id<R: Read>(
        &mut self,
        path: &str,
        expected_object_id: &DfsObjectId,
        expected_version: &DfsVersion,
        reader: R,
        options: DfsWriteOptions,
        operation_id: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_from_reader_checked_parent_with_operation_id_and_mode(
            path,
            reader,
            options,
            true,
            false,
            None,
            false,
            Some((expected_object_id, expected_version)),
            Some(operation_id.to_string()),
        )
    }

    pub fn posix_write_file_from_reader_with_create_attrs<R: Read>(
        &mut self,
        path: &str,
        reader: R,
        options: DfsWriteOptions,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_from_reader_checked_parent(
            path,
            reader,
            options,
            true,
            true,
            Some(attrs),
        )
    }

    pub fn posix_write_file_from_reader_with_create_attrs_and_operation_id<R: Read>(
        &mut self,
        path: &str,
        reader: R,
        options: DfsWriteOptions,
        attrs: DfsPosixCreateAttrs,
        operation_id: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_from_reader_checked_parent_with_operation_id(
            path,
            reader,
            options,
            true,
            true,
            Some(attrs),
            Some(operation_id.to_string()),
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_bytes_checked_parent_with_operation_id(
            path,
            bytes,
            options,
            check_parent,
            touch_parent_on_create,
            replay_response_journal,
            create_attrs,
            exclusive_create,
            follow_final_for_exclusive_create,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn posix_write_file_bytes_checked_parent_with_operation_id(
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
        operation_id: Option<String>,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_bytes_checked_parent_attempts(
            path,
            bytes,
            options,
            check_parent,
            touch_parent_on_create,
            replay_response_journal,
            create_attrs,
            exclusive_create,
            follow_final_for_exclusive_create,
            operation_id,
            true,
        )?
        .ok_or_else(|| DfsPosixError::Conflict {
            path: path.to_string(),
            reason: "committed write did not report its stat".into(),
        })
    }

    /// Commit a write whose caller discards the resulting stat. The commit,
    /// its parent validation and its durability are those of
    /// [`Self::posix_write_file`]; only the report is omitted, which for a
    /// caller without a metadata store is a separate authoritative read.
    pub(crate) fn posix_write_file_committed(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<()> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_bytes_checked_parent_attempts(
            path, bytes, options, true, true, true, None, false, false, None, false,
        )
        .map(drop)
    }

    /// Run write attempts until one commits. `report_stat` selects whether the
    /// committed head is reported; `None` is returned exactly when it is not.
    #[allow(clippy::too_many_arguments)]
    fn posix_write_file_bytes_checked_parent_attempts(
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
        operation_id: Option<String>,
        report_stat: bool,
    ) -> Result<Option<DfsStat>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        if let Some(operation_id) = operation_id.as_deref() {
            crate::service_routed_write::validate_write_operation_id(operation_id)?;
        }
        // The caller's operation deadline, if it installed one, is the only
        // bound in time on the write and on its serialization retries.
        let write_deadline = self.transport.operation_deadline();
        let mut resend = POSIX_WRITE_SERIALIZATION_RESEND_FIRST;
        {
            loop {
                // Resolution, parent validation, and terminal lookup overlap
                // heavily for a single write. Reuse their exact observations
                // within one attempt; every metadata mutation below clears the
                // cache before later reads can observe stale state.
                let attempt = self.with_metadata_lookup_cache_mut(|service| {
                    let commit = service.posix_write_file_bytes_once_commit(
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
                        operation_id.as_deref(),
                        None,
                        Default::default(),
                        None,
                        None,
                    )?;
                    if report_stat {
                        service.posix_write_commit_stat(commit).map(Some)
                    } else {
                        Ok(None)
                    }
                });
                match attempt {
                    Ok(stat) => return Ok(stat),
                    Err(error)
                        if posix_error_is_retryable_write_serialization(&error)
                            || (!exclusive_create
                                && posix_error_is_concurrent_create_winner(&error)) =>
                    {
                        self.check_cancelled("POSIX blind-write serialization retry")?;
                        // A caller's enclosing lookup scope outlives this
                        // attempt; the retry must plan from fresh observations.
                        self.clear_metadata_lookup_cache();
                        let now = Instant::now();
                        if write_deadline.is_some_and(|deadline| now >= deadline) {
                            return Err(error);
                        }
                        let wait = write_deadline.map_or(resend, |deadline| {
                            resend.min(deadline.saturating_duration_since(now))
                        });
                        self.wait_for_resend(wait);
                        resend = resend
                            .saturating_mul(2)
                            .min(POSIX_WRITE_SERIALIZATION_RESEND_MAX);
                        self.hydrate_from_metadata_backend()?;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_bytes_checked_parent_once_with_operation_id(
            path,
            bytes,
            options,
            check_parent,
            touch_parent_on_create,
            replay_response_journal,
            create_attrs,
            exclusive_create,
            follow_final_for_exclusive_create,
            expected_existing,
            resolve_links,
            None,
            None,
            Default::default(),
        )
    }

    fn posix_write_parent_from_main(&mut self, path: &str) -> Result<DfsObjectMetadata> {
        let parent = parent_posix_path(path)
            .ok_or_else(|| DfsPosixError::IsDirectory { path: path.into() })?;
        match self.lookup_sifs_path_direct_distributed_synced(&parent)? {
            Some(object) if object.kind == DfsObjectKind::Directory => Ok(object),
            Some(object) if object.kind != DfsObjectKind::Tombstone => {
                Err(DfsPosixError::NotDirectory { path: parent })
            }
            _ => Err(DfsPosixError::MissingParent { path: parent }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn posix_write_file_bytes_checked_parent_once_with_operation_id(
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
        operation_id: Option<&str>,
        opened_parent: Option<&DfsObjectMetadata>,
        publication: crate::DfsWritePublication,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let commit = self.posix_write_file_bytes_once_commit(
            path,
            bytes,
            options,
            check_parent,
            touch_parent_on_create,
            replay_response_journal,
            create_attrs,
            exclusive_create,
            follow_final_for_exclusive_create,
            expected_existing,
            resolve_links,
            operation_id,
            opened_parent,
            publication,
            None,
            None,
        )?;
        self.posix_write_commit_stat(commit)
    }

    /// The stat a committed write reports. A caller without a metadata store
    /// reads the committed head and its link count from the authorities.
    fn posix_write_commit_stat(&mut self, commit: PosixWriteCommit) -> Result<DfsStat> {
        match commit {
            PosixWriteCommit::Stat(stat) => Ok(stat),
            PosixWriteCommit::AuthorityHead {
                file_system_id,
                object_id,
                path,
            } => self
                .with_stat_deadline(|service| {
                    let authoritative = service
                        .current_object_from_authority(&file_system_id, &object_id)?
                        .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
                    service.with_stat_read(|service| {
                        service.stat_object_from_authorities(&authoritative, &path, None)
                    })
                })
                .map_err(metadata::committed_write_observation_error),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn posix_write_file_bytes_once_commit(
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
        operation_id: Option<&str>,
        opened_parent: Option<&DfsObjectMetadata>,
        publication: crate::DfsWritePublication,
        requester: Option<&NodeId>,
        staged: Option<crate::ContentStoreQuarantinedBlob>,
    ) -> Result<PosixWriteCommit> {
        let write_requester = requester.unwrap_or(&self.node_id).clone();
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let plans_from_authority = !self.plans_write_from_local_metadata(&path)?;
        let authoritative_parent =
            if check_parent && opened_parent.is_none() && plans_from_authority {
                Some(self.posix_write_parent_from_main(&path)?)
            } else {
                None
            };
        let opened_parent = opened_parent.or(authoritative_parent.as_ref());
        if check_parent {
            if let Some(parent) = opened_parent {
                // The no-link caller already checked this exact authoritative
                // parent. A remote observation need not exist in its mirror.
                if parent.file_system_id != sifs_file_system_id()
                    || parent.kind != DfsObjectKind::Directory
                    || parent_posix_path(&path).as_deref() != Some(parent.path.as_str())
                {
                    return Err(DfsPosixError::Conflict {
                        path: path.clone(),
                        reason: "opened parent does not belong to this exclusive create".into(),
                    });
                }
            } else {
                self.ensure_parent_directory(&path)?;
            }
        }
        let trace_started = Instant::now();
        let trace_path = path.clone();
        let content_size_bytes = staged
            .as_ref()
            .map_or(bytes.len() as u64, |blob| blob.size_bytes);
        let trace_bytes = content_size_bytes;
        let mut transient_create = false;
        let current_object = if plans_from_authority {
            self.lookup_sifs_path_direct_distributed_synced(&path)?
        } else {
            self.lookup_sifs_path(&path)?
        };
        let mut object = match current_object {
            Some(object)
                if exclusive_create
                    && object.kind != DfsObjectKind::Tombstone
                    && operation_id.is_some_and(|operation_id| {
                        object.version
                            == stable_write_target_version(
                                &self.node_id,
                                &object.object_id,
                                operation_id,
                            )
                    }) =>
            {
                // Exact exclusive-create retry after a lost response. The
                // durable operation admission below proves payload identity;
                // a different operation still takes the ordinary EEXIST arm.
                object
            }
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
                self.resolve_hardlink_target_distributed(&object, None)?
            }
            Some(object)
                if object.kind == DfsObjectKind::Tombstone && expected_existing.is_some() =>
            {
                return Err(DfsPosixError::NotFound { path });
            }
            Some(object) if object.kind == DfsObjectKind::Tombstone => {
                let mut object =
                    recreated_posix_metadata_object(&write_requester, &path, DfsObjectKind::File);
                let attrs = create_attrs.unwrap_or_else(DfsPosixCreateAttrs::file_default);
                let attrs = match opened_parent {
                    Some(parent) => inherit_posix_create_attrs(attrs, false, parent.mode, parent.gid),
                    None => self.posix_create_attrs_for_object_authority(&object, attrs, false)?,
                };
                apply_posix_create_attrs(&mut object, attrs);
                if let Some(operation_id) = operation_id {
                    object.object_id =
                        stable_create_object_id(&write_requester, &path, operation_id);
                }
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
                    new_posix_metadata_object(&write_requester, &path, DfsObjectKind::File);
                let attrs = create_attrs.unwrap_or_else(DfsPosixCreateAttrs::file_default);
                let attrs = match opened_parent {
                    Some(parent) => inherit_posix_create_attrs(attrs, false, parent.mode, parent.gid),
                    None => self.posix_create_attrs_for_object_authority(&object, attrs, false)?,
                };
                apply_posix_create_attrs(&mut object, attrs);
                if let Some(operation_id) = operation_id {
                    object.object_id =
                        stable_create_object_id(&write_requester, &path, operation_id);
                }
                self.add_transient_object_metadata(object.clone())?;
                transient_create = true;
                object
            }
        };
        if let Err(error) =
            self.apply_managed_policy_to_new_object(&mut object, options.replica_candidates.len())
        {
            if transient_create {
                self.remove_transient_object_metadata(&object.object_id);
            }
            return Err(error.into());
        }
        let target_version = operation_id
            .map(|operation_id| {
                stable_write_target_version(&write_requester, &object.object_id, operation_id)
            })
            .unwrap_or_else(|| next_version(&object.version));
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
            if &object.version != expected_version && object.version != target_version {
                return Err(DfsPosixError::Conflict {
                    path,
                    reason: format!(
                        "file changed since it was opened: expected version {expected_version}, found {}",
                        object.version
                    ),
                });
            }
        }
        // This fact comes from the successful namespace lookup/allocation
        // above; the transient overlay index alone cannot prove creation.
        let creation = DfsWriteCreation::from_namespace_resolution(
            exclusive_create,
            transient_create && !options.foreground_replica_repair,
        );
        let candidates = match self.posix_replica_candidates_for_write_creation(
            &object,
            &options,
            false,
            creation,
            creation.is_fresh_namespace().then_some(&target_version),
        ) {
            Ok(candidates) => candidates,
            Err(error) => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                return Err(error.into());
            }
        };
        let committed_locations = candidates.clone();
        #[cfg(any(test, feature = "test-support"))]
        let mut observed_content_shard = None;
        let remote_metadata_primary = match self.select_metadata_shard_for_object(&object) {
            Ok(Some(shard)) => {
                #[cfg(any(test, feature = "test-support"))]
                {
                    observed_content_shard =
                        Some(crate::DfsMetadataShardId::new(shard.shard_id.clone()));
                }
                shard.primary_node_id != self.node_id
            }
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
        let streamed_remote_primary_write = remote_metadata_primary;
        let content_started = Instant::now();
        let manifest = if let Some(blob) = staged {
            if streamed_remote_primary_write {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                return Err(DfsError::Metadata {
                    message: "received path write no longer belongs to this primary".into(),
                }
                .into());
            }
            match self.content.bind_quarantined_blob(
                object.object_id.as_str(),
                target_version.as_str(),
                blob,
            ) {
                Ok(manifest) => Some(manifest),
                Err(error) => {
                    if transient_create {
                        self.remove_transient_object_metadata(&object.object_id);
                    }
                    return Err(DfsPosixError::Dfs(error.into()));
                }
            }
        } else if streamed_remote_primary_write {
            None
        } else {
            match self.content.put_object_version_blob_only(
                object.object_id.as_str(),
                target_version.as_str(),
                bytes,
            ) {
                Ok(manifest) => Some(manifest),
                Err(error) => {
                    if transient_create {
                        self.remove_transient_object_metadata(&object.object_id);
                    }
                    return Err(DfsPosixError::Dfs(error.into()));
                }
            }
        };
        #[cfg(any(test, feature = "test-support"))]
        if let (Some(shard), Some(manifest)) = (observed_content_shard.as_ref(), manifest.as_ref())
        {
            self.observe_native_content_persistence(
                shard,
                &object.file_system_id,
                manifest,
                crate::native_shard_trace::NativeContentPersistenceKindV1::BlobOnly,
            );
        }
        let content_elapsed = content_started.elapsed();
        let distributed_started = Instant::now();
        let request = DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version: target_version.clone(),
            requesting_node_id: write_requester,
        };
        let outcome = if streamed_remote_primary_write {
            self.write_distributed_bytes_with_base(
                request,
                (!transient_create).then(|| object.version.clone()),
                bytes.to_vec(),
                candidates,
                options.policy,
                DfsDistributedWriteOptions {
                    publication: publication.clone(),
                    touch_parent_on_create,
                    creation,
                    repair: if options.foreground_replica_repair {
                        DfsWriteRepair::Foreground
                    } else {
                        DfsWriteRepair::Background
                    },
                    dispatch: if detach_request {
                        DfsWriteDispatch::Detached
                    } else if options.wait_for_commit {
                        DfsWriteDispatch::AwaitCommit
                    } else {
                        DfsWriteDispatch::AwaitResponse
                    },
                    replay_response_journal,
                    operation_id: operation_id.map(ToOwned::to_owned),
                },
            )
        } else {
            let manifest = manifest
                .as_ref()
                .expect("local metadata-primary write published a blob-only manifest");
            self.write_distributed_manifest_with_candidates(
                request,
                (!transient_create).then(|| object.version.clone()),
                manifest.content_hash.clone(),
                manifest.size_bytes,
                candidates,
                options.policy,
                DfsDistributedWriteOptions {
                    publication: publication.clone(),
                    touch_parent_on_create,
                    creation,
                    repair: if options.foreground_replica_repair {
                        DfsWriteRepair::Foreground
                    } else {
                        DfsWriteRepair::Background
                    },
                    dispatch: if detach_request {
                        DfsWriteDispatch::Detached
                    } else if options.wait_for_commit {
                        DfsWriteDispatch::AwaitCommit
                    } else {
                        DfsWriteDispatch::AwaitResponse
                    },
                    replay_response_journal,
                    operation_id: operation_id.map(ToOwned::to_owned),
                },
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
            streamed_remote_primary_write,
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
                if self.transport.delegates_local_namespace_authority()
                    && (!remote_metadata_primary || self.metadata_backend.is_none())
                {
                    // Main owns the committed object and parent update. Its
                    // validated reply retired the planning overlay; use the
                    // actual durable head rather than rebuilding that overlay.
                    // A caller without a metadata store keeps no overlay of a
                    // committed write either: the authority holds the head, and
                    // nothing would retire an overlay once the object changes.
                    if self.metadata_backend.is_none() {
                        self.remove_transient_object_metadata(&object.object_id);
                        return Ok(PosixWriteCommit::AuthorityHead {
                            file_system_id: object.file_system_id,
                            object_id: object.object_id,
                            path,
                        });
                    }
                    let authoritative = self.stat_target_for_path(&path)?;
                    return self
                        .stat_object_for_path(&authoritative, &path)
                        .map(PosixWriteCommit::Stat);
                }
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
                committed_object.content_size_bytes = Some(content_size_bytes);
                let now = now_unix_nanos();
                committed_object.modified_at_unix_nanos = now;
                committed_object.changed_at_unix_nanos = now;
                committed_object.last_modified_by_event_id = Some(EventId::generate());
                self.add_transient_object_metadata(committed_object.clone())?;
                if transient_create
                    && touch_parent_on_create
                    && !self.transport.delegates_local_namespace_authority()
                    && !self
                        .coordinator
                        .intent(&intent_id)
                        .is_some_and(|intent| intent.touch_parent_on_create)
                    && self.is_local_metadata_primary_for_object(&object)?
                {
                    self.touch_parent_directory_modified(&path)?;
                }
                if !options.foreground_replica_repair {
                    if transient_create && remote_metadata_primary {
                        return self.committed_write_stat(&path).map(PosixWriteCommit::Stat);
                    }
                    return self
                        .stat_object_for_path(&committed_object, &path)
                        .map(PosixWriteCommit::Stat);
                }
                self.committed_write_stat(&path).map(PosixWriteCommit::Stat)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } if !options.wait_for_commit && !options.foreground_replica_repair => {
                let mut pending_object = object.clone();
                pending_object.version = target_version;
                pending_object.content_hash = manifest
                    .as_ref()
                    .map(|manifest| manifest.content_hash.clone());
                pending_object.current_locations = if streamed_remote_primary_write {
                    Vec::new()
                } else {
                    vec![self.node_id.clone()]
                };
                pending_object.content_size_bytes = Some(content_size_bytes);
                let now = now_unix_nanos();
                pending_object.modified_at_unix_nanos = now;
                pending_object.changed_at_unix_nanos = now;
                pending_object.last_modified_by_event_id = Some(EventId::generate());
                self.add_transient_object_metadata(pending_object.clone())?;
                if transient_create
                    && touch_parent_on_create
                    && !self.transport.delegates_local_namespace_authority()
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
                    .map(PosixWriteCommit::Stat)
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
                Ok(PosixWriteCommit::Stat(DfsStat {
                    file_system_id: object.file_system_id,
                    path,
                    object_id: object.object_id,
                    kind: object.kind,
                    mode: object.mode,
                    uid: object.uid,
                    gid: object.gid,
                    size_bytes: Some(content_size_bytes),
                    link_count: 1,
                    version: target_version,
                    modified_at_unix_nanos: object.modified_at_unix_nanos,
                    accessed_at_unix_nanos: object.accessed_at_unix_nanos,
                    created_at_unix_nanos: object.created_at_unix_nanos,
                    changed_at_unix_nanos: object.changed_at_unix_nanos,
                    content_hash: manifest
                        .as_ref()
                        .map(|manifest| manifest.content_hash.clone()),
                    policy: object.policy,
                    current_locations: object.current_locations,
                }))
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_from_reader_checked_parent_with_operation_id(
            path,
            reader,
            options,
            check_parent,
            touch_parent_on_create,
            create_attrs,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn posix_write_file_from_reader_checked_parent_with_operation_id<R: Read>(
        &mut self,
        path: &str,
        reader: R,
        options: DfsWriteOptions,
        check_parent: bool,
        touch_parent_on_create: bool,
        create_attrs: Option<DfsPosixCreateAttrs>,
        operation_id: Option<String>,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_from_reader_checked_parent_with_operation_id_and_mode(
            path,
            reader,
            options,
            check_parent,
            touch_parent_on_create,
            create_attrs,
            false,
            None,
            operation_id,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn posix_write_file_from_reader_checked_parent_with_operation_id_and_mode<R: Read>(
        &mut self,
        path: &str,
        reader: R,
        options: DfsWriteOptions,
        check_parent: bool,
        touch_parent_on_create: bool,
        create_attrs: Option<DfsPosixCreateAttrs>,
        exclusive_create: bool,
        expected_existing: Option<(&DfsObjectId, &DfsVersion)>,
        operation_id: Option<String>,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_write_file_from_reader_checked_parent_with_operation_id_and_mode_and_requester(
            path,
            reader,
            options,
            check_parent,
            touch_parent_on_create,
            create_attrs,
            exclusive_create,
            expected_existing,
            operation_id,
            None,
            None,
        )
    }

    /// Versioned internal stores use authoritative reads and need only the
    /// durable commit receipt, not a synchronously rebuilt requester mirror.
    /// Existing remote files are prepared from a fresh authority lookup so a
    /// lagging local mirror cannot create a false CAS conflict. Creation and
    /// local-primary writes retain the normal POSIX implementation.
    pub fn posix_write_internal_file_from_reader<R: Read>(
        &mut self,
        path: &str,
        mut reader: R,
        options: DfsWriteOptions,
        expected: Option<(&DfsObjectId, &DfsVersion)>,
        operation_id: &str,
    ) -> Result<DfsStat> {
        self.posix_write_internal_file_from_reader_impl(
            path,
            &mut reader,
            options,
            expected,
            operation_id,
        )
    }

    fn posix_write_internal_file_from_reader_impl(
        &mut self,
        path: &str,
        reader: &mut dyn Read,
        mut options: DfsWriteOptions,
        expected: Option<(&DfsObjectId, &DfsVersion)>,
        operation_id: &str,
    ) -> Result<DfsStat> {
        options.wait_for_commit = true;
        let mut phase = crate::service_trace::DfsPhaseTimer::new(operation_id);
        let path = normalize_posix_path(path)?;
        if let Some((object_id, version)) = expected
            && !options.foreground_replica_repair
            && let Some(shard) = self.routed_namespace_shard_for_path(&path)?
            && shard.primary_node_id != self.node_id
        {
            let mut lookup = self.route_authoritative_bulk_lookup_paths(
                &sifs_file_system_id(),
                shard.primary_node_id.clone(),
                BTreeSet::from([path.clone()]),
            )?;
            let mut object = lookup
                .exact_objects_by_path
                .remove(&path)
                .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
            let target_version =
                stable_write_target_version(&self.node_id, object_id, operation_id);
            if object.kind != DfsObjectKind::File
                || &object.object_id != object_id
                || (&object.version != version && object.version != target_version)
            {
                return Err(DfsPosixError::Conflict {
                    path,
                    reason: "internal file identity or revision changed".into(),
                });
            }
            self.ensure_object_not_in_cross_shard_flight(&object)?;
            let candidates =
                self.posix_replica_candidates_for_write_origin(&object, &options, true)?;
            phase.mark("internal_cas_planning");
            let manifest = self
                .content
                .put_object_version_from_reader(object_id.as_str(), target_version.as_str(), reader)
                .map_err(DfsError::from)?;
            phase.mark("internal_cas_staging");
            let outcome = self.route_distributed_manifest(
                DfsWriteRequest {
                    file_system_id: object.file_system_id.clone(),
                    object_id: object_id.clone(),
                    target_version,
                    requesting_node_id: self.node_id.clone(),
                },
                Some(version.clone()),
                manifest.content_hash.clone(),
                manifest.size_bytes,
                candidates.clone(),
                options.policy,
                object.clone(),
                shard,
                DfsDistributedWriteOptions {
                    operation_id: Some(operation_id.into()),
                    replay_response_journal: false,
                    repair: DfsWriteRepair::Background,
                    ..Default::default()
                },
            )?;
            phase.mark("internal_cas_dispatch");
            let outcome = self.settle_ready_write_outcome(outcome, false)?;
            phase.mark("internal_cas_settle");
            return match outcome {
                NetworkedDfsWriteOutcome::Committed {
                    version,
                    content_hash,
                    repair_jobs,
                    ..
                } => {
                    object.version = version;
                    object.content_hash = Some(content_hash);
                    object.content_size_bytes = Some(manifest.size_bytes);
                    object.current_locations = candidates
                        .into_iter()
                        .filter(|n| !repair_jobs.iter().any(|j| &j.missing_replica_node_id == n))
                        .collect();
                    self.stat_object_for_path(&object, &path)
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
            };
        }
        match expected {
            Some((object, version)) => self
                .posix_replace_file_if_version_from_reader_with_operation_id(
                    &path,
                    object,
                    version,
                    reader,
                    options,
                    operation_id,
                ),
            None => self.posix_create_file_from_reader_with_operation_id(
                &path,
                reader,
                options,
                operation_id,
            ),
        }
    }

    pub(crate) fn content_copy_destination_path(
        &mut self,
        request: &crate::DfsContentCopyRequest,
    ) -> Result<String> {
        let (path, requires_directory) =
            normalize_posix_path_with_trailing_directory(&request.destination_path)?;
        if requires_directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        let path =
            self.resolve_posix_path_distributed(&path, request.expected_destination.is_some())?;
        if request.expected_destination.is_some() {
            Ok(self.file_history_capture_object(&path)?.path)
        } else {
            Ok(path)
        }
    }

    pub(crate) fn posix_copy_file_content_for_requester(
        &mut self,
        path: &str,
        request: &crate::DfsContentCopyRequest,
        input: crate::service_file_restore::VerifiedContentInput,
        mut options: DfsWriteOptions,
        requester: &NodeId,
        operation_id: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        if self.content_copy_destination_path(request)? != path {
            return Err(DfsPosixError::Conflict {
                path: path.into(),
                reason: "restore destination resolution changed".into(),
            });
        }
        if self
            .routed_namespace_primary_for_path(path)?
            .is_some_and(|primary| primary != self.node_id)
        {
            return Err(DfsPosixError::Dfs(DfsError::Distributed { message: "restore destination authority moved; resume its durable job on the current primary".into() }));
        }
        options.wait_for_commit = true;
        let expected = request
            .expected_destination
            .as_ref()
            .map(|value| (&value.object_id, &value.version));
        self.posix_write_file_from_reader_checked_parent_with_operation_id_and_mode_and_requester(
            path,
            std::io::empty(),
            options,
            true,
            expected.is_none(),
            None,
            expected.is_none(),
            expected,
            Some(operation_id.into()),
            Some(requester),
            Some(input),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn posix_write_file_from_reader_checked_parent_with_operation_id_and_mode_and_requester<
        R: Read,
    >(
        &mut self,
        path: &str,
        mut reader: R,
        options: DfsWriteOptions,
        check_parent: bool,
        touch_parent_on_create: bool,
        create_attrs: Option<DfsPosixCreateAttrs>,
        exclusive_create: bool,
        expected_existing: Option<(&DfsObjectId, &DfsVersion)>,
        operation_id: Option<String>,
        requester: Option<&NodeId>,
        restore_input: Option<crate::service_file_restore::VerifiedContentInput>,
    ) -> Result<DfsStat> {
        self.posix_write_file_from_reader_checked_impl(
            path,
            &mut reader,
            options,
            check_parent,
            touch_parent_on_create,
            create_attrs,
            exclusive_create,
            expected_existing,
            operation_id,
            requester,
            restore_input,
        )
    }

    // All public reader entrypoints retain their generic API while sharing
    // this one checked write body across concrete reader and adapter types.
    #[allow(clippy::too_many_arguments)]
    fn posix_write_file_from_reader_checked_impl(
        &mut self,
        path: &str,
        reader: &mut dyn Read,
        options: DfsWriteOptions,
        check_parent: bool,
        touch_parent_on_create: bool,
        create_attrs: Option<DfsPosixCreateAttrs>,
        exclusive_create: bool,
        expected_existing: Option<(&DfsObjectId, &DfsVersion)>,
        operation_id: Option<String>,
        requester: Option<&NodeId>,
        restore_input: Option<crate::service_file_restore::VerifiedContentInput>,
    ) -> Result<DfsStat> {
        self.with_metadata_lookup_cache_mut(|service| {
            service.posix_write_file_from_reader_with_current_lookup_cache(
                path,
                reader,
                options,
                check_parent,
                touch_parent_on_create,
                create_attrs,
                exclusive_create,
                expected_existing,
                operation_id,
                requester,
                restore_input,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn posix_write_file_from_reader_with_current_lookup_cache(
        &mut self,
        path: &str,
        reader: &mut dyn Read,
        options: DfsWriteOptions,
        check_parent: bool,
        touch_parent_on_create: bool,
        create_attrs: Option<DfsPosixCreateAttrs>,
        exclusive_create: bool,
        expected_existing: Option<(&DfsObjectId, &DfsVersion)>,
        operation_id: Option<String>,
        requester: Option<&NodeId>,
        restore_input: Option<crate::service_file_restore::VerifiedContentInput>,
    ) -> Result<DfsStat> {
        let mut timing = crate::service_trace::DfsPhaseTimer::new(
            operation_id.as_deref().unwrap_or("reader-write"),
        );
        let _namespace_plan = self.namespace_execution.begin_planning();
        let write_requester = requester.cloned().unwrap_or_else(|| self.node_id.clone());
        if let Some(operation_id) = operation_id.as_deref() {
            crate::service_routed_write::validate_write_operation_id(operation_id)?;
        }
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path_distributed(&path, !exclusive_create)?;
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
        let plans_from_authority = !self.plans_write_from_local_metadata(&path)?;
        let authoritative_parent = if check_parent && plans_from_authority {
            Some(self.posix_write_parent_from_main(&path)?)
        } else {
            if check_parent {
                self.ensure_parent_directory(&path)?;
            }
            None
        };
        let current_object = if plans_from_authority {
            self.lookup_sifs_path_direct_distributed_synced(&path)?
        } else {
            self.lookup_sifs_path(&path)?
        };
        let mut transient_create = false;
        let mut object = match current_object {
            Some(object)
                if exclusive_create
                    && object.kind != DfsObjectKind::Tombstone
                    && operation_id.as_deref().is_some_and(|operation_id| {
                        object.version
                            == stable_write_target_version(
                                &write_requester,
                                &object.object_id,
                                operation_id,
                            )
                    }) =>
            {
                object
            }
            Some(object) if exclusive_create && object.kind != DfsObjectKind::Tombstone => {
                return Err(DfsPosixError::AlreadyExists { path });
            }
            Some(object) if object.kind == DfsObjectKind::Directory => {
                return Err(DfsPosixError::IsDirectory { path });
            }
            Some(object) if object.kind == DfsObjectKind::Hardlink => {
                self.resolve_hardlink_target_distributed(&object, None)?
            }
            Some(object)
                if object.kind == DfsObjectKind::Tombstone && expected_existing.is_some() =>
            {
                return Err(DfsPosixError::NotFound { path });
            }
            Some(object) if object.kind == DfsObjectKind::Tombstone => {
                let mut object =
                    recreated_posix_metadata_object(&write_requester, &path, DfsObjectKind::File);
                let attrs = create_attrs.unwrap_or_else(DfsPosixCreateAttrs::file_default);
                let attrs = match authoritative_parent.as_ref() {
                    Some(parent) => {
                        inherit_posix_create_attrs(attrs, false, parent.mode, parent.gid)
                    }
                    None => self.posix_create_attrs_for_object_authority(&object, attrs, false)?,
                };
                apply_posix_create_attrs(&mut object, attrs);
                if let Some(operation_id) = operation_id.as_deref() {
                    object.object_id =
                        stable_create_object_id(&write_requester, &path, operation_id);
                }
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
                    new_posix_metadata_object(&write_requester, &path, DfsObjectKind::File);
                let attrs = create_attrs.unwrap_or_else(DfsPosixCreateAttrs::file_default);
                let attrs = match authoritative_parent.as_ref() {
                    Some(parent) => {
                        inherit_posix_create_attrs(attrs, false, parent.mode, parent.gid)
                    }
                    None => self.posix_create_attrs_for_object_authority(&object, attrs, false)?,
                };
                apply_posix_create_attrs(&mut object, attrs);
                if let Some(operation_id) = operation_id.as_deref() {
                    object.object_id =
                        stable_create_object_id(&write_requester, &path, operation_id);
                }
                self.add_transient_object_metadata(object.clone())?;
                transient_create = true;
                object
            }
        };
        if let Err(error) =
            self.apply_managed_policy_to_new_object(&mut object, options.replica_candidates.len())
        {
            if transient_create {
                self.remove_transient_object_metadata(&object.object_id);
            }
            return Err(error.into());
        }
        let target_version = operation_id
            .as_deref()
            .map(|operation_id| {
                stable_write_target_version(&write_requester, &object.object_id, operation_id)
            })
            .unwrap_or_else(|| next_version(&object.version));
        let target_version_for_stat = target_version.clone();
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
            if &object.version != expected_version && object.version != target_version {
                return Err(DfsPosixError::Conflict {
                    path,
                    reason: format!(
                        "file changed since it was opened: expected version {expected_version}, found {}",
                        object.version
                    ),
                });
            }
        }
        let candidates = match self.posix_replica_candidates_for_write_origin(
            &object,
            &options,
            options.policy.min_commit_replicas == 1
                || restore_input.is_some()
                || exclusive_create
                || expected_existing.is_some(),
        ) {
            Ok(candidates) => candidates,
            Err(error) => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                return Err(error.into());
            }
        };
        let committed_locations = candidates.clone();
        let request = DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version,
            requesting_node_id: write_requester.clone(),
        };
        timing.mark("reader_planning");
        let outcome = if let Some(input) = restore_input {
            let operation_id = operation_id.ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Metadata {
                    message: "restore has no stable write identity".into(),
                })
            })?;
            match input.into_quarantined(&self.content) {
                Ok(blob) => self.write_stable_quarantined_distributed_blob(
                    request,
                    (!transient_create).then(|| object.version.clone()),
                    blob,
                    candidates,
                    options.policy,
                    operation_id,
                    DfsDistributedWriteOptions {
                        touch_parent_on_create,
                        creation: if exclusive_create {
                            DfsWriteCreation::Exclusive
                        } else {
                            DfsWriteCreation::Upsert
                        },
                        repair: if options.foreground_replica_repair {
                            DfsWriteRepair::Foreground
                        } else {
                            DfsWriteRepair::Background
                        },
                        ..Default::default()
                    },
                ),
                Err(source) => Err(source),
            }
        } else if exclusive_create || expected_existing.is_some() {
            let operation_id = operation_id.ok_or_else(|| {
                DfsPosixError::Dfs(DfsError::Serialization {
                    message: "reader-backed exclusive/CAS write requires an operation id"
                        .to_string(),
                })
            })?;
            self.write_stable_distributed_reader(
                request,
                (!transient_create).then(|| object.version.clone()),
                reader,
                candidates,
                options.policy,
                operation_id,
                DfsDistributedWriteOptions {
                    publication: Default::default(),
                    touch_parent_on_create,
                    creation: if exclusive_create {
                        DfsWriteCreation::Exclusive
                    } else {
                        DfsWriteCreation::Upsert
                    },
                    repair: if options.foreground_replica_repair {
                        DfsWriteRepair::Foreground
                    } else {
                        DfsWriteRepair::Background
                    },
                    ..DfsDistributedWriteOptions::default()
                },
            )
        } else {
            self.write_distributed_reader_with_base(
                request,
                (!transient_create).then(|| object.version.clone()),
                reader,
                candidates,
                options.policy,
                DfsDistributedWriteOptions {
                    publication: Default::default(),
                    touch_parent_on_create,
                    repair: if options.foreground_replica_repair {
                        DfsWriteRepair::Foreground
                    } else {
                        DfsWriteRepair::Background
                    },
                    operation_id,
                    ..DfsDistributedWriteOptions::default()
                },
            )
        };
        timing.mark("reader_dispatch");
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
        timing.mark("reader_settle");
        match outcome {
            NetworkedDfsWriteOutcome::Committed {
                intent_id,
                version,
                content_hash,
                repair_jobs,
            } => {
                if self.transport.delegates_local_namespace_authority()
                    && self.is_local_metadata_primary_for_object(&object)?
                {
                    if self.metadata_backend.is_none() {
                        return self.with_stat_deadline(|service| {
                            let authoritative = service
                                .current_object_from_authority(
                                    &object.file_system_id,
                                    &object.object_id,
                                )?
                                .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
                            service.with_stat_read(|service| {
                                service.stat_object_from_authorities(&authoritative, &path, None)
                            })
                        });
                    }
                    let authoritative = self.stat_target_for_path(&path)?;
                    return self.stat_object_for_path(&authoritative, &path);
                }
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
                    && !self.transport.delegates_local_namespace_authority()
                    && !self
                        .coordinator
                        .intent(&intent_id)
                        .is_some_and(|intent| intent.touch_parent_on_create)
                    && self.is_local_metadata_primary_for_object(&object)?
                {
                    self.touch_parent_directory_modified(&path)?;
                }
                if !options.foreground_replica_repair {
                    if transient_create {
                        return self.committed_write_stat(&path);
                    }
                    return self.stat_object_for_path(&committed_object, &path);
                }
                // The local view may not show the committed object; its
                // metadata authority does.
                self.committed_write_stat(&path)
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
                let mut stat = self.committed_write_stat(&path)?;
                if let Ok(manifest) = self
                    .content
                    .read_manifest(stat.object_id.as_str(), target_version_for_stat.as_str())
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
        }
    }

    pub fn posix_write_file_at_from_reader<R: Read>(
        &mut self,
        path: &str,
        offset: u64,
        mut reader: R,
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        self.posix_write_file_at_from_reader_impl(path, offset, &mut reader, options)
    }

    /// The current object a patch, write-at or truncate of the resolved `path`
    /// replaces: this service's own record when it plans writes from local
    /// metadata, otherwise the path authority's.
    fn posix_patch_base_object(&mut self, path: &str) -> Result<Option<DfsObjectMetadata>> {
        if self.plans_write_from_local_metadata(path)? {
            return self.lookup_sifs_path(path);
        }
        self.lookup_sifs_path_direct_distributed_synced(path)
    }

    /// Rejection for a patch whose path ends in a slash: IsDirectory when the
    /// path names a directory, NotDirectory otherwise.
    fn posix_patch_trailing_slash_error(&mut self, path: String) -> DfsPosixError {
        match self.posix_patch_base_object(&path) {
            Ok(Some(object)) if object.kind == DfsObjectKind::Directory => {
                DfsPosixError::IsDirectory { path }
            }
            Ok(_) => DfsPosixError::NotDirectory { path },
            Err(error) => error,
        }
    }

    /// The current bytes a patch rewrites. An object read from its authority
    /// is fetched as that authority described it, not re-resolved locally.
    fn posix_patch_current_bytes(
        &mut self,
        path: &str,
        object: &DfsObjectMetadata,
    ) -> Result<NetworkedDfsReadOutcome> {
        if self.plans_write_from_local_metadata(path)? {
            return self.read_current_object_or_fetch_remote(&object.object_id);
        }
        self.read_object_metadata_or_fetch_remote_without_cache(object)
    }

    fn posix_write_file_at_from_reader_impl(
        &mut self,
        path: &str,
        offset: u64,
        reader: &mut dyn Read,
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
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
            return Err(self.posix_patch_trailing_slash_error(path));
        }
        let object = self
            .posix_patch_base_object(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target_distributed(&object, None)?;
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
        let patch_outcome = if let Some(current_size) = object.content_size_bytes {
            if offset <= current_size {
                self.write_distributed_eventual_patch_from_reader_with_base(
                    request.clone(),
                    base_version.clone(),
                    offset,
                    &mut reader,
                    plan.desired_replica_nodes.clone(),
                    options.policy.clone(),
                    options.foreground_replica_repair,
                )?
            } else {
                // Represent a sparse POSIX extension as one streamed patch from
                // the old EOF. The zero-filled gap is generated in bounded
                // chunks and becomes an extent beside the unchanged base,
                // avoiding both a full-object read and an offset-sized Vec.
                let mut sparse_patch = std::io::repeat(0)
                    .take(offset - current_size)
                    .chain(&mut reader);
                self.write_distributed_eventual_patch_from_reader_with_base(
                    request.clone(),
                    base_version.clone(),
                    current_size,
                    &mut sparse_patch,
                    plan.desired_replica_nodes.clone(),
                    options.policy.clone(),
                    options.foreground_replica_repair,
                )?
            }
        } else {
            None
        };
        if let Some(outcome) = patch_outcome {
            let outcome = if options.wait_for_commit {
                self.settle_ready_write_outcome(outcome, options.foreground_replica_repair)?
            } else {
                outcome
            };
            return match outcome {
                NetworkedDfsWriteOutcome::Committed { .. } => {
                    self.committed_write_stat(&path)
                }
                NetworkedDfsWriteOutcome::Pending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                } if !options.wait_for_commit => {
                    let mut stat = self.committed_write_stat(&path)?;
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
            };
        }

        let current = match self.posix_patch_current_bytes(&path, &object)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => bytes,
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. } => {
                return Err(DfsError::Busy { path }.into());
            }
            NetworkedDfsReadOutcome::NotFound => return Err(DfsPosixError::NotFound { path }),
        };
        let mut overlay = RangedWriteOverlayReader::new(current, offset, reader);
        let outcome = self.write_distributed_reader_with_base(
            request,
            Some(base_version),
            &mut overlay,
            plan.desired_replica_nodes,
            options.policy,
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                repair: if options.foreground_replica_repair {
                    DfsWriteRepair::Foreground
                } else {
                    DfsWriteRepair::Background
                },
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )?;
        let result_size = overlay
            .result_size_bytes()
            .ok_or_else(|| DfsError::ContentStore {
                message: format!("POSIX write_at fallback did not consume its input for {path}"),
            })?;
        let outcome = if options.wait_for_commit {
            self.settle_ready_write_outcome(outcome, options.foreground_replica_repair)?
        } else {
            outcome
        };
        match outcome {
            NetworkedDfsWriteOutcome::Committed { .. } => {
                self.committed_write_stat(&path)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } if !options.wait_for_commit => {
                let mut stat = self.committed_write_stat(&path)?;
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
        }
    }

    pub fn posix_write_file_at(
        &mut self,
        path: &str,
        offset: u64,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        let object = self
            .current_read_object_metadata_distributed(object_id)
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if requires_directory {
            return Err(self.posix_patch_trailing_slash_error(path));
        }
        let object = self
            .posix_patch_base_object(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target_distributed(&object, None)?;
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
                    return self.committed_write_stat(&path);
                }
                NetworkedDfsWriteOutcome::Pending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                } if !options.wait_for_commit => {
                    let mut stat = self.committed_write_stat(&path)?;
                    stat.size_bytes = stat.size_bytes.map(|size| size.max(patch_end));
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
            }
        }
        let mut current = match self.posix_patch_current_bytes(&path, &object)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => bytes,
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. } => {
                return Err(DfsError::Busy { path }.into());
            }
            NetworkedDfsReadOutcome::NotFound => return Err(DfsPosixError::NotFound { path }),
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
        let outcome = self.write_distributed_bytes_with_base(
            request,
            Some(base_version),
            current,
            plan.desired_replica_nodes,
            options.policy,
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                creation: DfsWriteCreation::Upsert,
                repair: if options.foreground_replica_repair {
                    DfsWriteRepair::Foreground
                } else {
                    DfsWriteRepair::Background
                },
                dispatch: DfsWriteDispatch::AwaitResponse,
                replay_response_journal: true,
                operation_id: None,
            },
        )?;
        let outcome = if options.wait_for_commit {
            self.settle_ready_write_outcome(outcome, options.foreground_replica_repair)?
        } else {
            outcome
        };
        match outcome {
            NetworkedDfsWriteOutcome::Committed { .. } => {
                self.committed_write_stat(&path)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } if !options.wait_for_commit => {
                let mut stat = self.committed_write_stat(&path)?;
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
        }
    }

    pub fn posix_truncate_file(
        &mut self,
        path: &str,
        len: u64,
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        let object = self
            .current_read_object_metadata_distributed(object_id)
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if requires_directory {
            return Err(self.posix_patch_trailing_slash_error(path));
        }
        let object = self
            .posix_patch_base_object(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target_distributed(&object, None)?;
        if expected_object_id.is_some_and(|expected| expected != &object.object_id) {
            return Err(DfsPosixError::NotFound { path });
        }
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        let base_version = object.version.clone();
        let mut current = match self.posix_patch_current_bytes(&path, &object)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => bytes,
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. } => {
                return Err(DfsError::Busy { path }.into());
            }
            NetworkedDfsReadOutcome::NotFound => return Err(DfsPosixError::NotFound { path }),
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
        let outcome = self.write_distributed_bytes_with_base(
            request,
            Some(base_version),
            current,
            plan.desired_replica_nodes,
            options.policy,
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                creation: DfsWriteCreation::Upsert,
                repair: if options.foreground_replica_repair {
                    DfsWriteRepair::Foreground
                } else {
                    DfsWriteRepair::Background
                },
                dispatch: DfsWriteDispatch::AwaitResponse,
                replay_response_journal: true,
                operation_id: None,
            },
        )?;
        let outcome = if options.wait_for_commit {
            self.settle_ready_write_outcome(outcome, options.foreground_replica_repair)?
        } else {
            outcome
        };
        match outcome {
            NetworkedDfsWriteOutcome::Committed { .. } => {
                self.committed_write_stat(&path)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } if !options.wait_for_commit => {
                let mut stat = self.committed_write_stat(&path)?;
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

#[cfg(any(test, feature = "test-support"))]
pub use metadata::{
    posix_metadata_full_directory_collect_count, reset_posix_metadata_full_directory_collect_count,
};

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
    fn concurrent_create_winner_is_retried_only_by_nonexclusive_writes() {
        for error in [
            DfsPosixError::AlreadyExists {
                path: "/notes/shared.md".to_string(),
            },
            DfsPosixError::Dfs(DfsError::AlreadyExists {
                path: "/notes/shared.md".to_string(),
            }),
        ] {
            assert!(posix_error_is_concurrent_create_winner(&error));
            assert!(!posix_error_is_retryable_write_serialization(&error));
        }
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
