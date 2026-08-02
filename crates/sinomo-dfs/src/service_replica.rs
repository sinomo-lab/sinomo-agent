//! Replica command authorization and metadata-authority helpers.
//!
//! This module keeps shard-primary proof, local authority checks, and
//! replica cache updates together so transfer handlers can validate
//! inbound stage, repair, commit, and fetch commands through one service
//! boundary.

use sinomo_core::DfsObjectPolicy;
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::*;

const DFS_METADATA_AUTHORITY_PROBE_TIMEOUT: Duration = Duration::from_millis(250);
/// Bound authority-side preparation when an inbound served request carries no
/// caller deadline. Healthy operations finish far inside this cap; reaching it
/// means the optional all-replica fast path should yield to the exact quorum
/// fence instead of holding the served write lane indefinitely.
const DFS_METADATA_AUTHORITY_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
// A large streamed prepare needs longer than an ordinary probe on a healthy
// debug/WAN path, but this is only an optimization before the exact
// operation-bound cluster-quorum fence. Waiting five seconds for one wedged
// sibling made a single batch exceed seven seconds once the fallback ran.
const DFS_METADATA_AUTHORITY_LARGE_PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const DFS_METADATA_AUTHORITY_PROBE_IDLE_WAIT: Duration = Duration::from_millis(5);
const DFS_METADATA_AUTHORITY_LARGE_NAMESPACE_RECORDS: usize = 256;

static DFS_METADATA_SHARD_PUBLICATION_LOCKS: OnceLock<(Mutex<BTreeSet<String>>, Condvar)> =
    OnceLock::new();

#[cfg(test)]
thread_local! {
    static DFS_METADATA_AUTHORITY_BATCH_READS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static DFS_NAMESPACE_PREPARE_CANONICAL_DIGEST_PASSES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_namespace_prepare_canonical_digest_passes() {
    DFS_NAMESPACE_PREPARE_CANONICAL_DIGEST_PASSES.with(|passes| passes.set(0));
}

#[cfg(test)]
pub(crate) fn namespace_prepare_canonical_digest_passes() -> usize {
    DFS_NAMESPACE_PREPARE_CANONICAL_DIGEST_PASSES.with(std::cell::Cell::get)
}

fn metadata_authority_probe_timeout(
    shard: &DfsMetadataShardConfig,
    operation: &DfsMetadataAuthorityOperation,
    prepared_batch: &DfsMetadataJournalBatch,
) -> Duration {
    let DfsMetadataAuthorityOperation::NamespaceMutation {
        operation_id,
        file_system_id,
        prepare_sequence,
        payload_digest,
    } = operation
    else {
        return DFS_METADATA_AUTHORITY_PROBE_TIMEOUT;
    };
    let Some(entry) = prepared_batch
        .entries
        .iter()
        .find(|entry| entry.sequence == *prepare_sequence)
    else {
        return DFS_METADATA_AUTHORITY_PROBE_TIMEOUT;
    };
    let DfsMetadataMutation::PutNamespacePrepare {
        file_system_id: mutation_file_system_id,
        operation_id: mutation_operation_id,
        status,
        ..
    } = &entry.mutation
    else {
        return DFS_METADATA_AUTHORITY_PROBE_TIMEOUT;
    };
    if entry.shard_id.0 != shard.shard_id
        || file_system_id != &shard.file_system_id
        || mutation_file_system_id != file_system_id
        || mutation_operation_id != operation_id
        || status != &DfsNamespacePrepareStatus::Prepared
    {
        return DFS_METADATA_AUTHORITY_PROBE_TIMEOUT;
    }
    // `prepared_batch` is the exact locally-authored suffix returned by the
    // backend after this process prepared the operation. Its packed body was
    // validated while it was built and persisted; timeout selection only
    // needs its bounded identity and row count, so do not rescan every packed
    // column before the real replica apply validates the wire value.
    let Some(prepare) = entry.mutation.namespace_prepare_payload_ref_prevalidated() else {
        return DFS_METADATA_AUTHORITY_PROBE_TIMEOUT;
    };
    if prepare.validate_file_system_scope(file_system_id).is_err()
        || !prepare.matches_authority(
            file_system_id,
            operation_id,
            &DfsNamespacePrepareStatus::Prepared,
            &entry.shard_id,
            shard.epoch,
            payload_digest,
        )
    {
        return DFS_METADATA_AUTHORITY_PROBE_TIMEOUT;
    }
    let record_count = prepare.record_count();
    if record_count > DFS_METADATA_AUTHORITY_LARGE_NAMESPACE_RECORDS {
        DFS_METADATA_AUTHORITY_LARGE_PROBE_TIMEOUT
    } else {
        DFS_METADATA_AUTHORITY_PROBE_TIMEOUT
    }
}

/// Process-wide serialization between the final canonical metadata publication
/// check and retiring-primary fence capture for one shard.
///
/// All service lanes in a daemon share this guard. A retiring primary cannot
/// acknowledge its post-fence head while an authorized publication is between
/// its last exact assignment check and the backend commit, and a
/// publication that arrives afterward re-checks the durable pending handoff
/// and fails before touching visible state.
#[derive(Debug)]
pub(crate) struct DfsMetadataShardPublicationGuard {
    key: String,
}

pub(crate) fn metadata_shard_publication_guard(
    file_system_id: &DfsFileSystemId,
    shard_id: &str,
) -> DfsMetadataShardPublicationGuard {
    let key = format!("{file_system_id}/{shard_id}");
    let (mutex, condvar) = DFS_METADATA_SHARD_PUBLICATION_LOCKS
        .get_or_init(|| (Mutex::new(BTreeSet::new()), Condvar::new()));
    let mut locked = mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    while !locked.insert(key.clone()) {
        locked = condvar
            .wait(locked)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
    DfsMetadataShardPublicationGuard { key }
}

impl Drop for DfsMetadataShardPublicationGuard {
    fn drop(&mut self) {
        let Some((mutex, condvar)) = DFS_METADATA_SHARD_PUBLICATION_LOCKS.get() else {
            return;
        };
        let mut locked = mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        locked.remove(&self.key);
        condvar.notify_all();
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn select_metadata_shard_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(None);
        };
        DfsControlPlaneStore::new(kv.as_mut())
            .select_metadata_shard_for_path_strict_default_sifs_split(
                &object.file_system_id,
                &object.path,
            )
            .map_err(DfsError::from)
    }

    pub(crate) fn local_metadata_owner_epoch_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<u64, DfsError> {
        Ok(self
            .certified_local_metadata_owner_shard_for_object(object)?
            .map_or(0, |shard| shard.epoch))
    }

    /// Return the exact shard assignment after refreshing cluster
    /// configuration, not merely the epoch from an earlier object-to-shard
    /// selection.
    pub(crate) fn certified_local_metadata_owner_shard_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let Some(mut shard) = self.select_metadata_shard_for_object(object)? else {
            if self.control_plane.is_none() {
                return Ok(None);
            }
            return Err(missing_metadata_shard_assignment(object));
        };
        if shard.primary_node_id != self.node_id {
            self.refresh_control_plane_latest()?;
            let Some(refreshed) = self.select_metadata_shard_for_object(object)? else {
                return Err(missing_metadata_shard_assignment(object));
            };
            shard = refreshed;
        }
        if shard.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: object.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }

        let certified = self
            .control_plane
            .as_mut()
            .map(|kv| {
                DfsControlPlaneStore::new(kv.as_mut()).metadata_shard_authority_is_current(&shard)
            })
            .transpose()?
            .unwrap_or(false);
        if !certified {
            self.refresh_control_plane_latest()?;
            let refreshed = self
                .select_metadata_shard_for_object(object)?
                .unwrap_or_else(|| shard.clone());
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: object.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: refreshed.primary_node_id,
                epoch: refreshed.epoch,
            });
        }
        Ok(Some(shard))
    }

    /// Capture the locally committed shard epoch for a new intent without a
    /// Raft write barrier. This is only an intent hint: every visible commit
    /// must still call `ensure_intent_epoch_current*`, which obtains fresh
    /// operation-bound full-shard or cluster-quorum authorization at the
    /// metadata publication boundary.
    pub(crate) fn local_metadata_owner_epoch_hint_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<u64, DfsError> {
        let Some(shard) = self.select_metadata_shard_for_object(object)? else {
            if self.control_plane.is_none() {
                return Ok(0);
            }
            return Err(missing_metadata_shard_assignment(object));
        };
        if shard.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: object.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }
        Ok(shard.epoch)
    }

    #[cfg(test)]
    pub(crate) fn authorize_prepared_intent_for_object(
        &mut self,
        intent: &DistributedWriteIntent,
        object: &DfsObjectMetadata,
        operation: &DfsMetadataAuthorityOperation,
        prepared_batch: &DfsMetadataJournalBatch,
    ) -> Result<Option<DfsCanonicalAuthorization>, DfsError> {
        let Some(shard) = self.select_metadata_shard_for_object(object)? else {
            if self.control_plane.is_none() && intent.owner_epoch == 0 {
                return Ok(None);
            }
            return Err(missing_metadata_shard_assignment(object));
        };
        self.authorize_prepared_intent_for_selected_shard(
            intent,
            object,
            shard,
            operation,
            prepared_batch,
        )
        .map(Some)
    }

    /// Authorize an intent using the operation-scoped shard snapshot that was
    /// already used to persist its hidden prepare. A healthy commit must not
    /// rescan every shard config immediately after selecting that same shard;
    /// stale/non-local snapshots still refresh and fail closed below.
    pub(crate) fn authorize_prepared_intent_for_selected_shard(
        &mut self,
        intent: &DistributedWriteIntent,
        object: &DfsObjectMetadata,
        mut shard: DfsMetadataShardConfig,
        operation: &DfsMetadataAuthorityOperation,
        prepared_batch: &DfsMetadataJournalBatch,
    ) -> Result<DfsCanonicalAuthorization, DfsError> {
        if object.file_system_id != intent.file_system_id || object.object_id != intent.object_id {
            return Err(DfsError::Distributed {
                message: format!(
                    "intent {} base object does not match filesystem {} object {}",
                    intent.intent_id, intent.file_system_id, intent.object_id
                ),
            });
        }
        if shard.primary_node_id != self.node_id {
            self.refresh_control_plane_latest()?;
            let Some(refreshed) = self.select_metadata_shard_for_object(object)? else {
                return Err(DfsError::StaleOwnerEpoch {
                    file_system_id: intent.file_system_id.clone(),
                    local_node_id: self.node_id.clone(),
                    primary_node_id: shard.primary_node_id,
                    epoch: shard.epoch,
                });
            };
            shard = refreshed;
        }
        if shard.primary_node_id != self.node_id || shard.epoch != intent.owner_epoch {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: intent.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }

        self.authorize_metadata_shard_operation(&shard, operation, prepared_batch)
    }

    /// Authorize one exact canonical mutation against one metadata shard.
    ///
    /// The healthy path requires a fresh, operation-bound acknowledgement from
    /// every configured shard member and does not touch Raft. When a member is
    /// unavailable, the exact prepared operation may instead be fenced through
    /// cluster KV's existing consensus log. KV carries only the permit and
    /// assignment/epoch comparison; DFS remains the owner of the prepare,
    /// journal, metadata, and repair state.
    /// The local committed assignment is compared again after the data-plane
    /// round so an observed reconfiguration cannot slip between evidence
    /// collection and the caller's immediately following metadata CAS.
    pub(crate) fn authorize_metadata_shard_operation(
        &mut self,
        shard: &DfsMetadataShardConfig,
        operation: &DfsMetadataAuthorityOperation,
        prepared_batch: &DfsMetadataJournalBatch,
    ) -> Result<DfsCanonicalAuthorization, DfsError> {
        let trace_started = Instant::now();
        if self.control_plane.is_none() {
            return Err(DfsError::Distributed {
                message: "clustered metadata authorization has no control-plane client".to_string(),
            });
        }
        if shard.primary_node_id != self.node_id
            || !metadata_authority_operation_is_well_formed_for_shard(operation, shard)
            || prepared_batch.shard_id.0 != shard.shard_id
            || prepared_batch.entries.is_empty()
            || prepared_batch.next_after_sequence
                != metadata_authority_operation_prepare_sequence(operation)
        {
            dfs_perf_trace(format_args!(
                "metadata_authority action=reject reason=malformed shard={} epoch={} local_primary={} operation_well_formed={} batch_shard={} batch_entries={} batch_next={} operation_prepare={}",
                shard.shard_id,
                shard.epoch,
                shard.primary_node_id == self.node_id,
                metadata_authority_operation_is_well_formed_for_shard(operation, shard),
                prepared_batch.shard_id.0,
                prepared_batch.entries.len(),
                prepared_batch.next_after_sequence,
                metadata_authority_operation_prepare_sequence(operation),
            ));
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: shard.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id.clone(),
                epoch: shard.epoch,
            });
        }
        let required_node_ids = std::iter::once(shard.primary_node_id.clone())
            .chain(shard.replica_node_ids.iter().cloned())
            .collect::<BTreeSet<_>>();
        // Metadata object RPC encodes every typed value into credit-bounded
        // transport segments. Large prepares therefore use the same stream-
        // native replica path as small ones; semantic record count must never
        // select the degraded KV-only authorization path.
        let probe_timeout = metadata_authority_probe_timeout(shard, operation, prepared_batch);
        let probe_started = Instant::now();
        let acknowledged_node_ids = self.fresh_metadata_shard_authority_acknowledgements(
            shard,
            operation,
            prepared_batch,
            probe_timeout,
        )?;
        let trace_probe_ms = probe_started.elapsed().as_millis();
        let trace_acknowledged = acknowledged_node_ids.len();
        dfs_perf_trace(format_args!(
            "metadata_authority action=probe shard={} epoch={} prepare={} acknowledgements={} required={} timeout_ms={}",
            shard.shard_id,
            shard.epoch,
            prepared_batch.next_after_sequence,
            acknowledged_node_ids.len(),
            required_node_ids.len(),
            probe_timeout.as_millis(),
        ));
        let assignment_started = Instant::now();
        let assignment_still_exact = acknowledged_node_ids == required_node_ids
            && self.metadata_shard_assignment_still_exact(shard)?;
        let trace_assignment_ms = assignment_started.elapsed().as_millis();
        if assignment_still_exact {
            dfs_perf_trace(format_args!(
                "metadata-authority-confirmation node={} shard={} epoch={} mode=full-shard required={} acknowledged={} probe_ms={} assignment_ms={} quorum_ms=0 total_ms={}",
                self.node_id,
                shard.shard_id,
                shard.epoch,
                required_node_ids.len(),
                trace_acknowledged,
                trace_probe_ms,
                trace_assignment_ms,
                trace_started.elapsed().as_millis()
            ));
            return Ok(DfsCanonicalAuthorization::FullShard {
                operation_digest: metadata_authority_operation_digest(operation).to_string(),
                expected_shard: shard.clone(),
                acknowledged_node_ids: acknowledged_node_ids.into_iter().collect(),
                prepare_sequence: prepared_batch.next_after_sequence,
            });
        }
        dfs_perf_trace(format_args!(
            "metadata_authority action=cluster_quorum_fallback shard={} epoch={} prepare={} acknowledgements={} required={}",
            shard.shard_id,
            shard.epoch,
            metadata_authority_operation_prepare_sequence(operation),
            acknowledged_node_ids.len(),
            required_node_ids.len(),
        ));
        let operation_digest = metadata_authority_operation_digest(operation);
        let quorum_started = Instant::now();
        let Some(permit_id) = self.certify_exact_metadata_shard_operation(
            shard,
            operation_digest,
            metadata_authority_operation_prepare_sequence(operation),
        )?
        else {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: shard.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id.clone(),
                epoch: shard.epoch,
            });
        };
        let trace_quorum_ms = quorum_started.elapsed().as_millis();
        dfs_perf_trace(format_args!(
            "metadata-authority-confirmation node={} shard={} epoch={} mode=cluster-quorum required={} acknowledged={} probe_ms={} assignment_ms={} quorum_ms={} total_ms={}",
            self.node_id,
            shard.shard_id,
            shard.epoch,
            required_node_ids.len(),
            trace_acknowledged,
            trace_probe_ms,
            trace_assignment_ms,
            trace_quorum_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(DfsCanonicalAuthorization::ClusterQuorum {
            operation_digest: operation_digest.to_string(),
            expected_shard: shard.clone(),
            permit_id,
        })
    }

    fn metadata_shard_assignment_still_exact(
        &mut self,
        expected: &DfsMetadataShardConfig,
    ) -> Result<bool, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(false);
        };
        let store = DfsControlPlaneStore::new(control_plane.as_mut());
        let current = store
            .get_metadata_shard(&expected.file_system_id, &expected.shard_id)
            .map_err(DfsError::from)?;
        if current.as_ref() != Some(expected) {
            return Ok(false);
        }
        Ok(store
            .get_metadata_shard_membership_handoff(&expected.file_system_id, &expected.shard_id)
            .map_err(DfsError::from)?
            .is_none())
    }

    /// Revalidate typed evidence immediately before the backend publication
    /// call. Payload/digest/epoch mismatches fail closed; callers never reduce
    /// authorization to an unstructured boolean.
    pub(crate) fn validate_canonical_authorization(
        &mut self,
        authorization: &DfsCanonicalAuthorization,
        operation: &DfsMetadataAuthorityOperation,
    ) -> Result<DfsMetadataShardPublicationGuard, DfsError> {
        let operation_digest = metadata_authority_operation_digest(operation);
        let (expected_shard, valid) = match authorization {
            DfsCanonicalAuthorization::FullShard {
                operation_digest: authorized_digest,
                expected_shard,
                acknowledged_node_ids,
                prepare_sequence,
            } => {
                let required = std::iter::once(expected_shard.primary_node_id.clone())
                    .chain(expected_shard.replica_node_ids.iter().cloned())
                    .collect::<BTreeSet<_>>();
                let acknowledged = acknowledged_node_ids
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>();
                (
                    expected_shard,
                    authorized_digest == operation_digest
                        && acknowledged == required
                        && *prepare_sequence
                            == metadata_authority_operation_prepare_sequence(operation),
                )
            }
            DfsCanonicalAuthorization::ClusterQuorum {
                operation_digest: authorized_digest,
                expected_shard,
                permit_id,
            } => (
                expected_shard,
                authorized_digest == operation_digest && !permit_id.is_empty(),
            ),
        };
        let publication_guard = metadata_shard_publication_guard(
            &expected_shard.file_system_id,
            &expected_shard.shard_id,
        );
        if !valid
            || expected_shard.primary_node_id != self.node_id
            || !metadata_authority_operation_is_well_formed_for_shard(operation, expected_shard)
            || !self.metadata_shard_assignment_still_exact(expected_shard)?
        {
            dfs_perf_trace(format_args!(
                "metadata_authority action=reject reason=publication_revalidation shard={} epoch={} typed_valid={} local_primary={}",
                expected_shard.shard_id,
                expected_shard.epoch,
                valid,
                expected_shard.primary_node_id == self.node_id,
            ));
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: expected_shard.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: expected_shard.primary_node_id.clone(),
                epoch: expected_shard.epoch,
            });
        }
        Ok(publication_guard)
    }

    fn certify_exact_metadata_shard_operation(
        &mut self,
        shard: &DfsMetadataShardConfig,
        operation_digest: &str,
        prepare_sequence: u64,
    ) -> Result<Option<String>, DfsError> {
        self.control_plane
            .as_mut()
            .map(|kv| {
                DfsControlPlaneStore::new(kv.as_mut()).certify_metadata_shard_operation(
                    shard,
                    operation_digest,
                    prepare_sequence,
                )
            })
            .transpose()
            .map_err(DfsError::from)
            .map(Option::flatten)
    }

    fn fresh_metadata_shard_authority_acknowledgements(
        &mut self,
        shard: &DfsMetadataShardConfig,
        operation: &DfsMetadataAuthorityOperation,
        prepared_batch: &DfsMetadataJournalBatch,
        timeout: Duration,
    ) -> Result<BTreeSet<NodeId>, DfsError> {
        let targets = shard
            .replica_node_ids
            .iter()
            .filter(|node_id| *node_id != &self.node_id)
            .cloned()
            .collect::<BTreeSet<_>>();
        if targets.is_empty() {
            return Ok(BTreeSet::from([self.node_id.clone()]));
        }

        let fallback_deadline = Instant::now()
            .checked_add(DFS_METADATA_AUTHORITY_OPERATION_TIMEOUT)
            .unwrap_or_else(Instant::now);
        let observed_previous_deadline = self.transport.operation_deadline();
        let effective_operation_deadline = observed_previous_deadline
            .map_or(fallback_deadline, |deadline| {
                deadline.min(fallback_deadline)
            });
        let replaced_previous_deadline = self
            .transport
            .set_operation_deadline(Some(effective_operation_deadline));
        let previous_deadline = replaced_previous_deadline.or(observed_previous_deadline);
        let result = self.try_all_metadata_shard_members_accept_fresh_authority(
            shard,
            operation,
            prepared_batch,
            targets,
            effective_operation_deadline,
            timeout,
        );
        self.transport.set_operation_deadline(previous_deadline);
        match result {
            Err(DfsError::Network { .. }) => Ok(BTreeSet::from([self.node_id.clone()])),
            other => other,
        }
    }

    fn try_all_metadata_shard_members_accept_fresh_authority(
        &mut self,
        shard: &DfsMetadataShardConfig,
        operation: &DfsMetadataAuthorityOperation,
        prepared_batch: &DfsMetadataJournalBatch,
        targets: BTreeSet<NodeId>,
        operation_deadline: Instant,
        timeout: Duration,
    ) -> Result<BTreeSet<NodeId>, DfsError> {
        let mut request_ids = Vec::with_capacity(targets.len());
        let mut request_targets = Vec::with_capacity(targets.len());
        let mut acknowledged_node_ids = BTreeSet::from([self.node_id.clone()]);
        let required_acknowledgements = targets.len() + 1;
        if Instant::now() >= operation_deadline {
            return Ok(acknowledged_node_ids);
        }
        let prepared_prefix = self
            .metadata_backend
            .as_deref()
            .ok_or_else(|| DfsError::Metadata {
                message: "metadata authority probe has no durable metadata backend".to_string(),
            })?
            .metadata_prefix_proof(&prepared_batch.shard_id)?;
        if Instant::now() >= operation_deadline {
            return Ok(acknowledged_node_ids);
        }
        let Some(prepared_prefix) = prepared_prefix.filter(|prefix| {
            prefix.is_well_formed() && prefix.journal_sequence == prepared_batch.next_after_sequence
        }) else {
            // The fast path is optional. A missing or moving exact prefix
            // cannot authorize publication.
            return Ok(acknowledged_node_ids);
        };
        let result = (|| {
            let mut targets_by_replicated_through_sequence = BTreeMap::<u64, Vec<NodeId>>::new();
            for target_node_id in targets {
                if Instant::now() >= operation_deadline {
                    return Ok(acknowledged_node_ids);
                }
                let replicated_through_sequence = self
                    .metadata_authority_replicated_through_sequence_for_target(
                        &target_node_id,
                        prepared_batch,
                    )?;
                if Instant::now() >= operation_deadline {
                    return Ok(acknowledged_node_ids);
                }
                targets_by_replicated_through_sequence
                    .entry(replicated_through_sequence)
                    .or_default()
                    .push(target_node_id);
            }

            // Construct one owned command per distinct replica cursor. The
            // production transport encodes it once and Arc-replays the body to
            // every target in this group. Previously the service deep-cloned
            // the same multi-megabyte prepare into one command per replica
            // before the transport could discover that all values were equal.
            let mut authority_groups =
                Vec::with_capacity(targets_by_replicated_through_sequence.len());
            for (replicated_through_sequence, target_node_ids) in
                targets_by_replicated_through_sequence
            {
                let target_batch = self
                    .metadata_authority_batch_since(replicated_through_sequence, prepared_batch)?;
                if Instant::now() >= operation_deadline {
                    return Ok(acknowledged_node_ids);
                }
                let Some(target_batch) = target_batch else {
                    // A member farther behind than one bounded catch-up batch
                    // cannot count toward this operation's full-shard proof.
                    // Background replication can close the gap independently.
                    continue;
                };
                let request_id = self.next_request_id();
                let probe_id = sinomo_core::Id::generate().to_string();
                let command = DfsMetadataJournalBatchCommand {
                    request_id,
                    source_node_id: Some(self.node_id.clone()),
                    file_system_id: Some(shard.file_system_id.clone()),
                    authority_epoch: Some(shard.epoch),
                    verify_prefix_only: false,
                    prefix_probe: None,
                    authority_probe: Some(DfsMetadataAuthorityProbe {
                        probe_id,
                        expected_shard: shard.clone(),
                        operation: operation.clone(),
                        prepared_prefix: Some(prepared_prefix.clone()),
                    }),
                    // Include every entry the target has not yet acknowledged,
                    // through the exact prepare. The receiver refuses gaps;
                    // older peers can still apply the prefix but cannot echo
                    // the nonce and therefore cannot authorize publication.
                    batch: target_batch,
                };
                authority_groups.push(DfsMetadataJournalBatchDirectGroup::new(
                    target_node_ids,
                    command,
                ));
            }
            // Replayable object-RPC construction can encode a multi-megabyte
            // catch-up value. The duration-aware transport keeps that local
            // work under `operation_deadline` and begins this shorter budget
            // only after encoding, immediately before peer fan-out.
            match self
                .transport
                .send_grouped_metadata_journal_batches_direct_with_response_timeout(
                    &authority_groups,
                    timeout,
                ) {
                Ok(Some(responses)) => {
                    for group in &authority_groups {
                        let expected_confirmation_id = group
                            .command
                            .authority_probe
                            .as_ref()
                            .and_then(metadata_authority_probe_confirmation_id);
                        for target_node_id in &group.target_node_ids {
                            let Some((_, response)) = responses
                                .iter()
                                .find(|(response_node_id, _)| response_node_id == target_node_id)
                            else {
                                continue;
                            };
                            if response.request_id != group.command.request_id
                                || response.shard_id != group.command.batch.shard_id
                                || response.next_after_sequence
                                    != group.command.batch.next_after_sequence
                                || response.result != DfsMetadataJournalBatchResult::Applied
                                || response.verified_authority_probe_id.as_deref()
                                    != expected_confirmation_id.as_deref()
                            {
                                continue;
                            }
                            self.record_metadata_authority_replication_cursor(
                                target_node_id,
                                &group.command.batch.shard_id,
                                group.command.batch.next_after_sequence,
                            );
                            acknowledged_node_ids.insert(target_node_id.clone());
                        }
                    }
                    return Ok(acknowledged_node_ids);
                }
                Ok(None) => {}
                Err(_) => return Ok(acknowledged_node_ids),
            }

            // Routed responses are correlated through one process-wide pending
            // map. Only transports without the grouped direct accelerator pay
            // to materialize one command per target, each with a unique wire
            // identity. Move the last command in each group to avoid retaining
            // an otherwise unused extra copy even on this compatibility path.
            let mut authority_requests = Vec::with_capacity(required_acknowledgements - 1);
            for group in authority_groups {
                let target_count = group.target_node_ids.len();
                let mut shared_command = Some(group.command);
                for (target_index, target_node_id) in group.target_node_ids.into_iter().enumerate()
                {
                    if Instant::now() >= operation_deadline {
                        return Ok(acknowledged_node_ids);
                    }
                    let mut command = if target_index + 1 == target_count {
                        shared_command
                            .take()
                            .expect("last routed target owns grouped authority command")
                    } else {
                        shared_command
                            .as_ref()
                            .expect("grouped authority command remains available")
                            .clone()
                    };
                    command.request_id = self.next_request_id();
                    command
                        .authority_probe
                        .as_mut()
                        .expect("authority request carries probe")
                        .probe_id = sinomo_core::Id::generate().to_string();
                    authority_requests.push((target_node_id, command));
                }
            }
            let response_deadline = Instant::now()
                .checked_add(timeout)
                .unwrap_or_else(Instant::now)
                .min(operation_deadline);
            if Instant::now() >= response_deadline {
                return Ok(acknowledged_node_ids);
            }
            let observed_previous_deadline = self.transport.operation_deadline();
            let replaced_previous_deadline = self
                .transport
                .set_operation_deadline(Some(response_deadline));
            let previous_deadline = replaced_previous_deadline.or(observed_previous_deadline);
            let routed_result = (|| {
                for (target_node_id, command) in authority_requests {
                    let request_id = command.request_id.clone();
                    let confirmation_id = metadata_authority_probe_confirmation_id(
                        command
                            .authority_probe
                            .as_ref()
                            .expect("authority request carries probe"),
                    )
                    .expect("prepared authority probe has an exact prefix confirmation");
                    self.pending_metadata_authority_probes.insert(
                        request_id.clone(),
                        PendingMetadataAuthorityProbe {
                            target_node_id: target_node_id.clone(),
                            confirmation_id,
                            expected_shard: shard.clone(),
                        },
                    );
                    let sent = match self.send_dfs_message_if_routable(
                        &target_node_id,
                        DfsNetworkMessage::MetadataJournalBatch(command),
                    ) {
                        Ok(sent) => sent,
                        Err(DfsError::Network { .. }) => false,
                        Err(error) => return Err(error),
                    };
                    if !sent {
                        self.pending_metadata_authority_probes.remove(&request_id);
                        continue;
                    }
                    request_ids.push(request_id.clone());
                    request_targets.push((request_id, target_node_id));
                }
                loop {
                    self.process_incoming()?;
                    let mut unresolved = false;
                    for (request_id, target_node_id) in &request_targets {
                        match self.metadata_authority_probe_results.get(request_id) {
                            Some(true) => {
                                acknowledged_node_ids.insert(target_node_id.clone());
                            }
                            Some(false) => {}
                            None => unresolved = true,
                        }
                    }
                    if acknowledged_node_ids.len() == required_acknowledgements {
                        let acknowledged_replicas = acknowledged_node_ids
                            .iter()
                            .filter(|id| *id != &self.node_id)
                            .cloned()
                            .collect::<Vec<_>>();
                        for target_node_id in &acknowledged_replicas {
                            self.record_metadata_authority_replication_cursor(
                                target_node_id,
                                &prepared_batch.shard_id,
                                prepared_batch.next_after_sequence,
                            );
                        }
                        return Ok(acknowledged_node_ids);
                    }
                    if !unresolved {
                        return Ok(acknowledged_node_ids);
                    }
                    let Some(remaining) = response_deadline.checked_duration_since(Instant::now())
                    else {
                        return Ok(acknowledged_node_ids);
                    };
                    self.transport
                        .wait_for_incoming(DFS_METADATA_AUTHORITY_PROBE_IDLE_WAIT.min(remaining))
                        .map_err(DfsError::from_network)?;
                }
            })();
            self.transport.set_operation_deadline(previous_deadline);
            routed_result
        })();
        self.clear_metadata_authority_probe_requests(&request_ids);
        result
    }

    #[cfg(test)]
    pub(crate) fn metadata_authority_batch_for_target(
        &self,
        target_node_id: &NodeId,
        prepared_batch: &DfsMetadataJournalBatch,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        let replicated_through_sequence = self
            .metadata_authority_replicated_through_sequence_for_target(
                target_node_id,
                prepared_batch,
            )?;
        self.metadata_authority_batch_since(replicated_through_sequence, prepared_batch)
    }

    fn metadata_authority_replicated_through_sequence_for_target(
        &self,
        target_node_id: &NodeId,
        prepared_batch: &DfsMetadataJournalBatch,
    ) -> Result<u64, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Err(DfsError::Metadata {
                message: "metadata authority probe has no durable metadata backend".to_string(),
            });
        };
        let durable_replicated_through_sequence =
            backend.metadata_replication_cursor(target_node_id, &prepared_batch.shard_id)?;
        let replicated_through_sequence = self
            .metadata_replication_cursors
            .get(&(target_node_id.clone(), prepared_batch.shard_id.clone()))
            .copied()
            .unwrap_or(0)
            .max(durable_replicated_through_sequence);
        Ok(replicated_through_sequence)
    }

    fn metadata_authority_batch_since(
        &self,
        replicated_through_sequence: u64,
        prepared_batch: &DfsMetadataJournalBatch,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        if replicated_through_sequence >= prepared_batch.next_after_sequence {
            // Exact retries still carry the prepare entry so a current peer can
            // re-read it and echo the fresh request nonce without mutating its
            // replay cursor.
            return Ok(Some(prepared_batch.clone()));
        }
        let prepared_batch_is_exact_suffix =
            prepared_batch.entries.first().is_some_and(|entry| {
                entry.sequence == replicated_through_sequence.saturating_add(1)
            }) && prepared_batch
                .entries
                .last()
                .is_some_and(|entry| entry.sequence == prepared_batch.next_after_sequence)
                && prepared_batch
                    .entries
                    .windows(2)
                    .all(|entries| entries[1].sequence == entries[0].sequence.saturating_add(1));
        if prepared_batch_is_exact_suffix {
            // The foreground authority path already owns this exact decoded
            // suffix. Re-reading it from the journal serializes and decodes the
            // same multi-megabyte prepare before any network work can begin.
            return Ok(Some(prepared_batch.clone()));
        }
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Err(DfsError::Metadata {
                message: "metadata authority probe has no durable metadata backend".to_string(),
            });
        };
        #[cfg(test)]
        DFS_METADATA_AUTHORITY_BATCH_READS.with(|reads| reads.set(reads.get().saturating_add(1)));
        let Some(batch) = backend.journal_batch_for_shard_since_limited(
            &prepared_batch.shard_id,
            replicated_through_sequence,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )?
        else {
            return Ok(None);
        };
        let prepare_sequence = prepared_batch.next_after_sequence;
        if batch.next_after_sequence < prepare_sequence
            || !batch
                .entries
                .iter()
                .any(|entry| entry.sequence == prepare_sequence)
        {
            return Ok(None);
        }
        Ok(Some(batch))
    }

    fn record_metadata_authority_replication_cursor(
        &mut self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        replayed_through_sequence: u64,
    ) {
        let acknowledged_sequence = {
            let cursor = self
                .metadata_replication_cursors
                .entry((target_node_id.clone(), shard_id.clone()))
                .or_insert(0);
            *cursor = (*cursor).max(replayed_through_sequence);
            *cursor
        };
        // The fresh receiver nonce and full-shard proof authorize this request.
        // This sender-side cursor only avoids resending an acknowledged prefix;
        // persist it immediately so the fenced background publisher observes
        // the prepare acknowledgement and starts with the later commit suffix.
        //
        // Cursor persistence remains best effort: the receiver has already
        // durably applied and verified this prefix, so turning a local advisory
        // checkpoint failure into a rejected foreground operation would report
        // false failure after successful replication. A crash or write failure
        // safely resends the idempotent prefix.
        let Some(backend) = self.metadata_backend.as_mut() else {
            return;
        };
        if let Err(error) =
            backend.put_metadata_replication_cursor(target_node_id, shard_id, acknowledged_sequence)
        {
            dfs_perf_trace(format_args!(
                "metadata_authority action=cursor_checkpoint_failed target={} shard={} acknowledged_sequence={} error={}",
                target_node_id, shard_id.0, acknowledged_sequence, error
            ));
        }
    }

    fn clear_metadata_authority_probe_requests(&mut self, request_ids: &[String]) {
        for request_id in request_ids {
            self.pending_metadata_authority_probes.remove(request_id);
            self.metadata_authority_probe_results.remove(request_id);
        }
    }

    pub(crate) fn ensure_replica_object_metadata(
        &mut self,
        object: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let Some(object) = object else {
            return Ok(());
        };
        let persist_authoritative_metadata =
            self.local_metadata_backend_is_authoritative_for_object(object)?;
        if !self
            .metadata
            .filesystems
            .contains_key(&object.file_system_id)
        {
            let file_system = DfsFileSystem {
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            };
            if persist_authoritative_metadata {
                self.add_file_system(file_system)?;
            } else {
                self.metadata.add_file_system(file_system);
            }
        }
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(existing) = backend.get_object(&object.object_id)?
        {
            self.metadata.add_object(existing)?;
        } else if self.metadata.object(&object.object_id).is_none() {
            if persist_authoritative_metadata {
                self.add_object_metadata(object.clone())?;
            } else {
                self.add_transient_object_metadata(object.clone())?;
            }
        }
        Ok(())
    }

    pub(crate) fn command_authority_shard_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let Some(object) = object else {
            return Ok(None);
        };
        let Some(shard) = self.select_metadata_shard_for_object(object)? else {
            return Ok(None);
        };
        Ok((shard.primary_node_id == self.node_id).then_some(shard))
    }

    fn local_metadata_backend_is_authoritative_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<bool, DfsError> {
        Ok(self
            .select_metadata_shard_for_object(object)?
            .is_none_or(|shard| shard.primary_node_id == self.node_id))
    }

    fn command_authority_shard_proves_metadata_primary(
        &self,
        requester_node_id: &NodeId,
        object: &DfsObjectMetadata,
        local_shard: Option<&DfsMetadataShardConfig>,
        authority_shard: Option<&DfsMetadataShardConfig>,
    ) -> Result<bool, DfsError> {
        let Some(authority_shard) = authority_shard else {
            return Ok(false);
        };
        if authority_shard.primary_node_id != *requester_node_id
            || authority_shard.file_system_id != object.file_system_id
        {
            return Ok(false);
        }
        let selected = crate::control_plane::select_metadata_shard_config_for_path(
            &object.file_system_id,
            std::slice::from_ref(authority_shard),
            &object.path,
        )
        .map_err(DfsError::from)?;
        if selected.as_ref() != Some(authority_shard) {
            return Ok(false);
        }
        if let Some(local_shard) = local_shard
            && authority_shard.epoch < local_shard.epoch
        {
            return Ok(false);
        }
        Ok(true)
    }

    fn replica_command_metadata_is_authoritative(
        &mut self,
        object_id: &DfsObjectId,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<bool, DfsError> {
        let object = match self.current_read_object_metadata(object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. }) => {
                object_metadata
                    .cloned()
                    .ok_or_else(|| DfsError::UnknownObject {
                        object_id: object_id.clone(),
                    })?
            }
            Err(error) => return Err(error),
        };
        self.local_metadata_backend_is_authoritative_for_object(&object)
    }

    pub(crate) fn put_replica_receipt_if_authoritative(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        if self.replica_command_metadata_is_authoritative(object_id, object_metadata)? {
            self.put_metadata_receipt(
                file_system_id,
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        } else {
            self.ensure_metadata_receipt_accepts(
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        }
        Ok(())
    }

    pub(crate) fn commit_replica_metadata_cache(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let persist_authoritative_metadata =
            self.replica_command_metadata_is_authoritative(object_id, object_metadata)?;
        if persist_authoritative_metadata {
            self.put_metadata_receipt(
                file_system_id,
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        } else {
            self.ensure_metadata_receipt_accepts(
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        }
        let object =
            self.commit_local_metadata(object_id, version, content_hash, dfs_now_unix_nanos())?;
        if persist_authoritative_metadata {
            self.put_visible_object_in_metadata_backend(&object)?;
        }
        Ok(())
    }

    pub(crate) fn ensure_replica_command_authorized_with_metadata(
        &mut self,
        requester_node_id: &NodeId,
        intent_id: &str,
        object_id: &DfsObjectId,
        object_metadata: Option<&DfsObjectMetadata>,
        authority_shard: Option<&DfsMetadataShardConfig>,
    ) -> Result<(), DfsError> {
        let (object, object_from_command) = match self.current_read_object_metadata(object_id) {
            Ok(object) => (object, false),
            Err(DfsError::UnknownObject { .. }) => {
                let object = object_metadata
                    .cloned()
                    .ok_or_else(|| DfsError::UnknownObject {
                        object_id: object_id.clone(),
                    })?;
                (object, true)
            }
            Err(error) => return Err(error),
        };
        let mut metadata_shard = self.select_metadata_shard_for_object(&object)?;
        let mut requester_has_supplied_metadata_authority = self
            .command_authority_shard_proves_metadata_primary(
                requester_node_id,
                &object,
                metadata_shard.as_ref(),
                authority_shard,
            )?;
        if requester_has_supplied_metadata_authority
            && authority_shard.is_some_and(|authority| metadata_shard.as_ref() != Some(authority))
        {
            // A long-lived content-replica lane may still hold the retiring
            // shard assignment after a certified primary handoff. The signed
            // command stamp is enough to trigger a fresh observation, but it
            // is not itself a control-plane replacement: require the refreshed
            // view to converge to that exact assignment before later replica
            // cache decisions can mistake this node for the old primary.
            self.refresh_control_plane_latest()?;
            metadata_shard = self.select_metadata_shard_for_object(&object)?;
            requester_has_supplied_metadata_authority = self
                .command_authority_shard_proves_metadata_primary(
                    requester_node_id,
                    &object,
                    metadata_shard.as_ref(),
                    authority_shard,
                )?;
            if authority_shard.is_some_and(|authority| metadata_shard.as_ref() != Some(authority)) {
                return Err(DfsError::UnauthorizedReplicaCommand {
                    source_node_id: requester_node_id.clone(),
                    reason: format!(
                        "local metadata authority view for {} did not converge to authenticated shard {} epoch {}",
                        object.file_system_id,
                        authority_shard.expect("authority checked above").shard_id,
                        authority_shard.expect("authority checked above").epoch,
                    ),
                });
            }
        }
        let requester_is_metadata_primary = metadata_shard
            .as_ref()
            .is_some_and(|shard| &shard.primary_node_id == requester_node_id)
            || requester_has_supplied_metadata_authority;
        if let Some(backend) = self.metadata_backend.as_ref() {
            if let Some(intent) = backend.get_intent(object_id, intent_id)? {
                let requester_has_intent_authority = &intent.coordinator_node_id
                    == requester_node_id
                    || (requester_is_metadata_primary && object_metadata.is_some());
                if !requester_has_intent_authority {
                    return Err(DfsError::UnauthorizedReplicaCommand {
                        source_node_id: requester_node_id.clone(),
                        reason: format!(
                            "source is not coordinator for DFS write intent {intent_id}"
                        ),
                    });
                }
                if !intent.desired_replica_node_ids.contains(&self.node_id) {
                    return Err(DfsError::UnauthorizedReplicaCommand {
                        source_node_id: requester_node_id.clone(),
                        reason: dfs_replica_not_desired_reason(intent_id),
                    });
                }
                return Ok(());
            }
        }
        if requester_is_metadata_primary && object_metadata.is_some() {
            return Ok(());
        }

        if !object_from_command || metadata_shard.is_none() {
            if &object.owner_node_id == requester_node_id {
                return Ok(());
            }
            if matches!(
                object.policy,
                DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
            ) && object.current_locations.contains(requester_node_id)
            {
                return Ok(());
            }
        } else if object_metadata.is_some()
            && let Some(shard) = metadata_shard.as_ref()
        {
            return Err(DfsError::UnauthorizedReplicaCommand {
                source_node_id: requester_node_id.clone(),
                reason: format!(
                    "source is not current metadata shard primary for {}; primary is {} at epoch {}",
                    object.file_system_id, shard.primary_node_id, shard.epoch
                ),
            });
        }

        Err(DfsError::UnauthorizedReplicaCommand {
            source_node_id: requester_node_id.clone(),
            reason: format!(
                "source is neither object owner nor current replica for {object_id}; owner is {}",
                object.owner_node_id
            ),
        })
    }

    pub(crate) fn ensure_replica_command_hash_matches_intent(
        &self,
        intent_id: &str,
        object_id: &DfsObjectId,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let Some(intent) = backend.get_intent(object_id, intent_id)? else {
            return Ok(());
        };
        if let Some(expected_hash) = intent.content_hash.as_deref() {
            if expected_hash != content_hash {
                return Err(DfsError::ContentHashMismatch {
                    expected: expected_hash.to_string(),
                    actual: content_hash.to_string(),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn ensure_blob_fetch_authorized(
        &mut self,
        requester_node_id: &NodeId,
        intent_id: &str,
        object_id: &DfsObjectId,
        authority_shard: Option<&DfsMetadataShardConfig>,
    ) -> Result<(), DfsError> {
        let Some(intent) = ({
            let Some(backend) = self.metadata_backend.as_ref() else {
                return Ok(());
            };
            backend.get_intent(object_id, intent_id)?
        }) else {
            return Ok(());
        };
        let intent_object_metadata = intent.object_metadata.clone();
        let requester_is_metadata_primary = match self
            .current_read_object_metadata(object_id)
            .ok()
            .or(intent_object_metadata)
        {
            Some(object) => {
                let metadata_shard = self.select_metadata_shard_for_object(&object)?;
                metadata_shard
                    .as_ref()
                    .is_some_and(|shard| shard.primary_node_id == *requester_node_id)
                    || self.command_authority_shard_proves_metadata_primary(
                        requester_node_id,
                        &object,
                        metadata_shard.as_ref(),
                        authority_shard,
                    )?
            }
            None => false,
        };
        if &intent.coordinator_node_id != requester_node_id
            && !intent.desired_replica_node_ids.contains(requester_node_id)
            && !requester_is_metadata_primary
        {
            return Err(DfsError::UnauthorizedReplicaCommand {
                source_node_id: requester_node_id.clone(),
                reason: format!(
                    "source is not authorized to fetch DFS blob for intent {intent_id}"
                ),
            });
        }
        if !intent.desired_replica_node_ids.contains(&self.node_id) {
            if self
                .backend_receipt_for_replica(object_id, &intent.target_version, &self.node_id)?
                .is_none()
            {
                return Err(DfsError::UnauthorizedReplicaCommand {
                    source_node_id: requester_node_id.clone(),
                    reason: format!(
                        "local node is not an expected or durable replica for DFS intent {intent_id}"
                    ),
                });
            }
        }
        Ok(())
    }
}

pub(crate) fn missing_metadata_shard_assignment(object: &DfsObjectMetadata) -> DfsError {
    DfsError::Distributed {
        message: format!(
            "clustered DFS object {} at {} has no committed metadata-shard assignment",
            object.object_id, object.path
        ),
    }
}

fn metadata_authority_payload_digest<T: serde::Serialize>(value: &T) -> Result<String, DfsError> {
    // Authority payloads can contain a complete bulk namespace transaction.
    // Buffering their JSON first retained another multi-megabyte copy on both
    // the primary and every replica solely to feed BLAKE3. `blake3::Hasher`
    // implements `Write`, so stream the byte-identical serde representation
    // directly into the digest instead.
    let mut hasher = blake3::Hasher::new();
    serde_json::to_writer(&mut hasher, value).map_err(|source| DfsError::Serialization {
        message: format!("encode DFS metadata authority operation: {source}"),
    })?;
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

pub(crate) fn metadata_authority_operation_for_intent(
    intent: &DistributedWriteIntent,
    content_hash: &str,
    receipt_node_ids: &[NodeId],
    namespace_prepare: Option<&DfsNamespacePrepareRecord>,
    prepare_sequence: u64,
    shard: &DfsMetadataShardConfig,
) -> Result<DfsMetadataAuthorityOperation, DfsError> {
    let mut receipt_node_ids = receipt_node_ids.to_vec();
    receipt_node_ids.sort();
    receipt_node_ids.dedup();
    let namespace_prepare_identity = namespace_prepare
        .map(|prepare| {
            let digest = metadata_namespace_prepare_payload_digest(prepare)?;
            if prepare.operation_digest != digest {
                return Err(DfsError::Metadata {
                    message: format!(
                        "namespace prepare {} carries a non-canonical operation digest",
                        prepare.operation_id
                    ),
                });
            }
            Ok((prepare.operation_id.clone(), digest))
        })
        .transpose()?;
    let payload_digest = metadata_authority_payload_digest(&(
        intent,
        content_hash,
        &receipt_node_ids,
        &namespace_prepare_identity,
        prepare_sequence,
        shard,
    ))?;
    Ok(DfsMetadataAuthorityOperation::WriteIntent {
        intent_id: intent.intent_id.clone(),
        file_system_id: intent.file_system_id.clone(),
        object_id: intent.object_id.clone(),
        base_version: intent.base_version.clone(),
        target_version: intent.target_version.clone(),
        owner_epoch: intent.owner_epoch,
        content_hash: content_hash.to_string(),
        receipt_node_ids,
        namespace_prepare_operation_id: namespace_prepare_identity
            .as_ref()
            .map(|(operation_id, _)| operation_id.clone()),
        namespace_prepare_digest: namespace_prepare_identity.map(|(_, digest)| digest),
        prepare_sequence,
        payload_digest,
    })
}

pub(crate) fn metadata_namespace_prepare_payload_digest(
    prepare: &DfsNamespacePrepareRecord,
) -> Result<String, DfsError> {
    let prepare = crate::metadata_records::normalized_namespace_prepare(prepare);
    metadata_namespace_prepare_payload_digest_normalized(&prepare)
}

/// Hash a namespace prepare that has already been canonicalized in place.
///
/// Callers that own a large prepare can normalize it once and keep transferring
/// that same allocation through journal and table materialization. The public
/// compatibility helper above retains the defensive clone for borrowed and
/// recovery callers whose normalization state is not known.
pub(crate) fn metadata_namespace_prepare_payload_digest_normalized(
    prepare: &DfsNamespacePrepareRecord,
) -> Result<String, DfsError> {
    #[cfg(test)]
    DFS_NAMESPACE_PREPARE_CANONICAL_DIGEST_PASSES.with(|passes| {
        passes.set(passes.get().saturating_add(1));
    });
    metadata_authority_payload_digest(&(
        &prepare.operation_id,
        &prepare.file_system_id,
        &prepare.shard_id,
        prepare.owner_epoch,
        &prepare.expected_object_versions,
        &prepare.expected_path_occupants,
        &prepare.transaction,
        &prepare.completed_request,
        &prepare.completed_request_completion,
    ))
}

pub(crate) fn metadata_authority_operation_for_namespace_prepare(
    prepare: &DfsNamespacePrepareRecord,
    prepare_sequence: u64,
) -> Result<DfsMetadataAuthorityOperation, DfsError> {
    let payload_digest = metadata_namespace_prepare_payload_digest(prepare)?;
    if prepare.operation_digest != payload_digest {
        return Err(DfsError::Metadata {
            message: format!(
                "namespace prepare {} carries a non-canonical operation digest",
                prepare.operation_id
            ),
        });
    }
    Ok(DfsMetadataAuthorityOperation::NamespaceMutation {
        operation_id: prepare.operation_id.clone(),
        file_system_id: prepare.file_system_id.clone(),
        prepare_sequence,
        payload_digest,
    })
}

/// Build the authority identity for a newly-created namespace prepare whose
/// canonical payload digest was computed by this process immediately before
/// the same record was durably prepared.
///
/// Recovered or network-provided records must use
/// [`metadata_authority_operation_for_namespace_prepare`] so their complete
/// payload is independently rehashed. This narrow constructor avoids hashing
/// a freshly-built multi-megabyte bulk transaction twice on the primary.
pub(crate) fn metadata_authority_operation_for_prevalidated_namespace_prepare(
    prepare: &DfsNamespacePrepareRecord,
    prepare_sequence: u64,
) -> Result<DfsMetadataAuthorityOperation, DfsError> {
    let digest = prepare
        .operation_digest
        .strip_prefix("blake3:")
        .filter(|digest| digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
    if prepare_sequence == 0
        || prepare.operation_id.trim().is_empty()
        || prepare.status != DfsNamespacePrepareStatus::Prepared
        || digest.is_none()
    {
        return Err(DfsError::Metadata {
            message: format!(
                "new namespace prepare {} has no prevalidated canonical authority identity",
                prepare.operation_id
            ),
        });
    }
    Ok(DfsMetadataAuthorityOperation::NamespaceMutation {
        operation_id: prepare.operation_id.clone(),
        file_system_id: prepare.file_system_id.clone(),
        prepare_sequence,
        payload_digest: prepare.operation_digest.clone(),
    })
}

pub(crate) fn metadata_authority_operation_digest(
    operation: &DfsMetadataAuthorityOperation,
) -> &str {
    match operation {
        DfsMetadataAuthorityOperation::WriteIntent { payload_digest, .. }
        | DfsMetadataAuthorityOperation::NamespaceMutation { payload_digest, .. } => payload_digest,
    }
}

pub(crate) fn metadata_authority_operation_prepare_sequence(
    operation: &DfsMetadataAuthorityOperation,
) -> u64 {
    match operation {
        DfsMetadataAuthorityOperation::WriteIntent {
            prepare_sequence, ..
        }
        | DfsMetadataAuthorityOperation::NamespaceMutation {
            prepare_sequence, ..
        } => *prepare_sequence,
    }
}

pub(crate) fn metadata_authority_operation_is_well_formed_for_shard(
    operation: &DfsMetadataAuthorityOperation,
    shard: &DfsMetadataShardConfig,
) -> bool {
    match operation {
        DfsMetadataAuthorityOperation::WriteIntent {
            intent_id,
            file_system_id,
            object_id: _,
            base_version,
            target_version,
            owner_epoch,
            content_hash,
            receipt_node_ids,
            namespace_prepare_operation_id,
            namespace_prepare_digest,
            prepare_sequence,
            payload_digest,
        } => {
            !intent_id.is_empty()
                && file_system_id == &shard.file_system_id
                && base_version.as_ref() != Some(target_version)
                && *owner_epoch == shard.epoch
                && !content_hash.is_empty()
                && !receipt_node_ids.is_empty()
                && namespace_prepare_operation_id.is_some() == namespace_prepare_digest.is_some()
                && namespace_prepare_operation_id
                    .as_ref()
                    .is_none_or(|operation_id| !operation_id.is_empty())
                && namespace_prepare_digest
                    .as_ref()
                    .is_none_or(|digest| !digest.is_empty())
                && *prepare_sequence > 0
                && !payload_digest.is_empty()
        }
        DfsMetadataAuthorityOperation::NamespaceMutation {
            operation_id,
            file_system_id,
            prepare_sequence,
            payload_digest,
        } => {
            !operation_id.is_empty()
                && file_system_id == &shard.file_system_id
                && *prepare_sequence > 0
                && !payload_digest.is_empty()
        }
    }
}

/// Verify that the exact operation named by an authority acknowledgement is
/// durably present in the DFS metadata backend. This is deliberately a DFS
/// metadata check: ordinary writes do not put file metadata into cluster KV.
pub(crate) fn metadata_authority_probe_confirmation_id(
    probe: &DfsMetadataAuthorityProbe,
) -> Option<String> {
    let prefix = probe.prepared_prefix.as_ref()?;
    if probe.probe_id.is_empty()
        || !prefix.is_well_formed()
        || prefix.journal_sequence
            != metadata_authority_operation_prepare_sequence(&probe.operation)
    {
        return None;
    }
    Some(format!(
        "dfs-authority-prefix-v1:{}:{}:{}",
        probe.probe_id, prefix.journal_sequence, prefix.digest
    ))
}

/// Verify an authority probe immediately after applying its journal batch.
///
/// A newly applied PackedV2 prepare carries compact evidence computed from the
/// exact canonical allocation installed into the table. Reuse that evidence
/// instead of fetching, cloning, normalizing, and hashing the full prepare a
/// second time. Duplicate delivery and recovery intentionally carry no fresh
/// evidence and retain the durable-table fallback.
pub(crate) fn verify_persisted_metadata_authority_probe_after_apply(
    backend: &dyn DfsMetadataBackend,
    shard: &DfsMetadataShardConfig,
    probe: &DfsMetadataAuthorityProbe,
    apply_report: &DfsMetadataReplicaApplyReport,
) -> Result<String, DfsError> {
    let operation_verified = apply_report
        .namespace_authority_proof_for(shard, &probe.operation)
        .is_some();
    verify_persisted_metadata_authority_probe_inner(backend, shard, probe, operation_verified)
}

fn verify_persisted_metadata_authority_probe_inner(
    backend: &dyn DfsMetadataBackend,
    shard: &DfsMetadataShardConfig,
    probe: &DfsMetadataAuthorityProbe,
    operation_verified: bool,
) -> Result<String, DfsError> {
    if operation_verified {
        if !metadata_authority_operation_is_well_formed_for_shard(&probe.operation, shard) {
            return Err(DfsError::Metadata {
                message: format!(
                    "metadata authority operation is not valid for shard {} epoch {}",
                    shard.shard_id, shard.epoch
                ),
            });
        }
    } else {
        verify_persisted_metadata_authority_operation(backend, shard, &probe.operation)?;
    }
    let confirmation_id = metadata_authority_probe_confirmation_id(probe).ok_or_else(|| {
        DfsError::Metadata {
            message: format!(
                "metadata authority probe has no well-formed exact prepared prefix for shard {} epoch {}",
                shard.shard_id, shard.epoch
            ),
        }
    })?;
    let local_prefix = backend
        .metadata_prefix_proof(&DfsMetadataShardId::new(shard.shard_id.clone()))?
        .ok_or_else(|| DfsError::Metadata {
            message: format!(
                "metadata authority probe cannot read an exact local prefix for shard {}",
                shard.shard_id
            ),
        })?;
    if probe.prepared_prefix.as_ref() != Some(&local_prefix) {
        return Err(DfsError::Metadata {
            message: format!(
                "metadata authority operation is present but shard {} prefix does not match the primary's exact prepared history",
                shard.shard_id
            ),
        });
    }
    Ok(confirmation_id)
}

pub(crate) fn verify_persisted_metadata_authority_operation(
    backend: &dyn DfsMetadataBackend,
    shard: &DfsMetadataShardConfig,
    operation: &DfsMetadataAuthorityOperation,
) -> Result<(), DfsError> {
    if !metadata_authority_operation_is_well_formed_for_shard(operation, shard) {
        return Err(DfsError::Metadata {
            message: format!(
                "metadata authority operation is not valid for shard {} epoch {}",
                shard.shard_id, shard.epoch
            ),
        });
    }
    match operation {
        DfsMetadataAuthorityOperation::WriteIntent {
            intent_id,
            file_system_id,
            object_id,
            base_version,
            target_version,
            owner_epoch,
            content_hash,
            receipt_node_ids,
            namespace_prepare_operation_id,
            namespace_prepare_digest,
            prepare_sequence,
            ..
        } => {
            let intent =
                backend
                    .get_intent(object_id, intent_id)?
                    .ok_or_else(|| DfsError::Metadata {
                        message: format!(
                            "metadata authority intent {intent_id} is not durably prepared"
                        ),
                    })?;
            if intent.file_system_id != *file_system_id
                || intent.object_id != *object_id
                || intent.base_version != *base_version
                || intent.target_version != *target_version
                || intent.owner_epoch != *owner_epoch
                || intent.content_hash.as_deref() != Some(content_hash.as_str())
                || !matches!(
                    intent.status,
                    DfsWriteIntentStatus::ReceiptsPending | DfsWriteIntentStatus::Committed
                )
            {
                return Err(DfsError::Metadata {
                    message: format!(
                        "metadata authority intent {intent_id} does not match its prepared operation"
                    ),
                });
            }
            let mut normalized_receipt_nodes = receipt_node_ids.clone();
            normalized_receipt_nodes.sort();
            normalized_receipt_nodes.dedup();
            if normalized_receipt_nodes != *receipt_node_ids
                || normalized_receipt_nodes.len() < intent.policy.min_commit_replicas as usize
                || normalized_receipt_nodes
                    .iter()
                    .any(|node_id| !intent.desired_replica_node_ids.contains(node_id))
            {
                return Err(DfsError::Metadata {
                    message: format!(
                        "metadata authority intent {intent_id} has an invalid receipt set"
                    ),
                });
            }
            let durable_receipts = backend.receipts_for_version(object_id, target_version)?;
            for replica_node_id in &normalized_receipt_nodes {
                let prepared = durable_receipts.iter().any(|receipt| {
                    receipt.receipt.file_system_id == *file_system_id
                        && receipt.receipt.object_id == *object_id
                        && receipt.receipt.committed_version == *target_version
                        && receipt.receipt.owner_node_id == shard.primary_node_id
                        && receipt.replica_node_id == *replica_node_id
                        && receipt.content_hash.as_deref() == Some(content_hash.as_str())
                });
                if !prepared {
                    return Err(DfsError::Metadata {
                        message: format!(
                            "metadata authority intent {intent_id} is missing the durable receipt from {replica_node_id}"
                        ),
                    });
                }
            }
            let prepared_intent = DistributedWriteIntent {
                intent_id: intent.intent_id.clone(),
                file_system_id: intent.file_system_id.clone(),
                object_id: intent.object_id.clone(),
                base_version: intent.base_version.clone(),
                target_version: intent.target_version.clone(),
                requesting_node_id: intent.coordinator_node_id.clone(),
                owner_epoch: intent.owner_epoch,
                desired_replica_nodes: intent.desired_replica_node_ids.clone(),
                policy: intent.policy.clone(),
                touch_parent_on_create: intent.touch_parent_on_create,
                create_predecessor_object_id: intent.create_predecessor_object_id.clone(),
                state: DfsWriteIntentState::AwaitingReceipts,
            };
            let namespace_prepare = match (
                namespace_prepare_operation_id.as_deref(),
                namespace_prepare_digest.as_deref(),
            ) {
                (Some(operation_id), Some(expected_digest)) => {
                    let prepare = backend
                        .get_namespace_prepare(operation_id)?
                        .ok_or_else(|| DfsError::Metadata {
                            message: format!(
                                "metadata authority intent {intent_id} is missing namespace prepare {operation_id}"
                            ),
                        })?;
                    if prepare.operation_digest != expected_digest
                        || prepare.file_system_id != *file_system_id
                        || prepare.shard_id.0 != shard.shard_id
                        || prepare.owner_epoch != shard.epoch
                        || !matches!(
                            prepare.status,
                            DfsNamespacePrepareStatus::Prepared
                                | DfsNamespacePrepareStatus::Committed
                        )
                    {
                        return Err(DfsError::Metadata {
                            message: format!(
                                "metadata authority intent {intent_id} has a mismatched namespace prepare"
                            ),
                        });
                    }
                    Some(prepare)
                }
                (None, None) => None,
                _ => {
                    return Err(DfsError::Metadata {
                        message: format!(
                            "metadata authority intent {intent_id} has an incomplete namespace prepare identity"
                        ),
                    });
                }
            };
            let expected = metadata_authority_operation_for_intent(
                &prepared_intent,
                content_hash,
                &normalized_receipt_nodes,
                namespace_prepare.as_ref(),
                *prepare_sequence,
                shard,
            )?;
            if &expected != operation {
                return Err(DfsError::Metadata {
                    message: format!(
                        "metadata authority intent {intent_id} has a mismatched operation digest"
                    ),
                });
            }
        }
        DfsMetadataAuthorityOperation::NamespaceMutation {
            operation_id,
            file_system_id,
            prepare_sequence,
            ..
        } => {
            let prepare = backend
                .get_namespace_prepare(operation_id)?
                .ok_or_else(|| DfsError::Metadata {
                    message: format!(
                        "namespace authority operation {operation_id} is not durably prepared"
                    ),
                })?;
            if prepare.file_system_id != *file_system_id
                || prepare.shard_id.0 != shard.shard_id
                || prepare.owner_epoch != shard.epoch
                || !matches!(
                    prepare.status,
                    DfsNamespacePrepareStatus::Prepared | DfsNamespacePrepareStatus::Committed
                )
            {
                return Err(DfsError::Metadata {
                    message: format!(
                        "namespace authority operation {operation_id} does not match its shard prepare"
                    ),
                });
            }
            let expected =
                metadata_authority_operation_for_namespace_prepare(&prepare, *prepare_sequence)?;
            if &expected != operation {
                return Err(DfsError::Metadata {
                    message: format!(
                        "namespace authority operation {operation_id} has a mismatched operation digest"
                    ),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod authority_probe_tests {
    use super::*;
    use sinomo_core::DfsWriteReceipt;
    use sinomo_core::events::EventId;
    use sinomo_core::kv::{InProcessKvStore, KvEntry, KvError, KvRevision, KvTxn, KvTxnResponse};
    use sinomo_core::network::NetworkError;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use time::OffsetDateTime;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ProbeBehavior {
        Accept,
        Reject,
        Unroutable,
        Capture,
    }

    #[derive(Debug)]
    struct AuthorityProbeTransport {
        local_node_id: NodeId,
        behavior: ProbeBehavior,
        direct_metadata_journal_batches: bool,
        direct_commands: Vec<DfsMetadataJournalBatchCommand>,
        direct_group_target_counts: Vec<usize>,
        direct_next_after_sequence_override: Option<u64>,
        unroutable_node_ids: BTreeSet<NodeId>,
        incoming: VecDeque<DfsRoutedMessage>,
        sent: Arc<Mutex<Vec<DfsRoutedMessage>>>,
        operation_deadline: Option<Instant>,
        deadline_updates: Vec<Option<Instant>>,
    }

    impl AuthorityProbeTransport {
        fn new(local_node_id: NodeId, behavior: ProbeBehavior) -> Self {
            Self {
                local_node_id,
                behavior,
                direct_metadata_journal_batches: false,
                direct_commands: Vec::new(),
                direct_group_target_counts: Vec::new(),
                direct_next_after_sequence_override: None,
                unroutable_node_ids: BTreeSet::new(),
                incoming: VecDeque::new(),
                sent: Arc::new(Mutex::new(Vec::new())),
                operation_deadline: None,
                deadline_updates: Vec::new(),
            }
        }
    }

    impl DfsTransportIdentity for AuthorityProbeTransport {
        fn local_node_id(&self) -> &NodeId {
            &self.local_node_id
        }
    }

    impl DfsMessageTransport for AuthorityProbeTransport {
        fn send_routed_call(
            &mut self,
            target_node_id: &NodeId,
            call: DfsRoutedCall,
        ) -> Result<(), NetworkError> {
            let message = crate::materialize_dfs_routed_call_sync(call)?;
            self.sent
                .lock()
                .expect("sent messages")
                .push(message.clone());
            let DfsNetworkMessage::MetadataJournalBatch(command) = message.message else {
                return Ok(());
            };
            let Some(probe) = command.authority_probe else {
                return Ok(());
            };
            if self.behavior == ProbeBehavior::Unroutable
                || self.unroutable_node_ids.contains(target_node_id)
            {
                return Err(NetworkError::UnknownNode {
                    node_id: target_node_id.clone(),
                });
            }
            if self.behavior == ProbeBehavior::Capture {
                return Ok(());
            }
            let accepted = self.behavior == ProbeBehavior::Accept;
            self.incoming.push_back(DfsRoutedMessage {
                source_node_id: target_node_id.clone(),
                target_node_id: self.local_node_id.clone(),
                frame_id: message.frame_id,
                message_id: message.message_id,
                message: DfsNetworkMessage::MetadataJournalBatchResponse(
                    DfsMetadataJournalBatchResponse {
                        request_id: command.request_id,
                        shard_id: command.batch.shard_id,
                        next_after_sequence: command.batch.next_after_sequence,
                        applied_entries: 0,
                        verified_prefix_only: false,
                        verified_authority_probe_id: accepted
                            .then(|| metadata_authority_probe_confirmation_id(&probe))
                            .flatten(),
                        result: if accepted {
                            DfsMetadataJournalBatchResult::Applied
                        } else {
                            DfsMetadataJournalBatchResult::Rejected {
                                reason: "stale authority".to_string(),
                            }
                        },
                    },
                ),
            });
            Ok(())
        }

        fn send_metadata_journal_batch_direct(
            &mut self,
            target_node_id: &NodeId,
            command: &DfsMetadataJournalBatchCommand,
        ) -> Result<Option<DfsMetadataJournalBatchResponse>, NetworkError> {
            if !self.direct_metadata_journal_batches {
                return Ok(None);
            }
            self.direct_commands.push(command.clone());
            if self.behavior == ProbeBehavior::Unroutable
                || self.unroutable_node_ids.contains(target_node_id)
            {
                return Err(NetworkError::UnknownNode {
                    node_id: target_node_id.clone(),
                });
            }
            let Some(probe) = command.authority_probe.as_ref() else {
                return Ok(None);
            };
            if self.behavior == ProbeBehavior::Capture {
                return Ok(None);
            }
            let accepted = self.behavior == ProbeBehavior::Accept;
            Ok(Some(DfsMetadataJournalBatchResponse {
                request_id: command.request_id.clone(),
                shard_id: command.batch.shard_id.clone(),
                next_after_sequence: self
                    .direct_next_after_sequence_override
                    .unwrap_or(command.batch.next_after_sequence),
                applied_entries: 0,
                verified_prefix_only: false,
                verified_authority_probe_id: accepted
                    .then(|| metadata_authority_probe_confirmation_id(probe))
                    .flatten(),
                result: if accepted {
                    DfsMetadataJournalBatchResult::Applied
                } else {
                    DfsMetadataJournalBatchResult::Rejected {
                        reason: "stale authority".to_string(),
                    }
                },
            }))
        }

        fn send_grouped_metadata_journal_batches_direct_with_response_timeout(
            &mut self,
            groups: &[DfsMetadataJournalBatchDirectGroup],
            _response_timeout: Duration,
        ) -> Result<Option<Vec<(NodeId, DfsMetadataJournalBatchResponse)>>, NetworkError> {
            if !self.direct_metadata_journal_batches {
                return Ok(None);
            }
            self.direct_group_target_counts
                .extend(groups.iter().map(|group| group.target_node_ids.len()));
            let mut responses = Vec::new();
            for group in groups {
                for target_node_id in &group.target_node_ids {
                    let Some(response) =
                        self.send_metadata_journal_batch_direct(target_node_id, &group.command)?
                    else {
                        return Ok(None);
                    };
                    responses.push((target_node_id.clone(), response));
                }
            }
            Ok(Some(responses))
        }

        fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
            Ok(self.incoming.drain(..).collect())
        }

        fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
            Ok(Some(self.incoming.len()))
        }

        fn set_operation_deadline(&mut self, deadline: Option<Instant>) -> Option<Instant> {
            self.deadline_updates.push(deadline);
            std::mem::replace(&mut self.operation_deadline, deadline)
        }

        fn operation_deadline(&self) -> Option<Instant> {
            self.operation_deadline
        }
    }

    #[derive(Debug)]
    struct BarrierCountingControlPlane {
        inner: InProcessKvStore,
        txn_calls: Arc<AtomicUsize>,
        range_prefix_calls: Arc<AtomicUsize>,
    }

    impl DfsControlPlaneKvClient for BarrierCountingControlPlane {
        fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
            DfsControlPlaneKvClient::get(&self.inner, key)
        }

        fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
            self.range_prefix_calls.fetch_add(1, Ordering::SeqCst);
            DfsControlPlaneKvClient::range_prefix(&self.inner, prefix)
        }

        fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
            DfsControlPlaneKvClient::put(&mut self.inner, key, value)
        }

        fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
            self.txn_calls.fetch_add(1, Ordering::SeqCst);
            DfsControlPlaneKvClient::txn(&mut self.inner, txn)
        }
    }

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn authority_fixture(
        behavior: ProbeBehavior,
        replica_node_ids: Vec<NodeId>,
    ) -> (
        NetworkedDfsService<AuthorityProbeTransport>,
        DfsObjectMetadata,
        DistributedWriteIntent,
        DfsMetadataShardConfig,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let root = sinomo_testdir::TestDir::new("dfs-authority-probe").keep();
        let primary = node("node_authority_probe_primary");
        let file_system_id = sifs_file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: primary.clone(),
            replica_node_ids,
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let mut inner = InProcessKvStore::new();
        DfsControlPlaneStore::new(&mut inner)
            .put_metadata_shard(&shard)
            .expect("metadata shard");
        let txn_calls = Arc::new(AtomicUsize::new(0));
        let range_prefix_calls = Arc::new(AtomicUsize::new(0));
        let control_plane = BarrierCountingControlPlane {
            inner,
            txn_calls: Arc::clone(&txn_calls),
            range_prefix_calls: Arc::clone(&range_prefix_calls),
        };
        let object_id = DfsObjectId::from_legacy_named("object_authority_probe").expect("object");
        let version = DfsVersion::from_legacy_named("version_authority_probe_1").expect("version");
        let target_version =
            DfsVersion::from_legacy_named("version_authority_probe_2").expect("version");
        let object = DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system_id.clone(),
            path: "/authority-probe.txt".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: primary.clone(),
            current_locations: vec![primary.clone()],
            content_hash: Some("blake3:authority-probe".to_string()),
            content_size_bytes: Some(1),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 1,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 1,
            version: version.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        let intent = DistributedWriteIntent {
            intent_id: "intent_authority_probe".to_string(),
            file_system_id,
            object_id,
            base_version: Some(version),
            target_version,
            requesting_node_id: primary.clone(),
            owner_epoch: shard.epoch,
            desired_replica_nodes: vec![primary.clone()],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            state: DfsWriteIntentState::AwaitingReceipts,
        };
        let mut metadata_backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new(shard.shard_id.clone()),
            root.join("metadata"),
        )
        .expect("metadata backend");
        metadata_backend
            .put_file_system(&DfsFileSystem::sifs(primary.clone()))
            .expect("filesystem metadata");
        metadata_backend
            .put_object(&object)
            .expect("base object metadata");
        let service = NetworkedDfsService::new(
            AuthorityProbeTransport::new(primary, behavior),
            LocalContentStore::new(root.join("content")).expect("content"),
        )
        .with_control_plane(control_plane)
        .with_metadata_backend(metadata_backend)
        .expect("metadata-backed authority service");
        (
            service,
            object,
            intent,
            shard,
            txn_calls,
            range_prefix_calls,
        )
    }

    fn prepared_authority_fixture(
        service: &mut NetworkedDfsService<AuthorityProbeTransport>,
        object: &DfsObjectMetadata,
        intent: &DistributedWriteIntent,
        shard: &DfsMetadataShardConfig,
    ) -> (DfsMetadataAuthorityOperation, DfsMetadataJournalBatch) {
        let content_hash = "blake3:prepared-authority";
        let receipt_node_ids = intent.desired_replica_nodes.clone();
        let intent_record = write_intent_record(intent, Some(content_hash), Some(object.clone()));
        let receipts = receipt_node_ids
            .iter()
            .map(|replica_node_id| DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: intent.file_system_id.clone(),
                    object_id: intent.object_id.clone(),
                    committed_version: intent.target_version.clone(),
                    owner_node_id: shard.primary_node_id.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: replica_node_id.clone(),
                content_hash: Some(content_hash.to_string()),
                received_at: OffsetDateTime::now_utc(),
            })
            .collect::<Vec<_>>();
        let entries = service
            .metadata_backend
            .as_mut()
            .expect("authority fixture metadata backend")
            .put_intent_and_receipts_for_object(Some(object), &intent_record, &receipts)
            .expect("persist exact authority prepare");
        let prepare_sequence = entries.last().expect("authority prepare entry").sequence;
        let operation = metadata_authority_operation_for_intent(
            intent,
            content_hash,
            &receipt_node_ids,
            None,
            prepare_sequence,
            shard,
        )
        .expect("authority operation");
        (
            operation,
            DfsMetadataJournalBatch {
                shard_id: DfsMetadataShardId::new(shard.shard_id.clone()),
                entries,
                next_after_sequence: prepare_sequence,
            },
        )
    }

    fn prepared_namespace_authority_fixture(
        service: &mut NetworkedDfsService<AuthorityProbeTransport>,
        object: &DfsObjectMetadata,
        shard: &DfsMetadataShardConfig,
        object_count: usize,
    ) -> (DfsMetadataAuthorityOperation, DfsMetadataJournalBatch) {
        let objects = (0..object_count)
            .map(|index| {
                let mut candidate = object.clone();
                candidate.object_id =
                    DfsObjectId::from_legacy_named(format!("object_namespace_authority_{index}"))
                        .expect("namespace object id");
                candidate.path = format!("/namespace-authority-{index}");
                candidate.version =
                    DfsVersion::from_legacy_named(format!("version_namespace_authority_{index}"))
                        .expect("namespace object version");
                candidate
            })
            .collect::<Vec<_>>();
        let expected_object_versions = objects
            .iter()
            .map(|object| DfsNamespaceObjectVersionExpectation {
                object_id: object.object_id.clone(),
                expected_version: None,
            })
            .collect();
        let expected_path_occupants = objects
            .iter()
            .map(|object| DfsNamespacePathOccupantExpectation {
                path: object.path.clone(),
                expected_object_id: None,
            })
            .collect();
        let mut prepare = DfsNamespacePrepareRecord {
            operation_id: "operation_namespace_authority_probe".to_string(),
            file_system_id: shard.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(shard.shard_id.clone()),
            owner_epoch: shard.epoch,
            operation_digest: String::new(),
            expected_object_versions,
            expected_path_occupants,
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                transaction_id: "transaction_namespace_authority_probe".to_string(),
                file_system_id: shard.file_system_id.clone(),
                objects,
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::now_utc(),
            },
            completed_request: None,
            completed_request_completion: None,
            status: DfsNamespacePrepareStatus::Prepared,
        };
        prepare.operation_digest =
            metadata_namespace_prepare_payload_digest(&prepare).expect("namespace prepare digest");
        let entry = service
            .metadata_backend
            .as_mut()
            .expect("namespace authority metadata backend")
            .prepare_namespace_transaction(&prepare)
            .expect("persist namespace prepare");
        let prepare_sequence = entry.sequence;
        let independently_hashed =
            metadata_authority_operation_for_namespace_prepare(&prepare, prepare_sequence)
                .expect("independently hashed namespace authority operation");
        let operation = metadata_authority_operation_for_prevalidated_namespace_prepare(
            &prepare,
            prepare_sequence,
        )
        .expect("prevalidated namespace authority operation");
        assert_eq!(operation, independently_hashed);
        (
            operation,
            DfsMetadataJournalBatch {
                shard_id: prepare.shard_id.clone(),
                entries: vec![entry],
                next_after_sequence: prepare_sequence,
            },
        )
    }

    #[test]
    fn streamed_authority_payload_digest_matches_legacy_buffered_bytes() {
        let payload = (
            "namespace-authority-streaming",
            (0..512)
                .map(|index| format!("object-{index:04}-{}", "x".repeat(128)))
                .collect::<Vec<_>>(),
            vec![7_u64, 11, 13, 17],
        );
        let buffered = serde_json::to_vec(&payload).expect("legacy buffered authority payload");
        let expected = format!("blake3:{}", blake3::hash(&buffered).to_hex());

        assert_eq!(
            metadata_authority_payload_digest(&payload).expect("streamed authority digest"),
            expected
        );
    }

    fn persisted_small_namespace_authority_fixture(
        service: &mut NetworkedDfsService<AuthorityProbeTransport>,
        object: &DfsObjectMetadata,
        shard: &DfsMetadataShardConfig,
    ) -> (DfsMetadataAuthorityOperation, DfsMetadataJournalBatch) {
        let mut committed = object.clone();
        committed.version = DfsVersion::from_legacy_named("version_small_namespace_authority_2")
            .expect("committed version");
        committed.changed_at_unix_nanos = object.changed_at_unix_nanos.saturating_add(1);
        let mut prepare = DfsNamespacePrepareRecord {
            operation_id: "operation_small_namespace_authority".to_string(),
            file_system_id: object.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(shard.shard_id.clone()),
            owner_epoch: shard.epoch,
            operation_digest: String::new(),
            expected_object_versions: vec![DfsNamespaceObjectVersionExpectation {
                object_id: object.object_id.clone(),
                expected_version: Some(object.version.clone()),
            }],
            expected_path_occupants: vec![DfsNamespacePathOccupantExpectation {
                path: object.path.clone(),
                expected_object_id: Some(object.object_id.clone()),
            }],
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                transaction_id: "transaction_small_namespace_authority".to_string(),
                file_system_id: object.file_system_id.clone(),
                objects: vec![committed],
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::now_utc(),
            },
            completed_request: None,
            completed_request_completion: None,
            status: DfsNamespacePrepareStatus::Prepared,
        };
        prepare.operation_digest =
            metadata_namespace_prepare_payload_digest(&prepare).expect("namespace prepare digest");
        let entry = service
            .metadata_backend
            .as_mut()
            .expect("namespace authority metadata backend")
            .prepare_namespace_transaction(&prepare)
            .expect("persist small namespace prepare");
        let prepare_sequence = entry.sequence;
        let operation =
            metadata_authority_operation_for_namespace_prepare(&prepare, prepare_sequence)
                .expect("namespace authority operation");
        (
            operation,
            DfsMetadataJournalBatch {
                shard_id: prepare.shard_id,
                entries: vec![entry],
                next_after_sequence: prepare_sequence,
            },
        )
    }

    #[test]
    fn persisted_write_authority_requires_the_exact_intent_and_w_receipts() {
        let replica = node("node_persisted_authority_replica");
        let (_, object, mut intent, shard, _, _) =
            authority_fixture(ProbeBehavior::Capture, vec![replica.clone()]);
        intent.desired_replica_nodes = vec![shard.primary_node_id.clone(), replica.clone()];
        intent.policy = DfsReplicationPolicy::new(2, 2).expect("W=2 policy");
        let content_hash = "blake3:persisted-authority";
        let root = sinomo_testdir::TestDir::new("dfs-persisted-write-authority");
        let mut backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new(shard.shard_id.clone()),
            root.join("metadata"),
        )
        .expect("metadata backend");
        backend
            .put_file_system(&DfsFileSystem::sifs(shard.primary_node_id.clone()))
            .expect("filesystem");
        backend.put_object(&object).expect("base object");

        let intent_record = write_intent_record(&intent, Some(content_hash), Some(object.clone()));
        let receipts = intent
            .desired_replica_nodes
            .iter()
            .map(|replica_node_id| DfsReplicaReceiptRecord {
                receipt: DfsWriteReceipt {
                    file_system_id: intent.file_system_id.clone(),
                    object_id: intent.object_id.clone(),
                    committed_version: intent.target_version.clone(),
                    owner_node_id: shard.primary_node_id.clone(),
                    event_id: EventId::generate(),
                },
                replica_node_id: replica_node_id.clone(),
                content_hash: Some(content_hash.to_string()),
                received_at: OffsetDateTime::now_utc(),
            })
            .collect::<Vec<_>>();
        let entries = backend
            .put_intent_and_receipts_for_object(Some(&object), &intent_record, &receipts)
            .expect("persist hidden intent and receipts");
        let prepare_sequence = entries.last().expect("prepare entry").sequence;
        let operation = metadata_authority_operation_for_intent(
            &intent,
            content_hash,
            &intent.desired_replica_nodes,
            None,
            prepare_sequence,
            &shard,
        )
        .expect("canonical write operation");

        verify_persisted_metadata_authority_operation(&backend, &shard, &operation)
            .expect("exact persisted prepare is eligible for acknowledgement");

        let mut altered_operation = operation.clone();
        let DfsMetadataAuthorityOperation::WriteIntent {
            ref mut content_hash,
            ..
        } = altered_operation
        else {
            unreachable!("write operation fixture")
        };
        *content_hash = "blake3:altered-authority".to_string();
        let altered_error =
            verify_persisted_metadata_authority_operation(&backend, &shard, &altered_operation)
                .expect_err("an altered operation must not reuse persisted prepare evidence");
        assert!(altered_error.to_string().contains("does not match"));

        backend
            .remove_receipt(&intent.object_id, &intent.target_version, &replica)
            .expect("remove one W receipt")
            .expect("receipt existed");
        let missing_receipt_error =
            verify_persisted_metadata_authority_operation(&backend, &shard, &operation)
                .expect_err("fewer than W durable receipts must reject authority");
        assert!(
            missing_receipt_error
                .to_string()
                .contains("missing the durable receipt")
        );
    }

    #[test]
    fn persisted_namespace_authority_requires_the_exact_prepare_digest() {
        let (_, object, _, shard, _, _) = authority_fixture(ProbeBehavior::Capture, Vec::new());
        let root = sinomo_testdir::TestDir::new("dfs-persisted-namespace-authority");
        let mut backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new(shard.shard_id.clone()),
            root.join("metadata"),
        )
        .expect("metadata backend");
        backend
            .put_file_system(&DfsFileSystem::sifs(shard.primary_node_id.clone()))
            .expect("filesystem");
        backend.put_object(&object).expect("base object");

        let mut committed = object.clone();
        committed.version =
            DfsVersion::from_legacy_named("version_namespace_authority_2").expect("version");
        committed.changed_at_unix_nanos = object.changed_at_unix_nanos.saturating_add(1);
        let mut prepare = DfsNamespacePrepareRecord {
            operation_id: "operation_namespace_authority".to_string(),
            file_system_id: object.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(shard.shard_id.clone()),
            owner_epoch: shard.epoch,
            operation_digest: String::new(),
            expected_object_versions: vec![DfsNamespaceObjectVersionExpectation {
                object_id: object.object_id.clone(),
                expected_version: Some(object.version.clone()),
            }],
            expected_path_occupants: vec![DfsNamespacePathOccupantExpectation {
                path: object.path.clone(),
                expected_object_id: Some(object.object_id.clone()),
            }],
            content_receipts: Vec::new(),
            transaction: DfsNamespaceTransaction {
                transaction_id: "transaction_namespace_authority".to_string(),
                file_system_id: object.file_system_id.clone(),
                objects: vec![committed],
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::now_utc(),
            },
            completed_request: None,
            completed_request_completion: None,
            status: DfsNamespacePrepareStatus::Prepared,
        };
        prepare.operation_digest =
            metadata_namespace_prepare_payload_digest(&prepare).expect("canonical prepare digest");
        let prepare_entry = backend
            .prepare_namespace_transaction(&prepare)
            .expect("persist namespace prepare");
        let operation =
            metadata_authority_operation_for_namespace_prepare(&prepare, prepare_entry.sequence)
                .expect("canonical namespace operation");

        verify_persisted_metadata_authority_operation(&backend, &shard, &operation)
            .expect("exact persisted namespace prepare is eligible for acknowledgement");

        let mut altered_operation = operation;
        let DfsMetadataAuthorityOperation::NamespaceMutation {
            ref mut payload_digest,
            ..
        } = altered_operation
        else {
            unreachable!("namespace operation fixture")
        };
        *payload_digest = content_hash(b"different namespace operation");
        let digest_error =
            verify_persisted_metadata_authority_operation(&backend, &shard, &altered_operation)
                .expect_err("a digest mismatch must not acknowledge the prepare");
        assert!(
            digest_error
                .to_string()
                .contains("mismatched operation digest")
        );

        let commit_error = backend
            .commit_prepared_namespace_transaction(
                &prepare.operation_id,
                "blake3:different-prepare",
                prepare.owner_epoch,
            )
            .expect_err("the backend commit fence must reject a different digest");
        assert!(commit_error.to_string().contains("digest/epoch fence"));
    }

    #[test]
    fn clustered_metadata_owner_epoch_hints_fail_closed_without_a_shard_assignment() {
        let (mut service, object, _, _, _, _) =
            authority_fixture(ProbeBehavior::Capture, Vec::new());
        service.control_plane = Some(Box::new(InProcessKvStore::new()));

        let hint_error = service
            .local_metadata_owner_epoch_hint_for_object(&object)
            .expect_err("clustered hint must not fall back to standalone epoch zero");
        assert!(
            hint_error
                .to_string()
                .contains("no committed metadata-shard assignment")
        );

        let certified_error = service
            .local_metadata_owner_epoch_for_object(&object)
            .expect_err("certified clustered epoch must also fail closed");
        assert!(
            certified_error
                .to_string()
                .contains("no committed metadata-shard assignment")
        );
    }

    #[test]
    fn fresh_full_shard_authority_uses_no_kv_operation_barrier() {
        let replicas = vec![
            node("node_authority_probe_replica_a"),
            node("node_authority_probe_replica_b"),
        ];
        let (mut service, object, intent, shard, txn_calls, _) =
            authority_fixture(ProbeBehavior::Accept, replicas);
        let (operation, prepared_batch) =
            prepared_authority_fixture(&mut service, &object, &intent, &shard);

        service
            .authorize_prepared_intent_for_object(&intent, &object, &operation, &prepared_batch)
            .expect("fresh full-shard authority");

        assert_eq!(txn_calls.load(Ordering::SeqCst), 0);
        assert!(service.pending_metadata_authority_probes.is_empty());
        assert!(service.metadata_authority_probe_results.is_empty());
    }

    #[test]
    fn direct_full_shard_authority_ack_checkpoints_replication_cursor_durably() {
        let replica = node("node_direct_authority_cursor_replica");
        let (mut service, object, intent, shard, txn_calls, _) =
            authority_fixture(ProbeBehavior::Accept, vec![replica.clone()]);
        service.transport.direct_metadata_journal_batches = true;
        let sent = Arc::clone(&service.transport.sent);
        let (operation, prepared_batch) =
            prepared_authority_fixture(&mut service, &object, &intent, &shard);

        let authorization = service
            .authorize_metadata_shard_operation(&shard, &operation, &prepared_batch)
            .expect("direct full-shard authority");

        assert!(matches!(
            authorization,
            DfsCanonicalAuthorization::FullShard { .. }
        ));
        assert_eq!(txn_calls.load(Ordering::SeqCst), 0);
        assert!(
            sent.lock().expect("sent messages").is_empty(),
            "the direct acknowledgement must not fall back to routed messaging"
        );
        assert_eq!(
            service
                .metadata_backend
                .as_ref()
                .expect("metadata backend")
                .metadata_replication_cursor(&replica, &prepared_batch.shard_id)
                .expect("durable direct authority cursor"),
            prepared_batch.next_after_sequence
        );
    }

    #[test]
    fn authority_probe_restores_caller_deadline_on_every_direct_outcome() {
        for (case, direct, behavior) in [
            ("direct-success", true, ProbeBehavior::Accept),
            ("routed-fallback", false, ProbeBehavior::Accept),
            ("direct-error", true, ProbeBehavior::Unroutable),
        ] {
            let replica = node(&format!("node_authority_deadline_{case}_replica"));
            let (mut service, object, intent, shard, _, _) =
                authority_fixture(behavior, vec![replica]);
            service.transport.direct_metadata_journal_batches = direct;
            let (operation, prepared_batch) =
                prepared_authority_fixture(&mut service, &object, &intent, &shard);
            let caller_deadline = Instant::now() + Duration::from_secs(2);
            service.transport.operation_deadline = Some(caller_deadline);
            service.transport.deadline_updates.clear();

            service
                .fresh_metadata_shard_authority_acknowledgements(
                    &shard,
                    &operation,
                    &prepared_batch,
                    DFS_METADATA_AUTHORITY_PROBE_TIMEOUT,
                )
                .expect("authority probe outcome");

            assert_eq!(
                service.transport.operation_deadline,
                Some(caller_deadline),
                "{case} must restore the caller's absolute deadline"
            );
            assert_eq!(
                service.transport.deadline_updates.last(),
                Some(&Some(caller_deadline)),
                "{case} must finish by restoring the caller deadline"
            );
        }
    }

    #[test]
    fn malformed_direct_authority_next_sequence_cannot_advance_cursor() {
        let replica = node("node_malformed_direct_authority_cursor_replica");
        let (mut service, object, intent, shard, _, _) =
            authority_fixture(ProbeBehavior::Accept, vec![replica.clone()]);
        service.transport.direct_metadata_journal_batches = true;
        let (operation, prepared_batch) =
            prepared_authority_fixture(&mut service, &object, &intent, &shard);
        service.transport.direct_next_after_sequence_override =
            Some(prepared_batch.next_after_sequence.saturating_add(10_000));

        let acknowledged = service
            .fresh_metadata_shard_authority_acknowledgements(
                &shard,
                &operation,
                &prepared_batch,
                DFS_METADATA_AUTHORITY_PROBE_TIMEOUT,
            )
            .expect("malformed response is ignored");

        assert_eq!(acknowledged, BTreeSet::from([service.node_id.clone()]));
        assert_eq!(
            service
                .metadata_backend
                .as_ref()
                .expect("metadata backend")
                .metadata_replication_cursor(&replica, &prepared_batch.shard_id)
                .expect("durable cursor after malformed response"),
            0,
            "an untrusted response must not advance beyond the command's primary journal head"
        );
        assert!(
            !service
                .metadata_replication_cursors
                .contains_key(&(replica, prepared_batch.shard_id)),
            "the in-memory optimization cursor must reject the malformed response too"
        );
    }

    #[test]
    fn routed_full_shard_authority_ack_checkpoints_replication_cursor_durably() {
        let replica = node("node_routed_authority_cursor_replica");
        let (mut service, object, intent, shard, txn_calls, _) =
            authority_fixture(ProbeBehavior::Accept, vec![replica.clone()]);
        let sent = Arc::clone(&service.transport.sent);
        let (operation, prepared_batch) =
            prepared_authority_fixture(&mut service, &object, &intent, &shard);

        let authorization = service
            .authorize_metadata_shard_operation(&shard, &operation, &prepared_batch)
            .expect("routed full-shard authority");

        assert!(matches!(
            authorization,
            DfsCanonicalAuthorization::FullShard { .. }
        ));
        assert_eq!(txn_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            sent.lock().expect("sent messages").len(),
            1,
            "message-only transport should exercise the routed acknowledgement path"
        );
        assert_eq!(
            service
                .metadata_backend
                .as_ref()
                .expect("metadata backend")
                .metadata_replication_cursor(&replica, &prepared_batch.shard_id)
                .expect("durable routed authority cursor"),
            prepared_batch.next_after_sequence
        );
    }

    #[test]
    fn exact_authority_retry_preserves_the_monotonic_durable_cursor() {
        let replica = node("node_exact_authority_retry_cursor_replica");
        let (mut service, object, intent, shard, _, _) =
            authority_fixture(ProbeBehavior::Accept, vec![replica.clone()]);
        let (operation, prepared_batch) =
            prepared_authority_fixture(&mut service, &object, &intent, &shard);
        service
            .authorize_metadata_shard_operation(&shard, &operation, &prepared_batch)
            .expect("initial full-shard authority");
        DFS_METADATA_AUTHORITY_BATCH_READS.with(|reads| reads.set(0));

        let retry_authorization = service
            .authorize_metadata_shard_operation(&shard, &operation, &prepared_batch)
            .expect("exact full-shard authority retry");

        assert!(matches!(
            retry_authorization,
            DfsCanonicalAuthorization::FullShard { .. }
        ));
        assert_eq!(
            DFS_METADATA_AUTHORITY_BATCH_READS.with(std::cell::Cell::get),
            0,
            "an exact retry should reuse its prepared batch instead of rereading the journal"
        );
        service.record_metadata_authority_replication_cursor(
            &replica,
            &prepared_batch.shard_id,
            prepared_batch.next_after_sequence.saturating_sub(1),
        );
        assert_eq!(
            service
                .metadata_backend
                .as_ref()
                .expect("metadata backend")
                .metadata_replication_cursor(&replica, &prepared_batch.shard_id)
                .expect("monotonic durable authority cursor"),
            prepared_batch.next_after_sequence,
            "an older acknowledgement must not regress the durable cursor"
        );
        assert_eq!(
            service
                .metadata_replication_cursors
                .get(&(replica, prepared_batch.shard_id.clone())),
            Some(&prepared_batch.next_after_sequence),
            "the in-memory optimization cursor must remain monotonic too"
        );
    }

    #[test]
    fn partial_shard_authority_uses_operation_bound_kv_fallback() {
        let replica_a = node("node_authority_probe_quorum_replica_a");
        let replica_b = node("node_authority_probe_quorum_replica_b");
        let offline = node("node_authority_probe_quorum_offline");
        let (mut service, object, intent, shard, txn_calls, _) = authority_fixture(
            ProbeBehavior::Accept,
            vec![replica_a.clone(), replica_b.clone(), offline.clone()],
        );
        service
            .transport
            .unroutable_node_ids
            .insert(offline.clone());
        let (operation, prepared_batch) =
            prepared_authority_fixture(&mut service, &object, &intent, &shard);

        let authorization = service
            .authorize_prepared_intent_for_object(&intent, &object, &operation, &prepared_batch)
            .expect("the DFS recovery quorum may use the operation-bound Raft fence")
            .expect("clustered authorization");
        assert!(
            matches!(
                authorization,
                DfsCanonicalAuthorization::ClusterQuorum { .. }
            ),
            "the unavailable member must force the explicit degraded-path authorization"
        );
        assert_eq!(txn_calls.load(Ordering::SeqCst), 1);
        assert!(service.pending_metadata_authority_probes.is_empty());
        assert!(service.metadata_authority_probe_results.is_empty());
    }

    #[test]
    fn oversized_namespace_authority_replicates_prepare_before_publication() {
        let replicas = vec![
            node("node_oversized_namespace_authority_replica_a"),
            node("node_oversized_namespace_authority_replica_b"),
        ];
        let expected_probe_count = replicas.len();
        let (mut service, object, _, shard, txn_calls, _) =
            authority_fixture(ProbeBehavior::Accept, replicas.clone());
        let sent = Arc::clone(&service.transport.sent);
        let (operation, prepared_batch) =
            prepared_namespace_authority_fixture(&mut service, &object, &shard, 257);
        assert_eq!(
            metadata_authority_probe_timeout(&shard, &operation, &prepared_batch),
            DFS_METADATA_AUTHORITY_LARGE_PROBE_TIMEOUT
        );

        let authorization = service
            .authorize_metadata_shard_operation(&shard, &operation, &prepared_batch)
            .expect("oversized namespace prepare replicates through the DFS data plane");
        assert!(
            matches!(authorization, DfsCanonicalAuthorization::FullShard { .. }),
            "batch size alone must not bypass full-shard DFS durability"
        );
        assert_eq!(txn_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            sent.lock().expect("sent messages").len(),
            expected_probe_count,
            "every configured replica must receive the prepared batch"
        );
        drop(
            service
                .validate_canonical_authorization(&authorization, &operation)
                .expect("full-shard evidence remains subject to canonical revalidation"),
        );
    }

    #[test]
    fn authority_fanout_reuses_exact_prepared_suffix_for_five_equal_cursors() {
        let replicas = (0..5)
            .map(|index| node(&format!("node_shared_authority_cursor_replica_{index}")))
            .collect::<Vec<_>>();
        let expected_probe_count = replicas.len();
        let (mut service, object, _, shard, txn_calls, _) =
            authority_fixture(ProbeBehavior::Accept, replicas.clone());
        let sent = Arc::clone(&service.transport.sent);
        let (operation, prepared_batch) =
            prepared_namespace_authority_fixture(&mut service, &object, &shard, 257);
        for replica in &replicas {
            service.record_metadata_authority_replication_cursor(
                replica,
                &prepared_batch.shard_id,
                prepared_batch.next_after_sequence.saturating_sub(1),
            );
        }
        DFS_METADATA_AUTHORITY_BATCH_READS.with(|reads| reads.set(0));

        let authorization = service
            .authorize_metadata_shard_operation(&shard, &operation, &prepared_batch)
            .expect("equal replica cursors should reuse the decoded prepared suffix");

        assert!(matches!(
            authorization,
            DfsCanonicalAuthorization::FullShard { .. }
        ));
        assert_eq!(txn_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            DFS_METADATA_AUTHORITY_BATCH_READS.with(std::cell::Cell::get),
            0,
            "the decoded prepared suffix should be reused without rereading the journal"
        );
        let sent = sent.lock().expect("sent messages");
        let commands = sent
            .iter()
            .filter_map(|message| match &message.message {
                DfsNetworkMessage::MetadataJournalBatch(command) => Some(command),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(commands.len(), expected_probe_count);
        assert_eq!(
            commands
                .iter()
                .map(|command| command.request_id.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            expected_probe_count,
            "shared batch construction must retain per-target request identities"
        );
        assert!(
            commands
                .iter()
                .all(|command| command.batch == commands[0].batch),
            "equal cursors should receive the same exact bounded journal batch"
        );
    }

    #[test]
    fn direct_authority_fanout_coalesces_equal_suffixes_into_one_wire_value() {
        let replicas = (0..5)
            .map(|index| node(&format!("node_replayable_authority_replica_{index}")))
            .collect::<Vec<_>>();
        let (mut service, object, _, shard, txn_calls, _) =
            authority_fixture(ProbeBehavior::Accept, replicas.clone());
        service.transport.direct_metadata_journal_batches = true;
        let (operation, prepared_batch) =
            prepared_namespace_authority_fixture(&mut service, &object, &shard, 257);
        for replica in &replicas {
            service.record_metadata_authority_replication_cursor(
                replica,
                &prepared_batch.shard_id,
                prepared_batch.next_after_sequence.saturating_sub(1),
            );
        }

        let authorization = service
            .authorize_metadata_shard_operation(&shard, &operation, &prepared_batch)
            .expect("direct equal-suffix authority");

        assert!(matches!(
            authorization,
            DfsCanonicalAuthorization::FullShard { .. }
        ));
        assert_eq!(txn_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            service.transport.direct_group_target_counts,
            vec![replicas.len()]
        );
        assert_eq!(service.transport.direct_commands.len(), replicas.len());
        assert!(
            service
                .transport
                .direct_commands
                .iter()
                .all(|command| command == &service.transport.direct_commands[0]),
            "equal direct suffixes must share request/probe identity so transport encoding can be replayed"
        );
    }

    #[test]
    fn packed_oversized_namespace_authority_replicates_prepare_before_publication() {
        let replicas = vec![
            node("node_packed_oversized_namespace_authority_replica_a"),
            node("node_packed_oversized_namespace_authority_replica_b"),
        ];
        let expected_probe_count = replicas.len();
        let (mut service, object, _, shard, txn_calls, _) =
            authority_fixture(ProbeBehavior::Accept, replicas);
        let sent = Arc::clone(&service.transport.sent);
        let (operation, mut prepared_batch) =
            prepared_namespace_authority_fixture(&mut service, &object, &shard, 257);
        let entry = prepared_batch
            .entries
            .iter_mut()
            .find(|entry| {
                entry.sequence == metadata_authority_operation_prepare_sequence(&operation)
            })
            .expect("namespace prepare entry");
        let prepare = match &entry.mutation {
            DfsMetadataMutation::PutNamespacePrepare {
                prepare: Some(prepare),
                packed_prepare_v1: None,
                ..
            } => prepare.clone(),
            _ => panic!("fixture should contain one canonical namespace prepare"),
        };
        entry.mutation = DfsMetadataMutation::put_namespace_prepare_packed_v1(&prepare)
            .expect("pack namespace prepare");
        crate::metadata_records::reset_namespace_prepare_payload_validation_passes();
        assert_eq!(
            metadata_authority_probe_timeout(&shard, &operation, &prepared_batch),
            DFS_METADATA_AUTHORITY_LARGE_PROBE_TIMEOUT
        );
        assert_eq!(
            crate::metadata_records::namespace_prepare_payload_validation_passes(),
            0,
            "locally-authored timeout selection must not rescan a packed prepare"
        );

        let authorization = service
            .authorize_metadata_shard_operation(&shard, &operation, &prepared_batch)
            .expect("packed oversized namespace prepare replicates through the DFS data plane");
        assert!(
            matches!(authorization, DfsCanonicalAuthorization::FullShard { .. }),
            "packed encoding must not bypass full-shard DFS durability"
        );
        assert_eq!(txn_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            sent.lock().expect("sent messages").len(),
            expected_probe_count,
            "every configured replica must receive the packed prepared batch"
        );
        drop(
            service
                .validate_canonical_authorization(&authorization, &operation)
                .expect("packed full-shard evidence remains subject to canonical revalidation"),
        );
    }

    #[test]
    fn small_namespace_authority_keeps_full_shard_probe() {
        let replicas = vec![
            node("node_small_namespace_authority_replica_a"),
            node("node_small_namespace_authority_replica_b"),
        ];
        let expected_probe_count = replicas.len();
        let (mut service, object, _, shard, txn_calls, _) =
            authority_fixture(ProbeBehavior::Accept, replicas);
        let sent = Arc::clone(&service.transport.sent);
        let (operation, prepared_batch) =
            persisted_small_namespace_authority_fixture(&mut service, &object, &shard);
        assert_eq!(
            metadata_authority_probe_timeout(&shard, &operation, &prepared_batch),
            DFS_METADATA_AUTHORITY_PROBE_TIMEOUT
        );

        let authorization = service
            .authorize_metadata_shard_operation(&shard, &operation, &prepared_batch)
            .expect("small namespace prepare keeps full-shard authority");
        assert!(
            matches!(authorization, DfsCanonicalAuthorization::FullShard { .. }),
            "small namespace prepares should retain the healthy data-plane path"
        );
        assert_eq!(txn_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            sent.lock().expect("sent messages").len(),
            expected_probe_count
        );
        drop(
            service
                .validate_canonical_authorization(&authorization, &operation)
                .expect("full-shard evidence remains subject to canonical revalidation"),
        );
    }

    #[test]
    fn operation_scoped_shard_authorization_does_not_rescan_all_shard_configs() {
        let replica = node("node_authority_probe_selected_shard_replica");
        let (mut service, object, intent, shard, txn_calls, range_prefix_calls) =
            authority_fixture(ProbeBehavior::Accept, vec![replica]);
        let (operation, prepared_batch) =
            prepared_authority_fixture(&mut service, &object, &intent, &shard);
        range_prefix_calls.store(0, Ordering::SeqCst);

        service
            .authorize_prepared_intent_for_selected_shard(
                &intent,
                &object,
                shard,
                &operation,
                &prepared_batch,
            )
            .expect("fresh operation-scoped shard authority");

        assert_eq!(range_prefix_calls.load(Ordering::SeqCst), 0);
        assert_eq!(txn_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn rejected_or_unroutable_authority_probe_uses_kv_fallback() {
        for behavior in [ProbeBehavior::Reject, ProbeBehavior::Unroutable] {
            let replicas = vec![node("node_authority_probe_fallback_replica")];
            let (mut service, object, intent, shard, txn_calls, _) =
                authority_fixture(behavior, replicas);
            let (operation, prepared_batch) =
                prepared_authority_fixture(&mut service, &object, &intent, &shard);

            let authorization = service
                .authorize_prepared_intent_for_object(&intent, &object, &operation, &prepared_batch)
                .expect("operation-bound cluster quorum fallback")
                .expect("clustered authorization");
            assert!(
                matches!(
                    authorization,
                    DfsCanonicalAuthorization::ClusterQuorum { .. }
                ),
                "a rejected or unroutable replica must not be counted as DFS evidence"
            );

            assert_eq!(txn_calls.load(Ordering::SeqCst), 1);
            assert!(service.pending_metadata_authority_probes.is_empty());
            assert!(service.metadata_authority_probe_results.is_empty());
        }
    }

    #[test]
    fn explicit_lock_authority_path_reads_configuration_without_kv_write() {
        let replicas = vec![node("node_authority_probe_lock_replica")];
        let (mut service, object, _, _, txn_calls, _) =
            authority_fixture(ProbeBehavior::Accept, replicas);

        assert_eq!(
            service
                .local_metadata_owner_epoch_for_object(&object)
                .expect("lock authority epoch"),
            7
        );
        assert_eq!(txn_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn authority_probe_rejects_stale_or_missing_control_plane_view() {
        let replica = node("node_authority_probe_validation_replica");
        let primary = node("node_authority_probe_validation_primary");
        let (mut replica_service, _, _, current_shard, _, _) =
            authority_fixture(ProbeBehavior::Capture, vec![replica.clone()]);
        // Rebuild the fixture at the actual receiver identity with a newer
        // committed epoch than the command carries.
        replica_service.node_id = replica.clone();
        replica_service.transport.local_node_id = replica.clone();
        let mut stale_shard = current_shard.clone();
        stale_shard.primary_node_id = primary.clone();
        stale_shard.replica_node_ids = vec![replica.clone()];
        stale_shard.epoch = current_shard.epoch.saturating_sub(1);
        let command = DfsMetadataJournalBatchCommand {
            request_id: "request-stale-authority-probe".to_string(),
            source_node_id: Some(primary.clone()),
            file_system_id: Some(stale_shard.file_system_id.clone()),
            authority_epoch: Some(stale_shard.epoch),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: Some(DfsMetadataAuthorityProbe {
                probe_id: "probe-stale-authority".to_string(),
                expected_shard: stale_shard.clone(),
                operation: DfsMetadataAuthorityOperation::NamespaceMutation {
                    operation_id: "operation-stale-authority".to_string(),
                    file_system_id: stale_shard.file_system_id.clone(),
                    prepare_sequence: 1,
                    payload_digest: content_hash(b"stale-authority"),
                },
                prepared_prefix: None,
            }),
            batch: DfsMetadataJournalBatch {
                shard_id: DfsMetadataShardId::new(stale_shard.shard_id.clone()),
                entries: Vec::new(),
                next_after_sequence: 0,
            },
        };
        let event = replica_service
            .handle_metadata_journal_batch(primary.clone(), command.clone())
            .expect("stale probe response");
        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::MetadataJournalBatchRejected { .. }
        ));

        replica_service.control_plane = None;
        let event = replica_service
            .handle_metadata_journal_batch(primary, command)
            .expect("missing control-plane probe response");
        assert!(matches!(
            event,
            NetworkedDfsServiceEvent::MetadataJournalBatchRejected { ref reason, .. }
                if reason.contains("requires a committed control-plane")
        ));
    }
}
