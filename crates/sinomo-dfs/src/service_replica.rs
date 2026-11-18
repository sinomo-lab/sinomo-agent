//! Replica command authorization and metadata-authority helpers.
//!
//! This module keeps shard-primary proof, local authority checks, and
//! replica cache updates together so transfer handlers can validate
//! inbound stage, repair, commit, and fetch commands through one service
//! boundary.

use sinomo_core::DfsObjectPolicy;
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use crate::*;

/// A replica whose direct prepare has not answered by this point gets an empty
/// confirmation over the routed path, and again after each further interval.
/// This is only a re-send timer: the direct call keeps running, bounded by the
/// caller's deadline alone.
const DFS_METADATA_AUTHORITY_HEDGE_AFTER: Duration = Duration::from_millis(250);
/// Re-send intervals double up to the network layer's own liveness interval:
/// a re-send more often than the network checks the path adds nothing.
const DFS_METADATA_AUTHORITY_RESEND_MAX: Duration = sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
// Namespace ACKs share one durable exact-frame path regardless of record
// count, which can sit behind an already-running bounded materialization pass.
// An empty confirmation sent after the ordinary 250ms would queue behind the
// same pass, so namespace prepares wait longer before hedging.
const DFS_METADATA_AUTHORITY_NAMESPACE_HEDGE_AFTER: Duration = Duration::from_secs(1);

fn metadata_authority_hedge_after(
    operation: &DfsMetadataAuthorityOperation,
    prepared_batch: &DfsMetadataJournalBatch,
) -> Duration {
    let DfsMetadataAuthorityOperation::PreparedOperation {
        prepare_sequence, ..
    } = operation;
    if prepared_batch.entries.iter().any(|entry| entry.sequence == *prepare_sequence && matches!(
        &entry.mutation, DfsMetadataMutation::PrepareOperationV7 { record, .. } if record.payload.prepared_intent.is_none())) {
        DFS_METADATA_AUTHORITY_NAMESPACE_HEDGE_AFTER
    } else { DFS_METADATA_AUTHORITY_HEDGE_AFTER }
}

/// The next re-send interval after `interval`.
fn metadata_authority_next_resend_interval(interval: Duration) -> Duration {
    interval
        .saturating_mul(2)
        .min(DFS_METADATA_AUTHORITY_RESEND_MAX)
}

/// Whether the caller's deadline, if it set one, has passed.
fn metadata_authority_deadline_passed(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|deadline| Instant::now() >= deadline)
}

/// How one direct prepare reply bears on its target's acknowledgement.
enum MetadataAuthorityDirectReply {
    Acknowledged,
    Rejected,
    Unconfirmed,
}

fn metadata_authority_direct_reply(
    command: &DfsMetadataJournalBatchCommand,
    response: &DfsMetadataJournalBatchResponse,
) -> MetadataAuthorityDirectReply {
    let correlated = response.request_id == command.request_id
        && response.shard_id == command.batch.shard_id
        && response.next_after_sequence == command.batch.next_after_sequence
        && !response.verified_prefix_only;
    let expected_confirmation_id = command
        .authority_probe
        .as_ref()
        .and_then(metadata_authority_probe_confirmation_id);
    if correlated
        && response.result == DfsMetadataJournalBatchResult::Applied
        && response.verified_authority_probe_id.as_deref() == expected_confirmation_id.as_deref()
        && command
            .authority_probe
            .as_ref()
            .is_none_or(|probe| metadata_member_view_matches(response.member_view.as_ref(), probe))
    {
        MetadataAuthorityDirectReply::Acknowledged
    } else if correlated
        && response.verified_authority_probe_id.is_none()
        && matches!(&response.result,
            DfsMetadataJournalBatchResult::Rejected { code, .. }
                if *code != DfsMetadataRejectionCode::PrimaryMismatch)
    {
        MetadataAuthorityDirectReply::Rejected
    } else {
        MetadataAuthorityDirectReply::Unconfirmed
    }
}

/// An attempt a write-set member refused at this epoch ends unknown as a
/// record change: the member promised a takeover candidate, or its own record
/// names a later epoch. The cause names the latest epoch a refusal shows, at
/// least the next one, and the outcome names that record's primary when it
/// is another node. A refusal is a hint: the attempt's own context is kept.
pub(crate) fn metadata_authority_refusal_outcome(
    local_node_id: &NodeId,
    shard: &DfsMetadataShardConfig,
    operation: &DfsMetadataAuthorityOperation,
    refusals: &BTreeMap<NodeId, crate::DfsMetadataMemberRecordView>,
) -> Option<DfsError> {
    let latest = refusals.values().max_by_key(|record| record.epoch)?;
    let DfsMetadataAuthorityOperation::PreparedOperation { operation_id, .. } = operation;
    Some(DfsError::MetadataCommitOutcomeUnknown {
        file_system_id: shard.file_system_id.clone(),
        shard_id: shard.shard_id.clone(),
        epoch: shard.epoch,
        operation_id: operation_id.clone(),
        cause: crate::DfsCommitOutcomeUnknownCause::RecordChanged {
            epoch: latest.epoch.max(shard.epoch.saturating_add(1)),
        },
        current_primary_node_id: (latest.epoch > shard.epoch
            && &latest.primary_node_id != local_node_id)
            .then(|| latest.primary_node_id.clone()),
    })
}

/// The empty confirmation sent as a hedge: the same operation and exact
/// prefix challenge, without journal frames.
fn metadata_authority_hedge_command(
    command: &DfsMetadataJournalBatchCommand,
) -> DfsMetadataJournalBatchCommand {
    DfsMetadataJournalBatchCommand {
        request_id: command.request_id.clone(),
        source_node_id: command.source_node_id.clone(),
        file_system_id: command.file_system_id.clone(),
        authority_epoch: command.authority_epoch,
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: command.authority_probe.clone(),
        durable_replication: None,
        batch: DfsMetadataJournalFrameBatch {
            shard_id: command.batch.shard_id.clone(),
            next_after_sequence: command.batch.next_after_sequence,
            journal_frames: Vec::new(),
        },
    }
}

/// Where the reply to one routed authority request arrives.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetadataAuthorityRoutedDelivery {
    /// As one fan-out event.
    Fanout,
    /// Through the transport's ordinary inbound messages.
    Inbound,
}

#[derive(Debug)]
struct MetadataAuthorityRoutedRequest {
    request_id: String,
    target_node_id: NodeId,
    hedge: Option<MetadataAuthorityHedge>,
    delivery: MetadataAuthorityRoutedDelivery,
}

/// One empty confirmation sent while the target's direct prepare is in flight.
#[derive(Debug)]
struct MetadataAuthorityHedge {
    /// Direct-call request bytes handed to the network when the previous
    /// hedge to this target was sent; `None` for the first hedge.
    previous_upload_bytes: Option<u64>,
    /// Whether its unconfirmed reply has already been judged.
    judged: bool,
}

/// A hedge reply that does not confirm the prepare means the replica does not
/// hold it yet. The direct upload is then stalled when it handed no further
/// request bytes to the network since the previous hedge; a still-advancing
/// upload is left alone so a slow link does not carry the body twice.
fn metadata_authority_upload_stalled(
    previous_upload_bytes: Option<u64>,
    current_upload_bytes: Option<u64>,
) -> bool {
    matches!(
        (previous_upload_bytes, current_upload_bytes),
        (Some(previous), Some(current)) if current <= previous
    )
}

/// Which members one commit on a shard must reach.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsMetadataCommitRequirement {
    /// A shard on the all-member rule: the primary and every configured
    /// replica.
    AllMembers,
    /// A shard on the write-set protocol: the write set at the retained
    /// generation plus the durable pending adds the record has not moved past.
    /// The write set itself must be at least `floor` members.
    WriteSet {
        write_set: crate::DfsMetadataShardWriteSet,
        pending_adds: BTreeSet<NodeId>,
        floor: usize,
        /// The commit's own content W; the record must already accept it.
        intent_w: usize,
    },
}

impl DfsMetadataCommitRequirement {
    pub(crate) fn required_node_ids(&self, shard: &DfsMetadataShardConfig) -> BTreeSet<NodeId> {
        match self {
            Self::AllMembers => std::iter::once(shard.primary_node_id.clone())
                .chain(shard.replica_node_ids.iter().cloned())
                .collect(),
            Self::WriteSet {
                write_set,
                pending_adds,
                ..
            } => write_set
                .write_set
                .iter()
                .chain(pending_adds)
                .cloned()
                .collect(),
        }
    }
}

/// Content W of what one commit makes visible: its terminal intent, or the
/// intent it prepares. Metadata-only publications carry none.
pub(crate) fn prepared_payload_commit_intent_w(
    payload: &crate::DfsPreparedOperationPayload,
) -> usize {
    payload
        .terminal_intent
        .as_ref()
        .or(payload.prepared_intent.as_ref())
        .map_or(0, |intent| intent.policy.min_commit_replicas)
}

/// Whether an authority probe names the assignment a follower serves.
///
/// A follower of a write-set shard compares the filesystem, shard, epoch and
/// primary: configured-member changes at one epoch (recruit and remove) do not
/// change who may commit, and the primary's own fence evaluates the required
/// members against its record. This is the model's `accept` guard. A shard on
/// the all-member rule keeps comparing the whole assignment.
pub(crate) fn metadata_authority_probe_names_follower_assignment(
    expected: &DfsMetadataShardConfig,
    current: &DfsMetadataShardConfig,
    write_set_shard: bool,
) -> bool {
    if !write_set_shard {
        return expected == current;
    }
    expected.file_system_id == current.file_system_id
        && expected.shard_id == current.shard_id
        && expected.epoch == current.epoch
        && expected.primary_node_id == current.primary_node_id
}

/// Whether a member's acknowledgement view agrees with the challenge it
/// answers. A member without a view has not yet observed the switch; its
/// confirmation id alone proves the durable prefix.
fn metadata_member_view_matches(
    view: Option<&crate::DfsMetadataMemberAckView>,
    probe: &DfsMetadataAuthorityProbe,
) -> bool {
    view.is_none_or(|view| {
        view.epoch == probe.expected_shard.epoch && view.durable_prefix == probe.prepared_prefix
    })
}

pub(crate) use crate::publication_guard::ShardFenceGuard;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Serialization is owner/backend scoped and grants no author permission.
    pub(crate) fn metadata_shard_fence_guard(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<ShardFenceGuard, DfsError> {
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "shard fencing requires an actual owned backend".into(),
            })?;
        let scope =
            backend.publication_owner_scope(file_system_id, &DfsMetadataShardId::new(shard_id))?;
        Ok(crate::publication_guard::shard_fence_guard(&scope))
    }

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

    /// Select the retained route. Publication separately requires coherent host
    /// configuration, all assigned participants and the held writer generation.
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

        Ok(Some(shard))
    }

    /// Capture the locally committed shard epoch for a new intent without a
    /// Raft write barrier. This is only an intent hint: every visible commit
    /// must still call `ensure_intent_epoch_current*`, which obtains fresh
    /// operation-bound full-shard authorization at the
    /// metadata publication boundary.
    pub(crate) fn local_metadata_owner_epoch_hint_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<u64, DfsError> {
        Ok(self
            .local_metadata_owner_shard_hint_for_object(object)?
            .map_or(0, |shard| shard.epoch))
    }

    pub(crate) fn local_metadata_owner_shard_hint_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let Some(shard) = self.select_metadata_shard_for_object(object)? else {
            if self.control_plane.is_none() {
                return Ok(None);
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
        Ok(Some(shard))
    }

    /// What a commit of `payload` must reach on the retained record.
    ///
    /// A switched shard needs its write set at the retained generation plus
    /// every durable pending add the record has not moved past, and a write
    /// set of at least max(shard W floor, the payload's intent W). Pending adds
    /// come from the member-state file and the bounds from the retained
    /// projection; nothing here reads KV.
    pub(crate) fn metadata_commit_requirement(
        &self,
        context: &DfsMetadataReplicaShardContext,
        payload: &crate::DfsPreparedOperationPayload,
    ) -> Result<DfsMetadataCommitRequirement, DfsError> {
        let Some(write_set) = context.write_set.clone() else {
            return Ok(DfsMetadataCommitRequirement::AllMembers);
        };
        let bounds = self.retained_metadata_shard_bounds(&context.shard)?;
        let intent_w = prepared_payload_commit_intent_w(payload);
        let floor = crate::metadata_commit_write_set_floor(&bounds, intent_w);
        let shard_id = DfsMetadataShardId::new(context.shard.shard_id.clone());
        let pending_adds = self
            .metadata_backend
            .as_deref()
            .ok_or_else(|| DfsError::Metadata {
                message: "a write-set commit requires its durable member state".into(),
            })?
            .write_set_member_state(&shard_id)?
            .map(|state| {
                state
                    .pending_adds
                    .into_iter()
                    .filter(|add| {
                        !crate::metadata_pending_write_set_add_superseded(
                            &add.cut,
                            &context.shard,
                            Some(&write_set),
                        )
                    })
                    .map(|add| add.node_id)
                    .collect()
            })
            .unwrap_or_default();
        Ok(DfsMetadataCommitRequirement::WriteSet {
            write_set,
            pending_adds,
            floor,
            intent_w,
        })
    }

    /// Authorize one exact canonical mutation against one metadata shard.
    /// Every configured member must acknowledge the same durable preparation.
    /// Missing participants leave the operation unavailable; content receipts
    /// and KV voting majorities grant no alternate publication authority.
    /// The publication guard consumes this proof with the actual retained
    /// configuration and holds the native writer/epoch/freeze through its CAS.
    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.metadata.authority",
        skip_all,
        fields(stage = "dfs.metadata.authority")
    )]
    pub(crate) fn authorize_metadata_shard_operation(
        &mut self,
        shard: &DfsMetadataShardConfig,
        requirement: &DfsMetadataCommitRequirement,
        operation: &DfsMetadataAuthorityOperation,
        prepared_batch: &DfsMetadataJournalBatch,
    ) -> Result<DfsCanonicalAuthorization, DfsError> {
        let trace_started = Instant::now();
        #[cfg(any(test, feature = "test-support"))]
        let _native_context =
            self.native_publication_owner_for_observation()
                .map(|(root, owner)| {
                    crate::native_shard_trace::enter_boundary_context(
                        &root,
                        &owner,
                        &prepared_batch.shard_id,
                    )
                });
        let operation_digest = metadata_authority_operation_digest(operation)?;
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
        let required_node_ids = requirement.required_node_ids(shard);
        // Metadata object RPC encodes every typed value into credit-bounded
        // transport segments. Large prepares therefore use the same stream-
        // native replica path as small ones; semantic record count must never
        // change the required participant set.
        let hedge_after = metadata_authority_hedge_after(operation, prepared_batch);
        let probe_started = Instant::now();
        let acknowledged_node_ids = self.fresh_metadata_shard_authority_acknowledgements(
            shard,
            &required_node_ids,
            operation,
            prepared_batch,
            hedge_after,
        )?;
        let trace_probe_ms = probe_started.elapsed().as_millis();
        #[cfg(any(test, feature = "test-support"))]
        if let Some((root, _)) = self.native_publication_owner_for_observation() {
            if matches!(operation, DfsMetadataAuthorityOperation::PreparedOperation { operation_id, .. }
                if crate::native_boundary_diagnostics::selected_operation(&root, operation_id))
            {
                crate::native_boundary_diagnostics::record(
                    &root,
                    "authorization-participants",
                    || {
                        serde_json::json!({
                            "source": self.node_id, "operation": operation, "expected_shard": shard,
                            "required": required_node_ids, "acknowledged": acknowledged_node_ids,
                            "probe_hedge_after_ms": hedge_after.as_millis(), "probe_elapsed_ms": trace_probe_ms,
                        })
                    },
                    false,
                );
            }
        }
        let trace_acknowledged = acknowledged_node_ids.len();
        dfs_perf_trace(format_args!(
            "metadata_authority action=probe shard={} epoch={} prepare={} acknowledgements={} required={} hedge_after_ms={}",
            shard.shard_id,
            shard.epoch,
            prepared_batch.next_after_sequence,
            acknowledged_node_ids.len(),
            required_node_ids.len(),
            hedge_after.as_millis(),
        ));
        if required_node_ids.is_subset(&acknowledged_node_ids) {
            dfs_perf_trace(format_args!(
                "metadata-authority-confirmation node={} shard={} epoch={} mode=full-shard required={} acknowledged={} probe_ms={} assignment_ms=0 quorum_ms=0 total_ms={}",
                self.node_id,
                shard.shard_id,
                shard.epoch,
                required_node_ids.len(),
                trace_acknowledged,
                trace_probe_ms,
                trace_started.elapsed().as_millis()
            ));
            let DfsMetadataCommitRequirement::WriteSet {
                write_set,
                floor,
                intent_w,
                ..
            } = requirement
            else {
                return Ok(DfsCanonicalAuthorization::FullShard {
                    operation_digest: operation_digest.to_string(),
                    expected_shard: shard.clone(),
                    acknowledged_node_ids: acknowledged_node_ids.into_iter().collect(),
                    prepare_sequence: prepared_batch.next_after_sequence,
                });
            };
            // Content committed at W needs metadata kept at N >= W, which the
            // record must order before the commit: the write stays pending
            // until the keeper raises the shard's N.
            if *intent_w > write_set.accepted_w() {
                let DfsMetadataAuthorityOperation::PreparedOperation { operation_id, .. } =
                    operation;
                crate::write_set_keeping_signals::post_commit_demand(
                    &self.node_id,
                    &shard.file_system_id,
                    &DfsMetadataShardId::new(shard.shard_id.clone()),
                    crate::write_set_keeping_signals::DfsWriteSetCommitDemand::RaiseN(*intent_w),
                );
                return Err(DfsError::MetadataCommitOutcomeUnknown {
                    file_system_id: shard.file_system_id.clone(),
                    shard_id: shard.shard_id.clone(),
                    epoch: shard.epoch,
                    operation_id: operation_id.clone(),
                    cause: crate::DfsCommitOutcomeUnknownCause::ReplicationBelowIntent {
                        accepted: write_set.accepted_w(),
                        intent_w: *intent_w,
                    },
                    current_primary_node_id: None,
                });
            }
            // Below its floor the live write set durably holds the change but
            // cannot commit it: the write stays pending.
            if write_set.write_set.len() < *floor {
                let DfsMetadataAuthorityOperation::PreparedOperation { operation_id, .. } =
                    operation;
                return Err(DfsError::MetadataCommitOutcomeUnknown {
                    file_system_id: shard.file_system_id.clone(),
                    shard_id: shard.shard_id.clone(),
                    epoch: shard.epoch,
                    operation_id: operation_id.clone(),
                    cause: crate::DfsCommitOutcomeUnknownCause::BelowWriteSetFloor {
                        write_set: write_set.write_set.len(),
                        floor: *floor,
                    },
                    current_primary_node_id: None,
                });
            }
            return Ok(DfsCanonicalAuthorization::WriteSet {
                operation_digest: operation_digest.to_string(),
                expected_shard: shard.clone(),
                expected_write_set: write_set.clone(),
                required_node_ids: required_node_ids.into_iter().collect(),
                commit_floor: *floor,
                acknowledged_node_ids: acknowledged_node_ids.into_iter().collect(),
                prepare_sequence: prepared_batch.next_after_sequence,
            });
        }
        // Retain the exact failed proof scope, not just a participant count.
        // This is failure-only opt-in tracing and holds no additional custody.
        dfs_perf_trace(format_args!(
            "metadata-authority-incomplete operation={operation:?} primary={} missing={:?} acknowledged={acknowledged_node_ids:?} probe_ms={trace_probe_ms}",
            self.node_id,
            required_node_ids
                .difference(&acknowledged_node_ids)
                .collect::<Vec<_>>(),
        ));
        let DfsMetadataAuthorityOperation::PreparedOperation { operation_id, .. } = operation;
        // The caller's deadline is the only time that ends this wait.
        let deadline_passed = metadata_authority_deadline_passed(self.transport.operation_deadline());
        if !deadline_passed && matches!(requirement, DfsMetadataCommitRequirement::WriteSet { .. }) {
            // Every attempt to these members ended without an acknowledgement:
            // the shard primary's keeper asks the network to verify them and
            // acts on the settled verdict.
            crate::write_set_keeping_signals::post_commit_demand(
                &self.node_id,
                &shard.file_system_id,
                &DfsMetadataShardId::new(shard.shard_id.clone()),
                crate::write_set_keeping_signals::DfsWriteSetCommitDemand::Unreachable(
                    required_node_ids
                        .difference(&acknowledged_node_ids)
                        .cloned()
                        .collect(),
                ),
            );
        }
        let cause = if deadline_passed {
            crate::DfsCommitOutcomeUnknownCause::Deadline
        } else {
            crate::DfsCommitOutcomeUnknownCause::AcknowledgementsMissing {
                required: required_node_ids.len(),
                acknowledged: acknowledged_node_ids
                    .intersection(&required_node_ids)
                    .count(),
                missing: required_node_ids
                    .difference(&acknowledged_node_ids)
                    .cloned()
                    .collect(),
            }
        };
        Err(DfsError::MetadataCommitOutcomeUnknown {
            file_system_id: shard.file_system_id.clone(),
            shard_id: shard.shard_id.clone(),
            epoch: shard.epoch,
            operation_id: operation_id.clone(),
            cause,
            current_primary_node_id: None,
        })
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.metadata.acknowledgements",
        skip_all,
        fields(stage = "dfs.metadata.acknowledgements")
    )]
    fn fresh_metadata_shard_authority_acknowledgements(
        &mut self,
        shard: &DfsMetadataShardConfig,
        required_node_ids: &BTreeSet<NodeId>,
        operation: &DfsMetadataAuthorityOperation,
        prepared_batch: &DfsMetadataJournalBatch,
        hedge_after: Duration,
    ) -> Result<BTreeSet<NodeId>, DfsError> {
        // Only required members get the prepare. Others receive the journal
        // through ordinary replication.
        let targets = required_node_ids
            .iter()
            .filter(|node_id| *node_id != &self.node_id)
            .cloned()
            .collect::<BTreeSet<_>>();
        if targets.is_empty() {
            return Ok(BTreeSet::from([self.node_id.clone()]));
        }

        // Only the caller's deadline bounds this wait. Without one it ends on
        // complete evidence, cancellation, a network verdict for every attempt
        // to a member, a definite rejection or a change of record.
        let deadline = self.transport.operation_deadline();
        let mut rejected_node_ids = BTreeSet::new();
        let mut lagging_node_ids = BTreeSet::new();
        let mut refusals = BTreeMap::new();
        let mut unaligned = BTreeSet::new();
        let acknowledged = self.try_all_metadata_shard_members_accept_fresh_authority(
            shard,
            operation,
            prepared_batch,
            targets.clone(),
            deadline,
            hedge_after,
            &mut rejected_node_ids,
            &mut lagging_node_ids,
            &mut refusals,
            &mut unaligned,
        )?;
        self.check_cancelled("metadata authority result")?;
        // A member that refuses this epoch, or that must first be aligned with
        // the epoch start, is not caught up: no catch-up can make it
        // acknowledge the epoch.
        self.end_metadata_authority_attempt_on_refusal(shard, operation, &refusals)?;
        self.post_metadata_authority_alignment_demand(shard, &unaligned);
        if acknowledged.len() == targets.len() + 1
            || (rejected_node_ids.is_empty() && lagging_node_ids.is_empty())
        {
            return Ok(acknowledged);
        }
        // A rejecting member, or one farther behind than one bounded batch, is
        // caught up instead of being skipped, then every member is challenged
        // again.
        // An authority rejection grants no repair permission. Use the
        // existing ordinary durability owner to send its actual journal
        // prefix. Only that path's own typed rejection/source-history
        // checks can select its existing snapshot repair.
        let prefix = self
            .metadata_backend
            .as_deref()
            .ok_or_else(|| DfsError::Metadata {
                message: "metadata authority catch-up has no durable backend".into(),
            })?
            .metadata_prefix_proof(&prepared_batch.shard_id)?
            .filter(|prefix| {
                prefix.is_well_formed()
                    && prefix.journal_sequence >= prepared_batch.next_after_sequence
            })
            .ok_or_else(|| DfsError::Metadata {
                message: "metadata authority catch-up has no current prepared prefix".into(),
            })?;
        // Each member gets its own one-replica target, so the catch-up waits
        // for that member rather than for a majority of the targets.
        let behind = rejected_node_ids
            .union(&lagging_node_ids)
            .filter(|node_id| !acknowledged.contains(*node_id) && !unaligned.contains(*node_id))
            .cloned()
            .collect::<Vec<_>>();
        if behind.is_empty() && !unaligned.is_empty() {
            return Ok(acknowledged);
        }
        for member in behind {
            if !self.catch_up_metadata_authority_member(
                shard,
                prepared_batch,
                &prefix,
                &member,
                deadline,
            )? {
                // Round one's evidence stands; it is never discarded. Ordinary
                // replication, woken here, continues the catch-up and a retry
                // with the same operation id commits.
                self.record_message_committed_work(DfsMessageCommittedWork {
                    publish_metadata: true,
                    ..Default::default()
                });
                return Ok(acknowledged);
            }
        }
        self.check_cancelled("metadata authority after catch-up")?;
        let context = self
            .retained_metadata_replica_context(&shard.file_system_id, &prepared_batch.shard_id)?
            .ok_or_else(|| DfsError::Metadata {
                message: "metadata authority catch-up lost its retained shard context".into(),
            })?;
        if context.shard != *shard || context.pending_membership_handoff.is_some() {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: shard.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: context.shard.primary_node_id,
                epoch: context.shard.epoch,
            });
        }

        // Discard every old ACK. The same immutable preparation is checked
        // again against the current exact prefix with new nonces on ALL
        // assigned replicas. No further catch-up round is started here.
        let mut rejected_again = BTreeSet::new();
        let mut lagging_again = BTreeSet::new();
        let mut refusals = BTreeMap::new();
        let mut unaligned = BTreeSet::new();
        let acknowledged = self.try_all_metadata_shard_members_accept_fresh_authority(
            shard,
            operation,
            prepared_batch,
            targets,
            deadline,
            hedge_after,
            &mut rejected_again,
            &mut lagging_again,
            &mut refusals,
            &mut unaligned,
        )?;
        self.check_cancelled("metadata authority refreshed result")?;
        self.end_metadata_authority_attempt_on_refusal(shard, operation, &refusals)?;
        self.post_metadata_authority_alignment_demand(shard, &unaligned);
        Ok(acknowledged)
    }

    /// Members that must be aligned with the epoch start count as missing
    /// acknowledgements; the keeper's primary role aligns them.
    fn post_metadata_authority_alignment_demand(
        &self,
        shard: &DfsMetadataShardConfig,
        unaligned: &BTreeSet<NodeId>,
    ) {
        if unaligned.is_empty() {
            return;
        }
        crate::write_set_keeping_signals::post_commit_demand(
            &self.node_id,
            &shard.file_system_id,
            &DfsMetadataShardId::new(shard.shard_id.clone()),
            crate::write_set_keeping_signals::DfsWriteSetCommitDemand::AlignmentRequired(
                unaligned.clone(),
            ),
        );
    }

    /// End an attempt that a write-set member refused at this epoch. A member
    /// still at this epoch promised a takeover candidate: it is posted to the
    /// keeper, which may shrink it rather than let it stall every write.
    fn end_metadata_authority_attempt_on_refusal(
        &self,
        shard: &DfsMetadataShardConfig,
        operation: &DfsMetadataAuthorityOperation,
        refusals: &BTreeMap<NodeId, crate::DfsMetadataMemberRecordView>,
    ) -> Result<(), DfsError> {
        let fenced = refusals
            .iter()
            .filter(|(_, record)| record.epoch <= shard.epoch)
            .map(|(member, _)| member.clone())
            .collect::<BTreeSet<_>>();
        if !fenced.is_empty() {
            crate::write_set_keeping_signals::post_commit_demand(
                &self.node_id,
                &shard.file_system_id,
                &DfsMetadataShardId::new(shard.shard_id.clone()),
                crate::write_set_keeping_signals::DfsWriteSetCommitDemand::Fenced(fenced),
            );
        }
        match metadata_authority_refusal_outcome(&self.node_id, shard, operation, refusals) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Catch one member up to `prefix` through the existing durability path
    /// and report whether it got there.
    ///
    /// A caller deadline bounds the catch-up. Without one it runs in slices of
    /// the network layer's liveness interval and continues for as long as the
    /// member's acknowledged cursor advances, so elapsed time alone never ends
    /// a catch-up that is making progress.
    fn catch_up_metadata_authority_member(
        &mut self,
        shard: &DfsMetadataShardConfig,
        prepared_batch: &DfsMetadataJournalBatch,
        prefix: &crate::DfsMetadataPrefixProof,
        member: &NodeId,
        deadline: Option<Instant>,
    ) -> Result<bool, DfsError> {
        self.catch_up_metadata_shard_member(
            shard,
            &prepared_batch.shard_id,
            prefix,
            member,
            deadline,
            false,
        )
    }

    /// Catch one member of `shard` up to `prefix` through the existing
    /// durability barrier and report whether it got there.
    ///
    /// A caller deadline bounds the catch-up. Without one it runs in slices of
    /// the network layer's liveness interval and continues for as long as the
    /// member's acknowledged cursor advances, so elapsed time alone never ends
    /// a catch-up that is making progress.
    ///
    /// With `only_member` on a write-set shard, the barrier names only
    /// `member`, so it certifies that member alone rather than a majority of
    /// every configured member as well.
    pub(crate) fn catch_up_metadata_shard_member(
        &mut self,
        shard: &DfsMetadataShardConfig,
        shard_id: &DfsMetadataShardId,
        prefix: &crate::DfsMetadataPrefixProof,
        member: &NodeId,
        deadline: Option<Instant>,
        only_member: bool,
    ) -> Result<bool, DfsError> {
        loop {
            let slice = match deadline {
                Some(deadline) => match deadline
                    .checked_duration_since(Instant::now())
                    .filter(|remaining| !remaining.is_zero())
                {
                    Some(remaining) => remaining,
                    None => return Ok(false),
                },
                None => DFS_METADATA_AUTHORITY_RESEND_MAX,
            };
            let before = self.metadata_replicated_through_sequence_for_target(member, shard_id)?;
            // The barrier names the shard's whole replica set, as its
            // authority check requires, and must include this member.
            let mut replica_node_ids = if only_member {
                vec![member.clone()]
            } else {
                shard
                    .replica_node_ids
                    .iter()
                    .filter(|node_id| **node_id != shard.primary_node_id)
                    .cloned()
                    .collect::<Vec<_>>()
            };
            replica_node_ids.sort();
            replica_node_ids.dedup();
            let target = DfsMetadataQuorumSyncTarget {
                file_system_id: shard.file_system_id.clone(),
                shard_id: shard_id.clone(),
                primary_node_id: self.node_id.clone(),
                authority_epoch: Some(shard.epoch),
                replica_node_ids,
                expected_shard: Some(shard.clone()),
                journal_sequence: prefix.journal_sequence,
                barrier_id: sinomo_core::Id::generate().to_string(),
                state_fingerprint: prefix.digest.clone(),
                checkpoint_snapshot: None,
            };
            self.check_cancelled("metadata authority catch-up")?;
            // This invocation already validates the retained assignment, native
            // owner/freeze and response identities, and restores/cleans up only
            // its own pending requests. Its certificate is not write authority.
            match self.sync_metadata_shard_to_quorum_intersecting(
                &target,
                std::slice::from_ref(member),
                slice,
            ) {
                Ok(_) => return Ok(true),
                Err(DfsError::Network { .. } | DfsError::NetworkDelivery { .. }) => {
                    let after = self.metadata_replicated_through_sequence_for_target(member, shard_id)?;
                    if deadline.is_none() && after > before {
                        continue;
                    }
                    return Ok(false);
                }
                Err(error) => return Err(error),
            }
        }
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.metadata.probe",
        skip_all,
        fields(stage = "dfs.metadata.probe")
    )]
    fn try_all_metadata_shard_members_accept_fresh_authority(
        &mut self,
        shard: &DfsMetadataShardConfig,
        operation: &DfsMetadataAuthorityOperation,
        prepared_batch: &DfsMetadataJournalBatch,
        targets: BTreeSet<NodeId>,
        operation_deadline: Option<Instant>,
        hedge_after: Duration,
        rejected_node_ids: &mut BTreeSet<NodeId>,
        lagging_node_ids: &mut BTreeSet<NodeId>,
        refusals: &mut BTreeMap<NodeId, crate::DfsMetadataMemberRecordView>,
        unaligned: &mut BTreeSet<NodeId>,
    ) -> Result<BTreeSet<NodeId>, DfsError> {
        #[cfg(any(test, feature = "test-support"))]
        let diagnostic_root = self.native_publication_owner_for_observation().map(|(root, _)| root)
            .filter(|root| crate::native_boundary_diagnostics::enabled(root)
                && matches!(operation, DfsMetadataAuthorityOperation::PreparedOperation { operation_id, .. }
                    if crate::native_boundary_diagnostics::selected_operation(root, operation_id)));
        #[cfg(any(test, feature = "test-support"))]
        let mut diagnostic_groups = Vec::new();
        #[cfg(any(test, feature = "test-support"))]
        let mut diagnostic_selections = Vec::new();
        #[cfg(any(test, feature = "test-support"))]
        let mut diagnostic_unavailable = Vec::new();
        #[cfg(any(test, feature = "test-support"))]
        let mut diagnostic_direct = Vec::new();
        #[cfg(any(test, feature = "test-support"))]
        let mut diagnostic_routed = Vec::new();
        let mut request_ids = Vec::with_capacity(targets.len());
        let mut routed_requests =
            Vec::<MetadataAuthorityRoutedRequest>::with_capacity(targets.len());
        let mut acknowledged_node_ids = BTreeSet::from([self.node_id.clone()]);
        let required_acknowledgements = targets.len() + 1;
        if metadata_authority_deadline_passed(operation_deadline) {
            return Ok(acknowledged_node_ids);
        }
        verify_persisted_metadata_authority_operation(
            self.metadata_backend
                .as_deref()
                .ok_or_else(|| DfsError::Metadata {
                    message: "metadata authority probe has no durable metadata backend".into(),
                })?,
            shard,
            operation,
        )?;
        let prepared_prefix = self
            .metadata_backend
            .as_deref()
            .ok_or_else(|| DfsError::Metadata {
                message: "metadata authority probe has no durable metadata backend".to_string(),
            })?
            .metadata_prefix_proof(&prepared_batch.shard_id)?;
        if metadata_authority_deadline_passed(operation_deadline) {
            return Ok(acknowledged_node_ids);
        }
        let Some(prepared_prefix) = prepared_prefix.filter(|prefix| {
            prefix.is_well_formed() && prefix.journal_sequence >= prepared_batch.next_after_sequence
        }) else {
            // The fast path is optional. A missing or moving exact prefix
            // cannot authorize publication.
            return Ok(acknowledged_node_ids);
        };
        let result = (|| {
            let mut targets_by_replicated_through_sequence = BTreeMap::<u64, Vec<NodeId>>::new();
            for target_node_id in targets {
                if metadata_authority_deadline_passed(operation_deadline) {
                    return Ok(acknowledged_node_ids);
                }
                let replicated_through_sequence = self
                    .metadata_authority_replicated_through_sequence_for_target(
                        &target_node_id,
                        prepared_batch,
                    )?;
                if metadata_authority_deadline_passed(operation_deadline) {
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
                let target_batch = self.metadata_authority_batch_since(
                    replicated_through_sequence,
                    prepared_batch,
                    prepared_prefix.journal_sequence,
                )?;
                if metadata_authority_deadline_passed(operation_deadline) {
                    return Ok(acknowledged_node_ids);
                }
                let Some(target_batch) = target_batch else {
                    #[cfg(any(test, feature = "test-support"))]
                    if diagnostic_root.is_some() {
                        diagnostic_unavailable
                            .push((target_node_ids.clone(), replicated_through_sequence));
                    }
                    // A member farther behind than one bounded catch-up batch
                    // cannot acknowledge this round; the caller catches it up
                    // within its deadline before the next round.
                    lagging_node_ids.extend(target_node_ids.iter().cloned());
                    continue;
                };
                #[cfg(any(test, feature = "test-support"))]
                if diagnostic_root.is_some() {
                    diagnostic_selections.push(serde_json::json!({
                        "target_node_ids": target_node_ids,
                        "replicated_through_sequence": replicated_through_sequence,
                        "captured_current_cut_sequence": prepared_prefix.journal_sequence,
                        "actual_first_selected_sequence": target_batch.entries.first().map(|entry| entry.sequence),
                        "actual_last_selected_sequence": target_batch.entries.last().map(|entry| entry.sequence),
                        "actual_selected_entry_count": target_batch.entries.len(),
                    }));
                }
                let journal_frames = self
                    .metadata_backend
                    .as_ref()
                    .ok_or_else(|| DfsError::Metadata {
                        message: "metadata frame selection requires current durable storage".into(),
                    })?
                    .journal_frames_for_batch(&target_batch)?;
                if journal_frames.len() != target_batch.entries.len() {
                    return Err(DfsError::Metadata {
                        message: "current authority frame selection omitted journal entries".into(),
                    });
                }
                let wire_batch = DfsMetadataJournalFrameBatch {
                    shard_id: target_batch.shard_id,
                    next_after_sequence: target_batch.next_after_sequence,
                    journal_frames,
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
                        prepared_prefix: prepared_prefix.clone(),
                    }),
                    // Include every entry the target has not yet acknowledged,
                    // through the complete captured current cut. The receiver
                    // refuses gaps and separately verifies the named original
                    // preparation's full body, slot, epoch and live status.
                    durable_replication: None,
                    batch: wire_batch,
                };
                authority_groups.push(DfsMetadataJournalBatchDirectGroup::new(
                    target_node_ids,
                    command,
                ));
            }
            #[cfg(any(test, feature = "test-support"))]
            if diagnostic_root.is_some() {
                diagnostic_groups = authority_groups
                    .iter()
                    .map(|group| {
                        (
                            group.target_node_ids.clone(),
                            group.command.request_id.clone(),
                            group.command.authority_probe.clone(),
                            group.command.batch.next_after_sequence,
                            group.command.batch.journal_frames.len(),
                        )
                    })
                    .collect::<Vec<_>>();
            }
            // Every target's prepare starts at once on the canonical metadata
            // route, each group encoded once. A call's network deadline is the
            // operation deadline: a prepare over a poor mobile link commonly
            // needs a few hundred milliseconds, so `hedge_after` only decides
            // when a still-silent target also gets an empty routed confirmation.
            let mut fanout = self
                .transport
                .start_metadata_authority_fanout(&authority_groups, operation_deadline)
                // The direct route is an accelerator. Without it every target
                // receives its prepare over the ordinary routed path below.
                .unwrap_or(None);
            // Hedges and routed re-sends back off to the network's own
            // liveness interval; neither ends a call.
            let mut hedge_interval = hedge_after;
            let mut next_hedge_at = Instant::now() + hedge_interval;
            let mut routed_resends = BTreeMap::<NodeId, (Instant, Duration)>::new();
            let mut delivery_failed = BTreeSet::<NodeId>::new();
            let mut hedge_upload_bytes = BTreeMap::<NodeId, u64>::new();
            // Cancellation wakes whichever wait the loop below is in. Register
            // before its first cancellation check so no cancellation is missed.
            let fanout_waker = fanout.as_ref().map(DfsMetadataAuthorityFanout::waker);
            let incoming_waker = self.transport.incoming_wait_waker();
            let cancellation_waker: crate::service_state::DfsCancellationWaker =
                std::sync::Arc::new(move || {
                    if let Some(waker) = &fanout_waker {
                        waker();
                    }
                    if let Some(waker) = &incoming_waker {
                        waker();
                    }
                });
            let _cancellation_wake = self.register_cancellation_waker(&cancellation_waker);
            let mut direct_in_flight = BTreeSet::new();
            let mut needs_routed_prepare = Vec::new();
            for target_node_id in authority_groups
                .iter()
                .flat_map(|group| &group.target_node_ids)
            {
                if fanout
                    .as_ref()
                    .is_some_and(|fanout| fanout.has_direct_call(target_node_id))
                {
                    direct_in_flight.insert(target_node_id.clone());
                } else {
                    needs_routed_prepare.push(target_node_id.clone());
                }
            }
            let mut routed_prepares = BTreeSet::new();
            let mut fanout_routed_in_flight = BTreeMap::<NodeId, usize>::new();
            let mut routed_acknowledged = Vec::new();
            loop {
                self.check_cancelled("metadata authority acknowledgement")?;
                // A target without a usable direct reply receives its prepare
                // over the routed path as soon as that is known.
                for target_node_id in std::mem::take(&mut needs_routed_prepare) {
                    if acknowledged_node_ids.contains(&target_node_id)
                        || rejected_node_ids.contains(&target_node_id)
                        || !routed_prepares.insert(target_node_id.clone())
                    {
                        continue;
                    }
                    if metadata_authority_deadline_passed(operation_deadline) {
                        return Ok(acknowledged_node_ids);
                    }
                    let group = authority_groups
                        .iter()
                        .find(|group| group.target_node_ids.contains(&target_node_id))
                        .expect("every authority target belongs to one group");
                    // Journal frames are shared `Bytes`: this per-target command
                    // copies only identities, not the prepared body.
                    let request = self.send_metadata_authority_request_routed(
                        fanout.as_mut(),
                        &target_node_id,
                        group.command.clone(),
                        shard,
                        None,
                        &mut request_ids,
                    )?;
                    #[cfg(any(test, feature = "test-support"))]
                    if diagnostic_root.is_some() {
                        diagnostic_routed.push(format!("prepare {target_node_id}: {request:?}"));
                    }
                    if let Some(request) = request {
                        if request.delivery == MetadataAuthorityRoutedDelivery::Fanout {
                            *fanout_routed_in_flight
                                .entry(target_node_id.clone())
                                .or_default() += 1;
                        }
                        routed_requests.push(request);
                    }
                }
                // A slow direct call keeps running. Once per hedge interval its
                // target also gets an empty confirmation, which succeeds once
                // that replica holds the prepared prefix; whichever exact ACK
                // arrives first counts.
                let hedge_targets = direct_in_flight
                    .iter()
                    .filter(|target_node_id| {
                        !acknowledged_node_ids.contains(*target_node_id)
                            && !rejected_node_ids.contains(*target_node_id)
                            && !routed_prepares.contains(*target_node_id)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if !hedge_targets.is_empty() && Instant::now() >= next_hedge_at {
                    hedge_interval = metadata_authority_next_resend_interval(hedge_interval);
                    next_hedge_at = Instant::now() + hedge_interval;
                    for target_node_id in hedge_targets {
                        if metadata_authority_deadline_passed(operation_deadline) {
                            return Ok(acknowledged_node_ids);
                        }
                        let group = authority_groups
                            .iter()
                            .find(|group| group.target_node_ids.contains(&target_node_id))
                            .expect("every authority target belongs to one group");
                        let upload_bytes = fanout
                            .as_ref()
                            .and_then(|fanout| fanout.direct_upload_bytes(&target_node_id))
                            .unwrap_or_default();
                        let hedge = MetadataAuthorityHedge {
                            previous_upload_bytes: hedge_upload_bytes
                                .insert(target_node_id.clone(), upload_bytes),
                            judged: false,
                        };
                        let request = self.send_metadata_authority_request_routed(
                            fanout.as_mut(),
                            &target_node_id,
                            metadata_authority_hedge_command(&group.command),
                            shard,
                            Some(hedge),
                            &mut request_ids,
                        )?;
                        #[cfg(any(test, feature = "test-support"))]
                        if diagnostic_root.is_some() {
                            diagnostic_routed.push(format!("hedge {target_node_id}: {request:?}"));
                        }
                        if let Some(request) = request {
                            if request.delivery == MetadataAuthorityRoutedDelivery::Fanout {
                                *fanout_routed_in_flight
                                    .entry(target_node_id.clone())
                                    .or_default() += 1;
                            }
                            routed_requests.push(request);
                        }
                    }
                }
                // Routed replies arrive as ordinary inbound messages. Direct
                // replies do not, so a round answered only on the direct route
                // processes no unrelated inbound work.
                let events = if routed_requests.is_empty() {
                    Vec::new()
                } else {
                    self.process_incoming()?
                };
                for event in events {
                    // A hedge rejection only shows that the replica has not yet
                    // applied the prepare still in flight on its direct call.
                    if let NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                        request_id,
                        source_node_id,
                        shard_id,
                        ..
                    } = event
                        && shard_id == prepared_batch.shard_id
                        && routed_requests.iter().any(|request| {
                            request.hedge.is_none()
                                && request.request_id == request_id
                                && request.target_node_id == source_node_id
                        })
                    {
                        rejected_node_ids.insert(source_node_id);
                    }
                }
                let mut inbound_pending = BTreeSet::new();
                for request in &mut routed_requests {
                    match self
                        .replication
                        .metadata_authority_probe_results
                        .get(&request.request_id)
                    {
                        Some(true) => {
                            if acknowledged_node_ids.insert(request.target_node_id.clone()) {
                                routed_acknowledged.push(request.target_node_id.clone());
                            }
                        }
                        Some(false) => {
                            let target_node_id = &request.target_node_id;
                            // A refused epoch is definite even for an empty
                            // confirmation: no re-send can be acknowledged.
                            match self
                                .replication
                                .metadata_authority_rejections
                                .get(&request.request_id)
                            {
                                Some((code, Some(record))) if code.refuses_source_epoch() => {
                                    refusals.insert(target_node_id.clone(), record.clone());
                                    rejected_node_ids.insert(target_node_id.clone());
                                }
                                Some((DfsMetadataRejectionCode::EpochAlignmentRequired, _)) => {
                                    unaligned.insert(target_node_id.clone());
                                    rejected_node_ids.insert(target_node_id.clone());
                                }
                                _ => {}
                            }
                            if let Some(hedge) =
                                request.hedge.as_mut().filter(|hedge| !hedge.judged)
                            {
                                hedge.judged = true;
                                // Send the body routed once; the direct call may
                                // keep running and whichever ACK arrives first counts.
                                if direct_in_flight.contains(target_node_id)
                                    && !acknowledged_node_ids.contains(target_node_id)
                                    && !routed_prepares.contains(target_node_id)
                                    && metadata_authority_upload_stalled(
                                        hedge.previous_upload_bytes,
                                        fanout.as_ref().and_then(|fanout| {
                                            fanout.direct_upload_bytes(target_node_id)
                                        }),
                                    )
                                {
                                    needs_routed_prepare.push(target_node_id.clone());
                                } else if !direct_in_flight.contains(target_node_id)
                                    && routed_prepares.contains(target_node_id)
                                    && !acknowledged_node_ids.contains(target_node_id)
                                {
                                    // Its routed body was delivered, yet the
                                    // member answers that it does not hold it:
                                    // no attempt is left for this round.
                                    delivery_failed.insert(target_node_id.clone());
                                }
                            }
                        }
                        None if request.delivery == MetadataAuthorityRoutedDelivery::Inbound => {
                            inbound_pending.insert(request.target_node_id.clone());
                        }
                        None => {}
                    }
                }
                if acknowledged_node_ids.len() == required_acknowledgements {
                    let cursors = routed_acknowledged
                        .iter()
                        .map(|node| (node.clone(), prepared_prefix.journal_sequence))
                        .collect::<Vec<_>>();
                    self.record_metadata_authority_replication_cursors(
                        &prepared_batch.shard_id,
                        &cursors,
                    );
                    return Ok(acknowledged_node_ids);
                }
                // A target reached only over the routed path has no call whose
                // completion reports a lost reply. Re-send its empty
                // confirmation on a backing-off timer: each re-send returns the
                // acknowledgement or a fresh delivery verdict, so the wait ends
                // on a network verdict rather than on a DFS deadline.
                let now = Instant::now();
                for target_node_id in &inbound_pending {
                    if acknowledged_node_ids.contains(target_node_id)
                        || rejected_node_ids.contains(target_node_id)
                        || direct_in_flight.contains(target_node_id)
                        || delivery_failed.contains(target_node_id)
                    {
                        continue;
                    }
                    let (resend_at, interval) = routed_resends
                        .entry(target_node_id.clone())
                        .or_insert((now + hedge_after, hedge_after));
                    if now < *resend_at {
                        continue;
                    }
                    *interval = metadata_authority_next_resend_interval(*interval);
                    *resend_at = now + *interval;
                    let group = authority_groups
                        .iter()
                        .find(|group| group.target_node_ids.contains(target_node_id))
                        .expect("every authority target belongs to one group");
                    let request = self.send_metadata_authority_request_routed(
                        fanout.as_mut(),
                        target_node_id,
                        metadata_authority_hedge_command(&group.command),
                        shard,
                        Some(MetadataAuthorityHedge {
                            previous_upload_bytes: None,
                            judged: false,
                        }),
                        &mut request_ids,
                    )?;
                    match request {
                        Some(request) => {
                            if request.delivery == MetadataAuthorityRoutedDelivery::Fanout {
                                *fanout_routed_in_flight
                                    .entry(target_node_id.clone())
                                    .or_default() += 1;
                            }
                            routed_requests.push(request);
                        }
                        // The network has no path to this member now.
                        None => {
                            delivery_failed.insert(target_node_id.clone());
                        }
                    }
                }
                let unresolved = authority_groups
                    .iter()
                    .flat_map(|group| &group.target_node_ids)
                    .any(|target_node_id| {
                        !acknowledged_node_ids.contains(target_node_id)
                            && !rejected_node_ids.contains(target_node_id)
                            && (direct_in_flight.contains(target_node_id)
                                || fanout_routed_in_flight
                                    .get(target_node_id)
                                    .is_some_and(|count| *count > 0)
                                || (inbound_pending.contains(target_node_id)
                                    && !delivery_failed.contains(target_node_id)))
                    });
                if !unresolved {
                    return Ok(acknowledged_node_ids);
                }
                if metadata_authority_deadline_passed(operation_deadline) {
                    return Ok(acknowledged_node_ids);
                }
                // A stalled upload found from a hedge reply sends its body now.
                if !needs_routed_prepare.is_empty() {
                    continue;
                }
                // Hedges and routed re-sends are the only timers; replies,
                // completions and cancellation wake the wait below.
                let mut wake_at = operation_deadline;
                let mut earliest = |at: Instant| {
                    wake_at = Some(wake_at.map_or(at, |current| current.min(at)));
                };
                if direct_in_flight.iter().any(|target_node_id| {
                    !acknowledged_node_ids.contains(target_node_id)
                        && !rejected_node_ids.contains(target_node_id)
                        && !routed_prepares.contains(target_node_id)
                }) {
                    earliest(next_hedge_at);
                }
                for (target_node_id, (resend_at, _)) in &routed_resends {
                    if inbound_pending.contains(target_node_id)
                        && !acknowledged_node_ids.contains(target_node_id)
                        && !delivery_failed.contains(target_node_id)
                    {
                        earliest(*resend_at);
                    }
                }
                let Some(fanout) = fanout.as_mut().filter(|fanout| fanout.in_flight() > 0) else {
                    // Only ordinary routed replies remain outstanding; their
                    // re-send timer always gives the wait an end.
                    let wait = wake_at.map_or(DFS_METADATA_AUTHORITY_RESEND_MAX, |wake_at| {
                        wake_at.saturating_duration_since(Instant::now())
                    });
                    self.transport
                        .wait_for_incoming(wait)
                        .map_err(DfsError::from_network)?;
                    continue;
                };
                match fanout.next_event(wake_at) {
                    None => {}
                    Some(DfsMetadataAuthorityFanoutEvent::Direct {
                        target_node_id,
                        result,
                    }) => {
                        direct_in_flight.remove(&target_node_id);
                        #[cfg(any(test, feature = "test-support"))]
                        if diagnostic_root.is_some() {
                            diagnostic_direct.push(format!("{target_node_id}: {result:?}"));
                        }
                        // After the first exact ACK, later replies for the same
                        // target change nothing.
                        if acknowledged_node_ids.contains(&target_node_id) {
                            continue;
                        }
                        let group = authority_groups
                            .iter()
                            .find(|group| group.target_node_ids.contains(&target_node_id))
                            .expect("every authority target belongs to one group");
                        let reply = result.map(|response| {
                            let reply = metadata_authority_direct_reply(&group.command, &response);
                            (reply, response)
                        });
                        match reply {
                            Ok((MetadataAuthorityDirectReply::Acknowledged, response)) => {
                                crate::write_set_member_evidence::record_metadata_member_view(
                                    &self.node_id,
                                    &prepared_batch.shard_id,
                                    &target_node_id,
                                    response.member_view.as_ref(),
                                );
                                #[cfg(any(test, feature = "test-support"))]
                                if let Some(mut trace) =
                                    crate::native_shard_trace::current_boundary_interest()
                                {
                                    trace.observe(
                                        crate::native_shard_trace::NativeBoundaryFactsV1::SourceJournalResponseAccepted {
                                            authenticated_source_node_id: target_node_id.clone(),
                                            response,
                                            expected_target_node_id: target_node_id.clone(),
                                            expected_next_after_sequence: group.command.batch.next_after_sequence,
                                            expected_prefix_only: false,
                                            expected_confirmation_id: group
                                                .command
                                                .authority_probe
                                                .as_ref()
                                                .and_then(metadata_authority_probe_confirmation_id),
                                        }, None, None,
                                    );
                                }
                                self.record_metadata_authority_replication_cursors(
                                    &prepared_batch.shard_id,
                                    &[(
                                        target_node_id.clone(),
                                        group.command.batch.next_after_sequence,
                                    )],
                                );
                                acknowledged_node_ids.insert(target_node_id);
                            }
                            Ok((MetadataAuthorityDirectReply::Rejected, response)) => {
                                if let Some(record) = response.refusing_member_record() {
                                    refusals.insert(target_node_id.clone(), record.clone());
                                }
                                if response.result.rejection_code()
                                    == Some(DfsMetadataRejectionCode::EpochAlignmentRequired)
                                {
                                    unaligned.insert(target_node_id.clone());
                                }
                                rejected_node_ids.insert(target_node_id);
                            }
                            // An unconfirmed reply or a failed direct call
                            // gets the prepare over the routed path.
                            Ok((MetadataAuthorityDirectReply::Unconfirmed, _)) | Err(_) => {
                                needs_routed_prepare.push(target_node_id);
                            }
                        }
                    }
                    Some(DfsMetadataAuthorityFanoutEvent::Routed {
                        target_node_id,
                        result,
                    }) => {
                        if let Some(count) = fanout_routed_in_flight.get_mut(&target_node_id) {
                            *count = count.saturating_sub(1);
                        }
                        // A routed delivery failure leaves that request
                        // unacknowledged, exactly like an unroutable send.
                        match result.and_then(|messages| {
                            self.transport
                                .deliver_metadata_authority_fanout_replies(messages)
                        }) {
                            Ok(()) => {}
                            Err(error) => dfs_perf_trace(format_args!(
                                "metadata_authority action=routed-unacknowledged target={target_node_id} error={error}"
                            )),
                        }
                    }
                }
            }
        })();
        #[cfg(any(test, feature = "test-support"))]
        if let Some(root) = &diagnostic_root {
            crate::native_boundary_diagnostics::record(
                root,
                "participant-final-result",
                || {
                    serde_json::json!({
                        "operation": operation, "result": format!("{result:?}"),
                        "original_direct_groups": diagnostic_groups,
                        "actual_batch_selections": diagnostic_selections,
                        "unavailable_batches": diagnostic_unavailable,
                        "actual_direct_result": format!("{diagnostic_direct:?}"),
                        "actual_routed_sends": format!("{diagnostic_routed:?}"),
                        "requests": routed_requests.iter().map(|request| serde_json::json!({
                            "request_id": request.request_id, "target": request.target_node_id, "hedge": request.hedge.is_some(),
                            "accepted": self.replication.metadata_authority_probe_results.get(&request.request_id),
                        })).collect::<Vec<_>>(),
                        "remaining_operation_ms": operation_deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()).as_millis()),
                    })
                },
                false,
            );
        }
        self.clear_metadata_authority_probe_requests(&request_ids);
        result
    }

    /// Send one authority prepare, or its empty hedge, over the routed path
    /// with a fresh request and probe identity. Returns `None` when the target
    /// is not routable now; that target stays unacknowledged.
    fn send_metadata_authority_request_routed(
        &mut self,
        fanout: Option<&mut DfsMetadataAuthorityFanout>,
        target_node_id: &NodeId,
        mut command: DfsMetadataJournalBatchCommand,
        shard: &DfsMetadataShardConfig,
        hedge: Option<MetadataAuthorityHedge>,
        request_ids: &mut Vec<String>,
    ) -> Result<Option<MetadataAuthorityRoutedRequest>, DfsError> {
        command.request_id = self.next_request_id();
        let probe = command
            .authority_probe
            .as_mut()
            .expect("authority request carries probe");
        probe.probe_id = sinomo_core::Id::generate().to_string();
        let confirmation_id = metadata_authority_probe_confirmation_id(probe)
            .expect("prepared authority probe has an exact prefix confirmation");
        let request_id = command.request_id.clone();
        request_ids.push(request_id.clone());
        self.replication.pending_metadata_authority_probes.insert(
            request_id.clone(),
            PendingMetadataAuthorityProbe {
                target_node_id: target_node_id.clone(),
                confirmation_id,
                expected_shard: shard.clone(),
                expected_next_after_sequence: command.batch.next_after_sequence,
                expected_prefix: probe.prepared_prefix.clone(),
            },
        );
        let request = |delivery| MetadataAuthorityRoutedRequest {
            request_id: request_id.clone(),
            target_node_id: target_node_id.clone(),
            hedge,
            delivery,
        };
        let mut message = self.next_dfs_routed_message(
            target_node_id,
            DfsNetworkMessage::MetadataJournalBatch(command),
        );
        if let Some(fanout) = fanout {
            match self
                .transport
                .send_routed_message_on_metadata_authority_fanout(fanout, message)
            {
                Ok(None) => return Ok(Some(request(MetadataAuthorityRoutedDelivery::Fanout))),
                Ok(Some(returned)) => message = returned,
                Err(_) => {
                    self.replication
                        .pending_metadata_authority_probes
                        .remove(&request_id);
                    return Ok(None);
                }
            }
        }
        let sent = match self.send_routed_dfs_message_if_routable(message) {
            Ok(sent) => sent,
            Err(DfsError::Network { .. } | DfsError::NetworkDelivery { .. }) => false,
            Err(error) => return Err(error),
        };
        if !sent {
            self.replication
                .pending_metadata_authority_probes
                .remove(&request_id);
            return Ok(None);
        }
        Ok(Some(request(MetadataAuthorityRoutedDelivery::Inbound)))
    }

    fn metadata_authority_replicated_through_sequence_for_target(
        &self,
        target_node_id: &NodeId,
        prepared_batch: &DfsMetadataJournalBatch,
    ) -> Result<u64, DfsError> {
        self.metadata_replicated_through_sequence_for_target(target_node_id, &prepared_batch.shard_id)
    }

    fn metadata_replicated_through_sequence_for_target(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Err(DfsError::Metadata {
                message: "metadata authority probe has no durable metadata backend".to_string(),
            });
        };
        let durable_replicated_through_sequence =
            backend.metadata_replication_cursor(target_node_id, shard_id)?;
        let replicated_through_sequence = self
            .replication
            .metadata_replication_cursors
            .get(&(target_node_id.clone(), shard_id.clone()))
            .copied()
            .unwrap_or(0)
            .max(durable_replicated_through_sequence);
        Ok(replicated_through_sequence)
    }

    fn metadata_authority_batch_since(
        &self,
        replicated_through_sequence: u64,
        prepared_batch: &DfsMetadataJournalBatch,
        current_cut_sequence: u64,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        if current_cut_sequence < prepared_batch.next_after_sequence {
            return Ok(None);
        }
        if replicated_through_sequence >= current_cut_sequence {
            // A cursor is only a scheduling hint. Ask the receiver to verify
            // its actual stored operation and exact prefix with a fresh nonce.
            // A checkpoint can legitimately omit original covered frames.
            return Ok(Some(DfsMetadataJournalBatch {
                shard_id: prepared_batch.shard_id.clone(),
                entries: Vec::new(),
                next_after_sequence: current_cut_sequence,
            }));
        }
        let prepared_batch_covers_cut = prepared_batch.next_after_sequence == current_cut_sequence
            && prepared_batch
                .entries
                .last()
                .is_some_and(|entry| entry.sequence == prepared_batch.next_after_sequence)
            && prepared_batch
                .entries
                .windows(2)
                .all(|entries| entries[1].sequence == entries[0].sequence.saturating_add(1));
        if prepared_batch_covers_cut
            && prepared_batch.entries.first().is_some_and(|entry| {
                entry.sequence == replicated_through_sequence.saturating_add(1)
            })
        {
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
        let batch = backend.journal_batch_for_shard_since_limited(
            &prepared_batch.shard_id,
            replicated_through_sequence,
            DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
        )?;
        // This bounded batch must contain every frame through the captured
        // current cut. Neither an incomplete suffix nor a newer moving head
        // can stand in for that exact prefix. The original body may already be
        // retained by the receiver, including in a snapshot-covered record.
        if let Some(batch) = batch.filter(|batch| batch.next_after_sequence == current_cut_sequence)
        {
            return Ok(Some(batch));
        }
        // A promoted primary may have no outgoing cursor for a peer that
        // already holds the complete handoff prefix. That advisory absence
        // must not prevent a fresh operation-bound challenge. Send the original
        // prepare and every later frame through the captured cut; a genuinely
        // lagging receiver still rejects the gap before append or ACK.
        let Some(first) = prepared_batch.entries.first() else {
            return Ok(None);
        };
        let original_after = first.sequence.saturating_sub(1);
        if original_after <= replicated_through_sequence {
            return Ok(None);
        }
        // A peer can have acknowledged the preceding prepare without yet
        // receiving its commit. After losing an outgoing hint, carry that
        // actual commit with this challenge when the bounded suffix reaches
        // the captured cut. The receiver still verifies the complete prefix;
        // selecting these frames does not advance or certify a peer cursor.
        if original_after > 0 {
            let before_predecessor = original_after - 1;
            let predecessor = backend.journal_batch_for_shard_since_limited(
                &prepared_batch.shard_id,
                before_predecessor,
                1,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )?;
            if predecessor.as_ref().is_some_and(|batch| {
                batch.entries.len() == 1
                    && batch.entries[0].sequence == original_after
                    && matches!(
                        batch.entries[0].mutation,
                        DfsMetadataMutation::CommitPreparedOperationV7 { .. }
                    )
            }) && let Some(batch) = backend
                .journal_batch_for_shard_since_limited(
                    &prepared_batch.shard_id,
                    before_predecessor,
                    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                    DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
                )?
                .filter(|batch| batch.next_after_sequence == current_cut_sequence)
            {
                return Ok(Some(batch));
            }
        }
        if prepared_batch_covers_cut {
            return Ok(Some(prepared_batch.clone()));
        }
        Ok(backend
            .journal_batch_for_shard_since_limited(
                &prepared_batch.shard_id,
                original_after,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )?
            .filter(|batch| batch.next_after_sequence == current_cut_sequence))
    }

    fn record_metadata_authority_replication_cursors(
        &mut self,
        shard: &DfsMetadataShardId,
        cursors: &[(NodeId, u64)],
    ) {
        if cursors.is_empty() {
            return;
        }
        let started = dfs_perf_trace_enabled().then(Instant::now);
        for (node, sequence) in cursors {
            let current = self
                .replication
                .metadata_replication_cursors
                .entry((node.clone(), shard.clone()))
                .or_insert(0);
            *current = (*current).max(*sequence);
        }
        if let Some(backend) = self.metadata_backend.as_ref() {
            backend.observe_metadata_replication_cursors(shard, cursors);
        }
        // Fresh operation nonces and receiver ACK floors authorize this write;
        // sender cursors only select the next catch-up suffix. Keep this cheap
        // projection across lazy request-shell rebuilds. The existing journal
        // publisher consumes the same local hints and independently checkpoints
        // its own verified responses, so a
        // lost shell or restart can safely resend from an older on-disk hint.
        // Do not take the metadata writer lock or create cursor-file workers
        // on the foreground full-shard acknowledgement path.
        if let Some(started) = started {
            dfs_perf_trace(format_args!(
                "metadata-authority-cursors shard={} count={} storage=memory elapsed_us={}",
                shard.0,
                cursors.len(),
                started.elapsed().as_micros()
            ));
        }
    }

    fn clear_metadata_authority_probe_requests(&mut self, request_ids: &[String]) {
        for request_id in request_ids {
            self.replication
                .pending_metadata_authority_probes
                .remove(request_id);
            self.replication
                .metadata_authority_probe_results
                .remove(request_id);
            self.replication
                .metadata_authority_rejections
                .remove(request_id);
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
                catalog_generation: 0,
                retention_policies: Default::default(),
                module_metadata: Default::default(),
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
            // This receiver holds the content it just stored, so its own
            // receipt plans against the slot's current generation.
            let generation =
                self.backend_receipt_slot_generation(object_id, version, replica_node_id)?;
            self.put_metadata_receipt(
                file_system_id,
                object_id,
                version,
                replica_node_id,
                content_hash,
                generation,
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
            // This receiver verified its own committed content just now.
            let generation =
                self.backend_receipt_slot_generation(object_id, version, replica_node_id)?;
            self.put_metadata_receipt(
                file_system_id,
                object_id,
                version,
                replica_node_id,
                content_hash,
                generation,
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
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(intent) = backend.get_intent(object_id, intent_id)?
        {
            let requester_has_intent_authority = &intent.coordinator_node_id == requester_node_id
                || (requester_is_metadata_primary && object_metadata.is_some());
            if !requester_has_intent_authority {
                return Err(DfsError::UnauthorizedReplicaCommand {
                    source_node_id: requester_node_id.clone(),
                    reason: format!("source is not coordinator for DFS write intent {intent_id}"),
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
        if let Some(expected_hash) = intent.content_hash.as_deref()
            && expected_hash != content_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash.to_string(),
                actual: content_hash.to_string(),
            });
        }
        Ok(())
    }

    pub(crate) fn ensure_blob_fetch_authorized(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsFetchBlobCommand,
    ) -> Result<(), DfsError> {
        let intent_id = &command.intent_id;
        let object_id = &command.object_id;
        let Some(intent) = ({
            let Some(backend) = self.metadata_backend.as_ref() else {
                return Ok(());
            };
            backend.get_intent(object_id, intent_id)?
        }) else {
            return Ok(());
        };
        let intent_object_metadata = intent.object_metadata.clone();
        let (requester_is_projected_metadata_primary, requester_is_metadata_primary) = match self
            .current_read_object_metadata(object_id)
            .ok()
            .or(intent_object_metadata)
        {
            Some(object) => {
                let metadata_shard = self.select_metadata_shard_for_object(&object)?;
                let projected_primary = metadata_shard
                    .as_ref()
                    .is_some_and(|shard| shard.primary_node_id == *requester_node_id);
                let existing_replica_authority = projected_primary
                    || self.command_authority_shard_proves_metadata_primary(
                        requester_node_id,
                        &object,
                        metadata_shard.as_ref(),
                        command.authority_shard.as_ref(),
                    )?;
                (projected_primary, existing_replica_authority)
            }
            None => (false, false),
        };
        ensure_blob_fetch_intent_permission(
            &self.node_id,
            requester_node_id,
            command,
            &intent,
            requester_is_projected_metadata_primary,
            requester_is_metadata_primary,
            || {
                Ok(self
                    .backend_receipt_for_replica(object_id, &intent.target_version, &self.node_id)?
                    .is_some())
            },
        )
    }
}

pub(crate) fn ensure_blob_fetch_intent_permission(
    local_node_id: &NodeId,
    requester_node_id: &NodeId,
    command: &DfsFetchBlobCommand,
    intent: &DfsWriteIntentRecord,
    requester_is_projected_metadata_primary: bool,
    requester_is_metadata_primary: bool,
    local_has_receipt: impl FnOnce() -> Result<bool, DfsError>,
) -> Result<(), DfsError> {
    let intent_id = &command.intent_id;
    if &intent.coordinator_node_id != requester_node_id
        && !intent.desired_replica_node_ids.contains(requester_node_id)
        && !requester_is_metadata_primary
    {
        return Err(DfsError::UnauthorizedReplicaCommand {
            source_node_id: requester_node_id.clone(),
            reason: format!("source is not authorized to fetch DFS blob for intent {intent_id}"),
        });
    }
    // The primary may replace a requester's pre-staged copy with other
    // replicas when it binds the final placement plan. That requester is
    // still the durable transfer source for this pending write, but its
    // copy must never contribute a receipt to W/N. Limit this source-only
    // permission to the exact pending intent and current metadata primary;
    // it grants no authority to serve a terminal or different target.
    let pending_requester_source = requester_is_projected_metadata_primary
        && intent.publication.is_current()
        && intent.coordinator_node_id == *local_node_id
        && matches!(
            intent.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        )
        && intent.target_version == command.version
        && intent.content_hash.as_deref() == Some(command.content_hash.as_str());
    if !pending_requester_source
        && !intent.desired_replica_node_ids.contains(local_node_id)
        && !local_has_receipt()?
    {
        return Err(DfsError::UnauthorizedReplicaCommand {
            source_node_id: requester_node_id.clone(),
            reason: format!(
                "local node is not an expected or durable replica for DFS intent {intent_id}"
            ),
        });
    }
    Ok(())
}

pub(crate) fn missing_metadata_shard_assignment(object: &DfsObjectMetadata) -> DfsError {
    DfsError::Distributed {
        message: format!(
            "clustered DFS object {} at {} has no committed metadata-shard assignment",
            object.object_id, object.path
        ),
    }
}

/// Hash a namespace prepare that has already been canonicalized in place.
///
/// Callers that own a large prepare can normalize it once and keep transferring
/// that same allocation through journal and table materialization. The public
/// compatibility helper above retains the defensive clone for borrowed and
/// recovery callers whose normalization state is not known.

/// Build the authority identity for a newly-created namespace prepare whose
/// canonical payload digest was computed by this process immediately before
/// the same record was durably prepared.
///
/// Recovered or network-provided records must use
/// [`metadata_authority_operation_for_namespace_prepare`] so their complete
/// payload is independently rehashed. This narrow constructor avoids hashing
/// a freshly-built multi-megabyte bulk transaction twice on the primary.

/// Called only for enabled tracing; keeps the exact publication operation on
/// each disjoint subphase without cloning it or changing the guard lifetime.

pub(crate) fn metadata_authority_operation_digest(
    operation: &DfsMetadataAuthorityOperation,
) -> Result<&str, DfsError> {
    let DfsMetadataAuthorityOperation::PreparedOperation { payload_digest, .. } = operation;
    Ok(payload_digest)
}

pub(crate) fn metadata_authority_operation_prepare_sequence(
    operation: &DfsMetadataAuthorityOperation,
) -> u64 {
    let DfsMetadataAuthorityOperation::PreparedOperation {
        prepare_sequence, ..
    } = operation;
    *prepare_sequence
}

pub(crate) fn metadata_authority_operation_is_well_formed_for_shard(
    operation: &DfsMetadataAuthorityOperation,
    shard: &DfsMetadataShardConfig,
) -> bool {
    let DfsMetadataAuthorityOperation::PreparedOperation {
        operation_id,
        file_system_id,
        shard_id,
        owner_epoch,
        prepare_sequence,
        payload_digest,
    } = operation;
    !operation_id.is_empty()
        && file_system_id == &shard.file_system_id
        && shard_id.0 == shard.shard_id
        && *owner_epoch == shard.epoch
        && *prepare_sequence > 0
        && payload_digest.starts_with("dfs-prepared-operation-v7:blake3:")
}

/// Verify that the exact operation named by an authority acknowledgement is
/// durably present in the DFS metadata backend. This is deliberately a DFS
/// metadata check: ordinary writes do not put file metadata into cluster KV.
pub(crate) fn metadata_authority_probe_confirmation_id(
    probe: &DfsMetadataAuthorityProbe,
) -> Option<String> {
    let prefix = &probe.prepared_prefix;
    if probe.probe_id.is_empty()
        || !prefix.is_well_formed()
        || metadata_authority_operation_prepare_sequence(&probe.operation) == 0
        || prefix.journal_sequence < metadata_authority_operation_prepare_sequence(&probe.operation)
    {
        return None;
    }
    Some(format!(
        "dfs-authority-prefix-v7:{}:{}:{}",
        probe.probe_id, prefix.journal_sequence, prefix.digest
    ))
}

/// Verify an authority probe immediately after applying its journal batch.
///
/// Current frame ingest carries evidence from the exact validated preparation.
/// Reuse it before the durable acknowledgement; duplicate delivery verifies the
/// same operation against the retained current record.
pub(crate) fn verify_persisted_metadata_authority_probe_after_apply(
    backend: &dyn DfsMetadataBackend,
    shard: &DfsMetadataShardConfig,
    probe: &DfsMetadataAuthorityProbe,
    apply_report: &DfsMetadataReplicaApplyReport,
) -> Result<String, DfsError> {
    let operation_verified = apply_report
        .namespace_authority_proof_for(shard, &probe.operation)
        .is_some();
    if let Some(durable_prefix) = apply_report.durable_prefix_proof() {
        if !operation_verified
            || !metadata_authority_operation_is_well_formed_for_shard(&probe.operation, shard)
        {
            return Err(DfsError::Metadata {
                message: format!(
                    "durable metadata authority ingest did not validate the exact operation for shard {} epoch {}",
                    shard.shard_id, shard.epoch
                ),
            });
        }
        let confirmation_id = metadata_authority_probe_confirmation_id(probe).ok_or_else(|| {
            DfsError::Metadata {
                message: format!(
                    "metadata authority probe has no well-formed exact prepared prefix for shard {} epoch {}",
                    shard.shard_id, shard.epoch
                ),
            }
        })?;
        if &probe.prepared_prefix != durable_prefix {
            return Err(DfsError::Metadata {
                message: format!(
                    "durable metadata authority journal for shard {} does not match the primary's exact prepared history",
                    shard.shard_id
                ),
            });
        }
        return Ok(confirmation_id);
    }
    verify_persisted_metadata_authority_probe_inner(backend, shard, probe, operation_verified)
}

pub(crate) fn verify_persisted_metadata_authority_probe_inner(
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
    if probe.prepared_prefix != local_prefix {
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
    let DfsMetadataAuthorityOperation::PreparedOperation { operation_id, .. } = operation;
    let record = backend
        .prepared_operation_record(operation_id)?
        .ok_or_else(|| DfsError::Metadata {
            message: "authority operation has no durable complete preparation".into(),
        })?;
    crate::metadata_api::validated_metadata_authority_operation_digest(&record, operation)?;
    if !metadata_authority_operation_is_well_formed_for_shard(operation, shard) {
        return Err(DfsError::Metadata {
            message: "authority operation differs from actual durable preparation".into(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod authority_cursor_tests {
    use super::*;

    #[test]
    fn write_set_followers_compare_epoch_and_primary_but_not_replicas() {
        let node = |name: &str| NodeId::from_legacy_named(name).unwrap();
        let shard = DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "follower-assignment".into(),
            primary_node_id: node("assignment-primary"),
            replica_node_ids: vec![node("assignment-b")],
            epoch: 4,
            assignment: crate::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let mut recruited = shard.clone();
        recruited.replica_node_ids.push(node("assignment-recruit"));
        assert!(metadata_authority_probe_names_follower_assignment(
            &recruited, &shard, true
        ));
        assert!(!metadata_authority_probe_names_follower_assignment(
            &recruited, &shard, false
        ));
        let mut later = shard.clone();
        later.epoch += 1;
        assert!(!metadata_authority_probe_names_follower_assignment(
            &later, &shard, true
        ));
        let mut moved = shard.clone();
        moved.primary_node_id = node("assignment-b");
        assert!(!metadata_authority_probe_names_follower_assignment(
            &moved, &shard, true
        ));
    }

    #[test]
    fn a_refused_epoch_ends_the_attempt_as_a_record_change() {
        let node = |name: &str| NodeId::from_legacy_named(name).unwrap();
        let shard = DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "refusal".into(),
            primary_node_id: node("refusal-a"),
            replica_node_ids: vec![node("refusal-b"), node("refusal-c")],
            epoch: 4,
            assignment: crate::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let operation = DfsMetadataAuthorityOperation::PreparedOperation {
            operation_id: "refused-op".into(),
            file_system_id: shard.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new("refusal"),
            owner_epoch: 4,
            prepare_sequence: 7,
            payload_digest: "digest".into(),
        };
        let record = |epoch, primary: &str| crate::DfsMetadataMemberRecordView {
            epoch,
            generation: 9,
            primary_node_id: node(primary),
        };
        let outcome = |refusals: &BTreeMap<NodeId, crate::DfsMetadataMemberRecordView>| {
            match metadata_authority_refusal_outcome(&node("refusal-a"), &shard, &operation, refusals) {
                Some(DfsError::MetadataCommitOutcomeUnknown {
                    cause: crate::DfsCommitOutcomeUnknownCause::RecordChanged { epoch },
                    current_primary_node_id,
                    operation_id,
                    epoch: attempt_epoch,
                    ..
                }) => {
                    assert_eq!((operation_id.as_str(), attempt_epoch), ("refused-op", 4));
                    Some((epoch, current_primary_node_id))
                }
                Some(other) => panic!("{other:?}"),
                None => None,
            }
        };
        assert_eq!(outcome(&BTreeMap::new()), None);
        // A member fenced for a candidate still names this primary: the
        // record is changing, to no known primary yet.
        let fenced = BTreeMap::from([(node("refusal-b"), record(4, "refusal-a"))]);
        assert_eq!(outcome(&fenced), Some((5, None)));
        // A member whose record moved on names its primary.
        let mut superseded = fenced.clone();
        superseded.insert(node("refusal-c"), record(6, "refusal-c"));
        assert_eq!(outcome(&superseded), Some((6, Some(node("refusal-c")))));
    }

    #[test]
    fn re_sends_back_off_to_the_network_liveness_interval_and_no_deadline_never_passes() {
        let mut interval = DFS_METADATA_AUTHORITY_HEDGE_AFTER;
        let mut seen = Vec::new();
        for _ in 0..8 {
            interval = metadata_authority_next_resend_interval(interval);
            seen.push(interval);
        }
        assert_eq!(seen[0], Duration::from_millis(500));
        assert!(seen.windows(2).all(|pair| pair[0] <= pair[1]));
        assert_eq!(*seen.last().unwrap(), DFS_METADATA_AUTHORITY_RESEND_MAX);
        assert!(!metadata_authority_deadline_passed(None));
        assert!(metadata_authority_deadline_passed(Some(Instant::now())));
        assert!(!metadata_authority_deadline_passed(Some(
            Instant::now() + Duration::from_secs(60)
        )));
    }

    #[test]
    fn an_unconfirmed_hedge_resends_the_body_only_without_upload_progress() {
        // The first hedge has no earlier sample and only records one.
        assert!(!metadata_authority_upload_stalled(None, Some(0)));
        // No bytes handed to the network since the previous hedge.
        assert!(metadata_authority_upload_stalled(Some(0), Some(0)));
        assert!(metadata_authority_upload_stalled(
            Some(64 * 1024),
            Some(64 * 1024)
        ));
        // A slow but advancing upload is left alone.
        assert!(!metadata_authority_upload_stalled(
            Some(64 * 1024),
            Some(80 * 1024)
        ));
        // Without a direct call there is no upload to judge.
        assert!(!metadata_authority_upload_stalled(Some(0), None));
    }

    #[test]
    fn unknown_outgoing_cursor_still_challenges_the_exact_original_and_current_cut() {
        let (runtime, requester, shard) = crate::test_support::current_native_metadata_service(
            "native-authority-unknown-cursor",
            3,
            "/",
        )
        .unwrap();
        drop(requester);
        let mut service = runtime.metadata_primary_service("/").unwrap();
        let shard_id = DfsMetadataShardId::new(&shard.shard_id);
        let file_system = runtime.file_system();
        let prepare = |service: &mut NetworkedDfsService<crate::DfsNodeTransport>, index: usize| {
            service
                .metadata_backend
                .as_mut()
                .unwrap()
                .prepare_operation(
                    DfsPreparedOperationPayload::for_metadata_effects(
                        DfsPreparedOperationScope {
                            file_system_id: shard.file_system_id.clone(),
                            shard_id: shard_id.clone(),
                            owner_epoch: shard.epoch,
                            operation_id: format!("unknown-cursor-{index}"),
                        },
                        Vec::new(),
                        Vec::new(),
                        vec![DfsPreparedMetadataEffect::PutFileSystem {
                            file_system: file_system.clone(),
                        }],
                        time::OffsetDateTime::now_utc(),
                    )
                    .unwrap(),
                    None,
                )
                .unwrap()
        };
        // Actual journal DATA larger than one bounded catch-up batch. This
        // test grants no ACKs: receiver gap/fork tests own that boundary.
        for index in 0..DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT {
            prepare(&mut service, index);
        }
        let original = prepare(&mut service, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT);
        let original_batch = service
            .metadata_backend
            .as_ref()
            .unwrap()
            .journal_batch_for_shard_since_limited(
                &shard_id,
                original.prepare_sequence - 1,
                1,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .unwrap()
            .unwrap();
        let challenge = service
            .metadata_authority_batch_since(0, &original_batch, original.prepare_sequence)
            .unwrap()
            .expect("an unknown scheduling cursor must not suppress a fresh proof request");
        assert_eq!(challenge.entries, original_batch.entries);
        let later = prepare(&mut service, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT + 1);
        let challenge = service
            .metadata_authority_batch_since(0, &original_batch, later.prepare_sequence)
            .unwrap()
            .unwrap();
        assert_eq!(challenge.entries.len(), 2);
        assert_eq!(challenge.entries[0].sequence, original.prepare_sequence);
        assert_eq!(challenge.entries[1].sequence, later.prepare_sequence);
        assert_eq!(challenge.next_after_sequence, later.prepare_sequence);
        // A captured cut that omits later durable frames cannot be proved.
        assert!(
            service
                .metadata_authority_batch_since(0, &original_batch, later.prepare_sequence + 1)
                .unwrap()
                .is_none()
        );
        drop(service);
        runtime.close().unwrap();
    }
}

#[cfg(test)]
#[path = "service_replica/promoted_primary_native_tests.rs"]
mod promoted_primary_native_tests;
