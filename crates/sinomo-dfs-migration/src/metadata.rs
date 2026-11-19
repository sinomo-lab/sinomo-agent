//! Owned, resumable capture of a metadata shard's retained copy: the bounded,
//! source-fenced replay its per-shard conversion builds the shard's image from.
pub(crate) mod ack_floor;
pub(crate) mod canonical_generation;
pub(crate) mod combined_root;
mod legacy_projection;
mod placement_reservations;
pub use placement_reservations::RetainedPlacementReservation;
pub(crate) use placement_reservations::validate_placement_reservations;
mod retained_content;
pub(crate) mod scope_mapping;
pub(crate) mod sobj_cursor;
pub(crate) mod source;
pub(crate) mod source_document;
pub(crate) mod source_ancestry;
mod source_proof;
mod wire_json;

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinomo_dfs::{
    DfsFileSystemId, DfsMetadataPrefixProof, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsStoragePreparation, MetadataBackendError,
};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

type Result<T> = std::result::Result<T, MetadataBackendError>;

#[cfg(test)]
pub(crate) fn count_stopped_source_records_for_test(root: &Path) -> Result<usize> {
    // The fixture's real owner is stopped before this read-only census. Verify
    // the complete inventory again; this observation grants no source custody
    // or publication authority to the subsequently mounted converter.
    let inventory = source::discover(root)?;
    let mut cursor = source::SourceCursor::new(inventory.clone());
    let mut records = 0;
    while source::next(root, &mut cursor)?.is_some() {
        records += 1;
    }
    assert_eq!(source::discover(root)?, inventory);
    Ok(records)
}

/// Exact original control-plane reservation retained as conversion DATA.
/// Its old fence/ACK fields remain in `entry.value`; this never grants author
/// permission. Fresh native import guards must compare that complete entry.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct OriginalMetadataHandoffReservation {
    pub entry: sinomo_core::kv::KvEntry,
    pub retiring_shard: DfsMetadataShardConfig,
    pub desired_shard: DfsMetadataShardConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MetadataSourceCut {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retained_placement_reservations: Vec<RetainedPlacementReservation>,
    pub source_root: PathBuf,
    pub source_identity: String,
    pub scope_shard: DfsMetadataShardId,
    pub file_system_ids: Vec<DfsFileSystemId>,
    pub source_snapshot_fingerprint: String,
    /// Actual independently reread source SnapshotHeader; never target format.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_format_version: Option<u16>,
    pub source_exact_prefix: Option<DfsMetadataPrefixProof>,
    /// Conversion projection digest; distinct from the native image identity.
    pub converted_image_fingerprint: String,
    pub current_image_fingerprint: String,
    pub target_exact_prefix: DfsMetadataPrefixProof,
    pub source_journal_sequence: u64,
    pub source_state_fingerprint: String,
    pub projection_assignments: Vec<DfsMetadataShardConfig>,
    #[serde(default)]
    pub original_handoff: Option<OriginalMetadataHandoffReservation>,
    pub staged_root: PathBuf,
    pub pending_operations: Vec<Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MetadataCapturedKvEntry {
    pub key: String,
    pub entry: Option<sinomo_core::kv::KvEntry>,
}
// Addition to existing metadata.rs; no independent persistence family.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(deny_unknown_fields)]
pub(crate) struct MetadataDataReservationRef {
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) original_shard_id: DfsMetadataShardId,
    pub(crate) capsule_digest: String,
}
/// The native owner under which the converter captures a shard's own copy:
/// the copy's ownership, the current assignments and import policy, and any
/// original handoff reservation the copy's source still names.
pub trait MetadataImportAuthority: Send + Sync {
    fn validate_owned_source(
        &self,
        root: &Path,
        scope: &DfsMetadataShardId,
        fs_ids: &[DfsFileSystemId],
    ) -> Result<()>;
    fn source_policy_candidate_count(&self, source_root: &Path) -> Result<usize>;
    fn assignments(
        &self,
        file_system_ids: &[DfsFileSystemId],
    ) -> Result<Vec<DfsMetadataShardConfig>>;
    fn original_handoff_reservation(
        &self,
        shard: &DfsMetadataShardId,
        file_system_ids: &[DfsFileSystemId],
    ) -> Result<Option<OriginalMetadataHandoffReservation>>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Phase {
    Source,
    JournalTail,
    Build,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    source: source::SourceCursor,
    events: sinomo_core::storage::migration::stream::StreamCursor,
    phase: Phase,
    tail_after: u64,
    current_import_candidate_count: Option<usize>,
}

/// Immutable source facts captured before target partitioning/materialization.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct CapturedSourceCut {
    source_identity: String,
    source_snapshot_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_format_version: Option<u16>,
    source_exact_prefix: Option<DfsMetadataPrefixProof>,
    source_journal_sequence: u64,
    source_state_fingerprint: String,
    file_system_ids: Vec<DfsFileSystemId>,
    projection_assignments: Vec<DfsMetadataShardConfig>,
    #[serde(default)]
    original_handoff: Option<OriginalMetadataHandoffReservation>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
enum ConversionEvent {
    Source {
        record: source::SourceRecord,
        point: source::SourceCursor,
    },
    Tail(Value),
}
struct Session {
    captured_unit: Option<Arc<canonical_generation::CapturedMetadataUnit>>,
    _source_lock: Arc<File>,
    source_root: PathBuf,
    destination_root: PathBuf,
    staged_root: PathBuf,
    inventory: source::SourceInventory,
    progress: Progress,
    replay_cursor: sinomo_core::storage::migration::stream::StreamCursor,
    replayed_events: u64,
    replay_source: source::SourceCursor,
    replay_tail_after: Option<u64>,
    replay_validated: bool,
    current_import_policy_bound: bool,
    projection: legacy_projection::LegacyProjection,
    header: serde_json::Map<String, Value>,
    materialized_through: u64,
    source_head: u64,
    source_prefix: Option<DfsMetadataPrefixProof>,
    journal_sequences: std::collections::BTreeSet<u64>,
    journal_frame_digests: BTreeMap<u64, String>,
    // Only this bounded turn owns these unpublished files. They are synced and
    // installed before a reader or the durable event cursor can consume them.
    pending_journal_evidence: BTreeMap<u64, (PathBuf, File)>,
    journal_evidence_dirty: bool,
}
struct RootSlot {
    shard: DfsMetadataShardId,
    session: Mutex<Option<Session>>,
}
impl RootSlot {
    fn try_lock_session(&self) -> Option<std::sync::MutexGuard<'_, Option<Session>>> {
        match self.session.try_lock() {
            Ok(session) => Some(session),
            Err(std::sync::TryLockError::WouldBlock) => None,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                let mut session = poisoned.into_inner();
                session.take();
                self.session.clear_poison();
                Some(session)
            }
        }
    }
}
/// A component-owned controller; sessions retain the physical source lock,
/// source cut and conversion cursor across Pending and cancellation boundaries.
#[derive(Default)]
pub struct MetadataConverter {
    roots: Mutex<BTreeMap<PathBuf, Arc<RootSlot>>>,
    authority: Option<Arc<dyn MetadataImportAuthority>>,
    #[cfg(test)]
    step_hook: Mutex<Option<PreparationStepHook>>,
}
#[cfg(test)]
type PreparationStepHook = Arc<dyn Fn(&Path, &DfsMetadataShardId, usize, Option<DfsStoragePreparation>) -> Result<()> + Send + Sync>;
impl MetadataConverter {
    #[cfg(test)]
    pub(crate) fn observe_steps_for_test(&self, hook: PreparationStepHook) {
        *self.step_hook.lock().unwrap() = Some(hook);
    }
    /// Release warm source custody after the sole owner has joined its workers.
    /// Durable event/checkpoint files remain the restart source of truth.
    pub(crate) fn release_checkpointed_sessions(&self) -> Result<()> {
        let mut roots = self.roots.lock().map_err(error)?;
        let slots = roots.values().cloned().collect::<Vec<_>>();
        let mut held = Vec::with_capacity(slots.len());
        for slot in &slots {
            let guard = match slot.session.try_lock() {
                Ok(guard) => guard,
                Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => return Err(error("source sessions cannot drain while an owner step is still active")),
            };
            held.push(guard);
        }
        let sessions = held.iter_mut().filter_map(|session| session.take()).collect::<Vec<_>>();
        for slot in &slots { slot.session.clear_poison(); }
        roots.clear();
        drop(held);
        drop(roots);
        drop(sessions);
        Ok(())
    }

    fn capture_metadata_unit_progress(&self, destination: &Path, source: &Path,
        shard: &DfsMetadataShardId, budget: usize)
        -> Result<(DfsStoragePreparation, Option<Arc<canonical_generation::CapturedMetadataUnit>>)> {
        if source.join("metadata-generation.sobj").exists()
            && matches!(sinomo_dfs::inspect_metadata_storage_generation(source)?.state,
                sinomo_dfs::DfsMetadataGenerationState::Staged { .. }) {
            return Err(error("a staged current target is not a new original source; resume its existing owner job"));
        }
        let progress = self.prepare_source(destination, source, shard, budget)?;
        let destination = canonical_destination(destination)?;
        let slot = self.roots.lock().map_err(error)?.get(&destination).cloned()
            .ok_or_else(|| error("source capture lost its checkpointed session"))?;
        let Some(session) = slot.try_lock_session() else { return Ok((progress, None)); };
        Ok((progress, session.as_ref().and_then(|session| session.captured_unit.clone())))
    }

    pub fn with_authority(authority: Arc<dyn MetadataImportAuthority>) -> Self {
        Self {
            roots: Mutex::new(BTreeMap::new()),
            authority: Some(authority),
            #[cfg(test)]
            step_hook: Mutex::new(None),
        }
    }

    fn prepare_source(
        &self,
        destination: &Path,
        source: &Path,
        shard: &DfsMetadataShardId,
        budget: usize,
    ) -> Result<DfsStoragePreparation> {
        if budget == 0 {
            return Err(error("metadata conversion budget must be positive"));
        }
        let destination = canonical_destination(destination)?;
        // No filesystem work runs under the shared directory mutex. A stable
        // slot also serializes waiting aliases across errors and final rename.
        let slot = {
            let mut roots = self.roots.lock().unwrap_or_else(|error| error.into_inner());
            Arc::clone(roots.entry(destination.clone()).or_insert_with(|| {
                Arc::new(RootSlot {
                    shard: shard.clone(),
                    session: Mutex::new(None),
                })
            }))
        };
        if slot.shard != *shard {
            return Err(error(
                "one physical metadata destination was requested under different shard identities",
            ));
        }
        // A lifecycle caller can retain admission/cache state needed by the
        // active owner. Yield without waiting, advancing or replacing its slot.
        // The same owner retains physical source custody and the exact cursor.
        let Some(mut session) = slot.try_lock_session() else {
            #[cfg(test)]
            sinomo_dfs::test_support::record_metadata_operation_boundary(
                &destination,
                "migration-prepare-slot-contended",
                || serde_json::json!({"slot":format!("{:p}", Arc::as_ptr(&slot)),"shard":shard,"source":source,"budget":budget}),
            );
            return Ok(DfsStoragePreparation::Pending {
                processed_records: 0,
            });
        };
        #[cfg(test)]
        let step_hook = self.step_hook.lock().unwrap().clone();
        #[cfg(test)]
        if let Some(hook) = &step_hook {
            match hook(&destination, shard, budget, None) {
                Err(MetadataBackendError::StoragePreparationPending { processed_records, .. }) =>
                    return Ok(DfsStoragePreparation::Pending { processed_records }),
                result => result?,
            }
        }
        let replayed_events_before = session.as_ref().map(|state| state.replayed_events);
        tracing::trace!(
            target: "sinomo.dfs.perf",
            converter = ?(self as *const Self),
            slot = ?Arc::as_ptr(&slot),
            destination = %destination.display(),
            source = %source.display(),
            shard = %shard.0,
            budget,
            warm_session = session.is_some(),
            replayed_events = ?replayed_events_before,
            durable_events = ?session.as_ref().map(|state| state.progress.events.records),
            phase = ?session.as_ref().map(|state| &state.progress.phase),
            "migration bounded metadata prepare started"
        );
        #[cfg(test)]
        sinomo_dfs::test_support::record_metadata_operation_boundary(
            &destination,
            "migration-prepare-turn-start",
            || serde_json::json!({"slot":format!("{:p}", Arc::as_ptr(&slot)),"shard":shard,"source":source,"budget":budget,"phase":format!("{:?}",session.as_ref().map(|state| &state.progress.phase))}),
        );
        let result = self.prepare_with_source_inner(&destination, source, shard, budget, &mut session);
        #[cfg(test)]
        if let (Some(hook), Ok(progress)) = (&step_hook, &result) {
            hook(&destination, shard, budget, Some(*progress))?;
        }
        let reset_session = matches!(&result, Ok(DfsStoragePreparation::Ready))
            || (result.is_err() && !matches!(&result, Err(MetadataBackendError::StoragePreparationPending { .. })));
        tracing::trace!(
            target: "sinomo.dfs.perf",
            converter = ?(self as *const Self),
            slot = ?Arc::as_ptr(&slot),
            destination = %destination.display(),
            shard = %shard.0,
            replayed_events_before = ?replayed_events_before,
            replayed_events_after = ?session.as_ref().map(|state| state.replayed_events),
            durable_events = ?session.as_ref().map(|state| state.progress.events.records),
            phase = ?session.as_ref().map(|state| &state.progress.phase),
            replay_validated = ?session.as_ref().map(|state| state.replay_validated),
            outcome = ?result,
            reset_session,
            "migration bounded metadata prepare returned"
        );
        #[cfg(test)]
        sinomo_dfs::test_support::record_metadata_operation_boundary(
            &destination,
            "migration-prepare-turn-result",
            || serde_json::json!({"slot":format!("{:p}", Arc::as_ptr(&slot)),"shard":shard,"source":source,"result":format!("{result:?}"),"phase":format!("{:?}",session.as_ref().map(|state| &state.progress.phase))}),
        );
        if reset_session {
            // Invalid source state drops the warm view under the same lock.
            // A typed native wait keeps its exact cursor and source custody;
            // the next attempt still verifies the source and native authority.
            session.take();
        }
        drop(session);
        if matches!(&result, Ok(DfsStoragePreparation::Ready))
            && !self.current_generation_ready_before_open(&destination, shard)?
        {
            return Ok(DfsStoragePreparation::Pending { processed_records: 0 });
        }
        result
    }

    fn current_generation_ready_before_open(
        &self,
        destination: &Path,
        shard: &DfsMetadataShardId,
    ) -> Result<bool> {
        // Current admission is generation-bound. Core owns the actual
        // activation witness and checks it again on ordinary open.
        let header = sinomo_dfs::inspect_metadata_storage_generation(destination)?;
        if header.identity.shard_id != *shard {
            return Err(error("current generation belongs to another physical scope"));
        }
        Ok(matches!(header.state, sinomo_dfs::DfsMetadataGenerationState::Active { .. }))
    }

    fn prepare_with_source_inner(
        &self,
        destination: &Path,
        source: &Path,
        shard: &DfsMetadataShardId,
        budget: usize,
        warm_session: &mut Option<Session>,
    ) -> Result<DfsStoragePreparation> {
        if budget == 0 {
            return Err(error("metadata conversion budget must be positive"));
        }
        let discovered = if shard_conversion_capture_requested(destination, shard)?
            && current_root(destination)?
        {
            Some(destination.to_owned())
        } else {
            discover_metadata_source(destination, source)?
        };
        let Some(source) = discovered else {
            return Ok(DfsStoragePreparation::Ready);
        };
        let authority = self.authority.as_ref().ok_or_else(|| {
            error("retained metadata conversion requires actual native import authority")
        })?;
        if warm_session.is_none() {
            *warm_session = Some(open_session_at_stage(
                destination,
                &source,
                conversion_stage_base_path(destination, shard)?,
            )?);
        }
        let session = warm_session
            .as_mut()
            .expect("initialized per-root conversion session");
        if session.source_root != source {
            session.inventory.verify(&source)?;
            register_source_fence(&source, &session._source_lock)?;
            session.source_root = source.clone();
        }
        if !session.current_import_policy_bound {
            // This is a NEW current-policy decoder context, not evidence of
            // historical source W/N. Original source rows stay byte-exact.
            let count = authority.source_policy_candidate_count(&session.source_root)?;
            if count == 0 {
                // The normal host may still be publishing its first sampled
                // advertisement. Retain this same source/controller without
                // binding fabricated eligibility or current policy defaults.
                return Ok(DfsStoragePreparation::Pending {
                    processed_records: 0,
                });
            }
            if session
                .progress
                .current_import_candidate_count
                .is_some_and(|old| old.clamp(1, 2) != count.clamp(1, 2))
            {
                return Err(error(
                    "current import policy defaults changed during immutable conversion",
                ));
            }
            session.projection.bind_current_import_policy(count)?;
            session.progress.current_import_candidate_count = Some(count);
            session.current_import_policy_bound = true;
            save_progress(session)?;
        }
        let mut processed = 0;
        // Restart reconstruction is bounded by the same request budget. Warm
        // sessions retain the ordinary current projection in memory.
        while processed < budget && session.replayed_events < session.progress.events.records {
            let mut reader =
                sinomo_core::storage::migration::stream::MigrationStreamReader::resume(
                    &session.staged_root.join("events.slog"),
                    &session.replay_cursor,
                )
                .map_err(error)?;
            if reader.header != event_header(session)? {
                return Err(error(
                    "durable conversion event stream changed its source header",
                ));
            }
            let bytes = reader
                .next_record(64 * 1024 * 1024)
                .map_err(error)?
                .ok_or_else(|| error("durable conversion event stream ended before its cursor"))?;
            let event: ConversionEvent =
                sinomo_core::object::serde_ext::from_bytes(&bytes).map_err(error)?;
            validate_replayed_event(session, &event)?;
            apply_event(session, &event)?;
            session.replay_cursor = reader.cursor().clone();
            session.replayed_events += 1;
            processed += 1;
        }
        checkpoint_journal_evidence(session)?;
        if session.replayed_events < session.progress.events.records {
            return Ok(DfsStoragePreparation::Pending {
                processed_records: processed,
            });
        }
        if !session.replay_validated {
            if !matches!(session.progress.phase, Phase::Source)
                && source::next(&session.source_root, &mut session.replay_source)?.is_some()
            {
                return Err(error("conversion event ledger omits actual source rows"));
            }
            if serde_json::to_value(session.progress.source.point()).map_err(error)?
                != serde_json::to_value(session.replay_source.point()).map_err(error)?
            {
                return Err(error(
                    "saved source cursor does not follow verified actual source events",
                ));
            }
            session.replay_validated = true;
        }
        // One lifecycle turn is one durable event batch. No source cursor is
        // acknowledged until its entire bounded stream prefix is synced below.
        // On error the warm session is discarded; restart replays only the
        // saved checkpoint and truncates this private ledger's unacknowledged tail.
        let mut event_batch = if matches!(session.progress.phase, Phase::Build) {
            None
        } else {
            Some(open_event_batch(session)?)
        };
        while processed < budget {
            match session.progress.phase {
                Phase::Source => {
                    let root = session.source_root.clone();
                    let mut cursor = std::mem::take(&mut session.progress.source);
                    let point = cursor.point();
                    if let Some(record) = source::next(&root, &mut cursor)? {
                        append_event(
                            session,
                            ConversionEvent::Source { record, point },
                            event_batch.as_mut().expect("source phase owns its batch"),
                        )?;
                        session.progress.source = cursor;
                        processed += 1;
                    } else {
                        session.progress.source = cursor;
                        checkpoint_journal_evidence(session)?;
                        validate_source_snapshot(&session)?;
                        session.progress.tail_after = session.materialized_through;
                        session.progress.phase = Phase::JournalTail;
                    }
                }
                Phase::JournalTail => {
                    let next = session
                        .journal_sequences
                        .range((
                            std::ops::Bound::Excluded(session.progress.tail_after),
                            std::ops::Bound::Unbounded,
                        ))
                        .next()
                        .copied();
                    if let Some(sequence) = next {
                        if sequence
                            != session
                                .progress
                                .tail_after
                                .checked_add(1)
                                .ok_or_else(|| error("source journal sequence overflow"))?
                        {
                            return Err(error(
                                "retained source journal has an uncovered sequence gap",
                            ));
                        }
                        let entry = read_verified_journal_evidence(
                            &session.staged_root,
                            sequence,
                            &session.journal_frame_digests,
                        )?;
                        append_event(
                            session,
                            ConversionEvent::Tail(entry),
                            event_batch.as_mut().expect("journal phase owns its batch"),
                        )?;
                        session.progress.tail_after = sequence;
                        processed += 1;
                    } else {
                        if session.source_head > session.progress.tail_after {
                            return Err(error("retained journal head names missing source data"));
                        }
                        session.inventory.verify(&session.source_root)?;
                        session.progress.phase = Phase::Build;
                    }
                }
                Phase::Build => break,
            }
        }
        if let Some(mut writer) = event_batch {
            checkpoint_event_batch(session, &mut writer)?;
        }
        if !matches!(session.progress.phase, Phase::Build) {
            return Ok(DfsStoragePreparation::Pending {
                processed_records: processed,
            });
        }
        if processed == budget {
            return Ok(DfsStoragePreparation::Pending {
                processed_records: processed,
            });
        }
        let ids = session.projection.source_file_system_ids()?;
        authority.validate_owned_source(&session.source_root, shard, &ids)?;
        let retained_cut_path = session.staged_root.join("source-cut.json");
        let captured_path = session.staged_root.join("captured-source-cut.json");
        let assignments = if captured_path.is_file() {
            let captured: CapturedSourceCut = read_json(&captured_path)?;
            captured.projection_assignments
        } else if retained_cut_path.is_file() {
            let retained: MetadataSourceCut = read_json(&retained_cut_path)?;
            if retained.source_identity != session.inventory.fingerprint {
                return Err(error("retained blueprint names another source cut"));
            }
            retained.projection_assignments
        } else {
            authority.assignments(&ids)?
        };
        if !assignments.iter().any(|cfg| cfg.shard_id == shard.0) {
            return Err(error(
                "conversion target is absent from the captured native assignments",
            ));
        }
        let original_handoff = if captured_path.is_file() {
            read_json::<CapturedSourceCut>(&captured_path)?.original_handoff
        } else if retained_cut_path.is_file() {
            read_json::<MetadataSourceCut>(&retained_cut_path)?.original_handoff
        } else {
            authority.original_handoff_reservation(shard, &ids)?
        };
        let actual_source_format = session
            .header
            .get("format_version")
            .map(|value| {
                let format = value
                    .as_u64()
                    .ok_or_else(|| error("actual source header format is not u64"))?;
                u16::try_from(format).map_err(error)
            })
            .transpose()?;
        let captured = CapturedSourceCut {
            source_identity: session.inventory.fingerprint.clone(),
            source_snapshot_fingerprint: session
                .inventory
                .snapshot_fingerprint()
                .unwrap_or(&session.inventory.fingerprint)
                .to_owned(),
            source_format_version: {
                let recorded = if captured_path.is_file() {
                    read_json::<CapturedSourceCut>(&captured_path)?.source_format_version
                } else if retained_cut_path.is_file() {
                    read_json::<MetadataSourceCut>(&retained_cut_path)?.source_format_version
                } else {
                    actual_source_format
                };
                // Older None remains hash-compatible; a recorded Some must be
                // the exact independently reread actual source header fact.
                if recorded.is_some() && recorded != actual_source_format {
                    return Err(error(
                        "captured source format differs from actual verified header",
                    ));
                }
                recorded
            },
            source_exact_prefix: session.source_prefix.clone(),
            source_journal_sequence: session.source_head.max(session.materialized_through),
            source_state_fingerprint: source_state_fingerprint(&session)?,
            file_system_ids: ids.clone(),
            projection_assignments: assignments.clone(),
            original_handoff: original_handoff.clone(),
        };
        if captured_path.is_file() {
            let retained: CapturedSourceCut = read_json(&captured_path)?;
            if retained != captured {
                return Err(error(
                    "source facts changed before target partition/materialization",
                ));
            }
        } else {
            write_json(&captured_path, &captured)?;
        }
        session.projection.bind_scope(shard.clone())?;
        let stage = session.staged_root.clone();
        let digests = &session.journal_frame_digests;
        session
            .projection
            .resolve_deferred(&|seq| read_verified_journal_document(&stage, seq, digests))?;
        if session.captured_unit.is_none() {
            session.captured_unit = Some(Arc::new(canonical_generation::capture_projected_source(session, &captured, shard)?));
        }
        Ok(DfsStoragePreparation::Pending { processed_records: processed })
    }
}
struct SourceCustody {
    fence: std::sync::Weak<File>,
}
static SOURCE_FENCES: std::sync::OnceLock<Mutex<BTreeMap<PathBuf, SourceCustody>>> =
    std::sync::OnceLock::new();
fn register_source_fence(root: &Path, file: &Arc<File>) -> Result<()> {
    let key = root.canonicalize().map_err(error)?;
    SOURCE_FENCES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry(key)
        .and_modify(|custody| custody.fence = Arc::downgrade(file))
        .or_insert_with(|| SourceCustody {
            fence: Arc::downgrade(file),
        });
    Ok(())
}
/// Register under `actual` the writer fence a session of this process holds
/// on the copy a capture recorded at `recorded`, when that copy has since
/// moved to `actual`: the held fence is `actual`'s own lock file. Without
/// such a fence nothing is registered, and the next retain takes a new one.
pub(crate) fn rebind_relocated_source_fence(recorded: &Path, actual: &Path) -> Result<()> {
    if recorded == actual {
        return Ok(());
    }
    let Ok(key) = recorded.canonicalize() else {
        return Ok(());
    };
    let held = SOURCE_FENCES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
        .and_then(|custody| custody.fence.upgrade());
    let Some(file) = held else {
        return Ok(());
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let lock = match fs::metadata(actual.join("journal/write.lock")) {
            Ok(lock) => lock,
            Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(cause) => return Err(error(cause)),
        };
        let held = file.metadata().map_err(error)?;
        if held.dev() == lock.dev() && held.ino() == lock.ino() {
            register_source_fence(actual, &file)?;
        }
    }
    Ok(())
}
/// Retain the actual converter writer fence across a self or peer stage RPC.
/// The registry carries only weak custody; it cannot manufacture ownership.
pub(crate) fn retain_source_fence(root: &Path) -> Result<Arc<File>> {
    let key = root.canonicalize().map_err(error)?;
    if let Some(file) = SOURCE_FENCES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
        .and_then(|custody| custody.fence.upgrade())
    {
        return Ok(file);
    }
    let path = root.join("journal/write.lock");
    fs::create_dir_all(
        path.parent()
            .ok_or_else(|| error("source writer fence has no parent"))?,
    )
    .map_err(error)?;
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(error)?;
    match file.try_lock_exclusive() {
        Ok(()) => {}
        Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => {
            #[cfg(test)]
            sinomo_dfs::test_support::record_metadata_operation_boundary(
                root,
                "migration-source-fence-contended",
                || serde_json::json!({"path":path,"canonical_root":key,"error":source.to_string()}),
            );
            return Err(MetadataBackendError::StoragePreparationPending {
                root: key,
                processed_records: 0,
            });
        }
        Err(source) => return Err(error(source)),
    }
    let file = Arc::new(file);
    register_source_fence(root, &file)?;
    Ok(file)
}
fn error(e: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(e.to_string())
}
fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    serde_json::from_slice(&fs::read(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })?)
    .map_err(error)
}
fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    write_json_checkpoint(path, value).map(|_| ())
}
fn write_json_checkpoint<T: Serialize>(path: &Path, value: &T) -> Result<String> {
    let bytes = serde_json::to_vec(value).map_err(error)?;
    sinomo_core::storage::durable_file::write_file_atomic(
        path,
        &bytes,
        sinomo_core::storage::durable_file::DurableAtomicFileOptions::new("import", "tmp")
            .sync_all(),
    )
    .map_err(error)?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}
/// Exact generation DATA for the two unchanged native ACK encodings. This
/// does not grant normal DFS admission or turn a source header into authority.
pub(crate) fn original_native_root_format(root: &Path) -> Result<Option<u16>> {
    if current_root(root)? {
        Ok(Some(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION))
    } else if metadata_root_at_format(root, 11)? {
        Ok(Some(11))
    } else if metadata_root_at_format(root, 10)? {
        Ok(Some(10))
    } else if metadata_root_at_format(root, 9)? {
        Ok(Some(9))
    } else {
        Ok(None)
    }
}
pub(crate) fn current_root(root: &Path) -> Result<bool> {
    let snapshot_present = root.join("records/tables/snapshot.sobj").exists();
    let snapshot_current = snapshot_present
        && metadata_root_at_format(root, sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION)?;
    // A normal new replica can materialize current journal/table frames before
    // its first checkpoint snapshot. Read the actual declaration; never try a
    // legacy decoder after a failed current reader or infer health from a path.
    let mut current = false;
    let mut legacy = false;
    for relative in [
        "records/tables/materialized.checkpoint.slog",
        "records/tables/materialized.slog",
    ] {
        let path = root.join(relative);
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(MetadataBackendError::Io { path, source }),
        };
        let length = file.metadata().map_err(error)?.len();
        let record = sinomo_core::storage::durable_file::read_length_prefixed_record(
            &mut file,
            &path,
            sinomo_core::storage::durable_file::DurableRecordReadOptions::new(
                sinomo_core::storage::durable_file::DurableLengthEndian::Little,
            ),
        )
        .map_err(error)?;
        let Some(record) = record else {
            if length != 0 && !snapshot_current {
                return Err(error("metadata table log declaration is incomplete"));
            }
            continue;
        };
        if record.bytes.len() < 6 || &record.bytes[..4] != sinomo_core::object::MAGIC {
            return Err(error("metadata table log declaration is invalid"));
        }
        if u16::from_le_bytes([record.bytes[4], record.bytes[5]])
            == sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION
        {
            current = true;
        } else {
            legacy = true;
        }
    }
    if current && legacy
        || snapshot_present && !snapshot_current && current
        || snapshot_current && legacy
    {
        return Err(error(
            "metadata snapshot/log roles declare different storage generations",
        ));
    }
    if !current && !snapshot_current {
        return Ok(false);
    }
    let first_prefix = sinomo_dfs::read_current_metadata_table_log_prefix(root)?;
    Ok(snapshot_current || first_prefix.is_some())
}
/// Exact LOCAL original snapshot DATA, distinct from an acknowledged floor.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceCheckpointCapture {
    pub(crate) source_root: PathBuf,
    pub(crate) source_identity: String,
    pub(crate) snapshot_fingerprint: String,
    pub(crate) source_format_version: u16,
    pub(crate) generation: Option<String>,
    pub(crate) semantic_state_fingerprint: Option<String>,
    pub(crate) anchor: source_ancestry::SourceCheckpointAnchor,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceCheckpointHeader {
    pub(crate) source_format_version: u16,
    pub(crate) generation: Option<String>,
    pub(crate) semantic_state_fingerprint: Option<String>,
    pub(crate) prefix_proof: DfsMetadataPrefixProof,
}
/// Qualify only the actual original header. Stream qualification reads the
/// bounded native header, not a source inventory or the snapshot row stream.
pub(crate) fn legacy_source_checkpoint_header(
    root: &Path,
) -> Result<Option<SourceCheckpointHeader>> {
    let path = root.join("records/tables/snapshot.sobj");
    if !path.is_file() {
        return Ok(None);
    }
    let stream = sinomo_core::storage::migration::stream::is_stream(&path).map_err(error)?;
    let mut header = serde_json::Map::new();
    if stream {
        let bytes = sinomo_core::storage::migration::stream::read_header(&path).map_err(error)?;
        header = source_document::decode_original_document(&bytes)?
            .as_object()
            .ok_or_else(|| error("original snapshot header is not an object"))?
            .clone();
    } else {
        let mut cursor = sobj_cursor::Cursor::open(&path)?;
        while let Some(item) = cursor.next(&path)? {
            if let sobj_cursor::Item::Header(key, value) = item {
                header.insert(key, value);
            }
        }
    }
    let Some(format @ (3 | 6)) = header.get("format_version").and_then(Value::as_u64) else {
        return Ok(None);
    };
    let sequence = header
        .get("materialized_through_sequence")
        .and_then(Value::as_u64)
        .ok_or_else(|| error("original shard checkpoint omits its materialized sequence"))?;
    let proof: DfsMetadataPrefixProof = serde_json::from_value(
        header
            .get("prefix_proof")
            .cloned()
            .ok_or_else(|| error("original shard checkpoint omits its actual prefix"))?,
    )
    .map_err(error)?;
    source_proof::validate(&proof)?;
    if proof.version != 2 || proof.journal_sequence != sequence {
        return Err(error(
            "original shard checkpoint changes its exact native clock",
        ));
    }
    let generation = header
        .get("generation")
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_str()
                .filter(|value| {
                    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
                .map(str::to_owned)
                .ok_or_else(|| error("original shard checkpoint has an invalid generation"))
        })
        .transpose()?;
    let semantic_state_fingerprint = header
        .get("semantic_state_fingerprint")
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| error("original shard checkpoint has an invalid semantic checksum"))
        })
        .transpose()?;
    if format == 3 {
        if !stream {
            return Err(error(
                "original format3 checkpoint lacks its released stream framing",
            ));
        }
        let expected = blake3::hash(
            &serde_json::to_vec(&(3u16, Some(sequence), Some(&proof))).map_err(error)?,
        )
        .to_hex()
        .to_string();
        if generation.as_deref() != Some(expected.as_str()) {
            return Err(error(
                "original format3 checkpoint changes its released generation binding",
            ));
        }
    } else if generation.is_none() && semantic_state_fingerprint.is_none() {
        return Err(error(
            "original format6 checkpoint omits its actual source binding",
        ));
    }
    if !stream {
        let expected = semantic_state_fingerprint
            .as_ref()
            .ok_or_else(|| error("original plain checkpoint omits its semantic checksum"))?;
        if wire_json::snapshot_fingerprint(&path)? != *expected {
            return Err(error(
                "original plain checkpoint changes its semantic checksum",
            ));
        }
    }
    Ok(Some(SourceCheckpointHeader {
        source_format_version: format as u16,
        generation,
        semantic_state_fingerprint,
        prefix_proof: proof,
    }))
}
/// Capture full LOCAL checkpoint custody only for an actual uncovered
/// height-only obligation. The caller retains the physical source fence.
pub(crate) fn capture_legacy_source_checkpoint(
    root: &Path,
    shard: &DfsMetadataShardId,
) -> Result<Option<SourceCheckpointCapture>> {
    let Some(header) = legacy_source_checkpoint_header(root)? else {
        return Ok(None);
    };
    let snapshot = Path::new("records/tables/snapshot.sobj");
    let inventory = source::discover(root)?;
    let mut selected = inventory.clone();
    selected.files.retain(|file| file.relative == snapshot);
    let mut cursor = source::SourceCursor::new(selected);
    while source::next(root, &mut cursor)?.is_some() {}
    if legacy_source_checkpoint_header(root)?.as_ref() != Some(&header) {
        return Err(error(
            "original checkpoint header changed during full capture",
        ));
    }
    inventory.verify(root)?;
    let snapshot_fingerprint = inventory
        .snapshot_fingerprint()
        .ok_or_else(|| error("original shard checkpoint has no exact snapshot fingerprint"))?
        .to_owned();
    Ok(Some(SourceCheckpointCapture {
        source_root: root.to_owned(),
        source_identity: inventory.fingerprint,
        snapshot_fingerprint,
        source_format_version: header.source_format_version,
        generation: header.generation,
        semantic_state_fingerprint: header.semantic_state_fingerprint,
        anchor: source_ancestry::SourceCheckpointAnchor {
            shard_id: shard.clone(),
            prefix_proof: header.prefix_proof,
        },
    }))
}
pub(crate) fn metadata_root_at_format(root: &Path, format: u16) -> Result<bool> {
    let path = root.join("records/tables/snapshot.sobj");
    if !path.exists() {
        return Ok(false);
    }
    if !sinomo_core::storage::migration::stream::is_stream(&path).map_err(error)? {
        return Ok(false);
    }
    let bytes = sinomo_core::storage::migration::stream::read_header(&path).map_err(error)?;
    let header: Value = sinomo_core::object::serde_ext::from_bytes(&bytes).map_err(error)?;
    if header.get("format_version").and_then(Value::as_u64) != Some(u64::from(format)) {
        return Ok(false);
    }
    let proof: DfsMetadataPrefixProof = serde_json::from_value(
        header
            .get("prefix_proof")
            .cloned()
            .ok_or_else(|| error("current metadata header has no prefix"))?,
    )
    .map_err(error)?;
    source_proof::validate(&proof)?;
    if proof.version != 2
        || header
            .get("materialized_through_sequence")
            .and_then(Value::as_u64)
            != Some(proof.journal_sequence)
        || header
            .get("semantic_state_fingerprint")
            .and_then(Value::as_str)
            .is_none()
    {
        return Err(error("invalid current metadata checkpoint header"));
    }
    // Normal DFS performs the full native framing/table validation at mount.
    Ok(true)
}
/// Source discovery never treats a retained combined root as an empty shard.
/// A completed import records where its still-retained original image lives,
/// allowing other logical shards to convert on demand after root activation.
pub fn discover_metadata_source(
    destination: &Path,
    default_source: &Path,
) -> Result<Option<PathBuf>> {
    if current_root(destination)? {
        return Ok(None);
    }
    if source::has_migration_source_data(destination)? {
        return Ok(Some(destination.to_owned()));
    }
    let imported = default_source.join("metadata-import-v7.json");
    if imported.is_file() {
        let record: Value = read_json(&imported)?;
        let retained = record
            .get("retained_source_root")
            .and_then(Value::as_str)
            .ok_or_else(|| error("current import has no retained source identity"))?;
        let retained = PathBuf::from(retained);
        let parent = default_source
            .parent()
            .ok_or_else(|| error("metadata source has no owning parent"))?
            .canonicalize()
            .map_err(error)?;
        let actual = retained.canonicalize().map_err(error)?;
        if !actual.starts_with(&parent) || actual == destination {
            return Err(error(
                "retained metadata source escapes the owned storage root",
            ));
        }
        return Ok(Some(actual));
    }
    if !default_source.exists() || current_root(default_source)? {
        return Ok(None);
    }
    if source::has_migration_source_data(default_source)? {
        return Ok(Some(default_source.to_owned()));
    }
    Ok(None)
}
#[cfg(test)]
fn open_session(destination: &Path, source: &Path, shard: &DfsMetadataShardId) -> Result<Session> {
    open_session_at_stage(destination, source, conversion_stage_base_path(destination, shard)?)
}
fn open_session_at_stage(
    destination: &Path,
    source: &Path,
    staged_root: PathBuf,
) -> Result<Session> {
    let mut session = open_session_shell(destination, source, staged_root)?;
    let progress_path = session.staged_root.join("progress.json");
    if progress_path.exists() {
        let progress: Progress = read_json(&progress_path)?;
        if progress.source.inventory.as_ref() != Some(&session.inventory) {
            return Err(error("resumed conversion names a different source cut"));
        }
        session.progress = progress;
    }
    save_progress(&session)?;
    Ok(session)
}
/// Fresh fenced context for a source replay.
fn open_session_shell(
    destination: &Path,
    source: &Path,
    staged_root: PathBuf,
) -> Result<Session> {
    let lock = retain_source_fence(source)?;
    let inventory = source::discover(source)?;
    fs::create_dir_all(&staged_root).map_err(error)?;
    let progress = Progress {
        source: source::SourceCursor::new(inventory.clone()),
        events: Default::default(),
        phase: Phase::Source,
        tail_after: 0,
        current_import_candidate_count: None,
    };
    let replay_source = source::SourceCursor::new(inventory.clone());
    Ok(Session {
        captured_unit: None,
        _source_lock: lock,
        source_root: source.to_owned(),
        destination_root: destination.to_owned(),
        staged_root,
        inventory,
        progress,
        replay_cursor: Default::default(),
        replayed_events: 0,
        replay_source,
        replay_tail_after: None,
        replay_validated: false,
        current_import_policy_bound: false,
        projection: Default::default(),
        header: Default::default(),
        materialized_through: 0,
        source_head: 0,
        source_prefix: None,
        journal_sequences: Default::default(),
        journal_frame_digests: BTreeMap::new(),
        pending_journal_evidence: BTreeMap::new(),
        journal_evidence_dirty: false,
    })
}
fn unversioned_conversion_stage_base_path(
    destination: &Path,
    shard: &DfsMetadataShardId,
) -> Result<PathBuf> {
    let parent = destination
        .parent()
        .ok_or_else(|| error("metadata destination has no parent"))?;
    let key = blake3::hash(&serde_json::to_vec(&(destination, shard)).map_err(error)?)
        .to_hex()
        .to_string();
    Ok(parent.join(".metadata-conversion").join(key))
}
fn conversion_stage_base_path(destination: &Path, shard: &DfsMetadataShardId) -> Result<PathBuf> {
    let base = unversioned_conversion_stage_base_path(destination, shard)?;
    // Completed source replay/candidate receipts belong to their target schema.
    // Preserve them while a new generation imports the actual live source root.
    Ok(base.join(format!(
        "target-format-{}",
        sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION
    )))
}

/// Existing filesystem ancestry supplies one physical slot key even before
/// the destination directory exists. No data directory is created here.
fn canonical_destination(destination: &Path) -> Result<PathBuf> {
    let absolute = if destination.is_absolute() {
        destination.to_owned()
    } else {
        std::env::current_dir().map_err(error)?.join(destination)
    };
    let mut ancestor = absolute;
    let mut missing = Vec::new();
    loop {
        match fs::metadata(&ancestor) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => return Err(error("metadata destination ancestry is not a directory")),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                if fs::symlink_metadata(&ancestor).is_ok() {
                    return Err(error(
                        "metadata destination contains an unresolved symbolic link",
                    ));
                }
                let name = ancestor
                    .file_name()
                    .ok_or_else(|| {
                        error("metadata destination has no resolvable existing ancestor")
                    })?
                    .to_owned();
                missing.push(name);
                if !ancestor.pop() {
                    return Err(error("metadata destination escapes its existing ancestry"));
                }
            }
            Err(source) => {
                return Err(MetadataBackendError::Io {
                    path: ancestor,
                    source,
                });
            }
        }
    }
    let mut resolved = ancestor.canonicalize().map_err(error)?;
    for name in missing.into_iter().rev() {
        resolved.push(name);
    }
    Ok(resolved)
}
fn save_progress(session: &Session) -> Result<()> {
    write_json(
        &session.staged_root.join("progress.json"),
        &session.progress,
    )
}
fn event_header(session: &Session) -> Result<Vec<u8>> {
    #[derive(Serialize)]
    struct Header<'a> {
        schema_version: u16,
        source_fingerprint: &'a str,
        destination_root: &'a Path,
    }
    sinomo_core::object::serde_ext::to_bytes(
        &Header {
            schema_version: 7,
            source_fingerprint: &session.inventory.fingerprint,
            destination_root: &session.destination_root,
        },
        7,
        sinomo_core::object::LengthMode::U32,
    )
    .map_err(error)
}
fn open_event_batch(
    session: &mut Session,
) -> Result<sinomo_core::storage::migration::stream::MigrationStreamWriter> {
    if session.replayed_events != session.progress.events.records {
        return Err(error(
            "source batch cannot skip retained event reconstruction",
        ));
    }
    let writer = sinomo_core::storage::migration::stream::MigrationStreamWriter::resume(
        &session.staged_root.join("events.slog"),
        &event_header(session)?,
        &session.progress.events,
    )
    .map_err(error)?;
    // A new stream has a real header cursor even when this turn finds no rows.
    session.progress.events = writer.cursor().clone();
    session.replay_cursor = writer.cursor().clone();
    Ok(writer)
}
fn append_event(
    session: &mut Session,
    event: ConversionEvent,
    writer: &mut sinomo_core::storage::migration::stream::MigrationStreamWriter,
) -> Result<()> {
    let bytes =
        sinomo_core::object::serde_ext::to_bytes(&event, 7, sinomo_core::object::LengthMode::U32)
            .map_err(error)?;
    writer.append(&bytes).map_err(error)?;
    apply_event(session, &event)?;
    // This cursor is still private to the owned call. checkpoint_event_batch
    // syncs its exact bytes before save_progress makes it restart-visible.
    session.progress.events = writer.cursor().clone();
    session.replay_cursor = writer.cursor().clone();
    session.replayed_events += 1;
    Ok(())
}
fn checkpoint_event_batch(
    session: &mut Session,
    writer: &mut sinomo_core::storage::migration::stream::MigrationStreamWriter,
) -> Result<()> {
    checkpoint_journal_evidence(session)?;
    let durable = writer.checkpoint().map_err(error)?;
    if durable != session.progress.events {
        return Err(error(
            "event batch differs from its exact source progress cursor",
        ));
    }
    save_progress(session)
}

fn validate_replayed_event(session: &mut Session, event: &ConversionEvent) -> Result<()> {
    match event {
        ConversionEvent::Source { point, record } => {
            if session.replay_tail_after.is_some()
                || serde_json::to_value(point.point()).map_err(error)?
                    != serde_json::to_value(session.replay_source.point()).map_err(error)?
            {
                return Err(error(
                    "staged source event changes actual decoder order or position",
                ));
            }
            let actual = source::next(&session.source_root, &mut session.replay_source)?
                .ok_or_else(|| {
                    error("staged source event extends beyond the actual immutable source")
                })?;
            if serde_json::to_value(&actual).map_err(error)?
                != serde_json::to_value(record).map_err(error)?
            {
                return Err(error(
                    "staged source event differs from independently decoded source bytes",
                ));
            }
        }
        ConversionEvent::Tail(entry) => {
            if session.replay_tail_after.is_none()
                && source::next(&session.source_root, &mut session.replay_source)?.is_some()
            {
                return Err(error(
                    "staged journal replay begins before complete actual source discovery",
                ));
            }
            let after = session
                .replay_tail_after
                .unwrap_or(session.materialized_through);
            let sequence = entry
                .get("sequence")
                .and_then(Value::as_u64)
                .ok_or_else(|| error("staged journal replay has no actual sequence"))?;
            if after.checked_add(1) != Some(sequence)
                || read_verified_journal_evidence(
                    &session.staged_root,
                    sequence,
                    &session.journal_frame_digests,
                )? != *entry
            {
                return Err(error(
                    "staged journal replay changes its exact source frame or contiguous cut",
                ));
            }
            session.replay_tail_after = Some(sequence);
        }
    }
    Ok(())
}
fn evidence_path(stage: &Path, sequence: u64) -> PathBuf {
    stage
        .join("journal-evidence")
        .join(format!("{sequence:020}.sobj"))
}
fn read_verified_journal_evidence(
    stage: &Path,
    sequence: u64,
    digests: &BTreeMap<u64, String>,
) -> Result<Value> {
    read_verified_journal_document(stage, sequence, digests).map(|(entry, _)| entry)
}
pub(crate) fn read_verified_journal_document(
    stage: &Path,
    sequence: u64,
    digests: &BTreeMap<u64, String>,
) -> Result<(Value, source_document::SourceRawDocument)> {
    let bytes = fs::read(evidence_path(stage, sequence)).map_err(error)?;
    if digests
        .get(&sequence)
        .is_none_or(|expected| *expected != blake3::hash(&bytes).to_hex().as_str())
    {
        return Err(error(
            "retained staged journal bytes are not the actual captured source frame",
        ));
    }
    let entry = source_document::decode_original_document(&bytes)?;
    Ok((entry, source_document::SourceRawDocument::row(bytes)))
}
fn store_journal_evidence(session: &mut Session, entry: &Value, bytes: &[u8]) -> Result<()> {
    let sequence = entry
        .get("sequence")
        .and_then(Value::as_u64)
        .ok_or_else(|| error("retained journal frame has no sequence"))?;
    let path = evidence_path(&session.staged_root, sequence);
    if let Some((pending, _)) = session.pending_journal_evidence.get(&sequence) {
        if fs::read(pending).map_err(error)? != bytes {
            return Err(error(
                "retained journal has conflicting exact frames at the same slot",
            ));
        }
    } else if path.exists() {
        if fs::read(&path).map_err(error)? != bytes {
            return Err(error(
                "retained journal has conflicting exact frames at the same slot",
            ));
        }
    } else {
        fs::create_dir_all(
            path.parent()
                .ok_or_else(|| error("journal evidence has no parent"))?,
        )
        .map_err(error)?;
        // Unpublished bytes are not evidence. A cancelled/failed turn can leave
        // this uniquely named private file behind; replay never reads it.
        let pending = path.with_extension(format!("{}.pending", sinomo_core::Id::generate()));
        let mut file = sinomo_core::storage::durable_file::create_private_staged_file(&pending)
            .map_err(error)?;
        sinomo_core::storage::io_trace::write_all(&mut file, bytes).map_err(error)?;
        session
            .pending_journal_evidence
            .insert(sequence, (pending, file));
    }
    session.journal_evidence_dirty = true;
    session.journal_sequences.insert(sequence);
    let digest = blake3::hash(bytes).to_hex().to_string();
    if session
        .journal_frame_digests
        .insert(sequence, digest.clone())
        .is_some_and(|old| old != digest)
    {
        return Err(error("actual source journal forks a retained frame digest"));
    }
    session.source_head = session.source_head.max(sequence);
    Ok(())
}
fn checkpoint_journal_evidence(session: &mut Session) -> Result<()> {
    if !session.journal_evidence_dirty {
        return Ok(());
    }
    let mut files = Vec::with_capacity(session.pending_journal_evidence.len());
    for (sequence, (path, file)) in std::mem::take(&mut session.pending_journal_evidence) {
        let synced = sinomo_core::storage::durable_file::sync_private_staged_file(&path, file)
            .map_err(error)?;
        files.push((synced, evidence_path(&session.staged_root, sequence)));
    }
    if files.is_empty() {
        // A previous interrupted batch can have published synced files before
        // its directory barrier failed. Exact bytes alone do not prove names
        // durable, so replay also synchronizes reused canonical names.
        sinomo_core::storage::durable_file::sync_directory(
            &session.staged_root.join("journal-evidence"),
        )
        .map_err(error)?;
    } else {
        sinomo_core::storage::durable_file::install_synced_private_staged_files(&files)
            .map_err(error)?;
    }
    // The journal-evidence directory itself may have been created by this turn.
    sinomo_core::storage::durable_file::sync_directory(&session.staged_root).map_err(error)?;
    session.journal_evidence_dirty = false;
    Ok(())
}
fn apply_event(session: &mut Session, event: &ConversionEvent) -> Result<()> {
    // Projection of a table batch/tail can consume earlier journal documents.
    // Their canonical names must already identify synced exact source bytes.
    if matches!(
        event,
        ConversionEvent::Tail(_)
            | ConversionEvent::Source {
                record: source::SourceRecord::TableBatch { .. },
                ..
            }
    ) {
        checkpoint_journal_evidence(session)?;
    }
    match event {
        ConversionEvent::Source {
            record: source::SourceRecord::SnapshotHeader(value),
            ..
        } => {
            for (key, value) in value
                .as_object()
                .ok_or_else(|| error("retained snapshot header is not a record"))?
            {
                session.header.insert(key.clone(), value.clone());
            }
            if let Some(sequence) = session
                .header
                .get("materialized_through_sequence")
                .and_then(Value::as_u64)
            {
                session.materialized_through = sequence;
                session.source_head = session.source_head.max(sequence);
            }
            if let Some(value) = session.header.get("prefix_proof").filter(|v| !v.is_null()) {
                let proof = serde_json::from_value::<DfsMetadataPrefixProof>(value.clone())
                    .map_err(error)?;
                source_proof::validate(&proof)?;
                if proof.journal_sequence != session.materialized_through {
                    return Err(error(
                        "retained snapshot proof differs from its materialization cut",
                    ));
                }
                session.source_prefix = Some(proof);
                source_ancestry::store_anchor(
                    &session.staged_root,
                    session.source_prefix.as_ref().expect("captured proof"),
                )?;
            }
        }
        ConversionEvent::Source {
            record:
                source::SourceRecord::SnapshotRow {
                    table,
                    row,
                    raw_document,
                },
            ..
        } => session.projection.push_snapshot_row_from_source(
            table,
            row.clone(),
            &raw_document.context()?,
        )?,
        ConversionEvent::Source {
            record:
                source::SourceRecord::LooseRow {
                    family,
                    row,
                    raw_document,
                },
            ..
        } => {
            let family = match family.as_str() {
                "filesystems" => "file_systems",
                "filesystem-objects" => "file_system_objects",
                "visible-commits" => "visible_commits",
                "namespace-transactions" => "namespace_transactions",
                other => other,
            };
            session.projection.push_snapshot_row_from_source(
                &family.replace('-', "_"),
                row.clone(),
                &raw_document.context()?,
            )?;
        }
        ConversionEvent::Source {
            record: source::SourceRecord::JournalFrame { entry, exact_bytes },
            ..
        } => store_journal_evidence(session, entry, exact_bytes)?,
        ConversionEvent::Source {
            record:
                source::SourceRecord::TableBatch {
                    batch,
                    raw_document,
                },
            ..
        } => {
            let sequence = batch
                .get("sequence")
                .and_then(Value::as_u64)
                .ok_or_else(|| error("retained table batch has no sequence"))?;
            let prefix = batch
                .get("prefix_proof")
                .filter(|v| !v.is_null())
                .map(|value| {
                    serde_json::from_value::<DfsMetadataPrefixProof>(value.clone()).map_err(error)
                })
                .transpose()?;
            if let Some(proof) = &prefix {
                source_proof::validate(proof)?;
                if proof.journal_sequence != sequence {
                    return Err(error("retained table batch proof differs from its cut"));
                }
                source_ancestry::store_anchor(&session.staged_root, proof)?;
            }
            let ops = batch
                .get("ops")
                .and_then(Value::as_array)
                .ok_or_else(|| error("retained table batch has no operation list"))?;
            if sequence > session.materialized_through {
                let stage = session.staged_root.clone();
                let digests = &session.journal_frame_digests;
                let source_context = raw_document.context()?;
                for (index, op) in ops.iter().enumerate() {
                    session.projection.apply_table_op_from_source(
                        op.clone(),
                        &source_context,
                        &format!("/ops/{index}"),
                        &|seq| read_verified_journal_document(&stage, seq, digests),
                    )?;
                }
                session.materialized_through = sequence;
                session.source_head = session.source_head.max(sequence);
                session.source_prefix = prefix;
            }
        }
        ConversionEvent::Source {
            record: source::SourceRecord::Evidence { relative, value },
            ..
        } => {
            let directory = session.staged_root.join("source-evidence");
            fs::create_dir_all(&directory).map_err(error)?;
            let key = blake3::hash(relative.to_string_lossy().as_bytes())
                .to_hex()
                .to_string();
            write_json(&directory.join(format!("{key}.json")), &(relative, value))?;
        }
        ConversionEvent::Tail(entry) => {
            let sequence = entry
                .get("sequence")
                .and_then(Value::as_u64)
                .ok_or_else(|| error("retained tail has no sequence"))?;
            let (recorded_entry, raw_document) = read_verified_journal_document(
                &session.staged_root,
                sequence,
                &session.journal_frame_digests,
            )?;
            if recorded_entry != *entry {
                return Err(error(
                    "retained tail differs from its original verified source document",
                ));
            }
            session
                .projection
                .apply_journal_from_source(recorded_entry, &raw_document.context()?)?;
            if let Some(proof) = session.source_prefix.as_ref() {
                let bytes =
                    fs::read(evidence_path(&session.staged_root, sequence)).map_err(error)?;
                let anchor = source_ancestry::retained_anchor(&session.staged_root, sequence)?;
                let extended = source_proof::extend_as(
                    proof,
                    sequence,
                    &bytes,
                    anchor
                        .as_ref()
                        .map_or(proof.version, |anchor| anchor.version),
                )?;
                if anchor.as_ref().is_some_and(|anchor| anchor != &extended) {
                    return Err(error("retained source tail forks its actual prefix anchor"));
                }
                session.source_prefix = Some(extended);
            }
            session.source_head = session.source_head.max(sequence);
        }
    }
    Ok(())
}

fn validate_source_snapshot(session: &Session) -> Result<()> {
    if let Some(expected) = session
        .header
        .get("semantic_state_fingerprint")
        .and_then(Value::as_str)
    {
        let path = session.source_root.join("records/tables/snapshot.sobj");
        if !sinomo_core::storage::migration::stream::is_stream(&path).map_err(error)?
            && wire_json::snapshot_fingerprint(&path)? != expected
        {
            return Err(error("retained checkpoint semantic checksum mismatch"));
        }
    }
    Ok(())
}
fn source_state_fingerprint(session: &Session) -> Result<String> {
    if let Some(prefix) = &session.source_prefix {
        return Ok(prefix.digest.clone());
    }
    if let Some(fingerprint) = session
        .header
        .get("semantic_state_fingerprint")
        .and_then(Value::as_str)
    {
        return Ok(fingerprint.to_owned());
    }
    // Old proof-less sources bind the complete captured image; this remains
    // converter evidence until real native joint activation certifies it.
    Ok(session.inventory.fingerprint.clone())
}
/// Independently decoded immutable source DATA; it grants no authority.
/// An expected identity is checked before verifying the same inventory, exactly
/// as in ordinary source validation. Inspection never creates a source grant.
pub(crate) fn read_retained_source_identity(
    actual_root: &Path,
    expected: Option<(&str, &str)>,
) -> Result<(String, String)> {
    let inventory = source::discover(actual_root)?;
    let snapshot = inventory
        .snapshot_fingerprint()
        .unwrap_or(&inventory.fingerprint);
    if expected.is_some_and(|(identity, expected_snapshot)| {
        inventory.fingerprint != identity || snapshot != expected_snapshot
    }) {
        return Err(error(
            "actual retained source differs from its immutable captured identity",
        ));
    }
    inventory.verify(actual_root)?;
    Ok((inventory.fingerprint.clone(), snapshot.to_owned()))
}
pub(crate) fn validate_retained_source_identity(
    actual_root: &Path,
    expected_source_identity: &str,
    expected_snapshot_fingerprint: &str,
) -> Result<()> {
    read_retained_source_identity(
        actual_root,
        Some((expected_source_identity, expected_snapshot_fingerprint)),
    )
    .map(|_| ())
}

#[cfg(test)]
mod current_layout_data_tests {
    use super::*;

    fn copy_data(from: &Path, into: &Path) {
        fs::create_dir_all(into).unwrap();
        for entry in fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = into.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_data(&entry.path(), &target);
            } else {
                assert!(entry.file_type().unwrap().is_file());
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    #[test]
    #[ignore = "requires actual saved current bootstrap metadata tree"]
    fn actual_current_log_only_bootstrap_is_current_and_preserves_invalid_data() {
        let saved = PathBuf::from(
            std::env::var("SINOMO_DFS_CURRENT_BOOTSTRAP_METADATA_ROOT")
                .expect("genuine saved current root/services/users DATA required"),
        );
        let copied = sinomo_testdir::TestDir::new("current-log-bootstrap-data");
        for scope in ["root", "services", "users"] {
            let root = copied.join(scope);
            copy_data(&saved.join(scope), &root);
            assert!(!root.join("records/tables/snapshot.sobj").exists());
            assert!(current_root(&root).unwrap());
            assert!(discover_metadata_source(&root, &root).unwrap().is_none());
            let expected = sinomo_dfs::read_current_metadata_table_log_prefix(&root)
                .unwrap()
                .expect("actual complete native9 log prefix");
            let normal = sinomo_dfs::LocalSobjMetadataBackend::open_for_inspection(
                DfsMetadataShardId::new(scope),
                root.clone(),
            )
            .unwrap();
            assert!(
                normal.export_table_snapshot().unwrap().materialized_through_sequence
                    >= expected.journal_sequence
            );
            drop(normal);

            let active = root.join("records/tables/materialized.slog");
            let checkpoint = root.join("records/tables/materialized.checkpoint.slog");
            let original = fs::read(&active).unwrap();
            fs::rename(&active, &checkpoint).unwrap();
            fs::write(&active, []).unwrap();
            assert!(
                current_root(&root).unwrap(),
                "checkpoint-only role remains current"
            );
            assert_eq!(
                sinomo_dfs::read_current_metadata_table_log_prefix(&root).unwrap(),
                Some(expected)
            );
            fs::rename(&checkpoint, &active).unwrap();

            fs::write(&active, &original[..9]).unwrap();
            assert!(
                current_root(&root).is_err(),
                "incomplete first declaration cannot become an empty legacy source"
            );
            assert_eq!(fs::read(&active).unwrap(), original[..9]);
            let mut corrupt = original.clone();
            // The actual first batch begins after its four-byte record length.
            corrupt[4] ^= 1;
            fs::write(&active, &corrupt).unwrap();
            assert!(current_root(&root).is_err());
            assert_eq!(fs::read(&active).unwrap(), corrupt);
            let mut legacy = original.clone();
            legacy[8..10].copy_from_slice(&8_u16.to_le_bytes());
            fs::write(&active, &legacy).unwrap();
            assert!(
                !current_root(&root).unwrap(),
                "declared old data stays in migration only"
            );
            assert!(sinomo_dfs::read_current_metadata_table_log_prefix(&root).is_err());
            assert_eq!(fs::read(&active).unwrap(), legacy);
            fs::write(&active, &original).unwrap();
            let mut torn_suffix = original.clone();
            torn_suffix.extend_from_slice(&[1, 0]);
            fs::write(&active, &torn_suffix).unwrap();
            assert!(current_root(&root).unwrap());
            assert_eq!(fs::read(&active).unwrap(), torn_suffix);
            fs::write(&active, &original).unwrap();
            // Classification reads only the declaration. A later repeated
            // complete actual batch must still fail normal full replay.
            let first_len = u32::from_le_bytes(original[..4].try_into().unwrap()) as usize;
            let mut repeated = original.clone();
            repeated.extend_from_slice(&original[..4 + first_len]);
            fs::write(&active, &repeated).unwrap();
            assert!(current_root(&root).unwrap());
            assert!(
                sinomo_dfs::LocalSobjMetadataBackend::open_for_inspection(
                    DfsMetadataShardId::new(scope),
                    &root
                )
                .is_err()
            );
            assert_eq!(fs::read(&active).unwrap(), repeated);
            fs::write(&active, &original).unwrap();
        }
        // Obtain zero cursor bytes from the ordinary current backend, using
        // the existing shared executor. Copy only that real bookkeeping DATA;
        // the destination never receives a fabricated prefix or publication.
        let name = "current-empty-cursor-layout";
        let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, 1);
        let fixture =
            sinomo_dfs::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
        let handle = fixture.runtime_handle();
        let _entered = handle.enter();
        let initialized = copied.join("initialized/users");
        let backend = sinomo_dfs::LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("users"),
            &initialized,
        )
        .unwrap();
        let cursor =
            fs::read(initialized.join("records/local-materialization/cursor.sobj")).unwrap();
        let actual: Value = sinomo_core::object::serde_ext::from_bytes(&cursor).unwrap();
        assert_eq!(actual["last_sequence"], serde_json::json!(0));
        assert_eq!(actual["shard_id"], serde_json::json!("users"));
        drop(backend);
        let native_snapshot = initialized.join("records/tables/snapshot.sobj");
        let snapshot_bytes = fs::read(&native_snapshot).unwrap();
        let mixed = copied.join("mixed/users");
        fs::create_dir_all(mixed.join("records/tables")).unwrap();
        let mixed_snapshot = mixed.join("records/tables/snapshot.sobj");
        fs::write(&mixed_snapshot, &snapshot_bytes).unwrap();
        let mixed_log = mixed.join("records/tables/materialized.slog");
        let mut declared_old_log =
            fs::read(saved.join("users/records/tables/materialized.slog")).unwrap();
        declared_old_log[8..10].copy_from_slice(&8_u16.to_le_bytes());
        fs::write(&mixed_log, &declared_old_log).unwrap();
        assert!(
            current_root(&mixed).is_err(),
            "current snapshot cannot hide an old declared log"
        );
        assert_eq!(fs::read(&mixed_snapshot).unwrap(), snapshot_bytes);
        assert_eq!(fs::read(&mixed_log).unwrap(), declared_old_log);
        // A negative declaration mutation of a real current snapshot cannot
        // admit its unchanged current log as an original legacy source.
        let header_bytes =
            sinomo_core::storage::migration::stream::read_header(&native_snapshot).unwrap();
        let mut header: Value = sinomo_core::object::serde_ext::from_bytes(&header_bytes).unwrap();
        header["format_version"] = serde_json::json!(8);
        let old_header = sinomo_core::object::serde_ext::to_bytes(
            &header,
            8,
            sinomo_core::object::LengthMode::U32,
        )
        .unwrap();
        fs::remove_file(&mixed_snapshot).unwrap();
        drop(
            sinomo_core::storage::migration::stream::MigrationStreamWriter::resume(
                &mixed_snapshot,
                &old_header,
                &Default::default(),
            )
            .unwrap(),
        );
        let declared_old_snapshot = fs::read(&mixed_snapshot).unwrap();
        fs::copy(
            saved.join("users/records/tables/materialized.slog"),
            &mixed_log,
        )
        .unwrap();
        assert!(
            current_root(&mixed).is_err(),
            "old snapshot cannot hide a current declared log"
        );
        assert_eq!(fs::read(&mixed_snapshot).unwrap(), declared_old_snapshot);
        let foreign_current = copied.join("initialized/foreign");
        copy_data(&saved.join("users"), &foreign_current);
        let actual_log =
            fs::read(foreign_current.join("records/tables/materialized.slog")).unwrap();
        assert!(
            sinomo_dfs::LocalSobjMetadataBackend::new(
                DfsMetadataShardId::new("foreign"),
                &foreign_current
            )
            .is_err()
        );
        assert_eq!(
            fs::read(foreign_current.join("records/tables/materialized.slog")).unwrap(),
            actual_log
        );
        drop(_entered);
        fixture.close().unwrap();
        let empty = copied.join("bookkeeping/users");
        let cursor_path = empty.join("records/local-materialization/cursor.sobj");
        fs::create_dir_all(cursor_path.parent().unwrap()).unwrap();
        fs::write(&cursor_path, &cursor).unwrap();
        assert!(!source::has_migration_source_data(&empty).unwrap());
        assert!(original_unit_absent(&empty, &DfsMetadataShardId::new("users")).unwrap());
        assert!(discover_metadata_source(&empty, &empty).unwrap().is_none());
        assert_eq!(fs::read(&cursor_path).unwrap(), cursor);
        let foreign = copied.join("bookkeeping/foreign");
        copy_data(&empty, &foreign);
        assert!(source::has_migration_source_data(&foreign).unwrap());
        assert!(!original_unit_absent(&foreign, &DfsMetadataShardId::new("foreign")).unwrap());
        assert_eq!(
            discover_metadata_source(&foreign, &foreign).unwrap(),
            Some(foreign)
        );
        let advanced =
            fs::read(saved.join("users/records/local-materialization/cursor.sobj")).unwrap();
        let actual: Value = sinomo_core::object::serde_ext::from_bytes(&advanced).unwrap();
        assert!(actual["last_sequence"].as_u64().unwrap() > 0);
        fs::write(&cursor_path, &advanced).unwrap();
        assert!(!original_unit_absent(&empty, &DfsMetadataShardId::new("users")).unwrap());
        assert!(source::has_migration_source_data(&empty).unwrap());
        fs::write(&cursor_path, &cursor).unwrap();
        let ack = "coordination/metadata-prefix-ack-floors/users.sobj";
        let ack_path = empty.join(ack);
        fs::create_dir_all(ack_path.parent().unwrap()).unwrap();
        fs::copy(saved.join("users").join(ack), &ack_path).unwrap();
        assert!(source::has_migration_source_data(&empty).unwrap());
        assert_eq!(
            fs::read(ack_path).unwrap(),
            fs::read(saved.join("users").join(ack)).unwrap()
        );
    }
}

#[cfg(test)]
mod recovery_session_tests {
    use super::*;

    #[test]
    fn source_batch_restarts_from_only_its_acknowledged_checkpoint() {
        use sinomo_core::object::{LengthMode, serde_ext};
        use sinomo_core::storage::migration::stream::MigrationStreamReader;
        let dir = sinomo_testdir::TestDir::new("metadata-event-batch-checkpoint");
        let source = dir.join("source");
        let path = source.join("records/tables/snapshot.sobj");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        // A source-format DATA fixture only; this test neither certifies an
        // image nor grants native authority or readiness.
        let original = serde_ext::to_bytes(
            &serde_json::json!({"format_version":2,"tables":{}}),
            1,
            LengthMode::U32,
        )
        .unwrap();
        fs::write(&path, &original).unwrap();
        let destination = dir.join("destination");
        let shard = DfsMetadataShardId::new("root");
        let mut session = open_session(&destination, &source, &shard).unwrap();
        let progress_path = session.staged_root.join("progress.json");
        let before = fs::read(&progress_path).unwrap();
        let mut cursor = session.progress.source.clone();
        let point = cursor.point();
        let record = source::next(&source, &mut cursor).unwrap().unwrap();
        let event = ConversionEvent::Source { point, record };
        let bytes = serde_ext::to_bytes(&event, 7, LengthMode::U32).unwrap();
        let mut writer = open_event_batch(&mut session).unwrap();
        append_event(&mut session, event, &mut writer).unwrap();
        session.progress.source = cursor;
        writer.checkpoint().unwrap(); // Crash after event sync, before cursor publication.
        assert_eq!(fs::read(&progress_path).unwrap(), before);
        drop(writer);
        drop(session);

        let mut resumed = open_session(&destination, &source, &shard).unwrap();
        assert_eq!(resumed.progress.events.records, 0);
        let mut writer = open_event_batch(&mut resumed).unwrap();
        assert_eq!(writer.cursor().records, 0);
        let mut cursor = resumed.progress.source.clone();
        let point = cursor.point();
        let record = source::next(&source, &mut cursor).unwrap().unwrap();
        let event = ConversionEvent::Source { point, record };
        assert_eq!(
            serde_ext::to_bytes(&event, 7, LengthMode::U32).unwrap(),
            bytes
        );
        append_event(&mut resumed, event, &mut writer).unwrap();
        resumed.progress.source = cursor;
        checkpoint_event_batch(&mut resumed, &mut writer).unwrap();
        let expected = resumed.progress.events.clone();
        let ledger = resumed.staged_root.join("events.slog");
        drop(writer);
        drop(resumed);
        let recovered = open_session(&destination, &source, &shard).unwrap();
        assert_eq!(recovered.progress.events, expected);
        let mut reader = MigrationStreamReader::open(&ledger).unwrap();
        assert_eq!(reader.next_record(64 * 1024 * 1024).unwrap(), Some(bytes));
        assert_eq!(reader.cursor(), &expected);
        // This is an open conversion ledger, not a finished snapshot stream.
        // Its saved cursor is the boundary; EOF must not fabricate a finish seal.
        assert!(matches!(reader.next_record(64 * 1024 * 1024),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof));
        assert_eq!(fs::read(&path).unwrap(), original);
        assert!(!destination.join("metadata-import-v7.json").exists());
    }

    #[test]
    fn empty_source_batch_acknowledges_only_its_exact_header_cursor() {
        let dir = sinomo_testdir::TestDir::new("metadata-empty-event-batch");
        let source = dir.join("source");
        fs::create_dir(&source).unwrap();
        let destination = dir.join("destination");
        let shard = DfsMetadataShardId::new("root");
        let mut session = open_session(&destination, &source, &shard).unwrap();
        let mut writer = open_event_batch(&mut session).unwrap();
        let expected = writer.cursor().clone();
        assert_eq!(expected.records, 0);
        assert!(expected.offset > 0 && !expected.digest.is_empty());
        checkpoint_event_batch(&mut session, &mut writer).unwrap();
        drop(writer);
        drop(session);
        let mut recovered = open_session(&destination, &source, &shard).unwrap();
        assert_eq!(recovered.progress.events, expected);
        assert_eq!(
            open_event_batch(&mut recovered).unwrap().cursor(),
            &expected
        );
        assert!(
            !destination.exists(),
            "a header checkpoint is not publication"
        );
    }

    #[test]
    fn journal_evidence_batch_restart_preserves_exact_bytes_and_checkpoint_order() {
        let dir = sinomo_testdir::TestDir::new("metadata-journal-evidence-batch");
        let source = dir.join("source");
        fs::create_dir(&source).unwrap();
        let destination = dir.join("destination");
        let shard = DfsMetadataShardId::new("root");
        let mut session = open_session(&destination, &source, &shard).unwrap();
        let progress = session.staged_root.join("progress.json");
        let before = fs::read(&progress).unwrap();
        // Storage-only exact-byte inputs; these neither represent a native
        // authority proof nor pass the source decoder or certify an image.
        let row = serde_json::json!({"sequence":1});
        store_journal_evidence(&mut session, &row, b"first exact frame").unwrap();
        let abandoned = session.pending_journal_evidence[&1].0.clone();
        let canonical = evidence_path(&session.staged_root, 1);
        assert!(!canonical.exists());
        assert!(store_journal_evidence(&mut session, &row, b"conflicting frame").is_err());
        assert_eq!(fs::read(&abandoned).unwrap(), b"first exact frame");
        assert_eq!(fs::read(&progress).unwrap(), before);
        drop(session); // Cancellation before file sync/publication.

        let mut resumed = open_session(&destination, &source, &shard).unwrap();
        assert!(resumed.pending_journal_evidence.is_empty());
        store_journal_evidence(&mut resumed, &row, b"first exact frame").unwrap();
        assert_ne!(resumed.pending_journal_evidence[&1].0, abandoned);
        checkpoint_journal_evidence(&mut resumed).unwrap();
        assert!(resumed.pending_journal_evidence.is_empty());
        assert!(!resumed.journal_evidence_dirty);
        assert_eq!(fs::read(&canonical).unwrap(), b"first exact frame");
        assert_eq!(fs::read(&progress).unwrap(), before);
        drop(resumed); // Crash after evidence publication, before event checkpoint.

        let mut resumed = open_session(&destination, &source, &shard).unwrap();
        assert_eq!(resumed.progress.events.records, 0);
        store_journal_evidence(&mut resumed, &row, b"first exact frame").unwrap();
        assert!(resumed.pending_journal_evidence.is_empty());
        assert!(
            resumed.journal_evidence_dirty,
            "replay must sync reused names too"
        );
        checkpoint_journal_evidence(&mut resumed).unwrap();
        assert!(store_journal_evidence(&mut resumed, &row, b"conflicting frame").is_err());
        assert_eq!(fs::read(&canonical).unwrap(), b"first exact frame");
        assert_eq!(fs::read(&abandoned).unwrap(), b"first exact frame");
        assert_eq!(fs::read(&progress).unwrap(), before);
        assert!(!destination.exists());
    }

    #[test]
    fn prior_storage_generation_is_discovered_without_current_admission() {
        use sinomo_core::object::{LengthMode, serde_ext};
        use sinomo_core::storage::migration::stream::MigrationStreamWriter;
        let dir = sinomo_testdir::TestDir::new("metadata-prior-generation-discovery");
        let root = dir.join("root");
        let path = root.join("records/tables/snapshot.sobj");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        // A structural negative input: no prefix/rows/authority are fabricated.
        // Generation 8 is rejected before the current header fields are decoded.
        let header =
            serde_ext::to_bytes(&serde_json::json!({"format_version":8}), 8, LengthMode::U32)
                .unwrap();
        drop(MigrationStreamWriter::resume(&path, &header, &Default::default()).unwrap());
        let original = fs::read(&path).unwrap();
        assert_eq!(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION, 12);
        assert!(!current_root(&root).unwrap());
        assert_eq!(discover_metadata_source(&root, &root).unwrap(), Some(root));
        assert_eq!(fs::read(path).unwrap(), original);
    }

    #[test]
    fn actual_source_writer_lock_contention_is_typed_pending_without_capture() {
        let root = sinomo_testdir::TestDir::new("metadata-actual-source-writer-busy");
        let source = root.path().join("source");
        fs::create_dir_all(source.join("journal")).unwrap();
        let lock_path = source.join("journal/write.lock");
        let actual_writer = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        actual_writer.lock_exclusive().unwrap();
        // Model the unpublished file role of the normal atomic head producer.
        // Its contents carry no source or readiness authority.
        let unpublished = source.join(format!("journal/.head.sobj.tmp-{}-0", std::process::id()));
        fs::write(&unpublished, b"unpublished current head").unwrap();
        assert!(matches!(source::has_migration_source_data(&source),
            Err(MetadataBackendError::StoragePreparationPending { root: pending_root, processed_records: 0 })
            if pending_root == source.canonicalize().unwrap()));
        assert_eq!(fs::read(&unpublished).unwrap(), b"unpublished current head");
        let result = retain_source_fence(&source);
        assert!(matches!(result,
            Err(MetadataBackendError::StoragePreparationPending { root: pending_root, processed_records: 0 })
            if pending_root == source.canonicalize().unwrap()));
        drop(actual_writer);
        let captured = retain_source_fence(&source).unwrap();
        let competitor = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        assert_eq!(
            competitor.try_lock_exclusive().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        drop(captured);
        competitor.try_lock_exclusive().unwrap();
        drop(competitor);
        // An unlocked stale temporary name is still original DATA. Unknown
        // files remain complete inventory inputs, never silently discarded.
        assert!(source::has_migration_source_data(&source).unwrap());
        assert_eq!(fs::read(&unpublished).unwrap(), b"unpublished current head");
        let unknown = source.join("unknown-original.sobj");
        fs::write(&unknown, b"retained unknown original bytes").unwrap();
        assert!(source::has_migration_source_data(&source).unwrap());
        assert_eq!(
            fs::read(&unknown).unwrap(),
            b"retained unknown original bytes"
        );
        // The real native genesis owner establishes the current declaration.
        let (mut native, nodes) = sinomo_dfs::test_support::DfsTestRuntime::start_cluster(
            "metadata-writer-current-control", 1,
        ).unwrap();
        let current = sinomo_dfs::local_dfs_metadata_shard_path(
            native.data_dir(&nodes[0]), &DfsMetadataShardId::new("root"),
        );
        // This control classifies the native bytes. Drain ordinary background
        // writers before taking the source inspection fence exercised above.
        native.stop_node(&nodes[0]).unwrap();
        let backend =
            sinomo_dfs::LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &current)
                .unwrap();
        drop(backend);
        assert!(current_root(&current).unwrap());
        assert!(!source::has_migration_source_data(&current).unwrap());
        native.close().unwrap();
    }

    #[test]
    fn source_writer_open_error_is_not_reported_as_pending() {
        let root = sinomo_testdir::TestDir::new("metadata-source-writer-nonlock-error");
        let source = root.path().join("source");
        fs::create_dir_all(source.join("journal/write.lock")).unwrap();
        let result = retain_source_fence(&source);
        assert!(
            matches!(result, Err(MetadataBackendError::Serde(_))),
            "an actual open error stays an error, never a lock/recovery retry grant"
        );
    }

    #[test]
    fn busy_preparation_session_returns_pending_without_waiting_or_releasing_custody() {
        let root = sinomo_testdir::TestDir::new("metadata-conversion-busy-session");
        let source = root.path().join("source");
        fs::create_dir_all(&source).unwrap();
        let destination = canonical_destination(&root.path().join("destination")).unwrap();
        let shard = DfsMetadataShardId::new("current");
        let slot = Arc::new(RootSlot {
            shard: shard.clone(),
            session: Mutex::new(Some(open_session(&destination, &source, &shard).unwrap())),
        });
        let converter = Arc::new(MetadataConverter::default());
        converter
            .roots
            .lock()
            .unwrap()
            .insert(destination.clone(), Arc::clone(&slot));
        let custody = slot.session.lock().unwrap();
        let source_handle = Arc::as_ptr(&custody.as_ref().unwrap()._source_lock);
        let cursor = serde_json::to_value(&custody.as_ref().unwrap().progress).unwrap();
        let checkpoint_path = custody.as_ref().unwrap().staged_root.join("progress.json");
        let checkpoint = fs::read(&checkpoint_path).unwrap();
        let competing = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(source.join("journal/write.lock"))
            .unwrap();
        assert!(competing.try_lock_exclusive().is_err());
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let queued = Arc::clone(&converter);
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            result_tx
                .send(queued.prepare_source(&destination, &source, &shard, 256))
                .unwrap();
        });
        started_rx.recv().unwrap();
        let result = result_rx.recv_timeout(std::time::Duration::from_secs(1));
        assert!(
            custody.is_some(),
            "contention cannot discard the active source cursor"
        );
        assert!(
            competing.try_lock_exclusive().is_err(),
            "source custody must remain held"
        );
        // Drain the worker even on the old blocking implementation before asserting.
        drop(custody);
        worker.join().unwrap();
        assert!(
            matches!(
                result,
                Ok(Ok(DfsStoragePreparation::Pending {
                    processed_records: 0
                }))
            ),
            "a competing owner must receive honest no-progress Pending, not wait: {result:?}"
        );
        let retained = slot.session.lock().unwrap();
        let retained = retained
            .as_ref()
            .expect("the original owned session is retained");
        assert_eq!(Arc::as_ptr(&retained._source_lock), source_handle);
        assert_eq!(serde_json::to_value(&retained.progress).unwrap(), cursor);
        assert_eq!(fs::read(&checkpoint_path).unwrap(), checkpoint);
        assert!(competing.try_lock_exclusive().is_err());
    }

    #[test]
    fn poisoned_projection_is_discarded_before_releasing_source_custody() {
        let root = sinomo_testdir::TestDir::new("metadata-conversion-poisoned-session");
        let source = root.path().join("source");
        fs::create_dir_all(&source).unwrap();
        let destination = root.path().join("destination");
        let shard = DfsMetadataShardId::new("current");
        let slot = Arc::new(RootSlot {
            shard: shard.clone(),
            session: Mutex::new(Some(open_session(&destination, &source, &shard).unwrap())),
        });
        let queued = Arc::clone(&slot);
        assert!(
            std::thread::spawn(move || {
                let mut state = queued.try_lock_session().unwrap();
                state.as_mut().unwrap().replayed_events = 11;
                panic!("unwind after partial scratch projection");
            })
            .join()
            .is_err()
        );
        let lock_path = source.join("journal/write.lock");
        let competing = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        assert!(
            competing.try_lock_exclusive().is_err(),
            "source custody must survive until the poisoned state is discarded"
        );
        let state = slot.try_lock_session().unwrap();
        assert!(
            state.is_none(),
            "a queued caller must not receive the partial projection"
        );
        assert!(!slot.session.is_poisoned());
        competing.try_lock_exclusive().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn concurrent_directory_aliases_share_one_stable_slot() {
        let root = sinomo_testdir::TestDir::new("metadata-conversion-alias-slot");
        let (native, nodes) = sinomo_dfs::test_support::DfsTestRuntime::start_cluster(
            "metadata-conversion-current-alias-slot", 1,
        ).unwrap();
        let shard = DfsMetadataShardId::new("root");
        let destination = sinomo_dfs::local_dfs_metadata_shard_path(
            native.data_dir(&nodes[0]), &shard,
        );
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(destination.parent().unwrap(), &alias).unwrap();
        let aliased = alias.join("root");
        let physical = canonical_destination(&destination).unwrap();
        assert_eq!(physical, canonical_destination(&aliased).unwrap());
        let converter = Arc::new(MetadataConverter::default());
        // Actual native genesis needs no conversion. Preparation establishes
        // the slot without fabricating source data or a Ready callback.
        assert_eq!(
            converter.prepare_source(&destination, &destination, &shard, 1).unwrap(),
            DfsStoragePreparation::Ready
        );
        let slot = Arc::clone(converter.roots.lock().unwrap().get(&physical).unwrap());
        let owner = slot.session.lock().unwrap();
        let queued = Arc::clone(&converter);
        let queued_alias = aliased.clone();
        let queued_shard = shard.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            tx.send(queued.prepare_source(&queued_alias, &queued_alias, &queued_shard, 1))
                .unwrap();
        });
        let waiting = rx.recv_timeout(std::time::Duration::from_secs(1));
        // Always drain the old blocking implementation before failing a test.
        drop(owner);
        worker.join().unwrap();
        assert!(
            matches!(
                waiting,
                Ok(Ok(DfsStoragePreparation::Pending {
                    processed_records: 0
                }))
            ),
            "an alias must yield without claiming progress while the owner is active: {waiting:?}"
        );
        for path in [&aliased, &destination] {
            assert_eq!(
                converter.prepare_source(path, path, &shard, 1).unwrap(),
                DfsStoragePreparation::Ready,
                "the same bounded lifecycle must finish after the owner releases its turn"
            );
            let slots = converter.roots.lock().unwrap();
            assert_eq!(slots.len(), 1);
            assert!(Arc::ptr_eq(slots.get(&physical).unwrap(), &slot));
        }
        assert!(slot.session.lock().unwrap().is_none());
        drop(slot);
        drop(converter);
        native.close().unwrap();
    }
}

#[cfg(test)]
mod native_source_fenced_inspection_tests {
    use super::*;

    #[test]
    fn current_prefix_ancestry_reads_actual_frames_while_its_source_writer_is_retained() {
        use sinomo_dfs::test_support::DfsTestRuntime;
        let name = "native-source-fenced-prefix-inspection";
        let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
        let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
        native.start_nodes().unwrap();
        let mut client = native.client(&node).unwrap();
        let shard = DfsMetadataShardId::new("services");
        let root = native.data_dir(&node).dfs_dir().join("metadata/services");
        let physical_prefix = || {
            let current = sinomo_dfs::LocalSobjMetadataBackend::open_for_inspection(
                shard.clone(),
                root.clone(),
            )
            .unwrap();
            sinomo_dfs::DfsMetadataBackend::metadata_prefix_proof(&current, &shard)
                .unwrap()
                .unwrap()
        };
        client
            .posix_write_file(
                "/services/source-fenced-inspection",
                b"first",
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        let anchor = physical_prefix();
        client
            .posix_write_file(
                "/services/source-fenced-inspection",
                b"second",
                sinomo_dfs::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        let current = physical_prefix();
        assert!(current.journal_sequence > anchor.journal_sequence);
        // SAME converter primitive, actual file and Main-generated frame chain.
        // The inspection must not try to acquire this OS writer a second time.
        let fence = retain_source_fence(&root).unwrap();
        assert_eq!(
            sinomo_dfs::inspect_metadata_prefix_descendant(&root, &shard, &anchor).unwrap(),
            Some(current.clone()),
        );
        let mut fork = anchor.clone();
        fork.digest = format!(
            "dfs-metadata-prefix-v2:blake3:{}",
            blake3::hash(b"foreign anchor").to_hex()
        );
        assert!(
            sinomo_dfs::inspect_metadata_prefix_descendant(&root, &shard, &fork)
                .unwrap()
                .is_none()
        );
        assert!(
            !matches!(
                sinomo_dfs::inspect_metadata_prefix_descendant(
                    &root,
                    &DfsMetadataShardId::new("foreign-shard"),
                    &anchor,
                ),
                Ok(Some(_))
            ),
            "a foreign shard cannot establish current ancestry"
        );
        let segments = root.join("journal/segments");
        let segment = std::fs::read_dir(&segments)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("slog"))
            .expect("actual normal journal segment");
        let hidden = segment.with_extension("source-test-held");
        std::fs::rename(&segment, &hidden).unwrap();
        let missing = sinomo_dfs::inspect_metadata_prefix_descendant(&root, &shard, &anchor);
        std::fs::rename(&hidden, &segment).unwrap();
        assert!(
            !matches!(missing, Ok(Some(_))),
            "missing actual suffix bytes cannot prove ancestry"
        );
        assert_eq!(
            sinomo_dfs::inspect_metadata_prefix_descendant(&root, &shard, &anchor).unwrap(),
            Some(current),
        );
        drop(fence);
        drop(client);
        native.close().unwrap();
    }
}

/// Write an immutable conversion checkpoint once; a later write must carry
/// the same bytes.
pub(crate) fn write_immutable_checkpoint(path: &Path, value: &impl Serialize) -> Result<()> {
    if path.exists() {
        if fs::read(path).map_err(error)? != serde_json::to_vec(value).map_err(error)? {
            return Err(error("immutable conversion checkpoint changed"));
        }
        return Ok(());
    }
    write_json_checkpoint(path, value).map(|_| ())
}

/// Intent naming a shard whose own copy its per-shard conversion captures.
const SHARD_CONVERSION_CAPTURE: &str = "shard-conversion-capture.json";

impl MetadataConverter {
    /// Capture `shard`'s local copy for its per-shard conversion, on the
    /// shard's primary. The same converter session replays the source under
    /// its writer fence and keeps the fence while the session is warm; the
    /// capture intent makes a current-format copy a source as well.
    ///
    /// `reservations` is the shard's share of the original placement goals,
    /// which the capture binds; it cannot change once the source is captured.
    ///
    /// `source` is the copy the capture reads: the shard's own copy at
    /// `destination`, or the legacy combined `root` copy that holds the
    /// shard's rows ([`combined_root`]). A capture of the combined copy, and
    /// every capture of `root`, keeps only the shard's rows.
    pub(crate) fn capture_shard_conversion_source(
        &self, destination: &Path, source: &Path, shard: &DfsMetadataShardId, file_system_id: &DfsFileSystemId,
        reservations: &[RetainedPlacementReservation], budget: usize,
    ) -> Result<(DfsStoragePreparation, Option<Arc<canonical_generation::CapturedMetadataUnit>>)> {
        let authority = self.authority.as_ref().ok_or_else(|| error("shard conversion requires its actual import owner"))?;
        let destination = canonical_destination(destination)?;
        let source = canonical_destination(source)?;
        authority.validate_owned_source(&source, shard, std::slice::from_ref(file_system_id))?;
        let stage = conversion_stage_base_path(&destination, shard)?;
        fs::create_dir_all(&stage).map_err(error)?;
        validate_placement_reservations(reservations, std::slice::from_ref(file_system_id), shard)?;
        let evidence = stage.join("placement-reservations.json");
        let captured = conversion_stage_base_path(&destination, shard)?.join("captured-unit.json");
        if captured.exists() {
            let original: canonical_generation::CapturedMetadataUnit = read_json(&captured)?;
            original.validate()?;
            if original.source.retained_placement_reservations != reservations {
                return Err(error("placement evidence cannot change after source capture"));
            }
        }
        if !evidence.exists() && !reservations.is_empty()
            && stage.join("captured-source-cut.json").exists() {
            return Err(error("new placement evidence cannot attach to retained original source facts"));
        }
        write_immutable_checkpoint(&evidence, &reservations)?;
        write_immutable_checkpoint(&stage.join(SHARD_CONVERSION_CAPTURE), file_system_id)?;
        if combined_root::capture_keeps_only_own_rows(&source, &destination, shard) {
            combined_root::record_combined_source_capture(&destination, shard, file_system_id)?;
        }
        self.capture_metadata_unit_progress(&destination, &source, shard, budget)
    }

    /// The placement evidence a capture of `destination` recorded, if any.
    pub(crate) fn recorded_placement_evidence(
        &self, destination: &Path, shard: &DfsMetadataShardId,
    ) -> Result<Option<Vec<RetainedPlacementReservation>>> {
        let destination = canonical_destination(destination)?;
        let path = conversion_stage_base_path(&destination, shard)?.join("placement-reservations.json");
        if !path.try_exists().map_err(error)? {
            return Ok(None);
        }
        read_placement_capture(&destination, shard).map(Some)
    }

    /// End the capture of `destination` once its shard's storage is Active:
    /// drop the warm session with its source fence and remove the capture
    /// intent, so the activated image is never read as a new source. The
    /// captured evidence stays in the conversion stage.
    pub(crate) fn finish_shard_conversion_capture(&self, destination: &Path, shard: &DfsMetadataShardId) -> Result<()> {
        let destination = canonical_destination(destination)?;
        let slot = self.roots.lock().map_err(error)?.get(&destination).cloned();
        if let Some(slot) = slot {
            let mut session = slot.try_lock_session().ok_or_else(|| MetadataBackendError::StoragePreparationPending {
                root: destination.clone(), processed_records: 0,
            })?;
            session.take();
        }
        sinomo_core::storage::durable_file::remove_file_if_exists_and_sync_parent(
            &conversion_stage_base_path(&destination, shard)?.join(SHARD_CONVERSION_CAPTURE),
        ).map(|_| ()).map_err(error)
    }
}

fn read_placement_capture(destination: &Path, shard: &DfsMetadataShardId) -> Result<Vec<RetainedPlacementReservation>> {
    let path = conversion_stage_base_path(destination, shard)?.join("placement-reservations.json");
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(error),
        Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(cause) => Err(MetadataBackendError::Io { path, source: cause }),
    }
}

/// Whether a per-shard conversion's capture intent names `destination`.
fn shard_conversion_capture_requested(destination: &Path, shard: &DfsMetadataShardId) -> Result<bool> {
    Ok(conversion_stage_base_path(destination, shard)?.join(SHARD_CONVERSION_CAPTURE).is_file())
}

/// Where the converter records its capture of the local copy at
/// `destination`: the session stage's `captured-unit.json`. The file is
/// written once, when the bounded source replay completes, and is not
/// rewritten afterwards.
pub(crate) fn shard_capture_path(destination: &Path, shard: &DfsMetadataShardId) -> Result<PathBuf> {
    let destination = canonical_destination(destination)?;
    Ok(conversion_stage_base_path(&destination, shard)?.join("captured-unit.json"))
}

#[cfg(test)]
mod count_native_frame_tests {
    use super::*;
    #[test]
    fn count_replay_rejects_changed_captured_native_frame_without_touching_original() {
        let directory = sinomo_testdir::TestDir::new("count-native-frame-provenance");
        let source_root = directory.join("source");
        let stage = directory.join("stage");
        let shard = DfsMetadataShardId::new("root");
        let writer = sinomo_dfs::DfsMetadataCandidateWriter::open_generation(
            &source_root,
            sinomo_dfs::DfsMetadataGenerationIdentity {
                file_system_id: sinomo_dfs::sifs_file_system_id(),
                shard_id: shard.clone(),
                generation: "original-native-frame".into(),
                origin: sinomo_dfs::DfsMetadataGenerationOrigin::StorageTransition {
                    transition_id: "original-frame-data".into(),
                },
            },
        ).unwrap();
        let prefix = source_proof::conversion_base(0, "original-frame", "empty-current-data");
        writer.write(Default::default(), prefix.clone()).unwrap();
        let payload = sinomo_dfs::DfsPreparedOperationPayload::for_metadata_effects(
            sinomo_dfs::DfsPreparedOperationScope { file_system_id: sinomo_dfs::sifs_file_system_id(),
                shard_id: shard, owner_epoch: 1, operation_id: "original-native-frame".into() },
            Vec::new(), Vec::new(), vec![sinomo_dfs::DfsPreparedMetadataEffect::PutFileSystem {
                file_system: sinomo_dfs::DfsFileSystem::sifs(sinomo_core::node::NodeId::from_legacy_named("original-frame-owner").unwrap()),
            }], time::OffsetDateTime::UNIX_EPOCH,
        ).unwrap();
        let (record, prefix) = writer.prepare(payload, &prefix).unwrap();
        writer.commit(record, &prefix, time::OffsetDateTime::UNIX_EPOCH).unwrap();
        drop(writer);
        let inventory = source::discover(&source_root).unwrap();
        let mut cursor = source::SourceCursor::new(inventory.clone());
        let mut frames = 0;
        while let Some(record) = source::next(&source_root, &mut cursor).unwrap() {
            let source::SourceRecord::JournalFrame { entry, exact_bytes } = record else { continue; };
            let sequence = entry["sequence"].as_u64().unwrap();
            let path = evidence_path(&stage, sequence);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, &exact_bytes).unwrap();
            let digests = BTreeMap::from([(sequence, blake3::hash(&exact_bytes).to_hex().to_string())]);
            assert_eq!(read_verified_journal_document(&stage, sequence, &digests).unwrap().0, entry);
            let mut changed = exact_bytes.clone();
            let last = changed.len() - 1; changed[last] ^= 1;
            fs::write(&path, changed).unwrap();
            assert!(read_verified_journal_document(&stage, sequence, &digests).is_err());
            inventory.verify(&source_root).unwrap();
            frames += 1;
        }
        assert_eq!(frames, 2, "real Prepare and Commit raw frames must both be checked");
    }
}

/// Bounded, read-only presence classification for the owner dispatcher. It
/// neither takes a source fence nor hashes/replays DATA. False means the known
/// metadata trees are empty; an exhausted probe is conservatively retained DATA.
pub(crate) fn original_source_present(root: &Path) -> Result<bool> {
    for name in ["metadata-import-v7.json", "metadata-candidate-v7.json"] {
        match fs::symlink_metadata(root.join(name)) {
            Ok(_) => return Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
            Err(source) => return Err(MetadataBackendError::Io { path: root.join(name), source }),
        }
    }
    let mut pending = vec![(root.join("records"), 0usize), (root.join("journal"), 0usize)];
    let mut budget = 64usize;
    while let Some((directory, depth)) = pending.pop() {
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(MetadataBackendError::Io { path: directory, source }),
        };
        for entry in entries {
            if budget == 0 { return Ok(true); }
            budget -= 1;
            let entry = entry.map_err(error)?;
            let path = entry.path();
            let kind = entry.file_type().map_err(error)?;
            if kind.is_dir() {
                if depth >= 4 { return Ok(true); }
                pending.push((path, depth + 1));
            } else if entry.file_name() != "write.lock" {
                // Even malformed or zero-length persisted records are retained
                // evidence, not permission to invent a fresh native generation.
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// True only for an empty physical unit with no original scope checkpoint.
/// This is not Ready or birth authority: the caller may report ReplicaUnreadable
/// so the core's existing authenticated snapshot repair can own the missing copy.
pub(crate) fn original_unit_absent(root: &Path, shard: &DfsMetadataShardId) -> Result<bool> {
    let canonical = canonical_destination(root)?;
    let root = canonical.as_path();
    fn present(path: &Path) -> Result<bool> {
        match fs::symlink_metadata(path) {
            Ok(_) => Ok(true),
            Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(source) => Err(MetadataBackendError::Io { path: path.to_owned(), source }),
        }
    }
    if present(&root.join("metadata-generation.sobj"))?
        || present(&unversioned_conversion_stage_base_path(root, shard)?)? {
        return Ok(false);
    }
    // Older combined roots can own an absent split scope. Do not classify that
    // source as empty merely because its later physical directory is missing,
    // whether the combined copy is still live or was retained beside the
    // image a per-shard conversion of `root` installed.
    let metadata_root = root.parent().ok_or_else(|| error("metadata unit has no parent"))?;
    let combined = metadata_root.join("root");
    if combined != root && original_source_present(&combined)?
        && sinomo_dfs::inspect_metadata_storage_generation(&combined).is_err() {
        return Ok(false);
    }
    if let Some(retained) = combined_root::legacy_combined_root(metadata_root, shard)?
        && original_source_present(&retained)? {
        return Ok(false);
    }
    let mut has_bookkeeping = false;
    let mut directories = vec![(root.to_owned(), 0usize)];
    let mut remaining = 64usize;
    while let Some((directory, depth)) = directories.pop() {
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(MetadataBackendError::Io { path: directory, source }),
        };
        for entry in entries {
            if remaining == 0 { return Ok(false); }
            remaining -= 1;
            let entry = entry.map_err(error)?;
            let path = entry.path();
            let kind = entry.file_type().map_err(error)?;
            if kind.is_dir() {
                if directory == root && !matches!(entry.file_name().to_str(), Some("records" | "journal")) {
                    return Ok(false);
                }
                if depth >= 5 { return Ok(false); }
                directories.push((path, depth + 1));
            } else if kind.is_file() && (path == root.join("records/local-materialization/cursor.sobj")
                || path == root.join("journal/head.sobj")) {
                has_bookkeeping = true;
            } else if path != root.join("journal/write.lock") || !kind.is_file()
                || entry.metadata().map_err(error)?.len() != 0 {
                return Ok(false);
            }
        }
    }
    if has_bookkeeping {
        // Older repair openers wrote empty hints before receiving an image.
        // Use the migration decoder's fenced source classification: a foreign,
        // advanced or malformed cursor must stay with its original owner.
        return Ok(root.file_name().and_then(|name| name.to_str()) == Some(shard.0.as_str())
            && matches!(source::has_migration_source_data(root), Ok(false))
            && !present(&root.join("metadata-generation.sobj"))?);
    }
    Ok(true)
}

#[cfg(test)]
mod original_unit_absence_tests {
    use super::*;
    #[test]
    fn missing_replica_is_distinct_from_retained_or_ambiguous_source() {
        let directory = sinomo_testdir::TestDir::new("original-metadata-unit-absence");
        let root = directory.join("metadata/users");
        let shard = DfsMetadataShardId::new("users");
        assert!(original_unit_absent(&root, &shard).unwrap());
        fs::create_dir_all(root.join("journal/segments")).unwrap();
        fs::write(root.join("journal/write.lock"), b"").unwrap();
        assert!(original_unit_absent(&root, &shard).unwrap());
        let cursor = root.join("records/local-materialization/cursor.sobj");
        fs::create_dir_all(cursor.parent().unwrap()).unwrap();
        for (name, sequence, absent) in [("users", 0, true), ("other", 0, false), ("users", 1, false)] {
            let bytes = sinomo_core::object::serde_ext::to_bytes(
                &serde_json::json!({"shard_id": name, "last_sequence": sequence}),
                1, sinomo_core::object::LengthMode::U32,
            ).unwrap();
            fs::write(&cursor, &bytes).unwrap();
            assert_eq!(original_unit_absent(&root, &shard).unwrap(), absent);
            assert_eq!(fs::read(&cursor).unwrap(), bytes);
        }
        fs::write(&cursor, b"unreadable").unwrap();
        assert!(!original_unit_absent(&root, &shard).unwrap());
        fs::remove_file(&cursor).unwrap();
        let head = root.join("journal/head.sobj");
        for (next_sequence, absent) in [(1, true), (2, false)] {
            let bytes = sinomo_core::object::serde_ext::to_bytes(
                &serde_json::json!({"next_sequence": next_sequence}), 1, sinomo_core::object::LengthMode::U32,
            ).unwrap();
            fs::write(&head, &bytes).unwrap();
            assert_eq!(original_unit_absent(&root, &shard).unwrap(), absent);
            assert_eq!(fs::read(&head).unwrap(), bytes);
        }
        fs::remove_file(&head).unwrap();
        fs::write(root.join("journal/write.lock"), b"unknown retained bytes").unwrap();
        assert!(!original_unit_absent(&root, &shard).unwrap());
        fs::write(root.join("journal/write.lock"), b"").unwrap();
        fs::write(root.join("journal/segments/retained.sobj"), b"unreadable original").unwrap();
        assert!(!original_unit_absent(&root, &shard).unwrap());
        fs::remove_file(root.join("journal/segments/retained.sobj")).unwrap();
        fs::write(root.join("metadata-generation.sobj"), b"invalid current identity").unwrap();
        assert!(!original_unit_absent(&root, &shard).unwrap());
        fs::remove_file(root.join("metadata-generation.sobj")).unwrap();
        let stage = unversioned_conversion_stage_base_path(&root, &shard).unwrap();
        fs::create_dir_all(&stage).unwrap();
        assert!(!original_unit_absent(&root, &shard).unwrap());
        fs::remove_dir(&stage).unwrap();
        let combined = root.parent().unwrap().join("root/records");
        fs::create_dir_all(&combined).unwrap();
        fs::write(combined.join("original.sobj"), b"combined original scope").unwrap();
        assert!(!original_unit_absent(&root, &shard).unwrap());
    }
}
