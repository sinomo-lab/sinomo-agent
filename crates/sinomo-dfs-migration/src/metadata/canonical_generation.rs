//! Migration-owned source projection and clean current-generation assembly.
//! Original bodies stay in the verified source/event archive. Exact eligible
//! V7 reservations may also cross the cut for guarded native recovery. This
//! module produces DATA only; the owner separately qualifies native activation.
mod prepared_recovery;
use super::*;
use sinomo_dfs::{
    DfsMetadataCandidateIdentity, DfsMetadataCandidateState, DfsMetadataCandidateWriter,
    DfsMetadataGenerationIdentity, DfsMetadataGenerationSeal, DfsMetadataTableSnapshotTables,
    DfsObjectMetadata, DfsPreparedOperationEviction,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapturedMetadataSource {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retained_placement_reservations: Vec<RetainedPlacementReservation>,
    pub actual_source_root: PathBuf,
    pub source_identity: String,
    pub source_format: Option<u16>,
    pub source_snapshot_fingerprint: String,
    pub exact_prefix: Option<DfsMetadataPrefixProof>,
    pub source_journal_sequence: u64,
    pub source_state_fingerprint: String,
    pub file_system_ids: Vec<DfsFileSystemId>,
    pub physical_shard: DfsMetadataShardId,
    pub projection_assignments: Vec<DfsMetadataShardConfig>,
    pub original_handoff: Option<OriginalMetadataHandoffReservation>,
    pub evidence_root: PathBuf,
    pub planned_target_root: PathBuf,
    pub event_ledger_cut: sinomo_core::storage::migration::stream::StreamCursor,
}
impl CapturedMetadataSource {
    pub fn matches_source_cut(&self, cut: &MetadataSourceCut) -> bool {
        self.retained_placement_reservations == cut.retained_placement_reservations
            && self.actual_source_root == cut.source_root
            && self.source_identity == cut.source_identity
            && self.source_format == cut.source_format_version
            && self.source_snapshot_fingerprint == cut.source_snapshot_fingerprint
            && self.exact_prefix == cut.source_exact_prefix
            && self.source_journal_sequence == cut.source_journal_sequence
            && self.source_state_fingerprint == cut.source_state_fingerprint
            && self.file_system_ids == cut.file_system_ids
            && self.physical_shard == cut.scope_shard
            && self.projection_assignments == cut.projection_assignments
            && self.original_handoff == cut.original_handoff
    }
}

/// An opaque original-wire outcome in the immutable verified source ledger.
/// Its digest is not normalized or promoted to the current operation protocol.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpaqueOriginalOutcomeRef {
    pub family: String,
    pub original_id: String,
    pub original_digest: Option<String>,
    pub original_status: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OriginalWorkRef {
    pub family: String,
    pub original_id: String,
    pub original_digest: Option<String>,
    pub reason: String,
}

/// Constructed only from the validated, source-fenced converter projection.
/// Public fields are inspectable DATA; assembly rechecks their private binding
/// so editing a returned copy does not create a qualified source projection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CapturedMetadataUnit {
    pub source: CapturedMetadataSource,
    #[serde(deserialize_with = "deserialize_projected_rows")]
    pub projected_rows: DfsMetadataTableSnapshotTables,
    pub original_outcomes: Vec<OpaqueOriginalOutcomeRef>,
    pub proven_v7_terminals: Vec<DfsPreparedOperationEviction>,
    pub unresolved: Vec<OriginalWorkRef>,
    capture_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recovery: Option<prepared_recovery::PreparedRecovery>,
}

impl CapturedMetadataUnit {
    pub fn projection_fingerprint(&self) -> &str {
        self.recovery.as_ref().map_or(&self.capture_fingerprint, |recovery| &recovery.binding)
    }
    pub fn validate(&self) -> Result<()> {
        validate_placement_reservations(&self.source.retained_placement_reservations,
            &self.source.file_system_ids, &self.source.physical_shard)?;
        let actual = fingerprint(&(
            &self.source,
            &self.projected_rows,
            &self.original_outcomes,
            &self.proven_v7_terminals,
            &self.unresolved,
        ))?;
        if actual != self.capture_fingerprint {
            return Err(error(
                "captured source projection differs from its verified ledger binding",
            ));
        }
        if let Some(recovery) = &self.recovery { recovery.validate(self)?; }
        Ok(())
    }
    pub fn verify_retained_source(&self) -> Result<()> {
        self.verify_retained_source_at(&self.source.actual_source_root)
    }
    pub fn verify_retained_source_at(&self, actual_root: &Path) -> Result<()> {
        self.validate()?;
        validate_retained_source_identity(
            actual_root,
            &self.source.source_identity,
            &self.source.source_snapshot_fingerprint,
        )
    }
    #[cfg(test)]
    pub(crate) fn from_test_projection(
        source: CapturedMetadataSource,
        projected_rows: DfsMetadataTableSnapshotTables,
        proven_v7_terminals: Vec<DfsPreparedOperationEviction>,
    ) -> Result<Self> {
        Self::bind_projection(
            source,
            projected_rows,
            Vec::new(),
            proven_v7_terminals,
            Vec::new(),
        )
    }
    #[cfg(test)]
    pub(crate) fn with_original_work_for_test(
        &self,
        unresolved: Vec<OriginalWorkRef>,
    ) -> Result<Self> {
        Self::bind_projection(
            self.source.clone(),
            self.projected_rows.clone(),
            self.original_outcomes.clone(),
            self.proven_v7_terminals.clone(),
            unresolved,
        )
    }
    /// This capture with its namespace jobs replaced by their settled form.
    /// A converting shard settles its own legacy cross-shard jobs from its
    /// own rows before counting; every other captured fact is kept, and the
    /// projection and any V7 recovery evidence are rebound to the settled
    /// rows, so the result is admitted only as this settled projection.
    pub(crate) fn with_settled_namespace_jobs(
        &self,
        namespace_jobs: Vec<sinomo_dfs::DfsNamespaceJobRecord>,
    ) -> Result<Self> {
        self.validate()?;
        let mut rows = self.projected_rows.clone();
        rows.namespace_jobs = namespace_jobs;
        let mut settled = Self::bind_projection(
            self.source.clone(),
            rows,
            self.original_outcomes.clone(),
            self.proven_v7_terminals.clone(),
            self.unresolved.clone(),
        )?;
        if let Some(recovery) = &self.recovery {
            let mut recovery = recovery.clone();
            recovery.binding = recovery.expected_binding(&settled)?;
            settled.recovery = Some(recovery);
        }
        settled.validate()?;
        Ok(settled)
    }
    fn bind_projection(
        source: CapturedMetadataSource,
        projected_rows: DfsMetadataTableSnapshotTables,
        original_outcomes: Vec<OpaqueOriginalOutcomeRef>,
        proven_v7_terminals: Vec<DfsPreparedOperationEviction>,
        unresolved: Vec<OriginalWorkRef>,
    ) -> Result<Self> {
        let capture_fingerprint = fingerprint(&(
            &source,
            &projected_rows,
            &original_outcomes,
            &proven_v7_terminals,
            &unresolved,
        ))?;
        Ok(Self {
            source,
            projected_rows,
            original_outcomes,
            proven_v7_terminals,
            unresolved,
            capture_fingerprint,
            recovery: None,
        })
    }
}

pub(crate) struct CanonicalMetadataGeneration {
    pub identity: DfsMetadataCandidateIdentity,
    pub seal: DfsMetadataGenerationSeal,
}

fn fingerprint(value: &impl Serialize) -> Result<String> {
    let mut hash = blake3::Hasher::new();
    serde_json::to_writer(&mut hash, value).map_err(error)?;
    Ok(format!("blake3:{}", hash.finalize().to_hex()))
}

/// Assemble a representation cut, not new namespace effects or old operations
/// under new IDs. `counted_objects` is the complete captured object table, with
/// only the independently qualified census count field allowed to differ.
pub(crate) fn build_canonical_generation(
    captured: &CapturedMetadataUnit,
    generation: &DfsMetadataGenerationIdentity,
    counted_objects: &[DfsObjectMetadata],
    target: &Path,
) -> Result<CanonicalMetadataGeneration> {
    // Remote sources are authenticated DATA supplied by their actual owners.
    // Only those owners can inspect their physical roots; the coordinator's
    // pure preview cannot assume remote paths exist on its machine.
    captured.validate()?;
    generation.validate()?;
    if captured.has_unresolved_work() {
        return Err(error(format!(
            "original source work remains unresolved: {}",
            captured
                .unresolved
                .iter()
                .take(8)
                .map(|work| format!("{}:{} ({})", work.family, work.original_id, work.reason))
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    if generation.shard_id != captured.source.physical_shard
        || !captured
            .source
            .file_system_ids
            .contains(&generation.file_system_id)
        || counted_objects.len() != captured.projected_rows.objects.len()
    {
        return Err(error(
            "canonical generation differs from its captured source scope/table",
        ));
    }
    let original = captured
        .projected_rows
        .objects
        .iter()
        .map(|object| (&object.object_id, object))
        .collect::<BTreeMap<_, _>>();
    let mut seen = std::collections::BTreeSet::new();
    for counted in counted_objects {
        let before = original
            .get(&counted.object_id)
            .ok_or_else(|| error("census introduced an uncaptured object"))?;
        let mut only_count = (*before).clone();
        only_count.namespace_link_count = counted.namespace_link_count;
        if &only_count != counted || !seen.insert(counted.object_id.clone()) {
            return Err(error(
                "census changes fields other than the captured namespace count or repeats an object",
            ));
        }
    }
    let mut rows = captured.projected_rows.clone();
    rows.objects = counted_objects.to_vec();
    rows.prepared_operations_v7 = captured.recovery_preparations().to_vec();
    captured.validate_recovery_counts(counted_objects)?;
    rows.prepared_operation_evictions = captured.proven_v7_terminals.clone();
    for terminal in &rows.prepared_operation_evictions {
        terminal.validate()?;
    }
    let rows = DfsMetadataCandidateState::new(rows)?.snapshot();
    let projection = fingerprint(&(generation, &rows, &captured.original_outcomes))?;
    // Source-to-representation checkpoint: no old journal frames or fabricated
    // commits. Inherited reservations keep their exact original V7 identity.
    let prefix = source_proof::conversion_base(
        captured.source.source_journal_sequence,
        &captured.source.source_identity,
        &projection,
    );
    let writer = DfsMetadataCandidateWriter::open_generation(target, generation.clone())?;
    let header = sinomo_dfs::inspect_metadata_storage_generation(target)?;
    let identity = match header.state {
        sinomo_dfs::DfsMetadataGenerationState::Staged { seal: Some(_) } => {
            let existing = sinomo_dfs::read_metadata_candidate(target)?;
            if existing.prefix_proof != prefix || *existing.tables != rows {
                return Err(error(
                    "sealed canonical target differs from the original source/census",
                ));
            }
            sinomo_dfs::inspect_metadata_candidate(target)?
        }
        sinomo_dfs::DfsMetadataGenerationState::Staged { seal: None } => {
            writer.write(rows, prefix)?
        }
        sinomo_dfs::DfsMetadataGenerationState::Active { .. } => {
            return Err(error("canonical assembly cannot rewrite active storage"));
        }
    };
    let seal = writer.seal_storage_generation(&identity)?;
    captured.validate()?;
    Ok(CanonicalMetadataGeneration { identity, seal })
}

pub(super) fn capture_projected_source(
    session: &mut Session,
    facts: &CapturedSourceCut,
    shard: &DfsMetadataShardId,
) -> Result<CapturedMetadataUnit> {
    session.inventory.verify(&session.source_root)?;
    let projected_rows = session.projection.current_tables()?;
    let mut operations = session.projection.pending_operations()?;
    let (original_outcomes, proven_v7_terminals, unresolved) = collect_original_outcomes(
        &projected_rows,
        &operations,
        session.projection.required_native_work(),
    )?;
    // Settled bodies remain archived. Do not retain their decoded JSON while
    // binding the capture and the small set of eligible original reservations.
    operations.retain(|operation| operation["status"] == "prepared"
        && operation["source_protocol_family"] == "prepared-operation-v7"
        && operation["source_v7_payload_exact"] == true);
    let source = CapturedMetadataSource {
        retained_placement_reservations: read_placement_capture(&session.destination_root, shard)?,
        actual_source_root: session.source_root.clone(),
        source_identity: facts.source_identity.clone(),
        source_format: facts.source_format_version,
        source_snapshot_fingerprint: facts.source_snapshot_fingerprint.clone(),
        exact_prefix: facts.source_exact_prefix.clone(),
        source_journal_sequence: facts.source_journal_sequence,
        source_state_fingerprint: facts.source_state_fingerprint.clone(),
        file_system_ids: facts.file_system_ids.clone(),
        physical_shard: shard.clone(),
        projection_assignments: facts.projection_assignments.clone(),
        original_handoff: facts.original_handoff.clone(),
        evidence_root: session.staged_root.clone(),
        planned_target_root: session.staged_root.join("namespace-count-current-image"),
        event_ledger_cut: session.progress.events.clone(),
    };
    let mut unit = CapturedMetadataUnit::bind_projection(
        source,
        projected_rows,
        original_outcomes,
        proven_v7_terminals,
        unresolved,
    )?;
    let checkpoint = session.staged_root.join("captured-unit.json");
    if checkpoint.exists() {
        let retained: CapturedMetadataUnit = read_json(&checkpoint)?;
        retained.validate()?;
        // Physical exchange can relocate the SAME original source. Preserve
        // its immutable logical location in the recorded capture; actual source
        // custody and verification use the session's resolved archive path.
        unit.source.actual_source_root = retained.source.actual_source_root.clone();
        unit = CapturedMetadataUnit::bind_projection(
            unit.source,
            unit.projected_rows,
            unit.original_outcomes,
            unit.proven_v7_terminals,
            unit.unresolved,
        )?;
        if unit.capture_fingerprint != retained.capture_fingerprint {
            return Err(error(
                "replayed source ledger differs from its immutable captured projection",
            ));
        }
        unit = retained;
    } else {
        write_count_cohort_checkpoint(&checkpoint, &unit)?;
    }
    unit.verify_retained_source_at(&session.source_root)?;
    unit.bind_replayed_recovery(&operations, &session.staged_root)?;
    Ok(unit)
}

// Migration-owned checkpoint DTO decoding. Core's current snapshot stream stays
// the sole runtime codec; this does not teach core to read a source JSON image.
fn deserialize_projected_rows<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<DfsMetadataTableSnapshotTables, D::Error> {
    let fields = BTreeMap::<String, Value>::deserialize(deserializer)?;
    let mut rows = DfsMetadataTableSnapshotTables::default();
    macro_rules! decode_fields {
        ($($field:ident),* $(,)?) => {
            for (name, value) in fields {
                match name.as_str() {
                    $(stringify!($field) => { rows.$field = serde_json::from_value(value).map_err(serde::de::Error::custom)?; },)*
                    _ => return Err(serde::de::Error::custom(format!("unknown captured source row family {name}"))),
                }
            }
        };
    }
    decode_fields!(
        file_systems,
        objects,
        file_system_objects,
        paths,
        inodes,
        dirfrags,
        dentries,
        hardlinks,
        compacted_tombstones,
        versions,
        version_pins,
        cross_shard_pin_transfers,
        receipts,
        receipt_withdrawals,
        intents,
        intent_receipts,
        intent_receipt_retention,
        advisory_lock_states,
        namespace_transaction_receipts,
        prepared_operations_v7,
        prepared_operation_evictions,
        namespace_jobs,
        namespace_job_receipts,
        namespace_request_receipts,
        namespace_receipt_retention,
        namespace_requests
    );
    Ok(rows)
}

struct BoundedWire(Vec<u8>, usize);
impl std::io::Write for BoundedWire {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.1.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other(
                "captured source DATA exceeds transport byte budget",
            ));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Bounded migration-owned wire DATA. It carries no native/source capability;
/// each actual retiring owner compares it with its own captured source before
/// publication. Current module payload codecs retain their canonical JSON bytes.
pub(crate) fn encode_captured_unit(
    unit: &CapturedMetadataUnit,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    unit.validate()?;
    let mut wire = BoundedWire(Vec::new(), max_bytes);
    serde_json::to_writer(&mut wire, unit).map_err(error)?;
    Ok(wire.0)
}

pub(crate) fn decode_captured_unit(
    bytes: &[u8],
    max_bytes: usize,
    max_records: usize,
) -> Result<CapturedMetadataUnit> {
    if bytes.len() > max_bytes {
        return Err(error("captured source DATA exceeds transport byte budget"));
    }
    let unit: CapturedMetadataUnit = serde_json::from_slice(bytes).map_err(error)?;
    let rows = &unit.projected_rows;
    let lengths = [
        rows.file_systems.len(),
        rows.objects.len(),
        rows.file_system_objects.len(),
        rows.paths.len(),
        rows.inodes.len(),
        rows.dirfrags.len(),
        rows.dentries.len(),
        rows.hardlinks.len(),
        rows.compacted_tombstones.len(),
        rows.versions.len(),
        rows.version_pins.len(),
        rows.cross_shard_pin_transfers.len(),
        rows.receipts.len(),
        rows.receipt_withdrawals.len(),
        rows.intents.len(),
        rows.intent_receipts.len(),
        rows.advisory_lock_states.len(),
        rows.namespace_transaction_receipts.len(),
        rows.prepared_operations_v7.len(),
        rows.prepared_operation_evictions.len(),
        rows.namespace_jobs.len(),
        rows.namespace_job_receipts.len(),
        rows.namespace_request_receipts.len(),
        rows.namespace_requests.len(),
        unit.original_outcomes.len(),
        unit.proven_v7_terminals.len(),
        unit.unresolved.len(),
        unit.recovery_preparations().len(),
    ];
    let count = lengths
        .into_iter()
        .try_fold(0usize, |sum, length| sum.checked_add(length))
        .ok_or_else(|| error("captured source record count overflow"))?;
    if count > max_records {
        return Err(error(
            "captured source DATA exceeds transport record budget",
        ));
    }
    unit.validate()?;
    Ok(unit)
}

fn collect_original_outcomes(
    projected_rows: &DfsMetadataTableSnapshotTables,
    operations: &[Value],
    required_work: Vec<super::scope_mapping::RequiredNativeWork>,
) -> Result<(
    Vec<OpaqueOriginalOutcomeRef>,
    Vec<DfsPreparedOperationEviction>,
    Vec<OriginalWorkRef>,
)> {
    let mut original_outcomes = Vec::new();
    let mut unresolved = Vec::new();
    let mut terminals = projected_rows
        .prepared_operation_evictions
        .iter()
        .map(|record| {
            record.validate()?;
            Ok((record.digest.operation_id.clone(), record.clone()))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    for operation in operations {
        let status = operation
            .get("status")
            .and_then(Value::as_str)
            .ok_or_else(|| error("original operation projection omits its status"))?;
        let payload = operation
            .get("payload")
            .ok_or_else(|| error("original operation projection omits its identity"))?;
        let intent_only =
            operation.get("source_intent_only").and_then(Value::as_bool) == Some(true);
        let family = operation
            .get("source_protocol_family")
            .and_then(Value::as_str)
            .unwrap_or(if intent_only {
                "intents"
            } else {
                "namespace-prepare"
            });
        let original_id = if intent_only {
            payload
                .get("prepared_intent")
                .or_else(|| payload.get("terminal_intent"))
                .and_then(|intent| intent.get("intent_id"))
                .and_then(Value::as_str)
                .or_else(|| {
                    // The source projection uses the current metadata-effect
                    // envelope, but this remains the original intent identity.
                    let effects = payload.get("metadata_effects")?.as_array()?;
                    let [effect] = effects.as_slice() else { return None; };
                    if effect.get("type")?.as_str()? != "put-intent" { return None; }
                    effect.get("intent")?.get("intent_id")?.as_str()
                })
        } else {
            payload.get("operation_id").and_then(Value::as_str)
        }
        .ok_or_else(|| error("original operation has no exact retained identity"))?
        .to_owned();
        let digest = (!intent_only)
            .then(|| {
                operation
                    .get("source_operation_digest")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .flatten();
        original_outcomes.push(OpaqueOriginalOutcomeRef {
            family: family.into(),
            original_id: original_id.clone(),
            original_digest: digest.clone(),
            original_status: status.into(),
        });
        if status == "prepared" {
            unresolved.push(OriginalWorkRef {
                family: family.into(),
                original_id,
                original_digest: digest,
                reason: "original preparation has no proven terminal outcome".into(),
            });
            continue;
        }
        if family == "prepared-operation-v7" {
            let Some(raw_digest) = operation
                .get("source_v7_exact_digest")
                .filter(|value| !value.is_null())
            else {
                unresolved.push(OriginalWorkRef {
                    family: family.into(),
                    original_id,
                    original_digest: digest,
                    reason: "original v7 digest cannot be represented without changing its scope"
                        .into(),
                });
                continue;
            };
            let original_digest: sinomo_dfs::DfsPreparedOperationDigest =
                serde_json::from_value(raw_digest.clone()).map_err(error)?;
            let status: sinomo_dfs::DfsPreparedOperationStatus =
                serde_json::from_value(Value::String(status.into())).map_err(error)?;
            let committed = operation
                .get("source_v7_commit_reference")
                .filter(|value| !value.is_null())
                .map(|value| {
                    serde_json::from_value::<sinomo_dfs::DfsPreparedOperationCommitReference>(
                        value.clone(),
                    )
                    .map(Box::new)
                    .map_err(error)
                })
                .transpose()?;
            if status == sinomo_dfs::DfsPreparedOperationStatus::Committed && committed.is_none() {
                unresolved.push(OriginalWorkRef {
                    family: family.into(),
                    original_id,
                    original_digest: digest,
                    reason: "original v7 commit has no exact source commit reference".into(),
                });
                continue;
            }
            let terminal = if let Some(terminal) = operation
                .get("source_v7_terminal")
                .filter(|value| !value.is_null())
            {
                let terminal: DfsPreparedOperationEviction =
                    serde_json::from_value(terminal.clone()).map_err(error)?;
                if terminal.digest != original_digest
                    || terminal.status != status
                    || terminal.committed_entry != committed
                {
                    return Err(error(
                        "original terminal source record differs from replayed outcome",
                    ));
                }
                terminal
            } else if operation
                .get("source_v7_payload_exact")
                .and_then(Value::as_bool)
                == Some(true)
            {
                let record = sinomo_dfs::DfsPreparedOperationRecord {
                    digest: original_digest,
                    payload: Arc::new(serde_json::from_value(payload.clone()).map_err(error)?),
                    status,
                    committed_entry: committed,
                };
                DfsPreparedOperationEviction::from_terminal_record(&record)?
            } else {
                DfsPreparedOperationEviction {
                    digest: original_digest,
                    status,
                    committed_entry: committed,
                    genesis_storage_activation: None,
                }
            };
            terminal.validate()?;
            if terminals
                .get(&terminal.digest.operation_id)
                .is_some_and(|old| old != &terminal)
            {
                return Err(error(
                    "original compact outcome contradicts its exact retained terminal body",
                ));
            }
            terminals.insert(terminal.digest.operation_id.clone(), terminal);
        }
    }
    for work in required_work {
        let id = [
            "operation_id",
            "job_id",
            "request_id",
            "intent_id",
            "handoff_id",
        ]
        .iter()
        .find_map(|key| work.source_row.get(*key).and_then(Value::as_str))
        .unwrap_or("<unkeyed original source work>");
        unresolved.push(OriginalWorkRef {
            family: work.family.clone(),
            original_id: id.into(),
            original_digest: None,
            reason: work.reason.clone(),
        });
    }
    for record in &projected_rows.prepared_operations_v7 {
        record.validate()?;
        if record.status == sinomo_dfs::DfsPreparedOperationStatus::Prepared {
            unresolved.push(OriginalWorkRef {
                family: "prepared-operation-v7".into(),
                original_id: record.digest.operation_id.clone(),
                original_digest: Some(record.digest.payload_digest.clone()),
                reason: "original native preparation is unsettled".into(),
            });
        } else {
            let terminal = DfsPreparedOperationEviction::from_terminal_record(record)?;
            terminal.validate()?;
            if terminals
                .get(&terminal.digest.operation_id)
                .is_some_and(|old| old != &terminal)
            {
                return Err(error(
                    "original native terminal evidence changed during projection",
                ));
            }
            terminals.insert(terminal.digest.operation_id.clone(), terminal);
        }
    }
    for job in &projected_rows.namespace_jobs {
        if job.status != sinomo_dfs::DfsNamespaceJobStatus::Completed {
            unresolved.push(OriginalWorkRef {
                family: "namespace_jobs".into(),
                original_id: job.job_id.clone(),
                original_digest: None,
                reason: "original namespace job has not completed".into(),
            });
        }
    }
    for request in &projected_rows.namespace_requests {
        if request.status == sinomo_dfs::DfsNamespaceRequestStatus::Pending {
            unresolved.push(OriginalWorkRef {
                family: "namespace_requests".into(),
                original_id: request.request_id.clone(),
                original_digest: Some(request.command_digest.clone()),
                reason: "original namespace request is pending".into(),
            });
        }
    }
    for intent in &projected_rows.intents {
        if matches!(
            intent.status,
            sinomo_dfs::DfsWriteIntentStatus::Staging
                | sinomo_dfs::DfsWriteIntentStatus::ReceiptsPending
        ) {
            unresolved.push(OriginalWorkRef {
                family: "intents".into(),
                original_id: intent.intent_id.clone(),
                original_digest: None,
                reason: "original write intent is unsettled".into(),
            });
        }
    }
    Ok((
        original_outcomes,
        terminals.into_values().collect(),
        unresolved,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn source(root: &Path) -> CapturedMetadataSource {
        CapturedMetadataSource {
            retained_placement_reservations: Vec::new(),
            actual_source_root: root.to_owned(),
            source_identity: "source-data-only".into(),
            source_format: Some(3),
            source_snapshot_fingerprint: "source-snapshot-data-only".into(),
            exact_prefix: None,
            source_journal_sequence: 0,
            source_state_fingerprint: "source-state-data-only".into(),
            file_system_ids: vec![sinomo_dfs::sifs_file_system_id()],
            physical_shard: DfsMetadataShardId::new("root"),
            projection_assignments: Vec::new(),
            original_handoff: None,
            evidence_root: root.to_owned(),
            planned_target_root: root.join("target-data"),
            event_ledger_cut: Default::default(),
        }
    }
    #[test]
    fn bounded_capture_wire_preserves_typed_module_payload_and_rejects_tampering() {
        let root = sinomo_testdir::TestDir::new("captured-unit-wire-data");
        let mut fs = sinomo_dfs::DfsFileSystem::sifs(
            sinomo_core::node::NodeId::from_legacy_named("capture-wire-data").unwrap(),
        );
        fs.module_metadata.insert(
            "module.test".into(),
            sinomo_dfs::DfsModuleMetadataValue {
                schema_version: 1,
                payload: serde_json::json!({"numbers":[110,117,108,108],"nested":{"value":null}}),
            },
        );
        let mut rows = DfsMetadataTableSnapshotTables::default();
        rows.file_systems.push(fs);
        let unit =
            CapturedMetadataUnit::from_test_projection(source(root.path()), rows, Vec::new())
                .unwrap();
        let bytes = encode_captured_unit(&unit, 64 * 1024).unwrap();
        let decoded = decode_captured_unit(&bytes, 64 * 1024, 16).unwrap();
        assert_eq!(decoded.projected_rows, unit.projected_rows);
        assert_eq!(
            decoded.projection_fingerprint(),
            unit.projection_fingerprint()
        );
        assert!(decode_captured_unit(&bytes, bytes.len() - 1, 16).is_err());
        assert!(decode_captured_unit(&bytes, 64 * 1024, 0).is_err());
        assert!(encode_captured_unit(&unit, bytes.len() - 1).is_err());
        let mut changed = decoded;
        changed.projected_rows.file_systems[0]
            .display_name
            .push_str(" changed");
        assert!(encode_captured_unit(&changed, 64 * 1024).is_err());
    }
    #[test]
    fn old_terminal_digest_stays_opaque_and_is_never_relabelled_as_v7() {
        let mut projection = super::super::legacy_projection::LegacyProjection::default();
        let receipt = serde_json::json!({"operation_id":"old-original", "file_system_id":sinomo_dfs::sifs_file_system_id(),
            "shard_id":"root","owner_epoch":4,"operation_digest":"original-v3-opaque-digest","transaction_id":"old-tx", "status":"committed",
            "created_at":"1970-01-01T00:00:00Z","retained_at":"1970-01-01T00:00:01Z"});
        projection
            .push_snapshot_row("namespace_prepare_receipts", receipt.clone())
            .unwrap();
        let operations = projection.pending_operations().unwrap();
        let rows = projection.current_tables().unwrap();
        let (outcomes, terminals, unresolved) =
            collect_original_outcomes(&rows, &operations, projection.required_native_work())
                .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].original_id, "old-original");
        assert_eq!(
            outcomes[0].original_digest.as_deref(),
            Some("original-v3-opaque-digest")
        );
        assert_eq!(outcomes[0].original_status, "committed");
        assert!(terminals.is_empty());
        assert!(rows.prepared_operations_v7.is_empty());
        assert!(unresolved.is_empty());
    }
    #[test]
    fn actual_v7_journal_terminal_keeps_original_commit_reference() {
        let root = sinomo_testdir::TestDir::new("capture-v7-terminal-reference");
        let shard = DfsMetadataShardId::new("root");
        let identity = sinomo_dfs::DfsMetadataGenerationIdentity {
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            shard_id: shard.clone(),
            generation: "source-data".into(),
            origin: sinomo_dfs::DfsMetadataGenerationOrigin::StorageTransition {
                transition_id: "source-data-transition".into(),
            },
        };
        let writer =
            DfsMetadataCandidateWriter::open_generation(root.path(), identity.clone()).unwrap();
        let initial =
            source_proof::conversion_base(0, "terminal-source-data", "terminal-projection-data");
        let payload = sinomo_dfs::DfsPreparedOperationPayload::for_metadata_effects(
            sinomo_dfs::DfsPreparedOperationScope {
                file_system_id: identity.file_system_id,
                shard_id: shard,
                owner_epoch: 1,
                operation_id: "original-v7-terminal-data".into(),
            },
            Vec::new(),
            Vec::new(),
            Vec::new(),
            time::OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap();
        let (prepared, prefix) = writer.prepare(payload, &initial).unwrap();
        let (committed, _) = writer
            .commit(
                prepared,
                &prefix,
                time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(3),
            )
            .unwrap();
        drop(writer);
        let inventory = super::super::source::discover(root.path()).unwrap();
        let mut cursor = super::super::source::SourceCursor::new(inventory);
        let mut projection = super::super::legacy_projection::LegacyProjection::default();
        let mut frames = 0;
        while let Some(record) = super::super::source::next(root.path(), &mut cursor).unwrap() {
            if let super::super::source::SourceRecord::JournalFrame { entry, exact_bytes } = record
            {
                let document = super::super::source_document::SourceRawDocument::row(exact_bytes);
                projection
                    .apply_journal_from_source(entry, &document.context().unwrap())
                    .unwrap();
                frames += 1;
            }
        }
        assert_eq!(frames, 2);
        let rows = projection.current_tables().unwrap();
        let (_, terminals, unresolved) = collect_original_outcomes(
            &rows,
            &projection.pending_operations().unwrap(),
            projection.required_native_work(),
        )
        .unwrap();
        assert!(unresolved.is_empty(), "{unresolved:?}");
        assert_eq!(
            terminals,
            vec![DfsPreparedOperationEviction::from_terminal_record(&committed).unwrap()]
        );
        assert_eq!(
            terminals[0].committed_entry.as_ref().unwrap().created_at,
            time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(3)
        );
    }
}
