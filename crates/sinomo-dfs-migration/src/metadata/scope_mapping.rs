//! Pure conversion planning over a captured authoritative routing catalog.
//! A mapping is neither publication permission nor proof of source retirement.
use super::{MetadataBackendError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinomo_dfs::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct ScopeTuple {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
}
impl ScopeTuple {
    fn of(config: &DfsMetadataShardConfig) -> Self {
        Self {
            file_system_id: config.file_system_id.clone(),
            shard_id: DfsMetadataShardId::new(config.shard_id.clone()),
        }
    }
}

/// The caller must obtain retirement of this ORIGINAL scope. A target's newer
/// epoch, a converted image, or this value cannot satisfy that requirement.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(super) struct RequiredSourceRetirement {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub owner_epoch: u64,
    pub operation_id: String,
    pub source_operation_digest: String,
    pub targets: Vec<ScopeTuple>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct RequiredNativeWork {
    pub family: String,
    pub reason: String,
    /// Complete unresolved row; the converter also retains the source archive.
    pub source_row: Value,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct ScopeMappingPlan {
    pub tables: DfsMetadataTableSnapshotTables,
    pub operation_mappings: Vec<Value>,
    pub required_source_retirements: Vec<RequiredSourceRetirement>,
    pub required_native_work: Vec<RequiredNativeWork>,
    /// Digest/scope changes and compact-body ambiguity stay outside native rows.
    pub row_provenance: Vec<Value>,
    /// These rows are absent only from this target, never from the source archive.
    pub retained_non_target_row_count: usize,
}

fn invalid(error: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(error.to_string())
}
fn value<T: Serialize>(row: &T) -> Result<Value> {
    serde_json::to_value(row).map_err(invalid)
}
fn decode<T: serde::de::DeserializeOwned>(row: &Value) -> Result<T> {
    serde_json::from_value(row.clone()).map_err(invalid)
}

struct Routing<'a> {
    assignments: &'a [DfsMetadataShardConfig],
    objects: BTreeMap<(DfsFileSystemId, DfsObjectId), ScopeTuple>,
    fallback_objects: BTreeMap<(DfsFileSystemId, DfsObjectId), BTreeSet<ScopeTuple>>,
    inode_objects: BTreeMap<(DfsFileSystemId, DfsInodeId), DfsObjectId>,
    directory_fragments:
        BTreeMap<(DfsFileSystemId, DfsInodeId, DfsDirFragmentId), BTreeSet<ScopeTuple>>,
    transactions: BTreeMap<(DfsFileSystemId, String), BTreeSet<ScopeTuple>>,
}
impl Routing<'_> {
    fn path(&self, fs: &DfsFileSystemId, path: &str) -> Result<ScopeTuple> {
        select_metadata_shard_config_for_path(fs, self.assignments, path)
            .map_err(invalid)?
            .as_ref()
            .map(ScopeTuple::of)
            .ok_or_else(|| invalid(format!("no captured assignment owns {fs}:{path}")))
    }
    fn original(&self, fs: &DfsFileSystemId, shard: &DfsMetadataShardId) -> Result<ScopeTuple> {
        self.assignments
            .iter()
            .find(|config| &config.file_system_id == fs && config.shard_id == shard.0)
            .map(ScopeTuple::of)
            .ok_or_else(|| {
                invalid(format!(
                    "original scope {fs}:{} is not in the captured catalog",
                    shard.0
                ))
            })
    }
    fn object(&self, fs: &DfsFileSystemId, object: &DfsObjectId) -> Result<ScopeTuple> {
        let key = (fs.clone(), object.clone());
        if let Some(scope) = self.objects.get(&key) {
            return Ok(scope.clone());
        }
        match self.fallback_objects.get(&key) {
            Some(scopes) if scopes.len() == 1 => Ok(scopes.first().expect("one scope").clone()),
            _ => Err(invalid(format!(
                "object {fs}:{object} lacks an unambiguous complete path witness"
            ))),
        }
    }
    fn directory_fragment(&self, row: &DfsDirFragmentRecord) -> Result<BTreeSet<ScopeTuple>> {
        let key = (
            row.file_system_id.clone(),
            row.dir_inode_id.clone(),
            row.dirfrag_id.clone(),
        );
        if let Some(scopes) = self.directory_fragments.get(&key) {
            return Ok(scopes.clone());
        }
        // Older rows can name an explicit captured object inode. Current
        // directory-parent IDs are instead witnessed by their stored dentries.
        singleton(
            self.inode_objects
                .get(&(row.file_system_id.clone(), row.dir_inode_id.clone()))
                .ok_or_else(|| invalid("directory fragment has no captured inode identity"))
                .and_then(|object| self.object(&row.file_system_id, object)),
        )
    }
    fn request(&self, request: &DfsNamespaceRequestRecord) -> Result<BTreeSet<ScopeTuple>> {
        let mut paths = request.operation.involved_paths();
        // This operation deliberately uses a global runtime lane, but its
        // complete immutable locator is still native source routing evidence.
        if let DfsNamespaceOperation::ContentReference { request } = &request.operation {
            paths.push(&request.path);
        }
        if paths.is_empty() {
            return Ok(BTreeSet::from([self.original(
                &request.file_system_id,
                &request.authority_shard_id,
            )?]));
        }
        paths
            .into_iter()
            .map(|path| self.path(&request.file_system_id, path))
            .collect()
    }
    fn request_coordinator(&self, request: &DfsNamespaceRequestRecord) -> Result<ScopeTuple> {
        if let DfsNamespaceOperation::ContentReference { request: reference } = &request.operation {
            return self.path(&request.file_system_id, &reference.path);
        }
        if let Some(path) = request.operation.involved_paths().first() {
            return self.path(&request.file_system_id, path);
        }
        self.original(&request.file_system_id, &request.authority_shard_id)
    }
    fn version(&self, version: &DfsObjectVersionRecord) -> Result<ScopeTuple> {
        self.object(&version.file_system_id, &version.object_id)
    }
    fn transaction(&self, transaction: &DfsNamespaceTransaction) -> Result<BTreeSet<ScopeTuple>> {
        let fs = &transaction.file_system_id;
        let mut scopes = BTreeSet::new();
        for object in &transaction.objects {
            scopes.insert(self.path(fs, &object.path)?);
        }
        for version in &transaction.versions {
            scopes.insert(self.version(version)?);
        }
        if let Some(pins) = &transaction.file_version_pins {
            for pin in pins.add.iter().chain(&pins.remove) {
                scopes.insert(self.object(&pin.file_system_id, &pin.object_id)?);
            }
        }
        for state in &transaction.advisory_lock_states {
            scopes.insert(self.object(&state.file_system_id, &state.object_id)?);
        }
        if let Some(identity) = &transaction.content_reference_identity {
            scopes.insert(self.object(&identity.file_system_id, &identity.object_id)?);
        }
        Ok(scopes)
    }
    fn job(
        &self,
        fs: &DfsFileSystemId,
        root: &str,
        target: Option<&str>,
    ) -> Result<BTreeSet<ScopeTuple>> {
        // Native hydration/resumption indexes jobs by root_path authority;
        // the complete target operand stays in the row.
        let _ = target;
        singleton(self.path(fs, root))
    }
    fn payload(&self, payload: &DfsPreparedOperationPayload) -> Result<BTreeSet<ScopeTuple>> {
        let fs = &payload.file_system_id;
        let mut scopes = self.transaction(&payload.transaction)?;
        for expected in &payload.expected_path_occupants {
            scopes.insert(self.path(fs, &expected.path)?);
        }
        for expected in &payload.expected_object_versions {
            scopes.insert(self.object(fs, &expected.object_id)?);
        }
        for receipt in &payload.content_receipts {
            scopes
                .insert(self.object(&receipt.receipt.file_system_id, &receipt.receipt.object_id)?);
        }
        for intent in [&payload.prepared_intent, &payload.terminal_intent]
            .into_iter()
            .flatten()
        {
            scopes.insert(self.object(&intent.file_system_id, &intent.object_id)?);
        }
        if let Some(request) = &payload.completed_request {
            scopes.insert(self.request_coordinator(request)?);
        }
        for effect in &payload.metadata_effects {
            use DfsPreparedMetadataEffect as E;
            match effect {
                E::PutObject { object }
                | E::FinalizeCrossShardSourceEviction {
                    tombstone: object, ..
                } => {
                    scopes.insert(self.path(&object.file_system_id, &object.path)?);
                }
                E::PutVersion { version } => {
                    scopes.insert(self.version(version)?);
                }
                E::ReconcileCrossShardPinTransfer { transfer, .. } => {
                    scopes.insert(self.path(&transfer.file_system_id, &transfer.target_path)?);
                }
                E::PutNamespaceJob { job } => {
                    scopes.extend(self.job(
                        &job.file_system_id,
                        &job.root_path,
                        job.target_path.as_deref(),
                    )?);
                }
                E::PutNamespaceJobReceipt { receipt } => {
                    scopes.extend(self.job(
                        &receipt.file_system_id,
                        &receipt.root_path,
                        receipt.target_path.as_deref(),
                    )?);
                }
                E::PutNamespaceRequest { request } => {
                    scopes.insert(self.request_coordinator(request)?);
                }
                E::PutNamespaceRequestReceipt { receipt } => {
                    scopes.insert(
                        self.original(&receipt.file_system_id, &receipt.authority_shard_id)?,
                    );
                }
                E::PutNamespaceTransactionReceipt { receipt } => {
                    scopes.extend(self.transaction_receipt(receipt)?);
                }
                E::MergeIntentReceiptRetention { retention } => {
                    let row = value(retention)?;
                    for name in [
                        "max_evicted_counter_by_object_and_coordinator",
                        "max_evicted_operation_order_token_by_object_and_coordinator",
                    ] {
                        if let Some(objects) = row.get(name).and_then(Value::as_object) {
                            for object in objects.keys() {
                                scopes.insert(self.object(fs, &decode(&json!(object))?)?);
                            }
                        }
                    }
                    for object in &retention.opaque_intent_history_evicted_object_ids {
                        scopes.insert(self.object(fs, object)?);
                    }
                }
                E::RemovePreparedOperation { digest }
                | E::ResolvePreparedOperation { digest, .. } => {
                    scopes.insert(self.original(&digest.file_system_id, &digest.shard_id)?);
                }
                // These identities have no path after compaction. They remain
                // on the explicitly captured original authority lane.
                E::PutFileSystem { .. }
                | E::RemoveNamespaceTransactionReceipt { .. }
                | E::RemoveNamespaceJobReceipt { .. }
                | E::RemoveNamespaceRequestReceipt { .. } => {
                    scopes.insert(self.original(fs, &payload.shard_id)?);
                }
                _ => {
                    for object in effect.affected_object_ids() {
                        scopes.insert(self.object(fs, &object)?);
                    }
                }
            }
        }
        if scopes.is_empty() {
            scopes.insert(self.original(fs, &payload.shard_id)?);
        }
        Ok(scopes)
    }
    fn transaction_receipt(
        &self,
        receipt: &DfsNamespaceTransactionReceipt,
    ) -> Result<BTreeSet<ScopeTuple>> {
        if let Some(identity) = &receipt.content_reference_identity {
            return Ok(BTreeSet::from([
                self.object(&identity.file_system_id, &identity.object_id)?
            ]));
        }
        self.transactions
            .get(&(
                receipt.file_system_id.clone(),
                receipt.transaction_id.clone(),
            ))
            .filter(|scopes| !scopes.is_empty())
            .cloned()
            .ok_or_else(|| {
                invalid("compact transaction receipt has no retained transaction routing footprint")
            })
    }
}

fn retain<T: Serialize>(
    rows: Vec<T>,
    family: &str,
    plan: &mut ScopeMappingPlan,
    route: impl Fn(&T) -> Result<BTreeSet<ScopeTuple>>,
    target: &ScopeTuple,
) -> Result<Vec<T>> {
    let mut kept = Vec::new();
    for row in rows {
        match route(&row) {
            Ok(scopes) if scopes.len() == 1 && scopes.contains(target) => kept.push(row),
            Ok(scopes) if scopes.len() == 1 => plan.retained_non_target_row_count += 1,
            Ok(_) => plan.required_native_work.push(RequiredNativeWork {
                family: family.into(), reason: "row spans multiple target authorities; native coordinator/retirement evidence is required".into(), source_row: value(&row)?,
            }),
            Err(error) => plan.required_native_work.push(RequiredNativeWork {
                family: family.into(), reason: error.to_string(), source_row: value(&row)?,
            }),
        }
    }
    Ok(kept)
}

fn singleton(scope: Result<ScopeTuple>) -> Result<BTreeSet<ScopeTuple>> {
    scope.map(|scope| BTreeSet::from([scope]))
}

/// Partition only with actual full captured configurations. No IO, minting,
/// sequence rebasing, journal writing, or terminal-status inference occurs here.
enum ImportedJobRouting<'a> {
    Native {
        assignments: &'a [DfsMetadataShardConfig],
        target: &'a DfsMetadataShardConfig,
    },
    // Entered after explicit standalone source-owner validation. Empty native
    // assignments cannot choose this mode or supply an owner identity.
    Standalone {
        scope: &'a DfsMetadataShardId,
        owner: &'a sinomo_core::node::NodeId,
    },
}
impl ImportedJobRouting<'_> {
    fn coordinator(&self, fs: &DfsFileSystemId, path: &str) -> Result<ScopeTuple> {
        match self {
            Self::Native { assignments, .. } => {
                select_metadata_shard_config_for_path(fs, assignments, path)
                    .map_err(invalid)?
                    .as_ref()
                    .map(ScopeTuple::of)
                    .ok_or_else(|| invalid("current adoption source coordinator missing"))
            }
            Self::Standalone { scope, .. } => Ok(ScopeTuple {
                file_system_id: fs.clone(),
                shard_id: (*scope).clone(),
            }),
        }
    }
    fn destination(&self, fs: &DfsFileSystemId, path: &str) -> Result<sinomo_core::node::NodeId> {
        match self {
            Self::Native { assignments, .. } => {
                select_metadata_shard_config_for_path(fs, assignments, path)
                    .map_err(invalid)?
                    .map(|config| config.primary_node_id)
                    .ok_or_else(|| invalid("current adoption destination scope missing"))
            }
            Self::Standalone { owner, .. } => Ok((*owner).clone()),
        }
    }
    fn retains(
        &self,
        fs: &DfsFileSystemId,
        original: &DfsMetadataShardId,
        coordinator: Option<&ScopeTuple>,
    ) -> Result<bool> {
        match self {
            Self::Native {
                assignments,
                target,
            } => {
                if !assignments
                    .iter()
                    .any(|cfg| &cfg.file_system_id == fs && cfg.shard_id == original.0)
                {
                    return Err(invalid(
                        "original reservation scope is absent from captured assignments",
                    ));
                }
                Ok(&target.file_system_id == fs
                    && (target.shard_id == original.0
                        || coordinator.is_some_and(|scope| scope == &ScopeTuple::of(target))))
            }
            Self::Standalone { .. } => Ok(true),
        }
    }
}
#[cfg(test)]
fn current_adoption_job(
    original: &Value,
    tables: &DfsMetadataTableSnapshotTables,
    assignments: &[DfsMetadataShardConfig],
) -> Result<DfsNamespaceJobRecord> {
    let target = assignments
        .first()
        .ok_or_else(|| invalid("native assignments absent"))?;
    current_adoption_job_with_routing(
        original,
        tables,
        &ImportedJobRouting::Native {
            assignments,
            target,
        },
    )
}
fn current_adoption_job_with_routing(
    original: &Value,
    tables: &DfsMetadataTableSnapshotTables,
    routing: &ImportedJobRouting<'_>,
) -> Result<DfsNamespaceJobRecord> {
    crate::legacy_wire::verify_original_namespace_request(original)?;
    let fs: DfsFileSystemId = decode(
        &crate::legacy_wire::normalize_source_leaf(
            json!({"file_system_id":original["file_system_id"]}),
        )?["file_system_id"],
    )?;
    let body = original["operation"]["adopt-object-at-path"]
        .as_object()
        .ok_or_else(|| invalid("old adoption has no exact declared variant body"))?;
    let source_path = body
        .get("source_path")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("old adoption source path is missing"))?
        .to_owned();
    let moved = crate::legacy_wire::normalize_object(
        body.get("object")
            .cloned()
            .ok_or_else(|| invalid("old adoption moved object is missing"))?,
    )?;
    if moved.file_system_id != fs {
        return Err(invalid("old adoption changes filesystem"));
    }
    let destination = routing.destination(&fs, &moved.path)?;
    let matching = tables
        .namespace_jobs
        .iter()
        .filter(|job| {
            job.file_system_id == fs
                && job.status == DfsNamespaceJobStatus::Running
                && job.pending_frames.iter().any(|frame| match frame {
                    DfsNamespaceJobFrame::CrossShardAdopt {
                        moved_object,
                        old_path,
                        ..
                    }
                    | DfsNamespaceJobFrame::CrossShardComplete {
                        moved_object,
                        old_path,
                    } => moved_object == &moved && old_path == &source_path,
                    _ => false,
                })
        })
        .collect::<Vec<_>>();
    if matching.len() > 1 {
        return Err(invalid(
            "old adoption has multiple exact current coordinator owners",
        ));
    }
    if let Some(job) = matching.first() {
        return Ok((**job).clone());
    }
    let clock = |key: &str| {
        time::OffsetDateTime::parse(
            original[key]
                .as_str()
                .ok_or_else(|| invalid("old adoption clock missing"))?,
            &time::format_description::well_known::Rfc3339,
        )
        .map_err(invalid)
    };
    let created_at = clock("created_at")?;
    let updated_at = clock("updated_at")?;
    if updated_at < created_at {
        return Err(invalid("old adoption clock regresses"));
    }
    Ok(DfsNamespaceJobRecord {
        job_id: format!(
            "storage-import-adoption:{}",
            source_capsule_digest(original)?
        ),
        file_system_id: fs,
        origin: None,
        kind: DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
        status: DfsNamespaceJobStatus::Running,
        root_path: source_path.clone(),
        target_path: Some(moved.path.clone()),
        pending_frames: vec![
            DfsNamespaceJobFrame::CrossShardComplete {
                moved_object: moved.clone(),
                old_path: source_path.clone(),
            },
            DfsNamespaceJobFrame::CrossShardAdopt {
                moved_object: moved,
                old_path: source_path,
                destination_primary_node_id: destination,
            },
        ],
        processed_object_count: 0,
        created_at,
        updated_at,
        completed_at: None,
        failure_reason: None,
    })
}

pub(super) fn source_capsule_digest(original: &Value) -> Result<String> {
    Ok(format!(
        "blake3:{}",
        blake3::hash(&serde_json::to_vec(original).map_err(invalid)?).to_hex()
    ))
}

/// Turn verified old request operands into current job DATA. The installer must
/// obtain the exact original-reservation seal before that data becomes mounted.
pub(super) fn resolve_adoption_work(
    tables: &mut DfsMetadataTableSnapshotTables,
    pending: &mut [Value],
    work: Vec<RequiredNativeWork>,
    assignments: &[DfsMetadataShardConfig],
    target: &DfsMetadataShardConfig,
) -> Result<(Vec<Value>, Vec<RequiredNativeWork>)> {
    resolve_adoption_work_with_routing(
        tables,
        pending,
        work,
        &ImportedJobRouting::Native {
            assignments,
            target,
        },
    )
}
pub(super) fn resolve_standalone_adoption_work(
    tables: &mut DfsMetadataTableSnapshotTables,
    pending: &mut [Value],
    work: Vec<RequiredNativeWork>,
    scope: &DfsMetadataShardId,
    actual_owner: &sinomo_core::node::NodeId,
) -> Result<(Vec<Value>, Vec<RequiredNativeWork>)> {
    resolve_adoption_work_with_routing(
        tables,
        pending,
        work,
        &ImportedJobRouting::Standalone {
            scope,
            owner: actual_owner,
        },
    )
}
fn resolve_adoption_work_with_routing(
    tables: &mut DfsMetadataTableSnapshotTables,
    pending: &mut [Value],
    work: Vec<RequiredNativeWork>,
    routing: &ImportedJobRouting<'_>,
) -> Result<(Vec<Value>, Vec<RequiredNativeWork>)> {
    let mut mappings = Vec::new();
    let mut unresolved = Vec::new();
    for item in work {
        if item.family == "namespace_requests.completed_preflight" {
            crate::legacy_wire::verify_original_namespace_request(&item.source_row)?;
            if item.source_row["status"].as_str() != Some("completed") {
                return Err(invalid(
                    "original preflight terminal capsule is not completed",
                ));
            }
            mappings.push(json!({"disposition":"preserved-original-terminal-data",
                "capsule_digest":source_capsule_digest(&item.source_row)?,
                "source_row":item.source_row,"current_effects_authored":false}));
            continue;
        }
        if item.family == "namespace_requests.old_preflight" {
            crate::legacy_wire::verify_original_namespace_request(&item.source_row)?;
            if item.source_row["status"].as_str() != Some("pending") {
                return Err(invalid("original preflight reservation is not pending"));
            }
            let declared = crate::legacy_wire::normalize_source_leaf(json!({
                "file_system_id":item.source_row["file_system_id"],
                "authority_shard_id":item.source_row["authority_shard_id"],
            }))?;
            let fs: DfsFileSystemId = decode(&declared["file_system_id"])?;
            let shard: DfsMetadataShardId = decode(&declared["authority_shard_id"])?;
            if !routing.retains(&fs, &shard, None)? {
                continue;
            }
            mappings.push(
                json!({"disposition":"data-after-original-reservation-retirement",
                "original_source_scope":{"file_system_id":fs,"shard_id":shard},
                "capsule_digest":source_capsule_digest(&item.source_row)?,
                "source_row":item.source_row,"current_effects_authored":false,
                "current_work_resolution":"retire-original-noeffect-preflight"}),
            );
            continue;
        }
        if item.family == "prepared_operations.old_adoption" {
            // This exact capsule is covered by its existing typed prepared
            // reservation, including the real source digest/epoch. Its planned
            // Completed request is not an observed historical terminal result.
            let matching = pending
                .iter_mut()
                .filter(|row| {
                    row["source_adoption_original"] == item.source_row
                        && row["retired_reservation"].as_bool() == Some(true)
                })
                .collect::<Vec<_>>();
            if matching.len() != 1 {
                return Err(invalid(
                    "old prepared adoption lacks one exact retained reservation",
                ));
            }
            let row = matching.into_iter().next().expect("one reservation");
            let requests = row["source_adoption_requests"]
                .as_array()
                .ok_or_else(|| invalid("old prepared adoption operands are missing"))?;
            let mut jobs = Vec::new();
            for original in requests {
                if original["operation"]
                    .get("advisory-lock-migration-prepare")
                    .is_some()
                {
                    // An uncommitted preflight has no effects or executable
                    // adoption operands. The real parent reservation seal
                    // retires it without creating any current work.
                    continue;
                }
                let job = current_adoption_job_with_routing(original, tables, routing)?;
                if !tables
                    .namespace_jobs
                    .iter()
                    .any(|old| old.job_id == job.job_id)
                {
                    tables.namespace_jobs.push(job.clone());
                }
                jobs.push(job);
            }
            row["current_jobs"] = value(&jobs)?;
            row["current_work_resolution"] = json!("resume-after-original-prepared-retirement");
            continue;
        }
        if item.family != "namespace_requests.old_adoption" {
            unresolved.push(item);
            continue;
        }
        let original = &item.source_row;
        if original["status"].as_str() != Some("pending") {
            return Err(invalid(
                "data reservation is not an actual pending source request",
            ));
        }
        let scalar = crate::legacy_wire::normalize_source_leaf(json!({
            "file_system_id":original["file_system_id"],
            "authority_shard_id":original["authority_shard_id"],
        }))?;
        let fs: DfsFileSystemId = decode(&scalar["file_system_id"])?;
        let original_shard: DfsMetadataShardId = decode(&scalar["authority_shard_id"])?;
        let job = current_adoption_job_with_routing(original, tables, routing)?;
        let coordinator = routing.coordinator(&fs, &job.root_path)?;
        if !routing.retains(&fs, &original_shard, Some(&coordinator))? {
            continue;
        }
        if !tables
            .namespace_jobs
            .iter()
            .any(|old| old.job_id == job.job_id)
        {
            tables.namespace_jobs.push(job.clone());
        }
        mappings.push(json!({
            "disposition":"data-after-original-reservation-retirement",
            "original_source_scope":{"file_system_id":fs,"shard_id":original_shard},
            "capsule_digest":source_capsule_digest(original)?,"source_row":original,
            "current_coordinator_scope":{"file_system_id":coordinator.file_system_id,"shard_id":coordinator.shard_id},
            "current_job_id":job.job_id,"current_job_data":job,"current_effects_authored":false,
        }));
    }
    Ok((mappings, unresolved))
}

pub(super) fn map_scope(
    mut tables: DfsMetadataTableSnapshotTables,
    mut pending: Vec<Value>,
    assignments: &[DfsMetadataShardConfig],
    target: &DfsMetadataShardConfig,
) -> Result<ScopeMappingPlan> {
    if !assignments.iter().any(|config| config == target) {
        return Err(invalid(
            "target is not an exact captured filesystem/shard configuration",
        ));
    }
    let mut identities = BTreeSet::new();
    for config in assignments {
        if !identities.insert(ScopeTuple::of(config)) {
            return Err(invalid(
                "captured assignments contain duplicate filesystem/shard identities",
            ));
        }
    }
    let target_tuple = ScopeTuple::of(target);
    let mut routing = Routing {
        assignments,
        objects: BTreeMap::new(),
        fallback_objects: BTreeMap::new(),
        inode_objects: BTreeMap::new(),
        directory_fragments: BTreeMap::new(),
        transactions: BTreeMap::new(),
    };
    for object in &tables.objects {
        let key = (object.file_system_id.clone(), object.object_id.clone());
        let scope = routing.path(&object.file_system_id, &object.path)?;
        if routing.objects.insert(key, scope).is_some() {
            return Err(invalid(
                "duplicate current object identity in complete source projection",
            ));
        }
    }
    for inode in &tables.inodes {
        let key = (inode.file_system_id.clone(), inode.inode_id.clone());
        if routing
            .inode_objects
            .insert(key, inode.object_id.clone())
            .is_some()
        {
            return Err(invalid(
                "duplicate current inode identity in source projection",
            ));
        }
    }
    // Native directory fragments use the stored parent path inode, while
    // object inode records use obj:<object_id>. Keep that recorded relationship;
    // never reconstruct an inode ID from a fragment hash or guess its owner.
    let captured_objects: BTreeMap<_, _> = tables
        .objects
        .iter()
        .map(|object| {
            (
                (object.file_system_id.clone(), object.object_id.clone()),
                object,
            )
        })
        .collect();
    for dentry in &tables.dentries {
        let Some(object) =
            captured_objects.get(&(dentry.file_system_id.clone(), dentry.object_id.clone()))
        else {
            continue;
        };
        if object.kind != dentry.kind
            || object.path.trim_matches('/') != dentry.path.trim_matches('/')
            || routing
                .inode_objects
                .get(&(dentry.file_system_id.clone(), dentry.inode_id.clone()))
                != Some(&dentry.object_id)
        {
            continue;
        }
        let scope = routing.object(&dentry.file_system_id, &dentry.object_id)?;
        routing
            .directory_fragments
            .entry((
                dentry.file_system_id.clone(),
                dentry.parent_inode_id.clone(),
                dentry.dirfrag_id.clone(),
            ))
            .or_default()
            .insert(scope);
    }
    for version in &tables.versions {
        for grant in version
            .retention
            .values()
            .filter_map(|state| state.grant.as_ref())
        {
            if grant.object.file_system_id != version.file_system_id
                || grant.object.object_id != version.object_id
                || grant.object.version != version.version
            {
                return Err(invalid(
                    "retained revision path witness changed its immutable identity",
                ));
            }
            let scope = routing.path(&version.file_system_id, &grant.object.path)?;
            routing
                .fallback_objects
                .entry((version.file_system_id.clone(), version.object_id.clone()))
                .or_default()
                .insert(scope);
        }
    }
    for object in &tables.compacted_tombstones {
        let scope = routing.path(&object.file_system_id, &object.path)?;
        routing
            .fallback_objects
            .entry((object.file_system_id.clone(), object.object_id.clone()))
            .or_default()
            .insert(scope);
    }
    for record in std::mem::take(&mut tables.prepared_operations_v7) {
        record.validate()?;
        let record_fs = value(&record.payload.file_system_id)?;
        let record_op = value(&record.payload.operation_id)?;
        let matching = pending.iter().position(|row| {
            row.get("payload").and_then(|body| body.get("operation_id")) == Some(&record_op)
                && row
                    .get("payload")
                    .and_then(|body| body.get("file_system_id"))
                    == Some(&record_fs)
        });
        if let Some(index) = matching {
            if pending[index]["payload"] != value(&record.payload)?
                || pending[index]["status"] != value(&record.status)?
            {
                return Err(invalid(
                    "pending map disagrees with the retained current prepared record",
                ));
            }
        } else {
            pending.push(json!({"payload":record.payload,"status":record.status,"source_operation_digest":record.digest.payload_digest,"source_prepare_sequence":record.digest.prepare_sequence,"source_committed_entry":record.committed_entry}));
        }
    }
    // Embedded new objects are fallback identity witnesses only. A captured
    // current object's actual route takes precedence over stale pending paths.
    for row in &pending {
        let payload: DfsPreparedOperationPayload = decode(
            row.get("payload")
                .ok_or_else(|| invalid("pending mapping has no complete payload"))?,
        )?;
        for object in &payload.transaction.objects {
            let scope = routing.path(&object.file_system_id, &object.path)?;
            routing
                .fallback_objects
                .entry((object.file_system_id.clone(), object.object_id.clone()))
                .or_default()
                .insert(scope);
        }
        for effect in &payload.metadata_effects {
            if let DfsPreparedMetadataEffect::PutObject { object }
            | DfsPreparedMetadataEffect::FinalizeCrossShardSourceEviction {
                tombstone: object,
                ..
            } = effect
            {
                let scope = routing.path(&object.file_system_id, &object.path)?;
                routing
                    .fallback_objects
                    .entry((object.file_system_id.clone(), object.object_id.clone()))
                    .or_default()
                    .insert(scope);
            }
        }
        for intent in [&payload.prepared_intent, &payload.terminal_intent]
            .into_iter()
            .flatten()
        {
            if let Some(object) = &intent.object_metadata {
                if object.file_system_id != intent.file_system_id
                    || object.object_id != intent.object_id
                {
                    return Err(invalid(
                        "intent path witness changed its immutable object identity",
                    ));
                }
                let scope = routing.path(&object.file_system_id, &object.path)?;
                routing
                    .fallback_objects
                    .entry((object.file_system_id.clone(), object.object_id.clone()))
                    .or_default()
                    .insert(scope);
            }
        }
    }
    // Resolve footprints after all complete new-object witnesses exist.
    for row in &pending {
        let payload: DfsPreparedOperationPayload = decode(&row["payload"])?;
        if let Ok(scopes) = routing.transaction(&payload.transaction) {
            routing
                .transactions
                .entry((
                    payload.file_system_id.clone(),
                    payload.transaction.transaction_id.clone(),
                ))
                .or_default()
                .extend(scopes);
        }
    }
    let mut plan = ScopeMappingPlan {
        tables: DfsMetadataTableSnapshotTables::default(),
        operation_mappings: Vec::new(),
        required_source_retirements: Vec::new(),
        required_native_work: Vec::new(),
        row_provenance: Vec::new(),
        retained_non_target_row_count: 0,
    };
    macro_rules! partition {
        ($field:ident, $route:expr) => {
            plan.tables.$field = retain(
                std::mem::take(&mut tables.$field),
                stringify!($field),
                &mut plan,
                $route,
                &target_tuple,
            )?;
        };
    }
    plan.tables.file_systems = tables
        .file_systems
        .into_iter()
        .filter(|fs| fs.file_system_id == target.file_system_id)
        .collect();
    partition!(objects, |row: &DfsObjectMetadata| singleton(
        routing.path(&row.file_system_id, &row.path)
    ));
    partition!(compacted_tombstones, |row: &DfsObjectMetadata| singleton(
        routing.path(&row.file_system_id, &row.path)
    ));
    partition!(file_system_objects, |row: &DfsPathRecord| singleton(
        routing.path(&row.file_system_id, &row.path)
    ));
    partition!(paths, |row: &DfsPathRecord| singleton(
        routing.path(&row.file_system_id, &row.path)
    ));
    partition!(dentries, |row: &DfsPathRecord| singleton(
        routing.path(&row.file_system_id, &row.path)
    ));
    partition!(hardlinks, |row: &DfsHardlinkIndexSnapshotRecord| singleton(
        routing.path(&row.path_record.file_system_id, &row.path_record.path)
    ));
    partition!(inodes, |row: &DfsInodeRecord| singleton(
        routing.object(&row.file_system_id, &row.object_id)
    ));
    partition!(dirfrags, |row: &DfsDirFragmentRecord| routing
        .directory_fragment(row));
    partition!(versions, |row: &DfsObjectVersionRecord| singleton(
        routing.version(row)
    ));
    partition!(version_pins, |row: &DfsMetadataVersionPinRecord| singleton(
        routing.object(&row.file_system_id, &row.object_id)
    ));
    partition!(
        cross_shard_pin_transfers,
        |row: &DfsCrossShardPinTransferRecord| singleton(
            routing.path(&row.file_system_id, &row.target_path)
        )
    );
    partition!(receipts, |row: &DfsReplicaReceiptRecord| singleton(
        routing.object(&row.receipt.file_system_id, &row.receipt.object_id)
    ));
    partition!(intents, |row: &DfsWriteIntentRecord| singleton(
        routing.object(&row.file_system_id, &row.object_id)
    ));
    partition!(intent_receipts, |row: &DfsWriteIntentReceipt| singleton(
        routing.object(&row.file_system_id, &row.object_id)
    ));
    partition!(advisory_lock_states, |row: &DfsAdvisoryLockStateRecord| {
        singleton(routing.object(&row.file_system_id, &row.object_id))
    });
    partition!(namespace_jobs, |row: &DfsNamespaceJobRecord| routing.job(
        &row.file_system_id,
        &row.root_path,
        row.target_path.as_deref()
    ));
    partition!(namespace_job_receipts, |row: &DfsNamespaceJobReceipt| {
        routing.job(
            &row.file_system_id,
            &row.root_path,
            row.target_path.as_deref(),
        )
    });

    // Compact outcomes have no reconstructible input. Preserve their exact
    // opaque identities on every destination of the source FS when routing is
    // unknown. This only rejects/returns witnessed old retries; it grants none.
    for receipt in tables.namespace_transaction_receipts {
        match routing.transaction_receipt(&receipt) {
            Ok(scopes) if scopes.len() == 1 && !scopes.contains(&target_tuple) => {
                plan.retained_non_target_row_count += 1
            }
            scopes => {
                if receipt.file_system_id == target.file_system_id {
                    if !matches!(&scopes, Ok(set) if set.len() == 1) {
                        plan.row_provenance.push(json!({"family":"namespace_transaction_receipts","transaction_id":receipt.transaction_id,"routing":"opaque-source-outcome-replicated","original_digest":receipt.transaction_digest}));
                    }
                    plan.tables.namespace_transaction_receipts.push(receipt);
                }
            }
        }
    }
    let mut request_targets = BTreeMap::new();
    let mut full_request_mappings = BTreeMap::new();
    for mut request in tables.namespace_requests {
        let original = value(&request)?;
        let original_request = request.clone();
        match routing.request_coordinator(&request) {
            Ok(scope) => {
                request_targets.insert(
                    (
                        request.file_system_id.clone(),
                        request.requester_node_id.clone(),
                        request.request_id.clone(),
                    ),
                    scope.clone(),
                );
                if scope != target_tuple {
                    plan.retained_non_target_row_count += 1;
                    continue;
                }
                let command = DfsNamespaceCommand {
                    request_id: request.request_id.clone(),
                    file_system_id: request.file_system_id.clone(),
                    requesting_node_id: request.requester_node_id.clone(),
                    include_metadata_journal_batches: false,
                    metadata_journal_after_sequences: Vec::new(),
                    authority_shards: Vec::new(),
                    operation: request.operation.clone(),
                };
                let involved_scopes = match routing.request(&request) {
                    Ok(scopes) => Some(value(&scopes)?),
                    Err(error) => {
                        plan.required_native_work.push(RequiredNativeWork {
                            family: "namespace_request_operands".into(),
                            reason: error.to_string(),
                            source_row: original.clone(),
                        });
                        None
                    }
                };
                request.command_digest = namespace_command_digest(&command).map_err(invalid)?;
                request.authority_shard_id = DfsMetadataShardId::new(target.shard_id.clone());
                plan.row_provenance.push(json!({"family":"namespace_requests","original_row":original,"target_scope":target_tuple,"involved_scopes":involved_scopes,"current_command_digest":request.command_digest}));
                full_request_mappings.insert(
                    (
                        request.file_system_id.clone(),
                        request.requester_node_id.clone(),
                        request.request_id.clone(),
                    ),
                    (original_request, request.clone()),
                );
                plan.tables.namespace_requests.push(request);
            }
            Err(error) => plan.required_native_work.push(RequiredNativeWork {
                family: "namespace_requests".into(),
                source_row: original,
                reason: error.to_string(),
            }),
        }
    }
    for mut receipt in tables.namespace_request_receipts {
        if receipt.file_system_id != target.file_system_id {
            plan.retained_non_target_row_count += 1;
            continue;
        }
        let key = (
            receipt.file_system_id.clone(),
            receipt.requester_node_id.clone(),
            receipt.request_id.clone(),
        );
        if request_targets
            .get(&key)
            .is_some_and(|scope| scope != &target_tuple)
        {
            plan.retained_non_target_row_count += 1;
            continue;
        }
        let original = value(&receipt)?;
        let routing_kind = if let Some((source, mapped)) = full_request_mappings.get(&key) {
            if receipt.command_digest != source.command_digest
                || receipt.operation_id != source.operation_id
                || receipt.authority_shard_id != source.authority_shard_id
                || source
                    .result
                    .as_ref()
                    .is_some_and(|result| result != &receipt.result)
            {
                plan.required_native_work.push(RequiredNativeWork {
                    family: "namespace_request_receipts".into(),
                    reason:
                        "compact outcome disagrees with its complete source request identity/result"
                            .into(),
                    source_row: original,
                });
                continue;
            }
            receipt.command_digest = mapped.command_digest.clone();
            receipt.authority_shard_id = mapped.authority_shard_id.clone();
            "complete-source-request-native-digest"
        } else {
            "exact-source-outcome-no-input-reconstruction"
        };
        plan.row_provenance.push(json!({"family":"namespace_request_receipts","original_row":original,"routing":routing_kind}));
        plan.tables.namespace_request_receipts.push(receipt);
    }
    for eviction in tables.prepared_operation_evictions {
        eviction.validate()?;
        if eviction.digest.file_system_id == target.file_system_id {
            plan.row_provenance.push(json!({"family":"prepared_operation_evictions","original_digest":eviction.digest,"routing":"exact-source-witness-no-rebinding"}));
            plan.tables.prepared_operation_evictions.push(eviction);
        } else {
            plan.retained_non_target_row_count += 1;
        }
    }
    // Object-specific anti-resurrection facts move with the actual family.
    macro_rules! retention_map {
        ($field:ident) => {
            for (object, state) in tables.intent_receipt_retention.$field {
                match route_object_id(&routing, &object) {
                    Ok(scope) if scope == target_tuple => { plan.tables.intent_receipt_retention.$field.insert(object, state); }
                    Ok(_) => plan.retained_non_target_row_count += 1,
                    Err(error) => plan.required_native_work.push(RequiredNativeWork { family: concat!("intent_receipt_retention.", stringify!($field)).into(), reason: error.to_string(), source_row: json!({"object_id":object,"state":state}) }),
                }
            }
        };
    }
    retention_map!(max_evicted_counter_by_object_and_coordinator);
    retention_map!(max_evicted_operation_order_token_by_object_and_coordinator);
    for object in tables
        .intent_receipt_retention
        .opaque_intent_history_evicted_object_ids
    {
        match route_object_id(&routing, &object) {
            Ok(scope) if scope == target_tuple => {
                plan.tables
                    .intent_receipt_retention
                    .opaque_intent_history_evicted_object_ids
                    .insert(object);
            }
            Ok(_) => plan.retained_non_target_row_count += 1,
            Err(error) => plan.required_native_work.push(RequiredNativeWork {
                family: "intent_receipt_retention.opaque_objects".into(),
                reason: error.to_string(),
                source_row: json!(object),
            }),
        }
    }
    let mut retention = tables.namespace_receipt_retention;
    for (object, state) in std::mem::take(&mut retention.content_reference_outcome_retention) {
        match routing.object(&state.file_system_id, &object) {
            Ok(scope) if scope == target_tuple => {
                plan.tables
                    .namespace_receipt_retention
                    .content_reference_outcome_retention
                    .insert(object, state);
            }
            Ok(_) => plan.retained_non_target_row_count += 1,
            Err(error) => plan.required_native_work.push(RequiredNativeWork {
                family: "content_reference_outcome_retention".into(),
                reason: error.to_string(),
                source_row: json!({"object_id":object,"state":state}),
            }),
        }
    }
    // Source-global expiry facts have no FS/path attribution. Conservatively
    // copying them preserves refusal of unknown retries; never erase a floor.
    plan.tables.namespace_receipt_retention.transaction_floor = retention.transaction_floor;
    plan.tables.namespace_receipt_retention.prepare_floor = retention.prepare_floor;
    plan.tables.namespace_receipt_retention.job_floor = retention.job_floor;
    plan.tables
        .namespace_receipt_retention
        .max_evicted_request_order_token_by_requester =
        retention.max_evicted_request_order_token_by_requester;
    plan.tables
        .namespace_receipt_retention
        .opaque_request_history_evicted_requester_node_ids =
        retention.opaque_request_history_evicted_requester_node_ids;

    for mut row in pending {
        let original = row.clone();
        let payload: DfsPreparedOperationPayload = decode(
            row.get("payload")
                .ok_or_else(|| invalid("pending mapping lacks payload"))?,
        )?;
        let status: DfsPreparedOperationStatus = decode(
            row.get("status")
                .ok_or_else(|| invalid("pending mapping lacks status"))?,
        )?;
        let source_digest = row
            .get("source_operation_digest")
            .and_then(Value::as_str)
            .filter(|digest| !digest.is_empty())
            .ok_or_else(|| invalid("pending mapping lacks its exact source digest"))?
            .to_owned();
        if row.get("source_terminal_receipt").is_some()
            && row.get("compact_terminal").and_then(Value::as_bool) == Some(true)
            && payload.transaction.objects.is_empty()
            && payload.transaction.versions.is_empty()
            && payload.metadata_effects.is_empty()
            && status != DfsPreparedOperationStatus::Prepared
        {
            if payload.file_system_id != target.file_system_id {
                plan.retained_non_target_row_count += 1;
                continue;
            }
            // The placeholder body is not evidence of the historical effect.
            // Keep the exact old outcome opaque; the importer must preserve
            // its source identity rather than commit this as a new effect.
            row["source_payload"] = value(&payload)?;
            row["source_scope"] = json!({"file_system_id":payload.file_system_id,"shard_id":payload.shard_id,"owner_epoch":payload.owner_epoch});
            row["target_scopes"] = json!([target_tuple]);
            row["source_status"] = value(&status)?;
            row["disposition"] = json!("compact-source-terminal-identity");
            row["import_terminal_identity_only"] = json!(true);
            plan.operation_mappings.push(row);
            continue;
        }
        let scopes = match routing.payload(&payload) {
            Ok(scopes) => scopes,
            Err(error) => {
                plan.required_native_work.push(RequiredNativeWork {
                    family: "prepared_operations".into(),
                    reason: error.to_string(),
                    source_row: original,
                });
                continue;
            }
        };
        let owns_original = row.get("original_source_scope").map_or_else(
            || {
                payload.file_system_id == target.file_system_id
                    && payload.shard_id.0 == target.shard_id
            },
            |scope| {
                scope.get("file_system_id") == Some(&json!(target.file_system_id))
                    && scope.get("shard_id") == Some(&json!(target.shard_id))
            },
        );
        if !scopes.contains(&target_tuple) && !owns_original {
            plan.retained_non_target_row_count += 1;
            continue;
        }
        row["source_payload"] = value(&payload)?;
        row["source_scope"] = json!({"file_system_id":payload.file_system_id,"shard_id":payload.shard_id,"owner_epoch":payload.owner_epoch});
        row["target_scopes"] = value(&scopes)?;
        row["source_status"] = value(&status)?;
        row["target_participates"] = json!(scopes.contains(&target_tuple));
        // No body/epoch split, and no Superseded status until actual ORIGINAL
        // authority retirement is verified by the controller.
        let retired = row.get("retired_reservation").and_then(Value::as_bool) == Some(true);
        let changed_scope = payload.shard_id.0 != target.shard_id
            || payload.file_system_id != target.file_system_id
            || payload.owner_epoch != target.epoch;
        if status == DfsPreparedOperationStatus::Prepared
            && (scopes.len() > 1 || changed_scope || retired)
        {
            if row.get("source_intent_only").and_then(Value::as_bool) == Some(true)
                && row.get("original_source_scope").is_none()
            {
                plan.required_native_work.push(RequiredNativeWork { family:"prepared_operations".into(), reason:"partial source intent has no witnessed original shard; bound conversion target is not original authority evidence".into(), source_row:original });
                row["disposition"] = json!("requires-original-source-scope");
            } else {
                let original_scope = if let Some(scope) = row.get("original_source_scope") {
                    #[derive(Deserialize)]
                    struct OriginalScope {
                        file_system_id: DfsFileSystemId,
                        shard_id: DfsMetadataShardId,
                        owner_epoch: u64,
                    }
                    let captured: OriginalScope = decode(scope)?;
                    if captured.file_system_id != payload.file_system_id
                        || captured.owner_epoch != payload.owner_epoch
                    {
                        return Err(invalid(
                            "original scope annotation changed captured filesystem or owner epoch",
                        ));
                    }
                    (
                        captured.file_system_id,
                        captured.shard_id,
                        captured.owner_epoch,
                    )
                } else {
                    (
                        payload.file_system_id.clone(),
                        payload.shard_id.clone(),
                        payload.owner_epoch,
                    )
                };
                row["source_scope"] = json!({"file_system_id":original_scope.0,"shard_id":original_scope.1,"owner_epoch":original_scope.2});
                plan.required_source_retirements
                    .push(RequiredSourceRetirement {
                        file_system_id: original_scope.0,
                        shard_id: original_scope.1,
                        owner_epoch: original_scope.2,
                        operation_id: payload.operation_id.clone(),
                        source_operation_digest: source_digest,
                        targets: scopes.iter().cloned().collect(),
                    });
                row["disposition"] = json!("requires-original-source-retirement");
            }
            row["retirement_verified"] = json!(false);
        } else if scopes.len() > 1 {
            // Terminal effects are already materialized and partitioned above;
            // their original atomic outcome remains a complete archive mapping.
            row["disposition"] = json!("terminal-source-atomic-outcome");
            row["import_terminal_identity_only"] = json!(true);
        } else {
            row["disposition"] = json!("single-target-conversion");
            let mut mapped = payload.clone();
            mapped.file_system_id = target.file_system_id.clone();
            mapped.shard_id = DfsMetadataShardId::new(target.shard_id.clone());
            // Preserve the actual captured epoch and sealed intent binding.
            // Assignment to a newer epoch requires a newly authorized body.
            row["proposed_target_scope"] = json!({"file_system_id":target.file_system_id,"shard_id":target.shard_id,"owner_epoch":target.epoch});
            // Original cut is archival. Only the native controller can assign
            // the target's real mapped journal precondition and prepare slot.
            row["requires_native_sequence_mapping"] =
                json!(mapped.expected_journal_sequence.is_some());
            row["payload"] = value(&mapped)?;
        }
        plan.operation_mappings.push(row);
    }
    Ok(plan)
}

fn route_object_id(routing: &Routing<'_>, object: &DfsObjectId) -> Result<ScopeTuple> {
    let mut matches = BTreeSet::new();
    let keys = routing
        .objects
        .keys()
        .chain(routing.fallback_objects.keys());
    for (fs, id) in keys {
        if id == object {
            matches.insert(routing.object(fs, id)?);
        }
    }
    if matches.len() != 1 {
        return Err(invalid(
            "unscoped retention object lacks unique filesystem ownership",
        ));
    }
    Ok(matches.into_iter().next().expect("one scope"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::node::NodeId;
    use time::OffsetDateTime;

    fn fs(name: &str) -> DfsFileSystemId {
        DfsFileSystemId::from_legacy_named(name).unwrap()
    }
    fn captured_fragment_tables(fs: &DfsFileSystemId) -> DfsMetadataTableSnapshotTables {
        let row = original_adoption(fs.clone());
        let mut object: DfsObjectMetadata =
            decode(&row["operation"]["adopt-object-at-path"]["object"]).unwrap();
        object.path = "/sinomo".into();
        let inode = DfsInodeRecord::from(&object);
        let dentry = DfsPathRecord {
            file_system_id: fs.clone(),
            path: "sinomo".into(),
            object_id: object.object_id.clone(),
            inode_id: inode.inode_id.clone(),
            parent_inode_id: DfsInodeId::new(format!("fs:{fs}:root")),
            dirfrag_id: DfsDirFragmentId::new("root"),
            name: "sinomo".into(),
            name_hash: 1,
            kind: object.kind.clone(),
        };
        DfsMetadataTableSnapshotTables {
            objects: vec![object],
            inodes: vec![inode],
            dirfrags: vec![DfsDirFragmentRecord {
                file_system_id: fs.clone(),
                dir_inode_id: dentry.parent_inode_id.clone(),
                dirfrag_id: dentry.dirfrag_id.clone(),
                hash_start_inclusive: 0,
                hash_end_exclusive: u64::MAX,
                entry_count: 1,
                split_after_count: 4096,
            }],
            dentries: vec![dentry],
            ..Default::default()
        }
    }
    #[test]
    fn directory_fragment_uses_captured_parent_dentry_namespace() {
        let fs = fs("captured-directory-fragment");
        let root = config(fs.clone(), "root", None);
        let tables = captured_fragment_tables(&fs);
        assert_ne!(tables.inodes[0].inode_id, tables.dirfrags[0].dir_inode_id);
        let expected = tables.dirfrags.clone();
        let plan = map_scope(tables, Vec::new(), &[root.clone()], &root).unwrap();
        assert!(plan.required_native_work.is_empty());
        assert_eq!(plan.tables.dirfrags, expected);
    }
    #[test]
    fn directory_fragment_rejects_stray_missing_and_ambiguous_dentry_witnesses() {
        let fs = fs("captured-directory-fragment-negative");
        let root = config(fs.clone(), "root", None);
        let services = config(fs.clone(), "services", Some("/services"));
        let original = captured_fragment_tables(&fs);
        let mut cases = Vec::new();
        let mut missing = original.clone();
        missing.objects.clear();
        cases.push(missing);
        let mut stale_path = original.clone();
        stale_path.objects[0].path = "/different".into();
        cases.push(stale_path);
        let mut wrong_inode = original.clone();
        wrong_inode.dentries[0].inode_id = DfsInodeId::new("missing-child-inode");
        cases.push(wrong_inode);
        let mut wrong_fs = original.clone();
        wrong_fs.dentries[0].file_system_id = self::fs("another-filesystem");
        cases.push(wrong_fs);
        let mut ambiguous = original.clone();
        let mut other = ambiguous.objects[0].clone();
        other.object_id = DfsObjectId::from_legacy_named("other-directory-entry").unwrap();
        other.path = "/services".into();
        let other_inode = DfsInodeRecord::from(&other);
        let mut other_dentry = ambiguous.dentries[0].clone();
        other_dentry.object_id = other.object_id.clone();
        other_dentry.inode_id = other_inode.inode_id.clone();
        other_dentry.path = "services".into();
        other_dentry.name = "services".into();
        ambiguous.objects.push(other);
        ambiguous.inodes.push(other_inode);
        ambiguous.dentries.push(other_dentry);
        cases.push(ambiguous);
        for tables in cases {
            let plan =
                map_scope(tables, Vec::new(), &[root.clone(), services.clone()], &root).unwrap();
            assert!(plan.tables.dirfrags.is_empty());
            assert!(
                plan.required_native_work
                    .iter()
                    .any(|row| row.family == "dirfrags")
            );
        }
    }
    fn original_adoption(fs: DfsFileSystemId) -> Value {
        let moved = DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named("import-adoption-object").unwrap(),
            file_system_id: fs.clone(),
            path: "/right/moved".into(),
            kind: DfsObjectKind::Directory,
            tombstone_provenance: None,
            owner_node_id: NodeId::from_legacy_named("actual-source-owner").unwrap(),
            current_locations: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o755,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 1,
            version: DfsVersion::from_legacy_named("original-planned-move").unwrap(),
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        let mut row = json!({"file_system_id":fs,"requester_node_id":NodeId::from_legacy_named("original-requester").unwrap(),
            "request_id":"original-adopt","operation_id":"original-op","authority_shard_id":"root",
            "operation":{"adopt-object-at-path":{"object":moved,"source_path":"/left/source","pin_transfer_id":null,
                "versions":[],"version_pins":[],"version_pin_set_fingerprint":null,"receipts":[],"intents":[],
                "intent_receipts":[],"intent_receipt_retention":{},"content_reference_outcomes":[],
                "content_reference_outcome_retention":null,"conflicts":[],"advisory_lock_state":null}},
            "command_digest":"","status":"pending","result":null,"posix_coordination_result":null,
            "recovery_attempts":0,"last_recovery_error":null,"created_at":"1970-01-01T00:00:00Z","updated_at":"1970-01-01T00:00:01Z"});
        row["command_digest"] =
            json!(crate::legacy_wire::original_namespace_request_digest(&row).unwrap());
        row
    }
    #[test]
    fn original_target_reservation_and_current_source_coordinator_are_distinct() {
        let file_system_id = fs("import-adoption-scope");
        let root = config(file_system_id.clone(), "root", None);
        let left = config(file_system_id.clone(), "left", Some("/left"));
        let mut right = config(file_system_id.clone(), "right", Some("/right"));
        right.primary_node_id = NodeId::from_legacy_named("actual-destination-primary").unwrap();
        let assignments = [root.clone(), left.clone(), right.clone()];
        let original = original_adoption(file_system_id);
        let mut tables = DfsMetadataTableSnapshotTables::default();
        let (mappings, unresolved) = resolve_adoption_work(
            &mut tables,
            &mut [],
            vec![RequiredNativeWork {
                family: "namespace_requests.old_adoption".into(),
                reason: "missing live offer".into(),
                source_row: original.clone(),
            }],
            &assignments,
            &root,
        )
        .unwrap();
        assert!(unresolved.is_empty());
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0]["source_row"], original);
        assert_eq!(
            mappings[0]["capsule_digest"],
            source_capsule_digest(&original).unwrap()
        );
        assert_eq!(mappings[0]["original_source_scope"]["shard_id"], "root");
        assert_eq!(mappings[0]["current_coordinator_scope"]["shard_id"], "left");
        assert!(
            mappings[0]["original_source_scope"]
                .get("owner_epoch")
                .is_none()
        );
        assert_eq!(tables.namespace_jobs.len(), 1);
        let job = tables.namespace_jobs[0].clone();
        assert_eq!(job.root_path, "/left/source");
        assert_eq!(job.origin, None);
        assert!(
            matches!(&job.pending_frames[1], DfsNamespaceJobFrame::CrossShardAdopt {
            old_path, destination_primary_node_id, ..
        } if old_path == "/left/source" && destination_primary_node_id == &right.primary_node_id)
        );
        let root_plan = map_scope(tables.clone(), Vec::new(), &assignments, &root).unwrap();
        assert!(root_plan.tables.namespace_jobs.is_empty());
        let source_plan = map_scope(tables, Vec::new(), &assignments, &left).unwrap();
        assert_eq!(source_plan.tables.namespace_jobs, vec![job.clone()]);
    }
    #[test]
    fn exact_existing_directory_job_keeps_all_frames_instead_of_parallel_job() {
        let file_system_id = fs("import-existing-directory-job");
        let assignments = [
            config(file_system_id.clone(), "root", None),
            config(file_system_id.clone(), "left", Some("/left")),
            config(file_system_id.clone(), "right", Some("/right")),
        ];
        let original = original_adoption(file_system_id);
        let mut existing =
            current_adoption_job(&original, &Default::default(), &assignments).unwrap();
        existing.job_id = "actual-existing-directory-job".into();
        existing.kind = DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries;
        existing.pending_frames.insert(
            0,
            DfsNamespaceJobFrame::CrossShardDirScan {
                source_path: "/left/source".into(),
                target_path: "/right/moved".into(),
                cursor: None,
            },
        );
        let tables = DfsMetadataTableSnapshotTables {
            namespace_jobs: vec![existing.clone()],
            ..Default::default()
        };
        assert_eq!(
            current_adoption_job(&original, &tables, &assignments).unwrap(),
            existing
        );
        let mut altered = original;
        altered["operation"]["adopt-object-at-path"]["source_path"] = json!("/left/another");
        assert!(current_adoption_job(&altered, &tables, &assignments).is_err());
        assert_eq!(tables.namespace_jobs, vec![existing]);
    }
    #[test]
    fn planned_completed_adoption_is_job_data_after_parent_retirement() {
        let file_system_id = fs("import-prepared-adoption");
        let root = config(file_system_id.clone(), "root", None);
        let assignments = [
            root.clone(),
            config(file_system_id.clone(), "left", Some("/left")),
            config(file_system_id, "right", Some("/right")),
        ];
        let mut request = original_adoption(root.file_system_id.clone());
        request["status"] = json!("completed");
        request["result"] = json!("applied");
        let original_parent =
            json!({"original-prepare":"immutable-original-bytes","completed_request":request});
        let mut pending = vec![
            json!({"retired_reservation":true,"source_adoption_original":original_parent,
            "source_adoption_requests":[request],"source_operation_digest":"exact-parent-digest"}),
        ];
        let mut tables = DfsMetadataTableSnapshotTables::default();
        let (mappings, unresolved) = resolve_adoption_work(
            &mut tables,
            &mut pending,
            vec![RequiredNativeWork {
                family: "prepared_operations.old_adoption".into(),
                reason: "uncommitted original preparation".into(),
                source_row: original_parent.clone(),
            }],
            &assignments,
            &root,
        )
        .unwrap();
        assert!(mappings.is_empty());
        assert!(unresolved.is_empty());
        assert_eq!(pending[0]["source_adoption_original"], original_parent);
        assert_eq!(pending[0]["source_operation_digest"], "exact-parent-digest");
        assert_eq!(pending[0]["retired_reservation"], true);
        assert_eq!(pending[0]["current_jobs"].as_array().unwrap().len(), 1);
        assert!(tables.namespace_request_receipts.is_empty());
        assert_eq!(
            tables.namespace_jobs[0].status,
            DfsNamespaceJobStatus::Running
        );
    }
    fn config(fs: DfsFileSystemId, shard: &str, prefix: Option<&str>) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: fs,
            shard_id: shard.into(),
            primary_node_id: NodeId::from_legacy_named("mapping-node").unwrap(),
            replica_node_ids: Vec::new(),
            epoch: 9,
            assignment: prefix
                .map(|prefix| DfsMetadataShardAssignment::PathPrefix {
                    prefix: prefix.into(),
                })
                .unwrap_or(DfsMetadataShardAssignment::Root),
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        }
    }
    fn payload(file_system_id: DfsFileSystemId, paths: &[&str]) -> DfsPreparedOperationPayload {
        DfsPreparedOperationPayload::for_metadata_effects(
            DfsPreparedOperationScope {
                file_system_id,
                shard_id: DfsMetadataShardId::new("original-combined"),
                owner_epoch: 3,
                operation_id: "source-operation".into(),
            },
            Vec::new(),
            paths
                .iter()
                .map(|path| DfsNamespacePathOccupantExpectation {
                    path: (*path).into(),
                    expected_object_id: None,
                })
                .collect(),
            Vec::new(),
            OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap()
    }
    #[test]
    fn target_requires_exact_filesystem_and_whole_configuration() {
        let captured = config(fs("mapping-a"), "same-label", None);
        let wrong_fs = config(fs("mapping-b"), "same-label", None);
        assert!(
            map_scope(
                Default::default(),
                Vec::new(),
                &[captured.clone()],
                &wrong_fs
            )
            .is_err()
        );
        let mut wrong_epoch = captured.clone();
        wrong_epoch.epoch += 1;
        assert!(map_scope(Default::default(), Vec::new(), &[captured], &wrong_epoch).is_err());
    }
    #[test]
    fn atomic_pending_requires_original_scope_retirement_and_keeps_body() {
        let file_system_id = fs("mapping-atomic");
        let root = config(file_system_id.clone(), "root", None);
        let left = config(file_system_id.clone(), "left", Some("/left"));
        let right = config(file_system_id.clone(), "right", Some("/right"));
        let body = payload(file_system_id.clone(), &["/left/a", "/right/b"]);
        let row = json!({"payload":body,"status":"prepared","source_operation_digest":"exact-source-digest","source_prepare_sequence":17});
        let original = row.clone();
        let plan = map_scope(
            Default::default(),
            vec![row],
            &[root, left.clone(), right],
            &left,
        )
        .unwrap();
        assert!(plan.required_native_work.is_empty());
        let retirement = &plan.required_source_retirements[0];
        assert_eq!(retirement.file_system_id, file_system_id);
        assert_eq!(
            retirement.shard_id,
            DfsMetadataShardId::new("original-combined")
        );
        assert_eq!(retirement.owner_epoch, 3);
        assert_eq!(retirement.targets.len(), 2);
        let mapping = &plan.operation_mappings[0];
        assert_eq!(mapping["payload"], original["payload"]);
        assert_eq!(mapping["source_payload"], original["payload"]);
        assert_eq!(mapping["status"], "prepared");
        assert_eq!(mapping["retirement_verified"], false);
        assert_eq!(
            mapping["disposition"],
            "requires-original-source-retirement"
        );
        assert!(plan.tables.prepared_operations_v7.is_empty());
    }
    #[test]
    fn full_request_keeps_original_digest_and_uses_native_source_coordinator() {
        let file_system_id = fs("mapping-request");
        let root = config(file_system_id.clone(), "root", None);
        let left = config(file_system_id.clone(), "left", Some("/left"));
        let right = config(file_system_id.clone(), "right", Some("/right"));
        let requester = NodeId::from_legacy_named("mapping-requester").unwrap();
        let request = DfsNamespaceRequestRecord {
            file_system_id: file_system_id.clone(),
            requester_node_id: requester,
            request_id: "request".into(),
            operation_id: "rename".into(),
            authority_shard_id: DfsMetadataShardId::new("original-combined"),
            operation: DfsNamespaceOperation::Rename {
                from: "/left/a".into(),
                to: "/right/b".into(),
            },
            command_digest: "exact-source-command-digest".into(),
            status: DfsNamespaceRequestStatus::Pending,
            result: None,
            posix_coordination_result: None,
            recovery_attempts: 0,
            last_recovery_error: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        };
        let expected = namespace_command_digest(&DfsNamespaceCommand {
            request_id: request.request_id.clone(),
            file_system_id,
            requesting_node_id: request.requester_node_id.clone(),
            operation: request.operation.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
        })
        .unwrap();
        let tables = DfsMetadataTableSnapshotTables {
            namespace_requests: vec![request],
            ..Default::default()
        };
        let plan = map_scope(tables, Vec::new(), &[root, left.clone(), right], &left).unwrap();
        assert!(plan.required_native_work.is_empty());
        assert_eq!(
            plan.tables.namespace_requests[0].authority_shard_id,
            DfsMetadataShardId::new("left")
        );
        assert_eq!(plan.tables.namespace_requests[0].command_digest, expected);
        assert_eq!(
            plan.row_provenance[0]["original_row"]["command_digest"],
            "exact-source-command-digest"
        );
        assert_eq!(
            plan.row_provenance[0]["involved_scopes"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }
    #[test]
    fn compact_terminal_without_body_is_preserved_without_reconstructed_effect() {
        let file_system_id = fs("mapping-compact");
        let target = config(file_system_id.clone(), "current-root", None);
        let row = json!({"payload":payload(file_system_id,&[]),"status":"committed","source_operation_digest":"opaque-old-digest","source_prepare_sequence":null,"compact_terminal":true,"source_terminal_receipt":{"original_result":"opaque-result"}});
        let original = row.clone();
        let plan = map_scope(Default::default(), vec![row], &[target.clone()], &target).unwrap();
        assert!(plan.required_native_work.is_empty());
        assert!(plan.required_source_retirements.is_empty());
        assert_eq!(
            plan.operation_mappings[0]["source_operation_digest"],
            "opaque-old-digest"
        );
        assert_eq!(plan.operation_mappings[0]["payload"], original["payload"]);
        assert_eq!(
            plan.operation_mappings[0]["source_terminal_receipt"],
            original["source_terminal_receipt"]
        );
        assert_eq!(
            plan.operation_mappings[0]["import_terminal_identity_only"],
            true
        );
    }
}
