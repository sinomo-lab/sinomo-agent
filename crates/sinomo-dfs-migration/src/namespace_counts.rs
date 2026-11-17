//! Explicit maintenance census over a complete, immutable namespace export.
//!
//! This module produces conversion DATA, not authority. Its caller must quiesce
//! namespace writers, capture every current shard through the existing owner,
//! and retain those source fences until normal native publication/installation.
//! Neither stat nor the automatic per-shard storage opener invokes this census.
use crate::metadata::canonical_generation::{CapturedMetadataSource, CapturedMetadataUnit};
use serde::{Deserialize, Serialize};
use sinomo_dfs::{
    DfsFileSystemId, DfsMetadataShardConfig, DfsMetadataShardId, DfsNamespaceJobStatus,
    DfsNamespaceRequestStatus, DfsObjectId, DfsObjectKind, DfsObjectMetadata,
    DfsPreparedOperationStatus, DfsWriteIntentStatus, MetadataBackendError,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

pub(crate) mod contributions;
pub use contributions::{
    NamespaceCountAliasContribution, NamespaceCountChildDirectoryContribution,
    ShardCountContributions,
};

type Result<T> = std::result::Result<T, MetadataBackendError>;
type ObjectKey = (DfsFileSystemId, DfsObjectId);
type ScopeKey = (DfsFileSystemId, DfsMetadataShardId);

/// An already captured/converted current-shaped export and its existing source
/// identity. Serialized source facts alone never qualify installation.
#[derive(Clone)]
pub struct NamespaceCountMaintenanceSource {
    pub captured: Arc<CapturedMetadataUnit>,
}

#[derive(Clone, Copy, Debug)]
pub struct NamespaceCountMaintenanceLimits {
    pub max_objects: usize,
    pub max_encoded_bytes: usize,
    /// Explicit caller budget. An already-owned background census uses its
    /// record/byte bounds and retained source checkpoints, not an RPC timeout.
    pub deadline: Option<Instant>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NamespaceCountMaintenanceUpdate {
    pub shard_id: DfsMetadataShardId,
    /// Full original canonical row: publication must compare this actual source,
    /// not rebase a count onto a newer namespace or overwrite historical bodies.
    pub original: DfsObjectMetadata,
    pub namespace_link_count: u64,
}

/// Reviewable initialization DATA, with the original export identities retained.
/// There is deliberately no `install` or authority-token conversion method.
pub struct NamespaceCountMaintenancePlan {
    pub catalog: Vec<DfsMetadataShardConfig>,
    pub sources: Vec<CapturedMetadataSource>,
    pub source_projection_fingerprints: Vec<String>,
    pub updates: Vec<NamespaceCountMaintenanceUpdate>,
}

fn invalid(message: impl Into<String>) -> MetadataBackendError {
    MetadataBackendError::Serde(message.into())
}
fn check_deadline(limits: NamespaceCountMaintenanceLimits) -> Result<()> {
    if limits.deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(MetadataBackendError::PreparationBudgetExhausted { work: "namespace-count census" });
    }
    Ok(())
}
fn canonical_path(path: &str) -> bool {
    path == "/"
        || (path.starts_with('/')
            && !path.ends_with('/')
            && !path.contains('\0')
            && path[1..]
                .split('/')
                .all(|part| !part.is_empty() && part != "." && part != ".."))
}
fn parent(path: &str) -> Option<&str> {
    if path == "/" {
        None
    } else {
        let index = path.rfind('/')?;
        Some(if index == 0 { "/" } else { &path[..index] })
    }
}
fn catalog_by_scope(
    catalog: &[DfsMetadataShardConfig],
) -> Result<BTreeMap<ScopeKey, DfsMetadataShardConfig>> {
    let mut result = BTreeMap::new();
    for shard in catalog {
        if result
            .insert(
                (
                    shard.file_system_id.clone(),
                    DfsMetadataShardId::new(&shard.shard_id),
                ),
                shard.clone(),
            )
            .is_some()
        {
            return Err(invalid(
                "namespace-count maintenance catalog repeats a logical shard",
            ));
        }
    }
    if result.is_empty() {
        return Err(invalid(
            "namespace-count maintenance requires the complete catalog",
        ));
    }
    Ok(result)
}

struct EncodedBudget {
    remaining: usize,
    deadline: Option<Instant>,
    deadline_exhausted: bool,
}
impl Write for EncodedBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(std::io::Error::other(
                "namespace-count maintenance encoded-data budget exhausted",
            ));
        }
        if self.deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            self.deadline_exhausted = true;
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "namespace-count caller budget exhausted"));
        }
        self.remaining -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Plan only from the complete captured catalog. Current stat never calls this.
/// Refuse unresolved work: a partial cross-shard saga is not a namespace cut.
/// Source snapshots and all original prepares/receipts are left byte-for-byte
/// unchanged; only proposed current-object updates appear in the result.
pub fn plan_namespace_count_maintenance(
    catalog: &[DfsMetadataShardConfig],
    sources: &[NamespaceCountMaintenanceSource],
    limits: NamespaceCountMaintenanceLimits,
) -> Result<NamespaceCountMaintenancePlan> {
    plan_count_census(Some(catalog), sources, limits)
}

/// Conversion DATA for a complete, fenced local inventory. The migration owner
/// must prove coverage and retain the source leases through publication. This
/// function grants no authority and refuses multiple roots for one filesystem.
pub(crate) fn plan_standalone_namespace_count_maintenance(
    sources: &[NamespaceCountMaintenanceSource],
    limits: NamespaceCountMaintenanceLimits,
) -> Result<NamespaceCountMaintenancePlan> {
    plan_count_census(None, sources, limits)
}

fn plan_count_census(
    native_catalog: Option<&[DfsMetadataShardConfig]>,
    sources: &[NamespaceCountMaintenanceSource],
    limits: NamespaceCountMaintenanceLimits,
) -> Result<NamespaceCountMaintenancePlan> {
    preflight_count_input(sources.iter().map(|source| source.captured.as_ref()), 0, limits)?;
    let scopes = native_catalog.map(catalog_by_scope).transpose()?;
    let mut standalone_filesystems = BTreeSet::new();
    if sources.is_empty() {
        return Err(invalid("namespace census requires a complete source set"));
    }
    let mut covered = BTreeSet::new();
    let mut owned = CountedRows::default();
    for source in sources {
        check_deadline(limits)?;
        admit_settled_count_source(&source.captured)?;
        let cut = &source.captured.source;
        if let Some(scopes) = &scopes {
            if catalog_by_scope(&cut.projection_assignments)? != *scopes {
                return Err(invalid("namespace-count source catalog differs from complete cut"));
            }
            for key in source_scopes(cut, scopes)? {
                if !covered.insert(key) {
                    return Err(invalid(
                        "namespace-count sources repeat or omit a filesystem/shard",
                    ));
                }
            }
        } else {
            if !cut.projection_assignments.is_empty() || cut.file_system_ids.is_empty() {
                return Err(invalid("standalone census requires explicit local source coverage"));
            }
            for file_system_id in &cut.file_system_ids {
                if !standalone_filesystems.insert(file_system_id.clone()) {
                    return Err(invalid("standalone census has ambiguous multi-root ownership"));
                }
            }
        }
        for object in owned_live_rows(&source.captured, native_catalog, limits)? {
            owned.insert(&cut.physical_shard, object)?;
        }
    }
    if scopes.as_ref().is_some_and(|scopes| covered != scopes.keys().cloned().collect()) {
        return Err(invalid(
            "namespace-count maintenance omits a current authority scope",
        ));
    }
    let counts = local_counts(&owned, CountScope::Complete, limits)?;
    let updates = verified_count_updates(counts, owned.rows, limits)?;
    finish_count_plan(native_catalog.unwrap_or_default().to_vec(), sources, updates, limits)
}

/// Reject an oversized input before cloning/reconstructing native tables or
/// hashing any candidate. Input snapshots are immutably borrowed.
/// `extra_records` counts already-extracted contribution entries.
fn preflight_count_input<'a>(
    sources: impl Iterator<Item = &'a CapturedMetadataUnit> + Clone,
    extra_records: usize,
    limits: NamespaceCountMaintenanceLimits,
) -> Result<()> {
    check_deadline(limits)?;
    let exhausted = || invalid("namespace-count maintenance object budget exhausted");
    let mut object_count = extra_records;
    if object_count > limits.max_objects {
        return Err(exhausted());
    }
    for source in sources.clone() {
        check_deadline(limits)?;
        object_count = object_count
            .checked_add(source.projected_rows.objects.len())
            .ok_or_else(|| invalid("namespace census record count overflow"))?;
        if object_count > limits.max_objects {
            return Err(exhausted());
        }
    }
    let mut budget = EncodedBudget {
        remaining: limits.max_encoded_bytes,
        deadline: limits.deadline,
        deadline_exhausted: false,
    };
    for source in sources {
        check_deadline(limits)?;
        serde_json::to_writer(&mut budget, source)
        .map_err(|error| if budget.deadline_exhausted {
            MetadataBackendError::PreparationBudgetExhausted { work: "namespace-count input validation" }
        } else { invalid(error.to_string()) })?;
    }
    Ok(())
}

/// Source admission shared by every count mode: an exact captured identity
/// with no unfinished conversion and no unsettled original work.
fn admit_settled_count_source(captured: &CapturedMetadataUnit) -> Result<()> {
    admit_count_source_identity(captured, captured.has_unresolved_work())?;
    refuse_unsettled_count_work(captured)
}

/// An exact captured identity with no unfinished conversion: `unresolved`
/// says whether original work without a proven outcome remains, and no
/// original handoff may be in flight.
fn admit_count_source_identity(captured: &CapturedMetadataUnit, unresolved: bool) -> Result<()> {
    captured.validate()?;
    let cut = &captured.source;
    if cut.source_identity.is_empty()
        || cut.source_snapshot_fingerprint.is_empty()
        || unresolved
        || cut.original_handoff.is_some()
    {
        return Err(invalid(
            "namespace-count source identity/catalog is incomplete or has unfinished conversion",
        ));
    }
    Ok(())
}

/// A partial cross-shard saga or write is not a namespace cut: refuse
/// Prepared V7 operations recovery does not cover, non-Completed namespace
/// jobs, Pending requests and staging intents.
fn refuse_unsettled_count_work(captured: &CapturedMetadataUnit) -> Result<()> {
    if has_unrecovered_preparation(captured) || has_unsettled_namespace_work(&captured.projected_rows) {
        return Err(invalid(
            "namespace-count maintenance requires settled original namespace and write operations",
        ));
    }
    Ok(())
}

/// A Prepared V7 operation the captured recovery does not replay: its
/// effects on the namespace are not known yet.
pub(crate) fn has_unrecovered_preparation(captured: &CapturedMetadataUnit) -> bool {
    captured.projected_rows.prepared_operations_v7.iter().any(|row| {
        row.status == DfsPreparedOperationStatus::Prepared
            && !captured.recovery_preparations().contains(row)
    })
}

/// Non-Completed namespace jobs, Pending requests and staging intents.
fn has_unsettled_namespace_work(tables: &sinomo_dfs::DfsMetadataTableSnapshotTables) -> bool {
    has_unsettled_namespace_work_beside(tables, |_| false)
}

/// [`has_unsettled_namespace_work`], not counting the non-Completed jobs
/// `kept` accepts.
fn has_unsettled_namespace_work_beside(
    tables: &sinomo_dfs::DfsMetadataTableSnapshotTables,
    kept: impl Fn(&sinomo_dfs::DfsNamespaceJobRecord) -> bool,
) -> bool {
    tables
        .namespace_jobs
        .iter()
        .any(|row| row.status != DfsNamespaceJobStatus::Completed && !kept(row))
        || tables
            .namespace_requests
            .iter()
            .any(|row| row.status == DfsNamespaceRequestStatus::Pending)
        || tables.intents.iter().any(|row| {
            matches!(
                row.status,
                DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
            )
        })
}

/// The catalog scopes a source's physical shard serves; each must be covered
/// by the source's captured filesystems.
fn source_scopes(
    cut: &CapturedMetadataSource,
    scopes: &BTreeMap<ScopeKey, DfsMetadataShardConfig>,
) -> Result<Vec<ScopeKey>> {
    let expected_scopes = scopes
        .keys()
        .filter(|(_, shard)| shard == &cut.physical_shard)
        .cloned()
        .collect::<Vec<_>>();
    if expected_scopes.is_empty() {
        return Err(invalid("namespace-count source has a foreign shard"));
    }
    if expected_scopes.iter().any(|key| !cut.file_system_ids.contains(&key.0)) {
        return Err(invalid(
            "namespace-count sources repeat or omit a filesystem/shard",
        ));
    }
    Ok(expected_scopes)
}

fn current_owner(
    file_system_id: &DfsFileSystemId,
    catalog: &[DfsMetadataShardConfig],
    path: &str,
) -> Result<DfsMetadataShardConfig> {
    sinomo_dfs::select_metadata_shard_config_for_path(file_system_id, catalog, path)
        .map_err(|error| invalid(error.to_string()))?
        .ok_or_else(|| invalid("live namespace object has no current catalog owner"))
}

/// One source's live rows attributed to it: tombstones are skipped, and
/// under a native catalog a row whose path another shard currently owns is a
/// historical physical copy, not an extra link.
fn owned_live_rows<'a>(
    captured: &'a CapturedMetadataUnit,
    native_catalog: Option<&[DfsMetadataShardConfig]>,
    limits: NamespaceCountMaintenanceLimits,
) -> Result<Vec<&'a DfsObjectMetadata>> {
    let cut = &captured.source;
    owned_live_rows_in(
        &cut.physical_shard,
        &cut.file_system_ids,
        &captured.projected_rows,
        native_catalog,
        limits,
    )
}

/// [`owned_live_rows`] over the rows of `physical_shard`, wherever they are
/// held: a capture or a built image.
fn owned_live_rows_in<'a>(
    physical_shard: &DfsMetadataShardId,
    file_system_ids: &[DfsFileSystemId],
    tables: &'a sinomo_dfs::DfsMetadataTableSnapshotTables,
    native_catalog: Option<&[DfsMetadataShardConfig]>,
    limits: NamespaceCountMaintenanceLimits,
) -> Result<Vec<&'a DfsObjectMetadata>> {
    let mut current_paths = BTreeMap::new();
    for row in &tables.paths {
        check_deadline(limits)?;
        let key = (
            row.file_system_id.clone(),
            row.path.trim_matches('/').to_owned(),
        );
        if current_paths.insert(key, row).is_some() {
            return Err(invalid(
                "namespace census contains duplicate path-index entries",
            ));
        }
    }
    let mut owned = Vec::new();
    for object in &tables.objects {
        check_deadline(limits)?;
        if !canonical_path(&object.path) {
            return Err(invalid("namespace census contains a noncanonical path"));
        }
        if object.kind == DfsObjectKind::Tombstone || object.tombstone_provenance.is_some() {
            continue;
        }
        if let Some(catalog) = native_catalog {
            let owner = current_owner(&object.file_system_id, catalog, &object.path)?;
            if owner.shard_id != physical_shard.0 {
                continue;
            } // historical physical copy, not an extra link
        } else if !file_system_ids.contains(&object.file_system_id) {
            return Err(invalid("standalone object is outside captured filesystem coverage"));
        }
        let current_path = current_paths.get(&(
            object.file_system_id.clone(),
            object.path.trim_matches('/').to_owned(),
        ));
        if current_path
            .is_none_or(|row| row.object_id != object.object_id || row.kind != object.kind)
        {
            return Err(invalid(
                "live authoritative object has no matching current namespace entry",
            ));
        }
        if object.kind == DfsObjectKind::Hardlink && object.namespace_link_count.is_some() {
            return Err(invalid(
                "alias marker cannot own a canonical namespace count",
            ));
        }
        owned.push(object);
    }
    Ok(owned)
}

/// The counted rows, unique by object identity and by path.
#[derive(Default)]
struct CountedRows {
    rows: BTreeMap<ObjectKey, (DfsMetadataShardId, DfsObjectMetadata)>,
    paths: BTreeMap<(DfsFileSystemId, String), ObjectKey>,
}
impl CountedRows {
    fn insert(&mut self, shard: &DfsMetadataShardId, object: &DfsObjectMetadata) -> Result<()> {
        let key = (object.file_system_id.clone(), object.object_id.clone());
        if self
            .rows
            .insert(key.clone(), (shard.clone(), object.clone()))
            .is_some()
            || self
                .paths
                .insert((object.file_system_id.clone(), object.path.clone()), key)
                .is_some()
        {
            return Err(invalid(
                "namespace census repeats an authoritative object identity or path",
            ));
        }
        Ok(())
    }
}

/// Which counts the counted rows complete.
#[derive(Clone, Copy)]
enum CountScope<'a> {
    /// Every current owner's rows: each parent and alias target is present.
    Complete,
    /// One shard's owned rows. A parent path or alias target another shard
    /// owns is counted by that owner from this shard's contributions.
    Shard {
        catalog: &'a [DfsMetadataShardConfig],
        shard: &'a DfsMetadataShardId,
    },
}
impl CountScope<'_> {
    fn owns(self, file_system_id: &DfsFileSystemId, path: &str) -> Result<bool> {
        match self {
            Self::Complete => Ok(true),
            Self::Shard { catalog, shard } => {
                Ok(current_owner(file_system_id, catalog, path)?.shard_id == shard.0)
            }
        }
    }
}

fn increment_count(counts: &mut BTreeMap<ObjectKey, u64>, key: &ObjectKey) -> Result<()> {
    let count = counts
        .get_mut(key)
        .ok_or_else(|| invalid("namespace count has no canonical owner"))?;
    *count = count
        .checked_add(1)
        .ok_or_else(|| invalid("namespace link count overflow"))?;
    Ok(())
}

/// Base counts (2 for a directory, 1 for another canonical object) plus the
/// increments the counted rows give each other: an alias to its target, a
/// directory to its parent.
fn local_counts(
    owned: &CountedRows,
    scope: CountScope<'_>,
    limits: NamespaceCountMaintenanceLimits,
) -> Result<BTreeMap<ObjectKey, u64>> {
    let mut counts = owned
        .rows
        .iter()
        .filter(|(_, (_, object))| object.kind != DfsObjectKind::Hardlink)
        .map(|(key, (_, object))| {
            (
                key.clone(),
                if object.kind == DfsObjectKind::Directory {
                    2u64
                } else {
                    1u64
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    for (key, (_, object)) in &owned.rows {
        check_deadline(limits)?;
        let mut parent_key = None;
        if let Some(parent_path) = parent(&object.path)
            && scope.owns(&key.0, parent_path)?
        {
            let found = owned
                .paths
                .get(&(key.0.clone(), parent_path.into()))
                .ok_or_else(|| {
                    invalid("live namespace object parent is absent from complete cut")
                })?;
            if owned.rows[found].1.kind != DfsObjectKind::Directory {
                return Err(invalid("live namespace object parent is not a directory"));
            }
            parent_key = Some(found);
        }
        let increment = if object.kind == DfsObjectKind::Hardlink {
            let target = object
                .link_target_object_id
                .as_ref()
                .ok_or_else(|| invalid("namespace alias has no canonical target"))?;
            let target_key = (object.file_system_id.clone(), target.clone());
            match owned.rows.get(&target_key) {
                Some((_, target_object)) => {
                    if matches!(
                        target_object.kind,
                        DfsObjectKind::Directory | DfsObjectKind::Hardlink | DfsObjectKind::Tombstone
                    ) {
                        return Err(invalid(
                            "namespace alias names a noncanonical or directory target",
                        ));
                    }
                    Some(target_key)
                }
                None if matches!(scope, CountScope::Complete) => {
                    return Err(invalid("namespace alias target is missing from complete cut"));
                }
                // Another shard's canonical object: its owner counts this
                // alias from this shard's contributions.
                None => None,
            }
        } else if object.kind == DfsObjectKind::Directory {
            parent_key.cloned()
        } else {
            None
        };
        if let Some(key) = increment {
            increment_count(&mut counts, &key)?;
        }
    }
    Ok(counts)
}

/// Existing initialized counts are verified, never recomputed; only absent
/// counts become updates.
fn verified_count_updates(
    counts: BTreeMap<ObjectKey, u64>,
    mut rows: BTreeMap<ObjectKey, (DfsMetadataShardId, DfsObjectMetadata)>,
    limits: NamespaceCountMaintenanceLimits,
) -> Result<Vec<NamespaceCountMaintenanceUpdate>> {
    let mut updates = Vec::new();
    for (key, count) in counts {
        check_deadline(limits)?;
        let (shard_id, original) = rows.remove(&key).expect("count belongs to canonical row");
        if original
            .namespace_link_count
            .is_some_and(|existing| existing != count)
        {
            return Err(invalid(
                "existing initialized count differs from complete namespace census",
            ));
        }
        if original.namespace_link_count.is_none() {
            updates.push(NamespaceCountMaintenanceUpdate {
                shard_id,
                original,
                namespace_link_count: count,
            });
        }
    }
    Ok(updates)
}

fn finish_count_plan(
    catalog: Vec<DfsMetadataShardConfig>,
    sources: &[NamespaceCountMaintenanceSource],
    updates: Vec<NamespaceCountMaintenanceUpdate>,
    limits: NamespaceCountMaintenanceLimits,
) -> Result<NamespaceCountMaintenancePlan> {
    let plan = NamespaceCountMaintenancePlan {
        catalog,
        sources: sources
            .iter()
            .map(|source| source.captured.source.clone())
            .collect(),
        source_projection_fingerprints: sources
            .iter()
            .map(|source| source.captured.projection_fingerprint().to_owned())
            .collect(),
        updates,
    };
    for source in sources {
        if !source.captured.recovery_preparations().is_empty() {
            source.captured.validate_recovery_counts(&plan.counted_objects(source, limits)?)?;
        }
    }
    Ok(plan)
}

impl NamespaceCountMaintenancePlan {
    pub(crate) fn counted_objects(
        &self,
        source: &NamespaceCountMaintenanceSource,
        limits: NamespaceCountMaintenanceLimits,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let updates = self
            .updates
            .iter()
            .filter(|update| update.shard_id == source.captured.source.physical_shard)
            .map(|update| {
                (
                    (&update.original.file_system_id, &update.original.object_id),
                    update.namespace_link_count,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut objects = Vec::with_capacity(source.captured.projected_rows.objects.len());
        for original in &source.captured.projected_rows.objects {
            check_deadline(limits)?;
            let mut object = original.clone();
            if let Some(count) = updates.get(&(&object.file_system_id, &object.object_id)) {
                object.namespace_link_count = Some(*count);
            }
            objects.push(object);
        }
        Ok(objects)
    }

    /// This local source check grants no publication authority. Remote source
    /// custody remains with its real owner and is checked at native activation.
    pub fn verify_retained_sources(&self) -> Result<()> {
        if self.sources.len() != self.source_projection_fingerprints.len() {
            return Err(invalid("namespace-count source identity set is incomplete"));
        }
        for source in &self.sources {
            crate::metadata::validate_retained_source_identity(
                &source.actual_source_root,
                &source.source_identity,
                &source.source_snapshot_fingerprint,
            )?;
        }
        Ok(())
    }

    /// Check clean target DATA against the original complete census. Original
    /// bodies remain archived; target rows retain proven terminal outcomes and
    /// exactly bound current recovery reservations. Native activation is independent.
    pub fn validate_activation_data(
        &self,
        originals: &[NamespaceCountMaintenanceSource],
        candidates: &[(
            DfsMetadataShardId,
            sinomo_dfs::DfsMetadataTableSnapshotTables,
        )],
        limits: NamespaceCountMaintenanceLimits,
    ) -> Result<()> {
        if originals.len() != self.sources.len()
            || candidates.len() != self.sources.len()
            || self.sources.len() != self.source_projection_fingerprints.len()
        {
            return Err(invalid(
                "namespace-count activation changed its original source set",
            ));
        }
        let checked = if self.catalog.is_empty() {
            plan_standalone_namespace_count_maintenance(originals, limits)?
        } else {
            plan_namespace_count_maintenance(&self.catalog, originals, limits)?
        };
        if checked.updates != self.updates
            || checked.source_projection_fingerprints != self.source_projection_fingerprints
            || serde_json::to_vec(&checked.sources).map_err(|e| invalid(e.to_string()))?
                != serde_json::to_vec(&self.sources).map_err(|e| invalid(e.to_string()))?
        {
            return Err(invalid(
                "namespace-count activation changed its original capture or planned census",
            ));
        }
        let mut seen = BTreeSet::new();
        for (shard, rows) in candidates {
            check_deadline(limits)?;
            if !seen.insert(shard.clone()) {
                return Err(invalid("namespace-count target repeats a shard"));
            }
            let source = originals
                .iter()
                .find(|source| &source.captured.source.physical_shard == shard)
                .ok_or_else(|| invalid("namespace-count target is outside captured source set"))?;
            let mut expected = source.captured.projected_rows.clone();
            expected.objects = self.counted_objects(source, limits)?;
            expected.prepared_operations_v7 = source.captured.recovery_preparations().to_vec();
            expected.prepared_operation_evictions = source.captured.proven_v7_terminals.clone();
            let expected = sinomo_dfs::DfsMetadataCandidateState::new(expected)?.snapshot();
            if &expected != rows {
                return Err(invalid(
                    "namespace-count target differs from canonical source rows and exact terminal outcomes",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use sinomo_core::{DfsObjectPolicy, node::NodeId};
    use sinomo_dfs::{
        DfsFileSystem, DfsMetadataCandidateState, DfsMetadataCandidateWriter,
        DfsMetadataPrefixProof, DfsMetadataShardAssignment, DfsNamespaceObjectVersionExpectation,
        DfsNamespacePathOccupantExpectation, DfsPreparedMetadataEffect,
        DfsPreparedOperationPayload, DfsPreparedOperationScope, DfsVersion,
    };
    use std::time::Duration;

    // Conversion DATA tests: ordinary candidate codecs create immutable staged
    // images, not an authenticated cluster cut or permission to install them.
    pub(super) fn limits() -> NamespaceCountMaintenanceLimits {
        NamespaceCountMaintenanceLimits {
            max_objects: 100,
            max_encoded_bytes: 4 * 1024 * 1024,
            deadline: Some(Instant::now() + Duration::from_secs(10)),
        }
    }

    #[test]
    fn explicit_census_deadline_is_a_budget_result_not_corrupt_data() {
        let (_root, catalog, sources) = fixture();
        let mut bounded = limits();
        bounded.deadline = Some(Instant::now());
        let error = plan_namespace_count_maintenance(&catalog, &sources, bounded).err().unwrap();
        assert!(matches!(error, MetadataBackendError::PreparationBudgetExhausted { .. }), "{error}");
        bounded.deadline = None;
        let background = plan_namespace_count_maintenance(&catalog, &sources, bounded).unwrap();
        assert_eq!(background.updates, plan_namespace_count_maintenance(&catalog, &sources, limits()).unwrap().updates);
        bounded.max_encoded_bytes = 1;
        assert!(matches!(plan_namespace_count_maintenance(&catalog, &sources, bounded), Err(MetadataBackendError::Serde(_))),
            "owned background work retains its hard input-size bound");
    }
    fn catalog() -> Vec<DfsMetadataShardConfig> {
        ["root", "services", "users"]
            .into_iter()
            .map(|shard| DfsMetadataShardConfig {
                file_system_id: sinomo_dfs::sifs_file_system_id(),
                shard_id: shard.into(),
                primary_node_id: NodeId::from_legacy_named(&format!("count-{shard}")).unwrap(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: if shard == "root" {
                    DfsMetadataShardAssignment::Root
                } else {
                    DfsMetadataShardAssignment::PathPrefix {
                        prefix: format!("/{shard}"),
                    }
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            })
            .collect()
    }
    pub(crate) fn object(name: &str, path: &str, kind: DfsObjectKind) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named(name).unwrap(),
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            path: path.into(),
            kind,
            namespace_link_count: None,
            tombstone_provenance: None,
            owner_node_id: NodeId::from_legacy_named("count-owner").unwrap(),
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
            changed_at_unix_nanos: 0,
            version: DfsVersion::from_legacy_named(&format!("{name}-version")).unwrap(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }
    pub(crate) fn source(
        root: &std::path::Path,
        catalog: &[DfsMetadataShardConfig],
        shard: &str,
        objects: Vec<DfsObjectMetadata>,
    ) -> NamespaceCountMaintenanceSource {
        let writer = DfsMetadataCandidateWriter::open_generation(
            root,
            sinomo_dfs::DfsMetadataGenerationIdentity {
                file_system_id: catalog[0].file_system_id.clone(),
                shard_id: DfsMetadataShardId::new(shard),
                generation: format!("census-source-{shard}"),
                origin: sinomo_dfs::DfsMetadataGenerationOrigin::StorageTransition {
                    transition_id: "census-data-fixture".into(),
                },
            },
        )
        .unwrap();
        let mut state = DfsMetadataCandidateState::new(Default::default()).unwrap();
        state
            .apply(DfsPreparedMetadataEffect::PutFileSystem {
                file_system: DfsFileSystem::sifs(catalog[0].primary_node_id.clone()),
            })
            .unwrap();
        for object in &objects {
            state
                .apply(DfsPreparedMetadataEffect::PutObject {
                    object: object.clone(),
                })
                .unwrap();
        }
        let initial = DfsMetadataPrefixProof {
            version: 2,
            journal_sequence: 0,
            digest: format!("dfs-metadata-prefix-v2:blake3:{}", "0".repeat(64)),
        };
        let first = &objects[0];
        let payload = DfsPreparedOperationPayload::for_metadata_effects(
            DfsPreparedOperationScope {
                file_system_id: first.file_system_id.clone(),
                shard_id: DfsMetadataShardId::new(shard),
                owner_epoch: 1,
                operation_id: format!("old-{shard}-operation"),
            },
            vec![DfsNamespaceObjectVersionExpectation {
                object_id: first.object_id.clone(),
                expected_version: Some(first.version.clone()),
            }],
            vec![DfsNamespacePathOccupantExpectation {
                path: first.path.clone(),
                expected_object_id: Some(first.object_id.clone()),
            }],
            vec![DfsPreparedMetadataEffect::PutObject {
                object: first.clone(),
            }],
            time::OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap();
        let (prepared, prefix) = writer.prepare(payload, &initial).unwrap();
        let (committed, prefix) = writer
            .commit(prepared, &prefix, time::OffsetDateTime::UNIX_EPOCH)
            .unwrap();
        let mut tables = state.snapshot();
        tables.prepared_operations_v7.push(committed);
        let identity = writer.write(tables.clone(), prefix.clone()).unwrap();
        let mut terminal = DfsMetadataCandidateState::new(tables.clone()).unwrap();
        terminal
            .apply(DfsPreparedMetadataEffect::RemovePreparedOperation {
                digest: tables.prepared_operations_v7[0].digest.clone(),
            })
            .unwrap();
        let source = CapturedMetadataSource {
            retained_placement_reservations: Vec::new(),
            actual_source_root: root.to_owned(),
            source_identity: format!("original-{shard}"),
            source_format: Some(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION),
            source_snapshot_fingerprint: identity.file_fingerprint,
            exact_prefix: Some(prefix.clone()),
            source_journal_sequence: prefix.journal_sequence,
            source_state_fingerprint: identity.state_fingerprint,
            file_system_ids: vec![first.file_system_id.clone()],
            physical_shard: DfsMetadataShardId::new(shard),
            projection_assignments: catalog.to_vec(),
            original_handoff: None,
            evidence_root: root.to_owned(),
            planned_target_root: root.join("namespace-count-current-image"),
            event_ledger_cut: Default::default(),
        };
        NamespaceCountMaintenanceSource {
            captured: Arc::new(
                CapturedMetadataUnit::from_test_projection(
                    source,
                    tables,
                    terminal.snapshot().prepared_operation_evictions,
                )
                .unwrap(),
            ),
        }
    }
    fn standalone_source(root: &std::path::Path) -> NamespaceCountMaintenanceSource {
        let file = object("local-file", "/file", DfsObjectKind::File);
        let mut alias = object("local-alias", "/alias", DfsObjectKind::Hardlink);
        alias.link_target_object_id = Some(file.object_id.clone());
        let captured = source(
            root,
            &catalog(),
            "root",
            vec![
                object("local-root", "/", DfsObjectKind::Directory),
                object("local-dir", "/dir", DfsObjectKind::Directory),
                file,
                alias,
            ],
        );
        let mut facts = captured.captured.source.clone();
        facts.projection_assignments.clear();
        NamespaceCountMaintenanceSource {
            captured: Arc::new(CapturedMetadataUnit::from_test_projection(
                facts,
                captured.captured.projected_rows.clone(),
                captured.captured.proven_v7_terminals.clone(),
            ).unwrap()),
        }
    }

    #[test]
    fn standalone_census_reuses_checked_alias_and_directory_counts() {
        let root = sinomo_testdir::TestDir::new("standalone-count-census");
        let source = standalone_source(&root.join("metadata"));
        let plan = plan_standalone_namespace_count_maintenance(
            std::slice::from_ref(&source), limits(),
        ).unwrap();
        assert!(plan.catalog.is_empty());
        let objects = plan.counted_objects(&source, limits()).unwrap();
        for (path, expected) in [("/", Some(3)), ("/dir", Some(2)),
                                 ("/file", Some(2)), ("/alias", None)] {
            assert_eq!(objects.iter().find(|object| object.path == path).unwrap()
                .namespace_link_count, expected);
        }
    }

    #[test]
    fn standalone_census_refuses_ambiguous_roots_and_native_catalog() {
        let root = sinomo_testdir::TestDir::new("standalone-count-ambiguous");
        let first = standalone_source(&root.join("first"));
        let second = standalone_source(&root.join("second"));
        assert!(plan_standalone_namespace_count_maintenance(&[first, second], limits()).is_err());
        let native = source(&root.join("native"), &catalog(), "root",
            vec![object("native-root", "/", DfsObjectKind::Directory)]);
        assert!(plan_standalone_namespace_count_maintenance(&[native], limits()).is_err());
        assert!(plan_standalone_namespace_count_maintenance(&[], limits()).is_err());
    }

    fn pending(catalog: &[DfsMetadataShardConfig]) -> sinomo_dfs::DfsFileSystemConfig {
        // Proposal DATA only: these pure census tests never release native storage.
        let mut config = sinomo_dfs::DfsFileSystemConfig::from(&DfsFileSystem::sifs(
            catalog[0].primary_node_id.clone(),
        ));
        config.catalog_generation = 3;
        config.metadata_storage = Some(sinomo_dfs::DfsMetadataStorageState::PendingTransition {
            source_activation: None,
            transition_id: "count-data-transition".into(),
            source_catalog_generation: 3,
            target_generation: "count-data-generation".into(),
            target_format: u32::from(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION),
            expected_catalog_digest: crate::namespace_count_cohort::storage_catalog_digest(catalog)
                .unwrap(),
        });
        config
    }
    pub(super) fn fixture() -> (
        sinomo_testdir::TestDir,
        Vec<DfsMetadataShardConfig>,
        Vec<NamespaceCountMaintenanceSource>,
    ) {
        let root = sinomo_testdir::TestDir::new("namespace-count-maintenance");
        let catalog = catalog();
        let file = object("count-file", "/services/file", DfsObjectKind::File);
        let mut alias = object("count-alias", "/users/alias", DfsObjectKind::Hardlink);
        alias.link_target_object_id = Some(file.object_id.clone());
        let sources = vec![
            source(
                &root.join("root"),
                &catalog,
                "root",
                vec![
                    object("count-root", "/", DfsObjectKind::Directory),
                    file.clone(),
                ],
            ),
            source(
                &root.join("services"),
                &catalog,
                "services",
                vec![
                    object("count-services", "/services", DfsObjectKind::Directory),
                    file,
                    object("count-subdir", "/services/sub", DfsObjectKind::Directory),
                ],
            ),
            source(
                &root.join("users"),
                &catalog,
                "users",
                vec![
                    object("count-users", "/users", DfsObjectKind::Directory),
                    alias,
                    object("count-text", "/users/text", DfsObjectKind::File),
                ],
            ),
        ];
        (root, catalog, sources)
    }
    #[test]
    fn count_cohort_staging_replays_exact_initialized_candidates() {
        let (root, catalog, sources) = fixture();
        let staging = root.join("cohort");
        let at = time::OffsetDateTime::UNIX_EPOCH;
        let first = crate::namespace_count_cohort::stage_namespace_count_cohort(
            &staging,
            &pending(&catalog),
            &catalog,
            &sources,
            at,
            limits(),
        )
        .unwrap();
        let again = crate::namespace_count_cohort::stage_namespace_count_cohort(
            &staging,
            &pending(&catalog),
            &catalog,
            &sources,
            at,
            limits(),
        )
        .unwrap();
        assert_eq!(first, again);
        for member in &first.members {
            assert_eq!(
                member.initialized_prefix.journal_sequence,
                member.original_source.source_journal_sequence
            );
            let image = sinomo_dfs::read_metadata_candidate(
                &staging
                    .join(&first.transition_id)
                    .join(&member.assignment.shard_id),
            )
            .unwrap();
            assert!(image.tables.prepared_operations_v7.is_empty());
            assert!(!image.tables.prepared_operation_evictions.is_empty());
            assert_eq!(image.prefix_proof, member.initialized_seal.prefix_proof);
        }
    }

    #[test]
    fn cohort_preserves_the_source_owned_target_path_as_opaque_cross_platform_data() {
        let (root, catalog, mut sources) = fixture();
        let source = Arc::make_mut(&mut sources[0].captured);
        source.source.planned_target_root =
            std::path::PathBuf::from(r"C:\owner\dfs\conversion\namespace-count-current-image");
        reseal(&mut sources[0]);
        let seal = crate::namespace_count_cohort::stage_namespace_count_cohort(
            &root.join("preview"),
            &pending(&catalog),
            &catalog,
            &sources,
            time::OffsetDateTime::UNIX_EPOCH,
            limits(),
        )
        .unwrap();
        let member = seal.member(&DfsMetadataShardId::new("root")).unwrap();
        let source = &member.original_source;
        let input = crate::MetadataSourceCut {
            retained_placement_reservations: Vec::new(),
            namespace_count_cohort: None,
            source_root: source.actual_source_root.clone(),
            source_identity: source.source_identity.clone(),
            scope_shard: source.physical_shard.clone(),
            file_system_ids: source.file_system_ids.clone(),
            source_snapshot_fingerprint: source.source_snapshot_fingerprint.clone(),
            source_format_version: source.source_format,
            source_exact_prefix: source.exact_prefix.clone(),
            converted_image_fingerprint: source.source_state_fingerprint.clone(),
            current_image_fingerprint: member.initialized_state_fingerprint.clone(),
            target_exact_prefix: member.initialized_prefix.clone(),
            source_journal_sequence: source.source_journal_sequence,
            source_state_fingerprint: source.source_state_fingerprint.clone(),
            projection_assignments: source.projection_assignments.clone(),
            original_handoff: source.original_handoff.clone(),
            staged_root: root.join("preview"),
            pending_operations: Vec::new(),
        };
        let mut bound = seal.bind_cut(&input).unwrap();
        assert_eq!(bound.staged_root, source.planned_target_root);
        seal.validate_bound_cut(&bound).unwrap();
        bound.staged_root = source.evidence_root.join("namespace-count-current-image");
        assert!(seal.validate_bound_cut(&bound).is_err());
    }

    #[test]
    fn count_cohort_changed_sealed_image_is_not_a_new_predecessor() {
        let (root, catalog, sources) = fixture();
        let staging = root.join("cohort");
        let seal = crate::namespace_count_cohort::stage_namespace_count_cohort(
            &staging,
            &pending(&catalog),
            &catalog,
            &sources,
            time::OffsetDateTime::UNIX_EPOCH,
            limits(),
        )
        .unwrap();
        let image = staging
            .join(&seal.transition_id)
            .join("services/records/tables/snapshot.sobj");
        let mut bytes = std::fs::read(&image).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        std::fs::write(&image, &bytes).unwrap();
        assert!(
            crate::namespace_count_cohort::stage_namespace_count_cohort(
                &staging,
                &pending(&catalog),
                &catalog,
                &sources,
                time::OffsetDateTime::UNIX_EPOCH,
                limits(),
            )
            .is_err()
        );
        assert_eq!(std::fs::read(&image).unwrap(), bytes);
    }

    #[test]
    fn count_cohort_invalid_catalog_refuses_before_staging_effects() {
        let (root, catalog, sources) = fixture();
        for invalid in ["../escaped", "/absolute", "", "a/b", "a\\b"] {
            let mut bad = catalog.clone();
            bad[0].shard_id = invalid.into();
            let staging = root.join("must-not-exist");
            assert!(
                crate::namespace_count_cohort::stage_namespace_count_cohort(
                    &staging,
                    &pending(&catalog),
                    &bad,
                    &sources,
                    time::OffsetDateTime::UNIX_EPOCH,
                    limits(),
                )
                .is_err()
            );
            assert!(!staging.exists());
        }
        let mut bad = catalog.clone();
        bad[1].file_system_id =
            sinomo_dfs::DfsFileSystemId::from_legacy_named("foreign-count-fs").unwrap();
        let staging = root.join("must-not-exist");
        assert!(
            crate::namespace_count_cohort::stage_namespace_count_cohort(
                &staging,
                &pending(&catalog),
                &bad,
                &sources,
                time::OffsetDateTime::UNIX_EPOCH,
                limits(),
            )
            .is_err()
        );
        assert!(!staging.exists());
    }

    pub(super) fn reseal(source: &mut NamespaceCountMaintenanceSource) {
        let old = &source.captured;
        source.captured = Arc::new(
            CapturedMetadataUnit::from_test_projection(
                old.source.clone(),
                old.projected_rows.clone(),
                old.proven_v7_terminals.clone(),
            )
            .unwrap(),
        );
    }

    #[test]
    fn complete_census_counts_cross_shard_alias_and_children_without_touching_original_bodies() {
        let (_root, catalog, sources) = fixture();
        let before = sources
            .iter()
            .map(|source| serde_json::to_vec(&source.captured.projected_rows).unwrap())
            .collect::<Vec<_>>();
        let plan = plan_namespace_count_maintenance(&catalog, &sources, limits()).unwrap();
        let counts = plan
            .updates
            .iter()
            .map(|row| (row.original.path.as_str(), row.namespace_link_count))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(counts["/"], 4);
        assert_eq!(counts["/services"], 3);
        assert_eq!(counts["/users"], 2);
        assert_eq!(counts["/services/file"], 2);
        assert_eq!(counts["/services/sub"], 2);
        assert_eq!(counts["/users/text"], 1);
        assert!(!counts.contains_key("/users/alias"));
        for (source, bytes) in sources.iter().zip(before) {
            assert_eq!(
                serde_json::to_vec(&source.captured.projected_rows).unwrap(),
                bytes
            );
        }
    }
    #[test]
    fn partial_duplicate_corrupt_or_inflight_cut_never_produces_counts() {
        let (_root, catalog, mut sources) = fixture();
        assert!(plan_namespace_count_maintenance(&catalog, &sources[..2], limits()).is_err());
        let mut duplicate = catalog.clone();
        duplicate.push(catalog[0].clone());
        assert!(plan_namespace_count_maintenance(&duplicate, &sources, limits()).is_err());
        Arc::make_mut(&mut sources[0].captured)
            .source
            .source_identity = "corrupt".into();
        assert!(plan_namespace_count_maintenance(&catalog, &sources, limits()).is_err());
        reseal(&mut sources[0]);
        sources[0].captured = Arc::new(
            sources[0]
                .captured
                .with_original_work_for_test(vec![
                    crate::metadata::canonical_generation::OriginalWorkRef {
                        family: "prepared".into(),
                        original_id: "pending-original".into(),
                        original_digest: None,
                        reason: "original remains pending".into(),
                    },
                ])
                .unwrap(),
        );
        sources[0].captured.validate().unwrap();
        assert!(plan_namespace_count_maintenance(&catalog, &sources, limits()).is_err());
    }
    #[test]
    fn missing_target_bad_initialized_count_and_exhausted_budget_fail_closed() {
        let (_root, catalog, mut sources) = fixture();
        Arc::make_mut(&mut sources[2].captured)
            .projected_rows
            .objects
            .iter_mut()
            .find(|row| row.kind == DfsObjectKind::Hardlink)
            .unwrap()
            .link_target_object_id = Some(DfsObjectId::from_legacy_named("missing").unwrap());
        reseal(&mut sources[2]);
        assert!(plan_namespace_count_maintenance(&catalog, &sources, limits()).is_err());
        let (_root, catalog, mut sources) = fixture();
        Arc::make_mut(&mut sources[1].captured)
            .projected_rows
            .objects
            .iter_mut()
            .find(|row| row.path == "/services/file")
            .unwrap()
            .namespace_link_count = Some(1);
        reseal(&mut sources[1]);
        assert!(plan_namespace_count_maintenance(&catalog, &sources, limits()).is_err());
        let (_root, catalog, sources) = fixture();
        assert!(
            plan_namespace_count_maintenance(
                &catalog,
                &sources,
                NamespaceCountMaintenanceLimits {
                    max_objects: 1,
                    ..limits()
                }
            )
            .is_err()
        );
        assert!(
            plan_namespace_count_maintenance(
                &catalog,
                &sources,
                NamespaceCountMaintenanceLimits {
                    max_encoded_bytes: 1,
                    ..limits()
                }
            )
            .is_err()
        );
        assert!(
            plan_namespace_count_maintenance(
                &catalog,
                &sources,
                NamespaceCountMaintenanceLimits {
                    deadline: Some(Instant::now()),
                    ..limits()
                }
            )
            .is_err()
        );
    }
    #[test]
    fn activation_requires_counts_and_clean_terminals_while_original_bodies_stay_unchanged() {
        let (_root, catalog, originals) = fixture();
        let before = originals
            .iter()
            .map(|source| serde_json::to_vec(&source.captured.projected_rows).unwrap())
            .collect::<Vec<_>>();
        let plan = plan_namespace_count_maintenance(&catalog, &originals, limits()).unwrap();
        let mut candidates = originals
            .iter()
            .map(|source| {
                (
                    source.captured.source.physical_shard.clone(),
                    source.captured.projected_rows.clone(),
                )
            })
            .collect::<Vec<_>>();
        assert!(
            plan.validate_activation_data(&originals, &candidates, limits())
                .is_err()
        );
        for (source, (_, rows)) in originals.iter().zip(&mut candidates) {
            rows.objects = plan.counted_objects(source, limits()).unwrap();
            rows.prepared_operations_v7.clear();
            rows.prepared_operation_evictions = source.captured.proven_v7_terminals.clone();
            *rows = DfsMetadataCandidateState::new(rows.clone())
                .unwrap()
                .snapshot();
        }
        plan.validate_activation_data(&originals, &candidates, limits())
            .unwrap();
        for (source, bytes) in originals.iter().zip(before) {
            assert_eq!(
                serde_json::to_vec(&source.captured.projected_rows).unwrap(),
                bytes
            );
            source.captured.validate().unwrap();
        }
        candidates[0].1.prepared_operation_evictions.clear();
        assert!(
            plan.validate_activation_data(&originals, &candidates, limits())
                .is_err()
        );
    }

    #[test]
    fn running_namespace_job_and_unfinished_prepare_prevent_census() {
        let (_root, catalog, mut sources) = fixture();
        let record = &mut Arc::make_mut(&mut sources[0].captured)
            .projected_rows
            .prepared_operations_v7[0];
        record.status = DfsPreparedOperationStatus::Prepared;
        record.committed_entry = None;
        reseal(&mut sources[0]);
        assert!(plan_namespace_count_maintenance(&catalog, &sources, limits()).is_err());
        let (_root, catalog, mut sources) = fixture();
        Arc::make_mut(&mut sources[0].captured)
            .projected_rows
            .namespace_jobs
            .push(sinomo_dfs::DfsNamespaceJobRecord {
                job_id: "unfinished-count-cut".into(),
                file_system_id: sinomo_dfs::sifs_file_system_id(),
                origin: None,
                kind: sinomo_dfs::DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
                status: DfsNamespaceJobStatus::Running,
                root_path: "/services/sub".into(),
                target_path: Some("/users/sub".into()),
                pending_frames: Vec::new(),
                processed_object_count: 0,
                created_at: time::OffsetDateTime::UNIX_EPOCH,
                updated_at: time::OffsetDateTime::UNIX_EPOCH,
                completed_at: None,
                failure_reason: None,
            });
        reseal(&mut sources[0]);
        assert!(plan_namespace_count_maintenance(&catalog, &sources, limits()).is_err());
    }
    #[test]
    fn complete_input_budgets_precede_candidate_identity_validation() {
        let (_root, catalog, mut sources) = fixture();
        // If native identity validation runs first this deliberately invalid
        // first candidate would report its fingerprint failure instead.
        Arc::make_mut(&mut sources[0].captured)
            .source
            .source_identity = "invalid-first-identity".into();
        let error = plan_namespace_count_maintenance(
            &catalog,
            &sources,
            NamespaceCountMaintenanceLimits {
                max_objects: 1,
                ..limits()
            },
        )
        .err()
        .unwrap();
        assert!(
            error.to_string().contains("object budget exhausted"),
            "{error}"
        );

        // The first export fits; the complete source set does not. Preflight
        // must check all encoded input before hashing even the first candidate.
        let first = &sources[0].captured;
        let first_bytes = serde_json::to_vec(&(
            &first.source,
            &first.projected_rows,
            &first.original_outcomes,
            &first.proven_v7_terminals,
            &first.unresolved,
        ))
        .unwrap()
        .len();
        let error = plan_namespace_count_maintenance(
            &catalog,
            &sources,
            NamespaceCountMaintenanceLimits {
                max_encoded_bytes: first_bytes + 1,
                ..limits()
            },
        )
        .err()
        .unwrap();
        assert!(
            error.to_string().contains("encoded-data budget exhausted"),
            "{error}"
        );
    }
}
