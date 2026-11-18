//! Namespace counts of a converting shard, from its captured rows and the
//! other shards' contributions ([`contributions`]).
//!
//! This module produces conversion DATA, not authority. Its caller keeps the
//! captured source fenced until the shard's activation. Current stat never
//! invokes it.
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

/// A Prepared V7 operation the captured rows hold: its effects on the
/// namespace are not known yet.
pub(crate) fn has_unrecovered_preparation(captured: &CapturedMetadataUnit) -> bool {
    captured
        .projected_rows
        .prepared_operations_v7
        .iter()
        .any(|row| row.status == DfsPreparedOperationStatus::Prepared)
}

/// Non-Completed namespace jobs, Pending requests and staging intents.
fn has_unsettled_namespace_work(tables: &sinomo_dfs::DfsMetadataTableSnapshotTables) -> bool {
    has_unsettled_namespace_work_beside(tables, |_| false)
}

/// [`has_unsettled_namespace_work`], not counting the non-Completed jobs
/// `kept` accepts or the Pending outer requests of those jobs.
fn has_unsettled_namespace_work_beside(
    tables: &sinomo_dfs::DfsMetadataTableSnapshotTables,
    kept: impl Fn(&sinomo_dfs::DfsNamespaceJobRecord) -> bool,
) -> bool {
    let kept_request = |request: &sinomo_dfs::DfsNamespaceRequestRecord| {
        tables.namespace_jobs.iter().any(|job| {
            job.status != DfsNamespaceJobStatus::Completed
                && kept(job)
                && job.file_system_id == request.file_system_id
                && job.origin.as_ref().is_some_and(|origin| {
                    origin.requester_node_id == request.requester_node_id
                        && origin.request_id == request.request_id
                        && origin.operation_id == request.operation_id
                })
        })
    };
    tables
        .namespace_jobs
        .iter()
        .any(|row| row.status != DfsNamespaceJobStatus::Completed && !kept(row))
        || tables
            .namespace_requests
            .iter()
            .any(|row| row.status == DfsNamespaceRequestStatus::Pending && !kept_request(row))
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

/// One source's live rows attributed to it: tombstones are skipped, and a
/// row whose path another shard currently owns is a historical physical copy,
/// not an extra link.
fn owned_live_rows<'a>(
    captured: &'a CapturedMetadataUnit,
    catalog: &[DfsMetadataShardConfig],
    limits: NamespaceCountMaintenanceLimits,
) -> Result<Vec<&'a DfsObjectMetadata>> {
    owned_live_rows_in(&captured.source.physical_shard, &captured.projected_rows, catalog, limits)
}

/// [`owned_live_rows`] over the rows of `physical_shard`, wherever they are
/// held: a capture or a built image.
fn owned_live_rows_in<'a>(
    physical_shard: &DfsMetadataShardId,
    tables: &'a sinomo_dfs::DfsMetadataTableSnapshotTables,
    catalog: &[DfsMetadataShardConfig],
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
        if current_owner(&object.file_system_id, catalog, &object.path)?.shard_id != physical_shard.0 {
            continue; // historical physical copy, not an extra link
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

/// The shard whose owned rows are counted. A parent path or alias target
/// another shard owns is counted by that owner from this shard's
/// contributions.
#[derive(Clone, Copy)]
struct CountScope<'a> {
    catalog: &'a [DfsMetadataShardConfig],
    shard: &'a DfsMetadataShardId,
}
impl CountScope<'_> {
    fn owns(self, file_system_id: &DfsFileSystemId, path: &str) -> Result<bool> {
        Ok(current_owner(file_system_id, self.catalog, path)?.shard_id == self.shard.0)
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
                .ok_or_else(|| invalid("live namespace object parent is absent from its shard"))?;
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
) -> Result<NamespaceCountMaintenancePlan> {
    Ok(NamespaceCountMaintenancePlan {
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
    })
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
}
