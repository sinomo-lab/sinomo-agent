//! Explicit maintenance census over a complete, immutable namespace export.
//!
//! This module produces conversion DATA, not authority. Its caller must quiesce
//! namespace writers, capture every current shard through the existing owner,
//! and retain those source fences until normal native publication/installation.
//! Neither stat nor the automatic per-shard storage opener invokes this census.
use crate::MetadataSourceCut;
use serde::{Deserialize, Serialize};
use sinomo_dfs::{
    DfsFileSystemId, DfsMetadataCandidateIdentity, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsMetadataTableSnapshot, DfsNamespaceJobStatus, DfsNamespaceRequestStatus, DfsObjectId,
    DfsObjectKind, DfsObjectMetadata, DfsPreparedOperationStatus, DfsWriteIntentStatus,
    MetadataBackendError,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::time::Instant;

type Result<T> = std::result::Result<T, MetadataBackendError>;
type ObjectKey = (DfsFileSystemId, DfsObjectId);
type ScopeKey = (DfsFileSystemId, DfsMetadataShardId);

/// An already captured/converted current-shaped export and its existing source
/// identity. Serialized source facts alone never qualify installation.
#[derive(Clone)]
pub struct NamespaceCountMaintenanceSource {
    pub source_cut: MetadataSourceCut,
    pub candidate_identity: DfsMetadataCandidateIdentity,
    pub snapshot: DfsMetadataTableSnapshot,
}

#[derive(Clone, Copy, Debug)]
pub struct NamespaceCountMaintenanceLimits {
    pub max_objects: usize,
    pub max_encoded_bytes: usize,
    pub deadline: Instant,
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
    pub sources: Vec<MetadataSourceCut>,
    pub candidate_identities: Vec<DfsMetadataCandidateIdentity>,
    pub updates: Vec<NamespaceCountMaintenanceUpdate>,
}

fn invalid(message: impl Into<String>) -> MetadataBackendError {
    MetadataBackendError::Serde(message.into())
}
fn check_deadline(limits: NamespaceCountMaintenanceLimits) -> Result<()> {
    if Instant::now() >= limits.deadline {
        return Err(invalid(
            "namespace-count maintenance reached its original deadline",
        ));
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
    deadline: Instant,
}
impl Write for EncodedBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.remaining || Instant::now() >= self.deadline {
            return Err(std::io::Error::other(
                "namespace-count maintenance encoded-data budget exhausted",
            ));
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
    check_deadline(limits)?;
    // Reject an oversized complete input before cloning/reconstructing native
    // tables or hashing any candidate. Input snapshots are immutably borrowed.
    let mut object_count = 0usize;
    for source in sources {
        check_deadline(limits)?;
        object_count = object_count
            .checked_add(source.snapshot.tables.objects.len())
            .ok_or_else(|| invalid("namespace census record count overflow"))?;
        if object_count > limits.max_objects {
            return Err(invalid(
                "namespace-count maintenance object budget exhausted",
            ));
        }
    }
    let mut budget = EncodedBudget {
        remaining: limits.max_encoded_bytes,
        deadline: limits.deadline,
    };
    for source in sources {
        check_deadline(limits)?;
        serde_json::to_writer(&mut budget, &source.snapshot)
            .map_err(|error| invalid(error.to_string()))?;
    }
    let scopes = catalog_by_scope(catalog)?;
    let mut covered = BTreeSet::new();
    let mut rows = BTreeMap::<ObjectKey, (DfsMetadataShardId, DfsObjectMetadata)>::new();
    let mut paths = BTreeMap::<(DfsFileSystemId, String), ObjectKey>::new();
    for source in sources {
        check_deadline(limits)?;
        source.snapshot.validate_candidate_image_identity()?;
        let cut = &source.source_cut;
        if cut.source_identity.is_empty()
            || cut.source_snapshot_fingerprint.is_empty()
            || cut.current_image_fingerprint != source.snapshot.semantic_state_fingerprint
            || cut.target_exact_prefix != source.snapshot.prefix_proof
            || source.candidate_identity.state_fingerprint
                != source.snapshot.semantic_state_fingerprint
            || source.candidate_identity.prefix_proof != source.snapshot.prefix_proof
            || source.candidate_identity.file_fingerprint.is_empty()
            || catalog_by_scope(&cut.projection_assignments)? != scopes
            || !cut.pending_operations.is_empty()
            || cut.original_handoff.is_some()
        {
            return Err(invalid(
                "namespace-count source identity/catalog is incomplete or has unfinished conversion",
            ));
        }
        let expected_scopes = scopes
            .keys()
            .filter(|(_, shard)| shard == &cut.scope_shard)
            .cloned()
            .collect::<Vec<_>>();
        if expected_scopes.is_empty() {
            return Err(invalid("namespace-count source has a foreign shard"));
        }
        for key in expected_scopes {
            if !cut.file_system_ids.contains(&key.0) || !covered.insert(key) {
                return Err(invalid(
                    "namespace-count sources repeat or omit a filesystem/shard",
                ));
            }
        }
        let tables = &source.snapshot.tables;
        if tables
            .prepared_operations_v7
            .iter()
            .any(|row| row.status == DfsPreparedOperationStatus::Prepared)
            || tables
                .namespace_jobs
                .iter()
                .any(|row| row.status != DfsNamespaceJobStatus::Completed)
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
        {
            return Err(invalid(
                "namespace-count maintenance requires settled original namespace and write operations",
            ));
        }
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
        for object in &tables.objects {
            check_deadline(limits)?;
            if !canonical_path(&object.path) {
                return Err(invalid("namespace census contains a noncanonical path"));
            }
            if object.kind == DfsObjectKind::Tombstone || object.tombstone_provenance.is_some() {
                continue;
            }
            let owner = sinomo_dfs::select_metadata_shard_config_for_path(
                &object.file_system_id,
                catalog,
                &object.path,
            )
            .map_err(|error| invalid(error.to_string()))?
            .ok_or_else(|| invalid("live namespace object has no current catalog owner"))?;
            if owner.shard_id != cut.scope_shard.0 {
                continue;
            } // historical physical copy, not an extra link
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
            let key = (object.file_system_id.clone(), object.object_id.clone());
            if rows
                .insert(key.clone(), (cut.scope_shard.clone(), object.clone()))
                .is_some()
                || paths
                    .insert((object.file_system_id.clone(), object.path.clone()), key)
                    .is_some()
            {
                return Err(invalid(
                    "namespace census repeats an authoritative object identity or path",
                ));
            }
        }
    }
    if covered != scopes.keys().cloned().collect() {
        return Err(invalid(
            "namespace-count maintenance omits a current authority scope",
        ));
    }
    let mut counts = rows
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
    for (key, (_, object)) in &rows {
        check_deadline(limits)?;
        if let Some(parent_path) = parent(&object.path) {
            let parent_key = paths
                .get(&(key.0.clone(), parent_path.into()))
                .ok_or_else(|| {
                    invalid("live namespace object parent is absent from complete cut")
                })?;
            if rows[parent_key].1.kind != DfsObjectKind::Directory {
                return Err(invalid("live namespace object parent is not a directory"));
            }
        }
        let increment = if object.kind == DfsObjectKind::Hardlink {
            let target = object
                .link_target_object_id
                .as_ref()
                .ok_or_else(|| invalid("namespace alias has no canonical target"))?;
            let target_key = (object.file_system_id.clone(), target.clone());
            let (_, target_object) = rows
                .get(&target_key)
                .ok_or_else(|| invalid("namespace alias target is missing from complete cut"))?;
            if matches!(
                target_object.kind,
                DfsObjectKind::Directory | DfsObjectKind::Hardlink | DfsObjectKind::Tombstone
            ) {
                return Err(invalid(
                    "namespace alias names a noncanonical or directory target",
                ));
            }
            Some(target_key)
        } else if object.kind == DfsObjectKind::Directory {
            parent(&object.path)
                .map(|parent| {
                    let parent_key =
                        paths.get(&(key.0.clone(), parent.into())).ok_or_else(|| {
                            invalid("namespace directory parent is absent from complete cut")
                        })?;
                    if rows[parent_key].1.kind != DfsObjectKind::Directory {
                        return Err(invalid("namespace directory parent is not a directory"));
                    }
                    Ok(parent_key.clone())
                })
                .transpose()?
        } else {
            None
        };
        if let Some(key) = increment {
            let count = counts
                .get_mut(&key)
                .ok_or_else(|| invalid("namespace count has no canonical owner"))?;
            *count = count
                .checked_add(1)
                .ok_or_else(|| invalid("namespace link count overflow"))?;
        }
    }
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
    Ok(NamespaceCountMaintenancePlan {
        catalog: catalog.to_vec(),
        sources: sources
            .iter()
            .map(|source| source.source_cut.clone())
            .collect(),
        candidate_identities: sources
            .iter()
            .map(|source| source.candidate_identity.clone())
            .collect(),
        updates,
    })
}

impl NamespaceCountMaintenancePlan {
    /// Recheck immutable source files immediately before an existing owner uses
    /// this DATA. Source custody, quiescence and native import authorization are
    /// still required independently; this method cannot activate a candidate.
    pub fn verify_retained_sources(&self) -> Result<()> {
        if self.sources.len() != self.candidate_identities.len() {
            return Err(invalid("namespace-count source identity set is incomplete"));
        }
        for (source, expected) in self.sources.iter().zip(&self.candidate_identities) {
            crate::metadata::validate_retained_source_identity(
                &source.source_root,
                &source.source_identity,
                &source.source_snapshot_fingerprint,
            )?;
            if &sinomo_dfs::inspect_metadata_candidate(&source.staged_root)? != expected {
                return Err(invalid(
                    "namespace-count source candidate changed after census",
                ));
            }
        }
        Ok(())
    }

    /// Activation DATA must itself pass the complete census, have no unknown
    /// canonical counts, and retain every original immutable prepared body.
    /// The existing native owner remains solely responsible for installation.
    pub fn validate_activation_data(
        &self,
        originals: &[NamespaceCountMaintenanceSource],
        candidates: &[NamespaceCountMaintenanceSource],
        limits: NamespaceCountMaintenanceLimits,
    ) -> Result<()> {
        if originals.len() != self.sources.len()
            || self.sources.len() != self.candidate_identities.len()
        {
            return Err(invalid(
                "namespace-count activation changed its original source set",
            ));
        }
        for ((original, expected_cut), expected_identity) in originals
            .iter()
            .zip(&self.sources)
            .zip(&self.candidate_identities)
        {
            if serde_json::to_vec(&original.source_cut)
                .map_err(|error| invalid(error.to_string()))?
                != serde_json::to_vec(expected_cut).map_err(|error| invalid(error.to_string()))?
                || &original.candidate_identity != expected_identity
            {
                return Err(invalid(
                    "namespace-count activation changed its original source identity",
                ));
            }
        }
        let original_plan = plan_namespace_count_maintenance(&self.catalog, originals, limits)?;
        if original_plan.updates != self.updates {
            return Err(invalid(
                "namespace-count activation changed its planned updates",
            ));
        }
        let checked = plan_namespace_count_maintenance(&self.catalog, candidates, limits)?;
        if !checked.updates.is_empty() {
            return Err(invalid(
                "namespace-count activation contains uninitialized canonical rows",
            ));
        }
        for source in originals {
            let candidate = candidates
                .iter()
                .find(|candidate| candidate.source_cut.scope_shard == source.source_cut.scope_shard)
                .ok_or_else(|| invalid("namespace-count activation omitted an original scope"))?;
            if candidate.snapshot.tables.objects.len() != source.snapshot.tables.objects.len() {
                return Err(invalid(
                    "namespace-count activation changed the original namespace",
                ));
            }
            for original in &source.snapshot.tables.objects {
                check_deadline(limits)?;
                let mut expected = original.clone();
                if let Some(update) = self.updates.iter().find(|update| {
                    update.shard_id == source.source_cut.scope_shard
                        && update.original.file_system_id == original.file_system_id
                        && update.original.object_id == original.object_id
                }) {
                    expected.namespace_link_count = Some(update.namespace_link_count);
                }
                if !candidate
                    .snapshot
                    .tables
                    .objects
                    .iter()
                    .any(|object| object == &expected)
                {
                    return Err(invalid(
                        "namespace-count activation changed more than an admitted count",
                    ));
                }
            }
            for original in &source.snapshot.tables.prepared_operations_v7 {
                check_deadline(limits)?;
                if !candidate
                    .snapshot
                    .tables
                    .prepared_operations_v7
                    .iter()
                    .any(|row| {
                        row.digest == original.digest
                            && row.payload == original.payload
                            && row.status == original.status
                    })
                {
                    return Err(invalid(
                        "namespace-count activation rewrote or discarded an original prepared operation",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
    fn limits() -> NamespaceCountMaintenanceLimits {
        NamespaceCountMaintenanceLimits {
            max_objects: 100,
            max_encoded_bytes: 4 * 1024 * 1024,
            deadline: Instant::now() + Duration::from_secs(10),
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
    fn object(name: &str, path: &str, kind: DfsObjectKind) -> DfsObjectMetadata {
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
    fn source(
        root: &std::path::Path,
        catalog: &[DfsMetadataShardConfig],
        shard: &str,
        objects: Vec<DfsObjectMetadata>,
    ) -> NamespaceCountMaintenanceSource {
        let writer = DfsMetadataCandidateWriter::open(root).unwrap();
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
        let candidate_identity = writer.write(tables, prefix.clone()).unwrap();
        let snapshot = sinomo_dfs::read_metadata_candidate(root).unwrap();
        let source_cut = MetadataSourceCut {
            namespace_count_cohort: None,
            source_root: root.to_path_buf(),
            source_identity: format!("original-{shard}"),
            scope_shard: DfsMetadataShardId::new(shard),
            file_system_ids: vec![first.file_system_id.clone()],
            source_snapshot_fingerprint: candidate_identity.file_fingerprint.clone(),
            source_format_version: Some(snapshot.format_version),
            source_exact_prefix: Some(prefix.clone()),
            converted_image_fingerprint: candidate_identity.state_fingerprint.clone(),
            current_image_fingerprint: candidate_identity.state_fingerprint.clone(),
            target_exact_prefix: prefix.clone(),
            source_journal_sequence: prefix.journal_sequence,
            source_state_fingerprint: candidate_identity.state_fingerprint.clone(),
            projection_assignments: catalog.to_vec(),
            original_handoff: None,
            staged_root: root.to_path_buf(),
            pending_operations: Vec::new(),
        };
        NamespaceCountMaintenanceSource {
            source_cut,
            candidate_identity,
            snapshot,
        }
    }
    fn fixture() -> (
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
            &catalog,
            &sources,
            at,
            limits(),
        )
        .unwrap();
        let again = crate::namespace_count_cohort::stage_namespace_count_cohort(
            &staging,
            &catalog,
            &sources,
            at,
            limits(),
        )
        .unwrap();
        assert_eq!(first, again);
        for member in &first.members {
            assert!(
                member.initialized_prefix.journal_sequence
                    > member.original_cut.target_exact_prefix.journal_sequence
            );
            first
                .validate_bound_cut(&first.bind_cut(&member.original_cut).unwrap())
                .unwrap();
        }
    }

    #[test]
    fn count_cohort_changed_batch_checkpoint_is_not_a_new_predecessor() {
        let (root, catalog, sources) = fixture();
        let staging = root.join("cohort");
        let at = time::OffsetDateTime::UNIX_EPOCH;
        let seal = crate::namespace_count_cohort::stage_namespace_count_cohort(
            &staging,
            &catalog,
            &sources,
            at,
            limits(),
        )
        .unwrap();
        let checkpoint = staging
            .join(&seal.initialization_id)
            .join("services")
            .join("namespace-count-batch-0.json");
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&checkpoint).unwrap()).unwrap();
        value["after"]["state_fingerprint"] = serde_json::json!("changed-checkpoint");
        std::fs::write(&checkpoint, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(
            crate::namespace_count_cohort::stage_namespace_count_cohort(
                &staging,
                &catalog,
                &sources,
                at,
                limits(),
            )
            .is_err()
        );
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
                    &bad,
                    &sources,
                    time::OffsetDateTime::UNIX_EPOCH,
                    limits(),
                )
                .is_err()
            );
            assert!(!staging.exists());
        }
        let mut bad = catalog;
        bad[1].file_system_id =
            sinomo_dfs::DfsFileSystemId::from_legacy_named("foreign-count-fs").unwrap();
        let staging = root.join("must-not-exist");
        assert!(
            crate::namespace_count_cohort::stage_namespace_count_cohort(
                &staging,
                &bad,
                &sources,
                time::OffsetDateTime::UNIX_EPOCH,
                limits(),
            )
            .is_err()
        );
        assert!(!staging.exists());
    }

    fn reseal(source: &mut NamespaceCountMaintenanceSource) {
        let writer = DfsMetadataCandidateWriter::open(&source.source_cut.staged_root).unwrap();
        source.candidate_identity = writer
            .write(
                (*source.snapshot.tables).clone(),
                source.snapshot.prefix_proof.clone(),
            )
            .unwrap();
        source.snapshot =
            sinomo_dfs::read_metadata_candidate(&source.source_cut.staged_root).unwrap();
        source.source_cut.current_image_fingerprint =
            source.snapshot.semantic_state_fingerprint.clone();
        source.source_cut.target_exact_prefix = source.snapshot.prefix_proof.clone();
    }

    #[test]
    fn complete_census_counts_cross_shard_alias_and_children_without_touching_original_bodies() {
        let (_root, catalog, sources) = fixture();
        let before = sources
            .iter()
            .map(|source| serde_json::to_vec(&source.snapshot).unwrap())
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
            assert_eq!(serde_json::to_vec(&source.snapshot).unwrap(), bytes);
        }
    }
    #[test]
    fn partial_duplicate_corrupt_or_inflight_cut_never_produces_counts() {
        let (_root, catalog, mut sources) = fixture();
        assert!(plan_namespace_count_maintenance(&catalog, &sources[..2], limits()).is_err());
        let mut duplicate = catalog.clone();
        duplicate.push(catalog[0].clone());
        assert!(plan_namespace_count_maintenance(&duplicate, &sources, limits()).is_err());
        sources[0].snapshot.semantic_state_fingerprint = "corrupt".into();
        assert!(plan_namespace_count_maintenance(&catalog, &sources, limits()).is_err());
        reseal(&mut sources[0]);
        sources[0]
            .source_cut
            .pending_operations
            .push(serde_json::json!({"original":"pending"}));
        assert!(plan_namespace_count_maintenance(&catalog, &sources, limits()).is_err());
    }
    #[test]
    fn missing_target_bad_initialized_count_and_exhausted_budget_fail_closed() {
        let (_root, catalog, mut sources) = fixture();
        sources[2]
            .snapshot
            .tables
            .objects
            .iter_mut()
            .find(|row| row.kind == DfsObjectKind::Hardlink)
            .unwrap()
            .link_target_object_id = Some(DfsObjectId::from_legacy_named("missing").unwrap());
        reseal(&mut sources[2]);
        assert!(plan_namespace_count_maintenance(&catalog, &sources, limits()).is_err());
        let (_root, catalog, mut sources) = fixture();
        sources[1]
            .snapshot
            .tables
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
                    deadline: Instant::now(),
                    ..limits()
                }
            )
            .is_err()
        );
    }
    #[test]
    fn activation_requires_initialized_counts_and_preserves_original_prepared_bodies() {
        let (root, catalog, originals) = fixture();
        let plan = plan_namespace_count_maintenance(&catalog, &originals, limits()).unwrap();
        assert!(
            plan.validate_activation_data(&originals, &originals, limits())
                .is_err()
        );
        let mut candidates = originals.clone();
        for (index, source) in candidates.iter_mut().enumerate() {
            source.source_cut.staged_root = root.join(format!("counted-{index}"));
            for object in &mut source.snapshot.tables.objects {
                if let Some(update) = plan.updates.iter().find(|update| {
                    update.shard_id == source.source_cut.scope_shard
                        && update.original.object_id == object.object_id
                }) {
                    object.namespace_link_count = Some(update.namespace_link_count);
                }
            }
            reseal(source);
        }
        plan.validate_activation_data(&originals, &candidates, limits())
            .unwrap();
        for (original, candidate) in originals.iter().zip(&candidates) {
            assert_eq!(
                original.snapshot.tables.prepared_operations_v7,
                candidate.snapshot.tables.prepared_operations_v7
            );
        }
        candidates[0].snapshot.tables.prepared_operations_v7.clear();
        reseal(&mut candidates[0]);
        assert!(
            plan.validate_activation_data(&originals, &candidates, limits())
                .is_err()
        );
    }

    #[test]
    fn running_namespace_job_and_unfinished_prepare_prevent_census() {
        let (_root, catalog, mut sources) = fixture();
        let record = &mut sources[0].snapshot.tables.prepared_operations_v7[0];
        record.status = DfsPreparedOperationStatus::Prepared;
        record.committed_entry = None;
        reseal(&mut sources[0]);
        assert!(plan_namespace_count_maintenance(&catalog, &sources, limits()).is_err());
        let (_root, catalog, mut sources) = fixture();
        sources[0]
            .snapshot
            .tables
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
        sources[0].snapshot.semantic_state_fingerprint = "invalid-first-identity".into();
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
        let first_bytes = serde_json::to_vec(&sources[0].snapshot).unwrap().len();
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
