//! Complete-set maintenance binding carried by the existing import blueprints.
//! The seal is immutable DATA. Native source fences and actual activation
//! receipts remain the only authority to publish or release storage readiness.
use crate::metadata::canonical_generation::{CapturedMetadataSource, build_canonical_generation};
use crate::{MetadataSourceCut, namespace_counts::*};
use serde::{Deserialize, Serialize};
use sinomo_dfs::{
    DfsFileSystemConfig, DfsMetadataGenerationIdentity, DfsMetadataGenerationOrigin,
    DfsMetadataGenerationSeal, DfsMetadataPrefixProof, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsMetadataStorageState, MetadataBackendError,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

type Result<T> = std::result::Result<T, MetadataBackendError>;
fn invalid(message: impl Into<String>) -> MetadataBackendError {
    MetadataBackendError::Serde(message.into())
}
fn digest(value: &impl Serialize) -> Result<String> {
    serde_json::to_vec(value)
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
        .map_err(|error| invalid(error.to_string()))
}

pub(crate) fn validate_catalog(catalog: &[DfsMetadataShardConfig]) -> Result<()> {
    let first = catalog
        .first()
        .ok_or_else(|| invalid("count cohort requires a complete catalog"))?;
    let mut scopes = BTreeSet::new();
    for assignment in catalog {
        let path = Path::new(&assignment.shard_id);
        if assignment.file_system_id != first.file_system_id
            || assignment.shard_id.contains(['\\', ':', '\0'])
            || path.components().count() != 1
            || !matches!(
                path.components().next(),
                Some(std::path::Component::Normal(_))
            )
            || !scopes.insert(&assignment.shard_id)
        {
            return Err(invalid(
                "count cohort requires unique safe scopes in one filesystem",
            ));
        }
    }
    Ok(())
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NamespaceCountCohortMember {
    pub assignment: DfsMetadataShardConfig,
    pub original_source: CapturedMetadataSource,
    pub captured_projection_fingerprint: String,
    pub initialized_generation: DfsMetadataGenerationIdentity,
    pub initialized_seal: DfsMetadataGenerationSeal,
    pub initialized_prefix: DfsMetadataPrefixProof,
    pub initialized_state_fingerprint: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NamespaceCountCohortSeal {
    pub version: u16,
    pub cohort_id: String,
    pub initialization_id: String,
    pub transition_id: String,
    pub source_catalog_generation: u64,
    pub target_generation: String,
    pub target_format: u32,
    pub catalog_digest: String,
    pub catalog: Vec<DfsMetadataShardConfig>,
    pub members: Vec<NamespaceCountCohortMember>,
    #[serde(with = "time::serde::rfc3339")]
    pub initialized_at: time::OffsetDateTime,
}
impl NamespaceCountCohortSeal {
    fn body_digest(&self) -> Result<String> {
        digest(&(
            "namespace-count-cohort-v2",
            self.version,
            &self.initialization_id,
            &self.transition_id,
            self.source_catalog_generation,
            &self.target_generation,
            self.target_format,
            &self.catalog_digest,
            &self.catalog,
            &self.members,
            self.initialized_at,
        ))
    }
    pub fn validate(&self) -> Result<()> {
        validate_catalog(&self.catalog)?;
        if self.version != 2
            || self.initialization_id.len() != 64
            || self.transition_id.is_empty()
            || self.target_generation.is_empty()
            || self.target_format != u32::from(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION)
            || self.catalog_digest != storage_catalog_digest(&self.catalog)?
            || self.cohort_id != self.body_digest()?
            || self.catalog.is_empty()
            || self.members.len() != self.catalog.len()
        {
            return Err(invalid("invalid complete namespace-count cohort seal"));
        }
        let catalog = self
            .catalog
            .iter()
            .map(|config| {
                (
                    (config.file_system_id.clone(), config.shard_id.clone()),
                    config,
                )
            })
            .collect::<BTreeMap<_, _>>();
        if catalog.len() != self.catalog.len() {
            return Err(invalid("count cohort repeats a catalog scope"));
        }
        let mut seen = BTreeSet::new();
        let mut placement_scopes = BTreeMap::<String, BTreeSet<String>>::new();
        for member in &self.members {
            let cut = &member.original_source;
            crate::metadata::validate_placement_reservations(&cut.retained_placement_reservations,
                &cut.file_system_ids, &cut.physical_shard)?;
            for evidence in &cut.retained_placement_reservations {
                let entry = evidence.capsule()["source_entry_digest"].as_str()
                    .ok_or_else(|| invalid("placement evidence omitted original entry digest"))?;
                if !placement_scopes.entry(entry.to_owned()).or_default().insert(cut.physical_shard.0.clone()) {
                    return Err(invalid("cohort repeats original placement evidence"));
                }
            }
            crate::metadata::validate_count_cohort_prefix(&member.initialized_prefix)?;
            member.initialized_generation.validate()?;
            let key = (
                member.assignment.file_system_id.clone(),
                member.assignment.shard_id.clone(),
            );
            if catalog.get(&key).copied() != Some(&member.assignment)
                || !seen.insert(key)
                || storage_catalog_digest(&cut.projection_assignments)? != self.catalog_digest
                || cut.physical_shard.0 != member.assignment.shard_id
                || cut.file_system_ids != vec![member.assignment.file_system_id.clone()]
                || cut.original_handoff.is_some()
                || member.initialized_prefix.version != 2
                || member.initialized_prefix.journal_sequence != cut.source_journal_sequence
                || member.captured_projection_fingerprint.is_empty()
                || member.initialized_state_fingerprint.is_empty()
                || member.initialized_generation.file_system_id != member.assignment.file_system_id
                || member.initialized_generation.shard_id != cut.physical_shard
                || member.initialized_generation.generation != self.target_generation
                || member.initialized_generation.origin
                    != (DfsMetadataGenerationOrigin::StorageTransition {
                        transition_id: self.transition_id.clone(),
                    })
                || member.initialized_seal.prefix_proof != member.initialized_prefix
                || member.initialized_seal.state_fingerprint != member.initialized_state_fingerprint
                || member.initialized_seal.file_fingerprint.is_empty()
            {
                return Err(invalid(
                    "count cohort changes or omits an original source scope",
                ));
            }
        }
        let expected_scopes = self.catalog.iter().map(|shard| shard.shard_id.clone()).collect::<BTreeSet<_>>();
        if placement_scopes.values().any(|scopes| scopes != &expected_scopes) {
            return Err(invalid("placement evidence omits or changes an original catalog member"));
        }
        Ok(())
    }
    pub fn member(&self, shard: &DfsMetadataShardId) -> Result<&NamespaceCountCohortMember> {
        self.validate()?;
        self.members
            .iter()
            .find(|member| member.assignment.shard_id == shard.0)
            .ok_or_else(|| invalid("count cohort does not contain requested physical scope"))
    }
    pub fn bind_cut(&self, cut: &MetadataSourceCut) -> Result<MetadataSourceCut> {
        let member = self.member(&cut.scope_shard)?;
        if !member.original_source.matches_source_cut(cut)
            || cut.namespace_count_cohort.is_some()
            || !cut.pending_operations.is_empty()
        {
            return Err(invalid(
                "count cohort source differs from its original captured cut",
            ));
        }
        let mut bound = cut.clone();
        bound.current_image_fingerprint = member.initialized_state_fingerprint.clone();
        bound.target_exact_prefix = member.initialized_prefix.clone();
        bound.staged_root = member.original_source.planned_target_root.clone();
        bound.namespace_count_cohort = Some(Box::new(self.clone()));
        Ok(bound)
    }
    pub fn validate_bound_cut(&self, cut: &MetadataSourceCut) -> Result<()> {
        let member = self.member(&cut.scope_shard)?;
        if !member.original_source.matches_source_cut(cut)
            || !cut.pending_operations.is_empty()
            || cut.current_image_fingerprint != member.initialized_state_fingerprint
            || cut.target_exact_prefix != member.initialized_prefix
            || cut.staged_root != member.original_source.planned_target_root
            || cut.namespace_count_cohort.as_deref() != Some(self)
        {
            return Err(invalid(
                "count cohort bound source/candidate identity changed",
            ));
        }
        Ok(())
    }
}

/// Stable complete-catalog DATA digest shared with the actual Pending CAS owner.
pub(crate) fn storage_catalog_digest(catalog: &[DfsMetadataShardConfig]) -> Result<String> {
    validate_catalog(catalog)?;
    let mut ordered = catalog.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    Ok(format!("blake3:{}", digest(&("metadata-storage-catalog-v1", ordered))?))
}

/// Build clean initialized DATA under the generation already selected by the
/// owner. The Pending configuration argument is DATA, never an activation permit.
pub fn stage_namespace_count_cohort(
    staging_root: &Path,
    pending_file_system: &DfsFileSystemConfig,
    catalog: &[DfsMetadataShardConfig],
    sources: &[NamespaceCountMaintenanceSource],
    at: time::OffsetDateTime,
    limits: NamespaceCountMaintenanceLimits,
) -> Result<NamespaceCountCohortSeal> {
    stage_namespace_count_cohort_at_roots(
        staging_root,
        pending_file_system,
        catalog,
        sources,
        at,
        limits,
        &BTreeMap::new(),
    )
}

pub(crate) fn stage_namespace_count_cohort_at_roots(
    staging_root: &Path,
    pending_file_system: &DfsFileSystemConfig,
    catalog: &[DfsMetadataShardConfig],
    sources: &[NamespaceCountMaintenanceSource],
    at: time::OffsetDateTime,
    limits: NamespaceCountMaintenanceLimits,
    owned_roots: &BTreeMap<DfsMetadataShardId, std::path::PathBuf>,
) -> Result<NamespaceCountCohortSeal> {
    validate_catalog(catalog)?;
    let Some(DfsMetadataStorageState::PendingTransition {
        transition_id,
        source_catalog_generation,
        target_generation,
        target_format,
        expected_catalog_digest,
        ..
    }) = &pending_file_system.metadata_storage
    else {
        return Err(invalid(
            "count staging requires the owner's captured Pending transition DATA",
        ));
    };
    pending_file_system
        .metadata_storage
        .as_ref()
        .unwrap()
        .validate_at_catalog_generation(pending_file_system.catalog_generation)
        .map_err(invalid)?;
    if pending_file_system.file_system_id != catalog[0].file_system_id
        || *source_catalog_generation != pending_file_system.catalog_generation
        || *target_format != u32::from(sinomo_dfs::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION)
        || *expected_catalog_digest != storage_catalog_digest(catalog)?
    {
        return Err(invalid(
            "count staging differs from its original Pending catalog/generation",
        ));
    }
    let plan = plan_namespace_count_maintenance(catalog, sources, limits)?;
    let projection_bindings = sources
        .iter()
        .map(|source| {
            (
                source.captured.source.physical_shard.clone(),
                source.captured.projection_fingerprint().to_owned(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let initialization_id = digest(&(
        "namespace-count-initialization-v2",
        transition_id,
        source_catalog_generation,
        target_generation,
        target_format,
        expected_catalog_digest,
        &projection_bindings,
        &plan.updates,
    ))?;
    let mut members = Vec::new();
    for source in sources {
        if limits.deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return Err(MetadataBackendError::PreparationBudgetExhausted { work: "count-cohort staging" });
        }
        let captured = &source.captured;
        captured.validate()?;
        let assignment = catalog
            .iter()
            .find(|config| config.shard_id == captured.source.physical_shard.0)
            .ok_or_else(|| invalid("count cohort source has no actual catalog scope"))?;
        let root = owned_roots
            .get(&captured.source.physical_shard)
            .cloned()
            .unwrap_or_else(|| staging_root.join(transition_id).join(&assignment.shard_id));
        let owned = owned_roots.contains_key(&captured.source.physical_shard);
        if owned {
            captured.verify_retained_source()?;
        }
        let counted = plan.counted_objects(source, limits)?;
        let generation = DfsMetadataGenerationIdentity {
            file_system_id: assignment.file_system_id.clone(),
            shard_id: captured.source.physical_shard.clone(),
            generation: target_generation.clone(),
            origin: DfsMetadataGenerationOrigin::StorageTransition {
                transition_id: transition_id.clone(),
            },
        };
        let output = build_canonical_generation(captured, &generation, &counted, &root)?;
        if owned {
            captured.verify_retained_source()?;
        }
        members.push(NamespaceCountCohortMember {
            assignment: assignment.clone(),
            original_source: captured.source.clone(),
            captured_projection_fingerprint: captured.projection_fingerprint().to_owned(),
            initialized_generation: generation,
            initialized_seal: output.seal,
            initialized_prefix: output.identity.prefix_proof,
            initialized_state_fingerprint: output.identity.state_fingerprint,
        });
    }
    members.sort_by(|left, right| left.assignment.shard_id.cmp(&right.assignment.shard_id));
    let mut canonical_catalog = catalog.to_vec();
    canonical_catalog.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    let mut seal = NamespaceCountCohortSeal {
        version: 2,
        cohort_id: String::new(),
        initialization_id,
        transition_id: transition_id.clone(),
        source_catalog_generation: *source_catalog_generation,
        target_generation: target_generation.clone(),
        target_format: *target_format,
        catalog_digest: expected_catalog_digest.clone(),
        catalog: canonical_catalog,
        members,
        initialized_at: at,
    };
    seal.cohort_id = seal.body_digest()?;
    seal.validate()?;
    Ok(seal)
}
