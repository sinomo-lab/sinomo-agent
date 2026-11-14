//! Complete-set maintenance binding carried by the existing import blueprints.
//! The seal is immutable DATA. Native source fences and actual activation
//! receipts remain the only authority to publish or release storage readiness.
use crate::{MetadataSourceCut, namespace_counts::*};
use serde::{Deserialize, Serialize};
use sinomo_dfs::{
    DfsMetadataCandidateIdentity, DfsMetadataPrefixProof, DfsMetadataShardConfig,
    DfsMetadataShardId, DfsPreparedOperationScope, MetadataBackendError,
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
    pub original_cut: MetadataSourceCut,
    pub initialized_prefix: DfsMetadataPrefixProof,
    pub initialized_state_fingerprint: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NamespaceCountCohortSeal {
    pub version: u16,
    pub cohort_id: String,
    pub initialization_id: String,
    pub catalog: Vec<DfsMetadataShardConfig>,
    pub members: Vec<NamespaceCountCohortMember>,
    #[serde(with = "time::serde::rfc3339")]
    pub initialized_at: time::OffsetDateTime,
}
impl NamespaceCountCohortSeal {
    fn body_digest(&self) -> Result<String> {
        digest(&(
            "namespace-count-cohort-v1",
            self.version,
            &self.initialization_id,
            &self.catalog,
            &self.members,
            self.initialized_at,
        ))
    }
    pub fn validate(&self) -> Result<()> {
        validate_catalog(&self.catalog)?;
        if self.version != 1
            || self.initialization_id.len() != 64
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
        for member in &self.members {
            let cut = &member.original_cut;
            crate::metadata::validate_count_cohort_prefix(&member.initialized_prefix)?;
            let key = (
                member.assignment.file_system_id.clone(),
                member.assignment.shard_id.clone(),
            );
            if catalog.get(&key).copied() != Some(&member.assignment)
                || !seen.insert(key)
                || cut.namespace_count_cohort.is_some()
                || cut.projection_assignments != self.catalog
                || cut.scope_shard.0 != member.assignment.shard_id
                || cut.file_system_ids != vec![member.assignment.file_system_id.clone()]
                || !cut.pending_operations.is_empty()
                || cut.original_handoff.is_some()
                || member.initialized_prefix.journal_sequence
                    <= cut.target_exact_prefix.journal_sequence
                || member.initialized_state_fingerprint.is_empty()
            {
                return Err(invalid(
                    "count cohort changes or omits an original source scope",
                ));
            }
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
        if cut != &member.original_cut {
            return Err(invalid(
                "count cohort source differs from its original captured cut",
            ));
        }
        let mut bound = cut.clone();
        bound.current_image_fingerprint = member.initialized_state_fingerprint.clone();
        bound.target_exact_prefix = member.initialized_prefix.clone();
        bound.staged_root = cut
            .staged_root
            .parent()
            .ok_or_else(|| invalid("count source has no owned staging parent"))?
            .join("namespace-count-current-image");
        bound.namespace_count_cohort = Some(Box::new(self.clone()));
        Ok(bound)
    }
    pub fn validate_bound_cut(&self, cut: &MetadataSourceCut) -> Result<()> {
        let member = self.member(&cut.scope_shard)?;
        if self.bind_cut(&member.original_cut)? != *cut {
            return Err(invalid(
                "count cohort bound source/candidate identity changed",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CountBatchReceipt {
    before: DfsMetadataCandidateIdentity,
    after: DfsMetadataCandidateIdentity,
}

/// Compute deterministic initialized candidate DATA in exclusively owned staging
/// roots. Original source images are only read. Actual retiring primaries must
/// independently bind their own source and reach the complete native stage gate.
pub fn stage_namespace_count_cohort(
    staging_root: &Path,
    catalog: &[DfsMetadataShardConfig],
    sources: &[NamespaceCountMaintenanceSource],
    at: time::OffsetDateTime,
    limits: NamespaceCountMaintenanceLimits,
) -> Result<NamespaceCountCohortSeal> {
    stage_namespace_count_cohort_at_roots(
        staging_root,
        catalog,
        sources,
        at,
        limits,
        &std::collections::BTreeMap::new(),
    )
}

pub(crate) fn stage_namespace_count_cohort_at_roots(
    staging_root: &Path,
    catalog: &[DfsMetadataShardConfig],
    sources: &[NamespaceCountMaintenanceSource],
    at: time::OffsetDateTime,
    limits: NamespaceCountMaintenanceLimits,
    owned_roots: &std::collections::BTreeMap<DfsMetadataShardId, std::path::PathBuf>,
) -> Result<NamespaceCountCohortSeal> {
    validate_catalog(catalog)?;
    let plan = plan_namespace_count_maintenance(catalog, sources, limits)?;
    if sources
        .iter()
        .any(|source| source.source_cut.namespace_count_cohort.is_some())
    {
        return Err(invalid(
            "count cohort cannot rebase an existing cohort onto another invocation",
        ));
    }
    let initialization_id = digest(&(
        "namespace-count-initialization-v1",
        catalog,
        sources
            .iter()
            .map(|source| (&source.source_cut, &source.candidate_identity))
            .collect::<Vec<_>>(),
        at,
    ))?;
    let mut members = Vec::new();
    for source in sources {
        if std::time::Instant::now() >= limits.deadline {
            return Err(invalid(
                "count cohort staging reached its original deadline",
            ));
        }
        let assignment = catalog
            .iter()
            .find(|config| config.shard_id == source.source_cut.scope_shard.0)
            .ok_or_else(|| invalid("count cohort candidate has no actual catalog scope"))?;
        let root = owned_roots
            .get(&source.source_cut.scope_shard)
            .cloned()
            .unwrap_or_else(|| {
                staging_root
                    .join(&initialization_id)
                    .join(&assignment.shard_id)
            });
        let writer = sinomo_dfs::DfsMetadataCandidateWriter::open(&root)?;
        let base_path = root.join("namespace-count-base-identity.json");
        let mut identity: DfsMetadataCandidateIdentity = if base_path.exists() {
            serde_json::from_slice(
                &std::fs::read(&base_path).map_err(|error| invalid(error.to_string()))?,
            )
            .map_err(|error| invalid(error.to_string()))?
        } else {
            if owned_roots.contains_key(&source.source_cut.scope_shard) {
                crate::metadata::copy_count_source_journal(&source.source_cut, &root)?;
            }
            let identity = writer.write(
                (*source.snapshot.tables).clone(),
                source.snapshot.prefix_proof.clone(),
            )?;
            crate::metadata::write_count_cohort_checkpoint(&base_path, &identity)?;
            identity
        };
        let selected = plan
            .updates
            .iter()
            .filter(|update| update.shard_id == source.source_cut.scope_shard)
            .collect::<Vec<_>>();
        for (ordinal, batch) in selected
            .chunks(sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET)
            .enumerate()
        {
            let before_rows = batch
                .iter()
                .map(|row| row.original.clone())
                .collect::<Vec<_>>();
            let after = batch
                .iter()
                .map(|row| {
                    let mut object = row.original.clone();
                    object.namespace_link_count = Some(row.namespace_link_count);
                    object
                })
                .collect::<Vec<_>>();
            let receipt_path = root.join(format!("namespace-count-batch-{ordinal}.json"));
            let retained: Option<CountBatchReceipt> = if receipt_path.exists() {
                Some(
                    serde_json::from_slice(
                        &std::fs::read(&receipt_path)
                            .map_err(|error| invalid(error.to_string()))?,
                    )
                    .map_err(|error| invalid(error.to_string()))?,
                )
            } else {
                None
            };
            let before = identity.clone();
            if retained
                .as_ref()
                .is_some_and(|receipt| receipt.before != before)
            {
                return Err(invalid(
                    "count initialization checkpoint changed its exact predecessor",
                ));
            }
            let actual = writer.initialize_namespace_counts(
                &before,
                DfsPreparedOperationScope {
                    file_system_id: assignment.file_system_id.clone(),
                    shard_id: source.source_cut.scope_shard.clone(),
                    owner_epoch: assignment
                        .epoch
                        .checked_add(1)
                        .ok_or_else(|| invalid("count cohort epoch exhausted"))?,
                    operation_id: format!(
                        "namespace-count-init:{initialization_id}:{}:{ordinal}",
                        assignment.shard_id
                    ),
                },
                &before_rows,
                &after,
                at,
            )?;
            let receipt = retained.unwrap_or(CountBatchReceipt {
                before,
                after: actual.clone(),
            });
            crate::metadata::write_count_cohort_checkpoint(&receipt_path, &receipt)?;
            if actual != receipt.after {
                if actual.prefix_proof.journal_sequence
                    <= receipt.after.prefix_proof.journal_sequence
                {
                    return Err(invalid(
                        "count batch checkpoint changed its actual output identity",
                    ));
                }
                let snapshot = sinomo_dfs::read_metadata_candidate(&root)?;
                let next_id = if ordinal + 1
                    < selected
                        .chunks(sinomo_dfs::DFS_STORAGE_PREPARATION_RECORD_BUDGET)
                        .len()
                {
                    format!(
                        "namespace-count-init:{initialization_id}:{}:{}",
                        assignment.shard_id,
                        ordinal + 1
                    )
                } else {
                    sinomo_dfs::DfsMetadataCandidateWriter::namespace_count_cohort_operation_id(
                        &initialization_id,
                        &source.source_cut.scope_shard,
                        &receipt.after,
                    )?
                };
                let binding = receipt.after.namespace_count_predecessor_binding()?;
                if !snapshot.tables.prepared_operations_v7.iter().any(|record| {
                    record.operation_id == next_id
                        && record.digest.prepare_sequence
                            == receipt.after.prefix_proof.journal_sequence + 1
                        && record.payload.journal_operation_id.as_ref() == Some(&binding)
                        && record.status == sinomo_dfs::DfsPreparedOperationStatus::Committed
                }) {
                    return Err(invalid(
                        "count batch checkpoint has no exact immutable successor binding",
                    ));
                }
            }
            identity = receipt.after;
        }
        let binding_path = root.join("namespace-count-binding.json");
        let before = identity.clone();
        let binding_id =
            sinomo_dfs::DfsMetadataCandidateWriter::namespace_count_cohort_operation_id(
                &initialization_id,
                &source.source_cut.scope_shard,
                &before,
            )?;
        let actual = writer.record_namespace_count_cohort(
            &before,
            DfsPreparedOperationScope {
                file_system_id: assignment.file_system_id.clone(),
                shard_id: source.source_cut.scope_shard.clone(),
                owner_epoch: assignment
                    .epoch
                    .checked_add(1)
                    .ok_or_else(|| invalid("count cohort epoch exhausted"))?,
                operation_id: binding_id,
            },
            at,
        )?;
        let binding = CountBatchReceipt {
            before,
            after: actual.clone(),
        };
        crate::metadata::write_count_cohort_checkpoint(&binding_path, &binding)?;
        identity = actual;
        if sinomo_dfs::inspect_metadata_candidate(&root)? != identity {
            return Err(invalid(
                "count cohort candidate has an unrecorded later owner",
            ));
        }
        members.push(NamespaceCountCohortMember {
            assignment: assignment.clone(),
            original_cut: source.source_cut.clone(),
            initialized_prefix: identity.prefix_proof,
            initialized_state_fingerprint: identity.state_fingerprint,
        });
    }
    let mut seal = NamespaceCountCohortSeal {
        version: 1,
        cohort_id: String::new(),
        initialization_id,
        catalog: catalog.to_vec(),
        members,
        initialized_at: at,
    };
    seal.cohort_id = seal.body_digest()?;
    seal.validate()?;
    Ok(seal)
}
