use super::*;
use crate::DfsNamespaceJobStatus;
use crate::publication_guard::{
    DfsPublicationOwnerScope, DfsSourceFamilyFence, ShardFenceGuard, shard_fence_guard,
};
use std::sync::{Arc, Mutex};

/// Native source or job custody; it cannot authorize effect. Only a capture
/// under the real backend writer constructs it. The capture reads under the
/// shard's writer and fence, fences the rows it captured, and then releases
/// that writer: across the wait on another node the custody holds only those
/// row fences, so every other publication on the shard goes ahead.
#[derive(Clone, Debug)]
pub(crate) struct DfsSourceAdoptionWriterCustody {
    scope: DfsPublicationOwnerScope,
    prefix: DfsMetadataPrefixProof,
    held: Arc<Mutex<Option<(MetadataWriteLock, ShardFenceGuard)>>>,
    fences: Arc<Mutex<Vec<DfsSourceFamilyFence>>>,
}
impl DfsSourceAdoptionWriterCustody {
    pub(crate) fn scope(&self) -> &DfsPublicationOwnerScope {
        &self.scope
    }
    /// Fence `object_id`'s family and its namespace entry at `path`. Called
    /// while the capture still holds the shard's writer.
    fn fence_object_family(&self, object_id: &DfsObjectId, path: &str) -> Result<()> {
        self.require_held()?;
        self.fences
            .lock()
            .map_err(|_| MetadataBackendError::Serde("source fences poisoned".into()))?
            .push(DfsSourceFamilyFence::object_family(
                &self.scope,
                object_id,
                path,
            ));
        Ok(())
    }
    /// Fence the job row `job_id`, while the capture still holds the writer.
    fn fence_job_row(&self, job_id: &str) -> Result<()> {
        self.require_held()?;
        self.fences
            .lock()
            .map_err(|_| MetadataBackendError::Serde("source fences poisoned".into()))?
            .push(DfsSourceFamilyFence::job_row(&self.scope, job_id));
        Ok(())
    }
    fn require_held(&self) -> Result<()> {
        if self
            .held
            .lock()
            .map_err(|_| MetadataBackendError::Serde("source writer custody poisoned".into()))?
            .is_none()
        {
            return Err(MetadataBackendError::Serde(
                "source rows are fenced only under the shard writer that captured them".into(),
            ));
        }
        Ok(())
    }
    /// End the capture: the shard's writer and fence are released, and only
    /// the row fences remain.
    fn release_shard_writer(&self) -> Result<()> {
        self.held
            .lock()
            .map_err(|_| MetadataBackendError::Serde("source writer custody poisoned".into()))?
            .take();
        Ok(())
    }
    pub(crate) fn validate_scope(&self, scope: &DfsPublicationOwnerScope) -> Result<()> {
        if &self.scope != scope {
            return Err(MetadataBackendError::Serde(
                "source writer custody names another actual physical owner scope".into(),
            ));
        }
        Ok(())
    }
}

/// Exact native source-family custody. Incoming family data cannot construct
/// this value or acquire its real writer/fence; the Actor supplies authority.
#[derive(Debug)]
pub struct DfsSourceAdoptionSourceCapture {
    family: DfsCrossShardSourceFamily,
    custody: DfsSourceAdoptionWriterCustody,
}
impl DfsSourceAdoptionSourceCapture {
    pub(crate) fn family(&self) -> &DfsCrossShardSourceFamily {
        &self.family
    }
    pub(crate) fn scope(&self) -> &DfsPublicationOwnerScope {
        self.custody.scope()
    }
    pub(crate) fn prefix(&self) -> &DfsMetadataPrefixProof {
        &self.custody.prefix
    }
}

/// Actual original coordinator-job custody, captured independently of any
/// incoming source family. This opaque value grants no publication permission.
#[derive(Debug)]
pub struct DfsSourceAdoptionJobCapture {
    job: DfsNamespaceJobRecord,
    custody: DfsSourceAdoptionWriterCustody,
}
impl DfsSourceAdoptionJobCapture {
    pub(crate) fn job(&self) -> &DfsNamespaceJobRecord {
        &self.job
    }
    pub(crate) fn scope(&self) -> &DfsPublicationOwnerScope {
        self.custody.scope()
    }
    pub(crate) fn prefix(&self) -> &DfsMetadataPrefixProof {
        &self.custody.prefix
    }
    pub(crate) fn writer_custody(&self) -> DfsSourceAdoptionWriterCustody {
        self.custody.clone()
    }
}

/// Exact source and original coordinator-job custody. Serializable rows never
/// create this value, and its row fences survive until the exact target
/// outcome.
#[derive(Debug)]
pub struct DfsSourceAdoptionCapture {
    family: DfsCrossShardSourceFamily,
    job: DfsNamespaceJobRecord,
    source: DfsSourceAdoptionWriterCustody,
    job_root: Option<DfsSourceAdoptionWriterCustody>,
}
impl DfsSourceAdoptionCapture {
    pub(crate) fn into_parts(
        self,
    ) -> (DfsSourceAdoptionSourceCapture, DfsSourceAdoptionJobCapture) {
        let job_custody = self.job_root.unwrap_or_else(|| self.source.clone());
        (
            DfsSourceAdoptionSourceCapture {
                family: self.family,
                custody: self.source,
            },
            DfsSourceAdoptionJobCapture {
                job: self.job,
                custody: job_custody,
            },
        )
    }
    pub(crate) fn family(&self) -> &DfsCrossShardSourceFamily {
        &self.family
    }
    pub(crate) fn prefix(&self) -> &DfsMetadataPrefixProof {
        &self.source.prefix
    }
    pub(crate) fn job(&self) -> &DfsNamespaceJobRecord {
        &self.job
    }
    pub(crate) fn from_held(
        source: DfsSourceAdoptionWriterCustody,
        family: DfsCrossShardSourceFamily,
        job_root: Option<DfsSourceAdoptionWriterCustody>,
        job: DfsNamespaceJobRecord,
    ) -> Result<Self> {
        if family.source_object.file_system_id != *source.scope.file_system_id()
            || source.prefix.journal_sequence != family.source_journal_sequence
            || job.file_system_id != family.source_object.file_system_id
            || job.status != DfsNamespaceJobStatus::Running
            || job_root
                .as_ref()
                .is_some_and(|root| root.scope.file_system_id() != &job.file_system_id)
        {
            return Err(MetadataBackendError::Serde(
                "actual source/job custody differs from captured family".into(),
            ));
        }
        source.fence_object_family(&family.source_object.object_id, &family.source_object.path)?;
        job_root
            .as_ref()
            .unwrap_or(&source)
            .fence_job_row(&job.job_id)?;
        source.release_shard_writer()?;
        if let Some(root) = job_root.as_ref() {
            root.release_shard_writer()?;
        }
        Ok(Self {
            family,
            job,
            source,
            job_root,
        })
    }
}
impl LocalSobjMetadataBackend {
    pub(crate) fn acquire_source_adoption_writer(
        &mut self,
        fs: &DfsFileSystemId,
    ) -> Result<DfsSourceAdoptionWriterCustody> {
        let scope = self.publication_scope_for_backend(fs, &self.shard_id)?;
        let fence = shard_fence_guard(&scope);
        let writer = self.begin_metadata_write()?;
        let prefix = self.prefix_proof.clone().ok_or_else(|| {
            MetadataBackendError::Serde("source/job capture has no actual certified prefix".into())
        })?;
        if prefix.version != 2
            || !prefix.is_well_formed()
            || prefix.journal_sequence != self.next_sequence.saturating_sub(1)
        {
            return Err(MetadataBackendError::Serde(
                "source/job writer does not hold its complete native cut".into(),
            ));
        }
        Ok(DfsSourceAdoptionWriterCustody {
            scope,
            prefix,
            held: Arc::new(Mutex::new(Some((writer, fence)))),
            fences: Arc::default(),
        })
    }
    pub(crate) fn source_adoption_family_while_held(
        &self,
        custody: &DfsSourceAdoptionWriterCustody,
        object_id: &DfsObjectId,
        source_path: &str,
    ) -> Result<DfsCrossShardSourceFamily> {
        custody.validate_scope(
            &self.publication_scope_for_backend(custody.scope.file_system_id(), &self.shard_id)?,
        )?;
        let family: DfsCrossShardSourceFamily =
            crate::metadata_shard_migration::collect_object_shard_migration_bundle(
                self,
                object_id,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?
            .into();
        if family.source_object.object_id != *object_id
            || family.source_object.file_system_id != *custody.scope.file_system_id()
            || normalize_path(&family.source_object.path) != normalize_path(source_path)
            || family.source_journal_sequence != custody.prefix.journal_sequence
        {
            return Err(MetadataBackendError::Serde(
                "source capture changed exact held object/path/prefix".into(),
            ));
        }
        Ok(family)
    }
    pub(crate) fn source_adoption_job_while_held(
        &self,
        custody: &DfsSourceAdoptionWriterCustody,
        job_id: &str,
    ) -> Result<DfsNamespaceJobRecord> {
        custody.validate_scope(
            &self.publication_scope_for_backend(custody.scope.file_system_id(), &self.shard_id)?,
        )?;
        let job = self
            .tables
            .namespace_jobs
            .get(job_id)
            .cloned()
            .ok_or_else(|| {
                MetadataBackendError::Serde(
                    "original coordinator root has no actual running job".into(),
                )
            })?;
        if job.status != DfsNamespaceJobStatus::Running
            || &job.file_system_id != custody.scope.file_system_id()
        {
            return Err(MetadataBackendError::Serde(
                "original coordinator job identity/status changed".into(),
            ));
        }
        Ok(job)
    }
    pub(crate) fn capture_source_adoption_source_for_backend(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
    ) -> Result<DfsSourceAdoptionSourceCapture> {
        let planned =
            self.get_object(object_id)?
                .ok_or_else(|| MetadataBackendError::UnknownObject {
                    object_id: object_id.clone(),
                })?;
        if normalize_path(&planned.path) != normalize_path(source_path) {
            return Err(MetadataBackendError::Serde(
                "source capture path differs from the actual source object".into(),
            ));
        }
        let custody = self.acquire_source_adoption_writer(&planned.file_system_id)?;
        let family = self.source_adoption_family_while_held(&custody, object_id, source_path)?;
        if family.source_object != planned {
            return Err(MetadataBackendError::Serde(
                "source object changed while its original physical writer custody queued".into(),
            ));
        }
        custody.fence_object_family(object_id, &family.source_object.path)?;
        custody.release_shard_writer()?;
        Ok(DfsSourceAdoptionSourceCapture { family, custody })
    }

    /// Capture only the real current job under this physical backend's writer.
    /// The complete row is revalidated after the existing writer/fence queues.
    pub(crate) fn capture_source_adoption_job_for_backend(
        &mut self,
        job_id: &str,
    ) -> Result<DfsSourceAdoptionJobCapture> {
        let planned = self.get_namespace_job(job_id)?.ok_or_else(|| {
            MetadataBackendError::Serde("original coordinator has no actual running job".into())
        })?;
        if planned.status != DfsNamespaceJobStatus::Running {
            return Err(MetadataBackendError::Serde(
                "original coordinator job is not running".into(),
            ));
        }
        let custody = self.acquire_source_adoption_writer(&planned.file_system_id)?;
        let job = self.source_adoption_job_while_held(&custody, job_id)?;
        if job != planned {
            return Err(MetadataBackendError::Serde(
                "original coordinator job changed while its writer custody queued".into(),
            ));
        }
        custody.fence_job_row(job_id)?;
        custody.release_shard_writer()?;
        Ok(DfsSourceAdoptionJobCapture { job, custody })
    }

    pub(crate) fn capture_source_adoption_for_backend(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
        job_id: &str,
    ) -> Result<DfsSourceAdoptionCapture> {
        let fs = self
            .get_object(object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            })?
            .file_system_id;
        let custody = self.acquire_source_adoption_writer(&fs)?;
        let family = self.source_adoption_family_while_held(&custody, object_id, source_path)?;
        let job = self.source_adoption_job_while_held(&custody, job_id)?;
        DfsSourceAdoptionCapture::from_held(custody, family, None, job)
    }
}

impl LocalSobjMetadataBackend {
    /// Read native terminal identity from the actual held inspection cut:
    /// the outcome of the adoption's whole prepare chain. Neither a pending
    /// durable suffix nor a prepare that can still commit establishes that
    /// an effect never happened.
    pub(crate) fn source_adoption_outcome(
        &self,
        scope: &crate::DfsPreparedOperationScope,
        payload_digest: &str,
        expected_digest: Option<&crate::DfsPreparedOperationDigest>,
    ) -> Result<crate::source_adoption::DfsSourceAdoptionOutcome> {
        use crate::source_adoption::DfsSourceAdoptionOutcome as Outcome;
        let Some(prefix) = self.prefix_proof.as_ref() else {
            tracing::warn!(
                target: "sinomo.dfs.source-adoption",
                operation = %scope.operation_id.chars().take(192).collect::<String>(),
                reason = "missing-certified-prefix",
                materialized_sequence = self.materialized_through_sequence,
                next_sequence = self.next_sequence,
                "native target outcome remains unavailable"
            );
            return Ok(Outcome::Unavailable);
        };
        if scope.shard_id != self.shard_id
            || prefix.version != 2
            || !prefix.is_well_formed()
            || prefix.journal_sequence != self.materialized_through_sequence
        {
            tracing::warn!(
                target: "sinomo.dfs.source-adoption",
                operation = %scope.operation_id.chars().take(192).collect::<String>(),
                reason = "uncertified-inspection-cut",
                shard_equal = (scope.shard_id == self.shard_id),
                prefix_version = prefix.version,
                prefix_sequence = prefix.journal_sequence,
                materialized_sequence = self.materialized_through_sequence,
                "native target outcome remains unavailable"
            );
            return Ok(Outcome::Unavailable);
        }
        let record = self.tables.prepared_operations_v7.get(&scope.operation_id);
        let eviction = self
            .tables
            .prepared_operation_evictions
            .get(&scope.operation_id);
        let (digest, commit, status) = match (record, eviction) {
            (Some(record), None) => {
                record.validate()?;
                (
                    Some(&record.digest),
                    record.committed_entry.as_deref(),
                    Some(record.status),
                )
            }
            (None, Some(eviction)) => {
                eviction.validate()?;
                (
                    Some(&eviction.digest),
                    eviction.committed_entry.as_deref(),
                    Some(eviction.status),
                )
            }
            (None, None) => (None, None, None),
            _ => {
                return Err(MetadataBackendError::Serde(
                    "native target has competing retained outcomes".into(),
                ));
            }
        };
        if let Some(digest) = digest {
            if digest.file_system_id != scope.file_system_id
                || digest.shard_id != scope.shard_id
                || digest.owner_epoch != scope.owner_epoch
                || digest.operation_id != scope.operation_id
                || digest.payload_digest != payload_digest
                || expected_digest.is_some_and(|expected| expected != digest)
            {
                return Err(MetadataBackendError::Serde(
                    "native target outcome changed exact source claim scope/digest".into(),
                ));
            }
            if let Some(commit) = commit {
                commit.validate(digest)?;
                if status != Some(crate::DfsPreparedOperationStatus::Committed)
                    || commit.sequence > prefix.journal_sequence
                {
                    tracing::warn!(
                        target: "sinomo.dfs.source-adoption",
                        operation = %scope.operation_id.chars().take(192).collect::<String>(),
                        reason = "commit-outside-certified-cut",
                        status = ?status,
                        commit_sequence = commit.sequence,
                        prefix_sequence = prefix.journal_sequence,
                        "native target outcome remains unavailable"
                    );
                    return Ok(Outcome::Unavailable);
                }
                return Ok(Outcome::Committed {
                    digest: digest.clone(),
                    commit: commit.clone(),
                });
            }
        }
        if digest.is_none()
            && expected_digest.is_some_and(|expected| {
                expected.prepare_sequence <= self.materialized_through_sequence
            })
        {
            // A missing/evicted operation below a certified head is unknown,
            // never evidence that its effect did not happen.
            tracing::warn!(
                target: "sinomo.dfs.source-adoption",
                operation = %scope.operation_id.chars().take(192).collect::<String>(),
                reason = "missing-operation-below-certified-head",
                expected_prepare_sequence = ?expected_digest.map(|digest| digest.prepare_sequence),
                materialized_sequence = self.materialized_through_sequence,
                "native target outcome remains unavailable"
            );
            return Ok(Outcome::Unavailable);
        }
        if self.next_sequence.saturating_sub(1) != self.materialized_through_sequence {
            tracing::warn!(
                target: "sinomo.dfs.source-adoption",
                operation = %scope.operation_id.chars().take(192).collect::<String>(),
                reason = "unmaterialized-journal-tail",
                next_sequence = self.next_sequence,
                materialized_sequence = self.materialized_through_sequence,
                "native target outcome remains unavailable"
            );
            return Ok(Outcome::Unavailable);
        }
        if status == Some(crate::DfsPreparedOperationStatus::Committed) {
            tracing::warn!(
                target: "sinomo.dfs.source-adoption",
                operation = %scope.operation_id.chars().take(192).collect::<String>(),
                reason = "committed-status-without-commit-reference",
                status = ?status,
                prepare_sequence = ?digest.map(|digest| digest.prepare_sequence),
                prefix_sequence = prefix.journal_sequence,
                "native target outcome remains unavailable"
            );
            return Ok(Outcome::Unavailable);
        }
        let (Some(digest), Some(status)) = (digest, status) else {
            // Never prepared on this shard, above its certified head.
            return Ok(Outcome::Inactive {
                digest: None,
                status: None,
            });
        };
        // The original prepare can still commit through a `recover:`
        // replacement after its target shard changed epoch, so the outcome is
        // the chain's, never the original record's alone.
        let root = ChainPrepare {
            digest,
            status,
            commit: None,
        };
        match adoption_prepare_chain(&self.tables, root)? {
            DfsAdoptionPrepareChain::Committed { digest, commit } => {
                commit.validate(digest)?;
                if commit.sequence > prefix.journal_sequence {
                    tracing::warn!(
                        target: "sinomo.dfs.source-adoption",
                        operation = %scope.operation_id.chars().take(192).collect::<String>(),
                        replacement = %digest.operation_id.chars().take(192).collect::<String>(),
                        reason = "chain-commit-outside-certified-cut",
                        commit_sequence = commit.sequence,
                        prefix_sequence = prefix.journal_sequence,
                        "native target outcome remains unavailable"
                    );
                    return Ok(Outcome::Unavailable);
                }
                Ok(Outcome::Committed {
                    digest: digest.clone(),
                    commit: commit.clone(),
                })
            }
            DfsAdoptionPrepareChain::Aborted => Ok(Outcome::Inactive {
                digest: Some(digest.clone()),
                status: Some(status),
            }),
            DfsAdoptionPrepareChain::Live => Ok(Outcome::Active),
            DfsAdoptionPrepareChain::Unresolved(reason) => {
                tracing::warn!(
                    target: "sinomo.dfs.source-adoption",
                    operation = %scope.operation_id.chars().take(192).collect::<String>(),
                    reason,
                    prefix_sequence = prefix.journal_sequence,
                    "native target outcome remains unavailable"
                );
                Ok(Outcome::Unavailable)
            }
        }
    }
}

/// One prepare of an adoption's chain as its target shard retains it.
#[derive(Clone, Copy, Debug)]
struct ChainPrepare<'a> {
    digest: &'a crate::DfsPreparedOperationDigest,
    status: crate::DfsPreparedOperationStatus,
    commit: Option<&'a crate::DfsPreparedOperationCommitReference>,
}

/// What an adoption's prepare chain on its target shard has settled. The
/// chain is the original prepare and every `recover:` replacement of it:
/// prepared-change recovery republishes a prepare at each newer epoch under a
/// new id, and the replacement supersedes its original only when it commits.
#[derive(Debug, Eq, PartialEq)]
enum DfsAdoptionPrepareChain<'a> {
    /// A prepare in the chain committed.
    Committed {
        digest: &'a crate::DfsPreparedOperationDigest,
        commit: &'a crate::DfsPreparedOperationCommitReference,
    },
    /// Every prepare in the chain is aborted.
    Aborted,
    /// A prepare in the chain is still prepared or superseded without a
    /// retained commit: the adoption may still take effect.
    Live,
    /// The tables cannot settle the chain: a committed status without its
    /// commit reference, or more than one commit.
    Unresolved(&'static str),
}

/// Judge the chain rooted at `root`. Committed if any prepare in it
/// committed; Aborted only if every prepare in it is aborted; Live otherwise,
/// so a prepared body is never reported as having no effect.
fn adoption_prepare_chain<'a>(
    tables: &'a DfsMetadataTables,
    root: ChainPrepare<'a>,
) -> Result<DfsAdoptionPrepareChain<'a>> {
    use crate::DfsPreparedOperationStatus as Status;
    let mut pending = vec![root];
    let mut committed = None;
    let mut live = false;
    while let Some(prepare) = pending.pop() {
        match prepare.status {
            Status::Committed => {
                let Some(commit) = prepare.commit else {
                    return Ok(DfsAdoptionPrepareChain::Unresolved(
                        "chain-committed-status-without-commit-reference",
                    ));
                };
                if committed.replace((prepare.digest, commit)).is_some() {
                    return Ok(DfsAdoptionPrepareChain::Unresolved(
                        "competing-chain-commits",
                    ));
                }
            }
            Status::Prepared | Status::Superseded => live = true,
            Status::Aborted => {}
        }
        // Each replacement is at a strictly newer epoch than the prepare it
        // recovers, so the walk ends.
        pending.extend(recovery_replacements(tables, prepare.digest)?);
    }
    Ok(match committed {
        Some((digest, commit)) => DfsAdoptionPrepareChain::Committed { digest, commit },
        None if live => DfsAdoptionPrepareChain::Live,
        None => DfsAdoptionPrepareChain::Aborted,
    })
}

/// The retained `recover:` replacements of `parent`: bodies whose id is
/// `parent`'s recovery id at their own newer epoch on the same shard. A
/// retained body must also name `parent` as the prepare it supersedes; a
/// terminal eviction keeps only its digest, so its id and scope identify it.
fn recovery_replacements<'a>(
    tables: &'a DfsMetadataTables,
    parent: &crate::DfsPreparedOperationDigest,
) -> Result<Vec<ChainPrepare<'a>>> {
    let prefix = format!("recover:{}:", parent.operation_id);
    let replaces_parent = |operation_id: &str, digest: &crate::DfsPreparedOperationDigest| {
        crate::metadata_records::recovered_operation_id(operation_id, digest.owner_epoch)
            == Some(parent.operation_id.as_str())
            && digest.operation_id == operation_id
            && digest.file_system_id == parent.file_system_id
            && digest.shard_id == parent.shard_id
            && digest.owner_epoch > parent.owner_epoch
            && digest.prepare_sequence > parent.prepare_sequence
    };
    let mut replacements = Vec::new();
    for (operation_id, record) in tables
        .prepared_operations_v7
        .range(prefix.clone()..)
        .take_while(|(operation_id, _)| operation_id.starts_with(&prefix))
    {
        if !replaces_parent(operation_id, &record.digest) {
            continue;
        }
        record.validate()?;
        let supersedes_parent = record.payload.metadata_effects.iter().any(|effect| {
            matches!(effect,
                crate::DfsPreparedMetadataEffect::ResolvePreparedOperation {
                    digest,
                    status: crate::DfsPreparedOperationStatus::Superseded,
                } if digest == parent)
        });
        if supersedes_parent {
            replacements.push(ChainPrepare {
                digest: &record.digest,
                status: record.status,
                commit: record.committed_entry.as_deref(),
            });
        }
    }
    for (operation_id, eviction) in tables
        .prepared_operation_evictions
        .range(prefix.clone()..)
        .take_while(|(operation_id, _)| operation_id.starts_with(&prefix))
    {
        if !replaces_parent(operation_id, &eviction.digest) {
            continue;
        }
        if tables.prepared_operations_v7.contains_key(operation_id) {
            return Err(MetadataBackendError::Serde(
                "native target has competing retained outcomes".into(),
            ));
        }
        eviction.validate()?;
        replacements.push(ChainPrepare {
            digest: &eviction.digest,
            status: eviction.status,
            commit: eviction.committed_entry.as_deref(),
        });
    }
    Ok(replacements)
}

#[cfg(test)]
mod prepare_chain_tests {
    use super::*;
    use crate::{
        DfsPreparedMetadataEffect as Effect, DfsPreparedOperationCommitReference as Commit,
        DfsPreparedOperationDigest as Digest, DfsPreparedOperationPayload as Payload,
        DfsPreparedOperationRecord as Record, DfsPreparedOperationStatus as Status,
    };

    struct Chain {
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
        tables: DfsMetadataTables,
    }

    impl Chain {
        fn new() -> Self {
            Self {
                file_system_id: sinomo_core::Id::generate(),
                shard_id: DfsMetadataShardId::new("services"),
                tables: DfsMetadataTables::default(),
            }
        }

        /// A body `operation_id` at `epoch`, prepared at `sequence`,
        /// superseding `supersedes` when it commits.
        fn body(
            &self,
            operation_id: &str,
            epoch: u64,
            sequence: u64,
            supersedes: Option<&Digest>,
        ) -> Record {
            let payload = Payload::for_metadata_effects(
                crate::DfsPreparedOperationScope {
                    file_system_id: self.file_system_id.clone(),
                    shard_id: self.shard_id.clone(),
                    owner_epoch: epoch,
                    operation_id: operation_id.into(),
                },
                Vec::new(),
                Vec::new(),
                supersedes
                    .map(|digest| Effect::ResolvePreparedOperation {
                        digest: digest.clone(),
                        status: Status::Superseded,
                    })
                    .into_iter()
                    .collect(),
                time::OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();
            Record {
                digest: Digest::at_sequence(&payload, sequence).unwrap(),
                payload: Arc::new(payload),
                status: Status::Prepared,
                committed_entry: None,
            }
        }

        fn put(&mut self, mut record: Record, status: Status) -> Digest {
            record.status = status;
            record.committed_entry = (status == Status::Committed).then(|| {
                Box::new(Commit {
                    shard_id: self.shard_id.clone(),
                    sequence: record.digest.prepare_sequence + 1,
                    created_at: time::OffsetDateTime::UNIX_EPOCH,
                    digest: record.digest.clone(),
                })
            });
            record.validate().unwrap();
            let digest = record.digest.clone();
            self.tables
                .prepared_operations_v7
                .insert(digest.operation_id.clone(), record);
            digest
        }

        fn evict(&mut self, digest: &Digest) {
            let record = self
                .tables
                .prepared_operations_v7
                .remove(&digest.operation_id)
                .unwrap();
            self.tables.prepared_operation_evictions.insert(
                digest.operation_id.clone(),
                crate::DfsPreparedOperationEviction::from_terminal_record(&record).unwrap(),
            );
        }

        fn judge(&self, original: &Digest) -> DfsAdoptionPrepareChain<'_> {
            let record = &self.tables.prepared_operations_v7[&original.operation_id];
            adoption_prepare_chain(
                &self.tables,
                ChainPrepare {
                    digest: &record.digest,
                    status: record.status,
                    commit: record.committed_entry.as_deref(),
                },
            )
            .unwrap()
        }

        fn committed_by(&self, original: &Digest) -> Option<Digest> {
            match self.judge(original) {
                DfsAdoptionPrepareChain::Committed { digest, commit } => {
                    commit.validate(digest).unwrap();
                    Some(digest.clone())
                }
                _ => None,
            }
        }
    }

    #[test]
    fn a_prepared_original_is_live_never_inactive() {
        let mut chain = Chain::new();
        let original = chain.put(chain.body("adopt-x", 4, 10, None), Status::Prepared);
        assert_eq!(chain.judge(&original), DfsAdoptionPrepareChain::Live);
        let mut aborted = Chain::new();
        let original = aborted.put(aborted.body("adopt-x", 4, 10, None), Status::Aborted);
        assert_eq!(aborted.judge(&original), DfsAdoptionPrepareChain::Aborted);
    }

    #[test]
    fn a_committed_recovery_replacement_commits_the_chain() {
        let mut chain = Chain::new();
        let x = chain.body("adopt-x", 4, 10, None);
        let replacement = chain.body("recover:adopt-x:5", 5, 20, Some(&x.digest));
        let x = chain.put(x, Status::Superseded);
        let replacement = chain.put(replacement, Status::Committed);
        assert_eq!(chain.committed_by(&x), Some(replacement.clone()));
        // A terminal eviction of the replacement still settles the chain.
        chain.evict(&replacement);
        assert_eq!(chain.committed_by(&x), Some(replacement));
    }

    #[test]
    fn a_prepared_replacement_keeps_the_chain_live_until_every_prepare_aborts() {
        let mut chain = Chain::new();
        let x = chain.body("adopt-x", 4, 10, None);
        let replacement = chain.body("recover:adopt-x:5", 5, 20, Some(&x.digest));
        let x = chain.put(x, Status::Prepared);
        let replacement_record = replacement.clone();
        chain.put(replacement, Status::Prepared);
        assert_eq!(chain.judge(&x), DfsAdoptionPrepareChain::Live);
        // The original alone aborted: its replacement is still prepared.
        let original = chain.tables.prepared_operations_v7[&x.operation_id].clone();
        chain.put(original, Status::Aborted);
        assert_eq!(chain.judge(&x), DfsAdoptionPrepareChain::Live);
        chain.put(replacement_record, Status::Aborted);
        assert_eq!(chain.judge(&x), DfsAdoptionPrepareChain::Aborted);
    }

    #[test]
    fn a_nested_replacement_and_a_supersession_without_its_commit_are_judged_by_the_chain() {
        let mut chain = Chain::new();
        let x = chain.body("adopt-x", 4, 10, None);
        let first = chain.body("recover:adopt-x:5", 5, 20, Some(&x.digest));
        let second = chain.body("recover:recover:adopt-x:5:7", 7, 30, Some(&first.digest));
        let x = chain.put(x, Status::Superseded);
        // Superseded with no retained replacement: never inactive.
        assert_eq!(chain.judge(&x), DfsAdoptionPrepareChain::Live);
        chain.put(first, Status::Superseded);
        assert_eq!(chain.judge(&x), DfsAdoptionPrepareChain::Live);
        let second = chain.put(second, Status::Committed);
        assert_eq!(chain.committed_by(&x), Some(second));
    }

    #[test]
    fn bodies_that_do_not_recover_the_original_are_not_in_its_chain() {
        let mut chain = Chain::new();
        let x = chain.body("adopt-x", 4, 10, None);
        let other = chain.body("adopt-y", 4, 11, None);
        // Named like replacements, but superseding another body, at an
        // epoch other than their own, or not newer than the original.
        let decoys = [
            chain.body("recover:adopt-x:5", 5, 20, Some(&other.digest)),
            chain.body("recover:adopt-x:6", 5, 21, Some(&x.digest)),
            chain.body("recover:adopt-x:4", 4, 22, Some(&x.digest)),
            chain.body("recover:adopt-x:05", 5, 23, Some(&x.digest)),
            chain.body("recover:adopt-x:5:6", 6, 24, Some(&x.digest)),
        ];
        let x = chain.put(x, Status::Aborted);
        chain.put(other, Status::Prepared);
        for decoy in decoys {
            chain.put(decoy, Status::Committed);
        }
        assert_eq!(chain.judge(&x), DfsAdoptionPrepareChain::Aborted);
    }

    #[test]
    fn the_source_accepts_only_the_original_or_its_recovery_chain_as_its_commit() {
        let chain = Chain::new();
        let x = chain.body("adopt-x", 4, 10, None).digest;
        let first = chain.body("recover:adopt-x:5", 5, 20, Some(&x)).digest;
        let second = chain
            .body("recover:recover:adopt-x:5:7", 7, 30, Some(&first))
            .digest;
        let accepts = crate::source_adoption::commits_adoption_for_test;
        assert!(accepts(&x, &x));
        assert!(accepts(&x, &first));
        assert!(accepts(&x, &second));
        let mut changed = x.clone();
        changed.payload_digest = first.payload_digest.clone();
        assert!(!accepts(&x, &changed), "the original itself is exact");
        for decoy in [
            chain.body("recover:adopt-y:5", 5, 20, Some(&x)).digest,
            chain.body("recover:adopt-x:4", 4, 20, Some(&x)).digest,
            chain.body("recover:adopt-x:05", 5, 20, Some(&x)).digest,
            chain.body("recover:adopt-x:5", 5, 9, Some(&x)).digest,
            chain
                .body("recover:recover:adopt-x:7:5", 5, 30, Some(&first))
                .digest,
            Chain::new()
                .body("recover:adopt-x:5", 5, 20, Some(&x))
                .digest,
        ] {
            assert!(!accepts(&x, &decoy), "{}", decoy.operation_id);
        }
    }
}
