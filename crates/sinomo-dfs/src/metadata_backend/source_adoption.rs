use super::*;
use crate::DfsNamespaceJobStatus;
use crate::publication_guard::{DfsPublicationOwnerScope, ShardFenceGuard, shard_fence_guard};
use std::sync::Arc;

/// Shares an already-acquired native writer/fence; it cannot authorize effect.
/// Only capture under the real backend writer constructs this opaque custody.
#[derive(Clone, Debug)]
pub(crate) struct DfsSourceAdoptionWriterCustody {
    scope: DfsPublicationOwnerScope,
    prefix: DfsMetadataPrefixProof,
    writer: Arc<MetadataWriteLock>,
    fence: Arc<ShardFenceGuard>,
}
impl DfsSourceAdoptionWriterCustody {
    pub(crate) fn scope(&self) -> &DfsPublicationOwnerScope {
        &self.scope
    }
    pub(crate) fn writer(&self) -> Arc<MetadataWriteLock> {
        Arc::clone(&self.writer)
    }
    pub(crate) fn fence(&self) -> Arc<ShardFenceGuard> {
        Arc::clone(&self.fence)
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
    pub(crate) fn writer_custody(&self) -> DfsSourceAdoptionWriterCustody {
        self.custody.clone()
    }
    pub(crate) fn into_parts(self) -> (DfsCrossShardSourceFamily, DfsSourceAdoptionWriterCustody) {
        (self.family, self.custody)
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
    pub(crate) fn into_parts(self) -> (DfsNamespaceJobRecord, DfsSourceAdoptionWriterCustody) {
        (self.job, self.custody)
    }
}

/// Exact source and original coordinator-job custody. Serializable rows never
/// create this value, and its locks survive until the exact target outcome.
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
    pub(crate) fn scope(&self) -> &DfsPublicationOwnerScope {
        self.source.scope()
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
    pub(crate) fn job_scope(&self) -> &DfsPublicationOwnerScope {
        self.job_root.as_ref().unwrap_or(&self.source).scope()
    }
    pub(crate) fn job_writer_custody(&self) -> DfsSourceAdoptionWriterCustody {
        self.job_root.as_ref().unwrap_or(&self.source).clone()
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
        let fence = Arc::new(shard_fence_guard(&scope));
        let writer = Arc::new(self.begin_metadata_write()?);
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
            writer,
            fence,
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
    /// Read native terminal identity from the actual held inspection cut.
    /// A pending durable suffix cannot establish that an effect never happened.
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
        Ok(Outcome::Inactive {
            digest: digest.cloned(),
            status,
        })
    }
}
