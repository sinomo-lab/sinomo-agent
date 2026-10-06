//! Durable restore execution and source-pin release, owned by namespace jobs.

use crate::*;
use std::time::Duration;

fn restore_error(message: impl Into<String>) -> DfsError {
    DfsError::Metadata {
        message: message.into(),
    }
}

use crate::validation::file_restore_job_id;
use sinomo_dfs::{
    validate_content_copy_job as validate_file_restore_job,
    validate_content_copy_job_receipt as validate_file_restore_job_receipt,
};

fn begin_request_id(requester: &NodeId, operation_id: &str) -> String {
    let key = file_restore_job_id(requester, operation_id);
    let order =
        sinomo_dfs::stable_operation_order_token(operation_id).unwrap_or_else(|| "opaque".into());
    format!("restore-begin:{key}_{order}")
}

pub(crate) fn restore_job_state(
    job: &DfsNamespaceJobRecord,
) -> Result<&DfsContentCopyJobState, DfsError> {
    let [DfsNamespaceJobFrame::ContentCopy { state }] = job.pending_frames.as_slice() else {
        return Err(restore_error("restore job has no unique durable state"));
    };
    if job.kind != DfsNamespaceJobKind::ContentCopy {
        return Err(restore_error("unexpected restore job kind"));
    }
    Ok(state)
}

fn response_from_result(
    result: &DfsContentCopyJobResult,
) -> Result<DfsFileHistoryRestoreResponse, DfsError> {
    match result {
        DfsContentCopyJobResult::Applied { response } => Ok(response.clone()),
        DfsContentCopyJobResult::Rejected { reason } => Err(restore_error(reason.clone())),
    }
}

impl<T: DfsMessageTransport> DfsHistoryService<'_, T> {
    fn find_file_restore_job(
        &mut self,
        job_id: &str,
    ) -> Result<Option<(DfsMetadataShardConfig, DfsMetadataQueryResult)>, DfsError> {
        for shard in self
            .core
            .metadata_shard_authorities(&sifs_file_system_id())?
        {
            if !shard.namespace_prepare_codec.supports_file_history() {
                continue;
            }
            let result = self.core.query_metadata_authority(
                &shard,
                DfsMetadataSelection::NamespaceJob {
                    job_id: job_id.into(),
                },
            )?;
            match &result {
                DfsMetadataQueryResult::NamespaceJob { job, receipt }
                    if job.is_some() || receipt.is_some() =>
                {
                    if let Some(job) = job {
                        validate_file_restore_job(job)?;
                    }
                    if let Some(receipt) = receipt {
                        validate_file_restore_job_receipt(receipt)?;
                    }
                    return Ok(Some((shard, result)));
                }
                DfsMetadataQueryResult::NamespaceJob { .. } => {}
                _ => {
                    return Err(restore_error(
                        "restore authority returned the wrong query kind",
                    ));
                }
            }
        }
        Ok(None)
    }

    pub fn restore_file_history(
        &mut self,
        request: DfsFileHistoryRestoreRequest,
        options: DfsWriteOptions,
    ) -> Result<DfsFileHistoryRestoreResponse, DfsError> {
        self.core
            .check_operation_cancellation("restore file history")?;
        sinomo_dfs::validate_write_operation_id(&request.operation_id)?;
        if request
            .expected_destination
            .as_ref()
            .is_some_and(|expected| expected.file_system_id != sifs_file_system_id())
        {
            return Err(restore_error(
                "restore destination must name the SIFS filesystem",
            ));
        }
        let requester = self.core.node_id().clone();
        let job_id = file_restore_job_id(&requester, &request.operation_id);
        // A completed result is independent of both source retention and the
        // destination's later namespace generations.
        let existing = self.find_file_restore_job(&job_id)?;
        let (shard, mut job) = if let Some((shard, result)) = existing {
            match result {
                DfsMetadataQueryResult::NamespaceJob { job: Some(job), .. } => (shard, *job),
                DfsMetadataQueryResult::NamespaceJob {
                    receipt: Some(receipt),
                    ..
                } => {
                    let outcome = receipt.content_copy.ok_or_else(|| {
                        restore_error("restore receipt omitted its durable result")
                    })?;
                    if outcome.request != request || outcome.requester_node_id != requester {
                        return Err(restore_error(
                            "restore operation identity was reused with different input",
                        ));
                    }
                    return response_from_result(&outcome.result);
                }
                _ => return Err(restore_error("restore job lookup omitted its state")),
            }
        } else {
            let path = self
                .core
                .reserved_content_destination_path(&request)
                .map_err(|source| restore_error(source.to_string()))?;
            self.core
                .require_retention_shard_support(&sifs_file_system_id(), &path)?;
            let shard = if self.core.is_distributed() {
                self.core
                    .namespace_shards_for_exact_paths(&sifs_file_system_id(), [path.as_str()])?
                    .into_iter()
                    .next()
                    .ok_or_else(|| restore_error("restore has no destination authority"))?
            } else {
                self.core
                    .metadata_shard_authorities(&sifs_file_system_id())?
                    .remove(0)
            };
            let operation = DfsNamespaceOperation::BeginContentCopy {
                path: path.clone(),
                request: request.clone(),
                options: options.clone(),
            };
            let begin_id = begin_request_id(&requester, &request.operation_id);
            let job = if !self.core.is_distributed() {
                self.core
                    .begin_reserved_content_job(&path, &request, &options)?
            } else if shard.primary_node_id == *self.core.node_id() {
                self.core
                    .execute_local_primary_namespace_operation_for_file_system(
                        sifs_file_system_id(),
                        begin_id,
                        operation,
                    )
                    .map_err(|source| restore_error(source.to_string()))?;
                self.core
                    .namespace_job(&job_id)
                    .map_err(|error| restore_error(error.to_string()))?
                    .ok_or_else(|| restore_error("restore admission omitted its durable job"))?
            } else {
                let result = self
                    .core
                    .route_namespace_operation_to_metadata_primary_with_request_id_and_options(
                        begin_id,
                        sifs_file_system_id(),
                        shard.primary_node_id.clone(),
                        operation,
                        Duration::from_secs(60),
                        false,
                        false,
                    )?;
                match (result.result, result.posix_coordination_result) {
                    (
                        DfsNamespaceResult::Applied,
                        Some(DfsPosixCoordinationResult::NamespaceJob(job)),
                    ) => job,
                    (DfsNamespaceResult::Rejected { reason, .. }, _) => {
                        return Err(restore_error(reason));
                    }
                    _ => return Err(restore_error("restore admission returned no job")),
                }
            };
            (shard, job)
        };
        let state = restore_job_state(&job)?;
        if job.job_id != job_id || state.request != request || state.requester_node_id != requester
        {
            return Err(restore_error(
                "restore operation identity was reused with different input",
            ));
        }
        if let Some(result) = &state.result {
            return response_from_result(result);
        }
        job = if !self.core.is_distributed() || shard.primary_node_id == *self.core.node_id() {
            self.core
                .resume_namespace_job_bounded(&job_id, 4)
                .map_err(|source| restore_error(source.to_string()))?
        } else {
            let result = self
                .core
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    sifs_file_system_id(),
                    shard.primary_node_id,
                    DfsNamespaceOperation::ResumeNamespaceJobBounded {
                        job_id,
                        path: job.root_path.clone(),
                        max_steps: 4,
                    },
                    Duration::from_secs(60),
                    false,
                    false,
                )?;
            match (result.result, result.posix_coordination_result) {
                (
                    DfsNamespaceResult::Applied,
                    Some(DfsPosixCoordinationResult::NamespaceJob(job)),
                ) => job,
                (DfsNamespaceResult::Rejected { reason, .. }, _) => {
                    return Err(restore_error(reason));
                }
                _ => return Err(restore_error("restore continuation returned no job")),
            }
        };
        match &restore_job_state(&job)?.result {
            Some(result) => response_from_result(result),
            None => Err(restore_error(format!(
                "restore-pending: operation {} has durable recovery ownership",
                request.operation_id
            ))),
        }
    }
}
