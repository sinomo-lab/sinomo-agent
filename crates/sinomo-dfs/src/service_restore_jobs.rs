//! Durable restore execution and source-pin release, owned by namespace jobs.

use crate::*;
use time::OffsetDateTime;

fn copy_error(message: impl Into<String>) -> DfsError {
    DfsError::Metadata {
        message: message.into(),
    }
}

pub(crate) fn content_copy_job_id(requester: &NodeId, operation_id: &str) -> String {
    let key = format!("{requester}\0{operation_id}");
    // Existing durable jobs keep their identity through the owner extraction.
    format!("file-restore:{}", blake3::hash(key.as_bytes()).to_hex())
}

fn begin_request_id(requester: &NodeId, operation_id: &str) -> String {
    let key = content_copy_job_id(requester, operation_id);
    let order = crate::intent::stable_operation_order_token(operation_id)
        .unwrap_or_else(|| "opaque".into());
    format!("restore-begin:{key}_{order}")
}

pub(crate) fn content_copy_job_state(
    job: &DfsNamespaceJobRecord,
) -> Result<&DfsContentCopyJobState, DfsError> {
    let [DfsNamespaceJobFrame::ContentCopy { state }] = job.pending_frames.as_slice() else {
        return Err(copy_error("content copy job has no unique durable state"));
    };
    if job.kind != DfsNamespaceJobKind::ContentCopy {
        return Err(copy_error("unexpected content copy job kind"));
    }
    Ok(state)
}

fn definitive_copy_rejection(source: &DfsPosixError) -> bool {
    matches!(
        source,
        DfsPosixError::NotFound { .. }
            | DfsPosixError::AlreadyExists { .. }
            | DfsPosixError::NotDirectory { .. }
            | DfsPosixError::IsDirectory { .. }
            | DfsPosixError::NotSymlink { .. }
            | DfsPosixError::MissingParent { .. }
            | DfsPosixError::InvalidPath { .. }
            | DfsPosixError::Conflict { .. }
            | DfsPosixError::DirectoryNotEmpty { .. }
            | DfsPosixError::Dfs(
                DfsError::UnsupportedPolicy { .. }
                    | DfsError::InvalidReplicationPolicy { .. }
                    | DfsError::WriteOperationIdConflict { .. }
            )
    )
}

fn copy_source_namespace_matches(reference: &DfsContentReferenceRequest) -> bool {
    match reference.wire_format {
        DfsContentReferenceFormat::NamespacedV6 => reference.namespace == "sinomo.content-copy",
        DfsContentReferenceFormat::Legacy => {
            reference.namespace == crate::lifecycle::legacy_reference_namespace()
        }
    }
}

pub fn validate_content_copy_job(job: &DfsNamespaceJobRecord) -> Result<(), MetadataBackendError> {
    let frames = job
        .pending_frames
        .iter()
        .filter(|frame| matches!(frame, DfsNamespaceJobFrame::ContentCopy { .. }))
        .count();
    if job.kind != DfsNamespaceJobKind::ContentCopy {
        return if frames == 0 && !job.job_id.starts_with("file-restore:") {
            Ok(())
        } else {
            Err(MetadataBackendError::Serde(
                "content copy state belongs only to a restore job".into(),
            ))
        };
    }
    let invalid = || MetadataBackendError::Serde("invalid durable content copy job state".into());
    let [DfsNamespaceJobFrame::ContentCopy { state }] = job.pending_frames.as_slice() else {
        return Err(invalid());
    };
    crate::service_routed_write::validate_write_operation_id(&state.request.operation_id)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    let expected_object = state
        .request
        .expected_destination
        .as_ref()
        .map(|expected| expected.object_id.clone())
        .unwrap_or_else(|| {
            crate::intent::stable_create_object_id(
                &state.requester_node_id,
                &job.root_path,
                &state.write_operation_id,
            )
        });
    if job.file_system_id != sifs_file_system_id()
        || state.planned.file_system_id != job.file_system_id
        || job.job_id != content_copy_job_id(&state.requester_node_id, &state.request.operation_id)
        || state.write_operation_id != format!("restore-write:{}", job.job_id)
        || state.planned.object_id != expected_object
        || state.planned.version
            != crate::intent::stable_write_target_version(
                &state.requester_node_id,
                &expected_object,
                &state.write_operation_id,
            )
        || job
            .origin
            .as_ref()
            .is_none_or(|origin| origin.requester_node_id != state.requester_node_id)
        || state
            .request
            .expected_destination
            .as_ref()
            .is_some_and(|expected| expected.file_system_id != job.file_system_id)
        || job.root_path != crate::metadata_keys::normalize_posix_path(&job.root_path)
        || state.content_hash.is_some() != state.content_size_bytes.is_some()
        || state.source_reference.is_some() != state.content_hash.is_some()
        || (state.source_reference_applied && state.source_reference.is_none())
    {
        return Err(invalid());
    }
    if let Some(pin) = &state.source_reference
        && (!copy_source_namespace_matches(pin)
            || pin.identity != state.request.source
            || pin.operation_id != format!("restore-pin:{}", job.job_id)
            || pin.operation
                != (DfsContentReferenceOperation::Retain {
                    reference_id: format!("restore:{}", job.job_id),
                }))
    {
        return Err(invalid());
    }
    if let Some(DfsContentCopyJobResult::Applied { response }) = &state.result
        && (!state.source_reference_applied
            || response.restored_from != state.request.source
            || response.committed != state.planned
            || response.path != job.root_path)
    {
        return Err(invalid());
    }
    match (
        &job.status,
        &state.result,
        state.cleanup_complete,
        job.completed_at,
    ) {
        (DfsNamespaceJobStatus::Running, _, false, None) => {}
        (
            DfsNamespaceJobStatus::Completed,
            Some(DfsContentCopyJobResult::Applied { .. }),
            true,
            Some(_),
        ) => {}
        (
            DfsNamespaceJobStatus::Failed,
            Some(DfsContentCopyJobResult::Rejected { .. }),
            true,
            Some(_),
        ) => {}
        _ => return Err(invalid()),
    }
    state
        .options
        .policy
        .validate()
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    Ok(())
}

pub(crate) fn validate_content_copy_job_transition(
    previous: &DfsNamespaceJobRecord,
    next: &DfsNamespaceJobRecord,
) -> Result<(), MetadataBackendError> {
    if previous.kind != DfsNamespaceJobKind::ContentCopy
        && next.kind != DfsNamespaceJobKind::ContentCopy
    {
        return Ok(());
    }
    validate_content_copy_job(previous)?;
    validate_content_copy_job(next)?;
    fn state(job: &DfsNamespaceJobRecord) -> Option<&DfsContentCopyJobState> {
        match job.pending_frames.first() {
            Some(DfsNamespaceJobFrame::ContentCopy { state }) => Some(state),
            _ => None,
        }
    }
    let old = state(previous)
        .ok_or_else(|| MetadataBackendError::Serde("content copy job kind cannot change".into()))?;
    let new = state(next)
        .ok_or_else(|| MetadataBackendError::Serde("content copy job kind cannot change".into()))?;
    if previous.job_id != next.job_id
        || previous.file_system_id != next.file_system_id
        || previous.origin != next.origin
        || previous.root_path != next.root_path
        || previous.created_at != next.created_at
        || old.request != new.request
        || old.requester_node_id != new.requester_node_id
        || old.options != new.options
        || old.write_operation_id != new.write_operation_id
        || old.planned != new.planned
        || old
            .source_reference
            .as_ref()
            .is_some_and(|pin| new.source_reference.as_ref() != Some(pin))
        || old
            .content_hash
            .as_ref()
            .is_some_and(|hash| new.content_hash.as_ref() != Some(hash))
        || old
            .content_size_bytes
            .is_some_and(|size| new.content_size_bytes != Some(size))
        || (old.source_reference_applied && !new.source_reference_applied)
        || old
            .result
            .as_ref()
            .is_some_and(|result| new.result.as_ref() != Some(result))
        || (old.cleanup_complete && !new.cleanup_complete)
        || next.processed_object_count < previous.processed_object_count
    {
        return Err(MetadataBackendError::Serde(
            "content copy job cannot change identity or regress durable progress".into(),
        ));
    }
    Ok(())
}

pub fn validate_content_copy_job_receipt(
    receipt: &DfsNamespaceJobReceipt,
) -> Result<(), MetadataBackendError> {
    let invalid = || MetadataBackendError::Serde("invalid durable content copy receipt".into());
    if receipt.kind != DfsNamespaceJobKind::ContentCopy {
        return if receipt.content_copy.is_none() && !receipt.job_id.starts_with("file-restore:") {
            Ok(())
        } else {
            Err(invalid())
        };
    }
    let outcome = receipt.content_copy.as_ref().ok_or_else(invalid)?;
    let write_operation_id = format!("restore-write:{}", receipt.job_id);
    let object_id = outcome
        .request
        .expected_destination
        .as_ref()
        .map(|expected| expected.object_id.clone())
        .unwrap_or_else(|| {
            crate::intent::stable_create_object_id(
                &outcome.requester_node_id,
                &receipt.root_path,
                &write_operation_id,
            )
        });
    let version = crate::intent::stable_write_target_version(
        &outcome.requester_node_id,
        &object_id,
        &write_operation_id,
    );
    if receipt.file_system_id != sifs_file_system_id()
        || receipt.job_id
            != content_copy_job_id(&outcome.requester_node_id, &outcome.request.operation_id)
        || receipt
            .origin
            .as_ref()
            .is_none_or(|origin| origin.requester_node_id != outcome.requester_node_id)
    {
        return Err(invalid());
    }
    match (&receipt.status, &outcome.result) {
        (DfsNamespaceJobStatus::Completed, DfsContentCopyJobResult::Applied { response })
            if response.restored_from == outcome.request.source
                && response.committed.file_system_id == receipt.file_system_id
                && response.committed.object_id == object_id
                && response.committed.version == version
                && response.path == receipt.root_path =>
        {
            Ok(())
        }
        (DfsNamespaceJobStatus::Failed, DfsContentCopyJobResult::Rejected { .. }) => Ok(()),
        _ => Err(invalid()),
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn begin_content_copy_job(
        &mut self,
        path: &str,
        request: &DfsContentCopyRequest,
        options: &DfsWriteOptions,
    ) -> Result<DfsNamespaceJobRecord, DfsError> {
        crate::service_routed_write::validate_write_operation_id(&request.operation_id)?;
        self.require_retention_shard_support(&sifs_file_system_id(), path)?;
        if self
            .namespace_execution
            .context()
            .is_some_and(|context| context.file_system_id != sifs_file_system_id())
        {
            return Err(copy_error(
                "content copy admission has a foreign filesystem",
            ));
        }
        let requester = self
            .namespace_execution
            .request()
            .map(|request| request.requester_node_id.clone())
            .unwrap_or_else(|| self.node_id.clone());
        let job_id = content_copy_job_id(&requester, &request.operation_id);
        if let Some(job) = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| copy_error("content copy requires durable metadata"))?
            .get_namespace_job(&job_id)?
        {
            let state = content_copy_job_state(&job)?;
            if state.request != *request || state.requester_node_id != requester {
                return Err(copy_error(
                    "content copy operation identity was reused with different input",
                ));
            }
            return Ok(job);
        }
        if self
            .content_copy_destination_path(request)
            .map_err(|source| copy_error(source.to_string()))?
            != path
        {
            return Err(copy_error(
                "content copy destination moved during admission",
            ));
        }
        options.policy.validate()?;
        let standalone = if self.control_plane.is_none() {
            Some(self.admit_standalone_content_copy(path, request, options)?)
        } else {
            None
        };
        let origin = self
            .namespace_execution
            .request()
            .map(DfsNamespaceJobOrigin::from_request)
            .or_else(|| standalone.as_ref().map(DfsNamespaceJobOrigin::from_request));
        let write_operation_id = format!("restore-write:{job_id}");
        let object_id = request
            .expected_destination
            .as_ref()
            .map(|expected| expected.object_id.clone())
            .unwrap_or_else(|| {
                crate::intent::stable_create_object_id(&requester, path, &write_operation_id)
            });
        let planned = DfsRevisionReference {
            file_system_id: sifs_file_system_id(),
            version: crate::intent::stable_write_target_version(
                &requester,
                &object_id,
                &write_operation_id,
            ),
            object_id,
        };
        let now = standalone
            .as_ref()
            .map(|record| record.created_at)
            .unwrap_or_else(OffsetDateTime::now_utc);
        let job = DfsNamespaceJobRecord {
            job_id,
            file_system_id: sifs_file_system_id(),
            origin,
            kind: DfsNamespaceJobKind::ContentCopy,
            status: DfsNamespaceJobStatus::Running,
            root_path: path.into(),
            target_path: None,
            pending_frames: vec![DfsNamespaceJobFrame::ContentCopy {
                state: Box::new(DfsContentCopyJobState {
                    request: request.clone(),
                    requester_node_id: requester,
                    options: options.clone(),
                    write_operation_id,
                    planned,
                    source_reference: None,
                    source_reference_applied: false,
                    content_hash: None,
                    content_size_bytes: None,
                    result: None,
                    cleanup_complete: false,
                }),
            }],
            processed_object_count: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
            failure_reason: None,
        };
        self.persist_namespace_job(&job)
            .map_err(|source| copy_error(source.to_string()))?;
        if let Some(mut admission) = standalone {
            admission.status = DfsNamespaceRequestStatus::Completed;
            admission.result = Some(DfsNamespaceResult::Applied);
            admission.posix_coordination_result =
                Some(DfsPosixCoordinationResult::NamespaceJob(job.clone()));
            admission.updated_at = OffsetDateTime::now_utc();
            self.metadata_backend
                .as_mut()
                .expect("content copy backend")
                .put_namespace_request_on_shard(&admission.authority_shard_id, &admission)?;
        }
        Ok(job)
    }

    fn admit_standalone_content_copy(
        &mut self,
        path: &str,
        request: &DfsContentCopyRequest,
        options: &DfsWriteOptions,
    ) -> Result<DfsNamespaceRequestRecord, DfsError> {
        let requester = self.node_id.clone();
        let request_id = begin_request_id(&requester, &request.operation_id);
        let operation = DfsNamespaceOperation::BeginContentCopy {
            path: path.into(),
            request: request.clone(),
            options: options.clone(),
        };
        let command = DfsNamespaceCommand {
            request_id: request_id.clone(),
            file_system_id: sifs_file_system_id(),
            requesting_node_id: requester.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: operation.clone(),
        };
        let digest = crate::service_routed_namespace::namespace_command_digest(&command)?;
        let backend = self
            .metadata_backend
            .as_mut()
            .ok_or_else(|| copy_error("content copy requires durable metadata"))?;
        let shard = backend
            .metadata_shard_statuses()?
            .into_iter()
            .next()
            .ok_or_else(|| copy_error("content copy backend has no shard"))?
            .shard_id;
        let admission =
            backend.namespace_request_admission_snapshot(&shard, &requester, &request_id)?;
        if let Some(existing) = admission.request {
            if existing.command_digest == digest
                && existing.status == DfsNamespaceRequestStatus::Pending
            {
                return Ok(existing);
            }
            return Err(copy_error(
                "content copy admission already completed or has different input; its result is unavailable",
            ));
        }
        if admission.receipt.is_some() || admission.history_expired {
            return Err(copy_error(
                "content copy operation outcome expired; refusing to repeat the write",
            ));
        }
        let now = OffsetDateTime::now_utc();
        let record = DfsNamespaceRequestRecord {
            file_system_id: sifs_file_system_id(),
            requester_node_id: requester,
            request_id: request_id.clone(),
            operation_id: format!("req-{request_id}"),
            authority_shard_id: shard.clone(),
            operation,
            command_digest: digest,
            status: DfsNamespaceRequestStatus::Pending,
            result: None,
            posix_coordination_result: None,
            recovery_attempts: 0,
            last_recovery_error: None,
            created_at: now,
            updated_at: now,
        };
        backend.put_namespace_request_on_shard(&shard, &record)?;
        Ok(record)
    }

    fn save_content_copy_state(
        &mut self,
        job: &mut DfsNamespaceJobRecord,
        state: DfsContentCopyJobState,
    ) -> Result<(), DfsError> {
        job.updated_at = OffsetDateTime::now_utc();
        job.processed_object_count = job.processed_object_count.saturating_add(1);
        if state.cleanup_complete {
            job.completed_at = Some(job.updated_at);
            job.status = match &state.result {
                Some(DfsContentCopyJobResult::Applied { .. }) => DfsNamespaceJobStatus::Completed,
                Some(DfsContentCopyJobResult::Rejected { reason }) => {
                    job.failure_reason = Some(reason.clone());
                    DfsNamespaceJobStatus::Failed
                }
                None => {
                    return Err(copy_error(
                        "content copy cannot finish without a durable outcome",
                    ));
                }
            };
        }
        job.pending_frames = vec![DfsNamespaceJobFrame::ContentCopy {
            state: Box::new(state),
        }];
        self.persist_namespace_job(job)
            .map_err(|source| copy_error(source.to_string()))?;
        if let Some(latest) = self
            .metadata_backend
            .as_ref()
            .expect("content copy backend")
            .get_namespace_job(&job.job_id)?
        {
            *job = latest;
        }
        Ok(())
    }

    pub(crate) fn run_content_copy_job(
        &mut self,
        job: DfsNamespaceJobRecord,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord, DfsError> {
        // Source queries and pins are independent namespace requests, possibly
        // on another authority. They must not complete or overwrite the
        // request which asked this durable job to resume.
        let outer = std::mem::take(&mut self.namespace_execution);
        let result = self.run_content_copy_job_independent(job, max_steps);
        self.namespace_execution = outer;
        if self.namespace_execution.request().is_some() {
            self.namespace_execution.record_job();
        }
        result
    }

    fn run_content_copy_job_independent(
        &mut self,
        mut job: DfsNamespaceJobRecord,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord, DfsError> {
        if self.control_plane.is_some() {
            let shards = self
                .namespace_shards_for_exact_paths(&job.file_system_id, [job.root_path.as_str()])?;
            if !shards
                .iter()
                .any(|shard| shard.primary_node_id == self.node_id)
            {
                return Ok(job);
            }
        }
        for _ in 0..max_steps {
            // Stop this executor between durable steps. The admitted job and
            // its source reservation remain owned for a later recovery pass.
            self.check_cancelled("content copy job")?;
            if let Some(latest) = self
                .metadata_backend
                .as_ref()
                .ok_or_else(|| copy_error("content copy requires durable metadata"))?
                .get_namespace_job(&job.job_id)?
            {
                job = latest;
            }
            if job.status != DfsNamespaceJobStatus::Running {
                break;
            }
            let mut state = content_copy_job_state(&job)?.clone();
            if state.result.is_some() {
                if let Some(pin) = &state.source_reference {
                    let DfsContentReferenceOperation::Retain { reference_id } = &pin.operation
                    else {
                        return Err(copy_error("content copy source reservation is not a pin"));
                    };
                    let release = (|| {
                        if self.content_reference_present(pin)? {
                            self.retain_content_revision(DfsContentReferenceRequest {
                                operation: DfsContentReferenceOperation::Release {
                                    reference_id: reference_id.clone(),
                                },
                                operation_id: format!("restore-unpin:{}", job.job_id),
                                ..pin.clone()
                            })?;
                        }
                        Ok::<_, DfsError>(())
                    })();
                    if let Err(source) = release {
                        tracing::warn!(job_id = %job.job_id, error = %source, "content copy source pin cleanup remains pending");
                        break;
                    }
                }
                state.cleanup_complete = true;
                self.save_content_copy_state(&mut job, state)?;
                break;
            }
            let intent_id = crate::intent::stable_write_intent_id(
                &state.requester_node_id,
                &state.planned.object_id,
                &state.write_operation_id,
            );
            if let Some(intent) = self
                .metadata_backend
                .as_ref()
                .expect("content copy backend")
                .get_intent(&state.planned.object_id, &intent_id)?
            {
                if !intent.publication.is_current()
                    || intent.file_system_id != state.planned.file_system_id
                    || intent.target_version != state.planned.version
                    || intent.content_hash != state.content_hash
                    || intent.coordinator_node_id != state.requester_node_id
                {
                    return Err(copy_error(
                        "content copy write has an unrelated durable identity",
                    ));
                }
                match intent.status {
                    DfsWriteIntentStatus::Committed => {
                        state.result = Some(DfsContentCopyJobResult::Applied {
                            response: DfsContentCopyResponse {
                                restored_from: state.request.source.clone(),
                                committed: state.planned.clone(),
                                path: job.root_path.clone(),
                            },
                        });
                        self.save_content_copy_state(&mut job, state)?;
                        continue;
                    }
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending => {
                        self.restore_pending_write_for_intent_if_missing(
                            &intent.intent_id,
                            &intent.object_id,
                        )?;
                        self.try_apply_committed_intent_if_ready(&intent.intent_id)?;
                        if self
                            .coordinator
                            .intent(&intent.intent_id)
                            .is_some_and(|intent| intent.state == DfsWriteIntentState::Committed)
                        {
                            continue;
                        }
                        let object = self.current_object_for_write(&intent.object_id)?;
                        self.stage_missing_desired_replicas_for_intent(&intent.intent_id, object)?;
                        break;
                    }
                    _ => {
                        state.result = Some(DfsContentCopyJobResult::Rejected {
                            reason: format!(
                                "content copy write terminated with {:?}",
                                intent.status
                            ),
                        });
                        self.save_content_copy_state(&mut job, state)?;
                        continue;
                    }
                }
            }
            if state.source_reference.is_none() {
                let descriptor = self.resolve_retained_revision(&state.request.source)?;
                let Some(descriptor) = descriptor.filter(|descriptor| {
                    descriptor.version.retention.values().any(|state| {
                        state.grant.as_ref().is_some_and(|grant| {
                            matches!(
                                grant.object.kind,
                                DfsObjectKind::File | DfsObjectKind::Artifact
                            )
                        })
                    })
                }) else {
                    state.result = Some(DfsContentCopyJobResult::Rejected {
                        reason: "content-not-retained: copy requires a granted regular file".into(),
                    });
                    self.save_content_copy_state(&mut job, state)?;
                    continue;
                };
                state.content_hash = descriptor.version.content_hash;
                state.content_size_bytes = descriptor.version.size_bytes;
                state.source_reference = Some(DfsContentReferenceRequest {
                    wire_format: DfsContentReferenceFormat::NamespacedV6,
                    namespace: "sinomo.content-copy".into(),
                    path: descriptor.current_object.path,
                    identity: state.request.source.clone(),
                    operation: DfsContentReferenceOperation::Retain {
                        reference_id: format!("restore:{}", job.job_id),
                    },
                    operation_id: format!("restore-pin:{}", job.job_id),
                });
                self.save_content_copy_state(&mut job, state)?;
                continue;
            }
            if !state.source_reference_applied {
                let pin = state.source_reference.as_ref().expect("source selected");
                let DfsContentReferenceOperation::Retain { .. } = &pin.operation else {
                    return Err(copy_error("invalid content copy source pin"));
                };
                if !self.content_reference_present(pin)?
                    && let Err(error) = self.retain_content_revision(pin.clone())
                {
                    // Selection on the destination authority does not
                    // reserve bytes on a different source authority. If
                    // that source lost its final grant before reservation,
                    // retain a terminal outcome rather than reviving it.
                    // Unknown transport outcomes remain recoverable; only
                    // a definitive execution failure plus authoritative
                    // absence can close this job.
                    if matches!(
                        error,
                        DfsError::Metadata { .. } | DfsError::MetadataRejection { .. }
                    ) && !self.content_reference_present(pin)?
                        && self
                            .resolve_retained_revision(&state.request.source)?
                            .is_none()
                    {
                        state.result = Some(DfsContentCopyJobResult::Rejected {
                            reason: format!("content source reservation rejected: {error}"),
                        });
                        self.save_content_copy_state(&mut job, state)?;
                        continue;
                    }
                    return Err(error);
                }
                state.source_reference_applied = true;
                self.save_content_copy_state(&mut job, state)?;
                continue;
            }
            let descriptor = self
                .resolve_retained_revision(&state.request.source)?
                .ok_or_else(|| copy_error("content copy source reservation is unavailable"))?;
            if descriptor.version.content_hash != state.content_hash
                || descriptor.version.size_bytes != state.content_size_bytes
            {
                return Err(copy_error("content copy source content identity changed"));
            }
            let hash = state
                .content_hash
                .as_deref()
                .ok_or_else(|| copy_error("content copy source has no hash"))?;
            let size = state
                .content_size_bytes
                .ok_or_else(|| copy_error("content copy source has no length"))?;
            let input = self.open_verified_content_input(&state.request.source, hash, size)?;
            let result = self.posix_copy_file_content_for_requester(
                &job.root_path,
                &state.request,
                input,
                state.options.clone(),
                &state.requester_node_id,
                &state.write_operation_id,
            );
            match result {
                Ok(stat) => {
                    let committed = DfsRevisionReference {
                        file_system_id: stat.file_system_id,
                        object_id: stat.object_id,
                        version: stat.version,
                    };
                    if committed != state.planned {
                        return Err(copy_error("content copy committed an unexpected identity"));
                    }
                    state.result = Some(DfsContentCopyJobResult::Applied {
                        response: DfsContentCopyResponse {
                            restored_from: state.request.source.clone(),
                            committed,
                            path: stat.path,
                        },
                    });
                    self.save_content_copy_state(&mut job, state)?;
                }
                Err(source) if definitive_copy_rejection(&source) => {
                    // Another exact runner may have committed before this
                    // attempt observed a later destination generation.
                    if self
                        .metadata_backend
                        .as_ref()
                        .expect("content copy backend")
                        .get_intent(&state.planned.object_id, &intent_id)?
                        .is_some_and(|intent| {
                            intent.status == DfsWriteIntentStatus::Committed
                                && intent.publication.is_current()
                                && intent.target_version == state.planned.version
                                && intent.content_hash == state.content_hash
                        })
                    {
                        continue;
                    }
                    state.result = Some(DfsContentCopyJobResult::Rejected {
                        reason: source.to_string(),
                    });
                    self.save_content_copy_state(&mut job, state)?;
                }
                Err(DfsPosixError::PendingWrite { .. }) => break,
                Err(source) => return Err(copy_error(source.to_string())),
            }
        }
        Ok(job)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::network::{InProcessNetwork, InProcessTransport};
    use std::path::Path;
    use std::time::Duration;

    fn service(root: &Path, node: &NodeId) -> NetworkedDfsService<InProcessTransport> {
        let network = InProcessNetwork::new();
        let mut service = NetworkedDfsService::new(
            network.add_node(node.clone()),
            LocalContentStore::new(root.join("content")).unwrap(),
        )
        .with_metadata_backend(open_root_local_metadata_backend(root.join("metadata")).unwrap())
        .unwrap();
        service.bootstrap_distributed_sifs().unwrap();
        service
    }

    fn options(node: &NodeId) -> DfsWriteOptions {
        DfsWriteOptions {
            policy: DfsReplicationPolicy::new(1, 1).unwrap(),
            replica_candidates: vec![node.clone()],
            wait_for_commit: true,
            foreground_replica_repair: false,
        }
    }

    fn granted_source<T: DfsMessageTransport>(
        service: &mut NetworkedDfsService<T>,
        node: &NodeId,
    ) -> DfsRevisionReference {
        let stat = service
            .posix_write_file("/users/source", b"protected source", options(node))
            .unwrap();
        let identity = DfsRevisionReference {
            file_system_id: stat.file_system_id,
            object_id: stat.object_id,
            version: stat.version,
        };
        service
            .retain_content_revision(DfsContentReferenceRequest {
                wire_format: DfsContentReferenceFormat::NamespacedV6,
                namespace: "org.example.retention".into(),
                path: stat.path,
                identity: identity.clone(),
                operation: DfsContentReferenceOperation::CaptureCurrent {
                    reference_id: "authored-source".into(),
                    metadata: None,
                },
                operation_id: "capture-source".into(),
            })
            .unwrap();
        identity
    }

    #[test]
    fn content_copy_resumes_after_cancellation_with_non_history_source_reference() {
        let root = sinomo_testdir::TestDir::new("core-content-copy-cancellation");
        let node = NodeId::from_legacy_named("core-content-copy-cancellation").unwrap();
        let mut service = service(&root, &node);
        let source = granted_source(&mut service, &node);
        let request = DfsContentCopyRequest {
            source,
            destination_path: "/users/copied".into(),
            expected_destination: None,
            operation_id: "copy-source".into(),
        };
        let job = service
            .begin_content_copy_job("/users/copied", &request, &options(&node))
            .unwrap();
        let job = service.run_content_copy_job(job, 2).unwrap();
        let reference = content_copy_job_state(&job)
            .unwrap()
            .source_reference
            .clone()
            .unwrap();
        assert!(
            content_copy_job_state(&job)
                .unwrap()
                .source_reference_applied
        );
        assert!(service.content_reference_present(&reference).unwrap());
        let cancel = DfsCancellationFlag::new();
        service.set_cancellation(Some(cancel.clone()));
        cancel.cancel();
        assert!(matches!(
            service.run_content_copy_job(job.clone(), 8),
            Err(DfsError::Cancelled { .. })
        ));
        drop(service);

        let mut service = self::service(&root, &node);
        assert!(service.content_reference_present(&reference).unwrap());
        let job = service
            .metadata_backend()
            .unwrap()
            .get_namespace_job(&job.job_id)
            .unwrap()
            .unwrap();
        let complete = service.run_content_copy_job(job, 16).unwrap();
        assert_eq!(complete.status, DfsNamespaceJobStatus::Completed);
        assert_eq!(
            service.posix_read_file("/users/copied").unwrap(),
            b"protected source"
        );
        assert!(!service.content_reference_present(&reference).unwrap());
        let replay = service
            .begin_content_copy_job("/users/copied", &request, &options(&node))
            .unwrap();
        assert_eq!(
            content_copy_job_state(&replay).unwrap().result,
            content_copy_job_state(&complete).unwrap().result
        );
    }

    #[test]
    fn content_copy_preserves_a_destination_created_after_admission() {
        let root = sinomo_testdir::TestDir::new("core-content-copy-destination-race");
        let node = NodeId::from_legacy_named("core-content-copy-destination-race").unwrap();
        let mut service = service(&root, &node);
        let source = granted_source(&mut service, &node);
        let request = DfsContentCopyRequest {
            source,
            destination_path: "/users/copied".into(),
            expected_destination: None,
            operation_id: "copy-destination-race".into(),
        };
        let job = service
            .begin_content_copy_job("/users/copied", &request, &options(&node))
            .unwrap();
        let job = service.run_content_copy_job(job, 2).unwrap();
        let reference = content_copy_job_state(&job)
            .unwrap()
            .source_reference
            .clone()
            .unwrap();
        service
            .posix_write_file("/users/copied", b"concurrent destination", options(&node))
            .unwrap();
        let complete = service.run_content_copy_job(job, 16).unwrap();
        assert_eq!(complete.status, DfsNamespaceJobStatus::Failed);
        assert!(matches!(
            content_copy_job_state(&complete).unwrap().result,
            Some(DfsContentCopyJobResult::Rejected { .. })
        ));
        assert_eq!(
            service.posix_read_file("/users/copied").unwrap(),
            b"concurrent destination"
        );
        assert!(!service.content_reference_present(&reference).unwrap());
    }

    fn authored_source_request(source: &DfsRevisionReference) -> DfsContentReferenceRequest {
        DfsContentReferenceRequest {
            wire_format: DfsContentReferenceFormat::NamespacedV6,
            namespace: "org.example.retention".into(),
            path: "/users/source".into(),
            identity: source.clone(),
            operation: DfsContentReferenceOperation::CaptureCurrent {
                reference_id: "authored-source".into(),
                metadata: None,
            },
            operation_id: "capture-source".into(),
        }
    }

    #[test]
    fn exact_noop_release_cannot_remove_a_later_reference_with_the_same_name() {
        let root = sinomo_testdir::TestDir::new("core-reference-noop-release-replay");
        let node = NodeId::from_legacy_named("core-reference-noop-release-replay").unwrap();
        let mut service = service(&root, &node);
        let source = granted_source(&mut service, &node);
        let release = DfsContentReferenceRequest {
            operation: DfsContentReferenceOperation::Release {
                reference_id: "later-consumer".into(),
            },
            operation_id: "release-before-later-consumer".into(),
            ..authored_source_request(&source)
        };
        let before_object = service
            .metadata_backend()
            .unwrap()
            .get_object(&source.object_id)
            .unwrap()
            .unwrap();
        let before_version = service
            .metadata_backend()
            .unwrap()
            .get_version(&source.object_id, &source.version)
            .unwrap()
            .unwrap();
        service.retain_content_revision(release.clone()).unwrap();
        assert_eq!(
            service
                .metadata_backend()
                .unwrap()
                .get_object(&source.object_id)
                .unwrap()
                .unwrap(),
            before_object
        );
        assert_eq!(
            service
                .metadata_backend()
                .unwrap()
                .get_version(&source.object_id, &source.version)
                .unwrap()
                .unwrap(),
            before_version
        );
        let transaction = service
            .metadata_backend()
            .unwrap()
            .get_namespace_transaction("file-history:release-before-later-consumer")
            .unwrap()
            .unwrap();
        assert!(transaction.contribution_digest.is_some());
        assert_eq!(transaction.content_reference_identity, Some(source.clone()));
        assert!(transaction.versions.is_empty());
        let mutation = transaction.file_version_pins.unwrap();
        assert!(mutation.add.is_empty() && mutation.remove.is_empty());
        let retain = DfsContentReferenceRequest {
            operation: DfsContentReferenceOperation::Retain {
                reference_id: "later-consumer".into(),
            },
            operation_id: "retain-after-noop-release".into(),
            ..authored_source_request(&source)
        };
        service.retain_content_revision(retain.clone()).unwrap();
        service.retain_content_revision(release.clone()).unwrap();
        assert!(service.content_reference_present(&retain).unwrap());
        service
            .metadata_backend_mut()
            .unwrap()
            .gc_metadata_history(DfsMetadataHistoryGcOptions::delete_eligible(
                Duration::ZERO,
                0,
            ))
            .unwrap();
        let compact = service
            .metadata_backend()
            .unwrap()
            .get_namespace_transaction_receipt("file-history:release-before-later-consumer")
            .unwrap()
            .unwrap();
        assert_eq!(compact.content_reference_identity, Some(source.clone()));
        assert_eq!(
            compact.contribution_digest,
            Some(
                blake3::hash(&serde_json::to_vec(&release).unwrap())
                    .to_hex()
                    .to_string()
            )
        );
        service.retain_content_revision(release.clone()).unwrap();
        assert!(service.content_reference_present(&retain).unwrap());
        for _ in 0..4 {
            service
                .metadata_backend_mut()
                .unwrap()
                .gc_metadata_history(DfsMetadataHistoryGcOptions::delete_eligible(
                    Duration::ZERO,
                    0,
                ))
                .unwrap();
        }
        assert!(
            service
                .metadata_backend()
                .unwrap()
                .get_namespace_transaction_receipt("file-history:release-before-later-consumer")
                .unwrap()
                .is_none()
        );
        assert!(service.retain_content_revision(release).is_err());
        assert!(service.content_reference_present(&retain).unwrap());
    }

    #[test]
    fn content_copy_rejects_a_selected_source_that_loses_its_last_grant_before_reservation() {
        let root = sinomo_testdir::TestDir::new("core-content-copy-source-gc-race");
        let node = NodeId::from_legacy_named("core-content-copy-source-gc-race").unwrap();
        let mut service = service(&root, &node);
        let source = granted_source(&mut service, &node);
        let request = DfsContentCopyRequest {
            source: source.clone(),
            destination_path: "/users/copied".into(),
            expected_destination: None,
            operation_id: "copy-source-before-gc".into(),
        };
        let job = service
            .begin_content_copy_job("/users/copied", &request, &options(&node))
            .unwrap();
        let selected = service.run_content_copy_job(job, 1).unwrap();
        let private_reference = content_copy_job_state(&selected)
            .unwrap()
            .source_reference
            .clone()
            .unwrap();
        assert!(
            !content_copy_job_state(&selected)
                .unwrap()
                .source_reference_applied
        );
        service
            .retain_content_revision(DfsContentReferenceRequest {
                operation: DfsContentReferenceOperation::Release {
                    reference_id: "authored-source".into(),
                },
                operation_id: "release-source-before-reservation".into(),
                ..authored_source_request(&source)
            })
            .unwrap();
        service.posix_unlink("/users/source").unwrap();
        assert!(
            service
                .object_lifecycle(&source.file_system_id, &source.object_id)
                .unwrap()
                .reclaimable
        );
        // The retained row still exists physically. Structural provenance by
        // itself must not reanimate a deleted, unrooted source.
        assert!(
            service
                .metadata_backend()
                .unwrap()
                .get_version(&source.object_id, &source.version)
                .unwrap()
                .is_some()
        );
        assert!(
            service
                .retain_content_revision(DfsContentReferenceRequest {
                    operation: DfsContentReferenceOperation::Retain {
                        reference_id: "new-after-gc".into()
                    },
                    operation_id: "retain-deleted-source".into(),
                    ..authored_source_request(&source)
                })
                .is_err()
        );
        assert!(
            service
                .object_lifecycle(&source.file_system_id, &source.object_id)
                .unwrap()
                .reclaimable
        );
        let terminal = service.run_content_copy_job(selected, 8).unwrap();
        assert_eq!(terminal.status, DfsNamespaceJobStatus::Failed);
        assert!(matches!(
            content_copy_job_state(&terminal).unwrap().result,
            Some(DfsContentCopyJobResult::Rejected { .. })
        ));
        assert!(content_copy_job_state(&terminal).unwrap().cleanup_complete);
        assert!(
            !service
                .content_reference_present(&private_reference)
                .unwrap()
        );
        assert!(service.posix_stat("/users/copied").is_err());
        let replay = service
            .begin_content_copy_job("/users/copied", &request, &options(&node))
            .unwrap();
        assert_eq!(
            content_copy_job_state(&replay).unwrap().result,
            content_copy_job_state(&terminal).unwrap().result
        );
    }

    #[test]
    fn standalone_reference_replay_uses_full_and_compact_witnesses_before_source_lookup() {
        let root = sinomo_testdir::TestDir::new("core-reference-replay-source-gc");
        let node = NodeId::from_legacy_named("core-reference-replay-source-gc").unwrap();
        let mut service = service(&root, &node);
        let stat = service
            .posix_write_file("/users/source", b"protected source", options(&node))
            .unwrap();
        let source = DfsRevisionReference {
            file_system_id: stat.file_system_id,
            object_id: stat.object_id,
            version: stat.version,
        };
        // Compact the ordinary write witness before creating the reference
        // command. Its receipt can then expire during the later GC passes,
        // while the newly compacted reference outcome remains available.
        let source_intent = service
            .metadata_backend()
            .unwrap()
            .intents_for_object(&source.object_id)
            .unwrap()
            .into_iter()
            .find(|intent| {
                intent.target_version == source.version && intent.publication.is_current()
            })
            .unwrap();
        for _ in 0..4 {
            service
                .metadata_backend_mut()
                .unwrap()
                .gc_metadata_history(DfsMetadataHistoryGcOptions::delete_eligible(
                    Duration::ZERO,
                    0,
                ))
                .unwrap();
            if service
                .metadata_backend()
                .unwrap()
                .get_intent_receipt(&source.object_id, &source_intent.intent_id)
                .unwrap()
                .is_some()
            {
                break;
            }
        }
        assert!(
            service
                .metadata_backend()
                .unwrap()
                .get_intent_receipt(&source.object_id, &source_intent.intent_id)
                .unwrap()
                .is_some()
        );
        let capture = authored_source_request(&source);
        service.retain_content_revision(capture.clone()).unwrap();
        service
            .posix_rename("/users/source", "/users/renamed")
            .unwrap();
        service.retain_content_revision(capture.clone()).unwrap();
        let mut changed_locator = capture.clone();
        changed_locator.path = "/users/renamed".into();
        assert!(service.retain_content_revision(changed_locator).is_err());
        let release = DfsContentReferenceRequest {
            operation: DfsContentReferenceOperation::Release {
                reference_id: "authored-source".into(),
            },
            operation_id: "release-renamed-source".into(),
            ..capture.clone()
        };
        service.retain_content_revision(release.clone()).unwrap();
        service.posix_unlink("/users/renamed").unwrap();
        let future =
            crate::metadata_records::dfs_now_unix_nanos().saturating_add(172_800_000_000_000);
        for _ in 0..4 {
            service
                .metadata_backend_mut()
                .unwrap()
                .gc_metadata_history(
                    DfsMetadataHistoryGcOptions::delete_eligible(Duration::from_secs(86_400), 0)
                        .with_now_unix_nanos(future),
                )
                .unwrap();
        }
        let backend = service.metadata_backend().unwrap();
        assert!(
            backend
                .get_namespace_transaction("file-history:capture-source")
                .unwrap()
                .is_none()
        );
        assert!(
            backend
                .get_namespace_transaction_receipt("file-history:capture-source")
                .unwrap()
                .is_some()
        );
        assert!(
            backend
                .get_version(&source.object_id, &source.version)
                .unwrap()
                .is_none()
        );
        service.retain_content_revision(capture.clone()).unwrap();
        service.retain_content_revision(release).unwrap();
        assert!(!service.content_reference_present(&capture).unwrap());
        let mut changed_payload = capture;
        changed_payload.operation = DfsContentReferenceOperation::CaptureCurrent {
            reference_id: "different-capture".into(),
            metadata: None,
        };
        assert!(service.retain_content_revision(changed_payload).is_err());
    }

    #[test]
    fn source_local_prepared_reference_protects_an_expired_grant_until_abort() {
        let root = sinomo_testdir::TestDir::new("core-reference-prepared-source-root");
        let node = NodeId::from_legacy_named("core-reference-prepared-source-root").unwrap();
        let mut service = service(&root, &node);
        let source = granted_source(&mut service, &node);
        let now = crate::metadata_records::dfs_now_unix_nanos();
        let mut version = service
            .metadata_backend()
            .unwrap()
            .get_version(&source.object_id, &source.version)
            .unwrap()
            .unwrap();
        let state = version.retention.get_mut("org.example.retention").unwrap();
        state.order = 1;
        state.grant.as_mut().unwrap().minimum = Some(DfsOrderedRetentionGrant {
            order: 1,
            granted_at_unix_nanos: now,
            minimum: DfsRetentionMinimum {
                keep_last: 0,
                keep_for_seconds: 1,
            },
        });
        service
            .metadata_backend_mut()
            .unwrap()
            .put_version(&version)
            .unwrap();
        service
            .retain_content_revision(DfsContentReferenceRequest {
                operation: DfsContentReferenceOperation::Release {
                    reference_id: "authored-source".into(),
                },
                operation_id: "release-before-source-prepare".into(),
                ..authored_source_request(&source)
            })
            .unwrap();
        service.posix_unlink("/users/source").unwrap();
        let backend = service.metadata_backend_mut().unwrap();
        let version = backend
            .get_version(&source.object_id, &source.version)
            .unwrap()
            .unwrap();
        let shard_status = backend.metadata_shard_statuses().unwrap()[0].clone();
        let shard_id = shard_status.shard_id;
        let owner_epoch = shard_status.epoch.unwrap_or(1).max(1);
        let transaction = DfsNamespaceTransaction {
            contribution_digest: Some("accepted-source-reference".into()),
            content_reference_identity: None,
            extension_expectations: vec![DfsRevisionMetadataExpectation {
                object_id: source.object_id.clone(),
                version: source.version.clone(),
                expected: Some(Box::new(version.clone())),
            }],
            transaction_id: "prepared-source-reference-tx".into(),
            file_system_id: source.file_system_id.clone(),
            objects: Vec::new(),
            versions: vec![version],
            advisory_lock_states: Vec::new(),
            file_version_pins: Some(DfsFileVersionPinMutation {
                add: vec![DfsMetadataVersionPinRecord {
                    file_system_id: source.file_system_id.clone(),
                    object_id: source.object_id.clone(),
                    version: source.version.clone(),
                    namespace: Some("org.example.retention".into()),
                    pin_id: content_reference_storage_id(
                        "org.example.retention",
                        "prepared-source",
                    ),
                    created_at: OffsetDateTime::now_utc(),
                }],
                remove: Vec::new(),
            }),
            created_at: OffsetDateTime::now_utc(),
        };
        let (expected_object_versions, expected_path_occupants, _) = backend
            .namespace_prepare_expectations(&shard_id, &transaction)
            .unwrap();
        let mut prepare = DfsNamespacePrepareRecord {
            operation_id: "prepared-source-reference".into(),
            file_system_id: source.file_system_id.clone(),
            shard_id,
            owner_epoch,
            operation_digest: String::new(),
            expected_object_versions,
            expected_path_occupants,
            content_receipts: Vec::new(),
            transaction,
            completed_request: None,
            completed_request_completion: None,
            status: DfsNamespacePrepareStatus::Prepared,
        };
        prepare.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&prepare).unwrap();
        backend.prepare_namespace_transaction(&prepare).unwrap();
        assert_eq!(
            backend
                .prepared_content_reference_protection(&source.file_system_id, &source.object_id, 0)
                .unwrap(),
            (false, false)
        );
        std::thread::sleep(Duration::from_millis(1100));
        assert!(
            service
                .resolve_retained_revision(&source)
                .unwrap()
                .is_none()
        );
        assert!(
            !service
                .object_lifecycle(&source.file_system_id, &source.object_id)
                .unwrap()
                .reclaimable
        );
        service
            .metadata_backend_mut()
            .unwrap()
            .abort_prepared_namespace_transaction(
                &prepare.operation_id,
                &prepare.operation_digest,
                prepare.owner_epoch,
            )
            .unwrap();
        assert!(
            service
                .object_lifecycle(&source.file_system_id, &source.object_id)
                .unwrap()
                .reclaimable
        );
        // A new author cannot reuse the old decision timestamp to resurrect
        // the expired grant after its accepted reservation was aborted.
        let mut backdated = prepare;
        backdated.operation_id = "fresh-backdated-source-reference".into();
        backdated.transaction.transaction_id = "fresh-backdated-source-reference-tx".into();
        backdated.operation_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&backdated).unwrap();
        assert!(
            service
                .metadata_backend_mut()
                .unwrap()
                .prepare_namespace_transaction(&backdated)
                .is_err()
        );
    }
}
