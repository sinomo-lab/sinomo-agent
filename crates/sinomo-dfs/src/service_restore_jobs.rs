//! Durable restore execution and source-pin release, owned by namespace jobs.

use crate::*;
use std::time::Duration;
use time::OffsetDateTime;

fn restore_error(message: impl Into<String>) -> DfsError {
    DfsError::Metadata {
        message: message.into(),
    }
}

pub(crate) use crate::file_history::file_restore_job_id;

fn begin_request_id(requester: &NodeId, operation_id: &str) -> String {
    let key = file_restore_job_id(requester, operation_id);
    let order = crate::intent::stable_operation_order_token(operation_id)
        .unwrap_or_else(|| "opaque".into());
    format!("restore-begin:{key}_{order}")
}

pub(crate) fn restore_job_state(
    job: &DfsNamespaceJobRecord,
) -> Result<&DfsFileRestoreJobState, DfsError> {
    let [DfsNamespaceJobFrame::FileHistoryRestore { state }] = job.pending_frames.as_slice() else {
        return Err(restore_error("restore job has no unique durable state"));
    };
    if job.kind != DfsNamespaceJobKind::FileHistoryRestore {
        return Err(restore_error("unexpected restore job kind"));
    }
    Ok(state)
}

fn response_from_result(
    result: &DfsFileRestoreJobResult,
) -> Result<DfsFileHistoryRestoreResponse, DfsError> {
    match result {
        DfsFileRestoreJobResult::Applied { response } => Ok(response.clone()),
        DfsFileRestoreJobResult::Rejected { reason } => Err(restore_error(reason.clone())),
    }
}

fn definitive_restore_rejection(source: &DfsPosixError) -> bool {
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

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn find_file_restore_job(
        &mut self,
        job_id: &str,
    ) -> Result<Option<(DfsMetadataShardConfig, DfsFileHistoryShardResult)>, DfsError> {
        for shard in self.file_history_authorities(&sifs_file_system_id())? {
            if !shard.namespace_prepare_codec.supports_file_history() {
                continue;
            }
            let result = self.query_file_history_authority(
                &shard,
                DfsFileHistorySelection::RestoreJob {
                    job_id: job_id.into(),
                },
            )?;
            match &result {
                DfsFileHistoryShardResult::RestoreJob { job, receipt }
                    if job.is_some() || receipt.is_some() =>
                {
                    if let Some(job) = job {
                        crate::file_history::validate_file_restore_job(job)?;
                    }
                    if let Some(receipt) = receipt {
                        crate::file_history::validate_file_restore_job_receipt(receipt)?;
                    }
                    return Ok(Some((shard, result)));
                }
                DfsFileHistoryShardResult::RestoreJob { .. } => {}
                _ => {
                    return Err(restore_error(
                        "restore authority returned the wrong query kind",
                    ));
                }
            }
        }
        Ok(None)
    }

    pub(crate) fn file_history_pin_present(
        &mut self,
        identity: &DfsFileVersionIdentity,
        pin_id: &str,
    ) -> Result<bool, DfsError> {
        for shard in self.file_history_authorities(&identity.file_system_id)? {
            if !shard.namespace_prepare_codec.supports_file_history() {
                continue;
            }
            match self.query_file_history_authority(
                &shard,
                DfsFileHistorySelection::VersionPin {
                    identity: identity.clone(),
                    pin_id: pin_id.into(),
                },
            )? {
                DfsFileHistoryShardResult::VersionPin {
                    identity: actual,
                    pin_id: actual_pin,
                    present,
                } if actual == *identity && actual_pin == pin_id => {
                    if present {
                        return Ok(true);
                    }
                }
                _ => {
                    return Err(restore_error(
                        "history authority returned an unrelated pin witness",
                    ));
                }
            }
        }
        Ok(false)
    }

    pub(crate) fn restore_file_history_durable(
        &mut self,
        request: DfsFileHistoryRestoreRequest,
        options: DfsWriteOptions,
    ) -> Result<DfsFileHistoryRestoreResponse, DfsError> {
        crate::service_routed_write::validate_write_operation_id(&request.operation_id)?;
        if request
            .expected_destination
            .as_ref()
            .is_some_and(|expected| expected.file_system_id != sifs_file_system_id())
        {
            return Err(restore_error(
                "restore destination must name the SIFS filesystem",
            ));
        }
        let requester = self.node_id.clone();
        let job_id = file_restore_job_id(&requester, &request.operation_id);
        // A completed result is independent of both source retention and the
        // destination's later namespace generations.
        let existing = self.find_file_restore_job(&job_id)?;
        let (shard, mut job) = if let Some((shard, result)) = existing {
            match result {
                DfsFileHistoryShardResult::RestoreJob { job: Some(job), .. } => (shard, *job),
                DfsFileHistoryShardResult::RestoreJob {
                    receipt: Some(receipt),
                    ..
                } => {
                    let outcome = receipt.file_restore.ok_or_else(|| {
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
                .file_restore_destination_path(&request)
                .map_err(|source| restore_error(source.to_string()))?;
            self.require_file_history_shard_support(&sifs_file_system_id(), &path)?;
            let shard = if self.control_plane.is_some() {
                self.namespace_shards_for_exact_paths(&sifs_file_system_id(), [path.as_str()])?
                    .into_iter()
                    .next()
                    .ok_or_else(|| restore_error("restore has no destination authority"))?
            } else {
                self.file_history_authorities(&sifs_file_system_id())?
                    .remove(0)
            };
            let operation = DfsNamespaceOperation::BeginFileHistoryRestore {
                path: path.clone(),
                request: request.clone(),
                options: options.clone(),
            };
            let begin_id = begin_request_id(&requester, &request.operation_id);
            let job = if self.control_plane.is_none() {
                self.begin_file_restore_job(&path, &request, &options)?
            } else if shard.primary_node_id == self.node_id {
                self.execute_local_primary_namespace_operation_for_file_system(
                    sifs_file_system_id(),
                    begin_id,
                    operation,
                )
                .map_err(|source| restore_error(source.to_string()))?;
                self.metadata_backend
                    .as_ref()
                    .ok_or_else(|| restore_error("restore requires durable metadata"))?
                    .get_namespace_job(&job_id)?
                    .ok_or_else(|| restore_error("restore admission omitted its durable job"))?
            } else {
                let result = self
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
        job = if self.control_plane.is_none() || shard.primary_node_id == self.node_id {
            self.posix_resume_namespace_job_bounded(&job_id, 4)
                .map_err(|source| restore_error(source.to_string()))?
        } else {
            let result = self
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

    pub(crate) fn begin_file_restore_job(
        &mut self,
        path: &str,
        request: &DfsFileHistoryRestoreRequest,
        options: &DfsWriteOptions,
    ) -> Result<DfsNamespaceJobRecord, DfsError> {
        crate::service_routed_write::validate_write_operation_id(&request.operation_id)?;
        self.require_file_history_shard_support(&sifs_file_system_id(), path)?;
        if self
            .namespace_execution
            .context()
            .is_some_and(|context| context.file_system_id != sifs_file_system_id())
        {
            return Err(restore_error("restore admission has a foreign filesystem"));
        }
        let requester = self
            .namespace_execution
            .request()
            .map(|request| request.requester_node_id.clone())
            .unwrap_or_else(|| self.node_id.clone());
        let job_id = file_restore_job_id(&requester, &request.operation_id);
        if let Some(job) = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| restore_error("restore requires durable metadata"))?
            .get_namespace_job(&job_id)?
        {
            let state = restore_job_state(&job)?;
            if state.request != *request || state.requester_node_id != requester {
                return Err(restore_error(
                    "restore operation identity was reused with different input",
                ));
            }
            return Ok(job);
        }
        if self
            .file_restore_destination_path(request)
            .map_err(|source| restore_error(source.to_string()))?
            != path
        {
            return Err(restore_error("restore destination moved during admission"));
        }
        options.policy.validate()?;
        let standalone = if self.control_plane.is_none() {
            Some(self.admit_standalone_file_restore(path, request, options)?)
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
        let planned = DfsFileVersionIdentity {
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
            kind: DfsNamespaceJobKind::FileHistoryRestore,
            status: DfsNamespaceJobStatus::Running,
            root_path: path.into(),
            target_path: None,
            pending_frames: vec![DfsNamespaceJobFrame::FileHistoryRestore {
                state: Box::new(DfsFileRestoreJobState {
                    request: request.clone(),
                    requester_node_id: requester,
                    options: options.clone(),
                    write_operation_id,
                    planned,
                    source_pin: None,
                    source_pin_applied: false,
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
            .map_err(|source| restore_error(source.to_string()))?;
        if let Some(mut admission) = standalone {
            admission.status = DfsNamespaceRequestStatus::Completed;
            admission.result = Some(DfsNamespaceResult::Applied);
            admission.posix_coordination_result =
                Some(DfsPosixCoordinationResult::NamespaceJob(job.clone()));
            admission.updated_at = OffsetDateTime::now_utc();
            self.metadata_backend
                .as_mut()
                .expect("restore backend")
                .put_namespace_request_on_shard(&admission.authority_shard_id, &admission)?;
        }
        Ok(job)
    }

    fn admit_standalone_file_restore(
        &mut self,
        path: &str,
        request: &DfsFileHistoryRestoreRequest,
        options: &DfsWriteOptions,
    ) -> Result<DfsNamespaceRequestRecord, DfsError> {
        let requester = self.node_id.clone();
        let request_id = begin_request_id(&requester, &request.operation_id);
        let operation = DfsNamespaceOperation::BeginFileHistoryRestore {
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
            .ok_or_else(|| restore_error("restore requires durable metadata"))?;
        let shard = backend
            .metadata_shard_statuses()?
            .into_iter()
            .next()
            .ok_or_else(|| restore_error("restore backend has no shard"))?
            .shard_id;
        let admission =
            backend.namespace_request_admission_snapshot(&shard, &requester, &request_id)?;
        if let Some(existing) = admission.request {
            if existing.command_digest == digest
                && existing.status == DfsNamespaceRequestStatus::Pending
            {
                return Ok(existing);
            }
            return Err(restore_error(
                "restore admission already completed or has different input; its result is unavailable",
            ));
        }
        if admission.receipt.is_some() || admission.history_expired {
            return Err(restore_error(
                "restore operation outcome expired; refusing to repeat the write",
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

    fn save_file_restore_state(
        &mut self,
        job: &mut DfsNamespaceJobRecord,
        state: DfsFileRestoreJobState,
    ) -> Result<(), DfsError> {
        job.updated_at = OffsetDateTime::now_utc();
        job.processed_object_count = job.processed_object_count.saturating_add(1);
        if state.cleanup_complete {
            job.completed_at = Some(job.updated_at);
            job.status = match &state.result {
                Some(DfsFileRestoreJobResult::Applied { .. }) => DfsNamespaceJobStatus::Completed,
                Some(DfsFileRestoreJobResult::Rejected { reason }) => {
                    job.failure_reason = Some(reason.clone());
                    DfsNamespaceJobStatus::Failed
                }
                None => {
                    return Err(restore_error(
                        "restore cannot finish without a durable outcome",
                    ));
                }
            };
        }
        job.pending_frames = vec![DfsNamespaceJobFrame::FileHistoryRestore {
            state: Box::new(state),
        }];
        self.persist_namespace_job(job)
            .map_err(|source| restore_error(source.to_string()))?;
        if let Some(latest) = self
            .metadata_backend
            .as_ref()
            .expect("restore backend")
            .get_namespace_job(&job.job_id)?
        {
            *job = latest;
        }
        Ok(())
    }

    pub(crate) fn run_file_restore_job(
        &mut self,
        job: DfsNamespaceJobRecord,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord, DfsError> {
        // Source queries and pins are independent namespace requests, possibly
        // on another authority. They must not complete or overwrite the
        // request which asked this durable job to resume.
        let outer = std::mem::take(&mut self.namespace_execution);
        let result = self.run_file_restore_job_independent(job, max_steps);
        self.namespace_execution = outer;
        if self.namespace_execution.request().is_some() {
            self.namespace_execution.record_job();
        }
        result
    }

    fn run_file_restore_job_independent(
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
            if let Some(latest) = self
                .metadata_backend
                .as_ref()
                .ok_or_else(|| restore_error("restore requires durable metadata"))?
                .get_namespace_job(&job.job_id)?
            {
                job = latest;
            }
            if job.status != DfsNamespaceJobStatus::Running {
                break;
            }
            let mut state = restore_job_state(&job)?.clone();
            if state.result.is_some() {
                if let Some(pin) = &state.source_pin {
                    let DfsFileHistoryPinOperation::Pin { pin_id } = &pin.operation else {
                        return Err(restore_error("restore source reservation is not a pin"));
                    };
                    let release = (|| {
                        if self.file_history_pin_present(&pin.identity, pin_id)? {
                            self.file_history_pin(DfsFileHistoryPinRequest {
                                operation: DfsFileHistoryPinOperation::Unpin {
                                    pin_id: pin_id.clone(),
                                },
                                operation_id: format!("restore-unpin:{}", job.job_id),
                                ..pin.clone()
                            })?;
                        }
                        Ok::<_, DfsError>(())
                    })();
                    if let Err(source) = release {
                        tracing::warn!(job_id = %job.job_id, error = %source, "restore source pin cleanup remains pending");
                        break;
                    }
                }
                state.cleanup_complete = true;
                self.save_file_restore_state(&mut job, state)?;
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
                .expect("restore backend")
                .get_intent(&state.planned.object_id, &intent_id)?
            {
                if !intent.publication.is_current()
                    || intent.file_system_id != state.planned.file_system_id
                    || intent.target_version != state.planned.version
                    || intent.content_hash != state.content_hash
                    || intent.coordinator_node_id != state.requester_node_id
                {
                    return Err(restore_error(
                        "restore write has an unrelated durable identity",
                    ));
                }
                match intent.status {
                    DfsWriteIntentStatus::Committed => {
                        state.result = Some(DfsFileRestoreJobResult::Applied {
                            response: DfsFileHistoryRestoreResponse {
                                restored_from: state.request.source.clone(),
                                committed: state.planned.clone(),
                                path: job.root_path.clone(),
                            },
                        });
                        self.save_file_restore_state(&mut job, state)?;
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
                        state.result = Some(DfsFileRestoreJobResult::Rejected {
                            reason: format!("restore write terminated with {:?}", intent.status),
                        });
                        self.save_file_restore_state(&mut job, state)?;
                        continue;
                    }
                }
            }
            if state.source_pin.is_none() {
                let descriptor = self.resolve_file_history_version(&state.request.source)?;
                let Some(descriptor) = descriptor.filter(|descriptor| {
                    descriptor
                        .version
                        .retained
                        .as_ref()
                        .is_some_and(|retained| {
                            matches!(
                                retained.object.kind,
                                DfsObjectKind::File | DfsObjectKind::Artifact
                            )
                        })
                }) else {
                    state.result = Some(DfsFileRestoreJobResult::Rejected {
                        reason: "history-not-retained: restore requires a retained regular file"
                            .into(),
                    });
                    self.save_file_restore_state(&mut job, state)?;
                    continue;
                };
                state.content_hash = descriptor.version.content_hash;
                state.content_size_bytes = descriptor.version.size_bytes;
                state.source_pin = Some(DfsFileHistoryPinRequest {
                    path: descriptor.current_object.path,
                    identity: state.request.source.clone(),
                    operation: DfsFileHistoryPinOperation::Pin {
                        pin_id: format!("restore:{}", job.job_id),
                    },
                    operation_id: format!("restore-pin:{}", job.job_id),
                });
                self.save_file_restore_state(&mut job, state)?;
                continue;
            }
            if !state.source_pin_applied {
                let pin = state.source_pin.as_ref().expect("source selected");
                let DfsFileHistoryPinOperation::Pin { pin_id } = &pin.operation else {
                    return Err(restore_error("invalid restore source pin"));
                };
                if !self.file_history_pin_present(&pin.identity, pin_id)? {
                    self.file_history_pin(pin.clone())?;
                }
                state.source_pin_applied = true;
                self.save_file_restore_state(&mut job, state)?;
                continue;
            }
            let descriptor = self
                .resolve_file_history_version(&state.request.source)?
                .ok_or_else(|| restore_error("restore source reservation is unavailable"))?;
            if descriptor.version.content_hash != state.content_hash
                || descriptor.version.size_bytes != state.content_size_bytes
            {
                return Err(restore_error("restore source content identity changed"));
            }
            let input = self.open_file_restore_reader(&descriptor)?;
            let result = self.posix_restore_file_content_for_requester(
                &job.root_path,
                &state.request,
                input,
                state.options.clone(),
                &state.requester_node_id,
                &state.write_operation_id,
            );
            match result {
                Ok(stat) => {
                    let committed = DfsFileVersionIdentity {
                        file_system_id: stat.file_system_id,
                        object_id: stat.object_id,
                        version: stat.version,
                    };
                    if committed != state.planned {
                        return Err(restore_error("restore committed an unexpected identity"));
                    }
                    state.result = Some(DfsFileRestoreJobResult::Applied {
                        response: DfsFileHistoryRestoreResponse {
                            restored_from: state.request.source.clone(),
                            committed,
                            path: stat.path,
                        },
                    });
                    self.save_file_restore_state(&mut job, state)?;
                }
                Err(source) if definitive_restore_rejection(&source) => {
                    // Another exact runner may have committed before this
                    // attempt observed a later destination generation.
                    if self
                        .metadata_backend
                        .as_ref()
                        .expect("restore backend")
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
                    state.result = Some(DfsFileRestoreJobResult::Rejected {
                        reason: source.to_string(),
                    });
                    self.save_file_restore_state(&mut job, state)?;
                }
                Err(DfsPosixError::PendingWrite { .. }) => break,
                Err(source) => return Err(restore_error(source.to_string())),
            }
        }
        Ok(job)
    }
}
