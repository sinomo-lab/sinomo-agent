//! Stage-transfer request and response handling.
//!
//! Stage blob, stage patch, and commit handlers report closely related transfer
//! results. This module owns stage validation, persistence, and the shared
//! response shape; the network session owns body framing while repair/fetch/read
//! handling lives in their dedicated service modules.

use sinomo_core::node::NodeId;
use std::time::Instant;

use crate::*;

struct StageBlobPhaseTrace<'a> {
    request_id: &'a str,
    intent_id: &'a str,
    object_id: &'a DfsObjectId,
    version: &'a DfsVersion,
    mode: &'static str,
    started: Option<Instant>,
    last: Option<Instant>,
}

impl<'a> StageBlobPhaseTrace<'a> {
    fn new(command: &'a DfsStageBlobCommand, mode: &'static str) -> Self {
        let started = dfs_perf_trace_enabled().then(Instant::now);
        Self {
            request_id: &command.request_id,
            intent_id: &command.intent_id,
            object_id: &command.object_id,
            version: &command.version,
            mode,
            started,
            last: started,
        }
    }

    fn mark(&mut self, node: &NodeId, phase: &'static str, success: bool) {
        let Some(last) = self.last.as_mut() else {
            return;
        };
        let now = Instant::now();
        let elapsed_us = now.duration_since(*last).as_micros();
        *last = now;
        dfs_perf_trace(format_args!(
            "stage-blob-phase node={} request={} intent={} object={} version={} mode={} phase={} success={} elapsed_us={} total_us={}",
            node,
            self.request_id,
            self.intent_id,
            self.object_id,
            self.version,
            self.mode,
            phase,
            success,
            elapsed_us,
            now.duration_since(self.started.expect("enabled phase trace has a start"))
                .as_micros(),
        ));
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn handle_stage_retained_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
        blob: Option<ContentStoreQuarantinedBlob>,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        self.handle_retained_blob(requester_node_id, command, blob, false)
    }

    pub(crate) fn handle_repair_retained_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
        blob: Option<ContentStoreQuarantinedBlob>,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        self.handle_retained_blob(requester_node_id, command, blob, true)
    }

    fn handle_retained_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
        blob: Option<ContentStoreQuarantinedBlob>,
        repair: bool,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let storage_result = self
            .validate_retained_blob_command(&requester_node_id, &command, repair)
            .and_then(|()| self.store_stage_command_content(&command, blob));
        if !repair {
            return self.finish_stage_blob_result(
                requester_node_id,
                command,
                storage_result,
                false,
            );
        }
        let result = match storage_result {
            Ok(_manifest) => {
                let local_node_id = self.node_id.clone();
                self.put_replica_receipt_if_authoritative(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                )?;
                DfsBlobTransferResult::Durable {
                    content_hash: command.content_hash.clone(),
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_repair_replica_response(
            &requester_node_id,
            command.request_id,
            command.intent_id.clone(),
            command.file_system_id,
            command.object_id.clone(),
            command.version.clone(),
            result.clone(),
            command.receipt_generation,
        )?;
        Ok(self.repair_replica_result_event(
            command.intent_id,
            command.object_id,
            command.version,
            result,
        ))
    }

    fn validate_retained_blob_command(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsStageBlobCommand,
        repair: bool,
    ) -> Result<(), DfsError> {
        let object = command
            .object_metadata
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "retained blob requires its canonical authority object".into(),
            })?;
        if object.file_system_id != command.file_system_id || object.object_id != command.object_id
        {
            return Err(DfsError::Metadata {
                message: "retained blob identity differs from its authority object".into(),
            });
        }
        self.ensure_replica_command_authorized_with_metadata(
            requester_node_id,
            &command.intent_id,
            &command.object_id,
            Some(object),
            command.authority_shard.as_ref(),
        )?;
        self.ensure_replica_command_hash_matches_intent(
            &command.intent_id,
            &command.object_id,
            &command.content_hash,
        )?;
        if let Some(backend) = &self.metadata_backend {
            if let Some(intent) = backend.get_intent(&command.object_id, &command.intent_id)?
                && (intent.file_system_id != command.file_system_id
                    || intent.target_version != command.version
                    || (!repair && intent.publication.is_current()))
            {
                return Err(DfsError::Metadata {
                    message: "retained blob does not match its durable publication intent".into(),
                });
            }
            if let Some(version) = backend.get_version(&command.object_id, &command.version)?
                && version.content_hash.as_deref() != Some(&command.content_hash)
            {
                return Err(DfsError::Metadata {
                    message: "retained blob conflicts with an existing immutable version".into(),
                });
            }
        }
        if let Ok(current) = self.current_read_object_metadata(&command.object_id) {
            if current.file_system_id != command.file_system_id {
                return Err(DfsError::Metadata {
                    message: "retained blob filesystem differs from its canonical object".into(),
                });
            }
            if current.version == command.version
                && current.content_hash.as_deref() != Some(&command.content_hash)
            {
                return Err(DfsError::Metadata {
                    message: "retained blob conflicts with current immutable content".into(),
                });
            }
        }
        self.ensure_metadata_receipt_accepts(
            &command.object_id,
            &command.version,
            &self.node_id,
            &command.content_hash,
        )
    }

    pub(crate) fn handle_stage_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        self.handle_stage_blob_content(requester_node_id, command, None)
    }

    pub(crate) fn handle_preingested_stage_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
        blob: ContentStoreQuarantinedBlob,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        self.handle_stage_blob_content(requester_node_id, command, Some(blob))
    }

    fn handle_stage_blob_content(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
        blob: Option<ContentStoreQuarantinedBlob>,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let mode = if blob.is_some() {
            "preingested"
        } else {
            "inline"
        };
        let mut timing = StageBlobPhaseTrace::new(&command, mode);
        let validation = self.validate_stage_blob_command(&requester_node_id, &command);
        timing.mark(&self.node_id, "validation", validation.is_ok());
        let storage_result =
            validation.and_then(|()| self.store_stage_command_content(&command, blob));
        timing.mark(&self.node_id, "content-persistence", storage_result.is_ok());
        self.finish_stage_blob(requester_node_id, command, storage_result)
    }

    fn store_stage_command_content(
        &self,
        command: &DfsStageBlobCommand,
        blob: Option<ContentStoreQuarantinedBlob>,
    ) -> Result<ContentManifest, DfsError> {
        match blob {
            Some(blob) => {
                if blob.content_hash != command.content_hash {
                    return Err(DfsError::ContentHashMismatch {
                        expected: command.content_hash.clone(),
                        actual: blob.content_hash.clone(),
                    });
                }
                Ok(self.content.bind_quarantined_blob(
                    command.object_id.as_str(),
                    command.version.as_str(),
                    blob,
                )?)
            }
            None => self
                .content
                .put_object_version_with_expected_hash(
                    command.object_id.as_str(),
                    command.version.as_str(),
                    &command.content_hash,
                    &command.bytes,
                )
                .map_err(|error| match error {
                    ContentStoreError::BlobHashMismatch { expected, actual } => {
                        DfsError::ContentHashMismatch { expected, actual }
                    }
                    error => error.into(),
                }),
        }
    }

    fn validate_stage_blob_command(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsStageBlobCommand,
    ) -> Result<(), DfsError> {
        let mut timing = StageBlobPhaseTrace::new(command, "validation");
        let publication =
            self.ensure_current_replica_publication(&command.intent_id, &command.object_id);
        timing.mark(&self.node_id, "current-publication", publication.is_ok());
        publication?;
        let authority = self.ensure_replica_command_authorized_with_metadata(
            requester_node_id,
            &command.intent_id,
            &command.object_id,
            command.object_metadata.as_ref(),
            command.authority_shard.as_ref(),
        );
        timing.mark(&self.node_id, "authority", authority.is_ok());
        authority
            .and_then(|()| {
                let metadata =
                    self.ensure_replica_object_metadata(command.object_metadata.as_ref());
                timing.mark(&self.node_id, "object-metadata", metadata.is_ok());
                metadata
            })
            .and_then(|()| {
                let pending = self.ensure_no_conflicting_pending_replica_write(
                    "stage",
                    requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                );
                timing.mark(&self.node_id, "pending-write-fence", pending.is_ok());
                pending
            })
            .and_then(|()| {
                let base = self.ensure_command_base_matches_current(
                    "stage",
                    &command.object_id,
                    command.expected_base_version.as_ref(),
                    &command.version,
                    &command.content_hash,
                );
                timing.mark(&self.node_id, "base-version-fence", base.is_ok());
                base
            })
            .and_then(|()| {
                let local_node_id = self.node_id.clone();
                let receipt = self.ensure_metadata_receipt_accepts(
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                );
                timing.mark(&self.node_id, "receipt-fence", receipt.is_ok());
                receipt
            })
    }

    fn finish_stage_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
        storage_result: Result<ContentManifest, DfsError>,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        self.finish_stage_blob_result(requester_node_id, command, storage_result, true)
    }

    fn finish_stage_blob_result(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
        storage_result: Result<ContentManifest, DfsError>,
        record_current_stage: bool,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let mut timing = StageBlobPhaseTrace::new(&command, "finish");
        #[cfg(any(test, feature = "test-support"))]
        let native_shard = command
            .authority_shard
            .as_ref()
            .map(|scope| DfsMetadataShardId::new(scope.shard_id.clone()));
        #[cfg(any(test, feature = "test-support"))]
        let mut native_trace = native_shard.as_ref().and_then(|shard| {
            self.native_publication_owner_for_observation()
                .and_then(|(root, owner)| {
                    crate::native_shard_trace::boundary_interest(&root, Some(&owner), shard)
                })
        });
        let result = match storage_result {
            Ok(_manifest) => {
                #[cfg(any(test, feature = "test-support"))]
                if let Some(shard) = native_shard.as_ref() {
                    self.observe_native_content_persistence(
                        shard,
                        &command.file_system_id,
                        &_manifest,
                        crate::native_shard_trace::NativeContentPersistenceKindV1::ManifestBound,
                    );
                }
                let local_node_id = self.node_id.clone();
                let receipt = self.put_replica_receipt_if_authoritative(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                );
                timing.mark(&self.node_id, "receipt-persistence", receipt.is_ok());
                receipt?;
                if record_current_stage {
                    self.record_active_replica_stage(
                        &command.object_id,
                        &command.intent_id,
                        &command.version,
                        &command.content_hash,
                        &requester_node_id,
                    );
                }
                timing.mark(&self.node_id, "active-stage-record", true);
                DfsBlobTransferResult::Durable {
                    content_hash: command.content_hash.clone(),
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        timing.mark(&self.node_id, "response-construction", true);
        // The response consumes part of the command, so trace its remaining
        // identities directly. Disabled tracing performs no clock read.
        let response_started = dfs_perf_trace_enabled().then(Instant::now);
        #[cfg(any(test, feature = "test-support"))]
        let native_return = (native_trace.is_some()
            && matches!(&result, DfsBlobTransferResult::Durable { .. }))
        .then(
            || crate::native_shard_trace::NativeBoundaryFactsV1::ContentStageDurableReturn {
                authenticated_requester_node_id: requester_node_id.clone(),
                command: (&command).into(),
                response: DfsStageBlobResponse {
                    request_id: command.request_id.clone(),
                    intent_id: command.intent_id.clone(),
                    file_system_id: command.file_system_id.clone(),
                    object_id: command.object_id.clone(),
                    version: command.version.clone(),
                    replica_node_id: self.node_id.clone(),
                    result: result.clone(),
                    receipt_generation: command.receipt_generation,
                },
            },
        );
        let response = self.send_stage_blob_response(
            &requester_node_id,
            command.request_id.clone(),
            command.intent_id.clone(),
            command.file_system_id,
            command.object_id.clone(),
            command.version.clone(),
            result.clone(),
            command.receipt_generation,
        );
        if let Some(started) = response_started {
            dfs_perf_trace(format_args!(
                "stage-blob-phase node={} request={} intent={} object={} version={} mode=finish phase=response-send success={} elapsed_us={}",
                self.node_id,
                command.request_id,
                command.intent_id,
                command.object_id,
                command.version,
                response.is_ok(),
                started.elapsed().as_micros(),
            ));
        }
        response?;
        #[cfg(any(test, feature = "test-support"))]
        if let (Some(trace), Some(fact)) = (native_trace.as_mut(), native_return) {
            trace.observe(fact, None, None);
        }
        Ok(self.stage_blob_result_event(
            command.intent_id,
            command.object_id,
            command.version,
            result,
        ))
    }

    pub(crate) fn handle_stage_patch(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStagePatchCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self
            .ensure_current_replica_publication(&command.intent_id, &command.object_id)
            .and_then(|()| {
                self.ensure_replica_command_authorized_with_metadata(
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    command.object_metadata.as_ref(),
                    command.authority_shard.as_ref(),
                )
            })
            .and_then(|()| self.ensure_replica_object_metadata(command.object_metadata.as_ref()))
            .and_then(|()| {
                self.ensure_no_conflicting_pending_replica_write(
                    "stage-patch",
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_command_base_matches_current(
                    "stage-patch",
                    &command.object_id,
                    Some(&command.base_version),
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let local_node_id = self.node_id.clone();
                self.ensure_metadata_receipt_accepts(
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let base_identity = self.patch_base_identity_for_current_object(
                    &command.object_id,
                    &command.base_version,
                )?;
                self.store_command_patch(
                    &command.object_id,
                    &command.base_version,
                    &command.version,
                    &command.content_hash,
                    command.offset,
                    &command.patch_hash,
                    &command.bytes,
                    base_identity.as_ref(),
                )
            }) {
            Ok(()) => {
                let local_node_id = self.node_id.clone();
                self.put_replica_receipt_if_authoritative(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                )?;
                self.record_active_replica_stage(
                    &command.object_id,
                    &command.intent_id,
                    &command.version,
                    &command.content_hash,
                    &requester_node_id,
                );
                DfsBlobTransferResult::Durable {
                    content_hash: command.content_hash.clone(),
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_stage_blob_response(
            &requester_node_id,
            command.request_id.clone(),
            command.intent_id.clone(),
            command.file_system_id,
            command.object_id.clone(),
            command.version.clone(),
            result.clone(),
            command.receipt_generation,
        )?;
        Ok(self.stage_blob_result_event(
            command.intent_id,
            command.object_id,
            command.version,
            result,
        ))
    }

    pub(crate) fn send_stage_blob_response(
        &mut self,
        requester_node_id: &NodeId,
        request_id: String,
        intent_id: String,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        result: DfsBlobTransferResult,
        receipt_generation: u64,
    ) -> Result<(), DfsError> {
        self.send_dfs_message_if_routable(
            requester_node_id,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id,
                intent_id,
                file_system_id,
                object_id,
                version,
                replica_node_id: self.node_id.clone(),
                result,
                receipt_generation,
            }),
        )?;
        Ok(())
    }

    pub(crate) fn stage_blob_result_event(
        &self,
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        result: DfsBlobTransferResult,
    ) -> NetworkedDfsServiceEvent {
        match result {
            DfsBlobTransferResult::Durable { content_hash }
            | DfsBlobTransferResult::Committed { content_hash } => {
                NetworkedDfsServiceEvent::BlobStaged {
                    intent_id,
                    object_id,
                    version,
                    replica_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::BlobStageRejected {
                    intent_id,
                    object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        }
    }

    pub(crate) fn handle_commit_version(
        &mut self,
        requester_node_id: NodeId,
        command: DfsCommitVersionCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let preflight = self
            .ensure_current_replica_publication(&command.intent_id, &command.object_id)
            .and_then(|()| {
                self.ensure_replica_command_authorized_with_metadata(
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    command.object_metadata.as_ref(),
                    command.authority_shard.as_ref(),
                )
            })
            .and_then(|()| self.ensure_replica_object_metadata(command.object_metadata.as_ref()))
            .and_then(|()| {
                self.ensure_replica_command_hash_matches_intent(
                    &command.intent_id,
                    &command.object_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_no_conflicting_pending_replica_write(
                    "commit",
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
            });
        let commit = preflight.and_then(|()| {
            if let Err(content_error) = self.ensure_commit_version_content_available(&command) {
                return match self.transition_missing_local_commit_content_to_repair(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                ) {
                    Ok(true) => Err(DfsError::CommitContentRepairPending {
                        object_id: command.object_id.clone(),
                        version: command.version.clone(),
                        content_hash: command.content_hash.clone(),
                    }),
                    Ok(false) => Err(content_error),
                    Err(transition_error) => Err(DfsError::Metadata {
                        message: format!(
                            "{content_error}; failed to transition stale replica receipt to repair: {transition_error}"
                        ),
                    }),
                };
            }
            // A delayed notification may follow a newer visible head. Exact
            // durable commit evidence settles that notification without replaying
            // an old object into the current namespace or changing its receipts.
            if self.commit_promotion_already_durable(&command)? {
                return Ok(());
            }
            self.ensure_command_base_matches_current(
                "commit",
                &command.object_id,
                command.expected_base_version.as_ref(),
                &command.version,
                &command.content_hash,
            )?;
            let local_node_id = self.node_id.clone();
            self.commit_replica_metadata_cache(
                &command.file_system_id,
                &command.object_id,
                &command.version,
                &local_node_id,
                &command.content_hash,
                command.object_metadata.as_ref(),
            )
        });
        let result = match commit {
            Ok(()) => {
                // The version is now durably committed on this replica, so the
                // in-flight stage guard for it is no longer active.
                self.clear_active_replica_stage(&command.object_id, &command.version);
                DfsBlobTransferResult::Committed {
                    content_hash: command.content_hash.clone(),
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_commit_version_response(
            &requester_node_id,
            command.request_id.clone(),
            command.intent_id.clone(),
            command.file_system_id.clone(),
            command.object_id.clone(),
            command.version.clone(),
            result.clone(),
        )?;
        Ok(self.commit_version_result_event(command, result))
    }

    fn commit_promotion_already_durable(
        &self,
        command: &DfsCommitVersionCommand,
    ) -> Result<bool, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(false);
        };
        let Some(intent) = backend.get_intent(&command.object_id, &command.intent_id)? else {
            return Ok(false);
        };
        if intent.file_system_id != command.file_system_id
            || intent.target_version != command.version
            || intent.status != DfsWriteIntentStatus::Committed
            || !intent.publication.is_current()
            || intent.content_hash.as_deref() != Some(command.content_hash.as_str())
        {
            return Ok(false);
        }
        let Some(version) = backend.get_version(&command.object_id, &command.version)? else {
            return Ok(false);
        };
        if version.file_system_id != command.file_system_id
            || !version.visible
            || version.content_hash.as_deref() != Some(command.content_hash.as_str())
        {
            return Ok(false);
        }
        Ok(self
            .backend_receipt_for_replica(&command.object_id, &command.version, &self.node_id)?
            .is_some_and(|receipt| {
                receipt.receipt.file_system_id == command.file_system_id
                    && receipt.content_hash.as_deref() == Some(command.content_hash.as_str())
            }))
    }

    fn ensure_commit_version_content_available(
        &self,
        command: &DfsCommitVersionCommand,
    ) -> Result<(), DfsError> {
        let manifest = self
            .content
            .read_manifest(command.object_id.as_str(), command.version.as_str())?;
        if manifest.content_hash != command.content_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: command.content_hash.clone(),
                actual: manifest.content_hash,
            });
        }
        if !self.content.manifest_content_available(&manifest)? {
            return Err(DfsError::ContentStore {
                message: format!(
                    "committed content bytes are missing for {} {}",
                    command.object_id, command.version
                ),
            });
        }
        Ok(())
    }

    fn send_commit_version_response(
        &mut self,
        requester_node_id: &NodeId,
        request_id: String,
        intent_id: String,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        result: DfsBlobTransferResult,
    ) -> Result<(), DfsError> {
        self.send_dfs_message_if_routable(
            requester_node_id,
            DfsNetworkMessage::CommitVersionResponse(DfsCommitVersionResponse {
                request_id,
                intent_id,
                file_system_id,
                object_id,
                version,
                replica_node_id: self.node_id.clone(),
                result,
            }),
        )?;
        Ok(())
    }

    fn commit_version_result_event(
        &self,
        command: DfsCommitVersionCommand,
        result: DfsBlobTransferResult,
    ) -> NetworkedDfsServiceEvent {
        match result {
            DfsBlobTransferResult::Committed { content_hash }
            | DfsBlobTransferResult::Durable { content_hash } => {
                NetworkedDfsServiceEvent::VersionCommitted {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::RepairRejected {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        }
    }

    /// A stage reply is also the outcome of that replica's attempt in the
    /// repair executor's registry, whichever lane observes it.
    pub(crate) fn handle_stage_blob_response(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsStageBlobResponse,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let key = (
            response.object_id.clone(),
            response.version.clone(),
            response.replica_node_id.clone(),
        );
        let request_id = response.request_id.clone();
        let event = self.handle_stage_blob_response_inner(frame_source_node_id, response);
        // Every exit after the reply ends its attempt; one that failed counts
        // as a failed attempt with its error.
        let (outcome, error) = match &event {
            Ok(Some(NetworkedDfsServiceEvent::BlobStaged { .. })) => {
                (crate::repair_executor::DfsRepairAttemptOutcome::Settled, None)
            }
            Ok(event) => (
                crate::repair_executor::DfsRepairAttemptOutcome::Failed,
                event
                    .as_ref()
                    .and_then(crate::repair_executor::repair_event_error),
            ),
            Err(error) => (
                crate::repair_executor::DfsRepairAttemptOutcome::Failed,
                Some(error.to_string()),
            ),
        };
        crate::repair_executor::repair_executor_attempt_ended_with_error(
            &self.node_id,
            &key,
            &request_id,
            outcome,
            error,
        );
        event
    }

    fn handle_stage_blob_response_inner(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsStageBlobResponse,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if frame_source_node_id != response.replica_node_id {
            let reason = format!(
                "DFS stage response source mismatch: frame source {}, payload replica {}",
                frame_source_node_id, response.replica_node_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                replica_node_id: frame_source_node_id,
                reason,
            }));
        }
        match response.result {
            DfsBlobTransferResult::Durable { ref content_hash }
            | DfsBlobTransferResult::Committed { ref content_hash } => {
                // Another admitted Main lane can have committed the abort
                // while this cached coordinator still awaits this real reply.
                // The exact durable terminal tuple denies a new receipt; it
                // grants no publication or recovery authority to the sender.
                let durable_closed = self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_intent(&response.object_id, &response.intent_id))
                    .transpose()?
                    .flatten()
                    .is_some_and(|record| {
                        record.file_system_id == response.file_system_id
                            && record.object_id == response.object_id
                            && record.intent_id == response.intent_id
                            && record.target_version == response.version
                            && matches!(
                                record.status,
                                DfsWriteIntentStatus::Aborted | DfsWriteIntentStatus::Rejected
                                    | DfsWriteIntentStatus::PreconditionFailed { .. }
                            )
                    });
                if durable_closed
                    || self
                        .coordinator
                        .intent(&response.intent_id)
                        .is_some_and(|intent| {
                            matches!(
                                intent.state,
                                DfsWriteIntentState::Aborted | DfsWriteIntentState::Rejected
                                    | DfsWriteIntentState::PreconditionFailed { .. }
                            )
                        })
                {
                    let intent_id = response.intent_id.clone();
                    return Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: format!("missing pending write for stage response: {intent_id}"),
                    }));
                }
                self.restore_pending_write_for_intent_if_missing(
                    &response.intent_id,
                    &response.object_id,
                )?;
                if let Err(error) =
                    self.ensure_stage_response_matches_pending(&response, content_hash)
                {
                    return Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    }));
                }
                self.record_replica_transfer_completed(
                    &response.intent_id,
                    &response.replica_node_id,
                    &response.request_id,
                    true,
                )?;
                let event =
                    self.with_placement_intent_authority(&response.intent_id, |service| {
                        service.recover_write_publications_before_receipt(
                            &response.intent_id,
                            content_hash,
                        )?;
                        if let Err(error) = service.put_metadata_receipt(
                            &response.file_system_id,
                            &response.object_id,
                            &response.version,
                            &response.replica_node_id,
                            content_hash,
                            response.receipt_generation,
                        ) {
                            if !matches!(error, DfsError::ReceiptGenerationSuperseded { .. }) {
                                return Err(error);
                            }
                            // A withdrawal after this stage was planned
                            // supersedes it; a later attempt plans afresh.
                            service.record_stage_retry_scheduled(
                                &response.intent_id,
                                &response.replica_node_id,
                            );
                            return Ok(NetworkedDfsServiceEvent::BlobStageRejected {
                                intent_id: response.intent_id.clone(),
                                object_id: response.object_id.clone(),
                                replica_node_id: response.replica_node_id.clone(),
                                reason: error.to_string(),
                            });
                        }
                        let intent_was_committed = service
                            .coordinator
                            .intent(&response.intent_id)
                            .is_some_and(|intent| {
                                matches!(intent.state, DfsWriteIntentState::Committed)
                            });
                        let updated_intent = service.coordinator.record_durable_receipt(
                            &response.intent_id,
                            response.replica_node_id.clone(),
                        )?;
                        #[cfg(any(test, feature = "test-support"))]
                        service.observe_native_coordinator_receipt(
                            None,
                            &updated_intent,
                            &response.replica_node_id,
                        );
                        let event = NetworkedDfsServiceEvent::BlobStaged {
                            intent_id: response.intent_id.clone(),
                            object_id: response.object_id.clone(),
                            version: response.version.clone(),
                            replica_node_id: response.replica_node_id.clone(),
                            content_hash: content_hash.clone(),
                        };
                        if intent_was_committed {
                            service.finalize_late_committed_stage_receipt_under_authority(
                                &updated_intent.intent_id,
                                &response.replica_node_id,
                            )?;
                        } else {
                            service
                                .try_apply_committed_intent_if_ready(&updated_intent.intent_id)?;
                        }
                        service.send_untracked_completed_routed_write_response_if_committed(
                            &updated_intent.intent_id,
                            &response.object_id,
                        )?;
                        if let Some(pending) =
                            service.writes.pending_writes.get_mut(&response.intent_id)
                        {
                            pending
                                .stage_inflight_request_ids
                                .remove(&response.replica_node_id);
                            pending.stage_retries.remove(&response.replica_node_id);
                        }
                        service.cleanup_committed_pending_write_if_repaired(&response.intent_id);
                        Ok(event)
                    })?;
                Ok(Some(event.unwrap_or_else(|| {
                    NetworkedDfsServiceEvent::BlobStageRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: "write intent is closed or its placement policy was superseded"
                            .to_string(),
                    }
                })))
            }
            DfsBlobTransferResult::Rejected { ref reason } => {
                let pending_hash = self
                    .writes
                    .pending_writes
                    .get(&response.intent_id)
                    .map(|pending| pending.content_hash.clone());
                let correlation = pending_hash
                    .as_deref()
                    .ok_or_else(|| DfsError::Distributed {
                        message: "missing exact pending stage for rejected response".into(),
                    })
                    .and_then(|hash| self.ensure_stage_response_matches_pending(&response, hash));
                match correlation {
                    Ok(()) => {
                        self.record_replica_transfer_completed(
                            &response.intent_id,
                            &response.replica_node_id,
                            &response.request_id,
                            true,
                        )?;
                        self.record_stage_patch_fallback(
                            &response.intent_id,
                            &response.replica_node_id,
                        );
                        self.record_stage_retry_scheduled(
                            &response.intent_id,
                            &response.replica_node_id,
                        );
                        let object_metadata = self.current_object_for_write(&response.object_id)?;
                        self.stage_missing_desired_replicas_for_intent(
                            &response.intent_id,
                            object_metadata,
                        )?;
                        Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                            intent_id: response.intent_id,
                            object_id: response.object_id,
                            replica_node_id: response.replica_node_id,
                            reason: reason.clone(),
                        }))
                    }
                    Err(error) => Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    })),
                }
            }
        }
    }

    pub(crate) fn ensure_stage_response_request_matches_pending(
        &self,
        response: &DfsStageBlobResponse,
    ) -> Result<(), DfsError> {
        let exact_pending = self
            .writes
            .pending_writes
            .get(&response.intent_id)
            .is_some_and(|pending| {
                pending
                    .stage_inflight_request_ids
                    .get(&response.replica_node_id)
                    == Some(&response.request_id)
            });
        if exact_pending {
            return Ok(());
        }
        Err(DfsError::Distributed {
            message: format!(
                "DFS stage response has no exact current same-kind request: intent {}, replica {}, request {}",
                response.intent_id, response.replica_node_id, response.request_id
            ),
        })
    }

    pub(crate) fn ensure_stage_response_matches_pending(
        &self,
        response: &DfsStageBlobResponse,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        self.ensure_stage_response_request_matches_pending(response)?;
        let pending = self
            .writes
            .pending_writes
            .get(&response.intent_id)
            .expect("pending write validated");
        self.ensure_response_matches_pending_write(
            "stage",
            &response.intent_id,
            &response.file_system_id,
            &response.object_id,
            &response.version,
            &response.replica_node_id,
            content_hash,
            pending,
        )
    }
}
