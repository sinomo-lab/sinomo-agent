//! Bind the public path command to the same terminal shard prepare as its
//! write. The namespace receipt remains usable after rename, deletion, or a
//! subsequent write. It contains the content digest rather than a second blob.
use super::*;
use crate::{
    DaemonDfsBrowserWriteReceipt, DfsNamespaceObjectPrecondition, DfsNamespaceRequestCompletion,
    DfsNamespaceRequestRecord, DfsNamespaceResult, DfsPosixCoordinationResult,
};

pub(crate) fn command_digest(
    fs: &DfsFileSystemId,
    requester: &NodeId,
    path: &str,
    expected: Option<&DfsNamespaceObjectPrecondition>,
    hash: &str,
    size: u64,
) -> std::result::Result<String, serde_json::Error> {
    let mut digest = blake3::Hasher::new();
    serde_json::to_writer(
        &mut digest,
        &(
            "dfs-owner-write-v1",
            fs,
            requester,
            path,
            expected,
            hash,
            size,
        ),
    )?;
    Ok(format!("blake3:{}", digest.finalize().to_hex()))
}

fn receipt_for(object: &DfsObjectMetadata) -> DaemonDfsBrowserWriteReceipt {
    DaemonDfsBrowserWriteReceipt {
        object_id: object.object_id.to_string(),
        version: object.version.to_string(),
        content_hash: object.content_hash.clone(),
        size_bytes: object.content_size_bytes,
    }
}

pub(crate) fn completion(
    request: Option<&DfsNamespaceRequestRecord>,
    intent: &crate::DfsWriteIntentRecord,
    object: &DfsObjectMetadata,
    transaction: &crate::DfsNamespaceTransaction,
) -> Option<DfsNamespaceRequestCompletion> {
    let request = request?;
    let DfsNamespaceOperation::WriteFile { path, .. } = &request.operation else {
        return None;
    };
    if path != &object.path
        || intent.operation_id.as_ref() != Some(&request.request_id)
        || request.requester_node_id != intent.coordinator_node_id
    {
        return None;
    }
    Some(DfsNamespaceRequestCompletion {
        file_system_id: request.file_system_id.clone(),
        requester_node_id: request.requester_node_id.clone(),
        request_id: request.request_id.clone(),
        operation_id: request.operation_id.clone(),
        authority_shard_id: request.authority_shard_id.clone(),
        command_digest: request.command_digest.clone(),
        transaction_id: transaction.transaction_id.clone(),
        result: DfsNamespaceResult::Applied,
        posix_coordination_result: Some(DfsPosixCoordinationResult::FileWritten(receipt_for(
            object,
        ))),
        created_at: request.created_at,
        completed_at: transaction.created_at.max(request.created_at),
    })
}

/// Verify the compact receipt from the immutable mutation even at a replica.
pub(crate) fn proves_completion(
    prepare: &crate::DfsPreparedOperationPayload,
    completion: &DfsNamespaceRequestCompletion,
) -> bool {
    let Some(DfsPosixCoordinationResult::FileWritten(receipt)) =
        &completion.posix_coordination_result
    else {
        return false;
    };
    let Some(intent) = prepare.terminal_intent.as_ref() else {
        return false;
    };
    let Some(object) = prepare
        .transaction
        .objects
        .iter()
        .find(|object| object.object_id == intent.object_id)
    else {
        return false;
    };
    let (Some(hash), Some(size)) = (object.content_hash.as_deref(), object.content_size_bytes)
    else {
        return false;
    };
    let expected = match &intent.publication {
        crate::DfsWritePublication::Current => None,
        crate::DfsWritePublication::ConditionalCurrent { precondition } => match &**precondition {
            crate::DfsCurrentWritePrecondition::CompareAndSwap { path, expected }
                if path == &object.path
                    && expected.object_id == object.object_id
                    && intent.base_version.as_ref() == Some(&expected.version) =>
            {
                Some(expected)
            }
            _ => return false,
        },
        _ => return false,
    };
    completion.requester_node_id == intent.coordinator_node_id
        && intent.operation_id.as_ref() == Some(&completion.request_id)
        && intent.intent_id
            == crate::intent::stable_write_intent_id(
                &completion.requester_node_id,
                &object.object_id,
                &completion.request_id,
            )
        && prepare.operation_id == format!("write:{}", intent.intent_id)
        && completion.operation_id
            == crate::service_routed_namespace::namespace_request_operation_id(
                &completion.requester_node_id,
                &completion.request_id,
            )
        && object.version == intent.target_version
        && receipt == &receipt_for(object)
        && command_digest(
            &prepare.file_system_id,
            &completion.requester_node_id,
            &object.path,
            expected,
            hash,
            size,
        )
        .as_ref()
        .ok()
            == Some(&completion.command_digest)
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// The committed reply may be replayed after the head changed. Promote
    /// the staged local version only while that exact current publication
    /// still names this receiver as a content holder.
    pub(crate) fn finish_owner_write_content(
        &self,
        fs: &DfsFileSystemId,
        path: &str,
        receipt: &DaemonDfsBrowserWriteReceipt,
    ) -> std::result::Result<(), DfsError> {
        let Some(object) = self.authoritative_metadata_object_for_path(fs, path)? else {
            return Ok(());
        };
        if !object.current_locations.contains(&self.node_id)
            || object.object_id.as_str() != receipt.object_id
            || object.version.as_str() != receipt.version
        {
            return Ok(());
        }
        let completion = receipt.content_hash.as_deref().zip(receipt.size_bytes)
            .and_then(|(hash, size)| {
                crate::service_routed_write::CanonicalCurrentWriteCompletion::from_current_publication(
                    Some(&crate::DfsWritePublication::Current), &object, &object.version, hash, size,
                )
            })
            .ok_or_else(|| DfsError::Metadata { message: "path write receipt and current content identity disagree".into() })?;
        completion.publish(&self.content)?;
        Ok(())
    }

    /// Existing-parent writes take one primary command. The transport streams
    /// larger values as a content attachment to the same admitted handler.
    pub(crate) fn try_route_browser_write(
        &mut self,
        path: &str,
        bytes: &[u8],
        expected: Option<DfsNamespaceObjectPrecondition>,
    ) -> Result<Option<crate::DaemonDfsBrowserWriteReceipt>> {
        if self.control_plane.is_none() {
            return Ok(None);
        }
        let operation_id = self.next_request_id();
        let expected_object = expected.as_ref().map(|guard| guard.object_id.clone());
        let operation = DfsNamespaceOperation::WriteFile {
            path: path.into(),
            bytes: crate::protocol::DfsOwnerWriteContent::Inline(bytes::Bytes::copy_from_slice(
                bytes,
            )),
            expected,
        };
        let result = if let Some(primary) = self.routed_namespace_primary_for_path(path)? {
            self.route_namespace_operation_with_request_id_and_result(
                operation_id.clone(),
                primary,
                operation,
            )?
        } else {
            self.local_browser_write_reply(operation_id.clone(), operation)?
        };
        let uncertain = || {
            DfsPosixError::from(DfsError::NetworkDelivery {
                message: format!(
                    "owner write {operation_id} returned no matching committed receipt"
                ),
                delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
            })
        };
        match result {
            Some(DfsPosixCoordinationResult::FileWritten(receipt)) => {
                let object = DfsObjectId::new(&receipt.object_id).map_err(|_| uncertain())?;
                if expected_object
                    .as_ref()
                    .is_some_and(|expected| expected != &object)
                    || crate::intent::stable_write_target_version(
                        &self.node_id,
                        &object,
                        &operation_id,
                    )
                    .as_str()
                        != receipt.version
                    || receipt.size_bytes != Some(bytes.len() as u64)
                    || receipt.content_hash.as_ref() != Some(&crate::content_hash(bytes))
                {
                    return Err(uncertain());
                }
                Ok(Some(receipt))
            }
            Some(DfsPosixCoordinationResult::FileWriteNeedsPathResolution) => Ok(None),
            _ => Err(uncertain()),
        }
    }

    pub(super) fn local_browser_write_reply(
        &mut self,
        operation_id: String,
        operation: DfsNamespaceOperation,
    ) -> Result<Option<DfsPosixCoordinationResult>> {
        let (result, receipt) = self.execute_local_primary_namespace_reply(
            sifs_file_system_id(),
            operation_id,
            operation,
        )?;
        match result {
            DfsNamespaceResult::Applied => Ok(receipt),
            DfsNamespaceResult::Rejected {
                reason,
                posix_error,
                ..
            } => Err(crate::posix_model::posix_error_for_namespace_rejection(
                reason,
                posix_error,
            )),
        }
    }

    pub(crate) fn write_browser_file_at_owner(
        &mut self,
        requester: &NodeId,
        operation_id: &str,
        path: &str,
        content: &crate::protocol::DfsOwnerWriteContent,
        blob: Option<crate::ContentStoreQuarantinedBlob>,
        expected: Option<&DfsNamespaceObjectPrecondition>,
    ) -> Result<DfsPosixCoordinationResult> {
        use crate::protocol::DfsOwnerWriteContent;
        let bytes = match (content, blob.as_ref()) {
            (DfsOwnerWriteContent::Inline(bytes), None) => bytes.as_ref(),
            (
                DfsOwnerWriteContent::Attachment {
                    content_hash,
                    size_bytes,
                },
                Some(blob),
            ) if blob.content_hash == *content_hash && blob.size_bytes == *size_bytes => &[],
            _ => {
                return Err(DfsError::Metadata {
                    message: "path write requires matching received bytes".into(),
                }
                .into());
            }
        };
        crate::service_routed_write::validate_write_operation_id(operation_id)?;
        let fs = sifs_file_system_id();
        self.ensure_path_not_in_cross_shard_flight(&fs, path)?;
        self.ensure_path_not_reserved_by_directory_move(&fs, path)?;
        let _plan = self.namespace_execution.begin_planning();
        self.with_metadata_lookup_cache_mut(|service| {
            let Some(parent) = parent_posix_path(path) else {
                return Err(DfsPosixError::IsDirectory {
                    path: path.to_owned(),
                });
            };
            let Some(cut) = service.namespace_execution.context() else {
                return Ok(DfsPosixCoordinationResult::FileWriteNeedsPathResolution);
            };
            let target =
                service.select_sifs_metadata_shard_for_path_from_snapshot(path, &cut.shards)?;
            let parent_shard =
                service.select_sifs_metadata_shard_for_path_from_snapshot(&parent, &cut.shards)?;
            if cut.file_system_id != fs
                || target
                    .as_ref()
                    .is_none_or(|shard| shard.primary_node_id != service.node_id)
                || target != parent_shard
                || service.resolve_posix_path_distributed(path, true)? != path
            {
                return Ok(DfsPosixCoordinationResult::FileWriteNeedsPathResolution);
            }
            let Some(directory) = service.lookup_sifs_path(&parent)? else {
                return Ok(DfsPosixCoordinationResult::FileWriteNeedsPathResolution);
            };
            if directory.kind != DfsObjectKind::Directory
                || service.lookup_sifs_path(path)?.is_some_and(|object| {
                    matches!(
                        object.kind,
                        DfsObjectKind::Hardlink | DfsObjectKind::Symlink
                    )
                })
            {
                return Ok(DfsPosixCoordinationResult::FileWriteNeedsPathResolution);
            }
            let Some(control) = service.control_plane.as_deref() else {
                return Ok(DfsPosixCoordinationResult::FileWriteNeedsPathResolution);
            };
            let configuration = match control
                .write_configuration_readiness()
                .map_err(DfsError::from)?
            {
                crate::DfsWriteConfigurationReadiness::Ready(Some(view)) => view,
                crate::DfsWriteConfigurationReadiness::Ready(None) => {
                    return Ok(DfsPosixCoordinationResult::FileWriteNeedsPathResolution);
                }
                crate::DfsWriteConfigurationReadiness::Pending => {
                    return Err(DfsError::Metadata {
                        message: "write configuration awaits authoritative projection".into(),
                    }
                    .into());
                }
            };
            let candidates = configuration
                .content_replica_candidates()
                .map_err(DfsError::from)?;
            let options = crate::browser_service::sifs_browser_write_options_with_configuration(
                service,
                path,
                &candidates,
                Some(&configuration),
            )
            .map_err(|e| DfsError::Metadata {
                message: e.to_string(),
            })?;
            let commit = service.posix_write_file_bytes_once_commit(
                path,
                bytes,
                options,
                true,
                expected.is_none(),
                false,
                None,
                false,
                false,
                expected.map(|p| (&p.object_id, &p.version)),
                false,
                Some(operation_id),
                Some(&directory),
                expected.map_or(crate::DfsWritePublication::Current, |expected| {
                    crate::DfsWritePublication::ConditionalCurrent {
                        precondition: Box::new(
                            crate::DfsCurrentWritePrecondition::CompareAndSwap {
                                path: path.into(),
                                expected: expected.clone(),
                            },
                        ),
                    }
                }),
                Some(requester),
                blob,
            )?;
            let stat = service.posix_write_commit_stat(commit)?;
            Ok(DfsPosixCoordinationResult::FileWritten(
                crate::browser_service::daemon_dfs_browser_write_receipt(&stat),
            ))
        })
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Only the exact native record of this caller's bytes and condition can
    /// cross a released admission. A wake is never permission to plan again.
    pub(crate) fn browser_write_prepare(
        &self,
        requester: &NodeId,
        command: &crate::DfsNamespaceCommand,
        operation_id: &str,
    ) -> std::result::Result<Option<crate::DfsPreparedOperationRecord>, DfsError> {
        let DfsNamespaceOperation::WriteFile {
            path,
            bytes,
            expected,
        } = &command.operation
        else {
            return Ok(None);
        };
        let [shard] = command.authority_shards.as_slice() else {
            return Ok(None);
        };
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        let Some(record) = backend.prepared_operation_record(operation_id)? else {
            return Ok(None);
        };
        record.validate()?;
        if requester != &command.requesting_node_id
            || record.file_system_id != command.file_system_id
            || record.shard_id.0 != shard.shard_id
            || record.owner_epoch != shard.epoch
            || shard.primary_node_id != self.node_id
            || !matches!(
                record.status,
                crate::DfsPreparedOperationStatus::Prepared
                    | crate::DfsPreparedOperationStatus::Committed
            )
        {
            return Ok(None);
        }
        if let Some(completed) = &record.completed_request_completion {
            return Ok((completed.requester_node_id == *requester
                && completed.request_id == command.request_id
                && completed.command_digest == crate::namespace_command_digest(command)?
                && proves_completion(&record.payload, completed))
            .then_some(record));
        }
        let Some(intent) = record
            .metadata_effects
            .iter()
            .find_map(|effect| match effect {
                crate::DfsPreparedMetadataEffect::PutIntent { intent } => Some(intent),
                _ => None,
            })
        else {
            return Ok(None);
        };
        let (hash, _size) = bytes.identity();
        if intent.coordinator_node_id != *requester
            || intent.operation_id.as_ref() != Some(&command.request_id)
            || intent
                .object_metadata
                .as_ref()
                .is_none_or(|o| o.path != *path || o.object_id != intent.object_id)
            || intent.intent_id
                != crate::intent::stable_write_intent_id(
                    requester,
                    &intent.object_id,
                    &command.request_id,
                )
            || intent.target_version
                != crate::intent::stable_write_target_version(
                    requester,
                    &intent.object_id,
                    &command.request_id,
                )
            || intent.content_hash.as_deref() != Some(&hash)
            || match (&intent.publication, expected) {
                (crate::DfsWritePublication::Current, None) => false,
                (crate::DfsWritePublication::ConditionalCurrent { precondition }, Some(wanted)) => {
                    !matches!(&**precondition,
                    crate::DfsCurrentWritePrecondition::CompareAndSwap {path:p,expected:original} if p==path && original==wanted && wanted.object_id==intent.object_id && intent.base_version.as_ref()==Some(&wanted.version))
                }
                _ => true,
            }
        {
            return Ok(None);
        }
        crate::service_routed_write::validate_retained_write_opening(
            &record,
            intent,
            &crate::intent::distributed_intent_from_record(intent),
            &hash,
        )?;
        Ok(Some(record))
    }

    pub(crate) fn resume_browser_write_at_owner(
        &mut self,
        requester: &NodeId,
        command: &crate::DfsNamespaceCommand,
        digest: &crate::DfsPreparedOperationDigest,
    ) -> Result<DfsPosixCoordinationResult> {
        let record = self
            .browser_write_prepare(requester, command, &digest.operation_id)?
            .filter(|record| record.digest == *digest)
            .ok_or_else(|| DfsError::Metadata {
                message: "conditional write no longer owns its retained preparation".into(),
            })?;
        self.commit_stored_prepared_operation(digest)?;
        if let Some(receipt) = record
            .completed_request_completion
            .as_ref()
            .and_then(|c| c.posix_coordination_result.clone())
        {
            return Ok(receipt);
        }
        let original = record
            .metadata_effects
            .iter()
            .find_map(|effect| match effect {
                crate::DfsPreparedMetadataEffect::PutIntent { intent } => Some(intent),
                _ => None,
            })
            .expect("validated opening");
        let DfsNamespaceOperation::WriteFile { bytes, .. } = &command.operation else {
            unreachable!()
        };
        let (hash, size) = bytes.identity();
        self.restore_pending_write_for_intent_if_missing(&original.intent_id, &original.object_id)?;
        self.restore_backend_receipt_pages_for_intent(original)?;
        let intent = self
            .coordinator
            .intent(&original.intent_id)
            .cloned()
            .ok_or_else(|| DfsError::Metadata {
                message: "retained opening did not restore its original write".into(),
            })?;
        let outcome = self
            .replayed_routed_write_outcome_for_intent(&intent, &hash, size)?
            .ok_or_else(|| DfsError::Metadata {
                message: "retained opening has no recoverable write outcome".into(),
            })?;
        match self.settle_ready_write_outcome(outcome, false)? {
            NetworkedDfsWriteOutcome::Committed {
                version,
                content_hash,
                ..
            } if version == original.target_version && content_hash == hash => Ok(
                DfsPosixCoordinationResult::FileWritten(DaemonDfsBrowserWriteReceipt {
                    object_id: original.object_id.to_string(),
                    version: version.to_string(),
                    content_hash: Some(content_hash),
                    size_bytes: Some(size),
                }),
            ),
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } => Err(DfsPosixError::PendingWrite {
                intent_id,
                durable_receipts,
                required_receipts,
            }),
            _ => Err(DfsError::Metadata {
                message: "retained opening returned another write outcome".into(),
            }
            .into()),
        }
    }
}
