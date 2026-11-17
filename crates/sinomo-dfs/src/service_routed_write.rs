//! Routed distributed-write request and response lifecycle.
//!
//! This module owns metadata-primary routed writes: replay detection,
//! requester-side response handling, responder-side pending response tracking,
//! authority redirects, and deferred parent-directory touches for newly
//! created objects. Generic inbound message dispatch remains in
//! `service_protocol`.

use crate::service_write::{
    DfsDistributedWriteOptions, DfsWriteCreation, DfsWriteDispatch, DfsWriteRepair,
};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use sinomo_core::DfsWriteRequest;
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;

use crate::intent::{
    stable_distributed_intent_matches_routed_write_command, stable_operation_order_token,
    stable_write_intent_id, stable_write_intent_record_matches_routed_write_command,
};
use crate::*;

pub(crate) const DFS_ROUTED_WRITE_LATE_RESPONSE_TTL: Duration = Duration::from_secs(60);
pub(crate) const DFS_ROUTED_WRITE_LATE_RESPONSE_MAX_REQUESTS: usize = 1024;
const DFS_WRITE_OPERATION_ID_MAX_BYTES: usize = 256;

pub fn validate_write_operation_id(operation_id: &str) -> Result<(), DfsError> {
    if operation_id.is_empty()
        || operation_id.len() > DFS_WRITE_OPERATION_ID_MAX_BYTES
        || operation_id.chars().any(char::is_control)
    {
        return Err(DfsError::Serialization {
            message: format!(
                "write operation id must contain 1..={DFS_WRITE_OPERATION_ID_MAX_BYTES} bytes and no control characters"
            ),
        });
    }
    Ok(())
}

pub(crate) fn prepared_intent_for_write<'a>(
    prepared: &'a DfsPreparedOperationRecord,
    object_id: &DfsObjectId,
    intent_id: &str,
) -> Option<&'a DfsWriteIntentRecord> {
    prepared
        .payload
        .prepared_intent
        .iter()
        .chain(prepared.payload.terminal_intent.iter())
        .chain(
            prepared
                .payload
                .metadata_effects
                .iter()
                .filter_map(|effect| match effect {
                    DfsPreparedMetadataEffect::PutIntent { intent } => Some(intent),
                    _ => None,
                }),
        )
        .find(|intent| intent.object_id == *object_id && intent.intent_id == intent_id)
}

/// A retry may recover only this exact initial opening, not an unrelated
/// metadata operation or a new publication body derived from its effects.
pub(crate) fn validate_retained_write_opening(
    record: &DfsPreparedOperationRecord,
    original: &DfsWriteIntentRecord,
    intent: &DistributedWriteIntent,
    content_hash: &str,
) -> Result<(), DfsError> {
    record.validate()?;
    if record.payload.prepared_intent.is_some()
        || record.payload.terminal_intent.is_some()
        || !record.payload.transaction.objects.is_empty()
        || !record.payload.transaction.versions.is_empty()
        || record.payload.completed_request.is_some()
        || record.payload.completed_request_completion.is_some()
        || crate::intent::distributed_intent_from_record(original) != *intent
        || original.content_hash.as_deref() != Some(content_hash)
        || record.digest.owner_epoch != original.owner_epoch
    {
        return Err(DfsError::WriteReconciliationRequired {
            operation_id: intent.operation_id.clone().unwrap_or_else(|| intent.intent_id.clone()),
            object_id: intent.object_id.clone(),
            reason: "retained opening differs from the original write; native reconciliation is required".into(),
        });
    }
    let mut intents = 0;
    let allowed = record
        .payload
        .metadata_effects
        .iter()
        .all(|effect| match effect {
            DfsPreparedMetadataEffect::PutIntent { intent: captured } => {
                intents += 1;
                captured == original
            }
            DfsPreparedMetadataEffect::PutReceipt { receipt } => {
                receipt.receipt.file_system_id == intent.file_system_id
                    && receipt.receipt.object_id == intent.object_id
                    && receipt.receipt.committed_version == intent.target_version
                    && receipt.content_hash.as_deref() == Some(content_hash)
                    && intent
                        .desired_replica_nodes
                        .contains(&receipt.replica_node_id)
            }
            DfsPreparedMetadataEffect::PutFileSystem { file_system } => {
                file_system.file_system_id == intent.file_system_id
            }
            _ => false,
        });
    if !allowed || intents != 1 {
        return Err(DfsError::WriteReconciliationRequired {
            operation_id: intent
                .operation_id
                .clone()
                .unwrap_or_else(|| intent.intent_id.clone()),
            object_id: intent.object_id.clone(),
            reason: "write opening has unrelated prepared effects".into(),
        });
    }
    Ok(())
}

/// Compact requester-local identity needed to publish a manifest after an
/// authoritative routed write commits. The content hash and version arrive in
/// the terminal outcome; the local content store proves whether this node
/// actually owns the blob and supplies its exact size.
#[derive(Clone, Debug, Eq, PartialEq)]
struct RoutedWriteManifestPublicationReceipt {
    request_id: String,
    object_id: DfsObjectId,
    file_system_id: DfsFileSystemId,
    path: Option<String>,
    publication: Option<DfsWritePublication>,
}

/// Minted only at routed completion, after response authority validation and
/// journal replay. A staging receipt or a locally readable blob cannot mint it.
pub(crate) struct CanonicalCurrentWriteCompletion {
    manifest: ContentManifest,
}

impl CanonicalCurrentWriteCompletion {
    fn from_current_publication(
        publication: Option<&DfsWritePublication>,
        object: &DfsObjectMetadata,
        version: &DfsVersion,
        content_hash: &str,
        size_bytes: u64,
    ) -> Option<Self> {
        if !publication.is_some_and(DfsWritePublication::is_current)
            || !matches!(object.kind, DfsObjectKind::File | DfsObjectKind::Artifact)
            || object.version != *version
            || object.content_hash.as_deref() != Some(content_hash)
            || object.content_size_bytes != Some(size_bytes)
        {
            return None;
        }
        Some(Self {
            manifest: ContentManifest {
                object_id: object.object_id.to_string(),
                version: version.to_string(),
                content_hash: content_hash.to_owned(),
                size_bytes,
                extents: Vec::new(),
            },
        })
    }

    pub(crate) fn manifest(&self) -> &ContentManifest {
        &self.manifest
    }
}

#[cfg(test)]
mod canonical_completion_proof_tests {
    use super::*;
    use sinomo_core::DfsObjectPolicy;

    #[test]
    fn native_primary_rejects_requester_supplied_fresh_link_aggregate() {
        let name = "native-fresh-count-admission";
        let (runtime, initial, shard) =
            crate::test_support::current_native_metadata_service(name, 1, "/services").unwrap();
        drop(initial);
        let mut owner = runtime.metadata_primary_service("/services/forged-fresh-count").unwrap();
        assert!(!owner.transport.delegates_local_namespace_authority());
        let mut offered = crate::posix_object::new_posix_metadata_object(
            &shard.primary_node_id, "/services/forged-fresh-count", DfsObjectKind::File,
        );
        offered.namespace_link_count = Some(99);
        let request = DfsWriteRequest {
            file_system_id: offered.file_system_id.clone(),
            object_id: offered.object_id.clone(),
            target_version: crate::namespace::next_version(&offered.version),
            requesting_node_id: shard.primary_node_id.clone(),
        };
        let error = owner.ensure_routed_write_object_metadata(
            &request, Some(&offered), true, None,
        ).unwrap_err();
        assert!(error.to_string().contains("count one"), "actual fresh-owner check: {error}");
        assert!(owner.metadata_backend().unwrap().get_object(&offered.object_id).unwrap().is_none());
        drop(owner);
        let mut caller = runtime.client(&shard.primary_node_id).unwrap();
        let created = caller.posix_write_file(
            &offered.path, b"owner creates one name",
            DfsWriteOptions::replicated_eventual(vec![shard.primary_node_id.clone()]),
        ).unwrap();
        assert_eq!(created.link_count, 1);
        drop(caller);
        runtime.close().unwrap();
    }

    #[test]
    fn current_completion_proof_rejects_retained_unknown_and_superseded_publication() {
        let version = DfsVersion::from_legacy_named("completion-version").unwrap();
        let hash = content_hash(b"canonical bytes");
        let size = b"canonical bytes".len() as u64;
        let current = DfsWritePublication::default();
        let object = DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named("completion-object").unwrap(),
            file_system_id: sifs_file_system_id(),
            path: "/completion".into(),
            namespace_link_count: (DfsObjectKind::File).initial_namespace_link_count(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: NodeId::from_legacy_named("completion-primary").unwrap(),
            current_locations: Vec::new(),
            content_hash: Some(hash.clone()),
            content_size_bytes: Some(size),
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: version.clone(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        let proof = CanonicalCurrentWriteCompletion::from_current_publication(
            Some(&current),
            &object,
            &version,
            &hash,
            size,
        )
        .unwrap();
        assert_eq!(proof.manifest().object_id, object.object_id.to_string());
        assert!(
            CanonicalCurrentWriteCompletion::from_current_publication(
                None, &object, &version, &hash, size,
            )
            .is_none()
        );
        let retained = DfsWritePublication::Retained {
            capture: Box::new(crate::DfsRetainedContentPublication {
                namespace: "module.dfs.history".into(),
                reference_id: "completion-pin".into(),
                metadata: Some(crate::DfsModuleMetadataValue {
                    schema_version: 1,
                    payload: serde_json::to_value(DfsFileVersionSource::LocalOperation {
                        operation_id: "completion-operation".into(),
                        base_version: None,
                    })
                    .unwrap(),
                }),
            }),
        };
        assert!(
            CanonicalCurrentWriteCompletion::from_current_publication(
                Some(&retained),
                &object,
                &version,
                &hash,
                size,
            )
            .is_none()
        );
        for change in [
            "version",
            "hash",
            "size",
            "unknown-size",
            "tombstone",
            "unresolved-hardlink",
        ] {
            let mut advanced = object.clone();
            match change {
                "version" => {
                    advanced.version =
                        DfsVersion::from_legacy_named("completion-next-version").unwrap()
                }
                "hash" => advanced.content_hash = Some(content_hash(b"subsequent bytes")),
                "size" => advanced.content_size_bytes = Some(size + 1),
                "unknown-size" => advanced.content_size_bytes = None,
                "tombstone" => advanced.kind = DfsObjectKind::Tombstone,
                "unresolved-hardlink" => advanced.kind = DfsObjectKind::Hardlink,
                _ => unreachable!(),
            }
            assert!(
                CanonicalCurrentWriteCompletion::from_current_publication(
                    Some(&current),
                    &advanced,
                    &version,
                    &hash,
                    size,
                )
                .is_none(),
                "{change} must retain generic renewal"
            );
        }
    }
}

fn routed_write_command_content_size_bytes(command: &DfsDistributedWriteCommand) -> Option<u64> {
    if command.content_hash.is_some() {
        command.content_size_bytes
    } else {
        Some(command.bytes.len() as u64)
    }
}

pub(crate) fn routed_exclusive_create_event_id(
    command: &DfsDistributedWriteCommand,
) -> Option<EventId> {
    if !command.exclusive_create {
        return None;
    }
    let object = command.object_metadata.as_ref()?;
    let identity = format!(
        "{}\0{}\0{}\0{:?}\0{}\0{}\0{}",
        command.request_id,
        command.request.object_id,
        object.path,
        object.kind,
        object.mode,
        object.uid,
        object.gid,
    );
    Some(EventId::derived(
        "sinomo-dfs.routed-exclusive-create.v1",
        identity.as_bytes(),
    ))
}

fn stable_write_operation_receipt(
    operation_id: &str,
    record: &DfsWriteIntentRecord,
    content_size_bytes: Option<u64>,
) -> DfsStableWriteOperationReceipt {
    DfsStableWriteOperationReceipt {
        operation_id: operation_id.to_string(),
        intent_id: record.intent_id.clone(),
        file_system_id: record.file_system_id.clone(),
        object_id: record.object_id.clone(),
        target_version: record.target_version.clone(),
        content_hash: record.content_hash.clone(),
        content_size_bytes,
        status: record.status.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
fn classify_stable_write_operation(
    operation_id: &str,
    expected_intent_id: &str,
    expected_file_system_id: &DfsFileSystemId,
    expected_object_id: &DfsObjectId,
    expected_target_version: &DfsVersion,
    expected_requester_node_id: &NodeId,
    expected_content_hash: &str,
    expected_content_size_bytes: u64,
    record: Option<&DfsWriteIntentRecord>,
    recorded_content_size_bytes: Option<u64>,
    history_expired: bool,
    locally_authoritative: bool,
) -> DfsStableWriteOperationReconciliation {
    if let Some(record) = record {
        if !record.publication.is_current()
            || record.intent_id != expected_intent_id
            || record.file_system_id != *expected_file_system_id
            || record.object_id != *expected_object_id
            || record.target_version != *expected_target_version
            || record.coordinator_node_id != *expected_requester_node_id
            || record.content_hash.as_deref() != Some(expected_content_hash)
            || recorded_content_size_bytes.is_some_and(|size| size != expected_content_size_bytes)
        {
            return DfsStableWriteOperationReconciliation::Conflict {
                reason: format!(
                    "write operation id {operation_id} is bound to a different durable request digest"
                ),
            };
        }
        let receipt =
            stable_write_operation_receipt(operation_id, record, recorded_content_size_bytes);
        return match record.status {
            DfsWriteIntentStatus::Committed => {
                DfsStableWriteOperationReconciliation::Applied { receipt }
            }
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending => {
                DfsStableWriteOperationReconciliation::AmbiguousOrExpired {
                    reason: format!(
                        "write operation {operation_id} has durable non-terminal admission status {:?}",
                        record.status
                    ),
                }
            }
            DfsWriteIntentStatus::Rejected | DfsWriteIntentStatus::Aborted
                | DfsWriteIntentStatus::PreconditionFailed { .. } => {
                DfsStableWriteOperationReconciliation::Conflict {
                    reason: format!(
                        "write operation {operation_id} terminated without committing (status {:?})",
                        record.status
                    ),
                }
            }
        };
    }
    if history_expired {
        return DfsStableWriteOperationReconciliation::AmbiguousOrExpired {
            reason: format!(
                "write operation {operation_id} has no exact receipt and its durable history expired"
            ),
        };
    }
    if locally_authoritative {
        DfsStableWriteOperationReconciliation::NotApplied
    } else {
        DfsStableWriteOperationReconciliation::AmbiguousOrExpired {
            reason: format!(
                "write operation {operation_id} has no local exact receipt and this node is not the current metadata authority"
            ),
        }
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    #[allow(clippy::too_many_arguments)]
    fn reconcile_stable_write_operation_locally_for_requester(
        &mut self,
        object_id: &DfsObjectId,
        operation_id: &str,
        requester_node_id: &NodeId,
        expected_content_hash: &str,
        expected_content_size_bytes: u64,
        shard_id: Option<&DfsMetadataShardId>,
        locally_authoritative: bool,
    ) -> Result<DfsStableWriteOperationReconciliation, DfsError> {
        let file_system_id = sifs_file_system_id();
        let target_version =
            crate::intent::stable_write_target_version(requester_node_id, object_id, operation_id);
        let intent_id = stable_write_intent_id(requester_node_id, object_id, operation_id);
        let (record, recorded_content_size_bytes, history_expired) = if let (
            Some(backend),
            Some(shard_id),
        ) =
            (self.metadata_backend.as_ref(), shard_id)
        {
            let observed = backend.write_operation_admission_snapshot(
                shard_id,
                &file_system_id,
                object_id,
                &intent_id,
            )?;
            let record = observed.intent;
            if record.is_none() {
                for prepared in &observed.prepared {
                    let Some(original) = prepared_intent_for_write(prepared, object_id, &intent_id)
                    else {
                        continue;
                    };
                    prepared.validate()?;
                    let size = prepared
                        .payload
                        .transaction
                        .objects
                        .iter()
                        .find(|object| {
                            object.object_id == *object_id
                                && object.version == original.target_version
                        })
                        .and_then(|object| object.content_size_bytes);
                    let identity = classify_stable_write_operation(
                        operation_id,
                        &intent_id,
                        &file_system_id,
                        object_id,
                        &target_version,
                        requester_node_id,
                        expected_content_hash,
                        expected_content_size_bytes,
                        Some(original),
                        size,
                        false,
                        locally_authoritative,
                    );
                    if matches!(
                        identity,
                        DfsStableWriteOperationReconciliation::Conflict { .. }
                    ) {
                        return Ok(identity);
                    }
                    return Ok(DfsStableWriteOperationReconciliation::AmbiguousOrExpired {
                        reason: format!(
                            "write operation {operation_id} retains prepared operation {}; no absence or terminal outcome is established",
                            prepared.digest.operation_id
                        ),
                    });
                }
                if self.coordinator.intent(&intent_id).is_some() {
                    return Ok(DfsStableWriteOperationReconciliation::AmbiguousOrExpired {
                        reason: format!(
                            "write operation {operation_id} still has live execution without a durable terminal outcome"
                        ),
                    });
                }
            }
            let history_expired = record.is_none()
                && observed.retention.operation_history_expired(
                    object_id,
                    requester_node_id,
                    &intent_id,
                    stable_operation_order_token(operation_id).as_deref(),
                );
            (record, observed.target_size_bytes, history_expired)
        } else {
            (None, None, false)
        };
        Ok(classify_stable_write_operation(
            operation_id,
            &intent_id,
            &file_system_id,
            object_id,
            &target_version,
            requester_node_id,
            expected_content_hash,
            expected_content_size_bytes,
            record.as_ref(),
            recorded_content_size_bytes,
            history_expired,
            locally_authoritative,
        ))
    }

    fn wait_for_stable_write_reconciliation(
        &mut self,
        request_id: &str,
        timeout: std::time::Duration,
    ) -> Result<DfsStableWriteOperationReconciliation, DfsError> {
        let started = std::time::Instant::now();
        loop {
            self.process_incoming()?;
            if let Some(result) = self
                .writes
                .stable_write_reconciliation_results
                .remove(request_id)
            {
                self.writes
                    .stable_write_reconciliation_targets
                    .remove(request_id);
                return Ok(result);
            }
            if started.elapsed() >= timeout {
                self.writes
                    .stable_write_reconciliation_targets
                    .remove(request_id);
                return Ok(DfsStableWriteOperationReconciliation::AmbiguousOrExpired {
                    reason: format!(
                        "stable write reconciliation {request_id} timed out before the current authority replied"
                    ),
                });
            }
            self.check_cancelled("stable write reconciliation")?;
            let remaining = timeout.saturating_sub(started.elapsed());
            self.transport
                .wait_for_incoming(std::time::Duration::from_millis(10).min(remaining))
                .map_err(DfsError::from_network)?;
        }
    }

    /// Inspect durable write-operation evidence without sending or executing a
    /// write. Replica receipts can prove `Applied`; only the current metadata
    /// authority may turn a history-complete absence into `NotApplied`.
    pub fn reconcile_stable_write_operation(
        &mut self,
        path: &str,
        object_id: &DfsObjectId,
        operation_id: &str,
        expected_content_hash: &str,
        expected_content_size_bytes: u64,
    ) -> Result<DfsStableWriteOperationReconciliation, DfsError> {
        validate_write_operation_id(operation_id)?;
        let path = crate::posix_path::normalized_posix_path_key(path).ok_or_else(|| {
            DfsError::Serialization {
                message: format!("invalid POSIX path for write reconciliation: {path}"),
            }
        })?;
        let requester_node_id = self.node_id.clone();
        if self.control_plane.is_none() {
            let shard_id = self
                .metadata_backend
                .as_ref()
                .map(|_| self.local_prepared_operation_shard())
                .transpose()?;
            return self.reconcile_stable_write_operation_locally_for_requester(
                object_id,
                operation_id,
                &requester_node_id,
                expected_content_hash,
                expected_content_size_bytes,
                shard_id.as_ref(),
                self.metadata_backend.is_some(),
            );
        }
        let file_system_id = sifs_file_system_id();
        self.refresh_control_plane_latest()?;
        let shards = self
            .metadata_shard_config_snapshot(&file_system_id)?
            .unwrap_or_default();
        let authority_shard =
            crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                &file_system_id,
                &shards,
                &path,
            )?
            .ok_or_else(|| DfsError::Distributed {
                message: format!("stable write reconciliation for {path} has no authority shard"),
            })?;
        if authority_shard.primary_node_id == self.node_id
            && !self.transport.delegates_local_namespace_authority()
        {
            return self.reconcile_stable_write_operation_locally_for_requester(
                object_id,
                operation_id,
                &requester_node_id,
                expected_content_hash,
                expected_content_size_bytes,
                Some(&DfsMetadataShardId::new(authority_shard.shard_id.clone())),
                true,
            );
        }
        let request_id = self.next_request_id();
        let authority_node_id = authority_shard.primary_node_id.clone();
        self.writes
            .stable_write_reconciliation_targets
            .insert(request_id.clone(), authority_node_id.clone());
        let sent = self.send_dfs_message_if_routable(
            &authority_node_id,
            DfsNetworkMessage::StableWriteReconciliationRequest(
                DfsStableWriteReconciliationRequest {
                    request_id: request_id.clone(),
                    operation_id: operation_id.to_string(),
                    path,
                    object_id: object_id.clone(),
                    requesting_node_id: requester_node_id,
                    expected_content_hash: expected_content_hash.to_string(),
                    expected_content_size_bytes,
                    authority_shard,
                },
            ),
        )?;
        if !sent {
            self.writes
                .stable_write_reconciliation_targets
                .remove(&request_id);
            return Ok(DfsStableWriteOperationReconciliation::AmbiguousOrExpired {
                reason: format!(
                    "stable write reconciliation authority {authority_node_id} is unroutable"
                ),
            });
        }
        self.wait_for_stable_write_reconciliation(&request_id, std::time::Duration::from_secs(5))
    }

    /// Read-only stable-create reconciliation convenience. The object identity
    /// and target version are derived exactly as they were for the exclusive
    /// create, so the caller does not need a response that may have been lost.
    pub fn reconcile_stable_create_file_operation(
        &mut self,
        path: &str,
        operation_id: &str,
        expected_content_hash: &str,
        expected_content_size_bytes: u64,
    ) -> Result<DfsStableWriteOperationReconciliation, DfsError> {
        validate_write_operation_id(operation_id)?;
        let normalized = crate::posix_path::normalized_posix_path_key(path).ok_or_else(|| {
            DfsError::Serialization {
                message: format!("invalid POSIX path for create reconciliation: {path}"),
            }
        })?;
        let object_id =
            crate::intent::stable_create_object_id(&self.node_id, &normalized, operation_id);
        self.reconcile_stable_write_operation(
            &normalized,
            &object_id,
            operation_id,
            expected_content_hash,
            expected_content_size_bytes,
        )
    }

    pub(crate) fn handle_stable_write_reconciliation_request(
        &mut self,
        source_node_id: NodeId,
        request: DfsStableWriteReconciliationRequest,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let reconciliation = if source_node_id != request.requesting_node_id {
            DfsStableWriteOperationReconciliation::Conflict {
                reason: "stable write reconciliation source does not match requester identity"
                    .to_string(),
            }
        } else {
            self.refresh_control_plane_latest()?;
            let shards = self
                .metadata_shard_config_snapshot(&request.authority_shard.file_system_id)?
                .unwrap_or_default();
            let current = crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                &request.authority_shard.file_system_id,
                &shards,
                &request.path,
            )?;
            if current.as_ref() != Some(&request.authority_shard)
                || request.authority_shard.primary_node_id != self.node_id
            {
                DfsStableWriteOperationReconciliation::AmbiguousOrExpired {
                    reason: "stable write reconciliation authority moved after dispatch"
                        .to_string(),
                }
            } else {
                self.reconcile_stable_write_operation_locally_for_requester(
                    &request.object_id,
                    &request.operation_id,
                    &request.requesting_node_id,
                    &request.expected_content_hash,
                    request.expected_content_size_bytes,
                    Some(&DfsMetadataShardId::new(
                        request.authority_shard.shard_id.clone(),
                    )),
                    true,
                )?
            }
        };
        self.send_dfs_message_if_routable(
            &source_node_id,
            DfsNetworkMessage::StableWriteReconciliationResponse(
                DfsStableWriteReconciliationResponse {
                    request_id: request.request_id.clone(),
                    reconciliation,
                },
            ),
        )?;
        Ok(NetworkedDfsServiceEvent::StableReconciliationServed {
            request_id: request.request_id,
            requester_node_id: source_node_id,
            kind: "write".to_string(),
        })
    }

    pub(crate) fn handle_stable_write_reconciliation_response(
        &mut self,
        authority_node_id: NodeId,
        response: DfsStableWriteReconciliationResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let Some(expected_authority) = self
            .writes
            .stable_write_reconciliation_targets
            .get(&response.request_id)
        else {
            return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                request_id: response.request_id,
                source_node_id: authority_node_id,
                reason: "untracked stable write reconciliation response".to_string(),
            });
        };
        if expected_authority != &authority_node_id {
            return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                request_id: response.request_id,
                source_node_id: authority_node_id,
                reason: format!(
                    "stable write reconciliation response did not come from authority {expected_authority}"
                ),
            });
        }
        self.writes
            .stable_write_reconciliation_results
            .insert(response.request_id.clone(), response.reconciliation);
        Ok(NetworkedDfsServiceEvent::StableReconciliationCompleted {
            request_id: response.request_id,
            authority_node_id,
            kind: "write".to_string(),
        })
    }

    fn validate_routed_write_command_authority(
        &mut self,
        command: &DfsDistributedWriteCommand,
    ) -> Result<(), DfsError> {
        let expected = command
            .authority_shard
            .as_ref()
            .ok_or_else(|| DfsError::Distributed {
                message: "routed write requires its exact current authority stamp".into(),
            })?;
        self.rebind_completed_local_metadata_shard_membership_handoff_if_frozen(expected)?;
        let Some(object) = command.object_metadata.as_ref() else {
            return Err(DfsError::Distributed {
                message: format!(
                    "authority-scoped routed write {} has no object metadata",
                    command.request_id
                ),
            });
        };
        if object.object_id != command.request.object_id
            || object.file_system_id != command.request.file_system_id
            || expected.file_system_id != command.request.file_system_id
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "authority-scoped routed write metadata mismatch for object {}",
                    command.request.object_id
                ),
            });
        }
        let mut current = self.select_metadata_shard_for_object(object)?;
        if current
            .as_ref()
            .is_none_or(|current| current != expected || current.primary_node_id != self.node_id)
        {
            // A sender can have observed a newer committed assignment than
            // this long-lived lane. Refresh once before rejecting so an epoch
            // bump, a newly added shard, or a primary move does not turn a
            // valid stamped request into a terminal stale-view failure.
            self.refresh_control_plane_latest()?;
            current = self.select_metadata_shard_for_object(object)?;
        }
        let Some(current) = current else {
            return Err(DfsError::Distributed {
                message: format!(
                    "routed write for {} has no refreshed metadata shard assignment",
                    command.request.object_id
                ),
            });
        };
        if current.shard_id != expected.shard_id {
            // A stale path/hardlink rebase can cross shard boundaries. Do not
            // redirect while the daemon still owns the old lane; surface the
            // existing retry class so the caller re-resolves and re-stamps.
            return Err(DfsError::Busy {
                path: object.path.clone(),
            });
        }
        if current != *expected || current.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: current.file_system_id,
                local_node_id: self.node_id.clone(),
                primary_node_id: current.primary_node_id,
                epoch: current.epoch,
            });
        }
        Ok(())
    }

    pub(crate) fn replayed_routed_write_outcome_if_known(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsDistributedWriteCommand,
    ) -> Result<Option<NetworkedDfsWriteOutcome>, DfsError> {
        if !command.publication.is_current() {
            return self.replayed_retained_write_outcome(requester_node_id, command);
        }
        if requester_node_id != &command.request.requesting_node_id {
            return Ok(None);
        }
        validate_write_operation_id(&command.request_id)?;
        let (
            exclusive_create_is_still_visible,
            exclusive_create_pending_generation_is_visible,
            exclusive_create_payload_mismatch_path,
        ) = if let Some(expected_event_id) = routed_exclusive_create_event_id(command) {
            let object = command
                .object_metadata
                .as_ref()
                .expect("exclusive-create replay identity requires object metadata");
            let path_key = crate::posix_path::normalized_posix_path_key(&object.path)
                .unwrap_or_else(|| object.path.clone());
            // A committed exclusive create is replayable only while the exact
            // target generation remains visible at the path carried by the
            // request. While its durable intent is still collecting receipts,
            // the transient namespace object intentionally retains its base
            // version; its derived event id still proves that it belongs to
            // this exact request and may safely replay the Pending outcome.
            // Resolve that path through its authoritative shard instead of
            // looking up a brand-new object id with no shard index. The latter
            // falls back across every shard and makes cold creates pay for
            // unrelated journal refreshes before their normal path check.
            let visible = self.authoritative_metadata_object_for_path(
                &command.request.file_system_id,
                &path_key,
            )?;
            let same_object = visible.as_ref().is_some_and(|visible| {
                visible.object_id == command.request.object_id
                    && visible.kind != DfsObjectKind::Tombstone
            });
            let same_request = same_object
                && visible.as_ref().is_some_and(|visible| {
                    visible.last_modified_by_event_id.as_ref() == Some(&expected_event_id)
                });
            let same_generation = same_object
                && visible
                    .as_ref()
                    .is_some_and(|visible| visible.version == command.request.target_version);
            let exact = same_generation && same_request;
            let mismatch_path = (same_generation && !exact)
                .then(|| visible.expect("same generation has a visible object").path);
            (Some(exact), Some(same_request), mismatch_path)
        } else {
            (None, None, None)
        };
        let expected_content_hash = routed_write_command_content_hash(command);
        let Some(expected_content_size_bytes) = routed_write_command_content_size_bytes(command)
        else {
            return Ok(None);
        };
        let stable_intent_id = stable_write_intent_id(
            requester_node_id,
            &command.request.object_id,
            &command.request_id,
        );
        if let Some(intent) = self.coordinator.intent(&stable_intent_id).cloned() {
            if !stable_distributed_intent_matches_routed_write_command(
                &intent,
                requester_node_id,
                command,
            ) || !self.routed_write_intent_matches_content_identity(
                &intent,
                &expected_content_hash,
                expected_content_size_bytes,
            )? {
                return Err(DfsError::WriteOperationIdConflict {
                    operation_id: command.request_id.clone(),
                    object_id: command.request.object_id.clone(),
                });
            }
            if let DfsWriteIntentState::PreconditionFailed { failure } = &intent.state {
                return Err(DfsError::WritePreconditionFailed { failure: failure.clone() });
            }
            if let Some(path) = exclusive_create_payload_mismatch_path.clone() {
                return Err(DfsError::AlreadyExists { path });
            }
            if exclusive_create_is_still_visible == Some(false)
                && (matches!(intent.state, DfsWriteIntentState::Committed)
                    || exclusive_create_pending_generation_is_visible != Some(true))
            {
                return Err(DfsError::WriteReconciliationRequired {
                    operation_id: command.request_id.clone(),
                    object_id: command.request.object_id.clone(),
                    reason: "the exclusive-create result is no longer the visible path generation"
                        .to_string(),
                });
            }
            return self.replayed_routed_write_outcome_for_intent(
                &intent,
                &expected_content_hash,
                expected_content_size_bytes,
            );
        }

        let mut stable_record = if let Some(backend) = self.metadata_backend.as_ref() {
            backend.get_intent(&command.request.object_id, &stable_intent_id)?
        } else {
            None
        };
        if stable_record.is_none() {
            stable_record = self.recover_retained_routed_write_opening(
                requester_node_id,
                command,
                &stable_intent_id,
                &expected_content_hash,
                expected_content_size_bytes,
            )?;
        }
        if let Some(record) = stable_record {
            let size_matches = match self.metadata_backend.as_ref() {
                Some(backend) => backend
                    .get_version(&record.object_id, &record.target_version)?
                    .is_none_or(|version| {
                        version
                            .size_bytes
                            .is_none_or(|size| size == expected_content_size_bytes)
                    }),
                None => true,
            };
            if !stable_write_intent_record_matches_routed_write_command(
                &record,
                requester_node_id,
                command,
                &expected_content_hash,
            ) || !size_matches
            {
                return Err(DfsError::WriteOperationIdConflict {
                    operation_id: command.request_id.clone(),
                    object_id: command.request.object_id.clone(),
                });
            }
            if let DfsWriteIntentStatus::PreconditionFailed { failure } = &record.status {
                return Err(DfsError::WritePreconditionFailed { failure: failure.clone() });
            }
            if let Some(path) = exclusive_create_payload_mismatch_path {
                return Err(DfsError::AlreadyExists { path });
            }
            if exclusive_create_is_still_visible == Some(false)
                && (matches!(record.status, DfsWriteIntentStatus::Committed)
                    || exclusive_create_pending_generation_is_visible != Some(true))
            {
                return Err(DfsError::WriteReconciliationRequired {
                    operation_id: command.request_id.clone(),
                    object_id: command.request.object_id.clone(),
                    reason: "the exclusive-create result is no longer the visible path generation"
                        .to_string(),
                });
            }
            self.restore_pending_write_for_intent_if_missing(&record.intent_id, &record.object_id)?;
            self.restore_backend_receipt_pages_for_intent(&record)?;
            if matches!(record.status, DfsWriteIntentStatus::Committed) {
                self.restore_recorded_content_receipts(&record)?;
            }
            let Some(intent) = self.coordinator.intent(&record.intent_id).cloned() else {
                return Ok(None);
            };
            return self.replayed_routed_write_outcome_for_intent(
                &intent,
                &expected_content_hash,
                expected_content_size_bytes,
            );
        }
        if let Some(backend) = self.metadata_backend.as_ref()
            && backend
                .intent_receipt_retention_for_object(&command.request.object_id)?
                .operation_history_expired(
                    &command.request.object_id,
                    requester_node_id,
                    &stable_intent_id,
                    stable_operation_order_token(&command.request_id).as_deref(),
                )
        {
            return Err(DfsError::WriteReconciliationRequired {
                operation_id: command.request_id.clone(),
                object_id: command.request.object_id.clone(),
                reason: "durable write-operation history expired or is unknown".to_string(),
            });
        }

        Ok(None)
    }

    /// After restart an initial prepare can be durable while neither the
    /// applied-intent row nor the resident coordinator exists. Its absence
    /// from those projections must not admit a newly timestamped opening.
    fn recover_retained_routed_write_opening(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsDistributedWriteCommand,
        intent_id: &str,
        content_hash: &str,
        content_size_bytes: u64,
    ) -> Result<Option<DfsWriteIntentRecord>, DfsError> {
        if self.metadata_backend.is_none() || self.transport.delegates_local_namespace_authority() {
            return Ok(None);
        }
        let object = match self.current_object_for_write(&command.request.object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. }) => match command.object_metadata.as_ref() {
                Some(object)
                    if object.object_id == command.request.object_id
                        && object.file_system_id == command.request.file_system_id =>
                {
                    object.clone()
                }
                _ => return Ok(None),
            },
            Err(error) => return Err(error),
        };
        let scope = match self.metadata_effect_scope_for_object(&object, command.request_id.clone())
        {
            Ok(scope) => scope,
            // Requester-side replay probing does not acquire a remote owner's
            // recovery authority. The ordinary routed write still owns that hop.
            Err(DfsError::StaleOwnerEpoch { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        let observed = self
            .metadata_backend
            .as_ref()
            .expect("backend checked above")
            .write_operation_admission_snapshot(
                &scope.shard_id,
                &object.file_system_id,
                &object.object_id,
                intent_id,
            )?;
        if observed.intent.is_some() {
            return Ok(observed.intent);
        }
        let mut opening = None;
        for prepared in observed.prepared {
            let Some(original) = prepared_intent_for_write(&prepared, &object.object_id, intent_id)
            else {
                continue;
            };
            if !stable_write_intent_record_matches_routed_write_command(
                original,
                requester_node_id,
                command,
                content_hash,
            ) {
                return Err(DfsError::WriteOperationIdConflict {
                    operation_id: command.request_id.clone(),
                    object_id: object.object_id.clone(),
                });
            }
            let intent = crate::intent::distributed_intent_from_record(original);
            validate_retained_write_opening(&prepared, original, &intent, content_hash)?;
            if self.pending_write_size_hint(
                &object.object_id,
                &original.target_version,
                content_hash,
            )? != Some(content_size_bytes)
            {
                return Err(DfsError::WriteReconciliationRequired {
                    operation_id: command.request_id.clone(),
                    object_id: object.object_id.clone(),
                    reason: "retained opening has no exact transfer size/source for this retry"
                        .into(),
                });
            }
            if opening.replace(prepared).is_some() {
                return Err(DfsError::WriteReconciliationRequired {
                    operation_id: command.request_id.clone(),
                    object_id: object.object_id.clone(),
                    reason: "write opening has competing prepared effects".into(),
                });
            }
        }
        let Some(prepared) = opening else {
            return Ok(None);
        };
        // The native recovery path alone certifies current authority and all
        // original participants, and publishes the captured immutable body.
        if !self.recover_one_prepared_operation(prepared)? {
            return Err(DfsError::WriteReconciliationRequired {
                operation_id: command.request_id.clone(),
                object_id: object.object_id.clone(),
                reason: "retained opening still requires its native recovery owner".into(),
            });
        }
        let record = self
            .metadata_backend
            .as_ref()
            .expect("backend remains installed")
            .get_intent_on_shard(&scope.shard_id, &object.object_id, intent_id)?;
        record
            .map(Some)
            .ok_or_else(|| DfsError::WriteReconciliationRequired {
                operation_id: command.request_id.clone(),
                object_id: object.object_id,
                reason: "recovered opening has no durable applied intent".into(),
            })
    }

    /// Proves that an in-memory intent belongs to this exact write payload
    /// before the response-safe transport path treats the command as a replay.
    /// The coordinator intentionally does not own content bytes, so consult the
    /// pending transfer first and then its durable intent/current version.
    fn routed_write_intent_matches_content_identity(
        &self,
        intent: &DistributedWriteIntent,
        expected_content_hash: &str,
        expected_size_bytes: u64,
    ) -> Result<bool, DfsError> {
        if let Some(pending) = self.writes.pending_writes.get(&intent.intent_id) {
            return Ok(pending.content_hash == expected_content_hash
                && pending.size_bytes == expected_size_bytes);
        }

        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(record) = backend.get_intent(&intent.object_id, &intent.intent_id)?
        {
            if record.content_hash.as_deref() != Some(expected_content_hash) {
                return Ok(false);
            }
            if let Some(version) = backend.get_version(&intent.object_id, &intent.target_version)?
                && version
                    .size_bytes
                    .is_some_and(|size| size != expected_size_bytes)
            {
                return Ok(false);
            }
            return Ok(true);
        }

        Ok(matches!(intent.state, DfsWriteIntentState::Committed)
            && self
                .current_read_object_metadata(&intent.object_id)
                .is_ok_and(|object| {
                    object.version == intent.target_version
                        && object.content_hash.as_deref() == Some(expected_content_hash)
                        && object
                            .content_size_bytes
                            .is_none_or(|size| size == expected_size_bytes)
                }))
    }

    fn replayed_routed_write_outcome_for_intent(
        &mut self,
        intent: &DistributedWriteIntent,
        content_hash: &str,
        content_size_bytes: u64,
    ) -> Result<Option<NetworkedDfsWriteOutcome>, DfsError> {
        self.refresh_committed_write_projection(&intent.intent_id)?;
        self.recover_prepared_write_for_retry(intent, content_hash, content_size_bytes)?;
        // Opening publication and its first transfer are a separate checkpoint
        // from the final write body. Resume them only on this explicit replay;
        // receipt-driven commit readiness must not recursively re-enter it.
        self.recover_prepared_intent_opening_for_retry(intent, content_hash, content_size_bytes)?;
        if self.metadata_backend.is_some() && !self.transport.delegates_local_namespace_authority()
        {
            // The first native prepare can be rejected by another reservation
            // after the resident owner has retained its input and real receipt.
            // There is then no durable body for the recovery helpers above to
            // resume. An explicit exact replay must also revisit ordinary
            // commit readiness; returning the resident Pending outcome alone
            // cannot make progress once that other reservation has settled.
            // This keeps the same intent/transfer and all native author,
            // namespace, content and participant checks. Proxy callers never
            // acquire author authority through this path.
            self.try_apply_committed_intent_if_ready(&intent.intent_id)?;
        }
        match self.write_outcome(&intent.intent_id) {
            Ok(outcome) => Ok(Some(outcome)),
            Err(error) if matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) => {
                let _ = error;
                Ok(Some(NetworkedDfsWriteOutcome::Pending {
                    intent_id: intent.intent_id.clone(),
                    durable_receipts: self.coordinator.durable_receipt_count(&intent.intent_id),
                    required_receipts: intent.policy.min_commit_replicas,
                }))
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn handle_distributed_write_request(
        &mut self,
        requester_node_id: NodeId,
        command: DfsDistributedWriteCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        self.handle_distributed_write_request_with_preingested_blob(
            requester_node_id,
            command,
            None,
            false,
        )
    }

    /// Bind a continuation to the real immutable prepare and original request.
    /// This grants no authority; recovery rechecks the current publication fence.
    pub(crate) fn prepared_routed_write_continuation(
        &self,
        requester: &NodeId,
        command: &DfsDistributedWriteCommand,
        operation_id: &str,
    ) -> Result<Option<crate::DfsPreparedOperationDigest>, DfsError> {
        if command.patch.is_some() || !command.publication.is_current() {
            return Ok(None);
        }
        let Some(record) = self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.prepared_operation_record(operation_id))
            .transpose()?
            .flatten()
        else {
            return Ok(None);
        };
        record.validate()?;
        let intent_id =
            stable_write_intent_id(requester, &command.request.object_id, &command.request_id);
        let Some(intent) =
            prepared_intent_for_write(&record, &command.request.object_id, &intent_id)
        else {
            return Ok(None);
        };
        let Some(shard) = command.authority_shard.as_ref() else {
            return Ok(None);
        };
        let hash = routed_write_command_content_hash(command);
        if requester != &command.request.requesting_node_id
            || record.digest.file_system_id != command.request.file_system_id
            || record.digest.shard_id.0 != shard.shard_id
            || record.digest.owner_epoch != shard.epoch
            || shard.primary_node_id != self.node_id
            || intent.operation_id.as_deref() != Some(command.request_id.as_str())
            || intent.policy != command.policy
            || intent
                .desired_replica_node_ids
                .iter()
                .collect::<BTreeSet<_>>()
                != command
                    .desired_replica_nodes
                    .iter()
                    .collect::<BTreeSet<_>>()
            || intent.touch_parent_on_create != command.touch_parent_on_create
            || command
                .expected_base_version
                .as_ref()
                .is_some_and(|base| intent.base_version.as_ref() != Some(base))
            || !stable_write_intent_record_matches_routed_write_command(
                intent, requester, command, &hash,
            )
        {
            return Ok(None);
        }
        Ok(Some(record.digest))
    }

    pub(crate) fn resume_prepared_routed_write(
        &mut self,
        requester: NodeId,
        command: DfsDistributedWriteCommand,
        prepared: &crate::DfsPreparedOperationDigest,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        if self
            .prepared_routed_write_continuation(&requester, &command, &prepared.operation_id)?
            .as_ref()
            != Some(prepared)
        {
            return Err(DfsError::WriteReconciliationRequired {
                operation_id: command.request_id,
                object_id: command.request.object_id,
                reason: "write continuation lost its original immutable prepare".into(),
            });
        }
        // The ordinary replay path checks content, base predicates and authority.
        // Its absence can never fall through to rebase or a fresh write.
        self.handle_distributed_write_request_with_preingested_blob(requester, command, None, true)
    }

    fn prepare_routed_patch_command(
        &mut self,
        command: &mut DfsDistributedWriteCommand,
        preingested_blob: &mut Option<ContentStoreQuarantinedBlob>,
    ) -> Result<Option<crate::content_store::ContentManifest>, DfsError> {
        let Some(patch) = command.patch.clone() else {
            return Ok(None);
        };
        let conflict = || DfsError::WriteOperationIdConflict {
            operation_id: command.request_id.clone(),
            object_id: command.request.object_id.clone(),
        };
        // Patch producers currently own the default-current contract only;
        // they must reject, rather than discard, an incoming namespace guard.
        if !command.publication.is_default_current()
            || command.exclusive_create
            || command.touch_parent_on_create
            || command.requester_blob_pre_staged
            || patch.patch_size_bytes == 0
            || patch.offset > patch.base_size_bytes
            || patch.offset.checked_add(patch.patch_size_bytes).is_none()
        {
            return Err(conflict());
        }
        let base = command.expected_base_version.clone().ok_or_else(conflict)?;
        let (input_hash, input_size) = if let Some(blob) = preingested_blob.as_ref() {
            (blob.content_hash.clone(), blob.size_bytes)
        } else {
            (
                crate::content_store::content_hash(&command.bytes),
                command.bytes.len() as u64,
            )
        };
        if input_hash != patch.patch_hash
            || input_size != patch.patch_size_bytes
            || command
                .content_hash
                .as_ref()
                .is_some_and(|hash| hash != &input_hash)
            || command
                .content_size_bytes
                .is_some_and(|size| size != input_size)
        {
            return Err(conflict());
        }
        let intent_id = stable_write_intent_id(
            &command.request.requesting_node_id,
            &command.request.object_id,
            &command.request_id,
        );
        let record = self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_intent(&command.request.object_id, &intent_id))
            .transpose()?
            .flatten();
        if let Some(record) = record.as_ref() {
            if record.file_system_id != command.request.file_system_id
                || record.object_id != command.request.object_id
                || record.target_version != command.request.target_version
                || record.base_version.as_ref() != Some(&base)
                || record.coordinator_node_id != command.request.requesting_node_id
                || record.operation_id.as_deref() != Some(command.request_id.as_str())
            {
                return Err(conflict());
            }
            let observed_base = self
                .metadata_backend
                .as_ref()
                .map(|backend| backend.get_version(&command.request.object_id, &base))
                .transpose()?
                .flatten()
                .ok_or_else(|| DfsError::WriteReconciliationRequired {
                    operation_id: command.request_id.clone(),
                    object_id: command.request.object_id.clone(),
                    reason: "routed patch base identity is no longer retained".into(),
                })?;
            if observed_base.file_system_id != command.request.file_system_id
                || observed_base.object_id != command.request.object_id
                || observed_base.version != base
                || observed_base.content_hash.as_ref() != Some(&patch.base_content_hash)
                || observed_base.size_bytes != Some(patch.base_size_bytes)
            {
                return Err(conflict());
            }
        } else {
            let current = self.current_object_for_write(&command.request.object_id)?;
            if current.version != base {
                return Err(crate::service_write::stale_write_base_error(
                    &current,
                    &base,
                    &command.request.target_version,
                ));
            }
            if current.file_system_id != command.request.file_system_id
                || !matches!(current.kind, DfsObjectKind::File | DfsObjectKind::Symlink)
                || current.content_hash.as_ref() != Some(&patch.base_content_hash)
                || current.content_size_bytes != Some(patch.base_size_bytes)
            {
                return Err(conflict());
            }
        }
        let base_identity = crate::content_store::ContentVersionIdentity {
            content_hash: patch.base_content_hash.clone(),
            size_bytes: patch.base_size_bytes,
        };
        let replay = self.content.replay_object_version_patch_manifest(
            command.request.object_id.as_str(),
            base.as_str(),
            command.request.target_version.as_str(),
            patch.offset,
            &patch.patch_hash,
            patch.patch_size_bytes,
            &base_identity,
        )?;
        let Some(manifest) = replay else {
            if record.is_some() {
                return Err(DfsError::WriteReconciliationRequired {
                    operation_id: command.request_id.clone(),
                    object_id: command.request.object_id.clone(),
                    reason: "routed patch has an actual intent but no retained exact extent layout"
                        .into(),
                });
            }
            return Ok(None);
        };
        // Only actual immutable replay DATA is normalized here. New content
        // binding waits for the existing final authority/object admission.
        preingested_blob.take();
        command.bytes.clear();
        command.content_hash = Some(manifest.content_hash.clone());
        command.content_size_bytes = Some(manifest.size_bytes);
        Ok(Some(manifest))
    }

    fn bind_admitted_routed_patch(
        &mut self,
        command: &DfsDistributedWriteCommand,
        preingested_blob: &mut Option<ContentStoreQuarantinedBlob>,
    ) -> Result<crate::content_store::ContentManifest, DfsError> {
        let patch = command
            .patch
            .as_ref()
            .ok_or_else(|| DfsError::Distributed {
                message: "admitted ranged write lost its patch descriptor".into(),
            })?;
        let base = command
            .expected_base_version
            .as_ref()
            .ok_or_else(|| DfsError::Distributed {
                message: "admitted ranged write lost its exact base version".into(),
            })?;
        let current = self.current_object_for_write(&command.request.object_id)?;
        if current.version != *base {
            return Err(crate::service_write::stale_write_base_error(
                &current,
                base,
                &command.request.target_version,
            ));
        }
        if current.file_system_id != command.request.file_system_id
            || current.content_hash.as_ref() != Some(&patch.base_content_hash)
            || current.content_size_bytes != Some(patch.base_size_bytes)
        {
            return Err(DfsError::WriteOperationIdConflict {
                operation_id: command.request_id.clone(),
                object_id: command.request.object_id.clone(),
            });
        }
        let identity = crate::content_store::ContentVersionIdentity {
            content_hash: patch.base_content_hash.clone(),
            size_bytes: patch.base_size_bytes,
        };
        Ok(if let Some(blob) = preingested_blob.take() {
            self.content
                .bind_quarantined_patch(
                    command.request.object_id.as_str(),
                    base.as_str(),
                    command.request.target_version.as_str(),
                    patch.offset,
                    blob,
                    &identity,
                )?
                .manifest
        } else {
            self.content
                .put_object_version_patch_with_info_and_base_identity(
                    command.request.object_id.as_str(),
                    base.as_str(),
                    command.request.target_version.as_str(),
                    patch.offset,
                    &command.bytes,
                    Some(&identity),
                )?
                .manifest
        })
    }

    pub(crate) fn handle_preingested_distributed_write_request(
        &mut self,
        requester_node_id: NodeId,
        command: DfsDistributedWriteCommand,
        blob: ContentStoreQuarantinedBlob,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        self.handle_distributed_write_request_with_preingested_blob(
            requester_node_id,
            command,
            Some(blob),
            false,
        )
    }

    fn handle_distributed_write_request_with_preingested_blob(
        &mut self,
        requester_node_id: NodeId,
        mut command: DfsDistributedWriteCommand,
        mut preingested_blob: Option<ContentStoreQuarantinedBlob>,
        replay_only: bool,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let request_id = command.request_id.clone();
        let object_id = command.request.object_id.clone();
        let file_system_id = command.request.file_system_id.clone();
        let target_version = command.request.target_version.clone();
        // A requester's durable claim belongs to the tuple it actually staged.
        // Authority-side path/hardlink rebasing below may choose another tuple;
        // matching blob bytes alone do not bind that canonical version.
        let requester_staged_binding = command.requester_blob_pre_staged.then(|| {
            (
                command.request.file_system_id.clone(),
                command.request.object_id.clone(),
                command.request.target_version.clone(),
                command.content_hash.clone(),
                command.content_size_bytes,
            )
        });
        let foreground_replica_repair = command.foreground_replica_repair;
        let metadata_journal_after_sequence = command.metadata_journal_after_sequence;
        let routed_object_path = command
            .object_metadata
            .as_ref()
            .map(|object| object.path.clone());
        let touch_parent_on_create = command.touch_parent_on_create;
        let trace_started = Instant::now();
        let trace_payload_bytes = command.bytes.len();
        let trace_manifest_only = command.content_hash.is_some();
        let mut created_object_path = None;
        let mut injected_transient_metadata = false;
        let mut patch_manifest = None;
        let apply_started = Instant::now();
        let result = if requester_node_id != command.request.requesting_node_id {
            DfsDistributedWriteResult::Rejected {
                reason: format!(
                    "distributed write requester mismatch: frame source {}, payload requester {}",
                    requester_node_id, command.request.requesting_node_id
                ),
                current_primary_node_id: None,
            }
        } else if let Err(error) = self.validate_routed_write_command_authority(&command) {
            let current_primary_node_id = distributed_write_rejection_current_primary(&error);
            DfsDistributedWriteResult::Rejected {
                reason: error.to_string(),
                current_primary_node_id,
            }
        } else if let Err(error) = self
            .prepare_routed_patch_command(&mut command, &mut preingested_blob)
            .map(|manifest| patch_manifest = manifest)
        {
            DfsDistributedWriteResult::Rejected {
                reason: error.to_string(),
                current_primary_node_id: None,
            }
        } else if let Some(replayed_result) =
            match self.replayed_routed_write_outcome_if_known(&requester_node_id, &command) {
                Ok(Some(outcome)) => Some(DfsDistributedWriteResult::Outcome { outcome }),
                Ok(None) => None,
                Err(
                    error @ (DfsError::WriteOperationIdConflict { .. }
                    | DfsError::AlreadyExists { .. }),
                ) => Some(DfsDistributedWriteResult::Rejected {
                    reason: error.to_string(),
                    current_primary_node_id: None,
                }),
                Err(DfsError::WritePreconditionFailed { failure }) => {
                    Some(DfsDistributedWriteResult::PreconditionFailed { failure })
                }
                Err(DfsError::WriteReconciliationRequired {
                    operation_id,
                    reason,
                    ..
                }) => Some(DfsDistributedWriteResult::ReconciliationRequired {
                    operation_id,
                    reason,
                }),
                Err(DfsError::MetadataCommitOutcomeUnknown {
                    file_system_id,
                    shard_id,
                    epoch,
                    operation_id,
                    cause,
                    current_primary_node_id,
                }) => Some(DfsDistributedWriteResult::CommitOutcomeUnknown {
                    intent_id: stable_write_intent_id(&requester_node_id, &object_id, &request_id),
                    file_system_id,
                    shard_id,
                    epoch,
                    operation_id,
                    cause,
                    current_primary_node_id,
                }),
                Err(error) => return Err(error),
            }
        {
            replayed_result
        } else if replay_only {
            DfsDistributedWriteResult::ReconciliationRequired {
                operation_id: request_id.clone(),
                reason: "retained write continuation no longer has an exact replayable outcome"
                    .into(),
            }
        } else {
            let authority_validation =
                if command.publication.is_default_current() && command.patch.is_none() {
                    self.rebase_routed_path_write_if_current(&mut command)
                        .and_then(|()| self.validate_routed_write_command_authority(&command))
                } else {
                    self.validate_routed_write_command_authority(&command)
                };
            let requester_binding_is_exact = requester_staged_binding.as_ref().is_some_and(
                |(file_system_id, object_id, version, hash, size)| {
                    file_system_id == &command.request.file_system_id
                        && object_id == &command.request.object_id
                        && version == &command.request.target_version
                        && hash == &command.content_hash
                        && size == &command.content_size_bytes
                },
            );
            let write_foreground_replica_repair = foreground_replica_repair
                && !self
                    .transport
                    .can_return_committed_foreground_response_before_replica_repair();
            let manifest = if command.patch.is_some() {
                Ok(None)
            } else {
                match command.content_hash.clone() {
                    Some(content_hash) => {
                        let content_size_bytes =
                            command
                                .content_size_bytes
                                .ok_or_else(|| DfsError::Distributed {
                                    message: format!(
                                        "routed reader-backed write for {} missing content size",
                                        command.request.object_id
                                    ),
                                });
                        if !command.bytes.is_empty() {
                            Err(DfsError::Distributed {
                                message: format!(
                                    "routed reader-backed write for {} must not include inline bytes",
                                    command.request.object_id
                                ),
                            })
                        } else if command.requester_blob_pre_staged && preingested_blob.is_some() {
                            Err(DfsError::Distributed {
                                message: format!(
                                    "routed streamed write for {} cannot claim requester-pre-staged content",
                                    command.request.object_id
                                ),
                            })
                        } else {
                            content_size_bytes.and_then(|size| {
                            if let Some(blob) = preingested_blob.as_ref() {
                                if blob.content_hash != content_hash {
                                    return Err(DfsError::ContentHashMismatch {
                                        expected: content_hash,
                                        actual: blob.content_hash.clone(),
                                    });
                                }
                                if blob.size_bytes != size {
                                    return Err(DfsError::Distributed {
                                        message: format!(
                                            "routed streamed write for {} declared {size} bytes but ingested {}",
                                            command.request.object_id, blob.size_bytes
                                        ),
                                    });
                                }
                            }
                            Ok(Some((content_hash, size)))
                        })
                        }
                    }
                    None if preingested_blob.is_some() => Err(DfsError::Distributed {
                        message: format!(
                            "routed streamed write for {} is missing its finalized content identity",
                            command.request.object_id
                        ),
                    }),
                    None => Ok(None),
                }
            };
            match authority_validation.and(manifest).and_then(|manifest| {
                if let Some(object) = command.object_metadata.as_ref() {
                    self.check_current_write_precondition(&command.publication,
                        &command.request.file_system_id, &command.request.object_id,
                        &object.path, command.expected_base_version.as_ref())?;
                } else if command.publication.current_precondition().is_some() {
                    return Err(DfsError::Metadata { message: "conditional request has no target object".into() });
                }
                let exclusive_create_event_id = routed_exclusive_create_event_id(&command);
                let admitted = if command.publication.is_current() {
                    self.ensure_routed_write_object_metadata(
                        &command.request,
                        command.object_metadata.as_ref(),
                        command.exclusive_create,
                        exclusive_create_event_id,
                    )
                } else {
                    self.ensure_retained_write_object(&command).map(|()| false)
                };
                admitted.and_then(|created| {
                    // The ordinary replay check ran before this actual native
                    // namespace result; no incoming wire bit grants freshness.
                    let creation = DfsWriteCreation::from_namespace_resolution(
                        command.exclusive_create,
                        created
                            && command.publication.is_current()
                            && command.patch.is_none()
                            && command.expected_base_version.is_none()
                            && !foreground_replica_repair,
                    );
                    injected_transient_metadata = created;
                    if created && touch_parent_on_create {
                        created_object_path = routed_object_path;
                    }
                    if let Some(patch) = command.patch.as_ref() {
                        let actual_manifest = match patch_manifest.take() {
                            Some(manifest) => manifest,
                            None => {
                                self.bind_admitted_routed_patch(&command, &mut preingested_blob)?
                            }
                        };
                        let base = command.expected_base_version.clone().ok_or_else(|| {
                            DfsError::Distributed {
                                message: "routed patch lost its exact base version".into(),
                            }
                        })?;
                        self.write_stable_ingested_distributed_patch_manifest(
                            command.request,
                            base,
                            patch,
                            actual_manifest,
                            command.desired_replica_nodes,
                            command.policy,
                            request_id.clone(),
                            write_foreground_replica_repair,
                        )
                    } else if let Some((content_hash, content_size_bytes)) = manifest {
                        if command.requester_blob_pre_staged {
                            self.write_stable_remote_distributed_manifest(
                                command.request,
                                command.expected_base_version,
                                content_hash,
                                content_size_bytes,
                                requester_binding_is_exact,
                                command.desired_replica_nodes,
                                command.policy,
                                request_id.clone(),
                                DfsDistributedWriteOptions {
                                    publication: command.publication.clone(),
                                    touch_parent_on_create: created_object_path.is_some(),
                                    creation,
                                    repair: if write_foreground_replica_repair {
                                        DfsWriteRepair::Foreground
                                    } else {
                                        DfsWriteRepair::Background
                                    },
                                    ..DfsDistributedWriteOptions::default()
                                },
                            )
                        } else if let Some(blob) = preingested_blob.take() {
                            self.write_stable_quarantined_distributed_blob(
                                command.request,
                                command.expected_base_version,
                                blob,
                                command.desired_replica_nodes,
                                command.policy,
                                request_id.clone(),
                                DfsDistributedWriteOptions {
                                    publication: command.publication.clone(),
                                    touch_parent_on_create: created_object_path.is_some(),
                                    creation,
                                    repair: if write_foreground_replica_repair {
                                        DfsWriteRepair::Foreground
                                    } else {
                                        DfsWriteRepair::Background
                                    },
                                    ..DfsDistributedWriteOptions::default()
                                },
                            )
                        } else {
                            self.write_stable_ingested_distributed_manifest(
                                command.request,
                                command.expected_base_version,
                                content_hash,
                                content_size_bytes,
                                command.desired_replica_nodes,
                                command.policy,
                                request_id.clone(),
                                DfsDistributedWriteOptions {
                                    publication: command.publication.clone(),
                                    touch_parent_on_create: created_object_path.is_some(),
                                    creation,
                                    repair: if write_foreground_replica_repair {
                                        DfsWriteRepair::Foreground
                                    } else {
                                        DfsWriteRepair::Background
                                    },
                                    ..DfsDistributedWriteOptions::default()
                                },
                            )
                        }
                    } else if !command.requester_blob_pre_staged {
                        // Compatibility transports may materialize the owned
                        // attachment back into the command. It is still a
                        // primary-owned ingest: persist it once here and never
                        // credit the requester with a replica it did not store.
                        let manifest = self.content.put_object_version(
                            command.request.object_id.as_str(),
                            command.request.target_version.as_str(),
                            &command.bytes,
                        )?;
                        self.write_stable_ingested_distributed_manifest(
                            command.request,
                            command.expected_base_version,
                            manifest.content_hash,
                            manifest.size_bytes,
                            command.desired_replica_nodes,
                            command.policy,
                            request_id.clone(),
                            DfsDistributedWriteOptions {
                                publication: command.publication.clone(),
                                touch_parent_on_create: created_object_path.is_some(),
                                creation,
                                repair: if write_foreground_replica_repair {
                                    DfsWriteRepair::Foreground
                                } else {
                                    DfsWriteRepair::Background
                                },
                                ..DfsDistributedWriteOptions::default()
                            },
                        )
                    } else {
                        self.write_distributed_bytes_with_base(
                            command.request,
                            command.expected_base_version,
                            command.bytes,
                            command.desired_replica_nodes,
                            command.policy,
                            DfsDistributedWriteOptions {
                                publication: command.publication.clone(),
                                touch_parent_on_create: created_object_path.is_some(),
                                creation,
                                repair: if write_foreground_replica_repair {
                                    DfsWriteRepair::Foreground
                                } else {
                                    DfsWriteRepair::Background
                                },
                                dispatch: DfsWriteDispatch::AwaitResponse,
                                replay_response_journal: true,
                                operation_id: Some(request_id.clone()),
                            },
                        )
                    }
                })
            }) {
                Ok(outcome) => {
                    let response_outcome = match &outcome {
                        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => {
                            self.track_pending_remote_write_response(
                                intent_id,
                                &requester_node_id,
                                &request_id,
                                &object_id,
                                metadata_journal_after_sequence,
                                created_object_path.clone(),
                                foreground_replica_repair,
                            );
                            outcome.clone()
                        }
                        NetworkedDfsWriteOutcome::Committed {
                            intent_id,
                            repair_jobs,
                            ..
                        } if foreground_replica_repair
                            && !repair_jobs.is_empty()
                            && !self
                                .transport
                                .can_return_committed_foreground_response_before_replica_repair(
                                ) =>
                        {
                            self.track_pending_remote_write_response(
                                intent_id,
                                &requester_node_id,
                                &request_id,
                                &object_id,
                                metadata_journal_after_sequence,
                                None,
                                foreground_replica_repair,
                            );
                            NetworkedDfsWriteOutcome::Pending {
                                intent_id: intent_id.clone(),
                                durable_receipts: self.coordinator.durable_receipt_count(intent_id),
                                required_receipts: self
                                    .coordinator
                                    .intent(intent_id)
                                    .map(|intent| intent.policy.min_commit_replicas)
                                    .unwrap_or_else(|| {
                                        self.coordinator.durable_receipt_count(intent_id)
                                    }),
                            }
                        }
                        NetworkedDfsWriteOutcome::Committed { .. } => outcome.clone(),
                    };
                    DfsDistributedWriteResult::Outcome {
                        outcome: response_outcome,
                    }
                }
                Err(DfsError::MetadataCommitOutcomeUnknown {
                    file_system_id: unknown_file_system_id,
                    shard_id,
                    epoch,
                    operation_id,
                    cause,
                    current_primary_node_id,
                }) => {
                    // The body is durable and can still commit; a cleanup or
                    // abort here would contradict it. The requester retries
                    // the same request id.
                    DfsDistributedWriteResult::CommitOutcomeUnknown {
                        intent_id: stable_write_intent_id(
                            &requester_node_id,
                            &object_id,
                            &request_id,
                        ),
                        file_system_id: unknown_file_system_id,
                        shard_id,
                        epoch,
                        operation_id,
                        cause,
                        current_primary_node_id,
                    }
                }
                Err(error) => {
                    let failed_intent = self
                        .coordinator
                        .claimed_intent_for_target(&file_system_id, &object_id, &target_version)
                        .cloned();
                    // A target collision can belong to another accepted request.
                    // Its body, claim and transient object are not this failure's cleanup.
                    let owns_failed_claim = failed_intent.as_ref().is_none_or(|intent| {
                        intent.requesting_node_id == requester_node_id
                            && intent.operation_id.as_deref() == Some(request_id.as_str())
                            && intent.intent_id
                                == stable_write_intent_id(
                                    &requester_node_id,
                                    &object_id,
                                    &request_id,
                                )
                    });
                    let committed = failed_intent.as_ref().is_some_and(|intent| {
                        matches!(intent.state, DfsWriteIntentState::Committed)
                    });
                    if let Some(intent) = failed_intent.as_ref().filter(|_| owns_failed_claim) {
                        let prepared = self
                            .metadata_backend
                            .as_ref()
                            .ok_or_else(|| DfsError::Metadata {
                                message:
                                    "routed write recovery requires its actual durable backend"
                                        .into(),
                            })?
                            .prepared_operation_record(&format!("write:{}", intent.intent_id))?;
                        if let Some(prepared) = prepared.filter(|prepared| {
                            matches!(
                                prepared.status,
                                DfsPreparedOperationStatus::Prepared
                                    | DfsPreparedOperationStatus::Committed
                            )
                        }) {
                            prepared.validate()?;
                            let original =
                                prepared.payload.prepared_intent.as_ref().ok_or_else(|| {
                                    DfsError::Metadata {
                                        message:
                                            "routed write preparation lost its captured intent"
                                                .into(),
                                    }
                                })?;
                            let mut sealed_intent =
                                crate::intent::distributed_intent_from_record(original);
                            // The prepared intent records the original active state;
                            // live execution may already have observed its terminal commit.
                            sealed_intent.state = intent.state.clone();
                            let target = prepared
                                .transaction
                                .objects
                                .iter()
                                .find(|object| object.object_id == object_id);
                            let descriptor_matches = target.is_some_and(|target| {
                                target.file_system_id == file_system_id
                                    && target.version == target_version
                                    && target.content_hash.is_some()
                                    && target.content_size_bytes.is_some()
                                    && original.content_hash == target.content_hash
                                    && match self.writes.pending_writes.get(&intent.intent_id) {
                                        Some(pending) => {
                                            target.content_hash.as_deref()
                                                == Some(pending.content_hash.as_str())
                                                && target.content_size_bytes
                                                    == Some(pending.size_bytes)
                                        }
                                        // A genuine committed body retains the complete
                                        // immutable descriptor after its transfer retires.
                                        None => {
                                            prepared.status == DfsPreparedOperationStatus::Committed
                                        }
                                    }
                            });
                            if sealed_intent != *intent
                                || !descriptor_matches
                                || prepared.digest.operation_id
                                    != format!("write:{}", intent.intent_id)
                                || prepared.digest.file_system_id != file_system_id
                                || prepared.digest.owner_epoch != intent.owner_epoch
                                || original.intent_id != intent.intent_id
                                || original.file_system_id != file_system_id
                                || original.object_id != object_id
                                || original.target_version != target_version
                                || original.base_version != intent.base_version
                                || original.owner_epoch != intent.owner_epoch
                                || original.coordinator_node_id != requester_node_id
                                || original.operation_id.as_deref() != Some(request_id.as_str())
                                || !prepared.transaction.objects.iter().any(|object| {
                                    object.file_system_id == file_system_id
                                        && object.object_id == object_id
                                        && object.version == target_version
                                })
                            {
                                return Err(DfsError::Metadata {
                                    message:
                                        "routed write failure differs from its retained preparation"
                                            .into(),
                                });
                            }
                            // The same complete kernel body still owns recovery.
                            // A failed authority read or uncertain publication cannot
                            // author a contradictory automatic abort of that body.
                            return Err(DfsError::NetworkDelivery {
                                message: format!(
                                    "write {request_id} retains its exact durable preparation and requires authoritative reconciliation: {error}"
                                ),
                                delivery_state:
                                    sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                            });
                        }
                    }
                    if let Some(intent) = failed_intent.as_ref().filter(|intent|
                        owns_failed_claim && intent.publication.current_precondition().is_some()
                            && intent.state == DfsWriteIntentState::AwaitingReceipts)
                    {
                        // Conditional execution retains its original input
                        // until the existing kernel can prove a terminal
                        // outcome. An unknown/evicted final body or interrupted
                        // negative publication cannot become an automatic abort.
                        return Err(DfsError::NetworkDelivery {
                            message: format!("write {request_id} retains conditional execution {} and requires authoritative terminal reconciliation: {error}", intent.intent_id),
                            delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                        });
                    }
                    let mut cleanup_error = None;
                    if injected_transient_metadata && !committed && owns_failed_claim {
                        let mut cleanup_completed = true;
                        if let Some(intent) = failed_intent {
                            if !matches!(intent.state, DfsWriteIntentState::PreconditionFailed { .. }) {
                                if let Err(source) = self.abort_pending_write_locally(&intent.intent_id) {
                                    cleanup_error = Some(source.to_string());
                                    cleanup_completed = false;
                                }
                            }
                        }
                        if cleanup_completed {
                            self.remove_transient_object_metadata(&object_id);
                        }
                    }
                    if let Some(cleanup_error) = cleanup_error {
                        return Err(DfsError::Distributed {
                            message: format!(
                                "{error}; failed to durably abort rejected routed create: {cleanup_error}; metadata authority is quarantining the create"
                            ),
                        });
                    }
                    let current_primary_node_id =
                        distributed_write_rejection_current_primary(&error);
                    match error {
                        DfsError::WritePreconditionFailed { failure } => {
                            DfsDistributedWriteResult::PreconditionFailed { failure }
                        }
                        DfsError::UnsupportedWritePrecondition { path, reason } => {
                            DfsDistributedWriteResult::UnsupportedPrecondition { path, reason }
                        }
                        error => DfsDistributedWriteResult::Rejected {
                            reason: error.to_string(),
                            current_primary_node_id,
                        },
                    }
                }
            }
        };
        let apply_elapsed = apply_started.elapsed();
        let journal_started = Instant::now();
        let metadata_journal_batches = self.routed_write_response_metadata_journal_batches(
            &object_id,
            &requester_node_id,
            &result,
            metadata_journal_after_sequence,
            if foreground_replica_repair {
                DFS_ROUTED_RESPONSE_JOURNAL_BATCH_LIMIT
            } else {
                DFS_ROUTED_WRITE_RESPONSE_JOURNAL_BATCH_LIMIT
            },
        )?;
        let journal_elapsed = journal_started.elapsed();
        let metadata_journal_batch_count = metadata_journal_batches.len();
        let send_started = Instant::now();
        let send_result = self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                request_id: request_id.clone(),
                object_id: object_id.clone(),
                result: result.clone(),
                metadata_journal_batches,
            }),
        );
        let send_elapsed = send_started.elapsed();
        dfs_perf_trace(format_args!(
            "distributed-write-serve node={} requester={} request={} object={} payload_bytes={} manifest_only={} result={} journal_batches={} apply_ms={} journal_ms={} response_send_ms={} total_ms={} response_sent={}",
            self.node_id,
            requester_node_id,
            request_id,
            object_id,
            trace_payload_bytes,
            trace_manifest_only,
            dfs_distributed_write_result_trace_label(&result),
            metadata_journal_batch_count,
            apply_elapsed.as_millis(),
            journal_elapsed.as_millis(),
            send_elapsed.as_millis(),
            trace_started.elapsed().as_millis(),
            send_result.is_ok()
        ));
        send_result?;
        self.schedule_committed_routed_write_local_fetch_if_needed(&result)?;
        Ok(NetworkedDfsServiceEvent::RemoteWriteServed {
            request_id,
            object_id,
            requester_node_id,
            result,
        })
    }

    fn schedule_committed_routed_write_local_fetch_if_needed(
        &mut self,
        result: &DfsDistributedWriteResult,
    ) -> Result<(), DfsError> {
        let DfsDistributedWriteResult::Outcome {
            outcome:
                NetworkedDfsWriteOutcome::Committed {
                    intent_id,
                    repair_jobs,
                    ..
                },
        } = result
        else {
            return Ok(());
        };
        if !repair_jobs
            .iter()
            .any(|job| job.missing_replica_node_id == self.node_id)
        {
            return Ok(());
        }
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Ok(());
        };
        let Some(pending) = self.writes.pending_writes.get(intent_id).cloned() else {
            return Ok(());
        };
        // Served transports return queued messages together with the commit
        // response. A "post-response" fetch here therefore still makes the
        // requester synchronously transfer bytes and process the resulting
        // repair before its write call can return. Background writes already
        // persist their missing-replica work for the maintenance owner.
        if !pending.foreground_replica_repair {
            return Ok(());
        }
        let _ = self.request_stage_blob_from_alternate(&intent, &pending)?;
        Ok(())
    }

    fn track_pending_remote_write_response(
        &mut self,
        intent_id: &str,
        requester_node_id: &NodeId,
        request_id: &str,
        object_id: &DfsObjectId,
        metadata_journal_after_sequence: Option<u64>,
        created_object_path: Option<String>,
        foreground_replica_repair: bool,
    ) {
        if let Some(existing) = self
            .writes
            .pending_remote_write_responses
            .get_mut(intent_id)
        {
            if existing.requester_node_id == *requester_node_id
                && existing.request_id == request_id
                && existing.object_id == *object_id
            {
                // A response-safe transport retry can redispatch the exact
                // request while its intent is still pending. Replay no longer
                // runs namespace creation, so preserve the first dispatch's
                // parent-touch obligation until the write commits.
                if existing.created_object_path.is_none() {
                    existing.created_object_path = created_object_path;
                }
            }
            return;
        }
        self.writes.pending_remote_write_responses.insert(
            intent_id.to_string(),
            PendingRemoteWriteResponse {
                requester_node_id: requester_node_id.clone(),
                request_id: request_id.to_string(),
                object_id: object_id.clone(),
                metadata_journal_after_sequence,
                created_object_path,
                foreground_replica_repair,
                created_at: Instant::now(),
                commit_outcome_unknown: None,
            },
        );
    }

    /// Settle a commit attempt made when a content receipt arrived. An
    /// unknown outcome for a tracked served write is its requester's answer,
    /// not a failure of the receipt: the durable prepare carries the receipt
    /// and the requester retries the same operation id. Cancellation, every
    /// other error, and an unknown with no tracked served write still end the
    /// receipt's handling.
    pub(crate) fn settle_receipt_commit_attempt(
        &mut self,
        intent_id: &str,
        attempt: Result<(), DfsError>,
    ) -> Result<(), DfsError> {
        match attempt {
            Err(error) if error.is_commit_outcome_unknown() && !error.is_cancellation() => {
                match self
                    .writes
                    .pending_remote_write_responses
                    .get_mut(intent_id)
                {
                    Some(pending) => {
                        pending.commit_outcome_unknown = Some(error);
                        Ok(())
                    }
                    None => Err(error),
                }
            }
            attempt => attempt,
        }
    }

    fn rebase_routed_path_write_if_current(
        &mut self,
        command: &mut DfsDistributedWriteCommand,
    ) -> Result<(), DfsError> {
        if command.bytes.is_empty() && command.content_hash.is_none() {
            return Ok(());
        }
        let Some(path_object) = command.object_metadata.as_ref() else {
            return Ok(());
        };
        let current = match self.current_object_for_write(&command.request.object_id) {
            Ok(current) => current,
            Err(DfsError::UnknownObject { .. })
                if !command.exclusive_create && path_object.content_hash.is_none() =>
            {
                // A requester that has not received a newly created hardlink
                // may propose a new path-derived inode. Resolve that blind
                // write through the primary's actual alias identity; aliases
                // need not share the requester's provisional object id.
                let current = match self.metadata_backend.as_ref() {
                    Some(backend) => backend
                        .get_object_by_path(&command.request.file_system_id, &path_object.path)?,
                    None => self
                        .metadata
                        .object_by_normalized_posix_path(
                            &command.request.file_system_id,
                            &path_object.path,
                        )
                        .cloned(),
                };
                match current {
                    Some(current) if current.kind == DfsObjectKind::Hardlink => current,
                    _ => return Ok(()),
                }
            }
            Err(DfsError::UnknownObject { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };
        let target = if current.kind == DfsObjectKind::Hardlink {
            self.resolve_hardlink_target(&current)
                .map_err(|error| DfsError::Metadata {
                    message: error.to_string(),
                })?
        } else {
            current.clone()
        };
        if !matches!(target.kind, DfsObjectKind::File | DfsObjectKind::Symlink) {
            return Ok(());
        }
        let path_resolves_to_hardlink = current.kind == DfsObjectKind::Hardlink;
        if path_object.content_hash.is_none() && !path_resolves_to_hardlink {
            return Ok(());
        }
        if path_object.path != current.path && path_object.path != target.path {
            return Ok(());
        }
        let stale_base = command
            .expected_base_version
            .as_ref()
            .is_some_and(|expected| expected != &target.version);
        let different_object = command.request.object_id != target.object_id;
        if !stale_base && !different_object {
            return Ok(());
        }
        command.request.object_id = target.object_id.clone();
        command.request.target_version = namespace::next_version(&target.version);
        command.expected_base_version = Some(target.version.clone());
        command.object_metadata = Some(target);
        if command.content_hash.is_some() && !different_object {
            command.foreground_replica_repair = false;
        }
        Ok(())
    }

    fn canonical_pending_routed_write_request_id(&self, request_id: &str) -> Option<String> {
        self.writes
            .pending_routed_writes
            .get(request_id)
            .map(|pending| pending.canonical_request_id.clone())
    }

    pub(crate) fn track_pending_routed_write_alias(
        &mut self,
        canonical_request_id: &str,
        alias: String,
    ) -> Result<(), DfsError> {
        if alias == canonical_request_id {
            return Ok(());
        }
        if let Some(existing) = self.writes.pending_routed_writes.get(&alias)
            && existing.canonical_request_id != canonical_request_id
        {
            return Err(DfsError::Distributed {
                message: "routed response alias belongs to a different operation".into(),
            });
        }
        let Some(canonical) = self.writes.pending_routed_writes.get(canonical_request_id) else {
            return Ok(());
        };
        if canonical.result.is_none() {
            self.transport
                .register_routed_write_response_owner(
                    canonical_request_id,
                    &canonical.object_id,
                    std::slice::from_ref(&alias),
                )
                .map_err(|error| DfsError::NetworkDelivery {
                    message: format!("routed write remains unresolved after alias delivery registration failed: {error}"),
                    delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                })?;
        }
        self.install_pending_routed_write_alias(canonical_request_id, alias);
        Ok(())
    }

    /// Infallible state installation after all delivery keys were reserved.
    pub(crate) fn install_pending_routed_write_alias(
        &mut self,
        canonical_request_id: &str,
        alias: String,
    ) {
        if alias == canonical_request_id || self.writes.pending_routed_writes.contains_key(&alias) {
            return;
        }
        let Some(canonical) = self.writes.pending_routed_writes.get(canonical_request_id) else {
            return;
        };
        let alias_pending = PendingRoutedWrite {
            canonical_request_id: canonical_request_id.to_string(),
            object_id: canonical.object_id.clone(),
            primary_node_id: canonical.primary_node_id.clone(),
            authority_shard: canonical.authority_shard.clone(),
            command: None,
            attachment_bytes: None,
            patch_attachment: None,
            authority_redirects_remaining: canonical.authority_redirects_remaining,
            remote_intent_id: canonical.remote_intent_id.clone(),
            durable_receipts: canonical.durable_receipts,
            required_receipts: canonical.required_receipts,
            result: canonical.result.clone(),
            compact_retained_at: canonical.compact_retained_at,
        };
        self.writes
            .pending_routed_writes
            .insert(alias, alias_pending);
    }

    /// Replace every requester-side copy of an unresolved routed command with
    /// the compact terminal outcome. Exact retries are recovered from the
    /// authoritative intent receipt applied before this transition.
    fn complete_pending_routed_write(
        &mut self,
        request_id: &str,
        result: Result<NetworkedDfsWriteOutcome, DfsError>,
    ) -> Option<RoutedWriteManifestPublicationReceipt> {
        let canonical_request_id = self.canonical_pending_routed_write_request_id(request_id)?;
        let publication = self
            .writes
            .pending_routed_writes
            .get(&canonical_request_id)
            .map(|pending| RoutedWriteManifestPublicationReceipt {
                request_id: canonical_request_id.clone(),
                publication: pending
                    .command
                    .as_ref()
                    .map(|command| command.publication.clone()),
                object_id: pending
                    .command
                    .as_ref()
                    .map(|command| command.request.object_id.clone())
                    .unwrap_or_else(|| pending.object_id.clone()),
                file_system_id: pending.authority_shard.file_system_id.clone(),
                path: pending
                    .command
                    .as_ref()
                    .and_then(|command| command.object_metadata.as_ref())
                    .map(|object| object.path.clone()),
            });
        let target_version = self
            .writes
            .pending_routed_writes
            .get(&canonical_request_id)
            .and_then(|pending| pending.command.as_ref())
            .map(|command| command.request.target_version.clone());
        for pending in self
            .writes
            .pending_routed_writes
            .values_mut()
            .filter(|pending| pending.canonical_request_id == canonical_request_id)
        {
            pending.command = None;
            pending.attachment_bytes = None;
            pending.patch_attachment = None;
            pending.result = Some(result.clone());
            pending.compact_retained_at = Some(Instant::now());
        }
        self.transport
            .release_routed_write_response_owner(&canonical_request_id);
        // A caller without a metadata store retires the overlay of this write
        // once its authority committed it; nothing else would retire it after
        // the object later changes or moves.
        if self.metadata_backend.is_none()
            && self.transport.delegates_local_namespace_authority()
            && let (Some(receipt), Ok(NetworkedDfsWriteOutcome::Committed { version, .. })) =
                (&publication, &result)
            && self
                .transient_metadata_object_ids
                .contains(&receipt.object_id)
            && self.metadata.object(&receipt.object_id).is_some_and(|overlay| {
                overlay.version == *version || Some(&overlay.version) == target_version.as_ref()
            })
        {
            self.remove_transient_object_metadata(&receipt.object_id);
        }
        publication
    }

    /// Drop an unresolved or consumed routed write and all of its aliases.
    /// A late response is intentionally discarded; retry reconciliation uses
    /// the durable primary intent identity instead of requester memory.
    pub(crate) fn abandon_pending_routed_write(&mut self, request_id: &str) {
        let Some(canonical_request_id) = self.canonical_pending_routed_write_request_id(request_id)
        else {
            return;
        };
        self.transport
            .release_routed_write_response_owner(&canonical_request_id);
        self.writes
            .pending_routed_writes
            .retain(|_, pending| pending.canonical_request_id != canonical_request_id);
    }

    /// Release the retry body without discarding enough identity to admit a
    /// late authoritative response. A timed-out foreground caller can return
    /// while metadata replay still converges through the resident service.
    pub(crate) fn compact_pending_routed_write(&mut self, request_id: &str) {
        let Some(canonical_request_id) = self.canonical_pending_routed_write_request_id(request_id)
        else {
            return;
        };
        // Redirect/rebase handling may have updated the wire command's object
        // identity. Preserve only that final lookup identity before releasing
        // the payload, replica list, metadata body, and all other retry state.
        let publication_object_id = self
            .writes
            .pending_routed_writes
            .get(&canonical_request_id)
            .and_then(|pending| pending.command.as_ref())
            .map(|command| command.request.object_id.clone())
            .or_else(|| {
                self.writes
                    .pending_routed_writes
                    .get(&canonical_request_id)
                    .map(|pending| pending.object_id.clone())
            });
        for pending in self
            .writes
            .pending_routed_writes
            .values_mut()
            .filter(|pending| pending.canonical_request_id == canonical_request_id)
        {
            if let Some(object_id) = publication_object_id.as_ref() {
                pending.object_id.clone_from(object_id);
            }
            pending.command = None;
            pending.attachment_bytes = None;
            pending.patch_attachment = None;
            pending.compact_retained_at = Some(Instant::now());
        }
        self.prune_retained_routed_writes();
    }

    /// Bound the requester-side late-response ledger by both age and request
    /// count. Aliases are removed with their canonical request so a surviving
    /// key can never point at an evicted operation.
    pub(crate) fn prune_retained_routed_writes(&mut self) {
        let mut compact_requests = self
            .writes
            .pending_routed_writes
            .iter()
            .filter_map(|(request_id, pending)| {
                (request_id == &pending.canonical_request_id)
                    .then_some(
                        pending
                            .compact_retained_at
                            .map(|retained_at| (request_id.clone(), retained_at)),
                    )
                    .flatten()
            })
            .collect::<Vec<_>>();
        if compact_requests.is_empty() {
            return;
        }
        compact_requests
            .sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
        let excess = compact_requests
            .len()
            .saturating_sub(DFS_ROUTED_WRITE_LATE_RESPONSE_MAX_REQUESTS);
        let now = Instant::now();
        let expired = compact_requests
            .into_iter()
            .enumerate()
            .filter_map(|(index, (request_id, retained_at))| {
                (index < excess
                    || now.saturating_duration_since(retained_at)
                        >= DFS_ROUTED_WRITE_LATE_RESPONSE_TTL)
                    .then_some(request_id)
            })
            .collect::<BTreeSet<_>>();
        if expired.is_empty() {
            return;
        }
        for canonical in &expired {
            self.transport
                .release_routed_write_response_owner(canonical);
        }
        self.writes
            .pending_routed_writes
            .retain(|_, pending| !expired.contains(&pending.canonical_request_id));
    }

    pub(crate) fn take_write_outcome(
        &mut self,
        intent_id: &str,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let result = self.write_outcome(intent_id);
        match &result {
            Ok(NetworkedDfsWriteOutcome::Pending { .. }) => {}
            // The primary still holds the durable body; keep enough identity
            // to admit a late authoritative response.
            Err(error) if error.is_commit_outcome_unknown() => {
                self.compact_pending_routed_write(intent_id);
            }
            _ => self.abandon_pending_routed_write(intent_id),
        }
        result
    }

    pub(crate) fn handle_distributed_write_response(
        &mut self,
        primary_node_id: NodeId,
        response: DfsDistributedWriteResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        // Late, duplicate, or mismatched responses (a request that already
        // settled, timed out, or was retried against another primary) are
        // discarded; a stray frame must never fail the whole event loop.
        let (canonical_request_id, expected_authority_shard) = {
            let Some(tracked) = self.writes.pending_routed_writes.get(&response.request_id) else {
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason: "distributed write response for an untracked request".to_string(),
                });
            };
            let canonical_request_id = tracked.canonical_request_id.clone();
            let Some(pending) = self.writes.pending_routed_writes.get(&canonical_request_id) else {
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason: "distributed write response for an orphaned intent alias".to_string(),
                });
            };
            if pending.result.is_some() {
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason: "distributed write response for an already completed request"
                        .to_string(),
                });
            }
            if pending.primary_node_id != primary_node_id {
                let reason = format!(
                    "distributed write response did not come from primary {}",
                    pending.primary_node_id
                );
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason,
                });
            }
            if pending.object_id != response.object_id {
                let reason = format!(
                    "distributed write response object mismatch: expected {}, actual {}",
                    pending.object_id, response.object_id
                );
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason,
                });
            }
            (canonical_request_id, pending.authority_shard.clone())
        };
        let validated_authority_shard =
            if matches!(&response.result, DfsDistributedWriteResult::Outcome { .. }) {
                match self.validate_routed_write_response_authority(
                    &primary_node_id,
                    &expected_authority_shard,
                ) {
                    Ok(validated) => Some(validated),
                    Err(error) => {
                        let reason = format!("routed write response authority is stale: {error}");
                        self.complete_pending_routed_write(
                            &canonical_request_id,
                            Err(DfsError::Distributed {
                                message: reason.clone(),
                            }),
                        );
                        return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                            request_id: response.request_id,
                            source_node_id: primary_node_id,
                            reason,
                        });
                    }
                }
            } else {
                None
            };
        if let DfsDistributedWriteResult::Outcome {
            outcome:
                NetworkedDfsWriteOutcome::Pending { intent_id, .. }
                | NetworkedDfsWriteOutcome::Committed { intent_id, .. },
        } = &response.result
            && let Err(error) =
                self.track_pending_routed_write_alias(&canonical_request_id, intent_id.clone())
        {
            return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                request_id: response.request_id,
                source_node_id: primary_node_id,
                reason: error.to_string(),
            });
        }
        // Rejections carry no authoritative journal. Keeping their payload out
        // of replica replay also lets the corroborated redirect path below
        // refresh and evaluate the response's explicit authority claim.
        if let Some(validated_authority_shard) = validated_authority_shard.as_ref() {
            for batch in &response.metadata_journal_batches {
                if let Err(error) = self.apply_routed_write_metadata_journal_batch(
                    &primary_node_id,
                    validated_authority_shard,
                    batch,
                ) {
                    // A replay failure is this requester's problem (for example a
                    // forked local journal); fail the request with the cause
                    // instead of poisoning the whole event loop.
                    let reason = format!("routed write journal replay failed: {error}");
                    self.complete_pending_routed_write(
                        &canonical_request_id,
                        Err(DfsError::Distributed {
                            message: reason.clone(),
                        }),
                    );
                    return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                        request_id: response.request_id,
                        source_node_id: primary_node_id,
                        reason,
                    });
                }
            }
        }
        if let DfsDistributedWriteResult::Rejected {
            reason,
            current_primary_node_id: Some(current_primary_node_id),
        } = &response.result
        {
            let redirect = self
                .writes
                .pending_routed_writes
                .get(&canonical_request_id)
                .and_then(|pending| {
                    let command = pending.command.as_ref()?;
                    Some((
                        current_primary_node_id.clone(),
                        command.clone(),
                        pending.attachment_bytes.clone(),
                        pending.patch_attachment.clone(),
                        pending.authority_shard.clone(),
                        pending.authority_redirects_remaining,
                    ))
                });
            if let Some((
                current_primary_node_id,
                mut command,
                attachment_bytes,
                patch_attachment,
                previous_authority,
                redirects_remaining,
            )) = redirect
            {
                if redirects_remaining == 0 {
                    self.complete_pending_routed_write(
                        &canonical_request_id,
                        Err(DfsError::Distributed {
                            message: format!(
                                "routed distributed write exceeded {DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT} authority redirects: {reason}"
                            ),
                        }),
                    );
                    return Ok(NetworkedDfsServiceEvent::RemoteWriteCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        primary_node_id,
                        result: response.result,
                    });
                } else {
                    if let Err(error) = self.refresh_control_plane_latest() {
                        let reason = format!(
                            "distributed write authority redirect could not refresh the committed shard assignment: {error}"
                        );
                        self.complete_pending_routed_write(
                            &canonical_request_id,
                            Err(DfsError::Distributed {
                                message: reason.clone(),
                            }),
                        );
                        return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                            request_id: response.request_id,
                            source_node_id: primary_node_id,
                            reason,
                        });
                    }
                    let redirected_authority = self
                        .control_plane
                        .as_mut()
                        .map(|control_plane| {
                            DfsControlPlaneStore::new(control_plane.as_mut()).get_metadata_shard(
                                &previous_authority.file_system_id,
                                &previous_authority.shard_id,
                            )
                        })
                        .transpose();
                    let redirected_authority = match redirected_authority {
                        Ok(authority) => authority.flatten(),
                        Err(error) => {
                            let reason = format!(
                                "distributed write authority redirect could not read the committed shard assignment: {error}"
                            );
                            self.complete_pending_routed_write(
                                &canonical_request_id,
                                Err(DfsError::Distributed {
                                    message: reason.clone(),
                                }),
                            );
                            return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                                request_id: response.request_id,
                                source_node_id: primary_node_id,
                                reason,
                            });
                        }
                    };
                    let Some(redirected_authority) = redirected_authority else {
                        self.complete_pending_routed_write(
                            &canonical_request_id,
                            Err(DfsError::Distributed {
                                message: format!(
                                    "redirected distributed write has no current shard assignment for {}",
                                    previous_authority.shard_id
                                ),
                            }),
                        );
                        return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                            request_id: response.request_id,
                            source_node_id: primary_node_id,
                            reason: "distributed write authority redirect could not be bound to a current shard assignment"
                                .to_string(),
                            });
                    };
                    if redirected_authority == previous_authority {
                        // A rejection that still names the exact assignment
                        // already used for dispatch is not a topology redirect.
                        // Retrying it recursively would only burn the redirect
                        // budget against an identical command.
                        self.complete_pending_routed_write(
                            &canonical_request_id,
                            Err(routed_write_rejection_error(reason)),
                        );
                        return Ok(NetworkedDfsServiceEvent::RemoteWriteCompleted {
                            request_id: response.request_id,
                            object_id: response.object_id,
                            primary_node_id,
                            result: response.result,
                        });
                    }
                    if redirected_authority.primary_node_id != current_primary_node_id {
                        self.complete_pending_routed_write(
                            &canonical_request_id,
                            Err(DfsError::Distributed {
                                message: format!(
                                    "distributed write redirect named primary {current_primary_node_id} but current shard {} epoch {} names {}",
                                    redirected_authority.shard_id,
                                    redirected_authority.epoch,
                                    redirected_authority.primary_node_id
                                ),
                            }),
                        );
                        return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                            request_id: response.request_id,
                            source_node_id: primary_node_id,
                            reason: "distributed write authority redirect does not match the current committed assignment"
                                .to_string(),
                            });
                    }
                    command.authority_shard = Some(redirected_authority.clone());
                    if current_primary_node_id == self.node_id {
                        // The requester has become the authority. Its loopback
                        // write updates the authoritative backend directly;
                        // asking it for replica catch-up would replay its own
                        // journal through a deliberately forbidden path.
                        command.metadata_journal_after_sequence = None;
                    }
                    let message = command.clone().into_network_message();
                    // The corroborated new assignment must own responses before
                    // dispatch. A lost reply can still be followed by its late
                    // authoritative completion; keeping the retiring primary
                    // here would discard that completion as mismatched.
                    let (_, previous_pending) = self.track_pending_routed_write(
                        canonical_request_id.clone(),
                        command.request.object_id.clone(),
                        current_primary_node_id.clone(),
                        redirected_authority.clone(),
                        command,
                        attachment_bytes.clone(),
                    )?;
                    if let Some(pending) = self
                        .writes
                        .pending_routed_writes
                        .get_mut(&canonical_request_id)
                    {
                        pending.authority_redirects_remaining = redirects_remaining - 1;
                        pending.patch_attachment = patch_attachment.clone();
                    }
                    let sent = if let Some(bytes) = attachment_bytes {
                        let routed =
                            self.next_dfs_routed_message(&current_primary_node_id, message);
                        self.transport.send_routed_call(
                            &current_primary_node_id,
                            DfsRoutedCall::with_attachment(routed,
                                DfsOutboundAttachment::from_shared_bytes_with_transport_integrity(bytes)),
                        ).map(|()| true).or_else(|error| match error {
                            sinomo_core::network::NetworkError::UnknownNode { .. } => Ok(false),
                            error => Err(DfsError::from_network(error)),
                        })
                    } else if let Some(attachment) = patch_attachment {
                        let routed =
                            self.next_dfs_routed_message(&current_primary_node_id, message);
                        let body = DfsOutboundAttachment::from_quarantined_patch(attachment.0)?;
                        self.transport
                            .send_routed_call(
                                &current_primary_node_id,
                                DfsRoutedCall::with_attachment(routed, body),
                            )
                            .map(|()| true)
                            .or_else(|error| match error {
                                sinomo_core::network::NetworkError::UnknownNode { .. } => Ok(false),
                                error => Err(DfsError::from_network(error)),
                            })
                    } else {
                        self.send_dfs_message_if_routable(&current_primary_node_id, message)
                    };
                    self.finish_routed_write_dispatch(
                        &canonical_request_id,
                        previous_pending,
                        &redirected_authority,
                        &current_primary_node_id,
                        sent,
                    )?;

                    return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                        request_id: response.request_id,
                        source_node_id: primary_node_id,
                        reason: format!(
                            "distributed write authority moved to {current_primary_node_id}; retrying"
                        ),
                    });
                }
            }
        }
        let mut committed_manifest_publish = None;
        match &response.result {
            DfsDistributedWriteResult::Outcome {
                outcome:
                    NetworkedDfsWriteOutcome::Pending {
                        intent_id,
                        durable_receipts,
                        required_receipts,
                        ..
                    },
            } => {
                let pending = self
                    .writes
                    .pending_routed_writes
                    .get_mut(&canonical_request_id)
                    .expect("pending routed write was validated before metadata replay");
                if pending.result.is_none() {
                    pending.remote_intent_id = Some(intent_id.clone());
                    pending.durable_receipts = *durable_receipts;
                    pending.required_receipts = *required_receipts;
                }
            }
            DfsDistributedWriteResult::Outcome {
                outcome: outcome @ NetworkedDfsWriteOutcome::Committed { .. },
            } => {
                committed_manifest_publish = self
                    .complete_pending_routed_write(&canonical_request_id, Ok(outcome.clone()))
                    .map(|publication| (publication, outcome.clone()));
            }
            DfsDistributedWriteResult::ReconciliationRequired {
                operation_id,
                reason,
            } => {
                self.complete_pending_routed_write(
                    &canonical_request_id,
                    Err(DfsError::WriteReconciliationRequired {
                        operation_id: operation_id.clone(),
                        object_id: response.object_id.clone(),
                        reason: reason.clone(),
                    }),
                );
            }
            DfsDistributedWriteResult::CommitOutcomeUnknown {
                file_system_id,
                shard_id,
                epoch,
                operation_id,
                cause,
                current_primary_node_id,
                ..
            } => {
                self.complete_pending_routed_write(
                    &canonical_request_id,
                    Err(DfsError::MetadataCommitOutcomeUnknown {
                        file_system_id: file_system_id.clone(),
                        shard_id: shard_id.clone(),
                        epoch: *epoch,
                        operation_id: operation_id.clone(),
                        cause: cause.clone(),
                        current_primary_node_id: current_primary_node_id.clone(),
                    }),
                );
            }
            DfsDistributedWriteResult::PreconditionFailed { failure } => {
                failure.validate().map_err(|message| DfsError::Serialization { message })?;
                let matches_original = self.writes.pending_routed_writes.get(&canonical_request_id)
                    .and_then(|pending| pending.command.as_ref())
                    .is_none_or(|command| command.publication.current_precondition()
                        .is_some_and(|condition| crate::write_precondition::failure_matches_condition(condition, failure)));
                if !matches_original {
                    return Err(DfsError::Serialization { message: "write failure does not bind the original conditional request".into() });
                }
                self.complete_pending_routed_write(&canonical_request_id,
                    Err(DfsError::WritePreconditionFailed { failure: failure.clone() }));
            }
            DfsDistributedWriteResult::UnsupportedPrecondition { path, reason } => {
                self.complete_pending_routed_write(&canonical_request_id,
                    Err(DfsError::UnsupportedWritePrecondition { path: path.clone(), reason: reason.clone() }));
            }
            DfsDistributedWriteResult::Rejected { reason, .. } => {
                let remote_intent_id = self
                    .writes
                    .pending_routed_writes
                    .get(&canonical_request_id)
                    .and_then(|pending| pending.remote_intent_id.clone());
                if remote_intent_id
                    .as_deref()
                    .is_some_and(|intent_id| response.request_id != intent_id)
                {
                    return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                        request_id: response.request_id,
                        source_node_id: primary_node_id,
                        reason: format!(
                            "routed write retry was rejected after primary intent {} became durable: {reason}",
                            remote_intent_id.expect("remote intent checked above")
                        ),
                    });
                }
                self.complete_pending_routed_write(
                    &canonical_request_id,
                    Err(routed_write_rejection_error(reason)),
                );
            }
        }
        if let Some((publication, outcome)) = committed_manifest_publish {
            if primary_node_id == self.node_id
                && self.transport.delegates_local_namespace_authority()
                && let Err(error) = self
                    .project_local_main_write_completion(&publication, &expected_authority_shard)
            {
                // The exact validated response has already retained its terminal
                // outcome. A requester projection failure cannot uncommit Main's
                // operation or cause it to be authored again.
                dfs_perf_trace(format_args!(
                    "routed-write-local-projection-failed node={} request={} object={} error={}",
                    self.node_id, publication.request_id, publication.object_id, error
                ));
            }
            self.publish_committed_routed_write_manifest_if_local(&publication, &outcome);
        }
        Ok(NetworkedDfsServiceEvent::RemoteWriteCompleted {
            request_id: response.request_id,
            object_id: response.object_id,
            primary_node_id,
            result: response.result,
        })
    }

    /// A same-process Main writes the shared durable tables itself and sends no
    /// replica journal to its requester. Retire that requester's planning overlay
    /// and read the current durable object; a retained or superseded completion
    /// must never install its historical target version as the current head.
    fn project_local_main_write_completion(
        &mut self,
        publication: &RoutedWriteManifestPublicationReceipt,
        authority_shard: &DfsMetadataShardConfig,
    ) -> Result<(), DfsError> {
        self.remove_transient_object_metadata(&publication.object_id);
        if self.metadata_backend.is_none() && self.transport.delegates_local_namespace_authority() {
            // The requester owns no durable image. Its result boundary reads
            // the current head through Main after response processing ends.
            self.clear_metadata_lookup_cache();
            return Ok(());
        }
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "same-node Main write completion requires a durable metadata backend"
                    .into(),
            })?;
        let object = backend.get_object_on_shard(
            &DfsMetadataShardId::new(authority_shard.shard_id.clone()),
            &publication.object_id,
        )?;
        let object = match (object, publication.path.as_deref()) {
            (Some(object), _) => Some(object),
            (None, Some(path)) => {
                // Main may resolve a blind path write to an existing hardlink.
                // Follow the normal durable path router, never the request's
                // provisional inode or its proposed readable locations.
                backend.get_object_by_path(&publication.file_system_id, path)?
            }
            (None, None) => None,
        };
        if let Some(object) = object {
            self.apply_metadata_object_to_service_state(object)?;
        }
        self.clear_metadata_lookup_cache();
        Ok(())
    }

    fn publish_committed_routed_write_manifest_if_local(
        &mut self,
        publication: &RoutedWriteManifestPublicationReceipt,
        outcome: &NetworkedDfsWriteOutcome,
    ) {
        let NetworkedDfsWriteOutcome::Committed {
            intent_id,
            version,
            content_hash,
            ..
        } = outcome
        else {
            return;
        };
        // Blob presence is a stronger and more durable publication capability
        // than the original request flag. It also lets a timed-out requester
        // release the complete command while preserving late-success behavior.
        // Direct-to-primary streams normally have no local blob and return
        // here without claiming local readability.
        let Ok(blob) = self.content.open_blob_stream(content_hash) else {
            return;
        };
        let size_bytes = blob.size_bytes();
        drop(blob);
        let committed_object_id = self
            .coordinator
            .intent(intent_id)
            .map(|intent| intent.object_id.clone());
        let publication_object_id = committed_object_id
            .as_ref()
            .unwrap_or(&publication.object_id);
        let current_object = self
            .current_read_object_metadata(publication_object_id)
            .ok()
            .and_then(|object| self.resolve_hardlink_target(&object).ok());
        let completion = current_object.as_ref().and_then(|object| {
            CanonicalCurrentWriteCompletion::from_current_publication(
                publication.publication.as_ref(),
                object,
                version,
                content_hash,
                size_bytes,
            )
        });
        let object_id = current_object
            .map(|object| object.object_id)
            .unwrap_or_else(|| publication_object_id.clone());
        let published = (|| {
            if let Some(completion) = completion.as_ref()
                && let Some(manifest) = self
                    .content
                    .try_complete_canonical_current_inline_manifest(completion)?
            {
                return Ok(manifest);
            }
            self.content.put_blob_only_manifest(
                object_id.as_str(),
                version.as_str(),
                content_hash,
                size_bytes,
            )
        })();
        if let Err(error) = published {
            dfs_perf_trace(format_args!(
                "routed-write-local-manifest-publish-failed node={} request={} object={} version={} error={}",
                self.node_id, publication.request_id, object_id, version, error
            ));
        }
    }

    pub(crate) fn flush_completed_pending_remote_write_responses(
        &mut self,
        events: &mut Vec<NetworkedDfsServiceEvent>,
    ) -> Result<(), DfsError> {
        if self.writes.pending_remote_write_responses.is_empty() {
            return Ok(());
        }
        let intent_ids = self
            .writes
            .pending_remote_write_responses
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for intent_id in intent_ids {
            self.refresh_committed_write_projection(&intent_id)?;
            let recorded_unknown = self
                .writes
                .pending_remote_write_responses
                .get(&intent_id)
                .and_then(|pending| pending.commit_outcome_unknown.clone());
            let result = match self.write_outcome(&intent_id) {
                Ok(NetworkedDfsWriteOutcome::Pending { .. }) => match recorded_unknown {
                    Some(unknown) => distributed_write_commit_outcome_unknown(&intent_id, unknown)?,
                    None => continue,
                },
                Ok(outcome) => DfsDistributedWriteResult::Outcome { outcome },
                Err(error) if error.is_commit_outcome_unknown() => {
                    distributed_write_commit_outcome_unknown(&intent_id, error)?
                }
                Err(error) => {
                    let current_primary_node_id =
                        distributed_write_rejection_current_primary(&error);
                    match error {
                        DfsError::WritePreconditionFailed { failure } => {
                            DfsDistributedWriteResult::PreconditionFailed { failure }
                        }
                        DfsError::UnsupportedWritePrecondition { path, reason } => {
                            DfsDistributedWriteResult::UnsupportedPrecondition { path, reason }
                        }
                        error => DfsDistributedWriteResult::Rejected {
                            reason: error.to_string(),
                            current_primary_node_id,
                        },
                    }
                }
            };
            let Some(pending) = self
                .writes
                .pending_remote_write_responses
                .get(&intent_id)
                .cloned()
            else {
                continue;
            };
            let committed_foreground_with_repairs = pending.foreground_replica_repair
                && matches!(
                    &result,
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { repair_jobs, .. },
                    } if !repair_jobs.is_empty()
                );
            if committed_foreground_with_repairs
                && !self
                    .transport
                    .can_return_committed_foreground_response_before_replica_repair()
            {
                continue;
            }
            // Node-routed DFS RPCs can return once the metadata primary has
            // committed the intent. Remaining repair jobs stay in the normal
            // repair backlog so a slow desired replica does not consume the
            // caller's data-plane deadline after the write is durable.
            let metadata_journal_batches = self.routed_write_response_metadata_journal_batches(
                &pending.object_id,
                &pending.requester_node_id,
                &result,
                pending.metadata_journal_after_sequence,
                if pending.foreground_replica_repair {
                    DFS_ROUTED_RESPONSE_JOURNAL_BATCH_LIMIT
                } else {
                    DFS_ROUTED_WRITE_RESPONSE_JOURNAL_BATCH_LIMIT
                },
            )?;
            let response =
                DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                    request_id: pending.request_id.clone(),
                    object_id: pending.object_id.clone(),
                    result: result.clone(),
                    metadata_journal_batches,
                });
            let sent = if self
                .transport
                .should_detach_completed_response(&pending.requester_node_id)
            {
                self.send_dfs_message_if_routable_detached(&pending.requester_node_id, response)?
            } else {
                self.send_dfs_message_if_routable(&pending.requester_node_id, response)?
            };
            if !sent {
                // Keep ownership in the pending map until the response is
                // either returned to the current caller or accepted by the
                // detached outbox.
                continue;
            }
            self.writes
                .pending_remote_write_responses
                .remove(&intent_id);
            events.push(NetworkedDfsServiceEvent::RemoteWriteServed {
                request_id: pending.request_id,
                object_id: pending.object_id,
                requester_node_id: pending.requester_node_id,
                result,
            });
        }
        Ok(())
    }

    pub(crate) fn send_untracked_completed_routed_write_response_if_committed(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
    ) -> Result<(), DfsError> {
        if !self.transport.can_send_synchronously() {
            self.writes
                .deferred_untracked_routed_write_responses
                .insert(intent_id.to_string(), object_id.clone());
            return Ok(());
        }
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Ok(());
        };
        if !matches!(intent.state, DfsWriteIntentState::Committed)
            || intent.requesting_node_id == self.node_id
            || self
                .writes
                .pending_remote_write_responses
                .contains_key(intent_id)
        {
            return Ok(());
        }
        let outcome = self.write_outcome(intent_id)?;
        if !matches!(outcome, NetworkedDfsWriteOutcome::Committed { .. }) {
            return Ok(());
        }
        let result = DfsDistributedWriteResult::Outcome { outcome };
        let metadata_journal_batches = self.routed_write_response_metadata_journal_batches(
            object_id,
            &intent.requesting_node_id,
            &result,
            None,
            DFS_ROUTED_WRITE_RESPONSE_JOURNAL_BATCH_LIMIT,
        )?;
        self.send_dfs_message_if_routable(
            &intent.requesting_node_id,
            DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                request_id: intent.intent_id,
                object_id: object_id.clone(),
                result,
                metadata_journal_batches,
            }),
        )?;
        Ok(())
    }

    /// Ensures the routed write's object metadata is known locally. Returns
    /// `true` when the object was unknown and the requester-provided metadata
    /// was injected, i.e. the routed write creates the object at its path.
    fn ensure_routed_write_object_metadata(
        &mut self,
        request: &DfsWriteRequest,
        object_metadata: Option<&DfsObjectMetadata>,
        exclusive_create: bool,
        exclusive_create_event_id: Option<EventId>,
    ) -> Result<bool, DfsError> {
        match self.current_object_for_write(&request.object_id) {
            Ok(current) if exclusive_create && current.kind != DfsObjectKind::Tombstone => {
                return Err(DfsError::AlreadyExists { path: current.path });
            }
            Ok(_) => return Ok(false),
            Err(DfsError::UnknownObject { .. }) => {}
            Err(error) => return Err(error),
        }
        let Some(object) = object_metadata else {
            return Err(DfsError::UnknownObject {
                object_id: request.object_id.clone(),
            });
        };
        if object.object_id != request.object_id || object.file_system_id != request.file_system_id
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "routed write metadata mismatch for object {}",
                    request.object_id
                ),
            });
        }
        // Absence was established by this authority above. A content create
        // introduces one name; caller-supplied metadata cannot mint an aggregate.
        // Existing inodes take the current-owner branch and preserve its count.
        if matches!(object.kind,
            DfsObjectKind::Directory | DfsObjectKind::Hardlink | DfsObjectKind::Tombstone)
            || object.namespace_link_count != Some(1)
        {
            return Err(DfsError::Metadata {
                message: "routed content create requires a fresh canonical name with count one".into(),
            });
        }
        let Some(mut shard) = self.select_metadata_shard_for_object(object)? else {
            return Err(DfsError::Distributed {
                message: format!(
                    "routed write for {} has no metadata shard assignment",
                    request.object_id
                ),
            });
        };
        if shard.primary_node_id != self.node_id {
            self.refresh_control_plane_latest()?;
            let Some(refreshed) = self.select_metadata_shard_for_object(object)? else {
                return Err(DfsError::Distributed {
                    message: format!(
                        "routed write for {} has no metadata shard assignment",
                        request.object_id
                    ),
                });
            };
            shard = refreshed;
        }
        if shard.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: object.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }
        if exclusive_create {
            let path_key = crate::posix_path::normalized_posix_path_key(&object.path)
                .unwrap_or_else(|| object.path.clone());
            let current =
                self.authoritative_metadata_object_for_path(&object.file_system_id, &path_key)?;
            if let Some(current) = current
                && current.kind != DfsObjectKind::Tombstone
            {
                return Err(DfsError::AlreadyExists { path: current.path });
            }
        }
        let mut object = object.clone();
        if object.content_hash.is_none() {
            let parent = self.ensure_routed_create_parent_current(&object)?;
            let attrs = DfsPosixCreateAttrs::new(object.mode, object.uid, object.gid);
            let attrs = parent.map_or(attrs, |parent| {
                crate::posix_object::inherit_posix_create_attrs(
                    attrs,
                    object.kind == DfsObjectKind::Directory,
                    parent.mode,
                    parent.gid,
                )
            });
            crate::posix_object::apply_posix_create_attrs(&mut object, attrs);
        }
        if exclusive_create {
            object.last_modified_by_event_id = exclusive_create_event_id;
        }
        self.add_transient_object_metadata(object)?;
        Ok(true)
    }

    fn ensure_routed_create_parent_current(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        let Some(parent_path) = crate::posix_path::parent_posix_path(&object.path) else {
            return Ok(None);
        };
        let parent_key = crate::posix_path::normalized_posix_path_key(&parent_path)
            .unwrap_or_else(|| parent_path.clone());
        let parent =
            self.authoritative_metadata_object_for_path(&object.file_system_id, &parent_key)?;
        match parent {
            Some(parent) if parent.kind == DfsObjectKind::Directory => Ok(Some(parent)),
            Some(parent) if parent.kind == DfsObjectKind::Tombstone => Err(DfsError::Distributed {
                message: format!("parent directory does not exist: {parent_path}"),
            }),
            Some(parent) => Err(DfsError::Distributed {
                message: format!(
                    "parent path {parent_path} is not a directory for routed create {}: {:?}",
                    object.path, parent.kind
                ),
            }),
            None => Err(DfsError::Distributed {
                message: format!("parent directory does not exist: {parent_path}"),
            }),
        }
    }

    fn touch_routed_create_parent_directory(&mut self, path: &str) {
        let _ = self.touch_parent_directory_modified(path);
    }

    pub(crate) fn drain_deferred_routed_create_parent_touches(&mut self) {
        if self.writes.deferred_routed_create_parent_touches.is_empty() {
            return;
        }
        let paths = self
            .writes
            .deferred_routed_create_parent_touches
            .iter()
            .take(DFS_DEFERRED_PARENT_TOUCH_DRAIN_LIMIT)
            .cloned()
            .collect::<Vec<_>>();
        for path in paths {
            self.writes
                .deferred_routed_create_parent_touches
                .remove(&path);
            self.touch_routed_create_parent_directory(&path);
        }
    }
}

/// The routed write reply for a durable body whose commit outcome is unknown.
fn distributed_write_commit_outcome_unknown(
    intent_id: &str,
    error: DfsError,
) -> Result<DfsDistributedWriteResult, DfsError> {
    match error {
        DfsError::MetadataCommitOutcomeUnknown {
            file_system_id,
            shard_id,
            epoch,
            operation_id,
            cause,
            current_primary_node_id,
        } => Ok(DfsDistributedWriteResult::CommitOutcomeUnknown {
            intent_id: intent_id.to_string(),
            file_system_id,
            shard_id,
            epoch,
            operation_id,
            cause,
            current_primary_node_id,
        }),
        error => Err(error),
    }
}

#[cfg(test)]
mod stable_reconciliation_tests {
    use super::*;
    use time::OffsetDateTime;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("node id")
    }

    fn object(value: &str) -> DfsObjectId {
        DfsObjectId::from_legacy_named(value).expect("object id")
    }

    fn committed_record(
        requester: &NodeId,
        object_id: &DfsObjectId,
        operation_id: &str,
        content_hash: &str,
    ) -> DfsWriteIntentRecord {
        DfsWriteIntentRecord {
            publication: Default::default(),
            intent_id: stable_write_intent_id(requester, object_id, operation_id),
            file_system_id: sifs_file_system_id(),
            object_id: object_id.clone(),
            base_version: None,
            target_version: crate::intent::stable_write_target_version(
                requester,
                object_id,
                operation_id,
            ),
            coordinator_node_id: requester.clone(),
            operation_id: None,
            operation_order_token: None,
            owner_epoch: 7,
            desired_replica_node_ids: vec![requester.clone()],
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: DfsWriteIntentStatus::Committed,
            content_hash: Some(content_hash.to_string()),
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        }
    }

    fn classify(
        operation_id: &str,
        requester: &NodeId,
        object_id: &DfsObjectId,
        expected_hash: &str,
        record: Option<&DfsWriteIntentRecord>,
        history_expired: bool,
        locally_authoritative: bool,
    ) -> DfsStableWriteOperationReconciliation {
        classify_stable_write_operation(
            operation_id,
            &stable_write_intent_id(requester, object_id, operation_id),
            &sifs_file_system_id(),
            object_id,
            &crate::intent::stable_write_target_version(requester, object_id, operation_id),
            requester,
            expected_hash,
            12,
            record,
            Some(12),
            history_expired,
            locally_authoritative,
        )
    }

    #[test]
    fn committed_exact_receipt_reconciles_applied_without_resend() {
        let requester = node("stable-reconcile-node");
        let object_id = object("stable-reconcile-object");
        let operation_id = "stable-write-17";
        let record = committed_record(&requester, &object_id, operation_id, "blake3:expected");

        let reconciled = classify(
            operation_id,
            &requester,
            &object_id,
            "blake3:expected",
            Some(&record),
            false,
            false,
        );
        let DfsStableWriteOperationReconciliation::Applied { receipt } = reconciled else {
            panic!("exact committed receipt must classify as applied");
        };
        assert_eq!(receipt.intent_id, record.intent_id);
        assert_eq!(receipt.target_version, record.target_version);
        assert_eq!(receipt.content_hash.as_deref(), Some("blake3:expected"));
    }

    #[test]
    fn file_history_retained_receipt_is_not_a_canonical_write_result() {
        let requester = node("retained-reconcile-node");
        let object_id = object("retained-reconcile-object");
        let operation_id = "retained-reconcile-operation";
        let mut record = committed_record(&requester, &object_id, operation_id, "blake3:expected");
        record.publication = DfsWritePublication::Retained {
            capture: Box::new(crate::DfsRetainedContentPublication {
                namespace: "module.dfs.history".into(),
                reference_id: "explicit-pin".into(),
                metadata: Some(crate::DfsModuleMetadataValue {
                    schema_version: 1,
                    payload: serde_json::to_value(crate::DfsFileVersionSource::LocalOperation {
                        operation_id: "local-operation".into(),
                        base_version: None,
                    })
                    .unwrap(),
                }),
            }),
        };
        assert!(matches!(
            classify(
                operation_id,
                &requester,
                &object_id,
                "blake3:expected",
                Some(&record),
                false,
                true
            ),
            DfsStableWriteOperationReconciliation::Conflict { .. }
        ));
    }

    #[test]
    fn same_write_id_with_different_content_reconciles_conflict() {
        let requester = node("stable-reconcile-digest-node");
        let object_id = object("stable-reconcile-digest-object");
        let operation_id = "stable-write-digest";
        let record = committed_record(&requester, &object_id, operation_id, "blake3:original");

        assert!(matches!(
            classify(
                operation_id,
                &requester,
                &object_id,
                "blake3:different",
                Some(&record),
                false,
                true,
            ),
            DfsStableWriteOperationReconciliation::Conflict { .. }
        ));
    }

    #[test]
    fn only_authoritative_history_complete_absence_is_not_applied() {
        let requester = node("stable-reconcile-absence-node");
        let object_id = object("stable-reconcile-absence-object");
        let operation_id = "stable-write-absence";

        assert_eq!(
            classify(
                operation_id,
                &requester,
                &object_id,
                "blake3:expected",
                None,
                false,
                true,
            ),
            DfsStableWriteOperationReconciliation::NotApplied
        );
        assert!(matches!(
            classify(
                operation_id,
                &requester,
                &object_id,
                "blake3:expected",
                None,
                false,
                false,
            ),
            DfsStableWriteOperationReconciliation::AmbiguousOrExpired { .. }
        ));
        assert!(matches!(
            classify(
                operation_id,
                &requester,
                &object_id,
                "blake3:expected",
                None,
                true,
                true,
            ),
            DfsStableWriteOperationReconciliation::AmbiguousOrExpired { .. }
        ));
    }
}
