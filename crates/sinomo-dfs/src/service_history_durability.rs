//! Fresh durability for legacy content whose original grant was lost.

use crate::*;
use sinomo_core::DfsObjectPolicy;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

fn grant_error(message: impl Into<String>) -> DfsError {
    DfsError::Metadata {
        message: message.into(),
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn establish_rule_capture_before_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        let roots = self.file_history_repair_roots_before_objects(
            [object],
            self.metadata.filesystems.get(&object.file_system_id),
        )?;
        self.establish_rule_capture_durability(&roots)
    }

    /// Copy exact immutable bytes before granting retention. Failed attempts
    /// leave only ordinary content copies/receipts, never a capture or a head.
    /// Once capture commits, its ordinary repair owner takes responsibility
    /// for the remaining N-W targets using this immutable plan.
    pub(crate) fn establish_file_history_durability(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
    ) -> Result<Box<DfsContentDurability>, DfsError> {
        let path = crate::metadata_keys::normalize_posix_path(&object.path);
        self.require_file_history_shard_support(&object.file_system_id, &path)?;
        let shard = self.certified_local_metadata_owner_shard_for_object(object)?;
        let fs = self
            .refresh_file_history_configuration(&object.file_system_id)?
            .ok_or_else(|| grant_error("legacy capture has no filesystem policy"))?;
        let candidates = self.placement_content_candidates()?;
        // This is the same fallback as an ordinary current-policy write: one
        // local candidate in standalone mode, otherwise up to two candidates.
        // Surviving content locations never determine a durability requirement.
        let resolved = crate::placement::resolve_placement_policy(
            &DfsFileSystemConfig::from(&fs),
            &path,
            object.policy.clone(),
            candidates.len().clamp(1, 2),
        )?;
        let node_zones = candidates
            .iter()
            .filter_map(|node| node.zone.clone().map(|zone| (node.node_id.clone(), zone)))
            .collect();
        let policy = resolved.policy.replication_policy(node_zones)?;
        let desired = if object.policy == DfsObjectPolicy::LocalOwned
            || resolved.policy.consistency == DfsObjectPolicy::LocalOwned
        {
            if object.owner_node_id != self.node_id
                || policy.min_commit_replicas != 1
                || policy.desired_replicas != 1
            {
                return Err(grant_error(
                    "local-owned capture requires its owner's W=1/N=1 policy",
                ));
            }
            vec![self.node_id.clone()]
        } else {
            plan_replicas(&DfsReplicaPlacementRequest {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                target_version: version.version.clone(),
                coordinator_node_id: self.node_id.clone(),
                policy: policy.clone(),
                candidates: candidates
                    .into_iter()
                    .map(|node| DfsReplicaCandidate::new(node.node_id).with_zone(node.zone))
                    .collect(),
            })?
            .desired_replica_nodes
        };
        crate::validate_replication_plan(&desired, &policy)?;
        let hash = version
            .content_hash
            .as_deref()
            .ok_or_else(|| grant_error("legacy capture has no content hash"))?;
        version
            .size_bytes
            .ok_or_else(|| grant_error("legacy capture has no content length"))?;
        let mut readable = version.clone();
        // The descriptor authorizes an immutable read only. It is not written
        // to metadata; rule provenance remains conditional on publication.
        readable.retained.get_or_insert_with(|| {
            Box::new(DfsRetainedFileVersion {
                object: Box::new(object.clone()),
                captured_at_unix_nanos: crate::metadata_records::dfs_now_unix_nanos(),
                source: DfsFileVersionSource::RetentionRule,
                policy_capture: None,
            })
        });
        let input = self.open_file_restore_reader(&DfsFileHistoryDescriptor {
            version: readable,
            current_object: object.clone(),
        })?;
        let blob = input.into_quarantined(&self.content)?;
        self.content.bind_quarantined_blob(
            object.object_id.as_str(),
            version.version.as_str(),
            blob,
        )?;

        let mut durable = BTreeSet::new();
        if desired.contains(&self.node_id) {
            durable.insert(self.node_id.clone());
        }
        let deadline = self
            .transport
            .operation_deadline()
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(30));
        for node in desired
            .iter()
            .filter(|node| *node != &self.node_id)
            .cloned()
            .collect::<Vec<_>>()
        {
            if durable.len() >= policy.min_commit_replicas {
                break;
            }
            if self.stage_history_grant_copy(&node, object, version, deadline)? {
                durable.insert(node);
            }
        }
        if durable.len() < policy.min_commit_replicas {
            return Err(grant_error(format!(
                "legacy capture current-policy durability requires {} receipts; established {}",
                policy.min_commit_replicas,
                durable.len(),
            )));
        }
        // Recheck author authority after network I/O. The subsequent capture
        // prepare carries these receipts through the normal metadata barrier.
        let _guard = shard.as_ref().map(|shard| {
            crate::service_replica::metadata_shard_publication_guard(
                &shard.file_system_id,
                &shard.shard_id,
            )
        });
        if let Some(shard) = &shard
            && !self.metadata_shard_assignment_still_exact(shard)?
        {
            return Err(grant_error(
                "legacy capture metadata authority changed while copying content",
            ));
        }
        for node in durable {
            self.ensure_metadata_receipt_accepts(&object.object_id, &version.version, &node, hash)?;
            let receipt = DfsReplicaReceiptRecord {
                receipt: sinomo_core::DfsWriteReceipt {
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    committed_version: version.version.clone(),
                    owner_node_id: self.node_id.clone(),
                    event_id: sinomo_core::events::EventId::generate(),
                },
                replica_node_id: node,
                content_hash: Some(hash.to_string()),
                received_at: OffsetDateTime::now_utc(),
            };
            self.metadata_backend
                .as_mut()
                .ok_or_else(|| grant_error("legacy capture requires durable metadata"))?
                .put_receipt_for_object(Some(object), &receipt)?;
        }
        self.record_message_committed_work(DfsMessageCommittedWork {
            publish_metadata: true,
            ..Default::default()
        });
        Ok(Box::new(DfsContentDurability {
            current_policy_granted_at_unix_nanos: Some(
                crate::metadata_records::dfs_now_unix_nanos(),
            ),
            source_version: version.version.clone(),
            coordinator_node_id: self.node_id.clone(),
            owner_epoch: shard.map(|shard| shard.epoch).unwrap_or(0),
            desired_replica_node_ids: desired,
            policy,
        }))
    }

    fn stage_history_grant_copy(
        &mut self,
        node: &NodeId,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
        deadline: Instant,
    ) -> Result<bool, DfsError> {
        if Instant::now() >= deadline {
            return Ok(false);
        }
        let hash = version
            .content_hash
            .as_deref()
            .expect("validated content identity");
        let size = version.size_bytes.expect("validated content length");
        let request_id = self.next_request_id();
        let intent_id = format!("history-grant:{request_id}");
        let authority_shard = self.command_authority_shard_for_object(Some(object))?;
        let (_, reader) =
            self.open_content_hash_stream(&object.object_id, &version.version, hash, Some(size))?;
        let outcome = self.send_best_effort_message_attachment_if_routable(
            node,
            DfsNetworkMessage::StageRetainedBlob(DfsStageBlobCommand {
                request_id: request_id.clone(),
                intent_id: intent_id.clone(),
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                expected_base_version: None,
                version: version.version.clone(),
                content_hash: hash.to_string(),
                bytes: Vec::new(),
                object_metadata: Some(object.clone()),
                authority_shard,
            }),
            DfsOutboundAttachment::from_reader(reader, size, hash),
        )?;
        if !outcome.sent {
            return Ok(false);
        }
        loop {
            let messages = self
                .transport
                .drain_incoming_messages()
                .map_err(DfsError::from_network)?;
            let mut acknowledged = None;
            let mut other = Vec::new();
            for message in messages {
                if let DfsNetworkMessage::StageBlobResponse(response) = &message.message
                    && response.request_id == request_id
                {
                    let exact = message.target_node_id == self.node_id
                        && message.source_node_id == *node
                        && response.replica_node_id == *node
                        && response.intent_id == intent_id
                        && response.file_system_id == object.file_system_id
                        && response.object_id == object.object_id
                        && response.version == version.version;
                    if exact {
                        acknowledged = Some(matches!(&response.result,
                            DfsBlobTransferResult::Durable { content_hash }
                            | DfsBlobTransferResult::Committed { content_hash } if content_hash == hash));
                    }
                } else {
                    other.push(message);
                }
            }
            self.process_routed_messages_with_options(other, false)?;
            if let Some(acknowledged) = acknowledged {
                return Ok(acknowledged);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(false);
            }
            self.transport
                .wait_for_incoming(remaining.min(Duration::from_millis(10)))
                .map_err(DfsError::from_network)?;
        }
    }

    /// Enrich outgoing generations without retaining them. The canonical
    /// publication remains the only operation that can create a rule capture.
    pub(crate) fn establish_rule_capture_durability(
        &mut self,
        roots: &BTreeSet<(DfsObjectId, DfsVersion)>,
    ) -> Result<(), DfsError> {
        for (object_id, generation) in roots {
            let Some(backend) = self.metadata_backend.as_ref() else {
                continue;
            };
            let Some(mut version) = backend.get_version(object_id, generation)? else {
                continue;
            };
            if version.content_durability.is_some() {
                continue;
            }
            let object = backend
                .get_object(object_id)?
                .ok_or_else(|| grant_error("legacy rule capture lost its object"))?;
            let recovered =
                crate::content_durability::recover_from_backend(&version, backend.as_ref())?;
            version.content_durability = Some(match recovered {
                Some(plan) => plan,
                None => self.establish_file_history_durability(&object, &version)?,
            });
            let shard = self.certified_local_metadata_owner_shard_for_object(&object)?;
            let _guard = shard.as_ref().map(|shard| {
                crate::service_replica::metadata_shard_publication_guard(
                    &shard.file_system_id,
                    &shard.shard_id,
                )
            });
            if let Some(shard) = &shard
                && !self.metadata_shard_assignment_still_exact(shard)?
            {
                return Err(grant_error(
                    "legacy rule capture metadata authority changed",
                ));
            }
            self.metadata_backend
                .as_mut()
                .expect("durable history backend")
                .apply_namespace_transaction(&DfsNamespaceTransaction {
                    transaction_id: format!("history-durability_{}", sinomo_core::Id::generate()),
                    file_system_id: version.file_system_id.clone(),
                    objects: Vec::new(),
                    versions: vec![version],
                    file_version_pins: None,
                    advisory_lock_states: Vec::new(),
                    created_at: OffsetDateTime::now_utc(),
                })?;
        }
        Ok(())
    }
}
