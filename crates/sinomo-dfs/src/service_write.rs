//! Distributed write entrypoints and metadata-primary write routing.
//!
//! This module owns the high-level `NetworkedDfsService` write APIs for
//! replicated-eventual and replicated-confirmed objects, including patch writes,
//! reader-backed writes, self-only fast paths, and forwarding writes to the
//! current metadata-shard primary. Lower-level stage/fetch/repair message
//! handlers remain in the service protocol module until those transfer paths are
//! split together.

use std::collections::BTreeSet;
use std::io::Read;
use std::time::Instant;

use sinomo_core::events::EventId;
use sinomo_core::network::NetworkError;
use sinomo_core::node::NodeId;
use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};

use crate::transfer::{PendingNetworkedWrite, PendingNetworkedWritePatch};
use crate::*;

fn stale_write_base_error(
    object: &DfsObjectMetadata,
    expected_base_version: &DfsVersion,
    target_version: &DfsVersion,
) -> DfsError {
    DfsError::Distributed {
        message: format!(
            "stale DFS write base for {}: expected {}, current {}, command {}",
            object.object_id, expected_base_version, object.version, target_version
        ),
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn record_durable_write_intent_work(
        &mut self,
        intent: &DistributedWriteIntent,
        receipt_node_ids: &[NodeId],
    ) {
        let mut durable_nodes = self
            .coordinator
            .receipts_for_intent(&intent.intent_id)
            .into_iter()
            .filter(|receipt| receipt.state == DfsReplicaReceiptState::Durable)
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<BTreeSet<_>>();
        durable_nodes.extend(receipt_node_ids.iter().cloned());
        self.record_message_committed_work(DfsMessageCommittedWork {
            publish_metadata: true,
            repair_maintenance: durable_nodes.len() < intent.policy.desired_replicas,
            namespace_maintenance: false,
            metadata_replica_progress: false,
        });
    }

    fn persist_receipts_and_apply_ready_intent(
        &mut self,
        intent: &DistributedWriteIntent,
        base_object: &DfsObjectMetadata,
        content_hash: &str,
        content_size_bytes: u64,
        receipt_node_ids: &[NodeId],
        foreground_replica_repair: bool,
    ) -> Result<DistributedWriteIntent, DfsError> {
        // A replicated-eventual write with W=1 is authoritative as soon as its
        // first durable receipt exists. Persist that receipt, the committed
        // intent, and the new visible object/version in one backend batch. The
        // object/version pair remains atomic, while the missing N-W copies stay
        // represented by coordinator repair jobs. First publication uses the
        // dedicated parent-inclusive namespace batch in the same helper.
        let can_commit_in_one_batch = !foreground_replica_repair
            && intent.policy.min_commit_replicas <= 1
            && !receipt_node_ids.is_empty()
            && self.can_commit_ready_intent_batched(intent)?;

        if can_commit_in_one_batch {
            let mut updated_intent = intent.clone();
            for replica_node_id in receipt_node_ids {
                updated_intent = self
                    .coordinator
                    .record_durable_receipt(&updated_intent.intent_id, replica_node_id.clone())?;
            }
            self.commit_ready_intent_batched(
                &updated_intent,
                base_object,
                content_hash,
                content_size_bytes,
            )?;
            self.record_durable_write_intent_work(&updated_intent, receipt_node_ids);
            return Ok(updated_intent);
        }

        self.put_metadata_intent_and_receipts(intent, content_hash, receipt_node_ids)?;
        // Intent and receipt rows are durable before the in-memory transition
        // and visible-commit path below. A later failure must not discard the
        // authority's publication or missing-replica ownership.
        self.record_durable_write_intent_work(intent, receipt_node_ids);
        let mut updated_intent = intent.clone();
        for replica_node_id in receipt_node_ids {
            updated_intent = self
                .coordinator
                .record_durable_receipt(&updated_intent.intent_id, replica_node_id.clone())?;
        }
        self.try_apply_committed_intent_if_ready(&updated_intent.intent_id)?;
        Ok(updated_intent)
    }

    pub fn plan_distributed_eventual_replicas(
        &self,
        request: &DfsWriteRequest,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<DfsReplicaPlacementPlan, DfsError> {
        let candidates = candidate_nodes
            .into_iter()
            .map(|node_id| {
                let zone = policy.node_zones.get(&node_id).cloned();
                DfsReplicaCandidate::new(node_id).with_zone(zone)
            })
            .collect();
        Ok(plan_replicas(&DfsReplicaPlacementRequest {
            file_system_id: request.file_system_id.clone(),
            object_id: request.object_id.clone(),
            target_version: request.target_version.clone(),
            coordinator_node_id: self.node_id.clone(),
            policy,
            candidates,
        })?)
    }

    pub fn write_distributed_eventual_with_candidates(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidate_nodes, policy.clone())?;
        self.write_distributed_eventual(request, bytes, plan.desired_replica_nodes, policy)
    }

    pub(crate) fn write_distributed_eventual_with_candidates_and_parent_touch(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        foreground_replica_repair: bool,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidate_nodes, policy.clone())?;
        let object = self.current_object_for_write(&request.object_id)?;
        self.write_distributed_eventual_with_base_and_parent_touch_and_repair_mode(
            request,
            Some(object.version),
            bytes,
            plan.desired_replica_nodes,
            policy,
            true,
            true,
            foreground_replica_repair,
            false,
            true,
        )
    }

    pub fn write_distributed_eventual_from_reader_with_candidates<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        reader: R,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.write_distributed_eventual_from_reader_with_candidates_and_parent_touch(
            request,
            reader,
            candidate_nodes,
            policy,
            false,
            true,
        )
    }

    pub(crate) fn write_distributed_eventual_from_reader_with_candidates_and_parent_touch<
        R: Read,
    >(
        &mut self,
        request: DfsWriteRequest,
        reader: R,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        touch_parent_on_create: bool,
        foreground_replica_repair: bool,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidate_nodes, policy.clone())?;
        self.write_distributed_eventual_from_reader_and_parent_touch(
            request,
            reader,
            plan.desired_replica_nodes,
            policy,
            touch_parent_on_create,
            foreground_replica_repair,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_distributed_eventual_from_local_manifest_with_candidates_and_parent_touch(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        content_hash: String,
        content_size_bytes: u64,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        touch_parent_on_create: bool,
        foreground_replica_repair: bool,
        detach_request: bool,
        replay_response_journal: bool,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidate_nodes, policy.clone())?;
        let object = self.current_object_for_write(&request.object_id)?;
        if let Some(shard) = self.select_metadata_shard_for_object(&object)? {
            if shard.primary_node_id != self.node_id {
                return self.route_distributed_manifest_write_to_metadata_primary(
                    request,
                    expected_base_version,
                    content_hash,
                    content_size_bytes,
                    plan.desired_replica_nodes,
                    policy,
                    object,
                    shard,
                    touch_parent_on_create,
                    foreground_replica_repair,
                    detach_request,
                    replay_response_journal,
                );
            }
        }
        self.write_distributed_eventual_from_remote_manifest_with_base_and_parent_touch(
            request,
            expected_base_version,
            content_hash,
            content_size_bytes,
            plan.desired_replica_nodes,
            policy,
            touch_parent_on_create,
            false,
            foreground_replica_repair,
        )
    }

    pub fn write_replicated_confirmed(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.ensure_replicated_confirmed_object(&request.object_id)?;
        let policy = DfsReplicationPolicy::confirmed(desired_replica_nodes.len())?;
        self.write_distributed_eventual(request, bytes, desired_replica_nodes, policy)
    }

    pub fn write_replicated_confirmed_with_candidates(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
        candidate_nodes: Vec<NodeId>,
        desired_replicas: usize,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.ensure_replicated_confirmed_object(&request.object_id)?;
        let policy = DfsReplicationPolicy::confirmed(desired_replicas)?;
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidate_nodes, policy.clone())?;
        self.write_distributed_eventual(request, bytes, plan.desired_replica_nodes, policy)
    }

    pub fn write_distributed_eventual(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.write_distributed_eventual_with_repair_mode(
            request,
            bytes,
            desired_replica_nodes,
            policy,
            true,
        )
    }

    pub(crate) fn write_distributed_eventual_with_repair_mode(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        foreground_replica_repair: bool,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let object = self.current_object_for_write(&request.object_id)?;
        self.write_distributed_eventual_with_base_and_repair_mode(
            request,
            Some(object.version),
            bytes,
            desired_replica_nodes,
            policy,
            foreground_replica_repair,
        )
    }

    pub fn write_distributed_eventual_from_reader<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        reader: R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.write_distributed_eventual_from_reader_and_parent_touch(
            request,
            reader,
            desired_replica_nodes,
            policy,
            false,
            true,
        )
    }

    pub(crate) fn write_distributed_eventual_from_reader_and_parent_touch<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        reader: R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        touch_parent_on_create: bool,
        foreground_replica_repair: bool,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let object = self.current_object_for_write(&request.object_id)?;
        self.write_distributed_eventual_from_reader_with_base_and_parent_touch(
            request,
            Some(object.version),
            reader,
            desired_replica_nodes,
            policy,
            touch_parent_on_create,
            foreground_replica_repair,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_distributed_eventual_patch_with_base(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: DfsVersion,
        offset: u64,
        bytes: &[u8],
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        foreground_replica_repair: bool,
    ) -> Result<Option<NetworkedDfsWriteOutcome>, DfsError> {
        if let Some(outcome) = self.write_self_only_eventual_patch_with_base(
            request.clone(),
            expected_base_version.clone(),
            offset,
            bytes,
            desired_replica_nodes.clone(),
            policy.clone(),
        )? {
            return Ok(Some(outcome));
        }
        if bytes.is_empty() {
            return Ok(None);
        }

        let object = self.current_object_for_write(&request.object_id)?;
        if !matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) {
            return Ok(None);
        }
        if expected_base_version != object.version {
            return Ok(None);
        }
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        let desired_replica_nodes =
            self.normalize_desired_replica_nodes(&object, desired_replica_nodes, &policy);
        let policy = if desired_replica_nodes.len() > policy.desired_replicas {
            let desired_replicas = desired_replica_nodes.len();
            let min_commit_replicas = if object.policy == DfsObjectPolicy::ReplicatedConfirmed {
                desired_replicas
            } else {
                policy.min_commit_replicas.min(desired_replicas)
            };
            policy.with_counts(min_commit_replicas, desired_replicas)?
        } else {
            policy
        };
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        let owner_epoch = match self.select_metadata_shard_for_object(&object)? {
            Some(shard) if shard.primary_node_id != self.node_id => return Ok(None),
            Some(shard) => shard.epoch,
            None if self.control_plane.is_none() => 0,
            None => {
                return Err(crate::service_replica::missing_metadata_shard_assignment(
                    &object,
                ));
            }
        };

        if self
            .ensure_no_backend_pending_write_intent(
                &request.object_id,
                &request.target_version,
                None,
            )
            .is_err()
        {
            return Ok(None);
        }
        if self
            .coordinator
            .validate_new_intent(
                &request.file_system_id,
                &request.object_id,
                Some(&expected_base_version),
                &request.target_version,
                &desired_replica_nodes,
                &policy,
            )
            .is_err()
        {
            return Ok(None);
        }

        let base_identity = object_content_identity(&object);
        let patch_info = match self
            .content
            .put_object_version_patch_with_info_and_base_identity(
                request.object_id.as_str(),
                expected_base_version.as_str(),
                request.target_version.as_str(),
                offset,
                bytes,
                base_identity.as_ref(),
            ) {
            Ok(patch_info) => patch_info,
            Err(_) => return Ok(None),
        };
        let manifest = patch_info.manifest;
        let Some(patch_hash) = patch_info.patch_hash.clone() else {
            return Ok(None);
        };
        let patch_size_bytes = patch_info.patch_size_bytes;
        let intent = match self.coordinator.create_intent_at_epoch(
            request.file_system_id.clone(),
            request.object_id.clone(),
            Some(expected_base_version.clone()),
            request.target_version.clone(),
            request.requesting_node_id.clone(),
            owner_epoch,
            desired_replica_nodes.clone(),
            policy,
        ) {
            Ok(intent) => intent,
            Err(error)
                if object.policy == DfsObjectPolicy::ReplicatedEventual
                    && distributed_error_is_stale_or_concurrent_write(&error) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        self.pending_writes.insert(
            intent.intent_id.clone(),
            PendingNetworkedWrite::with_patch(
                request.object_id.clone(),
                request.target_version.clone(),
                manifest.content_hash.clone(),
                manifest.size_bytes,
                Some(PendingNetworkedWritePatch {
                    base_version: expected_base_version.clone(),
                    offset,
                    patch_hash: patch_hash.clone(),
                    patch_size_bytes,
                    full_stage_fallback_nodes: BTreeSet::new(),
                }),
            )
            .with_foreground_replica_repair(foreground_replica_repair),
        );

        let receipt_node_ids = desired_replica_nodes
            .contains(&self.node_id)
            .then(|| vec![self.node_id.clone()])
            .unwrap_or_default();
        let updated_intent = self.persist_receipts_and_apply_ready_intent(
            &intent,
            &object,
            &manifest.content_hash,
            manifest.size_bytes,
            &receipt_node_ids,
            foreground_replica_repair,
        )?;

        if self.should_stage_missing_replicas_after_write(&updated_intent.intent_id) {
            self.stage_missing_desired_replicas_for_intent(
                &updated_intent.intent_id,
                object.clone(),
            )?;
        }

        let outcome = self.write_outcome(&updated_intent.intent_id)?;
        self.cleanup_committed_pending_write_if_repaired(&updated_intent.intent_id);
        Ok(Some(outcome))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_distributed_eventual_patch_from_reader_with_base<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: DfsVersion,
        offset: u64,
        reader: &mut R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        foreground_replica_repair: bool,
    ) -> Result<Option<NetworkedDfsWriteOutcome>, DfsError> {
        if let Some(outcome) = self.write_self_only_eventual_patch_from_reader_with_base(
            request.clone(),
            expected_base_version.clone(),
            offset,
            reader,
            desired_replica_nodes.clone(),
            policy.clone(),
        )? {
            return Ok(Some(outcome));
        }

        let object = self.current_object_for_write(&request.object_id)?;
        if !matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) {
            return Ok(None);
        }
        if expected_base_version != object.version {
            return Ok(None);
        }
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        let desired_replica_nodes =
            self.normalize_desired_replica_nodes(&object, desired_replica_nodes, &policy);
        let policy = if desired_replica_nodes.len() > policy.desired_replicas {
            let desired_replicas = desired_replica_nodes.len();
            let min_commit_replicas = if object.policy == DfsObjectPolicy::ReplicatedConfirmed {
                desired_replicas
            } else {
                policy.min_commit_replicas.min(desired_replicas)
            };
            policy.with_counts(min_commit_replicas, desired_replicas)?
        } else {
            policy
        };
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        let owner_epoch = match self.select_metadata_shard_for_object(&object)? {
            Some(shard) if shard.primary_node_id != self.node_id => return Ok(None),
            Some(shard) => shard.epoch,
            None if self.control_plane.is_none() => 0,
            None => {
                return Err(crate::service_replica::missing_metadata_shard_assignment(
                    &object,
                ));
            }
        };

        if self
            .ensure_no_backend_pending_write_intent(
                &request.object_id,
                &request.target_version,
                None,
            )
            .is_err()
        {
            return Ok(None);
        }
        if self
            .coordinator
            .validate_new_intent(
                &request.file_system_id,
                &request.object_id,
                Some(&expected_base_version),
                &request.target_version,
                &desired_replica_nodes,
                &policy,
            )
            .is_err()
        {
            return Ok(None);
        }

        let base_identity = object_content_identity(&object);
        let patch_info = self
            .content
            .put_object_version_patch_from_reader_with_info_and_base_identity(
                request.object_id.as_str(),
                expected_base_version.as_str(),
                request.target_version.as_str(),
                offset,
                reader,
                base_identity.as_ref(),
            )?;
        let manifest = patch_info.manifest;
        let Some(patch_hash) = patch_info.patch_hash.clone() else {
            return Ok(None);
        };
        let patch_size_bytes = patch_info.patch_size_bytes;
        let intent = match self.coordinator.create_intent_at_epoch(
            request.file_system_id.clone(),
            request.object_id.clone(),
            Some(expected_base_version.clone()),
            request.target_version.clone(),
            request.requesting_node_id.clone(),
            owner_epoch,
            desired_replica_nodes.clone(),
            policy,
        ) {
            Ok(intent) => intent,
            Err(error)
                if object.policy == DfsObjectPolicy::ReplicatedEventual
                    && distributed_error_is_stale_or_concurrent_write(&error) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        self.pending_writes.insert(
            intent.intent_id.clone(),
            PendingNetworkedWrite::with_patch(
                request.object_id.clone(),
                request.target_version.clone(),
                manifest.content_hash.clone(),
                manifest.size_bytes,
                Some(PendingNetworkedWritePatch {
                    base_version: expected_base_version.clone(),
                    offset,
                    patch_hash: patch_hash.clone(),
                    patch_size_bytes,
                    full_stage_fallback_nodes: BTreeSet::new(),
                }),
            )
            .with_foreground_replica_repair(foreground_replica_repair),
        );

        let receipt_node_ids = desired_replica_nodes
            .contains(&self.node_id)
            .then(|| vec![self.node_id.clone()])
            .unwrap_or_default();
        let updated_intent = self.persist_receipts_and_apply_ready_intent(
            &intent,
            &object,
            &manifest.content_hash,
            manifest.size_bytes,
            &receipt_node_ids,
            foreground_replica_repair,
        )?;

        if self.should_stage_missing_replicas_after_write(&updated_intent.intent_id) {
            self.stage_missing_desired_replicas_for_intent(
                &updated_intent.intent_id,
                object.clone(),
            )?;
        }

        let outcome = self.write_outcome(&updated_intent.intent_id)?;
        self.cleanup_committed_pending_write_if_repaired(&updated_intent.intent_id);
        Ok(Some(outcome))
    }

    pub(crate) fn write_self_only_eventual_patch_with_base(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: DfsVersion,
        offset: u64,
        bytes: &[u8],
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<Option<NetworkedDfsWriteOutcome>, DfsError> {
        if bytes.is_empty() || self.control_plane.is_some() {
            return Ok(None);
        }
        let object = self.current_object_for_write(&request.object_id)?;
        if object.policy != DfsObjectPolicy::ReplicatedEventual {
            return Ok(None);
        }
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        let desired_replica_nodes =
            self.normalize_desired_replica_nodes(&object, desired_replica_nodes, &policy);
        let policy = if desired_replica_nodes.len() > policy.desired_replicas {
            let desired_replicas = desired_replica_nodes.len();
            policy.with_counts(
                policy.min_commit_replicas.min(desired_replicas),
                desired_replicas,
            )?
        } else {
            policy
        };
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        if policy.min_commit_replicas != 1
            || desired_replica_nodes.len() != 1
            || desired_replica_nodes[0] != self.node_id
            || object.owner_node_id != self.node_id
        {
            return Ok(None);
        }
        if self
            .select_metadata_shard_for_object(&object)?
            .is_some_and(|shard| shard.primary_node_id != self.node_id)
        {
            return Ok(None);
        }
        if expected_base_version != object.version {
            return Ok(None);
        }
        if self
            .ensure_no_backend_pending_write_intent(
                &request.object_id,
                &request.target_version,
                None,
            )
            .is_err()
        {
            return Ok(None);
        }
        if self
            .coordinator
            .validate_new_intent(
                &request.file_system_id,
                &request.object_id,
                Some(&expected_base_version),
                &request.target_version,
                &desired_replica_nodes,
                &policy,
            )
            .is_err()
        {
            return Ok(None);
        }

        let base_identity = object_content_identity(&object);
        let manifest = match self
            .content
            .put_object_version_patch_with_info_and_base_identity(
                request.object_id.as_str(),
                expected_base_version.as_str(),
                request.target_version.as_str(),
                offset,
                bytes,
                base_identity.as_ref(),
            ) {
            Ok(patch_info) => patch_info.manifest,
            Err(_) => return Ok(None),
        };
        self.commit_self_only_eventual_patch_manifest(request, object, manifest)
    }

    pub(crate) fn write_self_only_eventual_patch_from_reader_with_base<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: DfsVersion,
        offset: u64,
        reader: &mut R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<Option<NetworkedDfsWriteOutcome>, DfsError> {
        if self.control_plane.is_some() {
            return Ok(None);
        }
        let object = self.current_object_for_write(&request.object_id)?;
        if object.policy != DfsObjectPolicy::ReplicatedEventual {
            return Ok(None);
        }
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        let desired_replica_nodes =
            self.normalize_desired_replica_nodes(&object, desired_replica_nodes, &policy);
        let policy = if desired_replica_nodes.len() > policy.desired_replicas {
            let desired_replicas = desired_replica_nodes.len();
            policy.with_counts(
                policy.min_commit_replicas.min(desired_replicas),
                desired_replicas,
            )?
        } else {
            policy
        };
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        if policy.min_commit_replicas != 1
            || desired_replica_nodes.len() != 1
            || desired_replica_nodes[0] != self.node_id
            || object.owner_node_id != self.node_id
        {
            return Ok(None);
        }
        if self
            .select_metadata_shard_for_object(&object)?
            .is_some_and(|shard| shard.primary_node_id != self.node_id)
        {
            return Ok(None);
        }
        if expected_base_version != object.version {
            return Ok(None);
        }
        if self
            .ensure_no_backend_pending_write_intent(
                &request.object_id,
                &request.target_version,
                None,
            )
            .is_err()
        {
            return Ok(None);
        }
        if self
            .coordinator
            .validate_new_intent(
                &request.file_system_id,
                &request.object_id,
                Some(&expected_base_version),
                &request.target_version,
                &desired_replica_nodes,
                &policy,
            )
            .is_err()
        {
            return Ok(None);
        }

        let base_identity = object_content_identity(&object);
        let manifest = self
            .content
            .put_object_version_patch_from_reader_with_base_identity(
                request.object_id.as_str(),
                expected_base_version.as_str(),
                request.target_version.as_str(),
                offset,
                reader,
                base_identity.as_ref(),
            )?;
        self.commit_self_only_eventual_patch_manifest(request, object, manifest)
    }

    pub(crate) fn commit_self_only_eventual_patch_manifest(
        &mut self,
        request: DfsWriteRequest,
        object: DfsObjectMetadata,
        manifest: ContentManifest,
    ) -> Result<Option<NetworkedDfsWriteOutcome>, DfsError> {
        if self.control_plane.is_some() {
            return Err(DfsError::Distributed {
                message: "clustered self-only writes require a canonical metadata prepare"
                    .to_string(),
            });
        }
        let mut committed_object = object.clone();
        let write_time = dfs_now_unix_nanos();
        committed_object.version = request.target_version.clone();
        committed_object.content_hash = Some(manifest.content_hash.clone());
        committed_object.content_size_bytes = Some(manifest.size_bytes);
        committed_object.modified_at_unix_nanos = write_time;
        committed_object.changed_at_unix_nanos = write_time;
        committed_object.current_locations = vec![self.node_id.clone()];
        committed_object.last_modified_by_event_id = Some(EventId::generate());
        self.put_visible_object_and_version_in_metadata_backend(
            &committed_object,
            Some(manifest.size_bytes),
        )?;
        self.coordinator.set_committed_version(
            committed_object.file_system_id.clone(),
            committed_object.object_id.clone(),
            committed_object.version.clone(),
        );
        self.track_metadata_tombstone(&committed_object);
        self.metadata.add_object(committed_object)?;
        Ok(Some(NetworkedDfsWriteOutcome::Committed {
            intent_id: format!(
                "local-eventual-patch:{}:{}",
                request.object_id, request.target_version
            ),
            version: request.target_version,
            content_hash: manifest.content_hash,
            repair_jobs: Vec::new(),
        }))
    }

    pub fn write_distributed_eventual_from_reader_with_base<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        reader: R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.write_distributed_eventual_from_reader_with_base_and_parent_touch(
            request,
            expected_base_version,
            reader,
            desired_replica_nodes,
            policy,
            false,
            true,
        )
    }

    pub(crate) fn write_distributed_eventual_from_reader_with_base_and_parent_touch<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        reader: R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        touch_parent_on_create: bool,
        foreground_replica_repair: bool,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let object = self.current_object_for_write(&request.object_id)?;
        let transient_create = self
            .transient_metadata_object_ids
            .contains(&request.object_id);
        let touch_parent_on_create = touch_parent_on_create && transient_create;
        let create_predecessor_object_id = transient_create
            .then(|| {
                self.transient_metadata_predecessor_object_ids
                    .get(&request.object_id)
                    .cloned()
            })
            .flatten();
        if !matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy,
            });
        }
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        let desired_replica_nodes =
            self.normalize_desired_replica_nodes(&object, desired_replica_nodes, &policy);
        let policy = if desired_replica_nodes.len() > policy.desired_replicas {
            let desired_replicas = desired_replica_nodes.len();
            let min_commit_replicas = if object.policy == DfsObjectPolicy::ReplicatedConfirmed {
                desired_replicas
            } else {
                policy.min_commit_replicas.min(desired_replicas)
            };
            policy.with_counts(min_commit_replicas, desired_replicas)?
        } else {
            policy
        };
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        let owner_epoch = match self.select_metadata_shard_for_object(&object)? {
            Some(shard) if shard.primary_node_id != self.node_id => {
                return self.route_distributed_write_from_reader_to_metadata_primary(
                    request,
                    expected_base_version,
                    reader,
                    desired_replica_nodes,
                    policy,
                    object,
                    shard,
                    touch_parent_on_create,
                    foreground_replica_repair,
                );
            }
            Some(shard) => shard.epoch,
            None if self.control_plane.is_none() => 0,
            None => {
                return Err(crate::service_replica::missing_metadata_shard_assignment(
                    &object,
                ));
            }
        };

        if expected_base_version
            .as_ref()
            .is_some_and(|expected| expected != &object.version)
        {
            let expected = expected_base_version
                .as_ref()
                .expect("checked expected base");
            return Err(stale_write_base_error(
                &object,
                expected,
                &request.target_version,
            ));
        }

        if let Err(error) = self.ensure_no_backend_pending_write_intent(
            &request.object_id,
            &request.target_version,
            None,
        ) {
            return Err(error.into());
        }

        if let Err(error) = self.coordinator.validate_new_intent(
            &request.file_system_id,
            &request.object_id,
            expected_base_version.as_ref(),
            &request.target_version,
            &desired_replica_nodes,
            &policy,
        ) {
            return Err(error.into());
        }

        let manifest = self.content.put_object_version_from_reader(
            request.object_id.as_str(),
            request.target_version.as_str(),
            reader,
        )?;

        if self.control_plane.is_none()
            && !touch_parent_on_create
            && object.policy == DfsObjectPolicy::ReplicatedEventual
            && policy.min_commit_replicas == 1
            && desired_replica_nodes.len() == 1
            && desired_replica_nodes[0] == self.node_id
            && object.owner_node_id == self.node_id
        {
            let mut committed_object = object.clone();
            let write_time = dfs_now_unix_nanos();
            committed_object.version = request.target_version.clone();
            committed_object.content_hash = Some(manifest.content_hash.clone());
            committed_object.content_size_bytes = Some(manifest.size_bytes);
            committed_object.modified_at_unix_nanos = write_time;
            committed_object.changed_at_unix_nanos = write_time;
            committed_object.current_locations = vec![self.node_id.clone()];
            committed_object.last_modified_by_event_id = Some(EventId::generate());
            self.put_visible_object_and_version_in_metadata_backend(
                &committed_object,
                Some(manifest.size_bytes),
            )?;
            self.record_message_committed_work(DfsMessageCommittedWork {
                publish_metadata: true,
                ..DfsMessageCommittedWork::default()
            });
            self.coordinator.set_committed_version(
                committed_object.file_system_id.clone(),
                committed_object.object_id.clone(),
                committed_object.version.clone(),
            );
            self.track_metadata_tombstone(&committed_object);
            self.metadata.add_object(committed_object)?;
            return Ok(NetworkedDfsWriteOutcome::Committed {
                intent_id: format!(
                    "local-eventual:{}:{}",
                    request.object_id, request.target_version
                ),
                version: request.target_version,
                content_hash: manifest.content_hash,
                repair_jobs: Vec::new(),
            });
        }

        let intent = match self.coordinator.create_intent_at_epoch_with_parent_touch(
            request.file_system_id.clone(),
            request.object_id.clone(),
            expected_base_version.clone(),
            request.target_version.clone(),
            request.requesting_node_id.clone(),
            owner_epoch,
            desired_replica_nodes.clone(),
            policy,
            touch_parent_on_create,
            create_predecessor_object_id,
        ) {
            Ok(intent) => intent,
            Err(error)
                if object.policy == DfsObjectPolicy::ReplicatedEventual
                    && distributed_error_is_stale_or_concurrent_write(&error) =>
            {
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        };
        self.pending_writes.insert(
            intent.intent_id.clone(),
            PendingNetworkedWrite::new(
                request.object_id.clone(),
                request.target_version.clone(),
                manifest.content_hash.clone(),
                manifest.size_bytes,
            )
            .with_foreground_replica_repair(foreground_replica_repair),
        );

        let receipt_node_ids = desired_replica_nodes
            .contains(&self.node_id)
            .then(|| vec![self.node_id.clone()])
            .unwrap_or_default();
        let updated_intent = self.persist_receipts_and_apply_ready_intent(
            &intent,
            &object,
            &manifest.content_hash,
            manifest.size_bytes,
            &receipt_node_ids,
            foreground_replica_repair,
        )?;

        if self.should_stage_missing_replicas_after_write(&updated_intent.intent_id) {
            self.stage_missing_desired_replicas_for_intent(
                &updated_intent.intent_id,
                object.clone(),
            )?;
        }

        let outcome = self.write_outcome(&updated_intent.intent_id)?;
        self.cleanup_committed_pending_write_if_repaired(&updated_intent.intent_id);
        Ok(outcome)
    }

    #[cfg(test)]
    pub(crate) fn write_distributed_eventual_from_remote_manifest_with_base(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        content_hash: String,
        content_size_bytes: u64,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        foreground_replica_repair: bool,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.write_distributed_eventual_from_remote_manifest_with_base_and_parent_touch(
            request,
            expected_base_version,
            content_hash,
            content_size_bytes,
            desired_replica_nodes,
            policy,
            false,
            false,
            foreground_replica_repair,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_distributed_eventual_from_remote_manifest_with_base_and_parent_touch(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        content_hash: String,
        content_size_bytes: u64,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        touch_parent_on_create: bool,
        _exclusive_create: bool,
        foreground_replica_repair: bool,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let trace_started = Instant::now();
        let trace_object_id = request.object_id.clone();
        let trace_version = request.target_version.clone();
        let trace_requester = request.requesting_node_id.clone();
        let trace_desired_count = desired_replica_nodes.len();
        let trace_policy_min = policy.min_commit_replicas;
        let trace_policy_desired = policy.desired_replicas;
        let trace_content_size_bytes = content_size_bytes;
        let object = self.current_object_for_write(&request.object_id)?;
        let transient_create = self
            .transient_metadata_object_ids
            .contains(&request.object_id);
        let touch_parent_on_create = touch_parent_on_create && transient_create;
        let create_predecessor_object_id = transient_create
            .then(|| {
                self.transient_metadata_predecessor_object_ids
                    .get(&request.object_id)
                    .cloned()
            })
            .flatten();
        if !matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy,
            });
        }
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        let desired_replica_nodes =
            self.normalize_desired_replica_nodes(&object, desired_replica_nodes, &policy);
        if !desired_replica_nodes.contains(&request.requesting_node_id) {
            return Err(DfsError::Distributed {
                message: format!(
                    "reader-backed routed write for {} must include requesting node {} as a desired replica",
                    request.object_id, request.requesting_node_id
                ),
            });
        }
        let policy = if desired_replica_nodes.len() > policy.desired_replicas {
            let desired_replicas = desired_replica_nodes.len();
            let min_commit_replicas = if object.policy == DfsObjectPolicy::ReplicatedConfirmed {
                desired_replicas
            } else {
                policy.min_commit_replicas.min(desired_replicas)
            };
            policy.with_counts(min_commit_replicas, desired_replicas)?
        } else {
            policy
        };
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        // Creating the in-memory intent is not the visible linearization
        // point. The commit path certifies this hinted epoch through a fresh
        // operation-bound full-shard acknowledgement immediately before
        // atomic publication, using cluster quorum only as the partition
        // fallback. A second barrier here only adds a WAN round trip (most
        // visibly for first publication of a newly created file).
        let owner_epoch = self.local_metadata_owner_epoch_hint_for_object(&object)?;

        if expected_base_version
            .as_ref()
            .is_some_and(|expected| expected != &object.version)
        {
            let expected = expected_base_version
                .as_ref()
                .expect("checked expected base");
            return Err(stale_write_base_error(
                &object,
                expected,
                &request.target_version,
            ));
        }

        if let Err(error) = self.ensure_no_backend_pending_write_intent(
            &request.object_id,
            &request.target_version,
            None,
        ) {
            return Err(error.into());
        }

        if let Err(error) = self.coordinator.validate_new_intent(
            &request.file_system_id,
            &request.object_id,
            expected_base_version.as_ref(),
            &request.target_version,
            &desired_replica_nodes,
            &policy,
        ) {
            return Err(error.into());
        }

        let create_intent_started = Instant::now();
        let intent = match self.coordinator.create_intent_at_epoch_with_parent_touch(
            request.file_system_id.clone(),
            request.object_id.clone(),
            expected_base_version.clone(),
            request.target_version.clone(),
            request.requesting_node_id.clone(),
            owner_epoch,
            desired_replica_nodes.clone(),
            policy,
            touch_parent_on_create,
            create_predecessor_object_id,
        ) {
            Ok(intent) => intent,
            Err(error)
                if object.policy == DfsObjectPolicy::ReplicatedEventual
                    && distributed_error_is_stale_or_concurrent_write(&error) =>
            {
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        };
        let create_intent_elapsed = create_intent_started.elapsed();
        let put_intent_started = Instant::now();
        let put_intent_elapsed = put_intent_started.elapsed();
        self.pending_writes.insert(
            intent.intent_id.clone(),
            PendingNetworkedWrite::new(
                request.object_id.clone(),
                request.target_version.clone(),
                content_hash.clone(),
                content_size_bytes,
            )
            .with_foreground_replica_repair(foreground_replica_repair),
        );

        let record_receipt_started = Instant::now();
        let updated_intent = self
            .coordinator
            .record_durable_receipt(&intent.intent_id, request.requesting_node_id.clone())?;
        let record_receipt_elapsed = record_receipt_started.elapsed();
        let commit_started = Instant::now();
        let receipt_node_ids = vec![request.requesting_node_id.clone()];
        if !foreground_replica_repair
            && updated_intent.policy.min_commit_replicas <= 1
            && self.can_commit_ready_intent_batched(&updated_intent)?
        {
            self.commit_ready_intent_batched(
                &updated_intent,
                &object,
                &content_hash,
                content_size_bytes,
            )?;
            self.record_durable_write_intent_work(&updated_intent, &receipt_node_ids);
        } else {
            self.put_metadata_intent_and_receipts(&intent, &content_hash, &receipt_node_ids)?;
            self.record_durable_write_intent_work(&intent, &receipt_node_ids);
            self.try_apply_committed_intent_if_ready(&updated_intent.intent_id)?;
        }
        let commit_elapsed = commit_started.elapsed();

        let stage_started = Instant::now();
        let is_committed = matches!(
            self.coordinator
                .intent(&updated_intent.intent_id)
                .map(|intent| &intent.state),
            Some(DfsWriteIntentState::Committed)
        );
        let local_desired_copy_missing = desired_replica_nodes.contains(&self.node_id)
            && self
                .local_object_version_manifest_size_if_matches(
                    &request.object_id,
                    &request.target_version,
                    &content_hash,
                )
                .is_none();
        if let Some(pending) = self.pending_writes.get(&updated_intent.intent_id).cloned() {
            if !is_committed {
                if self.open_pending_write_blob_stream(&pending).is_ok() {
                    self.stage_missing_desired_replicas_for_intent(
                        &updated_intent.intent_id,
                        object.clone(),
                    )?;
                } else {
                    let _ = self.request_stage_blob_from_alternate(&updated_intent, &pending)?;
                }
            } else if foreground_replica_repair && local_desired_copy_missing {
                let _ = self.request_stage_blob_from_alternate(&updated_intent, &pending)?;
            }
        }
        let stage_elapsed = stage_started.elapsed();

        let outcome = self.write_outcome(&updated_intent.intent_id)?;
        dfs_perf_trace(format_args!(
            "distributed-write-remote-manifest-apply node={} requester={} object={} version={} bytes={} desired={} policy_min={} policy_desired={} foreground_repair={} outcome={} create_intent_ms={} put_intent_receipts_ms={} record_receipt_ms={} commit_ms={} stage_ms={} total_ms={}",
            self.node_id,
            trace_requester,
            trace_object_id,
            trace_version,
            trace_content_size_bytes,
            trace_desired_count,
            trace_policy_min,
            trace_policy_desired,
            foreground_replica_repair,
            match &outcome {
                NetworkedDfsWriteOutcome::Pending { .. } => "pending",
                NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } => {
                    if repair_jobs.is_empty() {
                        "committed"
                    } else {
                        "committed-with-repair"
                    }
                }
                NetworkedDfsWriteOutcome::Conflict { .. } => "conflict",
            },
            create_intent_elapsed.as_millis(),
            put_intent_elapsed.as_millis(),
            record_receipt_elapsed.as_millis(),
            commit_elapsed.as_millis(),
            stage_elapsed.as_millis(),
            trace_started.elapsed().as_millis()
        ));
        self.cleanup_committed_pending_write_if_repaired(&updated_intent.intent_id);
        Ok(outcome)
    }

    pub fn write_distributed_eventual_with_base(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.write_distributed_eventual_with_base_and_repair_mode(
            request,
            expected_base_version,
            bytes,
            desired_replica_nodes,
            policy,
            true,
        )
    }

    pub(crate) fn write_distributed_eventual_with_base_and_repair_mode(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        foreground_replica_repair: bool,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.write_distributed_eventual_with_base_and_parent_touch_and_repair_mode(
            request,
            expected_base_version,
            bytes,
            desired_replica_nodes,
            policy,
            false,
            false,
            foreground_replica_repair,
            false,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_distributed_eventual_with_base_and_parent_touch_and_repair_mode(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        touch_parent_on_create: bool,
        exclusive_create: bool,
        foreground_replica_repair: bool,
        detach_remote_primary_request: bool,
        replay_response_journal: bool,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let object = self.current_object_for_write(&request.object_id)?;
        let transient_create = self
            .transient_metadata_object_ids
            .contains(&request.object_id);
        let touch_parent_on_create = touch_parent_on_create && transient_create;
        let create_predecessor_object_id = transient_create
            .then(|| {
                self.transient_metadata_predecessor_object_ids
                    .get(&request.object_id)
                    .cloned()
            })
            .flatten();
        if !matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy,
            });
        }
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        let desired_replica_nodes =
            self.normalize_desired_replica_nodes(&object, desired_replica_nodes, &policy);
        let policy = if desired_replica_nodes.len() > policy.desired_replicas {
            let desired_replicas = desired_replica_nodes.len();
            // Replicated-confirmed requires every desired replica to confirm before
            // commit. When normalization widens the replica set (e.g. by injecting
            // the owner node), the commit threshold must grow with it; otherwise the
            // write would commit before all desired replicas are durable and silently
            // degrade to eventual semantics.
            let min_commit_replicas = if object.policy == DfsObjectPolicy::ReplicatedConfirmed {
                desired_replicas
            } else {
                policy.min_commit_replicas.min(desired_replicas)
            };
            policy.with_counts(min_commit_replicas, desired_replicas)?
        } else {
            policy
        };
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        let owner_epoch = match self.select_metadata_shard_for_object(&object)? {
            Some(shard) if shard.primary_node_id != self.node_id => {
                return self.route_distributed_write_to_metadata_primary(
                    request,
                    expected_base_version,
                    bytes,
                    desired_replica_nodes,
                    policy,
                    object,
                    shard,
                    touch_parent_on_create,
                    exclusive_create,
                    foreground_replica_repair,
                    detach_remote_primary_request,
                    replay_response_journal,
                );
            }
            Some(shard) => shard.epoch,
            None if self.control_plane.is_none() => 0,
            None => {
                return Err(crate::service_replica::missing_metadata_shard_assignment(
                    &object,
                ));
            }
        };

        if expected_base_version
            .as_ref()
            .is_some_and(|expected| expected != &object.version)
        {
            let expected = expected_base_version
                .as_ref()
                .expect("checked expected base");
            return Err(stale_write_base_error(
                &object,
                expected,
                &request.target_version,
            ));
        }

        if let Err(error) = self.ensure_no_backend_pending_write_intent(
            &request.object_id,
            &request.target_version,
            None,
        ) {
            return Err(error.into());
        }

        if let Err(error) = self.coordinator.validate_new_intent(
            &request.file_system_id,
            &request.object_id,
            expected_base_version.as_ref(),
            &request.target_version,
            &desired_replica_nodes,
            &policy,
        ) {
            return Err(error.into());
        }

        if self.control_plane.is_none()
            && !touch_parent_on_create
            && object.policy == DfsObjectPolicy::ReplicatedEventual
            && policy.min_commit_replicas == 1
            && desired_replica_nodes.len() == 1
            && desired_replica_nodes[0] == self.node_id
            && object.owner_node_id == self.node_id
        {
            let mut committed_object = object.clone();
            let manifest = self.content.put_object_version(
                request.object_id.as_str(),
                request.target_version.as_str(),
                &bytes,
            )?;
            let write_time = dfs_now_unix_nanos();
            committed_object.version = request.target_version.clone();
            committed_object.content_hash = Some(manifest.content_hash.clone());
            committed_object.content_size_bytes = Some(manifest.size_bytes);
            committed_object.modified_at_unix_nanos = write_time;
            committed_object.changed_at_unix_nanos = write_time;
            committed_object.current_locations = vec![self.node_id.clone()];
            committed_object.last_modified_by_event_id = Some(EventId::generate());
            self.put_visible_object_and_version_in_metadata_backend(
                &committed_object,
                Some(manifest.size_bytes),
            )?;
            self.record_message_committed_work(DfsMessageCommittedWork {
                publish_metadata: true,
                ..DfsMessageCommittedWork::default()
            });
            self.coordinator.set_committed_version(
                committed_object.file_system_id.clone(),
                committed_object.object_id.clone(),
                committed_object.version.clone(),
            );
            self.track_metadata_tombstone(&committed_object);
            self.metadata.add_object(committed_object)?;
            return Ok(NetworkedDfsWriteOutcome::Committed {
                intent_id: format!(
                    "local-eventual:{}:{}",
                    request.object_id, request.target_version
                ),
                version: request.target_version,
                content_hash: manifest.content_hash,
                repair_jobs: Vec::new(),
            });
        }

        let trace_started = Instant::now();
        let trace_object_id = request.object_id.clone();
        let trace_version = request.target_version.clone();
        let trace_requester = request.requesting_node_id.clone();
        let trace_desired_count = desired_replica_nodes.len();
        let trace_policy_min = policy.min_commit_replicas;
        let trace_policy_desired = policy.desired_replicas;
        let content_started = Instant::now();
        let manifest = self.content.put_object_version(
            request.object_id.as_str(),
            request.target_version.as_str(),
            &bytes,
        )?;
        let content_elapsed = content_started.elapsed();
        let create_intent_started = Instant::now();
        let intent = match self.coordinator.create_intent_at_epoch_with_parent_touch(
            request.file_system_id.clone(),
            request.object_id.clone(),
            expected_base_version.clone(),
            request.target_version.clone(),
            request.requesting_node_id.clone(),
            owner_epoch,
            desired_replica_nodes.clone(),
            policy,
            touch_parent_on_create,
            create_predecessor_object_id,
        ) {
            Ok(intent) => intent,
            Err(error)
                if object.policy == DfsObjectPolicy::ReplicatedEventual
                    && distributed_error_is_stale_or_concurrent_write(&error) =>
            {
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        };
        let create_intent_elapsed = create_intent_started.elapsed();
        self.pending_writes.insert(
            intent.intent_id.clone(),
            PendingNetworkedWrite::new(
                request.object_id.clone(),
                request.target_version.clone(),
                manifest.content_hash.clone(),
                manifest.size_bytes,
            )
            .with_foreground_replica_repair(foreground_replica_repair),
        );

        let mut receipt_node_ids = Vec::new();
        if desired_replica_nodes.contains(&self.node_id) {
            receipt_node_ids.push(self.node_id.clone());
        }
        if request.requesting_node_id != self.node_id
            && desired_replica_nodes.contains(&request.requesting_node_id)
        {
            receipt_node_ids.push(request.requesting_node_id.clone());
        }
        let metadata_commit_started = Instant::now();
        let updated_intent = self.persist_receipts_and_apply_ready_intent(
            &intent,
            &object,
            &manifest.content_hash,
            manifest.size_bytes,
            &receipt_node_ids,
            foreground_replica_repair,
        )?;
        let metadata_commit_elapsed = metadata_commit_started.elapsed();

        let stage_started = Instant::now();
        if self.should_stage_missing_replicas_after_write(&updated_intent.intent_id) {
            self.stage_missing_desired_replicas_for_intent(
                &updated_intent.intent_id,
                object.clone(),
            )?;
        }
        let stage_elapsed = stage_started.elapsed();

        let outcome = self.write_outcome(&updated_intent.intent_id)?;
        dfs_perf_trace(format_args!(
            "distributed-write-apply node={} requester={} object={} version={} bytes={} desired={} policy_min={} policy_desired={} outcome={} content_ms={} create_intent_ms={} metadata_commit_ms={} stage_ms={} total_ms={}",
            self.node_id,
            trace_requester,
            trace_object_id,
            trace_version,
            manifest.size_bytes,
            trace_desired_count,
            trace_policy_min,
            trace_policy_desired,
            match &outcome {
                NetworkedDfsWriteOutcome::Pending { .. } => "pending",
                NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } => {
                    if repair_jobs.is_empty() {
                        "committed"
                    } else {
                        "committed-with-repair"
                    }
                }
                NetworkedDfsWriteOutcome::Conflict { .. } => "conflict",
            },
            content_elapsed.as_millis(),
            create_intent_elapsed.as_millis(),
            metadata_commit_elapsed.as_millis(),
            stage_elapsed.as_millis(),
            trace_started.elapsed().as_millis()
        ));
        self.cleanup_committed_pending_write_if_repaired(&updated_intent.intent_id);
        Ok(outcome)
    }

    fn metadata_primary_unroutable_error(
        shard: &DfsMetadataShardConfig,
        primary_node_id: &NodeId,
    ) -> DfsError {
        DfsError::Network {
            message: format!(
                "metadata shard {} primary {} is not routable",
                shard.shard_id, primary_node_id
            ),
        }
    }

    fn track_pending_routed_write(
        &mut self,
        request_id: String,
        object_id: DfsObjectId,
        primary_node_id: NodeId,
        authority_shard: DfsMetadataShardConfig,
        command: DfsDistributedWriteCommand,
    ) -> NetworkedDfsWriteOutcome {
        self.pending_routed_writes.insert(
            request_id.clone(),
            PendingRoutedWrite {
                object_id,
                primary_node_id,
                authority_shard,
                command,
                authority_redirects_remaining: DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT,
                remote_intent_id: None,
                durable_receipts: 0,
                required_receipts: 1,
                result: None,
            },
        );
        NetworkedDfsWriteOutcome::Pending {
            intent_id: request_id,
            durable_receipts: 0,
            required_receipts: 1,
        }
    }

    pub(crate) fn route_distributed_write_to_metadata_primary(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
        touch_parent_on_create: bool,
        exclusive_create: bool,
        foreground_replica_repair: bool,
        detach_request: bool,
        replay_response_journal: bool,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let request_id = self.next_request_id();
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        let metadata_journal_after_sequence = if !detach_request && replay_response_journal {
            self.local_metadata_journal_after_sequence_for_shard(&shard_id)?
        } else {
            None
        };
        let primary_node_id = shard.primary_node_id.clone();
        if request.requesting_node_id == self.node_id
            && desired_replica_nodes.contains(&self.node_id)
        {
            self.content.put_object_version_blob_only(
                request.object_id.as_str(),
                request.target_version.as_str(),
                &bytes,
            )?;
        }
        let command = DfsDistributedWriteCommand {
            request_id: request_id.clone(),
            request,
            expected_base_version,
            metadata_journal_after_sequence,
            bytes,
            content_hash: None,
            content_size_bytes: None,
            desired_replica_nodes,
            policy,
            object_metadata: Some(object.clone()),
            authority_shard: Some(shard.clone()),
            touch_parent_on_create,
            foreground_replica_repair,
            exclusive_create,
        };
        let message = if command.exclusive_create {
            DfsNetworkMessage::ExclusiveCreateRequest(command.clone())
        } else {
            DfsNetworkMessage::DistributedWriteRequest(command.clone())
        };
        let sent = if detach_request {
            self.send_dfs_message_if_routable_detached(&primary_node_id, message)?
        } else {
            self.send_dfs_message_if_routable(&primary_node_id, message)?
        };
        if !sent {
            return Err(Self::metadata_primary_unroutable_error(
                &shard,
                &primary_node_id,
            ));
        }
        Ok(self.track_pending_routed_write(
            request_id,
            object.object_id,
            primary_node_id,
            shard,
            command,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn route_distributed_manifest_write_to_metadata_primary(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        content_hash: String,
        content_size_bytes: u64,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
        touch_parent_on_create: bool,
        foreground_replica_repair: bool,
        detach_request: bool,
        replay_response_journal: bool,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let request_id = self.next_request_id();
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        let metadata_journal_after_sequence = if !detach_request && replay_response_journal {
            self.local_metadata_journal_after_sequence_for_shard(&shard_id)?
        } else {
            None
        };
        let primary_node_id = shard.primary_node_id.clone();
        let command = DfsDistributedWriteCommand {
            request_id: request_id.clone(),
            request,
            expected_base_version,
            metadata_journal_after_sequence,
            bytes: Vec::new(),
            content_hash: Some(content_hash),
            content_size_bytes: Some(content_size_bytes),
            desired_replica_nodes,
            policy,
            object_metadata: Some(object.clone()),
            authority_shard: Some(shard.clone()),
            touch_parent_on_create,
            foreground_replica_repair,
            exclusive_create: false,
        };
        let message = DfsNetworkMessage::DistributedWriteRequest(command.clone());
        let sent = if detach_request {
            self.send_dfs_message_if_routable_detached(&primary_node_id, message)?
        } else {
            self.send_dfs_message_if_routable(&primary_node_id, message)?
        };
        if !sent {
            return Err(Self::metadata_primary_unroutable_error(
                &shard,
                &primary_node_id,
            ));
        }
        Ok(self.track_pending_routed_write(
            request_id,
            object.object_id,
            primary_node_id,
            shard,
            command,
        ))
    }

    pub(crate) fn route_distributed_write_from_reader_to_metadata_primary<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        reader: R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
        touch_parent_on_create: bool,
        foreground_replica_repair: bool,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        if !desired_replica_nodes.contains(&request.requesting_node_id) {
            return Err(DfsError::Distributed {
                message: format!(
                    "reader-backed routed write for {} must include requesting node {} as a desired replica",
                    request.object_id, request.requesting_node_id
                ),
            });
        }
        let trace_started = Instant::now();
        let trace_object_id = request.object_id.clone();
        let trace_version = request.target_version.clone();
        let trace_requester = request.requesting_node_id.clone();
        let trace_desired_count = desired_replica_nodes.len();
        let trace_policy_min = policy.min_commit_replicas;
        let trace_policy_desired = policy.desired_replicas;
        let capture_started = Instant::now();
        let manifest = self.content.put_object_version_from_reader(
            request.object_id.as_str(),
            request.target_version.as_str(),
            reader,
        )?;
        let capture_elapsed = capture_started.elapsed();
        let request_id = self.next_request_id();
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        let metadata_sequence_started = Instant::now();
        let metadata_journal_after_sequence =
            self.local_metadata_journal_after_sequence_for_shard(&shard_id)?;
        let metadata_sequence_elapsed = metadata_sequence_started.elapsed();
        let primary_node_id = shard.primary_node_id.clone();
        let (content_hash, content_size_bytes) = if manifest.size_bytes == 0 {
            (None, None)
        } else {
            (
                Some(manifest.content_hash.clone()),
                Some(manifest.size_bytes),
            )
        };
        let command = DfsDistributedWriteCommand {
            request_id: request_id.clone(),
            request,
            expected_base_version,
            metadata_journal_after_sequence,
            bytes: Vec::new(),
            content_hash,
            content_size_bytes,
            desired_replica_nodes,
            policy,
            object_metadata: Some(object.clone()),
            authority_shard: Some(shard.clone()),
            touch_parent_on_create,
            foreground_replica_repair,
            exclusive_create: false,
        };
        let send_started = Instant::now();
        let message = DfsNetworkMessage::DistributedWriteRequest(command.clone());
        let sent = if manifest.size_bytes == 0 {
            self.send_dfs_message_if_routable(&primary_node_id, message)?
        } else {
            let (_, body_reader) = self.open_content_hash_stream(
                &command.request.object_id,
                &command.request.target_version,
                &manifest.content_hash,
                Some(manifest.size_bytes),
            )?;
            let routed = self.next_dfs_routed_message(&primary_node_id, message);
            match self.transport.send_routed_call(
                &primary_node_id,
                DfsRoutedCall::with_attachment(
                    routed,
                    DfsOutboundAttachment::from_reader(
                        body_reader,
                        manifest.size_bytes,
                        &manifest.content_hash,
                    ),
                ),
            ) {
                Ok(()) => true,
                Err(NetworkError::UnknownNode { .. }) => false,
                Err(error) => return Err(DfsError::from_network(error)),
            }
        };
        let send_elapsed = send_started.elapsed();
        dfs_perf_trace(format_args!(
            "routed-write-send node={} requester={} primary={} request={} object={} version={} bytes={} reader_attachment={} desired={} policy_min={} policy_desired={} capture_ms={} metadata_seq_ms={} send_ms={} total_ms={} sent={}",
            self.node_id,
            trace_requester,
            primary_node_id,
            request_id,
            trace_object_id,
            trace_version,
            manifest.size_bytes,
            manifest.size_bytes > 0,
            trace_desired_count,
            trace_policy_min,
            trace_policy_desired,
            capture_elapsed.as_millis(),
            metadata_sequence_elapsed.as_millis(),
            send_elapsed.as_millis(),
            trace_started.elapsed().as_millis(),
            sent
        ));
        if !sent {
            return Err(Self::metadata_primary_unroutable_error(
                &shard,
                &primary_node_id,
            ));
        }
        Ok(self.track_pending_routed_write(
            request_id,
            object.object_id,
            primary_node_id,
            shard,
            command,
        ))
    }
}
