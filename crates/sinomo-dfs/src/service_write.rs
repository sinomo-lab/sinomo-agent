//! Distributed write entrypoints and metadata-primary write routing.
//!
//! This module owns the high-level `NetworkedDfsService` write APIs for
//! replicated-eventual and replicated-confirmed objects, including patch writes,
//! reader-backed writes, self-only fast paths, and forwarding writes to the
//! current metadata-shard primary. Stage, fetch, and repair message handlers
//! live in the focused transfer modules.

use std::collections::BTreeSet;
use std::io::Read;
use std::time::Instant;

use sinomo_core::events::EventId;
use sinomo_core::network::NetworkError;
use sinomo_core::node::NodeId;
use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};

use crate::intent::{stable_operation_order_token, stable_write_intent_id};
use crate::transfer::{PendingNetworkedWrite, PendingNetworkedWritePatch};
use crate::*;

/// Internal write execution choices. Content identity and the consumed input
/// remain explicit parameters of the byte, reader and manifest entry points.
#[derive(Clone, Debug, Default)]
pub(crate) struct DfsDistributedWriteOptions {
    pub(crate) publication: DfsWritePublication,
    pub(crate) touch_parent_on_create: bool,
    pub(crate) creation: DfsWriteCreation,
    pub(crate) repair: DfsWriteRepair,
    pub(crate) dispatch: DfsWriteDispatch,
    pub(crate) replay_response_journal: bool,
    pub(crate) operation_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum DfsWriteCreation {
    #[default]
    Upsert,
    Exclusive,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum DfsWriteRepair {
    #[default]
    Foreground,
    Background,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum DfsWriteDispatch {
    #[default]
    AwaitResponse,
    Detached,
}

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

struct PreparedDistributedWrite {
    object: DfsObjectMetadata,
    desired_replica_nodes: Vec<NodeId>,
    policy: DfsReplicationPolicy,
    touch_parent_on_create: bool,
    create_predecessor_object_id: Option<DfsObjectId>,
}

fn planned_initial_receipt_nodes(
    node_ids: impl IntoIterator<Item = NodeId>,
    desired_replica_nodes: &[NodeId],
) -> Vec<NodeId> {
    node_ids
        .into_iter()
        .filter(|node_id| desired_replica_nodes.contains(node_id))
        .collect()
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn snapshot_replication_plan(
        &self,
        object: &DfsObjectMetadata,
        target_version: &DfsVersion,
        _supplied_replica_nodes: &[NodeId],
        policy: &DfsReplicationPolicy,
    ) -> Result<Option<(Vec<NodeId>, DfsReplicationPolicy)>, DfsError> {
        let Some(snapshot) = self.placement_snapshot_cache.snapshot() else {
            return Ok(None);
        };
        if snapshot.nodes.is_empty() {
            return Ok(None);
        }

        let pinned = self
            .content_pin_node_ids
            .get(&object.object_id)
            .cloned()
            .unwrap_or_default();
        let mut node_zones = policy.node_zones.clone();
        for node in &snapshot.nodes {
            if let Some(zone) = &node.zone {
                node_zones.insert(node.node_id.clone(), zone.clone());
            }
        }
        let effective_policy =
            DfsReplicationPolicy::new(policy.min_commit_replicas, policy.desired_replicas)?
                .with_placement_constraints(
                    policy.required_node_ids.clone(),
                    policy.forbidden_node_ids.clone(),
                    policy.min_distinct_zones,
                    node_zones,
                )?;

        let mut candidate_ids = snapshot
            .nodes
            .iter()
            .map(|node| node.node_id.clone())
            .collect::<BTreeSet<_>>();
        candidate_ids.extend(object.current_locations.iter().cloned());
        candidate_ids.extend(effective_policy.required_node_ids.iter().cloned());
        candidate_ids.insert(object.owner_node_id.clone());
        let candidates = candidate_ids
            .into_iter()
            .map(|node_id| {
                let weight = snapshot
                    .node(&node_id)
                    .map(|node| node.capacity_weight.max(1))
                    .unwrap_or(1);
                DfsReplicaCandidate::weighted(node_id.clone(), weight).map(|candidate| {
                    candidate.with_zone(effective_policy.node_zones.get(&node_id).cloned())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let request = DfsReplicaPlacementRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            // Version is provenance in the explanation; stable placement
            // scoring deliberately excludes it so normal writes do not churn.
            target_version: target_version.clone(),
            coordinator_node_id: self.node_id.clone(),
            policy: effective_policy.clone(),
            candidates,
        };
        let explanation = explain_replicas_with_placement_snapshot(
            &request,
            &snapshot,
            &object.current_locations,
            &pinned.into_iter().collect::<Vec<_>>(),
        )?;
        Ok(Some((
            explanation.plan.desired_replica_nodes,
            effective_policy,
        )))
    }

    fn normalize_distributed_write_replication_plan(
        &self,
        object: &DfsObjectMetadata,
        target_version: &DfsVersion,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<(Vec<NodeId>, DfsReplicationPolicy), DfsError> {
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }

        validate_replication_plan(&desired_replica_nodes, &policy)?;
        // The metadata primary repeats this step after a routed command arrives,
        // so caller-provided candidates are only a compatibility hint. The
        // durable intent is bound to the primary's immutable snapshot.
        let (desired_replica_nodes, policy) = self
            .snapshot_replication_plan(object, target_version, &desired_replica_nodes, &policy)?
            .unwrap_or((desired_replica_nodes, policy));
        let desired_replica_nodes =
            self.normalize_desired_replica_nodes(object, desired_replica_nodes, &policy);
        let policy = if desired_replica_nodes.len() > policy.desired_replicas {
            let desired_replicas = desired_replica_nodes.len();
            // A confirmed write waits for every desired replica. If normalization
            // adds the owner, widen both counts so it cannot silently become an
            // eventual write.
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
        Ok((desired_replica_nodes, policy))
    }

    fn prepare_distributed_write(
        &mut self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        touch_parent_on_create: bool,
        publication: &DfsWritePublication,
    ) -> Result<PreparedDistributedWrite, DfsError> {
        let object = self.current_object_for_write(object_id)?;
        let retained_local =
            !publication.is_current() && object.policy == DfsObjectPolicy::LocalOwned;
        if !retained_local
            && !matches!(
                object.policy,
                DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
            )
        {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy,
            });
        }
        let (desired_replica_nodes, policy) = if retained_local {
            if desired_replica_nodes != vec![object.owner_node_id.clone()]
                || policy.min_commit_replicas != 1
                || policy.desired_replicas != 1
            {
                return Err(DfsError::InvalidReplicationPolicy {
                    message: "local-owned history must remain on its single content owner".into(),
                });
            }
            (desired_replica_nodes, policy)
        } else {
            self.normalize_distributed_write_replication_plan(
                &object,
                target_version,
                desired_replica_nodes,
                policy,
            )?
        };

        let transient_create = self.transient_metadata_object_ids.contains(object_id);
        let touch_parent_on_create = touch_parent_on_create && transient_create;
        let create_predecessor_object_id = transient_create
            .then(|| {
                self.transient_metadata_predecessor_object_ids
                    .get(object_id)
                    .cloned()
            })
            .flatten();

        Ok(PreparedDistributedWrite {
            object,
            desired_replica_nodes,
            policy,
            touch_parent_on_create,
            create_predecessor_object_id,
        })
    }

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
            metadata_materialization: false,
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
        if let Ok(object) = self.current_read_object_metadata(&request.object_id)
            && let Some((desired_replica_nodes, effective_policy)) = self
                .snapshot_replication_plan(
                    &object,
                    &request.target_version,
                    &candidate_nodes,
                    &policy,
                )?
        {
            let preferred_commit_nodes = desired_replica_nodes
                .iter()
                .take(effective_policy.min_commit_replicas)
                .cloned()
                .collect();
            return Ok(DfsReplicaPlacementPlan {
                desired_replica_nodes,
                preferred_commit_nodes,
            });
        }
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
        self.write_distributed_bytes_with_base(
            request,
            Some(object.version),
            bytes,
            plan.desired_replica_nodes,
            policy,
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: true,
                creation: DfsWriteCreation::Exclusive,
                repair: if foreground_replica_repair {
                    DfsWriteRepair::Foreground
                } else {
                    DfsWriteRepair::Background
                },
                dispatch: DfsWriteDispatch::AwaitResponse,
                replay_response_journal: true,
                operation_id: None,
            },
        )
    }

    pub fn write_distributed_eventual_from_reader_with_candidates<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        reader: R,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.write_distributed_reader_with_candidates(
            request,
            reader,
            candidate_nodes,
            policy,
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                repair: DfsWriteRepair::Foreground,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_distributed_reader_with_candidates<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        reader: R,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let touch_parent_on_create = options.touch_parent_on_create;
        let operation_id = options.operation_id;

        let plan =
            self.plan_distributed_eventual_replicas(&request, candidate_nodes, policy.clone())?;
        self.write_distributed_reader(
            request,
            reader,
            plan.desired_replica_nodes,
            policy,
            DfsDistributedWriteOptions {
                publication: options.publication.clone(),
                touch_parent_on_create,
                repair: options.repair,
                operation_id,
                ..DfsDistributedWriteOptions::default()
            },
        )
    }

    /// Capture a caller reader once into immutable DFS content, then execute
    /// an exact-base and optionally exclusive stable write from that manifest.
    /// Routed retries reopen the captured manifest and never re-read the
    /// caller's mutable source.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_stable_distributed_reader<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        reader: R,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        operation_id: String,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let touch_parent_on_create = options.touch_parent_on_create;

        crate::service_routed_write::validate_write_operation_id(&operation_id)?;
        let manifest = self.content.put_object_version_from_reader(
            request.object_id.as_str(),
            request.target_version.as_str(),
            reader,
        )?;
        self.write_distributed_manifest_with_candidates(
            request,
            expected_base_version,
            manifest.content_hash,
            manifest.size_bytes,
            candidate_nodes,
            policy,
            DfsDistributedWriteOptions {
                publication: options.publication.clone(),
                touch_parent_on_create,
                creation: options.creation,
                repair: options.repair,
                dispatch: DfsWriteDispatch::AwaitResponse,
                replay_response_journal: true,
                operation_id: Some(operation_id),
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_distributed_manifest_with_candidates(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        content_hash: String,
        content_size_bytes: u64,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let touch_parent_on_create = options.touch_parent_on_create;
        let replay_response_journal = options.replay_response_journal;
        let operation_id = options.operation_id;

        let object = self.current_object_for_write(&request.object_id)?;
        let plan = if !options.publication.is_current()
            && object.policy == DfsObjectPolicy::LocalOwned
        {
            if candidate_nodes != vec![object.owner_node_id.clone()]
                || policy.min_commit_replicas != 1
                || policy.desired_replicas != 1
            {
                return Err(DfsError::InvalidReplicationPolicy {
                    message: "local-owned history must remain on its single content owner".into(),
                });
            }
            DfsReplicaPlacementPlan {
                desired_replica_nodes: candidate_nodes.clone(),
                preferred_commit_nodes: candidate_nodes,
            }
        } else {
            self.plan_distributed_eventual_replicas(&request, candidate_nodes, policy.clone())?
        };
        if let Some(shard) = self.select_metadata_shard_for_object(&object)?
            && shard.primary_node_id != self.node_id
        {
            return self.route_distributed_manifest(
                request,
                expected_base_version,
                content_hash,
                content_size_bytes,
                plan.desired_replica_nodes,
                policy,
                object,
                shard,
                DfsDistributedWriteOptions {
                    publication: options.publication.clone(),
                    touch_parent_on_create,
                    creation: options.creation,
                    repair: options.repair,
                    dispatch: options.dispatch,
                    replay_response_journal,
                    operation_id,
                },
            );
        }
        match operation_id {
            Some(operation_id) => self.write_stable_ingested_distributed_manifest(
                request,
                expected_base_version,
                content_hash,
                content_size_bytes,
                plan.desired_replica_nodes,
                policy,
                operation_id,
                DfsDistributedWriteOptions {
                    publication: options.publication.clone(),
                    touch_parent_on_create,
                    creation: options.creation,
                    repair: options.repair,
                    ..DfsDistributedWriteOptions::default()
                },
            ),
            None => self.write_remote_distributed_manifest(
                request,
                expected_base_version,
                content_hash,
                content_size_bytes,
                plan.desired_replica_nodes,
                policy,
                DfsDistributedWriteOptions {
                    publication: options.publication.clone(),
                    touch_parent_on_create,
                    creation: options.creation,
                    repair: options.repair,
                    ..DfsDistributedWriteOptions::default()
                },
            ),
        }
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
        self.write_distributed_bytes_with_base(
            request,
            Some(object.version),
            bytes,
            desired_replica_nodes,
            policy,
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                creation: DfsWriteCreation::Upsert,
                repair: if foreground_replica_repair {
                    DfsWriteRepair::Foreground
                } else {
                    DfsWriteRepair::Background
                },
                dispatch: DfsWriteDispatch::AwaitResponse,
                replay_response_journal: true,
                operation_id: None,
            },
        )
    }

    pub fn write_distributed_eventual_from_reader<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        reader: R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.write_distributed_reader(
            request,
            reader,
            desired_replica_nodes,
            policy,
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                repair: DfsWriteRepair::Foreground,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_distributed_reader<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        reader: R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let touch_parent_on_create = options.touch_parent_on_create;
        let operation_id = options.operation_id;

        let object = self.current_object_for_write(&request.object_id)?;
        self.write_distributed_reader_with_base(
            request,
            Some(object.version),
            reader,
            desired_replica_nodes,
            policy,
            DfsDistributedWriteOptions {
                publication: options.publication.clone(),
                touch_parent_on_create,
                repair: options.repair,
                operation_id,
                ..DfsDistributedWriteOptions::default()
            },
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
        let (desired_replica_nodes, policy) = self.normalize_distributed_write_replication_plan(
            &object,
            &request.target_version,
            desired_replica_nodes,
            policy,
        )?;
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
        self.writes.pending_writes.insert(
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

        let receipt_node_ids = if desired_replica_nodes.contains(&self.node_id) {
            vec![self.node_id.clone()]
        } else {
            Default::default()
        };
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
        let (desired_replica_nodes, policy) = self.normalize_distributed_write_replication_plan(
            &object,
            &request.target_version,
            desired_replica_nodes,
            policy,
        )?;
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
                // The reader has already been consumed into the immutable
                // patch blob. Returning `None` would ask the POSIX caller to
                // replay a source that is now at EOF and could silently turn
                // a racing ranged write into an empty fallback write.
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        };
        self.writes.pending_writes.insert(
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

        let receipt_node_ids = if desired_replica_nodes.contains(&self.node_id) {
            vec![self.node_id.clone()]
        } else {
            Default::default()
        };
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
        let (desired_replica_nodes, policy) = self.normalize_distributed_write_replication_plan(
            &object,
            &request.target_version,
            desired_replica_nodes,
            policy,
        )?;
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
        let (desired_replica_nodes, policy) = self.normalize_distributed_write_replication_plan(
            &object,
            &request.target_version,
            desired_replica_nodes,
            policy,
        )?;
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
        self.write_distributed_reader_with_base(
            request,
            expected_base_version,
            reader,
            desired_replica_nodes,
            policy,
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                repair: DfsWriteRepair::Foreground,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_distributed_reader_with_base<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        reader: R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        if !options.publication.is_current() {
            let operation_id = options
                .operation_id
                .clone()
                .ok_or_else(|| DfsError::Metadata {
                    message: "retained write requires an explicit stable operation identity".into(),
                })?;
            return self.write_stable_distributed_reader(
                request,
                expected_base_version,
                reader,
                desired_replica_nodes,
                policy,
                operation_id,
                options,
            );
        }
        let touch_parent_on_create = options.touch_parent_on_create;
        let foreground_replica_repair = options.repair == DfsWriteRepair::Foreground;
        let operation_id = options.operation_id;

        if let Some(operation_id) = operation_id.as_deref() {
            crate::service_routed_write::validate_write_operation_id(operation_id)?;
        }
        let PreparedDistributedWrite {
            object,
            desired_replica_nodes,
            policy,
            touch_parent_on_create,
            create_predecessor_object_id,
        } = self.prepare_distributed_write(
            &request.object_id,
            &request.target_version,
            desired_replica_nodes,
            policy,
            touch_parent_on_create,
            &DfsWritePublication::Current,
        )?;
        let owner_epoch = match self.select_metadata_shard_for_object(&object)? {
            Some(shard) if shard.primary_node_id != self.node_id => {
                return self.route_distributed_reader(
                    request,
                    expected_base_version,
                    reader,
                    desired_replica_nodes,
                    policy,
                    object,
                    shard,
                    DfsDistributedWriteOptions {
                        publication: options.publication.clone(),
                        touch_parent_on_create,
                        repair: options.repair,
                        operation_id,
                        ..DfsDistributedWriteOptions::default()
                    },
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

        // A stable operation captures its reader exactly once before replay
        // admission. The finalized manifest is immutable, so transport retry
        // and durable-outcome lookup never touch the caller's mutable source
        // again.
        let mut reader = Some(reader);
        let captured_manifest = if operation_id.is_some() {
            Some(self.content.put_object_version_from_reader(
                request.object_id.as_str(),
                request.target_version.as_str(),
                reader.take().expect("stable reader captured once"),
            )?)
        } else {
            None
        };
        if let (Some(operation_id), Some(manifest)) =
            (operation_id.as_deref(), captured_manifest.as_ref())
        {
            let replay_command = DfsDistributedWriteCommand {
                publication: options.publication.clone(),
                request_id: operation_id.to_string(),
                request: request.clone(),
                expected_base_version: expected_base_version.clone(),
                metadata_journal_after_sequence: None,
                bytes: Vec::new(),
                content_hash: Some(manifest.content_hash.clone()),
                content_size_bytes: Some(manifest.size_bytes),
                requester_blob_pre_staged: true,
                desired_replica_nodes: desired_replica_nodes.clone(),
                policy: policy.clone(),
                object_metadata: Some(object.clone()),
                authority_shard: None,
                touch_parent_on_create,
                foreground_replica_repair,
                exclusive_create: false,
            };
            let requester_node_id = request.requesting_node_id.clone();
            if let Some(outcome) =
                self.replayed_routed_write_outcome_if_known(&requester_node_id, &replay_command)?
            {
                return Ok(outcome);
            }
        }

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

        let manifest = match captured_manifest {
            Some(manifest) => manifest,
            None => self.content.put_object_version_from_reader(
                request.object_id.as_str(),
                request.target_version.as_str(),
                reader.expect("legacy reader remains available until content ingest"),
            )?,
        };

        if operation_id.is_none()
            && self.control_plane.is_none()
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

        let explicit_intent_id = operation_id.as_deref().map(|operation_id| {
            stable_write_intent_id(
                &request.requesting_node_id,
                &request.object_id,
                operation_id,
            )
        });
        let intent = self
            .coordinator
            .create_intent_at_epoch_with_parent_touch_and_id(
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
                explicit_intent_id,
                operation_id.clone(),
                operation_id
                    .as_deref()
                    .and_then(stable_operation_order_token),
            )?;
        self.writes.pending_writes.insert(
            intent.intent_id.clone(),
            PendingNetworkedWrite::new(
                request.object_id.clone(),
                request.target_version.clone(),
                manifest.content_hash.clone(),
                manifest.size_bytes,
            )
            .with_foreground_replica_repair(foreground_replica_repair),
        );

        let receipt_node_ids = if desired_replica_nodes.contains(&self.node_id) {
            vec![self.node_id.clone()]
        } else {
            Default::default()
        };
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_remote_distributed_manifest(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        content_hash: String,
        content_size_bytes: u64,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let touch_parent_on_create = options.touch_parent_on_create;

        let requester_node_id = request.requesting_node_id.clone();
        self.apply_distributed_manifest(
            request,
            expected_base_version,
            content_hash,
            content_size_bytes,
            None,
            desired_replica_nodes,
            policy,
            vec![requester_node_id],
            DfsDistributedWriteOptions {
                publication: options.publication.clone(),
                touch_parent_on_create,
                creation: options.creation,
                repair: options.repair,
                operation_id: None,
                ..DfsDistributedWriteOptions::default()
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_stable_remote_distributed_manifest(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        content_hash: String,
        content_size_bytes: u64,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        operation_id: String,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let touch_parent_on_create = options.touch_parent_on_create;

        let requester_node_id = request.requesting_node_id.clone();
        self.apply_distributed_manifest(
            request,
            expected_base_version,
            content_hash,
            content_size_bytes,
            None,
            desired_replica_nodes,
            policy,
            vec![requester_node_id],
            DfsDistributedWriteOptions {
                publication: options.publication.clone(),
                touch_parent_on_create,
                creation: options.creation,
                repair: options.repair,
                operation_id: Some(operation_id),
                ..DfsDistributedWriteOptions::default()
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_stable_quarantined_distributed_blob(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        blob: ContentStoreQuarantinedBlob,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        operation_id: String,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let touch_parent_on_create = options.touch_parent_on_create;

        let content_hash = blob.content_hash.clone();
        let content_size_bytes = blob.size_bytes;
        let initial_receipt_node_ids = if desired_replica_nodes.contains(&self.node_id) {
            vec![self.node_id.clone()]
        } else {
            Default::default()
        };
        self.apply_distributed_manifest(
            request,
            expected_base_version,
            content_hash,
            content_size_bytes,
            Some(blob),
            desired_replica_nodes,
            policy,
            initial_receipt_node_ids,
            DfsDistributedWriteOptions {
                publication: options.publication.clone(),
                touch_parent_on_create,
                creation: options.creation,
                repair: options.repair,
                operation_id: Some(operation_id),
                ..DfsDistributedWriteOptions::default()
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_stable_ingested_distributed_manifest(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        content_hash: String,
        content_size_bytes: u64,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        operation_id: String,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let touch_parent_on_create = options.touch_parent_on_create;

        let initial_receipt_node_ids = if desired_replica_nodes.contains(&self.node_id) {
            vec![self.node_id.clone()]
        } else {
            Default::default()
        };
        self.apply_distributed_manifest(
            request,
            expected_base_version,
            content_hash,
            content_size_bytes,
            None,
            desired_replica_nodes,
            policy,
            initial_receipt_node_ids,
            DfsDistributedWriteOptions {
                publication: options.publication.clone(),
                touch_parent_on_create,
                creation: options.creation,
                repair: options.repair,
                operation_id: Some(operation_id),
                ..DfsDistributedWriteOptions::default()
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_distributed_manifest(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        content_hash: String,
        content_size_bytes: u64,
        quarantined_blob: Option<ContentStoreQuarantinedBlob>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        initial_receipt_node_ids: Vec<NodeId>,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let publication = options.publication.clone();
        publication
            .validate()
            .map_err(|message| DfsError::Metadata { message })?;
        let touch_parent_on_create = options.touch_parent_on_create;
        let exclusive_create = options.creation == DfsWriteCreation::Exclusive;
        let foreground_replica_repair = options.repair == DfsWriteRepair::Foreground;
        let operation_id = options.operation_id;

        if let Some(operation_id) = operation_id.as_deref() {
            crate::service_routed_write::validate_write_operation_id(operation_id)?;
        }
        let trace_started = Instant::now();
        let trace_object_id = request.object_id.clone();
        let trace_version = request.target_version.clone();
        let trace_requester = request.requesting_node_id.clone();
        let trace_desired_count = desired_replica_nodes.len();
        let trace_policy_min = policy.min_commit_replicas;
        let trace_policy_desired = policy.desired_replicas;
        let trace_content_size_bytes = content_size_bytes;
        let PreparedDistributedWrite {
            mut object,
            desired_replica_nodes,
            policy,
            touch_parent_on_create,
            create_predecessor_object_id,
        } = self.prepare_distributed_write(
            &request.object_id,
            &request.target_version,
            desired_replica_nodes,
            policy,
            touch_parent_on_create,
            &publication,
        )?;
        if !publication.is_current() {
            if touch_parent_on_create
                || object.tombstone_provenance.is_some()
                || create_predecessor_object_id.is_some()
                || exclusive_create
                || self
                    .transient_metadata_object_ids
                    .contains(&object.object_id)
                || object.file_system_id != request.file_system_id
                || !matches!(
                    object.kind,
                    DfsObjectKind::File | DfsObjectKind::Artifact | DfsObjectKind::Tombstone
                )
                || self
                    .metadata_backend
                    .as_ref()
                    .map(|backend| backend.get_object(&object.object_id))
                    .transpose()?
                    .flatten()
                    .is_none()
            {
                return Err(DfsError::Metadata {
                    message: "retained content requires an existing canonical file identity".into(),
                });
            }
            self.require_file_history_shard_support(&object.file_system_id, &object.path)?;
        }
        // Snapshot planning may replace the requester with healthier content
        // replicas. Its pre-staged blob remains a valid transfer source via
        // intent.requesting_node_id, but it must not count as a W/N receipt.
        let initial_receipt_node_ids =
            planned_initial_receipt_nodes(initial_receipt_node_ids, &desired_replica_nodes);
        // Creating the in-memory intent is not the visible linearization
        // point. The commit path certifies this hinted epoch through a fresh
        // operation-bound full-shard acknowledgement immediately before
        // atomic publication; cluster KV is used only to validate the configured
        // shard assignment. A second barrier here only adds a WAN round trip (most
        // visibly for first publication of a newly created file).
        let owner_epoch = self.local_metadata_owner_epoch_hint_for_object(&object)?;

        if let Some(operation_id) = operation_id.as_deref() {
            let mut replay_command = DfsDistributedWriteCommand {
                publication: publication.clone(),
                request_id: operation_id.to_string(),
                request: request.clone(),
                expected_base_version: expected_base_version.clone(),
                metadata_journal_after_sequence: None,
                bytes: Vec::new(),
                content_hash: Some(content_hash.clone()),
                content_size_bytes: Some(content_size_bytes),
                requester_blob_pre_staged: true,
                desired_replica_nodes: desired_replica_nodes.clone(),
                policy: policy.clone(),
                object_metadata: Some(object.clone()),
                authority_shard: None,
                touch_parent_on_create,
                foreground_replica_repair,
                exclusive_create,
            };
            if exclusive_create {
                let event_id =
                    crate::service_routed_write::routed_exclusive_create_event_id(&replay_command)
                        .ok_or_else(|| DfsError::Serialization {
                            message: "stable exclusive create is missing object metadata"
                                .to_string(),
                        })?;
                object.last_modified_by_event_id = Some(event_id);
                replay_command.object_metadata = Some(object.clone());
                if self
                    .transient_metadata_object_ids
                    .contains(&object.object_id)
                {
                    self.add_transient_object_metadata(object.clone())?;
                }
            }
            let requester_node_id = request.requesting_node_id.clone();
            if let Some(outcome) =
                self.replayed_routed_write_outcome_if_known(&requester_node_id, &replay_command)?
            {
                return Ok(outcome);
            }
        }

        if publication.is_current()
            && expected_base_version
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

        if publication.is_current()
            && let Err(error) = self.ensure_no_backend_pending_write_intent(
                &request.object_id,
                &request.target_version,
                None,
            )
        {
            return Err(error.into());
        }

        if let Err(error) = self.coordinator.validate_new_intent_for_publication(
            &request.file_system_id,
            &request.object_id,
            expected_base_version.as_ref(),
            &request.target_version,
            &desired_replica_nodes,
            &policy,
            &publication,
        ) {
            return Err(error.into());
        }

        if let Some(blob) = quarantined_blob {
            self.content.bind_quarantined_blob(
                request.object_id.as_str(),
                request.target_version.as_str(),
                blob,
            )?;
        }

        let create_intent_started = Instant::now();
        let explicit_intent_id = operation_id.as_deref().map(|operation_id| {
            stable_write_intent_id(
                &request.requesting_node_id,
                &request.object_id,
                operation_id,
            )
        });
        let intent = self.coordinator.create_intent_for_publication(
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
            explicit_intent_id,
            operation_id.clone(),
            operation_id
                .as_deref()
                .and_then(stable_operation_order_token),
            publication.clone(),
        )?;
        let create_intent_elapsed = create_intent_started.elapsed();
        self.writes.pending_writes.insert(
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
        let mut updated_intent = intent.clone();
        for node_id in &initial_receipt_node_ids {
            updated_intent = self
                .coordinator
                .record_durable_receipt(&intent.intent_id, node_id.clone())?;
        }
        let record_receipt_elapsed = record_receipt_started.elapsed();
        let commit_started = Instant::now();
        let receipt_node_ids = initial_receipt_node_ids;
        if publication.is_current() && !foreground_replica_repair
            && updated_intent.policy.min_commit_replicas <= 1
            // Snapshot planning can replace the caller's initially durable
            // node with a healthier replica. In that case the manifest path
            // starts with zero receipts and must stage before visibility; the
            // one-batch fast path is valid only after W is already satisfied.
            && self
                .coordinator
                .is_commit_ready(&updated_intent.intent_id)?
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
        if let Some(pending) = self
            .writes
            .pending_writes
            .get(&updated_intent.intent_id)
            .cloned()
        {
            if !is_committed {
                if self.open_pending_write_blob_stream(&pending).is_ok() {
                    self.stage_missing_desired_replicas_for_intent(
                        &updated_intent.intent_id,
                        object.clone(),
                    )?;
                } else {
                    let _ = self.request_stage_blob_from_alternate(&updated_intent, &pending)?;
                }
            } else if foreground_replica_repair
                && desired_replica_nodes.contains(&self.node_id)
                // This verifies the complete local blob. Normal background
                // repair must not pay that O(file-size) read after the same
                // request already hashed and durably ingested the attachment.
                && self
                    .local_object_version_manifest_size_if_matches(
                        &request.object_id,
                        &request.target_version,
                        &content_hash,
                    )
                    .is_none()
            {
                let _ = self.request_stage_blob_from_alternate(&updated_intent, &pending)?;
            }
        }
        let stage_elapsed = stage_started.elapsed();

        let outcome = self.write_outcome(&updated_intent.intent_id)?;
        dfs_perf_trace(format_args!(
            "distributed-write-remote-manifest-apply node={} requester={} object={} version={} bytes={} desired={} policy_min={} policy_desired={} foreground_repair={} outcome={} create_intent_ms={} record_receipt_ms={} commit_ms={} stage_ms={} total_ms={}",
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
        self.write_distributed_bytes_with_base(
            request,
            expected_base_version,
            bytes,
            desired_replica_nodes,
            policy,
            DfsDistributedWriteOptions {
                publication: Default::default(),
                touch_parent_on_create: false,
                creation: DfsWriteCreation::Upsert,
                repair: DfsWriteRepair::Foreground,
                dispatch: DfsWriteDispatch::AwaitResponse,
                replay_response_journal: true,
                operation_id: None,
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_distributed_bytes_with_base(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        if !options.publication.is_current() {
            let operation_id = options
                .operation_id
                .clone()
                .ok_or_else(|| DfsError::Metadata {
                    message: "retained write requires an explicit stable operation identity".into(),
                })?;
            return self.write_stable_distributed_reader(
                request,
                expected_base_version,
                std::io::Cursor::new(bytes),
                desired_replica_nodes,
                policy,
                operation_id,
                options,
            );
        }
        let touch_parent_on_create = options.touch_parent_on_create;
        let exclusive_create = options.creation == DfsWriteCreation::Exclusive;
        let foreground_replica_repair = options.repair == DfsWriteRepair::Foreground;
        let replay_response_journal = options.replay_response_journal;
        let operation_id = options.operation_id;

        if let Some(operation_id) = operation_id.as_deref() {
            crate::service_routed_write::validate_write_operation_id(operation_id)?;
        }
        let PreparedDistributedWrite {
            mut object,
            desired_replica_nodes,
            policy,
            touch_parent_on_create,
            create_predecessor_object_id,
        } = self.prepare_distributed_write(
            &request.object_id,
            &request.target_version,
            desired_replica_nodes,
            policy,
            touch_parent_on_create,
            &DfsWritePublication::Current,
        )?;
        let owner_epoch = match self.select_metadata_shard_for_object(&object)? {
            Some(shard) if shard.primary_node_id != self.node_id => {
                return self.route_distributed_bytes(
                    request,
                    expected_base_version,
                    bytes,
                    desired_replica_nodes,
                    policy,
                    object,
                    shard,
                    DfsDistributedWriteOptions {
                        publication: options.publication.clone(),
                        touch_parent_on_create,
                        creation: options.creation,
                        repair: options.repair,
                        dispatch: options.dispatch,
                        replay_response_journal,
                        operation_id,
                    },
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

        if let Some(operation_id) = operation_id.as_deref() {
            let mut replay_command = DfsDistributedWriteCommand {
                publication: options.publication.clone(),
                request_id: operation_id.to_string(),
                request: request.clone(),
                expected_base_version: expected_base_version.clone(),
                metadata_journal_after_sequence: None,
                bytes: Vec::new(),
                content_hash: Some(content_hash(&bytes)),
                content_size_bytes: Some(u64::try_from(bytes.len()).unwrap_or(u64::MAX)),
                requester_blob_pre_staged: false,
                desired_replica_nodes: desired_replica_nodes.clone(),
                policy: policy.clone(),
                object_metadata: Some(object.clone()),
                authority_shard: None,
                touch_parent_on_create,
                foreground_replica_repair,
                exclusive_create,
            };
            if exclusive_create {
                let event_id =
                    crate::service_routed_write::routed_exclusive_create_event_id(&replay_command)
                        .ok_or_else(|| DfsError::Serialization {
                            message: "stable exclusive create is missing object metadata"
                                .to_string(),
                        })?;
                object.last_modified_by_event_id = Some(event_id);
                replay_command.object_metadata = Some(object.clone());
                if self
                    .transient_metadata_object_ids
                    .contains(&object.object_id)
                {
                    self.add_transient_object_metadata(object.clone())?;
                }
            }
            let requester_node_id = request.requesting_node_id.clone();
            if let Some(outcome) =
                self.replayed_routed_write_outcome_if_known(&requester_node_id, &replay_command)?
            {
                return Ok(outcome);
            }
        }

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

        if operation_id.is_none()
            && self.control_plane.is_none()
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
        let explicit_intent_id = operation_id.as_deref().map(|operation_id| {
            stable_write_intent_id(
                &request.requesting_node_id,
                &request.object_id,
                operation_id,
            )
        });
        let intent = self
            .coordinator
            .create_intent_at_epoch_with_parent_touch_and_id(
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
                explicit_intent_id,
                operation_id.clone(),
                operation_id
                    .as_deref()
                    .and_then(stable_operation_order_token),
            )?;
        let create_intent_elapsed = create_intent_started.elapsed();
        self.writes.pending_writes.insert(
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
        attachment_bytes: Option<bytes::Bytes>,
    ) -> NetworkedDfsWriteOutcome {
        self.prune_retained_routed_writes();
        self.writes.pending_routed_writes.insert(
            request_id.clone(),
            PendingRoutedWrite {
                canonical_request_id: request_id.clone(),
                object_id,
                primary_node_id,
                authority_shard,
                command: Some(command),
                attachment_bytes,
                authority_redirects_remaining: DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT,
                remote_intent_id: None,
                durable_receipts: 0,
                required_receipts: 1,
                result: None,
                compact_retained_at: None,
            },
        );
        NetworkedDfsWriteOutcome::Pending {
            intent_id: request_id,
            durable_receipts: 0,
            required_receipts: 1,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn route_distributed_bytes(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let touch_parent_on_create = options.touch_parent_on_create;
        let exclusive_create = options.creation == DfsWriteCreation::Exclusive;
        let foreground_replica_repair = options.repair == DfsWriteRepair::Foreground;
        let detach_request = options.dispatch == DfsWriteDispatch::Detached;
        let replay_response_journal = options.replay_response_journal;
        let operation_id = options.operation_id;

        let trace_started = Instant::now();
        let trace_requester = request.requesting_node_id.clone();
        let trace_object_id = request.object_id.clone();
        let trace_version = request.target_version.clone();
        let trace_bytes = bytes.len() as u64;
        let trace_desired_count = desired_replica_nodes.len();
        let trace_policy_min = policy.min_commit_replicas;
        let trace_policy_desired = policy.desired_replicas;
        let request_id = match operation_id {
            Some(operation_id) => {
                crate::service_routed_write::validate_write_operation_id(&operation_id)?;
                operation_id
            }
            None => self.next_request_id(),
        };
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        let metadata_sequence_started = Instant::now();
        let metadata_journal_after_sequence = if !detach_request && replay_response_journal {
            self.local_metadata_journal_after_sequence_for_shard(&shard_id)?
        } else {
            None
        };
        let metadata_sequence_elapsed = metadata_sequence_started.elapsed();
        let primary_node_id = shard.primary_node_id.clone();
        // An empty object has a real canonical content identity, but it has no
        // semantic attachment. Sending a zero-length `ObjectRpcBodySource`
        // would clear the attachment body role at the network boundary while
        // retaining an attachment-shaped request head. Route the command as an
        // ordinary value instead; the metadata primary derives the canonical
        // empty manifest/hash through the normal content-store path. Empty
        // content is intrinsic and needs neither a blob file nor a pre-catalog
        // content reservation.
        let bytes = bytes::Bytes::from(bytes);
        let attachment = if bytes.is_empty() {
            None
        } else {
            Some(DfsOutboundAttachment::from_shared_bytes_with_transport_integrity(bytes.clone()))
        };
        let command = DfsDistributedWriteCommand {
            publication: options.publication.clone(),
            request_id: request_id.clone(),
            request,
            expected_base_version,
            metadata_journal_after_sequence,
            bytes: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            requester_blob_pre_staged: false,
            desired_replica_nodes,
            policy,
            object_metadata: Some(object.clone()),
            authority_shard: Some(shard.clone()),
            touch_parent_on_create,
            foreground_replica_repair,
            exclusive_create,
        };
        let message = command.clone().into_network_message();
        let routed = self.next_dfs_routed_message(&primary_node_id, message);
        let call = match attachment {
            Some(attachment) => DfsRoutedCall::with_attachment(routed, attachment),
            None => DfsRoutedCall::value(routed),
        };
        // The body and pending redirect state share one payload allocation.
        // Dispatch it before returning even for a logically detached metadata
        // request; queueing it would require persisting or cloning the complete
        // payload on the requester, defeating direct streamed ingest.
        let send_started = Instant::now();
        let sent = match self.transport.send_routed_call(&primary_node_id, call) {
            Ok(()) => true,
            Err(NetworkError::UnknownNode { .. }) => false,
            Err(error) => return Err(DfsError::from_network(error)),
        };
        let send_elapsed = send_started.elapsed();
        dfs_perf_trace(format_args!(
            "routed-write-send node={} requester={} primary={} request={} object={} version={} bytes={} reader_attachment=false owned_attachment={} desired={} policy_min={} policy_desired={} capture_ms=0 metadata_seq_ms={} send_ms={} total_ms={} sent={}",
            self.node_id,
            trace_requester,
            primary_node_id,
            request_id,
            trace_object_id,
            trace_version,
            trace_bytes,
            trace_bytes > 0,
            trace_desired_count,
            trace_policy_min,
            trace_policy_desired,
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
            (!bytes.is_empty()).then_some(bytes),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn route_distributed_manifest(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        content_hash: String,
        content_size_bytes: u64,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let touch_parent_on_create = options.touch_parent_on_create;
        let exclusive_create = options.creation == DfsWriteCreation::Exclusive;
        let foreground_replica_repair = options.repair == DfsWriteRepair::Foreground;
        let detach_request = options.dispatch == DfsWriteDispatch::Detached;
        let replay_response_journal = options.replay_response_journal;
        let operation_id = options.operation_id;

        let request_id = match operation_id {
            Some(operation_id) => {
                crate::service_routed_write::validate_write_operation_id(&operation_id)?;
                operation_id
            }
            None => self.next_request_id(),
        };
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        let metadata_journal_after_sequence = if !detach_request && replay_response_journal {
            self.local_metadata_journal_after_sequence_for_shard(&shard_id)?
        } else {
            None
        };
        let primary_node_id = shard.primary_node_id.clone();
        let command = DfsDistributedWriteCommand {
            publication: options.publication.clone(),
            request_id: request_id.clone(),
            request,
            expected_base_version,
            metadata_journal_after_sequence,
            bytes: Vec::new(),
            content_hash: Some(content_hash),
            content_size_bytes: Some(content_size_bytes),
            requester_blob_pre_staged: true,
            desired_replica_nodes,
            policy,
            object_metadata: Some(object.clone()),
            authority_shard: Some(shard.clone()),
            touch_parent_on_create,
            foreground_replica_repair,
            exclusive_create,
        };
        let message = command.clone().into_network_message();
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
            None,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn route_distributed_reader<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        reader: R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let touch_parent_on_create = options.touch_parent_on_create;
        let foreground_replica_repair = options.repair == DfsWriteRepair::Foreground;
        let operation_id = options.operation_id;

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
        let request_id = match operation_id {
            Some(operation_id) => {
                crate::service_routed_write::validate_write_operation_id(&operation_id)?;
                operation_id
            }
            None => self.next_request_id(),
        };
        let capture_started = Instant::now();
        let manifest = self.content.put_object_version_from_reader(
            request.object_id.as_str(),
            request.target_version.as_str(),
            reader,
        )?;
        let capture_elapsed = capture_started.elapsed();
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
            publication: options.publication.clone(),
            request_id: request_id.clone(),
            request,
            expected_base_version,
            metadata_journal_after_sequence,
            bytes: Vec::new(),
            content_hash,
            content_size_bytes,
            requester_blob_pre_staged: true,
            desired_replica_nodes,
            policy,
            object_metadata: Some(object.clone()),
            authority_shard: Some(shard.clone()),
            touch_parent_on_create,
            foreground_replica_repair,
            exclusive_create: false,
        };
        let send_started = Instant::now();
        let message = command.clone().into_network_message();
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
            None,
        ))
    }
}

#[cfg(test)]
mod initial_receipt_tests {
    use super::*;

    #[test]
    fn snapshot_replanning_drops_non_replica_receipts() {
        let requester = NodeId::from_legacy_named("receipt-requester").expect("requester");
        let replica = NodeId::from_legacy_named("receipt-replica").expect("replica");

        assert_eq!(
            planned_initial_receipt_nodes(
                [requester, replica.clone()],
                std::slice::from_ref(&replica),
            ),
            vec![replica]
        );
    }
}
