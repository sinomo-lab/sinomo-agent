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
use sinomo_core::network::{NetworkDeliveryState, NetworkError};
use sinomo_core::node::NodeId;
use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};

use crate::content_store::ContentPatchManifest;
use crate::intent::{stable_operation_order_token, stable_write_intent_id};
use crate::transfer::{PendingNetworkedWrite, PendingNetworkedWritePatch};
use crate::*;

#[cfg(test)]
mod fresh_admission_native_tests;

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
    /// A result of the existing namespace caller's actual lookup/allocation,
    /// never a wire claim or inference from the transient overlay index.
    FreshNamespace {
        exclusive: bool,
    },
}

impl DfsWriteCreation {
    pub(crate) fn from_namespace_resolution(exclusive: bool, created: bool) -> Self {
        if created {
            Self::FreshNamespace { exclusive }
        } else if exclusive {
            Self::Exclusive
        } else {
            Self::Upsert
        }
    }

    fn is_exclusive(self) -> bool {
        matches!(
            self,
            Self::Exclusive | Self::FreshNamespace { exclusive: true }
        )
    }

    pub(crate) fn is_fresh_namespace(self) -> bool {
        matches!(self, Self::FreshNamespace { .. })
    }
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
    /// The caller waits through the committed outcome, so a new requester
    /// content binding may precede dispatch. Non-waiting creates retain their
    /// streamed delivery path and its asynchronous replica progress contract.
    AwaitCommit,
    Detached,
}

pub(crate) fn stale_write_base_error(
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

/// Byte input can be replayed after patch admission fails. A streamed input
/// must retain its error after ingestion: falling back would reread it at EOF.
enum DfsPatchSource<'a> {
    Bytes(&'a [u8]),
    Reader(&'a mut dyn Read),
}

impl DfsPatchSource<'_> {
    fn can_replay(&self) -> bool {
        matches!(self, Self::Bytes(_))
    }

    fn is_empty(&self) -> bool {
        matches!(self, Self::Bytes(bytes) if bytes.is_empty())
    }

    fn ingest(
        &mut self,
        content: &LocalContentStore,
        request: &DfsWriteRequest,
        base_version: &DfsVersion,
        offset: u64,
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<ContentPatchManifest, DfsError> {
        Ok(match self {
            Self::Bytes(bytes) => content.put_object_version_patch_with_info_and_base_identity(
                request.object_id.as_str(),
                base_version.as_str(),
                request.target_version.as_str(),
                offset,
                bytes,
                base_identity,
            )?,
            Self::Reader(reader) => content
                .put_object_version_patch_from_reader_with_info_and_base_identity(
                    request.object_id.as_str(),
                    base_version.as_str(),
                    request.target_version.as_str(),
                    offset,
                    &mut **reader,
                    base_identity,
                )?,
        })
    }
}

#[derive(Clone, Copy)]
enum DfsLocalEventualWrite {
    Full,
    Patch,
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

pub(crate) struct PreviousRoutedWriteState {
    pub(crate) pending: Option<PendingRoutedWrite>,
    pub(crate) registration_checkpoint: usize,
    pub(crate) new_aliases: Vec<String>,
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn snapshot_fresh_namespace_replication_plan(
        &self,
        object: &DfsObjectMetadata,
        target_version: &DfsVersion,
        policy: &DfsReplicationPolicy,
        creation: DfsWriteCreation,
        repair: DfsWriteRepair,
    ) -> Result<Option<(Vec<NodeId>, DfsReplicationPolicy)>, DfsError> {
        if !creation.is_fresh_namespace()
            || repair != DfsWriteRepair::Background
            || object.policy != DfsObjectPolicy::ReplicatedEventual
            || object.kind != DfsObjectKind::File
            || policy.min_commit_replicas >= policy.desired_replicas
            || self.publication_provider.current().is_none()
        {
            return Ok(None);
        }
        let unavailable = |message: &str| DfsError::Metadata {
            message: message.into(),
        };
        let control = self.control_plane.as_deref().ok_or_else(|| {
            unavailable("fresh native placement has no retained configuration owner")
        })?;
        let configuration = control
            .write_configuration_projection()?
            .ok_or_else(|| unavailable("fresh native placement has no complete configuration"))?;
        let filesystem = configuration
            .file_system_config(&object.file_system_id)?
            .filter(|config| config.file_system_id == object.file_system_id)
            .ok_or_else(|| {
                unavailable("fresh native placement has no exact filesystem configuration")
            })?;
        let prefix = format!("{}/", crate::metadata_shards_prefix(&object.file_system_id));
        let shard_entries = configuration
            .entries(&prefix)
            .ok_or_else(|| unavailable("fresh native placement has no complete shard scope"))?;
        let shards = crate::metadata_shard_index_from_authority_entries(shard_entries)
            .remove(&object.file_system_id.to_string())
            .ok_or_else(|| {
                unavailable("fresh native placement has no exact filesystem shard records")
            })?
            .map_err(|message| DfsError::Metadata { message })?;
        let shard = crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
            &object.file_system_id,
            &shards,
            &object.path,
        )?
        .ok_or_else(|| unavailable("fresh native placement has no exact object shard"))?;
        if shard.primary_node_id == self.node_id
            && !self.transport.delegates_local_namespace_authority()
        {
            // Requester-side selection is planning DATA only. The actual local
            // author must independently own admission and prove canonical
            // absence; a transient overlay may have survived a failed read.
            let execution = self.publication_execution.current().ok_or_else(|| {
                unavailable("fresh native placement has no actual shard execution admission")
            })?;
            let backend = self.metadata_backend.as_deref().ok_or_else(|| {
                unavailable("fresh native placement has no actual metadata backend")
            })?;
            let shard_id = DfsMetadataShardId::new(&shard.shard_id);
            let scope = backend.publication_owner_scope(&object.file_system_id, &shard_id)?;
            if !execution.admits(&scope, &object.file_system_id, &shard_id) {
                return Err(unavailable(
                    "fresh native placement lacks its actual shard admission",
                ));
            }
            if backend
                .get_object_on_shard(&shard_id, &object.object_id)?
                .is_some()
            {
                return Err(unavailable(
                    "fresh namespace object already has canonical metadata",
                ));
            }
            if let Some(existing) =
                backend.get_object_by_path(&object.file_system_id, &object.path)?
            {
                if existing.kind != DfsObjectKind::Tombstone
                    || self
                        .transient_metadata_predecessor_object_ids
                        .get(&object.object_id)
                        != Some(&existing.object_id)
                {
                    return Err(unavailable(
                        "fresh namespace path absence or predecessor changed",
                    ));
                }
            }
        }
        let resolved = crate::placement::resolve_placement_policy(
            &filesystem,
            &object.path,
            object.policy.clone(),
            policy.desired_replicas,
        )?;
        if resolved.source_path.is_some()
            && (resolved.policy.consistency != object.policy
                || resolved.policy.min_commit_replicas != policy.min_commit_replicas
                || resolved.policy.desired_replicas != policy.desired_replicas
                || resolved.policy.required_node_ids != policy.required_node_ids
                || resolved.policy.forbidden_node_ids != policy.forbidden_node_ids
                || resolved.policy.min_distinct_zones != policy.min_distinct_zones)
        {
            return Err(unavailable(
                "fresh namespace placement differs from its current managed policy",
            ));
        }
        let inventory = configuration.admitted_content_targets()?;
        let mut zones = policy.node_zones.clone();
        for advertisement in configuration.content_advertisements()? {
            if let Some(zone) = advertisement.zone {
                if zones
                    .get(&advertisement.node_id)
                    .is_some_and(|captured| captured != &zone)
                {
                    return Err(unavailable(
                        "fresh namespace placement has conflicting captured zone evidence",
                    ));
                }
                zones.insert(advertisement.node_id, zone);
            }
        }
        let effective_policy =
            DfsReplicationPolicy::new(policy.min_commit_replicas, policy.desired_replicas)?
                .with_placement_constraints(
                    policy.required_node_ids.clone(),
                    policy.forbidden_node_ids.clone(),
                    policy.min_distinct_zones,
                    zones,
                )?;
        let request = DfsReplicaPlacementRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version: target_version.clone(),
            coordinator_node_id: self.node_id.clone(),
            policy: effective_policy.clone(),
            candidates: Vec::new(),
        };
        let pinned = self
            .content_pin_node_ids
            .get(&object.object_id)
            .cloned()
            .unwrap_or_default();
        let snapshot = self.placement_snapshot_cache.snapshot();
        let plan =
            crate::placement_environment::plan_fresh_eventual_replicas_with_admitted_targets(
                &request,
                &inventory,
                snapshot.as_deref(),
                &object.current_locations,
                &pinned.into_iter().collect::<Vec<_>>(),
            )?;
        dfs_perf_trace(format_args!(
            "fresh_namespace_content_plan configuration_revision={} desired={} attemptable={} observed_ready={} deferred={}",
            plan.configuration_revision.0,
            plan.placement.desired_replica_nodes.len(),
            plan.attemptable_node_ids.len(),
            plan.observed_ready_node_ids.len(),
            plan.deferred_desired_node_ids.len(),
        ));
        Ok(Some((
            plan.placement.desired_replica_nodes,
            effective_policy,
        )))
    }

    pub(crate) fn plan_namespace_creation_replicas(
        &self,
        request: &DfsWriteRequest,
        object: &DfsObjectMetadata,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        creation: DfsWriteCreation,
        repair: DfsWriteRepair,
    ) -> Result<DfsReplicaPlacementPlan, DfsError> {
        if let Some((desired_replica_nodes, policy)) = self
            .snapshot_fresh_namespace_replication_plan(
                object,
                &request.target_version,
                &policy,
                creation,
                repair,
            )?
        {
            let preferred_commit_nodes = desired_replica_nodes
                .iter()
                .take(policy.min_commit_replicas)
                .cloned()
                .collect();
            return Ok(DfsReplicaPlacementPlan {
                desired_replica_nodes,
                preferred_commit_nodes,
            });
        }
        self.plan_distributed_eventual_replicas(request, candidate_nodes, policy)
    }

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
        let plan = crate::placement_environment::plan_replicas_with_placement_snapshot(
            &request,
            &snapshot,
            &object.current_locations,
            &pinned.into_iter().collect::<Vec<_>>(),
        )?;
        Ok(Some((plan.desired_replica_nodes, effective_policy)))
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
        let (desired_replica_nodes, policy) = match self.snapshot_replication_plan(
            object,
            target_version,
            &desired_replica_nodes,
            &policy,
        )? {
            Some(plan) => plan,
            // A cold caller preserves its validated selection and W/N until
            // the primary replans. A historical owner is not an extra replica.
            None => (desired_replica_nodes, policy),
        };
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        Ok((desired_replica_nodes, policy))
    }

    fn current_object_for_write_request(
        &mut self,
        request: &DfsWriteRequest,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let crate::service_read::DfsObjectReadSource::Authority { hinted_shard, .. } =
            self.object_read_source(&request.object_id, Some(&request.file_system_id))?
        else {
            return self.current_object_for_write(&request.object_id);
        };
        // This is requester planning DATA. The metadata primary still owns
        // write admission, the current-head comparison and publication.
        let object = self
            .current_object_from_hinted_authority(
                &request.file_system_id,
                &request.object_id,
                hinted_shard,
            )?
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: request.object_id.clone(),
            })?;
        self.ensure_object_not_in_cross_shard_flight(&object)?;
        Ok(object)
    }

    fn prepare_distributed_write(
        &mut self,
        request: &DfsWriteRequest,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        touch_parent_on_create: bool,
        creation: DfsWriteCreation,
        repair: DfsWriteRepair,
    ) -> Result<PreparedDistributedWrite, DfsError> {
        let object_id = &request.object_id;
        let target_version = &request.target_version;
        let object = self.current_object_for_write_request(request)?;
        let local_owned = object.policy == DfsObjectPolicy::LocalOwned;
        if !local_owned
            && !matches!(
                object.policy,
                DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
            )
        {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy,
            });
        }
        let (desired_replica_nodes, policy) = if local_owned {
            if desired_replica_nodes != vec![object.owner_node_id.clone()]
                || policy.min_commit_replicas != 1
                || policy.desired_replicas != 1
            {
                return Err(DfsError::InvalidReplicationPolicy {
                    message: "local-owned content must remain on its single content owner".into(),
                });
            }
            (desired_replica_nodes, policy)
        } else if let Some(plan) = self.snapshot_fresh_namespace_replication_plan(
            &object,
            target_version,
            &policy,
            creation,
            repair,
        )? {
            plan
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

    /// Replay is checked before fresh admission. Current publications compare
    /// the visible base and exclude pending writers; retained publications keep
    /// their immutable source version without claiming the current head.
    fn validate_distributed_write_admission(
        &mut self,
        request: &DfsWriteRequest,
        expected_base_version: Option<&DfsVersion>,
        object: &DfsObjectMetadata,
        desired_replica_nodes: &[NodeId],
        policy: &DfsReplicationPolicy,
        publication: &DfsWritePublication,
    ) -> Result<(), DfsError> {
        self.check_current_write_precondition(publication, &request.file_system_id,
            &request.object_id, &object.path, expected_base_version)?;
        if publication.is_current() {
            if let Some(expected) = expected_base_version
                && expected != &object.version
            {
                return Err(stale_write_base_error(
                    object,
                    expected,
                    &request.target_version,
                ));
            }
            self.ensure_no_backend_pending_write_intent(
                &request.object_id,
                &request.target_version,
                None,
            )?;
        }
        self.coordinator.validate_new_intent_for_publication(
            &request.file_system_id,
            &request.object_id,
            expected_base_version,
            &request.target_version,
            desired_replica_nodes,
            policy,
            publication,
        )?;
        Ok(())
    }

    pub(crate) fn record_durable_write_intent_work(
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
            semantic_changes: false,
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
        let can_commit_in_one_batch = intent.publication.current_precondition().is_none()
            && !foreground_replica_repair
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
        // Current content evidence remains owned by the coordinator until the
        // complete visibility preparation seals it with its terminal intent.
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
        self.write_distributed_bytes_with_base(
            request,
            // This exclusive parent-touch path creates a new transient link.
            // Its provisional metadata version is not a durable base head.
            None,
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
        mut reader: R,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        operation_id: String,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.write_stable_distributed_reader_impl(
            request,
            expected_base_version,
            &mut reader,
            candidate_nodes,
            policy,
            operation_id,
            options,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn write_stable_distributed_reader_impl(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        reader: &mut dyn Read,
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

        let object = self.current_object_for_write_request(&request)?;
        let plan = if object.policy == DfsObjectPolicy::LocalOwned {
            if candidate_nodes != vec![object.owner_node_id.clone()]
                || policy.min_commit_replicas != 1
                || policy.desired_replicas != 1
            {
                return Err(DfsError::InvalidReplicationPolicy {
                    message: "local-owned content must remain on its single content owner".into(),
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
            && (shard.primary_node_id != self.node_id
                || self.transport.delegates_local_namespace_authority())
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
        let object = self.current_object_for_write_request(&request)?;
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

        let object = self.current_object_for_write_request(&request)?;
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
        self.write_distributed_eventual_patch_with_source(
            request,
            expected_base_version,
            offset,
            DfsPatchSource::Bytes(bytes),
            desired_replica_nodes,
            policy,
            foreground_replica_repair,
        )
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
        self.write_distributed_eventual_patch_with_source(
            request,
            expected_base_version,
            offset,
            DfsPatchSource::Reader(reader),
            desired_replica_nodes,
            policy,
            foreground_replica_repair,
        )
    }

    fn write_distributed_eventual_patch_with_source(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: DfsVersion,
        offset: u64,
        mut source: DfsPatchSource<'_>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        foreground_replica_repair: bool,
    ) -> Result<Option<NetworkedDfsWriteOutcome>, DfsError> {
        if let Some(outcome) = self.write_self_only_eventual_patch_with_source(
            request.clone(),
            expected_base_version.clone(),
            offset,
            &mut source,
            desired_replica_nodes.clone(),
            policy.clone(),
        )? {
            return Ok(Some(outcome));
        }
        if source.is_empty() {
            return Ok(None);
        }

        let object = self.current_object_for_write_request(&request)?;
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
        let owner_shard = self.select_metadata_shard_for_object(&object)?;
        if let Some(shard) = owner_shard.as_ref()
            && (shard.primary_node_id != self.node_id
                || self.transport.delegates_local_namespace_authority())
        {
            return self
                .route_distributed_patch(
                    request,
                    expected_base_version,
                    offset,
                    source,
                    desired_replica_nodes,
                    policy,
                    foreground_replica_repair,
                    object,
                    shard.clone(),
                )
                .map(Some);
        }
        let owner_epoch = match owner_shard {
            Some(shard)
                if shard.primary_node_id != self.node_id
                    || self.transport.delegates_local_namespace_authority() =>
            {
                return Ok(None);
            }
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
        let patch_info = match source.ingest(
            &self.content,
            &request,
            &expected_base_version,
            offset,
            base_identity.as_ref(),
        ) {
            Ok(patch_info) => patch_info,
            Err(_) if source.can_replay() => return Ok(None),
            Err(error) => return Err(error),
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
                    && distributed_error_is_stale_or_concurrent_write(&error)
                    && source.can_replay() =>
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
            .with_new_intent_transfer_targets(&desired_replica_nodes)
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

    fn write_self_only_eventual_patch_with_source(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: DfsVersion,
        offset: u64,
        source: &mut DfsPatchSource<'_>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<Option<NetworkedDfsWriteOutcome>, DfsError> {
        if source.is_empty() || self.control_plane.is_some() {
            return Ok(None);
        }
        let object = self.current_object_for_write_request(&request)?;
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
        let manifest = match source.ingest(
            &self.content,
            &request,
            &expected_base_version,
            offset,
            base_identity.as_ref(),
        ) {
            Ok(patch_info) => patch_info.manifest,
            Err(_) if source.can_replay() => return Ok(None),
            Err(error) => return Err(error),
        };
        self.commit_self_only_eventual_manifest(
            request,
            object,
            manifest,
            DfsLocalEventualWrite::Patch,
        )
        .map(Some)
    }

    fn commit_self_only_eventual_manifest(
        &mut self,
        request: DfsWriteRequest,
        object: DfsObjectMetadata,
        manifest: ContentManifest,
        kind: DfsLocalEventualWrite,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        if self.control_plane.is_some() {
            return Err(DfsError::Distributed {
                message: "clustered self-only writes require a canonical metadata prepare"
                    .to_string(),
            });
        }
        let mut committed_object = object;
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
        if matches!(kind, DfsLocalEventualWrite::Full) {
            self.record_message_committed_work(DfsMessageCommittedWork {
                publish_metadata: true,
                ..DfsMessageCommittedWork::default()
            });
        }
        self.coordinator.set_committed_version(
            committed_object.file_system_id.clone(),
            committed_object.object_id.clone(),
            committed_object.version.clone(),
        );
        self.track_metadata_tombstone(&committed_object);
        self.metadata.add_object(committed_object)?;
        let intent_prefix = match kind {
            DfsLocalEventualWrite::Full => "local-eventual",
            DfsLocalEventualWrite::Patch => "local-eventual-patch",
        };
        Ok(NetworkedDfsWriteOutcome::Committed {
            intent_id: format!(
                "{intent_prefix}:{}:{}",
                request.object_id, request.target_version
            ),
            version: request.target_version,
            content_hash: manifest.content_hash,
            repair_jobs: Vec::new(),
        })
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
    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.write.reader",
        skip_all,
        fields(stage = "dfs.write.reader")
    )]
    pub(crate) fn write_distributed_reader_with_base<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        mut reader: R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.write_distributed_reader_with_base_impl(
            request,
            expected_base_version,
            &mut reader,
            desired_replica_nodes,
            policy,
            options,
        )
    }

    // Reader ownership stays in the caller's thin wrapper. Keeping the full
    // write algorithm here avoids cloning it for every reader adapter type.
    #[allow(clippy::too_many_arguments)]
    fn write_distributed_reader_with_base_impl(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        reader: &mut dyn Read,
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
        let publication = options.publication.clone();
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
            &request,
            desired_replica_nodes,
            policy,
            touch_parent_on_create,
            if options.publication.is_current() && expected_base_version.is_none() {
                options.creation
            } else {
                DfsWriteCreation::Upsert
            },
            options.repair,
        )?;
        let owner_epoch = match self.select_metadata_shard_for_object(&object)? {
            Some(shard)
                if shard.primary_node_id != self.node_id
                    || self.transport.delegates_local_namespace_authority() =>
            {
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
                patch: None,
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

        self.validate_distributed_write_admission(
            &request,
            expected_base_version.as_ref(),
            &object,
            &desired_replica_nodes,
            &policy,
            &publication,
        )?;

        let manifest = match captured_manifest {
            Some(manifest) => manifest,
            None => self.content.put_object_version_from_reader(
                request.object_id.as_str(),
                request.target_version.as_str(),
                reader.expect("one-shot reader remains available until content ingest"),
            )?,
        };

        if publication.is_default_current() && operation_id.is_none()
            && self.control_plane.is_none()
            && !touch_parent_on_create
            && object.policy == DfsObjectPolicy::ReplicatedEventual
            && policy.min_commit_replicas == 1
            && desired_replica_nodes.len() == 1
            && desired_replica_nodes[0] == self.node_id
            && object.owner_node_id == self.node_id
        {
            return self.commit_self_only_eventual_manifest(
                request,
                object,
                manifest,
                DfsLocalEventualWrite::Full,
            );
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
            .create_intent_for_publication(
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
        self.writes.pending_writes.insert(
            intent.intent_id.clone(),
            PendingNetworkedWrite::new(
                request.object_id.clone(),
                request.target_version.clone(),
                manifest.content_hash.clone(),
                manifest.size_bytes,
            )
            .with_new_intent_transfer_targets(&desired_replica_nodes)
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
        requester_binding_is_exact: bool,
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
            None,
            desired_replica_nodes,
            policy,
            // Rebased object/version tuples require the existing fetch/stage
            // path to persist their canonical binding before W can count.
            // The requester remains a hash-addressed transfer source even
            // when it contributes no initial receipt.
            if requester_binding_is_exact {
                vec![requester_node_id]
            } else {
                Vec::new()
            },
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
        // This SAME admitted blob supplies local durability after binding.
        // The final placement filters the candidate inside apply_distributed_manifest.
        let initial_receipt_node_ids = vec![self.node_id.clone()];
        self.apply_distributed_manifest(
            request,
            expected_base_version,
            content_hash,
            content_size_bytes,
            Some(blob),
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

        // Ingestion already durably bound this exact local object/version.
        // The primary can change the caller's replica hint during planning;
        // filter this real receipt only against that final plan in apply.
        let initial_receipt_node_ids = vec![self.node_id.clone()];
        self.apply_distributed_manifest(
            request,
            expected_base_version,
            content_hash,
            content_size_bytes,
            None,
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
    pub(crate) fn write_stable_ingested_distributed_patch_manifest(
        &mut self,
        request: DfsWriteRequest,
        base_version: DfsVersion,
        patch: &crate::DfsDistributedWritePatch,
        manifest: crate::content_store::ContentManifest,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        operation_id: String,
        foreground_replica_repair: bool,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        // This actual ingested patch is local durability DATA. Final native
        // replanning below decides whether the local node may count toward W.
        let receipt_nodes = vec![self.node_id.clone()];
        self.apply_distributed_manifest(
            request,
            Some(base_version.clone()),
            manifest.content_hash,
            manifest.size_bytes,
            None,
            Some(PendingNetworkedWritePatch {
                base_version,
                offset: patch.offset,
                patch_hash: patch.patch_hash.clone(),
                patch_size_bytes: patch.patch_size_bytes,
                full_stage_fallback_nodes: BTreeSet::new(),
            }),
            desired_replica_nodes,
            policy,
            receipt_nodes,
            DfsDistributedWriteOptions {
                operation_id: Some(operation_id),
                repair: if foreground_replica_repair {
                    DfsWriteRepair::Foreground
                } else {
                    DfsWriteRepair::Background
                },
                ..DfsDistributedWriteOptions::default()
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.write.manifest",
        skip_all,
        fields(stage = "dfs.write.manifest")
    )]
    fn apply_distributed_manifest(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        content_hash: String,
        content_size_bytes: u64,
        quarantined_blob: Option<ContentStoreQuarantinedBlob>,
        patch: Option<PendingNetworkedWritePatch>,
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
        let exclusive_create = options.creation.is_exclusive();
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
            &request,
            desired_replica_nodes,
            policy,
            touch_parent_on_create,
            if publication.is_current() && expected_base_version.is_none() {
                options.creation
            } else {
                DfsWriteCreation::Upsert
            },
            options.repair,
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
        let owner_shard = self.local_metadata_owner_shard_hint_for_object(&object)?;
        let owner_epoch = owner_shard.as_ref().map_or(0, |shard| shard.epoch);

        if let Some(operation_id) = operation_id.as_deref() {
            let mut replay_command = DfsDistributedWriteCommand {
                publication: publication.clone(),
                patch: None,
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

        self.validate_distributed_write_admission(
            &request,
            expected_base_version.as_ref(),
            &object,
            &desired_replica_nodes,
            &policy,
            &publication,
        )?;

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
            PendingNetworkedWrite::with_patch(
                request.object_id.clone(),
                request.target_version.clone(),
                content_hash.clone(),
                content_size_bytes,
                patch,
            )
            .with_new_intent_transfer_targets(&desired_replica_nodes)
            .with_foreground_replica_repair(foreground_replica_repair),
        );

        let record_receipt_started = Instant::now();
        let mut updated_intent = intent.clone();
        for node_id in &initial_receipt_node_ids {
            updated_intent = self
                .coordinator
                .record_durable_receipt(&intent.intent_id, node_id.clone())?;
            #[cfg(any(test, feature = "test-support"))]
            if let Some(shard) = owner_shard.as_ref() {
                self.observe_native_coordinator_receipt(
                    Some(&DfsMetadataShardId::new(shard.shard_id.clone())),
                    &updated_intent,
                    node_id,
                );
            }
        }
        let record_receipt_elapsed = record_receipt_started.elapsed();
        let commit_started = Instant::now();
        let receipt_node_ids = initial_receipt_node_ids;
        if publication.is_default_current() && !foreground_replica_repair
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
    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.write.bytes",
        skip_all,
        fields(stage = "dfs.write.bytes")
    )]
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
        let publication = options.publication.clone();
        let touch_parent_on_create = options.touch_parent_on_create;
        let exclusive_create = options.creation.is_exclusive();
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
            &request,
            desired_replica_nodes,
            policy,
            touch_parent_on_create,
            if options.publication.is_current() && expected_base_version.is_none() {
                options.creation
            } else {
                DfsWriteCreation::Upsert
            },
            options.repair,
        )?;
        let owner_epoch = match self.select_metadata_shard_for_object(&object)? {
            Some(shard)
                if shard.primary_node_id != self.node_id
                    || self.transport.delegates_local_namespace_authority() =>
            {
                // When the requester is already a selected content replica and
                // W=1, make that copy durable here. Sending the bytes to a
                // metadata primary outside the selected set otherwise leaves
                // zero usable receipts and forces another transfer back to a
                // selected replica before publication.
                // Creating a local content reservation does not publish a
                // namespace object. The primary still owns parent/exclusive
                // admission and final placement, and suppresses this initial
                // receipt if it rebinds the request's object/version tuple.
                // New creates require an explicitly waiting caller; ordinary
                // asynchronous creates, detached writes and foreground repair
                // retain their existing content-delivery paths.
                if policy.min_commit_replicas == 1
                    && !foreground_replica_repair
                    && options.dispatch != DfsWriteDispatch::Detached
                    && (options.dispatch == DfsWriteDispatch::AwaitCommit
                        || (object.content_hash.is_some()
                            && !self
                                .transient_metadata_object_ids
                                .contains(&object.object_id)))
                    && request.requesting_node_id == self.node_id
                    && desired_replica_nodes.contains(&self.node_id)
                {
                    let manifest = self.content.put_object_version(
                        request.object_id.as_str(),
                        request.target_version.as_str(),
                        &bytes,
                    )?;
                    return self.route_distributed_manifest(
                        request,
                        expected_base_version,
                        manifest.content_hash,
                        manifest.size_bytes,
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
                patch: None,
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

        self.validate_distributed_write_admission(
            &request,
            expected_base_version.as_ref(),
            &object,
            &desired_replica_nodes,
            &policy,
            &publication,
        )?;

        if publication.is_default_current() && operation_id.is_none()
            && self.control_plane.is_none()
            && !touch_parent_on_create
            && object.policy == DfsObjectPolicy::ReplicatedEventual
            && policy.min_commit_replicas == 1
            && desired_replica_nodes.len() == 1
            && desired_replica_nodes[0] == self.node_id
            && object.owner_node_id == self.node_id
        {
            let manifest = self.content.put_object_version(
                request.object_id.as_str(),
                request.target_version.as_str(),
                &bytes,
            )?;
            return self.commit_self_only_eventual_manifest(
                request,
                object,
                manifest,
                DfsLocalEventualWrite::Full,
            );
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
            .create_intent_for_publication(
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
                manifest.content_hash.clone(),
                manifest.size_bytes,
            )
            .with_new_intent_transfer_targets(&desired_replica_nodes)
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

    pub(crate) fn track_pending_routed_write(
        &mut self,
        request_id: String,
        object_id: DfsObjectId,
        primary_node_id: NodeId,
        authority_shard: DfsMetadataShardConfig,
        command: DfsDistributedWriteCommand,
        attachment_bytes: Option<bytes::Bytes>,
    ) -> Result<(NetworkedDfsWriteOutcome, PreviousRoutedWriteState), DfsError> {
        self.prune_retained_routed_writes();
        let earlier_unresolved = self
            .writes
            .pending_routed_writes
            .get(&request_id)
            .is_some_and(|pending| {
                pending.canonical_request_id == request_id && pending.result.is_none()
            });
        let registration_error = |error| {
            let error = DfsError::from_network(error);
            if earlier_unresolved {
                DfsError::NetworkDelivery {
                    message: format!(
                        "response registration failed before retry; earlier write remains unresolved: {error}"
                    ),
                    delivery_state: NetworkDeliveryState::PossiblyApplied,
                }
            } else {
                error
            }
        };
        let alias = crate::intent::stable_write_intent_id(
            &command.request.requesting_node_id,
            &command.request.object_id,
            &request_id,
        );
        for key in [&request_id, &alias] {
            if self
                .writes
                .pending_routed_writes
                .get(key)
                .is_some_and(|pending| pending.canonical_request_id != request_id)
            {
                return Err(registration_error(NetworkError::delivery_failed(
                    "register routed write",
                    NetworkDeliveryState::DefinitelyNotSent,
                    "request or stable alias already belongs to another operation",
                )));
            }
        }
        let mut delivery_aliases = self
            .writes
            .pending_routed_writes
            .iter()
            .filter(|(_, pending)| pending.canonical_request_id == request_id)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        delivery_aliases.push(alias.clone());
        let registration_checkpoint = self
            .transport
            .routed_write_response_registration_checkpoint(&request_id);
        self.transport
            .register_routed_write_response_owner(&request_id, &object_id, &delivery_aliases)
            .map_err(registration_error)?;
        let new_aliases = (alias != request_id
            && !self.writes.pending_routed_writes.contains_key(&alias))
        .then_some(alias.clone())
        .into_iter()
        .collect();
        // Registration has no receipt evidence. Its required count is still
        // the exact prepared command's policy; an actual owner response may
        // replace both counts after normal authoritative plan resolution.
        let required_receipts = command.policy.min_commit_replicas;
        let previous = self.writes.pending_routed_writes.insert(
            request_id.clone(),
            PendingRoutedWrite {
                canonical_request_id: request_id.clone(),
                object_id,
                primary_node_id,
                authority_shard,
                command: Some(command),
                attachment_bytes,
                patch_attachment: None,
                authority_redirects_remaining: DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT,
                remote_intent_id: None,
                durable_receipts: 0,
                required_receipts,
                result: None,
                compact_retained_at: None,
            },
        );
        // Both service alias state and delivery custody exist before transport
        // dispatch, including completion arriving before the first Pending.
        self.install_pending_routed_write_alias(&request_id, alias);
        Ok((
            NetworkedDfsWriteOutcome::Pending {
                intent_id: request_id,
                durable_receipts: 0,
                required_receipts,
            },
            PreviousRoutedWriteState {
                pending: previous,
                registration_checkpoint,
                new_aliases,
            },
        ))
    }

    /// Dispatch failure cannot erase an earlier attempt with the same stable
    /// identity. In particular, a definitely-unsent retry says nothing about
    /// whether its predecessor committed. Keep the compact late-response owner
    /// for ambiguous delivery, without retaining or automatically resending the
    /// body; an explicit exact-ID retry reconciles the primary's durable record.
    pub(crate) fn finish_routed_write_dispatch(
        &mut self,
        request_id: &str,
        previous: PreviousRoutedWriteState,
        shard: &DfsMetadataShardConfig,
        primary_node_id: &NodeId,
        sent: Result<bool, DfsError>,
    ) -> Result<(), DfsError> {
        let unroutable = matches!(sent, Ok(false));
        let error = match sent {
            Ok(true) => return Ok(()),
            Ok(false) => Self::metadata_primary_unroutable_error(shard, primary_node_id),
            Err(error) => error,
        };
        let earlier_unresolved = previous
            .pending
            .as_ref()
            .is_some_and(|pending| pending.result.is_none());
        let object_id = self.writes.pending_routed_writes[request_id]
            .object_id
            .clone();
        let definitely_not_sent = matches!(
            error,
            DfsError::NetworkDelivery {
                delivery_state: NetworkDeliveryState::DefinitelyNotSent,
                ..
            }
        ) || unroutable;
        if definitely_not_sent {
            if let Some(prior) = previous.pending {
                for alias in previous.new_aliases {
                    self.writes.pending_routed_writes.remove(&alias);
                }
                self.transport.rollback_routed_write_response_registration(
                    request_id,
                    previous.registration_checkpoint,
                );
                let previous = prior;
                let had_retry_body =
                    previous.command.is_some() || previous.attachment_bytes.is_some();
                self.writes
                    .pending_routed_writes
                    .insert(request_id.to_string(), previous);
                if had_retry_body {
                    self.compact_pending_routed_write(request_id);
                }
            } else {
                self.abandon_pending_routed_write(request_id);
            }
        } else {
            // The latest dispatched authority owns reconciliation if this
            // attempt also became ambiguous. Restoring an older assignment
            // here would reject a valid completion from the new primary.
            self.compact_pending_routed_write(request_id);
        }
        let message = format!("DFS write operation {request_id} for object {object_id}: {error}");
        if earlier_unresolved {
            return Err(DfsError::NetworkDelivery {
                message: format!("{message}; an earlier attempt remains unresolved"),
                delivery_state: NetworkDeliveryState::PossiblyApplied,
            });
        }
        match error {
            DfsError::NetworkDelivery { delivery_state, .. } => Err(DfsError::NetworkDelivery {
                message,
                delivery_state,
            }),
            DfsError::Network { .. } => Err(DfsError::Network { message }),
            error => Err(error),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.write.route.bytes",
        skip_all,
        fields(stage = "dfs.write.route.bytes")
    )]
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
        let exclusive_create = options.creation.is_exclusive();
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
        let metadata_journal_after_sequence = if !detach_request
            && replay_response_journal
            && shard.primary_node_id != self.node_id
        {
            self.local_metadata_journal_after_sequence_for_shard(&shard)?
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
            patch: None,
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
        // Register before entering the transport: a response may arrive even
        // when the transport returns an ambiguous send/response failure.
        let (outcome, previous) = self.track_pending_routed_write(
            request_id.clone(),
            object.object_id,
            primary_node_id.clone(),
            shard.clone(),
            command,
            (!bytes.is_empty()).then_some(bytes),
        )?;
        // The body and pending redirect state share one payload allocation.
        // Dispatch it before returning even for a logically detached metadata
        // request; queueing it would require persisting or cloning the complete
        // payload on the requester, defeating direct streamed ingest.
        let send_started = Instant::now();
        let sent = match self.transport.send_routed_call(&primary_node_id, call) {
            Ok(()) => Ok(true),
            Err(NetworkError::UnknownNode { .. }) => Ok(false),
            Err(error) => Err(DfsError::from_network(error)),
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
            matches!(sent, Ok(true))
        ));
        self.finish_routed_write_dispatch(&request_id, previous, &shard, &primary_node_id, sent)?;
        Ok(outcome)
    }

    #[allow(clippy::too_many_arguments)]
    fn route_distributed_patch(
        &mut self,
        request: DfsWriteRequest,
        base_version: DfsVersion,
        offset: u64,
        source: DfsPatchSource<'_>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        foreground_replica_repair: bool,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let request_id = self.next_request_id();
        let (hash, size, attachment, attachment_bytes, patch_attachment) = match source {
            DfsPatchSource::Bytes(bytes) => {
                let bytes = bytes::Bytes::copy_from_slice(bytes);
                let hash = crate::content_store::content_hash(&bytes);
                let size = bytes.len() as u64;
                (
                    hash,
                    size,
                    DfsOutboundAttachment::from_shared_bytes_with_transport_integrity(
                        bytes.clone(),
                    ),
                    Some(bytes),
                    None,
                )
            }
            DfsPatchSource::Reader(reader) => {
                let blob =
                    std::sync::Arc::new(self.content.capture_quarantined_patch_reader(reader)?);
                (
                    blob.content_hash.clone(),
                    blob.size_bytes,
                    DfsOutboundAttachment::from_quarantined_patch(std::sync::Arc::clone(&blob))?,
                    None,
                    Some(crate::service_state::OwnedRoutedPatchAttachment(blob)),
                )
            }
        };
        offset
            .checked_add(size)
            .ok_or_else(|| DfsError::Distributed {
                message: "routed patch range overflows u64".into(),
            })?;
        let base_hash = object
            .content_hash
            .clone()
            .ok_or_else(|| DfsError::Distributed {
                message: "routed patch has no observed base content hash".into(),
            })?;
        let base_size = object
            .content_size_bytes
            .ok_or_else(|| DfsError::Distributed {
                message: "routed patch has no observed base content length".into(),
            })?;
        let primary = shard.primary_node_id.clone();
        let after = if primary == self.node_id {
            None
        } else {
            self.local_metadata_journal_after_sequence_for_shard(&shard)?
        };
        let command = DfsDistributedWriteCommand {
            publication: Default::default(),
            patch: Some(crate::DfsDistributedWritePatch {
                base_content_hash: base_hash,
                base_size_bytes: base_size,
                offset,
                patch_hash: hash.clone(),
                patch_size_bytes: size,
            }),
            request_id: request_id.clone(),
            request,
            expected_base_version: Some(base_version),
            metadata_journal_after_sequence: after,
            bytes: Vec::new(),
            content_hash: Some(hash),
            content_size_bytes: Some(size),
            requester_blob_pre_staged: false,
            desired_replica_nodes,
            policy,
            object_metadata: Some(object.clone()),
            authority_shard: Some(shard.clone()),
            touch_parent_on_create: false,
            foreground_replica_repair,
            exclusive_create: false,
        };
        let message = command.clone().into_network_message();
        let routed = self.next_dfs_routed_message(&primary, message);
        let (outcome, previous) = self.track_pending_routed_write(
            request_id.clone(),
            object.object_id,
            primary.clone(),
            shard.clone(),
            command,
            attachment_bytes,
        )?;
        if let Some(pending) = self.writes.pending_routed_writes.get_mut(&request_id) {
            pending.patch_attachment = patch_attachment;
        }
        let sent = self
            .transport
            .send_routed_call(&primary, DfsRoutedCall::with_attachment(routed, attachment))
            .map(|()| true)
            .or_else(|error| match error {
                NetworkError::UnknownNode { .. } => Ok(false),
                error => Err(DfsError::from_network(error)),
            });
        self.finish_routed_write_dispatch(&request_id, previous, &shard, &primary, sent)?;
        Ok(outcome)
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.write.route.manifest",
        skip_all,
        fields(stage = "dfs.write.route.manifest")
    )]
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
        let trace_started = Instant::now();
        let touch_parent_on_create = options.touch_parent_on_create;
        let exclusive_create = options.creation.is_exclusive();
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
        let metadata_sequence_started = Instant::now();
        let metadata_journal_after_sequence = if !detach_request
            && replay_response_journal
            && shard.primary_node_id != self.node_id
        {
            self.local_metadata_journal_after_sequence_for_shard(&shard)?
        } else {
            None
        };
        let metadata_sequence_elapsed = metadata_sequence_started.elapsed();
        // Stable retained writes need the captured body at Main before pending
        // restoration; detached manifest delivery keeps its existing contract.
        if !options.publication.is_current() && !detach_request {
            return self.route_distributed_captured_reader(
                request,
                expected_base_version,
                content_hash,
                content_size_bytes,
                desired_replica_nodes,
                policy,
                object,
                shard,
                metadata_journal_after_sequence,
                DfsDistributedWriteOptions {
                    operation_id: Some(request_id),
                    ..options
                },
                exclusive_create,
                trace_started,
                None,
                metadata_sequence_elapsed,
            );
        }
        let primary_node_id = shard.primary_node_id.clone();
        let command = DfsDistributedWriteCommand {
            publication: options.publication.clone(),
            patch: None,
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
        let (outcome, previous) = self.track_pending_routed_write(
            request_id.clone(),
            object.object_id,
            primary_node_id.clone(),
            shard.clone(),
            command,
            None,
        )?;
        let sent = if detach_request {
            self.send_dfs_message_if_routable_detached(&primary_node_id, message)
        } else {
            self.send_dfs_message_if_routable(&primary_node_id, message)
        };
        self.finish_routed_write_dispatch(&request_id, previous, &shard, &primary_node_id, sent)?;
        Ok(outcome)
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.write.route.reader",
        skip_all,
        fields(stage = "dfs.write.route.reader")
    )]
    pub(crate) fn route_distributed_reader<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        mut reader: R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.route_distributed_reader_impl(
            request,
            expected_base_version,
            &mut reader,
            desired_replica_nodes,
            policy,
            object,
            shard,
            options,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn route_distributed_reader_impl(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        reader: &mut dyn Read,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
        options: DfsDistributedWriteOptions,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let operation_id = options.operation_id;

        if !desired_replica_nodes.contains(&request.requesting_node_id)
            && shard.primary_node_id != self.node_id
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "reader-backed routed write for {} must include requesting node {} as a desired replica",
                    request.object_id, request.requesting_node_id
                ),
            });
        }
        let trace_started = Instant::now();
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
        // The captured manifest route chooses current manifest delivery or
        // retained owned-body delivery without reading the caller again.
        if policy.min_commit_replicas == 1 {
            return self.route_distributed_manifest(
                request,
                expected_base_version,
                manifest.content_hash,
                manifest.size_bytes,
                desired_replica_nodes,
                policy,
                object,
                shard,
                DfsDistributedWriteOptions {
                    operation_id: Some(request_id),
                    replay_response_journal: true,
                    ..options
                },
            );
        }
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        let metadata_sequence_started = Instant::now();
        let metadata_journal_after_sequence = if shard.primary_node_id == self.node_id {
            None
        } else {
            self.local_metadata_journal_after_sequence_for_shard(&shard)?
        };
        let metadata_sequence_elapsed = metadata_sequence_started.elapsed();
        self.route_distributed_captured_reader(
            request,
            expected_base_version,
            manifest.content_hash,
            manifest.size_bytes,
            desired_replica_nodes,
            policy,
            object,
            shard,
            metadata_journal_after_sequence,
            DfsDistributedWriteOptions {
                operation_id: Some(request_id),
                ..options
            },
            false,
            trace_started,
            Some(capture_elapsed),
            metadata_sequence_elapsed,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn route_distributed_captured_reader(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        captured_hash: String,
        captured_size: u64,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
        metadata_journal_after_sequence: Option<u64>,
        options: DfsDistributedWriteOptions,
        exclusive_create: bool,
        trace_started: Instant,
        capture_elapsed: Option<std::time::Duration>,
        metadata_sequence_elapsed: std::time::Duration,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let touch_parent_on_create = options.touch_parent_on_create;
        let foreground_replica_repair = options.repair == DfsWriteRepair::Foreground;
        let trace_object_id = request.object_id.clone();
        let trace_version = request.target_version.clone();
        let trace_requester = request.requesting_node_id.clone();
        let trace_desired_count = desired_replica_nodes.len();
        let trace_policy_min = policy.min_commit_replicas;
        let trace_policy_desired = policy.desired_replicas;
        let request_id = match options.operation_id {
            Some(operation_id) => {
                crate::service_routed_write::validate_write_operation_id(&operation_id)?;
                operation_id
            }
            None => self.next_request_id(),
        };
        let primary_node_id = shard.primary_node_id.clone();
        let (command_content_hash, command_content_size_bytes) = if captured_size == 0 {
            (None, None)
        } else {
            (Some(captured_hash.clone()), Some(captured_size))
        };
        let command = DfsDistributedWriteCommand {
            publication: options.publication.clone(),
            patch: None,
            request_id: request_id.clone(),
            request,
            expected_base_version,
            metadata_journal_after_sequence,
            bytes: Vec::new(),
            content_hash: command_content_hash,
            content_size_bytes: command_content_size_bytes,
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
        let routed = self.next_dfs_routed_message(&primary_node_id, message);
        let call = if captured_size == 0 {
            DfsRoutedCall::value(routed)
        } else {
            let (_, body_reader) = self.open_content_hash_stream(
                &command.request.object_id,
                &command.request.target_version,
                &captured_hash,
                Some(captured_size),
            )?;
            DfsRoutedCall::with_attachment(
                routed,
                DfsOutboundAttachment::from_reader(body_reader, captured_size, &captured_hash),
            )
        };
        let (outcome, previous) = self.track_pending_routed_write(
            request_id.clone(),
            object.object_id,
            primary_node_id.clone(),
            shard.clone(),
            command,
            None,
        )?;
        let send_started = Instant::now();
        let sent = match self.transport.send_routed_call(&primary_node_id, call) {
            Ok(()) => Ok(true),
            Err(NetworkError::UnknownNode { .. }) => Ok(false),
            Err(error) => Err(DfsError::from_network(error)),
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
            captured_size,
            captured_size > 0,
            trace_desired_count,
            trace_policy_min,
            trace_policy_desired,
            capture_elapsed
                .map(|elapsed| elapsed.as_millis().to_string())
                .unwrap_or_else(|| "unavailable".to_string()),
            metadata_sequence_elapsed.as_millis(),
            send_elapsed.as_millis(),
            trace_started.elapsed().as_millis(),
            matches!(sent, Ok(true))
        ));
        self.finish_routed_write_dispatch(&request_id, previous, &shard, &primary_node_id, sent)?;
        Ok(outcome)
    }
}

#[cfg(test)]
mod initial_receipt_tests {
    use super::*;

    #[test]
    fn no_snapshot_replica_plan_keeps_counts_and_selected_members() {
        let root = sinomo_testdir::TestDir::new("no-snapshot-plan-counts");
        let owner = NodeId::from_legacy_named("historical-plan-owner").unwrap();
        let targets = ["selected-a", "selected-b", "selected-c"]
            .map(|name| NodeId::from_legacy_named(name).unwrap());
        let service = NetworkedDfsService::new(
            crate::test_runtime::DfsTestRuntime::local_transport(owner.clone()),
            LocalContentStore::open_for_inspection(root.join("content")),
        );
        assert!(service.placement_snapshot_cache().snapshot().is_none());
        // Pure plan DATA: no metadata is published and no authority is fabricated.
        let mut object = crate::namespace::distributed_sifs_bootstrap_objects(owner.clone())
            .into_iter()
            .find(|object| object.path == "/services")
            .unwrap();
        for (w, n, object_policy) in [
            (1, 2, DfsObjectPolicy::ReplicatedEventual),
            (2, 3, DfsObjectPolicy::ReplicatedEventual),
            (2, 2, DfsObjectPolicy::ReplicatedConfirmed),
        ] {
            object.policy = object_policy;
            let selected = targets[..n].to_vec();
            let zones: std::collections::BTreeMap<NodeId, String> = targets
                .iter()
                .enumerate()
                .map(|(index, node)| (node.clone(), format!("zone-{}", index % 2)))
                .collect();
            for historical_owner in [targets[0].clone(), owner.clone()] {
                object.owner_node_id = historical_owner;
                for forbidden in [Vec::new(), vec![owner.clone()]] {
                    let policy = DfsReplicationPolicy::new(w, n)
                        .unwrap()
                        .with_placement_constraints(
                            vec![targets[0].clone()],
                            forbidden,
                            2,
                            zones.clone(),
                        )
                        .unwrap();
                    let (actual_nodes, actual_policy) = service
                        .normalize_distributed_write_replication_plan(
                            &object,
                            &object.version,
                            selected.clone(),
                            policy.clone(),
                        )
                        .unwrap();
                    assert_eq!(
                        actual_policy, policy,
                        "historical owner must not change W/N or constraints"
                    );
                    assert_eq!(
                        actual_nodes, selected,
                        "historical owner is not another selected candidate"
                    );
                }
            }
        }
    }

    #[test]
    fn no_snapshot_replica_plan_rejects_invalid_constraints_and_candidates() {
        let root = sinomo_testdir::TestDir::new("no-snapshot-plan-invalid");
        let owner = NodeId::from_legacy_named("unselected-owner").unwrap();
        let [a, b, c] =
            ["plan-a", "plan-b", "plan-c"].map(|name| NodeId::from_legacy_named(name).unwrap());
        let service = NetworkedDfsService::new(
            crate::test_runtime::DfsTestRuntime::local_transport(owner.clone()),
            LocalContentStore::open_for_inspection(root.join("content")),
        );
        let mut object = crate::namespace::distributed_sifs_bootstrap_objects(owner)
            .into_iter()
            .find(|object| object.path == "/services")
            .unwrap();
        object.policy = DfsObjectPolicy::ReplicatedEventual;
        let plain = DfsReplicationPolicy::new(1, 2).unwrap();
        for selected in [
            vec![a.clone()],
            vec![a.clone(), a.clone()],
            vec![a.clone(), b.clone(), c.clone()],
        ] {
            assert!(
                service
                    .normalize_distributed_write_replication_plan(
                        &object,
                        &object.version,
                        selected,
                        plain.clone(),
                    )
                    .is_err()
            );
        }
        for policy in [
            plain
                .clone()
                .with_placement_constraints(vec![c.clone()], Vec::new(), 0, Default::default())
                .unwrap(),
            plain
                .clone()
                .with_placement_constraints(Vec::new(), vec![b.clone()], 0, Default::default())
                .unwrap(),
            plain
                .clone()
                .with_placement_constraints(
                    Vec::new(),
                    Vec::new(),
                    2,
                    [
                        (a.clone(), "same".into()),
                        (b.clone(), "same".into()),
                        (c, "other".into()),
                    ]
                    .into(),
                )
                .unwrap(),
        ] {
            assert!(
                service
                    .normalize_distributed_write_replication_plan(
                        &object,
                        &object.version,
                        vec![a.clone(), b.clone()],
                        policy,
                    )
                    .is_err()
            );
        }
        object.policy = DfsObjectPolicy::ReplicatedConfirmed;
        assert!(
            service
                .normalize_distributed_write_replication_plan(
                    &object,
                    &object.version,
                    vec![a, b],
                    plain,
                )
                .is_err(),
            "confirmed writes must not accept W<N"
        );
    }

    #[test]
    fn native_no_snapshot_routed_producer_preserves_managed_counts() {
        let name = "native-no-snapshot-routed-counts";
        let path = "/services/native-no-snapshot-routed-counts.bin";
        let (runtime, initial, shard) =
            crate::test_support::current_native_metadata_service(name, 3, path).unwrap();
        drop(initial);
        assert_eq!(shard.replica_node_ids.len(), 2);
        let primary = shard.primary_node_id.clone();
        let requester = shard.replica_node_ids[0].clone();
        let other = shard.replica_node_ids[1].clone();
        let mut caller = runtime
            .client(&requester)
            .unwrap()
            .with_placement_snapshot_cache(Default::default());
        // Model the ordinary cold CLI handle, not a new author: it has no local
        // publication provider or placement snapshot. Native Main still owns
        // authenticated dispatch, current configuration and durable publication.
        caller.publication_provider = Default::default();
        assert!(caller.transport.delegates_local_namespace_authority());
        assert!(caller.placement_snapshot_cache().snapshot().is_none());
        assert!(caller.publication_provider.current().is_none());
        let (resolved, _, policy) = caller
            .managed_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 3)
            .unwrap();
        assert!(
            resolved.source_path.is_none(),
            "no managed override supplies the count"
        );
        assert_eq!(
            (policy.min_commit_replicas, policy.desired_replicas),
            (1, 2)
        );
        // Both hints are real admitted members. Ingress replaces the requester
        // slot with the remote primary; normalization must not add it back.
        let bytes = b"actual cold requester write without count inflation";
        let stat = caller
            .posix_write_file_with_known_parent_deferred_local_replay(
                path,
                bytes,
                crate::DfsWriteOptions {
                    policy,
                    replica_candidates: vec![requester.clone(), other],
                    wait_for_commit: false,
                    foreground_replica_repair: false,
                },
            )
            .expect("the original routed write must complete without a test retry");
        drop(caller);
        let authority = runtime.metadata_primary_service(path).unwrap();
        let version = authority
            .metadata_backend()
            .unwrap()
            .get_version(&stat.object_id, &stat.version)
            .unwrap()
            .unwrap();
        assert!(version.visible);
        assert_eq!(
            version.content_hash.as_deref(),
            Some(crate::content_hash(bytes).as_str())
        );
        let durability = version.content_durability.unwrap();
        let completed = runtime.rpc_observations().iter().any(|call| {
            call.source == requester
                && call.target == primary
                && call.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && call.origin == crate::test_runtime::RpcOrigin::RegisteredHost
                && call.phase == crate::test_runtime::RpcPhase::Completed
                && call.outcome_error.is_none()
        });
        eprintln!(
            "native-no-snapshot-counts {}",
            serde_json::json!({
                "object": stat.object_id, "version": stat.version, "requester": requester,
                "primary": primary, "durability": durability,
                "native_registered_write_completed": completed,
            })
        );
        drop(authority);
        runtime.close().unwrap();
        assert!(
            completed,
            "the actual authenticated Main producer must have completed"
        );
        assert_eq!(
            (
                durability.policy.min_commit_replicas,
                durability.policy.desired_replicas
            ),
            (1, 2)
        );
        assert_eq!(durability.desired_replica_node_ids.len(), 2);
    }

    #[test]
    fn authoritative_replica_plan_does_not_append_historical_owner() {
        for (w, n, object_policy) in [
            (1, 2, DfsObjectPolicy::ReplicatedEventual),
            (2, 3, DfsObjectPolicy::ReplicatedEventual),
            (2, 2, DfsObjectPolicy::ReplicatedConfirmed),
        ] {
            let name = format!("native-placement-historical-owner-{w}-{n}");
            let (runtime, service, config) =
                crate::test_support::current_native_metadata_service(&name, n + 1, "/services")
                    .unwrap();
            let historical_owner = config.primary_node_id;
            drop(service);
            let selected = crate::test_runtime::DfsTestRuntime::ordered_node_ids(&name, n + 1)
                .into_iter()
                .filter(|node| *node != historical_owner)
                .collect::<Vec<_>>();
            let path = format!("/services/planned-{w}-{n}.bin");
            let mut policy = crate::DfsPlacementPolicy::new(object_policy.clone(), w, n).unwrap();
            policy.required_node_ids = selected.clone();
            runtime
                .apply_placement_policy(
                    &historical_owner,
                    &path,
                    crate::DfsPlacementScope::Subtree,
                    policy,
                )
                .unwrap();
            // Candidate node hints are not a placement policy. Consume the
            // actual managed constraint at the current owning Core producer.
            let mut service = runtime.metadata_primary_service(&path).unwrap();
            let native_placement = service
                .placement_snapshot_cache()
                .snapshot()
                .expect("the actual mounted host already published its placement evidence");
            assert!(!native_placement.nodes.is_empty());
            let planned_policy = service
                .managed_write_replication_policy_for_path(&path, object_policy, n)
                .unwrap();
            assert_eq!(planned_policy.min_commit_replicas, w);
            assert_eq!(planned_policy.desired_replicas, n);
            assert_eq!(
                planned_policy
                    .required_node_ids
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                selected.iter().cloned().collect()
            );
            let stat = service
                .posix_write_file(
                    &path,
                    b"actual selected current replicas",
                    crate::DfsWriteOptions {
                        policy: planned_policy,
                        replica_candidates: selected.clone(),
                        wait_for_commit: true,
                        foreground_replica_repair: true,
                    },
                )
                .unwrap();
            // Reopen only the actual current Core view after its complete
            // foreground replica work, without relying on the producer cache.
            drop(service);
            let service = runtime.metadata_primary_service(&path).unwrap();
            let object = service
                .metadata_backend()
                .unwrap()
                .get_object(&stat.object_id)
                .unwrap()
                .unwrap();
            assert_eq!(object.owner_node_id, historical_owner);
            assert_eq!(
                object.version, stat.version,
                "inspect the actual published generation"
            );
            assert_eq!(
                object.content_hash.as_deref(),
                Some(crate::content_hash(b"actual selected current replicas").as_str())
            );
            assert_eq!(
                object
                    .current_locations
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                selected.iter().cloned().collect(),
                "actual object {object:?}; native write history {:?}",
                service
                    .metadata_backend()
                    .unwrap()
                    .object_write_intent_history(&stat.object_id)
                    .unwrap(),
            );
            let policy = DfsReplicationPolicy::new(w, n)
                .unwrap()
                .with_placement_constraints(selected.clone(), Vec::new(), 0, Default::default())
                .unwrap();
            let (actual_nodes, actual_policy) = service
                .normalize_distributed_write_replication_plan(
                    &object,
                    &crate::namespace::next_version(&stat.version),
                    selected.clone(),
                    policy,
                )
                .unwrap();
            assert_eq!(actual_policy.min_commit_replicas, w);
            assert_eq!(actual_policy.desired_replicas, n);
            assert_eq!(
                actual_nodes.into_iter().collect::<BTreeSet<_>>(),
                selected.into_iter().collect()
            );
            drop(service);
            runtime.close().unwrap();
        }
    }

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

#[cfg(test)]
mod routed_patch_native_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn main_patch_exact_replay_and_changed_descriptor_use_actual_immutable_layout() {
        let node = NodeId::generate();
        let mut runtime =
            crate::test_runtime::DfsTestRuntime::new("main-patch-exact-replay", node.clone())
                .unwrap();
        runtime.start_nodes().unwrap();
        let mut client = runtime.client(&node).unwrap();
        let path = "/services/main-patch-exact.bin";
        let initial = vec![0x31; 192 * 1024];
        let before = client
            .posix_write_file(
                path,
                &initial,
                crate::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        let object = client
            .current_read_object_metadata_distributed(&before.object_id)
            .unwrap();
        let shard = client
            .select_metadata_shard_for_object(&object)
            .unwrap()
            .unwrap();
        let patch = b"actual-normal-patch";
        let offset = 64 * 1024;
        let request = DfsWriteRequest {
            file_system_id: before.file_system_id.clone(),
            object_id: before.object_id.clone(),
            target_version: crate::namespace::next_version(&before.version),
            requesting_node_id: node.clone(),
        };
        // This is the normal canonical producer, not a manually authored wire
        // header. Copy only its actual unresolved command before consuming the
        // native Main reply queued to the original caller.
        let pending = client
            .route_distributed_patch(
                request,
                before.version.clone(),
                offset,
                DfsPatchSource::Bytes(patch),
                vec![node.clone()],
                DfsReplicationPolicy::new(1, 1).unwrap(),
                false,
                object,
                shard,
            )
            .unwrap();
        let NetworkedDfsWriteOutcome::Pending { ref intent_id, .. } = pending else {
            panic!("canonical producer must retain its caller response custody");
        };
        let command = client.writes.pending_routed_writes[intent_id]
            .command
            .as_ref()
            .unwrap()
            .clone();
        assert!(command.patch.is_some());
        assert!(matches!(
            client.settle_ready_write_outcome(pending, false).unwrap(),
            NetworkedDfsWriteOutcome::Committed { .. }
        ));
        let committed = client.posix_stat_distributed(path).unwrap();
        let mut expected = initial;
        expected[offset as usize..offset as usize + patch.len()].copy_from_slice(patch);
        assert_eq!(client.posix_read_file_distributed(path).unwrap(), expected);
        assert!(
            !client
                .content
                .has_blob(committed.content_hash.as_ref().unwrap())
                .unwrap()
        );

        for changed in [false, true] {
            let mut replay = command.clone();
            replay.bytes = patch.to_vec();
            if changed {
                replay.patch.as_mut().unwrap().offset += 1;
            }
            let routed =
                client.next_dfs_routed_message(&node, replay.clone().into_network_message());
            let request = crate::dfs_routed_message_rpc_request(&routed).unwrap();
            let response = runtime
                .rpc_request(&node, &node, request, Duration::from_secs(5))
                .unwrap();
            let response = crate::decode_dfs_message_rpc_response(&response).unwrap();
            let result = response
                .outbound_messages
                .iter()
                .find_map(|message| match &message.message {
                    DfsNetworkMessage::DistributedWriteResponse(actual)
                        if actual.request_id == replay.request_id =>
                    {
                        Some(&actual.result)
                    }
                    _ => None,
                })
                .expect("actual Main must return this exact canonical operation's result");
            if changed {
                assert!(matches!(
                    result,
                    crate::DfsDistributedWriteResult::Rejected { .. }
                ));
            } else {
                assert!(matches!(
                    result,
                    crate::DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { .. },
                    }
                ));
            }
            assert_eq!(client.posix_stat_distributed(path).unwrap(), committed);
            assert_eq!(client.posix_read_file_distributed(path).unwrap(), expected);
        }
        drop(client);
        runtime.close().unwrap();
    }

    #[test]
    fn main_patch_rejects_obsolete_base_before_publishing_new_content() {
        let node = NodeId::generate();
        let mut runtime =
            crate::test_runtime::DfsTestRuntime::new("main-patch-obsolete-base", node.clone())
                .unwrap();
        runtime.start_nodes().unwrap();
        let mut client = runtime.client(&node).unwrap();
        let path = "/services/main-patch-stale.bin";
        let options = crate::DfsWriteOptions::replicated_eventual(vec![node.clone()]);
        let before = client
            .posix_write_file(path, b"original", options.clone())
            .unwrap();
        let old_object = client
            .current_read_object_metadata_distributed(&before.object_id)
            .unwrap();
        let shard = client
            .select_metadata_shard_for_object(&old_object)
            .unwrap()
            .unwrap();
        let current = client
            .posix_write_file(path, b"current replacement", options)
            .unwrap();
        let candidate_hash = crate::content_store::content_hash(b"forbidden obsolete patch");
        assert!(!client.content.has_blob(&candidate_hash).unwrap());
        let pending = client
            .route_distributed_patch(
                DfsWriteRequest {
                    file_system_id: before.file_system_id.clone(),
                    object_id: before.object_id.clone(),
                    target_version: crate::namespace::next_version(&current.version),
                    requesting_node_id: node.clone(),
                },
                before.version,
                0,
                DfsPatchSource::Bytes(b"forbidden obsolete patch"),
                vec![node.clone()],
                DfsReplicationPolicy::new(1, 1).unwrap(),
                false,
                old_object,
                shard,
            )
            .unwrap();
        assert!(client.settle_ready_write_outcome(pending, false).is_err());
        assert_eq!(client.posix_stat_distributed(path).unwrap(), current);
        assert_eq!(
            client.posix_read_file_distributed(path).unwrap(),
            b"current replacement"
        );
        assert!(
            !client.content.has_blob(&candidate_hash).unwrap(),
            "rejected old base must leave actual delta DATA quarantined and unbound"
        );
        drop(client);
        runtime.close().unwrap();
    }
}
