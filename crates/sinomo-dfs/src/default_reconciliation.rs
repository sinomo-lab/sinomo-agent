//! State contracts for default distributed-DFS reconciliation.
//!
//! Process hosts own subscriptions and wake delivery. This module owns the
//! exact projected generation, handoff identity, and authority predicates the
//! reconciliation algorithm consumes.

use crate::default_projection::{
    DefaultDfsCommittedProjectionObserver, default_dfs_reconciliation_exact_input_keys,
    default_dfs_reconciliation_exact_retry_key, default_dfs_reconciliation_prefixes,
};
use crate::{
    DfsFileSystemId, DfsMetadataShardConfig, DfsMetadataShardId, DfsMetadataShardMembershipHandoff,
    DfsMetadataShardMembershipHandoffProof,
};
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::kv::{KvEntry, KvError, KvRevision, KvVersion};
use sinomo_core::network::ClusterPaths;
use sinomo_core::node::NodeId;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, Weak};

mod primary_transfer;
pub(crate) use primary_transfer::{
    capture_primary_transfer_selection, notify_primary_transfer_checkpoint,
};

const DEFAULT_DFS_RECONCILIATION_PROJECTION_REPLAN_MAX_ATTEMPTS: usize = 8;

static DEFAULT_DFS_RECONCILIATION_WAKES: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<DefaultDfsReconciliationWake>>>,
> = OnceLock::new();

#[derive(Debug)]
struct DefaultDfsReconciliationWake {
    generation_tx: tokio::sync::watch::Sender<u64>,
    primary_transfer: Mutex<primary_transfer::PrimaryTransferSelectionState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DefaultDfsPermitGeneration {
    pub modify_revision: KvRevision,
    pub version: KvVersion,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DefaultDfsReconciliationProjection {
    pub revision: Option<KvRevision>,
    pub inputs: Vec<(String, Vec<u8>)>,
    pub placement_authority_permit: Option<DefaultDfsPermitGeneration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DefaultDfsReconciliationWork {
    pub inputs: Vec<(String, Vec<u8>)>,
    pub placement_authority_permit: Option<DefaultDfsPermitGeneration>,
    pub local_generation: u64,
    pub reconcile_defaults: bool,
    pub recover_prior_process_permit: bool,
    pub refresh_before_recovery: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DefaultDfsExactRetryGeneration {
    pub inputs: Vec<(String, Vec<u8>)>,
    pub placement_authority_permit: Option<DefaultDfsPermitGeneration>,
    pub local_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DefaultDfsParkedHandoff {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: DfsMetadataShardId,
    pub handoff_id: String,
    pub retiring_epoch: u64,
    pub desired_epoch: u64,
    wake_keys: [String; 4],
}

impl DefaultDfsParkedHandoff {
    pub fn new(
        file_system_id: DfsFileSystemId,
        shard_id: String,
        handoff_id: String,
        retiring_epoch: u64,
        desired_epoch: u64,
    ) -> Self {
        let shard = shard_id.as_str();
        let wake_keys = [
            crate::metadata_shard_key(&file_system_id, shard),
            crate::metadata_shard_membership_handoff_key(&file_system_id, shard),
            crate::metadata_shard_membership_handoff_proof_key(&file_system_id, shard),
            crate::metadata_shard_head_watermark_key(&file_system_id, shard),
        ];
        Self {
            file_system_id,
            shard_id: DfsMetadataShardId::new(shard_id),
            handoff_id,
            retiring_epoch,
            desired_epoch,
            wake_keys,
        }
    }

    #[doc(hidden)]
    pub fn owns_key(&self, key: &str) -> bool {
        self.wake_keys.iter().any(|owned| owned == key)
    }

    pub fn wake_inputs(&self, inputs: &[(String, Vec<u8>)]) -> Vec<(String, Vec<u8>)> {
        inputs
            .iter()
            .filter(|(key, _)| self.wake_keys.iter().any(|owned| owned == key))
            .cloned()
            .collect()
    }

    pub fn is_owned_by_retiring_primary(
        &self,
        local_node_id: &NodeId,
        inputs: &[(String, Vec<u8>)],
    ) -> bool {
        let handoff_key = &self.wake_keys[1];
        inputs
            .iter()
            .find(|(key, _)| key == handoff_key)
            .and_then(|(_, value)| {
                sinomo_core::kv::decode_kv_value::<DfsMetadataShardMembershipHandoff>(value).ok()
            })
            .is_some_and(|handoff| {
                handoff.handoff_id == self.handoff_id
                    && handoff.retiring_shard.file_system_id == self.file_system_id
                    && handoff.retiring_shard.shard_id == self.shard_id.0
                    && handoff.retiring_shard.epoch == self.retiring_epoch
                    && handoff.desired_shard.epoch == self.desired_epoch
                    && handoff.retiring_shard.primary_node_id == *local_node_id
            })
    }

    pub fn is_completed_by_desired_primary(
        &self,
        local_node_id: &NodeId,
        inputs: &[(String, Vec<u8>)],
    ) -> bool {
        let active_shard = inputs
            .iter()
            .find(|(key, _)| key == &self.wake_keys[0])
            .and_then(|(_, value)| {
                sinomo_core::kv::decode_kv_value::<DfsMetadataShardConfig>(value).ok()
            });
        let pending_handoff_absent = inputs.iter().all(|(key, _)| key != &self.wake_keys[1]);
        let completion_proof = inputs
            .iter()
            .find(|(key, _)| key == &self.wake_keys[2])
            .and_then(|(_, value)| {
                sinomo_core::kv::decode_kv_value::<DfsMetadataShardMembershipHandoffProof>(value)
                    .ok()
            });
        active_shard
            .zip(completion_proof)
            .is_some_and(|(active, proof)| {
                pending_handoff_absent
                    && active.epoch == self.desired_epoch
                    && completed_promoted_primary_handoff_matches(
                        local_node_id,
                        &active,
                        &self.handoff_id,
                        self.retiring_epoch,
                        &proof,
                    )
            })
    }
}

pub fn default_dfs_locally_owned_pending_handoff(
    local_node_id: &NodeId,
    inputs: &[(String, Vec<u8>)],
) -> Option<DefaultDfsParkedHandoff> {
    let file_system_id = crate::sifs_file_system_id();
    ["root", "services", "users"]
        .into_iter()
        .find_map(|shard_id| {
            let key = crate::metadata_shard_membership_handoff_key(&file_system_id, shard_id);
            let handoff = inputs
                .iter()
                .find(|(input_key, _)| input_key == &key)
                .and_then(|(_, value)| {
                    sinomo_core::kv::decode_kv_value::<DfsMetadataShardMembershipHandoff>(value)
                        .ok()
                })?;
            if handoff.retiring_shard.file_system_id != file_system_id
                || handoff.retiring_shard.shard_id != shard_id
                || handoff.desired_shard.file_system_id != file_system_id
                || handoff.desired_shard.shard_id != shard_id
            {
                return None;
            }
            let parked = DefaultDfsParkedHandoff::new(
                file_system_id.clone(),
                shard_id.to_string(),
                handoff.handoff_id,
                handoff.retiring_shard.epoch,
                handoff.desired_shard.epoch,
            );
            parked
                .is_owned_by_retiring_primary(local_node_id, inputs)
                .then_some(parked)
        })
}

/// Return whether the committed default-DFS projection assigns at least one
/// authoritative metadata shard to this node.
///
/// Cluster-KV voter authority and DFS metadata authority are independent. A
/// learner may therefore need to publish a journal produced by default
/// reconciliation even though it must not publish cluster-control state.
/// Malformed or mismatched inputs never grant local metadata authority.
pub fn default_dfs_reconciliation_local_owns_metadata_primary(
    local_node_id: &NodeId,
    inputs: &[(String, Vec<u8>)],
) -> bool {
    let file_system_id = crate::sifs_file_system_id();
    ["root", "services", "users"].into_iter().any(|shard_id| {
        let key = crate::metadata_shard_key(&file_system_id, shard_id);
        inputs
            .iter()
            .find(|(input_key, _)| input_key == &key)
            .and_then(|(_, value)| {
                sinomo_core::kv::decode_kv_value::<DfsMetadataShardConfig>(value).ok()
            })
            .is_some_and(|shard| {
                shard.file_system_id == file_system_id
                    && shard.shard_id == shard_id
                    && shard.primary_node_id == *local_node_id
            })
    })
}

pub fn completed_promoted_primary_handoff_matches(
    local_node_id: &NodeId,
    active: &DfsMetadataShardConfig,
    handoff_id: &str,
    retiring_epoch: u64,
    proof: &DfsMetadataShardMembershipHandoffProof,
) -> bool {
    proof.handoff_id == handoff_id
        && proof.retiring_shard.file_system_id == active.file_system_id
        && proof.retiring_shard.shard_id == active.shard_id
        && proof.retiring_shard.epoch == retiring_epoch
        && proof.retiring_shard.primary_node_id != proof.desired_shard.primary_node_id
        && retiring_epoch.checked_add(1) == Some(active.epoch)
        && proof.desired_shard == *active
        && active.primary_node_id == *local_node_id
        && proof.acknowledged_node_ids.contains(local_node_id)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DefaultDfsParkedGeneration {
    handoff: DefaultDfsParkedHandoff,
    wake_inputs: Vec<(String, Vec<u8>)>,
    local_generation: u64,
}

/// Owner state for exact-generation default DFS reconciliation.
#[derive(Clone, Debug, Default)]
pub struct DefaultDfsReconciliationState {
    reconciled_inputs: Option<Vec<(String, Vec<u8>)>>,
    reconciled_local_generation: Option<u64>,
    recovered_placement_authority_permit: Option<Option<DefaultDfsPermitGeneration>>,
    parked_generation: Option<DefaultDfsParkedGeneration>,
}

impl DefaultDfsReconciliationState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn startup_reconciliation_pending(&self) -> bool {
        self.reconciled_inputs.is_none()
    }

    pub fn startup_recovery_pending(&self) -> bool {
        self.recovered_placement_authority_permit.is_none()
    }

    pub fn local_replica_progress_pending(&self) -> bool {
        self.reconciled_local_generation.is_none()
    }

    pub fn parked_handoff(&self) -> Option<&DefaultDfsParkedHandoff> {
        self.parked_generation
            .as_ref()
            .map(|parked| &parked.handoff)
    }

    pub fn exact_retry_generation(
        &self,
        projection: DefaultDfsReconciliationProjection,
        local_generation: u64,
        exact_retry_key: impl Fn(&str) -> bool,
    ) -> DefaultDfsExactRetryGeneration {
        DefaultDfsExactRetryGeneration {
            inputs: projection
                .inputs
                .into_iter()
                .filter(|(key, _)| exact_retry_key(key))
                .collect(),
            placement_authority_permit: projection.placement_authority_permit,
            local_generation,
        }
    }

    pub fn pending_work(
        &self,
        current: DefaultDfsReconciliationProjection,
        local_generation: u64,
    ) -> Option<DefaultDfsReconciliationWork> {
        let parked_wake_inputs = self
            .parked_generation
            .as_ref()
            .map(|parked| parked.handoff.wake_inputs(&current.inputs))
            .unwrap_or_default();
        let reconcile_defaults = match self.parked_generation.as_ref() {
            Some(parked) => {
                parked.wake_inputs != parked_wake_inputs
                    || parked.local_generation != local_generation
            }
            None => {
                self.reconciled_inputs.as_ref() != Some(&current.inputs)
                    || self.reconciled_local_generation != Some(local_generation)
            }
        };
        let recover_prior_process_permit = self.recovered_placement_authority_permit.as_ref()
            != Some(&current.placement_authority_permit);
        (reconcile_defaults || recover_prior_process_permit).then_some(
            DefaultDfsReconciliationWork {
                inputs: current.inputs,
                placement_authority_permit: current.placement_authority_permit,
                local_generation,
                reconcile_defaults,
                recover_prior_process_permit,
                refresh_before_recovery: self.recovered_placement_authority_permit.is_none(),
            },
        )
    }

    pub fn complete_default_reconciliation(&mut self, work: &DefaultDfsReconciliationWork) {
        self.reconciled_inputs = Some(work.inputs.clone());
        self.reconciled_local_generation = Some(work.local_generation);
        self.parked_generation = None;
    }

    pub fn complete_after_control_plane_observation(
        &mut self,
        work: &DefaultDfsReconciliationWork,
        local_node_id: &NodeId,
    ) -> bool {
        let completed_parked_generation = self.parked_generation.is_some();
        let promoted_primary_rebind = self.parked_generation.as_ref().is_some_and(|parked| {
            parked
                .handoff
                .is_completed_by_desired_primary(local_node_id, &work.inputs)
        });
        self.complete_default_reconciliation(work);
        if completed_parked_generation {
            self.reconciled_inputs = None;
        }
        promoted_primary_rebind
    }

    pub fn park(&mut self, work: &DefaultDfsReconciliationWork, handoff: DefaultDfsParkedHandoff) {
        let wake_inputs = handoff.wake_inputs(&work.inputs);
        self.reconciled_inputs = Some(work.inputs.clone());
        self.reconciled_local_generation = Some(work.local_generation);
        self.parked_generation = Some(DefaultDfsParkedGeneration {
            handoff,
            wake_inputs,
            local_generation: work.local_generation,
        });
    }

    pub fn complete_prior_process_recovery(&mut self, work: &DefaultDfsReconciliationWork) {
        self.recovered_placement_authority_permit = Some(work.placement_authority_permit.clone());
    }
}

/// Resident committed-input reactor for default DFS reconciliation.
///
/// This owner component retains the bounded projection, committed-delta
/// subscription, parked-handoff filter, local replica-progress wake, and exact
/// completion generations. The embedding daemon executes the resulting work
/// with its lifecycle and authority capabilities.
pub struct DefaultDfsReconciliationRuntime {
    projection_tx: tokio::sync::watch::Sender<DefaultDfsReconciliationProjection>,
    projection_rx: tokio::sync::watch::Receiver<DefaultDfsReconciliationProjection>,
    _delta_subscription: sinomo_core::cluster::ClusterKvCommittedDeltaSubscription,
    _metadata_authority_generation: Option<Arc<crate::DfsMetadataAuthorityRuntime>>,
    _local_wake: Arc<DefaultDfsReconciliationWake>,
    local_wake_rx: tokio::sync::watch::Receiver<u64>,
    parked_subscription: Arc<Mutex<Option<DefaultDfsParkedHandoff>>>,
    owner_state: DefaultDfsReconciliationState,
    local_node_id: NodeId,
    placement_authority_process_identity: Option<crate::DfsPlacementAuthorityProcessIdentity>,
}

impl std::fmt::Debug for DefaultDfsReconciliationRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DefaultDfsReconciliationRuntime")
            .field(
                "startup_reconciliation_pending",
                &self.owner_state.startup_reconciliation_pending(),
            )
            .field(
                "startup_recovery_pending",
                &self.owner_state.startup_recovery_pending(),
            )
            .field(
                "local_replica_progress_pending",
                &self.owner_state.local_replica_progress_pending(),
            )
            .finish_non_exhaustive()
    }
}

impl DefaultDfsReconciliationRuntime {
    pub fn new(
        cluster_paths: &ClusterPaths,
        local_node_id: &NodeId,
        metadata_authority_generation: Option<Arc<crate::DfsMetadataAuthorityRuntime>>,
        placement_authority_process_identity: Option<crate::DfsPlacementAuthorityProcessIdentity>,
    ) -> Result<Self, crate::DfsError> {
        let local_wake = default_dfs_reconciliation_wake(cluster_paths)?;
        let local_wake_rx = local_wake.generation_tx.subscribe();
        let (projection_tx, projection_rx) =
            tokio::sync::watch::channel(DefaultDfsReconciliationProjection::default());
        let parked_subscription = Arc::new(Mutex::new(None));
        let projection_observer = Arc::new(Mutex::new(
            DefaultDfsCommittedProjectionObserver::new_with_parked_subscription(Arc::clone(
                &parked_subscription,
            )),
        ));
        let delta_subscription_observer = Arc::clone(&projection_observer);
        let delta_projection_tx = projection_tx.clone();
        let delta_local_node_id = local_node_id.clone();
        let delta_cluster_paths = cluster_paths.clone();
        let delta_selection_wake = Arc::clone(&local_wake);
        let delta_subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_deltas(
            cluster_paths,
            move |delta| {
                let applied = delta_subscription_observer
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .observe_delta(&delta_projection_tx, delta);
                if applied {
                    primary_transfer::observe_projection(
                        &delta_selection_wake,
                        &delta_projection_tx.borrow(),
                    );
                    return;
                }
                match default_dfs_resident_reconciliation_projection(
                    &delta_cluster_paths,
                    &delta_local_node_id,
                ) {
                    Ok((revision, entries)) => {
                        delta_subscription_observer
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .rebuild_from_entries(&delta_projection_tx, revision, &entries);
                    }
                    Err(error) => tracing::warn!(
                        target: "sinomo.dfs.reconciliation",
                        revision = delta.revision.0,
                        "default DFS committed-delta gap rebuild failed: {error}"
                    ),
                }
                primary_transfer::observe_projection(
                    &delta_selection_wake,
                    &delta_projection_tx.borrow(),
                );
            },
        );
        // Subscribe before sampling. A newer committed delta wins the race and
        // the observer rejects this older bounded resident projection.
        if let Ok((revision, entries)) =
            default_dfs_resident_reconciliation_projection(cluster_paths, local_node_id)
        {
            projection_observer
                .lock()
                .map_err(|_| crate::DfsError::Network {
                    message: "default DFS committed projection observer is poisoned".to_string(),
                })?
                .rebuild_from_entries(&projection_tx, revision, &entries);
        }
        primary_transfer::observe_projection(&local_wake, &projection_tx.borrow());
        Ok(Self {
            projection_tx,
            projection_rx,
            _delta_subscription: delta_subscription,
            _metadata_authority_generation: metadata_authority_generation,
            _local_wake: local_wake,
            local_wake_rx,
            parked_subscription,
            owner_state: DefaultDfsReconciliationState::new(),
            local_node_id: local_node_id.clone(),
            placement_authority_process_identity,
        })
    }

    pub async fn changed(&mut self) {
        loop {
            if self.pending_work().is_some() {
                return;
            }
            self.changed_signal().await;
            if self.pending_work().is_some() {
                return;
            }
        }
    }

    pub async fn changed_signal(&mut self) {
        tokio::select! {
            _ = self.projection_rx.changed() => {}
            _ = self.local_wake_rx.changed() => {}
        }
    }

    pub fn exact_retry_generation(&self) -> DefaultDfsExactRetryGeneration {
        let current = self.normalized_projection(self.projection_rx.borrow().clone());
        self.owner_state.exact_retry_generation(
            current,
            *self.local_wake_rx.borrow(),
            default_dfs_reconciliation_exact_retry_key,
        )
    }

    pub fn pending_work(&mut self) -> Option<DefaultDfsReconciliationWork> {
        let current = self.projection_rx.borrow_and_update().clone();
        let current = self.normalized_projection(current);
        primary_transfer::observe_projection(&self._local_wake, &current);
        let local_generation = *self.local_wake_rx.borrow_and_update();
        self.owner_state.pending_work(current, local_generation)
    }

    pub fn complete_after_control_plane_observation(
        &mut self,
        work: &DefaultDfsReconciliationWork,
        local_node_id: &NodeId,
    ) -> bool {
        let promoted_primary_rebind = self
            .owner_state
            .complete_after_control_plane_observation(work, local_node_id);
        self.clear_parked_subscription();
        promoted_primary_rebind
    }

    pub fn complete_default_reconciliation(&mut self, work: &DefaultDfsReconciliationWork) {
        self.owner_state.complete_default_reconciliation(work);
        self.clear_parked_subscription();
    }

    pub fn park(&mut self, work: &DefaultDfsReconciliationWork, handoff: DefaultDfsParkedHandoff) {
        // Handoff now owns its exact prefix/fence wake inputs; selection waits
        // from an earlier pass must not survive into that different phase.
        primary_transfer::clear_dependencies(&self._local_wake);
        self.owner_state.park(work, handoff.clone());
        if let Ok(mut parked) = self.parked_subscription.lock() {
            *parked = Some(handoff);
        }
    }

    pub fn complete_prior_process_recovery(&mut self, work: &DefaultDfsReconciliationWork) {
        self.owner_state.complete_prior_process_recovery(work);
    }

    pub fn projection_sender(
        &self,
    ) -> tokio::sync::watch::Sender<DefaultDfsReconciliationProjection> {
        self.projection_tx.clone()
    }

    pub fn parked_handoff(&self) -> Option<DefaultDfsParkedHandoff> {
        self.owner_state.parked_handoff().cloned()
    }

    fn clear_parked_subscription(&self) {
        if let Ok(mut parked) = self.parked_subscription.lock() {
            *parked = None;
        }
    }

    fn normalized_projection(
        &self,
        projection: DefaultDfsReconciliationProjection,
    ) -> DefaultDfsReconciliationProjection {
        normalize_default_dfs_reconciliation_projection_for_process(
            projection,
            &self.local_node_id,
            self.placement_authority_process_identity.as_ref(),
        )
    }
}

fn normalize_default_dfs_reconciliation_projection_for_process(
    mut projection: DefaultDfsReconciliationProjection,
    local_node_id: &NodeId,
    process: Option<&crate::DfsPlacementAuthorityProcessIdentity>,
) -> DefaultDfsReconciliationProjection {
    if let (Some(permit), Some(process)) = (projection.placement_authority_permit.as_ref(), process)
        && !crate::control_plane::placement_authority_permit_value_requires_local_process_recovery(
            &permit.value,
            local_node_id,
            process,
        )
        .unwrap_or(false)
    {
        // Current-process and remote-node placement work is already owned by a
        // live authority. Only this node's prior process incarnation can enter
        // the automatic startup/replacement recovery lane.
        projection.placement_authority_permit = None;
    }
    projection
}

pub fn notify_default_dfs_reconciliation_replica_progress(cluster_paths: &ClusterPaths) {
    let Some(wakes) = DEFAULT_DFS_RECONCILIATION_WAKES.get() else {
        return;
    };
    let Ok(wakes) = wakes.lock() else {
        return;
    };
    let Some(wake) = wakes.get(&cluster_paths.root).and_then(Weak::upgrade) else {
        return;
    };
    wake.generation_tx
        .send_modify(|generation| *generation = generation.wrapping_add(1));
}

#[cfg(any(test, feature = "test-support"))]
pub struct DefaultDfsReconciliationWakeSubscription {
    receiver: tokio::sync::watch::Receiver<u64>,
    _wake: Arc<DefaultDfsReconciliationWake>,
}

#[cfg(any(test, feature = "test-support"))]
impl std::ops::Deref for DefaultDfsReconciliationWakeSubscription {
    type Target = tokio::sync::watch::Receiver<u64>;

    fn deref(&self) -> &Self::Target {
        &self.receiver
    }
}

#[cfg(any(test, feature = "test-support"))]
impl std::ops::DerefMut for DefaultDfsReconciliationWakeSubscription {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.receiver
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn subscribe_default_dfs_reconciliation_replica_progress(
    cluster_paths: &ClusterPaths,
) -> Result<DefaultDfsReconciliationWakeSubscription, crate::DfsError> {
    let wake = default_dfs_reconciliation_wake(cluster_paths)?;
    Ok(DefaultDfsReconciliationWakeSubscription {
        receiver: wake.generation_tx.subscribe(),
        _wake: wake,
    })
}

fn default_dfs_reconciliation_wake(
    cluster_paths: &ClusterPaths,
) -> Result<Arc<DefaultDfsReconciliationWake>, crate::DfsError> {
    let key = cluster_paths.root.clone();
    let mut wakes = DEFAULT_DFS_RECONCILIATION_WAKES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| crate::DfsError::Network {
            message: "default DFS reconciliation wake registry is poisoned".to_string(),
        })?;
    if let Some(wake) = wakes.get(&key).and_then(Weak::upgrade) {
        return Ok(wake);
    }
    let (generation_tx, _) = tokio::sync::watch::channel(0);
    let wake = Arc::new(DefaultDfsReconciliationWake {
        generation_tx,
        primary_transfer: Mutex::new(Default::default()),
    });
    wakes.insert(key, Arc::downgrade(&wake));
    Ok(wake)
}

fn default_dfs_resident_reconciliation_projection(
    cluster_paths: &ClusterPaths,
    local_node_id: &NodeId,
) -> Result<(KvRevision, Vec<KvEntry>), KvError> {
    let mut exact_keys = default_dfs_reconciliation_exact_input_keys()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    exact_keys.push(crate::placement_authority_permit_key(
        &crate::sifs_file_system_id(),
    ));
    exact_keys.sort();
    exact_keys.dedup();
    let prefixes = default_dfs_reconciliation_prefixes().map(str::to_string);
    for _ in 0..DEFAULT_DFS_RECONCILIATION_PROJECTION_REPLAN_MAX_ATTEMPTS {
        let exact = sinomo_core::kv::read_cluster_kv_resident_local_entries(
            &cluster_paths.kv_paths(),
            local_node_id,
            &exact_keys,
        )?;
        let revision = exact.revision;
        let mut entries = exact.value.into_iter().flatten().collect::<Vec<_>>();
        let mut coherent = true;
        for prefix in &prefixes {
            let read = sinomo_core::kv::read_cluster_kv_resident_local_prefix(
                &cluster_paths.kv_paths(),
                local_node_id,
                prefix,
            )?;
            if read.revision != revision {
                coherent = false;
                break;
            }
            entries.extend(read.value);
        }
        if !coherent {
            continue;
        }
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        entries.dedup_by(|left, right| left.key == right.key);
        return Ok((revision, entries));
    }
    Err(KvError::Busy {
        path: cluster_paths.root.display().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn projection(inputs: Vec<(String, Vec<u8>)>) -> DefaultDfsReconciliationProjection {
        DefaultDfsReconciliationProjection {
            revision: Some(KvRevision(1)),
            inputs,
            placement_authority_permit: None,
        }
    }

    #[derive(serde::Serialize)]
    struct TestPlacementPermitRecord {
        job_id: String,
        holder_id: String,
        holder_cluster_id: Option<String>,
        holder_node_id: Option<NodeId>,
        holder_session_id: Option<String>,
    }

    #[test]
    fn nonrecoverable_permit_does_not_reopen_recovery_work() {
        let local_node_id =
            NodeId::from_legacy_named("default-dfs-current-permit").expect("local node");
        let process = crate::DfsPlacementAuthorityProcessIdentity {
            cluster_id: "cluster-current-permit".to_string(),
            process_session_id: "session-current-permit".to_string(),
        };
        let value = sinomo_core::kv::encode_kv_value(&TestPlacementPermitRecord {
            job_id: "job".to_string(),
            holder_id: "holder".to_string(),
            holder_cluster_id: Some(process.cluster_id.clone()),
            holder_node_id: Some(local_node_id.clone()),
            holder_session_id: Some(process.process_session_id.clone()),
        })
        .expect("encode current permit");
        let mut current = projection(Vec::new());
        current.placement_authority_permit = Some(DefaultDfsPermitGeneration {
            modify_revision: KvRevision(2),
            version: sinomo_core::kv::KvVersion(1),
            value,
        });

        let normalized = normalize_default_dfs_reconciliation_projection_for_process(
            current,
            &local_node_id,
            Some(&process),
        );

        assert_eq!(normalized.placement_authority_permit, None);

        let remote_value = sinomo_core::kv::encode_kv_value(&TestPlacementPermitRecord {
            job_id: "job".to_string(),
            holder_id: "holder".to_string(),
            holder_cluster_id: Some(process.cluster_id.clone()),
            holder_node_id: Some(
                NodeId::from_legacy_named("default-dfs-remote-permit").expect("remote node"),
            ),
            holder_session_id: Some("remote-session".to_string()),
        })
        .expect("encode remote permit");
        let mut remote = projection(Vec::new());
        remote.placement_authority_permit = Some(DefaultDfsPermitGeneration {
            modify_revision: KvRevision(3),
            version: sinomo_core::kv::KvVersion(1),
            value: remote_value,
        });
        assert_eq!(
            normalize_default_dfs_reconciliation_projection_for_process(
                remote,
                &local_node_id,
                Some(&process),
            )
            .placement_authority_permit,
            None
        );
    }

    #[test]
    fn learner_metadata_primary_is_independent_of_cluster_kv_voter_role() {
        let local = NodeId::from_legacy_named("default-dfs-learner-primary").expect("local node");
        let other = NodeId::from_legacy_named("default-dfs-kv-voter").expect("other node");
        let file_system_id = crate::sifs_file_system_id();
        let shard = |shard_id: &str, primary_node_id: NodeId| DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.to_string(),
            primary_node_id,
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: match shard_id {
                "root" => crate::DfsMetadataShardAssignment::Root,
                "services" => crate::DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
                _ => unreachable!("test shard assignment"),
            },
            namespace_prepare_codec: Default::default(),
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let inputs = vec![
            (
                crate::metadata_shard_key(&file_system_id, "root"),
                sinomo_core::kv::encode_kv_value(&shard("root", other)).expect("encode root shard"),
            ),
            (
                crate::metadata_shard_key(&file_system_id, "services"),
                sinomo_core::kv::encode_kv_value(&shard("services", local.clone()))
                    .expect("encode services shard"),
            ),
        ];

        assert!(default_dfs_reconciliation_local_owns_metadata_primary(
            &local, &inputs
        ));
        assert!(!default_dfs_reconciliation_local_owns_metadata_primary(
            &NodeId::from_legacy_named("default-dfs-replica-only").expect("replica node"),
            &inputs,
        ));
    }

    #[test]
    fn completed_generation_coalesces_until_inputs_or_local_progress_change() {
        let inputs = vec![("/dfs/test".to_string(), vec![1])];
        let mut state = DefaultDfsReconciliationState::new();
        let startup = state
            .pending_work(projection(inputs.clone()), 0)
            .expect("dirty startup work");
        state.complete_prior_process_recovery(&startup);
        state.complete_default_reconciliation(&startup);

        assert!(state.pending_work(projection(inputs.clone()), 0).is_none());
        assert!(state.pending_work(projection(inputs.clone()), 1).is_some());
        assert!(
            state
                .pending_work(projection(vec![("/dfs/test".to_string(), vec![2])]), 0)
                .is_some()
        );
    }

    #[test]
    fn parked_generation_wakes_only_for_owned_handoff_inputs_or_local_progress() {
        let file_system_id = crate::sifs_file_system_id();
        let handoff = DefaultDfsParkedHandoff::new(
            file_system_id.clone(),
            "root".to_string(),
            "handoff-1".to_string(),
            1,
            2,
        );
        let handoff_key = crate::metadata_shard_membership_handoff_key(&file_system_id, "root");
        let inputs = vec![
            (handoff_key.clone(), vec![1]),
            ("/cluster/unrelated".to_string(), vec![1]),
        ];
        let mut state = DefaultDfsReconciliationState::new();
        let startup = state
            .pending_work(projection(inputs.clone()), 0)
            .expect("startup work");
        state.complete_prior_process_recovery(&startup);
        state.park(&startup, handoff);

        let mut unrelated_changed = inputs.clone();
        unrelated_changed[1].1 = vec![2];
        assert!(
            state
                .pending_work(projection(unrelated_changed), 0)
                .is_none()
        );

        let mut handoff_changed = inputs;
        handoff_changed[0].1 = vec![2];
        assert!(state.pending_work(projection(handoff_changed), 0).is_some());
    }
}
