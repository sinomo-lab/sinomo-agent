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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

mod primary_transfer;
pub(crate) use primary_transfer::{
    capture_primary_transfer_selection, notify_primary_transfer_checkpoint,
};

static DEFAULT_DFS_RECONCILIATION_WAKES: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<DefaultDfsReconciliationWake>>>,
> = OnceLock::new();

#[derive(Debug)]
struct DefaultDfsReconciliationWake {
    generation_tx: tokio::sync::watch::Sender<u64>,
    primary_transfer: Mutex<primary_transfer::PrimaryTransferSelectionState>,
    #[cfg(any(test, feature = "test-support"))]
    completion: Mutex<Weak<DefaultDfsReconciliationCompletionSource>>,
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

    /// Completed ordered input DATA only makes the normal lifecycle runnable.
    /// It grants no writer, publication, or startup-completion permission.
    fn completed_in_projection(&self, inputs: &[(String, Vec<u8>)]) -> bool {
        if inputs.iter().any(|(key, _)| key == &self.wake_keys[1]) {
            return false;
        }
        let actual = inputs
            .iter()
            .find(|(key, _)| key == &self.wake_keys[0])
            .and_then(|(_, bytes)| {
                sinomo_core::kv::decode_kv_value::<DfsMetadataShardConfig>(bytes).ok()
            });
        let proof = inputs
            .iter()
            .find(|(key, _)| key == &self.wake_keys[2])
            .and_then(|(_, bytes)| {
                sinomo_core::kv::decode_kv_value::<DfsMetadataShardMembershipHandoffProof>(bytes)
                    .ok()
            });
        actual.zip(proof).is_some_and(|(actual, proof)| {
            let acknowledged = proof
                .acknowledged_node_ids
                .iter()
                .collect::<std::collections::BTreeSet<_>>();
            let majority = |cfg: &DfsMetadataShardConfig| {
                let members = std::iter::once(&cfg.primary_node_id)
                    .chain(&cfg.replica_node_ids)
                    .collect::<std::collections::BTreeSet<_>>();
                members.intersection(&acknowledged).count() >= members.len() / 2 + 1
            };
            actual.file_system_id == self.file_system_id
                && actual.shard_id == self.shard_id.0
                && actual.epoch == self.desired_epoch
                && proof.handoff_id == self.handoff_id
                && proof.retiring_shard.file_system_id == self.file_system_id
                && proof.retiring_shard.shard_id == self.shard_id.0
                && proof.retiring_shard.epoch == self.retiring_epoch
                && self.retiring_epoch.checked_add(1) == Some(self.desired_epoch)
                && proof.desired_shard == actual
                && !proof.state_fingerprint.is_empty()
                && acknowledged.contains(&proof.retiring_shard.primary_node_id)
                && acknowledged.contains(&proof.desired_shard.primary_node_id)
                && majority(&proof.retiring_shard)
                && majority(&proof.desired_shard)
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

    fn foreground_quiet_eligible(&self) -> bool {
        !self.startup_reconciliation_pending() && self.parked_generation.is_none()
    }

    fn mandatory_generation_pending(
        &self,
        current: &DefaultDfsReconciliationProjection,
        local_generation: u64,
    ) -> bool {
        if self.startup_reconciliation_pending()
            || self.startup_recovery_pending()
            || self.parked_generation.is_some()
            || self.reconciled_local_generation != Some(local_generation)
            || self.recovered_placement_authority_permit.as_ref()
                != Some(&current.placement_authority_permit)
        {
            return true;
        }
        // Handoff activation and its exact proof/watermark must be consumed by
        // the normal owner before its generation can be background work. Policy
        // and provider changes retain the existing foreground-preemptible path.
        let completed = self.reconciled_inputs.as_deref().unwrap_or_default();
        completed
            .iter()
            .filter(|(key, _)| default_dfs_reconciliation_exact_retry_key(key))
            .ne(current
                .inputs
                .iter()
                .filter(|(key, _)| default_dfs_reconciliation_exact_retry_key(key)))
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
                    // The lifecycle can return Parked after its ordered
                    // projection already observes activation. Capturing those
                    // completed inputs must not park forever without a new edge.
                    || parked.handoff.completed_in_projection(&current.inputs)
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
    _observation_subscription: sinomo_core::kv::KvCommittedObservationSubscription,
    projection_observer: Arc<Mutex<DefaultDfsCommittedProjectionObserver>>,
    projection_discontinuity: Arc<AtomicBool>,
    projection_floor: Arc<AtomicU64>,
    cluster_paths: ClusterPaths,
    refresh_completion_rx: Option<tokio::sync::watch::Receiver<u64>>,
    refresh_completed: bool,
    _metadata_authority_generation: Option<Arc<crate::DfsMetadataAuthorityRuntime>>,
    _local_wake: Arc<DefaultDfsReconciliationWake>,
    local_wake_rx: tokio::sync::watch::Receiver<u64>,
    parked_subscription: Arc<Mutex<Option<DefaultDfsParkedHandoff>>>,
    owner_state: DefaultDfsReconciliationState,
    local_node_id: NodeId,
    placement_authority_process_identity: Option<crate::DfsPlacementAuthorityProcessIdentity>,
    #[cfg(any(test, feature = "test-support"))]
    owner_state_tx: tokio::sync::watch::Sender<DefaultDfsReconciliationState>,
    #[cfg(any(test, feature = "test-support"))]
    _completion: Arc<DefaultDfsReconciliationCompletionSource>,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug)]
struct DefaultDfsReconciliationCompletionSource {
    owner_state: tokio::sync::watch::Receiver<DefaultDfsReconciliationState>,
    projection: tokio::sync::watch::Receiver<DefaultDfsReconciliationProjection>,
    local_generation: tokio::sync::watch::Receiver<u64>,
    discontinuity: Arc<AtomicBool>,
    local_node_id: NodeId,
    process_identity: Option<crate::DfsPlacementAuthorityProcessIdentity>,
}

#[cfg(any(test, feature = "test-support"))]
/// Observes the mounted owner without executing a second reconciliation pass.
pub(crate) struct DefaultDfsReconciliationCompletionSubscription {
    source: Arc<DefaultDfsReconciliationCompletionSource>,
    owner_state: tokio::sync::watch::Receiver<DefaultDfsReconciliationState>,
    projection: tokio::sync::watch::Receiver<DefaultDfsReconciliationProjection>,
    local_generation: tokio::sync::watch::Receiver<u64>,
}

#[cfg(any(test, feature = "test-support"))]
impl std::fmt::Debug for DefaultDfsReconciliationCompletionSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.owner_state.borrow().clone();
        let projection = normalize_default_dfs_reconciliation_projection_for_process(
            self.projection.borrow().clone(),
            &self.source.local_node_id,
            self.source.process_identity.as_ref(),
        );
        let prior = state.reconciled_inputs.as_deref().unwrap_or_default();
        let mismatched_keys = projection
            .inputs
            .iter()
            .filter(|input| !prior.contains(input))
            .chain(
                prior
                    .iter()
                    .filter(|input| !projection.inputs.contains(input)),
            )
            .map(|(key, _)| key.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        formatter
            .debug_struct("DfsReconciliationOwner")
            .field("node", &self.source.local_node_id)
            .field("revision", &projection.revision)
            .field(
                "discontinuous",
                &self.source.discontinuity.load(Ordering::Acquire),
            )
            .field("startup", &state.startup_reconciliation_pending())
            .field("recovery", &state.startup_recovery_pending())
            .field("parked", &state.parked_handoff())
            .field(
                "inputs_match",
                &(state.reconciled_inputs.as_ref() == Some(&projection.inputs)),
            )
            .field("current_input_count", &projection.inputs.len())
            .field(
                "completed_input_count",
                &state.reconciled_inputs.as_ref().map(Vec::len),
            )
            .field(
                "mismatched_keys",
                &mismatched_keys.iter().take(16).collect::<Vec<_>>(),
            )
            .field("local_generation", &*self.local_generation.borrow())
            .field(
                "completed_local_generation",
                &state.reconciled_local_generation,
            )
            .field(
                "permit_match",
                &(state.recovered_placement_authority_permit.as_ref()
                    == Some(&projection.placement_authority_permit)),
            )
            .field("stopped", &self.owner_state.has_changed().is_err())
            .finish()
    }
}

#[cfg(any(test, feature = "test-support"))]
impl DefaultDfsReconciliationCompletionSubscription {
    pub(crate) fn is_current_complete(&self) -> Result<bool, crate::DfsError> {
        self.owner_state
            .has_changed()
            .map_err(|_| crate::DfsError::Network {
                message: "default DFS reconciliation owner stopped".into(),
            })?;
        let state = self.owner_state.borrow().clone();
        // Revision-only projection advances deliberately do not send a watch
        // notification. Hold this brief synchronous read through the predicate
        // so such an advance cannot yield false and then strand its waiter.
        let sampled_projection = self.projection.borrow();
        let local_generation = *self.local_generation.borrow();
        let projection = normalize_default_dfs_reconciliation_projection_for_process(
            (*sampled_projection).clone(),
            &self.source.local_node_id,
            self.source.process_identity.as_ref(),
        );
        let complete = !self.source.discontinuity.load(Ordering::Acquire)
            && projection.revision.is_some()
            && state.parked_handoff().is_none()
            && !state.startup_reconciliation_pending()
            && !state.startup_recovery_pending()
            && state.pending_work(projection, local_generation).is_none()
            && *self.local_generation.borrow() == local_generation
            && !self.source.discontinuity.load(Ordering::Acquire);
        drop(sampled_projection);
        Ok(complete)
    }

    pub(crate) async fn wait_current_complete(&mut self) -> Result<(), crate::DfsError> {
        while !self.is_current_complete()? {
            let changed = tokio::select! {
                changed = self.owner_state.changed() => changed,
                changed = self.projection.changed() => changed,
                changed = self.local_generation.changed() => changed,
            };
            changed.map_err(|_| crate::DfsError::Network {
                message: "default DFS reconciliation completion owner stopped".into(),
            })?;
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn subscribe_default_dfs_reconciliation_completion(
    cluster_paths: &ClusterPaths,
) -> Result<DefaultDfsReconciliationCompletionSubscription, crate::DfsError> {
    let wake = default_dfs_reconciliation_wake(cluster_paths)?;
    let source = wake
        .completion
        .lock()
        .map_err(|_| crate::DfsError::Network {
            message: "default DFS reconciliation completion registry is poisoned".into(),
        })?
        .upgrade()
        .ok_or_else(|| crate::DfsError::Network {
            message: "default DFS reconciliation owner is not mounted".into(),
        })?;
    Ok(DefaultDfsReconciliationCompletionSubscription {
        owner_state: source.owner_state.clone(),
        projection: source.projection.clone(),
        local_generation: source.local_generation.clone(),
        source,
    })
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
        let projection_discontinuity = Arc::new(AtomicBool::new(false));
        let projection_floor = Arc::new(AtomicU64::new(0));
        let refresh_completion_rx = metadata_authority_generation
            .as_ref()
            .map(|runtime| runtime.subscribe_refresh_completion());
        let callback_observer = Arc::clone(&projection_observer);
        let callback_projection_tx = projection_tx.clone();
        let callback_local_node_id = local_node_id.clone();
        let callback_cluster_paths = cluster_paths.clone();
        let callback_selection_wake = Arc::clone(&local_wake);
        let callback_discontinuity = Arc::clone(&projection_discontinuity);
        let callback_floor = Arc::clone(&projection_floor);
        let callback_authority = metadata_authority_generation.clone();
        let observation_subscription =
            sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
                cluster_paths,
                move |observation| {
                    if observation.applied_index.is_none() {
                        // A derived-file hint cannot retire this native view.
                        return;
                    }
                    use sinomo_core::kv::{
                        KvCommittedObservationKind, KvCommittedObservationValue,
                    };
                    let mut observer = callback_observer
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    // A materialized repair has no native applied index. Its
                    // revision remains a hint and cannot fence native recovery.
                    let canonical_revision = observation.applied_index.and(observation.revision);
                    let withdrawn =
                        matches!(
                            observation.value,
                            KvCommittedObservationValue::Discontinuity(_)
                        ) || matches!(observation.value, KvCommittedObservationValue::Snapshot(_))
                            && observation.kind == KvCommittedObservationKind::Replacement;
                    if withdrawn {
                        if let Some(revision) = observer.revision() {
                            callback_floor.fetch_max(revision.0, Ordering::AcqRel);
                        }
                        if let Some(revision) = canonical_revision {
                            callback_floor.fetch_max(revision.0, Ordering::AcqRel);
                        }
                        callback_discontinuity.store(true, Ordering::Release);
                        observer.withdraw(&callback_projection_tx);
                        primary_transfer::clear_dependencies(&callback_selection_wake);
                    }
                    let applied = match &observation.value {
                        KvCommittedObservationValue::Delta(delta)
                            if !callback_discontinuity.load(Ordering::Acquire) =>
                        {
                            observer.observe_delta(&callback_projection_tx, delta)
                        }
                        KvCommittedObservationValue::Snapshot(_)
                            if !withdrawn && !callback_discontinuity.load(Ordering::Acquire) =>
                        {
                            true
                        }
                        _ => false,
                    };
                    if !applied {
                        if let Some(revision) = canonical_revision {
                            callback_floor.fetch_max(revision.0, Ordering::AcqRel);
                        }
                        callback_discontinuity.store(true, Ordering::Release);
                        observer.withdraw(&callback_projection_tx);
                        match default_dfs_resident_reconciliation_projection(
                            &callback_cluster_paths,
                            &callback_local_node_id,
                        ) {
                            Ok((revision, entries))
                                if revision.0 >= callback_floor.load(Ordering::Acquire) =>
                            {
                                observer.rebuild_from_entries(
                                    &callback_projection_tx,
                                    revision,
                                    &entries,
                                );
                                callback_discontinuity.store(false, Ordering::Release);
                            }
                            Ok(_) | Err(_) => {
                                callback_discontinuity.store(true, Ordering::Release);
                                observer.withdraw(&callback_projection_tx);
                                if let Some(authority) = callback_authority.as_ref() {
                                    authority.request_authoritative_refresh();
                                }
                            }
                        }
                    }
                    drop(observer);
                    primary_transfer::observe_projection(
                        &callback_selection_wake,
                        &callback_projection_tx.borrow(),
                    );
                },
            );
        // Serialize registration sampling with discontinuity withdrawal. A
        // same-revision gap cannot cross this read and restore an old seed.
        {
            let mut observer =
                projection_observer
                    .lock()
                    .map_err(|_| crate::DfsError::Network {
                        message: "default DFS committed projection observer is poisoned"
                            .to_string(),
                    })?;
            if let Ok((revision, entries)) =
                default_dfs_resident_reconciliation_projection(cluster_paths, local_node_id)
                && revision.0 >= projection_floor.load(Ordering::Acquire)
            {
                observer.rebuild_from_entries(&projection_tx, revision, &entries);
                projection_discontinuity.store(false, Ordering::Release);
            }
        }
        primary_transfer::observe_projection(&local_wake, &projection_tx.borrow());
        let owner_state = DefaultDfsReconciliationState::new();
        #[cfg(any(test, feature = "test-support"))]
        let (owner_state_tx, completion) = {
            let (owner_state_tx, owner_state_rx) = tokio::sync::watch::channel(owner_state.clone());
            let completion = Arc::new(DefaultDfsReconciliationCompletionSource {
                owner_state: owner_state_rx,
                projection: projection_rx.clone(),
                local_generation: local_wake_rx.clone(),
                discontinuity: Arc::clone(&projection_discontinuity),
                local_node_id: local_node_id.clone(),
                process_identity: placement_authority_process_identity.clone(),
            });
            *local_wake
                .completion
                .lock()
                .map_err(|_| crate::DfsError::Network {
                    message: "default DFS reconciliation completion registry is poisoned".into(),
                })? = Arc::downgrade(&completion);
            (owner_state_tx, completion)
        };
        Ok(Self {
            projection_tx,
            projection_rx,
            _observation_subscription: observation_subscription,
            projection_observer,
            projection_discontinuity,
            projection_floor,
            cluster_paths: cluster_paths.clone(),
            refresh_completion_rx,
            refresh_completed: false,
            _metadata_authority_generation: metadata_authority_generation,
            _local_wake: local_wake,
            local_wake_rx,
            parked_subscription,
            owner_state,
            local_node_id: local_node_id.clone(),
            placement_authority_process_identity,
            #[cfg(any(test, feature = "test-support"))]
            owner_state_tx,
            #[cfg(any(test, feature = "test-support"))]
            _completion: completion,
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
            _ = async {
                match self.refresh_completion_rx.as_mut() {
                    Some(completion) => { let _ = completion.changed().await; }
                    None => std::future::pending::<()>().await,
                }
            } => { self.refresh_completed = true; }
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
        let was_discontinuous = self.projection_discontinuity.load(Ordering::Acquire);
        if was_discontinuous {
            let completed = std::mem::take(&mut self.refresh_completed)
                || self
                    .refresh_completion_rx
                    .as_mut()
                    .is_some_and(|completion| {
                        let changed = completion.has_changed().unwrap_or(false);
                        completion.borrow_and_update();
                        changed
                    });
            if completed {
                let mut observer = self
                    .projection_observer
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if let Ok((revision, entries)) = default_dfs_resident_reconciliation_projection(
                    &self.cluster_paths,
                    &self.local_node_id,
                ) && revision.0 >= self.projection_floor.load(Ordering::Acquire)
                {
                    observer.rebuild_from_entries(&self.projection_tx, revision, &entries);
                    self.projection_discontinuity
                        .store(false, Ordering::Release);
                }
            }
            if self.projection_discontinuity.load(Ordering::Acquire) {
                return None;
            }
        }
        let current = self.projection_rx.borrow_and_update().clone();
        let current = self.normalized_projection(current);
        primary_transfer::observe_projection(&self._local_wake, &current);
        let local_generation = *self.local_wake_rx.borrow_and_update();
        let work = self.owner_state.pending_work(current, local_generation);
        #[cfg(any(test, feature = "test-support"))]
        if was_discontinuous || work.is_none() {
            // A committed callback can send its rebuilt projection before
            // clearing the fence. Acknowledge the owner's subsequent idle
            // observation even when the semantic inputs remained unchanged.
            self.owner_state_tx.send_modify(|_| {});
        }
        work
    }

    pub(crate) fn mandatory_generation_pending(&self) -> bool {
        if self.projection_discontinuity.load(Ordering::Acquire) {
            return true;
        }
        let current = self.normalized_projection(self.projection_rx.borrow().clone());
        self.owner_state
            .mandatory_generation_pending(&current, *self.local_wake_rx.borrow())
    }

    pub(crate) fn foreground_quiet_generation(&self) -> Option<DefaultDfsExactRetryGeneration> {
        if self.projection_discontinuity.load(Ordering::Acquire)
            || !self.owner_state.foreground_quiet_eligible()
        {
            return None;
        }
        let current = self.normalized_projection(self.projection_rx.borrow().clone());
        let local_generation = *self.local_wake_rx.borrow();
        // A preempted pass has no completed effects. Never let its captured
        // current projection delay the still-owned generation behind quiet.
        if self
            .owner_state
            .pending_work(current.clone(), local_generation)
            .is_some()
        {
            return None;
        }
        Some(DefaultDfsExactRetryGeneration {
            inputs: current.inputs,
            placement_authority_permit: current.placement_authority_permit,
            local_generation: *self.local_wake_rx.borrow(),
        })
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
        #[cfg(any(test, feature = "test-support"))]
        self.owner_state_tx.send_replace(self.owner_state.clone());
        promoted_primary_rebind
    }

    pub fn complete_default_reconciliation(&mut self, work: &DefaultDfsReconciliationWork) {
        self.owner_state.complete_default_reconciliation(work);
        self.clear_parked_subscription();
        #[cfg(any(test, feature = "test-support"))]
        self.owner_state_tx.send_replace(self.owner_state.clone());
    }

    pub fn park(&mut self, work: &DefaultDfsReconciliationWork, handoff: DefaultDfsParkedHandoff) {
        // Handoff now owns its exact prefix/fence wake inputs; selection waits
        // from an earlier pass must not survive into that different phase.
        primary_transfer::clear_dependencies(&self._local_wake);
        self.owner_state.park(work, handoff.clone());
        #[cfg(any(test, feature = "test-support"))]
        self.owner_state_tx.send_replace(self.owner_state.clone());
        if let Ok(mut parked) = self.parked_subscription.lock() {
            *parked = Some(handoff);
        }
    }

    pub fn complete_prior_process_recovery(&mut self, work: &DefaultDfsReconciliationWork) {
        self.owner_state.complete_prior_process_recovery(work);
        #[cfg(any(test, feature = "test-support"))]
        self.owner_state_tx.send_replace(self.owner_state.clone());
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
        #[cfg(any(test, feature = "test-support"))]
        completion: Mutex::new(Weak::new()),
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
    let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    // Join only the existing resident owner's release condition. One borrowed
    // view keeps every exact key and prefix at the same committed revision;
    // cold/invalid state never opens storage or falls back to a snapshot.
    let read = sinomo_core::kv::with_cluster_kv_resident_read_projection_before(
        &cluster_paths.kv_paths().raft_wal_paths(local_node_id),
        local_node_id,
        deadline,
        |projection| {
            let mut entries = projection
                .read_index_entries(&exact_keys)?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            for prefix in default_dfs_reconciliation_prefixes() {
                entries.extend(projection.read_index_prefix_limited_by_payload_bytes(
                    prefix,
                    sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                    sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
                )?);
            }
            Ok::<_, KvError>(entries)
        },
    )
    .ok_or_else(|| KvError::Busy {
        path: cluster_paths.root.display().to_string(),
    })??;
    let mut entries = read.value?;
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    entries.dedup_by(|left, right| left.key == right.key);
    Ok((read.revision, entries))
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

    #[test]
    fn completion_observer_rejects_new_generations_parked_and_discontinuous_owners() {
        let current = projection(vec![("/dfs/test".into(), vec![1])]);
        let (projection_tx, projection_rx) = tokio::sync::watch::channel(current.clone());
        let (local_tx, local_rx) = tokio::sync::watch::channel(0);
        let mut state = DefaultDfsReconciliationState::new();
        let (owner_tx, owner_rx) = tokio::sync::watch::channel(state.clone());
        let discontinuity = Arc::new(AtomicBool::new(false));
        let source = Arc::new(DefaultDfsReconciliationCompletionSource {
            owner_state: owner_rx.clone(),
            projection: projection_rx.clone(),
            local_generation: local_rx.clone(),
            discontinuity: Arc::clone(&discontinuity),
            local_node_id: NodeId::from_legacy_named("default-completion-owner").unwrap(),
            process_identity: None,
        });
        let completion = DefaultDfsReconciliationCompletionSubscription {
            source,
            owner_state: owner_rx,
            projection: projection_rx,
            local_generation: local_rx,
        };
        assert!(!completion.is_current_complete().unwrap());
        let work = state.pending_work(current.clone(), 0).unwrap();
        state.complete_prior_process_recovery(&work);
        state.complete_default_reconciliation(&work);
        owner_tx.send_replace(state.clone());
        assert!(completion.is_current_complete().unwrap());
        local_tx.send_replace(1);
        assert!(!completion.is_current_complete().unwrap());
        local_tx.send_replace(0);
        let changed = projection(vec![("/dfs/test".into(), vec![2])]);
        projection_tx.send_replace(changed);
        assert!(!completion.is_current_complete().unwrap());
        projection_tx.send_replace(current.clone());
        discontinuity.store(true, Ordering::Release);
        assert!(!completion.is_current_complete().unwrap());
        discontinuity.store(false, Ordering::Release);
        state.park(
            &work,
            DefaultDfsParkedHandoff::new(
                crate::sifs_file_system_id(),
                "root".into(),
                "completion-handoff".into(),
                1,
                2,
            ),
        );
        assert!(state.pending_work(current, 0).is_none());
        owner_tx.send_replace(state);
        assert!(!completion.is_current_complete().unwrap());
        drop(owner_tx);
        assert!(completion.is_current_complete().is_err());
    }

    #[derive(serde::Serialize)]
    struct TestPlacementPermitRecord {
        job_id: String,
        holder_id: String,
        holder_cluster_id: Option<String>,
        holder_node_id: Option<NodeId>,
        holder_session_id: Option<String>,
    }

    fn default_runtime_observation_provenance(future_snapshot: bool, known_prestate_gap: bool) {
        let name = match (future_snapshot, known_prestate_gap) {
            (false, false) => "default-runtime-native-gap-resident-release",
            (true, false) => "default-runtime-future-snapshot",
            (false, true) => "default-runtime-future-prestate-gap",
            (true, true) => unreachable!(),
        };
        let root = sinomo_testdir::TestDir::new(name);
        let data_dir = sinomo_core::local::SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().unwrap();
        let local = NodeId::from_legacy_named("default-runtime-gap-local").unwrap();
        let kv_paths = paths.kv_paths();
        let store = kv_paths.raft_store();
        store.set_leader_compact(&local, 1).unwrap();
        let config_key = crate::file_system_config_key(&crate::sifs_file_system_id());
        let snapshot = store
            .append_command_and_publish(
                &local,
                sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Put {
                    key: config_key.clone(),
                    value: vec![7],
                    lease_id: None,
                }),
                |_| false,
            )
            .unwrap();
        let mut future_store =
            sinomo_core::kv::InProcessKvStore::restore(snapshot.clone()).unwrap();
        future_store.put(config_key.clone(), vec![9], None).unwrap();
        let mut future = future_store.try_snapshot().unwrap();
        future.revision = KvRevision(snapshot.revision.0 + 64);
        let future_revision = future.revision;
        if known_prestate_gap {
            sinomo_core::kv::write_cluster_kv_snapshot(&kv_paths, &future).unwrap();
        }
        let mut reactor = DefaultDfsReconciliationRuntime::new(&paths, &local, None, None).unwrap();
        let initial = reactor.pending_work().expect("seeded configuration work");
        reactor.complete_prior_process_recovery(&initial);
        reactor.complete_default_reconciliation(&initial);
        assert!(reactor.pending_work().is_none());
        let native_baseline = reactor.projection_rx.borrow().clone();
        assert_eq!(native_baseline.revision, Some(snapshot.revision));
        let baseline_floor = reactor.projection_floor.load(Ordering::Acquire);
        let baseline_rebuilds = reactor.projection_observer.lock().unwrap().rebuild_count();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&seen);
        // Observe the genuine producer envelope independently of the reactor;
        // collection completes when the ordered callbacks finish.
        let _subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
            &paths,
            move |event| captured.lock().unwrap().push(event.clone()),
        );
        let native_receipt = if !future_snapshot && !known_prestate_gap {
            let committed = store
                .append_command_with_response(
                    &local,
                    sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Delete {
                        key: "/default-fixture/missing-key".into(),
                    }),
                )
                .unwrap();
            assert_eq!(committed.snapshot.revision, snapshot.revision);
            assert_eq!(committed.committed_request.log_index, 2);
            Some(
                committed
                    .observation_receipt
                    .expect("actual native index advance owns publication"),
            )
        } else {
            None
        };
        let mut observed = reactor.projection_rx.clone();
        let held = store.open_node(&local).unwrap();
        let publication_paths = kv_paths.clone();
        let classifier_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let writer_classifier_calls = Arc::clone(&classifier_calls);
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let publication = std::thread::spawn(move || {
            if let Some(receipt) = native_receipt {
                receipt.abandon_publication().unwrap();
                receipt.wait_delivered().unwrap();
            } else if future_snapshot {
                sinomo_core::kv::write_cluster_kv_snapshot(&publication_paths, &future).unwrap();
            } else {
                let mut desired_store = sinomo_core::kv::InProcessKvStore::restore(future).unwrap();
                desired_store.put(config_key, vec![10], None).unwrap();
                let desired = desired_store.try_snapshot().unwrap();
                sinomo_core::kv::write_imported_cluster_kv_snapshot_if_changed(
                    &publication_paths,
                    &desired,
                    |_| {
                        assert_eq!(writer_classifier_calls.fetch_add(1, Ordering::AcqRel), 0);
                        // Test-only external IO fault after the writer captured its
                        // known prestate. Production classifier callbacks are pure.
                        std::fs::remove_file(&publication_paths.snapshot).unwrap();
                        std::fs::create_dir(&publication_paths.snapshot).unwrap();
                        false
                    },
                )
                .unwrap_err();
            }
            finished_tx.send(()).unwrap();
        });
        if future_snapshot || known_prestate_gap {
            // Any resident-prefix read would wait on the held owner. The real
            // derived writer must finish without that read or a refresh job.
            finished_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            publication.join().unwrap();
            assert_eq!(*reactor.projection_rx.borrow(), native_baseline);
            assert_eq!(
                reactor.projection_floor.load(Ordering::Acquire),
                baseline_floor
            );
            assert_eq!(
                reactor.projection_observer.lock().unwrap().rebuild_count(),
                baseline_rebuilds
            );
            assert!(!reactor.projection_discontinuity.load(Ordering::Acquire));
            assert!(!observed.has_changed().unwrap());
            assert!(!reactor.refresh_completed);
            assert!(
                reactor.pending_work().is_none(),
                "derived IO cannot retire native eligibility or create work"
            );
            let events = seen.lock().unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].applied_index, None);
            assert_eq!(events[0].revision, Some(future_revision));
            if known_prestate_gap {
                assert_eq!(events[0].previous_revision, Some(future_revision));
                assert_eq!(classifier_calls.load(Ordering::Acquire), 1);
                assert!(matches!(
                    events[0].value,
                    sinomo_core::kv::KvCommittedObservationValue::Discontinuity(_)
                ));
            } else {
                assert!(matches!(
                    events[0].value,
                    sinomo_core::kv::KvCommittedObservationValue::Snapshot(_)
                ));
            }
            drop(events);
            drop(held);
            return;
        }
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        executor.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                observed.wait_for(|projection| projection.revision.is_none()),
            )
            .await
            .unwrap()
            .unwrap();
        });
        assert!(reactor.projection_discontinuity.load(Ordering::Acquire));
        assert_eq!(
            reactor.projection_floor.load(Ordering::Acquire),
            snapshot.revision.0
        );
        assert!(
            reactor.pending_work().is_none(),
            "native uncertainty cannot schedule stale work"
        );
        drop(held); // Availability recovery does not require another commit.
        finished_rx
            .recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap();
        publication.join().unwrap();
        assert!(!reactor.projection_discontinuity.load(Ordering::Acquire));
        assert_eq!(*reactor.projection_rx.borrow(), native_baseline);
        assert_eq!(
            reactor.projection_floor.load(Ordering::Acquire),
            snapshot.revision.0
        );
        assert_eq!(
            reactor.projection_observer.lock().unwrap().rebuild_count(),
            baseline_rebuilds + 1
        );
        let events = seen.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].applied_index, Some(2));
        assert_eq!(events[0].revision, Some(snapshot.revision));
        assert!(matches!(
            events[0].value,
            sinomo_core::kv::KvCommittedObservationValue::Discontinuity(_)
        ));
        drop(events);
        assert!(
            reactor.pending_work().is_none(),
            "same native inputs do not create reconciliation work"
        );
    }

    #[test]
    fn default_runtime_gap_withdraws_until_checked_out_resident_releases_without_a_commit() {
        default_runtime_observation_provenance(false, false);
    }

    #[test]
    fn default_runtime_future_derived_snapshot_preserves_native_authority_without_a_read() {
        default_runtime_observation_provenance(true, false);
    }

    #[test]
    fn default_runtime_future_known_prestate_gap_preserves_native_authority_without_a_read() {
        default_runtime_observation_provenance(false, true);
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
    fn completed_parked_generation_revalidates_before_foreground_quiet() {
        let local = NodeId::from_legacy_named("quiet-followup-local").unwrap();
        let inputs = vec![("/dfs/test".to_string(), vec![1])];
        let mut state = DefaultDfsReconciliationState::new();
        let startup = state.pending_work(projection(inputs.clone()), 0).unwrap();
        assert!(!state.foreground_quiet_eligible());
        state.complete_prior_process_recovery(&startup);
        state.complete_default_reconciliation(&startup);
        assert!(state.foreground_quiet_eligible());

        // Selector DATA only. A real completed lifecycle still requires its
        // normal follow-up pass after the parked generation is consumed.
        state.park(
            &startup,
            DefaultDfsParkedHandoff::new(
                crate::sifs_file_system_id(),
                "root".into(),
                "quiet-followup-handoff".into(),
                1,
                2,
            ),
        );
        assert!(!state.foreground_quiet_eligible());
        state.complete_after_control_plane_observation(&startup, &local);
        assert!(state.parked_handoff().is_none());
        assert!(state.startup_reconciliation_pending());
        assert!(!state.foreground_quiet_eligible());
        let followup = state.pending_work(projection(inputs.clone()), 0).unwrap();
        assert!(followup.reconcile_defaults);
        state.complete_after_control_plane_observation(&followup, &local);
        assert!(state.pending_work(projection(inputs), 0).is_none());
        assert!(state.foreground_quiet_eligible());
    }

    #[test]
    fn exact_authority_and_post_handoff_generations_require_normal_admission() {
        let local = NodeId::from_legacy_named("mandatory-generation-local").unwrap();
        let shard_key = crate::metadata_shard_key(&crate::sifs_file_system_id(), "users");
        let initial = projection(vec![(shard_key.clone(), vec![1])]);
        let mut state = DefaultDfsReconciliationState::new();
        assert!(state.mandatory_generation_pending(&initial, 0));
        let work = state.pending_work(initial.clone(), 0).unwrap();
        state.complete_prior_process_recovery(&work);
        state.complete_default_reconciliation(&work);
        assert!(!state.mandatory_generation_pending(&initial, 0));

        let mut provider_only = initial.clone();
        provider_only
            .inputs
            .push(("/cluster/services/dfs/example".into(), vec![2]));
        provider_only.inputs.sort();
        assert!(state.pending_work(provider_only.clone(), 0).is_some());
        assert!(!state.mandatory_generation_pending(&provider_only, 0));
        let changed_authority = projection(vec![(shard_key, vec![3])]);
        assert!(state.mandatory_generation_pending(&changed_authority, 0));
        assert!(state.mandatory_generation_pending(&initial, 1));

        state.park(
            &work,
            DefaultDfsParkedHandoff::new(
                crate::sifs_file_system_id(),
                "users".into(),
                "mandatory-handoff".into(),
                1,
                2,
            ),
        );
        state.complete_after_control_plane_observation(&work, &local);
        assert!(state.startup_reconciliation_pending());
        assert!(state.mandatory_generation_pending(&initial, 0));
        let followup = state.pending_work(initial.clone(), 0).unwrap();
        state.complete_after_control_plane_observation(&followup, &local);
        assert!(!state.mandatory_generation_pending(&initial, 0));
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
    #[test]
    fn completed_handoff_captured_at_park_schedules_normal_revalidation() {
        let file_system_id = crate::sifs_file_system_id();
        let old = NodeId::from_legacy_named("parked-data-old-primary").unwrap();
        let new = NodeId::from_legacy_named("parked-data-new-primary").unwrap();
        let retiring = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".into(),
            primary_node_id: old.clone(),
            replica_node_ids: vec![new.clone()],
            epoch: 7,
            assignment: crate::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let mut desired = retiring.clone();
        desired.primary_node_id = new.clone();
        desired.replica_node_ids = vec![old.clone()];
        desired.epoch = 8;
        // These are selector input DATA, not a minted native receipt or permit.
        let proof = DfsMetadataShardMembershipHandoffProof {
            cause: crate::DfsMetadataShardHandoffCause::Membership,
            handoff_id: "completed-at-park".into(),
            retiring_shard: retiring,
            desired_shard: desired.clone(),
            journal_sequence: 2,
            state_fingerprint: "ordered-native-checkpoint-data".into(),
            acknowledged_node_ids: vec![old, new],
        };
        let handoff = DefaultDfsParkedHandoff::new(
            file_system_id.clone(),
            "root".into(),
            proof.handoff_id.clone(),
            7,
            8,
        );
        let inputs = vec![
            (
                crate::metadata_shard_key(&file_system_id, "root"),
                sinomo_core::kv::encode_kv_value(&desired).unwrap(),
            ),
            (
                crate::metadata_shard_membership_handoff_proof_key(&file_system_id, "root"),
                sinomo_core::kv::encode_kv_value(&proof).unwrap(),
            ),
        ];
        let mut state = DefaultDfsReconciliationState::new();
        let work = state.pending_work(projection(inputs.clone()), 0).unwrap();
        state.complete_prior_process_recovery(&work);
        state.park(&work, handoff.clone());
        assert!(
            state
                .pending_work(projection(inputs.clone()), 0)
                .unwrap()
                .reconcile_defaults
        );
        assert!(
            state.parked_handoff().is_some(),
            "selection does not synthesize completion"
        );
        let mut pending = inputs.clone();
        pending.push((
            crate::metadata_shard_membership_handoff_key(&file_system_id, "root"),
            Vec::new(),
        ));
        let mut waiting = DefaultDfsReconciliationState::new();
        let work = waiting
            .pending_work(projection(pending.clone()), 0)
            .unwrap();
        waiting.complete_prior_process_recovery(&work);
        waiting.park(&work, handoff.clone());
        assert!(waiting.pending_work(projection(pending), 0).is_none());
        let mut wrong = proof;
        wrong.handoff_id = "another-handoff".into();
        let mut foreign = inputs;
        foreign[1].1 = sinomo_core::kv::encode_kv_value(&wrong).unwrap();
        let mut waiting = DefaultDfsReconciliationState::new();
        let work = waiting
            .pending_work(projection(foreign.clone()), 0)
            .unwrap();
        waiting.complete_prior_process_recovery(&work);
        waiting.park(&work, handoff);
        assert!(waiting.pending_work(projection(foreign), 0).is_none());
    }
}
