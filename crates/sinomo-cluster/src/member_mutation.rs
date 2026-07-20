use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
use crate::join_registration::ClusterLocalDaemonMembershipObserver;
#[cfg(any(test, feature = "test-support"))]
use crate::keyspace::cluster_certificate_key;
use crate::keyspace::cluster_member_key;
#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
use crate::kv_local_store::{
    append_cluster_raft_command, open_cluster_raft_kv_node, read_cluster_kv_current_snapshot,
    write_cluster_kv_snapshot,
};
#[cfg(any(test, feature = "test-support"))]
use crate::kv_local_store::{read_cluster_kv_cached_snapshot_if_present, read_cluster_kv_snapshot};
use crate::kv_read_model::read_cluster_capability_policies_from_snapshot;
use crate::local_membership::apply_cluster_node_role;
use crate::local_membership::refresh_local_daemon_membership_from_disk;
#[cfg(test)]
use crate::local_state_store::write_cluster_state;
use crate::local_state_store::{
    acquire_local_cluster_state_mutation_lock, read_cluster_state_without_kv,
    write_cluster_state_unlocked,
};
use crate::member_store::{
    apply_cluster_raft_role, cluster_legacy_shadow_orphan_artifact_cleanup_txns_from_store,
    cluster_legacy_shadow_orphan_artifact_node_ids_from_store,
    cluster_member_removal_txns_from_store, cluster_member_update_ops_from_store,
    cluster_member_update_txn_from_store, cluster_raft_state_for_role,
    cluster_state_sync_txn_from_store,
};
#[cfg(any(test, feature = "test-support"))]
use serde::Serialize;
#[cfg(any(test, feature = "test-support"))]
use sinomo_base::ClusterCertificate;
use sinomo_base::{
    ClusterBootstrapState, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState,
    ClusterNodeRole, Id,
};
use sinomo_kv::{
    ClusterKvRaftConfigChange, ClusterKvRaftConfigChangeEvidence, ClusterKvRaftConfigChangeKind,
    ClusterKvRaftConfigChangePhase, ClusterKvRaftConfigRecord, ClusterKvTxnCommitResult,
    InProcessKvStore, KvAuthoritativeRead, KvAuthorityProof, KvCommand, KvCompare, KvOp,
    KvSnapshot, KvTxn, KvTxnRequest,
};
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::cleanup_cluster_endpoint_duplicates;
use sinomo_node::{NodeId, cluster_member_is_legacy_shadow_daemon, shadow_daemon_node_id};
use std::collections::BTreeSet;
use time::OffsetDateTime;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterNodeRoleUpdate {
    pub node_id: NodeId,
    pub previous_role: ClusterNodeRole,
    pub role: ClusterNodeRole,
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterNodeNameUpdate {
    pub node_id: NodeId,
    pub previous_name: String,
    pub name: String,
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterStateAuthorityUpdate {
    pub node_id: NodeId,
    pub previous_role: ClusterMemberRole,
    pub previous_state: ClusterMemberState,
    pub role: ClusterMemberRole,
    pub state: ClusterMemberState,
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterMemberRemoval {
    pub removed: Vec<ClusterMemberRecord>,
}

pub const MAX_CLUSTER_RAFT_CONFIG_RECONCILIATIONS_PER_PASS: usize = 32;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterRaftConfigReconciliationReport {
    pub inspected_change_count: usize,
    pub deferred_change_count: usize,
    pub resumed_change_ids: Vec<Id>,
    pub committed_change_ids: Vec<Id>,
    pub failed_change_ids: Vec<Id>,
}

pub trait ClusterMemberMutationHost {
    fn prepare_cluster_state_for_kv_sync(
        &mut self,
        _paths: &ClusterPaths,
        _state: &mut ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        Ok(())
    }

    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError>;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<ClusterKvTxnCommitResult, NetworkError>;

    fn prove_cluster_raft_member_caught_up(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        node_id: &NodeId,
        required_log_index: u64,
        _now: OffsetDateTime,
    ) -> Result<sinomo_kv::ClusterKvRaftCatchUpProof, NetworkError> {
        Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "cannot prove learner `{node_id}` caught up to Raft index {required_log_index}"
            ),
        })
    }
}

pub trait ClusterMemberMutationAsyncHost: Send {
    fn prepare_cluster_state_for_kv_sync_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvTxnCommitResult>;

    fn authoritative_cluster_kv_snapshot_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, KvAuthoritativeRead<KvSnapshot>> {
        Box::pin(async move {
            let result = self
                .commit_cluster_kv_txn_with_quorum_async(
                    paths,
                    state,
                    KvTxnRequest::generated(KvTxn {
                        compares: Vec::new(),
                        success: Vec::new(),
                        failure: Vec::new(),
                    }),
                )
                .await?;
            if !matches!(
                result.authority.basis,
                sinomo_kv::KvAuthorityBasis::CurrentTermQuorum { .. }
            ) {
                return Err(NetworkError::ClusterReadOnly {
                    detail: "membership authoritative read lacked a fresh quorum barrier"
                        .to_string(),
                });
            }
            Ok(KvAuthoritativeRead {
                value: result.snapshot,
                authority: result.authority,
            })
        })
    }
}

pub struct ClusterMemberMutationDriver<T, O> {
    driver: ClusterControlPlaneDriver<T>,
    observer: O,
}

impl<T, O> ClusterMemberMutationDriver<T, O> {
    pub fn new(driver: ClusterControlPlaneDriver<T>, observer: O) -> Self {
        Self { driver, observer }
    }

    pub fn with_transport(transport: T, observer: O) -> Self {
        Self::new(ClusterControlPlaneDriver::new(transport), observer)
    }

    pub fn driver_mut(&mut self) -> &mut ClusterControlPlaneDriver<T> {
        &mut self.driver
    }

    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }
}

impl<T, O> ClusterMemberMutationHost for ClusterMemberMutationDriver<T, O>
where
    T: ClusterControlPlaneTransport,
    O: ClusterLocalDaemonMembershipObserver,
{
    fn prepare_cluster_state_for_kv_sync(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        if let Some(observation) = self
            .observer
            .local_daemon_membership_observation(paths, state)?
        {
            refresh_local_daemon_membership_from_disk(paths, state, observation)?;
        }
        Ok(())
    }

    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
        ClusterMemberMutationHost::authoritative_cluster_kv_snapshot(&mut self.driver, paths, state)
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<ClusterKvTxnCommitResult, NetworkError> {
        ClusterMemberMutationHost::commit_cluster_kv_txn_with_quorum(
            &mut self.driver,
            paths,
            state,
            request,
        )
    }

    fn prove_cluster_raft_member_caught_up(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        node_id: &NodeId,
        required_log_index: u64,
        now: OffsetDateTime,
    ) -> Result<sinomo_kv::ClusterKvRaftCatchUpProof, NetworkError> {
        ClusterMemberMutationHost::prove_cluster_raft_member_caught_up(
            &mut self.driver,
            paths,
            state,
            node_id,
            required_log_index,
            now,
        )
    }
}

/// Mirrors a cluster state into the local durable KV store without proving
/// replication to a quorum.
///
/// Runtime cluster-control writes should go through the CLI quorum wrapper.
/// This helper remains public for bootstrapping, recovery tooling, and tests
/// that need to seed a local store deliberately.
pub fn sync_cluster_state_to_local_kv_unchecked(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvSnapshot, NetworkError> {
    let initial_snapshot = match open_cluster_raft_kv_node(paths, &state.local_node_id) {
        Ok(node) => node.store().try_snapshot().map_err(kv_err)?,
        Err(error) => {
            eprintln!("warning: replacing unreadable cluster kv raft state: {error}");
            let snapshot = InProcessKvStore::new().try_snapshot().map_err(kv_err)?;
            write_cluster_kv_snapshot(paths, &snapshot)?;
            snapshot
        }
    };
    let store = InProcessKvStore::restore(initial_snapshot.clone()).map_err(kv_err)?;
    let txn = cluster_state_sync_txn_from_store(&store, state, false)?;
    if txn.success.is_empty() {
        return Ok(initial_snapshot);
    }
    append_cluster_raft_command(paths, &state.local_node_id, KvCommand::Txn(txn))
}

#[cfg(any(test, feature = "test-support"))]
pub fn sync_cluster_state_to_local_raft_unchecked(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvSnapshot, NetworkError> {
    sync_cluster_state_to_local_kv_unchecked(paths, state)
}

pub fn cluster_state_sync_txn(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvTxn, NetworkError> {
    cluster_state_sync_txn_with_current_snapshot(paths, state).map(|(_, txn)| txn)
}

pub fn sync_cluster_state_to_kv_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    host: &mut (impl ClusterMemberMutationHost + ?Sized),
) -> Result<KvSnapshot, NetworkError> {
    let mut keyed_state = state.clone();
    host.prepare_cluster_state_for_kv_sync(paths, &mut keyed_state)?;
    cleanup_cluster_endpoint_duplicates(&mut keyed_state);
    let authoritative = host.authoritative_cluster_kv_snapshot(paths, &mut keyed_state)?;
    let store = InProcessKvStore::restore(authoritative.value.clone()).map_err(kv_err)?;
    let txn = cluster_state_sync_txn_from_store(&store, &keyed_state, true)?;
    if txn.success.is_empty() {
        return Ok(authoritative.value);
    }
    let mut commit_state = keyed_state;
    let result = host.commit_cluster_kv_txn_with_quorum(
        paths,
        &mut commit_state,
        KvTxnRequest::generated(txn),
    )?;
    require_cluster_kv_txn_succeeded(result, "synchronizing cluster state")
        .map(|result| result.snapshot)
}

pub async fn sync_cluster_state_to_kv_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    host: &mut impl ClusterMemberMutationAsyncHost,
) -> Result<KvSnapshot, NetworkError> {
    let mut keyed_state = state.clone();
    host.prepare_cluster_state_for_kv_sync_async(paths, &mut keyed_state)
        .await?;
    cleanup_cluster_endpoint_duplicates(&mut keyed_state);
    let authoritative = host
        .authoritative_cluster_kv_snapshot_async(paths, &mut keyed_state)
        .await?;
    let keyed_state_for_txn = keyed_state.clone();
    let snapshot_for_txn = authoritative.value.clone();
    let txn = runtime
        .spawn_blocking(move || {
            let store = InProcessKvStore::restore(snapshot_for_txn).map_err(kv_err)?;
            cluster_state_sync_txn_from_store(&store, &keyed_state_for_txn, true)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster state sync txn".to_string(),
            detail: format!("cluster state sync txn task failed: {source}"),
        })??;
    if txn.success.is_empty() {
        return Ok(authoritative.value);
    }
    let mut commit_state = keyed_state;
    let result = host
        .commit_cluster_kv_txn_with_quorum_async(
            paths,
            &mut commit_state,
            KvTxnRequest::generated(txn),
        )
        .await?;
    require_cluster_kv_txn_succeeded(result, "synchronizing cluster state")
        .map(|result| result.snapshot)
}

pub fn cluster_state_sync_txn_with_current_snapshot(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(KvSnapshot, KvTxn), NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot(paths)?;
    let store = InProcessKvStore::restore(snapshot.clone()).map_err(kv_err)?;
    cluster_state_sync_txn_from_store(&store, state, true).map(|txn| (snapshot, txn))
}

pub fn update_cluster_node_role_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_id: NodeId,
    role: ClusterNodeRole,
    now: OffsetDateTime,
    host: &mut (impl ClusterMemberMutationHost + ?Sized),
) -> Result<ClusterNodeRoleUpdate, NetworkError> {
    let authoritative = host.authoritative_cluster_kv_snapshot(paths, &mut state)?;
    let store = InProcessKvStore::restore(authoritative.value).map_err(kv_err)?;
    let member_key = cluster_member_key(&node_id);
    let Some(member_entry) = store.get(&member_key).cloned() else {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!("cluster member `{node_id}` was not found"),
        });
    };
    let mut member: ClusterMemberRecord = member_entry.decode().map_err(kv_err)?;
    let previous_role = member.node_role.clone();
    let changed = previous_role != role;
    if !changed {
        update_local_cluster_node_role_if_present(paths, &node_id, role.clone(), now)?;
        return Ok(ClusterNodeRoleUpdate {
            node_id,
            previous_role,
            role,
            changed,
        });
    }
    apply_cluster_node_role(&mut member, role.clone(), now);
    let txn = cluster_member_update_txn_from_store(&store, member_entry, &member)?;
    let result =
        host.commit_cluster_kv_txn_with_quorum(paths, &mut state, KvTxnRequest::generated(txn))?;
    require_cluster_kv_txn_succeeded(result, "updating cluster node role")?;
    update_local_cluster_node_role_if_present(paths, &node_id, role.clone(), now)?;
    Ok(ClusterNodeRoleUpdate {
        node_id,
        previous_role,
        role,
        changed,
    })
}

pub fn update_cluster_node_name_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_id: NodeId,
    name: String,
    now: OffsetDateTime,
    host: &mut (impl ClusterMemberMutationHost + ?Sized),
) -> Result<ClusterNodeNameUpdate, NetworkError> {
    let authoritative = host.authoritative_cluster_kv_snapshot(paths, &mut state)?;
    let store = InProcessKvStore::restore(authoritative.value).map_err(kv_err)?;
    let member_key = cluster_member_key(&node_id);
    let Some(member_entry) = store.get(&member_key).cloned() else {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!("cluster member `{node_id}` was not found"),
        });
    };
    let mut member: ClusterMemberRecord = member_entry.decode().map_err(kv_err)?;
    let previous_name = member.display_name.clone();
    let changed = previous_name != name;
    if !changed {
        update_local_cluster_node_name_if_present(paths, &node_id, name.clone(), now)?;
        return Ok(ClusterNodeNameUpdate {
            node_id,
            previous_name,
            name,
            changed,
        });
    }
    apply_cluster_node_name(&mut member, name.clone(), now);
    let txn = cluster_member_update_txn_from_store(&store, member_entry, &member)?;
    let result =
        host.commit_cluster_kv_txn_with_quorum(paths, &mut state, KvTxnRequest::generated(txn))?;
    require_cluster_kv_txn_succeeded(result, "updating cluster node name")?;
    update_local_cluster_node_name_if_present(paths, &node_id, name.clone(), now)?;
    Ok(ClusterNodeNameUpdate {
        node_id,
        previous_name,
        name,
        changed,
    })
}

fn update_local_cluster_node_role_if_present(
    paths: &ClusterPaths,
    node_id: &NodeId,
    role: ClusterNodeRole,
    now: OffsetDateTime,
) -> Result<(), NetworkError> {
    let _mutation_lock = acquire_local_cluster_state_mutation_lock(paths)?;
    let mut state = read_cluster_state_without_kv(paths)?;
    let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == *node_id)
    else {
        return Ok(());
    };
    if member.node_role == role {
        return Ok(());
    }
    apply_cluster_node_role(member, role, now);
    state.updated_at = now;
    write_cluster_state_unlocked(paths, &state)?;
    Ok(())
}

fn update_local_cluster_node_name_if_present(
    paths: &ClusterPaths,
    node_id: &NodeId,
    name: String,
    now: OffsetDateTime,
) -> Result<(), NetworkError> {
    let _mutation_lock = acquire_local_cluster_state_mutation_lock(paths)?;
    let mut state = read_cluster_state_without_kv(paths)?;
    let mut changed = false;
    if state.local_node_id == *node_id && state.local_node_display_name != name {
        state.local_node_display_name = name.clone();
        changed = true;
    }
    let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == *node_id)
    else {
        if changed {
            state.updated_at = now;
            write_cluster_state_unlocked(paths, &state)?;
        }
        return Ok(());
    };
    if member.display_name != name {
        apply_cluster_node_name(member, name, now);
        changed = true;
    }
    if changed {
        state.updated_at = now;
        write_cluster_state_unlocked(paths, &state)?;
    }
    Ok(())
}

fn apply_cluster_node_name(member: &mut ClusterMemberRecord, name: String, now: OffsetDateTime) {
    member.display_name = name;
    member.updated_at = now;
}

pub(crate) fn cluster_raft_config_change_record_txn(
    store: &InProcessKvStore,
    change: &ClusterKvRaftConfigChange,
) -> Result<KvTxn, NetworkError> {
    let key = sinomo_kv::cluster_kv_raft_config_change_key(&change.change_id);
    let compares = match store.get(&key) {
        Some(entry) => vec![KvCompare::ValueEquals {
            key: key.clone(),
            value: entry.value.clone(),
        }],
        None => vec![KvCompare::Missing { key: key.clone() }],
    };
    Ok(KvTxn {
        compares,
        success: vec![KvOp::put_serde(key, change, None).map_err(kv_err)?],
        failure: Vec::new(),
    })
}

pub(crate) fn require_cluster_kv_txn_succeeded(
    result: ClusterKvTxnCommitResult,
    stage: &str,
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    if result
        .txn_response
        .as_ref()
        .is_some_and(|response| response.succeeded)
    {
        Ok(result)
    } else {
        Err(NetworkError::InvalidClusterCommand {
            detail: format!("cluster KV compare failed while {stage}"),
        })
    }
}

fn commit_cluster_raft_config_change_record(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    store: &InProcessKvStore,
    change: &ClusterKvRaftConfigChange,
    stage: &str,
    host: &mut (impl ClusterMemberMutationHost + ?Sized),
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    let txn = cluster_raft_config_change_record_txn(store, change)?;
    let result =
        host.commit_cluster_kv_txn_with_quorum(paths, state, KvTxnRequest::generated(txn))?;
    require_cluster_kv_txn_succeeded(result, stage)
}

fn cluster_raft_config_change_apply_txn_with_lifecycle_fence(
    store: &InProcessKvStore,
    node_id: &NodeId,
    change: &ClusterKvRaftConfigChange,
    term: u64,
    next_revision: sinomo_kv::KvRevision,
    now: OffsetDateTime,
    lifecycle_fence: KvCompare,
) -> Result<KvTxn, NetworkError> {
    let config_key = sinomo_kv::cluster_kv_raft_config_key();
    let config_entry =
        store
            .get(&config_key)
            .cloned()
            .ok_or_else(|| NetworkError::ClusterReadOnly {
                detail: "cluster raft config is missing".to_string(),
            })?;
    let current_config: ClusterKvRaftConfigRecord = config_entry.decode().map_err(kv_err)?;
    let next_config = sinomo_kv::apply_cluster_kv_raft_config_change(
        &current_config,
        change,
        term,
        next_revision,
        now,
    )
    .map_err(kv_err)?;
    let member_key = cluster_member_key(node_id);
    let member_entry =
        store
            .get(&member_key)
            .cloned()
            .ok_or_else(|| NetworkError::InvalidClusterCommand {
                detail: format!("cluster member `{node_id}` was not found"),
            })?;
    let mut member: ClusterMemberRecord = member_entry.decode().map_err(kv_err)?;
    let next_role = match change.kind {
        ClusterKvRaftConfigChangeKind::AddLearner => ClusterMemberRole::Learner,
        ClusterKvRaftConfigChangeKind::PromoteLearner => ClusterMemberRole::Voter,
        ClusterKvRaftConfigChangeKind::DemoteVoter
        | ClusterKvRaftConfigChangeKind::RemoveMember => ClusterMemberRole::NonVoting,
    };
    apply_cluster_raft_role(&mut member, next_role, now);
    let mut success = cluster_member_update_ops_from_store(store, &member)?;
    success.push(KvOp::put_serde(config_key.clone(), &next_config, None).map_err(kv_err)?);
    Ok(KvTxn {
        compares: vec![
            KvCompare::ValueEquals {
                key: config_key,
                value: config_entry.value,
            },
            KvCompare::ValueEquals {
                key: member_key,
                value: member_entry.value,
            },
            lifecycle_fence,
        ],
        success,
        failure: Vec::new(),
    })
}

pub(crate) fn cluster_raft_config_change_apply_txn(
    store: &InProcessKvStore,
    node_id: &NodeId,
    change: &ClusterKvRaftConfigChange,
    term: u64,
    next_revision: sinomo_kv::KvRevision,
    now: OffsetDateTime,
) -> Result<KvTxn, NetworkError> {
    let change_key = sinomo_kv::cluster_kv_raft_config_change_key(&change.change_id);
    let change_entry =
        store
            .get(&change_key)
            .cloned()
            .ok_or_else(|| NetworkError::ClusterReadOnly {
                detail: format!("Raft config change `{}` is missing", change.change_id),
            })?;
    cluster_raft_config_change_apply_txn_with_lifecycle_fence(
        store,
        node_id,
        change,
        term,
        next_revision,
        now,
        KvCompare::ValueEquals {
            key: change_key,
            value: change_entry.value,
        },
    )
}

/// Atomically records a fresh Planned change and applies its target config.
///
/// A new learner does not need a separately visible plan commit: the member,
/// config, and durable lifecycle record either all commit or none do. Existing
/// Planned records still use `cluster_raft_config_change_apply_txn`, preserving
/// crash recovery for older/in-flight admissions.
pub(crate) fn cluster_raft_config_change_plan_apply_txn(
    store: &InProcessKvStore,
    node_id: &NodeId,
    change: &ClusterKvRaftConfigChange,
    term: u64,
    next_revision: sinomo_kv::KvRevision,
    now: OffsetDateTime,
) -> Result<KvTxn, NetworkError> {
    let change_key = sinomo_kv::cluster_kv_raft_config_change_key(&change.change_id);
    if store.get(&change_key).is_some() {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!("Raft config change `{}` already exists", change.change_id),
        });
    }
    let plan_op = KvOp::put_serde(change_key.clone(), change, None).map_err(kv_err)?;
    let mut txn = cluster_raft_config_change_apply_txn_with_lifecycle_fence(
        store,
        node_id,
        change,
        term,
        next_revision,
        now,
        KvCompare::Missing { key: change_key },
    )?;
    txn.success.insert(0, plan_op);
    Ok(txn)
}

pub(crate) enum ClusterRaftLearnerAdmissionPreparation {
    AlreadyAdmitted(KvSnapshot),
    Pending {
        store: InProcessKvStore,
        change: ClusterKvRaftConfigChange,
        plan_required: bool,
    },
}

pub(crate) fn prepare_cluster_raft_learner_admission(
    snapshot: KvSnapshot,
    node_id: &NodeId,
    now: OffsetDateTime,
) -> Result<ClusterRaftLearnerAdmissionPreparation, NetworkError> {
    let store = InProcessKvStore::restore(snapshot).map_err(kv_err)?;
    let member_key = cluster_member_key(node_id);
    let member_entry =
        store
            .get(&member_key)
            .cloned()
            .ok_or_else(|| NetworkError::InvalidClusterCommand {
                detail: format!("cluster member `{node_id}` was not found"),
            })?;
    let member: ClusterMemberRecord = member_entry.decode().map_err(kv_err)?;
    let current_config: ClusterKvRaftConfigRecord = store
        .get(&sinomo_kv::cluster_kv_raft_config_key())
        .ok_or_else(|| NetworkError::ClusterReadOnly {
            detail: "cluster raft config is missing".to_string(),
        })?
        .decode()
        .map_err(kv_err)?;
    match sinomo_kv::cluster_kv_raft_config_member_role(&current_config, node_id) {
        Some(ClusterMemberRole::Learner)
            if member.role == ClusterMemberRole::Learner
                && member.state == ClusterMemberState::Learner =>
        {
            return Ok(ClusterRaftLearnerAdmissionPreparation::AlreadyAdmitted(
                store.try_snapshot().map_err(kv_err)?,
            ));
        }
        Some(role) => {
            return Err(NetworkError::InvalidClusterCommand {
                detail: format!(
                    "cluster member `{node_id}` already has incompatible Raft role {role:?}"
                ),
            });
        }
        None if member.role == ClusterMemberRole::NonVoting
            && member.state == ClusterMemberState::NonVoting => {}
        None => {
            return Err(NetworkError::InvalidClusterCommand {
                detail: format!(
                    "cluster member `{node_id}` must be seeded as non-voting before learner admission"
                ),
            });
        }
    }

    let mut matching_change = None;
    for entry in store.prefix_entries("/cluster/raft/config-changes/") {
        let change: ClusterKvRaftConfigChange = entry.decode().map_err(kv_err)?;
        if change.node_id != *node_id
            || matches!(
                change.phase,
                ClusterKvRaftConfigChangePhase::Committed | ClusterKvRaftConfigChangePhase::Failed
            )
        {
            continue;
        }
        let reusable = change.kind == ClusterKvRaftConfigChangeKind::AddLearner
            && change.phase == ClusterKvRaftConfigChangePhase::Planned
            && change.base_config_revision == current_config.kv_revision
            && change.required_log_index.is_none()
            && change.catch_up_proof.is_none();
        if !reusable {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "cluster member `{node_id}` has incompatible in-flight Raft config change `{}`",
                    change.change_id
                ),
            });
        }
        if matching_change.replace(change).is_some() {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "cluster member `{node_id}` has multiple in-flight Raft learner admissions"
                ),
            });
        }
    }
    if let Some(change) = matching_change {
        return Ok(ClusterRaftLearnerAdmissionPreparation::Pending {
            store,
            change,
            plan_required: false,
        });
    }

    let change = sinomo_kv::begin_cluster_kv_raft_config_change(
        &current_config,
        Id::generate(),
        ClusterKvRaftConfigChangeKind::AddLearner,
        node_id.clone(),
        None,
        now,
    )
    .map_err(kv_err)?;
    Ok(ClusterRaftLearnerAdmissionPreparation::Pending {
        store,
        change,
        plan_required: true,
    })
}

fn persist_failed_cluster_raft_config_change(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    store: &InProcessKvStore,
    change: &ClusterKvRaftConfigChange,
    evidence: ClusterKvRaftConfigChangeEvidence,
    host: &mut (impl ClusterMemberMutationHost + ?Sized),
) {
    let mut failed = change.clone();
    if sinomo_kv::finish_cluster_kv_raft_config_change(&mut failed, false, evidence).is_ok() {
        let _ = commit_cluster_raft_config_change_record(
            paths,
            state,
            store,
            &failed,
            "recording failed Raft config change",
            host,
        );
    }
}

fn cluster_raft_config_change_target_role(
    change: &ClusterKvRaftConfigChange,
) -> Option<ClusterMemberRole> {
    match change.kind {
        ClusterKvRaftConfigChangeKind::AddLearner => Some(ClusterMemberRole::Learner),
        ClusterKvRaftConfigChangeKind::PromoteLearner => Some(ClusterMemberRole::Voter),
        ClusterKvRaftConfigChangeKind::DemoteVoter => Some(ClusterMemberRole::NonVoting),
        ClusterKvRaftConfigChangeKind::RemoveMember => None,
    }
}

fn cluster_raft_config_change_is_applied(
    store: &InProcessKvStore,
    change: &ClusterKvRaftConfigChange,
) -> Result<bool, NetworkError> {
    let current_config: ClusterKvRaftConfigRecord =
        match store.get(&sinomo_kv::cluster_kv_raft_config_key()) {
            Some(entry) => entry.decode().map_err(kv_err)?,
            None => return Ok(false),
        };
    let config_role =
        sinomo_kv::cluster_kv_raft_config_member_role(&current_config, &change.node_id);
    let config_matches = config_role == cluster_raft_config_change_target_role(change);
    if !config_matches {
        return Ok(false);
    }
    let Some(member_entry) = store.get(&cluster_member_key(&change.node_id)) else {
        return Ok(false);
    };
    let member: ClusterMemberRecord = member_entry.decode().map_err(kv_err)?;
    let expected_role =
        cluster_raft_config_change_target_role(change).unwrap_or(ClusterMemberRole::NonVoting);
    Ok(member.role == expected_role && member.state == cluster_raft_state_for_role(&expected_role))
}

fn cluster_raft_config_change_evidence(
    detail: impl Into<String>,
    authority: &KvAuthorityProof,
    now: OffsetDateTime,
) -> ClusterKvRaftConfigChangeEvidence {
    ClusterKvRaftConfigChangeEvidence {
        detail: detail.into(),
        term: authority.term,
        log_index: authority.commit_index,
        revision: authority.revision,
        observed_at: now,
    }
}

fn finish_reconciled_cluster_raft_config_change(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    store: &InProcessKvStore,
    mut change: ClusterKvRaftConfigChange,
    succeeded: bool,
    detail: impl Into<String>,
    authority: &KvAuthorityProof,
    now: OffsetDateTime,
    host: &mut (impl ClusterMemberMutationHost + ?Sized),
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    sinomo_kv::finish_cluster_kv_raft_config_change(
        &mut change,
        succeeded,
        cluster_raft_config_change_evidence(detail, authority, now),
    )
    .map_err(kv_err)?;
    commit_cluster_raft_config_change_record(
        paths,
        state,
        store,
        &change,
        if succeeded {
            "finishing recovered Raft config change"
        } else {
            "recording failed recovered Raft config change"
        },
        host,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterRaftConfigReconciliationOutcome {
    ResumedAndCommitted,
    AlreadyAppliedAndCommitted,
    Failed,
}

fn reconcile_cluster_raft_config_change_with_host(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    mut store: InProcessKvStore,
    mut authority: KvAuthorityProof,
    mut change: ClusterKvRaftConfigChange,
    now: OffsetDateTime,
    host: &mut (impl ClusterMemberMutationHost + ?Sized),
) -> Result<
    (
        InProcessKvStore,
        KvAuthorityProof,
        ClusterRaftConfigReconciliationOutcome,
    ),
    NetworkError,
> {
    if cluster_raft_config_change_is_applied(&store, &change)? {
        let finished = finish_reconciled_cluster_raft_config_change(
            paths,
            state,
            &store,
            change.clone(),
            true,
            format!(
                "reconciled {:?} for node {} from the authoritative committed config",
                change.kind, change.node_id
            ),
            &authority,
            now,
            host,
        )?;
        let next_authority = finished.authority;
        let next_store = InProcessKvStore::restore(finished.snapshot).map_err(kv_err)?;
        return Ok((
            next_store,
            next_authority,
            ClusterRaftConfigReconciliationOutcome::AlreadyAppliedAndCommitted,
        ));
    }

    let current_config = match store.get(&sinomo_kv::cluster_kv_raft_config_key()) {
        Some(entry) => entry
            .decode::<ClusterKvRaftConfigRecord>()
            .map_err(kv_err)?,
        None => {
            let failed = finish_reconciled_cluster_raft_config_change(
                paths,
                state,
                &store,
                change,
                false,
                "authoritative snapshot is missing the Raft config",
                &authority,
                now,
                host,
            )?;
            let next_authority = failed.authority;
            let next_store = InProcessKvStore::restore(failed.snapshot).map_err(kv_err)?;
            return Ok((
                next_store,
                next_authority,
                ClusterRaftConfigReconciliationOutcome::Failed,
            ));
        }
    };
    if current_config.kv_revision != change.base_config_revision {
        let failed = finish_reconciled_cluster_raft_config_change(
            paths,
            state,
            &store,
            change.clone(),
            false,
            format!(
                "stale Raft config change planned at config revision {} but authoritative config is revision {}",
                change.base_config_revision, current_config.kv_revision
            ),
            &authority,
            now,
            host,
        )?;
        let next_authority = failed.authority;
        let next_store = InProcessKvStore::restore(failed.snapshot).map_err(kv_err)?;
        return Ok((
            next_store,
            next_authority,
            ClusterRaftConfigReconciliationOutcome::Failed,
        ));
    }

    match change.phase {
        ClusterKvRaftConfigChangePhase::CatchingUp => {
            if change.kind != ClusterKvRaftConfigChangeKind::PromoteLearner {
                let failed = finish_reconciled_cluster_raft_config_change(
                    paths,
                    state,
                    &store,
                    change,
                    false,
                    "only learner promotion may remain in the catching-up phase",
                    &authority,
                    now,
                    host,
                )?;
                let next_authority = failed.authority;
                let next_store = InProcessKvStore::restore(failed.snapshot).map_err(kv_err)?;
                return Ok((
                    next_store,
                    next_authority,
                    ClusterRaftConfigReconciliationOutcome::Failed,
                ));
            }
            let Some(required_log_index) = change.required_log_index else {
                let failed = finish_reconciled_cluster_raft_config_change(
                    paths,
                    state,
                    &store,
                    change,
                    false,
                    "catching-up learner promotion has no required log index",
                    &authority,
                    now,
                    host,
                )?;
                let next_authority = failed.authority;
                let next_store = InProcessKvStore::restore(failed.snapshot).map_err(kv_err)?;
                return Ok((
                    next_store,
                    next_authority,
                    ClusterRaftConfigReconciliationOutcome::Failed,
                ));
            };
            // Transport or reachability failure is not durable failure evidence.
            // Leave the record CatchingUp so a later lifecycle retry can prove it.
            let proof = host.prove_cluster_raft_member_caught_up(
                paths,
                state,
                &change.node_id,
                required_log_index,
                now,
            )?;
            if let Err(error) = sinomo_kv::record_cluster_kv_raft_catch_up_proof(&mut change, proof)
            {
                let failed = finish_reconciled_cluster_raft_config_change(
                    paths,
                    state,
                    &store,
                    change,
                    false,
                    format!("invalid learner catch-up evidence: {error}"),
                    &authority,
                    now,
                    host,
                )?;
                let next_authority = failed.authority;
                let next_store = InProcessKvStore::restore(failed.snapshot).map_err(kv_err)?;
                return Ok((
                    next_store,
                    next_authority,
                    ClusterRaftConfigReconciliationOutcome::Failed,
                ));
            }
            let ready = commit_cluster_raft_config_change_record(
                paths,
                state,
                &store,
                &change,
                "recovering learner catch-up proof",
                host,
            )?;
            authority = ready.authority;
            store = InProcessKvStore::restore(ready.snapshot).map_err(kv_err)?;
        }
        ClusterKvRaftConfigChangePhase::Planned => {
            if change.kind == ClusterKvRaftConfigChangeKind::PromoteLearner {
                let failed = finish_reconciled_cluster_raft_config_change(
                    paths,
                    state,
                    &store,
                    change,
                    false,
                    "learner promotion cannot be resumed from the planned phase",
                    &authority,
                    now,
                    host,
                )?;
                let next_authority = failed.authority;
                let next_store = InProcessKvStore::restore(failed.snapshot).map_err(kv_err)?;
                return Ok((
                    next_store,
                    next_authority,
                    ClusterRaftConfigReconciliationOutcome::Failed,
                ));
            }
        }
        ClusterKvRaftConfigChangePhase::ReadyToCommit => {
            if change.kind != ClusterKvRaftConfigChangeKind::PromoteLearner
                || change
                    .catch_up_proof
                    .as_ref()
                    .map_or(true, |proof| !proof.proves_required_index())
            {
                let failed = finish_reconciled_cluster_raft_config_change(
                    paths,
                    state,
                    &store,
                    change,
                    false,
                    "ready Raft config change lacks valid learner catch-up evidence",
                    &authority,
                    now,
                    host,
                )?;
                let next_authority = failed.authority;
                let next_store = InProcessKvStore::restore(failed.snapshot).map_err(kv_err)?;
                return Ok((
                    next_store,
                    next_authority,
                    ClusterRaftConfigReconciliationOutcome::Failed,
                ));
            }
        }
        ClusterKvRaftConfigChangePhase::Committed | ClusterKvRaftConfigChangePhase::Failed => {
            unreachable!("terminal config changes are filtered before reconciliation")
        }
    }

    let apply_txn = match cluster_raft_config_change_apply_txn(
        &store,
        &change.node_id,
        &change,
        authority.term,
        store.try_snapshot().map_err(kv_err)?.revision.next(),
        now,
    ) {
        Ok(txn) => txn,
        Err(error) => {
            let failed = finish_reconciled_cluster_raft_config_change(
                paths,
                state,
                &store,
                change,
                false,
                format!("invalid recovered Raft config change: {error}"),
                &authority,
                now,
                host,
            )?;
            let next_authority = failed.authority;
            let next_store = InProcessKvStore::restore(failed.snapshot).map_err(kv_err)?;
            return Ok((
                next_store,
                next_authority,
                ClusterRaftConfigReconciliationOutcome::Failed,
            ));
        }
    };
    let applied =
        host.commit_cluster_kv_txn_with_quorum(paths, state, KvTxnRequest::generated(apply_txn))?;
    let applied = require_cluster_kv_txn_succeeded(applied, "resuming Raft config change")?;
    authority = applied.authority;
    store = InProcessKvStore::restore(applied.snapshot).map_err(kv_err)?;
    if !cluster_raft_config_change_is_applied(&store, &change)? {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!(
                "recovered Raft config change `{}` committed without its target state",
                change.change_id
            ),
        });
    }
    let finished = finish_reconciled_cluster_raft_config_change(
        paths,
        state,
        &store,
        change.clone(),
        true,
        format!(
            "resumed and committed {:?} for node {}",
            change.kind, change.node_id
        ),
        &authority,
        now,
        host,
    )?;
    let next_authority = finished.authority;
    let next_store = InProcessKvStore::restore(finished.snapshot).map_err(kv_err)?;
    Ok((
        next_store,
        next_authority,
        ClusterRaftConfigReconciliationOutcome::ResumedAndCommitted,
    ))
}

/// Reconciles durable, nonterminal Raft membership changes from a fresh quorum
/// snapshot before a new lifecycle operation starts.
///
/// Every write retains the record/config/member CAS fences used by the original
/// transition. A transient catch-up failure leaves the record retryable, while
/// stale or malformed records receive quorum-committed failure evidence.
pub fn reconcile_cluster_raft_config_changes_with_host(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    now: OffsetDateTime,
    host: &mut (impl ClusterMemberMutationHost + ?Sized),
) -> Result<ClusterRaftConfigReconciliationReport, NetworkError> {
    reconcile_cluster_raft_config_changes_bounded_with_host(
        paths,
        state,
        now,
        MAX_CLUSTER_RAFT_CONFIG_RECONCILIATIONS_PER_PASS,
        host,
    )
}

pub fn reconcile_cluster_raft_config_changes_bounded_with_host(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    now: OffsetDateTime,
    max_change_count: usize,
    host: &mut (impl ClusterMemberMutationHost + ?Sized),
) -> Result<ClusterRaftConfigReconciliationReport, NetworkError> {
    let max_change_count = max_change_count
        .max(1)
        .min(MAX_CLUSTER_RAFT_CONFIG_RECONCILIATIONS_PER_PASS);
    let authoritative = host.authoritative_cluster_kv_snapshot(paths, state)?;
    let mut authority = authoritative.authority;
    let mut store = InProcessKvStore::restore(authoritative.value).map_err(kv_err)?;
    let changes = store
        .prefix_entries("/cluster/raft/config-changes/")
        .map(|entry| entry.decode::<ClusterKvRaftConfigChange>().map_err(kv_err))
        .collect::<Result<Vec<_>, _>>()?;
    let pending_change_count = changes
        .iter()
        .filter(|change| {
            !matches!(
                change.phase,
                ClusterKvRaftConfigChangePhase::Committed | ClusterKvRaftConfigChangePhase::Failed
            )
        })
        .count();
    let mut report = ClusterRaftConfigReconciliationReport {
        inspected_change_count: changes.len(),
        deferred_change_count: pending_change_count.saturating_sub(max_change_count),
        ..ClusterRaftConfigReconciliationReport::default()
    };
    for original in changes
        .into_iter()
        .filter(|change| {
            !matches!(
                change.phase,
                ClusterKvRaftConfigChangePhase::Committed | ClusterKvRaftConfigChangePhase::Failed
            )
        })
        .take(max_change_count)
    {
        let key = sinomo_kv::cluster_kv_raft_config_change_key(&original.change_id);
        let Some(entry) = store.get(&key) else {
            continue;
        };
        let change: ClusterKvRaftConfigChange = entry.decode().map_err(kv_err)?;
        if matches!(
            change.phase,
            ClusterKvRaftConfigChangePhase::Committed | ClusterKvRaftConfigChangePhase::Failed
        ) {
            continue;
        }
        let change_id = change.change_id.clone();
        let (next_store, next_authority, outcome) = reconcile_cluster_raft_config_change_with_host(
            paths, state, store, authority, change, now, host,
        )?;
        store = next_store;
        authority = next_authority;
        match outcome {
            ClusterRaftConfigReconciliationOutcome::ResumedAndCommitted => {
                report.resumed_change_ids.push(change_id.clone());
                report.committed_change_ids.push(change_id);
            }
            ClusterRaftConfigReconciliationOutcome::AlreadyAppliedAndCommitted => {
                report.committed_change_ids.push(change_id);
            }
            ClusterRaftConfigReconciliationOutcome::Failed => {
                report.failed_change_ids.push(change_id);
            }
        }
    }
    Ok(report)
}

fn execute_cluster_raft_config_change(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    store: InProcessKvStore,
    node_id: &NodeId,
    kind: ClusterKvRaftConfigChangeKind,
    required_log_index: Option<u64>,
    now: OffsetDateTime,
    host: &mut (impl ClusterMemberMutationHost + ?Sized),
) -> Result<(InProcessKvStore, ClusterKvTxnCommitResult), NetworkError> {
    let current_config: ClusterKvRaftConfigRecord = store
        .get(&sinomo_kv::cluster_kv_raft_config_key())
        .ok_or_else(|| NetworkError::ClusterReadOnly {
            detail: "cluster raft config is missing".to_string(),
        })?
        .decode()
        .map_err(kv_err)?;
    let mut change = sinomo_kv::begin_cluster_kv_raft_config_change(
        &current_config,
        Id::generate(),
        kind,
        node_id.clone(),
        required_log_index,
        now,
    )
    .map_err(kv_err)?;
    let planned = commit_cluster_raft_config_change_record(
        paths,
        state,
        &store,
        &change,
        "planning Raft config change",
        host,
    )?;
    let mut store = InProcessKvStore::restore(planned.snapshot.clone()).map_err(kv_err)?;
    let mut latest_authority = planned.authority;

    if kind == ClusterKvRaftConfigChangeKind::PromoteLearner {
        let required_log_index = required_log_index.expect("promotion requires log index");
        let proof = match host.prove_cluster_raft_member_caught_up(
            paths,
            state,
            node_id,
            required_log_index,
            now,
        ) {
            Ok(proof) => proof,
            Err(error) => {
                persist_failed_cluster_raft_config_change(
                    paths,
                    state,
                    &store,
                    &change,
                    ClusterKvRaftConfigChangeEvidence {
                        detail: error.to_string(),
                        term: latest_authority.term,
                        log_index: latest_authority.commit_index,
                        revision: latest_authority.revision,
                        observed_at: now,
                    },
                    host,
                );
                return Err(error);
            }
        };
        sinomo_kv::record_cluster_kv_raft_catch_up_proof(&mut change, proof).map_err(kv_err)?;
        let proved = commit_cluster_raft_config_change_record(
            paths,
            state,
            &store,
            &change,
            "recording learner catch-up proof",
            host,
        )?;
        latest_authority = proved.authority;
        store = InProcessKvStore::restore(proved.snapshot).map_err(kv_err)?;
    }

    let apply_txn = cluster_raft_config_change_apply_txn(
        &store,
        node_id,
        &change,
        latest_authority.term,
        store.try_snapshot().map_err(kv_err)?.revision.next(),
        now,
    )?;
    let applied = match host.commit_cluster_kv_txn_with_quorum(
        paths,
        state,
        KvTxnRequest::generated(apply_txn),
    ) {
        Ok(result) => require_cluster_kv_txn_succeeded(result, "applying Raft config change"),
        Err(error) => Err(error),
    };
    let applied = match applied {
        Ok(result) => result,
        Err(error) => {
            persist_failed_cluster_raft_config_change(
                paths,
                state,
                &store,
                &change,
                ClusterKvRaftConfigChangeEvidence {
                    detail: error.to_string(),
                    term: latest_authority.term,
                    log_index: latest_authority.commit_index,
                    revision: latest_authority.revision,
                    observed_at: now,
                },
                host,
            );
            return Err(error);
        }
    };
    store = InProcessKvStore::restore(applied.snapshot.clone()).map_err(kv_err)?;
    sinomo_kv::finish_cluster_kv_raft_config_change(
        &mut change,
        true,
        ClusterKvRaftConfigChangeEvidence {
            detail: format!("committed {:?} for node {}", kind, node_id),
            term: applied.authority.term,
            log_index: applied.authority.commit_index,
            revision: applied.authority.revision,
            observed_at: now,
        },
    )
    .map_err(kv_err)?;
    let finished = commit_cluster_raft_config_change_record(
        paths,
        state,
        &store,
        &change,
        "finishing Raft config change",
        host,
    )?;
    let store = InProcessKvStore::restore(finished.snapshot.clone()).map_err(kv_err)?;
    Ok((store, finished))
}

/// Adds an already-seeded non-voting member to the replicated Raft learner
/// set using the same planned -> applied -> finished evidence lifecycle as the
/// synchronous membership mutation path.
///
/// Join registration uses this narrow async form after its invitation and
/// member-row CAS has committed. Keeping the network awaits in the async host
/// avoids blocking a Tokio worker while preserving the durable config-change
/// record sequence.
pub async fn add_cluster_raft_learner_async_with_host(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    node_id: &NodeId,
    now: OffsetDateTime,
    host: &mut impl ClusterMemberMutationAsyncHost,
) -> Result<KvSnapshot, NetworkError> {
    let authoritative = host
        .authoritative_cluster_kv_snapshot_async(paths, state)
        .await?;
    let mut apply_term = authoritative.authority.term;
    let (mut store, mut change, plan_required) =
        match prepare_cluster_raft_learner_admission(authoritative.value, node_id, now)? {
            ClusterRaftLearnerAdmissionPreparation::AlreadyAdmitted(snapshot) => {
                // An exact registration retry still crosses a fresh quorum
                // barrier above, but does not create duplicate change evidence.
                return Ok(snapshot);
            }
            ClusterRaftLearnerAdmissionPreparation::Pending {
                store,
                change,
                plan_required,
            } => (store, change, plan_required),
        };
    if plan_required {
        let planned = host
            .commit_cluster_kv_txn_with_quorum_async(
                paths,
                state,
                KvTxnRequest::generated(cluster_raft_config_change_record_txn(&store, &change)?),
            )
            .await
            .and_then(|result| {
                require_cluster_kv_txn_succeeded(result, "planning Raft learner admission")
            })?;
        apply_term = planned.authority.term;
        store = InProcessKvStore::restore(planned.snapshot).map_err(kv_err)?;
    }

    let apply_txn = cluster_raft_config_change_apply_txn(
        &store,
        node_id,
        &change,
        apply_term,
        store.try_snapshot().map_err(kv_err)?.revision.next(),
        now,
    )?;
    let applied = host
        .commit_cluster_kv_txn_with_quorum_async(paths, state, KvTxnRequest::generated(apply_txn))
        .await
        .and_then(|result| {
            require_cluster_kv_txn_succeeded(result, "applying Raft learner admission")
        })?;
    store = InProcessKvStore::restore(applied.snapshot).map_err(kv_err)?;
    sinomo_kv::finish_cluster_kv_raft_config_change(
        &mut change,
        true,
        ClusterKvRaftConfigChangeEvidence {
            detail: format!("committed AddLearner for node {node_id}"),
            term: applied.authority.term,
            log_index: applied.authority.commit_index,
            revision: applied.authority.revision,
            observed_at: now,
        },
    )
    .map_err(kv_err)?;
    let finished = host
        .commit_cluster_kv_txn_with_quorum_async(
            paths,
            state,
            KvTxnRequest::generated(cluster_raft_config_change_record_txn(&store, &change)?),
        )
        .await
        .and_then(|result| {
            require_cluster_kv_txn_succeeded(result, "finishing Raft learner admission")
        })?;
    Ok(finished.snapshot)
}

pub fn update_cluster_state_authority_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_id: NodeId,
    role: ClusterMemberRole,
    now: OffsetDateTime,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterStateAuthorityUpdate, NetworkError> {
    let reconciliation =
        reconcile_cluster_raft_config_changes_with_host(paths, &mut state, now, host)?;
    if reconciliation.deferred_change_count > 0 {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "deferred {} Raft config lifecycle recoveries; retry before starting a new config change",
                reconciliation.deferred_change_count
            ),
        });
    }
    let authoritative = host.authoritative_cluster_kv_snapshot(paths, &mut state)?;
    let required_log_index = authoritative.authority.commit_index;
    let mut store = InProcessKvStore::restore(authoritative.value).map_err(kv_err)?;
    let member_key = cluster_member_key(&node_id);
    let Some(member_entry) = store.get(&member_key).cloned() else {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!("cluster member `{node_id}` was not found"),
        });
    };
    let new_state = cluster_raft_state_for_role(&role);
    let mut member: ClusterMemberRecord = member_entry.decode().map_err(kv_err)?;
    let previous_role = member.role.clone();
    let previous_state = member.state.clone();
    let config_role = store
        .get(&sinomo_kv::cluster_kv_raft_config_key())
        .and_then(|entry| entry.decode::<ClusterKvRaftConfigRecord>().ok())
        .and_then(|config| sinomo_kv::cluster_kv_raft_config_member_role(&config, &node_id));
    let config_matches = if role == ClusterMemberRole::NonVoting {
        config_role.is_none() || config_role == Some(ClusterMemberRole::NonVoting)
    } else {
        config_role.as_ref() == Some(&role)
    };
    let changed = previous_role != role || previous_state != new_state || !config_matches;
    if changed {
        let run_change =
            |kind,
             required_index,
             store: InProcessKvStore,
             state: &mut ClusterBootstrapState,
             host: &mut dyn ClusterMemberMutationHost|
             -> Result<(InProcessKvStore, ClusterKvTxnCommitResult), NetworkError> {
                execute_cluster_raft_config_change(
                    paths,
                    state,
                    store,
                    &node_id,
                    kind,
                    required_index,
                    now,
                    host,
                )
            };

        match role {
            ClusterMemberRole::Voter => {
                if config_role == Some(ClusterMemberRole::Voter) {
                    // Only the member record needs repair.
                } else {
                    let promotion_index = if config_role == Some(ClusterMemberRole::Learner) {
                        required_log_index
                    } else {
                        let (next_store, result) = run_change(
                            ClusterKvRaftConfigChangeKind::AddLearner,
                            None,
                            store,
                            &mut state,
                            host,
                        )?;
                        store = next_store;
                        result.authority.commit_index
                    };
                    let (next_store, _) = run_change(
                        ClusterKvRaftConfigChangeKind::PromoteLearner,
                        Some(promotion_index),
                        store,
                        &mut state,
                        host,
                    )?;
                    store = next_store;
                }
            }
            ClusterMemberRole::Learner => {
                if config_role == Some(ClusterMemberRole::Voter) {
                    let (next_store, _) = run_change(
                        ClusterKvRaftConfigChangeKind::DemoteVoter,
                        None,
                        store,
                        &mut state,
                        host,
                    )?;
                    store = next_store;
                }
                if config_role != Some(ClusterMemberRole::Learner) {
                    let (next_store, _) = run_change(
                        ClusterKvRaftConfigChangeKind::AddLearner,
                        None,
                        store,
                        &mut state,
                        host,
                    )?;
                    store = next_store;
                }
            }
            ClusterMemberRole::NonVoting => match config_role {
                Some(ClusterMemberRole::Voter) => {
                    let (next_store, _) = run_change(
                        ClusterKvRaftConfigChangeKind::DemoteVoter,
                        None,
                        store,
                        &mut state,
                        host,
                    )?;
                    store = next_store;
                }
                Some(ClusterMemberRole::Learner) => {
                    let (next_store, _) = run_change(
                        ClusterKvRaftConfigChangeKind::RemoveMember,
                        None,
                        store,
                        &mut state,
                        host,
                    )?;
                    store = next_store;
                }
                Some(ClusterMemberRole::NonVoting) | None => {}
            },
        }

        let latest_member_entry =
            store
                .get(&member_key)
                .cloned()
                .ok_or_else(|| NetworkError::InvalidClusterCommand {
                    detail: format!("cluster member `{node_id}` was not found after config change"),
                })?;
        member = latest_member_entry.decode().map_err(kv_err)?;
        if member.role != role || member.state != new_state {
            apply_cluster_raft_role(&mut member, role.clone(), now);
            let result = host.commit_cluster_kv_txn_with_quorum(
                paths,
                &mut state,
                KvTxnRequest::generated(cluster_member_update_txn_from_store(
                    &store,
                    latest_member_entry,
                    &member,
                )?),
            )?;
            require_cluster_kv_txn_succeeded(
                result,
                "repairing cluster state-authority member record",
            )?;
        }
    }
    Ok(ClusterStateAuthorityUpdate {
        node_id,
        previous_role,
        previous_state,
        role,
        state: new_state,
        changed,
    })
}

pub fn remove_cluster_members_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_ids: Vec<NodeId>,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterMemberRemoval, NetworkError> {
    let requested = node_ids.into_iter().collect::<BTreeSet<_>>();
    if requested.is_empty() {
        return Err(NetworkError::InvalidClusterCommand {
            detail: "no cluster members were specified".to_string(),
        });
    }
    let local_shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    if requested.contains(&state.local_node_id) || requested.contains(&local_shadow_node_id) {
        return Err(NetworkError::InvalidClusterCommand {
            detail: "use `sinomo cluster leave` to remove the local daemon members".to_string(),
        });
    }
    // Member deletion is itself a Raft-config lifecycle entrypoint. Recover
    // any older nonterminal transition before deriving removal work so this
    // operation cannot race or bypass durable config-change ownership.
    let reconciliation = reconcile_cluster_raft_config_changes_with_host(
        paths,
        &mut state,
        OffsetDateTime::now_utc(),
        host,
    )?;
    if reconciliation.deferred_change_count > 0 {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "deferred {} Raft config lifecycle recoveries; retry before removing cluster members",
                reconciliation.deferred_change_count
            ),
        });
    }
    let authoritative = host.authoritative_cluster_kv_snapshot(paths, &mut state)?;
    let mut store = InProcessKvStore::restore(authoritative.value).map_err(kv_err)?;

    let mut removed = Vec::new();
    for node_id in &requested {
        let member_key = cluster_member_key(node_id);
        let Some(member_entry) = store.get(&member_key).cloned() else {
            return Err(NetworkError::InvalidClusterCommand {
                detail: format!("cluster member `{node_id}` was not found"),
            });
        };
        let member: ClusterMemberRecord = member_entry.decode().map_err(kv_err)?;
        if sinomo_kv::cluster_kv_store_member_is_raft_voter(&store, &member.node_id)
            .map_err(kv_err)?
        {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "cluster member `{}` is a state authority; change its authority role before removal",
                    member.node_id
                ),
            });
        }
        removed.push(member);
    }

    for member in &removed {
        let config_role = store
            .get(&sinomo_kv::cluster_kv_raft_config_key())
            .and_then(|entry| entry.decode::<ClusterKvRaftConfigRecord>().ok())
            .and_then(|config| {
                sinomo_kv::cluster_kv_raft_config_member_role(&config, &member.node_id)
            });
        if config_role.is_some() {
            let (next_store, _) = execute_cluster_raft_config_change(
                paths,
                &mut state,
                store,
                &member.node_id,
                ClusterKvRaftConfigChangeKind::RemoveMember,
                None,
                OffsetDateTime::now_utc(),
                host,
            )?;
            store = next_store;
        }
    }

    for txn in cluster_member_removal_txns_from_store(&store, &removed)? {
        let result = host.commit_cluster_kv_txn_with_quorum(
            paths,
            &mut state,
            KvTxnRequest::generated(txn),
        )?;
        require_cluster_kv_txn_succeeded(result, "removing cluster members")?;
    }
    Ok(ClusterMemberRemoval { removed })
}

pub fn cleanup_legacy_shadow_daemon_members_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterMemberRemoval, NetworkError> {
    let authoritative = host.authoritative_cluster_kv_snapshot(paths, &mut state)?;
    let store = InProcessKvStore::restore(authoritative.value).map_err(kv_err)?;
    let snapshot = store.try_snapshot().map_err(kv_err)?;
    let policies = read_cluster_capability_policies_from_snapshot(&snapshot)?;
    let mut current_member_ids = BTreeSet::new();
    let mut removed = Vec::new();
    for entry in store.prefix_entries("/cluster/members/") {
        let mut member: ClusterMemberRecord = entry.decode().map_err(kv_err)?;
        current_member_ids.insert(member.node_id.clone());
        if let Some(policy) = policies.get(&member.node_id) {
            member.capabilities = policy.capabilities.clone();
            member.allowed_capabilities = policy.allowed_capabilities.clone();
        }
        if !cluster_member_is_legacy_shadow_daemon(&member) {
            continue;
        }
        if sinomo_kv::cluster_kv_store_member_is_raft_voter(&store, &member.node_id)
            .map_err(kv_err)?
        {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "legacy shadow member `{}` is a state authority; change its authority role before cleanup",
                    member.node_id
                ),
            });
        }
        removed.push(member);
    }
    removed.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    let orphan_artifact_node_ids =
        cluster_legacy_shadow_orphan_artifact_node_ids_from_store(&store, &current_member_ids)?;

    for txn in cluster_member_removal_txns_from_store(&store, &removed)? {
        let result = host.commit_cluster_kv_txn_with_quorum(
            paths,
            &mut state,
            KvTxnRequest::generated(txn),
        )?;
        require_cluster_kv_txn_succeeded(result, "removing legacy shadow members")?;
    }
    for txn in cluster_legacy_shadow_orphan_artifact_cleanup_txns_from_store(
        &store,
        &orphan_artifact_node_ids,
    )? {
        let result = host.commit_cluster_kv_txn_with_quorum(
            paths,
            &mut state,
            KvTxnRequest::generated(txn),
        )?;
        require_cluster_kv_txn_succeeded(result, "cleaning legacy shadow member artifacts")?;
    }
    Ok(ClusterMemberRemoval { removed })
}

#[cfg(any(test, feature = "test-support"))]
pub fn sync_cluster_certificate_to_kv(
    paths: &ClusterPaths,
    certificate: &ClusterCertificate,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_certificate_key(certificate),
        certificate,
    )
}

#[cfg(any(test, feature = "test-support"))]
fn put_cluster_kv_value<T: Serialize + ?Sized>(
    paths: &ClusterPaths,
    node_id: &NodeId,
    key: String,
    value: &T,
) -> Result<KvSnapshot, NetworkError> {
    let op = KvOp::put_serde(key, value, None).map_err(kv_err)?;
    if cluster_kv_put_value_is_noop(paths, &op)? {
        return read_cluster_kv_snapshot(paths);
    }
    append_cluster_raft_command(paths, node_id, KvCommand::Op(op))
}

#[cfg(any(test, feature = "test-support"))]
fn cluster_kv_put_value_is_noop(paths: &ClusterPaths, op: &KvOp) -> Result<bool, NetworkError> {
    let KvOp::Put {
        key,
        value,
        lease_id,
    } = op
    else {
        return Ok(false);
    };
    let Some(snapshot) = read_cluster_kv_cached_snapshot_if_present(paths)? else {
        return Ok(false);
    };
    let Some(entry) = snapshot.get(key) else {
        return Ok(false);
    };
    Ok(entry.value.as_slice() == value.as_slice() && entry.lease_id == *lease_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_kv::{
        ClusterKvRaftCatchUpProof, ClusterKvRaftConfigChangePhase, KvAuthorityBasis,
        KvAuthorityProof,
    };
    use sinomo_node::ClusterId;

    struct MemoryMutationHost {
        store: InProcessKvStore,
        commit_index: u64,
        catch_up_failure: bool,
        force_compare_failure: bool,
        catch_up_requests: Vec<(NodeId, u64)>,
    }

    impl MemoryMutationHost {
        fn new(state: &ClusterBootstrapState) -> Self {
            let mut store = InProcessKvStore::new();
            let txn = cluster_state_sync_txn_from_store(&store, state, true)
                .expect("initial cluster state txn");
            store.txn(txn).expect("initial cluster state");
            Self {
                store,
                commit_index: 10,
                catch_up_failure: false,
                force_compare_failure: false,
                catch_up_requests: Vec::new(),
            }
        }

        fn authority(&self) -> KvAuthorityProof {
            KvAuthorityProof {
                term: 1,
                commit_index: self.commit_index,
                revision: self.store.revision(),
                basis: KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 1,
                    required_quorum: 1,
                },
            }
        }

        fn config_changes(&self) -> Vec<ClusterKvRaftConfigChange> {
            self.store
                .prefix_entries("/cluster/raft/config-changes/")
                .map(|entry| entry.decode().expect("config change record"))
                .collect()
        }

        fn raft_config(&self) -> ClusterKvRaftConfigRecord {
            self.store
                .get(&sinomo_kv::cluster_kv_raft_config_key())
                .expect("raft config")
                .decode()
                .expect("decode raft config")
        }

        fn put_config_change(&mut self, change: &ClusterKvRaftConfigChange) {
            let txn = cluster_raft_config_change_record_txn(&self.store, change)
                .expect("config change record txn");
            let response = self.store.txn(txn).expect("store config change record");
            assert!(response.succeeded);
        }

        fn config_change(&self, change_id: &Id) -> ClusterKvRaftConfigChange {
            self.store
                .get(&sinomo_kv::cluster_kv_raft_config_change_key(change_id))
                .expect("config change")
                .decode()
                .expect("decode config change")
        }
    }

    impl ClusterMemberMutationHost for MemoryMutationHost {
        fn authoritative_cluster_kv_snapshot(
            &mut self,
            _paths: &ClusterPaths,
            _state: &mut ClusterBootstrapState,
        ) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
            Ok(KvAuthoritativeRead {
                value: self.store.try_snapshot().map_err(kv_err)?,
                authority: self.authority(),
            })
        }

        fn commit_cluster_kv_txn_with_quorum(
            &mut self,
            _paths: &ClusterPaths,
            _state: &mut ClusterBootstrapState,
            request: KvTxnRequest,
        ) -> Result<ClusterKvTxnCommitResult, NetworkError> {
            let request_id = request.request_id.clone();
            let txn_response = if self.force_compare_failure {
                sinomo_kv::KvTxnResponse {
                    succeeded: false,
                    revision: self.store.revision(),
                    responses: Vec::new(),
                }
            } else {
                self.store.txn(request.txn).map_err(kv_err)?
            };
            self.commit_index = self.commit_index.saturating_add(1);
            let snapshot = self.store.try_snapshot().map_err(kv_err)?;
            Ok(ClusterKvTxnCommitResult {
                request_id,
                snapshot,
                txn_response: Some(txn_response),
                authority: self.authority(),
                replayed_committed_request: false,
            })
        }

        fn prove_cluster_raft_member_caught_up(
            &mut self,
            _paths: &ClusterPaths,
            _state: &ClusterBootstrapState,
            node_id: &NodeId,
            required_log_index: u64,
            now: OffsetDateTime,
        ) -> Result<ClusterKvRaftCatchUpProof, NetworkError> {
            self.catch_up_requests
                .push((node_id.clone(), required_log_index));
            if self.catch_up_failure {
                return Err(NetworkError::ClusterReadOnly {
                    detail: "learner is offline".to_string(),
                });
            }
            Ok(ClusterKvRaftCatchUpProof {
                node_id: node_id.clone(),
                term: 1,
                required_log_index,
                observed_match_index: required_log_index,
                observed_at: now,
            })
        }
    }

    fn non_voting_member_state() -> (ClusterBootstrapState, NodeId, OffsetDateTime) {
        let now = OffsetDateTime::UNIX_EPOCH;
        let cluster_id =
            ClusterId::from_legacy_named("config_lifecycle_cluster").expect("cluster id");
        let local = NodeId::from_legacy_named("config_lifecycle_local").expect("local node id");
        let target = NodeId::from_legacy_named("config_lifecycle_target").expect("target node id");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "config lifecycle",
            local,
            "local",
            "local-pk",
            now,
        );
        state.members.push(ClusterMemberRecord::local_non_voting(
            cluster_id,
            target.clone(),
            "target",
            "target-pk",
            now,
        ));
        (state, target, now)
    }

    #[test]
    fn voter_transition_persists_add_catch_up_promote_lifecycle() {
        let (state, target, now) = non_voting_member_state();
        let temp = sinomo_testdir::TestDir::new("cluster-config-lifecycle");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(temp.path()));
        let mut host = MemoryMutationHost::new(&state);

        let update = update_cluster_state_authority_with_host(
            &paths,
            state,
            target.clone(),
            ClusterMemberRole::Voter,
            now,
            &mut host,
        )
        .expect("promote non-voter through learner lifecycle");

        assert!(update.changed);
        let config: ClusterKvRaftConfigRecord = host
            .store
            .get(&sinomo_kv::cluster_kv_raft_config_key())
            .expect("raft config")
            .decode()
            .expect("decode raft config");
        assert!(config.voters.contains(&target));
        assert!(!config.learners.contains(&target));
        assert!(!config.non_voting.contains(&target));
        assert_eq!(host.catch_up_requests.len(), 1);

        let changes = host.config_changes();
        assert_eq!(changes.len(), 2);
        assert!(changes.iter().all(|change| {
            change.phase == ClusterKvRaftConfigChangePhase::Committed && !change.evidence.is_empty()
        }));
        let promotion = changes
            .iter()
            .find(|change| change.kind == ClusterKvRaftConfigChangeKind::PromoteLearner)
            .expect("promotion lifecycle");
        assert!(promotion.catch_up_proof.is_some());
        assert_eq!(
            promotion.required_log_index,
            Some(host.catch_up_requests[0].1)
        );
    }

    #[test]
    fn failed_catch_up_records_failed_lifecycle_without_promoting() {
        let (mut state, target, now) = non_voting_member_state();
        state.members[1].role = ClusterMemberRole::Learner;
        state.members[1].state = ClusterMemberState::Learner;
        let temp = sinomo_testdir::TestDir::new("cluster-config-lifecycle-failed");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(temp.path()));
        let mut host = MemoryMutationHost::new(&state);
        host.catch_up_failure = true;

        let error = update_cluster_state_authority_with_host(
            &paths,
            state,
            target.clone(),
            ClusterMemberRole::Voter,
            now,
            &mut host,
        )
        .expect_err("offline learner must not be promoted");
        assert!(error.to_string().contains("learner is offline"));

        let config: ClusterKvRaftConfigRecord = host
            .store
            .get(&sinomo_kv::cluster_kv_raft_config_key())
            .expect("raft config")
            .decode()
            .expect("decode raft config");
        assert!(config.learners.contains(&target));
        assert!(!config.voters.contains(&target));
        let changes = host.config_changes();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].phase, ClusterKvRaftConfigChangePhase::Failed);
        assert!(changes[0].catch_up_proof.is_none());
        assert!(
            changes[0]
                .evidence
                .iter()
                .any(|evidence| evidence.detail.contains("learner is offline"))
        );
    }

    #[test]
    fn restart_reconciliation_resumes_planned_learner_admission_idempotently() {
        let (mut state, target, now) = non_voting_member_state();
        let temp = sinomo_testdir::TestDir::new("cluster-config-recover-planned");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(temp.path()));
        let mut host = MemoryMutationHost::new(&state);
        let change = sinomo_kv::begin_cluster_kv_raft_config_change(
            &host.raft_config(),
            Id::from_legacy_named("recover_planned_learner").expect("change id"),
            ClusterKvRaftConfigChangeKind::AddLearner,
            target.clone(),
            None,
            now,
        )
        .expect("plan learner admission");
        host.put_config_change(&change);

        let report =
            reconcile_cluster_raft_config_changes_with_host(&paths, &mut state, now, &mut host)
                .expect("resume planned learner admission");
        assert_eq!(report.inspected_change_count, 1);
        assert_eq!(report.resumed_change_ids, vec![change.change_id.clone()]);
        assert_eq!(report.committed_change_ids, vec![change.change_id.clone()]);
        assert!(host.raft_config().learners.contains(&target));
        assert_eq!(
            host.config_change(&change.change_id).phase,
            ClusterKvRaftConfigChangePhase::Committed
        );

        let commit_index_after_recovery = host.commit_index;
        let retry =
            reconcile_cluster_raft_config_changes_with_host(&paths, &mut state, now, &mut host)
                .expect("exact recovery retry");
        assert!(retry.resumed_change_ids.is_empty());
        assert!(retry.committed_change_ids.is_empty());
        assert!(retry.failed_change_ids.is_empty());
        assert_eq!(host.commit_index, commit_index_after_recovery);
    }

    #[test]
    fn restart_reconciliation_recovers_catching_up_promotion_through_ready() {
        let (mut state, target, now) = non_voting_member_state();
        state.members[1].role = ClusterMemberRole::Learner;
        state.members[1].state = ClusterMemberState::Learner;
        let temp = sinomo_testdir::TestDir::new("cluster-config-recover-catching-up");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(temp.path()));
        let mut host = MemoryMutationHost::new(&state);
        let required_log_index = host.commit_index;
        let change = sinomo_kv::begin_cluster_kv_raft_config_change(
            &host.raft_config(),
            Id::from_legacy_named("recover_catching_up").expect("change id"),
            ClusterKvRaftConfigChangeKind::PromoteLearner,
            target.clone(),
            Some(required_log_index),
            now,
        )
        .expect("plan learner promotion");
        assert_eq!(change.phase, ClusterKvRaftConfigChangePhase::CatchingUp);
        host.put_config_change(&change);

        let report =
            reconcile_cluster_raft_config_changes_with_host(&paths, &mut state, now, &mut host)
                .expect("recover learner promotion");
        assert_eq!(report.resumed_change_ids, vec![change.change_id.clone()]);
        assert_eq!(
            host.catch_up_requests,
            vec![(target.clone(), required_log_index)]
        );
        assert!(host.raft_config().voters.contains(&target));
        let recovered = host.config_change(&change.change_id);
        assert_eq!(recovered.phase, ClusterKvRaftConfigChangePhase::Committed);
        assert!(recovered.catch_up_proof.is_some());
        assert!(!recovered.evidence.is_empty());
    }

    #[test]
    fn restart_reconciliation_finishes_ready_change_before_or_after_apply() {
        for apply_before_restart in [false, true] {
            let (mut state, target, now) = non_voting_member_state();
            state.members[1].role = ClusterMemberRole::Learner;
            state.members[1].state = ClusterMemberState::Learner;
            let temp = sinomo_testdir::TestDir::new(if apply_before_restart {
                "cluster-config-recover-ready-applied"
            } else {
                "cluster-config-recover-ready-unapplied"
            });
            let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(temp.path()));
            let mut host = MemoryMutationHost::new(&state);
            let mut change = sinomo_kv::begin_cluster_kv_raft_config_change(
                &host.raft_config(),
                Id::from_legacy_named(format!("recover_ready_{apply_before_restart}"))
                    .expect("change id"),
                ClusterKvRaftConfigChangeKind::PromoteLearner,
                target.clone(),
                Some(host.commit_index),
                now,
            )
            .expect("plan learner promotion");
            sinomo_kv::record_cluster_kv_raft_catch_up_proof(
                &mut change,
                ClusterKvRaftCatchUpProof {
                    node_id: target.clone(),
                    term: 1,
                    required_log_index: host.commit_index,
                    observed_match_index: host.commit_index,
                    observed_at: now,
                },
            )
            .expect("record catch-up proof");
            host.put_config_change(&change);
            if apply_before_restart {
                let txn = cluster_raft_config_change_apply_txn(
                    &host.store,
                    &target,
                    &change,
                    1,
                    host.store.revision().next(),
                    now,
                )
                .expect("apply txn");
                assert!(host.store.txn(txn).expect("apply before crash").succeeded);
                assert_eq!(
                    host.config_change(&change.change_id).phase,
                    ClusterKvRaftConfigChangePhase::ReadyToCommit
                );
            }

            let report =
                reconcile_cluster_raft_config_changes_with_host(&paths, &mut state, now, &mut host)
                    .expect("finish ready learner promotion");
            assert_eq!(report.committed_change_ids, vec![change.change_id.clone()]);
            assert_eq!(
                report.resumed_change_ids,
                if apply_before_restart {
                    Vec::new()
                } else {
                    vec![change.change_id.clone()]
                }
            );
            assert!(host.raft_config().voters.contains(&target));
            assert_eq!(
                host.config_change(&change.change_id).phase,
                ClusterKvRaftConfigChangePhase::Committed
            );
        }
    }

    #[test]
    fn restart_reconciliation_terminalizes_stale_record_with_failure_evidence() {
        let (mut state, target, now) = non_voting_member_state();
        let temp = sinomo_testdir::TestDir::new("cluster-config-recover-stale");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(temp.path()));
        let mut host = MemoryMutationHost::new(&state);
        let change = sinomo_kv::begin_cluster_kv_raft_config_change(
            &host.raft_config(),
            Id::from_legacy_named("recover_stale_change").expect("change id"),
            ClusterKvRaftConfigChangeKind::AddLearner,
            target,
            None,
            now,
        )
        .expect("plan learner admission");
        host.put_config_change(&change);
        let mut advanced = host.raft_config();
        advanced.kv_revision = advanced.kv_revision.saturating_add(17);
        let response = host
            .store
            .txn(KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::put_serde(sinomo_kv::cluster_kv_raft_config_key(), &advanced, None)
                        .expect("config op"),
                ],
                failure: Vec::new(),
            })
            .expect("advance config revision");
        assert!(response.succeeded);

        let report =
            reconcile_cluster_raft_config_changes_with_host(&paths, &mut state, now, &mut host)
                .expect("terminalize stale config change");
        assert_eq!(report.failed_change_ids, vec![change.change_id.clone()]);
        let failed = host.config_change(&change.change_id);
        assert_eq!(failed.phase, ClusterKvRaftConfigChangePhase::Failed);
        assert!(
            failed
                .evidence
                .iter()
                .any(|evidence| evidence.detail.contains("stale Raft config change"))
        );
    }

    #[test]
    fn member_removal_recovers_pending_config_lifecycle_before_new_transition() {
        let (state, target, now) = non_voting_member_state();
        let temp = sinomo_testdir::TestDir::new("cluster-remove-recovers-config-lifecycle");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(temp.path()));
        let mut host = MemoryMutationHost::new(&state);
        let pending = sinomo_kv::begin_cluster_kv_raft_config_change(
            &host.raft_config(),
            Id::from_legacy_named("remove_recovers_pending_learner").expect("change id"),
            ClusterKvRaftConfigChangeKind::AddLearner,
            target.clone(),
            None,
            now,
        )
        .expect("plan pending learner admission");
        host.put_config_change(&pending);

        let removal =
            remove_cluster_members_with_host(&paths, state, vec![target.clone()], &mut host)
                .expect("recover lifecycle then remove member");

        assert_eq!(
            removal
                .removed
                .iter()
                .map(|member| member.node_id.clone())
                .collect::<Vec<_>>(),
            vec![target.clone()]
        );
        assert_eq!(
            host.config_change(&pending.change_id).phase,
            ClusterKvRaftConfigChangePhase::Committed,
            "the older learner admission must be terminal before removal starts"
        );
        assert!(
            sinomo_kv::cluster_kv_raft_config_member_role(&host.raft_config(), &target).is_none(),
            "the removal lifecycle must own the final config transition"
        );
        assert!(host.store.get(&cluster_member_key(&target)).is_none());
    }

    #[test]
    fn cluster_state_sync_rejects_a_quorum_committed_compare_failure() {
        let (mut state, _target, _now) = non_voting_member_state();
        let temp = sinomo_testdir::TestDir::new("cluster-state-sync-compare-failure");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(temp.path()));
        let mut host = MemoryMutationHost::new(&state);
        state.local_node_display_name = "changed after barrier".to_string();
        host.force_compare_failure = true;

        let error = sync_cluster_state_to_kv_with_host(&paths, &state, &mut host)
            .expect_err("failed config CAS must not report a successful state sync");
        assert!(error.to_string().contains("compare failed"), "{error}");
    }

    #[test]
    fn generic_cluster_state_sync_cannot_bypass_config_change_lifecycle() {
        let (mut state, _target, _now) = non_voting_member_state();
        let host = MemoryMutationHost::new(&state);
        state.members[1].role = ClusterMemberRole::Learner;
        state.members[1].state = ClusterMemberState::Learner;

        let error = cluster_state_sync_txn_from_store(&host.store, &state, true)
            .expect_err("established config mutation must require lifecycle");
        assert!(
            error
                .to_string()
                .contains("explicit config-change lifecycle"),
            "{error}"
        );
    }

    #[test]
    fn unchanged_member_attributes_do_not_commit_empty_transactions() {
        let (state, target, now) = non_voting_member_state();
        let temp = sinomo_testdir::TestDir::new("cluster-member-unchanged-read-only");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(temp.path()));
        paths.ensure().expect("cluster paths");
        write_cluster_state(&paths, &state).expect("local cluster state");
        let target_member = state
            .members
            .iter()
            .find(|member| member.node_id == target)
            .expect("target member");
        let role = target_member.node_role.clone();
        let name = target_member.display_name.clone();
        let mut host = MemoryMutationHost::new(&state);
        let commit_index = host.commit_index;

        let role_update = update_cluster_node_role_with_host(
            &paths,
            state.clone(),
            target.clone(),
            role,
            now,
            &mut host,
        )
        .expect("unchanged role");
        assert!(!role_update.changed);
        assert_eq!(host.commit_index, commit_index);

        let name_update =
            update_cluster_node_name_with_host(&paths, state, target, name, now, &mut host)
                .expect("unchanged name");
        assert!(!name_update.changed);
        assert_eq!(host.commit_index, commit_index);
    }

    #[test]
    fn member_mutations_do_not_report_or_apply_compare_failures() {
        let (state, target, now) = non_voting_member_state();
        let temp = sinomo_testdir::TestDir::new("cluster-member-mutation-compare-failure");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(temp.path()));
        paths.ensure().expect("cluster paths");
        write_cluster_state(&paths, &state).expect("local cluster state");

        let mut name_host = MemoryMutationHost::new(&state);
        name_host.force_compare_failure = true;
        let error = update_cluster_node_name_with_host(
            &paths,
            state.clone(),
            target.clone(),
            "renamed".to_string(),
            now,
            &mut name_host,
        )
        .expect_err("name CAS failure");
        assert!(error.to_string().contains("compare failed"), "{error}");
        let local = read_cluster_state_without_kv(&paths).expect("local state after name failure");
        assert_ne!(
            local
                .members
                .iter()
                .find(|member| member.node_id == target)
                .expect("target")
                .display_name,
            "renamed"
        );

        let mut role_host = MemoryMutationHost::new(&state);
        role_host.force_compare_failure = true;
        let error = update_cluster_node_role_with_host(
            &paths,
            state.clone(),
            target.clone(),
            ClusterNodeRole::Mobile,
            now,
            &mut role_host,
        )
        .expect_err("role CAS failure");
        assert!(error.to_string().contains("compare failed"), "{error}");
        let local = read_cluster_state_without_kv(&paths).expect("local state after role failure");
        assert_ne!(
            local
                .members
                .iter()
                .find(|member| member.node_id == target)
                .expect("target")
                .node_role,
            ClusterNodeRole::Mobile
        );

        let mut remove_host = MemoryMutationHost::new(&state);
        remove_host.force_compare_failure = true;
        let error =
            remove_cluster_members_with_host(&paths, state, vec![target.clone()], &mut remove_host)
                .expect_err("remove CAS failure");
        assert!(error.to_string().contains("compare failed"), "{error}");
        assert!(
            remove_host
                .store
                .get(&cluster_member_key(&target))
                .is_some(),
            "failed removal must retain the member"
        );
    }
}
