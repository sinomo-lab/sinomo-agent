use crate::ClusterPaths;
use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
use crate::endpoint_records::cleanup_cluster_iroh_endpoints;
use crate::error::NetworkError;
use crate::join_registration::ClusterLocalDaemonMembershipObserver;
#[cfg(test)]
use crate::keyspace::cluster_certificate_key;
use crate::keyspace::cluster_member_key;
use crate::kv_local_store::{
    append_cluster_raft_command, open_cluster_raft_kv_node, read_cluster_kv_current_snapshot,
    write_cluster_kv_snapshot,
};
#[cfg(test)]
use crate::kv_local_store::{read_cluster_kv_cached_snapshot_if_present, read_cluster_kv_snapshot};
use crate::kv_read_model::read_cluster_capability_policies_from_snapshot;
use crate::local_membership::apply_cluster_node_role;
use crate::local_membership::refresh_local_daemon_membership_from_disk;
use crate::local_state_store::{read_cluster_state_without_kv, write_cluster_state};
use crate::member_store::{
    apply_cluster_raft_role, cluster_legacy_shadow_orphan_artifact_cleanup_txns_from_store,
    cluster_legacy_shadow_orphan_artifact_node_ids_from_store,
    cluster_member_raft_update_txn_from_store, cluster_member_removal_txns_from_store,
    cluster_member_update_txn_from_store, cluster_raft_state_for_role,
    cluster_state_sync_txn_from_store,
};
use crate::rpc_future::ClusterMembershipRpcFuture;
#[cfg(test)]
use serde::Serialize;
#[cfg(test)]
use sinomo_core::ClusterCertificate;
use sinomo_core::{
    ClusterBootstrapState, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState,
    ClusterNodeRole,
};
#[cfg(test)]
use sinomo_kv::KvOp;
use sinomo_kv::{ClusterKvRaftConfigRecord, InProcessKvStore, KvCommand, KvSnapshot, KvTxn};
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

pub trait ClusterMemberMutationHost {
    fn prepare_cluster_state_for_kv_sync(
        &mut self,
        _paths: &ClusterPaths,
        _state: &mut ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        Ok(())
    }

    fn current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<InProcessKvStore, NetworkError>;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError>;
}

pub trait ClusterMemberMutationAsyncHost {
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
        txn: KvTxn,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;
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

    fn current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<InProcessKvStore, NetworkError> {
        ClusterMemberMutationHost::current_cluster_kv_store(&mut self.driver, paths, state)
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError> {
        ClusterMemberMutationHost::commit_cluster_kv_txn_with_quorum(
            &mut self.driver,
            paths,
            state,
            txn,
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
        Ok(node) => node.store().try_snapshot()?,
        Err(error) => {
            eprintln!("warning: replacing unreadable cluster kv raft state: {error}");
            let snapshot = InProcessKvStore::new().try_snapshot()?;
            write_cluster_kv_snapshot(paths, &snapshot)?;
            snapshot
        }
    };
    let store = InProcessKvStore::restore(initial_snapshot.clone())?;
    let txn = cluster_state_sync_txn_from_store(&store, state, false)?;
    if txn.success.is_empty() {
        return Ok(initial_snapshot);
    }
    append_cluster_raft_command(paths, &state.local_node_id, KvCommand::Txn(txn))
}

#[cfg(test)]
pub(crate) fn sync_cluster_state_to_local_raft_unchecked(
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
    host: &mut impl ClusterMemberMutationHost,
) -> Result<KvSnapshot, NetworkError> {
    let mut keyed_state = state.clone();
    host.prepare_cluster_state_for_kv_sync(paths, &mut keyed_state)?;
    cleanup_cluster_iroh_endpoints(&mut keyed_state);
    let (current_snapshot, txn) =
        cluster_state_sync_txn_with_current_snapshot(paths, &keyed_state)?;
    if txn.success.is_empty() {
        return Ok(current_snapshot);
    }
    let mut commit_state = keyed_state;
    host.commit_cluster_kv_txn_with_quorum(paths, &mut commit_state, txn)
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
    cleanup_cluster_iroh_endpoints(&mut keyed_state);
    let paths_for_txn = paths.clone();
    let keyed_state_for_txn = keyed_state.clone();
    let (current_snapshot, txn) = runtime
        .spawn_blocking(move || {
            cluster_state_sync_txn_with_current_snapshot(&paths_for_txn, &keyed_state_for_txn)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster state sync txn".to_string(),
            detail: format!("cluster state sync txn task failed: {source}"),
        })??;
    if txn.success.is_empty() {
        return Ok(current_snapshot);
    }
    let mut commit_state = keyed_state;
    host.commit_cluster_kv_txn_with_quorum_async(paths, &mut commit_state, txn)
        .await
}

pub fn cluster_state_sync_txn_with_current_snapshot(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(KvSnapshot, KvTxn), NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot(paths)?;
    let store = InProcessKvStore::restore(snapshot.clone())?;
    cluster_state_sync_txn_from_store(&store, state, true).map(|txn| (snapshot, txn))
}

pub fn update_cluster_node_role_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_id: NodeId,
    role: ClusterNodeRole,
    now: OffsetDateTime,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterNodeRoleUpdate, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let member_key = cluster_member_key(&node_id);
    let Some(member_entry) = store.get(&member_key).cloned() else {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!("cluster member `{node_id}` was not found"),
        });
    };
    let mut member: ClusterMemberRecord = member_entry.decode()?;
    let previous_role = member.node_role.clone();
    let changed = previous_role != role;
    if changed {
        apply_cluster_node_role(&mut member, role.clone(), now);
        host.commit_cluster_kv_txn_with_quorum(
            paths,
            &mut state,
            cluster_member_update_txn_from_store(&store, member_entry, &member)?,
        )?;
    }
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
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterNodeNameUpdate, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let member_key = cluster_member_key(&node_id);
    let Some(member_entry) = store.get(&member_key).cloned() else {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!("cluster member `{node_id}` was not found"),
        });
    };
    let mut member: ClusterMemberRecord = member_entry.decode()?;
    let previous_name = member.display_name.clone();
    let changed = previous_name != name;
    if changed {
        apply_cluster_node_name(&mut member, name.clone(), now);
        host.commit_cluster_kv_txn_with_quorum(
            paths,
            &mut state,
            cluster_member_update_txn_from_store(&store, member_entry, &member)?,
        )?;
    }
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
    write_cluster_state(paths, &state)?;
    Ok(())
}

fn update_local_cluster_node_name_if_present(
    paths: &ClusterPaths,
    node_id: &NodeId,
    name: String,
    now: OffsetDateTime,
) -> Result<(), NetworkError> {
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
            write_cluster_state(paths, &state)?;
        }
        return Ok(());
    };
    if member.display_name != name {
        apply_cluster_node_name(member, name, now);
        changed = true;
    }
    if changed {
        state.updated_at = now;
        write_cluster_state(paths, &state)?;
    }
    Ok(())
}

fn apply_cluster_node_name(member: &mut ClusterMemberRecord, name: String, now: OffsetDateTime) {
    member.display_name = name;
    member.updated_at = now;
}

pub fn update_cluster_state_authority_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_id: NodeId,
    role: ClusterMemberRole,
    now: OffsetDateTime,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterStateAuthorityUpdate, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let member_key = cluster_member_key(&node_id);
    let Some(member_entry) = store.get(&member_key).cloned() else {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!("cluster member `{node_id}` was not found"),
        });
    };
    let new_state = cluster_raft_state_for_role(&role);
    let mut member: ClusterMemberRecord = member_entry.decode()?;
    let previous_role = member.role.clone();
    let previous_state = member.state.clone();
    let config_role = store
        .get(&sinomo_kv::cluster_kv_raft_config_key())
        .and_then(|entry| entry.decode::<ClusterKvRaftConfigRecord>().ok())
        .and_then(|config| sinomo_kv::cluster_kv_raft_config_member_role(&config, &node_id));
    let changed =
        previous_role != role || previous_state != new_state || config_role.as_ref() != Some(&role);
    if changed {
        apply_cluster_raft_role(&mut member, role.clone(), now);
        host.commit_cluster_kv_txn_with_quorum(
            paths,
            &mut state,
            cluster_member_raft_update_txn_from_store(&store, member_entry, &member, now)?,
        )?;
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
    let store = host.current_cluster_kv_store(paths, &state)?;
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

    let mut removed = Vec::new();
    for node_id in &requested {
        let member_key = cluster_member_key(node_id);
        let Some(member_entry) = store.get(&member_key).cloned() else {
            return Err(NetworkError::InvalidClusterCommand {
                detail: format!("cluster member `{node_id}` was not found"),
            });
        };
        let member: ClusterMemberRecord = member_entry.decode()?;
        if sinomo_kv::cluster_kv_store_member_is_raft_voter(&store, &member.node_id)? {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "cluster member `{}` is a state authority; change its authority role before removal",
                    member.node_id
                ),
            });
        }
        removed.push(member);
    }

    for txn in cluster_member_removal_txns_from_store(&store, &removed)? {
        host.commit_cluster_kv_txn_with_quorum(paths, &mut state, txn)?;
    }
    Ok(ClusterMemberRemoval { removed })
}

pub fn cleanup_legacy_shadow_daemon_members_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterMemberRemoval, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let snapshot = store.try_snapshot()?;
    let policies = read_cluster_capability_policies_from_snapshot(&snapshot)?;
    let mut current_member_ids = BTreeSet::new();
    let mut removed = Vec::new();
    for entry in store.prefix_entries("/cluster/members/") {
        let mut member: ClusterMemberRecord = entry.decode()?;
        current_member_ids.insert(member.node_id.clone());
        if let Some(policy) = policies.get(&member.node_id) {
            member.capabilities = policy.capabilities.clone();
            member.allowed_capabilities = policy.allowed_capabilities.clone();
        }
        if !cluster_member_is_legacy_shadow_daemon(&member) {
            continue;
        }
        if sinomo_kv::cluster_kv_store_member_is_raft_voter(&store, &member.node_id)? {
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
        host.commit_cluster_kv_txn_with_quorum(paths, &mut state, txn)?;
    }
    for txn in cluster_legacy_shadow_orphan_artifact_cleanup_txns_from_store(
        &store,
        &orphan_artifact_node_ids,
    )? {
        host.commit_cluster_kv_txn_with_quorum(paths, &mut state, txn)?;
    }
    Ok(ClusterMemberRemoval { removed })
}

#[cfg(test)]
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

#[cfg(test)]
fn put_cluster_kv_value<T: Serialize + ?Sized>(
    paths: &ClusterPaths,
    node_id: &NodeId,
    key: String,
    value: &T,
) -> Result<KvSnapshot, NetworkError> {
    let op = KvOp::put_serde(key, value, None)?;
    if cluster_kv_put_value_is_noop(paths, &op)? {
        return read_cluster_kv_snapshot(paths);
    }
    append_cluster_raft_command(paths, node_id, KvCommand::Op(op))
}

#[cfg(test)]
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
