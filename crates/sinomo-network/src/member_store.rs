use crate::connectivity::ClusterConnectivityRecord;
use crate::endpoint_records::{
    cleanup_cluster_iroh_endpoints, cleanup_member_iroh_endpoints, upsert_member_endpoint,
};
use crate::error::NetworkError;
use crate::keyspace::{
    cluster_capability_policy_key, cluster_connectivity_source_prefix, cluster_endpoint_key,
    cluster_endpoint_prefix, cluster_identity_key, cluster_member_key, cluster_node_status_key,
    cluster_node_status_key_for_process,
};
use crate::membership::cluster_capabilities_are_legacy_shadow_only;
use crate::object::SinomoObjectCodec;
use crate::records::{
    ClusterCapabilityPolicyRecord, ClusterIdentityRecord, cluster_capability_policy_stable_eq,
    cluster_identity_record_stable_eq,
};
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointRecord, ClusterMemberRecord, ClusterMemberRole,
    ClusterMemberState, ClusterNodeStatusRecord,
};
#[cfg(test)]
use sinomo_kv::KvCommand;
use sinomo_kv::{
    ClusterKvRaftConfigRecord, InProcessKvStore, KvCompare, KvEntry, KvOp, KvTxn,
    push_bounded_txn_success_op,
};
use sinomo_node::{
    DAEMON_SHADOW_CLUSTER_ROLE, NodeId, cluster_endpoint_stable_eq, cluster_member_stable_eq,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use time::OffsetDateTime;

pub(crate) const MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS: usize =
    sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;

pub(crate) fn apply_cluster_raft_role(
    member: &mut ClusterMemberRecord,
    role: ClusterMemberRole,
    timestamp: OffsetDateTime,
) {
    sinomo_node::apply_cluster_raft_role(member, role, timestamp);
}

pub(crate) fn cluster_raft_state_for_role(role: &ClusterMemberRole) -> ClusterMemberState {
    match role {
        ClusterMemberRole::Voter => ClusterMemberState::Voter,
        ClusterMemberRole::Learner => ClusterMemberState::Learner,
        ClusterMemberRole::NonVoting => ClusterMemberState::NonVoting,
    }
}

#[cfg(test)]
fn cluster_member_put_op(member: &ClusterMemberRecord) -> Result<KvOp, NetworkError> {
    KvOp::put_serde(cluster_member_key(&member.node_id), member, None).map_err(NetworkError::from)
}

#[cfg(test)]
pub(crate) fn cluster_member_put_command(
    member: &ClusterMemberRecord,
) -> Result<KvCommand, NetworkError> {
    cluster_member_put_op(member).map(KvCommand::Op)
}

pub(crate) fn merge_cluster_state_from_peer(
    local: &mut ClusterBootstrapState,
    peer: ClusterBootstrapState,
) -> bool {
    if peer.cluster_id != local.cluster_id {
        return false;
    }
    let mut changed = false;
    for member in peer.members {
        changed |= merge_member_record(local, member);
    }
    if peer.kv_revision > local.kv_revision {
        local.kv_revision = peer.kv_revision;
    }
    if peer.updated_at > local.updated_at {
        local.updated_at = peer.updated_at;
    }
    changed
}

pub(crate) fn merge_member_record(
    local: &mut ClusterBootstrapState,
    incoming: ClusterMemberRecord,
) -> bool {
    if incoming.cluster_id != local.cluster_id {
        return false;
    }
    if let Some(existing) = local
        .members
        .iter_mut()
        .find(|member| member.node_id == incoming.node_id)
    {
        let mut changed = false;
        if incoming.updated_at > existing.updated_at {
            *existing = incoming;
            cleanup_member_iroh_endpoints(existing);
            return true;
        }
        for endpoint in incoming.endpoints {
            changed |= upsert_member_endpoint(existing, endpoint);
        }
        changed |= cleanup_member_iroh_endpoints(existing);
        changed
    } else {
        let mut incoming = incoming;
        cleanup_member_iroh_endpoints(&mut incoming);
        local.members.push(incoming);
        true
    }
}

pub(crate) fn cluster_state_sync_txn_from_store(
    store: &InProcessKvStore,
    state: &ClusterBootstrapState,
    validate_config_transition: bool,
) -> Result<KvTxn, NetworkError> {
    let mut state = state.clone();
    cleanup_cluster_iroh_endpoints(&mut state);
    let new_config = ClusterKvRaftConfigRecord::from(&state);
    let current_config_entry = store.get(&sinomo_kv::cluster_kv_raft_config_key()).cloned();
    let mut compares = Vec::new();
    if let Some(current_config_entry) = &current_config_entry {
        let current_config: ClusterKvRaftConfigRecord = current_config_entry.decode()?;
        if validate_config_transition {
            sinomo_kv::validate_cluster_kv_raft_config_transition(&current_config, &new_config)?;
        }
        compares.push(KvCompare::ValueEquals {
            key: sinomo_kv::cluster_kv_raft_config_key(),
            value: current_config_entry.value.clone(),
        });
    } else {
        compares.push(KvCompare::Missing {
            key: sinomo_kv::cluster_kv_raft_config_key(),
        });
    }

    let mut success = Vec::new();

    let new_identity = ClusterIdentityRecord::from(&state);
    let identity_unchanged = store
        .get(&cluster_identity_key())
        .and_then(|entry| entry.decode::<ClusterIdentityRecord>().ok())
        .is_some_and(|stored| cluster_identity_record_stable_eq(&stored, &new_identity));
    if !identity_unchanged {
        success.push(KvOp::put_serde(
            cluster_identity_key(),
            &new_identity,
            None,
        )?);
    }

    let config_unchanged = current_config_entry
        .as_ref()
        .and_then(|entry| entry.decode::<ClusterKvRaftConfigRecord>().ok())
        .is_some_and(|stored| {
            sinomo_kv::cluster_kv_raft_config_record_stable_eq(&stored, &new_config)
        });
    if !config_unchanged {
        success.push(KvOp::put_serde(
            sinomo_kv::cluster_kv_raft_config_key(),
            &new_config,
            None,
        )?);
    }

    push_stale_member_identity_replacement_deletes(store, &state, &mut success)?;

    for member in &state.members {
        let member_op = KvOp::put_serde(cluster_member_key(&member.node_id), member, None)?;
        if cluster_member_put_changes_store(store, &member_op, member) {
            success.push(member_op);
        }
        let policy = ClusterCapabilityPolicyRecord::from(member);
        let policy_op = KvOp::put_serde(
            cluster_capability_policy_key(&member.node_id),
            &policy,
            None,
        )?;
        if cluster_capability_policy_put_changes_store(store, &policy_op, &policy) {
            success.push(policy_op);
        }

        let endpoint_prefix = cluster_endpoint_prefix(&member.node_id);
        let stored_endpoints: BTreeMap<String, (Vec<u8>, Option<ClusterEndpointRecord>)> = store
            .range_prefix_limited(&endpoint_prefix, MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS)?
            .into_iter()
            .map(|entry| {
                let endpoint =
                    sinomo_kv::decode_kv_value::<ClusterEndpointRecord>(&entry.value).ok();
                (entry.key, (entry.value, endpoint))
            })
            .collect();
        let mut desired_endpoints: BTreeMap<String, (Vec<u8>, ClusterEndpointRecord)> =
            BTreeMap::new();
        for endpoint in &member.endpoints {
            let key = cluster_endpoint_key(&member.node_id, endpoint);
            desired_endpoints.insert(
                key,
                (
                    SinomoObjectCodec::default().encode(endpoint)?,
                    endpoint.clone(),
                ),
            );
        }
        if !cluster_endpoint_maps_stable_eq(&stored_endpoints, &desired_endpoints) {
            for key in stored_endpoints
                .keys()
                .filter(|key| !desired_endpoints.contains_key(*key))
            {
                success.push(KvOp::Delete { key: key.clone() });
            }
            for (key, (value, endpoint)) in desired_endpoints {
                if !stored_endpoints
                    .get(&key)
                    .is_some_and(|(stored_value, stored)| {
                        stored_value == &value
                            || stored
                                .as_ref()
                                .is_some_and(|stored| cluster_endpoint_stable_eq(stored, &endpoint))
                    })
                {
                    success.push(KvOp::Put {
                        key,
                        value,
                        lease_id: None,
                    });
                }
            }
        }
    }

    if success.is_empty() {
        compares.clear();
    }
    Ok(KvTxn {
        compares,
        success,
        failure: Vec::new(),
    })
}

fn cluster_endpoint_maps_stable_eq(
    stored: &BTreeMap<String, (Vec<u8>, Option<ClusterEndpointRecord>)>,
    desired: &BTreeMap<String, (Vec<u8>, ClusterEndpointRecord)>,
) -> bool {
    stored.len() == desired.len()
        && desired.iter().all(|(key, (desired_value, desired))| {
            stored.get(key).is_some_and(|(stored_value, stored)| {
                stored_value == desired_value
                    || stored
                        .as_ref()
                        .is_some_and(|stored| cluster_endpoint_stable_eq(stored, desired))
            })
        })
}

pub(crate) fn cluster_member_update_txn_from_store(
    store: &InProcessKvStore,
    current_entry: KvEntry,
    member: &ClusterMemberRecord,
) -> Result<KvTxn, NetworkError> {
    Ok(KvTxn {
        compares: vec![KvCompare::ValueEquals {
            key: current_entry.key,
            value: current_entry.value,
        }],
        success: cluster_member_update_ops_from_store(store, member)?,
        failure: Vec::new(),
    })
}

pub(crate) fn cluster_member_raft_update_txn_from_store(
    store: &InProcessKvStore,
    current_entry: KvEntry,
    member: &ClusterMemberRecord,
    now: OffsetDateTime,
) -> Result<KvTxn, NetworkError> {
    let raft_config_key = sinomo_kv::cluster_kv_raft_config_key();
    let Some(current_config_entry) = store.get(&raft_config_key).cloned() else {
        return Err(NetworkError::ClusterReadOnly {
            detail: "cluster raft config is missing".to_string(),
        });
    };
    let current_config: ClusterKvRaftConfigRecord = current_config_entry.decode()?;
    let next_config = cluster_raft_config_from_store(store, member, &current_config, now)?;
    sinomo_kv::validate_cluster_kv_raft_config_transition(&current_config, &next_config)?;
    let mut success = cluster_member_update_ops_from_store(store, member)?;
    push_bounded_txn_success_op(
        &mut success,
        KvOp::put_serde(raft_config_key.clone(), &next_config, None)?,
    )?;
    Ok(KvTxn {
        compares: vec![
            KvCompare::ValueEquals {
                key: current_entry.key,
                value: current_entry.value,
            },
            KvCompare::ValueEquals {
                key: raft_config_key,
                value: current_config_entry.value,
            },
        ],
        success,
        failure: Vec::new(),
    })
}

pub(crate) fn cluster_member_update_ops_from_store(
    store: &InProcessKvStore,
    member: &ClusterMemberRecord,
) -> Result<Vec<KvOp>, NetworkError> {
    let mut member = member.clone();
    cleanup_member_iroh_endpoints(&mut member);
    let mut ops = Vec::new();
    let member_key = cluster_member_key(&member.node_id);
    if store.get(&member_key).is_none_or(|entry| {
        entry.lease_id.is_some()
            || entry
                .decode::<ClusterMemberRecord>()
                .map(|stored| !cluster_member_stable_eq(&stored, &member))
                .unwrap_or(true)
    }) {
        push_bounded_txn_success_op(&mut ops, KvOp::put_serde(member_key, &member, None)?)?;
    }

    let policy = ClusterCapabilityPolicyRecord::from(&member);
    let policy_key = cluster_capability_policy_key(&member.node_id);
    if store.get(&policy_key).is_none_or(|entry| {
        entry.lease_id.is_some()
            || entry
                .decode::<ClusterCapabilityPolicyRecord>()
                .map(|stored| !cluster_capability_policy_stable_eq(&stored, &policy))
                .unwrap_or(true)
    }) {
        push_bounded_txn_success_op(&mut ops, KvOp::put_serde(policy_key, &policy, None)?)?;
    }
    push_store_cluster_member_endpoint_diff_ops(store, &member, &mut ops)?;
    Ok(ops)
}

fn push_store_cluster_member_endpoint_diff_ops(
    store: &InProcessKvStore,
    member: &ClusterMemberRecord,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let endpoint_prefix = cluster_endpoint_prefix(&member.node_id);
    let mut desired_endpoints = BTreeMap::new();
    for endpoint in &member.endpoints {
        let key = cluster_endpoint_key(&member.node_id, endpoint);
        let op = KvOp::put_serde(key, endpoint, None)?;
        let KvOp::Put { key, value, .. } = op else {
            unreachable!("put_serde must produce a put operation");
        };
        desired_endpoints.insert(key, (value, endpoint.clone()));
    }

    for entry in
        store.range_prefix_limited(&endpoint_prefix, MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS)?
    {
        if !desired_endpoints.contains_key(&entry.key) {
            push_bounded_txn_success_op(ops, KvOp::Delete { key: entry.key })?;
        }
    }

    for (key, (value, endpoint)) in desired_endpoints {
        if store.get(&key).is_none_or(|entry| {
            entry.lease_id.is_some()
                || (entry.value != value
                    && entry
                        .decode::<ClusterEndpointRecord>()
                        .map(|stored| !cluster_endpoint_stable_eq(&stored, &endpoint))
                        .unwrap_or(true))
        }) {
            push_bounded_txn_success_op(
                ops,
                KvOp::Put {
                    key,
                    value,
                    lease_id: None,
                },
            )?;
        }
    }

    Ok(())
}

pub(crate) fn cluster_raft_config_from_store(
    store: &InProcessKvStore,
    updated_member: &ClusterMemberRecord,
    current_config: &ClusterKvRaftConfigRecord,
    now: OffsetDateTime,
) -> Result<ClusterKvRaftConfigRecord, NetworkError> {
    let mut voters = Vec::new();
    let mut learners = Vec::new();
    let mut non_voting = Vec::new();
    for entry in store.range_prefix_limited(
        "/cluster/members/",
        sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    )? {
        let mut member: ClusterMemberRecord = entry.decode()?;
        if member.node_id == updated_member.node_id {
            member = updated_member.clone();
        }
        if !member.state.is_authorized() {
            continue;
        }
        match member.state {
            ClusterMemberState::Voter => voters.push(member.node_id),
            ClusterMemberState::Learner => learners.push(member.node_id),
            ClusterMemberState::NonVoting => non_voting.push(member.node_id),
            ClusterMemberState::Active => match member.role {
                ClusterMemberRole::Voter => voters.push(member.node_id),
                ClusterMemberRole::Learner => learners.push(member.node_id),
                ClusterMemberRole::NonVoting => non_voting.push(member.node_id),
            },
            ClusterMemberState::Pending
            | ClusterMemberState::Suspect
            | ClusterMemberState::Offline
            | ClusterMemberState::Removed => {}
        }
    }
    voters.sort();
    learners.sort();
    non_voting.sort();
    Ok(ClusterKvRaftConfigRecord {
        cluster_id: current_config.cluster_id.clone(),
        term: current_config.term,
        kv_revision: current_config.kv_revision,
        voters,
        learners,
        non_voting,
        updated_at: now,
    })
}

pub(crate) fn cluster_member_removal_txns_from_store(
    store: &InProcessKvStore,
    members: &[ClusterMemberRecord],
) -> Result<Vec<KvTxn>, NetworkError> {
    let mut txns = Vec::new();
    let final_endpoint_prefix_delete_limit =
        sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND.saturating_sub(2);
    let explicit_delete_batch_limit =
        sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH.min(sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND);
    for member in members {
        let mut connectivity_delete_ops = Vec::new();
        for key in cluster_member_connectivity_delete_keys_from_store(store, &member.node_id)? {
            connectivity_delete_ops.push(KvOp::Delete { key });
            if connectivity_delete_ops.len() >= explicit_delete_batch_limit {
                txns.push(KvTxn {
                    compares: Vec::new(),
                    success: std::mem::take(&mut connectivity_delete_ops),
                    failure: Vec::new(),
                });
            }
        }
        if !connectivity_delete_ops.is_empty() {
            txns.push(KvTxn {
                compares: Vec::new(),
                success: connectivity_delete_ops,
                failure: Vec::new(),
            });
        }

        let endpoint_prefix = cluster_endpoint_prefix(&member.node_id);
        let mut residual_endpoint_keys = VecDeque::new();
        let mut explicit_delete_ops = Vec::new();
        for entry in store.prefix_entries(&endpoint_prefix) {
            residual_endpoint_keys.push_back(entry.key.clone());
            if residual_endpoint_keys.len() <= final_endpoint_prefix_delete_limit {
                continue;
            }
            if let Some(key) = residual_endpoint_keys.pop_front() {
                explicit_delete_ops.push(KvOp::Delete { key });
            }
            if explicit_delete_ops.len() >= explicit_delete_batch_limit {
                txns.push(KvTxn {
                    compares: Vec::new(),
                    success: std::mem::take(&mut explicit_delete_ops),
                    failure: Vec::new(),
                });
            }
        }
        if !explicit_delete_ops.is_empty() {
            txns.push(KvTxn {
                compares: Vec::new(),
                success: explicit_delete_ops,
                failure: Vec::new(),
            });
        }

        txns.push(KvTxn {
            compares: Vec::new(),
            success: vec![
                KvOp::Delete {
                    key: cluster_member_key(&member.node_id),
                },
                KvOp::Delete {
                    key: cluster_capability_policy_key(&member.node_id),
                },
                KvOp::DeletePrefix {
                    prefix: endpoint_prefix,
                },
            ],
            failure: Vec::new(),
        });
    }
    Ok(txns)
}

fn cluster_member_connectivity_delete_keys_from_store(
    store: &InProcessKvStore,
    node_id: &NodeId,
) -> Result<Vec<String>, NetworkError> {
    let mut keys = BTreeSet::new();
    for entry in store.prefix_entries(&cluster_connectivity_source_prefix(node_id)) {
        keys.insert(entry.key.clone());
    }
    for entry in store.prefix_entries("/cluster/network/connectivity/") {
        if keys.contains(&entry.key) {
            continue;
        }
        let record: ClusterConnectivityRecord = entry.decode()?;
        if record.source_node_id == *node_id || record.target_node_id == *node_id {
            keys.insert(entry.key.clone());
        }
    }
    Ok(keys.into_iter().collect())
}

pub(crate) fn cluster_legacy_shadow_orphan_artifact_node_ids_from_store(
    store: &InProcessKvStore,
    current_member_ids: &BTreeSet<NodeId>,
) -> Result<BTreeSet<NodeId>, NetworkError> {
    let mut node_ids = BTreeSet::new();
    for entry in store.prefix_entries("/cluster/policy/capabilities/") {
        let policy: ClusterCapabilityPolicyRecord = entry.decode()?;
        if !current_member_ids.contains(&policy.node_id)
            && cluster_capabilities_are_legacy_shadow_only(&policy.capabilities)
        {
            node_ids.insert(policy.node_id);
        }
    }
    for entry in store.prefix_entries("/cluster/node-status/") {
        let status: ClusterNodeStatusRecord = entry.decode()?;
        if !current_member_ids.contains(&status.node_id)
            && status.process == DAEMON_SHADOW_CLUSTER_ROLE
        {
            node_ids.insert(status.node_id);
        }
    }
    for entry in store.prefix_entries("/cluster/network/connectivity/") {
        let record: ClusterConnectivityRecord = entry.decode()?;
        if !current_member_ids.contains(&record.source_node_id) {
            node_ids.insert(record.source_node_id);
        }
        if !current_member_ids.contains(&record.target_node_id) {
            node_ids.insert(record.target_node_id);
        }
    }
    Ok(node_ids)
}

pub(crate) fn cluster_legacy_shadow_orphan_artifact_cleanup_txns_from_store(
    store: &InProcessKvStore,
    node_ids: &BTreeSet<NodeId>,
) -> Result<Vec<KvTxn>, NetworkError> {
    let explicit_delete_batch_limit =
        sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH.min(sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND);
    let mut txns = Vec::new();
    let mut ops = Vec::new();
    for node_id in node_ids {
        for key in cluster_member_connectivity_delete_keys_from_store(store, node_id)? {
            ops.push(KvOp::Delete { key });
            if ops.len() >= explicit_delete_batch_limit {
                txns.push(KvTxn {
                    compares: Vec::new(),
                    success: std::mem::take(&mut ops),
                    failure: Vec::new(),
                });
            }
        }
        ops.push(KvOp::Delete {
            key: cluster_capability_policy_key(node_id),
        });
        ops.push(KvOp::Delete {
            key: cluster_node_status_key(node_id),
        });
        ops.push(KvOp::Delete {
            key: cluster_node_status_key_for_process(node_id, DAEMON_SHADOW_CLUSTER_ROLE),
        });
        ops.push(KvOp::DeletePrefix {
            prefix: cluster_endpoint_prefix(node_id),
        });
        if ops.len() >= explicit_delete_batch_limit {
            txns.push(KvTxn {
                compares: Vec::new(),
                success: std::mem::take(&mut ops),
                failure: Vec::new(),
            });
        }
    }
    if !ops.is_empty() {
        txns.push(KvTxn {
            compares: Vec::new(),
            success: ops,
            failure: Vec::new(),
        });
    }
    Ok(txns)
}

fn cluster_member_put_changes_store(
    store: &InProcessKvStore,
    op: &KvOp,
    member: &ClusterMemberRecord,
) -> bool {
    match op {
        KvOp::Put { key, value, .. } => store
            .get(key)
            .map(|entry| {
                entry.value.as_slice() != value.as_slice()
                    && entry
                        .decode::<ClusterMemberRecord>()
                        .map(|stored| !cluster_member_stable_eq(&stored, member))
                        .unwrap_or(true)
            })
            .unwrap_or(true),
        _ => true,
    }
}

fn push_stale_member_identity_replacement_deletes(
    store: &InProcessKvStore,
    state: &ClusterBootstrapState,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let mut desired_nodes_by_public_key: BTreeMap<String, BTreeSet<NodeId>> = BTreeMap::new();
    for member in &state.members {
        if member.node_public_key.trim().is_empty() {
            continue;
        }
        desired_nodes_by_public_key
            .entry(member.node_public_key.clone())
            .or_default()
            .insert(member.node_id.clone());
    }
    if desired_nodes_by_public_key.is_empty() {
        return Ok(());
    }

    for entry in store.range_prefix_limited(
        "/cluster/members/",
        sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    )? {
        let stored: ClusterMemberRecord = entry.decode()?;
        let Some(desired_nodes) = desired_nodes_by_public_key.get(&stored.node_public_key) else {
            continue;
        };
        if desired_nodes.contains(&stored.node_id) {
            continue;
        }

        push_bounded_txn_success_op(ops, KvOp::Delete { key: entry.key })?;
        push_bounded_txn_success_op(
            ops,
            KvOp::Delete {
                key: cluster_capability_policy_key(&stored.node_id),
            },
        )?;
        for endpoint in store.range_prefix_limited(
            &cluster_endpoint_prefix(&stored.node_id),
            MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS,
        )? {
            push_bounded_txn_success_op(ops, KvOp::Delete { key: endpoint.key })?;
        }
    }
    Ok(())
}

fn cluster_capability_policy_put_changes_store(
    store: &InProcessKvStore,
    op: &KvOp,
    policy: &ClusterCapabilityPolicyRecord,
) -> bool {
    match op {
        KvOp::Put { key, value, .. } => store
            .get(key)
            .map(|entry| {
                entry.value.as_slice() != value.as_slice()
                    && entry
                        .decode::<ClusterCapabilityPolicyRecord>()
                        .map(|stored| !cluster_capability_policy_stable_eq(&stored, policy))
                        .unwrap_or(true)
            })
            .unwrap_or(true),
        _ => true,
    }
}
