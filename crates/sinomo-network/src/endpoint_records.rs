use crate::ClusterPaths;
use crate::error::NetworkError;
use crate::prelude::*;
use sinomo_node::cluster_endpoint_stable_eq;

pub fn cleanup_cluster_iroh_endpoints(state: &mut ClusterBootstrapState) -> bool {
    state.members.iter_mut().fold(false, |changed, member| {
        cleanup_member_iroh_endpoints(member) || changed
    })
}

fn local_daemon_iroh_node_ids(state: &ClusterBootstrapState) -> [NodeId; 2] {
    [
        state.local_node_id.clone(),
        shadow_daemon_node_id(&state.local_node_id),
    ]
}

fn apply_local_iroh_endpoint_record_to_state(
    state: &mut ClusterBootstrapState,
    record: crate::IrohEndpointRecord,
) -> bool {
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    let mut endpoint = record.endpoint();
    if endpoint.node_id == shadow_node_id {
        endpoint.node_id = state.local_node_id.clone();
    }
    let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == endpoint.node_id)
    else {
        return false;
    };
    let mut changed = upsert_member_endpoint(member, endpoint);
    changed |= cleanup_member_iroh_endpoints(member);
    changed
}

pub(crate) fn refresh_local_iroh_endpoints_from_disk(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    timestamp: OffsetDateTime,
) -> Result<bool, NetworkError> {
    let mut changed = false;
    for node_id in local_daemon_iroh_node_ids(state) {
        let Ok(record) = crate::read_iroh_endpoint_record(paths, &node_id) else {
            continue;
        };
        changed |= apply_local_iroh_endpoint_record_to_state(state, record);
    }
    if changed {
        state.updated_at = timestamp;
    }
    Ok(changed)
}

pub(crate) async fn refresh_local_iroh_endpoints_from_disk_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    timestamp: OffsetDateTime,
) -> Result<bool, NetworkError> {
    let mut changed = false;
    for node_id in local_daemon_iroh_node_ids(state) {
        let Ok(record) = crate::read_iroh_endpoint_record_async(runtime, paths, &node_id).await
        else {
            continue;
        };
        changed |= apply_local_iroh_endpoint_record_to_state(state, record);
    }
    if changed {
        state.updated_at = timestamp;
    }
    Ok(changed)
}

pub fn apply_cluster_iroh_endpoint_advertisement(
    state: &mut ClusterBootstrapState,
    record: crate::IrohEndpointRecord,
    persisted_endpoints: Option<&[ClusterEndpointRecord]>,
    timestamp: OffsetDateTime,
) -> Option<ClusterMemberRecord> {
    let endpoint = record.endpoint();
    let member = state
        .members
        .iter_mut()
        .find(|member| member.node_id == endpoint.node_id)?;
    upsert_member_endpoint(member, endpoint);
    cleanup_member_iroh_endpoints(member);
    if persisted_endpoints == Some(member.endpoints.as_slice()) {
        return None;
    }
    state.updated_at = timestamp;
    Some(member.clone())
}

pub(crate) fn cleanup_member_iroh_endpoints(member: &mut ClusterMemberRecord) -> bool {
    let original = member.endpoints.clone();
    let mut endpoints: Vec<ClusterEndpointRecord> = Vec::new();
    let mut iroh_stable_flags: Vec<bool> = Vec::new();
    let mut iroh_by_public_key: BTreeMap<String, usize> = BTreeMap::new();

    for mut endpoint in original.iter().cloned() {
        let Some(public_key) = iroh_endpoint_public_key(&endpoint) else {
            endpoints.push(endpoint);
            iroh_stable_flags.push(false);
            continue;
        };
        let was_stable =
            endpoint.endpoint_id == crate::stable_iroh_endpoint_id(&endpoint.node_id, &public_key);
        endpoint.endpoint_id = crate::stable_iroh_endpoint_id(&endpoint.node_id, &public_key);
        if let Some(index) = iroh_by_public_key.get(&public_key).copied() {
            if iroh_endpoint_should_replace(
                &endpoints[index],
                iroh_stable_flags[index],
                &endpoint,
                was_stable,
            ) {
                endpoints[index] = endpoint;
                iroh_stable_flags[index] = was_stable;
            }
        } else {
            iroh_by_public_key.insert(public_key, endpoints.len());
            endpoints.push(endpoint);
            iroh_stable_flags.push(was_stable);
        }
    }

    if member.endpoints == endpoints {
        false
    } else {
        member.endpoints = endpoints;
        true
    }
}

fn iroh_endpoint_should_replace(
    current: &ClusterEndpointRecord,
    current_was_stable: bool,
    candidate: &ClusterEndpointRecord,
    candidate_was_stable: bool,
) -> bool {
    let current_rank = iroh_endpoint_preference_rank(current, current_was_stable);
    let candidate_rank = iroh_endpoint_preference_rank(candidate, candidate_was_stable);
    candidate_rank > current_rank
}

fn iroh_endpoint_preference_rank(endpoint: &ClusterEndpointRecord, was_stable: bool) -> (u8, u8) {
    let stable_rank = u8::from(was_stable);
    let reachability_rank = match endpoint.privacy {
        ClusterEndpointPrivacy::InternetDirect => 3,
        ClusterEndpointPrivacy::Lan => 2,
        ClusterEndpointPrivacy::Relayed => 1,
        ClusterEndpointPrivacy::LocalOnly => 0,
    };
    (stable_rank, reachability_rank)
}

pub(crate) fn upsert_member_endpoint(
    member: &mut ClusterMemberRecord,
    mut endpoint: ClusterEndpointRecord,
) -> bool {
    if let Some(public_key) = iroh_endpoint_public_key(&endpoint) {
        endpoint.endpoint_id = crate::stable_iroh_endpoint_id(&endpoint.node_id, &public_key);
    }
    let incoming_iroh_public_key = iroh_endpoint_public_key(&endpoint);
    let mut changed = false;
    if let Some(public_key) = incoming_iroh_public_key.as_deref() {
        let endpoint_id = endpoint.endpoint_id.clone();
        let before = member.endpoints.len();
        member.endpoints.retain(|candidate| {
            candidate.endpoint_id == endpoint_id
                || iroh_endpoint_public_key(candidate).as_deref() != Some(public_key)
        });
        changed |= member.endpoints.len() != before;
    }

    if let Some(existing) = member
        .endpoints
        .iter_mut()
        .find(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
    {
        if *existing == endpoint {
            changed
        } else if cluster_endpoint_stable_eq(existing, &endpoint) {
            changed
        } else {
            *existing = endpoint;
            true
        }
    } else if member.endpoints.iter().any(|candidate| {
        candidate.transport == endpoint.transport && candidate.address == endpoint.address
    }) {
        changed
    } else {
        member.endpoints.push(endpoint);
        true
    }
}

pub(crate) fn iroh_endpoint_public_key(endpoint: &ClusterEndpointRecord) -> Option<String> {
    if endpoint.transport != ClusterTransportKind::Iroh {
        return None;
    }
    let rest = endpoint.address.strip_prefix("iroh://")?;
    let public_key = rest.split(['?', '#']).next()?.trim();
    if public_key.is_empty() {
        None
    } else {
        Some(public_key.to_string())
    }
}
