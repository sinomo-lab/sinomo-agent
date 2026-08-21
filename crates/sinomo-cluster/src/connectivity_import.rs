//! Validation and selection policy for importing peer connectivity views.
//!
//! Callers own transport and persistence. This module owns which records are
//! admissible, how accepted-join snapshots are grouped, and which reachable
//! peer endpoint is preferred for a follow-up connectivity fetch.

use sinomo_base::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource,
    ClusterTransportKind,
};
use sinomo_network::{
    ClusterConnectivityProbeState, ClusterConnectivityRecord, ClusterConnectivityState,
    MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS, NetworkError,
    cluster_endpoint_can_carry_main_daemon_rpc, cluster_network_record_live,
    validate_cluster_connectivity_records_for_source,
};
use sinomo_node::{DAEMON_MAIN_RUN_CAPABILITY, NodeId};
use std::collections::{BTreeMap, BTreeSet};
use time::OffsetDateTime;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerConnectivityFetchScope {
    EndpointSource,
    FullSnapshot,
}

pub fn newly_healthy_connectivity_target_node_ids(
    previous: &BTreeMap<String, ClusterConnectivityProbeState>,
    probed: &[(String, ClusterConnectivityRecord)],
) -> Vec<NodeId> {
    let previously_healthy = previous
        .values()
        .filter(|state| state.record.state == ClusterConnectivityState::Healthy)
        .map(|state| state.record.target_node_id.clone())
        .collect::<BTreeSet<_>>();
    probed
        .iter()
        .filter(|(_, record)| {
            record.state == ClusterConnectivityState::Healthy
                && !previously_healthy.contains(&record.target_node_id)
        })
        .map(|(_, record)| record.target_node_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Preserves live source-owned observations for endpoints that were outside a
/// bounded probe batch, while replacing every endpoint included in the batch.
pub fn merge_connectivity_probe_records_with_live_unprobed(
    source_node_id: &NodeId,
    mut probe_records: Vec<ClusterConnectivityRecord>,
    existing_records: Vec<ClusterConnectivityRecord>,
    now: OffsetDateTime,
) -> Vec<ClusterConnectivityRecord> {
    let mut probed_keys = probe_records
        .iter()
        .map(connectivity_record_target_endpoint_key)
        .collect::<BTreeSet<_>>();
    for record in existing_records {
        if record.source_node_id != *source_node_id
            || !cluster_network_record_live(record.expires_at, now)
        {
            continue;
        }
        if probed_keys.insert(connectivity_record_target_endpoint_key(&record)) {
            probe_records.push(record);
        }
    }
    probe_records
}

fn connectivity_record_target_endpoint_key(record: &ClusterConnectivityRecord) -> (String, String) {
    (
        record.target_node_id.to_string(),
        record.endpoint_id.to_string(),
    )
}

pub fn accepted_join_connectivity_record_groups(
    state: &ClusterBootstrapState,
    records: Vec<ClusterConnectivityRecord>,
    now: OffsetDateTime,
) -> Result<BTreeMap<NodeId, Vec<ClusterConnectivityRecord>>, NetworkError> {
    if records.len() > MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS {
        return Err(NetworkError::ClusterConnectivitySnapshotRecordLimit {
            observed_records: records.len(),
            max_records: MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
        });
    }
    let authorized_main_nodes = state
        .members
        .iter()
        .filter(|member| {
            member.state.is_authorized()
                && member
                    .capabilities
                    .iter()
                    .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
        })
        .map(|member| member.node_id.clone())
        .collect::<BTreeSet<_>>();
    let mut newest_by_key = BTreeMap::new();
    for record in records {
        if record.cluster_id != state.cluster_id
            || record.source_node_id == state.local_node_id
            || !cluster_network_record_live(record.expires_at, now)
            || !authorized_main_nodes.contains(&record.source_node_id)
            || !authorized_main_nodes.contains(&record.target_node_id)
        {
            continue;
        }
        let key = (
            record.source_node_id.clone(),
            record.target_node_id.clone(),
            record.endpoint_id.clone(),
        );
        if newest_by_key
            .get(&key)
            .is_some_and(|existing: &ClusterConnectivityRecord| {
                existing.observed_at > record.observed_at
            })
        {
            continue;
        }
        newest_by_key.insert(key, record);
    }

    let mut groups = BTreeMap::<NodeId, Vec<ClusterConnectivityRecord>>::new();
    for record in newest_by_key.into_values() {
        groups
            .entry(record.source_node_id.clone())
            .or_default()
            .push(record);
    }
    for (source_node_id, records) in &groups {
        validate_cluster_connectivity_records_for_source(source_node_id, records)?;
    }
    Ok(groups)
}

pub fn peer_connectivity_import_records_for_reachable_peers(
    state: &ClusterBootstrapState,
    records: &[ClusterConnectivityRecord],
    now: OffsetDateTime,
) -> Vec<ClusterConnectivityRecord> {
    let mut best_by_peer = BTreeMap::<NodeId, ClusterConnectivityRecord>::new();
    for record in records {
        if record.expires_at <= now
            || peer_connectivity_import_endpoint_for_record(state, record).is_none()
        {
            continue;
        }
        match best_by_peer.entry(record.target_node_id.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(record.clone());
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if peer_connectivity_import_record_rank(record)
                    < peer_connectivity_import_record_rank(entry.get())
                {
                    entry.insert(record.clone());
                }
            }
        }
    }
    best_by_peer.into_values().collect()
}

fn peer_connectivity_import_record_rank(
    record: &ClusterConnectivityRecord,
) -> (u8, u16, u64, u8, &str, &str) {
    (
        match record.state {
            ClusterConnectivityState::Healthy => 0,
            ClusterConnectivityState::Degraded => 1,
            ClusterConnectivityState::Unreachable => 2,
        },
        record.consecutive_failures,
        record.latency_ms.unwrap_or(u64::MAX),
        match record.transport {
            ClusterTransportKind::Loopback => 0,
            ClusterTransportKind::LanTcp => 1,
            ClusterTransportKind::Iroh => 2,
            ClusterTransportKind::LanQuic => 3,
            ClusterTransportKind::WebRtc => 4,
            ClusterTransportKind::TurnRelay => 5,
            ClusterTransportKind::HttpsRelay => 6,
            ClusterTransportKind::InProcess => 7,
        },
        record.endpoint_id.as_str(),
        record.address.as_str(),
    )
}

pub fn filter_peer_connectivity_response_records(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    records: Vec<ClusterConnectivityRecord>,
    scope: PeerConnectivityFetchScope,
) -> Result<Vec<ClusterConnectivityRecord>, String> {
    if records.len() > MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS {
        return Err(NetworkError::ClusterConnectivitySnapshotRecordLimit {
            observed_records: records.len(),
            max_records: MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
        }
        .to_string());
    }
    Ok(records
        .into_iter()
        .filter(|record| {
            record.cluster_id == state.cluster_id
                && (scope == PeerConnectivityFetchScope::FullSnapshot
                    || record.source_node_id == endpoint.node_id)
        })
        .collect())
}

pub fn peer_connectivity_import_endpoint_for_record(
    state: &ClusterBootstrapState,
    record: &ClusterConnectivityRecord,
) -> Option<ClusterEndpointRecord> {
    if record.cluster_id != state.cluster_id
        || record.source_node_id != state.local_node_id
        || record.target_node_id == state.local_node_id
        || record.state == ClusterConnectivityState::Unreachable
    {
        return None;
    }
    let member = state
        .members
        .iter()
        .find(|member| member.node_id == record.target_node_id)?;
    if !member.state.is_authorized()
        || !member
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
    {
        return None;
    }
    if !matches!(
        record.transport,
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback | ClusterTransportKind::Iroh
    ) {
        return None;
    }
    let endpoint = ClusterEndpointRecord {
        endpoint_id: record.endpoint_id.clone(),
        node_id: record.target_node_id.clone(),
        transport: record.transport.clone(),
        address: record.address.clone(),
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 0,
        last_observed_latency_ms: record
            .latency_ms
            .map(|latency_ms| latency_ms.min(u64::from(u32::MAX)) as u32),
    };
    cluster_endpoint_can_carry_main_daemon_rpc(&endpoint).then_some(endpoint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::{ClusterMemberRecord, ClusterMemberState};

    fn state() -> ClusterBootstrapState {
        let cluster_id = sinomo_base::Id::generate();
        let local_node_id = NodeId::from_legacy_named("local-connectivity-import").expect("node");
        let peer_node_id = NodeId::from_legacy_named("peer-connectivity-import").expect("node");
        let now = OffsetDateTime::now_utc();
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity-import",
            local_node_id,
            "local",
            "local-key",
            now,
        );
        let mut peer = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            peer_node_id,
            "peer",
            "peer-key",
            now,
        );
        peer.state = ClusterMemberState::Active;
        peer.capabilities.push(
            sinomo_base::CapabilityName::new(DAEMON_MAIN_RUN_CAPABILITY).expect("capability"),
        );
        state.members.push(peer);
        state
    }

    #[test]
    fn import_endpoint_requires_authorized_main_peer_and_supported_transport() {
        let state = state();
        let peer = state.members.last().expect("peer member").node_id.clone();
        let advertised =
            sinomo_network::daemon_lan_endpoint(&peer, "10.0.0.2:21353".to_string(), 0);
        let record = ClusterConnectivityRecord {
            cluster_id: state.cluster_id.clone(),
            source_node_id: state.local_node_id.clone(),
            target_node_id: peer.clone(),
            endpoint_id: advertised.endpoint_id,
            transport: ClusterTransportKind::LanTcp,
            address: advertised.address,
            state: ClusterConnectivityState::Healthy,
            failure_reason: None,
            latency_ms: Some(5),
            consecutive_failures: 0,
            last_success_at: None,
            last_failure_at: None,
            observed_at: OffsetDateTime::now_utc(),
            expires_at: OffsetDateTime::now_utc() + time::Duration::minutes(1),
        };
        let endpoint = peer_connectivity_import_endpoint_for_record(&state, &record)
            .expect("eligible endpoint");
        assert_eq!(endpoint.node_id, peer);
        assert_eq!(endpoint.address, record.address);
    }
}
