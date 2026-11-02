//! Validation policy for the accepted-join connectivity handoff and for
//! merging probe results.
//!
//! Callers own transport and persistence. This module owns which records are
//! admissible and how accepted-join snapshots are grouped. After joining, a
//! node learns other sources' edges from committed cluster KV.

use sinomo_base::ClusterBootstrapState;
use sinomo_network::{
    ClusterConnectivityProbeState, ClusterConnectivityRecord, ClusterConnectivityState,
    MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS, NetworkError, cluster_network_record_live,
    validate_cluster_connectivity_records_for_source,
};
use sinomo_node::{DAEMON_MAIN_RUN_CAPABILITY, NodeId};
use std::collections::{BTreeMap, BTreeSet};
use time::OffsetDateTime;

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

pub fn filter_peer_connectivity_response_records(
    state: &ClusterBootstrapState,
    records: Vec<ClusterConnectivityRecord>,
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
        .filter(|record| record.cluster_id == state.cluster_id)
        .collect())
}
