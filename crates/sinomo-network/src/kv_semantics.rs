use crate::ClusterPaths;
use crate::connectivity::ClusterConnectivityRecord;
use crate::connectivity_publish::{
    cluster_connectivity_stable_eq, cluster_network_record_should_refresh,
};
use crate::endpoint_records::cluster_endpoint_stable_eq;
use crate::error::NetworkError;
use crate::keyspace::{
    cluster_capability_policy_key, cluster_connectivity_key, cluster_endpoint_key,
    cluster_identity_key, cluster_network_info_key, cluster_node_status_key_matches_record,
};
use crate::network_info::{ClusterNetworkInfoRecord, cluster_network_info_stable_eq};
use crate::prelude::*;
use crate::records::{
    ClusterCapabilityPolicyRecord, ClusterIdentityRecord, cluster_capability_policy_stable_eq,
    cluster_identity_record_stable_eq,
};
use crate::status_publish::cluster_node_status_stable_eq;
use sinomo_kv::ClusterKvRaftConfigRecord;
use time::OffsetDateTime;

pub(crate) fn cluster_semantic_txn_no_effect_response(
    paths: &ClusterPaths,
    txn: &KvTxn,
    now: OffsetDateTime,
) -> Result<Option<(KvSnapshot, KvTxnResponse)>, NetworkError> {
    let snapshot = crate::storage::read_cluster_kv_current_snapshot(paths)?;
    let succeeded = txn
        .compares
        .iter()
        .all(|compare| sinomo_kv::cluster_kv_snapshot_compare(&snapshot, compare));
    let ops = if succeeded {
        txn.success.as_slice()
    } else {
        txn.failure.as_slice()
    };
    let mut responses = Vec::with_capacity(ops.len());
    for op in ops {
        let Some(response) = cluster_semantic_op_no_effect_response(&snapshot, op, now)? else {
            return Ok(None);
        };
        responses.push(response);
    }

    Ok(Some((
        snapshot.clone(),
        KvTxnResponse {
            succeeded,
            revision: snapshot.revision,
            responses,
        },
    )))
}

fn cluster_semantic_op_no_effect_response(
    snapshot: &KvSnapshot,
    op: &KvOp,
    now: OffsetDateTime,
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    match op {
        KvOp::Put {
            key,
            value,
            lease_id,
        } if lease_id.is_none() => {
            if let Some(entry) = snapshot.get(key)
                && entry.value.as_slice() == value.as_slice()
                && entry.lease_id.is_none()
            {
                return Ok(Some(sinomo_kv::KvOpResponse::Put {
                    key: key.clone(),
                    revision: entry.modify_revision,
                }));
            }
            if let Some(response) = cluster_member_put_no_effect_response(snapshot, key, value)? {
                return Ok(Some(response));
            }
            if let Some(response) =
                cluster_capability_policy_put_no_effect_response(snapshot, key, value)?
            {
                return Ok(Some(response));
            }
            if let Some(response) = cluster_endpoint_put_no_effect_response(snapshot, key, value)? {
                return Ok(Some(response));
            }
            if let Some(response) = cluster_identity_put_no_effect_response(snapshot, key, value)? {
                return Ok(Some(response));
            }
            if let Some(response) =
                cluster_raft_config_put_no_effect_response(snapshot, key, value)?
            {
                return Ok(Some(response));
            }
            if let Some(response) =
                cluster_connectivity_put_no_effect_response(snapshot, key, value, now)?
            {
                return Ok(Some(response));
            }
            if let Some(response) =
                cluster_network_info_put_no_effect_response(snapshot, key, value, now)?
            {
                return Ok(Some(response));
            }
            cluster_node_status_put_no_effect_response(snapshot, key, value)
        }
        KvOp::Delete { key } => {
            Ok(snapshot
                .get(key)
                .is_none()
                .then(|| sinomo_kv::KvOpResponse::Delete {
                    key: key.clone(),
                    deleted: false,
                }))
        }
        KvOp::DeletePrefix { prefix } => Ok(snapshot
            .range_prefix_limited(prefix, 1)?
            .is_empty()
            .then(|| sinomo_kv::KvOpResponse::DeletePrefix {
                prefix: prefix.clone(),
                deleted_count: 0,
            })),
        _ => Ok(None),
    }
}

fn cluster_member_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if !key.starts_with("/cluster/members/") {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |member: &ClusterMemberRecord| key == crate::keyspace::cluster_member_key(&member.node_id),
        crate::storage::cluster_member_stable_eq,
    ))
}

fn cluster_capability_policy_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if !key.starts_with("/cluster/policy/capabilities/") {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |policy: &ClusterCapabilityPolicyRecord| {
            key == cluster_capability_policy_key(&policy.node_id)
        },
        cluster_capability_policy_stable_eq,
    ))
}

fn cluster_endpoint_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if !key.starts_with("/cluster/endpoints/") {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |endpoint: &ClusterEndpointRecord| key == cluster_endpoint_key(&endpoint.node_id, endpoint),
        cluster_endpoint_stable_eq,
    ))
}

fn cluster_identity_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if key != cluster_identity_key() {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |_identity: &ClusterIdentityRecord| true,
        cluster_identity_record_stable_eq,
    ))
}

fn cluster_raft_config_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if key != sinomo_kv::cluster_kv_raft_config_key() {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |_config: &ClusterKvRaftConfigRecord| true,
        sinomo_kv::cluster_kv_raft_config_record_stable_eq,
    ))
}

fn cluster_connectivity_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
    now: OffsetDateTime,
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if !key.starts_with("/cluster/network/connectivity/") {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |record: &ClusterConnectivityRecord| {
            key == cluster_connectivity_key(
                &record.source_node_id,
                &record.target_node_id,
                &record.endpoint_id,
            )
        },
        |existing, record| {
            cluster_connectivity_stable_eq(existing, record)
                && !cluster_network_record_should_refresh(existing.expires_at, now)
        },
    ))
}

fn cluster_network_info_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
    now: OffsetDateTime,
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if !key.starts_with("/cluster/network/info/") {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |info: &ClusterNetworkInfoRecord| key == cluster_network_info_key(&info.node_id),
        |existing, info| {
            cluster_network_info_stable_eq(existing, info)
                && !cluster_network_record_should_refresh(existing.expires_at, now)
        },
    ))
}

fn cluster_node_status_put_no_effect_response(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
) -> Result<Option<sinomo_kv::KvOpResponse>, NetworkError> {
    if !key.starts_with("/cluster/node-status/") {
        return Ok(None);
    }
    Ok(cluster_semantic_stable_put_no_effect_response(
        snapshot,
        key,
        value,
        |status: &ClusterNodeStatusRecord| cluster_node_status_key_matches_record(key, status),
        cluster_node_status_stable_eq,
    ))
}

fn cluster_semantic_stable_put_no_effect_response<T, K, S>(
    snapshot: &KvSnapshot,
    key: &str,
    value: &[u8],
    key_matches_record: K,
    stable_eq: S,
) -> Option<sinomo_kv::KvOpResponse>
where
    T: serde::de::DeserializeOwned,
    K: FnOnce(&T) -> bool,
    S: FnOnce(&T, &T) -> bool,
{
    let Ok(record) = sinomo_kv::decode_kv_value::<T>(value) else {
        return None;
    };
    if !key_matches_record(&record) {
        return None;
    }
    let entry = snapshot.get(key)?;
    let Ok(existing) = entry.decode::<T>() else {
        return None;
    };
    stable_eq(&existing, &record).then(|| sinomo_kv::KvOpResponse::Put {
        key: key.to_string(),
        revision: entry.modify_revision,
    })
}
