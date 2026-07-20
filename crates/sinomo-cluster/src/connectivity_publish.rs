use crate::keyspace::{
    cluster_connectivity_key, cluster_connectivity_source_prefix, cluster_endpoint_prefix,
};
use crate::kv_local_store::ClusterKvPathsExt;
#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
#[cfg(any(test, feature = "test-support"))]
use crate::kv_local_store::read_cluster_kv_current_snapshot_arc;
use crate::kv_local_store::{
    append_cluster_raft_ops_in_batches, read_cluster_kv_current_or_empty_snapshot_arc,
    read_cluster_kv_snapshot,
};
use crate::local_state_store::read_cluster_state_without_kv;
use crate::prelude::*;
use sinomo_kv::{
    CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES, ClusterKvTxnCommitResult, KvCompare, KvOp, KvSnapshot,
    KvTxn, KvTxnRequest,
};
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterNetworkInfoRecord;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::cluster_network_record_should_refresh;
use sinomo_network::read_local_cluster_connectivity_records_for_source;
use sinomo_network::write_local_cluster_connectivity_record_set;
use sinomo_network::{CLUSTER_NETWORK_PUBLISH_INTERVAL, cluster_network_publish_interval_for_role};
use sinomo_network::{
    ClusterConnectivityRecord, MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS,
    cluster_connectivity_stable_eq, validate_cluster_connectivity_records_for_source,
};
use sinomo_node::cluster_node_is_local_process;
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;

// The network-owned record cap must match the KV prefix-read cap or a full
// source record set could not be synced/read back through cluster KV.
const _: () =
    assert!(MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS == CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES);
const CLUSTER_NETWORK_EXPIRY_GRACE_SECONDS: i64 = 600;

pub trait ClusterNetworkRecordExpiryHost {
    type Error: From<NetworkError>;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<ClusterKvTxnCommitResult, Self::Error>;
}

pub trait ClusterConnectivityPublishHost {
    type Error: From<NetworkError>;

    fn published_connectivity_record_set_matches(
        &mut self,
        _paths: &ClusterPaths,
        _source_node_id: &NodeId,
        _records: &[ClusterConnectivityRecord],
    ) -> bool {
        false
    }

    fn remember_published_connectivity_record_set(
        &mut self,
        _paths: &ClusterPaths,
        _source_node_id: &NodeId,
        _records: &[ClusterConnectivityRecord],
    ) {
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<KvSnapshot, Self::Error>;
}

pub trait ClusterConnectivityPublishAsyncHost {
    type Error: From<NetworkError>;

    fn published_connectivity_record_set_matches_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _source_node_id: &'a NodeId,
        _records: &'a [ClusterConnectivityRecord],
    ) -> ClusterMembershipRpcFuture<'a, bool> {
        Box::pin(async { Ok(false) })
    }

    fn remember_published_connectivity_record_set_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _source_node_id: &'a NodeId,
        _records: &'a [ClusterConnectivityRecord],
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;
}

pub fn cluster_network_publish_interval_for_node(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Duration {
    read_cluster_state_without_kv(paths)
        .ok()
        .and_then(|state| {
            state
                .members
                .iter()
                .find(|member| member.node_id == *node_id)
                .map(|member| cluster_network_publish_interval_for_role(&member.node_role))
        })
        .unwrap_or(CLUSTER_NETWORK_PUBLISH_INTERVAL)
}

pub fn sync_cluster_connectivity_records_to_kv(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<ClusterConnectivityRecord>,
) -> Result<KvSnapshot, NetworkError> {
    write_local_cluster_connectivity_record_set(paths, source_node_id, &records)?;
    // A newer observation may already have won a concurrent local merge. Use
    // that authoritative merged set instead of publishing the caller's stale
    // snapshot and making cluster KV oscillate backward.
    let now = OffsetDateTime::now_utc();
    let mut snapshot =
        read_cluster_kv_current_or_empty_snapshot_arc_for_source(paths, source_node_id)?;
    let mut local_node_id = None;
    let mut appended = false;
    loop {
        // A probe can complete while an earlier publication batch is being
        // appended. Re-read on every pass so the operation cannot finish with
        // cluster KV reflecting the older set.
        let records = read_local_cluster_connectivity_records_for_source(paths, source_node_id)?;
        let success = cluster_connectivity_full_sync_batch_ops_from_snapshot(
            snapshot.as_ref(),
            source_node_id,
            &records,
            now,
            sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND,
        )?;
        if success.is_empty() {
            return if appended {
                Ok(snapshot.as_ref().clone())
            } else {
                read_cluster_kv_snapshot(paths)
            };
        }
        if local_node_id.is_none() {
            local_node_id = Some(read_cluster_state_without_kv(paths)?.local_node_id);
        }
        snapshot = Arc::new(append_cluster_raft_ops_in_batches(
            paths,
            local_node_id
                .as_ref()
                .expect("local node id is initialized before append"),
            success,
        )?);
        appended = true;
    }
}

fn read_cluster_kv_current_or_empty_snapshot_arc_for_source(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
) -> Result<Arc<KvSnapshot>, NetworkError> {
    if !paths.kv_paths().raft.exists() {
        return read_cluster_kv_current_or_empty_snapshot_arc(paths);
    }
    sinomo_kv::read_cluster_kv_current_snapshot_arc_for_node(
        &paths.kv_paths(),
        source_node_id.clone(),
    )
    .map_err(kv_err)
}

pub fn sync_cluster_connectivity_records_to_quorum_with_host<H>(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<ClusterConnectivityRecord>,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterConnectivityPublishHost,
{
    write_local_cluster_connectivity_record_set(paths, source_node_id, &records)?;
    // Full-set writers reject observations older than the locally retained
    // record, but callers can race while holding snapshots captured before
    // that rejection. Always plan the quorum diff from the post-merge set.
    let mut state = None;
    let mut latest = None;
    let mut previous_ops: Option<Vec<KvOp>> = None;
    loop {
        // Re-read after every quorum round. If a newer local observation landed
        // while the previous transaction was in flight, immediately plan a
        // corrective transaction instead of returning on the stale response.
        let records = read_local_cluster_connectivity_records_for_source(paths, source_node_id)?;
        let local_snapshot = match latest.as_ref() {
            Some(_) => None,
            None => Some(read_cluster_kv_current_or_empty_snapshot_arc_for_source(
                paths,
                source_node_id,
            )?),
        };
        let snapshot = latest
            .as_ref()
            .or_else(|| local_snapshot.as_deref())
            .expect("connectivity sync always has a snapshot");
        let success = cluster_connectivity_full_sync_batch_ops_from_snapshot(
            snapshot,
            source_node_id,
            &records,
            OffsetDateTime::now_utc(),
            sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND,
        )?;
        if success.is_empty() {
            if let Some(published) =
                cluster_connectivity_records_for_source_from_snapshot(snapshot, source_node_id)
            {
                host.remember_published_connectivity_record_set(paths, source_node_id, &published);
            }
            // The local materialized snapshot already contains the complete
            // source-owned record set. An unchanged publication is terminal;
            // it must not manufacture an empty Raft transaction merely to
            // refresh a cache derived from that same snapshot.
            return Ok(latest);
        }
        if previous_ops.as_ref() == Some(&success) {
            return Ok(latest);
        }
        previous_ops = Some(success.clone());
        if state.is_none() {
            state = Some(read_cluster_state_without_kv(paths)?);
        }
        latest = Some(
            host.commit_cluster_kv_txn_with_quorum(
                paths,
                state
                    .as_mut()
                    .expect("cluster state is initialized before connectivity publish"),
                KvTxnRequest::generated(KvTxn {
                    compares: Vec::new(),
                    success,
                    failure: Vec::new(),
                }),
            )?,
        );
    }
}

pub async fn sync_cluster_connectivity_records_to_quorum_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<ClusterConnectivityRecord>,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterConnectivityPublishAsyncHost,
{
    sync_cluster_connectivity_records_to_quorum_async_inner(
        runtime,
        paths,
        source_node_id,
        records,
        None,
        host,
    )
    .await
}

pub async fn sync_cluster_connectivity_records_to_quorum_async_with_state_and_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<ClusterConnectivityRecord>,
    state: ClusterBootstrapState,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterConnectivityPublishAsyncHost,
{
    sync_cluster_connectivity_records_to_quorum_async_inner(
        runtime,
        paths,
        source_node_id,
        records,
        Some(state),
        host,
    )
    .await
}

async fn sync_cluster_connectivity_records_to_quorum_async_inner<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<ClusterConnectivityRecord>,
    initial_state: Option<ClusterBootstrapState>,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterConnectivityPublishAsyncHost,
{
    let write_paths = paths.clone();
    let write_source_node_id = source_node_id.clone();
    let write_records = records.clone();
    runtime
        .spawn_blocking(move || {
            write_local_cluster_connectivity_record_set(
                &write_paths,
                &write_source_node_id,
                &write_records,
            )
        })
        .await
        .map_err(|source| {
            H::Error::from(NetworkError::Io {
                operation: "merge local cluster connectivity record set".to_string(),
                detail: source.to_string(),
            })
        })?
        .map_err(H::Error::from)?;

    let mut state = initial_state;
    let mut latest: Option<KvSnapshot> = None;
    let mut previous_ops: Option<Vec<KvOp>> = None;
    loop {
        let read_paths = paths.clone();
        let read_source_node_id = source_node_id.clone();
        let records = runtime
            .spawn_blocking(move || {
                read_local_cluster_connectivity_records_for_source(
                    &read_paths,
                    &read_source_node_id,
                )
            })
            .await
            .map_err(|source| {
                H::Error::from(NetworkError::Io {
                    operation: "refresh local cluster connectivity record set".to_string(),
                    detail: source.to_string(),
                })
            })?
            .map_err(H::Error::from)?;
        let (success, published_on_noop) = match latest.as_ref() {
            // After a forwarded commit the local store may not reflect the txn
            // yet (a learner depends on the voter's fan-out or a snapshot
            // import to observe it), so diff against the authoritative
            // post-commit snapshot: the loop must terminate on what the quorum
            // contains, not on local replay catching up.
            Some(snapshot) => {
                let success = cluster_connectivity_full_sync_batch_ops_from_snapshot(
                    snapshot,
                    source_node_id,
                    &records,
                    OffsetDateTime::now_utc(),
                    sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND,
                )
                .map_err(H::Error::from)?;
                let published = success.is_empty().then(|| {
                    cluster_connectivity_records_for_source_from_snapshot(snapshot, source_node_id)
                });
                (success, published.flatten())
            }
            None => {
                let batch_paths = paths.clone();
                let batch_source_node_id = source_node_id.clone();
                let batch_records = records.clone();
                runtime
                    .spawn_blocking(move || {
                        let snapshot = read_cluster_kv_current_or_empty_snapshot_arc_for_source(
                            &batch_paths,
                            &batch_source_node_id,
                        )?;
                        let success = cluster_connectivity_full_sync_batch_ops_from_snapshot(
                            snapshot.as_ref(),
                            &batch_source_node_id,
                            &batch_records,
                            OffsetDateTime::now_utc(),
                            sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND,
                        )?;
                        let published = success.is_empty().then(|| {
                            cluster_connectivity_records_for_source_from_snapshot(
                                snapshot.as_ref(),
                                &batch_source_node_id,
                            )
                        });
                        Ok::<_, NetworkError>((success, published.flatten()))
                    })
                    .await
                    .map_err(|source| {
                        H::Error::from(NetworkError::Io {
                            operation: "build cluster connectivity sync batch".to_string(),
                            detail: source.to_string(),
                        })
                    })?
                    .map_err(H::Error::from)?
            }
        };
        if success.is_empty() {
            if let Some(published) = published_on_noop {
                host.remember_published_connectivity_record_set_async(
                    paths,
                    source_node_id,
                    &published,
                )
                .await?;
            }
            // Source-change publication is already converged. Keep the
            // operation read-only instead of appending an empty transaction.
            return Ok(latest);
        }
        if previous_ops.as_ref() == Some(&success) {
            return Ok(latest);
        }
        previous_ops = Some(success.clone());
        if state.is_none() {
            let state_paths = paths.clone();
            state = Some(
                runtime
                    .spawn_blocking(move || read_cluster_state_without_kv(&state_paths))
                    .await
                    .map_err(|source| {
                        H::Error::from(NetworkError::Io {
                            operation: "read cluster state".to_string(),
                            detail: format!("cluster state read task failed: {source}"),
                        })
                    })?
                    .map_err(H::Error::from)?,
            );
        }
        latest = Some(
            host.commit_cluster_kv_txn_with_quorum_async(
                paths,
                state
                    .as_mut()
                    .expect("cluster state is initialized before connectivity publish"),
                KvTxnRequest::generated(KvTxn {
                    compares: Vec::new(),
                    success,
                    failure: Vec::new(),
                }),
            )
            .await?,
        );
    }
}

#[allow(dead_code)]
pub fn cluster_connectivity_full_sync_batch_ops(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
    max_ops: usize,
) -> Result<Vec<KvOp>, NetworkError> {
    validate_cluster_connectivity_records_for_source(source_node_id, records)?;
    let now = OffsetDateTime::now_utc();
    let snapshot = read_cluster_kv_current_or_empty_snapshot_arc_for_source(paths, source_node_id)?;
    cluster_connectivity_full_sync_batch_ops_from_snapshot(
        snapshot.as_ref(),
        source_node_id,
        records,
        now,
        max_ops,
    )
}

pub(crate) fn cluster_connectivity_full_sync_batch_ops_from_snapshot(
    snapshot: &KvSnapshot,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
    now: OffsetDateTime,
    max_ops: usize,
) -> Result<Vec<KvOp>, NetworkError> {
    let max_ops = max_ops
        .min(sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH)
        .min(sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND);
    if max_ops == 0 {
        return Ok(Vec::new());
    }

    let mut incoming = BTreeMap::new();
    for record in records {
        let key = cluster_connectivity_key(
            &record.source_node_id,
            &record.target_node_id,
            &record.endpoint_id,
        );
        incoming.insert(key, record);
    }

    let mut ops = Vec::new();
    let prefix = cluster_connectivity_source_prefix(source_node_id);
    for entry in snapshot.prefix_entries(&prefix) {
        let stable = incoming.get(&entry.key).is_some_and(|record| {
            entry
                .decode::<ClusterConnectivityRecord>()
                .is_ok_and(|existing| {
                    existing == **record
                        || (cluster_connectivity_stable_eq(&existing, record)
                            && !cluster_network_record_should_refresh(existing.expires_at, now))
                })
        });
        if stable {
            incoming.remove(&entry.key);
            continue;
        }
        if incoming.contains_key(&entry.key) {
            continue;
        }
        ops.push(KvOp::Delete {
            key: entry.key.clone(),
        });
        if ops.len() >= max_ops {
            return Ok(ops);
        }
    }

    for (key, record) in incoming {
        ops.push(KvOp::put_serde(key, record, None).map_err(kv_err)?);
        if ops.len() >= max_ops {
            break;
        }
    }
    Ok(ops)
}

fn cluster_connectivity_records_for_source_from_snapshot(
    snapshot: &KvSnapshot,
    source_node_id: &NodeId,
) -> Option<Vec<ClusterConnectivityRecord>> {
    snapshot
        .prefix_entries(&cluster_connectivity_source_prefix(source_node_id))
        .map(|entry| {
            entry
                .decode::<ClusterConnectivityRecord>()
                .ok()
                .filter(|record| record.source_node_id == *source_node_id)
        })
        .collect()
}

/// Returns true when publishing `records` for `source_node_id` would not change
/// cluster KV: no records need adding/refreshing and none need deleting. Lets the
/// caller skip the raft append (and its disk churn) when connectivity is stable.
#[cfg(any(test, feature = "test-support"))]
pub fn cluster_connectivity_sync_is_noop(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> Result<bool, NetworkError> {
    validate_cluster_connectivity_records_for_source(source_node_id, records)?;
    let now = OffsetDateTime::now_utc();
    let incoming_keys: BTreeSet<String> = records
        .iter()
        .map(|record| {
            cluster_connectivity_key(
                &record.source_node_id,
                &record.target_node_id,
                &record.endpoint_id,
            )
        })
        .collect();
    // A stored record no longer present in `records` would need deletion.
    let snapshot = read_cluster_kv_current_or_empty_snapshot_arc(paths)?;
    let prefix = cluster_connectivity_source_prefix(source_node_id);
    for entry in snapshot.prefix_entries(&prefix) {
        if !incoming_keys.contains(&entry.key) {
            return Ok(false);
        }
    }
    // Any incoming record that differs from what is stored (or is due for a TTL
    // refresh) would need a put.
    Ok(cluster_connectivity_full_sync_batch_ops_from_snapshot(
        snapshot.as_ref(),
        source_node_id,
        records,
        now,
        sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND,
    )?
    .is_empty())
}

pub fn expire_cluster_network_record_ops(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    now: OffsetDateTime,
) -> Result<(Vec<KvCompare>, Vec<KvOp>), NetworkError> {
    let mut compares = Vec::new();
    let mut ops = Vec::new();
    let max_records = sinomo_kv::MAX_KV_TXN_COMPARES.min(sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH);
    let snapshot = read_cluster_kv_current_or_empty_snapshot_arc(paths)?;
    for entry in snapshot.prefix_entries("/cluster/network/info/") {
        let Ok(record) = entry.decode::<ClusterNetworkInfoRecord>() else {
            continue;
        };
        if cluster_node_is_local_process(state, &record.node_id) {
            continue;
        }
        if cluster_network_record_expired(record.expires_at, now) {
            compares.push(KvCompare::ValueEquals {
                key: entry.key.clone(),
                value: entry.value.clone(),
            });
            ops.push(KvOp::Delete {
                key: entry.key.clone(),
            });
            if ops.len() >= max_records {
                return Ok((compares, ops));
            }
        }
    }
    for entry in snapshot.prefix_entries("/cluster/network/connectivity/") {
        let Ok(record) = entry.decode::<ClusterConnectivityRecord>() else {
            continue;
        };
        if record.source_node_id == state.local_node_id {
            continue;
        }
        if cluster_connectivity_record_target_endpoint_missing(snapshot.as_ref(), &record)
            || cluster_network_record_expired(record.expires_at, now)
        {
            compares.push(KvCompare::ValueEquals {
                key: entry.key.clone(),
                value: entry.value.clone(),
            });
            ops.push(KvOp::Delete {
                key: entry.key.clone(),
            });
            if ops.len() >= max_records {
                return Ok((compares, ops));
            }
        }
    }
    Ok((compares, ops))
}

fn cluster_connectivity_record_target_endpoint_missing(
    snapshot: &KvSnapshot,
    record: &ClusterConnectivityRecord,
) -> bool {
    let endpoint_key = format!(
        "{}{}",
        cluster_endpoint_prefix(&record.target_node_id),
        record.endpoint_id
    );
    snapshot.get(&endpoint_key).is_none_or(|entry| {
        entry
            .decode::<ClusterEndpointRecord>()
            .map(|endpoint| {
                endpoint.node_id != record.target_node_id
                    || endpoint.endpoint_id != record.endpoint_id
                    || endpoint.address != record.address
                    || endpoint.transport != record.transport
            })
            .unwrap_or(true)
    })
}

pub fn expire_cluster_network_records_with_host<H>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    now: OffsetDateTime,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterNetworkRecordExpiryHost,
{
    let (compares, success) = expire_cluster_network_record_ops(paths, state, now)?;
    if success.is_empty() {
        return Ok(None);
    }
    let mut local_state = read_cluster_state_without_kv(paths)?;
    let result = host.commit_cluster_kv_txn_with_quorum(
        paths,
        &mut local_state,
        KvTxnRequest::generated(KvTxn {
            compares,
            success,
            failure: Vec::new(),
        }),
    )?;
    if !result
        .txn_response
        .as_ref()
        .is_some_and(|response| response.succeeded)
    {
        return Err(H::Error::from(NetworkError::InvalidClusterCommand {
            detail: "cluster network records changed while expiring stale observations".to_string(),
        }));
    }
    Ok(Some(result.snapshot))
}

fn cluster_network_record_expired(expires_at: OffsetDateTime, now: OffsetDateTime) -> bool {
    expires_at + time::Duration::seconds(CLUSTER_NETWORK_EXPIRY_GRACE_SECONDS) <= now
}

pub fn cluster_connectivity_publish_record_matches(
    known: &ClusterConnectivityRecord,
    record: &ClusterConnectivityRecord,
    now: OffsetDateTime,
) -> bool {
    cluster_connectivity_stable_eq(known, record)
        && !cluster_network_record_should_refresh(known.expires_at, now)
}

#[cfg(any(test, feature = "test-support"))]
pub fn expire_cluster_network_records(
    paths: &ClusterPaths,
    now: OffsetDateTime,
) -> Result<KvSnapshot, NetworkError> {
    let mut snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    let mut success = cluster_network_expiry_delete_ops(
        snapshot.as_ref(),
        now,
        sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND,
    );
    if success.is_empty() {
        return Ok(snapshot.as_ref().clone());
    }

    let state = read_cluster_state_without_kv(paths)?;
    loop {
        snapshot = Arc::new(append_cluster_raft_ops_in_batches(
            paths,
            &state.local_node_id,
            success,
        )?);
        success = cluster_network_expiry_delete_ops(
            snapshot.as_ref(),
            now,
            sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND,
        );
        if success.is_empty() {
            return Ok(snapshot.as_ref().clone());
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_network_expiry_delete_ops(
    snapshot: &KvSnapshot,
    now: OffsetDateTime,
    max_ops: usize,
) -> Vec<KvOp> {
    let mut success = Vec::new();
    for entry in snapshot.prefix_entries("/cluster/network/info/") {
        if success.len() >= max_ops {
            return success;
        }
        let Ok(record) = entry.decode::<ClusterNetworkInfoRecord>() else {
            continue;
        };
        if record.expires_at <= now {
            success.push(KvOp::Delete {
                key: entry.key.clone(),
            });
        }
    }
    for entry in snapshot.prefix_entries("/cluster/network/connectivity/") {
        if success.len() >= max_ops {
            return success;
        }
        let Ok(record) = entry.decode::<ClusterConnectivityRecord>() else {
            continue;
        };
        if record.expires_at <= now {
            success.push(KvOp::Delete {
                key: entry.key.clone(),
            });
        }
    }
    success
}
