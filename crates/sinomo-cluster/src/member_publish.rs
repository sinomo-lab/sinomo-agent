use crate::keyspace::{
    cluster_capability_policy_key, cluster_endpoint_key, cluster_endpoint_prefix,
    cluster_member_key,
};
use crate::kv_local_store::kv_err;
use crate::kv_local_store::{
    imported_snapshot_advances_local_kv, local_cluster_kv_snapshot_revision,
    read_cluster_kv_current_or_cached_snapshot_arc_if_present, read_cluster_kv_current_snapshot,
    read_cluster_kv_current_snapshot_arc, write_imported_cluster_kv_snapshot_if_changed,
};
use crate::local_state_store::read_cluster_state;
use crate::member_mutation::ClusterMemberMutationAsyncHost;
use crate::member_store::MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS;
use crate::membership_rpc::{ClusterMemberPublishRequest, ClusterMemberPublishResponse};
use crate::prelude::*;
#[allow(unused_imports)]
use sinomo_kv::{KvOp, KvSnapshot, KvTxn};
use sinomo_kv::{KvRevision, push_bounded_txn_success_op};
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::cleanup_member_iroh_endpoints;
use sinomo_network::{ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector};
use sinomo_node::{
    ClusterCapabilityPolicyRecord, cluster_capability_policy_stable_eq,
    cluster_endpoint_list_stable_eq, cluster_endpoint_stable_eq, cluster_local_process_members,
    cluster_member_stable_eq,
};

pub trait ClusterMemberPublishHost {
    type Error: From<NetworkError> + ToString;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error>;
}

pub struct ClusterLocalMemberPublishOptions {
    pub member_publish_timeout: Duration,
    /// Publish even when the local KV snapshot already reflects the local
    /// member records. Learners set this on their periodic publish so the
    /// voter can hand back a newer KV snapshot (see
    /// `ClusterMemberPublishRequest::local_kv_revision`); without it a learner
    /// whose own records are stable never publishes again and its local KV
    /// snapshot never converges.
    pub force_kv_snapshot_sync: bool,
}

pub trait ClusterLocalMemberPublishHost {
    fn cluster_has_local_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> bool;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError>;

    fn send_cluster_member_publish(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterMemberPublishRequest,
        timeout: Duration,
    ) -> Result<ClusterMemberPublishResponse, NetworkError>;
}

pub trait ClusterLocalMemberPublishAsyncHost {
    fn cluster_has_local_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, bool>;

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;

    fn send_cluster_member_publish_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterMemberPublishRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterMemberPublishResponse>;
}

pub fn cluster_member_publish_matches_kv(
    paths: &ClusterPaths,
    members: &[ClusterMemberRecord],
) -> Result<bool, NetworkError> {
    let Some(snapshot) = read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)? else {
        return Ok(false);
    };
    for member in members {
        let mut member = cluster_member_publish_record_for_snapshot(Some(&snapshot), member);
        cleanup_member_iroh_endpoints(&mut member);
        member
            .endpoints
            .sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));

        let Some(entry) = snapshot.get(&cluster_member_key(&member.node_id)) else {
            return Ok(false);
        };
        let Ok(mut existing) = entry.decode::<ClusterMemberRecord>() else {
            return Ok(false);
        };
        cleanup_member_iroh_endpoints(&mut existing);
        existing
            .endpoints
            .sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        if !cluster_member_stable_eq(&existing, &member) {
            return Ok(false);
        }

        let endpoint_prefix = cluster_endpoint_prefix(&member.node_id);
        let mut existing_endpoints = Vec::new();
        for entry in snapshot
            .range_prefix_limited(&endpoint_prefix, MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS)
            .map_err(kv_err)?
        {
            let Some(endpoint) = entry.decode::<ClusterEndpointRecord>().ok() else {
                continue;
            };
            sinomo_network::try_reserve_vec(
                &mut existing_endpoints,
                1,
                "collect existing member endpoints",
            )?;
            existing_endpoints.push(endpoint);
        }
        existing_endpoints.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        if !cluster_endpoint_list_stable_eq(&existing_endpoints, &member.endpoints) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn publish_local_members_via_voter_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterLocalMemberPublishOptions,
    host: &mut impl ClusterLocalMemberPublishHost,
) -> Result<(), NetworkError> {
    let members = cluster_local_process_members(state);
    if members.is_empty() {
        return Ok(());
    }
    let request = ClusterMemberPublishRequest {
        members,
        local_kv_revision: local_cluster_kv_snapshot_revision(paths),
    };
    let mut failures = Vec::new();
    let mut accepted_without_newer_snapshot = false;
    for endpoint in member_publish_targets_voter_first(paths, state) {
        match host.send_cluster_member_publish(&endpoint, &request, options.member_publish_timeout)
        {
            Ok(response) if response.accepted => {
                let mut imported_newer_snapshot = false;
                if let Some(snapshot) = response.kv_snapshot {
                    if imported_snapshot_advances_local_kv(paths, &snapshot) {
                        write_imported_cluster_kv_snapshot_if_changed(paths, &snapshot)?;
                        imported_newer_snapshot = true;
                    }
                }
                if !options.force_kv_snapshot_sync || imported_newer_snapshot {
                    return Ok(());
                }
                accepted_without_newer_snapshot = true;
            }
            Ok(response) => failures.push(format!(
                "{} ({:?}) rejected member publish: {}",
                endpoint.address, endpoint.transport, response.detail
            )),
            Err(error) => failures.push(format!(
                "{} ({:?}) failed: {}",
                endpoint.address, endpoint.transport, error
            )),
        }
    }
    if options.force_kv_snapshot_sync && accepted_without_newer_snapshot {
        return Ok(());
    }
    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "local node cannot commit cluster state and no reachable cluster state authority or bridge accepted member publication{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

pub(crate) async fn publish_local_members_via_voter_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterLocalMemberPublishOptions,
    host: &mut impl ClusterLocalMemberPublishAsyncHost,
) -> Result<(), NetworkError> {
    let members = cluster_local_process_members(state);
    if members.is_empty() {
        return Ok(());
    }
    let paths_for_targets = paths.clone();
    let state_for_targets = state.clone();
    let targets = runtime
        .spawn_blocking(move || {
            member_publish_targets_voter_first(&paths_for_targets, &state_for_targets)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster local member publish target selection".to_string(),
            detail: format!("member publish target selection task failed: {source}"),
        })?;
    let paths_for_revision = paths.clone();
    let local_kv_revision = runtime
        .spawn_blocking(move || local_cluster_kv_snapshot_revision(&paths_for_revision))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster local member publish revision read".to_string(),
            detail: format!("member publish revision read task failed: {source}"),
        })?;
    let request = ClusterMemberPublishRequest {
        members,
        local_kv_revision,
    };
    let mut failures = Vec::new();
    let mut accepted_without_newer_snapshot = false;
    for endpoint in targets {
        match host
            .send_cluster_member_publish_async(&endpoint, &request, options.member_publish_timeout)
            .await
        {
            Ok(response) if response.accepted => {
                let mut imported_newer_snapshot = false;
                if let Some(snapshot) = response.kv_snapshot {
                    let paths_for_write = paths.clone();
                    imported_newer_snapshot = runtime
                        .spawn_blocking(move || {
                            if !imported_snapshot_advances_local_kv(&paths_for_write, &snapshot) {
                                return Ok(false);
                            }
                            write_imported_cluster_kv_snapshot_if_changed(
                                &paths_for_write,
                                &snapshot,
                            )
                        })
                        .await
                        .map_err(|source| NetworkError::Io {
                            operation: "cluster local member publish snapshot import".to_string(),
                            detail: format!("member publish snapshot import task failed: {source}"),
                        })??;
                }
                if !options.force_kv_snapshot_sync || imported_newer_snapshot {
                    return Ok(());
                }
                accepted_without_newer_snapshot = true;
            }
            Ok(response) => failures.push(format!(
                "{} ({:?}) rejected member publish: {}",
                endpoint.address, endpoint.transport, response.detail
            )),
            Err(error) => failures.push(format!(
                "{} ({:?}) failed: {}",
                endpoint.address, endpoint.transport, error
            )),
        }
    }
    if options.force_kv_snapshot_sync && accepted_without_newer_snapshot {
        return Ok(());
    }
    Err(NetworkError::ClusterReadOnly {
        detail: format!(
            "local node cannot commit cluster state and no reachable cluster state authority or bridge accepted member publication{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

pub fn publish_local_members_to_quorum_if_needed_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterLocalMemberPublishOptions,
    host: &mut impl ClusterLocalMemberPublishHost,
) -> Result<Option<KvSnapshot>, NetworkError> {
    let members = cluster_local_process_members(state);
    if members.is_empty() {
        return Ok(None);
    }
    let already_published = cluster_member_publish_matches_kv(paths, &members).unwrap_or(false);
    if sinomo_kv::cluster_kv_raft_local_is_voter(state)
        && host.cluster_has_local_quorum(paths, state)
    {
        if already_published {
            return Ok(None);
        }
        let mut commit_state = state.clone();
        return host
            .commit_cluster_kv_txn_with_quorum(
                paths,
                &mut commit_state,
                cluster_member_publish_txn_from_kv(paths, &members)?,
            )
            .map(Some);
    }
    if already_published
        && !options.force_kv_snapshot_sync
        && member_publish_targets_voter_first(paths, state).is_empty()
    {
        return Ok(None);
    }
    publish_local_members_via_voter_with_host(paths, state, options, host)?;
    Ok(None)
}

pub async fn publish_local_members_to_quorum_if_needed_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterLocalMemberPublishOptions,
    host: &mut impl ClusterLocalMemberPublishAsyncHost,
) -> Result<Option<KvSnapshot>, NetworkError> {
    let members = cluster_local_process_members(state);
    if members.is_empty() {
        return Ok(None);
    }
    let paths_for_match = paths.clone();
    let members_for_match = members.clone();
    let already_published = runtime
        .spawn_blocking(move || {
            cluster_member_publish_matches_kv(&paths_for_match, &members_for_match).unwrap_or(false)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster local member publish match".to_string(),
            detail: format!("member publish match task failed: {source}"),
        })?;
    if sinomo_kv::cluster_kv_raft_local_is_voter(state)
        && host.cluster_has_local_quorum_async(paths, state).await?
    {
        if already_published {
            return Ok(None);
        }
        let paths_for_txn = paths.clone();
        let members_for_txn = members.clone();
        let txn = runtime
            .spawn_blocking(move || {
                cluster_member_publish_txn_from_kv(&paths_for_txn, &members_for_txn)
            })
            .await
            .map_err(|source| NetworkError::Io {
                operation: "cluster local member publish txn".to_string(),
                detail: format!("member publish txn task failed: {source}"),
            })??;
        let mut commit_state = state.clone();
        return host
            .commit_cluster_kv_txn_with_quorum_async(paths, &mut commit_state, txn)
            .await
            .map(Some);
    }
    if already_published && !options.force_kv_snapshot_sync {
        let paths_for_targets = paths.clone();
        let state_for_targets = state.clone();
        let has_publish_targets = runtime
            .spawn_blocking(move || {
                !member_publish_targets_voter_first(&paths_for_targets, &state_for_targets)
                    .is_empty()
            })
            .await
            .map_err(|source| NetworkError::Io {
                operation: "cluster local member publish target probe".to_string(),
                detail: format!("member publish target probe task failed: {source}"),
            })?;
        if !has_publish_targets {
            return Ok(None);
        }
    }
    publish_local_members_via_voter_async_with_host(runtime, paths, state, options, host).await?;
    Ok(None)
}

enum PreparedClusterMemberPublish {
    Response(ClusterMemberPublishResponse),
    Commit {
        state: ClusterBootstrapState,
        txn: KvTxn,
    },
}

fn prepare_cluster_member_publish_request(
    paths: &ClusterPaths,
    request: ClusterMemberPublishRequest,
    signer_node_id: &str,
) -> Result<PreparedClusterMemberPublish, NetworkError> {
    let state = read_cluster_state(paths)?;
    if request.members.is_empty() {
        return Ok(PreparedClusterMemberPublish::Response(
            ClusterMemberPublishResponse {
                accepted: false,
                detail: "member publish request was empty".to_string(),
                state,
                kv_snapshot: None,
            },
        ));
    }
    for member in &request.members {
        if member.cluster_id != state.cluster_id {
            return Ok(PreparedClusterMemberPublish::Response(
                ClusterMemberPublishResponse {
                    accepted: false,
                    detail: format!(
                        "member {} cluster `{}` does not match local cluster `{}`",
                        member.node_id, member.cluster_id, state.cluster_id
                    ),
                    state,
                    kv_snapshot: None,
                },
            ));
        }
        if !cluster_member_publish_authorized_for_signer(&member.node_id, signer_node_id) {
            return Ok(PreparedClusterMemberPublish::Response(
                ClusterMemberPublishResponse {
                    accepted: false,
                    detail: format!(
                        "member publisher {} cannot publish member {}",
                        signer_node_id, member.node_id
                    ),
                    state,
                    kv_snapshot: None,
                },
            ));
        }
        if !state.members.iter().any(|existing| {
            existing.node_id == member.node_id
                && existing.node_public_key == member.node_public_key
                && existing.state.is_authorized()
        }) {
            return Ok(PreparedClusterMemberPublish::Response(
                ClusterMemberPublishResponse {
                    accepted: false,
                    detail: format!(
                        "member {} is not an authorized cluster member with the published public key",
                        member.node_id
                    ),
                    state,
                    kv_snapshot: None,
                },
            ));
        }
    }
    if cluster_member_publish_matches_kv(paths, &request.members)? {
        let kv_snapshot =
            cluster_member_publish_response_snapshot(paths, &state, request.local_kv_revision)?;
        return Ok(PreparedClusterMemberPublish::Response(
            ClusterMemberPublishResponse {
                accepted: true,
                detail: "member endpoints already current".to_string(),
                state,
                kv_snapshot,
            },
        ));
    }
    let txn = cluster_member_publish_txn_from_kv(paths, &request.members)?;
    Ok(PreparedClusterMemberPublish::Commit { state, txn })
}

fn cluster_member_publish_response_snapshot(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    publisher_revision: Option<KvRevision>,
) -> Result<Option<KvSnapshot>, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot(paths)?;
    let Some(publisher_revision) = publisher_revision else {
        // No readable snapshot at the publisher (or it predates the revision
        // hint): only a full import converges it.
        return Ok(Some(snapshot));
    };
    if publisher_revision < snapshot.revision {
        return Ok(Some(snapshot));
    }
    if publisher_revision > snapshot.revision {
        // Lag-only invariant: revisions are positions in the leader's single
        // linear history. A publisher ahead of THIS node means this node is
        // the stale one (normal when a fresh learner syncs through a lagging
        // bridge) — never ship our older snapshot backwards. A publisher
        // ahead of the LEADER is impossible and indicates replicated-state
        // divergence worth surfacing loudly.
        if sinomo_kv::cluster_kv_raft_local_is_voter(state) {
            eprintln!(
                "warning: member publish reported KV revision {} ahead of this voter's {} — replicated KV history may have diverged",
                publisher_revision.0, snapshot.revision.0
            );
        }
    }
    Ok(None)
}

pub fn handle_cluster_member_publish_request_with_host<H>(
    paths: &ClusterPaths,
    request: ClusterMemberPublishRequest,
    signer_node_id: &str,
    host: &mut H,
) -> Result<ClusterMemberPublishResponse, H::Error>
where
    H: ClusterMemberPublishHost,
{
    let (state, txn) = match prepare_cluster_member_publish_request(paths, request, signer_node_id)
        .map_err(H::Error::from)?
    {
        PreparedClusterMemberPublish::Response(response) => return Ok(response),
        PreparedClusterMemberPublish::Commit { state, txn } => (state, txn),
    };
    let mut commit_state = state.clone();
    match host.commit_cluster_kv_txn_with_quorum(paths, &mut commit_state, txn) {
        Ok(snapshot) => Ok(ClusterMemberPublishResponse {
            accepted: true,
            detail: "member endpoints published".to_string(),
            state: read_cluster_state(paths).unwrap_or(state),
            kv_snapshot: Some(snapshot),
        }),
        Err(error) => Ok(ClusterMemberPublishResponse {
            accepted: false,
            detail: error.to_string(),
            state,
            kv_snapshot: None,
        }),
    }
}

pub async fn handle_cluster_member_publish_request_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: ClusterMemberPublishRequest,
    signer_node_id: &str,
    host: &mut H,
) -> Result<ClusterMemberPublishResponse, NetworkError>
where
    H: ClusterMemberMutationAsyncHost + Send,
{
    let paths_for_prepare = paths.clone();
    let signer_node_id = signer_node_id.to_string();
    let prepared = runtime
        .spawn_blocking(move || {
            prepare_cluster_member_publish_request(&paths_for_prepare, request, &signer_node_id)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster member publish prepare".to_string(),
            detail: format!("member publish prepare task failed: {source}"),
        })??;
    let (state, txn) = match prepared {
        PreparedClusterMemberPublish::Response(response) => return Ok(response),
        PreparedClusterMemberPublish::Commit { state, txn } => (state, txn),
    };
    let mut commit_state = state.clone();
    match host
        .commit_cluster_kv_txn_with_quorum_async(paths, &mut commit_state, txn)
        .await
    {
        Ok(snapshot) => {
            let paths_for_state = paths.clone();
            let fallback_state = state.clone();
            let response_state = runtime
                .spawn_blocking(move || {
                    read_cluster_state(&paths_for_state).unwrap_or(fallback_state)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster member publish state refresh".to_string(),
                    detail: format!("member publish state refresh task failed: {source}"),
                })?;
            Ok(ClusterMemberPublishResponse {
                accepted: true,
                detail: "member endpoints published".to_string(),
                state: response_state,
                kv_snapshot: Some(snapshot),
            })
        }
        Err(error) => Ok(ClusterMemberPublishResponse {
            accepted: false,
            detail: error.to_string(),
            state,
            kv_snapshot: None,
        }),
    }
}

pub fn cluster_member_publish_authorized_for_signer(
    member_node_id: &NodeId,
    signer_node_id: &str,
) -> bool {
    if member_node_id.as_str() == signer_node_id {
        return true;
    }
    if member_node_id
        .as_str()
        .starts_with(&format!("{signer_node_id}-"))
    {
        return true;
    }
    NodeId::new(signer_node_id)
        .map(|signer| shadow_daemon_node_id(&signer) == *member_node_id)
        .unwrap_or(false)
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_member_publish_txn(members: &[ClusterMemberRecord]) -> Result<KvTxn, NetworkError> {
    cluster_member_publish_txn_with_snapshot(None, members)
}

pub(crate) fn cluster_member_publish_txn_from_kv(
    paths: &ClusterPaths,
    members: &[ClusterMemberRecord],
) -> Result<KvTxn, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    cluster_member_publish_txn_with_snapshot(Some(snapshot.as_ref()), members)
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_member_publish_txn_from_snapshot(
    snapshot: &KvSnapshot,
    members: &[ClusterMemberRecord],
) -> Result<KvTxn, NetworkError> {
    cluster_member_publish_txn_with_snapshot(Some(snapshot), members)
}

fn cluster_member_publish_txn_with_snapshot(
    snapshot: Option<&KvSnapshot>,
    members: &[ClusterMemberRecord],
) -> Result<KvTxn, NetworkError> {
    let mut success = Vec::new();
    for member in members {
        let mut member = cluster_member_publish_record_for_snapshot(snapshot, member);
        cleanup_member_iroh_endpoints(&mut member);
        push_snapshot_member_diff_put_op(
            snapshot,
            cluster_member_key(&member.node_id),
            &member,
            &mut success,
        )?;
        push_snapshot_capability_policy_diff_put_op(
            snapshot,
            cluster_capability_policy_key(&member.node_id),
            &ClusterCapabilityPolicyRecord::from(&member),
            &mut success,
        )?;
        if let Some(snapshot) = snapshot {
            push_cluster_member_endpoint_diff_ops(snapshot, &member, &mut success)?;
        } else {
            push_bounded_txn_success_op(
                &mut success,
                KvOp::DeletePrefix {
                    prefix: cluster_endpoint_prefix(&member.node_id),
                },
            )
            .map_err(kv_err)?;
            for endpoint in &member.endpoints {
                push_bounded_txn_success_op(
                    &mut success,
                    KvOp::put_serde(
                        cluster_endpoint_key(&member.node_id, endpoint),
                        endpoint,
                        None,
                    )
                    .map_err(kv_err)?,
                )
                .map_err(kv_err)?;
            }
        }
    }
    Ok(KvTxn {
        compares: Vec::new(),
        success,
        failure: Vec::new(),
    })
}

fn cluster_member_publish_record_for_snapshot(
    snapshot: Option<&KvSnapshot>,
    member: &ClusterMemberRecord,
) -> ClusterMemberRecord {
    let mut member = member.clone();
    let Some(existing) = snapshot
        .and_then(|snapshot| snapshot.get(&cluster_member_key(&member.node_id)))
        .and_then(|entry| entry.decode::<ClusterMemberRecord>().ok())
    else {
        return member;
    };
    member.display_name = existing.display_name;
    member
}

fn push_snapshot_member_diff_put_op(
    snapshot: Option<&KvSnapshot>,
    key: String,
    member: &ClusterMemberRecord,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let op = KvOp::put_serde(key.clone(), member, None).map_err(kv_err)?;
    if snapshot.is_some_and(|snapshot| {
        kv_put_op_matches_snapshot(snapshot, &op)
            || snapshot
                .get(&key)
                .and_then(|entry| entry.decode::<ClusterMemberRecord>().ok())
                .is_some_and(|stored| cluster_member_stable_eq(&stored, member))
    }) {
        return Ok(());
    }
    push_bounded_txn_success_op(ops, op).map_err(kv_err)?;
    Ok(())
}

fn push_snapshot_capability_policy_diff_put_op(
    snapshot: Option<&KvSnapshot>,
    key: String,
    policy: &ClusterCapabilityPolicyRecord,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let op = KvOp::put_serde(key.clone(), policy, None).map_err(kv_err)?;
    if snapshot.is_some_and(|snapshot| {
        kv_put_op_matches_snapshot(snapshot, &op)
            || snapshot
                .get(&key)
                .and_then(|entry| entry.decode::<ClusterCapabilityPolicyRecord>().ok())
                .is_some_and(|stored| cluster_capability_policy_stable_eq(&stored, policy))
    }) {
        return Ok(());
    }
    push_bounded_txn_success_op(ops, op).map_err(kv_err)?;
    Ok(())
}

fn kv_put_op_matches_snapshot(snapshot: &KvSnapshot, op: &KvOp) -> bool {
    let KvOp::Put {
        key,
        value,
        lease_id,
    } = op
    else {
        return false;
    };
    snapshot.get(key).is_some_and(|entry| {
        entry.value.as_slice() == value.as_slice() && entry.lease_id == *lease_id
    })
}

fn push_cluster_member_endpoint_diff_ops(
    snapshot: &KvSnapshot,
    member: &ClusterMemberRecord,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let endpoint_prefix = cluster_endpoint_prefix(&member.node_id);
    let mut desired_endpoints = BTreeMap::new();
    for endpoint in &member.endpoints {
        let key = cluster_endpoint_key(&member.node_id, endpoint);
        let op = KvOp::put_serde(key, endpoint, None).map_err(kv_err)?;
        let KvOp::Put { key, value, .. } = op else {
            unreachable!("put_serde must produce a put operation");
        };
        desired_endpoints.insert(key, (value, endpoint.clone()));
    }

    for entry in snapshot
        .range_prefix_limited(&endpoint_prefix, MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS)
        .map_err(kv_err)?
    {
        if !desired_endpoints.contains_key(&entry.key) {
            push_bounded_txn_success_op(
                ops,
                KvOp::Delete {
                    key: entry.key.clone(),
                },
            )
            .map_err(kv_err)?;
        }
    }

    for (key, (value, endpoint)) in desired_endpoints {
        if snapshot.get(&key).is_none_or(|entry| {
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
            )
            .map_err(kv_err)?;
        }
    }

    Ok(())
}

fn member_publish_targets_voter_first(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    ClusterEndpointSelector::new(paths, state).select_internal(
        ClusterEndpointInternalSelectionPurpose::MemberPublishClusterStateAuthorityFirst,
    )
}
