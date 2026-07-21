use crate::keyspace::{
    cluster_capability_policy_key, cluster_endpoint_key, cluster_endpoint_prefix,
    cluster_member_key,
};
use crate::kv_local_store::kv_err;
use crate::kv_local_store::{
    imported_snapshot_advances_local_kv, local_cluster_kv_snapshot_revision,
    read_cluster_kv_current_or_cached_snapshot_arc_if_present,
    write_imported_cluster_kv_snapshot_if_changed,
};
use crate::local_state_store::read_cluster_state;
use crate::member_mutation::ClusterMemberMutationAsyncHost;
use crate::member_store::MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS;
use crate::membership_rpc::{ClusterMemberPublishRequest, ClusterMemberPublishResponse};
use crate::prelude::*;
use sinomo_kv::push_bounded_txn_success_op;
#[allow(unused_imports)]
use sinomo_kv::{
    ClusterKvTxnCommitResult, KvAuthoritativeRead, KvCompare, KvOp, KvRevision, KvSnapshot, KvTxn,
    KvTxnRequest,
};
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::cleanup_member_endpoint_duplicates;
use sinomo_network::{
    ClusterConnectivityRecord, ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector,
};
use sinomo_node::{
    ClusterCapabilityPolicyRecord, cluster_capability_policy_stable_eq,
    cluster_endpoint_list_stable_eq, cluster_endpoint_stable_eq, cluster_local_process_members,
    cluster_member_stable_eq,
};
use std::collections::BTreeSet;

pub trait ClusterMemberPublishHost {
    type Error: From<NetworkError> + ToString;

    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, Self::Error>;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<ClusterKvTxnCommitResult, Self::Error>;
}

pub struct ClusterLocalMemberPublishOptions {
    pub member_publish_timeout: Duration,
    /// Publish even when the local KV snapshot already reflects the local
    /// member records. This is reserved for an explicit, targeted learner
    /// recovery operation so a voter can hand back a newer KV snapshot (see
    /// `ClusterMemberPublishRequest::local_kv_revision`). Normal publication
    /// is source-change driven and must leave this disabled.
    pub force_kv_snapshot_sync: bool,
}

pub trait ClusterLocalMemberPublishHost {
    fn cluster_has_local_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> bool;

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

    fn authoritative_cluster_kv_snapshot_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, KvAuthoritativeRead<KvSnapshot>>;

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvTxnCommitResult>;

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
        cleanup_member_endpoint_duplicates(&mut member);
        member
            .endpoints
            .sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));

        let Some(entry) = snapshot.get(&cluster_member_key(&member.node_id)) else {
            return Ok(false);
        };
        let Ok(mut existing) = entry.decode::<ClusterMemberRecord>() else {
            return Ok(false);
        };
        cleanup_member_endpoint_duplicates(&mut existing);
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

fn cluster_member_publish_txn_from_authoritative_snapshot(
    snapshot: &KvSnapshot,
    members: &[ClusterMemberRecord],
    recover_missing_local_member: Option<&NodeId>,
) -> Result<KvTxn, NetworkError> {
    let mut txn = cluster_member_publish_txn_with_snapshot(Some(snapshot), members)?;
    let mut compared = BTreeSet::new();
    let mut config_compared = false;
    for member in members {
        let key = cluster_member_key(&member.node_id);
        if !compared.insert(key.clone()) {
            continue;
        }
        if let Some(entry) = snapshot.get(&key) {
            let current: ClusterMemberRecord = entry.decode().map_err(kv_err)?;
            if current.cluster_id != member.cluster_id
                || current.node_public_key != member.node_public_key
                || !current.state.is_authorized()
            {
                return Err(NetworkError::ClusterReadOnly {
                    detail: format!(
                        "member {} is not authorized by the current cluster membership with the published identity",
                        member.node_id
                    ),
                });
            }
            txn.compares.push(KvCompare::ValueEquals {
                key,
                value: entry.value.clone(),
            });
            continue;
        }

        // A local voter may repair its own accidentally missing membership
        // projection, but only while the same authoritative snapshot still
        // commits that node and role in the Raft configuration. The config
        // compare binds this recovery permission to the subsequent commit;
        // remote publication cannot use this path to self-register.
        if recover_missing_local_member != Some(&member.node_id) || !member.state.is_authorized() {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "member {} is absent from the authoritative cluster membership",
                    member.node_id
                ),
            });
        }
        let config_key = sinomo_kv::cluster_kv_raft_config_key();
        let config_entry =
            snapshot
                .get(&config_key)
                .ok_or_else(|| NetworkError::ClusterReadOnly {
                    detail: format!(
                        "member {} cannot be repaired without an authoritative Raft configuration",
                        member.node_id
                    ),
                })?;
        let config: sinomo_kv::ClusterKvRaftConfigRecord = config_entry.decode().map_err(kv_err)?;
        if sinomo_kv::cluster_kv_raft_config_member_role(&config, &member.node_id)
            != Some(member.role.clone())
        {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "member {} is absent from authoritative membership and its local role is not committed in the Raft configuration",
                    member.node_id
                ),
            });
        }
        if !config_compared {
            txn.compares.push(KvCompare::ValueEquals {
                key: config_key,
                value: config_entry.value.clone(),
            });
            config_compared = true;
        }
        txn.compares.push(KvCompare::Missing { key });
    }
    Ok(txn)
}

fn require_member_publish_commit(
    result: ClusterKvTxnCommitResult,
) -> Result<KvSnapshot, NetworkError> {
    if result
        .txn_response
        .as_ref()
        .is_some_and(|response| response.succeeded)
    {
        Ok(result.snapshot)
    } else {
        Err(NetworkError::ClusterReadOnly {
            detail: "cluster membership changed while publishing member state".to_string(),
        })
    }
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
    // Publication is source-change driven. A matching local materialized view
    // proves this source revision has already been applied; an unchanged
    // periodic network tick must not create an authoritative read barrier or a
    // duplicate publish RPC. Learners that need snapshot catch-up explicitly
    // set `force_kv_snapshot_sync` and therefore continue below.
    if already_published && !options.force_kv_snapshot_sync {
        return Ok(None);
    }
    if sinomo_kv::cluster_kv_raft_local_is_voter(state)
        && host.cluster_has_local_quorum(paths, state)
    {
        let mut commit_state = state.clone();
        let authoritative = host.authoritative_cluster_kv_snapshot(paths, &mut commit_state)?;
        let txn = cluster_member_publish_txn_from_authoritative_snapshot(
            &authoritative.value,
            &members,
            Some(&state.local_node_id),
        )?;
        // The authoritative read is already the consistency barrier. Do not
        // turn a stable publication into an empty replicated write.
        if txn.success.is_empty() {
            return Ok(Some(authoritative.value));
        }
        let result = host.commit_cluster_kv_txn_with_quorum(
            paths,
            &mut commit_state,
            KvTxnRequest::generated(txn),
        )?;
        return require_member_publish_commit(result).map(Some);
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
    // See the synchronous path above: stable publication is a quiet state, not
    // an opportunity to run a quorum barrier or ask a voter to compare the
    // same record again. Forced learner synchronization remains an explicit,
    // targeted recovery operation.
    if already_published && !options.force_kv_snapshot_sync {
        return Ok(None);
    }
    if sinomo_kv::cluster_kv_raft_local_is_voter(state)
        && host.cluster_has_local_quorum_async(paths, state).await?
    {
        let mut commit_state = state.clone();
        let authoritative = host
            .authoritative_cluster_kv_snapshot_async(paths, &mut commit_state)
            .await?;
        let members_for_txn = members.clone();
        let snapshot_for_txn = authoritative.value;
        let recovery_node_id = commit_state.local_node_id.clone();
        let (snapshot, txn) = runtime
            .spawn_blocking(move || {
                let txn = cluster_member_publish_txn_from_authoritative_snapshot(
                    &snapshot_for_txn,
                    &members_for_txn,
                    Some(&recovery_node_id),
                )?;
                Ok::<_, NetworkError>((snapshot_for_txn, txn))
            })
            .await
            .map_err(|source| NetworkError::Io {
                operation: "cluster local member publish txn".to_string(),
                detail: format!("member publish txn task failed: {source}"),
            })??;
        // Preserve ownership of the read-barrier snapshot so a stable
        // publication can return it without cloning or entering Raft.
        if txn.success.is_empty() {
            return Ok(Some(snapshot));
        }
        let result = host
            .commit_cluster_kv_txn_with_quorum_async(
                paths,
                &mut commit_state,
                KvTxnRequest::generated(txn),
            )
            .await?;
        return require_member_publish_commit(result).map(Some);
    }
    publish_local_members_via_voter_async_with_host(runtime, paths, state, options, host).await?;
    Ok(None)
}

enum PreparedClusterMemberPublish {
    Response(ClusterMemberPublishResponse),
    AlreadyCurrent {
        state: ClusterBootstrapState,
        snapshot: KvSnapshot,
        publisher_revision: Option<KvRevision>,
    },
    Commit {
        state: ClusterBootstrapState,
        txn: KvTxn,
        publisher_revision: Option<KvRevision>,
    },
}

fn prepare_cluster_member_publish_request(
    state: ClusterBootstrapState,
    snapshot: KvSnapshot,
    request: ClusterMemberPublishRequest,
    signer_node_id: &str,
) -> Result<PreparedClusterMemberPublish, NetworkError> {
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
        let key = cluster_member_key(&member.node_id);
        let authoritative_member = snapshot
            .get(&key)
            .and_then(|entry| entry.decode::<ClusterMemberRecord>().ok());
        if !authoritative_member.is_some_and(|existing| {
            existing.cluster_id == member.cluster_id
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
    let txn =
        cluster_member_publish_txn_from_authoritative_snapshot(&snapshot, &request.members, None)?;
    // Authentication and the desired records were checked against this
    // authoritative snapshot. Keep it for the response instead of appending
    // an empty transaction solely to recover the same snapshot.
    if txn.success.is_empty() {
        return Ok(PreparedClusterMemberPublish::AlreadyCurrent {
            state,
            snapshot,
            publisher_revision: request.local_kv_revision,
        });
    }
    Ok(PreparedClusterMemberPublish::Commit {
        state,
        txn,
        publisher_revision: request.local_kv_revision,
    })
}

fn cluster_member_publish_response_snapshot(
    snapshot: KvSnapshot,
    state: &ClusterBootstrapState,
    publisher_revision: Option<KvRevision>,
) -> Option<KvSnapshot> {
    let Some(publisher_revision) = publisher_revision else {
        return Some(snapshot);
    };
    if publisher_revision < snapshot.revision {
        return Some(snapshot);
    }
    if publisher_revision > snapshot.revision && sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        eprintln!(
            "warning: member publish reported KV revision {} ahead of the authoritative voter's {} -- replicated KV history may have diverged",
            publisher_revision.0, snapshot.revision.0
        );
    }
    None
}

fn cluster_member_publish_already_current_response(
    state: ClusterBootstrapState,
    snapshot: KvSnapshot,
    publisher_revision: Option<KvRevision>,
) -> ClusterMemberPublishResponse {
    let kv_snapshot =
        cluster_member_publish_response_snapshot(snapshot, &state, publisher_revision);
    ClusterMemberPublishResponse {
        accepted: true,
        detail: "member endpoints already current".to_string(),
        state,
        kv_snapshot,
    }
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
    let state = read_cluster_state(paths).map_err(H::Error::from)?;
    let mut authority_state = state.clone();
    let authoritative = host.authoritative_cluster_kv_snapshot(paths, &mut authority_state)?;
    let (state, txn, publisher_revision) = match prepare_cluster_member_publish_request(
        state,
        authoritative.value,
        request,
        signer_node_id,
    )
    .map_err(H::Error::from)?
    {
        PreparedClusterMemberPublish::Response(response) => return Ok(response),
        PreparedClusterMemberPublish::AlreadyCurrent {
            state,
            snapshot,
            publisher_revision,
        } => {
            let state = read_cluster_state(paths).unwrap_or(state);
            return Ok(cluster_member_publish_already_current_response(
                state,
                snapshot,
                publisher_revision,
            ));
        }
        PreparedClusterMemberPublish::Commit {
            state,
            txn,
            publisher_revision,
        } => (state, txn, publisher_revision),
    };
    let mut commit_state = state.clone();
    let request = KvTxnRequest::generated(txn);
    match host.commit_cluster_kv_txn_with_quorum(paths, &mut commit_state, request) {
        Ok(result)
            if result
                .txn_response
                .as_ref()
                .is_some_and(|response| response.succeeded) =>
        {
            let kv_snapshot = cluster_member_publish_response_snapshot(
                result.snapshot,
                &state,
                publisher_revision,
            );
            Ok(ClusterMemberPublishResponse {
                accepted: true,
                detail: "member endpoints published".to_string(),
                state: read_cluster_state(paths).unwrap_or(state),
                kv_snapshot,
            })
        }
        Ok(_) => Ok(ClusterMemberPublishResponse {
            accepted: false,
            detail: "cluster membership changed while publishing member state".to_string(),
            state,
            kv_snapshot: None,
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
    let paths_for_state = paths.clone();
    let state = runtime
        .spawn_blocking(move || read_cluster_state(&paths_for_state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster member publish state read".to_string(),
            detail: format!("member publish state read task failed: {source}"),
        })??;
    let mut authority_state = state.clone();
    let authoritative = host
        .authoritative_cluster_kv_snapshot_async(paths, &mut authority_state)
        .await?;
    let signer_node_id = signer_node_id.to_string();
    let prepared = runtime
        .spawn_blocking(move || {
            prepare_cluster_member_publish_request(
                state,
                authoritative.value,
                request,
                &signer_node_id,
            )
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster member publish prepare".to_string(),
            detail: format!("member publish prepare task failed: {source}"),
        })??;
    let (state, txn, publisher_revision) = match prepared {
        PreparedClusterMemberPublish::Response(response) => return Ok(response),
        PreparedClusterMemberPublish::AlreadyCurrent {
            state,
            snapshot,
            publisher_revision,
        } => {
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
            return Ok(cluster_member_publish_already_current_response(
                response_state,
                snapshot,
                publisher_revision,
            ));
        }
        PreparedClusterMemberPublish::Commit {
            state,
            txn,
            publisher_revision,
        } => (state, txn, publisher_revision),
    };
    let mut commit_state = state.clone();
    let request = KvTxnRequest::generated(txn);
    match host
        .commit_cluster_kv_txn_with_quorum_async(paths, &mut commit_state, request)
        .await
    {
        Ok(result)
            if result
                .txn_response
                .as_ref()
                .is_some_and(|response| response.succeeded) =>
        {
            let kv_snapshot = cluster_member_publish_response_snapshot(
                result.snapshot,
                &state,
                publisher_revision,
            );
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
                kv_snapshot,
            })
        }
        Ok(_) => Ok(ClusterMemberPublishResponse {
            accepted: false,
            detail: "cluster membership changed while publishing member state".to_string(),
            state,
            kv_snapshot: None,
        }),
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
        cleanup_member_endpoint_duplicates(&mut member);
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
    member
        .endpoints
        .retain(|endpoint| endpoint.privacy != ClusterEndpointPrivacy::LocalOnly);
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
    let mut removed_endpoint_ids = Vec::new();

    for entry in snapshot
        .range_prefix_limited(&endpoint_prefix, MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS)
        .map_err(kv_err)?
    {
        if !desired_endpoints.contains_key(&entry.key) {
            if let Ok(endpoint) = entry.decode::<ClusterEndpointRecord>() {
                sinomo_network::try_reserve_vec(
                    &mut removed_endpoint_ids,
                    1,
                    "collect removed member endpoint ids",
                )?;
                removed_endpoint_ids.push(endpoint.endpoint_id);
            }
            push_bounded_txn_success_op(
                ops,
                KvOp::Delete {
                    key: entry.key.clone(),
                },
            )
            .map_err(kv_err)?;
        }
    }

    push_removed_member_endpoint_connectivity_delete_ops(
        snapshot,
        &member.node_id,
        &removed_endpoint_ids,
        ops,
    )?;

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

fn push_removed_member_endpoint_connectivity_delete_ops(
    snapshot: &KvSnapshot,
    member_node_id: &NodeId,
    removed_endpoint_ids: &[ClusterEndpointId],
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    if removed_endpoint_ids.is_empty() {
        return Ok(());
    }
    for entry in snapshot.prefix_entries("/cluster/network/connectivity/") {
        let record: ClusterConnectivityRecord = entry.decode().map_err(kv_err)?;
        if record.target_node_id != *member_node_id
            || !removed_endpoint_ids.contains(&record.endpoint_id)
        {
            continue;
        }
        push_bounded_txn_success_op(
            ops,
            KvOp::Delete {
                key: entry.key.clone(),
            },
        )
        .map_err(kv_err)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct UnexpectedPublishHost {
        calls: usize,
    }

    impl ClusterLocalMemberPublishHost for UnexpectedPublishHost {
        fn cluster_has_local_quorum(
            &mut self,
            _paths: &ClusterPaths,
            _state: &ClusterBootstrapState,
        ) -> bool {
            self.calls += 1;
            panic!("stable member publication must not probe quorum")
        }

        fn authoritative_cluster_kv_snapshot(
            &mut self,
            _paths: &ClusterPaths,
            _state: &mut ClusterBootstrapState,
        ) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
            self.calls += 1;
            panic!("stable member publication must not run an authoritative read")
        }

        fn commit_cluster_kv_txn_with_quorum(
            &mut self,
            _paths: &ClusterPaths,
            _state: &mut ClusterBootstrapState,
            _request: KvTxnRequest,
        ) -> Result<ClusterKvTxnCommitResult, NetworkError> {
            self.calls += 1;
            panic!("stable member publication must not enter Raft")
        }

        fn send_cluster_member_publish(
            &mut self,
            _endpoint: &ClusterEndpointRecord,
            _request: &ClusterMemberPublishRequest,
            _timeout: Duration,
        ) -> Result<ClusterMemberPublishResponse, NetworkError> {
            self.calls += 1;
            panic!("stable member publication must not send a duplicate RPC")
        }
    }

    #[test]
    fn member_publish_record_does_not_publish_local_only_endpoints() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_member_publish_sanitize").expect("cluster");
        let node_id = NodeId::from_legacy_named("node_member_publish_sanitize").expect("node");
        let mut member = ClusterMemberRecord::local_voter(
            cluster_id,
            node_id.clone(),
            "publisher",
            "publisher-public-key",
            now,
        );
        member.endpoints.push(ClusterEndpointRecord::loopback(
            node_id.clone(),
            "127.0.0.1:21353",
        ));
        member.endpoints.push(sinomo_network::daemon_lan_endpoint(
            &node_id,
            "peer.example.test:21353".to_string(),
            0,
        ));

        let published = cluster_member_publish_record_for_snapshot(None, &member);

        assert_eq!(published.endpoints.len(), 1);
        assert!(
            published
                .endpoints
                .iter()
                .all(|endpoint| endpoint.privacy != ClusterEndpointPrivacy::LocalOnly),
            "{:?}",
            published.endpoints
        );
        assert_eq!(member.endpoints.len(), 2, "local state must stay untouched");
    }

    #[test]
    fn unchanged_member_publication_is_terminal_without_quorum_or_rpc() {
        let root = sinomo_testdir::TestDir::new("cluster-member-publish-source-change");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_member_publish_source_change").expect("cluster"),
            "member publish source change",
            NodeId::from_legacy_named("node_member_publish_source_change").expect("node"),
            "publisher",
            "publisher-public-key",
            OffsetDateTime::now_utc(),
        );
        crate::write_cluster_state(&paths, &state).expect("write cluster state");
        crate::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("materialize member state");
        let members = cluster_local_process_members(&state);
        assert!(cluster_member_publish_matches_kv(&paths, &members).expect("compare publication"));

        let mut host = UnexpectedPublishHost::default();
        let result = publish_local_members_to_quorum_if_needed_with_host(
            &paths,
            &state,
            ClusterLocalMemberPublishOptions {
                member_publish_timeout: Duration::from_secs(1),
                force_kv_snapshot_sync: false,
            },
            &mut host,
        )
        .expect("stable publication");

        assert_eq!(result, None);
        assert_eq!(host.calls, 0);
    }
}
