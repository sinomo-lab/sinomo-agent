use crate::keyspace::{cluster_invitation_key, cluster_invitation_state_key, cluster_member_key};
use crate::kv_local_store::kv_err;
#[cfg(any(test, feature = "test-support"))]
use crate::kv_local_store::{append_cluster_raft_command, read_cluster_kv_current_snapshot_arc};
use crate::kv_local_store::{read_cluster_kv_entry, read_cluster_kv_latest_observed_entry};
use crate::local_state_store::read_cluster_state;
#[cfg(any(test, feature = "test-support"))]
use crate::local_state_store::read_cluster_state_without_kv;
use crate::member_mutation::ClusterMemberMutationAsyncHost;
use crate::membership_rpc::{ClusterInvitePublishRequest, ClusterInvitePublishResponse};
use crate::prelude::*;
use sinomo_kv::push_bounded_txn_success_op;
#[allow(unused_imports)]
use sinomo_kv::{
    ClusterKvTxnCommitResult, KvAuthoritativeRead, KvCommand, KvCompare, KvEntry, KvOp, KvSnapshot,
    KvTxn, KvTxnRequest,
};
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_node::{ClusterInvitationStateRecord, cluster_invitation_state_stable_eq};

pub trait ClusterInvitePublishHost {
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

pub fn read_cluster_invitation_from_kv(
    paths: &ClusterPaths,
    invite_id: &ClusterInviteId,
) -> Result<ClusterJoinInvite, NetworkError> {
    let key = cluster_invitation_key(invite_id);
    decode_published_cluster_invitation(key.clone(), read_cluster_kv_entry(paths, &key)?)
}

/// Reads an invitation for a join introducer's transport authentication and
/// local preflight.
///
/// A voter validates against its applied Raft view. A non-voting bridge may
/// have just joined with a newer, authority-issued materialized snapshot while
/// its local Raft log is still catching up; in that narrow case the bridge may
/// use the newest observed read model so it can forward the registration. The
/// state authority revalidates the same request against applied Raft before it
/// can mutate membership.
pub fn read_cluster_invitation_for_join_introducer(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    invite_id: &ClusterInviteId,
) -> Result<ClusterJoinInvite, NetworkError> {
    if sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        return read_cluster_invitation_from_kv(paths, invite_id);
    }
    let key = cluster_invitation_key(invite_id);
    decode_published_cluster_invitation(
        key.clone(),
        read_cluster_kv_latest_observed_entry(paths, &key)?,
    )
}

fn decode_published_cluster_invitation(
    key: String,
    entry: Option<KvEntry>,
) -> Result<ClusterJoinInvite, NetworkError> {
    let entry = entry.ok_or_else(|| NetworkError::StorageIo {
        path: key.clone(),
        detail: "cluster invitation is not published".to_string(),
    })?;
    entry.decode().map_err(kv_err)
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_invitation_publish_txn_from_kv(
    paths: &ClusterPaths,
    invite: &ClusterJoinInvite,
) -> Result<KvTxn, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    cluster_invitation_publish_txn_with_snapshot(Some(snapshot.as_ref()), invite)
}

pub fn publish_cluster_invitation_to_quorum_with_host<H>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    host: &mut H,
) -> Result<KvSnapshot, H::Error>
where
    H: ClusterInvitePublishHost,
{
    let mut commit_state = state.clone();
    let authoritative = host.authoritative_cluster_kv_snapshot(paths, &mut commit_state)?;
    let txn =
        cluster_invitation_publish_txn_from_authoritative_snapshot(&authoritative.value, invite)?;
    let result = host.commit_cluster_kv_txn_with_quorum(
        paths,
        &mut commit_state,
        KvTxnRequest::generated(txn),
    )?;
    require_invite_publish_commit(result).map_err(H::Error::from)
}

enum PreparedClusterInvitePublish {
    Response(ClusterInvitePublishResponse),
    Commit {
        state: ClusterBootstrapState,
        txn: KvTxn,
    },
}

fn prepare_cluster_invite_publish_request(
    state: ClusterBootstrapState,
    snapshot: &KvSnapshot,
    request: ClusterInvitePublishRequest,
    signer_node_id: &str,
    signer_public_key: &str,
    now: OffsetDateTime,
) -> Result<PreparedClusterInvitePublish, NetworkError> {
    if request.invite.cluster_id != state.cluster_id {
        return Ok(PreparedClusterInvitePublish::Response(
            ClusterInvitePublishResponse {
                accepted: false,
                detail: format!(
                    "invite cluster `{}` does not match local cluster `{}`",
                    request.invite.cluster_id, state.cluster_id
                ),
                state,
                kv_snapshot: None,
            },
        ));
    }
    if request.invite.expires_at <= now {
        return Ok(PreparedClusterInvitePublish::Response(
            ClusterInvitePublishResponse {
                accepted: false,
                detail: format!(
                    "invite expired at {}",
                    request
                        .invite
                        .expires_at
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_else(|_| "unknown".to_string())
                ),
                state,
                kv_snapshot: None,
            },
        ));
    }
    if let Err(error) = crate::verify_cluster_join_invite_signature(&request.invite) {
        return Ok(PreparedClusterInvitePublish::Response(
            ClusterInvitePublishResponse {
                accepted: false,
                detail: format!("invite signature rejected: {error}"),
                state,
                kv_snapshot: None,
            },
        ));
    }
    let issuer_key = cluster_member_key(&request.invite.issuer_node_id);
    let authorized_issuer = snapshot
        .get(&issuer_key)
        .and_then(|entry| entry.decode::<ClusterMemberRecord>().ok())
        .is_some_and(|member| {
            member.cluster_id == request.invite.cluster_id
                && member.node_public_key == request.invite.issuer_public_key
                && member.state.is_authorized()
        });
    if !authorized_issuer {
        return Ok(PreparedClusterInvitePublish::Response(
            ClusterInvitePublishResponse {
                accepted: false,
                detail: format!(
                    "invite issuer {} is not an authorized cluster member",
                    request.invite.issuer_node_id
                ),
                state,
                kv_snapshot: None,
            },
        ));
    }
    if signer_node_id != request.invite.issuer_node_id.as_str()
        || signer_public_key != request.invite.issuer_public_key
    {
        return Ok(PreparedClusterInvitePublish::Response(
            ClusterInvitePublishResponse {
                accepted: false,
                detail: "invite publisher does not match invite issuer".to_string(),
                state,
                kv_snapshot: None,
            },
        ));
    }
    let txn =
        cluster_invitation_publish_txn_from_authoritative_snapshot(snapshot, &request.invite)?;
    Ok(PreparedClusterInvitePublish::Commit { state, txn })
}

pub fn handle_cluster_invite_publish_request_with_host<H>(
    paths: &ClusterPaths,
    request: ClusterInvitePublishRequest,
    signer_node_id: &str,
    signer_public_key: &str,
    now: OffsetDateTime,
    host: &mut H,
) -> Result<ClusterInvitePublishResponse, H::Error>
where
    H: ClusterInvitePublishHost,
{
    let state = read_cluster_state(paths).map_err(H::Error::from)?;
    let mut authority_state = state.clone();
    let authoritative = host.authoritative_cluster_kv_snapshot(paths, &mut authority_state)?;
    let (state, txn) = match prepare_cluster_invite_publish_request(
        state,
        &authoritative.value,
        request,
        signer_node_id,
        signer_public_key,
        now,
    )
    .map_err(H::Error::from)?
    {
        PreparedClusterInvitePublish::Response(response) => return Ok(response),
        PreparedClusterInvitePublish::Commit { state, txn } => (state, txn),
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
            Ok(ClusterInvitePublishResponse {
                accepted: true,
                detail: "invite published".to_string(),
                state: read_cluster_state(paths).unwrap_or(state),
                kv_snapshot: Some(result.snapshot),
            })
        }
        Ok(_) => Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: "cluster membership changed while publishing invitation".to_string(),
            state,
            kv_snapshot: None,
        }),
        Err(error) => Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: error.to_string(),
            state,
            kv_snapshot: None,
        }),
    }
}

pub async fn handle_cluster_invite_publish_request_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: ClusterInvitePublishRequest,
    signer_node_id: &str,
    signer_public_key: &str,
    now: OffsetDateTime,
    host: &mut H,
) -> Result<ClusterInvitePublishResponse, NetworkError>
where
    H: ClusterMemberMutationAsyncHost + Send,
{
    let paths_for_state = paths.clone();
    let state = runtime
        .spawn_blocking(move || read_cluster_state(&paths_for_state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster invite publish state read".to_string(),
            detail: format!("invite publish state read task failed: {source}"),
        })??;
    let mut authority_state = state.clone();
    let authoritative = host
        .authoritative_cluster_kv_snapshot_async(paths, &mut authority_state)
        .await?;
    let signer_node_id = signer_node_id.to_string();
    let signer_public_key = signer_public_key.to_string();
    let prepared = runtime
        .spawn_blocking(move || {
            prepare_cluster_invite_publish_request(
                state,
                &authoritative.value,
                request,
                &signer_node_id,
                &signer_public_key,
                now,
            )
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster invite publish prepare".to_string(),
            detail: format!("invite publish prepare task failed: {source}"),
        })??;
    let (state, txn) = match prepared {
        PreparedClusterInvitePublish::Response(response) => return Ok(response),
        PreparedClusterInvitePublish::Commit { state, txn } => (state, txn),
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
            let paths_for_state = paths.clone();
            let fallback_state = state.clone();
            let response_state = runtime
                .spawn_blocking(move || {
                    read_cluster_state(&paths_for_state).unwrap_or(fallback_state)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster invite publish state refresh".to_string(),
                    detail: format!("invite publish state refresh task failed: {source}"),
                })?;
            Ok(ClusterInvitePublishResponse {
                accepted: true,
                detail: "invite published".to_string(),
                state: response_state,
                kv_snapshot: Some(result.snapshot),
            })
        }
        Ok(_) => Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: "cluster membership changed while publishing invitation".to_string(),
            state,
            kv_snapshot: None,
        }),
        Err(error) => Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: error.to_string(),
            state,
            kv_snapshot: None,
        }),
    }
}

/// Seeds invitation state in the local Raft store without proving quorum.
///
/// This is deliberately unavailable to production callers. Runtime invitation
/// publication must use `publish_cluster_invitation_to_quorum_with_host`.
#[cfg(any(test, feature = "test-support"))]
pub fn sync_cluster_invitation_to_local_raft_unchecked(
    paths: &ClusterPaths,
    invite: &ClusterJoinInvite,
) -> Result<KvSnapshot, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    let txn = cluster_invitation_publish_txn_with_snapshot(Some(snapshot.as_ref()), invite)?;
    if txn.success.is_empty() {
        return Ok(snapshot.as_ref().clone());
    }
    let state = read_cluster_state_without_kv(paths)?;
    append_cluster_raft_command(paths, &state.local_node_id, KvCommand::Txn(txn))
}

fn cluster_invitation_publish_txn_from_authoritative_snapshot(
    snapshot: &KvSnapshot,
    invite: &ClusterJoinInvite,
) -> Result<KvTxn, NetworkError> {
    let issuer_key = cluster_member_key(&invite.issuer_node_id);
    let issuer_entry = snapshot
        .get(&issuer_key)
        .ok_or_else(|| NetworkError::ClusterReadOnly {
            detail: format!(
                "invitation issuer {} is absent from the authoritative cluster membership",
                invite.issuer_node_id
            ),
        })?;
    let issuer: ClusterMemberRecord = issuer_entry.decode().map_err(kv_err)?;
    if issuer.cluster_id != invite.cluster_id
        || issuer.node_public_key != invite.issuer_public_key
        || !issuer.state.is_authorized()
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "invitation issuer {} is not authorized by the current cluster membership",
                invite.issuer_node_id
            ),
        });
    }
    let mut txn = cluster_invitation_publish_txn_with_snapshot(Some(snapshot), invite)?;
    txn.compares.push(KvCompare::ValueEquals {
        key: issuer_key,
        value: issuer_entry.value.clone(),
    });
    Ok(txn)
}

fn require_invite_publish_commit(
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
            detail: "cluster membership changed while publishing invitation".to_string(),
        })
    }
}

fn cluster_invitation_publish_txn_with_snapshot(
    snapshot: Option<&KvSnapshot>,
    invite: &ClusterJoinInvite,
) -> Result<KvTxn, NetworkError> {
    let invitation_state = ClusterInvitationStateRecord {
        cluster_id: invite.cluster_id.clone(),
        invite_id: invite.invite_id.clone(),
        state: "active".to_string(),
        issuer_node_id: invite.issuer_node_id.clone(),
        expires_at: invite.expires_at,
        updated_at: OffsetDateTime::now_utc(),
    };
    let mut success = Vec::new();
    push_snapshot_diff_put_op(
        snapshot,
        cluster_invitation_key(&invite.invite_id),
        invite,
        &mut success,
    )?;
    push_invitation_state_diff_op(snapshot, &invitation_state, &mut success)?;
    Ok(KvTxn {
        compares: Vec::new(),
        success,
        failure: Vec::new(),
    })
}

fn push_snapshot_diff_put_op<T: Serialize + ?Sized>(
    snapshot: Option<&KvSnapshot>,
    key: String,
    value: &T,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let op = KvOp::put_serde(key, value, None).map_err(kv_err)?;
    if snapshot.is_some_and(|snapshot| kv_put_op_matches_snapshot(snapshot, &op)) {
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

fn push_invitation_state_diff_op(
    snapshot: Option<&KvSnapshot>,
    invitation_state: &ClusterInvitationStateRecord,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let key = cluster_invitation_state_key(&invitation_state.invite_id);
    if let Some(snapshot) = snapshot
        && let Some(entry) = snapshot.get(&key)
        && let Ok(existing) = entry.decode::<ClusterInvitationStateRecord>()
        && cluster_invitation_state_stable_eq(&existing, invitation_state)
    {
        return Ok(());
    }
    ops.push(KvOp::put_serde(key, invitation_state, None).map_err(kv_err)?);
    Ok(())
}
