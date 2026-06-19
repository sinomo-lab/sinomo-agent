use crate::ClusterPaths;
use crate::error::NetworkError;
use crate::keyspace::{cluster_invitation_key, cluster_invitation_state_key};
use crate::kv_local_store::{
    append_cluster_raft_command, read_cluster_kv_current_snapshot,
    read_cluster_kv_current_snapshot_arc, read_cluster_kv_entry,
};
use crate::local_state_store::{read_cluster_state, read_cluster_state_without_kv};
use crate::member_mutation::ClusterMemberMutationAsyncHost;
use crate::membership_rpc::{ClusterInvitePublishRequest, ClusterInvitePublishResponse};
use crate::prelude::*;
use sinomo_kv::push_bounded_txn_success_op;
use sinomo_node::{ClusterInvitationStateRecord, cluster_invitation_state_stable_eq};

pub trait ClusterInvitePublishHost {
    type Error: From<NetworkError> + ToString;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error>;
}

pub fn read_cluster_invitation_from_kv(
    paths: &ClusterPaths,
    invite_id: &ClusterInviteId,
) -> Result<ClusterJoinInvite, NetworkError> {
    let key = cluster_invitation_key(invite_id);
    let entry = read_cluster_kv_entry(paths, &key)?.ok_or_else(|| NetworkError::StorageIo {
        path: key.clone(),
        detail: "cluster invitation is not published".to_string(),
    })?;
    entry.decode().map_err(Into::into)
}

pub(crate) fn cluster_invitation_publish_txn_from_kv(
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
    let txn = if sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        cluster_invitation_publish_txn_from_kv(paths, invite)?
    } else {
        cluster_invitation_publish_txn_without_snapshot(invite)?
    };
    if txn.success.is_empty() {
        return read_cluster_kv_current_snapshot(paths).map_err(Into::into);
    }
    let mut commit_state = state.clone();
    host.commit_cluster_kv_txn_with_quorum(paths, &mut commit_state, txn)
}

enum PreparedClusterInvitePublish {
    Response(ClusterInvitePublishResponse),
    Commit {
        state: ClusterBootstrapState,
        txn: KvTxn,
    },
}

fn prepare_cluster_invite_publish_request(
    paths: &ClusterPaths,
    request: ClusterInvitePublishRequest,
    signer_node_id: &str,
    signer_public_key: &str,
    now: OffsetDateTime,
) -> Result<PreparedClusterInvitePublish, NetworkError> {
    let state = read_cluster_state(paths)?;
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
    if !state.members.iter().any(|member| {
        member.node_id == request.invite.issuer_node_id
            && member.node_public_key == request.invite.issuer_public_key
            && member.state.is_authorized()
    }) {
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
    let txn = if sinomo_kv::cluster_kv_raft_local_is_voter(&state) {
        cluster_invitation_publish_txn_from_kv(paths, &request.invite)?
    } else {
        cluster_invitation_publish_txn_without_snapshot(&request.invite)?
    };
    if txn.success.is_empty() {
        return Ok(PreparedClusterInvitePublish::Response(
            ClusterInvitePublishResponse {
                accepted: true,
                detail: "invite published".to_string(),
                state,
                kv_snapshot: Some(read_cluster_kv_current_snapshot(paths)?),
            },
        ));
    }
    Ok(PreparedClusterInvitePublish::Commit { state, txn })
}

pub(crate) fn handle_cluster_invite_publish_request_with_host<H>(
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
    let (state, txn) = match prepare_cluster_invite_publish_request(
        paths,
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
    match host.commit_cluster_kv_txn_with_quorum(paths, &mut commit_state, txn) {
        Ok(snapshot) => Ok(ClusterInvitePublishResponse {
            accepted: true,
            detail: "invite published".to_string(),
            state: read_cluster_state(paths).unwrap_or(state),
            kv_snapshot: Some(snapshot),
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
    let paths_for_prepare = paths.clone();
    let signer_node_id = signer_node_id.to_string();
    let signer_public_key = signer_public_key.to_string();
    let prepared = runtime
        .spawn_blocking(move || {
            prepare_cluster_invite_publish_request(
                &paths_for_prepare,
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
                    operation: "cluster invite publish state refresh".to_string(),
                    detail: format!("invite publish state refresh task failed: {source}"),
                })?;
            Ok(ClusterInvitePublishResponse {
                accepted: true,
                detail: "invite published".to_string(),
                state: response_state,
                kv_snapshot: Some(snapshot),
            })
        }
        Err(error) => Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: error.to_string(),
            state,
            kv_snapshot: None,
        }),
    }
}

pub fn sync_cluster_invitation_to_kv(
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

fn cluster_invitation_publish_txn_without_snapshot(
    invite: &ClusterJoinInvite,
) -> Result<KvTxn, NetworkError> {
    cluster_invitation_publish_txn_with_snapshot(None, invite)
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
    let op = KvOp::put_serde(key, value, None)?;
    if snapshot.is_some_and(|snapshot| kv_put_op_matches_snapshot(snapshot, &op)) {
        return Ok(());
    }
    push_bounded_txn_success_op(ops, op)?;
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
    ops.push(KvOp::put_serde(key, invitation_state, None)?);
    Ok(())
}
