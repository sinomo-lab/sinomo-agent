use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
#[cfg(any(test, feature = "test-support"))]
use crate::invite_publish::read_cluster_invitation_for_join_introducer;
use crate::keyspace::{
    cluster_capability_policy_key, cluster_endpoint_prefix, cluster_identity_key,
    cluster_invitation_key, cluster_invitation_state_key, cluster_member_key,
};
use crate::kv_local_store::kv_err;
use crate::kv_local_store::write_imported_cluster_kv_snapshot_if_changed;
use crate::local_membership::{
    ClusterLocalDaemonMembershipObservation, apply_local_daemon_membership_observation,
    merge_cluster_join_registration,
};
use crate::local_state_store::{
    read_cluster_state, read_cluster_state_without_kv, write_cluster_state,
};
use crate::member_mutation::{
    ClusterMemberMutationHost, require_cluster_kv_txn_succeeded,
    update_cluster_state_authority_with_host,
};
use crate::member_store::{apply_cluster_raft_role, cluster_member_update_ops_from_store};
use crate::membership_forward::{
    ClusterMembershipRpcForwardHost,
    forward_cluster_membership_rpc_to_state_authority_async_with_host,
    forward_cluster_membership_rpc_to_state_authority_with_host,
};
use crate::membership_rpc::{ClusterJoinRegistrationRequest, ClusterJoinRegistrationResponse};
use crate::prelude::*;
#[allow(unused_imports)]
use sinomo_kv::KvSnapshot;
use sinomo_kv::{
    CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES, ClusterKvRaftConfigRecord, ClusterKvTxnCommitResult,
    InProcessKvStore, KvAuthoritativeRead, KvCompare, KvOp, KvTxn, KvTxnRequest,
};
use sinomo_network::ClusterConnectivityState;
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterPaths;
use sinomo_network::ClusterSignedObjectRpcAsyncHost;
use sinomo_network::NetworkError;
use sinomo_network::{
    CLUSTER_JOIN_CHALLENGE_ROUTE, CLUSTER_JOIN_REGISTER_ROUTE, CLUSTER_MEMBERSHIP_RPC_CATEGORY,
    ObjectRpcRequest, ObjectRpcResponse,
};
use sinomo_network::{cleanup_member_endpoint_duplicates, upsert_member_endpoint};
#[cfg(any(test, feature = "test-support"))]
use sinomo_node::upsert_member_record;
use sinomo_node::{
    ClusterCapabilityPolicyRecord, ClusterIdentityRecord, ClusterInvitationStateRecord,
    cluster_member_is_local_process, cluster_member_stable_eq, shadow_daemon_node_id,
};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

const JOIN_RECONCILIATION_POLL_INTERVAL: Duration = Duration::from_millis(25);

fn join_registration_trace(args: std::fmt::Arguments<'_>) {
    let enabled = std::env::var("SINOMO_CLUSTER_JOIN_TRACE")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"));
    if enabled {
        eprintln!("[sinomo cluster join] {args}");
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterJoinReconciliation {
    pub accepted_endpoint: Option<ClusterEndpointRecord>,
    pub kv_snapshot: Option<KvSnapshot>,
}

#[cfg(any(test, feature = "test-support"))]
pub fn prepare_cluster_join_registration_response(
    paths: &ClusterPaths,
    state: ClusterBootstrapState,
    request: ClusterJoinRegistrationRequest,
    now: OffsetDateTime,
) -> Result<ClusterJoinRegistrationResponse, NetworkError> {
    let persisted_invite =
        read_cluster_invitation_for_join_introducer(paths, &state, &request.invite.invite_id)?;
    if request.invite.cluster_public_key != persisted_invite.cluster_public_key
        || request.invitation_secret.trim().is_empty()
        || request.invitation_secret != persisted_invite.invitation_secret
    {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: "invitation rejected".to_string(),
            state,
            kv_snapshot: None,
            responder_node_id: None,
        });
    }
    if let Err(error) = crate::verify_cluster_join_invite_signature(&persisted_invite) {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!("invite signature rejected: {error}"),
            state,
            kv_snapshot: None,
            responder_node_id: None,
        });
    }
    if !state.members.iter().any(|member| {
        member.node_id == persisted_invite.issuer_node_id
            && member.node_public_key == persisted_invite.issuer_public_key
            && member.state.is_authorized()
    }) {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite issuer {} is not an authorized cluster member",
                persisted_invite.issuer_node_id
            ),
            state,
            kv_snapshot: None,
            responder_node_id: None,
        });
    }
    if persisted_invite.cluster_id != state.cluster_id {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite cluster `{}` does not match introducer cluster `{}`",
                persisted_invite.cluster_id, state.cluster_id
            ),
            state,
            kv_snapshot: None,
            responder_node_id: None,
        });
    }
    if persisted_invite.expires_at <= now {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite expired at {}",
                persisted_invite
                    .expires_at
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_else(|_| "unknown".to_string())
            ),
            state,
            kv_snapshot: None,
            responder_node_id: None,
        });
    }
    if request
        .members
        .iter()
        .any(|member| !persisted_invite.allowed_member_roles.contains(&member.role))
    {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: "join request includes a member role not allowed by the invite".to_string(),
            state,
            kv_snapshot: None,
            responder_node_id: None,
        });
    }

    let mut accepted_state = state.clone();
    for mut member in request.members {
        member.cluster_id = accepted_state.cluster_id.clone();
        if matches!(member.state, ClusterMemberState::Pending) {
            member.state = match member.role {
                ClusterMemberRole::Learner => ClusterMemberState::Learner,
                ClusterMemberRole::NonVoting => ClusterMemberState::NonVoting,
                ClusterMemberRole::Voter => ClusterMemberState::Voter,
            };
        }
        member.updated_at = now;
        upsert_member_record(&mut accepted_state, member);
    }
    accepted_state.kv_revision = accepted_state.kv_revision.saturating_add(1);
    accepted_state.updated_at = now;
    Ok(ClusterJoinRegistrationResponse {
        accepted: true,
        detail: "join registered".to_string(),
        state: accepted_state,
        kv_snapshot: None,
        responder_node_id: None,
    })
}

struct PreparedAuthoritativeClusterJoin {
    state: ClusterBootstrapState,
    seed_txn: KvTxn,
    learner_node_ids: Vec<NodeId>,
}

enum AuthoritativeClusterJoinPreparation {
    Rejected(ClusterJoinRegistrationResponse),
    Commit(PreparedAuthoritativeClusterJoin),
}

fn rejected_cluster_join_registration(
    state: ClusterBootstrapState,
    detail: impl Into<String>,
) -> AuthoritativeClusterJoinPreparation {
    AuthoritativeClusterJoinPreparation::Rejected(ClusterJoinRegistrationResponse {
        accepted: false,
        detail: detail.into(),
        state,
        kv_snapshot: None,
        responder_node_id: None,
    })
}

fn prepare_authoritative_cluster_join_registration(
    state: ClusterBootstrapState,
    snapshot: &KvSnapshot,
    request: ClusterJoinRegistrationRequest,
    now: OffsetDateTime,
) -> Result<AuthoritativeClusterJoinPreparation, NetworkError> {
    let invite_key = cluster_invitation_key(&request.invite.invite_id);
    let Some(invite_entry) = snapshot.get(&invite_key).cloned() else {
        return Ok(rejected_cluster_join_registration(
            state,
            "invitation is not present in the authoritative cluster KV",
        ));
    };
    let persisted_invite: ClusterJoinInvite = invite_entry.decode().map_err(kv_err)?;
    if persisted_invite != request.invite
        || request.invitation_secret.trim().is_empty()
        || request.invitation_secret != persisted_invite.invitation_secret
    {
        return Ok(rejected_cluster_join_registration(
            state,
            "invitation rejected",
        ));
    }
    if let Err(error) = crate::verify_cluster_join_invite_signature(&persisted_invite) {
        return Ok(rejected_cluster_join_registration(
            state,
            format!("invite signature rejected: {error}"),
        ));
    }
    if persisted_invite.cluster_id != state.cluster_id {
        return Ok(rejected_cluster_join_registration(
            state.clone(),
            format!(
                "invite cluster `{}` does not match introducer cluster `{}`",
                persisted_invite.cluster_id, state.cluster_id
            ),
        ));
    }
    if persisted_invite.expires_at <= now {
        return Ok(rejected_cluster_join_registration(
            state,
            format!(
                "invite expired at {}",
                persisted_invite
                    .expires_at
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_else(|_| "unknown".to_string())
            ),
        ));
    }

    let invitation_state_key = cluster_invitation_state_key(&persisted_invite.invite_id);
    let Some(invitation_state_entry) = snapshot.get(&invitation_state_key).cloned() else {
        return Ok(rejected_cluster_join_registration(
            state,
            "invitation has no authoritative lifecycle state",
        ));
    };
    let invitation_state: ClusterInvitationStateRecord =
        invitation_state_entry.decode().map_err(kv_err)?;
    if invitation_state.state != "active"
        || invitation_state.cluster_id != persisted_invite.cluster_id
        || invitation_state.invite_id != persisted_invite.invite_id
        || invitation_state.issuer_node_id != persisted_invite.issuer_node_id
        || invitation_state.expires_at != persisted_invite.expires_at
    {
        return Ok(rejected_cluster_join_registration(
            state,
            "invitation is not active in the authoritative cluster KV",
        ));
    }

    let issuer_key = cluster_member_key(&persisted_invite.issuer_node_id);
    let Some(issuer_entry) = snapshot.get(&issuer_key).cloned() else {
        return Ok(rejected_cluster_join_registration(
            state,
            format!(
                "invite issuer {} is not an authorized cluster member",
                persisted_invite.issuer_node_id
            ),
        ));
    };
    let issuer: ClusterMemberRecord = issuer_entry.decode().map_err(kv_err)?;
    if issuer.cluster_id != persisted_invite.cluster_id
        || issuer.node_public_key != persisted_invite.issuer_public_key
        || !issuer.state.is_authorized()
    {
        return Ok(rejected_cluster_join_registration(
            state,
            format!(
                "invite issuer {} is not an authorized cluster member",
                persisted_invite.issuer_node_id
            ),
        ));
    }

    if request.members.is_empty() || request.members.len() > 2 {
        return Ok(rejected_cluster_join_registration(
            state,
            "join registration must contain one learner and at most one non-voting shadow",
        ));
    }
    if request.members.iter().any(|member| {
        member.role == ClusterMemberRole::Voter
            || !persisted_invite.allowed_member_roles.contains(&member.role)
    }) {
        return Ok(rejected_cluster_join_registration(
            state,
            "join request includes an unsupported or unauthorized member role",
        ));
    }
    let learner_members = request
        .members
        .iter()
        .filter(|member| member.role == ClusterMemberRole::Learner)
        .collect::<Vec<_>>();
    let non_voting_members = request
        .members
        .iter()
        .filter(|member| member.role == ClusterMemberRole::NonVoting)
        .collect::<Vec<_>>();
    if learner_members.len() != 1 || non_voting_members.len() > 1 {
        return Ok(rejected_cluster_join_registration(
            state,
            "join registration must contain exactly one learner and at most one non-voting shadow",
        ));
    }
    if let Some(shadow) = non_voting_members.first()
        && shadow.node_id != shadow_daemon_node_id(&learner_members[0].node_id)
    {
        return Ok(rejected_cluster_join_registration(
            state,
            "the non-voting join member is not the learner's shadow daemon",
        ));
    }
    let mut member_ids = BTreeSet::new();
    let mut member_public_keys = BTreeSet::new();
    if request.members.iter().any(|member| {
        member.cluster_id != persisted_invite.cluster_id
            || member.node_public_key.trim().is_empty()
            || !member_ids.insert(member.node_id.clone())
            || !member_public_keys.insert(member.node_public_key.clone())
    }) {
        return Ok(rejected_cluster_join_registration(
            state,
            "join request contains conflicting member identities",
        ));
    }

    let config_key = sinomo_kv::cluster_kv_raft_config_key();
    let config_entry =
        snapshot
            .get(&config_key)
            .cloned()
            .ok_or_else(|| NetworkError::ClusterReadOnly {
                detail: "established cluster Raft config is missing during join admission"
                    .to_string(),
            })?;
    let config: ClusterKvRaftConfigRecord = config_entry.decode().map_err(kv_err)?;
    if config.cluster_id != persisted_invite.cluster_id {
        return Err(NetworkError::ClusterReadOnly {
            detail: "authoritative Raft config belongs to a different cluster".to_string(),
        });
    }

    let store = InProcessKvStore::restore(snapshot.clone()).map_err(kv_err)?;
    let mut compares = vec![
        KvCompare::ValueEquals {
            key: invite_key,
            value: invite_entry.value,
        },
        KvCompare::ValueEquals {
            key: invitation_state_key,
            value: invitation_state_entry.value,
        },
        KvCompare::ValueEquals {
            key: issuer_key,
            value: issuer_entry.value,
        },
        KvCompare::ValueEquals {
            key: config_key,
            value: config_entry.value,
        },
    ];
    let mut compared_keys = compares
        .iter()
        .map(|compare| match compare {
            KvCompare::Exists { key }
            | KvCompare::Missing { key }
            | KvCompare::ValueEquals { key, .. }
            | KvCompare::VersionEquals { key, .. }
            | KvCompare::CreateRevisionEquals { key, .. }
            | KvCompare::ModifyRevisionEquals { key, .. }
            | KvCompare::LeaseEquals { key, .. } => key.clone(),
        })
        .collect::<BTreeSet<_>>();
    let mut success = Vec::new();
    let mut learner_node_ids = Vec::new();

    for requested in request.members {
        let desired_role = requested.role.clone();
        let mut seeded = requested.clone();
        seeded.cluster_id = persisted_invite.cluster_id.clone();
        seeded.updated_at = now;
        if desired_role == ClusterMemberRole::Learner {
            apply_cluster_raft_role(&mut seeded, ClusterMemberRole::NonVoting, now);
            learner_node_ids.push(seeded.node_id.clone());
        } else {
            seeded.state = ClusterMemberState::NonVoting;
        }
        let mut final_member = requested;
        final_member.cluster_id = persisted_invite.cluster_id.clone();
        if desired_role == ClusterMemberRole::Learner {
            apply_cluster_raft_role(&mut final_member, ClusterMemberRole::Learner, now);
        } else {
            final_member.state = ClusterMemberState::NonVoting;
            final_member.updated_at = now;
        }

        let member_key = cluster_member_key(&seeded.node_id);
        let config_role = sinomo_kv::cluster_kv_raft_config_member_role(&config, &seeded.node_id);
        if let Some(existing_entry) = snapshot.get(&member_key).cloned() {
            let existing: ClusterMemberRecord = existing_entry.decode().map_err(kv_err)?;
            let exact_seed_retry =
                config_role.is_none() && cluster_member_stable_eq(&existing, &seeded);
            let exact_finished_retry = desired_role == ClusterMemberRole::Learner
                && config_role == Some(ClusterMemberRole::Learner)
                && cluster_member_stable_eq(&existing, &final_member);
            if !exact_seed_retry && !exact_finished_retry {
                return Ok(rejected_cluster_join_registration(
                    state,
                    format!(
                        "cluster member `{}` conflicts with an existing identity or role",
                        seeded.node_id
                    ),
                ));
            }
            if compared_keys.insert(member_key.clone()) {
                compares.push(KvCompare::ValueEquals {
                    key: member_key,
                    value: existing_entry.value,
                });
            }
            continue;
        }
        if config_role.is_some() {
            return Ok(rejected_cluster_join_registration(
                state,
                format!(
                    "cluster member `{}` has a Raft role without a persisted member identity",
                    seeded.node_id
                ),
            ));
        }
        for op in cluster_member_update_ops_from_store(&store, &seeded)? {
            let key = match &op {
                KvOp::Put { key, .. } | KvOp::Delete { key } => key.clone(),
                KvOp::DeletePrefix { .. }
                | KvOp::LeaseGrant { .. }
                | KvOp::LeaseKeepAlive { .. }
                | KvOp::LeaseRevoke { .. } => {
                    return Err(NetworkError::InvalidClusterCommand {
                        detail: "join member seeding produced a non-member KV operation"
                            .to_string(),
                    });
                }
            };
            if compared_keys.insert(key.clone()) {
                match snapshot.get(&key) {
                    Some(entry) => compares.push(KvCompare::ValueEquals {
                        key,
                        value: entry.value.clone(),
                    }),
                    None => compares.push(KvCompare::Missing { key }),
                }
            }
            success.push(op);
        }
    }

    Ok(AuthoritativeClusterJoinPreparation::Commit(
        PreparedAuthoritativeClusterJoin {
            state,
            seed_txn: KvTxn {
                compares,
                success,
                failure: Vec::new(),
            },
            learner_node_ids,
        },
    ))
}

fn cluster_join_response_state_from_snapshot(
    mut state: ClusterBootstrapState,
    snapshot: &KvSnapshot,
    now: OffsetDateTime,
) -> Result<ClusterBootstrapState, NetworkError> {
    state.members = crate::read_cluster_members_from_snapshot(snapshot)?;
    state.kv_revision = snapshot.revision.0;
    if let Some(config) = snapshot
        .get(&sinomo_kv::cluster_kv_raft_config_key())
        .and_then(|entry| entry.decode::<ClusterKvRaftConfigRecord>().ok())
    {
        state.term = config.term;
    }
    state.updated_at = now;
    Ok(state)
}

pub fn handle_cluster_join_registration_request_with_host(
    paths: &ClusterPaths,
    request: ClusterJoinRegistrationRequest,
    now: OffsetDateTime,
    host: &mut impl ClusterJoinRegistrationHost,
) -> Result<ClusterJoinRegistrationResponse, NetworkError> {
    let mut state = read_cluster_state(paths)?;
    if let Some(observation) = host.local_daemon_membership_observation(paths, &state)? {
        apply_local_daemon_membership_observation(&mut state, observation);
    }
    if !cluster_kv_local_is_state_authority(&state) {
        let rpc_request = cluster_join_registration_rpc_request(&request)?;
        let response = forward_cluster_membership_rpc_to_state_authority_with_host(
            paths,
            &state,
            &rpc_request,
            "join introducer is not a state authority and could not forward the join to any state authority",
            host,
        )?;
        let response: ClusterJoinRegistrationResponse = response.decode_body()?;
        if response.accepted {
            record_forwarded_join_registration_acceptance_with_host(
                paths,
                &state,
                &request.members,
                &response,
                host,
            )?;
        }
        return Ok(response);
    }

    let mut authority_state = state.clone();
    let authoritative = host.authoritative_cluster_kv_snapshot(paths, &mut authority_state)?;
    let prepared = match prepare_authoritative_cluster_join_registration(
        state,
        &authoritative.value,
        request.clone(),
        now,
    )? {
        AuthoritativeClusterJoinPreparation::Rejected(response) => return Ok(response),
        AuthoritativeClusterJoinPreparation::Commit(prepared) => prepared,
    };
    let mut commit_state = prepared.state.clone();
    let seeded = host.commit_join_registration_txn(
        paths,
        &mut commit_state,
        KvTxnRequest::generated(prepared.seed_txn),
    )?;
    let seeded = match require_cluster_kv_txn_succeeded(seeded, "seeding join members") {
        Ok(seeded) => seeded,
        Err(_) => {
            return Ok(ClusterJoinRegistrationResponse {
                accepted: false,
                detail: "cluster invitation or membership changed during join admission"
                    .to_string(),
                state: prepared.state,
                kv_snapshot: None,
                responder_node_id: None,
            });
        }
    };
    let mut snapshot = seeded.snapshot;
    for learner_node_id in &prepared.learner_node_ids {
        snapshot =
            host.admit_cluster_raft_learner(paths, &mut commit_state, learner_node_id, now)?;
    }
    let response_state = cluster_join_response_state_from_snapshot(prepared.state, &snapshot, now)?;
    write_cluster_state(paths, &response_state)?;
    if join_registration_should_remember_successful_targets(&response_state, &request.invite) {
        host.remember_successful_join_targets(paths, &response_state, &request.members)?;
    }
    Ok(ClusterJoinRegistrationResponse {
        accepted: true,
        detail: "join registered".to_string(),
        state: response_state,
        kv_snapshot: Some(snapshot),
        responder_node_id: None,
    })
}

pub async fn handle_cluster_join_registration_request_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: ClusterJoinRegistrationRequest,
    now: OffsetDateTime,
    host: &mut (impl ClusterJoinRegistrationAsyncHost + Send),
) -> Result<ClusterJoinRegistrationResponse, NetworkError> {
    let state_paths = paths.clone();
    let mut state = runtime
        .spawn_blocking(move || read_cluster_state(&state_paths))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "read cluster state".to_string(),
            detail: format!("cluster state read task failed: {source}"),
        })??;
    if let Some(observation) = host
        .local_daemon_membership_observation_async(paths, &state)
        .await?
    {
        apply_local_daemon_membership_observation(&mut state, observation);
    }
    if !cluster_kv_local_is_state_authority(&state) {
        let rpc_request = cluster_join_registration_rpc_request(&request)?;
        let forward_started = Instant::now();
        join_registration_trace(format_args!(
            "registration forward invite={} started with {} member(s)",
            request.invite.invite_id,
            request.members.len()
        ));
        let response = forward_cluster_membership_rpc_to_state_authority_async_with_host(
            runtime,
            paths,
            &state,
            &rpc_request,
            "join introducer is not a state authority and could not forward the join to any state authority",
            host,
        )
        .await?;
        join_registration_trace(format_args!(
            "registration forward invite={} rpc returned in {:?}",
            request.invite.invite_id,
            forward_started.elapsed()
        ));
        let decode_started = Instant::now();
        let response: ClusterJoinRegistrationResponse = response.decode_body()?;
        join_registration_trace(format_args!(
            "registration forward invite={} decode completed in {:?}",
            request.invite.invite_id,
            decode_started.elapsed()
        ));
        if response.accepted {
            let record_started = Instant::now();
            join_registration_trace(format_args!(
                "registration forward invite={} record acceptance started",
                request.invite.invite_id
            ));
            record_forwarded_join_registration_acceptance_async_with_host(
                runtime,
                paths,
                &state,
                &request.members,
                &response,
                host,
            )
            .await?;
            join_registration_trace(format_args!(
                "registration forward invite={} record acceptance completed in {:?}",
                request.invite.invite_id,
                record_started.elapsed()
            ));
        }
        return Ok(response);
    }

    let mut authority_state = state.clone();
    let authoritative = host
        .authoritative_cluster_kv_snapshot_async(paths, &mut authority_state)
        .await?;
    let prepare_state = state;
    let prepare_snapshot = authoritative.value;
    let prepare_request = request.clone();
    let prepared = runtime
        .spawn_blocking(move || {
            prepare_authoritative_cluster_join_registration(
                prepare_state,
                &prepare_snapshot,
                prepare_request,
                now,
            )
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster join registration prepare".to_string(),
            detail: format!("join registration prepare task failed: {source}"),
        })??;
    let prepared = match prepared {
        AuthoritativeClusterJoinPreparation::Rejected(response) => return Ok(response),
        AuthoritativeClusterJoinPreparation::Commit(prepared) => prepared,
    };
    let mut commit_state = prepared.state.clone();
    let seeded = host
        .commit_join_registration_txn_async(
            paths,
            &mut commit_state,
            KvTxnRequest::generated(prepared.seed_txn),
        )
        .await?;
    let seeded = match require_cluster_kv_txn_succeeded(seeded, "seeding join members") {
        Ok(seeded) => seeded,
        Err(_) => {
            return Ok(ClusterJoinRegistrationResponse {
                accepted: false,
                detail: "cluster invitation or membership changed during join admission"
                    .to_string(),
                state: prepared.state,
                kv_snapshot: None,
                responder_node_id: None,
            });
        }
    };
    let mut snapshot = seeded.snapshot;
    for learner_node_id in &prepared.learner_node_ids {
        snapshot = host
            .admit_cluster_raft_learner_async(paths, &mut commit_state, learner_node_id, now)
            .await?;
    }
    let response_state_input = prepared.state;
    let response_snapshot = snapshot.clone();
    let response_state = runtime
        .spawn_blocking(move || {
            cluster_join_response_state_from_snapshot(response_state_input, &response_snapshot, now)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster join registration response projection".to_string(),
            detail: format!("join response projection task failed: {source}"),
        })??;
    let write_paths = paths.clone();
    let write_state = response_state.clone();
    runtime
        .spawn_blocking(move || write_cluster_state(&write_paths, &write_state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "write cluster state".to_string(),
            detail: format!("cluster state write task failed: {source}"),
        })??;
    if join_registration_should_remember_successful_targets(&response_state, &request.invite) {
        host.remember_successful_join_targets_async(paths, &response_state, &request.members)
            .await?;
    }
    Ok(ClusterJoinRegistrationResponse {
        accepted: true,
        detail: "join registered".to_string(),
        state: response_state,
        kv_snapshot: Some(snapshot),
        responder_node_id: None,
    })
}

pub fn send_cluster_join_registration_over_membership_rpc<S>(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterJoinRegistrationRequest,
    timeout: Duration,
    mut send_rpc: S,
) -> Result<ClusterJoinRegistrationResponse, NetworkError>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ObjectRpcRequest,
        Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>,
{
    let challenge = crate::invite::random_url_secret(32)?;
    let challenge_request = crate::invite::ClusterInviteChallengeRequest {
        protocol: crate::invite::CLUSTER_INVITE_CHALLENGE_PROTOCOL.to_string(),
        invite_id: request.invite.invite_id.clone(),
        challenge: challenge.clone(),
    };
    let challenge_rpc = ObjectRpcRequest::encode_body(
        CLUSTER_JOIN_CHALLENGE_ROUTE,
        CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        &challenge_request,
    )?;
    let proof_response = send_rpc(endpoint, &challenge_rpc, timeout)?;
    let proof: crate::invite::ClusterInviteChallengeProof = proof_response.decode_body()?;
    crate::invite::verify_cluster_invite_challenge_proof(&request.invite, &challenge, &proof)?;

    let register_rpc = cluster_join_registration_rpc_request(request)?;
    let response = send_rpc(endpoint, &register_rpc, timeout)?;
    let responder_node_id = response
        .responder_node_id()
        .and_then(|node_id| NodeId::new(node_id).ok());
    let mut decoded: ClusterJoinRegistrationResponse = response.decode_body()?;
    if decoded.responder_node_id.is_none() {
        decoded.responder_node_id = responder_node_id;
    }
    Ok(decoded)
}

/// Join registration is an idempotent member upsert keyed by the joining
/// nodes' stable ids. Explicitly allow one fresh-auth redispatch when a pooled
/// connection fails after dispatch; the repeated request cannot create a
/// duplicate member, and the normal snapshot reconciliation still verifies
/// the accepted state.
fn cluster_join_registration_rpc_request(
    request: &ClusterJoinRegistrationRequest,
) -> Result<ObjectRpcRequest, NetworkError> {
    Ok(ObjectRpcRequest::encode_body(
        CLUSTER_JOIN_REGISTER_ROUTE,
        CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        request,
    )?
    .with_response_retry_safe())
}

pub fn record_forwarded_join_registration_acceptance_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request_members: &[ClusterMemberRecord],
    response: &ClusterJoinRegistrationResponse,
    host: &mut impl ClusterJoinRegistrationHost,
) -> Result<(), NetworkError> {
    if !response.accepted {
        return Ok(());
    }
    if let Some(snapshot) = response.kv_snapshot.as_ref() {
        write_imported_cluster_kv_snapshot_if_changed(paths, snapshot)?;
    }
    let mut local_state = read_cluster_state_without_kv(paths)?;
    merge_cluster_join_registration(&mut local_state, response.state.clone());
    write_cluster_state(paths, &local_state)?;
    host.remember_successful_join_targets(paths, state, request_members)
}

pub async fn record_forwarded_join_registration_acceptance_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request_members: &[ClusterMemberRecord],
    response: &ClusterJoinRegistrationResponse,
    host: &mut (impl ClusterJoinRegistrationAsyncHost + Send),
) -> Result<(), NetworkError> {
    if !response.accepted {
        return Ok(());
    }
    let paths_for_state = paths.clone();
    let response_state = response.state.clone();
    let snapshot = response.kv_snapshot.clone();
    runtime
        .spawn_blocking(move || {
            if let Some(snapshot) = snapshot.as_ref() {
                write_imported_cluster_kv_snapshot_if_changed(&paths_for_state, snapshot)?;
            }
            let mut local_state = read_cluster_state_without_kv(&paths_for_state)?;
            merge_cluster_join_registration(&mut local_state, response_state);
            write_cluster_state(&paths_for_state, &local_state)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster forwarded join acceptance".to_string(),
            detail: format!("forwarded join acceptance task failed: {source}"),
        })??;
    host.remember_successful_join_targets_async(paths, state, request_members)
        .await
}

pub fn reconcile_cluster_join_with_introducers_with_host(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    tcp_timeout: Duration,
    iroh_timeout: Duration,
    connectivity_paths: Option<&ClusterPaths>,
    host: &mut impl ClusterJoinReconciliationHost,
) -> Result<ClusterJoinReconciliation, NetworkError> {
    if invite.communication_channels.is_empty() && invite.introducers.is_empty() {
        return Ok(ClusterJoinReconciliation {
            accepted_endpoint: None,
            kv_snapshot: None,
        });
    }
    let request = ClusterJoinRegistrationRequest {
        invitation_secret: invite.invitation_secret.clone(),
        invite: invite.clone(),
        members: state.members.clone(),
    };
    let deadline = Instant::now() + tcp_timeout.max(iroh_timeout);
    let mut failures = Vec::new();
    for channel in crate::invite_channels_for_join_registration(&request.invite) {
        let channel_started = Instant::now();
        let Some(global_remaining) = deadline
            .checked_duration_since(channel_started)
            .filter(|remaining| !remaining.is_zero())
        else {
            failures.push("join registration deadline elapsed".to_string());
            break;
        };
        let tcp_remaining = global_remaining.min(tcp_timeout);
        let iroh_remaining = global_remaining.min(iroh_timeout);
        let channel_remaining = match channel.endpoint.transport {
            ClusterTransportKind::Iroh => iroh_remaining,
            _ => tcp_remaining,
        };
        let channel_reconciliation_deadline = channel_started
            .checked_add(channel_remaining)
            .unwrap_or(deadline)
            .min(deadline);
        match host.send_join_registration(
            &channel.endpoint,
            &request,
            tcp_remaining,
            iroh_remaining,
        ) {
            Ok(response) => {
                if response.accepted {
                    if let Some(reconciliation) = reconcile_accepted_join_response_with_host(
                        state,
                        &request.invite,
                        &channel.endpoint,
                        response,
                        connectivity_paths,
                        host,
                    )? {
                        return Ok(reconciliation);
                    }
                    if let Some(reconciliation) =
                        reconcile_already_accepted_join_from_snapshot_with_host(
                            state,
                            &request.invite,
                            &channel.endpoint,
                            channel_reconciliation_deadline,
                            connectivity_paths,
                            host,
                        )?
                    {
                        return Ok(reconciliation);
                    }
                    failures.push(format!(
                        "{} ({:?}) returned unverified join acceptance",
                        channel.endpoint.address, channel.endpoint.transport
                    ));
                    continue;
                }
                if let Some(reconciliation) =
                    reconcile_already_accepted_join_from_snapshot_with_host(
                        state,
                        &request.invite,
                        &channel.endpoint,
                        channel_reconciliation_deadline,
                        connectivity_paths,
                        host,
                    )?
                {
                    return Ok(reconciliation);
                }
                failures.push(format!(
                    "{} ({:?}) rejected join: {}",
                    channel.endpoint.address, channel.endpoint.transport, response.detail
                ));
            }
            Err(error) => {
                if let Some(reconciliation) =
                    reconcile_already_accepted_join_from_snapshot_with_host(
                        state,
                        &request.invite,
                        &channel.endpoint,
                        channel_reconciliation_deadline,
                        connectivity_paths,
                        host,
                    )?
                {
                    return Ok(reconciliation);
                }
                failures.push(format!(
                    "{} ({:?}) failed: {}",
                    channel.endpoint.address, channel.endpoint.transport, error
                ));
            }
        }
    }
    if !failures.is_empty() {
        return Err(NetworkError::Io {
            operation: "cluster introducers".to_string(),
            detail: format!(
                "join registration failed for all introducers: {}. If the invite only contains 127.0.0.1 or localhost, create a new invite with `sinomo cluster invite --introducer <host-lan-ip>:21353`.",
                failures.join("; ")
            ),
        });
    }
    Ok(ClusterJoinReconciliation {
        accepted_endpoint: None,
        kv_snapshot: None,
    })
}

fn reconcile_accepted_join_response_with_host(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    response: ClusterJoinRegistrationResponse,
    connectivity_paths: Option<&ClusterPaths>,
    host: &mut impl ClusterJoinReconciliationHost,
) -> Result<Option<ClusterJoinReconciliation>, NetworkError> {
    let reconciliation =
        reconcile_accepted_cluster_join_response(state, invite, endpoint, response)?;
    observe_join_reconciliation_connectivity(
        connectivity_paths,
        state,
        reconciliation.as_ref(),
        host,
    )?;
    Ok(reconciliation)
}

fn reconcile_already_accepted_join_from_snapshot_with_host(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    deadline: Instant,
    connectivity_paths: Option<&ClusterPaths>,
    host: &mut impl ClusterJoinReconciliationHost,
) -> Result<Option<ClusterJoinReconciliation>, NetworkError> {
    // Once registration has been dispatched, its outcome is ambiguous until
    // the accepted member appears in an authoritative snapshot. Observe that
    // outcome until the original budget expires; never create a second write
    // attempt from this reconciliation path.
    loop {
        let Some(remaining) = deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
        else {
            return Ok(None);
        };
        if let Ok(snapshot) = host.fetch_cluster_kv_snapshot(endpoint, remaining) {
            let reconciliation =
                reconcile_accepted_cluster_join_snapshot(state, invite, endpoint, snapshot)?;
            observe_join_reconciliation_connectivity(
                connectivity_paths,
                state,
                reconciliation.as_ref(),
                host,
            )?;
            if reconciliation.is_some() {
                return Ok(reconciliation);
            }
        }

        let Some(remaining) = deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
        else {
            return Ok(None);
        };
        host.wait_for_join_reconciliation_retry(remaining.min(JOIN_RECONCILIATION_POLL_INTERVAL));
    }
}

fn observe_join_reconciliation_connectivity(
    connectivity_paths: Option<&ClusterPaths>,
    state: &ClusterBootstrapState,
    reconciliation: Option<&ClusterJoinReconciliation>,
    host: &mut impl ClusterJoinReconciliationHost,
) -> Result<(), NetworkError> {
    let (Some(paths), Some(reconciliation)) = (connectivity_paths, reconciliation) else {
        return Ok(());
    };
    host.observe_join_reconciliation_connectivity(paths, state, reconciliation)
}

pub fn accepted_cluster_join_state_from_kv_snapshot(
    pending: &ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    snapshot: &KvSnapshot,
) -> Result<Option<ClusterBootstrapState>, NetworkError> {
    let Some(identity_entry) = snapshot.get(&cluster_identity_key()) else {
        return Ok(None);
    };
    let identity: ClusterIdentityRecord = identity_entry.decode().map_err(kv_err)?;
    if crate::invite_has_embedded_cluster_identity(invite)
        && identity.cluster_id != invite.cluster_id
    {
        return Ok(None);
    }
    let mut members = Vec::new();
    for entry in snapshot
        .range_prefix_limited("/cluster/members/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
        .map_err(kv_err)?
    {
        let mut member: ClusterMemberRecord = entry.decode().map_err(kv_err)?;
        if member.cluster_id != identity.cluster_id {
            return Ok(None);
        }
        if let Some(policy_entry) = snapshot.get(&cluster_capability_policy_key(&member.node_id)) {
            let policy: ClusterCapabilityPolicyRecord = policy_entry.decode().map_err(kv_err)?;
            member.capabilities = policy.capabilities;
            member.allowed_capabilities = policy.allowed_capabilities;
        }
        for endpoint_entry in snapshot
            .range_prefix_limited(
                &cluster_endpoint_prefix(&member.node_id),
                CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
            )
            .map_err(kv_err)?
        {
            let endpoint: ClusterEndpointRecord = endpoint_entry.decode().map_err(kv_err)?;
            if endpoint.node_id == member.node_id {
                upsert_member_endpoint(&mut member, endpoint);
            }
        }
        cleanup_member_endpoint_duplicates(&mut member);
        members.push(member);
    }
    let authority_public_key = if invite.issuer_public_key.trim().is_empty() {
        invite.cluster_public_key.as_str()
    } else {
        invite.issuer_public_key.as_str()
    };
    let authority_member_is_authorized = members.iter().any(|member| {
        member.node_public_key == authority_public_key
            && member.state.is_authorized()
            && (invite.issuer_public_key.trim().is_empty()
                || member.node_id == invite.issuer_node_id)
    });
    if members.is_empty()
        || authority_public_key.trim().is_empty()
        || !authority_member_is_authorized
    {
        return Ok(None);
    }
    let local_processes = pending
        .members
        .iter()
        .filter(|member| cluster_member_is_local_process(pending, member))
        .filter(|member| !member.node_public_key.trim().is_empty())
        .collect::<Vec<_>>();
    if local_processes.is_empty()
        || local_processes.iter().any(|local| {
            !members.iter().any(|member| {
                member.node_id == local.node_id
                    && member.node_public_key == local.node_public_key
                    && member.state.is_authorized()
            })
        })
    {
        return Ok(None);
    }
    members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(Some(ClusterBootstrapState {
        cluster_id: identity.cluster_id,
        cluster_name: identity.cluster_name,
        local_node_id: identity.local_node_id,
        local_node_display_name: identity.local_node_display_name,
        term: identity.term,
        kv_revision: identity.kv_revision,
        members,
        initialized_at: identity.initialized_at,
        updated_at: identity.updated_at,
    }))
}

pub fn accepted_cluster_join_response_state_authorizes_local_processes(
    pending: &ClusterBootstrapState,
    accepted: &ClusterBootstrapState,
) -> bool {
    if accepted.cluster_id != pending.cluster_id {
        return false;
    }
    let local_processes = pending
        .members
        .iter()
        .filter(|member| cluster_member_is_local_process(pending, member))
        .filter(|member| !member.node_public_key.trim().is_empty())
        .collect::<Vec<_>>();
    !local_processes.is_empty()
        && local_processes.iter().all(|local| {
            accepted.members.iter().any(|member| {
                member.node_id == local.node_id
                    && member.node_public_key == local.node_public_key
                    && member.state.is_authorized()
            })
        })
}

pub fn reconcile_accepted_cluster_join_response(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    response: ClusterJoinRegistrationResponse,
) -> Result<Option<ClusterJoinReconciliation>, NetworkError> {
    let responder_node_id = response.responder_node_id.clone();
    if let Some(snapshot) = response.kv_snapshot {
        let snapshot = snapshot.canonicalized();
        let Some(accepted_state) =
            accepted_cluster_join_state_from_kv_snapshot(state, invite, &snapshot)?
        else {
            return Ok(None);
        };
        let endpoint = accepted_join_endpoint_with_responder(
            &accepted_state,
            endpoint,
            responder_node_id.as_ref(),
        );
        return Ok(Some(apply_accepted_cluster_join_state(
            state,
            invite,
            &endpoint,
            accepted_state,
            Some(snapshot),
        )));
    }

    if !crate::invite_has_embedded_cluster_identity(invite)
        || !accepted_cluster_join_response_state_authorizes_local_processes(state, &response.state)
    {
        return Ok(None);
    }

    let endpoint = accepted_join_endpoint_with_responder(
        &response.state,
        endpoint,
        responder_node_id.as_ref(),
    );
    Ok(Some(apply_accepted_cluster_join_state(
        state,
        invite,
        &endpoint,
        response.state,
        None,
    )))
}

pub fn reconcile_accepted_cluster_join_snapshot(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    snapshot: KvSnapshot,
) -> Result<Option<ClusterJoinReconciliation>, NetworkError> {
    let snapshot = snapshot.canonicalized();
    let Some(accepted_state) =
        accepted_cluster_join_state_from_kv_snapshot(state, invite, &snapshot)?
    else {
        return Ok(None);
    };
    Ok(Some(apply_accepted_cluster_join_state(
        state,
        invite,
        endpoint,
        accepted_state,
        Some(snapshot),
    )))
}

pub fn apply_accepted_cluster_join_state(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    accepted_state: ClusterBootstrapState,
    kv_snapshot: Option<KvSnapshot>,
) -> ClusterJoinReconciliation {
    merge_cluster_join_registration(state, accepted_state);
    let accepted_endpoint = join_channel_endpoint_for_accepted_state(state, invite, endpoint);
    remember_join_channel_endpoint_in_local_state(state, &accepted_endpoint);
    ClusterJoinReconciliation {
        accepted_endpoint: Some(accepted_endpoint),
        kv_snapshot,
    }
}

pub fn join_channel_endpoint_for_accepted_state(
    state: &ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
) -> ClusterEndpointRecord {
    let Some(host) = cluster_endpoint_address_host(&endpoint.address) else {
        return endpoint.clone();
    };
    let member = state.members.iter().find(|member| {
        member.state.is_authorized()
            && (member.display_name == host
                || member.endpoints.iter().any(|candidate| {
                    cluster_endpoint_address_host(&candidate.address).as_deref() == Some(host)
                }))
    });
    let node_id = member
        .map(|member| &member.node_id)
        .or_else(|| {
            state
                .members
                .iter()
                .any(|member| member.node_id == endpoint.node_id && member.state.is_authorized())
                .then_some(&endpoint.node_id)
        })
        .or_else(|| {
            state
                .members
                .iter()
                .any(|member| {
                    member.node_id == invite.issuer_node_id && member.state.is_authorized()
                })
                .then_some(&invite.issuer_node_id)
        })
        .unwrap_or(&endpoint.node_id);
    retarget_cluster_endpoint_to_node(endpoint, node_id)
}

fn join_registration_should_remember_successful_targets(
    state: &ClusterBootstrapState,
    invite: &ClusterJoinInvite,
) -> bool {
    let channels = crate::invite_channels_for_join_registration(invite);
    if channels.is_empty() {
        return true;
    }
    channels.into_iter().any(|channel| {
        join_channel_endpoint_for_accepted_state(state, invite, &channel.endpoint).node_id
            == state.local_node_id
    })
}

fn accepted_join_endpoint_with_responder(
    accepted_state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    responder_node_id: Option<&NodeId>,
) -> ClusterEndpointRecord {
    let Some(node_id) = responder_node_id else {
        return endpoint.clone();
    };
    if !accepted_state
        .members
        .iter()
        .any(|member| member.node_id == *node_id && member.state.is_authorized())
    {
        return endpoint.clone();
    }
    retarget_cluster_endpoint_to_node(endpoint, node_id)
}

fn retarget_cluster_endpoint_to_node(
    endpoint: &ClusterEndpointRecord,
    node_id: &NodeId,
) -> ClusterEndpointRecord {
    if *node_id == endpoint.node_id {
        return endpoint.clone();
    }
    match endpoint.transport {
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback => {
            sinomo_network::daemon_lan_endpoint(
                node_id,
                endpoint.address.clone(),
                endpoint.priority,
            )
        }
        _ => {
            let mut retargeted = endpoint.clone();
            retargeted.node_id = node_id.clone();
            retargeted
        }
    }
}

fn remember_join_channel_endpoint_in_local_state(
    state: &mut ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
) {
    if endpoint.node_id == state.local_node_id
        || endpoint.address.parse::<SocketAddr>().is_ok()
        || !matches!(
            endpoint.transport,
            ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
        )
    {
        return;
    }
    let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == endpoint.node_id && member.state.is_authorized())
    else {
        return;
    };
    upsert_member_endpoint(member, endpoint.clone());
}

fn cluster_endpoint_address_host(address: &str) -> Option<&str> {
    let host = address
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(address)
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']');
    (!host.is_empty()).then_some(host)
}

pub fn cluster_join_challenge_rpc_request<T: Serialize + ?Sized>(
    challenge: &T,
) -> Result<ObjectRpcRequest, NetworkError> {
    ObjectRpcRequest::encode_body(
        CLUSTER_JOIN_CHALLENGE_ROUTE,
        CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        challenge,
    )
}

fn cluster_kv_local_is_state_authority(state: &ClusterBootstrapState) -> bool {
    sinomo_kv::cluster_kv_raft_local_is_voter(state)
}

pub trait ClusterJoinRegistrationHost {
    fn local_daemon_membership_observation(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Result<Option<ClusterLocalDaemonMembershipObservation>, NetworkError> {
        Ok(None)
    }

    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError>;

    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        _state: &mut ClusterBootstrapState,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
        Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "join registration host cannot prove current authority for {}",
                paths.root.display()
            ),
        })
    }

    fn commit_join_registration_txn(
        &mut self,
        _paths: &ClusterPaths,
        _state: &mut ClusterBootstrapState,
        _request: KvTxnRequest,
    ) -> Result<ClusterKvTxnCommitResult, NetworkError> {
        Err(NetworkError::ClusterReadOnly {
            detail: "join registration host cannot commit fenced membership changes".to_string(),
        })
    }

    fn admit_cluster_raft_learner(
        &mut self,
        _paths: &ClusterPaths,
        _state: &mut ClusterBootstrapState,
        node_id: &NodeId,
        _now: OffsetDateTime,
    ) -> Result<KvSnapshot, NetworkError> {
        Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "join registration host cannot admit `{node_id}` through the Raft config lifecycle"
            ),
        })
    }

    fn remember_successful_join_targets(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        members: &[ClusterMemberRecord],
    ) -> Result<(), NetworkError> {
        for member in members {
            if member.node_id == state.local_node_id {
                continue;
            }
            for endpoint in &member.endpoints {
                if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly {
                    continue;
                }
                self.remember_successful_join_endpoint(paths, state, endpoint)?;
            }
        }
        Ok(())
    }

    fn remember_successful_join_endpoint(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _endpoint: &ClusterEndpointRecord,
    ) -> Result<(), NetworkError> {
        Ok(())
    }
}

impl<T> ClusterMembershipRpcForwardHost for T
where
    T: ClusterJoinRegistrationHost,
{
    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        ClusterJoinRegistrationHost::call_node_object_rpc(self, state, target_node_id, request)
    }
}

pub trait ClusterJoinRegistrationAsyncHost: ClusterSignedObjectRpcAsyncHost {
    fn local_daemon_membership_observation_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, Option<ClusterLocalDaemonMembershipObservation>> {
        Box::pin(async { Ok(None) })
    }

    fn authoritative_cluster_kv_snapshot_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, KvAuthoritativeRead<KvSnapshot>> {
        Box::pin(async move {
            let result = self
                .commit_join_registration_txn_async(
                    paths,
                    state,
                    KvTxnRequest::generated(KvTxn {
                        compares: Vec::new(),
                        success: Vec::new(),
                        failure: Vec::new(),
                    }),
                )
                .await?;
            if !matches!(
                result.authority.basis,
                sinomo_kv::KvAuthorityBasis::CurrentTermQuorum { .. }
            ) {
                return Err(NetworkError::ClusterReadOnly {
                    detail:
                        "join registration authoritative read lacked a current-term quorum barrier"
                            .to_string(),
                });
            }
            Ok(KvAuthoritativeRead {
                value: result.snapshot,
                authority: result.authority,
            })
        })
    }

    fn commit_join_registration_txn_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvTxnCommitResult>;

    fn admit_cluster_raft_learner_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
        node_id: &'a NodeId,
        _now: OffsetDateTime,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot> {
        Box::pin(async move {
            Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "join registration host cannot admit `{node_id}` through the Raft config lifecycle"
                ),
            })
        })
    }

    fn remember_successful_join_targets_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        members: &'a [ClusterMemberRecord],
    ) -> ClusterMembershipRpcFuture<'a, ()>
    where
        Self: Send,
    {
        Box::pin(async move {
            for member in members {
                if member.node_id == state.local_node_id {
                    continue;
                }
                for endpoint in &member.endpoints {
                    if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly {
                        continue;
                    }
                    self.remember_successful_join_endpoint_async(paths, state, endpoint)
                        .await?;
                }
            }
            Ok(())
        })
    }

    fn remember_successful_join_endpoint_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
        _endpoint: &'a ClusterEndpointRecord,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

pub trait ClusterLocalDaemonMembershipObserver {
    fn local_daemon_membership_observation(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Result<Option<ClusterLocalDaemonMembershipObservation>, NetworkError> {
        Ok(None)
    }
}

pub trait ClusterLocalDaemonMembershipAsyncObserver {
    fn local_daemon_membership_observation_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, Option<ClusterLocalDaemonMembershipObservation>> {
        Box::pin(async { Ok(None) })
    }
}

pub struct ClusterJoinRegistrationDriver<T, O> {
    driver: ClusterControlPlaneDriver<T>,
    observer: O,
}

impl<T, O> ClusterJoinRegistrationDriver<T, O> {
    pub fn new(driver: ClusterControlPlaneDriver<T>, observer: O) -> Self {
        Self { driver, observer }
    }

    pub fn with_transport(transport: T, observer: O) -> Self {
        Self::new(ClusterControlPlaneDriver::new(transport), observer)
    }

    pub fn driver_mut(&mut self) -> &mut ClusterControlPlaneDriver<T> {
        &mut self.driver
    }

    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }
}

impl<T, O> ClusterJoinRegistrationHost for ClusterJoinRegistrationDriver<T, O>
where
    T: ClusterControlPlaneTransport,
    O: ClusterLocalDaemonMembershipObserver,
{
    fn local_daemon_membership_observation(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<ClusterLocalDaemonMembershipObservation>, NetworkError> {
        self.observer
            .local_daemon_membership_observation(paths, state)
    }

    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        ClusterMembershipRpcForwardHost::call_node_object_rpc(
            &mut self.driver,
            state,
            target_node_id,
            request,
        )
    }

    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
        ClusterJoinRegistrationHost::authoritative_cluster_kv_snapshot(
            &mut self.driver,
            paths,
            state,
        )
    }

    fn commit_join_registration_txn(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<ClusterKvTxnCommitResult, NetworkError> {
        ClusterJoinRegistrationHost::commit_join_registration_txn(
            &mut self.driver,
            paths,
            state,
            request,
        )
    }

    fn admit_cluster_raft_learner(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        node_id: &NodeId,
        now: OffsetDateTime,
    ) -> Result<KvSnapshot, NetworkError> {
        ClusterJoinRegistrationHost::admit_cluster_raft_learner(
            &mut self.driver,
            paths,
            state,
            node_id,
            now,
        )
    }

    fn remember_successful_join_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<(), NetworkError> {
        ClusterJoinRegistrationHost::remember_successful_join_endpoint(
            &mut self.driver,
            paths,
            state,
            endpoint,
        )
    }
}

pub trait ClusterJoinReconciliationHost {
    fn send_join_registration(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterJoinRegistrationRequest,
        tcp_timeout: Duration,
        iroh_timeout: Duration,
    ) -> Result<ClusterJoinRegistrationResponse, NetworkError>;

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError>;

    fn wait_for_join_reconciliation_retry(&mut self, interval: Duration) {
        std::thread::sleep(interval);
    }

    fn observe_join_reconciliation_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _reconciliation: &ClusterJoinReconciliation,
    ) -> Result<(), NetworkError> {
        Ok(())
    }
}

impl<T> ClusterJoinRegistrationHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.transport.call_node_object_rpc(
            state,
            target_node_id,
            request,
            self.timeouts.membership_rpc_timeout,
        )
    }

    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
        ClusterMemberMutationHost::authoritative_cluster_kv_snapshot(self, paths, state)
    }

    fn commit_join_registration_txn(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<ClusterKvTxnCommitResult, NetworkError> {
        ClusterMemberMutationHost::commit_cluster_kv_txn_with_quorum(self, paths, state, request)
    }

    fn admit_cluster_raft_learner(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        node_id: &NodeId,
        now: OffsetDateTime,
    ) -> Result<KvSnapshot, NetworkError> {
        update_cluster_state_authority_with_host(
            paths,
            state.clone(),
            node_id.clone(),
            ClusterMemberRole::Learner,
            now,
            self,
        )?;
        ClusterMemberMutationHost::authoritative_cluster_kv_snapshot(self, paths, state)
            .map(|read| read.value)
    }

    fn remember_successful_join_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<(), NetworkError> {
        self.transport.record_endpoint_command_connectivity(
            paths,
            state,
            endpoint,
            ClusterConnectivityState::Healthy,
            None,
        );
        Ok(())
    }
}

impl<T> ClusterJoinReconciliationHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn send_join_registration(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterJoinRegistrationRequest,
        tcp_timeout: Duration,
        iroh_timeout: Duration,
    ) -> Result<ClusterJoinRegistrationResponse, NetworkError> {
        let timeout = match endpoint.transport {
            ClusterTransportKind::Iroh => iroh_timeout,
            _ => tcp_timeout,
        };
        send_cluster_join_registration_over_membership_rpc(
            endpoint,
            request,
            timeout,
            |endpoint, request, timeout| {
                self.transport
                    .call_endpoint_object_rpc(endpoint, request, timeout)
            },
        )
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError> {
        self.transport.fetch_cluster_kv_snapshot(endpoint, timeout)
    }

    fn observe_join_reconciliation_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        reconciliation: &ClusterJoinReconciliation,
    ) -> Result<(), NetworkError> {
        if let Some(endpoint) = reconciliation.accepted_endpoint.as_ref() {
            self.transport.record_endpoint_command_connectivity(
                paths,
                state,
                endpoint,
                ClusterConnectivityState::Healthy,
                None,
            );
        }
        Ok(())
    }
}
