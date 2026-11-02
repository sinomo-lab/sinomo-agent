use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
#[cfg(any(test, feature = "test-support"))]
use crate::invite_publish::read_cluster_invitation_for_join_introducer;
use crate::keyspace::{
    cluster_capability_policy_key, cluster_endpoint_prefix, cluster_identity_key,
    cluster_invitation_key, cluster_invitation_state_key, cluster_member_key,
};
use crate::kv_local_store::install_cluster_kv_anchored_snapshot;
use crate::kv_local_store::kv_err;
use crate::local_membership::{
    ClusterLocalDaemonMembershipObservation, apply_local_daemon_membership_observation,
    merge_cluster_join_registration,
};
use crate::local_state_store::{persist_accepted_cluster_join_state, read_cluster_state};
use crate::member_mutation::{
    ClusterMemberMutationHost, ClusterRaftLearnerAdmissionPreparation,
    cluster_raft_config_change_apply_txn, cluster_raft_config_change_plan_apply_txn,
    cluster_raft_config_change_record_txn, prepare_cluster_raft_learner_admission,
    update_cluster_state_authority_with_host,
};
use crate::member_store::{apply_cluster_raft_role, cluster_member_update_ops_from_store};
use crate::membership_forward::{
    ClusterMembershipRpcForwardHost, forward_cluster_membership_rpc_to_state_authority_with_host,
    forward_cluster_membership_value_to_state_authority_async_with_host,
};
use crate::membership_rpc::{
    ClusterJoinRaftAnchor, ClusterJoinRegistrationRequest, ClusterJoinRegistrationResponse,
};
use crate::prelude::*;
#[allow(unused_imports)]
use sinomo_kv::KvSnapshot;
use sinomo_kv::{
    CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES, ClusterKvRaftConfigChange,
    ClusterKvRaftConfigChangeEvidence, ClusterKvRaftConfigChangeKind,
    ClusterKvRaftConfigChangePhase, ClusterKvRaftConfigRecord, ClusterKvTxnCommitResult,
    InProcessKvStore, KvAuthoritativeRead, KvAuthorityProof, KvCommand, KvCompare, KvOp, KvTxn,
    KvTxnRequest,
};
use sinomo_network::ClusterConnectivityState;
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterObjectRpcAsyncHost;
use sinomo_network::ClusterPaths;
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
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

const JOIN_RECONCILIATION_POLL_INTERVAL: Duration = Duration::from_millis(25);
// A concurrent admission burst can legitimately put many requests on the same
// KV generation. One winner advances the generation per round, so a very small
// fixed cap rejects a still-valid tail request. Keep a generous safety cap
// while a short exponential delay prevents a hot retry loop. At most 191 ms is
// spent sleeping across all 16 attempts, leaving the enclosing RPC deadline
// for quorum I/O.
const CLUSTER_JOIN_ADMISSION_MAX_ATTEMPTS: usize = 16;
const CLUSTER_JOIN_ADMISSION_MAX_RETRY_BACKOFF: Duration = Duration::from_millis(16);

/// Serializes authority-local async join admission preparation while allowing
/// sequential and already-waiting requests to reuse the preceding admission's
/// current-term quorum result.
///
/// Without this boundary, a burst prepares every learner from the same Raft
/// config generation. All but one then spend a complete quorum round trip to
/// discover the expected compare failure, even though the winning commit
/// already returned the exact authoritative snapshot needed by the next
/// waiter. The coordinator is process-local because every authority RPC is
/// dispatched by one daemon process; the durable transaction fences and Raft
/// quorum remain the cross-process authority.
#[derive(Default)]
struct AsyncClusterJoinAdmissionCoordinator {
    turn: tokio::sync::Mutex<()>,
    completed_generation: AtomicU64,
    latest_authoritative: std::sync::Mutex<Option<KvAuthoritativeRead<KvSnapshot>>>,
}

const ASYNC_CLUSTER_JOIN_ADMISSION_COORDINATOR_CACHE_MAX_ENTRIES: usize = 64;

static ASYNC_CLUSTER_JOIN_ADMISSION_COORDINATORS: LazyLock<
    std::sync::Mutex<HashMap<PathBuf, Arc<AsyncClusterJoinAdmissionCoordinator>>>,
> = LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

fn async_cluster_join_admission_coordinator(
    paths: &ClusterPaths,
) -> Arc<AsyncClusterJoinAdmissionCoordinator> {
    let mut coordinators = ASYNC_CLUSTER_JOIN_ADMISSION_COORDINATORS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(coordinator) = coordinators.get(&paths.root) {
        return Arc::clone(coordinator);
    }
    // Retain the most recent authoritative snapshot between sequential join
    // waves. The old weak registry discarded it as soon as one admission
    // returned, forcing the next learner to reconstruct Raft authority from
    // disk and quorum even though the preceding commit already produced the
    // exact current-term snapshot. Bound inactive entries for test processes
    // that exercise many independent data directories; active admissions are
    // never evicted.
    if coordinators.len() >= ASYNC_CLUSTER_JOIN_ADMISSION_COORDINATOR_CACHE_MAX_ENTRIES {
        coordinators.retain(|_, coordinator| Arc::strong_count(coordinator) > 1);
    }
    let coordinator = Arc::new(AsyncClusterJoinAdmissionCoordinator::default());
    coordinators.insert(paths.root.clone(), Arc::clone(&coordinator));
    coordinator
}

impl AsyncClusterJoinAdmissionCoordinator {
    fn generation(&self) -> u64 {
        self.completed_generation.load(Ordering::Acquire)
    }

    fn latest_authoritative(&self) -> Option<KvAuthoritativeRead<KvSnapshot>> {
        self.latest_authoritative
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn publish(&self, authoritative: KvAuthoritativeRead<KvSnapshot>) {
        if !matches!(
            authoritative.authority.basis,
            sinomo_kv::KvAuthorityBasis::CurrentTermQuorum { .. }
        ) {
            return;
        }
        *self
            .latest_authoritative
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(authoritative);
        self.completed_generation.fetch_add(1, Ordering::Release);
    }
}

fn cluster_join_admission_retry_backoff(attempt: usize) -> Duration {
    Duration::from_millis(1_u64 << attempt.saturating_sub(1).min(4))
        .min(CLUSTER_JOIN_ADMISSION_MAX_RETRY_BACKOFF)
}

fn join_registration_trace(args: std::fmt::Arguments<'_>) {
    tracing::trace!(
        target: "sinomo.cluster.formation",
        detail = %args,
        "cluster join registration stage"
    );
    if cluster_join_stderr_trace_enabled() {
        eprintln!("[sinomo cluster join] {args}");
    }
}

fn cluster_join_stderr_trace_enabled() -> bool {
    std::env::var("SINOMO_CLUSTER_JOIN_TRACE")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
}

#[allow(clippy::too_many_arguments)]
fn join_registration_authority_phase_trace(
    phase: &'static str,
    invite_id: &ClusterInviteId,
    requested_learner_node_id: Option<&NodeId>,
    attempt: usize,
    duration: Duration,
    revision: u64,
    term: u64,
    outcome: &'static str,
    detail: std::fmt::Arguments<'_>,
) {
    let requested_learner_node_id = requested_learner_node_id.map(NodeId::as_str).unwrap_or("");
    let duration_us = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
    tracing::trace!(
        target: "sinomo.cluster.formation",
        phase,
        invite_id = %invite_id,
        requested_learner_node_id,
        attempt,
        duration_us,
        revision,
        term,
        outcome,
        detail = %detail,
        "cluster join authority registration phase"
    );
    if cluster_join_stderr_trace_enabled() {
        eprintln!(
            "[sinomo cluster join] authority phase={phase} invite={invite_id} learner={requested_learner_node_id} attempt={attempt} duration_us={duration_us} revision={revision} term={term} outcome={outcome}: {detail}"
        );
    }
}

#[derive(Clone)]
struct ForwardedJoinAcceptanceTrace {
    invite_id: ClusterInviteId,
    requested_learner_node_id: Option<NodeId>,
    stderr_enabled: bool,
}

impl ForwardedJoinAcceptanceTrace {
    fn new(
        invite_id: &ClusterInviteId,
        requested_learner_node_id: Option<&NodeId>,
    ) -> Option<Self> {
        let stderr_enabled = cluster_join_stderr_trace_enabled();
        if !stderr_enabled
            && !tracing::enabled!(target: "sinomo.cluster.formation", tracing::Level::TRACE)
        {
            return None;
        }
        Some(Self {
            invite_id: invite_id.clone(),
            requested_learner_node_id: requested_learner_node_id.cloned(),
            stderr_enabled,
        })
    }

    fn phase(
        &self,
        phase: &'static str,
        duration: Duration,
        revision: u64,
        term: u64,
        outcome: &'static str,
        detail: std::fmt::Arguments<'_>,
    ) {
        let requested_learner_node_id = self
            .requested_learner_node_id
            .as_ref()
            .map(NodeId::as_str)
            .unwrap_or("");
        let duration_us = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        tracing::trace!(
            target: "sinomo.cluster.formation",
            phase,
            invite_id = %self.invite_id,
            requested_learner_node_id,
            duration_us,
            revision,
            term,
            outcome,
            detail = %detail,
            "forwarded join acceptance phase"
        );
        if self.stderr_enabled {
            eprintln!(
                "[sinomo cluster join] forwarded acceptance phase={phase} invite={} learner={requested_learner_node_id} duration_us={duration_us} revision={revision} term={term} outcome={outcome}: {detail}",
                self.invite_id
            );
        }
    }

    fn start(
        &self,
        phase: &'static str,
        revision: u64,
        term: u64,
        detail: std::fmt::Arguments<'_>,
    ) -> Instant {
        self.phase(phase, Duration::ZERO, revision, term, "started", detail);
        // Exclude observation subscriber work from the measured operation.
        Instant::now()
    }
}

fn requested_join_learner_node_id(request: &ClusterJoinRegistrationRequest) -> Option<&NodeId> {
    request
        .members
        .iter()
        .find(|member| member.role == ClusterMemberRole::Learner)
        .map(|member| &member.node_id)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterJoinReconciliation {
    pub accepted_endpoint: Option<ClusterEndpointRecord>,
    pub kv_snapshot: Option<KvSnapshot>,
    pub raft_anchor: Option<ClusterJoinRaftAnchor>,
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
            raft_anchor: None,
        });
    }
    if let Err(error) = crate::verify_cluster_join_invite_signature(&persisted_invite) {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!("invite signature rejected: {error}"),
            state,
            kv_snapshot: None,
            responder_node_id: None,
            raft_anchor: None,
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
            raft_anchor: None,
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
            raft_anchor: None,
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
            raft_anchor: None,
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
            raft_anchor: None,
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
        raft_anchor: None,
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
        raft_anchor: None,
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
    // The ordinary `sinomo-join://` URI is deliberately a compact reference:
    // it carries the invitation id, cluster authentication key, secret, and
    // dialable channels, while policy, issuer, expiry, and signature remain in
    // authoritative KV. Authenticate that reference, then enforce the full
    // persisted invite below. A caller that supplies a full signed invite must
    // still present the exact authoritative value.
    let compact_reference_matches = request.invite.invite_id == persisted_invite.invite_id
        && request.invite.cluster_public_key == persisted_invite.cluster_public_key
        && request.invite.invitation_secret == persisted_invite.invitation_secret;
    let embedded_signed_invite_matches = !crate::invite_has_embedded_signature(&request.invite)
        || request.invite == persisted_invite;
    if !compact_reference_matches
        || !embedded_signed_invite_matches
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
        (member.cluster_id != persisted_invite.cluster_id
            && member.cluster_id.as_str() != crate::PENDING_INVITE_CLUSTER_ID)
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

    // The joiner routes to its acceptor from the committed snapshot. Commit
    // this node's current listener advertisements with the admission instead
    // of relying on a later maintenance publish, which a single-node cluster
    // never runs. Unchanged listeners add nothing.
    let publication =
        crate::member_publish::local_member_endpoint_publication_txn(&state, snapshot)?;
    for compare in publication.compares {
        let key = cluster_join_registration_compare_key(&compare).to_owned();
        if compared_keys.insert(key) {
            compares.push(compare);
        }
    }
    for op in publication.success {
        let Some(key) = cluster_join_registration_op_key(&op).map(str::to_owned) else {
            return Err(NetworkError::InvalidClusterCommand {
                detail: "acceptor endpoint publication produced a non-member KV operation"
                    .to_string(),
            });
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

fn cluster_join_registration_op_key(op: &KvOp) -> Option<&str> {
    match op {
        KvOp::Put { key, .. } | KvOp::Delete { key } => Some(key),
        KvOp::DeletePrefix { .. }
        | KvOp::LeaseGrant { .. }
        | KvOp::LeaseKeepAlive { .. }
        | KvOp::LeaseRevoke { .. } => None,
    }
}

fn cluster_join_registration_compare_key(compare: &KvCompare) -> &str {
    match compare {
        KvCompare::Exists { key }
        | KvCompare::Missing { key }
        | KvCompare::ValueEquals { key, .. }
        | KvCompare::VersionEquals { key, .. }
        | KvCompare::CreateRevisionEquals { key, .. }
        | KvCompare::ModifyRevisionEquals { key, .. }
        | KvCompare::LeaseEquals { key, .. } => key,
    }
}

/// Combine member seeding and Raft learner admission into one authoritative
/// transaction. All compares run against the pre-join snapshot; the local
/// staged store exists only to derive the final member, endpoint-index, config,
/// and planned lifecycle values that the admission commit publishes atomically.
/// The ownership edge terminalizes that lifecycle immediately afterward, once
/// the admission result supplies its exact committed term, index, and revision.
struct ClusterJoinLearnerAdmissionTxn {
    txn: KvTxn,
    lifecycle_change_id: Option<sinomo_base::Id>,
}

fn pending_cluster_join_learner_admission_change_id(
    store: &InProcessKvStore,
    learner_node_id: &NodeId,
) -> Result<Option<sinomo_base::Id>, NetworkError> {
    let mut matching_change_id = None;
    for entry in store.prefix_entries("/cluster/raft/config-changes/") {
        let change: ClusterKvRaftConfigChange = entry.decode().map_err(kv_err)?;
        if change.node_id != *learner_node_id
            || matches!(
                change.phase,
                ClusterKvRaftConfigChangePhase::Committed | ClusterKvRaftConfigChangePhase::Failed
            )
        {
            continue;
        }
        if change.kind != ClusterKvRaftConfigChangeKind::AddLearner
            || change.phase != ClusterKvRaftConfigChangePhase::Planned
        {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "cluster member `{learner_node_id}` has incompatible in-flight Raft config change `{}`",
                    change.change_id
                ),
            });
        }
        if matching_change_id.replace(change.change_id).is_some() {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "cluster member `{learner_node_id}` has multiple in-flight Raft learner admissions"
                ),
            });
        }
    }
    Ok(matching_change_id)
}

fn cluster_join_seed_and_learner_admission_txn(
    snapshot: &KvSnapshot,
    seed_txn: &KvTxn,
    learner_node_id: &NodeId,
    term: u64,
    now: OffsetDateTime,
) -> Result<ClusterJoinLearnerAdmissionTxn, NetworkError> {
    let mut staged = InProcessKvStore::restore(snapshot.clone()).map_err(kv_err)?;
    let (_, seed_response) = staged
        .apply_command_with_response(KvCommand::Txn(seed_txn.clone()))
        .map_err(kv_err)?;
    if !seed_response.is_some_and(|response| response.succeeded) {
        return Err(NetworkError::InvalidClusterCommand {
            detail: "locally staged join seed transaction did not satisfy its own fences"
                .to_string(),
        });
    }
    let staged_snapshot = staged.try_snapshot().map_err(kv_err)?;
    let (store, change, plan_required) =
        match prepare_cluster_raft_learner_admission(staged_snapshot, learner_node_id, now)? {
            ClusterRaftLearnerAdmissionPreparation::AlreadyAdmitted(snapshot) => {
                // An exact retry after the apply commit but before lifecycle
                // completion must finish that same record. A retry after full
                // completion has no lifecycle work and only rechecks the
                // invitation/member fences.
                let store = InProcessKvStore::restore(snapshot).map_err(kv_err)?;
                return Ok(ClusterJoinLearnerAdmissionTxn {
                    txn: seed_txn.clone(),
                    lifecycle_change_id: pending_cluster_join_learner_admission_change_id(
                        &store,
                        learner_node_id,
                    )?,
                });
            }
            ClusterRaftLearnerAdmissionPreparation::Pending {
                store,
                change,
                plan_required,
            } => (store, change, plan_required),
        };
    let lifecycle_change_id = change.change_id.clone();
    let next_revision = snapshot.revision.next();
    let apply_txn = if plan_required {
        cluster_raft_config_change_plan_apply_txn(
            &store,
            learner_node_id,
            &change,
            term,
            next_revision,
            now,
        )?
    } else {
        cluster_raft_config_change_apply_txn(
            &store,
            learner_node_id,
            &change,
            term,
            next_revision,
            now,
        )?
    };

    // Seed compares are the authoritative pre-transaction fences. In
    // particular, they assert that a fresh member key is Missing, while the
    // staged apply builder sees the seeded value and would otherwise compare it
    // as already present. Add only lifecycle/config fences for keys not already
    // covered by the security-sensitive seed transaction.
    let mut compares = seed_txn.compares.clone();
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
    for compare in apply_txn.compares {
        let key = match &compare {
            KvCompare::Exists { key }
            | KvCompare::Missing { key }
            | KvCompare::ValueEquals { key, .. }
            | KvCompare::VersionEquals { key, .. }
            | KvCompare::CreateRevisionEquals { key, .. }
            | KvCompare::ModifyRevisionEquals { key, .. }
            | KvCompare::LeaseEquals { key, .. } => key,
        };
        if compared_keys.insert(key.clone()) {
            compares.push(compare);
        }
    }

    // The final learner apply overwrites the main member and its endpoint-index
    // entries. Drop their intermediate seed puts while retaining a requested
    // non-voting shadow's disjoint seed operations.
    let apply_keys = apply_txn
        .success
        .iter()
        .filter_map(cluster_join_registration_op_key)
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let mut success = seed_txn
        .success
        .iter()
        .filter(|op| {
            cluster_join_registration_op_key(op).is_none_or(|key| !apply_keys.contains(key))
        })
        .cloned()
        .collect::<Vec<_>>();
    success.extend(apply_txn.success);
    Ok(ClusterJoinLearnerAdmissionTxn {
        txn: KvTxn {
            compares,
            success,
            failure: Vec::new(),
        },
        lifecycle_change_id: Some(lifecycle_change_id),
    })
}

enum AuthoritativeClusterJoinAdmissionPreparation {
    Rejected(ClusterJoinRegistrationResponse),
    Commit {
        prepared: PreparedAuthoritativeClusterJoin,
        txn: KvTxn,
        lifecycle_change_id: Option<sinomo_base::Id>,
    },
}

fn require_current_term_join_admission_authority(
    authoritative: &KvAuthoritativeRead<KvSnapshot>,
) -> Result<(), NetworkError> {
    if !matches!(
        authoritative.authority.basis,
        sinomo_kv::KvAuthorityBasis::CurrentTermQuorum { .. }
    ) {
        return Err(NetworkError::ClusterReadOnly {
            detail: "join admission requires a current-term quorum authority proof".to_string(),
        });
    }
    Ok(())
}

fn prepare_authoritative_cluster_join_admission(
    state: ClusterBootstrapState,
    snapshot: KvSnapshot,
    request: ClusterJoinRegistrationRequest,
    term: u64,
    now: OffsetDateTime,
) -> Result<AuthoritativeClusterJoinAdmissionPreparation, NetworkError> {
    let prepared =
        match prepare_authoritative_cluster_join_registration(state, &snapshot, request, now)? {
            AuthoritativeClusterJoinPreparation::Rejected(response) => {
                return Ok(AuthoritativeClusterJoinAdmissionPreparation::Rejected(
                    response,
                ));
            }
            AuthoritativeClusterJoinPreparation::Commit(prepared) => prepared,
        };
    let [learner_node_id] = prepared.learner_node_ids.as_slice() else {
        return Err(NetworkError::InvalidClusterCommand {
            detail: "validated join registration did not contain exactly one learner".to_string(),
        });
    };
    let admission = cluster_join_seed_and_learner_admission_txn(
        &snapshot,
        &prepared.seed_txn,
        learner_node_id,
        term,
        now,
    )?;
    Ok(AuthoritativeClusterJoinAdmissionPreparation::Commit {
        prepared,
        txn: admission.txn,
        lifecycle_change_id: admission.lifecycle_change_id,
    })
}

/// Build the terminal lifecycle record from the exact proof returned by the
/// admission commit. This cannot safely be folded into the admission
/// transaction: its final Raft log index and KV revision are unknowable until
/// quorum commit, especially when unrelated writers can advance the log.
fn cluster_join_learner_admission_completion_txn(
    snapshot: &KvSnapshot,
    lifecycle_change_id: &sinomo_base::Id,
    learner_node_id: &NodeId,
    authority: &KvAuthorityProof,
    now: OffsetDateTime,
) -> Result<Option<KvTxn>, NetworkError> {
    if authority.revision != snapshot.revision {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "join admission authority revision {:?} does not match committed snapshot revision {:?}",
                authority.revision, snapshot.revision
            ),
        });
    }
    let store = InProcessKvStore::restore(snapshot.clone()).map_err(kv_err)?;
    let change_key = sinomo_kv::cluster_kv_raft_config_change_key(lifecycle_change_id);
    let mut change: ClusterKvRaftConfigChange = store
        .get(&change_key)
        .ok_or_else(|| NetworkError::ClusterReadOnly {
            detail: format!(
                "committed join admission is missing Raft config lifecycle `{lifecycle_change_id}`"
            ),
        })?
        .decode()
        .map_err(kv_err)?;
    if change.node_id != *learner_node_id
        || change.kind != ClusterKvRaftConfigChangeKind::AddLearner
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "Raft config lifecycle `{lifecycle_change_id}` does not own learner admission for `{learner_node_id}`"
            ),
        });
    }
    match change.phase {
        ClusterKvRaftConfigChangePhase::Committed => return Ok(None),
        ClusterKvRaftConfigChangePhase::Planned => {}
        ClusterKvRaftConfigChangePhase::CatchingUp
        | ClusterKvRaftConfigChangePhase::ReadyToCommit
        | ClusterKvRaftConfigChangePhase::Failed => {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "Raft config lifecycle `{lifecycle_change_id}` has incompatible phase {:?} after join admission",
                    change.phase
                ),
            });
        }
    }
    let member: ClusterMemberRecord = store
        .get(&cluster_member_key(learner_node_id))
        .ok_or_else(|| NetworkError::ClusterReadOnly {
            detail: format!(
                "committed join admission is missing learner member `{learner_node_id}`"
            ),
        })?
        .decode()
        .map_err(kv_err)?;
    let config: ClusterKvRaftConfigRecord = store
        .get(&sinomo_kv::cluster_kv_raft_config_key())
        .ok_or_else(|| NetworkError::ClusterReadOnly {
            detail: "committed join admission is missing the Raft config".to_string(),
        })?
        .decode()
        .map_err(kv_err)?;
    if member.role != ClusterMemberRole::Learner
        || member.state != ClusterMemberState::Learner
        || sinomo_kv::cluster_kv_raft_config_member_role(&config, learner_node_id)
            != Some(ClusterMemberRole::Learner)
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "Raft config lifecycle `{lifecycle_change_id}` cannot finish before learner `{learner_node_id}` is atomically applied"
            ),
        });
    }
    sinomo_kv::finish_cluster_kv_raft_config_change(
        &mut change,
        true,
        ClusterKvRaftConfigChangeEvidence {
            detail: format!("join admission committed AddLearner for node {learner_node_id}"),
            term: authority.term,
            log_index: authority.commit_index,
            revision: authority.revision,
            observed_at: now,
        },
    )
    .map_err(kv_err)?;
    cluster_raft_config_change_record_txn(&store, &change).map(Some)
}

fn cluster_join_learner_admission_lifecycle_is_committed(
    snapshot: &KvSnapshot,
    lifecycle_change_id: &sinomo_base::Id,
    learner_node_id: &NodeId,
) -> Result<bool, NetworkError> {
    let Some(entry) = snapshot.get(&sinomo_kv::cluster_kv_raft_config_change_key(
        lifecycle_change_id,
    )) else {
        return Ok(false);
    };
    let change: ClusterKvRaftConfigChange = entry.decode().map_err(kv_err)?;
    Ok(change.node_id == *learner_node_id
        && change.kind == ClusterKvRaftConfigChangeKind::AddLearner
        && change.phase == ClusterKvRaftConfigChangePhase::Committed)
}

/// The authority's applied state anchored at its Raft position, which a
/// joiner can install as its initial Raft state.
fn cluster_join_response_raft_snapshot(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Option<sinomo_kv::RaftSnapshot> {
    let wal_paths = crate::ClusterKvPathsExt::kv_paths(paths).raft_wal_paths(node_id);
    let deadline = Instant::now() + sinomo_network::DEFAULT_NETWORK_TIMEOUT;
    match sinomo_kv::read_cluster_kv_raft_anchored_snapshot_before(&wal_paths, node_id, deadline)? {
        Ok(snapshot) => Some(snapshot),
        Err(error) => {
            join_registration_trace(format_args!(
                "join response Raft anchor unavailable: {error}"
            ));
            None
        }
    }
}

/// Answers with the anchored snapshot when it covers the admission. A voter
/// that has not applied the admission yet answers with the admitted snapshot
/// alone, and the joiner's Raft state then starts empty.
fn anchored_cluster_join_response_snapshot(
    admitted: KvSnapshot,
    anchored: Option<sinomo_kv::RaftSnapshot>,
) -> (KvSnapshot, Option<ClusterJoinRaftAnchor>) {
    match anchored {
        Some(anchored) if anchored.kv.revision >= admitted.revision => {
            let anchor = ClusterJoinRaftAnchor {
                last_included_index: anchored.last_included_index,
                last_included_term: anchored.last_included_term,
            };
            (Arc::unwrap_or_clone(anchored.kv), Some(anchor))
        }
        _ => (admitted, None),
    }
}

fn finish_cluster_join_learner_admission_with_host(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    admitted: ClusterKvTxnCommitResult,
    lifecycle_change_id: Option<&sinomo_base::Id>,
    learner_node_id: &NodeId,
    now: OffsetDateTime,
    host: &mut impl ClusterJoinRegistrationHost,
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    let Some(lifecycle_change_id) = lifecycle_change_id else {
        return Ok(admitted);
    };
    let Some(txn) = cluster_join_learner_admission_completion_txn(
        &admitted.snapshot,
        lifecycle_change_id,
        learner_node_id,
        &admitted.authority,
        now,
    )?
    else {
        return Ok(admitted);
    };
    let completed =
        host.commit_join_registration_txn(paths, state, KvTxnRequest::generated(txn))?;
    if completed
        .txn_response
        .as_ref()
        .is_some_and(|response| response.succeeded)
        || cluster_join_learner_admission_lifecycle_is_committed(
            &completed.snapshot,
            lifecycle_change_id,
            learner_node_id,
        )?
    {
        return Ok(completed);
    }
    Err(NetworkError::InvalidClusterCommand {
        detail: format!(
            "cluster KV compare failed while finishing join learner lifecycle `{lifecycle_change_id}`"
        ),
    })
}

async fn finish_cluster_join_learner_admission_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    admitted: ClusterKvTxnCommitResult,
    lifecycle_change_id: Option<&sinomo_base::Id>,
    learner_node_id: &NodeId,
    now: OffsetDateTime,
    host: &mut (impl ClusterJoinRegistrationAsyncHost + Send),
) -> Result<ClusterKvTxnCommitResult, NetworkError> {
    let Some(lifecycle_change_id) = lifecycle_change_id else {
        return Ok(admitted);
    };
    let completion_snapshot = admitted.snapshot.clone();
    let completion_change_id = lifecycle_change_id.clone();
    let completion_node_id = learner_node_id.clone();
    let completion_authority = admitted.authority;
    let txn = runtime
        .spawn_blocking(move || {
            cluster_join_learner_admission_completion_txn(
                &completion_snapshot,
                &completion_change_id,
                &completion_node_id,
                &completion_authority,
                now,
            )
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "finish cluster join learner admission".to_string(),
            detail: format!("join learner lifecycle preparation task failed: {source}"),
        })??;
    let Some(txn) = txn else {
        return Ok(admitted);
    };
    let completed = host
        .commit_join_registration_txn_async(paths, state, KvTxnRequest::generated(txn))
        .await?;
    if completed
        .txn_response
        .as_ref()
        .is_some_and(|response| response.succeeded)
        || cluster_join_learner_admission_lifecycle_is_committed(
            &completed.snapshot,
            lifecycle_change_id,
            learner_node_id,
        )?
    {
        return Ok(completed);
    }
    Err(NetworkError::InvalidClusterCommand {
        detail: format!(
            "cluster KV compare failed while finishing join learner lifecycle `{lifecycle_change_id}`"
        ),
    })
}

fn cluster_join_response_state_from_snapshot(
    mut state: ClusterBootstrapState,
    snapshot: &KvSnapshot,
    now: OffsetDateTime,
) -> Result<ClusterBootstrapState, NetworkError> {
    let identity: ClusterIdentityRecord = snapshot
        .get(&cluster_identity_key())
        .ok_or_else(|| NetworkError::InvalidClusterCommand {
            detail: "authoritative join snapshot is missing cluster identity".to_string(),
        })?
        .decode()
        .map_err(kv_err)?;
    if identity.cluster_id != state.cluster_id {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!(
                "authoritative join snapshot cluster `{}` does not match local cluster `{}`",
                identity.cluster_id, state.cluster_id
            ),
        });
    }
    // The name may have changed concurrently with join preparation. The
    // committed identity, not the pre-transaction local projection, is the
    // authority for the response generation.
    state.cluster_name = identity.cluster_name;
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

    let requested_learner_node_id = requested_join_learner_node_id(&request);
    let authority_open_started = Instant::now();
    join_registration_authority_phase_trace(
        "authority_snapshot_open",
        &request.invite.invite_id,
        requested_learner_node_id,
        0,
        Duration::ZERO,
        state.kv_revision,
        state.term,
        "started",
        format_args!("opening authoritative cluster KV snapshot"),
    );
    let mut authority_state = state.clone();
    let authoritative_result = host.authoritative_cluster_kv_snapshot(paths, &mut authority_state);
    match &authoritative_result {
        Ok(authoritative) => join_registration_authority_phase_trace(
            "authority_snapshot_open",
            &request.invite.invite_id,
            requested_learner_node_id,
            0,
            authority_open_started.elapsed(),
            authoritative.value.revision.0,
            authoritative.authority.term,
            "opened",
            format_args!("authoritative cluster KV snapshot opened"),
        ),
        Err(error) => join_registration_authority_phase_trace(
            "authority_snapshot_open",
            &request.invite.invite_id,
            requested_learner_node_id,
            0,
            authority_open_started.elapsed(),
            state.kv_revision,
            state.term,
            "error",
            format_args!("{error}"),
        ),
    }
    let mut authoritative = authoritative_result?;
    let mut commit_state = state.clone();
    let mut admission_attempt = 0usize;
    let (prepared, lifecycle_change_id, admitted) = loop {
        require_current_term_join_admission_authority(&authoritative)?;
        admission_attempt += 1;
        let prepare_revision = authoritative.value.revision.0;
        let prepare_term = authoritative.authority.term;
        let prepare_started = Instant::now();
        join_registration_authority_phase_trace(
            "admission_prepare",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            Duration::ZERO,
            prepare_revision,
            prepare_term,
            "started",
            format_args!("preparing fenced learner admission"),
        );
        let preparation_result = prepare_authoritative_cluster_join_admission(
            state.clone(),
            authoritative.value,
            request.clone(),
            prepare_term,
            now,
        );
        match &preparation_result {
            Ok(AuthoritativeClusterJoinAdmissionPreparation::Rejected(response)) => {
                join_registration_authority_phase_trace(
                    "admission_prepare",
                    &request.invite.invite_id,
                    requested_learner_node_id,
                    admission_attempt,
                    prepare_started.elapsed(),
                    prepare_revision,
                    prepare_term,
                    "rejected",
                    format_args!("{}", response.detail),
                );
            }
            Ok(AuthoritativeClusterJoinAdmissionPreparation::Commit { .. }) => {
                join_registration_authority_phase_trace(
                    "admission_prepare",
                    &request.invite.invite_id,
                    requested_learner_node_id,
                    admission_attempt,
                    prepare_started.elapsed(),
                    prepare_revision,
                    prepare_term,
                    "prepared",
                    format_args!("fenced learner admission prepared"),
                );
            }
            Err(error) => join_registration_authority_phase_trace(
                "admission_prepare",
                &request.invite.invite_id,
                requested_learner_node_id,
                admission_attempt,
                prepare_started.elapsed(),
                prepare_revision,
                prepare_term,
                "error",
                format_args!("{error}"),
            ),
        }
        let preparation = preparation_result?;
        let (prepared, txn, lifecycle_change_id) = match preparation {
            AuthoritativeClusterJoinAdmissionPreparation::Rejected(response) => {
                return Ok(response);
            }
            AuthoritativeClusterJoinAdmissionPreparation::Commit {
                prepared,
                txn,
                lifecycle_change_id,
            } => (prepared, txn, lifecycle_change_id),
        };
        let commit_request = KvTxnRequest::generated(txn);
        let commit_request_id = commit_request.request_id.clone();
        let commit_started = Instant::now();
        join_registration_authority_phase_trace(
            "quorum_commit",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            Duration::ZERO,
            prepare_revision,
            prepare_term,
            "started",
            format_args!(
                "committing fenced learner admission through quorum request_id={commit_request_id}"
            ),
        );
        let admitted_result =
            host.commit_join_registration_txn(paths, &mut commit_state, commit_request);
        match &admitted_result {
            Ok(admitted) => {
                let outcome = if admitted
                    .txn_response
                    .as_ref()
                    .is_some_and(|response| response.succeeded)
                {
                    "committed"
                } else {
                    "compare_raced"
                };
                join_registration_authority_phase_trace(
                    "quorum_commit",
                    &request.invite.invite_id,
                    requested_learner_node_id,
                    admission_attempt,
                    commit_started.elapsed(),
                    admitted.snapshot.revision.0,
                    admitted.authority.term,
                    outcome,
                    format_args!("quorum commit attempt completed request_id={commit_request_id}"),
                );
            }
            Err(error) => join_registration_authority_phase_trace(
                "quorum_commit",
                &request.invite.invite_id,
                requested_learner_node_id,
                admission_attempt,
                commit_started.elapsed(),
                prepare_revision,
                prepare_term,
                "error",
                format_args!("request_id={commit_request_id}: {error}"),
            ),
        }
        let admitted = admitted_result?;
        if admitted
            .txn_response
            .as_ref()
            .is_some_and(|response| response.succeeded)
        {
            break (prepared, lifecycle_change_id, admitted);
        }
        // A fresh quorum result can lend its snapshot to the next attempt.
        // An exact committed replay needs a separate authority read below.
        // Invite, issuer, membership and config checks run again for every
        // re-preparation, within the original admission-attempt bound.
        if admission_attempt >= CLUSTER_JOIN_ADMISSION_MAX_ATTEMPTS {
            return Ok(ClusterJoinRegistrationResponse {
                accepted: false,
                detail: "cluster invitation or membership changed during join admission"
                    .to_string(),
                state: prepared.state,
                kv_snapshot: None,
                responder_node_id: None,
                raft_anchor: None,
            });
        }
        // A replay proves this exact failed compare, not authority for a new
        // admission. Obtain a distinct current-term cut only for a marked,
        // known failed replay; malformed proof combinations still reach the
        // unchanged authority guard and fail closed.
        authoritative = if admitted.replayed_committed_request
            && matches!(
                admitted.authority.basis,
                sinomo_kv::KvAuthorityBasis::ExactCommittedRequest
            )
            && admitted
                .txn_response
                .as_ref()
                .is_some_and(|response| !response.succeeded)
        {
            host.authoritative_cluster_kv_snapshot(paths, &mut commit_state)?
        } else {
            KvAuthoritativeRead {
                value: admitted.snapshot,
                authority: admitted.authority,
            }
        };
        let retry_backoff = cluster_join_admission_retry_backoff(admission_attempt);
        join_registration_trace(format_args!(
            "atomic join admission compare raced with another cluster mutation; retrying after {retry_backoff:?}"
        ));
        std::thread::sleep(retry_backoff);
    };
    let learner_node_id = prepared
        .learner_node_ids
        .first()
        .expect("validated join admission has one learner")
        .clone();
    let lifecycle_started = Instant::now();
    let admitted = finish_cluster_join_learner_admission_with_host(
        paths,
        &mut commit_state,
        admitted,
        lifecycle_change_id.as_ref(),
        &learner_node_id,
        now,
        host,
    )?;
    join_registration_trace(format_args!(
        "join learner lifecycle completion node={learner_node_id} change={lifecycle_change_id:?} completed in {:?}",
        lifecycle_started.elapsed()
    ));
    let (snapshot, raft_anchor) = anchored_cluster_join_response_snapshot(
        admitted.snapshot,
        cluster_join_response_raft_snapshot(paths, &commit_state.local_node_id),
    );
    let response_revision = snapshot.revision.0;
    let response_term = admitted.authority.term;
    let projection_started = Instant::now();
    join_registration_authority_phase_trace(
        "response_projection",
        &request.invite.invite_id,
        requested_learner_node_id,
        admission_attempt,
        Duration::ZERO,
        response_revision,
        response_term,
        "started",
        format_args!("projecting accepted response from committed snapshot"),
    );
    let response_state_result =
        cluster_join_response_state_from_snapshot(prepared.state, &snapshot, now);
    match &response_state_result {
        Ok(response_state) => join_registration_authority_phase_trace(
            "response_projection",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            projection_started.elapsed(),
            response_state.kv_revision,
            response_state.term,
            "projected",
            format_args!("accepted response projection completed"),
        ),
        Err(error) => join_registration_authority_phase_trace(
            "response_projection",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            projection_started.elapsed(),
            response_revision,
            response_term,
            "error",
            format_args!("{error}"),
        ),
    }
    let response_state = response_state_result?;
    let persistence_started = Instant::now();
    join_registration_authority_phase_trace(
        "local_persistence",
        &request.invite.invite_id,
        requested_learner_node_id,
        admission_attempt,
        Duration::ZERO,
        response_state.kv_revision,
        response_state.term,
        "started",
        format_args!("persisting accepted response projection locally"),
    );
    let persistence_result = persist_accepted_cluster_join_state(paths, response_state.clone());
    match &persistence_result {
        Ok(crate::local_state_store::AcceptedClusterJoinStatePersistence::Applied {
            persisted_revision,
            ..
        }) => join_registration_authority_phase_trace(
            "local_persistence",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            persistence_started.elapsed(),
            *persisted_revision,
            response_state.term,
            "applied",
            format_args!("accepted response projection persisted locally"),
        ),
        Ok(crate::local_state_store::AcceptedClusterJoinStatePersistence::Superseded {
            persisted_revision,
            persisted_term,
            ..
        }) => join_registration_authority_phase_trace(
            "local_persistence",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            persistence_started.elapsed(),
            *persisted_revision,
            *persisted_term,
            "superseded",
            format_args!("a newer accepted response projection was already persisted"),
        ),
        Err(error) => join_registration_authority_phase_trace(
            "local_persistence",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            persistence_started.elapsed(),
            response_state.kv_revision,
            response_state.term,
            "error",
            format_args!("{error}"),
        ),
    }
    persistence_result?;
    if join_registration_should_remember_successful_targets(&response_state, &request.invite) {
        let handoff_started = Instant::now();
        join_registration_authority_phase_trace(
            "successful_target_handoff",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            Duration::ZERO,
            response_state.kv_revision,
            response_state.term,
            "started",
            format_args!("handing successful targets to connectivity tracking"),
        );
        let handoff_result =
            host.remember_successful_join_targets(paths, &response_state, &request.members);
        match &handoff_result {
            Ok(()) => join_registration_authority_phase_trace(
                "successful_target_handoff",
                &request.invite.invite_id,
                requested_learner_node_id,
                admission_attempt,
                handoff_started.elapsed(),
                response_state.kv_revision,
                response_state.term,
                "completed",
                format_args!("successful targets handed to connectivity tracking"),
            ),
            Err(error) => join_registration_authority_phase_trace(
                "successful_target_handoff",
                &request.invite.invite_id,
                requested_learner_node_id,
                admission_attempt,
                handoff_started.elapsed(),
                response_state.kv_revision,
                response_state.term,
                "error",
                format_args!("{error}"),
            ),
        }
        handoff_result?;
    } else {
        join_registration_authority_phase_trace(
            "successful_target_handoff",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            Duration::ZERO,
            response_state.kv_revision,
            response_state.term,
            "skipped",
            format_args!("accepted invite did not require a successful-target handoff"),
        );
    }
    Ok(ClusterJoinRegistrationResponse {
        accepted: true,
        detail: "join registered".to_string(),
        state: response_state,
        kv_snapshot: Some(snapshot),
        responder_node_id: None,
        raft_anchor,
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
        let rpc_head = cluster_join_registration_rpc_head();
        let forward_started = Instant::now();
        join_registration_trace(format_args!(
            "registration forward invite={} started with {} member(s)",
            request.invite.invite_id,
            request.members.len()
        ));
        let response: ClusterJoinRegistrationResponse =
            forward_cluster_membership_value_to_state_authority_async_with_host(
            runtime,
            paths,
            &state,
            &rpc_head,
            &request,
            "join introducer is not a state authority and could not forward the join to any state authority",
            host,
        )
        .await?;
        join_registration_trace(format_args!(
            "registration forward invite={} rpc returned in {:?}",
            request.invite.invite_id,
            forward_started.elapsed()
        ));
        join_registration_trace(format_args!(
            "registration forward invite={} stream decode completed in {:?}",
            request.invite.invite_id,
            forward_started.elapsed()
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
                &request.invite.invite_id,
                requested_join_learner_node_id(&request),
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

    let requested_learner_node_id = requested_join_learner_node_id(&request);
    let admission_coordinator = async_cluster_join_admission_coordinator(paths);
    let observed_admission_generation = admission_coordinator.generation();
    let admission_wait_started = Instant::now();
    join_registration_authority_phase_trace(
        "admission_coordinator",
        &request.invite.invite_id,
        requested_learner_node_id,
        0,
        Duration::ZERO,
        state.kv_revision,
        state.term,
        "started",
        format_args!("waiting for authority-local join admission turn"),
    );
    let admission_turn = admission_coordinator.turn.lock().await;
    let cached_authoritative = admission_coordinator
        .latest_authoritative()
        .filter(|authoritative| authoritative.authority.term >= state.term);
    let (coordinator_revision, coordinator_term, coordinator_outcome) =
        match cached_authoritative.as_ref() {
            Some(authoritative) => (
                authoritative.value.revision.0,
                authoritative.authority.term,
                if admission_coordinator.generation() > observed_admission_generation {
                    "coalesced"
                } else {
                    "cached"
                },
            ),
            None => (state.kv_revision, state.term, "acquired"),
        };
    join_registration_authority_phase_trace(
        "admission_coordinator",
        &request.invite.invite_id,
        requested_learner_node_id,
        0,
        admission_wait_started.elapsed(),
        coordinator_revision,
        coordinator_term,
        coordinator_outcome,
        format_args!(
            "authority-local join admission turn acquired at generation {}",
            admission_coordinator.generation()
        ),
    );

    // A committed snapshot remains a safe optimistic input after the preceding
    // request has returned: every admission transaction compares the Raft
    // generation and invitation/member state, and a concurrent mutation
    // returns the newer authoritative snapshot for the normal retry path.
    let mut authoritative = if let Some(authoritative) = cached_authoritative {
        authoritative
    } else {
        let authority_open_started = Instant::now();
        join_registration_authority_phase_trace(
            "authority_snapshot_open",
            &request.invite.invite_id,
            requested_learner_node_id,
            0,
            Duration::ZERO,
            state.kv_revision,
            state.term,
            "started",
            format_args!("opening authoritative cluster KV snapshot"),
        );
        let mut authority_state = state.clone();
        let authoritative_result = host
            .authoritative_cluster_kv_snapshot_async(paths, &mut authority_state)
            .await;
        match &authoritative_result {
            Ok(authoritative) => join_registration_authority_phase_trace(
                "authority_snapshot_open",
                &request.invite.invite_id,
                requested_learner_node_id,
                0,
                authority_open_started.elapsed(),
                authoritative.value.revision.0,
                authoritative.authority.term,
                "opened",
                format_args!("authoritative cluster KV snapshot opened"),
            ),
            Err(error) => join_registration_authority_phase_trace(
                "authority_snapshot_open",
                &request.invite.invite_id,
                requested_learner_node_id,
                0,
                authority_open_started.elapsed(),
                state.kv_revision,
                state.term,
                "error",
                format_args!("{error}"),
            ),
        }
        authoritative_result?
    };
    let mut commit_state = state.clone();
    let mut admission_attempt = 0usize;
    let (prepared, lifecycle_change_id, admitted) = loop {
        require_current_term_join_admission_authority(&authoritative)?;
        admission_attempt += 1;
        let prepare_state = state.clone();
        let prepare_revision = authoritative.value.revision.0;
        let prepare_snapshot = authoritative.value;
        let prepare_term = authoritative.authority.term;
        let prepare_request = request.clone();
        let prepare_started = Instant::now();
        join_registration_authority_phase_trace(
            "admission_prepare",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            Duration::ZERO,
            prepare_revision,
            prepare_term,
            "started",
            format_args!("preparing fenced learner admission"),
        );
        let preparation_result = match runtime
            .spawn_blocking(move || {
                prepare_authoritative_cluster_join_admission(
                    prepare_state,
                    prepare_snapshot,
                    prepare_request,
                    prepare_term,
                    now,
                )
            })
            .await
        {
            Ok(result) => result,
            Err(source) => Err(NetworkError::Io {
                operation: "cluster join registration prepare".to_string(),
                detail: format!("join registration prepare task failed: {source}"),
            }),
        };
        match &preparation_result {
            Ok(AuthoritativeClusterJoinAdmissionPreparation::Rejected(response)) => {
                join_registration_authority_phase_trace(
                    "admission_prepare",
                    &request.invite.invite_id,
                    requested_learner_node_id,
                    admission_attempt,
                    prepare_started.elapsed(),
                    prepare_revision,
                    prepare_term,
                    "rejected",
                    format_args!("{}", response.detail),
                );
            }
            Ok(AuthoritativeClusterJoinAdmissionPreparation::Commit { .. }) => {
                join_registration_authority_phase_trace(
                    "admission_prepare",
                    &request.invite.invite_id,
                    requested_learner_node_id,
                    admission_attempt,
                    prepare_started.elapsed(),
                    prepare_revision,
                    prepare_term,
                    "prepared",
                    format_args!("fenced learner admission prepared"),
                );
            }
            Err(error) => join_registration_authority_phase_trace(
                "admission_prepare",
                &request.invite.invite_id,
                requested_learner_node_id,
                admission_attempt,
                prepare_started.elapsed(),
                prepare_revision,
                prepare_term,
                "error",
                format_args!("{error}"),
            ),
        }
        let preparation = preparation_result?;
        let (prepared, txn, lifecycle_change_id) = match preparation {
            AuthoritativeClusterJoinAdmissionPreparation::Rejected(response) => {
                return Ok(response);
            }
            AuthoritativeClusterJoinAdmissionPreparation::Commit {
                prepared,
                txn,
                lifecycle_change_id,
            } => (prepared, txn, lifecycle_change_id),
        };
        let commit_request = KvTxnRequest::generated(txn);
        let commit_request_id = commit_request.request_id.clone();
        let commit_started = Instant::now();
        join_registration_authority_phase_trace(
            "quorum_commit",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            Duration::ZERO,
            prepare_revision,
            prepare_term,
            "started",
            format_args!(
                "committing fenced learner admission through quorum request_id={commit_request_id}"
            ),
        );
        let admitted_result = host
            .commit_join_registration_txn_async(paths, &mut commit_state, commit_request)
            .await;
        match &admitted_result {
            Ok(admitted) => {
                let outcome = if admitted
                    .txn_response
                    .as_ref()
                    .is_some_and(|response| response.succeeded)
                {
                    "committed"
                } else {
                    "compare_raced"
                };
                join_registration_authority_phase_trace(
                    "quorum_commit",
                    &request.invite.invite_id,
                    requested_learner_node_id,
                    admission_attempt,
                    commit_started.elapsed(),
                    admitted.snapshot.revision.0,
                    admitted.authority.term,
                    outcome,
                    format_args!("quorum commit attempt completed request_id={commit_request_id}"),
                );
            }
            Err(error) => join_registration_authority_phase_trace(
                "quorum_commit",
                &request.invite.invite_id,
                requested_learner_node_id,
                admission_attempt,
                commit_started.elapsed(),
                prepare_revision,
                prepare_term,
                "error",
                format_args!("request_id={commit_request_id}: {error}"),
            ),
        }
        let admitted = admitted_result?;
        if admitted
            .txn_response
            .as_ref()
            .is_some_and(|response| response.succeeded)
        {
            break (prepared, lifecycle_change_id, admitted);
        }
        if admission_attempt >= CLUSTER_JOIN_ADMISSION_MAX_ATTEMPTS {
            return Ok(ClusterJoinRegistrationResponse {
                accepted: false,
                detail: "cluster invitation or membership changed during join admission"
                    .to_string(),
                state: prepared.state,
                kv_snapshot: None,
                responder_node_id: None,
                raft_anchor: None,
            });
        }
        // A replay proves this exact failed compare, not authority for a new
        // admission. Obtain a distinct current-term cut only for a marked,
        // known failed replay; malformed proof combinations still reach the
        // unchanged authority guard and fail closed.
        authoritative = if admitted.replayed_committed_request
            && matches!(
                admitted.authority.basis,
                sinomo_kv::KvAuthorityBasis::ExactCommittedRequest
            )
            && admitted
                .txn_response
                .as_ref()
                .is_some_and(|response| !response.succeeded)
        {
            host.authoritative_cluster_kv_snapshot_async(paths, &mut commit_state)
                .await?
        } else {
            KvAuthoritativeRead {
                value: admitted.snapshot,
                authority: admitted.authority,
            }
        };
        let retry_backoff = cluster_join_admission_retry_backoff(admission_attempt);
        join_registration_trace(format_args!(
            "async atomic join admission compare raced with another cluster mutation; retrying after {retry_backoff:?}"
        ));
        tokio::time::sleep(retry_backoff).await;
    };
    let learner_node_id = prepared
        .learner_node_ids
        .first()
        .expect("validated join admission has one learner")
        .clone();
    let lifecycle_started = Instant::now();
    let admitted = finish_cluster_join_learner_admission_async_with_host(
        runtime,
        paths,
        &mut commit_state,
        admitted,
        lifecycle_change_id.as_ref(),
        &learner_node_id,
        now,
        host,
    )
    .await?;
    join_registration_trace(format_args!(
        "async join learner lifecycle completion node={learner_node_id} change={lifecycle_change_id:?} completed in {:?}",
        lifecycle_started.elapsed()
    ));
    admission_coordinator.publish(KvAuthoritativeRead {
        value: admitted.snapshot.clone(),
        authority: admitted.authority,
    });
    drop(admission_turn);
    let anchor_paths = paths.clone();
    let anchor_node_id = commit_state.local_node_id.clone();
    let anchored = runtime
        .spawn_blocking(move || cluster_join_response_raft_snapshot(&anchor_paths, &anchor_node_id))
        .await
        .ok()
        .flatten();
    let (snapshot, raft_anchor) =
        anchored_cluster_join_response_snapshot(admitted.snapshot, anchored);
    let response_revision = snapshot.revision.0;
    let response_term = admitted.authority.term;
    let response_state_input = prepared.state;
    let response_snapshot = snapshot.clone();
    let projection_started = Instant::now();
    join_registration_authority_phase_trace(
        "response_projection",
        &request.invite.invite_id,
        requested_learner_node_id,
        admission_attempt,
        Duration::ZERO,
        response_revision,
        response_term,
        "started",
        format_args!("projecting accepted response from committed snapshot"),
    );
    let response_state_result = match runtime
        .spawn_blocking(move || {
            cluster_join_response_state_from_snapshot(response_state_input, &response_snapshot, now)
        })
        .await
    {
        Ok(result) => result,
        Err(source) => Err(NetworkError::Io {
            operation: "cluster join registration response projection".to_string(),
            detail: format!("join response projection task failed: {source}"),
        }),
    };
    match &response_state_result {
        Ok(response_state) => join_registration_authority_phase_trace(
            "response_projection",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            projection_started.elapsed(),
            response_state.kv_revision,
            response_state.term,
            "projected",
            format_args!("accepted response projection completed"),
        ),
        Err(error) => join_registration_authority_phase_trace(
            "response_projection",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            projection_started.elapsed(),
            response_revision,
            response_term,
            "error",
            format_args!("{error}"),
        ),
    }
    let response_state = response_state_result?;
    let write_paths = paths.clone();
    let write_state = response_state.clone();
    let persistence_started = Instant::now();
    join_registration_authority_phase_trace(
        "local_persistence",
        &request.invite.invite_id,
        requested_learner_node_id,
        admission_attempt,
        Duration::ZERO,
        response_state.kv_revision,
        response_state.term,
        "started",
        format_args!("persisting accepted response projection locally"),
    );
    let persistence_result = match runtime
        .spawn_blocking(move || persist_accepted_cluster_join_state(&write_paths, write_state))
        .await
    {
        Ok(result) => result,
        Err(source) => Err(NetworkError::Io {
            operation: "write cluster state".to_string(),
            detail: format!("cluster state write task failed: {source}"),
        }),
    };
    match &persistence_result {
        Ok(crate::local_state_store::AcceptedClusterJoinStatePersistence::Applied {
            persisted_revision,
            ..
        }) => join_registration_authority_phase_trace(
            "local_persistence",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            persistence_started.elapsed(),
            *persisted_revision,
            response_state.term,
            "applied",
            format_args!("accepted response projection persisted locally"),
        ),
        Ok(crate::local_state_store::AcceptedClusterJoinStatePersistence::Superseded {
            persisted_revision,
            persisted_term,
            ..
        }) => join_registration_authority_phase_trace(
            "local_persistence",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            persistence_started.elapsed(),
            *persisted_revision,
            *persisted_term,
            "superseded",
            format_args!("a newer accepted response projection was already persisted"),
        ),
        Err(error) => join_registration_authority_phase_trace(
            "local_persistence",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            persistence_started.elapsed(),
            response_state.kv_revision,
            response_state.term,
            "error",
            format_args!("{error}"),
        ),
    }
    persistence_result?;
    if join_registration_should_remember_successful_targets(&response_state, &request.invite) {
        let handoff_started = Instant::now();
        join_registration_authority_phase_trace(
            "successful_target_handoff",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            Duration::ZERO,
            response_state.kv_revision,
            response_state.term,
            "started",
            format_args!("handing successful targets to connectivity tracking"),
        );
        let handoff_result = host
            .remember_successful_join_targets_async(paths, &response_state, &request.members)
            .await;
        match &handoff_result {
            Ok(()) => join_registration_authority_phase_trace(
                "successful_target_handoff",
                &request.invite.invite_id,
                requested_learner_node_id,
                admission_attempt,
                handoff_started.elapsed(),
                response_state.kv_revision,
                response_state.term,
                "completed",
                format_args!("successful targets handed to connectivity tracking"),
            ),
            Err(error) => join_registration_authority_phase_trace(
                "successful_target_handoff",
                &request.invite.invite_id,
                requested_learner_node_id,
                admission_attempt,
                handoff_started.elapsed(),
                response_state.kv_revision,
                response_state.term,
                "error",
                format_args!("{error}"),
            ),
        }
        handoff_result?;
    } else {
        join_registration_authority_phase_trace(
            "successful_target_handoff",
            &request.invite.invite_id,
            requested_learner_node_id,
            admission_attempt,
            Duration::ZERO,
            response_state.kv_revision,
            response_state.term,
            "skipped",
            format_args!("accepted invite did not require a successful-target handoff"),
        );
    }
    Ok(ClusterJoinRegistrationResponse {
        accepted: true,
        detail: "join registered".to_string(),
        state: response_state,
        kv_snapshot: Some(snapshot),
        responder_node_id: None,
        raft_anchor,
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

fn cluster_join_registration_rpc_head() -> ObjectRpcRequest {
    ObjectRpcRequest::from_encoded_body(
        CLUSTER_JOIN_REGISTER_ROUTE,
        CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        Vec::new(),
    )
    .with_response_retry_safe()
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
    if let Some(snapshot) = forwarded_acceptance_raft_snapshot(response)
        && let Err(error) =
            install_cluster_kv_anchored_snapshot(paths, &state.local_node_id, snapshot)
    {
        warn_forwarded_acceptance_raft_snapshot(&error);
    }
    persist_accepted_cluster_join_state(paths, response.state.clone())?;
    host.remember_successful_join_targets(paths, state, request_members)
}

/// The authority's admission snapshot at its anchored Raft position. An
/// introducer behind it installs it before answering the joiner, so a later
/// Raft append rewrites its view without losing the member it just introduced.
fn forwarded_acceptance_raft_snapshot(
    response: &ClusterJoinRegistrationResponse,
) -> Option<sinomo_kv::RaftSnapshot> {
    let anchor = response.raft_anchor?;
    Some(sinomo_kv::RaftSnapshot {
        last_included_index: anchor.last_included_index,
        last_included_term: anchor.last_included_term,
        kv: Arc::new(response.kv_snapshot.clone()?),
    })
}

fn warn_forwarded_acceptance_raft_snapshot(error: &NetworkError) {
    // The admission is already committed; replication still catches up.
    tracing::warn!(
        %error,
        "introducer could not install the authority's admission snapshot"
    );
}

pub async fn record_forwarded_join_registration_acceptance_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    invite_id: &ClusterInviteId,
    requested_learner_node_id: Option<&NodeId>,
    request_members: &[ClusterMemberRecord],
    response: &ClusterJoinRegistrationResponse,
    host: &mut (impl ClusterJoinRegistrationAsyncHost + Send),
) -> Result<(), NetworkError> {
    if !response.accepted {
        return Ok(());
    }
    let trace = ForwardedJoinAcceptanceTrace::new(invite_id, requested_learner_node_id);
    let paths_for_state = paths.clone();
    let local_node_id = state.local_node_id.clone();
    let response_state = response.state.clone();
    let raft_snapshot = forwarded_acceptance_raft_snapshot(response);
    let revision = response.state.kv_revision;
    let term = response.state.term;
    let state_worker_started = trace.as_ref().map(|trace| {
        trace.start(
            "forwarded_acceptance_state_worker",
            revision,
            term,
            format_args!("scheduling forwarded acceptance state persistence"),
        )
    });
    let trace_for_state = trace.clone();
    let state_result = match runtime
        .spawn_blocking(move || {
            if let Some(snapshot) = raft_snapshot {
                let started = trace_for_state.as_ref().map(|trace| {
                    trace.start(
                        "forwarded_anchored_snapshot",
                        revision,
                        term,
                        format_args!("installing the authority's anchored admission snapshot"),
                    )
                });
                let result = install_cluster_kv_anchored_snapshot(
                    &paths_for_state,
                    &local_node_id,
                    snapshot,
                );
                if let (Some(trace), Some(started)) = (trace_for_state.as_ref(), started) {
                    match &result {
                        Ok(installed) => trace.phase(
                            "forwarded_anchored_snapshot",
                            started.elapsed(),
                            revision,
                            term,
                            if *installed { "installed" } else { "current" },
                            format_args!("anchored admission snapshot install completed"),
                        ),
                        Err(error) => trace.phase(
                            "forwarded_anchored_snapshot",
                            started.elapsed(),
                            revision,
                            term,
                            "error",
                            format_args!("{error}"),
                        ),
                    }
                }
                if let Err(error) = result {
                    warn_forwarded_acceptance_raft_snapshot(&error);
                }
            } else if let Some(trace) = trace_for_state.as_ref() {
                trace.phase(
                    "forwarded_anchored_snapshot",
                    Duration::ZERO,
                    revision,
                    term,
                    "skipped",
                    format_args!("authority response did not anchor its snapshot"),
                );
            }
            let started = trace_for_state.as_ref().map(|trace| {
                trace.start(
                    "local_accepted_state_persistence",
                    revision,
                    term,
                    format_args!("persisting the forwarded accepted cluster state"),
                )
            });
            let result = persist_accepted_cluster_join_state(&paths_for_state, response_state);
            if let (Some(trace), Some(started)) = (trace_for_state.as_ref(), started) {
                match &result {
                    Ok(
                        crate::local_state_store::AcceptedClusterJoinStatePersistence::Applied {
                            persisted_revision,
                            ..
                        },
                    ) => trace.phase(
                        "local_accepted_state_persistence",
                        started.elapsed(),
                        *persisted_revision,
                        term,
                        "applied",
                        format_args!("forwarded accepted state persisted locally"),
                    ),
                    Ok(
                        crate::local_state_store::AcceptedClusterJoinStatePersistence::Superseded {
                            persisted_revision,
                            persisted_term,
                            ..
                        },
                    ) => trace.phase(
                        "local_accepted_state_persistence",
                        started.elapsed(),
                        *persisted_revision,
                        *persisted_term,
                        "superseded",
                        format_args!("a newer accepted state was already persisted locally"),
                    ),
                    Err(error) => trace.phase(
                        "local_accepted_state_persistence",
                        started.elapsed(),
                        revision,
                        term,
                        "error",
                        format_args!("{error}"),
                    ),
                }
            }
            result.map(|_| ())
        })
        .await
    {
        Ok(result) => result,
        Err(source) => Err(NetworkError::Io {
            operation: "cluster forwarded join acceptance".to_string(),
            detail: format!("forwarded join acceptance task failed: {source}"),
        }),
    };
    if let (Some(trace), Some(started)) = (trace.as_ref(), state_worker_started) {
        match &state_result {
            Ok(()) => trace.phase(
                "forwarded_acceptance_state_worker",
                started.elapsed(),
                revision,
                term,
                "completed",
                format_args!("forwarded acceptance state persistence completed"),
            ),
            Err(error) => trace.phase(
                "forwarded_acceptance_state_worker",
                started.elapsed(),
                revision,
                term,
                "error",
                format_args!("{error}"),
            ),
        }
    }
    state_result?;

    let handoff_started = trace.as_ref().map(|trace| {
        trace.start(
            "successful_target_handoff",
            revision,
            term,
            format_args!("handing successful join targets to connectivity tracking"),
        )
    });
    let handoff_result = host
        .remember_successful_join_targets_async(paths, state, request_members)
        .await;
    if let (Some(trace), Some(started)) = (trace.as_ref(), handoff_started) {
        match &handoff_result {
            Ok(()) => trace.phase(
                "successful_target_handoff",
                started.elapsed(),
                revision,
                term,
                "completed",
                format_args!("successful join targets handed to connectivity tracking"),
            ),
            Err(error) => trace.phase(
                "successful_target_handoff",
                started.elapsed(),
                revision,
                term,
                "error",
                format_args!("{error}"),
            ),
        }
    }
    handoff_result
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
            raft_anchor: None,
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
        raft_anchor: None,
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
        let mut reconciliation = apply_accepted_cluster_join_state(
            state,
            invite,
            &endpoint,
            accepted_state,
            Some(snapshot),
        );
        reconciliation.raft_anchor = response.raft_anchor;
        return Ok(Some(reconciliation));
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
        raft_anchor: None,
    }
}

/// Persists an accepted join's snapshot as the materialized checkpoint and,
/// when the authority anchored it at its Raft position, as this node's initial
/// Raft state, so its first reads already include the admission. Without an
/// anchor the Raft state starts empty and replication fills it.
pub fn persist_accepted_cluster_join_snapshot(
    paths: &ClusterPaths,
    local_node_id: &NodeId,
    snapshot: &KvSnapshot,
    raft_anchor: Option<ClusterJoinRaftAnchor>,
) -> Result<(), NetworkError> {
    let kv_paths = crate::ClusterKvPathsExt::kv_paths(paths);
    sinomo_kv::write_cluster_kv_snapshot_if_changed(&kv_paths, snapshot).map_err(kv_err)?;
    let Some(anchor) = raft_anchor else {
        return Ok(());
    };
    let installed = kv_paths.raft_store().install_anchored_snapshot(
        local_node_id,
        sinomo_kv::RaftSnapshot {
            last_included_index: anchor.last_included_index,
            last_included_term: anchor.last_included_term,
            kv: Arc::new(snapshot.clone()),
        },
    );
    if let Err(error) = installed {
        // The admission is already committed; replication still catches up.
        tracing::warn!(
            %error,
            "joined node could not start its Raft state at the admission snapshot"
        );
    }
    Ok(())
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
                    cluster_endpoint_address_host(&candidate.address) == Some(host)
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
    // Acceptance already bound this channel to an authorized member and
    // committed that member's listener advertisements. The joiner may have
    // reached it through an address it does not advertise (a hostname, NAT or
    // published port), so keep the channel as an observed route as well. This
    // supplies route data; the normal transport and membership checks still
    // authorize every call.
    if endpoint.node_id == state.local_node_id
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

pub trait ClusterJoinRegistrationAsyncHost: ClusterObjectRpcAsyncHost {
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
