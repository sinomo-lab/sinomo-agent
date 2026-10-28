//! Local, identity-preserving recovery of one missing peer transport endpoint.
//!
//! This is not membership repair. A current-term read over the existing peer's
//! authenticated carrier supplies route data only. The local projection and its
//! request receipt share the existing cross-process state mutation lock.

use crate::local_state_store::{
    acquire_local_cluster_state_mutation_lock, write_cluster_state_unlocked,
};
use serde::{Deserialize, Serialize};
use sinomo_base::{ClusterBootstrapState, ClusterEndpointRecord, ClusterMemberRecord, Id};
use sinomo_kv::{
    ClusterKvRaftConfigRecord, KvAuthoritativeRead, KvAuthorityBasis, KvSnapshot,
    cluster_kv_raft_config_key,
};
use sinomo_network::{
    ClusterPaths, NetworkError, read_object_file, read_optional_object_file, write_object_file,
};
use sinomo_node::{ClusterId, NodeId, cluster_member_key};
use std::time::{Duration, Instant};

pub const CLUSTER_CONTROL_RECOVER_ENDPOINT_ROUTE: &str = "/cluster/control/recover-endpoint";
pub const ENDPOINT_RECOVERY_PROOF_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EndpointRecoverySpec {
    pub operation_id: Id,
    pub cluster_id: ClusterId,
    pub local_node_id: NodeId,
    pub peer_node_id: NodeId,
    pub peer_public_key: String,
    pub endpoint_id: Id,
    /// Numeric TCP socket address, without a scheme. No DNS or alternate route.
    pub address: String,
    /// Returned by status before submission. Never recaptured during apply.
    pub expected_state_digest: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EndpointRecoveryAction {
    Apply,
    Status,
    Rollback,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EndpointRecoveryRequest {
    pub spec: EndpointRecoverySpec,
    pub action: EndpointRecoveryAction,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EndpointRecoveryOutcome {
    NotSubmitted,
    Prepared,
    Applied,
    RollingBack,
    RolledBack,
    Conflict,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EndpointRecoveryResponse {
    pub operation_id: Id,
    pub outcome: EndpointRecoveryOutcome,
    pub current_state_digest: String,
    pub matches_recorded_state: bool,
    /// Historical validation read, not current authority for another action.
    pub validation_authority: Option<sinomo_kv::KvAuthorityProof>,
    /// The effect owner must reconcile transport after this new or resumed
    /// transition. Status and ordinary terminal replays never request work.
    pub needs_transport_refresh: bool,
}

/// Only the daemon adapter implements the production transport. The request
/// cannot carry a caller-asserted snapshot, key, authority proof or member list.
pub trait EndpointRecoveryHost {
    fn authoritative_read(
        &mut self,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Receipt {
    spec: EndpointRecoverySpec,
    before: ClusterBootstrapState,
    after: ClusterBootstrapState,
    phase: EndpointRecoveryOutcome,
    authority: sinomo_kv::KvAuthorityProof,
}

fn invalid(detail: &str) -> NetworkError {
    NetworkError::directory(format!("endpoint recovery: {detail}"))
}

pub fn endpoint_recovery_state_digest(
    state: &ClusterBootstrapState,
) -> Result<String, NetworkError> {
    let bytes = serde_json::to_vec(state).map_err(|e| invalid(&e.to_string()))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn member<'a>(
    state: &'a ClusterBootstrapState,
    id: &NodeId,
) -> Result<&'a ClusterMemberRecord, NetworkError> {
    let mut found = state.members.iter().filter(|m| &m.node_id == id);
    let m = found
        .next()
        .ok_or_else(|| invalid("existing member absent"))?;
    if found.next().is_some() || m.cluster_id != state.cluster_id || !m.state.is_authorized() {
        return Err(invalid(
            "existing member is foreign, duplicated or unauthorized",
        ));
    }
    Ok(m)
}

fn validate_binding(
    state: &ClusterBootstrapState,
    spec: &EndpointRecoverySpec,
) -> Result<ClusterEndpointRecord, NetworkError> {
    if state.cluster_id != spec.cluster_id
        || state.local_node_id != spec.local_node_id
        || spec.peer_node_id == spec.local_node_id
    {
        return Err(invalid("cluster/local/peer identity mismatch"));
    }
    member(state, &spec.local_node_id)?;
    let peer = member(state, &spec.peer_node_id)?;
    if peer.node_public_key != spec.peer_public_key || !peer.has_raft_vote() {
        return Err(invalid("peer key or existing voter binding mismatch"));
    }
    let socket: std::net::SocketAddr = spec
        .address
        .parse()
        .map_err(|_| invalid("numeric TCP address required"))?;
    if socket.ip().is_unspecified() || socket.ip().is_multicast() || socket.port() == 0 {
        return Err(invalid("invalid TCP destination"));
    }
    let mut endpoint =
        sinomo_network::daemon_lan_endpoint(&spec.peer_node_id, spec.address.clone(), 0);
    endpoint.endpoint_id = spec.endpoint_id.clone();
    Ok(endpoint)
}

fn validate_proof(
    state: &ClusterBootstrapState,
    spec: &EndpointRecoverySpec,
    proof: &KvAuthoritativeRead<KvSnapshot>,
) -> Result<ClusterEndpointRecord, NetworkError> {
    let a = proof.authority;
    if a.term == 0
        || a.commit_index == 0
        || a.term < state.term
        || a.revision.0 < state.kv_revision
        || a.revision != proof.value.revision
    {
        return Err(invalid("stale or inconsistent authority proof"));
    }
    let config: ClusterKvRaftConfigRecord = proof
        .value
        .get(&cluster_kv_raft_config_key())
        .ok_or_else(|| invalid("authoritative Raft config absent"))?
        .decode()
        .map_err(|e| invalid(&e.to_string()))?;
    if config.cluster_id != state.cluster_id
        || config.voters.is_empty()
        || config.term > a.term
        || config.kv_revision > a.revision.0
        || config
            .voters
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != config.voters.len()
        || !config.voters.contains(&spec.peer_node_id)
        || !(config.voters.contains(&spec.local_node_id)
            || config.learners.contains(&spec.local_node_id)
            || config.non_voting.contains(&spec.local_node_id))
    {
        return Err(invalid("foreign config or peer not a current voter"));
    }
    // Use the committed configuration's actual quorum, never a caller-supplied count.
    let required = config.voters.len() / 2 + 1;
    if !matches!(a.basis, KvAuthorityBasis::CurrentTermQuorum { voter_acks, required_quorum }
        if required_quorum == required && voter_acks >= required && voter_acks <= config.voters.len())
    {
        return Err(invalid("current-term quorum proof required"));
    }
    let mut peer = None;
    for id in [&spec.local_node_id, &spec.peer_node_id] {
        let current: ClusterMemberRecord = proof
            .value
            .get(&cluster_member_key(id))
            .ok_or_else(|| invalid("authoritative member absent"))?
            .decode()
            .map_err(|e| invalid(&e.to_string()))?;
        let existing = member(state, id)?;
        if current.cluster_id != state.cluster_id
            || &current.node_id != id
            || !current.state.is_authorized()
            || current.node_public_key != existing.node_public_key
        {
            return Err(invalid("authoritative membership/key mismatch"));
        }
        if id == &spec.peer_node_id {
            peer = Some(current);
        }
    }
    let peer = peer.expect("peer checked");
    if !peer.has_raft_vote() {
        return Err(invalid("peer is no longer a voter"));
    }
    // The canonical endpoint table can replace an older embedded member route.
    // Use the same merged projection as ordinary routing, not the stale copy.
    let published_peer = crate::read_cluster_members_from_snapshot(&proof.value)?
        .into_iter()
        .find(|m| m.node_id == spec.peer_node_id)
        .ok_or_else(|| invalid("published peer projection absent"))?;
    let mut endpoints = published_peer
        .endpoints
        .iter()
        .filter(|e| e.endpoint_id == spec.endpoint_id);
    let endpoint = endpoints
        .next()
        .ok_or_else(|| invalid("endpoint not published by current peer"))?;
    if endpoints.next().is_some()
        || endpoint.node_id != spec.peer_node_id
        || endpoint.address != spec.address
        || endpoint.transport != sinomo_base::ClusterTransportKind::LanTcp
        || !sinomo_network::cluster_endpoint_can_carry_main_daemon_rpc(endpoint)
    {
        return Err(invalid(
            "published endpoint binding or main-process role mismatch",
        ));
    }
    Ok(endpoint.clone())
}

fn receipt_path(paths: &ClusterPaths, spec: &EndpointRecoverySpec) -> std::path::PathBuf {
    paths
        .root
        .join("endpoint-recovery")
        .join(format!("{}.sobj", spec.operation_id))
}

/// Bridge only a recovered route across an older observational KV view. This
/// never changes membership, keys, capabilities, votes or the view's revision.
/// Once KV reaches the validation read, ordinary endpoint retirement wins.
pub(crate) fn preserve_verified_recovery_routes(
    paths: &ClusterPaths,
    stored: &ClusterBootstrapState,
    projected: &mut [ClusterMemberRecord],
    observed_revision: sinomo_kv::KvRevision,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let candidates = stored
        .members
        .iter()
        .filter(|m| m.node_id != stored.local_node_id && m.state.is_authorized())
        .flat_map(|m| m.endpoints.iter().map(move |e| (m, e)))
        .filter(|(m, e)| {
            projected.iter().any(|p| {
                p.node_id == m.node_id
                    && p.cluster_id == stored.cluster_id
                    && p.state.is_authorized()
                    && p.node_public_key == m.node_public_key
                    && !p.endpoints.contains(e)
            })
        })
        .collect::<Vec<_>>();
    // No per-read receipt IO after the ordinary projection has caught up.
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let directory = paths.root.join("endpoint-recovery");
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(invalid(&format!("read recovery receipts: {e}"))),
    };
    let mut retained = Vec::new();
    for entry in entries {
        let path = entry.map_err(|e| invalid(&e.to_string()))?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("sobj") {
            continue;
        }
        let receipt: Receipt = read_object_file(&path)?;
        if !matches!(
            receipt.phase,
            EndpointRecoveryOutcome::Prepared | EndpointRecoveryOutcome::Applied
        ) || receipt.spec.cluster_id != stored.cluster_id
            || receipt.spec.local_node_id != stored.local_node_id
            || receipt.authority.revision <= observed_revision
            || path.file_stem().and_then(|s| s.to_str()) != Some(receipt.spec.operation_id.as_str())
        {
            continue;
        }
        for (member, endpoint) in &candidates {
            if member.node_id != receipt.spec.peer_node_id
                || member.node_public_key != receipt.spec.peer_public_key
                || endpoint.endpoint_id != receipt.spec.endpoint_id
                || endpoint.address != receipt.spec.address
            {
                continue;
            }
            if !receipt.after.members.iter().any(|m| {
                m.node_id == member.node_id
                    && m.node_public_key == member.node_public_key
                    && m.endpoints.contains(endpoint)
            }) {
                continue;
            }
            if let Some(current) = projected.iter_mut().find(|m| m.node_id == member.node_id) {
                // An existing endpoint with this ID is a newer/conflicting
                // projection, never something recovery may overwrite.
                if !current
                    .endpoints
                    .iter()
                    .any(|e| e.endpoint_id == endpoint.endpoint_id || e.address == endpoint.address)
                {
                    current.endpoints.push((*endpoint).clone());
                    retained.push((*endpoint).clone());
                }
            }
        }
    }
    Ok(retained)
}

fn read_receipt(
    paths: &ClusterPaths,
    spec: &EndpointRecoverySpec,
) -> Result<Option<Receipt>, NetworkError> {
    let receipt: Option<Receipt> = read_optional_object_file(&receipt_path(paths, spec))?;
    if receipt.as_ref().is_some_and(|r| r.spec != *spec) {
        return Err(invalid("operation ID reused with different body"));
    }
    Ok(receipt)
}

fn response(
    state: &ClusterBootstrapState,
    spec: &EndpointRecoverySpec,
    receipt: Option<&Receipt>,
) -> Result<EndpointRecoveryResponse, NetworkError> {
    use EndpointRecoveryOutcome::*;
    let (outcome, matches_recorded_state) = match receipt {
        None => (NotSubmitted, false),
        Some(r) => match r.phase {
            Prepared if state == &r.after => (Applied, true),
            Prepared if state == &r.before => (Prepared, true),
            RollingBack if state == &r.before => (RolledBack, true),
            RollingBack if state == &r.after => (RollingBack, true),
            Applied => (Applied, state == &r.after),
            RolledBack => (RolledBack, state == &r.before),
            _ => (Conflict, false),
        },
    };
    Ok(EndpointRecoveryResponse {
        operation_id: spec.operation_id.clone(),
        outcome,
        current_state_digest: endpoint_recovery_state_digest(state)?,
        matches_recorded_state,
        validation_authority: receipt.map(|r| r.authority),
        needs_transport_refresh: false,
    })
}

/// Synchronous owner transaction; daemon callers run it on their owned blocking
/// lane. No lock is held across the authenticated network read. Failed/uncertain
/// requests retain this same spec and operation ID; status never performs repair.
pub fn recover_endpoint<H: EndpointRecoveryHost>(
    paths: &ClusterPaths,
    request: &EndpointRecoveryRequest,
    host: &mut H,
) -> Result<EndpointRecoveryResponse, NetworkError> {
    use EndpointRecoveryOutcome::*;
    let spec = &request.spec;
    let captured;
    {
        let _lock = acquire_local_cluster_state_mutation_lock(paths)?;
        let state: ClusterBootstrapState = read_object_file(&paths.state)?;
        if state.cluster_id != spec.cluster_id || state.local_node_id != spec.local_node_id {
            return Err(invalid("status target cluster/local identity mismatch"));
        }
        let mut receipt = read_receipt(paths, spec)?;
        let status = response(&state, spec, receipt.as_ref())?;
        if request.action == EndpointRecoveryAction::Status {
            return Ok(status);
        }
        validate_binding(&state, spec)?;
        if request.action == EndpointRecoveryAction::Rollback {
            let r = receipt
                .as_mut()
                .ok_or_else(|| invalid("no operation to roll back"))?;
            if status.outcome == RolledBack {
                let needs_transport_refresh = r.phase != RolledBack;
                if r.phase != RolledBack {
                    crate::kv_read_model::invalidate_endpoint_recovery_routes(paths);
                    r.phase = RolledBack;
                    write_object_file(&receipt_path(paths, spec), r)?;
                }
                return Ok(EndpointRecoveryResponse {
                    needs_transport_refresh,
                    ..status
                });
            }
            if !matches!(status.outcome, Applied | RollingBack) || state != r.after {
                return Err(invalid(
                    "rollback conflicts with current state; nothing overwritten",
                ));
            }
            r.phase = RollingBack;
            write_object_file(&receipt_path(paths, spec), r)?;
            #[cfg(test)]
            fail_after("rollback-intent")?;
            write_cluster_state_unlocked(paths, &r.before)?;
            #[cfg(test)]
            fail_after("rollback-before-invalidation")?;
            crate::kv_read_model::invalidate_endpoint_recovery_routes(paths);
            #[cfg(test)]
            fail_after("rollback-state")?;
            r.phase = RolledBack;
            write_object_file(&receipt_path(paths, spec), r)?;
            let mut result = response(&r.before, spec, Some(r))?;
            result.needs_transport_refresh = true;
            return Ok(result);
        }
        if matches!(status.outcome, Applied | RolledBack) {
            let needs_transport_refresh =
                receipt.as_ref().is_some_and(|r| r.phase != status.outcome);
            if let Some(r) = receipt.as_mut()
                && r.phase != status.outcome
            {
                crate::kv_read_model::invalidate_endpoint_recovery_routes(paths);
                r.phase = status.outcome;
                write_object_file(&receipt_path(paths, spec), r)?;
            }
            return Ok(EndpointRecoveryResponse {
                needs_transport_refresh,
                ..status
            });
        }
        if matches!(status.outcome, RollingBack | Conflict) {
            return Err(invalid("unresolved operation; inspect original outcome"));
        }
        if endpoint_recovery_state_digest(&state)? != spec.expected_state_digest {
            return Err(invalid("captured state precondition changed"));
        }
        captured = state;
    }
    let hint = validate_binding(&captured, spec)?;
    let started = Instant::now();
    let proof = host.authoritative_read(&captured, &hint)?;
    let endpoint = validate_proof(&captured, spec, &proof)?;
    let _lock = acquire_local_cluster_state_mutation_lock(paths)?;
    if started.elapsed() > ENDPOINT_RECOVERY_PROOF_TIMEOUT {
        return Err(invalid("authority read expired before effect"));
    }
    let current: ClusterBootstrapState = read_object_file(&paths.state)?;
    let mut previous = read_receipt(paths, spec)?;
    let status = response(&current, spec, previous.as_ref())?;
    if matches!(status.outcome, Applied | RolledBack) {
        let needs_transport_refresh = previous.as_ref().is_some_and(|r| r.phase != status.outcome);
        if let Some(r) = previous.as_mut()
            && r.phase != status.outcome
        {
            crate::kv_read_model::invalidate_endpoint_recovery_routes(paths);
            r.phase = status.outcome;
            write_object_file(&receipt_path(paths, spec), r)?;
        }
        return Ok(EndpointRecoveryResponse {
            needs_transport_refresh,
            ..status
        });
    }
    if current != captured || !matches!(status.outcome, NotSubmitted | Prepared) {
        return Err(invalid(
            "local state or operation changed during authority read",
        ));
    }
    let mut after = current.clone();
    let peer = after
        .members
        .iter_mut()
        .find(|m| m.node_id == spec.peer_node_id)
        .expect("binding checked");
    if peer
        .endpoints
        .iter()
        .any(|e| e.endpoint_id == endpoint.endpoint_id || e.address == endpoint.address)
    {
        return Err(invalid(
            "endpoint already exists or conflicts; recovery only adds a missing route",
        ));
    }
    peer.endpoints.push(endpoint);
    let mut receipt = Receipt {
        spec: spec.clone(),
        before: current,
        after,
        phase: Prepared,
        authority: proof.authority,
    };
    if let Some(previous) = previous
        && (previous.before != receipt.before || previous.after != receipt.after)
    {
        return Err(invalid(
            "prepared operation no longer matches authoritative endpoint",
        ));
    }
    write_object_file(&receipt_path(paths, spec), &receipt)?;
    #[cfg(test)]
    fail_after("prepared")?;
    write_cluster_state_unlocked(paths, &receipt.after)?;
    #[cfg(test)]
    fail_after("applied-before-invalidation")?;
    crate::kv_read_model::invalidate_endpoint_recovery_routes(paths);
    #[cfg(test)]
    fail_after("applied-state")?;
    receipt.phase = Applied;
    write_object_file(&receipt_path(paths, spec), &receipt)?;
    let mut result = response(&receipt.after, spec, Some(&receipt))?;
    result.needs_transport_refresh = true;
    Ok(result)
}

#[cfg(test)]
thread_local! { static FAIL_AFTER: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) }; }

#[cfg(test)]
fn fail_after(stage: &'static str) -> Result<(), NetworkError> {
    FAIL_AFTER.with(|f| {
        if f.get() == Some(stage) {
            f.set(None);
            Err(invalid("isolated injected writer failure"))
        } else {
            Ok(())
        }
    })
}

#[cfg(test)]
mod tests;
