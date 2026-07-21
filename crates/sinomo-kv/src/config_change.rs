use crate::internal::*;

pub const MAX_CLUSTER_KV_RAFT_CONFIG_CHANGE_EVIDENCE: usize = 32;
pub const MAX_CLUSTER_KV_RAFT_PEER_PROGRESS_HINTS: usize = 256;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterKvRaftConfigChangeKind {
    AddLearner,
    PromoteLearner,
    DemoteVoter,
    RemoveMember,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterKvRaftConfigChangePhase {
    Planned,
    CatchingUp,
    ReadyToCommit,
    Committed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvRaftCatchUpProof {
    pub node_id: NodeId,
    pub term: u64,
    pub required_log_index: u64,
    pub observed_match_index: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
}

impl ClusterKvRaftCatchUpProof {
    pub fn proves_required_index(&self) -> bool {
        self.observed_match_index >= self.required_log_index
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvRaftConfigChangeEvidence {
    pub detail: String,
    pub term: u64,
    pub log_index: u64,
    pub revision: KvRevision,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvRaftConfigChange {
    pub change_id: Id,
    pub kind: ClusterKvRaftConfigChangeKind,
    pub node_id: NodeId,
    pub phase: ClusterKvRaftConfigChangePhase,
    pub base_config_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_log_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catch_up_proof: Option<ClusterKvRaftCatchUpProof>,
    #[serde(default, deserialize_with = "deserialize_fallible_vec")]
    pub evidence: Vec<ClusterKvRaftConfigChangeEvidence>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

pub fn cluster_kv_raft_config_change_key(change_id: &Id) -> String {
    format!("/cluster/raft/config-changes/{change_id}")
}

pub fn begin_cluster_kv_raft_config_change(
    current: &ClusterKvRaftConfigRecord,
    change_id: Id,
    kind: ClusterKvRaftConfigChangeKind,
    node_id: NodeId,
    required_log_index: Option<u64>,
    now: OffsetDateTime,
) -> Result<ClusterKvRaftConfigChange, KvError> {
    let current_role = cluster_kv_raft_config_member_role(current, &node_id);
    match kind {
        ClusterKvRaftConfigChangeKind::AddLearner
            if matches!(
                current_role,
                Some(ClusterMemberRole::Voter | ClusterMemberRole::Learner)
            ) =>
        {
            return Err(KvError::InvalidRaftConfigTransition {
                detail: format!("node `{node_id}` is already a voter or learner"),
            });
        }
        ClusterKvRaftConfigChangeKind::PromoteLearner
            if current_role != Some(ClusterMemberRole::Learner) =>
        {
            return Err(KvError::InvalidRaftConfigTransition {
                detail: format!("node `{node_id}` must be a committed learner before promotion"),
            });
        }
        ClusterKvRaftConfigChangeKind::DemoteVoter
            if current_role != Some(ClusterMemberRole::Voter) =>
        {
            return Err(KvError::InvalidRaftConfigTransition {
                detail: format!("node `{node_id}` is not a voter"),
            });
        }
        ClusterKvRaftConfigChangeKind::RemoveMember if current_role.is_none() => {
            return Err(KvError::InvalidRaftConfigTransition {
                detail: format!("node `{node_id}` is not present in the Raft config"),
            });
        }
        _ => {}
    }
    let phase = if kind == ClusterKvRaftConfigChangeKind::PromoteLearner {
        ClusterKvRaftConfigChangePhase::CatchingUp
    } else {
        ClusterKvRaftConfigChangePhase::Planned
    };
    if kind == ClusterKvRaftConfigChangeKind::PromoteLearner && required_log_index.is_none() {
        return Err(KvError::InvalidRaftConfigTransition {
            detail: "learner promotion requires a catch-up log index".to_string(),
        });
    }
    Ok(ClusterKvRaftConfigChange {
        change_id,
        kind,
        node_id,
        phase,
        base_config_revision: current.kv_revision,
        required_log_index,
        catch_up_proof: None,
        evidence: Vec::new(),
        updated_at: now,
    })
}

pub fn record_cluster_kv_raft_catch_up_proof(
    change: &mut ClusterKvRaftConfigChange,
    proof: ClusterKvRaftCatchUpProof,
) -> Result<(), KvError> {
    if change.kind != ClusterKvRaftConfigChangeKind::PromoteLearner
        || change.phase != ClusterKvRaftConfigChangePhase::CatchingUp
        || proof.node_id != change.node_id
        || Some(proof.required_log_index) != change.required_log_index
        || !proof.proves_required_index()
    {
        return Err(KvError::InvalidRaftConfigTransition {
            detail: "learner catch-up proof does not satisfy the planned promotion".to_string(),
        });
    }
    change.updated_at = proof.observed_at;
    change.catch_up_proof = Some(proof);
    change.phase = ClusterKvRaftConfigChangePhase::ReadyToCommit;
    Ok(())
}

pub fn apply_cluster_kv_raft_config_change(
    current: &ClusterKvRaftConfigRecord,
    change: &ClusterKvRaftConfigChange,
    term: u64,
    revision: KvRevision,
    now: OffsetDateTime,
) -> Result<ClusterKvRaftConfigRecord, KvError> {
    if change.base_config_revision != current.kv_revision {
        return Err(KvError::InvalidRaftConfigTransition {
            detail: "Raft config change was planned against a different config revision"
                .to_string(),
        });
    }
    let ready = match change.kind {
        ClusterKvRaftConfigChangeKind::PromoteLearner => {
            change.phase == ClusterKvRaftConfigChangePhase::ReadyToCommit
                && change
                    .catch_up_proof
                    .as_ref()
                    .is_some_and(ClusterKvRaftCatchUpProof::proves_required_index)
        }
        _ => change.phase == ClusterKvRaftConfigChangePhase::Planned,
    };
    if !ready {
        return Err(KvError::InvalidRaftConfigTransition {
            detail: "Raft config change is not ready to commit".to_string(),
        });
    }
    let mut next = current.clone();
    next.term = term;
    next.kv_revision = revision.0;
    next.updated_at = now;
    next.voters.retain(|node_id| node_id != &change.node_id);
    next.learners.retain(|node_id| node_id != &change.node_id);
    next.non_voting.retain(|node_id| node_id != &change.node_id);
    match change.kind {
        ClusterKvRaftConfigChangeKind::AddLearner => next.learners.push(change.node_id.clone()),
        ClusterKvRaftConfigChangeKind::PromoteLearner => next.voters.push(change.node_id.clone()),
        ClusterKvRaftConfigChangeKind::DemoteVoter => next.non_voting.push(change.node_id.clone()),
        ClusterKvRaftConfigChangeKind::RemoveMember => {}
    }
    next.voters.sort_unstable();
    next.voters.dedup();
    next.learners.sort_unstable();
    next.learners.dedup();
    next.non_voting.sort_unstable();
    next.non_voting.dedup();
    validate_cluster_kv_raft_config_transition(current, &next)?;
    Ok(next)
}

pub fn finish_cluster_kv_raft_config_change(
    change: &mut ClusterKvRaftConfigChange,
    succeeded: bool,
    evidence: ClusterKvRaftConfigChangeEvidence,
) -> Result<(), KvError> {
    if change.evidence.len() >= MAX_CLUSTER_KV_RAFT_CONFIG_CHANGE_EVIDENCE {
        return Err(KvError::ResourceExhausted {
            operation: "record Raft config change evidence".to_string(),
            requested_bytes: change
                .evidence
                .len()
                .saturating_add(1)
                .saturating_mul(std::mem::size_of::<ClusterKvRaftConfigChangeEvidence>().max(1)),
        });
    }
    change.updated_at = evidence.observed_at;
    change.evidence.push(evidence);
    change.phase = if succeeded {
        ClusterKvRaftConfigChangePhase::Committed
    } else {
        ClusterKvRaftConfigChangePhase::Failed
    };
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvRaftPeerProgress {
    pub peer_node_id: NodeId,
    pub term: u64,
    pub next_index: u64,
    pub match_index: u64,
    pub last_accepted_index: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
struct ClusterKvRaftPeerProgressLedger {
    #[serde(default, deserialize_with = "deserialize_fallible_vec")]
    hints: Vec<ClusterKvRaftPeerProgress>,
}

pub fn read_cluster_kv_raft_peer_progress(
    paths: &ClusterKvPaths,
) -> Result<Vec<ClusterKvRaftPeerProgress>, KvError> {
    let ledger =
        read_optional_object::<ClusterKvRaftPeerProgressLedger>(&paths.raft_peer_progress)?
            .unwrap_or_default();
    if ledger.hints.len() > MAX_CLUSTER_KV_RAFT_PEER_PROGRESS_HINTS {
        return Err(KvError::ResourceExhausted {
            operation: "read bounded Raft peer progress hints".to_string(),
            requested_bytes: ledger
                .hints
                .len()
                .saturating_mul(std::mem::size_of::<ClusterKvRaftPeerProgress>().max(1)),
        });
    }
    Ok(ledger.hints)
}

/// Returns a discardable catch-up starting point for one peer.
///
/// Peer progress is soft state, so unreadable, stale, or internally
/// inconsistent records are deliberately treated as a cache miss. The Raft
/// append response remains authoritative and will drive normal backtracking if
/// a peer has diverged or regressed since this hint was recorded.
pub fn cluster_kv_raft_peer_progress_next_index_hint(
    paths: &ClusterKvPaths,
    peer_node_id: &NodeId,
    current_term: u64,
    last_local_log_index: u64,
) -> Option<u64> {
    let hint = read_cluster_kv_raft_peer_progress(paths)
        .ok()?
        .into_iter()
        .find(|hint| hint.peer_node_id == *peer_node_id)?;
    let expected_next_index = hint.match_index.checked_add(1)?;
    let last_local_next_index = last_local_log_index.saturating_add(1);
    if hint.term != current_term
        || hint.last_accepted_index != hint.match_index
        || hint.next_index != expected_next_index
        || hint.next_index == 0
        || hint.next_index > last_local_next_index
    {
        return None;
    }
    Some(hint.next_index)
}

pub async fn cluster_kv_raft_peer_progress_next_index_hint_async(
    paths: ClusterKvPaths,
    peer_node_id: NodeId,
    current_term: u64,
    last_local_log_index: u64,
) -> Option<u64> {
    tokio::task::spawn_blocking(move || {
        cluster_kv_raft_peer_progress_next_index_hint(
            &paths,
            &peer_node_id,
            current_term,
            last_local_log_index,
        )
    })
    .await
    .ok()
    .flatten()
}

pub fn record_cluster_kv_raft_peer_progress(
    paths: &ClusterKvPaths,
    progress: ClusterKvRaftPeerProgress,
) -> Result<(), KvError> {
    record_cluster_kv_raft_peer_progress_batch(paths, vec![progress])
}

/// Persists the soft replication hints produced by one committed fanout wave.
///
/// Fanout owns these observations, so it records the whole wave at completion.
/// Taking the Raft operation lock and rewriting the bounded ledger once avoids
/// turning one commit into one storage transaction per peer.
pub fn record_cluster_kv_raft_peer_progress_batch(
    paths: &ClusterKvPaths,
    progress: Vec<ClusterKvRaftPeerProgress>,
) -> Result<(), KvError> {
    if progress.is_empty() {
        return Ok(());
    }
    let _guard = lock_cluster_kv_raft_operation_for_paths(paths)?;
    let mut hints = read_cluster_kv_raft_peer_progress(paths)?;
    for progress in progress {
        hints.retain(|hint| hint.peer_node_id != progress.peer_node_id);
        hints.push(progress);
    }
    hints.sort_by_key(|hint| hint.observed_at);
    if hints.len() > MAX_CLUSTER_KV_RAFT_PEER_PROGRESS_HINTS {
        let excess = hints
            .len()
            .saturating_sub(MAX_CLUSTER_KV_RAFT_PEER_PROGRESS_HINTS);
        hints.drain(..excess);
    }
    write_object_file(
        &paths.raft_peer_progress,
        &ClusterKvRaftPeerProgressLedger { hints },
    )
}

pub fn clear_cluster_kv_raft_peer_progress(paths: &ClusterKvPaths) -> Result<(), KvError> {
    let _guard = lock_cluster_kv_raft_operation_for_paths(paths)?;
    write_object_file(
        &paths.raft_peer_progress,
        &ClusterKvRaftPeerProgressLedger::default(),
    )
}
