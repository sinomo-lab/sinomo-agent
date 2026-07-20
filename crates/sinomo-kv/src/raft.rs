use crate::internal::*;

pub(crate) static CLUSTER_KV_RAFT_OPERATION_LOCKS: LazyLock<
    Mutex<HashMap<PathBuf, &'static Mutex<()>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
pub(crate) static CLUSTER_KV_RAFT_SNAPSHOT_BYTES_LIMIT_FOR_TEST: AtomicU64 =
    AtomicU64::new(MAX_CLUSTER_KV_RAFT_SNAPSHOT_BYTES);

pub type ClusterKvRaftOperationGuard = MutexGuard<'static, ()>;

pub fn lock_cluster_kv_raft_operation_for_root(
    root: impl AsRef<Path>,
) -> Result<ClusterKvRaftOperationGuard, KvError> {
    let root = root.as_ref().to_path_buf();
    let lock = {
        let mut guard = CLUSTER_KV_RAFT_OPERATION_LOCKS
            .lock()
            .map_err(|error| KvError::Io {
                path: "cluster kv raft operation locks".to_string(),
                detail: format!("cluster kv raft operation lock table poisoned: {error}"),
            })?;
        *guard.entry(root.clone()).or_insert_with(|| {
            let lock: &'static Mutex<()> = Box::leak(Box::new(Mutex::new(())));
            lock
        })
    };
    lock.lock().map_err(|error| KvError::Io {
        path: root.display().to_string(),
        detail: format!("cluster kv raft operation lock poisoned: {error}"),
    })
}

pub fn lock_cluster_kv_raft_operation_for_paths(
    paths: &ClusterKvPaths,
) -> Result<ClusterKvRaftOperationGuard, KvError> {
    lock_cluster_kv_raft_operation_for_root(&paths.root)
}

pub fn lock_cluster_kv_raft_operation_for_wal_paths(
    paths: &KvWalPaths,
) -> Result<ClusterKvRaftOperationGuard, KvError> {
    lock_cluster_kv_raft_operation_for_root(&paths.root)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvLogEntry {
    pub index: u64,
    pub term: u64,
    pub request_id: KvRequestId,
    pub command: KvCommand,
    pub revision: KvRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftHardState {
    pub current_term: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub voted_for: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_id: Option<NodeId>,
    pub commit_index: u64,
}

impl Default for RaftHardState {
    fn default() -> Self {
        Self {
            current_term: 1,
            voted_for: None,
            leader_id: None,
            commit_index: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftSnapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub kv: KvSnapshot,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftAppendEntriesRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub entries: Vec<KvLogEntry>,
    pub leader_commit: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftAppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftRequestVoteRequest {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftRequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterKvRaftUncommittedAppend {
    Appended(KvLogEntry),
    CommittedReplay {
        snapshot: KvSnapshot,
        record: KvCommittedRequestRecord,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftCommitResult {
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
    pub events: Vec<KvEvent>,
    pub committed_request: KvCommittedRequestRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftAppendRequest {
    pub snapshot: Option<RaftSnapshot>,
    pub append: RaftAppendEntriesRequest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftLocalVoteMetadata {
    pub current_term: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvRaftConfigRecord {
    pub cluster_id: Id,
    pub term: u64,
    pub kv_revision: u64,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub voters: Vec<NodeId>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub learners: Vec<NodeId>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub non_voting: Vec<NodeId>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Default)]
struct RaftNodeIdSet {
    node_ids: BTreeSet<NodeId>,
}

impl RaftNodeIdSet {
    fn from_iter(
        node_ids: impl IntoIterator<Item = NodeId>,
        operation: &str,
    ) -> Result<Self, KvError> {
        let mut set = Self::default();
        for node_id in node_ids {
            set.try_insert_owned(node_id, operation)?;
        }
        Ok(set)
    }

    fn len(&self) -> usize {
        self.node_ids.len()
    }

    fn contains(&self, node_id: &NodeId) -> bool {
        self.node_ids.contains(node_id)
    }

    fn try_insert_owned(&mut self, node_id: NodeId, operation: &str) -> Result<bool, KvError> {
        let _ = operation;
        Ok(self.node_ids.insert(node_id))
    }

    fn try_insert_clone(&mut self, node_id: &NodeId, operation: &str) -> Result<bool, KvError> {
        Ok(self
            .node_ids
            .insert(try_clone_kv_node_id(node_id, operation)?))
    }
}

fn cluster_kv_authority_failure(
    state: &ClusterBootstrapState,
    reason: KvAuthorityFailureReason,
    detail: String,
    observed_term: u64,
    revision: KvRevision,
    reachable_voters: usize,
    known_leader: Option<NodeId>,
) -> KvAuthorityFailure {
    let local_role = state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id)
        .map(|member| member.role.clone())
        .unwrap_or(ClusterMemberRole::NonVoting);
    let suggested_recovery = match reason {
        KvAuthorityFailureReason::HigherTermObserved | KvAuthorityFailureReason::VoterRejected => {
            KvRecoveryAction::RefreshClusterState
        }
        KvAuthorityFailureReason::TransportFailure => KvRecoveryAction::ContactKnownLeader,
        KvAuthorityFailureReason::LocalNodeNotVoter
        | KvAuthorityFailureReason::NoQuorum
        | KvAuthorityFailureReason::Timeout => KvRecoveryAction::RetryWhenQuorumIsReachable,
    };
    KvAuthorityFailure {
        reason,
        detail,
        diagnostics: KvAuthorityDiagnostics {
            local_node_id: state.local_node_id.clone(),
            local_role,
            known_leader,
            observed_term,
            reachable_voters,
            required_quorum: cluster_kv_raft_quorum_size_for_state(state),
            last_successful_append_at: None,
            last_snapshot_refresh_at: None,
            authority: KvReadAuthority::Local,
            revision,
            freshness: KvFreshness::Unknown,
            suggested_recovery,
        },
    }
}

fn node_id_slice_contains(node_ids: &[NodeId], node_id: &NodeId) -> bool {
    node_ids.iter().any(|candidate| candidate == node_id)
}

fn unique_node_id_count(node_ids: &[NodeId]) -> usize {
    let mut count = 0;
    for (index, node_id) in node_ids.iter().enumerate() {
        if node_id_slice_contains(&node_ids[..index], node_id) {
            continue;
        }
        count += 1;
    }
    count
}

fn unique_node_id_overlap_count(left: &[NodeId], right: &[NodeId]) -> usize {
    let mut count = 0;
    for (index, node_id) in left.iter().enumerate() {
        if node_id_slice_contains(&left[..index], node_id) {
            continue;
        }
        if node_id_slice_contains(right, node_id) {
            count += 1;
        }
    }
    count
}

fn try_push_unique_node_id(
    node_ids: &mut Vec<NodeId>,
    node_id: &NodeId,
    operation: &str,
) -> Result<(), KvError> {
    if node_id_slice_contains(node_ids, node_id) {
        return Ok(());
    }
    try_reserve_kv_items(node_ids, 1, operation)?;
    node_ids.push(try_clone_kv_node_id(node_id, operation)?);
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftLeaderElectionOptions {
    pub max_attempts: usize,
}

pub trait ClusterKvRaftLeaderElectionHost {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    fn begin_election_attempt(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn local_vote_metadata(&mut self) -> Result<ClusterKvRaftLocalVoteMetadata, Self::Error>;

    fn apply_local_vote(
        &mut self,
        vote: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, Self::Error>;

    fn should_request_more_remote_votes(&mut self) -> bool {
        true
    }

    fn request_remote_vote(
        &mut self,
        target_index: usize,
        voter_id: &NodeId,
        vote: &RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, Self::Error>;

    fn observe_raft_term(&mut self, term: u64) -> Result<(), Self::Error>;

    fn set_raft_leader(&mut self, term: u64) -> Result<(), Self::Error>;
}

pub fn cluster_kv_raft_quorum_size(voter_count: usize) -> usize {
    voter_count / 2 + 1
}

pub fn cluster_kv_member_has_raft_vote(member: &ClusterMemberRecord) -> bool {
    member.has_raft_vote()
}

pub fn cluster_kv_raft_node_is_voter(state: &ClusterBootstrapState, node_id: &NodeId) -> bool {
    state.node_is_raft_voter(node_id)
}

pub fn cluster_kv_raft_candidate_is_authorized_voter(
    state: &ClusterBootstrapState,
    candidate_id: &NodeId,
) -> bool {
    cluster_kv_raft_node_is_voter(state, candidate_id)
}

pub fn cluster_kv_raft_local_is_voter(state: &ClusterBootstrapState) -> bool {
    cluster_kv_raft_node_is_voter(state, &state.local_node_id)
}

pub fn cluster_kv_raft_voter_ids(state: &ClusterBootstrapState) -> Result<Vec<NodeId>, KvError> {
    let voter_count = state
        .members
        .iter()
        .filter(|member| cluster_kv_raft_node_is_voter(state, &member.node_id))
        .count();
    let mut voters = Vec::new();
    try_reserve_kv_items(&mut voters, voter_count, "collect cluster raft voters")?;
    for member in state
        .members
        .iter()
        .filter(|member| cluster_kv_raft_node_is_voter(state, &member.node_id))
    {
        voters.push(try_clone_kv_node_id(
            &member.node_id,
            "collect cluster raft voters",
        )?);
    }
    voters.sort_unstable();
    voters.dedup();
    Ok(voters)
}

pub fn cluster_kv_raft_quorum_size_for_state(state: &ClusterBootstrapState) -> usize {
    let voter_count = state
        .members
        .iter()
        .filter(|member| cluster_kv_raft_node_is_voter(state, &member.node_id))
        .count();
    cluster_kv_raft_quorum_size(voter_count)
}

pub fn cluster_kv_raft_config_key() -> String {
    "/cluster/raft/config".to_string()
}

pub fn cluster_kv_raft_config_record_stable_eq(
    left: &ClusterKvRaftConfigRecord,
    right: &ClusterKvRaftConfigRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.term == right.term
        && left.voters == right.voters
        && left.learners == right.learners
        && left.non_voting == right.non_voting
}

pub fn cluster_kv_raft_config_member_role(
    config: &ClusterKvRaftConfigRecord,
    node_id: &NodeId,
) -> Option<ClusterMemberRole> {
    if config.voters.iter().any(|candidate| candidate == node_id) {
        Some(ClusterMemberRole::Voter)
    } else if config.learners.iter().any(|candidate| candidate == node_id) {
        Some(ClusterMemberRole::Learner)
    } else if config
        .non_voting
        .iter()
        .any(|candidate| candidate == node_id)
    {
        Some(ClusterMemberRole::NonVoting)
    } else {
        None
    }
}

pub fn cluster_kv_store_member_is_raft_voter(
    store: &InProcessKvStore,
    node_id: &NodeId,
) -> Result<bool, KvError> {
    let Some(config_entry) = store.get(&cluster_kv_raft_config_key()) else {
        return Ok(false);
    };
    let config: ClusterKvRaftConfigRecord = config_entry.decode()?;
    Ok(config.voters.iter().any(|voter| voter == node_id))
}

pub fn validate_cluster_kv_raft_config_transition(
    current: &ClusterKvRaftConfigRecord,
    next: &ClusterKvRaftConfigRecord,
) -> Result<(), KvError> {
    if current.cluster_id != next.cluster_id {
        return Err(KvError::InvalidRaftConfigTransition {
            detail: format!(
                "cluster raft config cluster id changed from `{}` to `{}`",
                current.cluster_id, next.cluster_id
            ),
        });
    }
    let mut removed_voters = Vec::new();
    for voter in current
        .voters
        .iter()
        .filter(|voter| !node_id_slice_contains(&next.voters, voter))
    {
        try_push_unique_node_id(&mut removed_voters, voter, "validate raft removed voters")?;
    }
    if !removed_voters.is_empty() {
        let retained_voters = unique_node_id_overlap_count(&current.voters, &next.voters);
        let current_quorum = cluster_kv_raft_quorum_size(unique_node_id_count(&current.voters));
        let next_quorum = cluster_kv_raft_quorum_size(unique_node_id_count(&next.voters));
        if retained_voters < current_quorum || retained_voters < next_quorum {
            return Err(KvError::InvalidRaftConfigTransition {
                detail: format!(
                    "raft voter removal requires overlapping old/new quorum; refusing to remove {}",
                    removed_voters
                        .iter()
                        .map(NodeId::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            });
        }
    }
    let mut added_voters = Vec::new();
    for voter in next
        .voters
        .iter()
        .filter(|voter| !node_id_slice_contains(&current.voters, voter))
    {
        try_push_unique_node_id(&mut added_voters, voter, "validate raft added voters")?;
    }
    if added_voters.len() > 1 {
        return Err(KvError::InvalidRaftConfigTransition {
            detail: format!(
                "raft voter expansion is limited to one voter per committed config change; requested {} additions",
                added_voters.len()
            ),
        });
    }
    if let Some(added_voter) = added_voters.first() {
        if !current
            .learners
            .iter()
            .chain(current.non_voting.iter())
            .any(|node_id| node_id == added_voter)
        {
            return Err(KvError::InvalidRaftConfigTransition {
                detail: format!(
                    "raft voter `{added_voter}` must already be a committed learner or non-voting member before promotion"
                ),
            });
        }
    }
    Ok(())
}

pub fn elect_cluster_kv_raft_leader_with_host<H>(
    local_node_id: &NodeId,
    voter_ids: impl IntoIterator<Item = NodeId>,
    remote_vote_targets: impl IntoIterator<Item = NodeId>,
    options: ClusterKvRaftLeaderElectionOptions,
    host: &mut H,
) -> Result<bool, H::Error>
where
    H: ClusterKvRaftLeaderElectionHost,
{
    let voter_ids = RaftNodeIdSet::from_iter(voter_ids, "collect raft election voters")
        .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?;
    if !voter_ids.contains(local_node_id) {
        return Ok(false);
    }
    let quorum_size = cluster_kv_raft_quorum_size(voter_ids.len());
    let mut remote_vote_targets_vec = Vec::new();
    for voter_id in remote_vote_targets {
        if voter_id != *local_node_id && voter_ids.contains(&voter_id) {
            try_reserve_kv_items(&mut remote_vote_targets_vec, 1, "collect raft vote targets")
                .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?;
            remote_vote_targets_vec.push(voter_id);
        }
    }
    let remote_vote_targets = remote_vote_targets_vec;

    for _ in 0..options.max_attempts {
        host.begin_election_attempt()?;
        let metadata = host.local_vote_metadata()?;
        let election_term = metadata
            .current_term
            .saturating_add(CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP);
        let vote_request = RaftRequestVoteRequest {
            term: election_term,
            candidate_id: try_clone_kv_node_id(local_node_id, "build raft election vote request")
                .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?,
            last_log_index: metadata.last_log_index,
            last_log_term: metadata.last_log_term,
        };
        let local_vote = host.apply_local_vote(RaftRequestVoteRequest {
            term: vote_request.term,
            candidate_id: try_clone_kv_node_id(&vote_request.candidate_id, "apply local raft vote")
                .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?,
            last_log_index: vote_request.last_log_index,
            last_log_term: vote_request.last_log_term,
        })?;
        let mut highest_observed_term = local_vote.term.max(election_term);
        let mut granted = RaftNodeIdSet::default();
        if local_vote.vote_granted {
            granted
                .try_insert_clone(local_node_id, "record local raft vote")
                .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?;
        }

        for (target_index, voter_id) in remote_vote_targets.iter().enumerate() {
            if granted.len() >= quorum_size {
                break;
            }
            if granted.contains(voter_id) {
                continue;
            }
            if !host.should_request_more_remote_votes() {
                break;
            }
            let Ok(response) = host.request_remote_vote(target_index, voter_id, &vote_request)
            else {
                continue;
            };
            highest_observed_term = highest_observed_term.max(response.term);
            if response.vote_granted {
                granted
                    .try_insert_clone(voter_id, "record remote raft vote")
                    .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?;
            }
        }

        if highest_observed_term > election_term {
            host.observe_raft_term(highest_observed_term)?;
            continue;
        }
        if granted.len() < quorum_size {
            return Ok(false);
        }
        host.set_raft_leader(election_term)?;
        return Ok(true);
    }
    Ok(false)
}

pub trait ClusterKvRaftLeaderElectionAsyncHost {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    fn begin_election_attempt_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    fn local_vote_metadata_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftLocalVoteMetadata, Self::Error>;

    fn apply_local_vote_async<'a>(
        &'a mut self,
        vote: RaftRequestVoteRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, RaftRequestVoteResponse, Self::Error>;

    fn should_request_more_remote_votes(&mut self) -> bool;

    fn request_remote_vote_async<'a>(
        &'a mut self,
        target_index: usize,
        voter_id: &'a NodeId,
        vote: &'a RaftRequestVoteRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, RaftRequestVoteResponse, Self::Error>;

    fn observe_raft_term_async<'a>(
        &'a mut self,
        term: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    fn set_raft_leader_async<'a>(
        &'a mut self,
        term: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;
}

pub async fn elect_cluster_kv_raft_leader_async_with_host<H>(
    local_node_id: &NodeId,
    voter_ids: impl IntoIterator<Item = NodeId>,
    remote_vote_targets: impl IntoIterator<Item = NodeId>,
    options: ClusterKvRaftLeaderElectionOptions,
    host: &mut H,
) -> Result<bool, H::Error>
where
    H: ClusterKvRaftLeaderElectionAsyncHost,
{
    let voter_ids = RaftNodeIdSet::from_iter(voter_ids, "collect async raft election voters")
        .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?;
    if !voter_ids.contains(local_node_id) {
        return Ok(false);
    }
    let quorum_size = cluster_kv_raft_quorum_size(voter_ids.len());
    let mut remote_vote_targets_vec = Vec::new();
    for voter_id in remote_vote_targets {
        if voter_id != *local_node_id && voter_ids.contains(&voter_id) {
            try_reserve_kv_items(
                &mut remote_vote_targets_vec,
                1,
                "collect async raft vote targets",
            )
            .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?;
            remote_vote_targets_vec.push(voter_id);
        }
    }
    let remote_vote_targets = remote_vote_targets_vec;

    for _ in 0..options.max_attempts {
        host.begin_election_attempt_async().await?;
        let metadata = host.local_vote_metadata_async().await?;
        let election_term = metadata
            .current_term
            .saturating_add(CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP);
        let vote_request = RaftRequestVoteRequest {
            term: election_term,
            candidate_id: try_clone_kv_node_id(
                local_node_id,
                "build async raft election vote request",
            )
            .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?,
            last_log_index: metadata.last_log_index,
            last_log_term: metadata.last_log_term,
        };
        let local_vote = host
            .apply_local_vote_async(RaftRequestVoteRequest {
                term: vote_request.term,
                candidate_id: try_clone_kv_node_id(
                    &vote_request.candidate_id,
                    "apply local async raft vote",
                )
                .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?,
                last_log_index: vote_request.last_log_index,
                last_log_term: vote_request.last_log_term,
            })
            .await?;
        let mut highest_observed_term = local_vote.term.max(election_term);
        let mut granted = RaftNodeIdSet::default();
        if local_vote.vote_granted {
            granted
                .try_insert_clone(local_node_id, "record local async raft vote")
                .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?;
        }

        for (target_index, voter_id) in remote_vote_targets.iter().enumerate() {
            if granted.len() >= quorum_size {
                break;
            }
            if granted.contains(voter_id) {
                continue;
            }
            if !host.should_request_more_remote_votes() {
                break;
            }
            let Ok(response) = host
                .request_remote_vote_async(target_index, voter_id, &vote_request)
                .await
            else {
                continue;
            };
            highest_observed_term = highest_observed_term.max(response.term);
            if response.vote_granted {
                granted
                    .try_insert_clone(voter_id, "record remote async raft vote")
                    .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?;
            }
        }

        if highest_observed_term > election_term {
            host.observe_raft_term_async(highest_observed_term).await?;
            continue;
        }
        if granted.len() < quorum_size {
            return Ok(false);
        }
        host.set_raft_leader_async(election_term).await?;
        return Ok(true);
    }
    Ok(false)
}

/// Storage-side contract for applying a cluster KV command to the local Raft log.
///
/// Implementors own KV/Raft durability only. They must not choose peers, route
/// messages, score routes, or interpret the cluster connectivity graph.
pub trait ClusterKvRaftCommandStorage {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    /// Locks the whole commit sequence against concurrent committers (see
    /// [`lock_cluster_kv_commit_sequence`]). Hosts backed by a shared on-disk
    /// store must implement this; the default (no lock) is only safe for
    /// single-committer hosts such as tests.
    fn lock_cluster_raft_commit_sequence(&mut self) -> Result<Option<File>, Self::Error> {
        Ok(None)
    }

    fn replay_cluster_raft_request_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, Self::Error>;

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, Self::Error>;

    fn record_cluster_raft_request_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &KvAuthorityFailure,
    ) -> Result<(), Self::Error>;

    fn record_cluster_raft_request_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), Self::Error>;

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(&mut self)
    -> Result<(), Self::Error>;

    fn observe_cluster_raft_term(
        &mut self,
        _state: &ClusterBootstrapState,
        _term: u64,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, Self::Error>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftQuorumCommitOptions {
    pub timeout: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvRaftQuorumCommitResult {
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
    pub authority: KvAuthorityProof,
    pub replayed_committed_request: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvTxnCommitResult {
    pub request_id: KvRequestId,
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
    pub authority: KvAuthorityProof,
    pub replayed_committed_request: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterKvRaftTargetReplicationStatus {
    Accepted,
    Rejected,
    Failed,
}

pub struct ClusterKvRaftTargetReplicationAttempt<T, E> {
    pub target: T,
    pub latency_ms: Option<u64>,
    pub result: Result<ClusterRaftAppendResponse, E>,
}

pub type ClusterKvRaftTargetReplicationAttempts<T, E> =
    Vec<ClusterKvRaftTargetReplicationAttempt<T, E>>;

pub type ClusterKvRaftQuorumFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftAppendReplicationOptions {
    pub max_attempts: usize,
    pub timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftAppendReplicationOutcome {
    pub response: ClusterRaftAppendResponse,
    pub watch_event_fanout_needed: bool,
}

/// KV-owned append replication retry driver.
///
/// KV owns Raft log catch-up semantics: initial next-index selection, rejected
/// append backoff, acceptance detection, and watch-event fanout eligibility.
/// Hosts own durability access and transport-specific timeout/sending policy.
pub trait ClusterKvRaftAppendReplicationHost {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;
    type Target: Clone;

    fn last_local_raft_log_index(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<u64, Self::Error>;

    fn raft_peer_progress_next_index_hint(
        &mut self,
        _state: &ClusterBootstrapState,
        _target: &Self::Target,
        _last_local_raft_log_index: u64,
    ) -> Option<u64> {
        None
    }

    fn build_raft_append_request(
        &mut self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, Self::Error>;

    fn append_attempt_timeout(
        &mut self,
        target: &Self::Target,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration;

    fn on_raft_append_sent(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn send_raft_append(
        &mut self,
        target: &Self::Target,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, Self::Error>;
}

pub trait ClusterKvRaftAppendReplicationAsyncHost {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;
    type Target: Clone + Send + Sync;

    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, u64, Self::Error>;

    fn raft_peer_progress_next_index_hint_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        _target: &'a Self::Target,
        _last_local_raft_log_index: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<u64>, Self::Error> {
        Box::pin(async { Ok(None) })
    }

    fn build_raft_append_request_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendRequest, Self::Error>;

    fn append_attempt_timeout(
        &mut self,
        target: &Self::Target,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration;

    fn on_raft_append_sent_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn send_raft_append_async<'a>(
        &'a mut self,
        target: &'a Self::Target,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, Self::Error>;
}

pub fn cluster_kv_raft_next_index_after_rejection(
    next_index: u64,
    request: &ClusterRaftAppendRequest,
    match_index: u64,
) -> u64 {
    let rejected_next_index = match_index.saturating_add(1);
    if rejected_next_index < next_index {
        rejected_next_index
    } else if rejected_next_index == next_index
        && request.snapshot.is_none()
        && match_index == request.append.prev_log_index
        && request.append.prev_log_index == request.append.leader_commit
    {
        // The follower reports that its log ends at our prev_log_index but
        // rejected the committed anchor's term. Backtracking by one would resend
        // that conflicting committed entry. Restart from the catch-up anchor so
        // a compacted leader builds an authoritative snapshot instead.
        1
    } else if match_index >= next_index {
        next_index.saturating_div(2).max(1)
    } else {
        next_index.saturating_sub(1).max(1)
    }
}

pub fn replicate_cluster_kv_raft_to_target_with_host<H>(
    state: &ClusterBootstrapState,
    target: &H::Target,
    watch_events: &[KvEvent],
    options: ClusterKvRaftAppendReplicationOptions,
    host: &mut H,
) -> Result<ClusterKvRaftAppendReplicationOutcome, H::Error>
where
    H: ClusterKvRaftAppendReplicationHost,
{
    let deadline = Instant::now() + options.timeout;
    let last_local_raft_log_index = host.last_local_raft_log_index(state)?;
    let default_next_index = last_local_raft_log_index.saturating_add(1);
    let mut next_index = host
        .raft_peer_progress_next_index_hint(state, target, last_local_raft_log_index)
        .filter(|next_index| (1..=default_next_index).contains(next_index))
        .unwrap_or(default_next_index);
    let mut last_response = None;
    for _ in 0..options.max_attempts {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        let request = host.build_raft_append_request(state, next_index)?;
        let attempt_timeout = host.append_attempt_timeout(target, remaining, &request);
        host.on_raft_append_sent()?;
        let watch_event_fanout_needed =
            cluster_raft_append_request_needs_watch_event_fanout(&request, watch_events);
        let response = host.send_raft_append(target, &request, attempt_timeout)?;
        if response.accepted {
            return Ok(ClusterKvRaftAppendReplicationOutcome {
                response,
                watch_event_fanout_needed,
            });
        }
        next_index = cluster_kv_raft_next_index_after_rejection(
            next_index,
            &request,
            response.append.match_index,
        );
        last_response = Some(response);
    }
    Ok(ClusterKvRaftAppendReplicationOutcome {
        response: last_response.unwrap_or_else(|| ClusterRaftAppendResponse {
            accepted: false,
            detail: format!(
                "raft append did not complete before {}ms catch-up budget",
                options.timeout.as_millis()
            ),
            state: state.clone(),
            append: RaftAppendEntriesResponse {
                term: 0,
                success: false,
                match_index: 0,
            },
        }),
        watch_event_fanout_needed: false,
    })
}

pub async fn replicate_cluster_kv_raft_to_target_async_with_host<H>(
    state: &ClusterBootstrapState,
    target: &H::Target,
    watch_events: &[KvEvent],
    options: ClusterKvRaftAppendReplicationOptions,
    host: &mut H,
) -> Result<ClusterKvRaftAppendReplicationOutcome, H::Error>
where
    H: ClusterKvRaftAppendReplicationAsyncHost,
{
    let deadline = Instant::now() + options.timeout;
    let last_local_raft_log_index = host.last_local_raft_log_index_async(state).await?;
    let default_next_index = last_local_raft_log_index.saturating_add(1);
    let mut next_index = host
        .raft_peer_progress_next_index_hint_async(state, target, last_local_raft_log_index)
        .await?
        .filter(|next_index| (1..=default_next_index).contains(next_index))
        .unwrap_or(default_next_index);
    let mut last_response = None;
    for _ in 0..options.max_attempts {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        let request = host
            .build_raft_append_request_async(state, next_index)
            .await?;
        let attempt_timeout = host.append_attempt_timeout(target, remaining, &request);
        host.on_raft_append_sent_async().await?;
        let watch_event_fanout_needed =
            cluster_raft_append_request_needs_watch_event_fanout(&request, watch_events);
        let response = host
            .send_raft_append_async(target, &request, attempt_timeout)
            .await?;
        if response.accepted {
            return Ok(ClusterKvRaftAppendReplicationOutcome {
                response,
                watch_event_fanout_needed,
            });
        }
        next_index = cluster_kv_raft_next_index_after_rejection(
            next_index,
            &request,
            response.append.match_index,
        );
        last_response = Some(response);
    }
    Ok(ClusterKvRaftAppendReplicationOutcome {
        response: last_response.unwrap_or_else(|| ClusterRaftAppendResponse {
            accepted: false,
            detail: format!(
                "raft append did not complete before {}ms catch-up budget",
                options.timeout.as_millis()
            ),
            state: state.clone(),
            append: RaftAppendEntriesResponse {
                term: 0,
                success: false,
                match_index: 0,
            },
        }),
        watch_event_fanout_needed: false,
    })
}

/// Storage-side Raft quorum commit driver.
///
/// KV owns the Raft append/commit/rollback decision flow and quorum accounting.
/// The host owns all topology-specific work: choosing remote voter targets,
/// transmitting append RPCs, merging any peer cluster-state observations, and
/// reporting route/connectivity observations.
pub trait ClusterKvRaftQuorumCommitHost: ClusterKvRaftCommandStorage {
    type Target: Clone;

    /// Returns true only when durable local Raft state records this node as the
    /// elected leader for the current term. Reachability or routing preference
    /// alone is not sufficient.
    fn local_has_verified_raft_leadership(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, Self::Error>;

    fn remote_voter_targets(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<Vec<Self::Target>, Self::Error>;

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId;

    fn target_description(&self, target: &Self::Target) -> String {
        self.target_node_id(target).to_string()
    }

    fn replicate_cluster_raft_to_target(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, Self::Error>;

    fn record_target_replication_status(
        &mut self,
        _state: &ClusterBootstrapState,
        _target: &Self::Target,
        _status: ClusterKvRaftTargetReplicationStatus,
        _latency_ms: Option<u64>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn observe_accepted_append_state(
        &mut self,
        _state: &mut ClusterBootstrapState,
        _target: &Self::Target,
        _peer_state: ClusterBootstrapState,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<Self::Target>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn cluster_authority_error(&mut self, failure: KvAuthorityFailure) -> Self::Error;
}

/// Async storage-side Raft quorum commit host.
///
/// This mirrors `ClusterKvRaftQuorumCommitHost`, but every operation that may
/// cross an async storage or transport boundary is supplied by the caller. KV
/// still owns the quorum decision flow; the host owns transport and topology.
pub trait ClusterKvRaftQuorumCommitAsyncHost: Send {
    type Error: ToString + Send;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;
    type Target: Clone + Send + Sync;

    fn local_has_verified_raft_leadership_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, bool, Self::Error>;

    /// Async counterpart of
    /// [`ClusterKvRaftCommandStorage::lock_cluster_raft_commit_sequence`].
    fn lock_cluster_raft_commit_sequence_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<File>, Self::Error> {
        Box::pin(async { Ok(None) })
    }

    fn replay_cluster_raft_request_if_committed_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<(KvSnapshot, KvCommittedRequestRecord)>, Self::Error>;

    fn append_cluster_raft_request_uncommitted_async<'a>(
        &'a mut self,
        request: KvCommandRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, KvLogEntry, Self::Error>;

    fn record_cluster_raft_request_rejected_before_commit_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
        failure: &'a KvAuthorityFailure,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    fn record_cluster_raft_request_unknown_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
        detail: &'a str,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    fn observe_cluster_raft_term_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        _term: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn commit_cluster_raft_index_with_events_async<'a>(
        &'a mut self,
        index: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftCommitResult, Self::Error>;

    fn remote_voter_targets_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, Vec<Self::Target>, Self::Error>;

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId;

    fn target_description(&self, target: &Self::Target) -> String {
        self.target_node_id(target).to_string()
    }

    fn replicate_cluster_raft_to_target_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target: &'a Self::Target,
        timeout: Duration,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, Self::Error>;

    /// Replicates to enough remote voters to satisfy the requested remote
    /// acknowledgement count. The default preserves compatibility for hosts
    /// that can only issue one transport call at a time; network hosts should
    /// override this to race independent voter RPCs and return as soon as the
    /// required acknowledgements arrive.
    fn replicate_cluster_raft_to_quorum_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        targets: Vec<Self::Target>,
        required_remote_acks: usize,
        timeout: Duration,
    ) -> ClusterKvRaftQuorumFuture<
        'a,
        ClusterKvRaftTargetReplicationAttempts<Self::Target, Self::Error>,
        Self::Error,
    > {
        Box::pin(async move {
            let deadline = Instant::now() + timeout;
            let mut accepted = 0_usize;
            let mut attempts = Vec::new();
            for target in targets {
                if accepted >= required_remote_acks {
                    break;
                }
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or_default();
                if remaining.is_zero() {
                    break;
                }
                let started = Instant::now();
                let result = self
                    .replicate_cluster_raft_to_target_async(state, &target, remaining)
                    .await;
                let latency_ms =
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64);
                if result.as_ref().is_ok_and(|response| response.accepted) {
                    accepted = accepted.saturating_add(1);
                }
                try_reserve_kv_items(&mut attempts, 1, "record async raft replication attempt")
                    .map_err(Self::kv_error)?;
                attempts.push(ClusterKvRaftTargetReplicationAttempt {
                    target,
                    latency_ms,
                    result,
                });
            }
            Ok(attempts)
        })
    }

    fn record_target_replication_status_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        _target: &'a Self::Target,
        _status: ClusterKvRaftTargetReplicationStatus,
        _latency_ms: Option<u64>,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn observe_accepted_append_state_async<'a>(
        &'a mut self,
        _state: &'a mut ClusterBootstrapState,
        _target: &'a Self::Target,
        _peer_state: ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<Self::Target>,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn cluster_authority_error(&mut self, failure: KvAuthorityFailure) -> Self::Error;
}

pub fn commit_cluster_kv_raft_command_with_quorum<H>(
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCommitResult, H::Error>
where
    H: ClusterKvRaftQuorumCommitHost,
    H::Error: ToString,
{
    let mut command_metrics = KvCommandMetricGuard::start(&request.command);
    let mut concurrency_metrics = KvCommitConcurrencyMetricGuard::queued();
    let _commit_sequence_lock = {
        let _phase = KvCommandPhaseMetricGuard::start("commit_sequence_wait");
        host.lock_cluster_raft_commit_sequence()?
    };
    concurrency_metrics.acquired();
    let replay = {
        let _phase = KvCommandPhaseMetricGuard::start("request_replay");
        host.replay_cluster_raft_request_if_committed(&request)?
    };
    if let Some((snapshot, record)) = replay {
        command_metrics.finish("replayed");
        return Ok(ClusterKvRaftQuorumCommitResult {
            snapshot,
            txn_response: record.txn_response.clone(),
            authority: KvAuthorityProof {
                term: record.term,
                commit_index: record.log_index,
                revision: record.revision,
                basis: KvAuthorityBasis::ExactCommittedRequest,
            },
            replayed_committed_request: true,
        });
    }
    let local_has_leadership = {
        let _phase = KvCommandPhaseMetricGuard::start("leadership_check");
        host.local_has_verified_raft_leadership(state)?
    };
    if !local_has_leadership {
        let failure = cluster_kv_authority_failure(
            state,
            if cluster_kv_raft_local_is_voter(state) {
                KvAuthorityFailureReason::VoterRejected
            } else {
                KvAuthorityFailureReason::LocalNodeNotVoter
            },
            "local node lacks verified current-term Raft leadership".to_string(),
            0,
            KvRevision::ZERO,
            usize::from(cluster_kv_raft_local_is_voter(state)),
            None,
        );
        command_metrics.finish("not_leader");
        host.record_cluster_raft_request_rejected_before_commit(&request, &failure)?;
        return Err(host.cluster_authority_error(failure));
    }
    let request_for_outcome = request.clone();
    let entry = {
        let _phase = KvCommandPhaseMetricGuard::start("local_append");
        host.append_cluster_raft_request_uncommitted(request)?
    };

    let mut voter_acks = RaftNodeIdSet::default();
    let mut fanout_node_ids = RaftNodeIdSet::default();
    let mut fanout_targets = Vec::new();
    if cluster_kv_raft_local_is_voter(state) {
        voter_acks
            .try_insert_clone(&state.local_node_id, "record local raft commit ack")
            .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?;
    }
    let quorum_size = cluster_kv_raft_quorum_size_for_state(state);
    let mut failures = Vec::new();
    let mut higher_term_observed: Option<u64> = None;
    let deadline = Instant::now() + options.timeout;
    let quorum_metrics = KvCommandPhaseMetricGuard::start("quorum");
    let targets = host.remote_voter_targets(state)?;
    for target in &targets {
        if voter_acks.len() >= quorum_size {
            break;
        }
        if voter_acks.contains(host.target_node_id(target)) {
            continue;
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            failures.push(format!(
                "raft command did not reach quorum before {}ms replication budget",
                options.timeout.as_millis()
            ));
            break;
        }
        let started = Instant::now();
        match host.replicate_cluster_raft_to_target(state, target, remaining) {
            Ok(response) if response.accepted => {
                host.record_target_replication_status(
                    state,
                    target,
                    ClusterKvRaftTargetReplicationStatus::Accepted,
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
                )?;
                voter_acks
                    .try_insert_clone(host.target_node_id(target), "record remote raft commit ack")
                    .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?;
                host.observe_accepted_append_state(state, target, response.state)?;
                if fanout_node_ids
                    .try_insert_clone(
                        host.target_node_id(target),
                        "record accepted raft fanout target",
                    )
                    .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?
                {
                    try_reserve_kv_items(
                        &mut fanout_targets,
                        1,
                        "record accepted raft fanout target",
                    )
                    .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?;
                    fanout_targets.push(target.clone());
                }
            }
            Ok(response) => {
                if response.append.term > entry.term {
                    higher_term_observed = Some(
                        higher_term_observed
                            .unwrap_or_default()
                            .max(response.append.term),
                    );
                }
                host.observe_cluster_raft_term(state, response.append.term)?;
                host.record_target_replication_status(
                    state,
                    target,
                    ClusterKvRaftTargetReplicationStatus::Rejected,
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
                )?;
                failures.push(format!(
                    "{} rejected append: {} (term {}, match_index {})",
                    host.target_description(target),
                    response.detail,
                    response.append.term,
                    response.append.match_index
                ));
            }
            Err(error) => {
                host.record_target_replication_status(
                    state,
                    target,
                    ClusterKvRaftTargetReplicationStatus::Failed,
                    None,
                )?;
                failures.push(format!(
                    "{} failed: {}",
                    host.target_description(target),
                    error.to_string()
                ));
            }
        }
    }
    drop(quorum_metrics);
    metrics::histogram!("sinomo.kv.quorum.acks", "kind" => "received")
        .record(voter_acks.len() as f64);
    metrics::histogram!("sinomo.kv.quorum.acks", "kind" => "required").record(quorum_size as f64);

    if voter_acks.len() < quorum_size || higher_term_observed.is_some() {
        let _ = host.discard_cluster_raft_uncommitted_tail_and_clear_leader();
        let detail = format!(
            "raft command at index {} replicated to {} voters, {} required{}{}",
            entry.index,
            voter_acks.len(),
            quorum_size,
            higher_term_observed
                .map(|term| format!("; observed higher term {term}"))
                .unwrap_or_default(),
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        );
        let reason = if higher_term_observed.is_some() {
            command_metrics.finish("higher_term");
            KvAuthorityFailureReason::HigherTermObserved
        } else if Instant::now() >= deadline {
            command_metrics.finish("timeout");
            KvAuthorityFailureReason::Timeout
        } else {
            command_metrics.finish("no_quorum");
            KvAuthorityFailureReason::NoQuorum
        };
        host.record_cluster_raft_request_unknown(&request_for_outcome, &detail)?;
        return Err(host.cluster_authority_error(cluster_kv_authority_failure(
            state,
            reason,
            detail,
            entry.term,
            KvRevision::ZERO,
            voter_acks.len(),
            Some(state.local_node_id.clone()),
        )));
    }

    let commit = {
        let _phase = KvCommandPhaseMetricGuard::start("local_apply");
        host.commit_cluster_raft_index_with_events(entry.index)?
    };
    command_metrics.finish("committed");
    for target in targets {
        if fanout_node_ids
            .try_insert_clone(
                host.target_node_id(&target),
                "dedupe committed raft fanout target",
            )
            .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?
        {
            try_reserve_kv_items(
                &mut fanout_targets,
                1,
                "dedupe committed raft fanout target",
            )
            .map_err(<H as ClusterKvRaftCommandStorage>::kv_error)?;
            fanout_targets.push(target);
        }
    }
    // The fan-out replicates the committed history to every member (learners
    // included — they converge event-driven on commit), so it runs even when
    // there are no REMOTE VOTER targets (single-voter clusters).
    {
        let _phase = KvCommandPhaseMetricGuard::start("fanout_dispatch");
        host.fan_out_committed_cluster_raft_command(
            state.clone(),
            commit.events.clone(),
            fanout_targets,
        )?;
    }
    Ok(ClusterKvRaftQuorumCommitResult {
        snapshot: commit.snapshot,
        txn_response: commit.txn_response,
        authority: KvAuthorityProof {
            term: entry.term,
            commit_index: entry.index,
            revision: commit.committed_request.revision,
            basis: KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: voter_acks.len(),
                required_quorum: quorum_size,
            },
        },
        replayed_committed_request: false,
    })
}

pub async fn commit_cluster_kv_raft_command_with_quorum_async<H>(
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCommitResult, H::Error>
where
    H: ClusterKvRaftQuorumCommitAsyncHost,
{
    let mut command_metrics = KvCommandMetricGuard::start(&request.command);
    let mut concurrency_metrics = KvCommitConcurrencyMetricGuard::queued();
    let _commit_sequence_lock = {
        let _phase = KvCommandPhaseMetricGuard::start("commit_sequence_wait");
        host.lock_cluster_raft_commit_sequence_async().await?
    };
    concurrency_metrics.acquired();
    let replay = {
        let _phase = KvCommandPhaseMetricGuard::start("request_replay");
        host.replay_cluster_raft_request_if_committed_async(&request)
            .await?
    };
    if let Some((snapshot, record)) = replay {
        command_metrics.finish("replayed");
        return Ok(ClusterKvRaftQuorumCommitResult {
            snapshot,
            txn_response: record.txn_response.clone(),
            authority: KvAuthorityProof {
                term: record.term,
                commit_index: record.log_index,
                revision: record.revision,
                basis: KvAuthorityBasis::ExactCommittedRequest,
            },
            replayed_committed_request: true,
        });
    }
    let local_has_leadership = {
        let _phase = KvCommandPhaseMetricGuard::start("leadership_check");
        host.local_has_verified_raft_leadership_async(state).await?
    };
    if !local_has_leadership {
        let failure = cluster_kv_authority_failure(
            state,
            if cluster_kv_raft_local_is_voter(state) {
                KvAuthorityFailureReason::VoterRejected
            } else {
                KvAuthorityFailureReason::LocalNodeNotVoter
            },
            "local node lacks verified current-term Raft leadership".to_string(),
            0,
            KvRevision::ZERO,
            usize::from(cluster_kv_raft_local_is_voter(state)),
            None,
        );
        command_metrics.finish("not_leader");
        host.record_cluster_raft_request_rejected_before_commit_async(&request, &failure)
            .await?;
        return Err(host.cluster_authority_error(failure));
    }
    let request_for_outcome = request.clone();
    let entry = {
        let _phase = KvCommandPhaseMetricGuard::start("local_append");
        host.append_cluster_raft_request_uncommitted_async(request)
            .await?
    };

    let mut voter_acks = RaftNodeIdSet::default();
    let mut fanout_node_ids = RaftNodeIdSet::default();
    let mut fanout_targets = Vec::new();
    if cluster_kv_raft_local_is_voter(state) {
        voter_acks
            .try_insert_clone(&state.local_node_id, "record local async raft commit ack")
            .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?;
    }
    let quorum_size = cluster_kv_raft_quorum_size_for_state(state);
    let mut failures = Vec::new();
    let mut higher_term_observed: Option<u64> = None;
    let deadline = Instant::now() + options.timeout;
    let quorum_metrics = KvCommandPhaseMetricGuard::start("quorum");
    let targets = host.remote_voter_targets_async(state).await?;
    let replication_targets = targets
        .iter()
        .filter(|target| !voter_acks.contains(host.target_node_id(target)))
        .cloned()
        .collect::<Vec<_>>();
    let required_remote_acks = quorum_size.saturating_sub(voter_acks.len());
    let attempts = host
        .replicate_cluster_raft_to_quorum_async(
            state,
            replication_targets,
            required_remote_acks,
            options.timeout,
        )
        .await?;
    for attempt in attempts {
        let target = attempt.target;
        match attempt.result {
            Ok(response) if response.accepted => {
                host.record_target_replication_status_async(
                    state,
                    &target,
                    ClusterKvRaftTargetReplicationStatus::Accepted,
                    attempt.latency_ms,
                )
                .await?;
                voter_acks
                    .try_insert_clone(
                        host.target_node_id(&target),
                        "record remote async raft commit ack",
                    )
                    .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?;
                host.observe_accepted_append_state_async(state, &target, response.state)
                    .await?;
                if fanout_node_ids
                    .try_insert_clone(
                        host.target_node_id(&target),
                        "record accepted async raft fanout target",
                    )
                    .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?
                {
                    try_reserve_kv_items(
                        &mut fanout_targets,
                        1,
                        "record accepted async raft fanout target",
                    )
                    .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?;
                    fanout_targets.push(target);
                }
            }
            Ok(response) => {
                if response.append.term > entry.term {
                    higher_term_observed = Some(
                        higher_term_observed
                            .unwrap_or_default()
                            .max(response.append.term),
                    );
                }
                host.observe_cluster_raft_term_async(state, response.append.term)
                    .await?;
                host.record_target_replication_status_async(
                    state,
                    &target,
                    ClusterKvRaftTargetReplicationStatus::Rejected,
                    attempt.latency_ms,
                )
                .await?;
                failures.push(format!(
                    "{} rejected append: {} (term {}, match_index {})",
                    host.target_description(&target),
                    response.detail,
                    response.append.term,
                    response.append.match_index
                ));
            }
            Err(error) => {
                host.record_target_replication_status_async(
                    state,
                    &target,
                    ClusterKvRaftTargetReplicationStatus::Failed,
                    attempt.latency_ms,
                )
                .await?;
                failures.push(format!(
                    "{} failed: {}",
                    host.target_description(&target),
                    error.to_string()
                ));
            }
        }
    }
    drop(quorum_metrics);
    metrics::histogram!("sinomo.kv.quorum.acks", "kind" => "received")
        .record(voter_acks.len() as f64);
    metrics::histogram!("sinomo.kv.quorum.acks", "kind" => "required").record(quorum_size as f64);

    if voter_acks.len() < quorum_size && Instant::now() >= deadline {
        failures.push(format!(
            "raft command did not reach quorum before {}ms replication budget",
            options.timeout.as_millis()
        ));
    }

    if voter_acks.len() < quorum_size || higher_term_observed.is_some() {
        let _ = host
            .discard_cluster_raft_uncommitted_tail_and_clear_leader_async()
            .await;
        let detail = format!(
            "raft command at index {} replicated to {} voters, {} required{}{}",
            entry.index,
            voter_acks.len(),
            quorum_size,
            higher_term_observed
                .map(|term| format!("; observed higher term {term}"))
                .unwrap_or_default(),
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        );
        let reason = if higher_term_observed.is_some() {
            command_metrics.finish("higher_term");
            KvAuthorityFailureReason::HigherTermObserved
        } else if Instant::now() >= deadline {
            command_metrics.finish("timeout");
            KvAuthorityFailureReason::Timeout
        } else {
            command_metrics.finish("no_quorum");
            KvAuthorityFailureReason::NoQuorum
        };
        host.record_cluster_raft_request_unknown_async(&request_for_outcome, &detail)
            .await?;
        return Err(host.cluster_authority_error(cluster_kv_authority_failure(
            state,
            reason,
            detail,
            entry.term,
            KvRevision::ZERO,
            voter_acks.len(),
            Some(state.local_node_id.clone()),
        )));
    }

    let commit = {
        let _phase = KvCommandPhaseMetricGuard::start("local_apply");
        host.commit_cluster_raft_index_with_events_async(entry.index)
            .await?
    };
    command_metrics.finish("committed");
    for target in targets {
        if fanout_node_ids
            .try_insert_clone(
                host.target_node_id(&target),
                "dedupe committed async raft fanout target",
            )
            .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?
        {
            try_reserve_kv_items(
                &mut fanout_targets,
                1,
                "dedupe committed async raft fanout target",
            )
            .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error)?;
            fanout_targets.push(target);
        }
    }
    {
        let _phase = KvCommandPhaseMetricGuard::start("fanout_dispatch");
        host.fan_out_committed_cluster_raft_command_async(
            state.clone(),
            commit.events.clone(),
            fanout_targets,
        )
        .await?;
    }
    Ok(ClusterKvRaftQuorumCommitResult {
        snapshot: commit.snapshot,
        txn_response: commit.txn_response,
        authority: KvAuthorityProof {
            term: entry.term,
            commit_index: entry.index,
            revision: commit.committed_request.revision,
            basis: KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: voter_acks.len(),
                required_quorum: quorum_size,
            },
        },
        replayed_committed_request: false,
    })
}

/// Establishes a fresh current-term quorum barrier and returns the snapshot at
/// that barrier. A new request id is intentionally generated for every call so
/// an old committed barrier can never be mistaken for current authority.
pub fn read_cluster_kv_authoritative_snapshot_with_host<H>(
    state: &mut ClusterBootstrapState,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<KvSnapshot>, H::Error>
where
    H: ClusterKvRaftQuorumCommitHost,
    H::Error: ToString,
{
    let result = commit_cluster_kv_raft_command_with_quorum(
        state,
        KvCommandRequest::generated(KvCommand::Barrier),
        options,
        host,
    )?;
    Ok(KvAuthoritativeRead {
        value: result.snapshot,
        authority: result.authority,
    })
}

pub async fn read_cluster_kv_authoritative_snapshot_async_with_host<H>(
    state: &mut ClusterBootstrapState,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<KvSnapshot>, H::Error>
where
    H: ClusterKvRaftQuorumCommitAsyncHost,
{
    let result = commit_cluster_kv_raft_command_with_quorum_async(
        state,
        KvCommandRequest::generated(KvCommand::Barrier),
        options,
        host,
    )
    .await?;
    Ok(KvAuthoritativeRead {
        value: result.snapshot,
        authority: result.authority,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvTxnQuorumDriverOptions {
    pub allow_forwarding: bool,
}

const CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS: usize = 2;

fn invalid_forwarded_cluster_kv_commit(detail: impl Into<String>) -> KvError {
    KvError::AuthorityUnavailable {
        detail: detail.into(),
    }
}

fn validate_cluster_kv_txn_commit_response(
    state: &ClusterBootstrapState,
    request: &KvTxnRequest,
    response: &ClusterKvTxnCommitResult,
) -> Result<(), KvError> {
    if response.request_id != request.request_id {
        return Err(invalid_forwarded_cluster_kv_commit(format!(
            "cluster KV commit response request ID {} does not match submitted request ID {}",
            response.request_id, request.request_id
        )));
    }
    response.snapshot.validate_resource_bounds()?;
    let txn_response = response.txn_response.as_ref().ok_or_else(|| {
        invalid_forwarded_cluster_kv_commit(
            "cluster KV transaction commit response omitted its transaction result",
        )
    })?;
    if response.authority.term == 0 || response.authority.commit_index == 0 {
        return Err(invalid_forwarded_cluster_kv_commit(
            "cluster KV commit response has an empty authority term or commit index",
        ));
    }
    match response.authority.basis {
        KvAuthorityBasis::CurrentTermQuorum {
            voter_acks,
            required_quorum,
        } => {
            let voter_count = cluster_kv_raft_voter_ids(state)?.len();
            let expected_quorum = cluster_kv_raft_quorum_size(voter_count);
            if response.replayed_committed_request {
                return Err(invalid_forwarded_cluster_kv_commit(
                    "fresh-quorum commit response was marked as an exact committed replay",
                ));
            }
            if response.authority.term < state.term {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "cluster KV commit response term {} is older than observed term {}",
                    response.authority.term, state.term
                )));
            }
            if required_quorum != expected_quorum
                || voter_acks < expected_quorum
                || voter_acks > voter_count
            {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "cluster KV commit response reported {voter_acks} voter acknowledgements and quorum {required_quorum}, but membership has {voter_count} voters and requires {expected_quorum}"
                )));
            }
            if response.authority.revision != response.snapshot.revision {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "cluster KV commit proof revision {} does not match returned snapshot revision {}",
                    response.authority.revision.0, response.snapshot.revision.0
                )));
            }
            if txn_response.revision != response.authority.revision {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "cluster KV transaction result revision {} does not match authority revision {}",
                    txn_response.revision.0, response.authority.revision.0
                )));
            }
        }
        KvAuthorityBasis::ExactCommittedRequest => {
            if !response.replayed_committed_request {
                return Err(invalid_forwarded_cluster_kv_commit(
                    "exact committed-request authority was not marked as a replay",
                ));
            }
            let digest = request.clone().into_command_request().digest()?;
            let record = response
                .snapshot
                .committed_requests
                .iter()
                .find(|record| record.request_id == request.request_id)
                .ok_or_else(|| {
                    invalid_forwarded_cluster_kv_commit(format!(
                        "exact committed-request response omitted durable record {}",
                        request.request_id
                    ))
                })?;
            if record.command_digest != digest
                || record.term != response.authority.term
                || record.log_index != response.authority.commit_index
                || record.revision != response.authority.revision
                || record.txn_response != response.txn_response
                || response.snapshot.revision < record.revision
            {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "exact committed-request proof for {} does not match its durable snapshot record",
                    request.request_id
                )));
            }
        }
    }
    Ok(())
}

pub trait ClusterKvTxnQuorumHost {
    type Error: ToString;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    fn refresh_current_cluster_kv_store(
        &mut self,
        _state: &ClusterBootstrapState,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn local_is_raft_leader(&mut self, state: &ClusterBootstrapState) -> Result<bool, Self::Error>;

    fn forward_cluster_kv_txn_to_voter(
        &mut self,
        state: &ClusterBootstrapState,
        request: &KvTxnRequest,
    ) -> Result<Option<ClusterKvTxnCommitResult>, Self::Error>;

    fn elect_cluster_raft_leader(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, Self::Error>;

    fn reachable_voter_count(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<usize, Self::Error>;

    fn commit_cluster_raft_txn_with_quorum(
        &mut self,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<ClusterKvRaftQuorumCommitResult, Self::Error>;

    fn cluster_quorum_commit_error_allows_retry(&mut self, _error: &Self::Error) -> bool {
        false
    }

    fn cluster_authority_error(&mut self, failure: KvAuthorityFailure) -> Self::Error;
}

pub trait ClusterKvTxnQuorumAsyncHost {
    type Error: ToString;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    fn refresh_current_cluster_kv_store_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn local_is_raft_leader_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, bool, Self::Error>;

    fn forward_cluster_kv_txn_to_voter_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        request: &'a KvTxnRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<ClusterKvTxnCommitResult>, Self::Error>;

    fn elect_cluster_raft_leader_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, bool, Self::Error>;

    fn reachable_voter_count_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, usize, Self::Error>;

    fn commit_cluster_raft_txn_with_quorum_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftQuorumCommitResult, Self::Error>;

    fn cluster_quorum_commit_error_allows_retry(&mut self, _error: &Self::Error) -> bool {
        false
    }

    fn cluster_authority_error(&mut self, failure: KvAuthorityFailure) -> Self::Error;
}

pub fn commit_cluster_kv_txn_with_quorum_with_host<H>(
    state: &mut ClusterBootstrapState,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumDriverOptions,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, H::Error>
where
    H: ClusterKvTxnQuorumHost,
{
    let _ = host.refresh_current_cluster_kv_store(state);
    if !cluster_kv_raft_local_is_voter(state) {
        if options.allow_forwarding
            && let Some(response) = host.forward_cluster_kv_txn_to_voter(state, &request)?
        {
            validate_cluster_kv_txn_commit_response(state, &request, &response)
                .map_err(H::kv_error)?;
            return Ok(response);
        }
        let reachable_voters = host.reachable_voter_count(state)?;
        let detail = format!(
            "local node is not a Raft voter and could not forward to a voter ({} reachable voters, {} required)",
            reachable_voters,
            cluster_kv_raft_quorum_size_for_state(state)
        );
        return Err(host.cluster_authority_error(cluster_kv_authority_failure(
            state,
            KvAuthorityFailureReason::LocalNodeNotVoter,
            detail,
            0,
            KvRevision::ZERO,
            reachable_voters,
            None,
        )));
    }

    let mut previous_commit_failure: Option<H::Error> = None;
    for attempt in 0..CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS {
        if !host.local_is_raft_leader(state)? {
            if !options.allow_forwarding {
                let reachable_voters = host.reachable_voter_count(state)?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    "forwarded cluster KV transaction reached a non-leader voter; forwarded requests do not start elections".to_string(),
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
            let forwarding_failure = match host.forward_cluster_kv_txn_to_voter(state, &request) {
                Ok(Some(response)) => {
                    validate_cluster_kv_txn_commit_response(state, &request, &response)
                        .map_err(H::kv_error)?;
                    return Ok(response);
                }
                Ok(None) => None,
                Err(error) => Some(error),
            };
            if !host.elect_cluster_raft_leader(state)? {
                let mut detail = if let Some(forwarding_failure) = forwarding_failure {
                    format!(
                        "local node is not the Raft leader and could not win quorum; forwarding also failed: {}",
                        forwarding_failure.to_string()
                    )
                } else {
                    "local node is not the Raft leader and could not win quorum".to_string()
                };
                if let Some(error) = previous_commit_failure.as_ref() {
                    detail.push_str("; previous local commit failed: ");
                    detail.push_str(&error.to_string());
                }
                let reachable_voters = host.reachable_voter_count(state)?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    detail,
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
        }
        if !host.local_is_raft_leader(state)? {
            let mut detail =
                "local node is not the Raft leader and could not win quorum".to_string();
            if let Some(error) = previous_commit_failure.as_ref() {
                detail.push_str("; previous local commit failed: ");
                detail.push_str(&error.to_string());
            }
            let reachable_voters = host.reachable_voter_count(state)?;
            return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                state,
                KvAuthorityFailureReason::NoQuorum,
                detail,
                0,
                KvRevision::ZERO,
                reachable_voters,
                None,
            )));
        }
        match host.commit_cluster_raft_txn_with_quorum(state, request.clone()) {
            Ok(commit) => {
                return Ok(ClusterKvTxnCommitResult {
                    request_id: request.request_id.clone(),
                    snapshot: commit.snapshot,
                    txn_response: commit.txn_response,
                    authority: commit.authority,
                    replayed_committed_request: commit.replayed_committed_request,
                });
            }
            Err(error)
                if attempt + 1 < CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS
                    && options.allow_forwarding
                    && host.cluster_quorum_commit_error_allows_retry(&error) =>
            {
                previous_commit_failure = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    let detail = previous_commit_failure
        .as_ref()
        .map(|error| {
            format!(
                "local node could not commit after retrying Raft leadership: {}",
                error.to_string()
            )
        })
        .unwrap_or_else(|| {
            "local node could not commit after retrying Raft leadership".to_string()
        });
    let reachable_voters = host.reachable_voter_count(state)?;
    Err(host.cluster_authority_error(cluster_kv_authority_failure(
        state,
        KvAuthorityFailureReason::NoQuorum,
        detail,
        0,
        KvRevision::ZERO,
        reachable_voters,
        None,
    )))
}

pub async fn commit_cluster_kv_txn_with_quorum_async_with_host<H>(
    state: &mut ClusterBootstrapState,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumDriverOptions,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, H::Error>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let _ = host.refresh_current_cluster_kv_store_async(state).await;
    if !cluster_kv_raft_local_is_voter(state) {
        if options.allow_forwarding
            && let Some(response) = host
                .forward_cluster_kv_txn_to_voter_async(state, &request)
                .await?
        {
            validate_cluster_kv_txn_commit_response(state, &request, &response)
                .map_err(H::kv_error)?;
            return Ok(response);
        }
        let reachable_voters = host.reachable_voter_count_async(state).await?;
        let detail = format!(
            "local node is not a Raft voter and could not forward to a voter ({} reachable voters, {} required)",
            reachable_voters,
            cluster_kv_raft_quorum_size_for_state(state)
        );
        return Err(host.cluster_authority_error(cluster_kv_authority_failure(
            state,
            KvAuthorityFailureReason::LocalNodeNotVoter,
            detail,
            0,
            KvRevision::ZERO,
            reachable_voters,
            None,
        )));
    }

    let mut previous_commit_failure: Option<H::Error> = None;
    for attempt in 0..CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS {
        if !host.local_is_raft_leader_async(state).await? {
            if !options.allow_forwarding {
                let reachable_voters = host.reachable_voter_count_async(state).await?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    "forwarded cluster KV transaction reached a non-leader voter; forwarded requests do not start elections".to_string(),
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
            let forwarding_failure = match host
                .forward_cluster_kv_txn_to_voter_async(state, &request)
                .await
            {
                Ok(Some(response)) => {
                    validate_cluster_kv_txn_commit_response(state, &request, &response)
                        .map_err(H::kv_error)?;
                    return Ok(response);
                }
                Ok(None) => None,
                Err(error) => Some(error),
            };
            if !host.elect_cluster_raft_leader_async(state).await? {
                let mut detail = if let Some(forwarding_failure) = forwarding_failure {
                    format!(
                        "local node is not the Raft leader and could not win quorum; forwarding also failed: {}",
                        forwarding_failure.to_string()
                    )
                } else {
                    "local node is not the Raft leader and could not win quorum".to_string()
                };
                if let Some(error) = previous_commit_failure.as_ref() {
                    detail.push_str("; previous local commit failed: ");
                    detail.push_str(&error.to_string());
                }
                let reachable_voters = host.reachable_voter_count_async(state).await?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    detail,
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
        }
        if !host.local_is_raft_leader_async(state).await? {
            let mut detail =
                "local node is not the Raft leader and could not win quorum".to_string();
            if let Some(error) = previous_commit_failure.as_ref() {
                detail.push_str("; previous local commit failed: ");
                detail.push_str(&error.to_string());
            }
            let reachable_voters = host.reachable_voter_count_async(state).await?;
            return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                state,
                KvAuthorityFailureReason::NoQuorum,
                detail,
                0,
                KvRevision::ZERO,
                reachable_voters,
                None,
            )));
        }
        match host
            .commit_cluster_raft_txn_with_quorum_async(state, request.clone())
            .await
        {
            Ok(commit) => {
                return Ok(ClusterKvTxnCommitResult {
                    request_id: request.request_id.clone(),
                    snapshot: commit.snapshot,
                    txn_response: commit.txn_response,
                    authority: commit.authority,
                    replayed_committed_request: commit.replayed_committed_request,
                });
            }
            Err(error)
                if attempt + 1 < CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS
                    && options.allow_forwarding
                    && host.cluster_quorum_commit_error_allows_retry(&error) =>
            {
                previous_commit_failure = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    let detail = previous_commit_failure
        .as_ref()
        .map(|error| {
            format!(
                "local node could not commit after retrying Raft leadership: {}",
                error.to_string()
            )
        })
        .unwrap_or_else(|| {
            "local node could not commit after retrying Raft leadership".to_string()
        });
    let reachable_voters = host.reachable_voter_count_async(state).await?;
    Err(host.cluster_authority_error(cluster_kv_authority_failure(
        state,
        KvAuthorityFailureReason::NoQuorum,
        detail,
        0,
        KvRevision::ZERO,
        reachable_voters,
        None,
    )))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftCompactionPolicy {
    pub max_snapshot_bytes: u64,
    pub max_snapshot_events: usize,
}

pub fn default_cluster_kv_raft_compaction_policy() -> ClusterKvRaftCompactionPolicy {
    ClusterKvRaftCompactionPolicy {
        max_snapshot_bytes: cluster_kv_raft_snapshot_byte_limit(),
        max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
    }
}

pub fn cluster_kv_raft_snapshot_byte_limit() -> u64 {
    #[cfg(test)]
    {
        CLUSTER_KV_RAFT_SNAPSHOT_BYTES_LIMIT_FOR_TEST.load(Ordering::Relaxed)
    }
    #[cfg(not(test))]
    {
        MAX_CLUSTER_KV_RAFT_SNAPSHOT_BYTES
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn set_cluster_kv_raft_snapshot_byte_limit_for_test(limit: u64) -> u64 {
    CLUSTER_KV_RAFT_SNAPSHOT_BYTES_LIMIT_FOR_TEST.swap(limit, Ordering::Relaxed)
}

pub fn cluster_kv_raft_node_should_compact(
    node: &DurableRaftKvNode,
    snapshot_bytes: u64,
    policy: ClusterKvRaftCompactionPolicy,
) -> bool {
    // Snapshotting rewrites the entire WAL down to the retained tail plus the
    // snapshot file, so it must only run when it makes durable progress.
    //
    // Compaction advances the on-disk snapshot to `applied_index` and drops log
    // entries older than the retained tail. The durable progress is
    // `applied_index - snapshot_index`; the in-memory log length and event
    // history are not valid progress triggers because they include the retained
    // replication tail.
    let compaction_high_watermark = RAFT_COMPACTION_RETAINED_ENTRIES.saturating_mul(2);
    let unsnapshotted_applied = node.applied_index().saturating_sub(node.snapshot_index());
    let has_compactable_progress = unsnapshotted_applied > compaction_high_watermark;
    let oversized_snapshot_can_shrink = snapshot_bytes > policy.max_snapshot_bytes
        && node.store().event_history_len() > policy.max_snapshot_events;
    has_compactable_progress || oversized_snapshot_can_shrink
}

pub fn compact_cluster_kv_raft_node_if_needed(
    node: &mut DurableRaftKvNode,
    snapshot_bytes: u64,
    policy: ClusterKvRaftCompactionPolicy,
) -> Result<bool, KvError> {
    if !cluster_kv_raft_node_should_compact(node, snapshot_bytes, policy) {
        return Ok(false);
    }
    let mut metric = KvStorageMetricGuard::start("compaction", "sinomo.kv.compaction.us");
    let log_entries_before = node.log().len();
    node.compact_event_history_to(policy.max_snapshot_events);
    node.save_snapshot()?;
    metrics::counter!("sinomo.kv.compaction.runs", "outcome" => "ok").increment(1);
    metrics::histogram!("sinomo.kv.compaction.removed_log_entries")
        .record(log_entries_before.saturating_sub(node.log().len()) as f64);
    metric.succeed();
    Ok(true)
}

#[derive(Clone, Debug)]
pub struct ClusterKvRaftStore {
    paths: ClusterKvPaths,
    compaction_policy: ClusterKvRaftCompactionPolicy,
}

fn build_append_entries_request_from_open_node(
    node: &DurableRaftKvNode,
    leader_id: &NodeId,
    next_index: u64,
) -> Result<ClusterKvRaftAppendRequest, KvError> {
    let prev_log_index = next_index.saturating_sub(1).min(node.last_log_index());
    let earliest_log_index = node.log().first().map(|entry| entry.index);
    let prev_log_resolvable = prev_log_index == node.snapshot_index()
        || earliest_log_index.is_some_and(|first| prev_log_index >= first);
    let include_snapshot = node.snapshot_index() > 0 && !prev_log_resolvable;
    let (snapshot, prev_log_index, entries) = if include_snapshot {
        let mut kv = node.store().try_snapshot()?;
        compact_kv_snapshot_event_history(&mut kv, MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS);
        let snapshot = RaftSnapshot {
            last_included_index: node.applied_index(),
            last_included_term: node.term_at(node.applied_index()).unwrap_or(1),
            kv,
        };
        let mut entries = Vec::new();
        for entry in node
            .log()
            .iter()
            .filter(|entry| entry.index > snapshot.last_included_index)
        {
            try_reserve_kv_items(&mut entries, 1, "build raft append snapshot tail")?;
            entries.push(entry.clone());
        }
        (Some(snapshot), node.applied_index(), entries)
    } else {
        let mut entries = Vec::new();
        for entry in node.log().iter().filter(|entry| entry.index >= next_index) {
            try_reserve_kv_items(&mut entries, 1, "build raft append tail")?;
            entries.push(entry.clone());
        }
        (None, prev_log_index, entries)
    };
    let prev_log_term = node.term_at(prev_log_index).unwrap_or(0);
    Ok(ClusterKvRaftAppendRequest {
        snapshot,
        append: RaftAppendEntriesRequest {
            term: node.hard_state().current_term,
            leader_id: leader_id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: node.hard_state().commit_index,
        },
    })
}

impl ClusterKvRaftStore {
    pub fn new(paths: ClusterKvPaths, compaction_policy: ClusterKvRaftCompactionPolicy) -> Self {
        Self {
            paths,
            compaction_policy,
        }
    }

    pub fn paths(&self) -> &ClusterKvPaths {
        &self.paths
    }

    pub fn lock_operation(&self) -> Result<ClusterKvRaftOperationGuard, KvError> {
        lock_cluster_kv_raft_operation_for_paths(&self.paths)
    }

    pub fn wal_paths(&self, node_id: &NodeId) -> KvWalPaths {
        self.paths.raft_wal_paths(node_id)
    }

    fn snapshot_bytes(&self, node_id: &NodeId) -> u64 {
        fs::metadata(&self.wal_paths(node_id).snapshot)
            .map(|metadata| metadata.len())
            .unwrap_or(0)
    }

    fn compact_node_if_needed(&self, node: &mut DurableRaftKvNode) -> Result<bool, KvError> {
        compact_cluster_kv_raft_node_if_needed(
            node,
            self.snapshot_bytes(node.node_id()),
            self.compaction_policy,
        )
    }

    pub fn open_node(&self, node_id: &NodeId) -> Result<DurableRaftKvNode, KvError> {
        let cache = self.paths.local_cache();
        let mut node = cache.open_raft_node(node_id.clone())?;
        self.compact_node_if_needed(&mut node)?;
        Ok(node)
    }

    pub fn open_node_discarding_uncommitted_tail(
        &self,
        node_id: &NodeId,
    ) -> Result<DurableRaftKvNode, KvError> {
        let cache = self.paths.local_cache();
        let mut node = cache.open_raft_node_discarding_uncommitted_tail(node_id.clone())?;
        self.compact_node_if_needed(&mut node)?;
        Ok(node)
    }

    pub async fn open_node_async(&self, node_id: &NodeId) -> Result<DurableRaftKvNode, KvError> {
        let store = self.clone();
        let node_id = node_id.clone();
        let error_path = store.wal_paths(&node_id).root.display().to_string();
        tokio::task::spawn_blocking(move || store.open_node(&node_id))
            .await
            .map_err(|source| KvError::Io {
                path: error_path,
                detail: source.to_string(),
            })?
    }

    pub fn build_append_entries_request_from_next_index(
        &self,
        node_id: &NodeId,
        leader_id: &NodeId,
        next_index: u64,
    ) -> Result<ClusterKvRaftAppendRequest, KvError> {
        let node = self.open_node(node_id)?;
        build_append_entries_request_from_open_node(&node, leader_id, next_index)
    }

    pub fn build_cluster_raft_append_request_from_next_index(
        &self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        let request = self.build_append_entries_request_from_next_index(
            &state.local_node_id,
            &state.local_node_id,
            next_index,
        )?;
        Ok(ClusterRaftAppendRequest {
            state: state.clone(),
            snapshot: request.snapshot,
            append: request.append,
        })
    }

    pub fn locked_build_cluster_raft_append_request_from_next_index(
        &self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.build_cluster_raft_append_request_from_next_index(state, next_index)
    }

    pub async fn locked_build_cluster_raft_append_request_from_next_index_async(
        &self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        let store = self.clone();
        let state = state.clone();
        let error_path = store
            .wal_paths(&state.local_node_id)
            .root
            .display()
            .to_string();
        tokio::task::spawn_blocking(move || {
            let _raft_guard = store.lock_operation()?;
            store.build_cluster_raft_append_request_from_next_index(&state, next_index)
        })
        .await
        .map_err(|source| KvError::Io {
            path: error_path,
            detail: source.to_string(),
        })?
    }

    pub fn cache_snapshot_if_newer_than_raft(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        self.paths
            .local_cache()
            .cache_snapshot_if_newer_than_raft(node_id.clone(), snapshot)
    }

    pub fn cache_snapshot_if_newer_than_raft_and_notify(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        let cache_changed = self.cache_snapshot_if_newer_than_raft(node_id, snapshot)?;
        if cache_changed {
            notify_cluster_kv_watchers(&self.paths);
        }
        Ok(cache_changed)
    }

    pub fn locked_cache_snapshot_if_newer_than_raft_and_notify(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.cache_snapshot_if_newer_than_raft_and_notify(node_id, snapshot)
    }

    pub fn local_vote_metadata(
        &self,
        node_id: &NodeId,
    ) -> Result<ClusterKvRaftLocalVoteMetadata, KvError> {
        let node = self.open_node(node_id)?;
        Ok(ClusterKvRaftLocalVoteMetadata {
            current_term: node.hard_state().current_term,
            last_log_index: node.last_log_index(),
            last_log_term: node.last_log_term(),
        })
    }

    pub fn locked_local_vote_metadata(
        &self,
        node_id: &NodeId,
    ) -> Result<ClusterKvRaftLocalVoteMetadata, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.local_vote_metadata(node_id)
    }

    pub fn last_log_index(&self, node_id: &NodeId) -> Result<u64, KvError> {
        self.open_node(node_id).map(|node| node.last_log_index())
    }

    pub fn locked_last_log_index(&self, node_id: &NodeId) -> Result<u64, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.last_log_index(node_id)
    }

    pub async fn locked_last_log_index_async(&self, node_id: &NodeId) -> Result<u64, KvError> {
        let store = self.clone();
        let node_id = node_id.clone();
        let error_path = store.wal_paths(&node_id).root.display().to_string();
        tokio::task::spawn_blocking(move || {
            let _raft_guard = store.lock_operation()?;
            store.last_log_index(&node_id)
        })
        .await
        .map_err(|source| KvError::Io {
            path: error_path,
            detail: format!("cluster kv raft log index task failed: {source}"),
        })?
    }

    pub fn apply_vote(
        &self,
        node_id: &NodeId,
        vote: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        let mut node = self.open_node(node_id)?;
        node.request_vote(vote)
    }

    pub fn locked_apply_vote(
        &self,
        node_id: &NodeId,
        vote: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.apply_vote(node_id, vote)
    }

    pub fn apply_vote_response(
        &self,
        state: &ClusterBootstrapState,
        vote: RaftRequestVoteRequest,
    ) -> Result<ClusterRaftVoteResponse, KvError> {
        let vote = self.apply_vote(&state.local_node_id, vote)?;
        Ok(ClusterRaftVoteResponse {
            accepted: vote.vote_granted,
            detail: if vote.vote_granted {
                "raft vote granted".to_string()
            } else {
                "raft vote rejected by local hard state".to_string()
            },
            state: state.clone(),
            vote,
        })
    }

    pub fn apply_append_entries_with_optional_snapshot(
        &self,
        node_id: &NodeId,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesApplyOutcome, KvError> {
        let mut node = self.open_node(node_id)?;
        let outcome = node.apply_append_entries_with_optional_snapshot(snapshot, append)?;
        self.compact_node_if_needed(&mut node)?;
        Ok(outcome)
    }

    pub fn apply_append_entries_with_optional_snapshot_and_publish<F>(
        &self,
        node_id: &NodeId,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(RaftAppendEntriesApplyOutcome, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let outcome =
            self.apply_append_entries_with_optional_snapshot(node_id, snapshot, append)?;
        let wrote = self
            .publish_append_entries_apply_outcome(&outcome, key_can_defer_materialized_snapshot)?;
        Ok((outcome, wrote))
    }

    pub fn append_entries_rejection_response(
        &self,
        node_id: &NodeId,
    ) -> Result<RaftAppendEntriesResponse, KvError> {
        let node = self.open_node(node_id)?;
        Ok(RaftAppendEntriesResponse {
            term: node.hard_state().current_term,
            success: false,
            match_index: node.last_log_index(),
        })
    }

    pub fn request_vote_rejection_response(
        &self,
        node_id: &NodeId,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        let hard_state = self.wal_paths(node_id).read_persisted_hard_state()?;
        Ok(RaftRequestVoteResponse {
            term: hard_state.current_term,
            vote_granted: false,
        })
    }

    pub fn cluster_raft_vote_rejection_response(
        &self,
        state: &ClusterBootstrapState,
        detail: String,
    ) -> Result<ClusterRaftVoteResponse, KvError> {
        Ok(ClusterRaftVoteResponse {
            accepted: false,
            detail,
            state: state.clone(),
            vote: self.request_vote_rejection_response(&state.local_node_id)?,
        })
    }

    pub fn known_leader_id(&self, node_id: &NodeId) -> Result<Option<NodeId>, KvError> {
        // Read only the tiny hard-state file rather than opening the full Raft
        // node, which may replay a large WAL. Topology validity remains the
        // caller's job because KV does not interpret cluster membership.
        self.wal_paths(node_id)
            .read_persisted_hard_state()
            .map(|hard_state| hard_state.leader_id)
    }

    pub fn locked_known_leader_id(&self, node_id: &NodeId) -> Result<Option<NodeId>, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.known_leader_id(node_id)
    }

    pub fn known_voter_leader_id(
        &self,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, KvError> {
        Ok(self
            .known_leader_id(&state.local_node_id)?
            .filter(|leader_id| cluster_kv_raft_node_is_voter(state, leader_id)))
    }

    pub fn locked_known_voter_leader_id(
        &self,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.known_voter_leader_id(state)
    }

    pub fn local_is_leader(&self, state: &ClusterBootstrapState) -> Result<bool, KvError> {
        Ok(
            self.known_voter_leader_id(state)?.as_ref() == Some(&state.local_node_id)
                && cluster_kv_raft_local_is_voter(state),
        )
    }

    pub fn locked_local_is_leader(&self, state: &ClusterBootstrapState) -> Result<bool, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.local_is_leader(state)
    }

    pub fn observe_term(&self, node_id: &NodeId, term: u64) -> Result<Option<KvSnapshot>, KvError> {
        let mut node = self.open_node(node_id)?;
        let changed = node.observe_term(term)?;
        self.compact_node_if_needed(&mut node)?;
        if changed {
            Ok(Some(node.store().try_snapshot()?))
        } else {
            Ok(None)
        }
    }

    pub fn observe_term_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<bool, KvError> {
        let snapshot = self.observe_term(node_id, term)?;
        if let Some(snapshot) = snapshot {
            self.paths
                .local_cache()
                .write_snapshot_if_changed(&snapshot)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn locked_observe_term_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<bool, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.observe_term_and_write_snapshot_if_changed(node_id, term)
    }

    pub fn set_leader(&self, node_id: &NodeId, term: u64) -> Result<KvSnapshot, KvError> {
        let mut node = self.open_node(node_id)?;
        node.become_leader(term)?;
        self.compact_node_if_needed(&mut node)?;
        node.store().try_snapshot()
    }

    pub fn set_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<KvSnapshot, KvError> {
        let snapshot = self.set_leader(node_id, term)?;
        self.paths
            .local_cache()
            .write_snapshot_if_changed(&snapshot)?;
        Ok(snapshot)
    }

    pub fn locked_set_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<KvSnapshot, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.set_leader_and_write_snapshot_if_changed(node_id, term)
    }

    pub fn clear_leader(&self, node_id: &NodeId) -> Result<KvSnapshot, KvError> {
        let mut node = self.open_node(node_id)?;
        node.clear_known_leader()?;
        self.compact_node_if_needed(&mut node)?;
        node.store().try_snapshot()
    }

    pub fn clear_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
    ) -> Result<KvSnapshot, KvError> {
        let snapshot = self.clear_leader(node_id)?;
        self.paths
            .local_cache()
            .write_snapshot_if_changed(&snapshot)?;
        Ok(snapshot)
    }

    pub fn locked_clear_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
    ) -> Result<KvSnapshot, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.clear_leader_and_write_snapshot_if_changed(node_id)
    }

    pub fn append_command_with_response(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        self.append_request_with_response(node_id, KvCommandRequest::generated(command))
    }

    pub fn append_request_with_response(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        let digest = request.digest()?;
        if node.store().request_id_expired(&request.request_id) {
            return Err(KvError::RequestExpired {
                request_id: request.request_id,
            });
        }
        if let Some(record) = node.store().committed_request(&request.request_id) {
            if record.command_digest != digest {
                return Err(KvError::RequestIdConflict {
                    request_id: request.request_id,
                });
            }
            return Ok(ClusterKvRaftCommitResult {
                snapshot: node.store().try_snapshot()?,
                txn_response: record.txn_response.clone(),
                events: Vec::new(),
                committed_request: record.clone(),
            });
        }
        if let Err(error) = node.store().preflight_apply_command(&request.command) {
            let _ = mark_cluster_kv_request_rejected_before_commit(
                &self.paths,
                &request,
                error.to_string(),
            );
            return Err(error);
        }
        mark_cluster_kv_request_submitted(&self.paths, &request)?;
        let before_revision = node.store().revision();
        let term = node.hard_state().current_term.max(1);
        let request_for_outcome = request.clone();
        let entry = match node.append_local_request(term, request) {
            Ok(entry) => entry,
            Err(error) => {
                let _ = mark_cluster_kv_request_unknown(
                    &self.paths,
                    &request_for_outcome,
                    error.to_string(),
                );
                return Err(error);
            }
        };
        if let Err(error) = node.commit_to(entry.index) {
            let _ = mark_cluster_kv_request_unknown(
                &self.paths,
                &request_for_outcome,
                error.to_string(),
            );
            return Err(error);
        }
        let txn_response = node.txn_response_at(entry.index).cloned();
        let committed_request = node
            .store()
            .committed_request(&entry.request_id)
            .cloned()
            .ok_or(KvError::MissingCommittedEntry { index: entry.index })?;
        self.compact_node_if_needed(&mut node)?;
        let snapshot = node.store().try_snapshot()?;
        let events = Self::events_after_revision(&snapshot, before_revision)?;
        Ok(ClusterKvRaftCommitResult {
            snapshot,
            txn_response,
            events,
            committed_request,
        })
    }

    pub fn append_command(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<KvSnapshot, KvError> {
        self.append_command_with_response(node_id, command)
            .map(|result| result.snapshot)
    }

    pub fn append_command_with_response_and_publish<F>(
        &self,
        node_id: &NodeId,
        command: KvCommand,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<ClusterKvRaftCommitResult, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let (result, _) = self.append_command_with_response_and_publish_report(
            node_id,
            command,
            key_can_defer_materialized_snapshot,
        )?;
        Ok(result)
    }

    pub fn append_command_with_response_and_publish_report<F>(
        &self,
        node_id: &NodeId,
        command: KvCommand,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(ClusterKvRaftCommitResult, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let result = self.append_command_with_response(node_id, command)?;
        let wrote = self.publish_commit_result(&result, key_can_defer_materialized_snapshot)?;
        Ok((result, wrote))
    }

    pub fn append_command_and_publish<F>(
        &self,
        node_id: &NodeId,
        command: KvCommand,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<KvSnapshot, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        self.append_command_with_response_and_publish(
            node_id,
            command,
            key_can_defer_materialized_snapshot,
        )
        .map(|result| result.snapshot)
    }

    pub fn publish_commit_result<F>(
        &self,
        result: &ClusterKvRaftCommitResult,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<bool, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let wrote = self.write_snapshot_after_commit(
            &result.snapshot,
            &result.events,
            key_can_defer_materialized_snapshot,
        )?;
        notify_cluster_kv_watchers_for_events(&self.paths, &result.events);
        Ok(wrote)
    }

    pub fn publish_append_entries_apply_outcome<F>(
        &self,
        outcome: &RaftAppendEntriesApplyOutcome,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<bool, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        if outcome.snapshot_restored {
            let wrote = write_cluster_kv_snapshot_if_changed(&self.paths, &outcome.snapshot)?;
            return Ok(wrote);
        }
        if outcome.committed_events.is_empty() {
            return Ok(false);
        }
        let wrote = self.write_snapshot_after_commit(
            &outcome.snapshot,
            &outcome.committed_events,
            key_can_defer_materialized_snapshot,
        )?;
        notify_cluster_kv_watchers_for_events(&self.paths, &outcome.committed_events);
        Ok(wrote)
    }

    pub fn write_snapshot_after_commit<F>(
        &self,
        snapshot: &KvSnapshot,
        events: &[KvEvent],
        key_can_defer_materialized_snapshot: F,
    ) -> Result<bool, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        if cluster_kv_events_can_defer_materialized_snapshot(
            events,
            key_can_defer_materialized_snapshot,
        ) && self.materialized_snapshot_is_fresh_enough(snapshot)
        {
            return Ok(false);
        }
        self.paths.local_cache().write_snapshot_if_changed(snapshot)
    }

    fn materialized_snapshot_is_fresh_enough(&self, snapshot: &KvSnapshot) -> bool {
        let Some(existing) = self
            .paths
            .local_cache()
            .cached_snapshot_arc_if_present()
            .ok()
            .flatten()
        else {
            return false;
        };
        cluster_kv_materialized_snapshot_is_fresh_enough_against(
            existing.revision,
            snapshot.revision,
            self.materialized_snapshot_age(),
        )
    }

    fn materialized_snapshot_age(&self) -> Option<Duration> {
        cluster_kv_materialized_snapshot_age(&self.paths)
    }

    pub fn append_command_uncommitted(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<KvLogEntry, KvError> {
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        self.append_command_to_open_node(&mut node, command)
    }

    pub fn replay_request_if_committed(
        &self,
        node_id: &NodeId,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, KvError> {
        let node = self.open_node_discarding_uncommitted_tail(node_id)?;
        let digest = request.digest()?;
        if node.store().request_id_expired(&request.request_id) {
            return Err(KvError::RequestExpired {
                request_id: request.request_id.clone(),
            });
        }
        let Some(record) = node.store().committed_request(&request.request_id) else {
            return Ok(None);
        };
        if record.command_digest != digest {
            return Err(KvError::RequestIdConflict {
                request_id: request.request_id.clone(),
            });
        }
        Ok(Some((node.store().try_snapshot()?, record.clone())))
    }

    pub fn locked_replay_request_if_committed(
        &self,
        node_id: &NodeId,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.replay_request_if_committed(node_id, request)
    }

    pub fn append_request_uncommitted(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, KvError> {
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        let preflight = (|| {
            let digest = request.digest()?;
            if node.store().request_id_expired(&request.request_id) {
                return Err(KvError::RequestExpired {
                    request_id: request.request_id.clone(),
                });
            }
            if let Some(existing) = node.store().committed_request(&request.request_id)
                && existing.command_digest != digest
            {
                return Err(KvError::RequestIdConflict {
                    request_id: request.request_id.clone(),
                });
            }
            node.store().preflight_apply_command(&request.command)
        })();
        if let Err(error) = preflight {
            let _ = mark_cluster_kv_request_rejected_before_commit(
                &self.paths,
                &request,
                error.to_string(),
            );
            return Err(error);
        }
        mark_cluster_kv_request_submitted(&self.paths, &request)?;
        let request_for_outcome = request.clone();
        match self.append_request_to_open_node(&mut node, request) {
            Ok(entry) => Ok(entry),
            Err(error) => {
                let _ = mark_cluster_kv_request_unknown(
                    &self.paths,
                    &request_for_outcome,
                    error.to_string(),
                );
                Err(error)
            }
        }
    }

    pub fn locked_append_request_uncommitted(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.append_request_uncommitted(node_id, request)
    }

    pub fn append_request_uncommitted_or_replay(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        if let Some((snapshot, record)) = self.replay_request_if_committed(node_id, &request)? {
            return Ok(ClusterKvRaftUncommittedAppend::CommittedReplay { snapshot, record });
        }
        self.append_request_uncommitted(node_id, request)
            .map(ClusterKvRaftUncommittedAppend::Appended)
    }

    pub fn locked_append_request_uncommitted_or_replay(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.append_request_uncommitted_or_replay(node_id, request)
    }

    pub fn discard_uncommitted_tail(&self, node_id: &NodeId) -> Result<bool, KvError> {
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        let discarded = node.discard_uncommitted_tail()?;
        self.compact_node_if_needed(&mut node)?;
        Ok(discarded)
    }

    pub fn discard_uncommitted_tail_and_clear_leader(
        &self,
        node_id: &NodeId,
    ) -> Result<KvSnapshot, KvError> {
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        node.discard_uncommitted_tail()?;
        node.clear_known_leader()?;
        self.compact_node_if_needed(&mut node)?;
        node.store().try_snapshot()
    }

    pub fn discard_uncommitted_tail_and_clear_leader_and_publish_report(
        &self,
        node_id: &NodeId,
    ) -> Result<(KvSnapshot, bool), KvError> {
        let snapshot = self.discard_uncommitted_tail_and_clear_leader(node_id)?;
        let wrote = write_cluster_kv_snapshot_if_changed(&self.paths, &snapshot)?;
        Ok((snapshot, wrote))
    }

    pub fn locked_discard_uncommitted_tail_and_clear_leader_and_publish_report(
        &self,
        node_id: &NodeId,
    ) -> Result<(KvSnapshot, bool), KvError> {
        let _raft_guard = self.lock_operation()?;
        self.discard_uncommitted_tail_and_clear_leader_and_publish_report(node_id)
    }

    pub fn commit_index_with_events(
        &self,
        node_id: &NodeId,
        commit_index: u64,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        let mut node = self.open_node(node_id)?;
        let before_revision = node.store().revision();
        node.commit_to(commit_index)?;
        let txn_response = node.txn_response_at(commit_index).cloned();
        self.compact_node_if_needed(&mut node)?;
        let snapshot = node.store().try_snapshot()?;
        let events = Self::events_after_revision(&snapshot, before_revision)?;
        let committed_request = node
            .log_entry_at(commit_index)
            .and_then(|entry| node.store().committed_request(&entry.request_id))
            .cloned()
            .ok_or(KvError::MissingCommittedEntry {
                index: commit_index,
            })?;
        Ok(ClusterKvRaftCommitResult {
            snapshot,
            txn_response,
            events,
            committed_request,
        })
    }

    pub fn commit_index_with_events_and_publish_report<F>(
        &self,
        node_id: &NodeId,
        commit_index: u64,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(ClusterKvRaftCommitResult, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let result = self.commit_index_with_events(node_id, commit_index)?;
        let wrote = self.publish_commit_result(&result, key_can_defer_materialized_snapshot)?;
        Ok((result, wrote))
    }

    pub fn locked_commit_index_with_events_and_publish_report<F>(
        &self,
        node_id: &NodeId,
        commit_index: u64,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(ClusterKvRaftCommitResult, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let _raft_guard = self.lock_operation()?;
        self.commit_index_with_events_and_publish_report(
            node_id,
            commit_index,
            key_can_defer_materialized_snapshot,
        )
    }

    fn append_command_to_open_node(
        &self,
        node: &mut DurableRaftKvNode,
        command: KvCommand,
    ) -> Result<KvLogEntry, KvError> {
        let term = node.hard_state().current_term.max(1);
        let entry = node.append_local_command(term, command)?;
        self.compact_node_if_needed(node)?;
        Ok(entry)
    }

    fn append_request_to_open_node(
        &self,
        node: &mut DurableRaftKvNode,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, KvError> {
        let term = node.hard_state().current_term.max(1);
        let entry = node.append_local_request(term, request)?;
        self.compact_node_if_needed(node)?;
        Ok(entry)
    }

    fn events_after_revision(
        snapshot: &KvSnapshot,
        before_revision: KvRevision,
    ) -> Result<Vec<KvEvent>, KvError> {
        let mut events = Vec::new();
        for event in snapshot
            .events
            .iter()
            .filter(|event| event.revision > before_revision)
        {
            try_reserve_kv_items(&mut events, 1, "collect committed raft events")?;
            events.push(event.clone());
        }
        Ok(events)
    }
}

pub struct ClusterKvRaftLocalCommandStorage<F>
where
    F: FnMut(&str) -> bool,
{
    store: ClusterKvRaftStore,
    node_id: NodeId,
    node: Option<DurableRaftKvNode>,
    key_can_defer_materialized_snapshot: F,
    last_snapshot_publish_wrote: Option<bool>,
}

impl<F> ClusterKvRaftLocalCommandStorage<F>
where
    F: FnMut(&str) -> bool,
{
    pub fn new(
        store: ClusterKvRaftStore,
        node_id: NodeId,
        key_can_defer_materialized_snapshot: F,
    ) -> Self {
        Self {
            store,
            node_id,
            node: None,
            key_can_defer_materialized_snapshot,
            last_snapshot_publish_wrote: None,
        }
    }

    /// Opens the durable node once for the lifetime of this command-storage
    /// session. Quorum commits deliberately retain the node lock and restored
    /// state across replay, append, replication-request construction, and
    /// commit instead of deserializing the snapshot and replaying the WAL for
    /// every phase.
    fn node_mut_with_open_mode(
        &mut self,
        discard_uncommitted_tail: bool,
    ) -> Result<&mut DurableRaftKvNode, KvError> {
        if self.node.is_none() {
            let node = {
                let _raft_guard = self.store.lock_operation()?;
                if discard_uncommitted_tail {
                    self.store
                        .open_node_discarding_uncommitted_tail(&self.node_id)?
                } else {
                    self.store.open_node(&self.node_id)?
                }
            };
            self.node = Some(node);
        }
        self.node.as_mut().ok_or_else(|| KvError::Io {
            path: self
                .store
                .wal_paths(&self.node_id)
                .root
                .display()
                .to_string(),
            detail: "cluster KV Raft command session did not retain its node".to_string(),
        })
    }

    fn node_mut(&mut self) -> Result<&mut DurableRaftKvNode, KvError> {
        self.node_mut_with_open_mode(false)
    }

    fn node_mut_discarding_uncommitted_tail(&mut self) -> Result<&mut DurableRaftKvNode, KvError> {
        self.node_mut_with_open_mode(true)
    }

    pub fn local_is_leader(&mut self, state: &ClusterBootstrapState) -> Result<bool, KvError> {
        let local_node_id = state.local_node_id.clone();
        let node = self.node_mut()?;
        Ok(cluster_kv_raft_local_is_voter(state)
            && node.hard_state().leader_id.as_ref() == Some(&local_node_id))
    }

    pub fn last_log_index(&mut self) -> Result<u64, KvError> {
        self.node_mut().map(|node| node.last_log_index())
    }

    pub fn build_cluster_raft_append_request_from_next_index(
        &mut self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        let node = self.node_mut()?;
        let request =
            build_append_entries_request_from_open_node(node, &state.local_node_id, next_index)?;
        Ok(ClusterRaftAppendRequest {
            state: state.clone(),
            snapshot: request.snapshot,
            append: request.append,
        })
    }

    pub fn take_last_snapshot_publish_wrote(&mut self) -> Option<bool> {
        self.last_snapshot_publish_wrote.take()
    }

    pub fn append_uncommitted_or_replay(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        self.last_snapshot_publish_wrote = None;
        if let Some((snapshot, record)) = self.replay_if_committed(&request)? {
            return Ok(ClusterKvRaftUncommittedAppend::CommittedReplay { snapshot, record });
        }
        self.append_uncommitted(request)
            .map(ClusterKvRaftUncommittedAppend::Appended)
    }

    pub fn replay_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, KvError> {
        self.last_snapshot_publish_wrote = None;
        let digest = request.digest()?;
        let node = self.node_mut_discarding_uncommitted_tail()?;
        if node.store().request_id_expired(&request.request_id) {
            return Err(KvError::RequestExpired {
                request_id: request.request_id.clone(),
            });
        }
        let Some(record) = node.store().committed_request(&request.request_id) else {
            return Ok(None);
        };
        if record.command_digest != digest {
            return Err(KvError::RequestIdConflict {
                request_id: request.request_id.clone(),
            });
        }
        Ok(Some((node.store().try_snapshot()?, record.clone())))
    }

    pub fn append_uncommitted(&mut self, request: KvCommandRequest) -> Result<KvLogEntry, KvError> {
        self.last_snapshot_publish_wrote = None;
        let preflight = {
            let node = self.node_mut_discarding_uncommitted_tail()?;
            let digest = request.digest()?;
            if node.store().request_id_expired(&request.request_id) {
                Err(KvError::RequestExpired {
                    request_id: request.request_id.clone(),
                })
            } else if node
                .store()
                .committed_request(&request.request_id)
                .is_some_and(|existing| existing.command_digest != digest)
            {
                Err(KvError::RequestIdConflict {
                    request_id: request.request_id.clone(),
                })
            } else {
                node.store().preflight_apply_command(&request.command)
            }
        };
        if let Err(error) = preflight {
            let _ = mark_cluster_kv_request_rejected_before_commit(
                self.store.paths(),
                &request,
                error.to_string(),
            );
            return Err(error);
        }
        mark_cluster_kv_request_submitted(self.store.paths(), &request)?;
        let request_for_outcome = request.clone();
        let result: Result<KvLogEntry, KvError> = (|| {
            let term = self.node_mut()?.hard_state().current_term.max(1);
            let entry = self.node_mut()?.append_local_request(term, request)?;
            let store = self.store.clone();
            store.compact_node_if_needed(self.node_mut()?)?;
            Ok(entry)
        })();
        if let Err(error) = &result {
            let _ = mark_cluster_kv_request_unknown(
                self.store.paths(),
                &request_for_outcome,
                error.to_string(),
            );
        }
        result
    }

    pub fn record_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &KvAuthorityFailure,
    ) -> Result<(), KvError> {
        mark_cluster_kv_request_rejected_before_commit(
            self.store.paths(),
            request,
            failure.detail.clone(),
        )
    }

    pub fn record_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), KvError> {
        mark_cluster_kv_request_unknown(self.store.paths(), request, detail.to_string())
    }

    pub fn discard_uncommitted_tail_and_clear_leader(&mut self) -> Result<KvSnapshot, KvError> {
        self.last_snapshot_publish_wrote = None;
        let store = self.store.clone();
        let snapshot = {
            let node = self.node_mut_discarding_uncommitted_tail()?;
            node.discard_uncommitted_tail()?;
            node.clear_known_leader()?;
            store.compact_node_if_needed(node)?;
            node.store().try_snapshot()?
        };
        let wrote = write_cluster_kv_snapshot_if_changed(store.paths(), &snapshot)?;
        self.last_snapshot_publish_wrote = Some(wrote);
        Ok(snapshot)
    }

    pub fn observe_term(&mut self, term: u64) -> Result<(), KvError> {
        self.last_snapshot_publish_wrote = None;
        let store = self.store.clone();
        let snapshot = {
            let node = self.node_mut()?;
            let changed = node.observe_term(term)?;
            store.compact_node_if_needed(node)?;
            changed.then(|| node.store().try_snapshot()).transpose()?
        };
        if let Some(snapshot) = snapshot {
            store
                .paths()
                .local_cache()
                .write_snapshot_if_changed(&snapshot)?;
        }
        Ok(())
    }

    pub fn commit_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        self.last_snapshot_publish_wrote = None;
        let store = self.store.clone();
        let result = {
            let node = self.node_mut()?;
            let before_revision = node.store().revision();
            node.commit_to(index)?;
            let txn_response = node.txn_response_at(index).cloned();
            store.compact_node_if_needed(node)?;
            let snapshot = node.store().try_snapshot()?;
            let events = ClusterKvRaftStore::events_after_revision(&snapshot, before_revision)?;
            let committed_request = node
                .log_entry_at(index)
                .and_then(|entry| node.store().committed_request(&entry.request_id))
                .cloned()
                .ok_or(KvError::MissingCommittedEntry { index })?;
            ClusterKvRaftCommitResult {
                snapshot,
                txn_response,
                events,
                committed_request,
            }
        };
        let wrote =
            store.publish_commit_result(&result, &mut self.key_can_defer_materialized_snapshot)?;
        self.last_snapshot_publish_wrote = Some(wrote);
        Ok(result)
    }
}

impl<F> ClusterKvRaftCommandStorage for ClusterKvRaftLocalCommandStorage<F>
where
    F: FnMut(&str) -> bool,
{
    type Error = KvError;

    fn kv_error(error: KvError) -> Self::Error {
        error
    }

    fn replay_cluster_raft_request_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, Self::Error> {
        self.replay_if_committed(request)
    }

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, Self::Error> {
        self.append_uncommitted(request)
    }

    fn record_cluster_raft_request_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &KvAuthorityFailure,
    ) -> Result<(), Self::Error> {
        self.record_rejected_before_commit(request, failure)
    }

    fn record_cluster_raft_request_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), Self::Error> {
        self.record_unknown(request, detail)
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), Self::Error> {
        self.discard_uncommitted_tail_and_clear_leader().map(|_| ())
    }

    fn observe_cluster_raft_term(
        &mut self,
        _state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), Self::Error> {
        self.observe_term(term)
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, Self::Error> {
        self.commit_index_with_events(index)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftAppendEntriesApplyOutcome {
    pub response: RaftAppendEntriesResponse,
    pub snapshot: KvSnapshot,
    pub snapshot_restored: bool,
    pub committed_events: Vec<KvEvent>,
}

#[derive(Debug)]
pub struct DurableRaftKvNode {
    node_id: NodeId,
    pub(crate) paths: KvWalPaths,
    _lock: File,
    node: ReplicatedKvNode,
}

impl DurableRaftKvNode {
    pub fn open(node_id: NodeId, paths: KvWalPaths) -> Result<Self, KvError> {
        Self::open_inner(node_id, paths, false, RAFT_NODE_LOCK_WAIT)
    }

    pub fn open_discarding_uncommitted_tail(
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<Self, KvError> {
        Self::open_inner(node_id, paths, true, RAFT_NODE_LOCK_WAIT)
    }

    fn open_inner(
        node_id: NodeId,
        paths: KvWalPaths,
        discard_uncommitted_tail: bool,
        lock_wait: Duration,
    ) -> Result<Self, KvError> {
        let mut metric = KvStorageMetricGuard::start("raft_open", "sinomo.kv.raft.open.us");
        paths.ensure()?;
        let lock = open_node_lock(&paths)?;
        lock_node_exclusive(&lock, &paths.lock, lock_wait)?;
        let snapshot = read_optional_object::<RaftSnapshot>(&paths.snapshot)?;
        let hard_state = paths.read_persisted_hard_state()?;
        let mut node = if let Some(snapshot) = snapshot {
            let mut node = ReplicatedKvNode {
                store: InProcessKvStore::new(),
                log: Vec::new(),
                hard_state: hard_state.clone(),
                applied_index: 0,
                snapshot_index: 0,
                snapshot_term: 0,
                txn_responses: KvTxnResponseIndex::default(),
            };
            node.install_snapshot(snapshot)?;
            node.hard_state.current_term = hard_state.current_term;
            node.hard_state.voted_for = hard_state.voted_for;
            node.hard_state.leader_id = hard_state.leader_id;
            node.hard_state.commit_index =
                hard_state.commit_index.max(node.hard_state.commit_index);
            node
        } else {
            ReplicatedKvNode {
                store: InProcessKvStore::new(),
                log: Vec::new(),
                hard_state,
                applied_index: 0,
                snapshot_index: 0,
                snapshot_term: 0,
                txn_responses: KvTxnResponseIndex::default(),
            }
        };
        let committed_index = node.hard_state.commit_index.max(node.applied_index);
        let retain_after_index = node
            .snapshot_index
            .saturating_sub(RAFT_COMPACTION_RETAINED_ENTRIES);
        let WalRead { entries, truncated } = read_wal_entries(
            &paths.wal,
            discard_uncommitted_tail.then_some(committed_index),
            retain_after_index,
        )?;
        // Preserve the already-applied tail (kept by local_compact for incremental
        // replication) instead of discarding it as append_entries would.
        let load_truncated = node.load_persisted_entries(entries);
        let mut repaired_hard_state = false;
        let max_committable_index = node.last_log_index();
        if node.hard_state.commit_index > max_committable_index {
            eprintln!(
                "warning: repairing cluster kv hard-state commit index {} beyond local log {} in {}",
                node.hard_state.commit_index,
                max_committable_index,
                paths.root.display()
            );
            node.hard_state.commit_index = max_committable_index;
            repaired_hard_state = true;
        }
        let commit_index = node.hard_state.commit_index;
        node.commit_to(commit_index)?;
        if repaired_hard_state {
            paths.write_persisted_hard_state(&node.hard_state)?;
        }
        if truncated || load_truncated {
            rewrite_wal_entries(&paths.wal, &node.log)?;
        }
        let opened = Self {
            node_id,
            paths,
            _lock: lock,
            node,
        };
        metrics::histogram!("sinomo.kv.raft.open.log_entries").record(opened.log().len() as f64);
        opened.record_live_metrics();
        metric.succeed();
        Ok(opened)
    }

    pub async fn open_async(node_id: NodeId, paths: KvWalPaths) -> Result<Self, KvError> {
        let error_path = paths.root.display().to_string();
        tokio::task::spawn_blocking(move || Self::open(node_id, paths))
            .await
            .map_err(|source| KvError::Io {
                path: error_path,
                detail: source.to_string(),
            })?
    }

    pub fn open_with_materialized_cache(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: &KvMaterializedCache,
    ) -> Result<Self, KvError> {
        Self::open_with_materialized_cache_inner(node_id, paths, cache, false, RAFT_NODE_LOCK_WAIT)
    }

    pub fn open_discarding_uncommitted_tail_with_materialized_cache(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: &KvMaterializedCache,
    ) -> Result<Self, KvError> {
        Self::open_with_materialized_cache_inner(node_id, paths, cache, true, RAFT_NODE_LOCK_WAIT)
    }

    pub(crate) fn try_open_with_materialized_cache(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: &KvMaterializedCache,
    ) -> Result<Self, KvError> {
        Self::open_with_materialized_cache_inner(node_id, paths, cache, false, Duration::ZERO)
    }

    fn open_with_materialized_cache_inner(
        node_id: NodeId,
        paths: KvWalPaths,
        _cache: &KvMaterializedCache,
        discard_uncommitted_tail: bool,
        lock_wait: Duration,
    ) -> Result<Self, KvError> {
        // A materialized `KvSnapshot` has no Raft log position or term. Treating
        // it as an index-zero Raft snapshot makes a fresh learner apply entries
        // 1..N on top of state that already includes those entries, inflating KV
        // revisions and potentially changing compare/transaction results.
        //
        // The materialized snapshot remains a read cache. Writable Raft state
        // may only come from the durable Raft files or an explicitly anchored
        // `RaftSnapshot { last_included_index, last_included_term, .. }` received
        // through replication. Corrupt durable state must likewise fail closed;
        // silently quarantining it and restarting at index zero loses the
        // consensus position needed for a safe recovery.
        Self::open_inner(node_id.clone(), paths, discard_uncommitted_tail, lock_wait)
    }

    pub async fn open_async_with_materialized_cache(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: KvMaterializedCache,
    ) -> Result<Self, KvError> {
        let error_path = paths.root.display().to_string();
        tokio::task::spawn_blocking(move || {
            Self::open_with_materialized_cache(node_id, paths, &cache)
        })
        .await
        .map_err(|source| KvError::Io {
            path: error_path,
            detail: source.to_string(),
        })?
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn hard_state(&self) -> &RaftHardState {
        &self.node.hard_state
    }

    pub fn store(&self) -> &InProcessKvStore {
        &self.node.store
    }

    pub fn log(&self) -> &[KvLogEntry] {
        &self.node.log
    }

    pub fn last_log_index(&self) -> u64 {
        self.node.last_log_index()
    }

    pub fn last_log_term(&self) -> u64 {
        self.node.last_log_term()
    }

    pub fn term_at(&self, index: u64) -> Option<u64> {
        self.node.term_at(index)
    }

    pub fn log_entry_at(&self, index: u64) -> Option<&KvLogEntry> {
        self.node.log_entry_at(index)
    }

    pub fn applied_index(&self) -> u64 {
        self.node.applied_index
    }

    pub fn snapshot_index(&self) -> u64 {
        self.node.snapshot_index
    }

    pub fn txn_response_at(&self, index: u64) -> Option<&KvTxnResponse> {
        self.node.txn_responses.get(index)
    }

    pub fn append_entries(
        &mut self,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesResponse, KvError> {
        if let Some(response) = self.node.read_only_append_entries_response(&request) {
            return Ok(response);
        }

        let hard_state_before = self.node.hard_state.clone();
        let log_before = self.node.log.clone();
        let txn_responses_before = self.node.txn_responses.clone();
        let applied_index_before = self.node.applied_index;
        let preparation = match self.node.prepare_append_entries(request) {
            Ok(preparation) => preparation,
            Err(error) => {
                self.restore_after_append_entries_persist_failure(
                    None,
                    hard_state_before,
                    log_before,
                    txn_responses_before,
                    applied_index_before,
                );
                return Err(error);
            }
        };

        let entries_to_apply = match preparation.commit_index {
            Some(commit_index) if commit_index > self.node.applied_index => {
                match self.node.entries_to_commit(commit_index) {
                    Ok(entries) => {
                        if let Err(error) = self.node.preflight_commit_entries(entries.as_slice()) {
                            self.restore_after_append_entries_persist_failure(
                                None,
                                hard_state_before,
                                log_before,
                                txn_responses_before,
                                applied_index_before,
                            );
                            return Err(error);
                        }
                        Some((commit_index, entries))
                    }
                    Err(error) => {
                        self.restore_after_append_entries_persist_failure(
                            None,
                            hard_state_before,
                            log_before,
                            txn_responses_before,
                            applied_index_before,
                        );
                        return Err(error);
                    }
                }
            }
            Some(commit_index) => {
                self.node.hard_state.commit_index =
                    self.node.hard_state.commit_index.max(commit_index);
                None
            }
            None => None,
        };

        if preparation.response.success {
            if let Err(error) = append_wal_entries(&self.paths.wal, &preparation.appended_entries) {
                self.restore_after_append_entries_persist_failure(
                    None,
                    hard_state_before,
                    log_before,
                    txn_responses_before,
                    applied_index_before,
                );
                return Err(error);
            }
        }
        if let Some((commit_index, _)) = &entries_to_apply {
            self.node.hard_state.commit_index = *commit_index;
        }
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.restore_after_append_entries_persist_failure(
                None,
                hard_state_before,
                log_before,
                txn_responses_before,
                applied_index_before,
            );
            return Err(error);
        }
        if let Some((commit_index, entries)) = entries_to_apply {
            let mut metric = KvStorageMetricGuard::start("apply", "sinomo.kv.apply.us");
            self.node.apply_preflighted_commit(entries, commit_index)?;
            metric.succeed();
        }
        self.record_live_metrics();
        Ok(preparation.response)
    }

    pub fn apply_append_entries_with_optional_snapshot(
        &mut self,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesApplyOutcome, KvError> {
        let before_revision = self.store().revision();
        let mut snapshot_restored = false;
        if let Some(snapshot) = snapshot {
            let applied_index = self.applied_index();
            let current_term = self.hard_state().current_term;
            let snapshot_matches_append = append.prev_log_index >= snapshot.last_included_index
                && (append.prev_log_index != snapshot.last_included_index
                    || append.prev_log_term == snapshot.last_included_term);
            let snapshot_is_newer = snapshot.last_included_index > applied_index;
            // Cluster RPC admission authenticates `append.leader_id` before this
            // method is reached. A same-index install is nevertheless much more
            // destructive than normal forward catch-up, so only allow it from the
            // leader this follower has already recorded for the exact current term.
            // This repairs a compacted committed-log fork even when the divergent
            // local command happened to produce a higher KV revision.
            let snapshot_repairs_same_index_divergence = snapshot.last_included_index > 0
                && snapshot.last_included_index == applied_index
                && snapshot.last_included_index == self.hard_state().commit_index
                && self
                    .term_at(snapshot.last_included_index)
                    .is_some_and(|local_term| snapshot.last_included_term > local_term)
                && snapshot.last_included_term <= append.term
                && append.prev_log_index == snapshot.last_included_index
                && append.prev_log_term == snapshot.last_included_term
                && append.leader_commit == snapshot.last_included_index
                && append.term == current_term
                && self.hard_state().leader_id.as_ref() == Some(&append.leader_id);
            let append_term_is_current = append.term >= current_term;
            if append_term_is_current
                && snapshot_matches_append
                && (snapshot_is_newer || snapshot_repairs_same_index_divergence)
            {
                self.restore_snapshot(snapshot)?;
                snapshot_restored = true;
            }
        }
        let response = self.append_entries(append)?;
        let snapshot = self.store().try_snapshot()?;
        let committed_events = if snapshot_restored || snapshot.revision == before_revision {
            Vec::new()
        } else {
            let mut events = Vec::new();
            for event in snapshot
                .events
                .iter()
                .filter(|event| event.revision > before_revision)
            {
                try_reserve_kv_items(&mut events, 1, "collect append committed events")?;
                events.push(event.clone());
            }
            events
        };
        Ok(RaftAppendEntriesApplyOutcome {
            response,
            snapshot,
            snapshot_restored,
            committed_events,
        })
    }

    pub fn request_vote(
        &mut self,
        request: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        let hard_state_before = self.node.hard_state.clone();
        let response = match self.node.handle_request_vote(request) {
            Ok(response) => response,
            Err(error) => {
                self.node.hard_state = hard_state_before;
                return Err(error);
            }
        };
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        self.record_live_metrics();
        Ok(response)
    }

    pub fn observe_term(&mut self, term: u64) -> Result<bool, KvError> {
        let hard_state_before = self.node.hard_state.clone();
        if !self.node.observe_term(term) {
            return Ok(false);
        }
        if let Err(error) = self.persist_hard_state() {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        self.record_live_metrics();
        Ok(true)
    }

    pub fn become_leader(&mut self, term: u64) -> Result<(), KvError> {
        // Leadership was won for exactly `term`; a higher local term means a
        // concurrent vote or append advanced the node past this election and
        // claiming leadership for the newer term would not be backed by a
        // quorum of votes.
        if self.node.hard_state.current_term > term {
            return Err(KvError::StaleLeadershipTerm {
                term,
                current_term: self.node.hard_state.current_term,
            });
        }
        let hard_state_before = self.node.hard_state.clone();
        self.node.hard_state.current_term = term;
        self.node.hard_state.voted_for = Some(self.node_id.clone());
        self.node.hard_state.leader_id = Some(self.node_id.clone());
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        self.record_live_metrics();
        Ok(())
    }

    pub fn clear_known_leader(&mut self) -> Result<(), KvError> {
        let hard_state_before = self.node.hard_state.clone();
        self.node.hard_state.leader_id = None;
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        self.record_live_metrics();
        Ok(())
    }

    pub fn append_local_command(
        &mut self,
        term: u64,
        command: KvCommand,
    ) -> Result<KvLogEntry, KvError> {
        self.append_local_request(term, KvCommandRequest::generated(command))
    }

    pub fn append_local_request(
        &mut self,
        term: u64,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, KvError> {
        let hard_state_before = self.node.hard_state.clone();
        let log_before = self.node.log.clone();
        let entry = KvLogEntry {
            index: self.node.last_log_index().saturating_add(1),
            term,
            request_id: request.request_id,
            command: request.command,
            revision: KvRevision::ZERO,
        };
        self.node.hard_state.current_term = term;
        self.node.hard_state.leader_id = Some(self.node_id.clone());
        if let Err(error) = self.node.append_entries(vec![entry.clone()]) {
            self.node.hard_state = hard_state_before;
            self.node.log = log_before;
            return Err(error);
        }
        if let Err(error) = append_wal_entries(&self.paths.wal, std::slice::from_ref(&entry)) {
            self.node.hard_state = hard_state_before;
            self.node.log = log_before;
            return Err(error);
        }
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            self.node.log = log_before;
            return Err(error);
        }
        self.record_live_metrics();
        Ok(entry)
    }

    pub fn discard_uncommitted_tail(&mut self) -> Result<bool, KvError> {
        let hard_state_before = self.node.hard_state.clone();
        let log_before = self.node.log.clone();
        let txn_responses_before = self.node.txn_responses.clone();
        if !self.node.discard_uncommitted_tail() {
            return Ok(false);
        }
        if let Err(error) = rewrite_wal_entries(&self.paths.wal, &self.node.log) {
            self.node.hard_state = hard_state_before;
            self.node.log = log_before;
            self.node.txn_responses = txn_responses_before;
            return Err(error);
        }
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            self.node.log = log_before;
            self.node.txn_responses = txn_responses_before;
            return Err(error);
        }
        self.record_live_metrics();
        Ok(true)
    }

    pub fn compact_event_history_to(&mut self, max_events: usize) {
        self.node.store.compact_event_history_to(max_events);
    }

    pub fn commit_to(&mut self, commit_index: u64) -> Result<(), KvError> {
        let mut metric = KvStorageMetricGuard::start("apply", "sinomo.kv.apply.us");
        let hard_state_before = self.node.hard_state.clone();
        if commit_index <= self.node.applied_index {
            if let Err(error) = self.node.commit_to(commit_index) {
                self.node.hard_state = hard_state_before;
                return Err(error);
            }
            if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
                self.node.hard_state = hard_state_before;
                return Err(error);
            }
            self.record_live_metrics();
            metric.succeed();
            return Ok(());
        }

        let entries = self.node.entries_to_commit(commit_index)?;
        self.node.preflight_commit_entries(entries.as_slice())?;
        self.node.hard_state.commit_index = commit_index;
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        let result = self.node.apply_preflighted_commit(entries, commit_index);
        if result.is_ok() {
            self.record_live_metrics();
            metric.succeed();
        }
        result
    }

    pub fn save_snapshot(&mut self) -> Result<RaftSnapshot, KvError> {
        let mut metric =
            KvStorageMetricGuard::start("snapshot_write", "sinomo.kv.snapshot.write.us");
        let node_before = self.node.clone();
        let hard_state_before = node_before.hard_state.clone();
        let snapshot = RaftSnapshot {
            last_included_index: self.node.applied_index,
            last_included_term: self
                .node
                .term_at(self.node.applied_index)
                .unwrap_or(self.node.snapshot_term),
            kv: self.node.store.try_snapshot()?,
        };
        write_object_file(&self.paths.snapshot, &snapshot)?;
        // Local compaction keeps a tail of recent committed entries in memory for
        // incremental replication; the snapshot file still records the full state at
        // applied_index for cold recovery.
        self.node.local_compact(RAFT_COMPACTION_RETAINED_ENTRIES);
        if let Err(error) = rewrite_wal_entries(&self.paths.wal, &self.node.log) {
            self.node = node_before;
            return Err(error);
        }
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node = node_before;
            return Err(error);
        }
        let written_bytes = fs::metadata(&self.paths.snapshot)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        metrics::counter!("sinomo.kv.snapshot.write.bytes").increment(written_bytes);
        self.record_live_metrics();
        metric.succeed();
        Ok(snapshot)
    }

    pub fn restore_snapshot(&mut self, snapshot: RaftSnapshot) -> Result<(), KvError> {
        let mut metric =
            KvStorageMetricGuard::start("snapshot_restore", "sinomo.kv.snapshot.restore.us");
        let node_before = self.node.clone();
        let hard_state_before = node_before.hard_state.clone();
        write_object_file(&self.paths.snapshot, &snapshot)?;
        self.node.install_snapshot(snapshot)?;
        if let Err(error) = rewrite_wal_entries(&self.paths.wal, &self.node.log) {
            self.node = node_before;
            return Err(error);
        }
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node = node_before;
            return Err(error);
        }
        self.record_live_metrics();
        metric.succeed();
        Ok(())
    }

    fn persist_hard_state(&self) -> Result<(), KvError> {
        self.paths.write_persisted_hard_state(&self.node.hard_state)
    }

    fn persist_hard_state_if_changed(&self, before: &RaftHardState) -> Result<(), KvError> {
        if self.node.hard_state == *before {
            return Ok(());
        }
        self.persist_hard_state()
    }

    fn record_live_metrics(&self) {
        record_cluster_kv_raft_live_metrics(
            &self.paths.root,
            self.node.hard_state.current_term,
            self.node.hard_state.leader_id.clone(),
            self.node.hard_state.commit_index,
            self.node.applied_index,
            self.node.last_log_index(),
            self.node.snapshot_index,
            self.node.log.len(),
        );
    }

    fn restore_after_append_entries_persist_failure(
        &mut self,
        node_before: Option<ReplicatedKvNode>,
        hard_state_before: RaftHardState,
        log_before: Vec<KvLogEntry>,
        txn_responses_before: KvTxnResponseIndex,
        applied_index_before: u64,
    ) {
        if let Some(node_before) = node_before {
            self.node = node_before;
            return;
        }
        self.node.hard_state = hard_state_before;
        self.node.log = log_before;
        self.node.txn_responses = txn_responses_before;
        self.node.applied_index = applied_index_before;
    }
}

impl From<&ClusterBootstrapState> for ClusterKvRaftConfigRecord {
    fn from(state: &ClusterBootstrapState) -> Self {
        let mut voters = Vec::new();
        let mut learners = Vec::new();
        let mut non_voting = Vec::new();
        for member in &state.members {
            if !member.state.is_authorized() {
                continue;
            }
            match member.state {
                ClusterMemberState::Voter => voters.push(member.node_id.clone()),
                ClusterMemberState::Learner => learners.push(member.node_id.clone()),
                ClusterMemberState::NonVoting => non_voting.push(member.node_id.clone()),
                ClusterMemberState::Active => match member.role {
                    ClusterMemberRole::Voter => voters.push(member.node_id.clone()),
                    ClusterMemberRole::Learner => learners.push(member.node_id.clone()),
                    ClusterMemberRole::NonVoting => non_voting.push(member.node_id.clone()),
                },
                ClusterMemberState::Pending
                | ClusterMemberState::Suspect
                | ClusterMemberState::Offline
                | ClusterMemberState::Removed => {}
            }
        }
        voters.sort();
        learners.sort();
        non_voting.sort();
        Self {
            cluster_id: state.cluster_id.clone(),
            term: state.term,
            kv_revision: state.kv_revision,
            voters,
            learners,
            non_voting,
            updated_at: state.updated_at,
        }
    }
}
