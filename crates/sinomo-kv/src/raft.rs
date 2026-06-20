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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
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
    NoEffect {
        snapshot: KvSnapshot,
        txn_response: Option<KvTxnResponse>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftCommitResult {
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
    pub events: Vec<KvEvent>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub voters: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub learners: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub non_voting: Vec<NodeId>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftLeaderElectionOptions {
    pub max_attempts: usize,
}

pub trait ClusterKvRaftLeaderElectionHost {
    type Error: From<KvError>;

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
    matches!(member.state, ClusterMemberState::Voter)
        || (matches!(member.state, ClusterMemberState::Active)
            && matches!(member.role, ClusterMemberRole::Voter))
}

pub fn cluster_kv_raft_node_is_voter(state: &ClusterBootstrapState, node_id: &NodeId) -> bool {
    state.members.iter().any(|member| {
        member.node_id == *node_id
            && member.cluster_id == state.cluster_id
            && member.state.is_authorized()
            && cluster_kv_member_has_raft_vote(member)
            && member
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == sinomo_node::DAEMON_MAIN_RUN_CAPABILITY)
    })
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

pub fn cluster_kv_raft_voter_ids(state: &ClusterBootstrapState) -> BTreeSet<NodeId> {
    state
        .members
        .iter()
        .filter(|member| cluster_kv_raft_node_is_voter(state, &member.node_id))
        .map(|member| member.node_id.clone())
        .collect()
}

pub fn cluster_kv_raft_quorum_size_for_state(state: &ClusterBootstrapState) -> usize {
    cluster_kv_raft_quorum_size(cluster_kv_raft_voter_ids(state).len())
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
    let current_voters = current.voters.iter().cloned().collect::<BTreeSet<_>>();
    let next_voters = next.voters.iter().cloned().collect::<BTreeSet<_>>();
    let removed_voters = current_voters
        .difference(&next_voters)
        .cloned()
        .collect::<Vec<_>>();
    if !removed_voters.is_empty() {
        let retained_voters = current_voters
            .intersection(&next_voters)
            .cloned()
            .collect::<BTreeSet<_>>();
        let current_quorum = cluster_kv_raft_quorum_size(current_voters.len());
        let next_quorum = cluster_kv_raft_quorum_size(next_voters.len());
        if retained_voters.len() < current_quorum || retained_voters.len() < next_quorum {
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
    let added_voters = next_voters
        .difference(&current_voters)
        .cloned()
        .collect::<Vec<_>>();
    if added_voters.len() > 1 {
        return Err(KvError::InvalidRaftConfigTransition {
            detail: format!(
                "raft voter expansion is limited to one voter per committed config change; requested {} additions",
                added_voters.len()
            ),
        });
    }
    if let Some(added_voter) = added_voters.first() {
        let current_learners = current
            .learners
            .iter()
            .chain(current.non_voting.iter())
            .collect::<BTreeSet<_>>();
        if !current_learners.contains(added_voter) {
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
    let voter_ids = voter_ids.into_iter().collect::<BTreeSet<_>>();
    if !voter_ids.contains(local_node_id) {
        return Ok(false);
    }
    let quorum_size = cluster_kv_raft_quorum_size(voter_ids.len());
    let remote_vote_targets = remote_vote_targets
        .into_iter()
        .filter(|voter_id| voter_id != local_node_id && voter_ids.contains(voter_id))
        .collect::<Vec<_>>();

    for _ in 0..options.max_attempts {
        host.begin_election_attempt()?;
        let metadata = host.local_vote_metadata()?;
        let election_term = metadata
            .current_term
            .saturating_add(CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP);
        let vote_request = RaftRequestVoteRequest {
            term: election_term,
            candidate_id: local_node_id.clone(),
            last_log_index: metadata.last_log_index,
            last_log_term: metadata.last_log_term,
        };
        let local_vote = host.apply_local_vote(vote_request.clone())?;
        let mut highest_observed_term = local_vote.term.max(election_term);
        let mut granted = BTreeSet::new();
        if local_vote.vote_granted {
            granted.insert(local_node_id.clone());
        }

        for (target_index, voter_id) in remote_vote_targets.iter().enumerate() {
            if granted.len() >= quorum_size {
                break;
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
                granted.insert(voter_id.clone());
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
    type Error: From<KvError>;

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
    let voter_ids = voter_ids.into_iter().collect::<BTreeSet<_>>();
    if !voter_ids.contains(local_node_id) {
        return Ok(false);
    }
    let quorum_size = cluster_kv_raft_quorum_size(voter_ids.len());
    let remote_vote_targets = remote_vote_targets
        .into_iter()
        .filter(|voter_id| voter_id != local_node_id && voter_ids.contains(voter_id))
        .collect::<Vec<_>>();

    for _ in 0..options.max_attempts {
        host.begin_election_attempt_async().await?;
        let metadata = host.local_vote_metadata_async().await?;
        let election_term = metadata
            .current_term
            .saturating_add(CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP);
        let vote_request = RaftRequestVoteRequest {
            term: election_term,
            candidate_id: local_node_id.clone(),
            last_log_index: metadata.last_log_index,
            last_log_term: metadata.last_log_term,
        };
        let local_vote = host.apply_local_vote_async(vote_request.clone()).await?;
        let mut highest_observed_term = local_vote.term.max(election_term);
        let mut granted = BTreeSet::new();
        if local_vote.vote_granted {
            granted.insert(local_node_id.clone());
        }

        for (target_index, voter_id) in remote_vote_targets.iter().enumerate() {
            if granted.len() >= quorum_size {
                break;
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
                granted.insert(voter_id.clone());
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
/// messages, score endpoints, or interpret the cluster connectivity graph.
pub trait ClusterKvRaftCommandStorage {
    type Error: From<KvError>;

    /// Locks the whole commit sequence against concurrent committers (see
    /// [`lock_cluster_kv_commit_sequence`]). Hosts backed by a shared on-disk
    /// store must implement this; the default (no lock) is only safe for
    /// single-committer hosts such as tests.
    fn lock_cluster_raft_commit_sequence(&mut self) -> Result<Option<File>, Self::Error> {
        Ok(None)
    }

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<ClusterKvRaftUncommittedAppend, Self::Error>;

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(&mut self)
    -> Result<(), Self::Error>;

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, Self::Error>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftQuorumCommitOptions {
    pub timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftQuorumCommitResult {
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvTxnCommitResult {
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterKvRaftTargetReplicationStatus {
    Accepted,
    Rejected,
    Failed,
}

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
    type Error: From<KvError>;
    type Target: Clone;

    fn last_local_raft_log_index(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<u64, Self::Error>;

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
    type Error: From<KvError>;
    type Target: Clone + Send + Sync;

    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, u64, Self::Error>;

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
    let mut next_index = host.last_local_raft_log_index(state)?.saturating_add(1);
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
        let rejected_next_index = response.append.match_index.saturating_add(1);
        next_index = if rejected_next_index < next_index {
            rejected_next_index
        } else if response.append.match_index >= next_index {
            next_index.saturating_div(2).max(1)
        } else {
            next_index.saturating_sub(1).max(1)
        };
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
    let mut next_index = host
        .last_local_raft_log_index_async(state)
        .await?
        .saturating_add(1);
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
        let rejected_next_index = response.append.match_index.saturating_add(1);
        next_index = if rejected_next_index < next_index {
            rejected_next_index
        } else if response.append.match_index >= next_index {
            next_index.saturating_div(2).max(1)
        } else {
            next_index.saturating_sub(1).max(1)
        };
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

    fn cluster_read_only_error(&mut self, detail: String) -> Self::Error;
}

/// Async storage-side Raft quorum commit host.
///
/// This mirrors `ClusterKvRaftQuorumCommitHost`, but every operation that may
/// cross an async storage or transport boundary is supplied by the caller. KV
/// still owns the quorum decision flow; the host owns transport and topology.
pub trait ClusterKvRaftQuorumCommitAsyncHost {
    type Error: From<KvError> + ToString;
    type Target: Clone + Send + Sync;

    /// Async counterpart of
    /// [`ClusterKvRaftCommandStorage::lock_cluster_raft_commit_sequence`].
    fn lock_cluster_raft_commit_sequence_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<File>, Self::Error> {
        Box::pin(async { Ok(None) })
    }

    fn append_cluster_raft_command_uncommitted_or_no_effect_async<'a>(
        &'a mut self,
        command: KvCommand,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftUncommittedAppend, Self::Error>;

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

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

    fn cluster_read_only_error(&mut self, detail: String) -> Self::Error;
}

pub fn commit_cluster_kv_raft_command_with_quorum<H>(
    state: &mut ClusterBootstrapState,
    command: KvCommand,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCommitResult, H::Error>
where
    H: ClusterKvRaftQuorumCommitHost,
    H::Error: ToString,
{
    let _commit_sequence_lock = host.lock_cluster_raft_commit_sequence()?;
    let entry = match host.append_cluster_raft_command_uncommitted_or_no_effect(command)? {
        ClusterKvRaftUncommittedAppend::Appended(entry) => entry,
        ClusterKvRaftUncommittedAppend::NoEffect {
            snapshot,
            txn_response,
        } => {
            return Ok(ClusterKvRaftQuorumCommitResult {
                snapshot,
                txn_response,
            });
        }
    };

    let mut voter_acks = BTreeSet::new();
    if cluster_kv_raft_local_is_voter(state) {
        voter_acks.insert(state.local_node_id.clone());
    }
    let quorum_size = cluster_kv_raft_quorum_size_for_state(state);
    let mut failures = Vec::new();
    let deadline = Instant::now() + options.timeout;
    let targets = host.remote_voter_targets(state)?;
    for target in &targets {
        if voter_acks.len() >= quorum_size {
            break;
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
                voter_acks.insert(host.target_node_id(target).clone());
                host.observe_accepted_append_state(state, target, response.state)?;
            }
            Ok(response) => {
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

    if voter_acks.len() < quorum_size {
        let _ = host.discard_cluster_raft_uncommitted_tail_and_clear_leader();
        return Err(host.cluster_read_only_error(format!(
            "raft command at index {} replicated to {} voters, {} required{}",
            entry.index,
            voter_acks.len(),
            quorum_size,
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        )));
    }

    let commit = host.commit_cluster_raft_index_with_events(entry.index)?;
    // The fan-out replicates the committed history to every member (learners
    // included — they converge event-driven on commit), so it runs even when
    // there are no REMOTE VOTER targets (single-voter clusters).
    host.fan_out_committed_cluster_raft_command(state.clone(), commit.events.clone(), targets)?;
    Ok(ClusterKvRaftQuorumCommitResult {
        snapshot: commit.snapshot,
        txn_response: commit.txn_response,
    })
}

pub async fn commit_cluster_kv_raft_command_with_quorum_async<H>(
    state: &mut ClusterBootstrapState,
    command: KvCommand,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCommitResult, H::Error>
where
    H: ClusterKvRaftQuorumCommitAsyncHost,
{
    let _commit_sequence_lock = host.lock_cluster_raft_commit_sequence_async().await?;
    let entry = match host
        .append_cluster_raft_command_uncommitted_or_no_effect_async(command)
        .await?
    {
        ClusterKvRaftUncommittedAppend::Appended(entry) => entry,
        ClusterKvRaftUncommittedAppend::NoEffect {
            snapshot,
            txn_response,
        } => {
            return Ok(ClusterKvRaftQuorumCommitResult {
                snapshot,
                txn_response,
            });
        }
    };

    let mut voter_acks = BTreeSet::new();
    if cluster_kv_raft_local_is_voter(state) {
        voter_acks.insert(state.local_node_id.clone());
    }
    let quorum_size = cluster_kv_raft_quorum_size_for_state(state);
    let mut failures = Vec::new();
    let deadline = Instant::now() + options.timeout;
    let targets = host.remote_voter_targets_async(state).await?;
    for target in &targets {
        if voter_acks.len() >= quorum_size {
            break;
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
        match host
            .replicate_cluster_raft_to_target_async(state, target, remaining)
            .await
        {
            Ok(response) if response.accepted => {
                host.record_target_replication_status_async(
                    state,
                    target,
                    ClusterKvRaftTargetReplicationStatus::Accepted,
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
                )
                .await?;
                voter_acks.insert(host.target_node_id(target).clone());
                host.observe_accepted_append_state_async(state, target, response.state)
                    .await?;
            }
            Ok(response) => {
                host.record_target_replication_status_async(
                    state,
                    target,
                    ClusterKvRaftTargetReplicationStatus::Rejected,
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
                )
                .await?;
                failures.push(format!(
                    "{} rejected append: {} (term {}, match_index {})",
                    host.target_description(target),
                    response.detail,
                    response.append.term,
                    response.append.match_index
                ));
            }
            Err(error) => {
                host.record_target_replication_status_async(
                    state,
                    target,
                    ClusterKvRaftTargetReplicationStatus::Failed,
                    None,
                )
                .await?;
                failures.push(format!(
                    "{} failed: {}",
                    host.target_description(target),
                    error.to_string()
                ));
            }
        }
    }

    if voter_acks.len() < quorum_size {
        let _ = host
            .discard_cluster_raft_uncommitted_tail_and_clear_leader_async()
            .await;
        return Err(host.cluster_read_only_error(format!(
            "raft command at index {} replicated to {} voters, {} required{}",
            entry.index,
            voter_acks.len(),
            quorum_size,
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        )));
    }

    let commit = host
        .commit_cluster_raft_index_with_events_async(entry.index)
        .await?;
    host.fan_out_committed_cluster_raft_command_async(
        state.clone(),
        commit.events.clone(),
        targets,
    )
    .await?;
    Ok(ClusterKvRaftQuorumCommitResult {
        snapshot: commit.snapshot,
        txn_response: commit.txn_response,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvTxnQuorumDriverOptions {
    pub allow_forwarding: bool,
}

pub trait ClusterKvTxnQuorumHost {
    type Error: From<KvError> + ToString;

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
        txn: &KvTxn,
    ) -> Result<Option<ClusterKvTxnCommitResult>, Self::Error>;

    fn elect_cluster_raft_leader(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, Self::Error>;

    fn reachable_voter_count(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<usize, Self::Error>;

    fn semantic_txn_no_effect_response(
        &mut self,
        txn: &KvTxn,
    ) -> Result<Option<ClusterKvTxnCommitResult>, Self::Error>;

    fn commit_cluster_raft_txn_with_quorum(
        &mut self,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<ClusterKvRaftQuorumCommitResult, Self::Error>;

    fn cluster_read_only_error(&mut self, detail: String) -> Self::Error;
}

pub trait ClusterKvTxnQuorumAsyncHost {
    type Error: From<KvError> + ToString;

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
        txn: &'a KvTxn,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<ClusterKvTxnCommitResult>, Self::Error>;

    fn elect_cluster_raft_leader_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, bool, Self::Error>;

    fn reachable_voter_count_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, usize, Self::Error>;

    fn semantic_txn_no_effect_response_async<'a>(
        &'a mut self,
        txn: &'a KvTxn,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<ClusterKvTxnCommitResult>, Self::Error>;

    fn commit_cluster_raft_txn_with_quorum_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftQuorumCommitResult, Self::Error>;

    fn cluster_read_only_error(&mut self, detail: String) -> Self::Error;
}

pub fn commit_cluster_kv_txn_with_quorum_with_host<H>(
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
    options: ClusterKvTxnQuorumDriverOptions,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, H::Error>
where
    H: ClusterKvTxnQuorumHost,
{
    let _ = host.refresh_current_cluster_kv_store(state);
    if !cluster_kv_raft_local_is_voter(state) {
        if options.allow_forwarding
            && let Some(response) = host.forward_cluster_kv_txn_to_voter(state, &txn)?
        {
            return Ok(response);
        }
        let reachable_voters = host.reachable_voter_count(state)?;
        return Err(host.cluster_read_only_error(format!(
            "local node is not a Raft voter and could not forward to a voter ({} reachable voters, {} required)",
            reachable_voters,
            cluster_kv_raft_quorum_size_for_state(state)
        )));
    }

    if !host.local_is_raft_leader(state)? {
        let forwarding_failure = if options.allow_forwarding {
            match host.forward_cluster_kv_txn_to_voter(state, &txn) {
                Ok(Some(response)) => return Ok(response),
                Ok(None) => None,
                Err(error) => Some(error),
            }
        } else {
            None
        };
        if !options.allow_forwarding {
            return Err(host.cluster_read_only_error(
                "forwarded cluster KV transaction reached a non-leader voter".to_string(),
            ));
        }
        if !host.elect_cluster_raft_leader(state)? {
            let detail = if let Some(forwarding_failure) = forwarding_failure {
                format!(
                    "local node is not the Raft leader and could not win quorum; forwarding also failed: {}",
                    forwarding_failure.to_string()
                )
            } else {
                "local node is not the Raft leader and could not win quorum".to_string()
            };
            return Err(host.cluster_read_only_error(detail));
        }
    }
    if !host.local_is_raft_leader(state)? {
        return Err(host.cluster_read_only_error(
            "local node is not the Raft leader and could not win quorum".to_string(),
        ));
    }
    if let Some(response) = host.semantic_txn_no_effect_response(&txn)? {
        return Ok(response);
    }
    let commit = host.commit_cluster_raft_txn_with_quorum(state, txn)?;
    Ok(ClusterKvTxnCommitResult {
        snapshot: commit.snapshot,
        txn_response: commit.txn_response,
    })
}

pub async fn commit_cluster_kv_txn_with_quorum_async_with_host<H>(
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
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
                .forward_cluster_kv_txn_to_voter_async(state, &txn)
                .await?
        {
            return Ok(response);
        }
        let reachable_voters = host.reachable_voter_count_async(state).await?;
        return Err(host.cluster_read_only_error(format!(
            "local node is not a Raft voter and could not forward to a voter ({} reachable voters, {} required)",
            reachable_voters,
            cluster_kv_raft_quorum_size_for_state(state)
        )));
    }

    if !host.local_is_raft_leader_async(state).await? {
        let forwarding_failure = if options.allow_forwarding {
            match host
                .forward_cluster_kv_txn_to_voter_async(state, &txn)
                .await
            {
                Ok(Some(response)) => return Ok(response),
                Ok(None) => None,
                Err(error) => Some(error),
            }
        } else {
            None
        };
        if !options.allow_forwarding {
            return Err(host.cluster_read_only_error(
                "forwarded cluster KV transaction reached a non-leader voter".to_string(),
            ));
        }
        if !host.elect_cluster_raft_leader_async(state).await? {
            let detail = if let Some(forwarding_failure) = forwarding_failure {
                format!(
                    "local node is not the Raft leader and could not win quorum; forwarding also failed: {}",
                    forwarding_failure.to_string()
                )
            } else {
                "local node is not the Raft leader and could not win quorum".to_string()
            };
            return Err(host.cluster_read_only_error(detail));
        }
    }
    if !host.local_is_raft_leader_async(state).await? {
        return Err(host.cluster_read_only_error(
            "local node is not the Raft leader and could not win quorum".to_string(),
        ));
    }
    if let Some(response) = host.semantic_txn_no_effect_response_async(&txn).await? {
        return Ok(response);
    }
    let commit = host
        .commit_cluster_raft_txn_with_quorum_async(state, txn)
        .await?;
    Ok(ClusterKvTxnCommitResult {
        snapshot: commit.snapshot,
        txn_response: commit.txn_response,
    })
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
    node.compact_event_history_to(policy.max_snapshot_events);
    node.save_snapshot()?;
    Ok(true)
}

#[derive(Clone, Debug)]
pub struct ClusterKvRaftStore {
    paths: ClusterKvPaths,
    compaction_policy: ClusterKvRaftCompactionPolicy,
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
        let prev_log_index = next_index.saturating_sub(1).min(node.last_log_index());
        let earliest_log_index = node.log().first().map(|entry| entry.index);
        let prev_log_resolvable = prev_log_index == node.snapshot_index()
            || earliest_log_index.is_some_and(|first| prev_log_index >= first);
        let include_snapshot = node.snapshot_index() > 0 && !prev_log_resolvable;
        let (snapshot, prev_log_index, entries) = if include_snapshot {
            let mut kv = node.store().snapshot();
            compact_kv_snapshot_event_history(&mut kv, MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS);
            let snapshot = RaftSnapshot {
                last_included_index: node.applied_index(),
                last_included_term: node.term_at(node.applied_index()).unwrap_or(1),
                kv,
            };
            let entries = node
                .log()
                .iter()
                .filter(|entry| entry.index > snapshot.last_included_index)
                .cloned()
                .collect::<Vec<_>>();
            (Some(snapshot), node.applied_index(), entries)
        } else {
            let entries = node
                .log()
                .iter()
                .filter(|entry| entry.index >= next_index)
                .cloned()
                .collect::<Vec<_>>();
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

    pub fn restore_from_snapshot_if_newer(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        self.paths
            .local_cache()
            .restore_raft_from_snapshot_if_newer(node_id.clone(), snapshot)
    }

    pub fn restore_from_snapshot_if_newer_and_notify(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        let restored = self.restore_from_snapshot_if_newer(node_id, snapshot)?;
        if restored {
            notify_cluster_kv_watchers(&self.paths);
        }
        Ok(restored)
    }

    pub fn locked_restore_from_snapshot_if_newer_and_notify(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.restore_from_snapshot_if_newer_and_notify(node_id, snapshot)
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
        Ok(changed.then(|| node.store().snapshot()))
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
        Ok(node.store().snapshot())
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
        Ok(node.store().snapshot())
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
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        if let Some((snapshot, txn_response)) = Self::command_no_effect_response(&node, &command)? {
            return Ok(ClusterKvRaftCommitResult {
                snapshot,
                txn_response,
                events: Vec::new(),
            });
        }
        let before_revision = node.store().revision();
        let term = node.hard_state().current_term.max(1);
        let entry = node.append_local_command(term, command)?;
        node.commit_to(entry.index)?;
        let txn_response = node.txn_response_at(entry.index).cloned();
        self.compact_node_if_needed(&mut node)?;
        let snapshot = node.store().snapshot();
        let events = Self::events_after_revision(&snapshot, before_revision);
        Ok(ClusterKvRaftCommitResult {
            snapshot,
            txn_response,
            events,
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

    pub fn append_command_uncommitted_or_no_effect(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        if let Some((snapshot, txn_response)) = Self::command_no_effect_response(&node, &command)? {
            return Ok(ClusterKvRaftUncommittedAppend::NoEffect {
                snapshot,
                txn_response,
            });
        }
        self.append_command_to_open_node(&mut node, command)
            .map(ClusterKvRaftUncommittedAppend::Appended)
    }

    pub fn locked_append_command_uncommitted_or_no_effect(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.append_command_uncommitted_or_no_effect(node_id, command)
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
        Ok(node.store().snapshot())
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
        let snapshot = node.store().snapshot();
        let events = Self::events_after_revision(&snapshot, before_revision);
        Ok(ClusterKvRaftCommitResult {
            snapshot,
            txn_response,
            events,
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

    fn command_no_effect_response(
        node: &DurableRaftKvNode,
        command: &KvCommand,
    ) -> Result<Option<(KvSnapshot, Option<KvTxnResponse>)>, KvError> {
        match command {
            KvCommand::Op(op) => {
                if node.store().op_response_if_no_effect(op)?.is_some() {
                    return Ok(Some((node.store().snapshot(), None)));
                }
            }
            KvCommand::Txn(txn) => {
                if let Some(response) = node
                    .store()
                    .txn_response_if_selected_branch_no_effect(txn)?
                {
                    return Ok(Some((node.store().snapshot(), Some(response))));
                }
            }
        }
        Ok(None)
    }

    fn events_after_revision(snapshot: &KvSnapshot, before_revision: KvRevision) -> Vec<KvEvent> {
        snapshot
            .events
            .iter()
            .filter(|event| event.revision > before_revision)
            .cloned()
            .collect()
    }
}

pub struct ClusterKvRaftLocalCommandStorage<F>
where
    F: FnMut(&str) -> bool,
{
    store: ClusterKvRaftStore,
    node_id: NodeId,
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
            key_can_defer_materialized_snapshot,
            last_snapshot_publish_wrote: None,
        }
    }

    pub fn take_last_snapshot_publish_wrote(&mut self) -> Option<bool> {
        self.last_snapshot_publish_wrote.take()
    }

    pub fn append_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        self.last_snapshot_publish_wrote = None;
        self.store
            .locked_append_command_uncommitted_or_no_effect(&self.node_id, command)
    }

    pub fn discard_uncommitted_tail_and_clear_leader(&mut self) -> Result<KvSnapshot, KvError> {
        self.last_snapshot_publish_wrote = None;
        let (snapshot, wrote) = self
            .store
            .locked_discard_uncommitted_tail_and_clear_leader_and_publish_report(&self.node_id)?;
        self.last_snapshot_publish_wrote = Some(wrote);
        Ok(snapshot)
    }

    pub fn commit_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        self.last_snapshot_publish_wrote = None;
        let (result, wrote) = self
            .store
            .locked_commit_index_with_events_and_publish_report(
                &self.node_id,
                index,
                &mut self.key_can_defer_materialized_snapshot,
            )?;
        self.last_snapshot_publish_wrote = Some(wrote);
        Ok(result)
    }
}

impl<F> ClusterKvRaftCommandStorage for ClusterKvRaftLocalCommandStorage<F>
where
    F: FnMut(&str) -> bool,
{
    type Error = KvError;

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<ClusterKvRaftUncommittedAppend, Self::Error> {
        self.append_uncommitted_or_no_effect(command)
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), Self::Error> {
        self.discard_uncommitted_tail_and_clear_leader().map(|_| ())
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
                txn_responses: BTreeMap::new(),
            };
            node.install_snapshot(snapshot);
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
                txn_responses: BTreeMap::new(),
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
        Ok(Self {
            node_id,
            paths,
            _lock: lock,
            node,
        })
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
        cache: &KvMaterializedCache,
        discard_uncommitted_tail: bool,
        lock_wait: Duration,
    ) -> Result<Self, KvError> {
        let mut fallback_snapshot = None;
        let mut node = match Self::open_inner(
            node_id.clone(),
            paths.clone(),
            discard_uncommitted_tail,
            lock_wait,
        ) {
            Ok(node) => node,
            Err(error) if !kv_error_is_busy(&error) => {
                let Some(snapshot) = cache.read_snapshot_arc_if_present()? else {
                    return Err(error);
                };
                fallback_snapshot = Some(snapshot);
                quarantine_unreadable_raft_view(&paths);
                Self::open_inner(node_id, paths, discard_uncommitted_tail, lock_wait)?
            }
            Err(error) => return Err(error),
        };
        if node.applied_index() == 0 && node.last_log_index() == 0 {
            let snapshot = match fallback_snapshot {
                Some(snapshot) => Some(snapshot),
                None => cache.read_snapshot_arc_if_present()?,
            };
            let Some(snapshot) = snapshot else {
                return Ok(node);
            };
            node.restore_snapshot(RaftSnapshot {
                last_included_index: 0,
                last_included_term: 0,
                kv: (*snapshot).clone(),
            })?;
        }
        Ok(node)
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

    pub fn applied_index(&self) -> u64 {
        self.node.applied_index
    }

    pub fn snapshot_index(&self) -> u64 {
        self.node.snapshot_index
    }

    pub fn txn_response_at(&self, index: u64) -> Option<&KvTxnResponse> {
        self.node.txn_responses.get(&index)
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
            self.node.apply_preflighted_commit(entries, commit_index)?;
        }
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
            let snapshot_matches_append = append.prev_log_index >= snapshot.last_included_index
                && (append.prev_log_index != snapshot.last_included_index
                    || append.prev_log_term == snapshot.last_included_term);
            let snapshot_is_newer = snapshot.last_included_index > self.applied_index();
            let snapshot_repairs_divergent_compaction = snapshot.last_included_term
                > self.last_log_term()
                && snapshot.kv.revision >= self.store().revision();
            let append_term_is_current = append.term >= self.hard_state().current_term;
            if append_term_is_current
                && snapshot_matches_append
                && (snapshot_is_newer || snapshot_repairs_divergent_compaction)
            {
                self.restore_snapshot(snapshot)?;
                snapshot_restored = true;
            }
        }
        let response = self.append_entries(append)?;
        let snapshot = self.store().snapshot();
        let committed_events = if snapshot_restored || snapshot.revision == before_revision {
            Vec::new()
        } else {
            snapshot
                .events
                .iter()
                .filter(|event| event.revision > before_revision)
                .cloned()
                .collect()
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
        Ok(())
    }

    pub fn clear_known_leader(&mut self) -> Result<(), KvError> {
        let hard_state_before = self.node.hard_state.clone();
        self.node.hard_state.leader_id = None;
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        Ok(())
    }

    pub fn append_local_command(
        &mut self,
        term: u64,
        command: KvCommand,
    ) -> Result<KvLogEntry, KvError> {
        let hard_state_before = self.node.hard_state.clone();
        let log_before = self.node.log.clone();
        let entry = KvLogEntry {
            index: self.node.last_log_index().saturating_add(1),
            term,
            command,
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
        Ok(true)
    }

    pub fn compact_event_history_to(&mut self, max_events: usize) {
        self.node.store.compact_event_history_to(max_events);
    }

    pub fn commit_to(&mut self, commit_index: u64) -> Result<(), KvError> {
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
            return Ok(());
        }

        let entries = self.node.entries_to_commit(commit_index)?;
        self.node.preflight_commit_entries(entries.as_slice())?;
        self.node.hard_state.commit_index = commit_index;
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        self.node.apply_preflighted_commit(entries, commit_index)
    }

    pub fn save_snapshot(&mut self) -> Result<RaftSnapshot, KvError> {
        let node_before = self.node.clone();
        let hard_state_before = node_before.hard_state.clone();
        let snapshot = RaftSnapshot {
            last_included_index: self.node.applied_index,
            last_included_term: self
                .node
                .term_at(self.node.applied_index)
                .unwrap_or(self.node.snapshot_term),
            kv: self.node.store.snapshot(),
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
        Ok(snapshot)
    }

    pub fn restore_snapshot(&mut self, snapshot: RaftSnapshot) -> Result<(), KvError> {
        let node_before = self.node.clone();
        let hard_state_before = node_before.hard_state.clone();
        write_object_file(&self.paths.snapshot, &snapshot)?;
        self.node.install_snapshot(snapshot);
        if let Err(error) = rewrite_wal_entries(&self.paths.wal, &self.node.log) {
            self.node = node_before;
            return Err(error);
        }
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node = node_before;
            return Err(error);
        }
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

    fn restore_after_append_entries_persist_failure(
        &mut self,
        node_before: Option<ReplicatedKvNode>,
        hard_state_before: RaftHardState,
        log_before: Vec<KvLogEntry>,
        txn_responses_before: BTreeMap<u64, KvTxnResponse>,
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
