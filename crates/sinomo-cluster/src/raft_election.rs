use crate::control_plane_options::ClusterRaftLeaderElectionOptions;
use crate::kv_local_store::ClusterKvPathsExt;
use crate::kv_local_store::LocalClusterRaftCommandStorage;
use crate::kv_local_store::kv_err;
use crate::local_state_store::project_cluster_raft_config_record;
use crate::prelude::*;
use crate::raft_quorum::{
    ClusterRaftBlockingCancellationPolicy, ClusterRaftBlockingDeadlinePolicy,
    run_cluster_raft_supervised_blocking,
};
#[allow(unused_imports)]
use sinomo_kv::KvError;
use sinomo_kv::{ClusterKvRaftLocalVoteMetadata, ClusterRaftVoteRequest, ClusterRaftVoteResponse};
use sinomo_kv::{RaftRequestVoteRequest, RaftRequestVoteResponse};
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub trait ClusterRaftVoteTransportHost {
    #[cfg(any(test, feature = "test-support"))]
    fn send_raft_vote(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftVoteResponse, NetworkError>;

    /// Dispatch one election round without serializing voter latency. Real
    /// transports must override this seam; the default fails closed so a new
    /// production host cannot accidentally restore the WAN-sized serial path.
    fn send_raft_votes(
        &mut self,
        _state: &ClusterBootstrapState,
        targets: Vec<NodeId>,
        _request: &ClusterRaftVoteRequest,
        _timeout: Duration,
        _required_grants: usize,
    ) -> Result<Vec<(NodeId, Result<ClusterRaftVoteResponse, NetworkError>)>, NetworkError> {
        Ok(targets
            .into_iter()
            .map(|target| {
                (
                    target,
                    Err(NetworkError::ClusterReadOnly {
                        detail: "synchronous Raft election batch transport is unavailable"
                            .to_string(),
                    }),
                )
            })
            .collect())
    }
}

pub(crate) trait ClusterRaftVoteTransportAsyncHost {
    /// Async batch counterpart to [`ClusterRaftVoteTransportHost::send_raft_votes`].
    /// The fail-closed default prevents hidden sequential voter dispatch.
    fn send_raft_votes_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        targets: Vec<NodeId>,
        _request: &'a ClusterRaftVoteRequest,
        _timeout: Duration,
        _required_grants: usize,
    ) -> ClusterMembershipRpcFuture<'a, Vec<(NodeId, Result<ClusterRaftVoteResponse, NetworkError>)>>
    {
        Box::pin(async move {
            Ok(targets
                .into_iter()
                .map(|target| {
                    (
                        target,
                        Err(NetworkError::ClusterReadOnly {
                            detail: "asynchronous Raft election batch transport is unavailable"
                                .to_string(),
                        }),
                    )
                })
                .collect())
        })
    }
}

fn cluster_raft_campaign_jitter_for_voters(
    node_id: &NodeId,
    voter_ids: impl IntoIterator<Item = NodeId>,
    durable_term: u64,
    max: Duration,
) -> Duration {
    if max.is_zero() {
        return Duration::ZERO;
    }
    let max_nanos = max.as_nanos().min(u128::from(u64::MAX));
    if max_nanos == 0 {
        return Duration::ZERO;
    }
    let mut voters = voter_ids.into_iter().collect::<BTreeSet<_>>();
    voters.insert(node_id.clone());
    let voter_count = voters.len().max(1) as u128;
    let voter_rank = voters
        .iter()
        .position(|voter_id| voter_id == node_id)
        .unwrap_or_default() as u128;
    // Use a relative monotonic delay: an inter-node wall-clock offset can no
    // longer cancel the difference between two voter slots and phase-lock
    // their campaigns. Every persisted self-vote advances the term by the
    // election bump, so reverse the deterministic voter order on each durable
    // election generation. A fixed difference in caller arrival time that
    // aligns two candidates in one generation is therefore broken in the
    // next. Same-node calls for one generation remain coalesced by the
    // election-round lock below.
    let election_generation = durable_term / sinomo_kv::CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP;
    let scheduled_rank = if election_generation.is_multiple_of(2) {
        voter_rank
    } else {
        voter_count.saturating_sub(1).saturating_sub(voter_rank)
    };
    let delay_nanos = scheduled_rank.saturating_mul(max_nanos) / voter_count;
    Duration::from_nanos(delay_nanos as u64)
}

#[derive(Debug)]
struct ClusterRaftElectionRoundGuard {
    _lock: std::fs::File,
    waited_for_round_lock: bool,
}

fn cluster_raft_election_round_lock_is_busy(source: &std::io::Error) -> bool {
    source.kind() == std::io::ErrorKind::WouldBlock
        || matches!(source.raw_os_error(), Some(32 | 33))
}

fn cluster_raft_election_round_lock_retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(1_u64 << attempt.min(4))
}

fn lock_cluster_raft_election_round_before(
    paths: &ClusterPaths,
    local_node_id: &NodeId,
    deadline: Instant,
) -> Result<ClusterRaftElectionRoundGuard, NetworkError> {
    remaining_cluster_raft_election_budget(deadline)?;
    let lock_root = paths.kv_paths().raft_wal_paths(local_node_id).root;
    std::fs::create_dir_all(&lock_root).map_err(|source| NetworkError::Io {
        operation: "create cluster raft election round lock directory".to_string(),
        detail: format!("{}: {source}", lock_root.display()),
    })?;
    let lock_path = lock_root.join("election-round.lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|source| NetworkError::Io {
            operation: "open cluster raft election round lock".to_string(),
            detail: format!("{}: {source}", lock_path.display()),
        })?;
    let mut attempt = 0_u32;
    let mut waited_for_round_lock = false;
    loop {
        match fs2::FileExt::try_lock_exclusive(&lock) {
            Ok(()) => {
                if Instant::now() >= deadline {
                    drop(lock);
                    return Err(cluster_raft_election_timeout_error());
                }
                return Ok(ClusterRaftElectionRoundGuard {
                    _lock: lock,
                    waited_for_round_lock,
                });
            }
            Err(source) if cluster_raft_election_round_lock_is_busy(&source) => {
                waited_for_round_lock = true;
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(cluster_raft_election_timeout_error());
                }
                std::thread::sleep(
                    cluster_raft_election_round_lock_retry_delay(attempt).min(remaining),
                );
                attempt = attempt.saturating_add(1);
            }
            Err(source) => {
                return Err(NetworkError::Io {
                    operation: "lock cluster raft election round".to_string(),
                    detail: format!("{}: {source}", lock_path.display()),
                });
            }
        }
    }
}

fn cluster_raft_election_timeout_error() -> NetworkError {
    NetworkError::Io {
        operation: "cluster raft leader election".to_string(),
        detail: "absolute election deadline elapsed".to_string(),
    }
}

fn remaining_cluster_raft_election_budget(deadline: Instant) -> Result<Duration, NetworkError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(cluster_raft_election_timeout_error())
    } else {
        Ok(remaining)
    }
}

fn exact_remote_voter_ids(
    local_node_id: &NodeId,
    voter_ids: impl IntoIterator<Item = NodeId>,
) -> Vec<NodeId> {
    voter_ids
        .into_iter()
        .filter(|node_id| node_id != local_node_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn project_committed_cluster_raft_config_before(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    deadline: Instant,
) -> Result<bool, NetworkError> {
    remaining_cluster_raft_election_budget(deadline)?;
    let wal_paths = paths.kv_paths().raft_wal_paths(&state.local_node_id);
    if !wal_paths.has_durable_state().map_err(kv_err)? {
        // A brand-new cluster has no committed config to project. Avoid
        // manufacturing a resident node merely to prove that absence; the
        // later self-vote phase will initialize it under the campaign fence.
        return Ok(false);
    }
    let config_entry = {
        // Election recovery must also work before this process has retained a
        // resident node (or after the bounded resident registry evicts an idle
        // slot while another campaign is in its network phase). This is a
        // local committed-config projection, not an authoritative application
        // ReadIndex, so hydrate the durable node under the election deadline
        // instead of requiring a pre-existing resident entry.
        let mut storage = LocalClusterRaftCommandStorage::new_with_operation_deadline(
            paths,
            state.local_node_id.clone(),
            deadline,
        );
        storage.read_index_entry(&sinomo_kv::cluster_kv_raft_config_key())?
    };
    let Some(entry) = config_entry else {
        return Ok(false);
    };
    remaining_cluster_raft_election_budget(deadline)?;
    let config: sinomo_kv::ClusterKvRaftConfigRecord = entry.decode().map_err(kv_err)?;
    project_cluster_raft_config_record(state, &config)?;
    remaining_cluster_raft_election_budget(deadline)?;
    Ok(true)
}

async fn run_cluster_raft_election_blocking_before<T, F>(
    runtime: &tokio::runtime::Handle,
    deadline: Instant,
    operation: &'static str,
    work: F,
) -> Result<T, NetworkError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, NetworkError> + Send + 'static,
{
    remaining_cluster_raft_election_budget(deadline)?;
    run_cluster_raft_supervised_blocking(
        runtime,
        Some((
            deadline,
            cluster_raft_election_timeout_error(),
            ClusterRaftBlockingDeadlinePolicy::DeadlineBounded,
        )),
        ClusterRaftBlockingCancellationPolicy::CancelQueued,
        "leader-election",
        operation,
        operation,
        work,
    )
    .await
}

struct NetworkClusterKvRaftLeaderElectionHost<'a, H> {
    paths: &'a ClusterPaths,
    state: &'a ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    operation_deadline: Instant,
    config_fence: Option<sinomo_kv::KvEntry>,
    campaign: Option<ClusterRaftCampaignTraceGuard>,
    last_self_vote_term: Option<u64>,
    transport: &'a mut H,
}

impl<'a, H> NetworkClusterKvRaftLeaderElectionHost<'a, H> {
    fn new(
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        options: ClusterRaftLeaderElectionOptions,
        operation_deadline: Instant,
        config_fence: Option<sinomo_kv::KvEntry>,
        campaign: ClusterRaftCampaignTraceGuard,
        transport: &'a mut H,
    ) -> Self {
        Self {
            paths,
            state,
            options,
            operation_deadline,
            config_fence,
            campaign: Some(campaign),
            last_self_vote_term: None,
            transport,
        }
    }
}

impl<H> sinomo_kv::ClusterKvRaftLeaderElectionHost for NetworkClusterKvRaftLeaderElectionHost<'_, H>
where
    H: ClusterRaftVoteTransportHost,
{
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn begin_election_attempt(&mut self) -> Result<Option<bool>, Self::Error> {
        if self.campaign.is_some() {
            return Ok(None);
        }
        // The first attempt is paced before the outer election-round lock is
        // acquired. A retry reaches this hook only after its durable self-vote
        // released the short campaign/commit lock. Recompute the ranked delay
        // from that newly durable term, and never sleep while owning the
        // campaign lock.
        let observation_before_wait = locked_cluster_raft_leader_observation_before(
            self.paths,
            self.state,
            self.operation_deadline,
        )?;
        if self
            .last_self_vote_term
            .is_some_and(|term| observation_before_wait.current_term > term)
        {
            // Raft traffic outside this campaign advanced the local term after
            // our previous self-vote (for example, this node granted the
            // competing candidate during a crossed election). Treat that as a
            // completed competing round instead of immediately preempting it
            // with another term.
            return Ok(Some(false));
        }
        let scheduled_voter_ids =
            sinomo_kv::cluster_kv_raft_voter_ids(self.state).map_err(kv_err)?;
        let jitter = cluster_raft_campaign_jitter_for_voters(
            &self.state.local_node_id,
            scheduled_voter_ids,
            observation_before_wait.current_term,
            self.options.campaign_jitter_max,
        );
        if !jitter.is_zero() {
            let remaining = remaining_cluster_raft_election_budget(self.operation_deadline)?;
            if jitter >= remaining {
                return Err(cluster_raft_election_timeout_error());
            }
            std::thread::sleep(jitter);
        }

        let mut campaign = observed_cluster_raft_campaign_lock_before(
            self.paths,
            &self.state.local_node_id,
            "sync",
            self.operation_deadline,
        )?;
        let waited_for_campaign_lock = campaign.waited_for_campaign_lock;
        let (leader_id, metadata, config_entry) =
            locked_cluster_raft_leader_observation_and_config_before(
                self.paths,
                &self.state.local_node_id,
                self.operation_deadline,
            )?;
        if config_entry != self.config_fence {
            campaign.complete(
                "campaign_suppressed",
                format_args!("committed Raft configuration changed before retry admission"),
            );
            return Ok(Some(false));
        }
        let observation_after_wait = ClusterRaftLeaderObservation {
            leader_id: leader_id.filter(|leader_id| self.state.node_is_raft_voter(leader_id)),
            current_term: metadata.current_term,
        };
        let remote_leader_blocks_campaign =
            observed_remote_leader_blocks_campaign(self.paths, self.state, &observation_after_wait);
        if let Some(outcome) = cluster_raft_leader_outcome_after_campaign_wait(
            &self.state.local_node_id,
            waited_for_campaign_lock,
            &observation_before_wait,
            &observation_after_wait,
            remote_leader_blocks_campaign,
        ) {
            campaign.complete(
                if outcome {
                    "existing_local_leader"
                } else {
                    "campaign_suppressed"
                },
                format_args!(
                    "used durable cluster Raft state observed after retry pacing and admission"
                ),
            );
            return Ok(Some(outcome));
        }
        self.campaign = Some(campaign);
        Ok(None)
    }

    fn local_vote_metadata(&mut self) -> Result<ClusterKvRaftLocalVoteMetadata, Self::Error> {
        self.paths
            .kv_paths()
            .raft_store()
            .locked_local_vote_metadata_before(&self.state.local_node_id, self.operation_deadline)
            .map_err(kv_err)
    }

    fn apply_local_vote(
        &mut self,
        vote: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, Self::Error> {
        let result = self
            .paths
            .kv_paths()
            .raft_store()
            .locked_apply_vote_before(&self.state.local_node_id, vote, self.operation_deadline)
            .map_err(kv_err);
        // The global commit sequence protects only observation and the durable
        // self-vote. Endpoint selection and network fanout must never own it.
        if let Some(mut campaign) = self.campaign.take() {
            campaign.record_phase(
                "durable_self_vote",
                campaign.acquired_at.elapsed(),
                if result.is_ok() { "persisted" } else { "error" },
                format_args!("released campaign lock after durable self-vote phase"),
            );
            drop(campaign);
        }
        if let Ok(response) = &result {
            self.last_self_vote_term = Some(response.term);
        }
        result
    }

    fn request_remote_votes(
        &mut self,
        voter_ids: Vec<NodeId>,
        vote: &RaftRequestVoteRequest,
        required_grants: usize,
    ) -> Result<Vec<(NodeId, Result<RaftRequestVoteResponse, Self::Error>)>, Self::Error> {
        remaining_cluster_raft_election_budget(self.operation_deadline)?;
        // These IDs are the exact committed configuration captured under the
        // election fence. A KV-backed live-endpoint lookup here would be
        // self-referential: this vote may be required to restore KV authority.
        let selected = exact_remote_voter_ids(&self.state.local_node_id, voter_ids);
        remaining_cluster_raft_election_budget(self.operation_deadline)?;
        let request = ClusterRaftVoteRequest {
            state: self.state.clone(),
            vote: vote.clone(),
        };
        let timeout =
            self.options
                .routed_node_command_timeout(remaining_cluster_raft_election_budget(
                    self.operation_deadline,
                )?);
        self.transport
            .send_raft_votes(self.state, selected, &request, timeout, required_grants)
            .map(|outcomes| {
                outcomes
                    .into_iter()
                    .map(|(node_id, response)| (node_id, response.map(|response| response.vote)))
                    .collect()
            })
    }

    fn observe_raft_term(&mut self, term: u64) -> Result<(), Self::Error> {
        locked_observe_cluster_raft_term(self.paths, &self.state.local_node_id, term).map(|_| ())
    }

    fn set_raft_leader(&mut self, term: u64) -> Result<(), Self::Error> {
        let _campaign = observed_cluster_raft_campaign_lock_before(
            self.paths,
            &self.state.local_node_id,
            "sync-finalize",
            self.operation_deadline,
        )?;
        let published = self
            .paths
            .kv_paths()
            .raft_store()
            .locked_set_leader_compact_if_config_before(
                &self.state.local_node_id,
                term,
                self.config_fence.as_ref(),
                self.operation_deadline,
            )
            .map_err(kv_err)?;
        if published {
            Ok(())
        } else {
            Err(NetworkError::ClusterReadOnly {
                detail: "committed Raft configuration changed during leader election".to_string(),
            })
        }
    }
}

struct NetworkClusterKvRaftLeaderElectionAsyncHost<'a, H> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
    state: &'a ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    operation_deadline: Instant,
    config_fence: Option<sinomo_kv::KvEntry>,
    campaign: Option<ClusterRaftCampaignTraceGuard>,
    last_self_vote_term: Option<u64>,
    transport: &'a mut H,
}

impl<'a, H> NetworkClusterKvRaftLeaderElectionAsyncHost<'a, H> {
    fn new(
        runtime: &'a tokio::runtime::Handle,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        options: ClusterRaftLeaderElectionOptions,
        operation_deadline: Instant,
        config_fence: Option<sinomo_kv::KvEntry>,
        campaign: ClusterRaftCampaignTraceGuard,
        transport: &'a mut H,
    ) -> Self {
        Self {
            runtime,
            paths,
            state,
            options,
            operation_deadline,
            config_fence,
            campaign: Some(campaign),
            last_self_vote_term: None,
            transport,
        }
    }
}

impl<H> sinomo_kv::ClusterKvRaftLeaderElectionAsyncHost
    for NetworkClusterKvRaftLeaderElectionAsyncHost<'_, H>
where
    H: ClusterRaftVoteTransportAsyncHost + Send,
{
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn begin_election_attempt_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, Option<bool>, Self::Error> {
        Box::pin(async move {
            if self.campaign.is_some() {
                return Ok(None);
            }
            let observation_paths = self.paths.clone();
            let observation_state = self.state.clone();
            let deadline = self.operation_deadline;
            let runtime = self.runtime.clone();
            let observation_before_wait = run_cluster_raft_election_blocking_before(
                &runtime,
                deadline,
                "observe cluster Raft state before retry pacing",
                move || {
                    locked_cluster_raft_leader_observation_before(
                        &observation_paths,
                        &observation_state,
                        deadline,
                    )
                },
            )
            .await?;
            if self
                .last_self_vote_term
                .is_some_and(|term| observation_before_wait.current_term > term)
            {
                return Ok(Some(false));
            }
            let scheduled_voter_ids =
                sinomo_kv::cluster_kv_raft_voter_ids(self.state).map_err(kv_err)?;
            let jitter = cluster_raft_campaign_jitter_for_voters(
                &self.state.local_node_id,
                scheduled_voter_ids,
                observation_before_wait.current_term,
                self.options.campaign_jitter_max,
            );
            if !jitter.is_zero() {
                let remaining = remaining_cluster_raft_election_budget(deadline)?;
                if jitter >= remaining {
                    return Err(cluster_raft_election_timeout_error());
                }
                tokio::time::sleep(jitter).await;
            }

            let paths = self.paths.clone();
            let state = self.state.clone();
            let config_fence = self.config_fence.clone();
            let (campaign, outcome) = run_cluster_raft_election_blocking_before(
                &runtime,
                deadline,
                "cluster Raft campaign admission",
                move || {
                    let mut campaign = observed_cluster_raft_campaign_lock_before(
                        &paths,
                        &state.local_node_id,
                        "async",
                        deadline,
                    )?;
                    let waited_for_campaign_lock = campaign.waited_for_campaign_lock;
                    let (leader_id, metadata, config_entry) =
                        locked_cluster_raft_leader_observation_and_config_before(
                            &paths,
                            &state.local_node_id,
                            deadline,
                        )?;
                    if config_entry != config_fence {
                        campaign.complete(
                            "campaign_suppressed",
                            format_args!(
                                "committed Raft configuration changed before retry admission"
                            ),
                        );
                        return Ok((campaign, Some(false)));
                    }
                    let observation_after_wait = ClusterRaftLeaderObservation {
                        leader_id: leader_id
                            .filter(|leader_id| state.node_is_raft_voter(leader_id)),
                        current_term: metadata.current_term,
                    };
                    let remote_leader_blocks_campaign = observed_remote_leader_blocks_campaign(
                        &paths,
                        &state,
                        &observation_after_wait,
                    );
                    let outcome = cluster_raft_leader_outcome_after_campaign_wait(
                        &state.local_node_id,
                        waited_for_campaign_lock,
                        &observation_before_wait,
                        &observation_after_wait,
                        remote_leader_blocks_campaign,
                    );
                    if let Some(outcome) = outcome {
                        campaign.complete(
                            if outcome {
                                "existing_local_leader"
                            } else {
                                "campaign_suppressed"
                            },
                            format_args!(
                                "used durable cluster Raft state observed after retry pacing and admission"
                            ),
                        );
                    }
                    Ok::<_, NetworkError>((campaign, outcome))
                },
            )
            .await?;
            if outcome.is_none() {
                self.campaign = Some(campaign);
            }
            Ok(outcome)
        })
    }

    fn local_vote_metadata_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterKvRaftLocalVoteMetadata, Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        let deadline = self.operation_deadline;
        Box::pin(async move {
            let metadata = run_cluster_raft_election_blocking_before(
                &runtime,
                deadline,
                "cluster Raft local vote metadata",
                move || {
                    paths
                        .kv_paths()
                        .raft_store()
                        .locked_local_vote_metadata_before(&node_id, deadline)
                        .map_err(kv_err)
                },
            )
            .await?;
            remaining_cluster_raft_election_budget(deadline)?;
            Ok(metadata)
        })
    }

    fn apply_local_vote_async<'a>(
        &'a mut self,
        vote: RaftRequestVoteRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, RaftRequestVoteResponse, Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        let deadline = self.operation_deadline;
        Box::pin(async move {
            let mut campaign = self.campaign.take();
            let result = run_cluster_raft_election_blocking_before(
                &runtime,
                deadline,
                "cluster Raft durable self-vote",
                move || {
                    let result = paths
                        .kv_paths()
                        .raft_store()
                        .locked_apply_vote_before(&node_id, vote, deadline)
                        .map_err(kv_err);
                    if let Some(campaign) = campaign.as_mut() {
                        campaign.record_phase(
                            "durable_self_vote",
                            campaign.acquired_at.elapsed(),
                            if result.is_ok() { "persisted" } else { "error" },
                            format_args!("released campaign lock after durable self-vote phase"),
                        );
                    }
                    drop(campaign);
                    result
                },
            )
            .await;
            if let Ok(response) = &result {
                self.last_self_vote_term = Some(response.term);
            }
            result
        })
    }

    fn request_remote_votes_async<'a>(
        &'a mut self,
        voter_ids: Vec<NodeId>,
        vote: &'a RaftRequestVoteRequest,
        required_grants: usize,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        Vec<(NodeId, Result<RaftRequestVoteResponse, Self::Error>)>,
        Self::Error,
    > {
        let request = ClusterRaftVoteRequest {
            state: self.state.clone(),
            vote: vote.clone(),
        };
        let transport = &mut *self.transport;
        let state = self.state;
        let deadline = self.operation_deadline;
        let options = self.options;
        Box::pin(async move {
            remaining_cluster_raft_election_budget(deadline)?;
            let selected = exact_remote_voter_ids(&state.local_node_id, voter_ids);
            let timeout = options
                .routed_node_command_timeout(remaining_cluster_raft_election_budget(deadline)?);
            transport
                .send_raft_votes_async(state, selected, &request, timeout, required_grants)
                .await
                .map(|outcomes| {
                    outcomes
                        .into_iter()
                        .map(|(node_id, response)| {
                            (node_id, response.map(|response| response.vote))
                        })
                        .collect()
                })
        })
    }

    fn observe_raft_term_async<'a>(
        &'a mut self,
        term: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        // Close old-term authority synchronously at observation time. The
        // durable task may wait for operation/resident admission, and the
        // election future may be aborted while it waits, but neither can make
        // local reads/proposals authoritative again before hard state catches
        // up.
        let publish_result = crate::raft_quorum::publish_cluster_raft_higher_term_reactively(
            self.paths,
            &self.state.local_node_id,
            term,
        );
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            publish_result?;
            // Higher terms are safety information, not optional work. The
            // shared supervisor owns terminal completion independently of the
            // election future, so caller cancellation cannot detach durable
            // term persistence after the pending safety floor is published.
            run_cluster_raft_supervised_blocking(
                &runtime,
                None,
                ClusterRaftBlockingCancellationPolicy::CompleteAlways,
                "leader-election",
                "higher-term-observation",
                "cluster raft leader election term observation",
                move || locked_observe_cluster_raft_term(&paths, &node_id, term).map(|_| ()),
            )
            .await
        })
    }

    fn set_raft_leader_async<'a>(
        &'a mut self,
        term: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        let deadline = self.operation_deadline;
        let config_fence = self.config_fence.clone();
        Box::pin(async move {
            run_cluster_raft_election_blocking_before(
                &runtime,
                deadline,
                "cluster Raft leader publication",
                move || {
                    let _campaign = observed_cluster_raft_campaign_lock_before(
                        &paths,
                        &node_id,
                        "async-finalize",
                        deadline,
                    )?;
                    let published = paths
                        .kv_paths()
                        .raft_store()
                        .locked_set_leader_compact_if_config_before(
                            &node_id,
                            term,
                            config_fence.as_ref(),
                            deadline,
                        )
                        .map_err(kv_err)?;
                    if published {
                        Ok(())
                    } else {
                        Err(NetworkError::ClusterReadOnly {
                            detail: "committed Raft configuration changed during leader election"
                                .to_string(),
                        })
                    }
                },
            )
            .await
        })
    }
}

pub(crate) fn locked_observe_cluster_raft_term(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<bool, NetworkError> {
    let kv_paths = paths.kv_paths();
    crate::raft_quorum::publish_cluster_raft_higher_term_reactively(paths, node_id, term)?;
    kv_paths
        .locked_observe_raft_term_compact(node_id, term)
        .map_err(kv_err)
}

#[cfg(any(test, feature = "test-support"))]
pub fn locked_set_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<(), NetworkError> {
    paths
        .kv_paths()
        .locked_set_raft_leader_compact(node_id, term)
        .map_err(kv_err)
}

struct ClusterRaftCampaignPhaseRecord {
    phase: &'static str,
    recorded_at: SystemTime,
    duration: Duration,
    target_count: usize,
    outcome: &'static str,
    detail: String,
}

fn cluster_raft_campaign_phase_trace(
    mode: &'static str,
    local_node_id: &NodeId,
    waited_for_campaign_lock: bool,
    record: &ClusterRaftCampaignPhaseRecord,
) {
    let recorded_at_unix_ns = u64::try_from(
        record
            .recorded_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    )
    .unwrap_or(u64::MAX);
    let duration_us = u64::try_from(record.duration.as_micros()).unwrap_or(u64::MAX);
    tracing::trace!(
        target: "sinomo.cluster.formation",
        mode,
        phase = record.phase,
        local_node_id = %local_node_id,
        waited_for_campaign_lock,
        recorded_at_unix_ns,
        duration_us,
        target_count = record.target_count,
        outcome = record.outcome,
        detail = %record.detail,
        "cluster Raft campaign phase"
    );
}

struct ClusterRaftCampaignTraceGuard {
    lock: Option<std::fs::File>,
    mode: &'static str,
    local_node_id: NodeId,
    waited_for_campaign_lock: bool,
    acquired_at: Instant,
    target_count: usize,
    outcome: &'static str,
    trace_enabled: bool,
    phase_records: Vec<ClusterRaftCampaignPhaseRecord>,
}

impl ClusterRaftCampaignTraceGuard {
    fn new(
        lock: std::fs::File,
        mode: &'static str,
        local_node_id: NodeId,
        waited_for_campaign_lock: bool,
        acquired_at: Instant,
        acquisition_duration: Duration,
        trace_enabled: bool,
    ) -> Self {
        let mut campaign = Self {
            lock: Some(lock),
            mode,
            local_node_id,
            waited_for_campaign_lock,
            acquired_at,
            target_count: 0,
            outcome: "dropped_before_completion",
            trace_enabled,
            phase_records: if trace_enabled {
                Vec::with_capacity(5)
            } else {
                Vec::new()
            },
        };
        campaign.record_phase(
            "campaign_lock_acquire",
            acquisition_duration,
            "acquired",
            format_args!("acquired the cluster KV commit-sequence campaign lock"),
        );
        campaign
    }

    fn set_target_count(&mut self, target_count: usize) {
        self.target_count = target_count;
    }

    fn record_phase(
        &mut self,
        phase: &'static str,
        duration: Duration,
        outcome: &'static str,
        detail: std::fmt::Arguments<'_>,
    ) {
        if !self.trace_enabled {
            return;
        }
        self.phase_records.push(ClusterRaftCampaignPhaseRecord {
            phase,
            recorded_at: SystemTime::now(),
            duration,
            target_count: self.target_count,
            outcome,
            detail: detail.to_string(),
        });
    }

    fn record_leader_observation_error(
        &mut self,
        duration: Duration,
        error: &impl std::fmt::Display,
    ) {
        self.record_phase(
            "durable_leader_observation",
            duration,
            "error",
            format_args!("{error}"),
        );
        self.complete("leader_observation_error", format_args!("{error}"));
    }

    fn complete(&mut self, outcome: &'static str, detail: std::fmt::Arguments<'_>) {
        self.outcome = outcome;
        self.record_phase(
            "campaign_complete",
            self.acquired_at.elapsed(),
            outcome,
            detail,
        );
    }
}

impl Drop for ClusterRaftCampaignTraceGuard {
    fn drop(&mut self) {
        // Never run a tracing subscriber while holding the global commit
        // sequence lock. The observation layer records into a mutex-protected
        // buffer and may synchronously write formatted logs, so emitting an
        // event here would perturb the exclusion interval being measured.
        drop(self.lock.take());
        let outcome = if std::thread::panicking() {
            "unwound"
        } else {
            self.outcome
        };
        self.record_phase(
            "campaign_lock_drop",
            self.acquired_at.elapsed(),
            outcome,
            format_args!("released the cluster KV commit-sequence campaign lock"),
        );
        for record in std::mem::take(&mut self.phase_records) {
            cluster_raft_campaign_phase_trace(
                self.mode,
                &self.local_node_id,
                self.waited_for_campaign_lock,
                &record,
            );
        }
    }
}

#[cfg(test)]
fn try_lock_cluster_raft_campaign(
    paths: &ClusterPaths,
) -> (Result<std::fs::File, NetworkError>, bool) {
    try_lock_cluster_raft_campaign_before(
        paths,
        Instant::now() + sinomo_kv::CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT,
    )
}

fn try_lock_cluster_raft_campaign_before(
    paths: &ClusterPaths,
    deadline: Instant,
) -> (Result<std::fs::File, NetworkError>, bool) {
    let kv_paths = paths.kv_paths();
    match sinomo_kv::lock_cluster_kv_commit_sequence(&kv_paths, Duration::ZERO) {
        Ok(lock) if Instant::now() < deadline => (Ok(lock), false),
        Ok(lock) => {
            drop(lock);
            (Err(cluster_raft_election_timeout_error()), false)
        }
        Err(KvError::Busy { .. }) => {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return (Err(cluster_raft_election_timeout_error()), true);
            }
            let result = sinomo_kv::lock_cluster_kv_commit_sequence(&kv_paths, remaining)
                .map_err(kv_err)
                .and_then(|lock| {
                    if Instant::now() >= deadline {
                        drop(lock);
                        Err(cluster_raft_election_timeout_error())
                    } else {
                        Ok(lock)
                    }
                });
            (result, true)
        }
        Err(error) => (Err(kv_err(error)), false),
    }
}

#[cfg(test)]
fn lock_cluster_raft_campaign(paths: &ClusterPaths) -> Result<(std::fs::File, bool), NetworkError> {
    let (result, waited_for_campaign_lock) = try_lock_cluster_raft_campaign(paths);
    result.map(|lock| (lock, waited_for_campaign_lock))
}

#[cfg(test)]
fn observed_cluster_raft_campaign_lock(
    paths: &ClusterPaths,
    local_node_id: &NodeId,
    mode: &'static str,
) -> Result<ClusterRaftCampaignTraceGuard, NetworkError> {
    let trace_enabled =
        tracing::enabled!(target: "sinomo.cluster.formation", tracing::Level::TRACE);
    let started = Instant::now();
    let (result, waited_for_campaign_lock) = try_lock_cluster_raft_campaign(paths);
    match result {
        Ok(lock) => {
            let acquired_at = Instant::now();
            let acquisition_duration = started.elapsed();
            Ok(ClusterRaftCampaignTraceGuard::new(
                lock,
                mode,
                local_node_id.clone(),
                waited_for_campaign_lock,
                acquired_at,
                acquisition_duration,
                trace_enabled,
            ))
        }
        Err(error) => {
            if trace_enabled {
                cluster_raft_campaign_phase_trace(
                    mode,
                    local_node_id,
                    waited_for_campaign_lock,
                    &ClusterRaftCampaignPhaseRecord {
                        phase: "campaign_lock_acquire",
                        recorded_at: SystemTime::now(),
                        duration: started.elapsed(),
                        target_count: 0,
                        outcome: "error",
                        detail: error.to_string(),
                    },
                );
            }
            Err(error)
        }
    }
}

fn observed_cluster_raft_campaign_lock_before(
    paths: &ClusterPaths,
    local_node_id: &NodeId,
    mode: &'static str,
    deadline: Instant,
) -> Result<ClusterRaftCampaignTraceGuard, NetworkError> {
    remaining_cluster_raft_election_budget(deadline)?;
    let trace_enabled =
        tracing::enabled!(target: "sinomo.cluster.formation", tracing::Level::TRACE);
    let started = Instant::now();
    let (result, waited_for_campaign_lock) = try_lock_cluster_raft_campaign_before(paths, deadline);
    match result {
        Ok(lock) => Ok(ClusterRaftCampaignTraceGuard::new(
            lock,
            mode,
            local_node_id.clone(),
            waited_for_campaign_lock,
            Instant::now(),
            started.elapsed(),
            trace_enabled,
        )),
        Err(error) => {
            if trace_enabled {
                cluster_raft_campaign_phase_trace(
                    mode,
                    local_node_id,
                    waited_for_campaign_lock,
                    &ClusterRaftCampaignPhaseRecord {
                        phase: "campaign_lock_acquire",
                        recorded_at: SystemTime::now(),
                        duration: started.elapsed(),
                        target_count: 0,
                        outcome: "error",
                        detail: error.to_string(),
                    },
                );
            }
            Err(error)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClusterRaftLeaderObservation {
    leader_id: Option<NodeId>,
    current_term: u64,
}

fn locked_cluster_raft_leader_observation_before(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    deadline: Instant,
) -> Result<ClusterRaftLeaderObservation, NetworkError> {
    let (leader_id, metadata, _) = locked_cluster_raft_leader_observation_and_config_before(
        paths,
        &state.local_node_id,
        deadline,
    )?;
    Ok(ClusterRaftLeaderObservation {
        leader_id: leader_id.filter(|leader_id| state.node_is_raft_voter(leader_id)),
        current_term: metadata.current_term,
    })
}

fn locked_cluster_raft_leader_observation_and_config_before(
    paths: &ClusterPaths,
    local_node_id: &NodeId,
    deadline: Instant,
) -> Result<
    (
        Option<NodeId>,
        ClusterKvRaftLocalVoteMetadata,
        Option<sinomo_kv::KvEntry>,
    ),
    NetworkError,
> {
    paths
        .kv_paths()
        .raft_store()
        .locked_leader_vote_metadata_and_config_before(local_node_id, deadline)
        .map_err(kv_err)
}

fn project_captured_cluster_raft_config(
    state: &mut ClusterBootstrapState,
    config_entry: Option<&sinomo_kv::KvEntry>,
) -> Result<(), NetworkError> {
    if let Some(entry) = config_entry {
        let config: sinomo_kv::ClusterKvRaftConfigRecord = entry.decode().map_err(kv_err)?;
        project_cluster_raft_config_record(state, &config)?;
    }
    Ok(())
}

fn cluster_raft_leader_outcome_after_campaign_wait(
    local_node_id: &NodeId,
    waited_for_campaign_lock: bool,
    observation_before_wait: &ClusterRaftLeaderObservation,
    observation_after_wait: &ClusterRaftLeaderObservation,
    remote_leader_blocks_campaign: bool,
) -> Option<bool> {
    if observation_after_wait.leader_id.as_ref() == Some(local_node_id) {
        Some(true)
    } else if waited_for_campaign_lock
        || observation_after_wait != observation_before_wait
        || remote_leader_blocks_campaign
    {
        // A caller that actually queued must accept the completed campaign's
        // outcome, including a failed election. Immediately starting another
        // campaign here recreates a term storm when quorum is temporarily
        // unavailable. A durable remote leader also remains authoritative until
        // local liveness evidence explicitly marks it offline or unreachable;
        // one application forwarding failure is not an election timeout.
        Some(false)
    } else {
        None
    }
}

fn observed_remote_leader_blocks_campaign(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    observation: &ClusterRaftLeaderObservation,
) -> bool {
    observation.leader_id.as_ref().is_some_and(|leader_id| {
        leader_id != &state.local_node_id
            && !sinomo_network::cluster_node_blocks_leader_preference(paths, state, leader_id)
    })
}

pub fn elect_cluster_raft_leader_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    host: &mut impl ClusterRaftVoteTransportHost,
) -> Result<bool, NetworkError> {
    let operation_deadline = Instant::now() + options.timeout;
    let mut consensus_state = state.clone();
    remaining_cluster_raft_election_budget(operation_deadline)?;
    project_committed_cluster_raft_config_before(paths, &mut consensus_state, operation_deadline)?;
    remaining_cluster_raft_election_budget(operation_deadline)?;
    let observation_before_wait =
        locked_cluster_raft_leader_observation_before(paths, &consensus_state, operation_deadline)?;
    let scheduled_voter_ids =
        sinomo_kv::cluster_kv_raft_voter_ids(&consensus_state).map_err(kv_err)?;
    let jitter = cluster_raft_campaign_jitter_for_voters(
        &consensus_state.local_node_id,
        scheduled_voter_ids,
        observation_before_wait.current_term,
        options.campaign_jitter_max,
    );
    if !jitter.is_zero() {
        let remaining = remaining_cluster_raft_election_budget(operation_deadline)?;
        if jitter >= remaining {
            return Err(cluster_raft_election_timeout_error());
        }
        std::thread::sleep(jitter);
    }
    // A transaction that observed no usable leader may wait here behind
    // another transaction that is already campaigning or committing. Recheck
    // the durable leader while holding the same outer sequence lock used by
    // append -> replicate -> commit so a queued campaign cannot immediately
    // invalidate a leader that appeared while it waited. An unchanged remote
    // leader remains authoritative unless local status/connectivity evidence
    // explicitly marks it offline or unreachable. A forwarding failure alone
    // is not enough evidence to preempt an otherwise live leader.
    // This is deliberately distinct from the global commit-sequence lock. One
    // node may own only one network-bearing election round, while unrelated
    // proposals and commits remain free to use their short local critical
    // sections during the vote RPC.
    let election_round = lock_cluster_raft_election_round_before(
        paths,
        &consensus_state.local_node_id,
        operation_deadline,
    )?;
    let mut campaign = observed_cluster_raft_campaign_lock_before(
        paths,
        &consensus_state.local_node_id,
        "sync",
        operation_deadline,
    )?;
    let waited_for_campaign_lock = campaign.waited_for_campaign_lock;
    let leader_observation_started = Instant::now();
    let (raw_leader_id, metadata, config_fence) =
        match locked_cluster_raft_leader_observation_and_config_before(
            paths,
            &consensus_state.local_node_id,
            operation_deadline,
        ) {
            Ok(observation) => observation,
            Err(error) => {
                campaign
                    .record_leader_observation_error(leader_observation_started.elapsed(), &error);
                return Err(error);
            }
        };
    project_captured_cluster_raft_config(&mut consensus_state, config_fence.as_ref())?;
    let observation_after_wait = ClusterRaftLeaderObservation {
        leader_id: raw_leader_id.filter(|leader_id| consensus_state.node_is_raft_voter(leader_id)),
        current_term: metadata.current_term,
    };
    let state = &consensus_state;
    let remote_leader_blocks_campaign =
        observed_remote_leader_blocks_campaign(paths, state, &observation_after_wait);
    if let Some(existing_outcome) = cluster_raft_leader_outcome_after_campaign_wait(
        &state.local_node_id,
        election_round.waited_for_round_lock || waited_for_campaign_lock,
        &observation_before_wait,
        &observation_after_wait,
        remote_leader_blocks_campaign,
    ) {
        campaign.complete(
            if existing_outcome {
                "existing_local_leader"
            } else {
                "campaign_suppressed"
            },
            format_args!("used the durable campaign outcome observed after lock acquisition"),
        );
        return Ok(existing_outcome);
    }

    let voter_ids = sinomo_kv::cluster_kv_raft_voter_ids(state).map_err(kv_err)?;
    let remote_voter_ids = voter_ids
        .iter()
        .filter(|node_id| *node_id != &state.local_node_id)
        .cloned()
        .collect::<Vec<_>>();
    campaign.set_target_count(remote_voter_ids.len());
    let mut election_host = NetworkClusterKvRaftLeaderElectionHost::new(
        paths,
        state,
        options,
        operation_deadline,
        config_fence,
        campaign,
        host,
    );
    let result = sinomo_kv::elect_cluster_kv_raft_leader_with_host(
        &state.local_node_id,
        voter_ids,
        remote_voter_ids,
        options.kv_options(),
        &mut election_host,
    );
    drop(election_round);
    result
}

pub(crate) async fn elect_cluster_raft_leader_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    host: &mut (impl ClusterRaftVoteTransportAsyncHost + Send),
) -> Result<bool, NetworkError> {
    let operation_deadline = Instant::now() + options.timeout;
    let projection_paths = paths.clone();
    let mut consensus_state = state.clone();
    let (projected_state, observation_before_wait) = run_cluster_raft_election_blocking_before(
        runtime,
        operation_deadline,
        "project committed cluster Raft config",
        move || {
            remaining_cluster_raft_election_budget(operation_deadline)?;
            project_committed_cluster_raft_config_before(
                &projection_paths,
                &mut consensus_state,
                operation_deadline,
            )?;
            let observation_before_wait = locked_cluster_raft_leader_observation_before(
                &projection_paths,
                &consensus_state,
                operation_deadline,
            )?;
            Ok::<_, NetworkError>((consensus_state, observation_before_wait))
        },
    )
    .await?;
    consensus_state = projected_state;
    remaining_cluster_raft_election_budget(operation_deadline)?;
    let scheduled_voter_ids =
        sinomo_kv::cluster_kv_raft_voter_ids(&consensus_state).map_err(kv_err)?;
    let jitter = cluster_raft_campaign_jitter_for_voters(
        &consensus_state.local_node_id,
        scheduled_voter_ids,
        observation_before_wait.current_term,
        options.campaign_jitter_max,
    );
    if !jitter.is_zero() {
        let remaining = remaining_cluster_raft_election_budget(operation_deadline)?;
        if jitter >= remaining {
            return Err(cluster_raft_election_timeout_error());
        }
        tokio::time::sleep(jitter).await;
    }
    let paths_for_lock = paths.clone();
    let mut state_for_lock = consensus_state.clone();
    let (election_round, mut campaign, existing_outcome, projected_state, config_fence) =
        run_cluster_raft_election_blocking_before(
            runtime,
            operation_deadline,
            "cluster Raft initial campaign admission",
            move || {
                let election_round = lock_cluster_raft_election_round_before(
                    &paths_for_lock,
                    &state_for_lock.local_node_id,
                    operation_deadline,
                )?;
                let mut campaign = observed_cluster_raft_campaign_lock_before(
                    &paths_for_lock,
                    &state_for_lock.local_node_id,
                    "async",
                    operation_deadline,
                )?;
                let waited_for_campaign_lock = campaign.waited_for_campaign_lock;
                let leader_observation_started = Instant::now();
                let (raw_leader_id, metadata, config_fence) =
                    match locked_cluster_raft_leader_observation_and_config_before(
                        &paths_for_lock,
                        &state_for_lock.local_node_id,
                        operation_deadline,
                    ) {
                        Ok(observation) => observation,
                        Err(error) => {
                            campaign.record_leader_observation_error(
                                leader_observation_started.elapsed(),
                                &error,
                            );
                            return Err(error);
                        }
                    };
                project_captured_cluster_raft_config(&mut state_for_lock, config_fence.as_ref())?;
                let observation_after_wait = ClusterRaftLeaderObservation {
                    leader_id: raw_leader_id
                        .filter(|leader_id| state_for_lock.node_is_raft_voter(leader_id)),
                    current_term: metadata.current_term,
                };
                let remote_leader_blocks_campaign = observed_remote_leader_blocks_campaign(
                    &paths_for_lock,
                    &state_for_lock,
                    &observation_after_wait,
                );
                let existing_outcome = cluster_raft_leader_outcome_after_campaign_wait(
                    &state_for_lock.local_node_id,
                    election_round.waited_for_round_lock || waited_for_campaign_lock,
                    &observation_before_wait,
                    &observation_after_wait,
                    remote_leader_blocks_campaign,
                );
                Ok::<_, NetworkError>((
                    election_round,
                    campaign,
                    existing_outcome,
                    state_for_lock,
                    config_fence,
                ))
            },
        )
        .await?;
    if let Some(existing_outcome) = existing_outcome {
        campaign.complete(
            if existing_outcome {
                "existing_local_leader"
            } else {
                "campaign_suppressed"
            },
            format_args!("used the durable campaign outcome observed after lock acquisition"),
        );
        return Ok(existing_outcome);
    }
    consensus_state = projected_state;
    let state = &consensus_state;
    let voter_ids = sinomo_kv::cluster_kv_raft_voter_ids(state).map_err(kv_err)?;
    let remote_voter_ids = voter_ids
        .iter()
        .filter(|node_id| *node_id != &state.local_node_id)
        .cloned()
        .collect::<Vec<_>>();
    campaign.set_target_count(remote_voter_ids.len());
    let mut election_host = NetworkClusterKvRaftLeaderElectionAsyncHost::new(
        runtime,
        paths,
        state,
        options,
        operation_deadline,
        config_fence,
        campaign,
        host,
    );
    let result = sinomo_kv::elect_cluster_kv_raft_leader_async_with_host(
        &state.local_node_id,
        voter_ids,
        remote_voter_ids,
        options.kv_options(),
        &mut election_host,
    )
    .await;
    // Keep the distinct election-round lock live through the network vote and
    // final publication without retaining the global commit-sequence lock.
    drop(election_round);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn two_voter_election_state() -> (ClusterBootstrapState, NodeId, NodeId) {
        let now = time::OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_async_election_singleflight")
            .expect("cluster id");
        let local =
            NodeId::from_legacy_named("node_async_election_singleflight_local").expect("local");
        let peer =
            NodeId::from_legacy_named("node_async_election_singleflight_peer").expect("peer");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "async election singleflight",
            local.clone(),
            "local",
            "local-public-key",
            now,
        );
        let local_member = state.members.first_mut().expect("local member");
        local_member.role = ClusterMemberRole::Voter;
        local_member.state = ClusterMemberState::Voter;
        local_member.capabilities =
            vec![CapabilityName::new("daemon.main.run").expect("main daemon capability")];
        local_member.allowed_capabilities = local_member.capabilities.clone();
        let mut peer_member = local_member.clone();
        peer_member.node_id = peer.clone();
        peer_member.display_name = "peer".to_string();
        peer_member.node_public_key = "peer-public-key".to_string();
        peer_member.endpoints.clear();
        peer_member.joined_at = now;
        peer_member.updated_at = now;
        state.members.push(peer_member);
        (state, local, peer)
    }

    fn observation(leader_id: Option<NodeId>, current_term: u64) -> ClusterRaftLeaderObservation {
        ClusterRaftLeaderObservation {
            leader_id,
            current_term,
        }
    }

    #[test]
    fn election_targets_are_exact_deduplicated_voters_without_local_node() {
        let local = NodeId::from_legacy_named("election_exact_local").expect("local");
        let voter_a = NodeId::from_legacy_named("election_exact_a").expect("voter a");
        let voter_b = NodeId::from_legacy_named("election_exact_b").expect("voter b");

        let selected = exact_remote_voter_ids(
            &local,
            vec![
                voter_b.clone(),
                local.clone(),
                voter_a.clone(),
                voter_b.clone(),
                local.clone(),
            ],
        );

        let mut expected = vec![voter_a, voter_b];
        expected.sort();
        assert_eq!(selected, expected);
    }

    #[test]
    fn durable_term_ranked_relative_campaign_slots_are_unique_bounded_and_reverse() {
        let mut voters = vec![
            NodeId::from_legacy_named("election_slot_a").expect("voter a"),
            NodeId::from_legacy_named("election_slot_b").expect("voter b"),
            NodeId::from_legacy_named("election_slot_c").expect("voter c"),
        ];
        voters.sort();
        let max = Duration::from_millis(900);
        let initial_term = 1;
        let next_generation_term =
            initial_term + sinomo_kv::CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP;
        let schedule = |term| {
            let mut scheduled = voters
                .iter()
                .map(|voter| {
                    (
                        voter.clone(),
                        cluster_raft_campaign_jitter_for_voters(voter, voters.clone(), term, max),
                    )
                })
                .collect::<Vec<_>>();
            scheduled.sort_by_key(|(_, delay)| *delay);
            scheduled
        };
        let initial = schedule(initial_term);
        let next = schedule(next_generation_term);

        let mut initial_delays = initial.iter().map(|(_, delay)| *delay).collect::<Vec<_>>();
        assert!(initial_delays.iter().all(|delay| *delay < max));
        initial_delays.sort();
        initial_delays.dedup();
        assert_eq!(initial_delays.len(), voters.len());
        assert_eq!(
            initial
                .iter()
                .map(|(voter, _)| voter.clone())
                .collect::<Vec<_>>(),
            voters
        );
        assert_eq!(
            next.iter()
                .map(|(voter, _)| voter.clone())
                .collect::<Vec<_>>(),
            voters.iter().rev().cloned().collect::<Vec<_>>(),
            "the next durable election generation must reverse candidate priority"
        );
    }

    #[test]
    fn skewed_candidate_clocks_cannot_phase_lock_across_durable_terms() {
        let mut voters = vec![
            NodeId::from_legacy_named("skewed_election_slot_a").expect("voter a"),
            NodeId::from_legacy_named("skewed_election_slot_b").expect("voter b"),
            NodeId::from_legacy_named("skewed_election_slot_c").expect("voter c"),
            NodeId::from_legacy_named("skewed_election_slot_d").expect("voter d"),
        ];
        voters.sort();
        let candidate_a = &voters[0];
        let candidate_b = &voters[1];
        let max = Duration::from_millis(800);
        let initial_term = 1;
        let delay_a =
            cluster_raft_campaign_jitter_for_voters(candidate_a, voters.clone(), initial_term, max);
        let delay_b =
            cluster_raft_campaign_jitter_for_voters(candidate_b, voters.clone(), initial_term, max);
        let clock_skew = delay_b.saturating_sub(delay_a);
        assert!(!clock_skew.is_zero(), "adjacent voters need distinct slots");

        // Model the old failure: a stable clock-derived invocation phase can
        // exactly cancel the first generation's slot spacing.
        let candidate_a_start = clock_skew;
        let candidate_b_start = Duration::ZERO;
        assert_eq!(
            candidate_a_start.saturating_add(delay_a),
            candidate_b_start.saturating_add(delay_b),
            "the fixture must reproduce one phase-aligned campaign"
        );

        let next_generation_term =
            initial_term + sinomo_kv::CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP;
        let next_delay_a = cluster_raft_campaign_jitter_for_voters(
            candidate_a,
            voters.clone(),
            next_generation_term,
            max,
        );
        let next_delay_b = cluster_raft_campaign_jitter_for_voters(
            candidate_b,
            voters.clone(),
            next_generation_term,
            max,
        );
        assert_ne!(
            candidate_a_start.saturating_add(next_delay_a),
            candidate_b_start.saturating_add(next_delay_b),
            "durable-term perturbation must break a fixed inter-node clock phase on the next round"
        );
    }

    #[test]
    fn externally_cancelled_queued_election_blocking_work_never_runs() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let (blocker_started_tx, blocker_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_blocker_tx, release_blocker_rx) = std::sync::mpsc::sync_channel(0);
        let blocker = runtime.spawn_blocking(move || {
            blocker_started_tx.send(()).expect("blocker started");
            release_blocker_rx.recv().expect("release blocker");
        });
        blocker_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking pool saturated");
        let ran = Arc::new(AtomicUsize::new(0));
        let ran_in_work = Arc::clone(&ran);
        let handle = runtime.handle().clone();
        let queued = runtime.spawn(async move {
            run_cluster_raft_election_blocking_before(
                &handle,
                Instant::now() + Duration::from_secs(1),
                "test queued cancellation",
                move || {
                    ran_in_work.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
        });
        runtime.block_on(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            queued.abort();
            let _ = queued.await;
        });
        release_blocker_tx.send(()).expect("release blocker");
        runtime.block_on(blocker).expect("join blocker");
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn externally_cancelled_admitted_election_blocking_work_finishes() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let (completed_tx, completed_rx) = std::sync::mpsc::sync_channel(0);
        let handle = runtime.handle().clone();
        let admitted = runtime.spawn(async move {
            run_cluster_raft_election_blocking_before(
                &handle,
                Instant::now() + Duration::from_millis(50),
                "test admitted cancellation",
                move || {
                    started_tx.send(()).expect("work admitted");
                    release_rx.recv().expect("release admitted work");
                    completed_tx.send(()).expect("work completed");
                    Ok(())
                },
            )
            .await
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking work admitted");
        admitted.abort();
        runtime.block_on(async {
            let _ = admitted.await;
        });
        release_tx.send(()).expect("release admitted work");
        completed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("admitted work completed after caller cancellation");
    }

    #[test]
    fn async_election_rounds_coalesce_while_commit_sequence_remains_available() {
        struct BlockingAsyncVoteHost {
            vote_calls: Arc<AtomicUsize>,
            started: Arc<tokio::sync::Notify>,
            release: Option<Arc<tokio::sync::Notify>>,
        }

        impl ClusterRaftVoteTransportAsyncHost for BlockingAsyncVoteHost {
            fn send_raft_votes_async<'a>(
                &'a mut self,
                _state: &'a ClusterBootstrapState,
                targets: Vec<NodeId>,
                request: &'a ClusterRaftVoteRequest,
                _timeout: Duration,
                required_grants: usize,
            ) -> ClusterMembershipRpcFuture<
                'a,
                Vec<(NodeId, Result<ClusterRaftVoteResponse, NetworkError>)>,
            > {
                let vote_calls = Arc::clone(&self.vote_calls);
                let started = Arc::clone(&self.started);
                let release = self.release.clone();
                let request = request.clone();
                Box::pin(async move {
                    vote_calls.fetch_add(1, Ordering::SeqCst);
                    started.notify_one();
                    if let Some(release) = release {
                        release.notified().await;
                    }
                    Ok(targets
                        .into_iter()
                        .take(required_grants)
                        .map(|target| {
                            (
                                target,
                                Ok(ClusterRaftVoteResponse {
                                    accepted: true,
                                    detail: "granted".to_string(),
                                    state: request.state.clone(),
                                    vote: RaftRequestVoteResponse {
                                        term: request.vote.term,
                                        vote_granted: true,
                                    },
                                }),
                            )
                        })
                        .collect())
                })
            }
        }

        let root = sinomo_testdir::TestDir::new("sinomo-async-election-singleflight");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let (state, _local, _peer) = two_voter_election_state();
        crate::local_state_store::write_cluster_state(&paths, &state).expect("cluster state");
        let options = ClusterRaftLeaderElectionOptions {
            kv: sinomo_kv::ClusterKvRaftLeaderElectionOptions {
                max_attempts: sinomo_kv::CLUSTER_KV_RAFT_LEADER_ELECTION_ATTEMPTS,
            },
            timeout: Duration::from_secs(2),
            campaign_jitter_max: Duration::ZERO,
            tcp_command_timeout: Duration::from_secs(1),
            iroh_command_timeout: Duration::from_secs(1),
            tcp_foreground_attempt_timeout: Duration::from_secs(1),
            public_foreground_attempt_timeout: Duration::from_secs(1),
            iroh_foreground_attempt_timeout: Duration::from_secs(1),
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let vote_calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        runtime.block_on(async {
            let first = {
                let runtime = runtime.handle().clone();
                let paths = paths.clone();
                let state = state.clone();
                let mut host = BlockingAsyncVoteHost {
                    vote_calls: Arc::clone(&vote_calls),
                    started: Arc::clone(&started),
                    release: Some(Arc::clone(&release)),
                };
                tokio::spawn(async move {
                    elect_cluster_raft_leader_async_with_host(
                        &runtime, &paths, &state, options, &mut host,
                    )
                    .await
                })
            };
            tokio::time::timeout(Duration::from_secs(1), started.notified())
                .await
                .expect("first async vote started");
            let second = {
                let runtime = runtime.handle().clone();
                let paths = paths.clone();
                let state = state.clone();
                let mut host = BlockingAsyncVoteHost {
                    vote_calls: Arc::clone(&vote_calls),
                    started: Arc::clone(&started),
                    release: None,
                };
                tokio::spawn(async move {
                    elect_cluster_raft_leader_async_with_host(
                        &runtime, &paths, &state, options, &mut host,
                    )
                    .await
                })
            };
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(vote_calls.load(Ordering::SeqCst), 1);
            let commit_paths = paths.kv_paths();
            let commit_lock = tokio::task::spawn_blocking(move || {
                sinomo_kv::lock_cluster_kv_commit_sequence(
                    &commit_paths,
                    Duration::from_millis(100),
                )
            })
            .await
            .expect("commit lock task")
            .expect("vote RPC must not retain the global commit lock");
            drop(commit_lock);
            release.notify_one();
            assert!(
                first
                    .await
                    .expect("first election task")
                    .expect("first election")
            );
            assert!(
                second
                    .await
                    .expect("second election task")
                    .expect("second election")
            );
            assert_eq!(vote_calls.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn async_retry_suppresses_campaign_after_external_vote_advances_durable_term() {
        struct TermAdvancingVoteHost {
            paths: ClusterPaths,
            local_node_id: NodeId,
            competing_node_id: NodeId,
            vote_calls: usize,
        }

        impl ClusterRaftVoteTransportAsyncHost for TermAdvancingVoteHost {
            fn send_raft_votes_async<'a>(
                &'a mut self,
                state: &'a ClusterBootstrapState,
                targets: Vec<NodeId>,
                request: &'a ClusterRaftVoteRequest,
                _timeout: Duration,
                _required_grants: usize,
            ) -> ClusterMembershipRpcFuture<
                'a,
                Vec<(NodeId, Result<ClusterRaftVoteResponse, NetworkError>)>,
            > {
                self.vote_calls += 1;
                assert_eq!(self.vote_calls, 1, "advanced term must suppress a retry");
                let competing_term = request
                    .vote
                    .term
                    .saturating_add(sinomo_kv::CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP);
                let competing_vote = self
                    .paths
                    .kv_paths()
                    .raft_store()
                    .locked_apply_vote_before(
                        &self.local_node_id,
                        RaftRequestVoteRequest {
                            term: competing_term,
                            candidate_id: self.competing_node_id.clone(),
                            last_log_index: request.vote.last_log_index,
                            last_log_term: request.vote.last_log_term,
                        },
                        Instant::now() + Duration::from_secs(1),
                    )
                    .expect("persist competing vote");
                assert!(competing_vote.vote_granted);
                let target = targets.into_iter().next().expect("remote voter");
                let response = ClusterRaftVoteResponse {
                    accepted: false,
                    detail: "same-term split vote".to_string(),
                    state: state.clone(),
                    vote: RaftRequestVoteResponse {
                        term: request.vote.term,
                        vote_granted: false,
                    },
                };
                Box::pin(async move { Ok(vec![(target, Ok(response))]) })
            }
        }

        let root = sinomo_testdir::TestDir::new("sinomo-async-election-retry-term-advance");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let (state, local_node_id, competing_node_id) = two_voter_election_state();
        crate::local_state_store::write_cluster_state(&paths, &state).expect("cluster state");
        let mut host = TermAdvancingVoteHost {
            paths: paths.clone(),
            local_node_id: local_node_id.clone(),
            competing_node_id: competing_node_id.clone(),
            vote_calls: 0,
        };
        let options = ClusterRaftLeaderElectionOptions {
            kv: sinomo_kv::ClusterKvRaftLeaderElectionOptions { max_attempts: 3 },
            timeout: Duration::from_secs(2),
            campaign_jitter_max: Duration::from_millis(25),
            tcp_command_timeout: Duration::from_secs(1),
            iroh_command_timeout: Duration::from_secs(1),
            tcp_foreground_attempt_timeout: Duration::from_secs(1),
            public_foreground_attempt_timeout: Duration::from_secs(1),
            iroh_foreground_attempt_timeout: Duration::from_secs(1),
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");

        let elected = runtime
            .block_on(elect_cluster_raft_leader_async_with_host(
                runtime.handle(),
                &paths,
                &state,
                options,
                &mut host,
            ))
            .expect("bounded election");

        assert!(!elected);
        assert_eq!(host.vote_calls, 1);
        let hard_state = crate::kv_local_store::open_cluster_raft_kv_node(&paths, &local_node_id)
            .expect("local raft")
            .hard_state()
            .clone();
        assert_eq!(
            hard_state.current_term,
            1 + 2 * sinomo_kv::CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP
        );
        assert_eq!(hard_state.voted_for, Some(competing_node_id));
        assert_eq!(hard_state.leader_id, None);
    }

    #[test]
    fn queued_campaign_accepts_failed_predecessor_outcome() {
        let local_node_id = NodeId::from_legacy_named("queued_campaign_local").expect("node");
        let remote_node_id = NodeId::from_legacy_named("queued_campaign_remote").expect("node");
        let before = observation(Some(remote_node_id.clone()), 7);

        assert_eq!(
            cluster_raft_leader_outcome_after_campaign_wait(
                &local_node_id,
                true,
                &before,
                &before,
                false,
            ),
            Some(false)
        );
        assert_eq!(
            cluster_raft_leader_outcome_after_campaign_wait(
                &local_node_id,
                false,
                &before,
                &before,
                false,
            ),
            None,
            "an explicitly blocked stale leader must remain replaceable"
        );
        assert_eq!(
            cluster_raft_leader_outcome_after_campaign_wait(
                &local_node_id,
                false,
                &before,
                &before,
                true,
            ),
            Some(false),
            "a remote leader without explicit offline evidence must suppress the campaign"
        );
        assert_eq!(
            cluster_raft_leader_outcome_after_campaign_wait(
                &local_node_id,
                false,
                &before,
                &observation(Some(remote_node_id), 8),
                false,
            ),
            Some(false),
            "a new term observed while acquiring the lock suppresses a stale campaign"
        );
        assert_eq!(
            cluster_raft_leader_outcome_after_campaign_wait(
                &local_node_id,
                true,
                &before,
                &observation(Some(local_node_id.clone()), 8),
                false,
            ),
            Some(true)
        );
    }

    #[test]
    fn campaign_lock_reports_observed_contention() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-campaign-lock-contention");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let local_node_id = NodeId::from_legacy_named("campaign_lock_local").expect("node");
        let first_campaign = observed_cluster_raft_campaign_lock(&paths, &local_node_id, "test")
            .expect("first lock");
        assert!(!first_campaign.waited_for_campaign_lock);

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let queued_paths = paths.clone();
        let queued = std::thread::spawn(move || {
            started_tx.send(()).expect("started");
            let (_lock, waited) =
                lock_cluster_raft_campaign(&queued_paths).expect("queued campaign lock");
            result_tx.send(waited).expect("waited result");
        });
        started_rx.recv().expect("queued thread started");
        std::thread::sleep(Duration::from_millis(50));
        assert!(result_rx.try_recv().is_err(), "queued lock must still wait");
        drop(first_campaign);

        assert!(
            result_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("queued lock result")
        );
        queued.join().expect("queued thread");
    }

    #[test]
    fn campaign_trace_buffers_and_classifies_post_lock_observation_error() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-campaign-observation-error");
        let paths = ClusterPaths::new(&SinomoDataDir::new(&root));
        paths.ensure().expect("cluster paths");
        let local_node_id =
            NodeId::from_legacy_named("campaign_observation_error_local").expect("node");
        let (lock, waited_for_campaign_lock) =
            lock_cluster_raft_campaign(&paths).expect("campaign lock");
        let mut campaign = ClusterRaftCampaignTraceGuard::new(
            lock,
            "test",
            local_node_id,
            waited_for_campaign_lock,
            Instant::now(),
            Duration::from_micros(11),
            true,
        );
        let error = NetworkError::Io {
            operation: "observe durable leader".to_string(),
            detail: "injected observation failure".to_string(),
        };

        campaign.record_leader_observation_error(Duration::from_micros(17), &error);

        assert_eq!(campaign.outcome, "leader_observation_error");
        assert_eq!(
            campaign
                .phase_records
                .iter()
                .map(|record| record.phase)
                .collect::<Vec<_>>(),
            vec![
                "campaign_lock_acquire",
                "durable_leader_observation",
                "campaign_complete"
            ]
        );
        assert_eq!(
            campaign.phase_records[0].duration,
            Duration::from_micros(11)
        );
        assert_eq!(
            campaign.phase_records[1].duration,
            Duration::from_micros(17)
        );
        assert_eq!(campaign.phase_records[1].outcome, "error");
        assert!(
            campaign
                .phase_records
                .iter()
                .all(|record| record.recorded_at.duration_since(UNIX_EPOCH).is_ok()),
            "buffered phases must retain their actual wall-clock timestamp"
        );

        drop(campaign);
        let (_next_lock, next_waited) =
            lock_cluster_raft_campaign(&paths).expect("lock released when campaign guard dropped");
        assert!(!next_waited);
    }
}
