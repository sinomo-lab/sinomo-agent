use crate::control_plane_options::ClusterRaftLeaderElectionOptions;
use crate::kv_local_store::ClusterKvPathsExt;
use crate::kv_local_store::kv_err;
use crate::kv_local_store::locked_apply_cluster_raft_vote;
use crate::local_state_store::project_committed_cluster_raft_config;
use crate::prelude::*;
#[allow(unused_imports)]
use sinomo_kv::KvError;
use sinomo_kv::{ClusterKvRaftLocalVoteMetadata, ClusterRaftVoteRequest, ClusterRaftVoteResponse};
use sinomo_kv::{KvSnapshot, RaftRequestVoteRequest, RaftRequestVoteResponse};
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

pub trait ClusterRaftVoteTransportHost {
    fn send_raft_vote(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftVoteResponse, NetworkError>;
}

pub(crate) trait ClusterRaftVoteTransportAsyncHost {
    fn send_raft_vote_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftVoteResponse>;
}

fn cluster_raft_campaign_jitter_for_node(node_id: &NodeId, max: Duration) -> Duration {
    if max.is_zero() {
        return Duration::ZERO;
    }
    let max_nanos = max.as_nanos().min(u128::from(u64::MAX)) as u64;
    if max_nanos == 0 {
        return Duration::ZERO;
    }
    let time_bucket = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        / u128::from(max_nanos);
    let mut hasher = DefaultHasher::new();
    node_id.to_string().hash(&mut hasher);
    time_bucket.hash(&mut hasher);
    Duration::from_nanos(hasher.finish() % max_nanos)
}

struct NetworkClusterKvRaftLeaderElectionHost<'a, H> {
    paths: &'a ClusterPaths,
    state: &'a ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    remote_vote_targets: Vec<NodeId>,
    attempt_deadline: Instant,
    transport: &'a mut H,
}

impl<'a, H> NetworkClusterKvRaftLeaderElectionHost<'a, H> {
    fn new(
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        options: ClusterRaftLeaderElectionOptions,
        remote_vote_targets: Vec<NodeId>,
        transport: &'a mut H,
    ) -> Self {
        Self {
            paths,
            state,
            options,
            remote_vote_targets,
            attempt_deadline: Instant::now(),
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

    fn begin_election_attempt(&mut self) -> Result<(), Self::Error> {
        let jitter = cluster_raft_campaign_jitter_for_node(
            &self.state.local_node_id,
            self.options.campaign_jitter_max,
        );
        if !jitter.is_zero() {
            std::thread::sleep(jitter);
        }
        self.attempt_deadline = Instant::now() + self.options.timeout;
        Ok(())
    }

    fn local_vote_metadata(&mut self) -> Result<ClusterKvRaftLocalVoteMetadata, Self::Error> {
        locked_cluster_raft_local_vote_metadata(self.paths, &self.state.local_node_id)
    }

    fn apply_local_vote(
        &mut self,
        vote: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, Self::Error> {
        locked_apply_cluster_raft_vote(self.paths, &self.state.local_node_id, vote)
    }

    fn should_request_more_remote_votes(&mut self) -> bool {
        self.attempt_deadline
            .checked_duration_since(Instant::now())
            .is_some_and(|remaining| !remaining.is_zero())
    }

    fn request_remote_vote(
        &mut self,
        target_index: usize,
        voter_id: &NodeId,
        vote: &RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, Self::Error> {
        let target_node_id =
            self.remote_vote_targets
                .get(target_index)
                .ok_or_else(|| NetworkError::Io {
                    operation: "cluster raft vote".to_string(),
                    detail: format!("missing vote target for target index {target_index}"),
                })?;
        if target_node_id != voter_id {
            return Err(NetworkError::Io {
                operation: "cluster raft vote".to_string(),
                detail: format!(
                    "vote target mismatch: selected target is {}, election target is {}",
                    target_node_id, voter_id
                ),
            });
        }
        let remaining = self
            .attempt_deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            return Err(NetworkError::Io {
                operation: "cluster raft vote".to_string(),
                detail: "election attempt timed out".to_string(),
            });
        }
        let request = ClusterRaftVoteRequest {
            state: self.state.clone(),
            vote: vote.clone(),
        };
        self.transport
            .send_raft_vote(
                self.state,
                target_node_id,
                &request,
                self.options.routed_node_command_timeout(remaining),
            )
            .map(|response| response.vote)
    }

    fn observe_raft_term(&mut self, term: u64) -> Result<(), Self::Error> {
        locked_observe_cluster_raft_term(self.paths, &self.state.local_node_id, term).map(|_| ())
    }

    fn set_raft_leader(&mut self, term: u64) -> Result<(), Self::Error> {
        locked_set_cluster_raft_leader(self.paths, &self.state.local_node_id, term).map(|_| ())
    }
}

struct NetworkClusterKvRaftLeaderElectionAsyncHost<'a, H> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
    state: &'a ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    remote_vote_targets: Vec<NodeId>,
    attempt_deadline: Instant,
    transport: &'a mut H,
}

impl<'a, H> NetworkClusterKvRaftLeaderElectionAsyncHost<'a, H> {
    fn new(
        runtime: &'a tokio::runtime::Handle,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        options: ClusterRaftLeaderElectionOptions,
        remote_vote_targets: Vec<NodeId>,
        transport: &'a mut H,
    ) -> Self {
        Self {
            runtime,
            paths,
            state,
            options,
            remote_vote_targets,
            attempt_deadline: Instant::now(),
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
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async move {
            let jitter = cluster_raft_campaign_jitter_for_node(
                &self.state.local_node_id,
                self.options.campaign_jitter_max,
            );
            if !jitter.is_zero() {
                tokio::time::sleep(jitter).await;
            }
            self.attempt_deadline = Instant::now() + self.options.timeout;
            Ok(())
        })
    }

    fn local_vote_metadata_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterKvRaftLocalVoteMetadata, Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || locked_cluster_raft_local_vote_metadata(&paths, &node_id))
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft leader election".to_string(),
                    detail: format!("local vote metadata task failed: {source}"),
                })?
        })
    }

    fn apply_local_vote_async<'a>(
        &'a mut self,
        vote: RaftRequestVoteRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, RaftRequestVoteResponse, Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || locked_apply_cluster_raft_vote(&paths, &node_id, vote))
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft leader election".to_string(),
                    detail: format!("local vote apply task failed: {source}"),
                })?
        })
    }

    fn should_request_more_remote_votes(&mut self) -> bool {
        self.attempt_deadline
            .checked_duration_since(Instant::now())
            .is_some_and(|remaining| !remaining.is_zero())
    }

    fn request_remote_vote_async<'a>(
        &'a mut self,
        target_index: usize,
        voter_id: &'a NodeId,
        vote: &'a RaftRequestVoteRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, RaftRequestVoteResponse, Self::Error> {
        let target_node_id = match self.remote_vote_targets.get(target_index).cloned() {
            Some(target_node_id) => target_node_id,
            None => {
                return Box::pin(async move {
                    Err(NetworkError::Io {
                        operation: "cluster raft vote".to_string(),
                        detail: format!("missing vote target for target index {target_index}"),
                    })
                });
            }
        };
        if &target_node_id != voter_id {
            let voter_id = voter_id.clone();
            return Box::pin(async move {
                Err(NetworkError::Io {
                    operation: "cluster raft vote".to_string(),
                    detail: format!(
                        "vote target mismatch: selected target is {target_node_id}, election target is {voter_id}"
                    ),
                })
            });
        }
        let remaining = self
            .attempt_deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            return Box::pin(async {
                Err(NetworkError::Io {
                    operation: "cluster raft vote".to_string(),
                    detail: "election attempt timed out".to_string(),
                })
            });
        }
        let request = ClusterRaftVoteRequest {
            state: self.state.clone(),
            vote: vote.clone(),
        };
        let timeout = self.options.routed_node_command_timeout(remaining);
        let transport = &mut *self.transport;
        let state = self.state;
        Box::pin(async move {
            transport
                .send_raft_vote_async(state, &target_node_id, &request, timeout)
                .await
                .map(|response| response.vote)
        })
    }

    fn observe_raft_term_async<'a>(
        &'a mut self,
        term: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || locked_observe_cluster_raft_term(&paths, &node_id, term))
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft leader election".to_string(),
                    detail: format!("term observation task failed: {source}"),
                })?
                .map(|_| ())
        })
    }

    fn set_raft_leader_async<'a>(
        &'a mut self,
        term: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || locked_set_cluster_raft_leader(&paths, &node_id, term))
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft leader election".to_string(),
                    detail: format!("leader write task failed: {source}"),
                })?
                .map(|_| ())
        })
    }
}

pub(crate) fn locked_observe_cluster_raft_term(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<bool, NetworkError> {
    paths
        .kv_paths()
        .locked_observe_raft_term_and_write_snapshot_if_changed(node_id, term)
        .map_err(kv_err)
}

pub fn locked_set_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<KvSnapshot, NetworkError> {
    paths
        .kv_paths()
        .locked_set_raft_leader_and_write_snapshot_if_changed(node_id, term)
        .map_err(kv_err)
}

pub(crate) fn locked_cluster_raft_local_vote_metadata(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<ClusterKvRaftLocalVoteMetadata, NetworkError> {
    paths
        .kv_paths()
        .locked_local_raft_vote_metadata(node_id)
        .map_err(kv_err)
}

fn lock_cluster_raft_campaign(paths: &ClusterPaths) -> Result<(std::fs::File, bool), NetworkError> {
    let kv_paths = paths.kv_paths();
    match sinomo_kv::lock_cluster_kv_commit_sequence(&kv_paths, Duration::ZERO) {
        Ok(lock) => Ok((lock, false)),
        Err(KvError::Busy { .. }) => sinomo_kv::lock_cluster_kv_commit_sequence(
            &kv_paths,
            sinomo_kv::CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT,
        )
        .map(|lock| (lock, true))
        .map_err(kv_err),
        Err(error) => Err(kv_err(error)),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClusterRaftLeaderObservation {
    leader_id: Option<NodeId>,
    current_term: u64,
}

fn locked_cluster_raft_leader_observation(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<ClusterRaftLeaderObservation, NetworkError> {
    let store = paths.kv_paths().raft_store();
    let _raft_guard = store.lock_operation().map_err(kv_err)?;
    let leader_id = store.known_voter_leader_id(state).map_err(kv_err)?;
    let current_term = store
        .local_vote_metadata(&state.local_node_id)
        .map_err(kv_err)?
        .current_term;
    Ok(ClusterRaftLeaderObservation {
        leader_id,
        current_term,
    })
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
    let mut consensus_state = state.clone();
    project_committed_cluster_raft_config(paths, &mut consensus_state)?;
    let state = &consensus_state;
    // A transaction that observed no usable leader may wait here behind
    // another transaction that is already campaigning or committing. Recheck
    // the durable leader while holding the same outer sequence lock used by
    // append -> replicate -> commit so a queued campaign cannot immediately
    // invalidate a leader that appeared while it waited. An unchanged remote
    // leader remains authoritative unless local status/connectivity evidence
    // explicitly marks it offline or unreachable. A forwarding failure alone
    // is not enough evidence to preempt an otherwise live leader.
    let observation_before_wait = locked_cluster_raft_leader_observation(paths, state)?;
    let (_campaign_lock, waited_for_campaign_lock) = lock_cluster_raft_campaign(paths)?;
    let observation_after_wait = locked_cluster_raft_leader_observation(paths, state)?;
    let remote_leader_blocks_campaign =
        observed_remote_leader_blocks_campaign(paths, state, &observation_after_wait);
    if let Some(existing_outcome) = cluster_raft_leader_outcome_after_campaign_wait(
        &state.local_node_id,
        waited_for_campaign_lock,
        &observation_before_wait,
        &observation_after_wait,
        remote_leader_blocks_campaign,
    ) {
        return Ok(existing_outcome);
    }
    let voter_ids = sinomo_kv::cluster_kv_raft_voter_ids(state).map_err(kv_err)?;
    let mut remote_vote_targets = Vec::new();
    if !(voter_ids.len() == 1 && voter_ids.first() == Some(&state.local_node_id)) {
        for node_id in ClusterEndpointSelector::new(paths, state)
            .select_internal_unique_node_ids(
                ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
            )
            .into_iter()
            .filter(|node_id| node_id != &state.local_node_id && voter_ids.contains(node_id))
        {
            sinomo_network::try_reserve_vec(
                &mut remote_vote_targets,
                1,
                "collect raft vote targets",
            )?;
            remote_vote_targets.push(node_id);
        }
    }
    let remote_vote_target_ids = remote_vote_targets.clone();
    let mut election_host = NetworkClusterKvRaftLeaderElectionHost::new(
        paths,
        state,
        options,
        remote_vote_targets,
        host,
    );
    sinomo_kv::elect_cluster_kv_raft_leader_with_host(
        &state.local_node_id,
        voter_ids,
        remote_vote_target_ids,
        options.kv_options(),
        &mut election_host,
    )
}

pub(crate) async fn elect_cluster_raft_leader_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    host: &mut (impl ClusterRaftVoteTransportAsyncHost + Send),
) -> Result<bool, NetworkError> {
    let projection_paths = paths.clone();
    let mut consensus_state = state.clone();
    consensus_state = runtime
        .spawn_blocking(move || {
            project_committed_cluster_raft_config(&projection_paths, &mut consensus_state)?;
            Ok::<_, NetworkError>(consensus_state)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "project committed cluster Raft config".to_string(),
            detail: format!("Raft config projection task failed: {source}"),
        })??;
    let state = &consensus_state;
    let paths_for_lock = paths.clone();
    let state_for_lock = state.clone();
    let (_campaign_lock, existing_outcome) = runtime
        .spawn_blocking(move || {
            let observation_before_wait =
                locked_cluster_raft_leader_observation(&paths_for_lock, &state_for_lock)?;
            let (campaign_lock, waited_for_campaign_lock) =
                lock_cluster_raft_campaign(&paths_for_lock)?;
            let observation_after_wait =
                locked_cluster_raft_leader_observation(&paths_for_lock, &state_for_lock)?;
            let remote_leader_blocks_campaign = observed_remote_leader_blocks_campaign(
                &paths_for_lock,
                &state_for_lock,
                &observation_after_wait,
            );
            let existing_outcome = cluster_raft_leader_outcome_after_campaign_wait(
                &state_for_lock.local_node_id,
                waited_for_campaign_lock,
                &observation_before_wait,
                &observation_after_wait,
                remote_leader_blocks_campaign,
            );
            Ok::<_, NetworkError>((campaign_lock, existing_outcome))
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft leader election".to_string(),
            detail: format!("campaign lock task failed: {source}"),
        })??;
    if let Some(existing_outcome) = existing_outcome {
        return Ok(existing_outcome);
    }
    let voter_ids = sinomo_kv::cluster_kv_raft_voter_ids(state).map_err(kv_err)?;
    let mut remote_vote_targets = Vec::new();
    if !(voter_ids.len() == 1 && voter_ids.first() == Some(&state.local_node_id)) {
        for node_id in ClusterEndpointSelector::select_internal_unique_node_ids_async(
            runtime,
            paths,
            state,
            ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
        )
        .await?
        .into_iter()
        .filter(|node_id| node_id != &state.local_node_id && voter_ids.contains(node_id))
        {
            sinomo_network::try_reserve_vec(
                &mut remote_vote_targets,
                1,
                "collect async raft vote targets",
            )?;
            remote_vote_targets.push(node_id);
        }
    }
    let remote_vote_target_ids = remote_vote_targets.clone();
    let mut election_host = NetworkClusterKvRaftLeaderElectionAsyncHost::new(
        runtime,
        paths,
        state,
        options,
        remote_vote_targets,
        host,
    );
    sinomo_kv::elect_cluster_kv_raft_leader_async_with_host(
        &state.local_node_id,
        voter_ids,
        remote_vote_target_ids,
        options.kv_options(),
        &mut election_host,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(leader_id: Option<NodeId>, current_term: u64) -> ClusterRaftLeaderObservation {
        ClusterRaftLeaderObservation {
            leader_id,
            current_term,
        }
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
        let (first_lock, first_waited) = lock_cluster_raft_campaign(&paths).expect("first lock");
        assert!(!first_waited);

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
        drop(first_lock);

        assert!(
            result_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("queued lock result")
        );
        queued.join().expect("queued thread");
    }
}
