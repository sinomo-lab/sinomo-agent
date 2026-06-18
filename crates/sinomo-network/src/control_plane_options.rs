use crate::kv_forward::ClusterKvTxnForwardOptions;
use crate::kv_snapshot::ClusterKvSnapshotRefreshOptions;
use crate::voter_reachability::ClusterVoterReachabilityOptions;
use sinomo_core::{ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterTransportKind};
use sinomo_kv::{ClusterKvRaftLeaderElectionOptions, ClusterRaftAppendRequest};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRaftAppendReplicationOptions {
    pub max_attempts: usize,
    pub timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterRaftAppendReplicationOptions {
    pub fn kv_options(&self) -> sinomo_kv::ClusterKvRaftAppendReplicationOptions {
        sinomo_kv::ClusterKvRaftAppendReplicationOptions {
            max_attempts: self.max_attempts,
            timeout: self.timeout,
        }
    }

    pub fn command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        match endpoint.transport {
            ClusterTransportKind::Iroh => remaining.min(self.iroh_command_timeout),
            _ => remaining.min(self.tcp_command_timeout),
        }
    }

    pub fn fair_command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        let attempt_cap = match endpoint.transport {
            ClusterTransportKind::Iroh => self.iroh_foreground_attempt_timeout,
            _ if matches!(endpoint.privacy, ClusterEndpointPrivacy::InternetDirect) => {
                self.public_foreground_attempt_timeout
            }
            _ => self.tcp_foreground_attempt_timeout,
        };
        self.command_timeout(endpoint, remaining.min(attempt_cap))
    }

    pub fn append_attempt_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration {
        if request.snapshot.is_some() || request.append.entries.len() > 64 {
            return self.command_timeout(endpoint, remaining);
        }
        self.fair_command_timeout(endpoint, remaining)
    }
}

pub type ClusterRaftAppendReplicationOutcome = sinomo_kv::ClusterKvRaftAppendReplicationOutcome;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRaftCommittedFanoutOptions {
    pub raft_append_timeout: Duration,
    pub watch_event_timeout: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRaftLeaderElectionOptions {
    pub kv: ClusterKvRaftLeaderElectionOptions,
    pub timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterRaftLeaderElectionOptions {
    pub fn kv_options(&self) -> sinomo_kv::ClusterKvRaftLeaderElectionOptions {
        self.kv
    }

    pub fn command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        match endpoint.transport {
            ClusterTransportKind::Iroh => remaining.min(self.iroh_command_timeout),
            _ => remaining.min(self.tcp_command_timeout),
        }
    }

    pub fn fair_command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        let attempt_cap = match endpoint.transport {
            ClusterTransportKind::Iroh => self.iroh_foreground_attempt_timeout,
            _ if matches!(endpoint.privacy, ClusterEndpointPrivacy::InternetDirect) => {
                self.public_foreground_attempt_timeout
            }
            _ => self.tcp_foreground_attempt_timeout,
        };
        self.command_timeout(endpoint, remaining.min(attempt_cap))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvTxnQuorumOptions {
    pub allow_forwarding: bool,
    pub raft_append_timeout: Duration,
    pub raft_append_replication_options: ClusterRaftAppendReplicationOptions,
    pub forward_options: ClusterKvTxnForwardOptions,
    pub leader_election_options: ClusterRaftLeaderElectionOptions,
    pub voter_reachability_options: ClusterVoterReachabilityOptions,
}

pub const DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_MAX_ATTEMPTS: usize = 32;
pub const DEFAULT_CLUSTER_CONTROL_PLANE_RPC_TIMEOUT: Duration = Duration::from_millis(750);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT: Duration = Duration::from_secs(15);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_TCP_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_IROH_COMMAND_TIMEOUT: Duration = Duration::from_secs(3);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_TCP_FOREGROUND_ATTEMPT_TIMEOUT: Duration =
    Duration::from_secs(1);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT: Duration =
    Duration::from_secs(4);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_IROH_FOREGROUND_ATTEMPT_TIMEOUT: Duration =
    Duration::from_millis(1500);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterControlPlaneTimeouts {
    pub max_raft_append_attempts: usize,
    pub raft_append_timeout: Duration,
    pub rpc_timeout: Duration,
    pub membership_rpc_timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterControlPlaneTimeouts {
    pub fn default_daemon_transport() -> Self {
        Self {
            max_raft_append_attempts: DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_MAX_ATTEMPTS,
            raft_append_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT,
            rpc_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_RPC_TIMEOUT,
            membership_rpc_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT,
            tcp_command_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_TCP_COMMAND_TIMEOUT,
            iroh_command_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_IROH_COMMAND_TIMEOUT,
            tcp_foreground_attempt_timeout:
                DEFAULT_CLUSTER_CONTROL_PLANE_TCP_FOREGROUND_ATTEMPT_TIMEOUT,
            public_foreground_attempt_timeout:
                DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT,
            iroh_foreground_attempt_timeout:
                DEFAULT_CLUSTER_CONTROL_PLANE_IROH_FOREGROUND_ATTEMPT_TIMEOUT,
        }
    }

    pub fn raft_append_replication_options(
        &self,
        timeout: Duration,
    ) -> ClusterRaftAppendReplicationOptions {
        ClusterRaftAppendReplicationOptions {
            max_attempts: self.max_raft_append_attempts,
            timeout,
            tcp_command_timeout: self.tcp_command_timeout,
            iroh_command_timeout: self.iroh_command_timeout,
            tcp_foreground_attempt_timeout: self.tcp_foreground_attempt_timeout,
            public_foreground_attempt_timeout: self.public_foreground_attempt_timeout,
            iroh_foreground_attempt_timeout: self.iroh_foreground_attempt_timeout,
        }
    }

    pub fn committed_fanout_options(&self) -> ClusterRaftCommittedFanoutOptions {
        ClusterRaftCommittedFanoutOptions {
            raft_append_timeout: self.raft_append_timeout,
            watch_event_timeout: self.rpc_timeout,
        }
    }

    pub fn leader_election_options(&self) -> ClusterRaftLeaderElectionOptions {
        ClusterRaftLeaderElectionOptions {
            kv: ClusterKvRaftLeaderElectionOptions {
                max_attempts: sinomo_kv::CLUSTER_KV_RAFT_LEADER_ELECTION_ATTEMPTS,
            },
            timeout: self.raft_append_timeout,
            tcp_command_timeout: self.tcp_command_timeout,
            iroh_command_timeout: self.iroh_command_timeout,
            tcp_foreground_attempt_timeout: self.tcp_foreground_attempt_timeout,
            public_foreground_attempt_timeout: self.public_foreground_attempt_timeout,
            iroh_foreground_attempt_timeout: self.iroh_foreground_attempt_timeout,
        }
    }

    pub fn voter_reachability_options(&self) -> ClusterVoterReachabilityOptions {
        ClusterVoterReachabilityOptions {
            timeout: self.raft_append_timeout,
            tcp_command_timeout: self.tcp_command_timeout,
            iroh_command_timeout: self.iroh_command_timeout,
            tcp_foreground_attempt_timeout: self.tcp_foreground_attempt_timeout,
            public_foreground_attempt_timeout: self.public_foreground_attempt_timeout,
            iroh_foreground_attempt_timeout: self.iroh_foreground_attempt_timeout,
        }
    }

    pub fn kv_txn_forward_options(&self) -> ClusterKvTxnForwardOptions {
        ClusterKvTxnForwardOptions {
            timeout: self.raft_append_timeout,
            tcp_command_timeout: self.tcp_command_timeout,
            iroh_command_timeout: self.iroh_command_timeout,
        }
    }

    pub fn kv_txn_quorum_options(&self, allow_forwarding: bool) -> ClusterKvTxnQuorumOptions {
        ClusterKvTxnQuorumOptions {
            allow_forwarding,
            raft_append_timeout: self.raft_append_timeout,
            raft_append_replication_options: self
                .raft_append_replication_options(self.raft_append_timeout),
            forward_options: self.kv_txn_forward_options(),
            leader_election_options: self.leader_election_options(),
            voter_reachability_options: self.voter_reachability_options(),
        }
    }

    pub fn kv_snapshot_refresh_options(&self) -> ClusterKvSnapshotRefreshOptions {
        ClusterKvSnapshotRefreshOptions {
            without_local_snapshot_timeout: self.raft_append_timeout,
            with_local_snapshot_timeout: self.rpc_timeout,
            tcp_command_timeout: self.rpc_timeout,
            iroh_command_timeout: self.rpc_timeout,
            tcp_foreground_attempt_timeout: self.tcp_foreground_attempt_timeout,
            public_foreground_attempt_timeout: self.public_foreground_attempt_timeout,
            iroh_foreground_attempt_timeout: self.iroh_foreground_attempt_timeout,
        }
    }
}
