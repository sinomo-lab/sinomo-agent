use crate::kv_forward::ClusterKvTxnForwardOptions;
#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
use crate::kv_snapshot::ClusterKvSnapshotRefreshOptions;
use crate::state_authority_reachability::ClusterStateAuthorityReachabilityOptions;
use sinomo_base::{ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterTransportKind};
use sinomo_kv::{ClusterKvRaftLeaderElectionOptions, ClusterRaftAppendRequest};
use sinomo_network::DEFAULT_NETWORK_TIMEOUT;
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
    pub(crate) fn kv_options(&self) -> sinomo_kv::ClusterKvRaftAppendReplicationOptions {
        sinomo_kv::ClusterKvRaftAppendReplicationOptions {
            max_attempts: self.max_attempts,
            timeout: self.timeout,
        }
    }

    pub(crate) fn command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        match endpoint.transport {
            ClusterTransportKind::Iroh => remaining.min(self.iroh_command_timeout),
            _ => remaining.min(self.tcp_command_timeout),
        }
    }

    pub(crate) fn fair_command_timeout(
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

    pub(crate) fn append_attempt_timeout(
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

    pub(crate) fn routed_node_append_attempt_timeout(&self, remaining: Duration) -> Duration {
        // Routed AppendEntries carries its own per-connect and stream
        // no-progress limits. Give routing the remaining Raft operation budget
        // so it can fall back across endpoints and accept a progressing
        // snapshot response without an outer 5s command timeout racing it.
        remaining
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRaftCommittedFanoutOptions {
    pub raft_append_timeout: Duration,
    pub watch_event_timeout: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRaftLeaderElectionOptions {
    pub kv: ClusterKvRaftLeaderElectionOptions,
    pub timeout: Duration,
    pub campaign_jitter_max: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterRaftLeaderElectionOptions {
    pub(crate) fn kv_options(&self) -> sinomo_kv::ClusterKvRaftLeaderElectionOptions {
        self.kv
    }

    pub(crate) fn routed_node_command_timeout(&self, remaining: Duration) -> Duration {
        remaining.min(self.tcp_command_timeout.max(self.iroh_command_timeout))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvTxnQuorumOptions {
    pub allow_forwarding: bool,
    pub raft_append_timeout: Duration,
    pub raft_append_replication_options: ClusterRaftAppendReplicationOptions,
    pub forward_options: ClusterKvTxnForwardOptions,
    pub leader_election_options: ClusterRaftLeaderElectionOptions,
    pub state_authority_reachability_options: ClusterStateAuthorityReachabilityOptions,
}

impl ClusterKvTxnQuorumOptions {
    pub fn with_state_authority_reachability_options(
        mut self,
        options: ClusterStateAuthorityReachabilityOptions,
    ) -> Self {
        self.state_authority_reachability_options = options;
        self
    }
}

pub const DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_MAX_ATTEMPTS: usize = 32;
/// End-to-end budget for selecting and reaching a membership authority.
pub const DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT: Duration = Duration::from_secs(15);
/// Command budget for quorum-bearing control-plane RPCs. These requests can
/// legitimately spend several seconds committing and durably replicating on
/// the target after the route is established, so the ordinary five-second
/// request/read budget is too short for forwarded writes.
pub const DEFAULT_CLUSTER_CONTROL_PLANE_COMMAND_TIMEOUT: Duration = Duration::from_secs(15);
// Foreground attempts deliberately use shorter topology-specific caps so one
// degraded endpoint cannot consume the operation deadline before alternatives
// are tried. These are route-fallback budgets, not transport idle defaults.
pub const DEFAULT_CLUSTER_CONTROL_PLANE_TCP_FOREGROUND_ATTEMPT_TIMEOUT: Duration =
    Duration::from_secs(1);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT: Duration =
    Duration::from_secs(4);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_IROH_FOREGROUND_ATTEMPT_TIMEOUT: Duration =
    Duration::from_millis(1500);
/// End-to-end quorum/replication budget, shared by all endpoint attempts.
pub const DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_CAMPAIGN_JITTER_MAX: Duration =
    Duration::from_millis(750);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterControlPlaneTimeouts {
    pub max_raft_append_attempts: usize,
    pub raft_append_timeout: Duration,
    pub rpc_timeout: Duration,
    pub membership_rpc_timeout: Duration,
    pub raft_campaign_jitter_max: Duration,
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
            rpc_timeout: DEFAULT_NETWORK_TIMEOUT,
            membership_rpc_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT,
            raft_campaign_jitter_max: DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_CAMPAIGN_JITTER_MAX,
            tcp_command_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_COMMAND_TIMEOUT,
            iroh_command_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_COMMAND_TIMEOUT,
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

    pub fn raft_append_attempt_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration {
        self.raft_append_replication_options(remaining)
            .append_attempt_timeout(endpoint, remaining, request)
    }

    pub fn leader_election_options(&self) -> ClusterRaftLeaderElectionOptions {
        ClusterRaftLeaderElectionOptions {
            kv: ClusterKvRaftLeaderElectionOptions {
                max_attempts: sinomo_kv::CLUSTER_KV_RAFT_LEADER_ELECTION_ATTEMPTS,
            },
            timeout: self.raft_append_timeout,
            campaign_jitter_max: self.raft_campaign_jitter_max,
            tcp_command_timeout: self.tcp_command_timeout,
            iroh_command_timeout: self.iroh_command_timeout,
            tcp_foreground_attempt_timeout: self.tcp_foreground_attempt_timeout,
            public_foreground_attempt_timeout: self.public_foreground_attempt_timeout,
            iroh_foreground_attempt_timeout: self.iroh_foreground_attempt_timeout,
        }
    }

    pub fn state_authority_reachability_options(&self) -> ClusterStateAuthorityReachabilityOptions {
        ClusterStateAuthorityReachabilityOptions {
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
            state_authority_reachability_options: self.state_authority_reachability_options(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_control_plane_preserves_bounded_wan_command_budget() {
        let timeouts = ClusterControlPlaneTimeouts::default_daemon_transport();

        assert_eq!(timeouts.rpc_timeout, DEFAULT_NETWORK_TIMEOUT);
        assert_eq!(
            timeouts.tcp_command_timeout,
            DEFAULT_CLUSTER_CONTROL_PLANE_COMMAND_TIMEOUT
        );
        assert_eq!(
            timeouts.iroh_command_timeout,
            DEFAULT_CLUSTER_CONTROL_PLANE_COMMAND_TIMEOUT
        );
        assert!(timeouts.membership_rpc_timeout > DEFAULT_NETWORK_TIMEOUT);
        assert!(timeouts.raft_append_timeout > DEFAULT_NETWORK_TIMEOUT);
    }

    #[test]
    fn foreground_attempt_caps_remain_shorter_for_route_fallback() {
        let timeouts = ClusterControlPlaneTimeouts::default_daemon_transport();

        assert!(timeouts.tcp_foreground_attempt_timeout < DEFAULT_NETWORK_TIMEOUT);
        assert!(timeouts.public_foreground_attempt_timeout < DEFAULT_NETWORK_TIMEOUT);
        assert!(timeouts.iroh_foreground_attempt_timeout < DEFAULT_NETWORK_TIMEOUT);
    }

    #[test]
    fn routed_raft_append_preserves_operation_budget_above_transport_timeout() {
        let timeouts = ClusterControlPlaneTimeouts::default_daemon_transport();
        let options = timeouts.raft_append_replication_options(timeouts.raft_append_timeout);
        let remaining = timeouts.raft_append_timeout - Duration::from_secs(1);

        assert!(remaining > DEFAULT_NETWORK_TIMEOUT);
        assert_eq!(
            options.routed_node_append_attempt_timeout(remaining),
            remaining
        );
        assert_eq!(
            options.tcp_command_timeout,
            DEFAULT_CLUSTER_CONTROL_PLANE_COMMAND_TIMEOUT
        );
        assert_eq!(
            options.iroh_command_timeout,
            DEFAULT_CLUSTER_CONTROL_PLANE_COMMAND_TIMEOUT
        );
    }
}
