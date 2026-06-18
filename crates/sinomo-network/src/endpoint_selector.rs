use crate::storage::{
    ClusterPaths, cluster_daemon_command_targets_impl, cluster_dfs_data_targets_impl,
    cluster_invite_publish_targets_impl, cluster_main_daemon_command_endpoint_candidates_impl,
    cluster_main_daemon_command_targets_impl,
    cluster_member_daemon_status_targets_with_connectivity, cluster_member_publish_targets_impl,
    cluster_member_publish_targets_voter_first_impl, cluster_raft_command_targets_impl,
    cluster_raft_live_voter_targets, cluster_rollout_endpoint_targets_impl,
    cluster_rollout_target_endpoints_for_main_with_command_endpoint_impl,
    cluster_voter_command_targets_impl,
};
use sinomo_core::{ClusterBootstrapState, ClusterEndpointRecord, ClusterMemberRecord};
use sinomo_node::NodeId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterEndpointSelectionPurpose {
    DaemonCommand,
    DfsData,
    InvitePublish,
    LiveVoterCommand,
    MainDaemonCommand,
    MainDaemonCommandCandidates,
    MemberDaemonStatus,
    MemberPublish,
    MemberPublishVoterFirst,
    RaftCommand,
    Rollout,
    VoterCommand,
}

pub struct ClusterEndpointSelector<'a> {
    paths: &'a ClusterPaths,
    state: &'a ClusterBootstrapState,
}

impl<'a> ClusterEndpointSelector<'a> {
    pub fn new(paths: &'a ClusterPaths, state: &'a ClusterBootstrapState) -> Self {
        Self { paths, state }
    }

    pub fn select(&self, purpose: ClusterEndpointSelectionPurpose) -> Vec<ClusterEndpointRecord> {
        match purpose {
            ClusterEndpointSelectionPurpose::DaemonCommand => {
                cluster_daemon_command_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::DfsData => {
                cluster_dfs_data_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::InvitePublish => {
                cluster_invite_publish_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::LiveVoterCommand => {
                cluster_raft_live_voter_targets(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::MainDaemonCommand => {
                cluster_main_daemon_command_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::MainDaemonCommandCandidates => {
                cluster_main_daemon_command_endpoint_candidates_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::MemberDaemonStatus => Vec::new(),
            ClusterEndpointSelectionPurpose::MemberPublish => {
                cluster_member_publish_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::MemberPublishVoterFirst => {
                cluster_member_publish_targets_voter_first_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::RaftCommand => {
                cluster_raft_command_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::Rollout => {
                cluster_rollout_endpoint_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::VoterCommand => {
                cluster_voter_command_targets_impl(self.paths, self.state)
            }
        }
    }

    pub fn select_member(
        &self,
        member: &ClusterMemberRecord,
        purpose: ClusterEndpointSelectionPurpose,
    ) -> Vec<ClusterEndpointRecord> {
        match purpose {
            ClusterEndpointSelectionPurpose::MemberDaemonStatus => {
                cluster_member_daemon_status_targets_with_connectivity(
                    self.paths, self.state, member,
                )
            }
            _ => Vec::new(),
        }
    }

    pub fn rollout_targets_for_main_with_command_endpoint(
        &self,
        main_node_id: &NodeId,
        command_endpoint: Option<&ClusterEndpointRecord>,
        candidates: &[ClusterEndpointRecord],
    ) -> Vec<ClusterEndpointRecord> {
        cluster_rollout_target_endpoints_for_main_with_command_endpoint_impl(
            self.paths,
            self.state,
            main_node_id,
            command_endpoint,
            candidates,
        )
    }
}
