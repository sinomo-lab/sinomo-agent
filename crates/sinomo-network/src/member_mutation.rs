use crate::ClusterPaths;
use crate::error::NetworkError;
use crate::rpc_future::ClusterMembershipRpcFuture;
use sinomo_core::{
    ClusterBootstrapState, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState,
    ClusterNodeRole,
};
use sinomo_kv::{InProcessKvStore, KvSnapshot, KvTxn};
use sinomo_node::NodeId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterNodeRoleUpdate {
    pub node_id: NodeId,
    pub previous_role: ClusterNodeRole,
    pub role: ClusterNodeRole,
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterRaftRoleUpdate {
    pub node_id: NodeId,
    pub previous_role: ClusterMemberRole,
    pub previous_state: ClusterMemberState,
    pub role: ClusterMemberRole,
    pub state: ClusterMemberState,
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterMemberRemoval {
    pub removed: Vec<ClusterMemberRecord>,
}

pub trait ClusterMemberMutationHost {
    fn prepare_cluster_state_for_kv_sync(
        &mut self,
        _paths: &ClusterPaths,
        _state: &mut ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        Ok(())
    }

    fn current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<InProcessKvStore, NetworkError>;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError>;
}

pub trait ClusterMemberMutationAsyncHost {
    fn prepare_cluster_state_for_kv_sync_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;
}
