use crate::ClusterPaths;
use crate::error::NetworkError;
use crate::local_membership::ClusterLocalDaemonMembershipObservation;
use crate::membership_forward::ClusterMembershipRpcForwardHost;
use crate::membership_rpc::{ClusterJoinRegistrationRequest, ClusterJoinRegistrationResponse};
use crate::prelude::*;
use crate::route::ClusterSignedEndpointRpcAsyncHost;
use crate::rpc::{ObjectRpcRequest, ObjectRpcResponse};
use crate::rpc_future::ClusterMembershipRpcFuture;
use crate::storage::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterJoinReconciliation {
    pub accepted_endpoint: Option<ClusterEndpointRecord>,
    pub kv_snapshot: Option<KvSnapshot>,
}

pub trait ClusterJoinRegistrationHost: ClusterMembershipRpcForwardHost {
    fn local_daemon_membership_observation(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Result<Option<ClusterLocalDaemonMembershipObservation>, NetworkError> {
        Ok(None)
    }

    fn commit_join_registration_state(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<KvSnapshot, NetworkError>;

    fn remember_successful_join_targets(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        members: &[ClusterMemberRecord],
    ) -> Result<(), NetworkError> {
        for member in members {
            if member.node_id == state.local_node_id {
                continue;
            }
            for endpoint in &member.endpoints {
                if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly {
                    continue;
                }
                self.remember_successful_join_endpoint(paths, state, endpoint)?;
            }
        }
        Ok(())
    }

    fn remember_successful_join_endpoint(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _endpoint: &ClusterEndpointRecord,
    ) -> Result<(), NetworkError> {
        Ok(())
    }
}

pub trait ClusterJoinRegistrationAsyncHost: ClusterSignedEndpointRpcAsyncHost {
    fn local_daemon_membership_observation_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, Option<ClusterLocalDaemonMembershipObservation>> {
        Box::pin(async { Ok(None) })
    }

    fn commit_join_registration_state_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;

    fn remember_successful_join_targets_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        members: &'a [ClusterMemberRecord],
    ) -> ClusterMembershipRpcFuture<'a, ()>
    where
        Self: Send,
    {
        Box::pin(async move {
            for member in members {
                if member.node_id == state.local_node_id {
                    continue;
                }
                for endpoint in &member.endpoints {
                    if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly {
                        continue;
                    }
                    self.remember_successful_join_endpoint_async(paths, state, endpoint)
                        .await?;
                }
            }
            Ok(())
        })
    }

    fn remember_successful_join_endpoint_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
        _endpoint: &'a ClusterEndpointRecord,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

pub trait ClusterLocalDaemonMembershipObserver {
    fn local_daemon_membership_observation(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Result<Option<ClusterLocalDaemonMembershipObservation>, NetworkError> {
        Ok(None)
    }
}

pub trait ClusterLocalDaemonMembershipAsyncObserver {
    fn local_daemon_membership_observation_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, Option<ClusterLocalDaemonMembershipObservation>> {
        Box::pin(async { Ok(None) })
    }
}

pub struct ClusterJoinRegistrationDriver<T, O> {
    driver: ClusterControlPlaneDriver<T>,
    observer: O,
}

impl<T, O> ClusterJoinRegistrationDriver<T, O> {
    pub fn new(driver: ClusterControlPlaneDriver<T>, observer: O) -> Self {
        Self { driver, observer }
    }

    pub fn with_transport(transport: T, observer: O) -> Self {
        Self::new(ClusterControlPlaneDriver::new(transport), observer)
    }

    pub fn driver_mut(&mut self) -> &mut ClusterControlPlaneDriver<T> {
        &mut self.driver
    }

    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }
}

impl<T, O> ClusterMembershipRpcForwardHost for ClusterJoinRegistrationDriver<T, O>
where
    T: ClusterControlPlaneTransport,
{
    fn send_membership_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        ClusterMembershipRpcForwardHost::send_membership_rpc_to_node(
            &mut self.driver,
            state,
            target_node_id,
            request,
        )
    }
}

impl<T, O> ClusterJoinRegistrationHost for ClusterJoinRegistrationDriver<T, O>
where
    T: ClusterControlPlaneTransport,
    O: ClusterLocalDaemonMembershipObserver,
{
    fn local_daemon_membership_observation(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<ClusterLocalDaemonMembershipObservation>, NetworkError> {
        self.observer
            .local_daemon_membership_observation(paths, state)
    }

    fn commit_join_registration_state(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<KvSnapshot, NetworkError> {
        ClusterJoinRegistrationHost::commit_join_registration_state(&mut self.driver, paths, state)
    }

    fn remember_successful_join_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<(), NetworkError> {
        ClusterJoinRegistrationHost::remember_successful_join_endpoint(
            &mut self.driver,
            paths,
            state,
            endpoint,
        )
    }
}

pub trait ClusterJoinReconciliationHost {
    fn send_join_registration(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterJoinRegistrationRequest,
        tcp_timeout: Duration,
        iroh_timeout: Duration,
    ) -> Result<ClusterJoinRegistrationResponse, NetworkError>;

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError>;

    fn observe_join_reconciliation_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _reconciliation: &ClusterJoinReconciliation,
    ) -> Result<(), NetworkError> {
        Ok(())
    }
}
