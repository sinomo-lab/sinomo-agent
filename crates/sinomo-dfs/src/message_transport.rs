//! DFS message transport contracts.
//!
//! These traits adapt generic `sinomo-network` object-RPC transports to the
//! typed DFS message plane. Higher-level DFS services depend on these traits,
//! not on a concrete transport implementation.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use sinomo_network::NetworkError;
use sinomo_node::NodeId;

use crate::protocol::DfsRoutedMessage;
use crate::routes::{dfs_routed_message_from_object_rpc_request, dfs_routed_message_rpc_request};

pub trait DfsTransportIdentity {
    fn local_node_id(&self) -> &NodeId;
}

impl DfsTransportIdentity for sinomo_network::InProcessTransport {
    fn local_node_id(&self) -> &NodeId {
        self.local_node_id()
    }
}

impl DfsTransportIdentity for sinomo_network::AsyncInProcessTransport {
    fn local_node_id(&self) -> &NodeId {
        self.local_node_id()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DfsTransferProfile {
    Conservative,
    Lan,
    Local,
}

impl From<sinomo_network::ClusterEndpointTransferProfile> for DfsTransferProfile {
    fn from(profile: sinomo_network::ClusterEndpointTransferProfile) -> Self {
        match profile {
            sinomo_network::ClusterEndpointTransferProfile::Conservative => Self::Conservative,
            sinomo_network::ClusterEndpointTransferProfile::Lan => Self::Lan,
            sinomo_network::ClusterEndpointTransferProfile::Local => Self::Local,
        }
    }
}

pub trait DfsMessageTransport: DfsTransportIdentity {
    fn send_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError>;

    fn send_detached_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        self.send_routed_message(target_node_id, message)
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError>;

    fn flush_detached_sends(&mut self) -> Result<(), NetworkError> {
        Ok(())
    }

    fn can_send_synchronously(&self) -> bool {
        true
    }

    fn transfer_profile(&self, _target_node_id: &NodeId) -> DfsTransferProfile {
        DfsTransferProfile::Conservative
    }

    fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        Ok(None)
    }

    fn wait_for_incoming(&self, timeout: Duration) -> Result<bool, NetworkError> {
        if self
            .pending_incoming_count()?
            .is_some_and(|count| count > 0)
        {
            return Ok(true);
        }
        if !timeout.is_zero() {
            std::thread::sleep(timeout);
        }
        Ok(self
            .pending_incoming_count()?
            .is_some_and(|count| count > 0))
    }
}

pub type DfsMessageTransportFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait AsyncDfsMessageTransport: Send {
    fn local_node_id(&self) -> &NodeId;

    fn send_routed_message_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        message: DfsRoutedMessage,
    ) -> DfsMessageTransportFuture<'a, Result<(), NetworkError>>;

    fn drain_incoming_messages_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedMessage>, NetworkError>>;

    fn pending_incoming_count_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Option<usize>, NetworkError>> {
        Box::pin(async { Ok(None) })
    }

    fn wait_for_incoming_async<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> DfsMessageTransportFuture<'a, Result<bool, NetworkError>> {
        Box::pin(async move {
            if self
                .pending_incoming_count_async()
                .await?
                .is_some_and(|count| count > 0)
            {
                return Ok(true);
            }
            if !timeout.is_zero() {
                tokio::time::sleep(timeout).await;
            }
            Ok(self
                .pending_incoming_count_async()
                .await?
                .is_some_and(|count| count > 0))
        })
    }
}

impl DfsMessageTransport for sinomo_network::InProcessTransport {
    fn send_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        let request = dfs_routed_message_rpc_request(&message)?;
        self.send_object_rpc_request(target_node_id, request)
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(self
            .drain_incoming_object_rpc_requests()?
            .into_iter()
            .filter_map(|request| dfs_routed_message_from_object_rpc_request(&request).ok())
            .collect())
    }

    fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        self.pending_incoming_object_rpc_request_count()
    }

    fn wait_for_incoming(&self, timeout: Duration) -> Result<bool, NetworkError> {
        self.wait_for_incoming_object_rpc_request(timeout)
    }

    fn transfer_profile(&self, _target_node_id: &NodeId) -> DfsTransferProfile {
        DfsTransferProfile::Local
    }
}

impl AsyncDfsMessageTransport for sinomo_network::AsyncInProcessTransport {
    fn local_node_id(&self) -> &NodeId {
        self.local_node_id()
    }

    fn send_routed_message_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        message: DfsRoutedMessage,
    ) -> DfsMessageTransportFuture<'a, Result<(), NetworkError>> {
        Box::pin(async move {
            let request = dfs_routed_message_rpc_request(&message)?;
            self.send_object_rpc_request_async(
                target_node_id,
                request,
                sinomo_network::AsyncNetworkSendOptions::default(),
            )
            .await
        })
    }

    fn drain_incoming_messages_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedMessage>, NetworkError>> {
        Box::pin(async move {
            Ok(self
                .drain_incoming_object_rpc_requests(usize::MAX)
                .await?
                .into_iter()
                .filter_map(|request| dfs_routed_message_from_object_rpc_request(&request).ok())
                .collect())
        })
    }

    fn pending_incoming_count_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Option<usize>, NetworkError>> {
        Box::pin(async move { self.pending_incoming_object_rpc_request_count().await })
    }

    fn wait_for_incoming_async<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> DfsMessageTransportFuture<'a, Result<bool, NetworkError>> {
        Box::pin(async move { self.wait_for_incoming_object_rpc_request(timeout).await })
    }
}
