//! Built-in authenticated link composition for loopback and LAN TCP.
//!
//! Native network modules authenticate their own transport principals. The
//! built-in TCP transports instead bind the local daemon identity to a fresh
//! channel exporter in the fixed SAT1 prelude. End-to-end routed-session key
//! agreement then authenticates the destination before application bytes are
//! exposed.

use crate::direct_link::{
    AuthenticatedLink, AuthenticatedTransportMetadata, DirectLinkFuture, DirectStreamCapabilities,
    LinkProvider, write_authenticated_transport_prelude,
};
use crate::network_module::{RegisteredNetworkModuleLinkProvider, network_module_registry};
use crate::routing::RoutedSessionIdentity;
use crate::{DirectorySnapshot, NetworkError, NetworkHealthPolicy};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use sinomo_base::{ClusterEndpointRecord, ClusterTransportKind, Id};
use sinomo_node::NodeId;
use std::str::FromStr;
use std::sync::Arc;

const BUILTIN_LINK_AUTH_PROTOCOL: &str = "sinomo.builtin-link-auth.v1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct BuiltinLinkAuthenticationTranscript {
    protocol: String,
    transport: ClusterTransportKind,
    source_node_id: NodeId,
    destination_node_id: NodeId,
    channel_binding: [u8; 32],
    capabilities: DirectStreamCapabilities,
}

fn builtin_transport(transport: &ClusterTransportKind) -> bool {
    matches!(
        transport,
        ClusterTransportKind::Loopback | ClusterTransportKind::LanTcp
    )
}

fn builtin_capabilities() -> DirectStreamCapabilities {
    DirectStreamCapabilities {
        full_duplex: true,
        relay_forwarding: true,
        confidential: false,
        max_concurrent_streams: 1,
    }
}

fn builtin_transcript(
    transport: ClusterTransportKind,
    source_node_id: NodeId,
    destination_node_id: NodeId,
    channel_binding: [u8; 32],
    capabilities: DirectStreamCapabilities,
) -> Result<Vec<u8>, NetworkError> {
    crate::SinomoObjectCodec::network_frame().encode(&BuiltinLinkAuthenticationTranscript {
        protocol: BUILTIN_LINK_AUTH_PROTOCOL.to_string(),
        transport,
        source_node_id,
        destination_node_id,
        channel_binding,
        capabilities,
    })
}

/// Verifies built-in SAT1 metadata against the current directory and returns
/// the one authenticated adjacent peer.
pub fn verify_builtin_authenticated_transport_metadata(
    metadata: &AuthenticatedTransportMetadata,
    local_node_id: &NodeId,
    directory: &DirectorySnapshot,
    identity: &dyn RoutedSessionIdentity,
) -> Result<NodeId, NetworkError> {
    if !builtin_transport(&metadata.transport)
        || metadata.capabilities != builtin_capabilities()
        || identity.local_node_id() != local_node_id
    {
        return Err(NetworkError::AuthDenied);
    }
    let peer_node_id = NodeId::from_str(&metadata.peer_transport_identity)
        .map_err(|_| NetworkError::AuthDenied)?;
    if peer_node_id == *local_node_id {
        return Err(NetworkError::AuthDenied);
    }
    let peer = directory
        .node(&peer_node_id)
        .ok_or_else(|| NetworkError::UnauthorizedNode {
            node_id: peer_node_id.clone(),
        })?;
    let transcript = builtin_transcript(
        metadata.transport.clone(),
        peer_node_id.clone(),
        local_node_id.clone(),
        metadata.channel_binding,
        metadata.capabilities,
    )?;
    identity.verify(
        &peer_node_id,
        &peer.public_key,
        &transcript,
        &metadata.transport_proof,
    )?;
    Ok(peer_node_id)
}

/// Authenticated link provider for the built-in loopback/LAN TCP transports.
pub struct BuiltinTcpLinkProvider {
    local_node_id: NodeId,
    identity: Arc<dyn RoutedSessionIdentity>,
    health: NetworkHealthPolicy,
}

impl BuiltinTcpLinkProvider {
    pub fn new(
        local_node_id: NodeId,
        identity: Arc<dyn RoutedSessionIdentity>,
        health: NetworkHealthPolicy,
    ) -> Result<Self, NetworkError> {
        if identity.local_node_id() != &local_node_id {
            return Err(NetworkError::AuthDenied);
        }
        Ok(Self {
            local_node_id,
            identity,
            health,
        })
    }
}

impl LinkProvider for BuiltinTcpLinkProvider {
    fn connect<'a>(
        &'a self,
        endpoint: &'a ClusterEndpointRecord,
    ) -> DirectLinkFuture<'a, AuthenticatedLink> {
        let endpoint = endpoint.clone();
        let local_node_id = self.local_node_id.clone();
        let identity = Arc::clone(&self.identity);
        let health = self.health;
        Box::pin(async move {
            if !builtin_transport(&endpoint.transport) || endpoint.node_id == local_node_id {
                return Err(NetworkError::Io {
                    operation: "connect built-in authenticated link".to_string(),
                    detail: format!(
                        "transport {:?} to {} is not an adjacent built-in link",
                        endpoint.transport, endpoint.node_id
                    ),
                });
            }
            let mut channel_binding = [0_u8; 32];
            SystemRandom::new()
                .fill(&mut channel_binding)
                .map_err(|_| NetworkError::Io {
                    operation: "generate built-in link channel binding".to_string(),
                    detail: "system random source failed".to_string(),
                })?;
            let capabilities = builtin_capabilities();
            let transcript = builtin_transcript(
                endpoint.transport.clone(),
                local_node_id.clone(),
                endpoint.node_id.clone(),
                channel_binding,
                capabilities,
            )?;
            let transport_proof = identity.sign(&transcript)?;
            let metadata = AuthenticatedTransportMetadata::try_new(
                endpoint.transport.clone(),
                local_node_id.to_string(),
                channel_binding,
                transport_proof,
                capabilities,
            )?;
            let mut link_identity = Vec::new();
            link_identity.extend_from_slice(local_node_id.as_str().as_bytes());
            link_identity.push(0);
            link_identity.extend_from_slice(endpoint.node_id.as_str().as_bytes());
            link_identity.push(0);
            link_identity.extend_from_slice(&channel_binding);
            let link_id = Id::derived("sinomo.network.builtin-authenticated-link", &link_identity);
            let opener_endpoint = endpoint.clone();
            AuthenticatedLink::new(
                local_node_id,
                endpoint.node_id,
                link_id,
                channel_binding,
                capabilities,
                move || {
                    let endpoint = opener_endpoint.clone();
                    let metadata = metadata.clone();
                    Box::pin(async move {
                        let mut stream = crate::open_daemon_rpc_stream_to_cluster_endpoint_async(
                            &endpoint, health,
                        )
                        .await?;
                        write_authenticated_transport_prelude(&mut *stream, &metadata).await?;
                        Ok(stream)
                    })
                },
            )
        })
    }

    fn accept(&self) -> DirectLinkFuture<'_, AuthenticatedLink> {
        Box::pin(async {
            Err(NetworkError::Io {
                operation: "accept built-in authenticated link".to_string(),
                detail: "the daemon listener owns accepted built-in streams".to_string(),
            })
        })
    }
}

/// Production provider that composes registered native modules with the
/// built-in TCP transports behind the one direct-link boundary.
pub struct ClusterLinkProvider {
    builtin: BuiltinTcpLinkProvider,
    modules: RegisteredNetworkModuleLinkProvider,
}

impl ClusterLinkProvider {
    pub fn new(
        local_node_id: NodeId,
        identity: Arc<dyn RoutedSessionIdentity>,
        health: NetworkHealthPolicy,
    ) -> Result<Self, NetworkError> {
        Ok(Self {
            builtin: BuiltinTcpLinkProvider::new(local_node_id, identity, health)?,
            modules: RegisteredNetworkModuleLinkProvider::new(health),
        })
    }
}

impl LinkProvider for ClusterLinkProvider {
    fn connect<'a>(
        &'a self,
        endpoint: &'a ClusterEndpointRecord,
    ) -> DirectLinkFuture<'a, AuthenticatedLink> {
        if network_module_registry()
            .and_then(|registry| registry.module_for_endpoint(endpoint))
            .is_some()
        {
            self.modules.connect(endpoint)
        } else {
            self.builtin.connect(endpoint)
        }
    }

    fn accept(&self) -> DirectLinkFuture<'_, AuthenticatedLink> {
        Box::pin(async {
            Err(NetworkError::Io {
                operation: "accept cluster authenticated link".to_string(),
                detail: "native module and daemon listeners own accepted links".to_string(),
            })
        })
    }
}
