//! Transport-neutral contracts for authenticated adjacent-peer links.
//!
//! This is the lowest network layer. It knows how to open bounded duplex
//! streams to an already authenticated adjacent peer, but it does not know
//! about connectivity snapshots, node routes, relays, or object RPC.

use crate::error::NetworkError;
use serde::{Deserialize, Serialize};
use sinomo_base::{ClusterEndpointRecord, ClusterTransportKind, Id};
use sinomo_node::NodeId;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const AUTHENTICATED_TRANSPORT_PRELUDE_PREFIX: [u8; 4] = *b"SAT1";
pub const MAX_AUTHENTICATED_TRANSPORT_IDENTITY_BYTES: usize = 4 * 1024;
pub const MAX_AUTHENTICATED_TRANSPORT_PROOF_BYTES: usize = 4 * 1024;
const AUTHENTICATED_TRANSPORT_PRELUDE_HEADER_BYTES: usize = 46;

/// A transport-owned bidirectional byte stream.
///
/// Framing and application protocols live above this boundary. Implementors
/// preserve byte order and half-close behavior without interpreting bytes.
pub trait NetworkDuplexStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> NetworkDuplexStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

pub type BoxedNetworkStream = Box<dyn NetworkDuplexStream>;

/// Capabilities fixed for the lifetime of an authenticated direct link.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DirectStreamCapabilities {
    pub full_duplex: bool,
    pub relay_forwarding: bool,
    pub confidential: bool,
    pub max_concurrent_streams: u32,
}

impl DirectStreamCapabilities {
    fn validate(self) -> Result<Self, NetworkError> {
        if !self.full_duplex || self.max_concurrent_streams == 0 {
            return Err(NetworkError::Serialization {
                detail: "authenticated transport must provide a full-duplex stream".to_string(),
            });
        }
        Ok(self)
    }
}

impl DirectStreamCapabilities {
    pub const fn multiplexed_full_duplex(max_concurrent_streams: u32) -> Self {
        Self {
            full_duplex: true,
            relay_forwarding: true,
            confidential: true,
            max_concurrent_streams,
        }
    }
}

/// Transport-authenticated principal forwarded from a native listener to the
/// daemon composition boundary.
///
/// This deliberately carries no `NodeId`. The transport proves its own stable
/// principal (for example an Iroh endpoint public key); the daemon maps that
/// value to exactly one endpoint in the current directory before constructing
/// an [`AuthenticatedLink`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthenticatedTransportMetadata {
    pub transport: ClusterTransportKind,
    pub peer_transport_identity: String,
    pub channel_binding: [u8; 32],
    /// Opaque, module-owned proof binding the peer principal to this channel.
    /// The generic direct-link layer only bounds and transports these bytes;
    /// the module that owns `transport` validates their semantics.
    pub transport_proof: Vec<u8>,
    pub capabilities: DirectStreamCapabilities,
}

impl AuthenticatedTransportMetadata {
    pub fn try_new(
        transport: ClusterTransportKind,
        peer_transport_identity: impl Into<String>,
        channel_binding: [u8; 32],
        transport_proof: impl Into<Vec<u8>>,
        capabilities: DirectStreamCapabilities,
    ) -> Result<Self, NetworkError> {
        let peer_transport_identity = peer_transport_identity.into();
        let transport_proof = transport_proof.into();
        if peer_transport_identity.is_empty()
            || peer_transport_identity.len() > MAX_AUTHENTICATED_TRANSPORT_IDENTITY_BYTES
            || channel_binding == [0; 32]
            || transport_proof.is_empty()
            || transport_proof.len() > MAX_AUTHENTICATED_TRANSPORT_PROOF_BYTES
        {
            return Err(NetworkError::Serialization {
                detail: "authenticated transport metadata is missing a bounded peer identity, channel binding, or proof"
                    .to_string(),
            });
        }
        Ok(Self {
            transport,
            peer_transport_identity,
            channel_binding,
            transport_proof,
            capabilities: capabilities.validate()?,
        })
    }
}

fn authenticated_transport_kind_tag(transport: &ClusterTransportKind) -> u8 {
    match transport {
        ClusterTransportKind::InProcess => 1,
        ClusterTransportKind::Loopback => 2,
        ClusterTransportKind::LanTcp => 3,
        ClusterTransportKind::LanQuic => 4,
        ClusterTransportKind::Iroh => 5,
        ClusterTransportKind::WebRtc => 6,
        ClusterTransportKind::TurnRelay => 7,
        ClusterTransportKind::HttpsRelay => 8,
    }
}

fn authenticated_transport_kind_from_tag(tag: u8) -> Result<ClusterTransportKind, NetworkError> {
    match tag {
        1 => Ok(ClusterTransportKind::InProcess),
        2 => Ok(ClusterTransportKind::Loopback),
        3 => Ok(ClusterTransportKind::LanTcp),
        4 => Ok(ClusterTransportKind::LanQuic),
        5 => Ok(ClusterTransportKind::Iroh),
        6 => Ok(ClusterTransportKind::WebRtc),
        7 => Ok(ClusterTransportKind::TurnRelay),
        8 => Ok(ClusterTransportKind::HttpsRelay),
        _ => Err(NetworkError::Serialization {
            detail: format!("unknown authenticated transport kind tag {tag}"),
        }),
    }
}

fn authenticated_transport_capability_flags(capabilities: DirectStreamCapabilities) -> u8 {
    u8::from(capabilities.full_duplex)
        | (u8::from(capabilities.relay_forwarding) << 1)
        | (u8::from(capabilities.confidential) << 2)
}

fn authenticated_transport_capabilities(
    flags: u8,
    max_concurrent_streams: u32,
) -> Result<DirectStreamCapabilities, NetworkError> {
    if flags & !0b111 != 0 {
        return Err(NetworkError::Serialization {
            detail: "authenticated transport prelude contains unknown capability flags".to_string(),
        });
    }
    DirectStreamCapabilities {
        full_duplex: flags & 0b001 != 0,
        relay_forwarding: flags & 0b010 != 0,
        confidential: flags & 0b100 != 0,
        max_concurrent_streams,
    }
    .validate()
}

pub fn authenticated_transport_prelude_prefix_matches(prefix: &[u8]) -> bool {
    prefix.len() <= AUTHENTICATED_TRANSPORT_PRELUDE_PREFIX.len()
        && AUTHENTICATED_TRANSPORT_PRELUDE_PREFIX.starts_with(prefix)
}

/// Writes the fixed, bounded metadata prelude before any routed-session bytes.
pub async fn write_authenticated_transport_prelude<W>(
    writer: &mut W,
    metadata: &AuthenticatedTransportMetadata,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let metadata = AuthenticatedTransportMetadata::try_new(
        metadata.transport.clone(),
        metadata.peer_transport_identity.clone(),
        metadata.channel_binding,
        metadata.transport_proof.clone(),
        metadata.capabilities,
    )?;
    let identity = metadata.peer_transport_identity.as_bytes();
    let identity_len = u16::try_from(identity.len()).map_err(|_| NetworkError::FrameTooLarge {
        actual_bytes: identity.len(),
        max_bytes: MAX_AUTHENTICATED_TRANSPORT_IDENTITY_BYTES,
    })?;
    let proof_len =
        u16::try_from(metadata.transport_proof.len()).map_err(|_| NetworkError::FrameTooLarge {
            actual_bytes: metadata.transport_proof.len(),
            max_bytes: MAX_AUTHENTICATED_TRANSPORT_PROOF_BYTES,
        })?;
    let mut header = [0_u8; AUTHENTICATED_TRANSPORT_PRELUDE_HEADER_BYTES];
    header[..4].copy_from_slice(&AUTHENTICATED_TRANSPORT_PRELUDE_PREFIX);
    header[4] = authenticated_transport_kind_tag(&metadata.transport);
    header[5] = authenticated_transport_capability_flags(metadata.capabilities);
    header[6..10].copy_from_slice(&metadata.capabilities.max_concurrent_streams.to_be_bytes());
    header[10..42].copy_from_slice(&metadata.channel_binding);
    header[42..44].copy_from_slice(&identity_len.to_be_bytes());
    header[44..46].copy_from_slice(&proof_len.to_be_bytes());
    writer
        .write_all(&header)
        .await
        .map_err(|error| NetworkError::Io {
            operation: "write authenticated transport prelude".to_string(),
            detail: error.to_string(),
        })?;
    writer
        .write_all(identity)
        .await
        .map_err(|error| NetworkError::Io {
            operation: "write authenticated transport identity".to_string(),
            detail: error.to_string(),
        })?;
    writer
        .write_all(&metadata.transport_proof)
        .await
        .map_err(|error| NetworkError::Io {
            operation: "write authenticated transport proof".to_string(),
            detail: error.to_string(),
        })?;
    writer.flush().await.map_err(|error| NetworkError::Io {
        operation: "flush authenticated transport prelude".to_string(),
        detail: error.to_string(),
    })
}

/// Reads and validates one transport prelude without consuming routed bytes.
pub async fn read_authenticated_transport_prelude<R>(
    reader: &mut R,
) -> Result<AuthenticatedTransportMetadata, NetworkError>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut header = [0_u8; AUTHENTICATED_TRANSPORT_PRELUDE_HEADER_BYTES];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|error| NetworkError::Io {
            operation: "read authenticated transport prelude".to_string(),
            detail: error.to_string(),
        })?;
    if header[..4] != AUTHENTICATED_TRANSPORT_PRELUDE_PREFIX {
        return Err(NetworkError::Serialization {
            detail: "invalid authenticated transport prelude prefix".to_string(),
        });
    }
    let transport = authenticated_transport_kind_from_tag(header[4])?;
    let max_concurrent_streams = u32::from_be_bytes(
        header[6..10]
            .try_into()
            .expect("authenticated transport concurrency header"),
    );
    let capabilities = authenticated_transport_capabilities(header[5], max_concurrent_streams)?;
    let channel_binding = header[10..42]
        .try_into()
        .expect("authenticated transport channel binding");
    let identity_len = usize::from(u16::from_be_bytes(
        header[42..44]
            .try_into()
            .expect("authenticated transport identity length"),
    ));
    let proof_len = usize::from(u16::from_be_bytes(
        header[44..46]
            .try_into()
            .expect("authenticated transport proof length"),
    ));
    if identity_len == 0 || identity_len > MAX_AUTHENTICATED_TRANSPORT_IDENTITY_BYTES {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: identity_len,
            max_bytes: MAX_AUTHENTICATED_TRANSPORT_IDENTITY_BYTES,
        });
    }
    if proof_len == 0 || proof_len > MAX_AUTHENTICATED_TRANSPORT_PROOF_BYTES {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: proof_len,
            max_bytes: MAX_AUTHENTICATED_TRANSPORT_PROOF_BYTES,
        });
    }
    let mut identity = vec![0_u8; identity_len];
    reader
        .read_exact(&mut identity)
        .await
        .map_err(|error| NetworkError::Io {
            operation: "read authenticated transport identity".to_string(),
            detail: error.to_string(),
        })?;
    let peer_transport_identity =
        String::from_utf8(identity).map_err(|_| NetworkError::Serialization {
            detail: "authenticated transport identity is not UTF-8".to_string(),
        })?;
    let mut transport_proof = vec![0_u8; proof_len];
    reader
        .read_exact(&mut transport_proof)
        .await
        .map_err(|error| NetworkError::Io {
            operation: "read authenticated transport proof".to_string(),
            detail: error.to_string(),
        })?;
    AuthenticatedTransportMetadata::try_new(
        transport,
        peer_transport_identity,
        channel_binding,
        transport_proof,
        capabilities,
    )
}

pub type DirectLinkFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

type DirectStreamOpener =
    dyn Fn() -> DirectLinkFuture<'static, BoxedNetworkStream> + Send + Sync + 'static;

/// One authenticated adjacent-peer relationship.
///
/// Authentication here proves the transport peer only. Whether the peer is a
/// currently authorized cluster member is checked by routing before a link is
/// used.
#[derive(Clone)]
pub struct AuthenticatedLink {
    local_node_id: NodeId,
    peer_node_id: NodeId,
    link_id: Id,
    channel_binding: [u8; 32],
    capabilities: DirectStreamCapabilities,
    open_stream: Arc<DirectStreamOpener>,
}

impl AuthenticatedLink {
    pub fn new<F>(
        local_node_id: NodeId,
        peer_node_id: NodeId,
        link_id: Id,
        channel_binding: [u8; 32],
        capabilities: DirectStreamCapabilities,
        open_stream: F,
    ) -> Result<Self, NetworkError>
    where
        F: Fn() -> DirectLinkFuture<'static, BoxedNetworkStream> + Send + Sync + 'static,
    {
        if local_node_id == peer_node_id || channel_binding == [0; 32] {
            return Err(NetworkError::Serialization {
                detail: "an authenticated direct link must connect distinct nodes with a channel binding"
                    .to_string(),
            });
        }
        if !capabilities.full_duplex || capabilities.max_concurrent_streams == 0 {
            return Err(NetworkError::Serialization {
                detail: "an authenticated direct link must provide at least one full-duplex stream"
                    .to_string(),
            });
        }
        Ok(Self {
            local_node_id,
            peer_node_id,
            link_id,
            channel_binding,
            capabilities,
            open_stream: Arc::new(open_stream),
        })
    }

    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    pub fn peer_node_id(&self) -> &NodeId {
        &self.peer_node_id
    }

    pub fn link_id(&self) -> &Id {
        &self.link_id
    }

    pub fn channel_binding(&self) -> &[u8; 32] {
        &self.channel_binding
    }

    pub fn capabilities(&self) -> DirectStreamCapabilities {
        self.capabilities
    }

    pub async fn open_stream(&self) -> Result<AuthenticatedDirectStream, NetworkError> {
        let io = (self.open_stream)().await?;
        AuthenticatedDirectStream::accepted(
            io,
            self.local_node_id.clone(),
            self.peer_node_id.clone(),
            self.link_id.clone(),
            self.channel_binding,
        )
    }
}

/// A single independently cancellable stream opened from a direct link.
pub struct AuthenticatedDirectStream {
    pub io: BoxedNetworkStream,
    pub local_node_id: NodeId,
    pub peer_node_id: NodeId,
    pub link_id: Id,
    pub stream_id: Id,
    /// Transport exporter shared by both ends of the authenticated link.
    ///
    /// Routed-session establishment combines this with its signed transcript
    /// to derive the end-to-end stream binding. `channel_binding` below is the
    /// transport-local logical-stream binding and need not be exchanged on
    /// the routing wire.
    pub link_channel_binding: [u8; 32],
    pub channel_binding: [u8; 32],
}

impl AuthenticatedDirectStream {
    /// Constructs one accepted transport stream after the listener has mapped
    /// its authenticated transport principal to exactly one authorized peer.
    pub fn accepted(
        io: BoxedNetworkStream,
        local_node_id: NodeId,
        peer_node_id: NodeId,
        link_id: Id,
        link_channel_binding: [u8; 32],
    ) -> Result<Self, NetworkError> {
        if local_node_id == peer_node_id || link_channel_binding == [0; 32] {
            return Err(NetworkError::AuthDenied);
        }
        let stream_id = Id::generate();
        let mut binding_input = Vec::with_capacity(32 + stream_id.as_str().len());
        binding_input.extend_from_slice(&link_channel_binding);
        binding_input.extend_from_slice(stream_id.as_str().as_bytes());
        let channel_binding = *blake3::hash(&binding_input).as_bytes();
        Ok(Self {
            io,
            local_node_id,
            peer_node_id,
            link_id,
            stream_id,
            link_channel_binding,
            channel_binding,
        })
    }
}

/// Provider for one direct transport implementation.
pub trait LinkProvider: Send + Sync {
    fn connect<'a>(
        &'a self,
        endpoint: &'a ClusterEndpointRecord,
    ) -> DirectLinkFuture<'a, AuthenticatedLink>;

    fn accept(&self) -> DirectLinkFuture<'_, AuthenticatedLink>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    #[tokio::test]
    async fn authenticated_link_opens_independently_bound_streams() {
        let local = node("node_direct_link_local");
        let peer = node("node_direct_link_peer");
        let link = AuthenticatedLink::new(
            local.clone(),
            peer.clone(),
            Id::from_legacy_named("link_direct_authenticated").expect("link id"),
            *blake3::hash(b"direct-link-binding").as_bytes(),
            DirectStreamCapabilities::multiplexed_full_duplex(16),
            || {
                Box::pin(async {
                    let (stream, _peer) = tokio::io::duplex(64);
                    Ok(Box::new(stream) as BoxedNetworkStream)
                })
            },
        )
        .expect("authenticated link");

        let first = link.open_stream().await.expect("first stream");
        let second = link.open_stream().await.expect("second stream");
        assert_eq!(first.local_node_id, local);
        assert_eq!(first.peer_node_id, peer);
        assert_ne!(first.stream_id, second.stream_id);
        assert_ne!(first.channel_binding, second.channel_binding);
    }

    #[test]
    fn authenticated_link_rejects_self_and_unusable_capabilities() {
        let local = node("node_direct_link_invalid");
        let opener = || {
            Box::pin(async {
                let (stream, _peer) = tokio::io::duplex(64);
                Ok(Box::new(stream) as BoxedNetworkStream)
            }) as DirectLinkFuture<'static, BoxedNetworkStream>
        };
        assert!(
            AuthenticatedLink::new(
                local.clone(),
                local.clone(),
                Id::generate(),
                [0; 32],
                DirectStreamCapabilities::multiplexed_full_duplex(1),
                opener,
            )
            .is_err()
        );
        assert!(
            AuthenticatedLink::new(
                local,
                node("node_direct_link_invalid_peer"),
                Id::generate(),
                [0; 32],
                DirectStreamCapabilities {
                    full_duplex: false,
                    relay_forwarding: false,
                    confidential: false,
                    max_concurrent_streams: 0,
                },
                opener,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn authenticated_transport_prelude_round_trips_and_leaves_routed_bytes() {
        let metadata = AuthenticatedTransportMetadata::try_new(
            ClusterTransportKind::Iroh,
            "iroh-endpoint-public-key",
            *blake3::hash(b"iroh-tls-exporter").as_bytes(),
            b"module-owned-transport-proof".to_vec(),
            DirectStreamCapabilities::multiplexed_full_duplex(64),
        )
        .expect("transport metadata");
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        let expected = metadata.clone();
        let write = tokio::spawn(async move {
            write_authenticated_transport_prelude(&mut writer, &metadata)
                .await
                .expect("write prelude");
            writer
                .write_all(b"SRT1")
                .await
                .expect("write routed prefix");
        });

        let decoded = read_authenticated_transport_prelude(&mut reader)
            .await
            .expect("read prelude");
        assert_eq!(decoded, expected);
        let mut routed_prefix = [0_u8; 4];
        reader
            .read_exact(&mut routed_prefix)
            .await
            .expect("read routed prefix");
        assert_eq!(&routed_prefix, b"SRT1");
        write.await.expect("writer task");
    }

    #[test]
    fn authenticated_transport_prelude_prefix_is_incremental_and_disjoint() {
        for length in 0..=AUTHENTICATED_TRANSPORT_PRELUDE_PREFIX.len() {
            assert!(authenticated_transport_prelude_prefix_matches(
                &AUTHENTICATED_TRANSPORT_PRELUDE_PREFIX[..length]
            ));
        }
        assert!(!authenticated_transport_prelude_prefix_matches(b"SRT1"));
        assert!(!authenticated_transport_prelude_prefix_matches(b"SOF2"));
    }

    #[test]
    fn authenticated_transport_metadata_rejects_missing_proof_material() {
        assert!(
            AuthenticatedTransportMetadata::try_new(
                ClusterTransportKind::Iroh,
                "",
                [1; 32],
                vec![1],
                DirectStreamCapabilities::multiplexed_full_duplex(1),
            )
            .is_err()
        );
        assert!(
            AuthenticatedTransportMetadata::try_new(
                ClusterTransportKind::Iroh,
                "peer",
                [0; 32],
                vec![1],
                DirectStreamCapabilities::multiplexed_full_duplex(1),
            )
            .is_err()
        );
        assert!(
            AuthenticatedTransportMetadata::try_new(
                ClusterTransportKind::Iroh,
                "peer",
                [1; 32],
                Vec::new(),
                DirectStreamCapabilities::multiplexed_full_duplex(1),
            )
            .is_err()
        );
    }
}
