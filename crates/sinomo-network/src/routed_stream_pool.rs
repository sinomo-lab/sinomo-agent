//! Routing-owned pooling and multiplexing for authenticated routed streams.
//!
//! A physical [`PeerStream`] is an end-to-end authenticated carrier. Calls
//! receive independently cancellable logical `PeerStream`s over that carrier;
//! application protocols never participate in connection reuse.

use crate::owned_chunk::{
    AuthenticatedChunkReader, AuthenticatedChunkStream, AuthenticatedChunkWriter, ChunkAllocator,
    ChunkClass, ChunkLease, ChunkStreamFuture, MUX_RECORD_HEADER_BYTES, OwnedChunk,
    RPC_CONTROL_PAYLOAD_MAX, RPC_RECORD_HEADER_BYTES, ResetReason, SECURE_PLAINTEXT_MAX,
    SECURE_RECORD_HEADER_BYTES,
};
use crate::owned_mux::{
    IncomingLogicalStreamResolver, IncomingLogicalStreamSink, OwnedRoutedStreamAcceptor,
    OwnedRoutedStreamClient, accept_owned_routed_stream_carrier,
    owned_routed_stream_acceptor_and_client,
};
use crate::{
    BoxedNetworkStream, NetworkError, PeerStream, RouteConstraints, RouteTrace, RpcPeerContext,
    RpcPeerIdentity,
};
use ring::rand::{SecureRandom, SystemRandom};
use sinomo_base::Id;
use sinomo_node::NodeId;
use std::collections::BTreeMap;
use std::future::Future;
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::Poll;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

const LOCAL_OBJECT_RPC_CARRIER_PRELUDE_MAGIC: [u8; 4] = *b"SLC1";
const LOCAL_OBJECT_RPC_RECORD_MAGIC: [u8; 4] = *b"SLR1";
const LOCAL_OBJECT_RPC_RECORD_HEADER_BYTES: usize = 8;
const LOCAL_OBJECT_RPC_RECORD_CONTROL_FLAG: u32 = 1 << 31;
const LOCAL_OBJECT_RPC_RECORD_LENGTH_MASK: u32 = !LOCAL_OBJECT_RPC_RECORD_CONTROL_FLAG;
const LOCAL_OBJECT_RPC_CARRIER_MAX_SESSION_ID_BYTES: usize = 128;

fn mux_io_error(operation: &str, error: std::io::Error) -> NetworkError {
    NetworkError::Io {
        operation: operation.to_string(),
        detail: error.to_string(),
    }
}

/// Identifies a loopback-only multiplexed carrier before any logical RPC is
/// accepted. Each logical stream carries its own service request; the local
/// carrier context grants only local service authority.
pub fn local_object_rpc_carrier_prelude_prefix_matches(prefix: &[u8]) -> bool {
    prefix.len() <= LOCAL_OBJECT_RPC_CARRIER_PRELUDE_MAGIC.len()
        && LOCAL_OBJECT_RPC_CARRIER_PRELUDE_MAGIC.starts_with(prefix)
}

pub async fn write_local_object_rpc_carrier_prelude<S>(
    stream: &mut S,
) -> Result<RpcPeerContext, NetworkError>
where
    S: AsyncWrite + Unpin + ?Sized,
{
    let session_id = Id::generate();
    let session_id_bytes = session_id.as_str().as_bytes();
    if session_id_bytes.len() > LOCAL_OBJECT_RPC_CARRIER_MAX_SESSION_ID_BYTES {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: session_id_bytes.len(),
            max_bytes: LOCAL_OBJECT_RPC_CARRIER_MAX_SESSION_ID_BYTES,
        });
    }
    let session_id_len =
        u16::try_from(session_id_bytes.len()).map_err(|_| NetworkError::FrameTooLarge {
            actual_bytes: session_id_bytes.len(),
            max_bytes: LOCAL_OBJECT_RPC_CARRIER_MAX_SESSION_ID_BYTES,
        })?;
    let mut channel_binding = [0_u8; 32];
    SystemRandom::new()
        .fill(&mut channel_binding)
        .map_err(|_| NetworkError::Io {
            operation: "generate local RPC carrier binding".to_string(),
            detail: "system random failed".to_string(),
        })?;
    stream
        .write_all(&LOCAL_OBJECT_RPC_CARRIER_PRELUDE_MAGIC)
        .await
        .map_err(|error| mux_io_error("write local RPC carrier prelude", error))?;
    stream
        .write_all(&channel_binding)
        .await
        .map_err(|error| mux_io_error("write local RPC carrier binding", error))?;
    stream
        .write_all(&session_id_len.to_be_bytes())
        .await
        .map_err(|error| mux_io_error("write local RPC carrier session length", error))?;
    stream
        .write_all(session_id_bytes)
        .await
        .map_err(|error| mux_io_error("write local RPC carrier session", error))?;
    stream
        .flush()
        .await
        .map_err(|error| mux_io_error("flush local RPC carrier prelude", error))?;
    Ok(RpcPeerContext::unauthenticated_local(
        session_id,
        channel_binding,
    ))
}

pub async fn read_local_object_rpc_carrier_prelude<S>(
    stream: &mut S,
) -> Result<RpcPeerContext, NetworkError>
where
    S: AsyncRead + Unpin + ?Sized,
{
    let mut magic = [0_u8; 4];
    stream
        .read_exact(&mut magic)
        .await
        .map_err(|error| mux_io_error("read local RPC carrier prelude", error))?;
    if magic != LOCAL_OBJECT_RPC_CARRIER_PRELUDE_MAGIC {
        return Err(NetworkError::AuthDenied);
    }
    let mut channel_binding = [0_u8; 32];
    stream
        .read_exact(&mut channel_binding)
        .await
        .map_err(|error| mux_io_error("read local RPC carrier binding", error))?;
    if channel_binding == [0; 32] {
        return Err(NetworkError::AuthDenied);
    }
    let session_id_len = stream
        .read_u16()
        .await
        .map_err(|error| mux_io_error("read local RPC carrier session length", error))?
        as usize;
    if session_id_len == 0 || session_id_len > LOCAL_OBJECT_RPC_CARRIER_MAX_SESSION_ID_BYTES {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: session_id_len,
            max_bytes: LOCAL_OBJECT_RPC_CARRIER_MAX_SESSION_ID_BYTES,
        });
    }
    let mut session_id = vec![0_u8; session_id_len];
    stream
        .read_exact(&mut session_id)
        .await
        .map_err(|error| mux_io_error("read local RPC carrier session", error))?;
    let session_id = std::str::from_utf8(&session_id)
        .ok()
        .and_then(|value| Id::from_str(value).ok())
        .ok_or(NetworkError::AuthDenied)?;
    Ok(RpcPeerContext::unauthenticated_local(
        session_id,
        channel_binding,
    ))
}

fn local_object_rpc_carrier_node_ids() -> (NodeId, NodeId) {
    (
        NodeId::derived("sinomo.network.local-rpc-carrier", b"client"),
        NodeId::derived("sinomo.network.local-rpc-carrier", b"daemon"),
    )
}

fn local_object_rpc_carrier_route(context: &RpcPeerContext, client_side: bool) -> RouteTrace {
    let (client_node_id, daemon_node_id) = local_object_rpc_carrier_node_ids();
    let endpoint_id = Id::derived(
        "sinomo.network.local-rpc-carrier.endpoint",
        context.session_id.as_str().as_bytes(),
    );
    RouteTrace {
        nodes: if client_side {
            vec![client_node_id, daemon_node_id]
        } else {
            vec![daemon_node_id, client_node_id]
        },
        endpoint_ids: vec![endpoint_id],
    }
}

fn local_object_rpc_peer_carrier(
    io: BoxedNetworkStream,
    context: &RpcPeerContext,
    client_side: bool,
) -> Result<PeerStream, NetworkError> {
    if !matches!(context.identity, RpcPeerIdentity::UnauthenticatedLocal) {
        return Err(NetworkError::AuthDenied);
    }
    let (client_node_id, daemon_node_id) = local_object_rpc_carrier_node_ids();
    let (local_node_id, peer_node_id) = if client_side {
        (client_node_id, daemon_node_id)
    } else {
        (daemon_node_id, client_node_id)
    };
    PeerStream::new_owned(
        local_object_rpc_chunk_stream(io)?,
        local_node_id,
        peer_node_id,
        context.session_id.clone(),
        context.channel_binding,
        local_object_rpc_carrier_route(context, client_side),
    )
}

type LocalRecordReadHalf = Box<dyn AsyncRead + Unpin + Send>;
type LocalRecordWriteHalf = Box<dyn AsyncWrite + Unpin + Send>;

fn split_local_object_rpc_io(
    io: BoxedNetworkStream,
) -> (LocalRecordReadHalf, LocalRecordWriteHalf) {
    if io.as_any().is::<tokio::net::TcpStream>() {
        let io = io
            .into_any()
            .downcast::<tokio::net::TcpStream>()
            .expect("checked local object-RPC TCP stream");
        let (reader, writer) = (*io).into_split();
        return (Box::new(reader), Box::new(writer));
    }
    let (reader, writer) = tokio::io::split(io);
    (Box::new(reader), Box::new(writer))
}

struct LocalCarrierChunkReader {
    reader: LocalRecordReadHalf,
    allocator: ChunkAllocator,
    closed: bool,
}

impl LocalCarrierChunkReader {
    async fn recv_record(&mut self) -> Result<Option<ChunkLease>, NetworkError> {
        if self.closed {
            return Ok(None);
        }
        let mut header = [0_u8; LOCAL_OBJECT_RPC_RECORD_HEADER_BYTES];
        self.reader
            .read_exact(&mut header)
            .await
            .map_err(|error| mux_io_error("read local object-RPC record header", error))?;
        if header[..4] != LOCAL_OBJECT_RPC_RECORD_MAGIC {
            return Err(NetworkError::Serialization {
                detail: "invalid local object-RPC owned-record prefix".to_string(),
            });
        }
        let encoded_length =
            u32::from_be_bytes(header[4..].try_into().expect("local record length"));
        let class = if encoded_length & LOCAL_OBJECT_RPC_RECORD_CONTROL_FLAG == 0 {
            ChunkClass::Data
        } else {
            ChunkClass::Control
        };
        let payload_bytes = usize::try_from(encoded_length & LOCAL_OBJECT_RPC_RECORD_LENGTH_MASK)
            .expect("u32 length fits usize");
        let class_max = match class {
            ChunkClass::Data => SECURE_PLAINTEXT_MAX,
            ChunkClass::Control => {
                MUX_RECORD_HEADER_BYTES + RPC_RECORD_HEADER_BYTES + RPC_CONTROL_PAYLOAD_MAX
            }
        };
        if payload_bytes > class_max {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: payload_bytes,
                max_bytes: class_max,
            });
        }
        if payload_bytes == 0 {
            if class != ChunkClass::Control {
                return Err(NetworkError::Serialization {
                    detail: "local object-RPC final record used the data slab class".to_string(),
                });
            }
            self.closed = true;
            return Ok(None);
        }
        let mut chunk = match class {
            ChunkClass::Data => self.allocator.allocate_data(0).await?,
            ChunkClass::Control => self.allocator.allocate_control(0).await?,
        };
        chunk.set_range(
            SECURE_RECORD_HEADER_BYTES,
            SECURE_RECORD_HEADER_BYTES.saturating_add(payload_bytes),
        )?;
        self.reader
            .read_exact(chunk.as_mut_slice())
            .await
            .map_err(|error| mux_io_error("read local object-RPC owned record", error))?;
        Ok(Some(chunk.into_lease()))
    }
}

impl AuthenticatedChunkReader for LocalCarrierChunkReader {
    fn allocator(&self) -> &ChunkAllocator {
        &self.allocator
    }

    fn max_payload_bytes(&self) -> usize {
        SECURE_PLAINTEXT_MAX
    }

    fn recv_chunk(&mut self) -> ChunkStreamFuture<'_, Option<ChunkLease>> {
        Box::pin(self.recv_record())
    }
}

struct LocalCarrierChunkWriter {
    writer: LocalRecordWriteHalf,
    allocator: ChunkAllocator,
    closed: bool,
}

impl LocalCarrierChunkWriter {
    async fn send_record(
        &mut self,
        mut chunk: OwnedChunk,
        final_record: bool,
    ) -> Result<(), NetworkError> {
        if self.closed {
            return Err(NetworkError::Io {
                operation: "write local object-RPC owned record".to_string(),
                detail: "write direction is closed".to_string(),
            });
        }
        let payload_bytes = chunk.len();
        if payload_bytes > SECURE_PLAINTEXT_MAX {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: payload_bytes,
                max_bytes: SECURE_PLAINTEXT_MAX,
            });
        }
        if final_record && payload_bytes != 0 {
            return Err(NetworkError::Serialization {
                detail: "local object-RPC final record carried payload".to_string(),
            });
        }
        let payload_u32 =
            u32::try_from(payload_bytes).map_err(|_| NetworkError::FrameTooLarge {
                actual_bytes: payload_bytes,
                max_bytes: SECURE_PLAINTEXT_MAX,
            })?;
        let class_flag = match chunk.class() {
            ChunkClass::Data => 0,
            ChunkClass::Control => LOCAL_OBJECT_RPC_RECORD_CONTROL_FLAG,
        };
        let mut header = [0_u8; LOCAL_OBJECT_RPC_RECORD_HEADER_BYTES];
        header[..4].copy_from_slice(&LOCAL_OBJECT_RPC_RECORD_MAGIC);
        header[4..].copy_from_slice(&(payload_u32 | class_flag).to_be_bytes());
        chunk.prepend(&header)?;
        self.writer
            .write_all(chunk.as_slice())
            .await
            .map_err(|error| mux_io_error("write local object-RPC owned record", error))?;
        if final_record {
            self.closed = true;
            self.writer
                .shutdown()
                .await
                .map_err(|error| mux_io_error("half-close local object-RPC carrier", error))?;
        }
        Ok(())
    }
}

impl AuthenticatedChunkWriter for LocalCarrierChunkWriter {
    fn max_payload_bytes(&self) -> usize {
        SECURE_PLAINTEXT_MAX
    }

    fn allocator(&self) -> &ChunkAllocator {
        &self.allocator
    }

    fn send_chunk(&mut self, chunk: OwnedChunk) -> ChunkStreamFuture<'_, ()> {
        Box::pin(self.send_record(chunk, false))
    }

    fn finish(&mut self) -> ChunkStreamFuture<'_, ()> {
        Box::pin(async move {
            if self.closed {
                return Ok(());
            }
            let final_record = self.allocator.allocate_control(0).await?;
            self.send_record(final_record, true).await
        })
    }

    fn reset(&mut self, reason: ResetReason) -> ChunkStreamFuture<'_, ()> {
        Box::pin(async move {
            self.closed = true;
            self.writer
                .shutdown()
                .await
                .map_err(|error| NetworkError::Io {
                    operation: format!("reset local object-RPC carrier: {}", reason.detail),
                    detail: error.to_string(),
                })
        })
    }
}

fn local_object_rpc_chunk_stream(
    io: BoxedNetworkStream,
) -> Result<AuthenticatedChunkStream, NetworkError> {
    let allocator = ChunkAllocator::default();
    let (reader, writer) = split_local_object_rpc_io(io);
    AuthenticatedChunkStream::new(
        Box::new(LocalCarrierChunkReader {
            reader,
            allocator: allocator.clone(),
            closed: false,
        }),
        Box::new(LocalCarrierChunkWriter {
            writer,
            allocator,
            closed: false,
        }),
    )
}

type PooledCarrier = OwnedRoutedStreamClient;

struct RoutedStreamIncomingRegistration {
    token: Id,
    affinity: crate::DestinationProcessAffinity,
    sender: mpsc::Sender<Result<PeerStream, NetworkError>>,
}

static ROUTED_STREAM_INCOMING: OnceLock<Mutex<BTreeMap<NodeId, RoutedStreamIncomingRegistration>>> =
    OnceLock::new();

fn routed_stream_incoming_registry()
-> &'static Mutex<BTreeMap<NodeId, RoutedStreamIncomingRegistration>> {
    ROUTED_STREAM_INCOMING.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn registered_routed_stream_incoming_sender(
    local_node_id: &NodeId,
    token: Option<&Id>,
) -> Option<mpsc::Sender<Result<PeerStream, NetworkError>>> {
    routed_stream_incoming_registry()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(local_node_id)
        .filter(|registration| {
            Some(&registration.token) == token && !registration.sender.is_closed()
        })
        .map(|registration| registration.sender.clone())
}

pub(crate) fn registered_routed_stream_incoming_token(local_node_id: &NodeId) -> Option<Id> {
    routed_stream_incoming_registry()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(local_node_id)
        .filter(|registration| !registration.sender.is_closed())
        .map(|registration| registration.token.clone())
}

pub(crate) fn registered_routed_stream_incoming_affinity(
    local_node_id: &NodeId,
) -> Option<crate::DestinationProcessAffinity> {
    routed_stream_incoming_registry()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(local_node_id)
        .filter(|registration| !registration.sender.is_closed())
        .map(|registration| registration.affinity)
}

/// Process-local delivery boundary for logical streams opened by a routed peer.
///
/// A daemon registers its local node once, then handles these streams through
/// the same authenticated service-RPC path as streams accepted on a newly
/// established carrier. Keeping dispatch outside the routing crate preserves
/// the separation between node routing and object RPC.
pub struct RoutedStreamIncoming {
    local_node_id: NodeId,
    token: Id,
    incoming: mpsc::Receiver<Result<PeerStream, NetworkError>>,
}

impl RoutedStreamIncoming {
    pub async fn accept(&mut self) -> Result<Option<PeerStream>, NetworkError> {
        match self.incoming.recv().await {
            Some(Ok(stream)) => Ok(Some(stream)),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }
}

impl Drop for RoutedStreamIncoming {
    fn drop(&mut self) {
        let mut registry = routed_stream_incoming_registry()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if registry
            .get(&self.local_node_id)
            .is_some_and(|registration| registration.token == self.token)
        {
            registry.remove(&self.local_node_id);
        }
    }
}

/// Registers the current process to receive peer-opened logical streams for
/// `local_node_id`. Only one live service dispatcher may own a node identity in
/// a process.
pub fn register_routed_stream_incoming(
    local_node_id: NodeId,
) -> Result<RoutedStreamIncoming, NetworkError> {
    register_routed_stream_incoming_with_process_affinity(
        local_node_id,
        crate::DestinationProcessAffinity::Any,
    )
}

/// Registers the reverse service dispatcher and its process scope. A CLI that
/// only initiates requests has no registration and advertises no reverse route.
pub fn register_routed_stream_incoming_with_process_affinity(
    local_node_id: NodeId,
    affinity: crate::DestinationProcessAffinity,
) -> Result<RoutedStreamIncoming, NetworkError> {
    let token = Id::generate();
    let (sender, incoming) = mpsc::channel(32);
    let mut registry = routed_stream_incoming_registry()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if registry
        .get(&local_node_id)
        .is_some_and(|registration| !registration.sender.is_closed())
    {
        return Err(NetworkError::Io {
            operation: "register routed logical stream service dispatcher".to_string(),
            detail: format!(
                "node {} already has a live routed stream dispatcher",
                local_node_id
            ),
        });
    }
    registry.insert(
        local_node_id.clone(),
        RoutedStreamIncomingRegistration {
            token: token.clone(),
            affinity,
            sender,
        },
    );
    Ok(RoutedStreamIncoming {
        local_node_id,
        token,
        incoming,
    })
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PoolKey {
    scope: String,
    // Joining, replacing or dropping the local dispatcher requires a new
    // handshake so the peer cannot retain an obsolete reverse role.
    local_dispatcher: Option<Id>,
    /// Process-local compatibility fence for route realization inputs that do
    /// not belong to the distributed directory generation (currently the
    /// installed network-module registry). Unlike `directory_generation`, this
    /// is part of the relationship identity: a carrier opened through an old
    /// module implementation must not be adopted by the replacement registry.
    route_compatibility_generation: u64,
    directory_generation: u64,
    local_node_id: NodeId,
    peer_node_id: NodeId,
    protection: crate::RoutedSessionProtection,
    require_full_duplex: bool,
    require_confidentiality: bool,
    require_relay_forwarding: bool,
    max_hops: usize,
    destination_process_affinity: crate::DestinationProcessAffinity,
}

impl PoolKey {
    fn new(
        scope: String,
        route_compatibility_generation: u64,
        directory_generation: u64,
        constraints: RouteConstraints,
        destination_process_affinity: crate::DestinationProcessAffinity,
        local_node_id: NodeId,
        peer_node_id: NodeId,
        protection: crate::RoutedSessionProtection,
    ) -> Self {
        Self {
            local_dispatcher: registered_routed_stream_incoming_token(&local_node_id),
            scope,
            route_compatibility_generation,
            directory_generation,
            local_node_id,
            peer_node_id,
            protection,
            require_full_duplex: constraints.require_full_duplex,
            require_confidentiality: constraints.require_confidentiality,
            require_relay_forwarding: constraints.require_relay_forwarding,
            max_hops: constraints.max_hops,
            destination_process_affinity,
        }
    }

    fn same_relationship(&self, other: &Self) -> bool {
        self.route_compatibility_generation == other.route_compatibility_generation
            && self.local_dispatcher == other.local_dispatcher
            && self.same_base_relationship(other)
    }

    fn same_base_relationship(&self, other: &Self) -> bool {
        self.scope == other.scope
            && self.local_node_id == other.local_node_id
            && self.peer_node_id == other.peer_node_id
            && self.protection == other.protection
            && self.require_full_duplex == other.require_full_duplex
            && self.require_confidentiality == other.require_confidentiality
            && self.require_relay_forwarding == other.require_relay_forwarding
            && self.max_hops == other.max_hops
            && self.destination_process_affinity == other.destination_process_affinity
    }
}

// These process-wide registries are never held across an await. A Tokio mutex
// would couple their admission to the caller's runtime clock: a paused-time
// runtime can auto-advance its connect timeout while a different runtime owns
// the registry for a few wall-clock microseconds. Keep the tiny map critical
// sections runtime-neutral; the per-relationship connect mutex below remains
// asynchronous because it intentionally spans carrier establishment.
static ROUTED_STREAM_POOL: OnceLock<Mutex<BTreeMap<PoolKey, Arc<PooledCarrier>>>> = OnceLock::new();
#[derive(Default)]
struct RoutedStreamConnectAdmission {
    owner: tokio::sync::Mutex<()>,
    // A wake carries no carrier or authority. Callers revalidate the existing
    // pool after an independently established carrier is published.
    published: tokio::sync::Notify,
}
static ROUTED_STREAM_CONNECT_LOCKS: OnceLock<
    Mutex<BTreeMap<PoolKey, Arc<RoutedStreamConnectAdmission>>>,
> = OnceLock::new();

fn routed_stream_pool() -> &'static Mutex<BTreeMap<PoolKey, Arc<PooledCarrier>>> {
    ROUTED_STREAM_POOL.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn routed_stream_connect_locks()
-> &'static Mutex<BTreeMap<PoolKey, Arc<RoutedStreamConnectAdmission>>> {
    ROUTED_STREAM_CONNECT_LOCKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn lock_routed_stream_pool() -> std::sync::MutexGuard<'static, BTreeMap<PoolKey, Arc<PooledCarrier>>>
{
    routed_stream_pool()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn lock_routed_stream_connect_locks()
-> std::sync::MutexGuard<'static, BTreeMap<PoolKey, Arc<RoutedStreamConnectAdmission>>> {
    routed_stream_connect_locks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Reports whether this process currently owns a live routing carrier for the
/// exact node relationship in `scope`.
pub fn pooled_routed_relationship_is_active(
    scope: &str,
    local_node_id: &NodeId,
    peer_node_id: &NodeId,
) -> Option<bool> {
    let Some(pool) = ROUTED_STREAM_POOL.get() else {
        return Some(false);
    };
    let Ok(pool) = pool.try_lock() else {
        return None;
    };
    Some(pool.iter().any(|(key, client)| {
        key.scope == scope
            && &key.local_node_id == local_node_id
            && &key.peer_node_id == peer_node_id
            && !client.is_closed()
    }))
}

fn reusable_pooled_client(
    pool: &mut BTreeMap<PoolKey, Arc<PooledCarrier>>,
    key: &PoolKey,
) -> Option<Arc<PooledCarrier>> {
    pool.retain(|candidate, client| {
        !client.is_closed()
            && (!candidate.same_base_relationship(key)
                || (candidate.local_dispatcher == key.local_dispatcher
                    && candidate.route_compatibility_generation
                        >= key.route_compatibility_generation))
    });
    let client = pool.get(key).cloned().or_else(|| {
        pool.iter()
            .filter(|(candidate, _)| candidate.same_relationship(key))
            .max_by_key(|(candidate, _)| candidate.directory_generation)
            .map(|(_, client)| Arc::clone(client))
    })?;
    pool.insert(key.clone(), Arc::clone(&client));
    pool.retain(|candidate, _| !candidate.same_relationship(key) || candidate == key);
    Some(client)
}

fn current_pooled_client<G>(
    key: &PoolKey,
    current_generation: &G,
) -> Result<Option<Arc<PooledCarrier>>, NetworkError>
where
    G: Fn() -> u64,
{
    let validate = || {
        if registered_routed_stream_incoming_token(&key.local_node_id) != key.local_dispatcher {
            return Err(NetworkError::Directory {
                detail: "local reverse dispatcher changed during carrier establishment".into(),
            });
        }
        let observed = current_generation();
        if observed != key.route_compatibility_generation {
            return Err(stale_route_compatibility_error(
                key.route_compatibility_generation,
                observed,
            ));
        }
        Ok(())
    };
    validate()?;
    let client = {
        let mut pool = lock_routed_stream_pool();
        if let Some(newer) = newer_route_compatibility_generation(&pool, key) {
            return Err(stale_route_compatibility_error(
                key.route_compatibility_generation,
                newer,
            ));
        }
        reusable_pooled_client(&mut pool, key)
    };
    validate()?;
    Ok(client)
}

fn notify_pooled_carrier_publication(key: &PoolKey) {
    // Directory generations can share an already authenticated relationship.
    // Retain/wake only existing active admissions; there is no second registry.
    for (waiting_key, admission) in lock_routed_stream_connect_locks().iter() {
        if waiting_key.same_relationship(key) {
            admission.published.notify_waiters();
        }
    }
}

fn newer_route_compatibility_generation<T>(
    entries: &BTreeMap<PoolKey, T>,
    key: &PoolKey,
) -> Option<u64> {
    entries
        .keys()
        .filter(|candidate| {
            candidate.same_base_relationship(key)
                && candidate.local_dispatcher == key.local_dispatcher
        })
        .map(|candidate| candidate.route_compatibility_generation)
        .filter(|generation| *generation > key.route_compatibility_generation)
        .max()
}

fn stale_route_compatibility_error(expected: u64, observed: u64) -> NetworkError {
    NetworkError::Directory {
        detail: format!(
            "network module routing generation changed while establishing a pooled carrier (expected {expected}, observed {observed})"
        ),
    }
}

/// Opens a logical stream from a routing-owned authenticated carrier.
///
/// `establish` is polled only when no healthy carrier exists. A failed carrier
/// is retired before this function returns an error, but application bytes are
/// never replayed onto a replacement carrier.
pub async fn open_pooled_routed_stream<F>(
    scope: String,
    directory_generation: u64,
    constraints: RouteConstraints,
    local_node_id: NodeId,
    peer_node_id: NodeId,
    timeout: Duration,
    establish: F,
) -> Result<PeerStream, NetworkError>
where
    F: Future<Output = Result<PeerStream, NetworkError>>,
{
    open_pooled_routed_stream_with_protection(
        scope,
        directory_generation,
        constraints,
        local_node_id,
        peer_node_id,
        crate::RoutedSessionProtection::Plaintext,
        timeout,
        establish,
    )
    .await
}

/// Opens a logical stream whose persistent carrier uses the selected
/// routed-session protection. Protection is part of the pool key, so an AEAD
/// call can never reuse a plaintext carrier (or vice versa). The connect
/// timeout covers pool coordination, carrier establishment, and opening the
/// first logical stream; none of those phases has sent application bytes yet.
pub async fn open_pooled_routed_stream_with_protection<F>(
    scope: String,
    directory_generation: u64,
    constraints: RouteConstraints,
    local_node_id: NodeId,
    peer_node_id: NodeId,
    protection: crate::RoutedSessionProtection,
    timeout: Duration,
    establish: F,
) -> Result<PeerStream, NetworkError>
where
    F: Future<Output = Result<PeerStream, NetworkError>>,
{
    open_pooled_routed_stream_with_protection_and_destination_process_affinity(
        scope,
        directory_generation,
        constraints,
        crate::DestinationProcessAffinity::Any,
        local_node_id,
        peer_node_id,
        protection,
        timeout,
        establish,
    )
    .await
}

pub(crate) async fn open_pooled_routed_stream_with_protection_and_destination_process_affinity<F>(
    scope: String,
    directory_generation: u64,
    constraints: RouteConstraints,
    destination_process_affinity: crate::DestinationProcessAffinity,
    local_node_id: NodeId,
    peer_node_id: NodeId,
    protection: crate::RoutedSessionProtection,
    timeout: Duration,
    establish: F,
) -> Result<PeerStream, NetworkError>
where
    F: Future<Output = Result<PeerStream, NetworkError>>,
{
    let route_compatibility_generation = crate::network_module_routing_generation();
    open_pooled_routed_stream_with_protection_destination_affinity_and_route_compatibility(
        scope,
        route_compatibility_generation,
        directory_generation,
        constraints,
        destination_process_affinity,
        local_node_id,
        peer_node_id,
        protection,
        timeout,
        establish,
    )
    .await
}

pub(crate) async fn open_pooled_routed_stream_with_protection_destination_affinity_and_route_compatibility<
    F,
>(
    scope: String,
    route_compatibility_generation: u64,
    directory_generation: u64,
    constraints: RouteConstraints,
    destination_process_affinity: crate::DestinationProcessAffinity,
    local_node_id: NodeId,
    peer_node_id: NodeId,
    protection: crate::RoutedSessionProtection,
    timeout: Duration,
    establish: F,
) -> Result<PeerStream, NetworkError>
where
    F: Future<Output = Result<PeerStream, NetworkError>>,
{
    open_pooled_routed_stream_with_route_compatibility_source(
        scope,
        route_compatibility_generation,
        directory_generation,
        constraints,
        destination_process_affinity,
        local_node_id,
        peer_node_id,
        protection,
        timeout,
        establish,
        crate::network_module_routing_generation,
    )
    .await
}

async fn open_pooled_routed_stream_with_route_compatibility_source<F, G>(
    scope: String,
    route_compatibility_generation: u64,
    directory_generation: u64,
    constraints: RouteConstraints,
    destination_process_affinity: crate::DestinationProcessAffinity,
    local_node_id: NodeId,
    peer_node_id: NodeId,
    protection: crate::RoutedSessionProtection,
    timeout: Duration,
    establish: F,
    current_route_compatibility_generation: G,
) -> Result<PeerStream, NetworkError>
where
    F: Future<Output = Result<PeerStream, NetworkError>>,
    G: Fn() -> u64,
{
    // Observation only: these fields identify the exact attempted relationship,
    // not a retained carrier, admission permit, or delivery acknowledgement.
    let observation = tracing::trace_span!(target: "sinomo.perf", "network.carrier.open",
        stage = "network.carrier.open", scope = %scope, local_node_id = %local_node_id,
        peer_node_id = %peer_node_id, route_compatibility_generation, directory_generation,
        affinity = ?destination_process_affinity, protection = ?protection);
    let deadline = tokio::time::Instant::now() + timeout;
    let deadline_error = || {
        NetworkError::timeout(
            "open pooled routed carrier",
            crate::NetworkTimeoutPhase::Connect,
            timeout,
            crate::NetworkDeliveryState::DefinitelyNotSent,
        )
    };
    let mut phase = "pool-lookup";
    let open = async {
        let key = PoolKey::new(
            scope,
            route_compatibility_generation,
            directory_generation,
            constraints,
            destination_process_affinity,
            local_node_id,
            peer_node_id,
            protection,
        );
        let existing = current_pooled_client(&key, &current_route_compatibility_generation)?;
        if let Some(client) = existing {
            let observed_generation = current_route_compatibility_generation();
            if observed_generation != key.route_compatibility_generation {
                return Err(stale_route_compatibility_error(
                    key.route_compatibility_generation,
                    observed_generation,
                ));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(deadline_error());
            }
            match client.open() {
                Ok(stream) => return Ok(stream),
                Err(_) => {
                    let mut pool = lock_routed_stream_pool();
                    if pool
                        .get(&key)
                        .is_some_and(|current| Arc::ptr_eq(current, &client))
                    {
                        pool.remove(&key);
                    }
                }
            }
        }

        phase = "connect-registry";
        let connect_lock = {
            let mut locks = lock_routed_stream_connect_locks();
            if let Some(newer_generation) = newer_route_compatibility_generation(&locks, &key) {
                return Err(stale_route_compatibility_error(
                    key.route_compatibility_generation,
                    newer_generation,
                ));
            }
            locks.retain(|lock_key, admission| {
                if !lock_key.same_base_relationship(&key) {
                    return true;
                }
                match lock_key
                    .route_compatibility_generation
                    .cmp(&key.route_compatibility_generation)
                {
                    std::cmp::Ordering::Less => false,
                    std::cmp::Ordering::Greater => true,
                    std::cmp::Ordering::Equal => {
                        lock_key == &key || Arc::strong_count(admission) > 1
                    }
                }
            });
            Arc::clone(
                locks
                    .entry(key.clone())
                    .or_insert_with(|| Arc::new(RoutedStreamConnectAdmission::default())),
            )
        };
        let client = {
            let ready = async {
                loop {
                    let published = connect_lock.published.notified();
                    tokio::pin!(published);
                    // Register before checking the pool so publication cannot fall
                    // between an empty lookup and parking this existing caller.
                    published.as_mut().enable();
                    let current =
                        current_pooled_client(&key, &current_route_compatibility_generation)?;
                    if tokio::time::Instant::now() >= deadline {
                        return Err(deadline_error());
                    }
                    if let Some(client) = current {
                        return Ok::<_, NetworkError>(client);
                    }
                    published.await;
                }
            };
            let connect = async {
                phase = "connect-owner-wait";
                tracing::trace!(target: "sinomo.perf", parent: &observation, phase, "routed carrier open boundary");
                let connect_guard = connect_lock.owner.lock().await;
                phase = "connect-owner-acquired";
                tracing::trace!(target: "sinomo.perf", parent: &observation, phase, "routed carrier open boundary");
                let observed_generation = current_route_compatibility_generation();
                if observed_generation != key.route_compatibility_generation {
                    return Err(stale_route_compatibility_error(
                        key.route_compatibility_generation,
                        observed_generation,
                    ));
                }
                let existing =
                    current_pooled_client(&key, &current_route_compatibility_generation)?;
                let client = match existing {
                    Some(client) => client,
                    None => {
                        phase = "carrier-establish";
                        tracing::trace!(target: "sinomo.perf", parent: &observation, phase, "routed carrier open boundary");
                        let established = establish.await;
                        // Publication can race the final poll of a dial, including a
                        // failing dial. Prefer the independently usable current owner.
                        if let Some(client) =
                            current_pooled_client(&key, &current_route_compatibility_generation)?
                        {
                            client
                        } else {
                            let carrier = established?;
                            phase = "carrier-validate";
                            if registered_routed_stream_incoming_token(&key.local_node_id)
                                != key.local_dispatcher
                            {
                                return Err(NetworkError::Directory {
                        detail: "local reverse dispatcher changed during carrier establishment"
                            .into(),
                    });
                            }
                            if carrier.local_node_id != key.local_node_id
                                || carrier.peer_node_id != key.peer_node_id
                                || carrier.protection != key.protection
                            {
                                return Err(NetworkError::AuthDenied);
                            }
                            let observed_generation = current_route_compatibility_generation();
                            if observed_generation != key.route_compatibility_generation {
                                return Err(stale_route_compatibility_error(
                                    key.route_compatibility_generation,
                                    observed_generation,
                                ));
                            }
                            let incoming_node_id = key.local_node_id.clone();
                            let incoming_token = key.local_dispatcher.clone();
                            let incoming: IncomingLogicalStreamResolver = Arc::new(move || {
                                registered_routed_stream_incoming_sender(
                                    &incoming_node_id,
                                    incoming_token.as_ref(),
                                )
                            });
                            let client = Arc::new(PooledCarrier::new_with_incoming(
                                carrier,
                                1,
                                IncomingLogicalStreamSink::Resolve(incoming),
                            )?);
                            let client = {
                                let mut pool = lock_routed_stream_pool();
                                pool.retain(|candidate, pooled| {
                                    !pooled.is_closed()
                                        && (!candidate.same_base_relationship(&key)
                                            || (candidate.local_dispatcher == key.local_dispatcher
                                                && candidate.route_compatibility_generation
                                                    >= key.route_compatibility_generation))
                                });
                                if let Some(newer_generation) =
                                    newer_route_compatibility_generation(&pool, &key)
                                {
                                    return Err(stale_route_compatibility_error(
                                        key.route_compatibility_generation,
                                        newer_generation,
                                    ));
                                }
                                // A publication on another runtime may have won since the
                                // last lookup. An unused late dial must not replace it.
                                if let Some(published) = reusable_pooled_client(&mut pool, &key) {
                                    published
                                } else {
                                    pool.insert(key.clone(), Arc::clone(&client));
                                    client
                                }
                            };
                            let observed_generation = current_route_compatibility_generation();
                            if observed_generation != key.route_compatibility_generation {
                                let mut pool = lock_routed_stream_pool();
                                if pool
                                    .get(&key)
                                    .is_some_and(|current| Arc::ptr_eq(current, &client))
                                {
                                    pool.remove(&key);
                                }
                                return Err(stale_route_compatibility_error(
                                    key.route_compatibility_generation,
                                    observed_generation,
                                ));
                            }
                            notify_pooled_carrier_publication(&key);
                            client
                        }
                    }
                };
                drop(connect_guard);
                phase = "connect-owner-released";
                tracing::trace!(target: "sinomo.perf", parent: &observation, phase, "routed carrier open boundary");
                Ok::<_, NetworkError>(client)
            };
            tokio::select! {
                biased;
                client = ready => client?,
                client = connect => client?,
            }
        }; // Drops only this caller's unused dial/lock future before logical open.
        if tokio::time::Instant::now() >= deadline {
            return Err(deadline_error());
        }
        match client.open() {
            Ok(stream) => Ok(stream),
            Err(error) => {
                let mut pool = lock_routed_stream_pool();
                if pool
                    .get(&key)
                    .is_some_and(|current| Arc::ptr_eq(current, &client))
                {
                    pool.remove(&key);
                }
                Err(error)
            }
        }
    };

    match tokio::time::timeout_at(
        deadline,
        tracing::Instrument::instrument(open, observation.clone()),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            // The timed-out future (including any connect guard) is already
            // dropped. A waiter never becomes an owner merely by timing out.
            tracing::trace!(target: "sinomo.perf", parent: &observation,
                phase, outcome = "timeout", "routed carrier open boundary");
            Err(NetworkError::timeout(
                "open pooled routed carrier",
                crate::NetworkTimeoutPhase::Connect,
                timeout,
                crate::NetworkDeliveryState::DefinitelyNotSent,
            ))
        }
    }
}

/// Opens one independently authenticated loopback RPC stream over a persistent
/// local carrier. `establish` runs only when the address has no healthy
/// carrier and must return a stream after exchanging the `SLC1` prelude.
pub async fn open_pooled_local_object_rpc_stream<F>(
    scope: String,
    timeout: Duration,
    establish: F,
) -> Result<PeerStream, NetworkError>
where
    F: Future<Output = Result<(BoxedNetworkStream, RpcPeerContext), NetworkError>>,
{
    let (local_node_id, peer_node_id) = local_object_rpc_carrier_node_ids();
    let carrier = async move {
        let (io, context) = establish.await?;
        local_object_rpc_peer_carrier(io, &context, true)
    };
    // This loopback carrier never consults routing or a network module. Keep it
    // outside the process-local module compatibility fence so a transport
    // reload cannot churn unrelated local RPC sessions.
    open_pooled_routed_stream_with_route_compatibility_source(
        format!("local-object-rpc:{scope}"),
        0,
        0,
        RouteConstraints::default(),
        crate::DestinationProcessAffinity::Any,
        local_node_id,
        peer_node_id,
        crate::RoutedSessionProtection::Plaintext,
        timeout,
        carrier,
        || 0,
    )
    .await
}

/// Retires every pooled routed carrier in one cluster/process scope.
///
/// Daemon shutdown and tests use this explicit lifecycle boundary. Directory
/// generation changes are retired automatically by `open_pooled_routed_stream`.
pub async fn retire_pooled_routed_streams(scope: &str) -> usize {
    let mut pool = lock_routed_stream_pool();
    let before = pool.len();
    pool.retain(|key, _| key.scope != scope);
    let retired = before.saturating_sub(pool.len());
    drop(pool);
    lock_routed_stream_connect_locks().retain(|key, _| key.scope != scope);
    retired
}

/// Server side of one accepted routing-owned carrier.
pub struct RoutedStreamAcceptor {
    inner: Result<OwnedRoutedStreamAcceptor, Option<NetworkError>>,
}

impl RoutedStreamAcceptor {
    pub async fn accept(&mut self) -> Result<Option<PeerStream>, NetworkError> {
        match &mut self.inner {
            Ok(acceptor) => acceptor.accept().await,
            Err(error) => match error.take() {
                Some(error) => Err(error),
                None => Ok(None),
            },
        }
    }
}

fn routed_stream_acceptor_and_client(
    carrier: PeerStream,
) -> Result<(RoutedStreamAcceptor, Arc<PooledCarrier>), NetworkError> {
    let (acceptor, client) = owned_routed_stream_acceptor_and_client(carrier)?;
    Ok((
        RoutedStreamAcceptor {
            inner: Ok(acceptor),
        },
        client,
    ))
}

/// Accepts logical streams and reuses the carrier in reverse only when the
/// initiating process advertised a live reverse dispatcher during establishment.
pub async fn accept_pooled_routed_stream_carrier(
    scope: String,
    directory_generation: u64,
    constraints: RouteConstraints,
    carrier: PeerStream,
) -> Result<RoutedStreamAcceptor, NetworkError> {
    accept_pooled_routed_stream_carrier_for_scopes(
        vec![scope],
        directory_generation,
        constraints,
        carrier,
    )
    .await
}

/// Publishes reverse routes under exact routing scopes. The peer's advertised
/// role determines reuse; the accepting listener's role proves nothing about
/// that peer. Legacy and caller-only carriers remain receive-only here.
pub async fn accept_pooled_routed_stream_carrier_for_scopes(
    scopes: Vec<String>,
    directory_generation: u64,
    constraints: RouteConstraints,
    carrier: PeerStream,
) -> Result<RoutedStreamAcceptor, NetworkError> {
    let generation = crate::network_module_routing_generation();
    accept_pooled_routed_stream_carrier_for_scopes_with_peer_capability(
        scopes,
        generation,
        directory_generation,
        constraints,
        carrier,
        crate::network_module_routing_generation,
    )
    .await
}

async fn accept_pooled_routed_stream_carrier_for_scopes_with_peer_capability<G>(
    scopes: Vec<String>,
    route_compatibility_generation: u64,
    directory_generation: u64,
    constraints: RouteConstraints,
    carrier: PeerStream,
    current_route_compatibility_generation: G,
) -> Result<RoutedStreamAcceptor, NetworkError>
where
    G: Fn() -> u64,
{
    let Some(peer_affinity) = carrier.reverse_process_affinity else {
        return Ok(accept_routed_stream_carrier(carrier));
    };
    accept_pooled_routed_stream_carrier_for_scopes_with_route_compatibility_source(
        scopes,
        route_compatibility_generation,
        directory_generation,
        constraints,
        peer_affinity,
        carrier,
        current_route_compatibility_generation,
    )
    .await
}

#[cfg(test)]
async fn accept_pooled_routed_stream_carrier_with_route_compatibility_source<G>(
    scope: String,
    route_compatibility_generation: u64,
    directory_generation: u64,
    constraints: RouteConstraints,
    destination_process_affinity: crate::DestinationProcessAffinity,
    carrier: PeerStream,
    current_route_compatibility_generation: G,
) -> Result<RoutedStreamAcceptor, NetworkError>
where
    G: Fn() -> u64,
{
    accept_pooled_routed_stream_carrier_for_scopes_with_route_compatibility_source(
        vec![scope],
        route_compatibility_generation,
        directory_generation,
        constraints,
        destination_process_affinity,
        carrier,
        current_route_compatibility_generation,
    )
    .await
}

async fn accept_pooled_routed_stream_carrier_for_scopes_with_route_compatibility_source<G>(
    mut scopes: Vec<String>,
    route_compatibility_generation: u64,
    directory_generation: u64,
    constraints: RouteConstraints,
    destination_process_affinity: crate::DestinationProcessAffinity,
    carrier: PeerStream,
    current_route_compatibility_generation: G,
) -> Result<RoutedStreamAcceptor, NetworkError>
where
    G: Fn() -> u64,
{
    scopes.sort_unstable();
    scopes.dedup();
    if scopes.is_empty() {
        return Err(NetworkError::Directory {
            detail: "accepted routed carrier requires at least one pool scope".to_string(),
        });
    }
    let keys = scopes
        .into_iter()
        .map(|scope| {
            PoolKey::new(
                scope,
                route_compatibility_generation,
                directory_generation,
                constraints,
                destination_process_affinity,
                carrier.local_node_id.clone(),
                carrier.peer_node_id.clone(),
                carrier.protection,
            )
        })
        .collect::<Vec<_>>();
    let observed_generation = current_route_compatibility_generation();
    if observed_generation != route_compatibility_generation {
        return Err(stale_route_compatibility_error(
            route_compatibility_generation,
            observed_generation,
        ));
    }
    let (acceptor, client) = routed_stream_acceptor_and_client(carrier)?;
    let observed_generation = current_route_compatibility_generation();
    if observed_generation != route_compatibility_generation {
        return Err(stale_route_compatibility_error(
            route_compatibility_generation,
            observed_generation,
        ));
    }

    let mut published_keys = Vec::new();
    for key in &keys {
        match publish_accepted_pooled_routed_stream_scope(
            key,
            &client,
            &current_route_compatibility_generation,
        )
        .await
        {
            Ok(true) => published_keys.push(key.clone()),
            Ok(false) => {}
            Err(error) => {
                let mut pool = lock_routed_stream_pool();
                for published_key in &published_keys {
                    if pool
                        .get(published_key)
                        .is_some_and(|current| Arc::ptr_eq(current, &client))
                    {
                        pool.remove(published_key);
                    }
                }
                return Err(error);
            }
        }
    }
    Ok(acceptor)
}

async fn publish_accepted_pooled_routed_stream_scope<G>(
    key: &PoolKey,
    client: &Arc<PooledCarrier>,
    current_route_compatibility_generation: &G,
) -> Result<bool, NetworkError>
where
    G: Fn() -> u64,
{
    {
        let mut locks = lock_routed_stream_connect_locks();
        if let Some(newer_generation) = newer_route_compatibility_generation(&locks, key) {
            return Err(stale_route_compatibility_error(
                key.route_compatibility_generation,
                newer_generation,
            ));
        }
        locks.retain(|lock_key, admission| {
            if !lock_key.same_base_relationship(key) {
                return true;
            }
            match lock_key
                .route_compatibility_generation
                .cmp(&key.route_compatibility_generation)
            {
                std::cmp::Ordering::Less => false,
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Equal => lock_key == key || Arc::strong_count(admission) > 1,
            }
        });
        locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(RoutedStreamConnectAdmission::default()));
    }
    // This carrier is already authenticated and established. Waiting behind an
    // outbound dial can prevent both service dispatch and publication of the
    // very reverse route needed to reach an asymmetric peer. The pool mutex and
    // generation checks below fence publication without that dial's lock.
    if registered_routed_stream_incoming_token(&key.local_node_id) != key.local_dispatcher {
        return Err(NetworkError::Directory {
            detail: "local reverse dispatcher changed before carrier publication".into(),
        });
    }
    let observed_generation = current_route_compatibility_generation();
    if observed_generation != key.route_compatibility_generation {
        return Err(stale_route_compatibility_error(
            key.route_compatibility_generation,
            observed_generation,
        ));
    }
    let mut pool = lock_routed_stream_pool();
    pool.retain(|candidate, pooled| {
        !pooled.is_closed()
            && if !candidate.same_base_relationship(key) {
                true
            } else {
                match candidate
                    .route_compatibility_generation
                    .cmp(&key.route_compatibility_generation)
                {
                    std::cmp::Ordering::Less => false,
                    std::cmp::Ordering::Greater => true,
                    std::cmp::Ordering::Equal => candidate == key,
                }
            }
    });
    if let Some(newer_generation) = newer_route_compatibility_generation(&pool, key) {
        return Err(stale_route_compatibility_error(
            key.route_compatibility_generation,
            newer_generation,
        ));
    }
    let published = if pool.get(key).is_some_and(|pooled| !pooled.is_closed()) {
        false
    } else {
        pool.insert(key.clone(), Arc::clone(client));
        true
    };
    drop(pool);
    let observed_generation = current_route_compatibility_generation();
    if observed_generation != key.route_compatibility_generation {
        if published {
            let mut pool = lock_routed_stream_pool();
            if pool
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, client))
            {
                pool.remove(key);
            }
        }
        return Err(stale_route_compatibility_error(
            key.route_compatibility_generation,
            observed_generation,
        ));
    }
    if published {
        notify_pooled_carrier_publication(key);
    }
    Ok(published)
}

/// Converts one accepted authenticated routed stream into an owned logical-stream
/// acceptor. Byte-oriented callers must enter through an explicit carrier adapter.
pub fn accept_routed_stream_carrier(carrier: PeerStream) -> RoutedStreamAcceptor {
    match routed_stream_acceptor_and_client(carrier) {
        Ok((acceptor, _)) => acceptor,
        Err(error) => RoutedStreamAcceptor {
            inner: Err(Some(error)),
        },
    }
}

/// Serves logical streams through the canonical owned-record carrier.
pub async fn serve_routed_stream_carrier<F, Fut>(
    carrier: PeerStream,
    mut handler: F,
) -> Result<bool, NetworkError>
where
    F: FnMut(PeerStream) -> Fut,
    Fut: Future<Output = Result<bool, NetworkError>> + Send + 'static,
{
    let mut acceptor = accept_owned_routed_stream_carrier(carrier)?;
    let mut handlers = tokio::task::JoinSet::new();
    let mut accepting = true;
    let mut stop = false;
    while accepting || !handlers.is_empty() {
        tokio::select! {
            accepted = acceptor.accept(), if accepting => {
                let Some(stream) = accepted? else {
                    accepting = false;
                    continue;
                };
                let mut future = Box::pin(handler(stream));
                let immediate = std::future::poll_fn(|context| {
                    Poll::Ready(match future.as_mut().poll(context) {
                        Poll::Ready(result) => Some(result),
                        Poll::Pending => None,
                    })
                })
                .await;
                match immediate {
                    Some(Ok(true)) => {
                        stop = true;
                        accepting = false;
                        handlers.abort_all();
                    }
                    Some(Ok(false)) => {}
                    Some(Err(error)) => return Err(error),
                    None => {
                        handlers.spawn(future);
                    }
                }
            }
            joined = handlers.join_next(), if !handlers.is_empty() => {
                match joined {
                    Some(Ok(Ok(true))) => {
                        stop = true;
                        accepting = false;
                        handlers.abort_all();
                    }
                    Some(Ok(Ok(false))) => {}
                    Some(Ok(Err(error))) => return Err(error),
                    Some(Err(error)) => {
                        return Err(NetworkError::Io {
                            operation: "join owned routed logical stream handler".to_string(),
                            detail: error.to_string(),
                        });
                    }
                    None => {}
                }
            }
        }
    }
    Ok(stop)
}
pub async fn serve_local_object_rpc_stream_carrier<F, Fut>(
    carrier: BoxedNetworkStream,
    context: &RpcPeerContext,
    handler: F,
) -> Result<bool, NetworkError>
where
    F: FnMut(PeerStream) -> Fut,
    Fut: Future<Output = Result<bool, NetworkError>> + Send + 'static,
{
    serve_routed_stream_carrier(
        local_object_rpc_peer_carrier(carrier, context, false)?,
        handler,
    )
    .await
}

/// Accepts logical RPC streams from a loopback carrier after the daemon has
/// independently checked socket locality and consumed its `SLC1` prelude.
pub fn accept_local_object_rpc_stream_carrier(
    carrier: BoxedNetworkStream,
    context: &RpcPeerContext,
) -> Result<RoutedStreamAcceptor, NetworkError> {
    Ok(accept_routed_stream_carrier(local_object_rpc_peer_carrier(
        carrier, context, false,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::oneshot;

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    pub(super) fn carrier_pair() -> (PeerStream, PeerStream) {
        carrier_pair_for_nodes(node("node_mux_client"), node("node_mux_server"))
    }

    fn carrier_pair_for_nodes(
        client_node: NodeId,
        server_node: NodeId,
    ) -> (PeerStream, PeerStream) {
        let session_id = Id::generate();
        let binding = *blake3::hash(session_id.as_str().as_bytes()).as_bytes();
        let (client, server) = tokio::io::duplex(256 * 1024);
        let client_route = RouteTrace {
            nodes: vec![client_node.clone(), server_node.clone()],
            endpoint_ids: vec![Id::generate()],
        };
        let server_route = RouteTrace {
            nodes: vec![server_node.clone(), client_node.clone()],
            endpoint_ids: client_route.endpoint_ids.clone(),
        };
        (
            PeerStream::new_owned(
                local_object_rpc_chunk_stream(Box::new(client)).expect("client chunks"),
                client_node.clone(),
                server_node.clone(),
                session_id.clone(),
                binding,
                client_route,
            )
            .expect("client carrier"),
            PeerStream::new_owned(
                local_object_rpc_chunk_stream(Box::new(server)).expect("server chunks"),
                server_node,
                client_node,
                session_id,
                binding,
                server_route,
            )
            .expect("server carrier"),
        )
    }

    async fn open_test_pooled_routed_stream<F>(
        scope: String,
        route_compatibility_generation: u64,
        directory_generation: u64,
        constraints: RouteConstraints,
        local_node_id: NodeId,
        peer_node_id: NodeId,
        timeout: Duration,
        establish: F,
    ) -> Result<PeerStream, NetworkError>
    where
        F: Future<Output = Result<PeerStream, NetworkError>>,
    {
        open_test_pooled_routed_stream_with_affinity(
            scope,
            route_compatibility_generation,
            directory_generation,
            constraints,
            crate::DestinationProcessAffinity::Any,
            local_node_id,
            peer_node_id,
            timeout,
            establish,
        )
        .await
    }

    async fn open_test_pooled_routed_stream_with_affinity<F>(
        scope: String,
        route_compatibility_generation: u64,
        directory_generation: u64,
        constraints: RouteConstraints,
        destination_process_affinity: crate::DestinationProcessAffinity,
        local_node_id: NodeId,
        peer_node_id: NodeId,
        timeout: Duration,
        establish: F,
    ) -> Result<PeerStream, NetworkError>
    where
        F: Future<Output = Result<PeerStream, NetworkError>>,
    {
        open_pooled_routed_stream_with_route_compatibility_source(
            scope,
            route_compatibility_generation,
            directory_generation,
            constraints,
            destination_process_affinity,
            local_node_id,
            peer_node_id,
            crate::RoutedSessionProtection::Plaintext,
            timeout,
            establish,
            move || route_compatibility_generation,
        )
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn paused_runtime_open_does_not_spend_connect_budget_on_foreign_registry_owner() {
        let scope = Id::generate().to_string();
        let route_compatibility_generation = crate::network_module_routing_generation();
        let (client_carrier, server_carrier) = carrier_pair();
        let local = client_carrier.local_node_id.clone();
        let peer = client_carrier.peer_node_id.clone();
        let (registry_held, wait_until_registry_held) = std::sync::mpsc::sync_channel(0);

        // The pool is process-wide, while callers can run on independent
        // runtimes with independent clocks. Hold its tiny registry critical
        // section on another thread long enough for a paused Tokio runtime to
        // auto-advance any timer on which it is allowed to wait.
        let holder = std::thread::spawn(move || {
            let _registry = lock_routed_stream_pool();
            registry_held.send(()).expect("signal held registry");
            std::thread::sleep(Duration::from_millis(20));
        });
        wait_until_registry_held
            .recv()
            .expect("wait for foreign registry owner");

        let stream = open_test_pooled_routed_stream(
            scope.clone(),
            route_compatibility_generation,
            1,
            RouteConstraints::default(),
            local,
            peer,
            Duration::from_secs(5),
            async move { Ok(client_carrier) },
        )
        .await
        .expect("foreign registry ownership must not consume the connect budget");

        holder.join().expect("join registry holder");
        drop(stream);
        drop(server_carrier);
        assert_eq!(retire_pooled_routed_streams(&scope).await, 1);
    }

    #[tokio::test]
    async fn pooled_carrier_open_enforces_connect_timeout_and_releases_connect_lock() {
        let scope = Id::generate().to_string();
        let route_compatibility_generation = crate::network_module_routing_generation();
        let (client_carrier, server_carrier) = carrier_pair();
        let local = client_carrier.local_node_id.clone();
        let peer = client_carrier.peer_node_id.clone();
        let timeout = Duration::from_millis(20);

        let timed_out = match tokio::time::timeout(
            Duration::from_secs(1),
            open_test_pooled_routed_stream(
                scope.clone(),
                route_compatibility_generation,
                1,
                RouteConstraints::default(),
                local.clone(),
                peer.clone(),
                timeout,
                std::future::pending::<Result<PeerStream, NetworkError>>(),
            ),
        )
        .await
        .expect("the pool must enforce its own connect deadline")
        {
            Ok(_) => panic!("a stalled carrier establishment must time out"),
            Err(error) => error,
        };
        assert_eq!(
            timed_out,
            NetworkError::timeout(
                "open pooled routed carrier",
                crate::NetworkTimeoutPhase::Connect,
                timeout,
                crate::NetworkDeliveryState::DefinitelyNotSent,
            )
        );

        // Cancellation at the deadline must drop the relationship lock. A
        // later connection to the same peer must be able to establish without
        // waiting behind the abandoned attempt.
        let recovery_scope = scope.clone();
        let recovery = tokio::time::timeout(
            Duration::from_secs(1),
            open_test_pooled_routed_stream(
                recovery_scope,
                route_compatibility_generation,
                1,
                RouteConstraints::default(),
                local,
                peer,
                Duration::from_millis(200),
                async move { Ok(client_carrier) },
            ),
        )
        .await
        .expect("a later carrier open must not inherit the stalled lock")
        .expect("a later carrier open should recover");
        drop(recovery);
        drop(server_carrier);
        assert_eq!(retire_pooled_routed_streams(&scope).await, 1);
    }

    #[derive(Clone, Default)]
    struct CarrierOpenTrace {
        spans: Arc<Mutex<BTreeMap<u64, BTreeMap<String, String>>>>,
        rows: Arc<Mutex<Vec<(u64, BTreeMap<String, String>)>>>,
        next_span: Arc<std::sync::atomic::AtomicU64>,
        omitted: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[derive(Default)]
    struct CarrierTraceFields(BTreeMap<String, String>);
    impl tracing::field::Visit for CarrierTraceFields {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }
    }
    impl tracing::Subscriber for CarrierOpenTrace {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target() == "sinomo.perf"
        }
        fn new_span(&self, attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            let id = 1 + self
                .next_span
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut fields = CarrierTraceFields::default();
            attrs.record(&mut fields);
            let mut spans = self.spans.lock().unwrap();
            if spans.len() < 64 {
                spans.insert(id, fields.0);
            } else {
                self.omitted
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            tracing::span::Id::from_u64(id)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            let mut fields = CarrierTraceFields::default();
            event.record(&mut fields);
            if fields.0.get("message").map(String::as_str) != Some("routed carrier open boundary") {
                return;
            }
            let id = event
                .parent()
                .expect("explicit attempted relationship span")
                .into_u64();
            let mut rows = self.rows.lock().unwrap();
            if rows.len() < 64 {
                rows.push((id, fields.0));
            } else {
                self.omitted
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[tokio::test]
    async fn pooled_carrier_trace_distinguishes_waiter_from_owner_without_retaining_admission() {
        // Keep an unobserved dispatcher registered as well: another test's
        // first callsite use must not cache away this thread's capture.
        let _unobserved = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let trace = CarrierOpenTrace::default();
        let _capture = tracing::subscriber::set_default(trace.clone());
        let scope = Id::generate().to_string();
        let generation = crate::network_module_routing_generation();
        let (carrier, server_carrier) = carrier_pair();
        let local = carrier.local_node_id.clone();
        let peer = carrier.peer_node_id.clone();
        let mut owner = Box::pin(open_test_pooled_routed_stream(
            scope.clone(),
            generation,
            17,
            RouteConstraints::default(),
            local.clone(),
            peer.clone(),
            Duration::from_secs(5),
            std::future::pending::<Result<PeerStream, NetworkError>>(),
        ));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(owner.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        let result = open_test_pooled_routed_stream(
            scope.clone(),
            generation,
            17,
            RouteConstraints::default(),
            local.clone(),
            peer.clone(),
            Duration::from_millis(20),
            async { panic!("the waiter cannot establish while the actual owner holds admission") },
        )
        .await;
        assert!(matches!(
            result,
            Err(NetworkError::Timeout {
                delivery_state: crate::NetworkDeliveryState::DefinitelyNotSent,
                ..
            })
        ));
        {
            let rows = trace.rows.lock().unwrap();
            let owner_id = rows
                .iter()
                .find(|(_, f)| f.get("phase").map(String::as_str) == Some("carrier-establish"))
                .unwrap()
                .0;
            let waiter_id = rows
                .iter()
                .find(|(_, f)| {
                    f.get("phase").map(String::as_str) == Some("connect-owner-wait")
                        && f.get("outcome").map(String::as_str) == Some("timeout")
                })
                .unwrap()
                .0;
            assert_ne!(owner_id, waiter_id);
            let spans = trace.spans.lock().unwrap();
            for id in [owner_id, waiter_id] {
                assert_eq!(spans[&id]["scope"], scope);
                assert_eq!(spans[&id]["local_node_id"], local.to_string());
                assert_eq!(spans[&id]["peer_node_id"], peer.to_string());
                assert_eq!(spans[&id]["directory_generation"], "17");
                assert_eq!(
                    spans[&id]["route_compatibility_generation"],
                    generation.to_string()
                );
            }
        }
        // The captured rows remain alive. Cancelling only the real owner must
        // nevertheless release admission and permit actual carrier creation.
        drop(owner);
        let stream = open_test_pooled_routed_stream(
            scope.clone(),
            generation,
            17,
            RouteConstraints::default(),
            local,
            peer,
            Duration::from_millis(200),
            async { Ok(carrier) },
        )
        .await
        .expect("diagnostic state must not retain the cancelled owner's lock");
        assert_eq!(trace.omitted.load(std::sync::atomic::Ordering::Relaxed), 0);
        println!(
            "carrier_open_capture={{spans:{:?}, events:{:?}, omitted:0}}",
            trace.spans.lock().unwrap(),
            trace.rows.lock().unwrap()
        );
        drop(stream);
        drop(server_carrier);
        assert_eq!(retire_pooled_routed_streams(&scope).await, 1);
    }

    #[tokio::test]
    async fn accepted_reverse_carrier_does_not_wait_for_unreachable_outbound_dial() {
        let scope = Id::generate().to_string();
        let generation = crate::network_module_routing_generation();
        let (peer_carrier, mut local_carrier) =
            carrier_pair_for_nodes(Id::generate(), Id::generate());
        let local = local_carrier.local_node_id.clone();
        let peer = local_carrier.peer_node_id.clone();
        local_carrier.reverse_process_affinity =
            Some(crate::DestinationProcessAffinity::MainDaemon);
        let _incoming = register_routed_stream_incoming_with_process_affinity(
            local.clone(),
            crate::DestinationProcessAffinity::MainDaemon,
        )
        .expect("local dispatcher");
        let (dial_started, wait_for_dial) = tokio::sync::oneshot::channel();
        let stalled = tokio::spawn(open_test_pooled_routed_stream_with_affinity(
            scope.clone(),
            generation,
            1,
            RouteConstraints::default(),
            crate::DestinationProcessAffinity::MainDaemon,
            local.clone(),
            peer.clone(),
            Duration::from_secs(5),
            async move {
                dial_started.send(()).expect("signal outbound dial");
                std::future::pending::<Result<PeerStream, NetworkError>>().await
            },
        ));
        wait_for_dial
            .await
            .expect("outbound dial owns connect lock");

        let accepted = tokio::time::timeout(
            Duration::from_millis(100),
            accept_pooled_routed_stream_carrier_for_scopes_with_peer_capability(
                vec![scope.clone()],
                generation,
                1,
                RouteConstraints::default(),
                local_carrier,
                move || generation,
            ),
        )
        .await;
        let _accepted = accepted
            .expect(
                "an authenticated inbound carrier must not wait for an unreachable outbound dial",
            )
            .expect("accept inbound carrier");
        let (peer_sender, mut peer_incoming) = mpsc::channel(1);
        let _peer_client = PooledCarrier::new_with_incoming(
            peer_carrier,
            1,
            IncomingLogicalStreamSink::Fixed(peer_sender),
        )
        .expect("initiating peer carrier");
        let mut reverse = open_test_pooled_routed_stream_with_affinity(
            scope.clone(),
            generation,
            1,
            RouteConstraints::default(),
            crate::DestinationProcessAffinity::MainDaemon,
            local,
            peer,
            Duration::from_millis(100),
            async { panic!("the accepted reverse carrier must already be reusable") },
        )
        .await
        .expect("reuse accepted reverse carrier");
        reverse.write_all(b"r").await.expect("send reverse RPC");
        let mut received = tokio::time::timeout(Duration::from_millis(100), peer_incoming.recv())
            .await
            .expect("reverse stream arrives")
            .expect("accept reverse stream")
            .expect("reverse stream");
        let mut byte = [0];
        received
            .read_exact(&mut byte)
            .await
            .expect("reverse payload");
        assert_eq!(&byte, b"r");
        stalled.abort();
        let _ = stalled.await;
        assert_eq!(retire_pooled_routed_streams(&scope).await, 1);
    }

    #[tokio::test]
    async fn preexisting_opens_adopt_accepted_reverse_carrier_before_dial_deadline() {
        preexisting_opens_adopt_accepted_reverse_carrier_at_generation(1).await;
    }

    #[tokio::test]
    async fn accepted_publication_notifies_active_compatible_directory_generations() {
        preexisting_opens_adopt_accepted_reverse_carrier_at_generation(2).await;
    }

    async fn preexisting_opens_adopt_accepted_reverse_carrier_at_generation(
        published_directory_generation: u64,
    ) {
        let scope = Id::generate().to_string();
        let generation = crate::network_module_routing_generation();
        let (peer_carrier, mut local_carrier) =
            carrier_pair_for_nodes(Id::generate(), Id::generate());
        let local = local_carrier.local_node_id.clone();
        let peer = local_carrier.peer_node_id.clone();
        local_carrier.reverse_process_affinity =
            Some(crate::DestinationProcessAffinity::MainDaemon);
        let _incoming = register_routed_stream_incoming_with_process_affinity(
            local.clone(),
            crate::DestinationProcessAffinity::MainDaemon,
        )
        .expect("local main dispatcher");
        let establish_started = Arc::new(AtomicUsize::new(0));
        let establish_dropped = Arc::new(AtomicUsize::new(0));
        struct EstablishGuard(Arc<AtomicUsize>);
        impl Drop for EstablishGuard {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let started = Arc::clone(&establish_started);
        let dropped = Arc::clone(&establish_dropped);
        let mut owner = Box::pin(open_test_pooled_routed_stream_with_affinity(
            scope.clone(),
            generation,
            1,
            RouteConstraints::default(),
            crate::DestinationProcessAffinity::MainDaemon,
            local.clone(),
            peer.clone(),
            Duration::from_secs(5),
            async move {
                let _guard = EstablishGuard(dropped);
                started.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<Result<PeerStream, NetworkError>>().await
            },
        ));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(owner.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        assert_eq!(establish_started.load(Ordering::SeqCst), 1);
        let mut waiter = Box::pin(open_test_pooled_routed_stream_with_affinity(
            scope.clone(),
            generation,
            1,
            RouteConstraints::default(),
            crate::DestinationProcessAffinity::MainDaemon,
            local.clone(),
            peer.clone(),
            Duration::from_secs(5),
            async { panic!("the already parked waiter must never establish a duplicate carrier") },
        ));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(waiter.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        let key = PoolKey::new(
            scope.clone(),
            generation,
            1,
            RouteConstraints::default(),
            crate::DestinationProcessAffinity::MainDaemon,
            local.clone(),
            peer.clone(),
            crate::RoutedSessionProtection::Plaintext,
        );
        let admission = lock_routed_stream_connect_locks()
            .get(&key)
            .cloned()
            .unwrap();
        assert!(
            admission.owner.try_lock().is_err(),
            "the original dial still owns admission"
        );

        let _accepted = accept_pooled_routed_stream_carrier_for_scopes_with_peer_capability(
            vec![scope.clone()],
            generation,
            published_directory_generation,
            RouteConstraints::default(),
            local_carrier,
            move || generation,
        )
        .await
        .expect("publish authenticated reverse carrier while both callers are parked");
        let (peer_sender, mut peer_incoming) = mpsc::channel(3);
        let _peer_client = PooledCarrier::new_with_incoming(
            peer_carrier,
            1,
            IncomingLogicalStreamSink::Fixed(peer_sender),
        )
        .expect("initiating peer carrier");
        let mut fresh = open_test_pooled_routed_stream_with_affinity(
            scope.clone(),
            generation,
            published_directory_generation,
            RouteConstraints::default(),
            crate::DestinationProcessAffinity::MainDaemon,
            local,
            peer,
            Duration::from_millis(100),
            async { panic!("published capacity must already be usable by a fresh caller") },
        )
        .await
        .expect("fresh caller reuses accepted carrier");
        fresh.write_all(b"f").await.unwrap();
        let mut received = tokio::time::timeout(Duration::from_millis(100), peer_incoming.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mut byte = [0];
        received.read_exact(&mut byte).await.unwrap();
        assert_eq!(
            &byte, b"f",
            "the published carrier transfers bytes before owner release"
        );
        assert_eq!(establish_dropped.load(Ordering::SeqCst), 0);

        let completed = tokio::time::timeout(Duration::from_millis(100), async {
            tokio::join!(owner.as_mut(), waiter.as_mut())
        })
        .await;
        println!(
            "accepted_reverse_before_existing_opens={{usable:true, original_connect_seconds:5, original_establishments:{}, original_establishment_dropped:{}, preexisting_completed:{}}}",
            establish_started.load(Ordering::SeqCst),
            establish_dropped.load(Ordering::SeqCst),
            completed.is_ok()
        );
        let (owner_stream, waiter_stream) = match completed {
            Ok(streams) => streams,
            Err(_) => {
                drop(owner);
                drop(waiter);
                retire_pooled_routed_streams(&scope).await;
                panic!(
                    "usable accepted carrier left both preexisting opens parked behind an unrelated pending dial"
                );
            }
        };
        for (stream, expected) in [(owner_stream, b'o'), (waiter_stream, b'w')] {
            let mut stream = stream.expect("preexisting open adopts the authenticated carrier");
            stream.write_all(&[expected]).await.unwrap();
            let mut received =
                tokio::time::timeout(Duration::from_millis(100), peer_incoming.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            received.read_exact(&mut byte).await.unwrap();
            assert_eq!(byte, [expected]);
        }
        assert_eq!(establish_started.load(Ordering::SeqCst), 1);
        assert_eq!(establish_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(retire_pooled_routed_streams(&scope).await, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn publication_wakes_preserve_original_deadlines_and_dial_ownership() {
        let scope = Id::generate().to_string();
        let generation = crate::network_module_routing_generation();
        let local = Id::generate();
        let peer = Id::generate();
        let attempts = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&attempts);
        let mut owner = Box::pin(open_test_pooled_routed_stream(
            scope.clone(),
            generation,
            1,
            RouteConstraints::default(),
            local.clone(),
            peer.clone(),
            Duration::from_secs(5),
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<Result<PeerStream, NetworkError>>().await
            },
        ));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(owner.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        let mut waiter = Box::pin(open_test_pooled_routed_stream(
            scope.clone(),
            generation,
            1,
            RouteConstraints::default(),
            local.clone(),
            peer.clone(),
            Duration::from_secs(1),
            async { panic!("waiter must not acquire the live owner's dial") },
        ));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(waiter.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        let key = PoolKey::new(
            scope.clone(),
            generation,
            1,
            RouteConstraints::default(),
            crate::DestinationProcessAffinity::Any,
            local,
            peer,
            crate::RoutedSessionProtection::Plaintext,
        );
        let admission = lock_routed_stream_connect_locks()
            .get(&key)
            .cloned()
            .unwrap();
        tokio::time::advance(Duration::from_millis(500)).await;
        admission.published.notify_waiters();
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(owner.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(waiter.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(
            matches!(waiter.await, Err(NetworkError::Timeout { timeout, delivery_state: crate::NetworkDeliveryState::DefinitelyNotSent, .. }) if timeout == Duration::from_secs(1))
        );
        assert!(
            admission.owner.try_lock().is_err(),
            "timed-out waiter cannot release another caller's guard"
        );
        tokio::time::advance(Duration::from_millis(3500)).await;
        admission.published.notify_waiters();
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(owner.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(
            matches!(owner.await, Err(NetworkError::Timeout { timeout, delivery_state: crate::NetworkDeliveryState::DefinitelyNotSent, .. }) if timeout == Duration::from_secs(5))
        );
        assert!(admission.owner.try_lock().is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(retire_pooled_routed_streams(&scope).await, 0);
    }

    #[tokio::test]
    async fn publication_wake_revalidates_role_scope_dispatcher_and_generation() {
        for invalid in ["role", "scope", "dispatcher", "generation"] {
            let scope = Id::generate().to_string();
            let generation = crate::network_module_routing_generation();
            let current = Arc::new(std::sync::atomic::AtomicU64::new(generation));
            let (peer_carrier, mut local_carrier) =
                carrier_pair_for_nodes(Id::generate(), Id::generate());
            let local = local_carrier.local_node_id.clone();
            let peer = local_carrier.peer_node_id.clone();
            local_carrier.reverse_process_affinity =
                Some(crate::DestinationProcessAffinity::MainDaemon);
            let mut incoming = Some(
                register_routed_stream_incoming_with_process_affinity(
                    local.clone(),
                    crate::DestinationProcessAffinity::MainDaemon,
                )
                .unwrap(),
            );
            let fence = Arc::clone(&current);
            let mut owner = Box::pin(open_pooled_routed_stream_with_route_compatibility_source(
                scope.clone(),
                generation,
                1,
                RouteConstraints::default(),
                crate::DestinationProcessAffinity::MainDaemon,
                local.clone(),
                peer.clone(),
                crate::RoutedSessionProtection::Plaintext,
                Duration::from_secs(5),
                std::future::pending::<Result<PeerStream, NetworkError>>(),
                move || fence.load(Ordering::SeqCst),
            ));
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(owner.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            let key = PoolKey::new(
                scope.clone(),
                generation,
                1,
                RouteConstraints::default(),
                crate::DestinationProcessAffinity::MainDaemon,
                local.clone(),
                peer,
                crate::RoutedSessionProtection::Plaintext,
            );
            let admission = lock_routed_stream_connect_locks()
                .get(&key)
                .cloned()
                .unwrap();
            let mut accepted_scope = scope.clone();
            let mut accepted_generation = generation;
            match invalid {
                "role" => {
                    local_carrier.reverse_process_affinity =
                        Some(crate::DestinationProcessAffinity::Any)
                }
                "scope" => accepted_scope = Id::generate().to_string(),
                "dispatcher" => {
                    drop(incoming.take());
                    incoming = Some(
                        register_routed_stream_incoming_with_process_affinity(
                            local,
                            crate::DestinationProcessAffinity::MainDaemon,
                        )
                        .unwrap(),
                    );
                }
                "generation" => {
                    accepted_generation += 1;
                    current.store(accepted_generation, Ordering::SeqCst);
                }
                _ => unreachable!(),
            }
            let _accepted = accept_pooled_routed_stream_carrier_for_scopes_with_peer_capability(
                vec![accepted_scope.clone()],
                accepted_generation,
                1,
                RouteConstraints::default(),
                local_carrier,
                || current.load(Ordering::SeqCst),
            )
            .await
            .unwrap();
            // A wake is advisory, even if it arrives spuriously or from a
            // publication whose key no longer belongs to this caller.
            admission.published.notify_waiters();
            let outcome = std::future::poll_fn(|cx| Poll::Ready(owner.as_mut().poll(cx))).await;
            if matches!(invalid, "dispatcher" | "generation") {
                assert!(
                    matches!(outcome, Poll::Ready(Err(NetworkError::Directory { .. }))),
                    "{invalid}"
                );
            } else {
                assert!(
                    outcome.is_pending(),
                    "{invalid} cannot satisfy MainDaemon/current scope"
                );
            }
            drop(owner);
            drop(peer_carrier);
            drop(incoming);
            retire_pooled_routed_streams(&scope).await;
            retire_pooled_routed_streams(&accepted_scope).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn late_publication_cannot_revive_an_expired_connect_deadline() {
        let scope = Id::generate().to_string();
        let generation = crate::network_module_routing_generation();
        let (peer_carrier, mut accepted_carrier) =
            carrier_pair_for_nodes(Id::generate(), Id::generate());
        let local = accepted_carrier.local_node_id.clone();
        let peer = accepted_carrier.peer_node_id.clone();
        accepted_carrier.reverse_process_affinity =
            Some(crate::DestinationProcessAffinity::MainDaemon);
        let _incoming = register_routed_stream_incoming_with_process_affinity(
            local.clone(),
            crate::DestinationProcessAffinity::MainDaemon,
        )
        .unwrap();
        let mut expired = Box::pin(open_test_pooled_routed_stream_with_affinity(
            scope.clone(),
            generation,
            1,
            RouteConstraints::default(),
            crate::DestinationProcessAffinity::MainDaemon,
            local.clone(),
            peer.clone(),
            Duration::from_secs(5),
            std::future::pending::<Result<PeerStream, NetworkError>>(),
        ));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(expired.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        tokio::time::advance(Duration::from_secs(6)).await;
        let _accepted = accept_pooled_routed_stream_carrier_for_scopes_with_peer_capability(
            vec![scope.clone()],
            generation,
            1,
            RouteConstraints::default(),
            accepted_carrier,
            move || generation,
        )
        .await
        .unwrap();
        assert!(
            matches!(expired.await, Err(NetworkError::Timeout {
            timeout, delivery_state: crate::NetworkDeliveryState::DefinitelyNotSent, ..
        }) if timeout == Duration::from_secs(5)),
            "a ready publication cannot extend the original caller deadline"
        );
        let (sender, mut incoming) = mpsc::channel(1);
        let _peer_client = PooledCarrier::new_with_incoming(
            peer_carrier,
            1,
            IncomingLogicalStreamSink::Fixed(sender),
        )
        .unwrap();
        let mut fresh = open_test_pooled_routed_stream_with_affinity(
            scope.clone(),
            generation,
            1,
            RouteConstraints::default(),
            crate::DestinationProcessAffinity::MainDaemon,
            local,
            peer,
            Duration::from_secs(5),
            async { panic!("the expired caller must not remove the independent publication") },
        )
        .await
        .unwrap();
        fresh.write_all(b"l").await.unwrap();
        let mut received = incoming.recv().await.unwrap().unwrap();
        let mut byte = [0];
        received.read_exact(&mut byte).await.unwrap();
        assert_eq!(&byte, b"l");
        assert_eq!(retire_pooled_routed_streams(&scope).await, 1);
    }

    #[tokio::test]
    async fn unused_dial_completion_cannot_replace_a_published_reverse_carrier() {
        for failed_dial in [false, true] {
            let scope = Id::generate().to_string();
            let generation = crate::network_module_routing_generation();
            let (peer_carrier, mut accepted_carrier) =
                carrier_pair_for_nodes(Id::generate(), Id::generate());
            let local = accepted_carrier.local_node_id.clone();
            let peer = accepted_carrier.peer_node_id.clone();
            accepted_carrier.reverse_process_affinity =
                Some(crate::DestinationProcessAffinity::MainDaemon);
            let binding = accepted_carrier.channel_binding;
            let (unused_peer, unused_carrier) = carrier_pair_for_nodes(peer.clone(), local.clone());
            let _incoming = register_routed_stream_incoming_with_process_affinity(
                local.clone(),
                crate::DestinationProcessAffinity::MainDaemon,
            )
            .unwrap();
            let (peer_sender, mut peer_incoming) = mpsc::channel(1);
            let _peer_client = PooledCarrier::new_with_incoming(
                peer_carrier,
                1,
                IncomingLogicalStreamSink::Fixed(peer_sender),
            )
            .unwrap();
            let established_scope = scope.clone();
            let (accepted_tx, accepted_rx) = oneshot::channel();
            let mut stream = open_test_pooled_routed_stream_with_affinity(
                scope.clone(),
                generation,
                1,
                RouteConstraints::default(),
                crate::DestinationProcessAffinity::MainDaemon,
                local,
                peer,
                Duration::from_secs(5),
                async move {
                    let accepted =
                        accept_pooled_routed_stream_carrier_for_scopes_with_peer_capability(
                            vec![established_scope],
                            generation,
                            1,
                            RouteConstraints::default(),
                            accepted_carrier,
                            move || generation,
                        )
                        .await
                        .unwrap();
                    accepted_tx.send(accepted).ok().unwrap();
                    if failed_dial {
                        Err(NetworkError::Io {
                            operation: "unused dial".into(),
                            detail: "late failure".into(),
                        })
                    } else {
                        Ok(unused_carrier)
                    }
                },
            )
            .await
            .expect("independent publication wins the final dial poll");
            let _accepted = accepted_rx.await.unwrap();
            assert_eq!(stream.channel_binding, binding);
            stream.write_all(b"a").await.unwrap();
            let mut received =
                tokio::time::timeout(Duration::from_millis(100), peer_incoming.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            let mut byte = [0];
            received.read_exact(&mut byte).await.unwrap();
            assert_eq!(&byte, b"a");
            drop(unused_peer);
            assert_eq!(retire_pooled_routed_streams(&scope).await, 1);
        }
    }

    #[test]
    fn pool_replaces_a_carrier_whose_owner_runtime_stopped() {
        let scope = Id::generate().to_string();
        let route_compatibility_generation = crate::network_module_routing_generation();
        let (first_client_carrier, first_server_carrier) = carrier_pair();
        let (second_client_carrier, second_server_carrier) = carrier_pair();
        let local = first_client_carrier.local_node_id.clone();
        let peer = first_client_carrier.peer_node_id.clone();
        let first_session_id = first_client_carrier.session_id.clone();
        let second_session_id = second_client_carrier.session_id.clone();

        let first_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build first caller runtime");
        let first_stream = first_runtime
            .block_on(open_test_pooled_routed_stream(
                scope.clone(),
                route_compatibility_generation,
                1,
                RouteConstraints::default(),
                local.clone(),
                peer.clone(),
                Duration::from_secs(1),
                async move { Ok(first_client_carrier) },
            ))
            .expect("open first-runtime logical stream");
        assert_eq!(first_stream.session_id, first_session_id);
        drop(first_stream);
        drop(first_runtime);

        let second_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build second caller runtime");

        // A synchronous API may retain the process-wide pool across calls
        // even though its per-call Tokio runtime has been torn down. Inspect
        // under the short registry lock. Another concurrent pool operation may
        // already have purged the closed carrier; either state is safe, but a
        // carrier that still reports live would be unsafe to reuse.
        let first_carrier_stopped = {
            let pool = lock_routed_stream_pool();
            pool.iter()
                .find(|(key, _)| {
                    key.scope == scope && key.local_node_id == local && key.peer_node_id == peer
                })
                .map(|(_, client)| client.is_closed())
        };
        assert_ne!(first_carrier_stopped, Some(false));

        // The next open must establish on this runtime instead of enqueueing
        // onto the orphaned transmitter retained by the first runtime.
        let second_stream = second_runtime
            .block_on(open_test_pooled_routed_stream(
                scope.clone(),
                route_compatibility_generation,
                1,
                RouteConstraints::default(),
                local,
                peer,
                Duration::from_secs(1),
                async move { Ok(second_client_carrier) },
            ))
            .expect("replace stopped carrier on the second runtime");
        assert_eq!(second_stream.session_id, second_session_id);
        assert_ne!(second_stream.session_id, first_session_id);
        drop(second_stream);
        assert_eq!(
            second_runtime.block_on(retire_pooled_routed_streams(&scope)),
            1
        );
        drop(second_runtime);
        drop(first_server_carrier);
        drop(second_server_carrier);
    }

    #[tokio::test]
    async fn pool_reuses_one_carrier_for_independent_streams() {
        let (client_carrier, server_carrier) = carrier_pair();
        let local = client_carrier.local_node_id.clone();
        let peer = client_carrier.peer_node_id.clone();
        let scope = Id::generate().to_string();
        let route_compatibility_generation = crate::network_module_routing_generation();
        let establishments = Arc::new(AtomicUsize::new(0));
        let mut acceptor = accept_routed_stream_carrier(server_carrier);
        let (server_done, wait_for_done) = oneshot::channel();
        let server = tokio::spawn(async move {
            let first = acceptor
                .accept()
                .await
                .expect("accept first")
                .expect("first stream");
            let second = acceptor
                .accept()
                .await
                .expect("accept second")
                .expect("second stream");
            let _ = wait_for_done.await;
            (first.session_id, second.session_id)
        });

        let count = Arc::clone(&establishments);
        let mut first = open_test_pooled_routed_stream(
            scope.clone(),
            route_compatibility_generation,
            1,
            RouteConstraints::default(),
            local.clone(),
            peer.clone(),
            Duration::from_secs(1),
            async move {
                count.fetch_add(1, Ordering::Relaxed);
                Ok(client_carrier)
            },
        )
        .await
        .expect("first logical stream");
        let count = Arc::clone(&establishments);
        let mut second = open_test_pooled_routed_stream(
            scope,
            route_compatibility_generation,
            1,
            RouteConstraints::default(),
            local,
            peer,
            Duration::from_secs(1),
            async move {
                count.fetch_add(1, Ordering::Relaxed);
                Err(NetworkError::Io {
                    operation: "unexpected carrier establishment".to_string(),
                    detail: "the existing carrier should have been reused".to_string(),
                })
            },
        )
        .await
        .expect("second logical stream");

        assert_eq!(first.session_id, second.session_id);
        first
            .write_all(b"a")
            .await
            .expect("publish first logical open");
        second
            .write_all(b"b")
            .await
            .expect("publish second logical open");
        let _ = server_done.send(());
        let (server_first, server_second) = server.await.expect("server task");
        assert_eq!(first.session_id, server_first);
        assert_eq!(second.session_id, server_second);
        assert_eq!(establishments.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn main_daemon_open_never_reuses_an_affinity_neutral_carrier() {
        let (any_client_carrier, any_server_carrier) = carrier_pair();
        let (main_client_carrier, main_server_carrier) = carrier_pair();
        let local = any_client_carrier.local_node_id.clone();
        let peer = any_client_carrier.peer_node_id.clone();
        let scope = Id::generate().to_string();
        let route_compatibility_generation = crate::network_module_routing_generation();
        let any_establishments = Arc::new(AtomicUsize::new(0));
        let main_establishments = Arc::new(AtomicUsize::new(0));
        let mut any_acceptor = accept_routed_stream_carrier(any_server_carrier);
        let mut main_acceptor = accept_routed_stream_carrier(main_server_carrier);
        let any_server = tokio::spawn(async move {
            let mut stream = any_acceptor
                .accept()
                .await
                .expect("accept affinity-neutral stream")
                .expect("affinity-neutral stream");
            let mut byte = [0_u8; 1];
            stream
                .read_exact(&mut byte)
                .await
                .expect("read affinity-neutral stream");
            byte[0]
        });
        let main_server = tokio::spawn(async move {
            let mut stream = main_acceptor
                .accept()
                .await
                .expect("accept main-daemon stream")
                .expect("main-daemon stream");
            let mut byte = [0_u8; 1];
            stream
                .read_exact(&mut byte)
                .await
                .expect("read main-daemon stream");
            byte[0]
        });

        let any_count = Arc::clone(&any_establishments);
        let mut any_stream = open_test_pooled_routed_stream(
            scope.clone(),
            route_compatibility_generation,
            1,
            RouteConstraints::default(),
            local.clone(),
            peer.clone(),
            Duration::from_secs(1),
            async move {
                any_count.fetch_add(1, Ordering::Relaxed);
                Ok(any_client_carrier)
            },
        )
        .await
        .expect("open affinity-neutral stream");
        let any_session_id = any_stream.session_id.clone();

        let main_count = Arc::clone(&main_establishments);
        let mut main_stream = open_test_pooled_routed_stream_with_affinity(
            scope.clone(),
            route_compatibility_generation,
            1,
            RouteConstraints::default(),
            crate::DestinationProcessAffinity::MainDaemon,
            local,
            peer,
            Duration::from_secs(1),
            async move {
                main_count.fetch_add(1, Ordering::Relaxed);
                Ok(main_client_carrier)
            },
        )
        .await
        .expect("open main-daemon stream");

        assert_ne!(
            any_session_id, main_stream.session_id,
            "a main-daemon open must establish a carrier with matching affinity"
        );
        any_stream
            .write_all(b"a")
            .await
            .expect("write affinity-neutral stream");
        main_stream
            .write_all(b"m")
            .await
            .expect("write main-daemon stream");
        assert_eq!(any_server.await.expect("affinity-neutral server"), b'a');
        assert_eq!(main_server.await.expect("main-daemon server"), b'm');
        assert_eq!(any_establishments.load(Ordering::Relaxed), 1);
        assert_eq!(main_establishments.load(Ordering::Relaxed), 1);
        assert_eq!(retire_pooled_routed_streams(&scope).await, 2);
    }

    #[tokio::test]
    async fn accepted_main_daemon_carrier_supports_fenced_reverse_streams_without_redial() {
        let (client_carrier, mut server_carrier) =
            carrier_pair_for_nodes(Id::generate(), Id::generate());
        server_carrier.reverse_process_affinity =
            Some(crate::DestinationProcessAffinity::MainDaemon);
        let client_node = client_carrier.local_node_id.clone();
        let server_node = client_carrier.peer_node_id.clone();
        let dynamic_scope = Id::generate().to_string();
        let fenced_scope = format!("{dynamic_scope}:fenced-bootstrap:{}", "5a".repeat(32));
        let foreign_fenced_scope = format!("{dynamic_scope}:fenced-bootstrap:{}", "a5".repeat(32));
        let route_compatibility_generation = crate::network_module_routing_generation();
        let constraints = RouteConstraints::default();
        let mut client_incoming = register_routed_stream_incoming_with_process_affinity(
            client_node.clone(),
            crate::DestinationProcessAffinity::MainDaemon,
        )
        .expect("incoming dispatcher");
        let reverse_establishments = Arc::new(AtomicUsize::new(0));
        let foreign_establishments = Arc::new(AtomicUsize::new(0));
        let server_dynamic_scope = dynamic_scope.clone();
        let server_fenced_scope = fenced_scope.clone();
        let server_foreign_fenced_scope = foreign_fenced_scope.clone();
        let server_local = server_node.clone();
        let server_peer = client_node.clone();
        let reverse_count = Arc::clone(&reverse_establishments);
        let foreign_count = Arc::clone(&foreign_establishments);
        let server = tokio::spawn(async move {
            let mut acceptor = accept_pooled_routed_stream_carrier_for_scopes_with_peer_capability(
                vec![server_dynamic_scope, server_fenced_scope.clone()],
                route_compatibility_generation,
                1,
                constraints,
                server_carrier,
                move || route_compatibility_generation,
            )
            .await
            .expect("accept pooled carrier");
            let mut initial = acceptor
                .accept()
                .await
                .expect("accept initial stream")
                .expect("initial stream");
            assert!(initial.has_owned_chunks());
            let mut request = [0_u8; 4];
            initial
                .read_exact(&mut request)
                .await
                .expect("read initial request");
            assert_eq!(&request, b"ping");
            initial
                .write_all(b"pong")
                .await
                .expect("write initial response");

            let mut reverse = open_test_pooled_routed_stream_with_affinity(
                server_fenced_scope,
                route_compatibility_generation,
                2,
                constraints,
                crate::DestinationProcessAffinity::MainDaemon,
                server_local.clone(),
                server_peer.clone(),
                Duration::from_secs(1),
                async move {
                    reverse_count.fetch_add(1, Ordering::Relaxed);
                    Err(NetworkError::Io {
                        operation: "unexpected reverse carrier establishment".to_string(),
                        detail: "the accepted carrier must be reusable in reverse".to_string(),
                    })
                },
            )
            .await
            .expect("reverse logical stream");
            assert!(reverse.has_owned_chunks());
            reverse
                .write_all(b"back")
                .await
                .expect("write reverse request");
            let mut response = [0_u8; 4];
            reverse
                .read_exact(&mut response)
                .await
                .expect("read reverse response");
            assert_eq!(&response, b"okay");

            let result = open_test_pooled_routed_stream_with_affinity(
                server_foreign_fenced_scope,
                route_compatibility_generation,
                2,
                constraints,
                crate::DestinationProcessAffinity::MainDaemon,
                server_local,
                server_peer,
                Duration::from_secs(1),
                async move {
                    foreign_count.fetch_add(1, Ordering::Relaxed);
                    Err(NetworkError::Io {
                        operation: "expected foreign fenced carrier establishment".to_string(),
                        detail: "a different fenced fingerprint must not reuse the alias"
                            .to_string(),
                    })
                },
            )
            .await;
            let error = match result {
                Ok(_) => panic!("foreign fenced scope must miss the accepted alias"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains("different fenced fingerprint"),
                "unexpected foreign-scope error: {error}"
            );
        });

        let mut initial = open_test_pooled_routed_stream_with_affinity(
            dynamic_scope.clone(),
            route_compatibility_generation,
            1,
            constraints,
            crate::DestinationProcessAffinity::MainDaemon,
            client_node.clone(),
            server_node.clone(),
            Duration::from_secs(1),
            async move { Ok(client_carrier) },
        )
        .await
        .expect("initial logical stream");
        assert!(initial.has_owned_chunks());
        initial
            .write_all(b"ping")
            .await
            .expect("write initial request");
        let mut response = [0_u8; 4];
        initial
            .read_exact(&mut response)
            .await
            .expect("read initial response");
        assert_eq!(&response, b"pong");

        let mut reverse = client_incoming
            .accept()
            .await
            .expect("accept reverse stream")
            .expect("reverse stream");
        assert!(reverse.has_owned_chunks());
        let mut request = [0_u8; 4];
        reverse
            .read_exact(&mut request)
            .await
            .expect("read reverse request");
        assert_eq!(&request, b"back");
        reverse
            .write_all(b"okay")
            .await
            .expect("write reverse response");
        server.await.expect("server task");

        assert_eq!(reverse_establishments.load(Ordering::Relaxed), 0);
        assert_eq!(foreign_establishments.load(Ordering::Relaxed), 1);
        assert_eq!(retire_pooled_routed_streams(&dynamic_scope).await, 2);
        assert_eq!(retire_pooled_routed_streams(&fenced_scope).await, 1);
        // The public diagnostic is nonblocking and may return None while a
        // parallel test owns the global pool lock. Check retirement under the
        // existing lock, scoped to this test's exact relationship.
        let pool = lock_routed_stream_pool();
        assert!(!pool.iter().any(|(key, client)| {
            key.scope == dynamic_scope
                && key.local_node_id == client_node
                && key.peer_node_id == server_node
                && !client.is_closed()
        }));
    }

    #[tokio::test]
    async fn accepted_client_carrier_cannot_become_a_reverse_main_daemon_route() {
        let (_client_carrier, server_carrier) = carrier_pair();
        let local = server_carrier.local_node_id.clone();
        let peer = server_carrier.peer_node_id.clone();
        let scope = Id::generate().to_string();
        let generation = crate::network_module_routing_generation();
        let constraints = RouteConstraints::default();
        // The local listener is a main daemon, but the initiating peer has not
        // established that it serves main-daemon requests on reverse streams.
        let _acceptor =
            accept_pooled_routed_stream_carrier(scope.clone(), 1, constraints, server_carrier)
                .await
                .expect("accept the incoming client carrier");
        let result = open_test_pooled_routed_stream_with_affinity(
            scope.clone(),
            generation,
            1,
            constraints,
            crate::DestinationProcessAffinity::MainDaemon,
            local,
            peer,
            Duration::from_secs(1),
            async {
                Err(NetworkError::Io {
                    operation: "establish actual main daemon".into(),
                    detail: "the caller-only carrier must not satisfy this open".into(),
                })
            },
        )
        .await;
        retire_pooled_routed_streams(&scope).await;
        assert!(
            result.is_err(),
            "the accepting listener's role must not label the reverse peer"
        );
    }

    #[tokio::test]
    async fn accepted_shadow_carrier_does_not_satisfy_main_affinity() {
        let (_client, mut carrier) = carrier_pair();
        carrier.reverse_process_affinity = Some(crate::DestinationProcessAffinity::Any);
        let local = carrier.local_node_id.clone();
        let peer = carrier.peer_node_id.clone();
        let scope = Id::generate().to_string();
        let generation = crate::network_module_routing_generation();
        let _acceptor = accept_pooled_routed_stream_carrier_for_scopes_with_peer_capability(
            vec![scope.clone()],
            generation,
            1,
            RouteConstraints::default(),
            carrier,
            move || generation,
        )
        .await
        .expect("accept shadow");
        let result = open_test_pooled_routed_stream_with_affinity(
            scope.clone(),
            generation,
            1,
            RouteConstraints::default(),
            crate::DestinationProcessAffinity::MainDaemon,
            local,
            peer,
            Duration::from_secs(1),
            async { Err(NetworkError::AuthDenied) },
        )
        .await;
        assert!(matches!(result, Err(NetworkError::AuthDenied)));
        assert_eq!(retire_pooled_routed_streams(&scope).await, 1);
    }

    #[tokio::test]
    async fn dispatcher_change_during_connect_rejects_carrier_before_publication() {
        let (client, _server) = carrier_pair_for_nodes(Id::generate(), Id::generate());
        let local = client.local_node_id.clone();
        let peer = client.peer_node_id.clone();
        let scope = Id::generate().to_string();
        let registration = Arc::new(Mutex::new(None));
        let installed = Arc::clone(&registration);
        let source = local.clone();
        let result = open_test_pooled_routed_stream(
            scope.clone(),
            crate::network_module_routing_generation(),
            1,
            RouteConstraints::default(),
            local,
            peer,
            Duration::from_secs(1),
            async move {
                *installed.lock().unwrap() = Some(register_routed_stream_incoming(source).unwrap());
                Ok(client)
            },
        )
        .await;
        assert!(matches!(result, Err(NetworkError::Directory { .. })));
        assert_eq!(retire_pooled_routed_streams(&scope).await, 0);
    }

    #[test]
    fn changing_dispatcher_invalidates_carrier_reuse_and_old_reverse_delivery() {
        let local = Id::generate();
        let peer = Id::generate();
        let key = || {
            PoolKey::new(
                "dispatcher-lifetime".into(),
                1,
                1,
                RouteConstraints::default(),
                crate::DestinationProcessAffinity::MainDaemon,
                local.clone(),
                peer.clone(),
                crate::RoutedSessionProtection::Plaintext,
            )
        };
        let caller_only = key();
        assert_eq!(registered_routed_stream_incoming_affinity(&local), None);
        let main = register_routed_stream_incoming_with_process_affinity(
            local.clone(),
            crate::DestinationProcessAffinity::MainDaemon,
        )
        .unwrap();
        let main_key = key();
        assert!(!caller_only.same_relationship(&main_key));
        let token = main_key.local_dispatcher.clone();
        assert!(registered_routed_stream_incoming_sender(&local, token.as_ref()).is_some());
        drop(main);
        let _shadow = register_routed_stream_incoming(local.clone()).unwrap();
        assert_eq!(
            registered_routed_stream_incoming_affinity(&local),
            Some(crate::DestinationProcessAffinity::Any)
        );
        assert!(!main_key.same_relationship(&key()));
        assert!(registered_routed_stream_incoming_sender(&local, token.as_ref()).is_none());
    }

    #[tokio::test]
    async fn accepted_carrier_is_affinity_neutral_for_reverse_reuse() {
        let (client_carrier, server_carrier) = carrier_pair();
        let scope = Id::generate().to_string();
        let route_compatibility_generation = crate::network_module_routing_generation();
        let constraints = RouteConstraints::default();

        let acceptor = accept_pooled_routed_stream_carrier_with_route_compatibility_source(
            scope.clone(),
            route_compatibility_generation,
            1,
            constraints,
            crate::DestinationProcessAffinity::Any,
            server_carrier,
            move || route_compatibility_generation,
        )
        .await
        .expect("accept pooled carrier");
        {
            let pool = lock_routed_stream_pool();
            let accepted_key = pool
                .keys()
                .find(|key| key.scope == scope)
                .expect("accepted carrier pool key");
            assert_eq!(
                accepted_key.destination_process_affinity,
                crate::DestinationProcessAffinity::Any
            );
        }
        drop(acceptor);
        drop(client_carrier);
        assert_eq!(retire_pooled_routed_streams(&scope).await, 1);
    }

    #[tokio::test]
    async fn logical_open_does_not_wait_for_remote_accept() {
        let (client_carrier, _server_carrier) = carrier_pair();
        let client =
            Arc::new(OwnedRoutedStreamClient::new(client_carrier).expect("owned mux client"));

        let stream = client
            .open()
            .expect("logical open must not wait for a remote acknowledgement");

        drop(stream);
    }

    #[tokio::test]
    async fn carrier_server_handles_repeated_compound_streams() {
        let (client_carrier, server_carrier) = carrier_pair();
        let handled = Arc::new(AtomicUsize::new(0));
        let server_handled = Arc::clone(&handled);
        let server = tokio::spawn(async move {
            serve_routed_stream_carrier(server_carrier, move |mut stream| {
                let handled = Arc::clone(&server_handled);
                async move {
                    let mut request = Vec::new();
                    stream
                        .read_to_end(&mut request)
                        .await
                        .map_err(|error| mux_io_error("read test logical request", error))?;
                    assert_eq!(request, b"ping");
                    stream
                        .write_all(b"pong")
                        .await
                        .map_err(|error| mux_io_error("write test logical response", error))?;
                    stream
                        .shutdown()
                        .await
                        .map_err(|error| mux_io_error("finish test logical response", error))?;
                    handled.fetch_add(1, Ordering::Relaxed);
                    Ok(false)
                }
            })
            .await
            .expect("serve routed carrier")
        });
        let client =
            Arc::new(OwnedRoutedStreamClient::new(client_carrier).expect("owned mux client"));

        for _ in 0..2 {
            let mut stream = client.open().expect("open logical stream");
            stream.write_all(b"ping").await.expect("write request");
            stream.shutdown().await.expect("finish request");
            let mut response = Vec::new();
            stream
                .read_to_end(&mut response)
                .await
                .expect("read response");
            assert_eq!(response, b"pong");
        }

        drop(client);
        assert!(
            !tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .expect("carrier server stopped")
                .expect("join carrier server")
        );
        assert_eq!(handled.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn local_rpc_pool_reuses_transport_peer_context() {
        let scope = Id::generate().to_string();
        let establishments = Arc::new(AtomicUsize::new(0));
        let (mut client_carrier, mut server_carrier) = tokio::io::duplex(256 * 1024);
        let server = tokio::spawn(async move {
            let carrier_context = crate::read_local_object_rpc_carrier_prelude(&mut server_carrier)
                .await
                .expect("read local carrier prelude");
            let mut acceptor =
                accept_local_object_rpc_stream_carrier(Box::new(server_carrier), &carrier_context)
                    .expect("local carrier acceptor");
            let first = acceptor
                .accept()
                .await
                .expect("accept first")
                .expect("first logical stream");
            let first_context = RpcPeerContext::unauthenticated_local(
                first.session_id.clone(),
                first.channel_binding,
            );
            let second = acceptor
                .accept()
                .await
                .expect("accept second")
                .expect("second logical stream");
            let second_context = RpcPeerContext::unauthenticated_local(
                second.session_id.clone(),
                second.channel_binding,
            );
            (first_context, second_context)
        });

        let count = Arc::clone(&establishments);
        let mut first = open_pooled_local_object_rpc_stream(
            scope.clone(),
            Duration::from_secs(1),
            async move {
                count.fetch_add(1, Ordering::Relaxed);
                let context =
                    crate::write_local_object_rpc_carrier_prelude(&mut client_carrier).await?;
                Ok((Box::new(client_carrier) as BoxedNetworkStream, context))
            },
        )
        .await
        .expect("first local logical stream");
        let first_context =
            RpcPeerContext::unauthenticated_local(first.session_id.clone(), first.channel_binding);

        let count = Arc::clone(&establishments);
        let mut second =
            open_pooled_local_object_rpc_stream(scope, Duration::from_secs(1), async move {
                count.fetch_add(1, Ordering::Relaxed);
                Err(NetworkError::Io {
                    operation: "unexpected local carrier establishment".to_string(),
                    detail: "the existing local carrier should have been reused".to_string(),
                })
            })
            .await
            .expect("second local logical stream");
        let second_context = RpcPeerContext::unauthenticated_local(
            second.session_id.clone(),
            second.channel_binding,
        );
        // Owned mux streams publish OPEN together with their first record.
        // A byte-compatibility flush has no record-boundary meaning, so write
        // one byte to exercise the adapter and make each stream observable.
        first
            .write_all(b"a")
            .await
            .expect("publish first local logical open");
        second
            .write_all(b"b")
            .await
            .expect("publish second local logical open");

        let (server_first, server_second) = server.await.expect("server task");
        assert_eq!(first_context, server_first);
        assert_eq!(second_context, server_second);
        assert_eq!(first_context.session_id, second_context.session_id);
        assert_eq!(
            first_context.channel_binding,
            second_context.channel_binding
        );
        assert_eq!(establishments.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn retiring_pool_entry_does_not_interrupt_an_active_stream() {
        let (client_carrier, server_carrier) = carrier_pair();
        let local = client_carrier.local_node_id.clone();
        let peer = client_carrier.peer_node_id.clone();
        let scope = Id::generate().to_string();
        let route_compatibility_generation = crate::network_module_routing_generation();
        let mut acceptor = accept_routed_stream_carrier(server_carrier);
        let server = tokio::spawn(async move {
            let mut stream = acceptor
                .accept()
                .await
                .expect("accept logical stream")
                .expect("logical stream");
            let mut request = [0_u8; 4];
            stream.read_exact(&mut request).await.expect("read request");
            assert_eq!(&request, b"ping");
            stream.write_all(b"pong").await.expect("write response");
        });

        let mut stream = open_test_pooled_routed_stream(
            scope.clone(),
            route_compatibility_generation,
            1,
            RouteConstraints::default(),
            local,
            peer,
            Duration::from_secs(1),
            async move { Ok(client_carrier) },
        )
        .await
        .expect("logical stream");
        assert_eq!(retire_pooled_routed_streams(&scope).await, 1);

        tokio::time::timeout(Duration::from_secs(1), async {
            stream.write_all(b"ping").await.expect("write request");
            let mut response = [0_u8; 4];
            stream
                .read_exact(&mut response)
                .await
                .expect("read response");
            assert_eq!(&response, b"pong");
            server.await.expect("server task");
        })
        .await
        .expect("active stream survived pool retirement");
    }

    #[tokio::test]
    async fn retiring_idle_pool_entry_gracefully_closes_the_carrier() {
        let (client_carrier, server_carrier) = carrier_pair();
        let local = client_carrier.local_node_id.clone();
        let peer = client_carrier.peer_node_id.clone();
        let scope = Id::generate().to_string();
        let route_compatibility_generation = crate::network_module_routing_generation();
        let mut acceptor = accept_routed_stream_carrier(server_carrier);

        let mut stream = open_test_pooled_routed_stream(
            scope.clone(),
            route_compatibility_generation,
            1,
            RouteConstraints::default(),
            local,
            peer,
            Duration::from_secs(1),
            async move { Ok(client_carrier) },
        )
        .await
        .expect("logical stream");
        stream
            .write_all(b"x")
            .await
            .expect("publish idle logical stream");
        stream.shutdown().await.expect("close idle logical stream");
        let accepted = acceptor
            .accept()
            .await
            .expect("accept logical stream")
            .expect("logical stream");
        drop(stream);
        drop(accepted);
        assert_eq!(retire_pooled_routed_streams(&scope).await, 1);

        let closed = tokio::time::timeout(Duration::from_secs(1), acceptor.accept())
            .await
            .expect("carrier close propagated")
            .expect("clean carrier close");
        assert!(closed.is_none());
    }

    #[tokio::test]
    async fn logical_stream_transfers_bodies_larger_than_its_flow_control_window() {
        const REQUEST_BYTES: usize = 512 * 1024 + 31;
        const RESPONSE_BYTES: usize = 768 * 1024 + 47;

        let (client_carrier, server_carrier) = carrier_pair();
        let local = client_carrier.local_node_id.clone();
        let peer = client_carrier.peer_node_id.clone();
        let scope = Id::generate().to_string();
        let route_compatibility_generation = crate::network_module_routing_generation();
        let mut acceptor = accept_routed_stream_carrier(server_carrier);
        let server = tokio::spawn(async move {
            let mut stream = acceptor
                .accept()
                .await
                .expect("accept logical stream")
                .expect("logical stream");
            let mut request = Vec::new();
            stream
                .read_to_end(&mut request)
                .await
                .expect("read request body");
            assert_eq!(request.len(), REQUEST_BYTES);
            assert!(request.iter().all(|byte| *byte == 0x41));
            stream
                .write_all(&vec![0x52; RESPONSE_BYTES])
                .await
                .expect("write response body");
            stream.shutdown().await.expect("finish response body");
        });

        let stream = open_test_pooled_routed_stream(
            scope,
            route_compatibility_generation,
            1,
            RouteConstraints::default(),
            local,
            peer,
            Duration::from_secs(1),
            async move { Ok(client_carrier) },
        )
        .await
        .expect("logical stream");
        let (mut reader, mut writer) = tokio::io::split(stream);
        let send = tokio::spawn(async move {
            writer
                .write_all(&vec![0x41; REQUEST_BYTES])
                .await
                .expect("write request body");
            writer.shutdown().await.expect("finish request body");
        });
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), async {
            reader
                .read_to_end(&mut response)
                .await
                .expect("read response body");
            send.await.expect("request writer");
            server.await.expect("server task");
        })
        .await
        .expect("large logical transfer completed without a flow-control deadlock");
        eprintln!("chunk metrics: {:?}", crate::network_chunk_metrics());
        assert_eq!(response.len(), RESPONSE_BYTES);
        assert!(response.iter().all(|byte| *byte == 0x52));
    }

    #[tokio::test]
    async fn stalled_logical_stream_does_not_block_an_unrelated_stream() {
        let (client_carrier, server_carrier) = carrier_pair();
        let local = client_carrier.local_node_id.clone();
        let peer = client_carrier.peer_node_id.clone();
        let scope = Id::generate().to_string();
        let route_compatibility_generation = crate::network_module_routing_generation();
        let mut acceptor = accept_routed_stream_carrier(server_carrier);
        let (stalled_accepted, wait_for_stalled_accept) = oneshot::channel();
        let server = tokio::spawn(async move {
            let _stalled = acceptor
                .accept()
                .await
                .expect("accept stalled stream")
                .expect("stalled stream");
            let _ = stalled_accepted.send(());
            let mut healthy = acceptor
                .accept()
                .await
                .expect("accept healthy stream")
                .expect("healthy stream");
            let mut request = [0_u8; 4];
            healthy
                .read_exact(&mut request)
                .await
                .expect("read healthy request");
            assert_eq!(&request, b"ping");
            healthy
                .write_all(b"pong")
                .await
                .expect("write healthy response");
        });

        let mut stalled = open_test_pooled_routed_stream(
            scope.clone(),
            route_compatibility_generation,
            1,
            RouteConstraints::default(),
            local.clone(),
            peer.clone(),
            Duration::from_secs(1),
            async move { Ok(client_carrier) },
        )
        .await
        .expect("stalled logical stream");
        let stalled_writer = tokio::spawn(async move {
            let bytes = vec![0x5a; crate::owned_chunk::RPC_RECORD_MAX * 32];
            stalled.write_all(&bytes).await
        });
        wait_for_stalled_accept
            .await
            .expect("stalled stream was published first");

        let mut healthy = open_test_pooled_routed_stream(
            scope.clone(),
            route_compatibility_generation,
            1,
            RouteConstraints::default(),
            local,
            peer,
            Duration::from_secs(1),
            async move {
                Err(NetworkError::Io {
                    operation: "unexpected carrier establishment".to_string(),
                    detail: "the healthy stream must share the existing carrier".to_string(),
                })
            },
        )
        .await
        .expect("healthy logical stream");

        tokio::time::timeout(Duration::from_secs(1), async {
            healthy
                .write_all(b"ping")
                .await
                .expect("write healthy request");
            let mut response = [0_u8; 4];
            healthy
                .read_exact(&mut response)
                .await
                .expect("read healthy response");
            assert_eq!(&response, b"pong");
            server.await.expect("server task");
        })
        .await
        .expect("stalled stream must not head-of-line block the carrier");
        stalled_writer.abort();
        let _ = stalled_writer.await;
        retire_pooled_routed_streams(&scope).await;
    }
}

#[tokio::test]
async fn delayed_old_generation_cannot_replace_or_coexist_with_a_new_pooled_carrier() {
    let (old_client, old_server) = tests::carrier_pair();
    let (new_client, new_server) = tests::carrier_pair();
    let local = old_client.local_node_id.clone();
    let peer = old_client.peer_node_id.clone();
    let new_session = new_client.session_id.clone();
    let scope = Id::generate().to_string();
    let generation = Arc::new(std::sync::atomic::AtomicU64::new(7));
    let (old_establishment_started, wait_for_old_establishment) = tokio::sync::oneshot::channel();
    let (release_old_establishment, wait_for_old_release) = tokio::sync::oneshot::channel();
    let old_scope = scope.clone();
    let old_local = local.clone();
    let old_peer = peer.clone();
    let old_generation = Arc::clone(&generation);
    let old_open = tokio::spawn(async move {
        open_pooled_routed_stream_with_route_compatibility_source(
            old_scope,
            7,
            41,
            RouteConstraints::default(),
            crate::DestinationProcessAffinity::Any,
            old_local,
            old_peer,
            crate::RoutedSessionProtection::Plaintext,
            Duration::from_secs(1),
            async move {
                old_establishment_started
                    .send(())
                    .expect("signal old establishment");
                wait_for_old_release
                    .await
                    .expect("release old establishment");
                Ok(old_client)
            },
            move || old_generation.load(std::sync::atomic::Ordering::Acquire),
        )
        .await
    });
    wait_for_old_establishment
        .await
        .expect("old generation reached carrier establishment");

    generation.store(8, std::sync::atomic::Ordering::Release);
    let new_generation = Arc::clone(&generation);
    let new_stream = open_pooled_routed_stream_with_route_compatibility_source(
        scope.clone(),
        8,
        41,
        RouteConstraints::default(),
        crate::DestinationProcessAffinity::Any,
        local.clone(),
        peer.clone(),
        crate::RoutedSessionProtection::Plaintext,
        Duration::from_secs(1),
        async move { Ok(new_client) },
        move || new_generation.load(std::sync::atomic::Ordering::Acquire),
    )
    .await
    .expect("new generation carrier");
    assert_eq!(new_stream.session_id, new_session);

    release_old_establishment
        .send(())
        .expect("finish old establishment");
    assert!(matches!(
        old_open.await.expect("old open task"),
        Err(NetworkError::Directory { .. })
    ));

    let reuse_generation = Arc::clone(&generation);
    let reused = open_pooled_routed_stream_with_route_compatibility_source(
        scope.clone(),
        8,
        99,
        RouteConstraints::default(),
        crate::DestinationProcessAffinity::Any,
        local,
        peer,
        crate::RoutedSessionProtection::Plaintext,
        Duration::from_secs(1),
        async move {
            Err(NetworkError::Io {
                operation: "unexpected carrier establishment".to_string(),
                detail: "the new-generation carrier must remain reusable".to_string(),
            })
        },
        move || reuse_generation.load(std::sync::atomic::Ordering::Acquire),
    )
    .await
    .expect("reuse new generation carrier");
    assert_eq!(reused.session_id, new_session);
    let retained_generations = lock_routed_stream_pool()
        .keys()
        .filter(|key| key.scope == scope)
        .map(|key| key.route_compatibility_generation)
        .collect::<Vec<_>>();
    assert_eq!(retained_generations, vec![8]);

    drop(new_stream);
    drop(reused);
    assert_eq!(retire_pooled_routed_streams(&scope).await, 1);
    drop(old_server);
    drop(new_server);
}

#[test]
fn routed_carrier_pool_never_crosses_protection_modes() {
    let local = NodeId::from_legacy_named("pool-protection-local").expect("local node id");
    let peer = NodeId::from_legacy_named("pool-protection-peer").expect("peer node id");
    let plaintext = PoolKey::new(
        "pool-protection".to_string(),
        1,
        1,
        RouteConstraints::default(),
        crate::DestinationProcessAffinity::Any,
        local.clone(),
        peer.clone(),
        crate::RoutedSessionProtection::Plaintext,
    );
    let aead = PoolKey::new(
        "pool-protection".to_string(),
        1,
        1,
        RouteConstraints::default(),
        crate::DestinationProcessAffinity::Any,
        local,
        peer,
        crate::RoutedSessionProtection::Aead,
    );

    assert_ne!(plaintext, aead);
    assert!(!plaintext.same_relationship(&aead));
}

#[test]
fn routed_carrier_pool_never_crosses_destination_process_affinities() {
    let local = NodeId::from_legacy_named("pool-affinity-local").expect("local node id");
    let peer = NodeId::from_legacy_named("pool-affinity-peer").expect("peer node id");
    let any = PoolKey::new(
        "pool-affinity".to_string(),
        1,
        1,
        RouteConstraints::default(),
        crate::DestinationProcessAffinity::Any,
        local.clone(),
        peer.clone(),
        crate::RoutedSessionProtection::Plaintext,
    );
    let main = PoolKey::new(
        "pool-affinity".to_string(),
        1,
        1,
        RouteConstraints::default(),
        crate::DestinationProcessAffinity::MainDaemon,
        local,
        peer,
        crate::RoutedSessionProtection::Plaintext,
    );

    assert_ne!(any, main);
    assert!(!any.same_relationship(&main));
}

#[tokio::test]
async fn main_affinity_never_reuses_an_existing_process_agnostic_carrier() {
    let (any_client, any_server) = tests::carrier_pair();
    let (main_client, main_server) = tests::carrier_pair();
    let local = any_client.local_node_id.clone();
    let peer = any_client.peer_node_id.clone();
    let any_session = any_client.session_id.clone();
    let main_session = main_client.session_id.clone();
    let scope = Id::generate().to_string();
    let generation = crate::network_module_routing_generation();

    let any_stream = open_pooled_routed_stream_with_route_compatibility_source(
        scope.clone(),
        generation,
        1,
        RouteConstraints::default(),
        crate::DestinationProcessAffinity::Any,
        local.clone(),
        peer.clone(),
        crate::RoutedSessionProtection::Plaintext,
        Duration::from_secs(1),
        async move { Ok(any_client) },
        move || generation,
    )
    .await
    .expect("process-agnostic shadow-capable carrier");
    assert_eq!(any_stream.session_id, any_session);

    let main_stream = open_pooled_routed_stream_with_route_compatibility_source(
        scope.clone(),
        generation,
        1,
        RouteConstraints::default(),
        crate::DestinationProcessAffinity::MainDaemon,
        local,
        peer,
        crate::RoutedSessionProtection::Plaintext,
        Duration::from_secs(1),
        async move { Ok(main_client) },
        move || generation,
    )
    .await
    .expect("main-affine carrier");
    assert_eq!(main_stream.session_id, main_session);
    assert_ne!(
        main_stream.session_id, any_stream.session_id,
        "a main-affine Raft/KV call must establish a new relationship instead of reusing an Any carrier that may terminate at shadow"
    );

    drop(any_stream);
    drop(main_stream);
    assert_eq!(retire_pooled_routed_streams(&scope).await, 2);
    drop(any_server);
    drop(main_server);
}

#[test]
fn routed_carrier_pool_never_crosses_module_compatibility_generations() {
    let local = NodeId::from_legacy_named("pool-module-generation-local").expect("local node id");
    let peer = NodeId::from_legacy_named("pool-module-generation-peer").expect("peer node id");
    let key = |route_compatibility_generation| {
        PoolKey::new(
            "pool-module-generation".to_string(),
            route_compatibility_generation,
            41,
            RouteConstraints::default(),
            crate::DestinationProcessAffinity::Any,
            local.clone(),
            peer.clone(),
            crate::RoutedSessionProtection::Plaintext,
        )
    };

    let before = key(7);
    let after = key(8);
    assert_ne!(before, after);
    assert!(before.same_base_relationship(&after));
    assert!(
        !before.same_relationship(&after),
        "a registry transition must retire the old carrier relationship even when the distributed directory generation is unchanged"
    );
    let mut next_directory_revision = before.clone();
    next_directory_revision.directory_generation += 1;
    assert!(
        before.same_relationship(&next_directory_revision),
        "an unrelated distributed directory revision must preserve compatible carrier reuse"
    );
    let mut unrelated_peer = before.clone();
    unrelated_peer.peer_node_id =
        NodeId::from_legacy_named("pool-module-generation-unrelated-peer").expect("peer node id");
    assert!(
        !before.same_base_relationship(&unrelated_peer),
        "compatibility pruning must remain scoped to the exact node relationship"
    );
}
