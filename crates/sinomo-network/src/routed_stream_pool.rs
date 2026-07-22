//! Routing-owned pooling and multiplexing for authenticated routed streams.
//!
//! A physical [`PeerStream`] is an end-to-end authenticated carrier. Calls
//! receive independently cancellable logical `PeerStream`s over that carrier;
//! application protocols never participate in connection reuse.

use crate::{
    BoxedNetworkStream, NetworkDeliveryState, NetworkError, PeerStream, RouteConstraints,
    RouteTrace, RpcPeerContext, RpcPeerIdentity,
};
use bytes::Bytes;
use ring::rand::{SecureRandom, SystemRandom};
use sinomo_base::Id;
use sinomo_node::NodeId;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{
    Arc, Mutex, OnceLock, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::PollSender;

const ROUTED_STREAM_MUX_MAGIC: [u8; 4] = *b"SRM1";
const LOCAL_OBJECT_RPC_CARRIER_PRELUDE_MAGIC: [u8; 4] = *b"SLC1";
const LOCAL_OBJECT_RPC_CARRIER_MAX_SESSION_ID_BYTES: usize = 128;
const ROUTED_STREAM_MUX_HEADER_BYTES: usize = 17;
const ROUTED_STREAM_MUX_DATA_BYTES: usize = 64 * 1024;
const ROUTED_STREAM_MUX_COMMANDS: usize = 64;
const ROUTED_STREAM_MUX_INCOMING_FRAMES: usize = 8;
const ROUTED_STREAM_MUX_ACCEPTED_STREAMS: usize = 32;
const ROUTED_STREAM_MUX_INITIAL_WINDOW_FRAMES: usize = ROUTED_STREAM_MUX_INCOMING_FRAMES;
const FRAME_OPEN: u8 = 1;
const FRAME_ACCEPT: u8 = 2;
const FRAME_DATA: u8 = 3;
const FRAME_FINISH: u8 = 4;
const FRAME_RESET: u8 = 5;
const FRAME_WINDOW: u8 = 6;
const FRAME_DATA_FINISH: u8 = 7;
const FRAME_OPEN_DATA_FINISH: u8 = 8;
// Process-local command consumed by the mux writer; never serialized.
const FRAME_CARRIER_CLOSE: u8 = u8::MAX;

#[derive(Debug)]
struct MuxFrame {
    kind: u8,
    stream_id: u64,
    payload: Bytes,
}

impl MuxFrame {
    fn control(kind: u8, stream_id: u64) -> Self {
        Self {
            kind,
            stream_id,
            payload: Bytes::new(),
        }
    }
}

#[derive(Debug)]
enum LogicalInbound {
    Data(Bytes),
    Reset,
}

fn mux_io_error(operation: &str, error: std::io::Error) -> NetworkError {
    NetworkError::Io {
        operation: operation.to_string(),
        detail: error.to_string(),
    }
}

/// Identifies a loopback-only multiplexed carrier before any logical RPC is
/// accepted. Each logical stream still carries its own per-call transcript and
/// signed service request; this routing prelude grants no service authority.
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

async fn write_mux_frame<W>(writer: &mut W, frame: MuxFrame) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    if frame.stream_id == 0 || frame.payload.len() > ROUTED_STREAM_MUX_DATA_BYTES {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: frame.payload.len(),
            max_bytes: ROUTED_STREAM_MUX_DATA_BYTES,
        });
    }
    if !matches!(
        frame.kind,
        FRAME_DATA | FRAME_DATA_FINISH | FRAME_OPEN_DATA_FINISH
    ) && !frame.payload.is_empty()
    {
        return Err(NetworkError::Serialization {
            detail: "routed stream mux control frame carried a payload".to_string(),
        });
    }
    let payload_bytes =
        u32::try_from(frame.payload.len()).map_err(|_| NetworkError::FrameTooLarge {
            actual_bytes: frame.payload.len(),
            max_bytes: ROUTED_STREAM_MUX_DATA_BYTES,
        })?;
    let mut encoded =
        Vec::with_capacity(ROUTED_STREAM_MUX_HEADER_BYTES.saturating_add(frame.payload.len()));
    encoded.extend_from_slice(&ROUTED_STREAM_MUX_MAGIC);
    encoded.push(frame.kind);
    encoded.extend_from_slice(&frame.stream_id.to_be_bytes());
    encoded.extend_from_slice(&payload_bytes.to_be_bytes());
    encoded.extend_from_slice(&frame.payload);
    writer
        .write_all(&encoded)
        .await
        .map_err(|error| mux_io_error("write routed stream mux frame", error))
}

async fn read_mux_frame<R>(reader: &mut R) -> Result<Option<MuxFrame>, NetworkError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0_u8; ROUTED_STREAM_MUX_HEADER_BYTES];
    let first = reader
        .read(&mut header[..1])
        .await
        .map_err(|error| mux_io_error("read routed stream mux header", error))?;
    if first == 0 {
        return Ok(None);
    }
    reader
        .read_exact(&mut header[1..])
        .await
        .map_err(|error| mux_io_error("read routed stream mux header", error))?;
    if header[..4] != ROUTED_STREAM_MUX_MAGIC {
        return Err(NetworkError::Serialization {
            detail: "invalid routed stream mux magic".to_string(),
        });
    }
    let kind = header[4];
    if !matches!(
        kind,
        FRAME_OPEN
            | FRAME_ACCEPT
            | FRAME_DATA
            | FRAME_FINISH
            | FRAME_RESET
            | FRAME_WINDOW
            | FRAME_DATA_FINISH
            | FRAME_OPEN_DATA_FINISH
    ) {
        return Err(NetworkError::Serialization {
            detail: format!("unknown routed stream mux frame kind {kind}"),
        });
    }
    let stream_id = u64::from_be_bytes(header[5..13].try_into().expect("eight-byte stream id"));
    if stream_id == 0 {
        return Err(NetworkError::Serialization {
            detail: "routed stream mux stream id must be nonzero".to_string(),
        });
    }
    let payload_bytes =
        u32::from_be_bytes(header[13..].try_into().expect("four-byte payload length")) as usize;
    if payload_bytes > ROUTED_STREAM_MUX_DATA_BYTES {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: payload_bytes,
            max_bytes: ROUTED_STREAM_MUX_DATA_BYTES,
        });
    }
    if !matches!(
        kind,
        FRAME_DATA | FRAME_DATA_FINISH | FRAME_OPEN_DATA_FINISH
    ) && payload_bytes != 0
    {
        return Err(NetworkError::Serialization {
            detail: "routed stream mux control frame declared a payload".to_string(),
        });
    }
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(payload_bytes)
        .map_err(|_| NetworkError::ResourceExhausted {
            operation: "allocate routed stream mux frame".to_string(),
            requested_bytes: payload_bytes,
        })?;
    payload.resize(payload_bytes, 0);
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|error| mux_io_error("read routed stream mux payload", error))?;
    Ok(Some(MuxFrame {
        kind,
        stream_id,
        payload: Bytes::from(payload),
    }))
}

async fn write_mux_batch<W>(
    writer: &mut W,
    mut batch: VecDeque<MuxFrame>,
) -> Result<bool, NetworkError>
where
    W: AsyncWrite + Unpin,
{
    while let Some(frame) = batch.pop_front() {
        if frame.kind == FRAME_CARRIER_CLOSE {
            return Ok(false);
        }
        if frame.kind == FRAME_OPEN
            && batch
                .front()
                .is_some_and(|next| next.kind == FRAME_CARRIER_CLOSE)
        {
            // The logical stream never published bytes. Retiring its carrier
            // must not create a remote stream only to close it.
            return Ok(false);
        }
        if frame.kind == FRAME_OPEN
            && batch
                .front()
                .is_some_and(|data| data.kind == FRAME_DATA && data.stream_id == frame.stream_id)
            && batch.get(1).is_some_and(|finish| {
                finish.kind == FRAME_FINISH && finish.stream_id == frame.stream_id
            })
        {
            let data = batch.pop_front().expect("checked open data frame");
            let _finish = batch.pop_front();
            write_mux_frame(
                &mut *writer,
                MuxFrame {
                    kind: FRAME_OPEN_DATA_FINISH,
                    stream_id: frame.stream_id,
                    payload: data.payload,
                },
            )
            .await?;
            continue;
        }
        if frame.kind == FRAME_DATA
            && batch
                .front()
                .is_some_and(|next| next.kind == FRAME_FINISH && next.stream_id == frame.stream_id)
        {
            let _finish = batch.pop_front();
            write_mux_frame(
                &mut *writer,
                MuxFrame {
                    kind: FRAME_DATA_FINISH,
                    stream_id: frame.stream_id,
                    payload: frame.payload,
                },
            )
            .await?;
            continue;
        }
        write_mux_frame(&mut *writer, frame).await?;
    }
    writer
        .flush()
        .await
        .map_err(|error| mux_io_error("flush routed stream mux carrier", error))?;
    Ok(true)
}

async fn run_mux_writer<W>(
    mut writer: W,
    mut commands: mpsc::Receiver<MuxFrame>,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    while let Some(frame) = commands.recv().await {
        let mut batch = vec![frame];
        if batch[0].kind == FRAME_OPEN && commands.is_empty() {
            // PollSender can yield once while reserving capacity for the first
            // DATA frame even though the bounded command queue has room. Keep
            // an otherwise solitary OPEN local until the producer's next
            // command so a small half-closed RPC is emitted as one
            // OPEN_DATA_FINISH frame. This does not add a timer or scheduler
            // yield: the receiver is woken directly by DATA/FINISH. A stream
            // that never writes consumes no remote resources; its OPEN is
            // published when it next writes, closes, or the carrier retires.
            if let Some(next) = commands.recv().await {
                batch.push(next);
            }
        }
        while let Ok(frame) = commands.try_recv() {
            batch.push(frame);
        }
        if !write_mux_batch(&mut writer, VecDeque::from(batch)).await? {
            break;
        }
    }
    writer
        .shutdown()
        .await
        .map_err(|error| mux_io_error("close routed stream mux carrier", error))
}

fn logical_stream_metadata(
    carrier_session_id: &Id,
    carrier_binding: &[u8; 32],
    stream_id: u64,
) -> (Id, [u8; 32]) {
    let mut id_hasher = blake3::Hasher::new();
    id_hasher.update(carrier_session_id.as_str().as_bytes());
    id_hasher.update(&stream_id.to_be_bytes());
    let session_id = Id::derived(
        "sinomo.network.routed-logical-stream",
        id_hasher.finalize().as_bytes(),
    );
    let mut binding_hasher = blake3::Hasher::new();
    binding_hasher.update(b"sinomo.network.routed-logical-stream.binding.v1\0");
    binding_hasher.update(carrier_binding);
    binding_hasher.update(&stream_id.to_be_bytes());
    (session_id, *binding_hasher.finalize().as_bytes())
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
    PeerStream::new(
        io,
        local_node_id,
        peer_node_id,
        context.session_id.clone(),
        context.channel_binding,
        local_object_rpc_carrier_route(context, client_side),
    )
}

fn logical_stream_pair(
    stream_id: u64,
    commands: mpsc::Sender<MuxFrame>,
    carrier_owner: Option<Arc<ServerCarrierOwner>>,
) -> (
    BoxedNetworkStream,
    mpsc::Sender<LogicalInbound>,
    Weak<Semaphore>,
) {
    let (incoming_sender, incoming) = mpsc::channel(ROUTED_STREAM_MUX_INCOMING_FRAMES);
    let outbound_window = Arc::new(Semaphore::new(ROUTED_STREAM_MUX_INITIAL_WINDOW_FRAMES));
    let outbound_window_weak = Arc::downgrade(&outbound_window);
    (
        Box::new(LogicalStream {
            stream_id,
            read_commands: PollSender::new(commands.clone()),
            write_commands: PollSender::new(commands.clone()),
            drop_commands: commands,
            incoming,
            current: Bytes::new(),
            current_offset: 0,
            pending_window_credits: 0,
            outbound_window,
            window_acquire: None,
            outbound_credit_reserved: false,
            read_closed: false,
            write_closed: false,
            _carrier_owner: carrier_owner,
        }),
        incoming_sender,
        outbound_window_weak,
    )
}

type WindowAcquire =
    Pin<Box<dyn Future<Output = Result<OwnedSemaphorePermit, AcquireError>> + Send>>;

struct LogicalStream {
    stream_id: u64,
    read_commands: PollSender<MuxFrame>,
    write_commands: PollSender<MuxFrame>,
    drop_commands: mpsc::Sender<MuxFrame>,
    incoming: mpsc::Receiver<LogicalInbound>,
    current: Bytes,
    current_offset: usize,
    pending_window_credits: usize,
    outbound_window: Arc<Semaphore>,
    window_acquire: Option<WindowAcquire>,
    outbound_credit_reserved: bool,
    read_closed: bool,
    write_closed: bool,
    _carrier_owner: Option<Arc<ServerCarrierOwner>>,
}

fn logical_stream_closed_error(operation: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        format!("routed logical stream {operation}: carrier closed"),
    )
}

impl LogicalStream {
    fn poll_window_credits(&mut self, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        while self.pending_window_credits != 0 {
            match self.read_commands.poll_reserve(context) {
                Poll::Ready(Ok(())) => {
                    self.read_commands
                        .send_item(MuxFrame::control(FRAME_WINDOW, self.stream_id))
                        .map_err(|_| logical_stream_closed_error("return window credit"))?;
                    self.pending_window_credits -= 1;
                }
                Poll::Ready(Err(_)) => {
                    return Poll::Ready(Err(logical_stream_closed_error("return window credit")));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_outbound_credit(&mut self, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.outbound_credit_reserved {
            return Poll::Ready(Ok(()));
        }
        if self.window_acquire.is_none() {
            match Arc::clone(&self.outbound_window).try_acquire_owned() {
                Ok(permit) => {
                    permit.forget();
                    self.outbound_credit_reserved = true;
                    return Poll::Ready(Ok(()));
                }
                Err(tokio::sync::TryAcquireError::Closed) => {
                    return Poll::Ready(Err(logical_stream_closed_error(
                        "reserve outbound window",
                    )));
                }
                Err(tokio::sync::TryAcquireError::NoPermits) => {}
            }
        }
        let acquire = self
            .window_acquire
            .get_or_insert_with(|| Box::pin(Arc::clone(&self.outbound_window).acquire_owned()));
        match acquire.as_mut().poll(context) {
            Poll::Ready(Ok(permit)) => {
                permit.forget();
                self.window_acquire = None;
                self.outbound_credit_reserved = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(_)) => {
                Poll::Ready(Err(logical_stream_closed_error("reserve outbound window")))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncRead for LogicalStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buffer.remaining() == 0 || self.read_closed {
            return Poll::Ready(Ok(()));
        }
        loop {
            match self.poll_window_credits(context) {
                Poll::Ready(Ok(())) => {}
                other => return other,
            }
            if self.current_offset < self.current.len() {
                let available = &self.current[self.current_offset..];
                let copied = available.len().min(buffer.remaining());
                buffer.put_slice(&available[..copied]);
                self.current_offset += copied;
                if self.current_offset == self.current.len() {
                    self.current = Bytes::new();
                    self.current_offset = 0;
                    self.pending_window_credits += 1;
                }
                return Poll::Ready(Ok(()));
            }
            match self.incoming.poll_recv(context) {
                Poll::Ready(Some(LogicalInbound::Data(payload))) => {
                    self.current = payload;
                    self.current_offset = 0;
                }
                Poll::Ready(Some(LogicalInbound::Reset)) => {
                    self.read_closed = true;
                    return Poll::Ready(Err(logical_stream_closed_error("read")));
                }
                Poll::Ready(None) => {
                    self.read_closed = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AsyncWrite for LogicalStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.write_closed {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "routed logical stream write direction is closed",
            )));
        }
        if buffer.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match self.poll_outbound_credit(context) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        let written = buffer.len().min(ROUTED_STREAM_MUX_DATA_BYTES);
        let frame = MuxFrame {
            kind: FRAME_DATA,
            stream_id: self.stream_id,
            payload: Bytes::copy_from_slice(&buffer[..written]),
        };
        match self.drop_commands.try_send(frame) {
            Ok(()) => {
                // The common small-RPC path already has bounded queue
                // capacity. Keep DATA and the immediately following FINISH in
                // the producer's current poll so the mux writer can collapse
                // OPEN + DATA + FINISH into one encrypted carrier record.
                // PollSender remains the backpressure path when the queue is
                // actually full; no capacity or flow-control limit changes.
                self.outbound_credit_reserved = false;
                return Poll::Ready(Ok(written));
            }
            Err(mpsc::error::TrySendError::Full(frame)) => {
                match self.write_commands.poll_reserve(context) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(_)) => {
                        return Poll::Ready(Err(logical_stream_closed_error("write")));
                    }
                    Poll::Pending => return Poll::Pending,
                }
                if self.write_commands.send_item(frame).is_err() {
                    return Poll::Ready(Err(logical_stream_closed_error("write")));
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Poll::Ready(Err(logical_stream_closed_error("write")));
            }
        }
        self.outbound_credit_reserved = false;
        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.write_closed {
            return Poll::Ready(Ok(()));
        }
        let stream_id = self.stream_id;
        let frame = MuxFrame::control(FRAME_FINISH, stream_id);
        match self.drop_commands.try_send(frame) {
            Ok(()) => {
                self.write_closed = true;
                return Poll::Ready(Ok(()));
            }
            Err(mpsc::error::TrySendError::Full(frame)) => {
                match self.write_commands.poll_reserve(context) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(_)) => {
                        return Poll::Ready(Err(logical_stream_closed_error("shutdown")));
                    }
                    Poll::Pending => return Poll::Pending,
                }
                if self.write_commands.send_item(frame).is_err() {
                    return Poll::Ready(Err(logical_stream_closed_error("shutdown")));
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Poll::Ready(Err(logical_stream_closed_error("shutdown")));
            }
        }
        self.write_closed = true;
        Poll::Ready(Ok(()))
    }
}

impl Drop for LogicalStream {
    fn drop(&mut self) {
        if !self.write_closed {
            let _ = self
                .drop_commands
                .try_send(MuxFrame::control(FRAME_RESET, self.stream_id));
        }
    }
}

#[derive(Default)]
struct ClientMuxState {
    streams: BTreeMap<u64, mpsc::Sender<LogicalInbound>>,
    outbound_windows: BTreeMap<u64, Weak<Semaphore>>,
}

fn fail_client_mux(state: &Mutex<ClientMuxState>, closed: &AtomicBool, detail: impl Into<String>) {
    closed.store(true, Ordering::Release);
    let _detail = detail.into();
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    for (_, sender) in std::mem::take(&mut state.streams) {
        let _ = sender.try_send(LogicalInbound::Reset);
    }
    for (_, window) in std::mem::take(&mut state.outbound_windows) {
        if let Some(window) = window.upgrade() {
            window.close();
        }
    }
}

fn return_outbound_window(window: &Weak<Semaphore>) -> Result<(), NetworkError> {
    let Some(window) = window.upgrade() else {
        return Ok(());
    };
    if window.available_permits() >= ROUTED_STREAM_MUX_INITIAL_WINDOW_FRAMES {
        return Err(NetworkError::Serialization {
            detail: "routed stream mux received excess window credit".to_string(),
        });
    }
    window.add_permits(1);
    Ok(())
}

struct RoutedStreamClient {
    local_node_id: NodeId,
    peer_node_id: NodeId,
    carrier_session_id: Id,
    carrier_binding: [u8; 32],
    route_trace: RouteTrace,
    commands: mpsc::Sender<MuxFrame>,
    state: Arc<Mutex<ClientMuxState>>,
    closed: Arc<AtomicBool>,
    next_stream_id: AtomicU64,
    reader_task: Option<JoinHandle<()>>,
    writer_task: Option<JoinHandle<()>>,
}

struct PooledLogicalStream {
    io: BoxedNetworkStream,
    _carrier_owner: Arc<RoutedStreamClient>,
}

impl AsyncRead for PooledLogicalStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.io).poll_read(context, buffer)
    }
}

impl AsyncWrite for PooledLogicalStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut *self.io).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.io).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut *self.io).poll_shutdown(context)
    }
}

impl RoutedStreamClient {
    fn new(carrier: PeerStream) -> Self {
        let local_node_id = carrier.local_node_id.clone();
        let peer_node_id = carrier.peer_node_id.clone();
        let carrier_session_id = carrier.session_id.clone();
        let carrier_binding = carrier.channel_binding;
        let route_trace = carrier.route_trace.clone();
        let (reader, writer) = tokio::io::split(carrier);
        let (commands, command_receiver) = mpsc::channel(ROUTED_STREAM_MUX_COMMANDS);
        let state = Arc::new(Mutex::new(ClientMuxState::default()));
        let closed = Arc::new(AtomicBool::new(false));
        let writer_state = Arc::clone(&state);
        let writer_closed = Arc::clone(&closed);
        let writer_task = tokio::spawn(async move {
            if let Err(error) = run_mux_writer(writer, command_receiver).await {
                fail_client_mux(&writer_state, &writer_closed, error.to_string());
            }
        });
        let reader_state = Arc::clone(&state);
        let reader_closed = Arc::clone(&closed);
        let reader_commands = commands.clone();
        let reader_task = tokio::spawn(async move {
            let mut reader = reader;
            loop {
                let frame = match read_mux_frame(&mut reader).await {
                    Ok(Some(frame)) => frame,
                    Ok(None) => {
                        fail_client_mux(
                            &reader_state,
                            &reader_closed,
                            "routed stream mux carrier closed",
                        );
                        return;
                    }
                    Err(error) => {
                        fail_client_mux(&reader_state, &reader_closed, error.to_string());
                        return;
                    }
                };
                match frame.kind {
                    // Older servers acknowledge OPEN. New clients deliberately
                    // do not wait for this end-to-end round trip: ordering on
                    // the carrier already guarantees OPEN precedes DATA.
                    FRAME_ACCEPT => {}
                    FRAME_DATA | FRAME_DATA_FINISH => {
                        let finishes = frame.kind == FRAME_DATA_FINISH;
                        let sender = reader_state
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .streams
                            .get(&frame.stream_id)
                            .cloned();
                        if let Some(sender) = sender {
                            if sender
                                .send(LogicalInbound::Data(frame.payload))
                                .await
                                .is_err()
                            {
                                reader_state
                                    .lock()
                                    .unwrap_or_else(|error| error.into_inner())
                                    .streams
                                    .remove(&frame.stream_id);
                                let _ = reader_commands
                                    .send(MuxFrame::control(FRAME_RESET, frame.stream_id))
                                    .await;
                            } else if finishes {
                                reader_state
                                    .lock()
                                    .unwrap_or_else(|error| error.into_inner())
                                    .streams
                                    .remove(&frame.stream_id);
                            }
                        } else {
                            let _ = reader_commands
                                .send(MuxFrame::control(FRAME_RESET, frame.stream_id))
                                .await;
                        }
                    }
                    FRAME_FINISH => {
                        let sender = reader_state
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .streams
                            .remove(&frame.stream_id);
                        drop(sender);
                    }
                    FRAME_RESET => {
                        let (sender, window) = {
                            let mut state = reader_state
                                .lock()
                                .unwrap_or_else(|error| error.into_inner());
                            (
                                state.streams.remove(&frame.stream_id),
                                state.outbound_windows.remove(&frame.stream_id),
                            )
                        };
                        if let Some(sender) = sender {
                            let _ = sender.send(LogicalInbound::Reset).await;
                        }
                        if let Some(window) = window.and_then(|window| window.upgrade()) {
                            window.close();
                        }
                    }
                    FRAME_WINDOW => {
                        let window = reader_state
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .outbound_windows
                            .get(&frame.stream_id)
                            .cloned();
                        if let Some(window) = window
                            && let Err(error) = return_outbound_window(&window)
                        {
                            fail_client_mux(&reader_state, &reader_closed, error.to_string());
                            return;
                        }
                    }
                    FRAME_OPEN | FRAME_OPEN_DATA_FINISH => {
                        fail_client_mux(
                            &reader_state,
                            &reader_closed,
                            "routed stream mux server attempted to open a client stream",
                        );
                        return;
                    }
                    _ => unreachable!("validated mux frame kind"),
                }
            }
        });
        Self {
            local_node_id,
            peer_node_id,
            carrier_session_id,
            carrier_binding,
            route_trace,
            commands,
            state,
            closed,
            next_stream_id: AtomicU64::new(1),
            reader_task: Some(reader_task),
            writer_task: Some(writer_task),
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    async fn open(self: &Arc<Self>, timeout: Duration) -> Result<PeerStream, NetworkError> {
        if self.is_closed() {
            return Err(NetworkError::delivery_failed(
                "open pooled routed logical stream",
                NetworkDeliveryState::DefinitelyNotSent,
                "routed stream carrier is closed",
            ));
        }
        let stream_id = self.next_stream_id.fetch_add(2, Ordering::Relaxed);
        if stream_id == 0 || stream_id.checked_add(2).is_none() {
            self.closed.store(true, Ordering::Release);
            return Err(NetworkError::ResourceExhausted {
                operation: "allocate routed logical stream id".to_string(),
                requested_bytes: std::mem::size_of::<u64>(),
            });
        }
        let (io, incoming, outbound_window) =
            logical_stream_pair(stream_id, self.commands.clone(), None);
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state
                .outbound_windows
                .retain(|_, window| window.strong_count() != 0);
            state.streams.insert(stream_id, incoming);
            state.outbound_windows.insert(stream_id, outbound_window);
        }
        // Opening is unilateral: carrier ordering guarantees OPEN precedes
        // application DATA, so no remote acknowledgement round trip is needed.
        match tokio::time::timeout(
            timeout,
            self.commands.send(MuxFrame::control(FRAME_OPEN, stream_id)),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                fail_client_mux(
                    &self.state,
                    &self.closed,
                    "routed stream mux writer stopped",
                );
                return Err(NetworkError::delivery_failed(
                    "open pooled routed logical stream",
                    NetworkDeliveryState::DefinitelyNotSent,
                    "routed stream mux writer stopped",
                ));
            }
            Err(_) => {
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                state.streams.remove(&stream_id);
                if let Some(window) = state.outbound_windows.remove(&stream_id)
                    && let Some(window) = window.upgrade()
                {
                    window.close();
                }
                return Err(NetworkError::timeout(
                    "queue pooled routed logical stream open",
                    crate::NetworkTimeoutPhase::Connect,
                    timeout,
                    NetworkDeliveryState::DefinitelyNotSent,
                ));
            }
        }
        let (session_id, channel_binding) =
            logical_stream_metadata(&self.carrier_session_id, &self.carrier_binding, stream_id);
        PeerStream::new(
            Box::new(PooledLogicalStream {
                io,
                _carrier_owner: Arc::clone(self),
            }),
            self.local_node_id.clone(),
            self.peer_node_id.clone(),
            session_id,
            channel_binding,
            self.route_trace.clone(),
        )
    }
}

impl Drop for RoutedStreamClient {
    fn drop(&mut self) {
        let _ = self
            .commands
            .try_send(MuxFrame::control(FRAME_CARRIER_CLOSE, 1));
        if let Some(reader_task) = self.reader_task.take() {
            reader_task.abort();
        }
        // Detach rather than abort. Dropping the final command sender below
        // lets the writer drain queued mux frames, close its write half, and
        // emit the encrypted carrier's authenticated final frame. Aborting it
        // makes a healthy peer observe an unauthenticated early EOF.
        drop(self.writer_task.take());
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PoolKey {
    scope: String,
    directory_generation: u64,
    local_node_id: NodeId,
    peer_node_id: NodeId,
    require_full_duplex: bool,
    require_confidentiality: bool,
    require_relay_forwarding: bool,
    max_hops: usize,
}

static ROUTED_STREAM_POOL: OnceLock<
    tokio::sync::Mutex<BTreeMap<PoolKey, Arc<RoutedStreamClient>>>,
> = OnceLock::new();
static ROUTED_STREAM_CONNECT_LOCKS: OnceLock<
    tokio::sync::Mutex<BTreeMap<PoolKey, Arc<tokio::sync::Mutex<()>>>>,
> = OnceLock::new();

fn routed_stream_pool() -> &'static tokio::sync::Mutex<BTreeMap<PoolKey, Arc<RoutedStreamClient>>> {
    ROUTED_STREAM_POOL.get_or_init(|| tokio::sync::Mutex::new(BTreeMap::new()))
}

fn routed_stream_connect_locks()
-> &'static tokio::sync::Mutex<BTreeMap<PoolKey, Arc<tokio::sync::Mutex<()>>>> {
    ROUTED_STREAM_CONNECT_LOCKS.get_or_init(|| tokio::sync::Mutex::new(BTreeMap::new()))
}

/// Opens a logical stream from the current generation's routing-owned carrier.
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
    let key = PoolKey {
        scope,
        directory_generation,
        local_node_id,
        peer_node_id,
        require_full_duplex: constraints.require_full_duplex,
        require_confidentiality: constraints.require_confidentiality,
        require_relay_forwarding: constraints.require_relay_forwarding,
        max_hops: constraints.max_hops,
    };
    let existing = {
        let mut pool = routed_stream_pool().lock().await;
        pool.retain(|pool_key, client| {
            !client.is_closed()
                && (pool_key.scope != key.scope
                    || pool_key.local_node_id != key.local_node_id
                    || pool_key.directory_generation >= key.directory_generation)
        });
        pool.get(&key).cloned()
    };
    if let Some(client) = existing {
        match client.open(timeout).await {
            Ok(stream) => return Ok(stream),
            Err(_) => {
                let mut pool = routed_stream_pool().lock().await;
                if pool
                    .get(&key)
                    .is_some_and(|current| Arc::ptr_eq(current, &client))
                {
                    pool.remove(&key);
                }
            }
        }
    }

    let connect_lock = {
        let mut locks = routed_stream_connect_locks().lock().await;
        locks.retain(|lock_key, _| {
            lock_key.scope != key.scope
                || lock_key.local_node_id != key.local_node_id
                || lock_key.directory_generation >= key.directory_generation
        });
        Arc::clone(
            locks
                .entry(key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    };
    let connect_guard = connect_lock.lock().await;
    let existing = {
        let pool = routed_stream_pool().lock().await;
        pool.get(&key).filter(|client| !client.is_closed()).cloned()
    };
    let client = match existing {
        Some(client) => client,
        None => {
            let carrier = establish.await?;
            if carrier.local_node_id != key.local_node_id
                || carrier.peer_node_id != key.peer_node_id
            {
                return Err(NetworkError::AuthDenied);
            }
            let client = Arc::new(RoutedStreamClient::new(carrier));
            routed_stream_pool()
                .lock()
                .await
                .insert(key.clone(), Arc::clone(&client));
            client
        }
    };
    drop(connect_guard);
    match client.open(timeout).await {
        Ok(stream) => Ok(stream),
        Err(error) => {
            let mut pool = routed_stream_pool().lock().await;
            if pool
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, &client))
            {
                pool.remove(&key);
            }
            Err(error)
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
    open_pooled_routed_stream(
        format!("local-object-rpc:{scope}"),
        0,
        RouteConstraints::default(),
        local_node_id,
        peer_node_id,
        timeout,
        carrier,
    )
    .await
}

/// Retires every pooled routed carrier in one cluster/process scope.
///
/// Daemon shutdown and tests use this explicit lifecycle boundary. Directory
/// generation changes are retired automatically by `open_pooled_routed_stream`.
pub async fn retire_pooled_routed_streams(scope: &str) -> usize {
    let mut pool = routed_stream_pool().lock().await;
    let before = pool.len();
    pool.retain(|key, _| key.scope != scope);
    let retired = before.saturating_sub(pool.len());
    drop(pool);
    routed_stream_connect_locks()
        .lock()
        .await
        .retain(|key, _| key.scope != scope);
    retired
}

/// Server side of one accepted routing-owned carrier.
pub struct RoutedStreamAcceptor {
    incoming: mpsc::Receiver<Result<PeerStream, NetworkError>>,
    _carrier_owner: Arc<ServerCarrierOwner>,
}

#[derive(Default)]
struct ServerCarrierOwner {
    reader_task: Mutex<Option<JoinHandle<()>>>,
    writer_task: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for ServerCarrierOwner {
    fn drop(&mut self) {
        if let Some(task) = self
            .reader_task
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            task.abort();
        }
        let writer_task = self
            .writer_task
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        // Detach rather than abort: once the reader and logical bridges drop
        // their senders, the writer drains every queued DATA/FINISH frame and
        // then shuts down the carrier cleanly.
        drop(writer_task);
    }
}

impl RoutedStreamAcceptor {
    pub async fn accept(&mut self) -> Result<Option<PeerStream>, NetworkError> {
        match self.incoming.recv().await {
            Some(Ok(stream)) => Ok(Some(stream)),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }
}

/// Converts one accepted authenticated routed stream into a logical-stream
/// acceptor. The first frame must be the routing mux protocol; no legacy
/// application-stream fallback is retained.
pub fn accept_routed_stream_carrier(carrier: PeerStream) -> RoutedStreamAcceptor {
    let local_node_id = carrier.local_node_id.clone();
    let peer_node_id = carrier.peer_node_id.clone();
    let carrier_session_id = carrier.session_id.clone();
    let carrier_binding = carrier.channel_binding;
    let route_trace = carrier.route_trace.clone();
    let (reader, writer) = tokio::io::split(carrier);
    let (commands, command_receiver) = mpsc::channel(ROUTED_STREAM_MUX_COMMANDS);
    let (accepted, incoming) = mpsc::channel(ROUTED_STREAM_MUX_ACCEPTED_STREAMS);
    let carrier_owner = Arc::new(ServerCarrierOwner::default());
    let writer_task = tokio::spawn(async move {
        if let Err(error) = run_mux_writer(writer, command_receiver).await {
            tracing::debug!(%error, "routed stream mux writer stopped");
        }
    });
    *carrier_owner
        .writer_task
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(writer_task);
    let reader_commands = commands.clone();
    let reader_carrier_owner = Arc::downgrade(&carrier_owner);
    let reader_task = tokio::spawn(async move {
        let mut reader = reader;
        let mut streams = BTreeMap::<u64, mpsc::Sender<LogicalInbound>>::new();
        let mut outbound_windows = BTreeMap::<u64, Weak<Semaphore>>::new();
        loop {
            let frame = match read_mux_frame(&mut reader).await {
                Ok(Some(frame)) => frame,
                Ok(None) => return,
                Err(error) => {
                    let _ = accepted.send(Err(error)).await;
                    return;
                }
            };
            match frame.kind {
                FRAME_OPEN | FRAME_OPEN_DATA_FINISH => {
                    let opens_finished_stream = frame.kind == FRAME_OPEN_DATA_FINISH;
                    let opens_with_data = frame.kind == FRAME_OPEN_DATA_FINISH;
                    let opening_payload = frame.payload;
                    if frame.stream_id % 2 == 0
                        || streams.contains_key(&frame.stream_id)
                        || outbound_windows
                            .get(&frame.stream_id)
                            .is_some_and(|window| window.strong_count() != 0)
                    {
                        let _ = accepted.send(Err(NetworkError::AuthDenied)).await;
                        return;
                    }
                    let Some(logical_carrier_owner) = reader_carrier_owner.upgrade() else {
                        return;
                    };
                    let (io, inbound, outbound_window) = logical_stream_pair(
                        frame.stream_id,
                        reader_commands.clone(),
                        Some(logical_carrier_owner),
                    );
                    let (session_id, channel_binding) = logical_stream_metadata(
                        &carrier_session_id,
                        &carrier_binding,
                        frame.stream_id,
                    );
                    let stream = match PeerStream::new(
                        io,
                        local_node_id.clone(),
                        peer_node_id.clone(),
                        session_id,
                        channel_binding,
                        route_trace.clone(),
                    ) {
                        Ok(stream) => stream,
                        Err(error) => {
                            let _ = accepted.send(Err(error)).await;
                            return;
                        }
                    };
                    outbound_windows.retain(|_, window| window.strong_count() != 0);
                    outbound_windows.insert(frame.stream_id, outbound_window);
                    if opens_with_data
                        && inbound
                            .send(LogicalInbound::Data(opening_payload))
                            .await
                            .is_err()
                    {
                        let _ = reader_commands
                            .send(MuxFrame::control(FRAME_RESET, frame.stream_id))
                            .await;
                        continue;
                    }
                    if opens_finished_stream {
                        drop(inbound);
                    } else {
                        streams.insert(frame.stream_id, inbound);
                    }
                    if accepted.send(Ok(stream)).await.is_err() {
                        return;
                    }
                }
                FRAME_DATA | FRAME_DATA_FINISH => {
                    let finishes = frame.kind == FRAME_DATA_FINISH;
                    let sender = streams.get(&frame.stream_id).cloned();
                    if let Some(sender) = sender {
                        if sender
                            .send(LogicalInbound::Data(frame.payload))
                            .await
                            .is_err()
                        {
                            streams.remove(&frame.stream_id);
                            let _ = reader_commands
                                .send(MuxFrame::control(FRAME_RESET, frame.stream_id))
                                .await;
                        } else if finishes {
                            streams.remove(&frame.stream_id);
                        }
                    } else {
                        let _ = reader_commands
                            .send(MuxFrame::control(FRAME_RESET, frame.stream_id))
                            .await;
                    }
                }
                FRAME_FINISH => {
                    drop(streams.remove(&frame.stream_id));
                }
                FRAME_RESET => {
                    let sender = streams.remove(&frame.stream_id);
                    let window = outbound_windows.remove(&frame.stream_id);
                    if let Some(sender) = sender {
                        let _ = sender.send(LogicalInbound::Reset).await;
                    }
                    if let Some(window) = window.and_then(|window| window.upgrade()) {
                        window.close();
                    }
                }
                FRAME_WINDOW => {
                    let window = outbound_windows.get(&frame.stream_id).cloned();
                    if let Some(window) = window
                        && let Err(error) = return_outbound_window(&window)
                    {
                        let _ = accepted.send(Err(error)).await;
                        return;
                    }
                }
                FRAME_ACCEPT => {
                    let _ = accepted.send(Err(NetworkError::AuthDenied)).await;
                    return;
                }
                _ => unreachable!("validated mux frame kind"),
            }
        }
    });
    *carrier_owner
        .reader_task
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(reader_task);
    RoutedStreamAcceptor {
        incoming,
        _carrier_owner: carrier_owner,
    }
}

/// Serves logical streams while reading their carrier on the calling task.
///
/// This is the destination-daemon hot path. A compound inline request can be
/// decoded and handled directly from the carrier read without first crossing
/// the acceptor channel and waking a second task. Handlers that genuinely
/// block are spawned so the same carrier can continue delivering frames for
/// other logical streams.
pub async fn serve_routed_stream_carrier<F, Fut>(
    carrier: PeerStream,
    mut handler: F,
) -> Result<bool, NetworkError>
where
    F: FnMut(PeerStream) -> Fut,
    Fut: Future<Output = Result<bool, NetworkError>> + Send + 'static,
{
    let local_node_id = carrier.local_node_id.clone();
    let peer_node_id = carrier.peer_node_id.clone();
    let carrier_session_id = carrier.session_id.clone();
    let carrier_binding = carrier.channel_binding;
    let route_trace = carrier.route_trace.clone();
    let (mut reader, mut writer) = tokio::io::split(carrier);
    let (commands, mut command_receiver) = mpsc::channel(ROUTED_STREAM_MUX_COMMANDS);
    let carrier_owner = Arc::new(ServerCarrierOwner::default());

    let mut streams = BTreeMap::<u64, mpsc::Sender<LogicalInbound>>::new();
    let mut outbound_windows = BTreeMap::<u64, Weak<Semaphore>>::new();
    let mut handlers = tokio::task::JoinSet::new();
    let mut stop = false;
    'carrier: loop {
        let frame = read_mux_frame(&mut reader);
        tokio::pin!(frame);
        let frame = loop {
            tokio::select! {
                biased;
                command = command_receiver.recv() => {
                    let Some(command) = command else {
                        return Err(NetworkError::Io {
                            operation: "serve routed stream mux carrier".to_string(),
                            detail: "routed stream mux command channel closed".to_string(),
                        });
                    };
                    let mut batch = vec![command];
                    while let Ok(command) = command_receiver.try_recv() {
                        batch.push(command);
                    }
                    if !write_mux_batch(&mut writer, VecDeque::from(batch)).await? {
                        break 'carrier;
                    }
                }
                frame = &mut frame => break frame?,
                joined = handlers.join_next(), if !handlers.is_empty() => {
                    match joined {
                        Some(Ok(Ok(true))) => {
                            handlers.abort_all();
                            stop = true;
                            break 'carrier;
                        }
                        Some(Ok(Ok(false))) => {}
                        Some(Ok(Err(error))) => return Err(error),
                        Some(Err(error)) => {
                            return Err(NetworkError::Io {
                                operation: "join routed logical stream handler".to_string(),
                                detail: error.to_string(),
                            });
                        }
                        None => {}
                    }
                }
            }
        };
        let Some(frame) = frame else {
            break;
        };
        match frame.kind {
            FRAME_OPEN | FRAME_OPEN_DATA_FINISH => {
                let opens_finished_stream = frame.kind == FRAME_OPEN_DATA_FINISH;
                let opening_payload = frame.payload;
                if frame.stream_id % 2 == 0
                    || streams.contains_key(&frame.stream_id)
                    || outbound_windows
                        .get(&frame.stream_id)
                        .is_some_and(|window| window.strong_count() != 0)
                {
                    return Err(NetworkError::AuthDenied);
                }
                let (io, inbound, outbound_window) = logical_stream_pair(
                    frame.stream_id,
                    commands.clone(),
                    Some(Arc::clone(&carrier_owner)),
                );
                let (session_id, channel_binding) =
                    logical_stream_metadata(&carrier_session_id, &carrier_binding, frame.stream_id);
                let stream = PeerStream::new(
                    io,
                    local_node_id.clone(),
                    peer_node_id.clone(),
                    session_id,
                    channel_binding,
                    route_trace.clone(),
                )?;
                outbound_windows.retain(|_, window| window.strong_count() != 0);
                outbound_windows.insert(frame.stream_id, outbound_window);
                if opens_finished_stream {
                    if inbound
                        .send(LogicalInbound::Data(opening_payload))
                        .await
                        .is_err()
                    {
                        let _ = commands
                            .send(MuxFrame::control(FRAME_RESET, frame.stream_id))
                            .await;
                        continue;
                    }
                    drop(inbound);
                } else {
                    streams.insert(frame.stream_id, inbound);
                }

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
                        handlers.abort_all();
                        stop = true;
                        break 'carrier;
                    }
                    Some(Ok(false)) => {}
                    Some(Err(error)) => return Err(error),
                    None => {
                        handlers.spawn(future);
                    }
                }
            }
            FRAME_DATA | FRAME_DATA_FINISH => {
                let finishes = frame.kind == FRAME_DATA_FINISH;
                let sender = streams.get(&frame.stream_id).cloned();
                if let Some(sender) = sender {
                    if sender
                        .send(LogicalInbound::Data(frame.payload))
                        .await
                        .is_err()
                    {
                        streams.remove(&frame.stream_id);
                        let _ = commands
                            .send(MuxFrame::control(FRAME_RESET, frame.stream_id))
                            .await;
                    } else if finishes {
                        streams.remove(&frame.stream_id);
                    }
                } else {
                    let _ = commands
                        .send(MuxFrame::control(FRAME_RESET, frame.stream_id))
                        .await;
                }
            }
            FRAME_FINISH => {
                drop(streams.remove(&frame.stream_id));
            }
            FRAME_RESET => {
                let sender = streams.remove(&frame.stream_id);
                let window = outbound_windows.remove(&frame.stream_id);
                if let Some(sender) = sender {
                    let _ = sender.send(LogicalInbound::Reset).await;
                }
                if let Some(window) = window.and_then(|window| window.upgrade()) {
                    window.close();
                }
            }
            FRAME_WINDOW => {
                let window = outbound_windows.get(&frame.stream_id).cloned();
                if let Some(window) = window {
                    return_outbound_window(&window)?;
                }
            }
            FRAME_ACCEPT => return Err(NetworkError::AuthDenied),
            _ => unreachable!("validated mux frame kind"),
        }
    }
    while !handlers.is_empty() {
        tokio::select! {
            biased;
            command = command_receiver.recv() => {
                let Some(command) = command else { break; };
                let mut batch = vec![command];
                while let Ok(command) = command_receiver.try_recv() {
                    batch.push(command);
                }
                if !write_mux_batch(&mut writer, VecDeque::from(batch)).await? {
                    break;
                }
            }
            joined = handlers.join_next() => {
                match joined {
                    Some(Ok(Ok(true))) => {
                        stop = true;
                        handlers.abort_all();
                    }
                    Some(Ok(Ok(false))) => {}
                    Some(Ok(Err(error))) => return Err(error),
                    Some(Err(error)) => {
                        return Err(NetworkError::Io {
                            operation: "join routed logical stream handler".to_string(),
                            detail: error.to_string(),
                        });
                    }
                    None => {}
                }
            }
        }
    }
    drop(streams);
    drop(carrier_owner);
    drop(commands);
    while let Ok(command) = command_receiver.try_recv() {
        let mut batch = vec![command];
        while let Ok(command) = command_receiver.try_recv() {
            batch.push(command);
        }
        if !write_mux_batch(&mut writer, VecDeque::from(batch)).await? {
            break;
        }
    }
    writer
        .shutdown()
        .await
        .map_err(|error| mux_io_error("close routed stream mux carrier", error))?;
    Ok(stop)
}

/// Serves loopback-only logical RPC streams through the same carrier-native
/// path used by routed peers after the daemon has validated locality and read
/// the `SLC1` prelude.
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
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::oneshot;

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn carrier_pair() -> (PeerStream, PeerStream) {
        let client_node = node("node_mux_client");
        let server_node = node("node_mux_server");
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
            PeerStream::new(
                Box::new(client),
                client_node,
                server_node,
                session_id.clone(),
                binding,
                client_route,
            )
            .expect("client carrier"),
            PeerStream::new(
                Box::new(server),
                node("node_mux_server"),
                node("node_mux_client"),
                session_id,
                binding,
                server_route,
            )
            .expect("server carrier"),
        )
    }

    #[tokio::test]
    async fn pool_reuses_one_carrier_for_independent_streams() {
        let (client_carrier, server_carrier) = carrier_pair();
        let local = client_carrier.local_node_id.clone();
        let peer = client_carrier.peer_node_id.clone();
        let scope = Id::generate().to_string();
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
        let mut first = open_pooled_routed_stream(
            scope.clone(),
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
        let mut second = open_pooled_routed_stream(
            scope,
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

        assert_ne!(first.session_id, second.session_id);
        first.flush().await.expect("publish first logical open");
        second.flush().await.expect("publish second logical open");
        let _ = server_done.send(());
        let (server_first, server_second) = server.await.expect("server task");
        assert_eq!(first.session_id, server_first);
        assert_eq!(second.session_id, server_second);
        assert_eq!(establishments.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn logical_open_does_not_wait_for_remote_accept() {
        let (client_carrier, _server_carrier) = carrier_pair();
        let client = Arc::new(RoutedStreamClient::new(client_carrier));

        let stream = tokio::time::timeout(
            Duration::from_millis(50),
            client.open(Duration::from_millis(10)),
        )
        .await
        .expect("logical open must not wait for a remote acknowledgement")
        .expect("logical stream");

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
        let client = Arc::new(RoutedStreamClient::new(client_carrier));

        for _ in 0..2 {
            let mut stream = client
                .open(Duration::from_secs(1))
                .await
                .expect("open logical stream");
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
    async fn local_rpc_pool_reuses_carrier_but_not_call_context() {
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
        first
            .flush()
            .await
            .expect("publish first local logical open");
        second
            .flush()
            .await
            .expect("publish second local logical open");

        let (server_first, server_second) = server.await.expect("server task");
        assert_eq!(first_context, server_first);
        assert_eq!(second_context, server_second);
        assert_ne!(first_context.session_id, second_context.session_id);
        assert_ne!(
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

        let mut stream = open_pooled_routed_stream(
            scope.clone(),
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
        let mut acceptor = accept_routed_stream_carrier(server_carrier);

        let mut stream = open_pooled_routed_stream(
            scope.clone(),
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
            .shutdown()
            .await
            .expect("publish idle logical stream close");
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
            assert_eq!(request, vec![0x41; REQUEST_BYTES]);
            stream
                .write_all(&vec![0x52; RESPONSE_BYTES])
                .await
                .expect("write response body");
            stream.shutdown().await.expect("finish response body");
        });

        let stream = open_pooled_routed_stream(
            scope,
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
        assert_eq!(response, vec![0x52; RESPONSE_BYTES]);
    }

    #[tokio::test]
    async fn stalled_logical_stream_does_not_block_an_unrelated_stream() {
        let (client_carrier, server_carrier) = carrier_pair();
        let local = client_carrier.local_node_id.clone();
        let peer = client_carrier.peer_node_id.clone();
        let scope = Id::generate().to_string();
        let mut acceptor = accept_routed_stream_carrier(server_carrier);
        let server = tokio::spawn(async move {
            let _stalled = acceptor
                .accept()
                .await
                .expect("accept stalled stream")
                .expect("stalled stream");
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

        let mut stalled = open_pooled_routed_stream(
            scope.clone(),
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
            let bytes = vec![0x5a; ROUTED_STREAM_MUX_DATA_BYTES * 32];
            stalled.write_all(&bytes).await
        });

        let mut healthy = open_pooled_routed_stream(
            scope.clone(),
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
