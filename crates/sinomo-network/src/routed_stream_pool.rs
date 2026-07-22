//! Routing-owned pooling and multiplexing for authenticated routed streams.
//!
//! A physical [`PeerStream`] is an end-to-end authenticated carrier. Calls
//! receive independently cancellable logical `PeerStream`s over that carrier;
//! application protocols never participate in connection reuse.

use crate::{
    BoxedNetworkStream, NetworkDeliveryState, NetworkError, PeerStream, RouteConstraints,
    RouteTrace,
};
use bytes::Bytes;
use sinomo_base::Id;
use sinomo_node::NodeId;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex, OnceLock, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;

const ROUTED_STREAM_MUX_MAGIC: [u8; 4] = *b"SRM1";
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
    if frame.kind != FRAME_DATA && !frame.payload.is_empty() {
        return Err(NetworkError::Serialization {
            detail: "routed stream mux control frame carried a payload".to_string(),
        });
    }
    let payload_bytes =
        u32::try_from(frame.payload.len()).map_err(|_| NetworkError::FrameTooLarge {
            actual_bytes: frame.payload.len(),
            max_bytes: ROUTED_STREAM_MUX_DATA_BYTES,
        })?;
    let mut header = [0_u8; ROUTED_STREAM_MUX_HEADER_BYTES];
    header[..4].copy_from_slice(&ROUTED_STREAM_MUX_MAGIC);
    header[4] = frame.kind;
    header[5..13].copy_from_slice(&frame.stream_id.to_be_bytes());
    header[13..].copy_from_slice(&payload_bytes.to_be_bytes());
    writer
        .write_all(&header)
        .await
        .map_err(|error| mux_io_error("write routed stream mux header", error))?;
    writer
        .write_all(&frame.payload)
        .await
        .map_err(|error| mux_io_error("write routed stream mux payload", error))?;
    writer
        .flush()
        .await
        .map_err(|error| mux_io_error("flush routed stream mux frame", error))
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
        FRAME_OPEN | FRAME_ACCEPT | FRAME_DATA | FRAME_FINISH | FRAME_RESET | FRAME_WINDOW
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
    if kind != FRAME_DATA && payload_bytes != 0 {
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

async fn run_mux_writer<W>(
    mut writer: W,
    mut commands: mpsc::Receiver<MuxFrame>,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    while let Some(frame) = commands.recv().await {
        write_mux_frame(&mut writer, frame).await?;
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

fn logical_stream_pair(
    stream_id: u64,
    commands: mpsc::Sender<MuxFrame>,
    carrier_owner: Option<Arc<ServerCarrierOwner>>,
) -> (
    BoxedNetworkStream,
    mpsc::Sender<LogicalInbound>,
    Weak<Semaphore>,
    JoinHandle<()>,
    JoinHandle<()>,
) {
    let (application, bridge) = tokio::io::duplex(ROUTED_STREAM_MUX_DATA_BYTES);
    let (mut bridge_read, mut bridge_write) = tokio::io::split(bridge);
    let (incoming_sender, mut incoming_receiver) = mpsc::channel(ROUTED_STREAM_MUX_INCOMING_FRAMES);
    let outbound_window = Arc::new(Semaphore::new(ROUTED_STREAM_MUX_INITIAL_WINDOW_FRAMES));
    let outbound_window_weak = Arc::downgrade(&outbound_window);
    let outgoing_commands = commands.clone();
    let outgoing = tokio::spawn(async move {
        let _carrier_owner = carrier_owner;
        let mut buffer = Vec::new();
        if buffer
            .try_reserve_exact(ROUTED_STREAM_MUX_DATA_BYTES)
            .is_err()
        {
            let _ = outgoing_commands
                .send(MuxFrame::control(FRAME_RESET, stream_id))
                .await;
            return;
        }
        buffer.resize(ROUTED_STREAM_MUX_DATA_BYTES, 0);
        loop {
            let read = match bridge_read.read(&mut buffer).await {
                Ok(read) => read,
                Err(_) => {
                    let _ = outgoing_commands
                        .send(MuxFrame::control(FRAME_RESET, stream_id))
                        .await;
                    return;
                }
            };
            if read == 0 {
                let _ = outgoing_commands
                    .send(MuxFrame::control(FRAME_FINISH, stream_id))
                    .await;
                return;
            }
            let Ok(permit) = Arc::clone(&outbound_window).acquire_owned().await else {
                return;
            };
            permit.forget();
            let mut payload = Vec::new();
            if payload.try_reserve_exact(read).is_err() {
                let _ = outgoing_commands
                    .send(MuxFrame::control(FRAME_RESET, stream_id))
                    .await;
                return;
            }
            payload.extend_from_slice(&buffer[..read]);
            if outgoing_commands
                .send(MuxFrame {
                    kind: FRAME_DATA,
                    stream_id,
                    payload: Bytes::from(payload),
                })
                .await
                .is_err()
            {
                return;
            }
        }
    });
    let incoming_commands = commands;
    let incoming = tokio::spawn(async move {
        while let Some(message) = incoming_receiver.recv().await {
            match message {
                LogicalInbound::Data(payload) => {
                    if bridge_write.write_all(&payload).await.is_err() {
                        let _ = incoming_commands
                            .send(MuxFrame::control(FRAME_RESET, stream_id))
                            .await;
                        return;
                    }
                    if incoming_commands
                        .send(MuxFrame::control(FRAME_WINDOW, stream_id))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                LogicalInbound::Reset => return,
            }
        }
        let _ = bridge_write.shutdown().await;
    });
    (
        Box::new(application),
        incoming_sender,
        outbound_window_weak,
        outgoing,
        incoming,
    )
}

#[derive(Default)]
struct ClientMuxState {
    streams: BTreeMap<u64, mpsc::Sender<LogicalInbound>>,
    outbound_windows: BTreeMap<u64, Weak<Semaphore>>,
    pending_accepts: BTreeMap<u64, oneshot::Sender<Result<(), String>>>,
}

struct PendingLogicalOpen<'a> {
    state: &'a Mutex<ClientMuxState>,
    commands: mpsc::Sender<MuxFrame>,
    stream_id: u64,
    armed: bool,
}

impl PendingLogicalOpen<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingLogicalOpen<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.pending_accepts.remove(&self.stream_id);
        state.streams.remove(&self.stream_id);
        if let Some(window) = state.outbound_windows.remove(&self.stream_id)
            && let Some(window) = window.upgrade()
        {
            window.close();
        }
        drop(state);
        let _ = self
            .commands
            .try_send(MuxFrame::control(FRAME_RESET, self.stream_id));
    }
}

fn fail_client_mux(state: &Mutex<ClientMuxState>, closed: &AtomicBool, detail: impl Into<String>) {
    closed.store(true, Ordering::Release);
    let detail = detail.into();
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    for (_, sender) in std::mem::take(&mut state.pending_accepts) {
        let _ = sender.send(Err(detail.clone()));
    }
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
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
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
                    FRAME_ACCEPT => {
                        let sender = reader_state
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .pending_accepts
                            .remove(&frame.stream_id);
                        if let Some(sender) = sender {
                            let _ = sender.send(Ok(()));
                        }
                    }
                    FRAME_DATA => {
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
                    FRAME_OPEN => {
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
            reader_task,
            writer_task,
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
        let (io, incoming, outbound_window, _outgoing_task, _incoming_task) =
            logical_stream_pair(stream_id, self.commands.clone(), None);
        let (accept_sender, accept_receiver) = oneshot::channel();
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state
                .outbound_windows
                .retain(|_, window| window.strong_count() != 0);
            state.streams.insert(stream_id, incoming);
            state.outbound_windows.insert(stream_id, outbound_window);
            state.pending_accepts.insert(stream_id, accept_sender);
        }
        let mut pending_open = PendingLogicalOpen {
            state: &self.state,
            commands: self.commands.clone(),
            stream_id,
            armed: true,
        };
        let open_deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| NetworkError::Serialization {
                detail: "routed logical stream timeout exceeds the monotonic clock range"
                    .to_string(),
            })?;
        match tokio::time::timeout_at(
            open_deadline,
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
                return Err(NetworkError::timeout(
                    "queue pooled routed logical stream open",
                    crate::NetworkTimeoutPhase::Connect,
                    timeout,
                    NetworkDeliveryState::DefinitelyNotSent,
                ));
            }
        }
        match tokio::time::timeout_at(open_deadline, accept_receiver).await {
            Ok(Ok(Ok(()))) => pending_open.disarm(),
            Ok(Ok(Err(detail))) => {
                return Err(NetworkError::delivery_failed(
                    "open pooled routed logical stream",
                    NetworkDeliveryState::DefinitelyNotSent,
                    detail,
                ));
            }
            Ok(Err(_)) => {
                return Err(NetworkError::delivery_failed(
                    "open pooled routed logical stream",
                    NetworkDeliveryState::DefinitelyNotSent,
                    "routed stream carrier closed before accepting the logical stream",
                ));
            }
            Err(_) => {
                return Err(NetworkError::timeout(
                    "open pooled routed logical stream",
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
        self.reader_task.abort();
        self.writer_task.abort();
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
                FRAME_OPEN => {
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
                    let (io, inbound, outbound_window, _outgoing_task, _incoming_task) =
                        logical_stream_pair(
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
                    streams.insert(frame.stream_id, inbound);
                    outbound_windows.insert(frame.stream_id, outbound_window);
                    if accepted.send(Ok(stream)).await.is_err() {
                        return;
                    }
                    if reader_commands
                        .send(MuxFrame::control(FRAME_ACCEPT, frame.stream_id))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                FRAME_DATA => {
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
                        }
                    } else {
                        let _ = reader_commands
                            .send(MuxFrame::control(FRAME_RESET, frame.stream_id))
                            .await;
                    }
                }
                FRAME_FINISH => {
                    let sender = streams.remove(&frame.stream_id);
                    drop(sender);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

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
        let first = open_pooled_routed_stream(
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
        let second = open_pooled_routed_stream(
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
        let _ = server_done.send(());
        let (server_first, server_second) = server.await.expect("server task");
        assert_eq!(first.session_id, server_first);
        assert_eq!(second.session_id, server_second);
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
