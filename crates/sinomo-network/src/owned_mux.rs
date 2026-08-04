//! Ownership-native multiplexing for routed carriers.
//!
//! The carrier has one receive task and one transmit task. Logical streams use
//! shared stream slots with bounded chunk rings; body records never cross a
//! per-frame channel and receive credit remains attached to the chunk lease.

use crate::owned_chunk::{
    AuthenticatedChunkReader, AuthenticatedChunkStream, AuthenticatedChunkWriter, ByteCredit,
    ChunkAllocator, ChunkClass, ChunkLease, ChunkStreamFuture, MUX_RECORD_HEADER_BYTES, OwnedChunk,
    RPC_RECORD_MAX, ReceiveCreditLease, ResetReason, observe_chunk_identity,
};
use crate::{NetworkDeliveryState, NetworkError, PeerStream, RouteTrace};
use futures_util::task::AtomicWaker;
use sinomo_base::Id;
use sinomo_node::NodeId;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

const MUX_MAGIC: [u8; 4] = *b"SRM1";
const FRAME_OPEN: u8 = 1;
const FRAME_ACCEPT: u8 = 2;
const FRAME_DATA: u8 = 3;
const FRAME_FINISH: u8 = 4;
const FRAME_RESET: u8 = 5;
const FRAME_WINDOW: u8 = 6;
const FRAME_DATA_FINISH: u8 = 7;
const FRAME_OPEN_DATA_FINISH: u8 = 8;
const FRAME_OPEN_DATA: u8 = 9;
const FRAME_CONNECTION_WINDOW: u8 = 10;
const FRAME_WINDOW_REQUEST: u8 = 11;
const FRAME_CONNECTION_WINDOW_REQUEST: u8 = 12;
const STREAM_WINDOW_BYTES: usize = 64 * 1024 * 1024;
const CONNECTION_WINDOW_BYTES: usize = 512 * 1024 * 1024;
const WINDOW_RETURN_THRESHOLD_BYTES: usize = 8 * 1024 * 1024;
const CONNECTION_WINDOW_RETURN_THRESHOLD_BYTES: usize = 32 * 1024 * 1024;
const TX_QUANTUM_BYTES: usize = crate::owned_chunk::BODY_CHUNK_MAX;
const MAX_CONTROL_COMMANDS: usize = 1024;
const MAX_ACCEPTED_STREAMS: usize = 32;
const INCOMING_DISPATCHER_ACTIVATION_GRACE: Duration = Duration::from_millis(150);

fn mux_error(operation: &str, detail: impl std::fmt::Display) -> NetworkError {
    NetworkError::Io {
        operation: operation.to_string(),
        detail: detail.to_string(),
    }
}

#[derive(Debug)]
enum SlotTerminal {
    Open,
    Finished,
    Reset(String),
}

#[derive(Debug)]
struct ChunkRingState {
    chunks: VecDeque<ChunkLease>,
    terminal: SlotTerminal,
}

struct ChunkRing {
    state: Mutex<ChunkRingState>,
    reader_waker: AtomicWaker,
}

impl std::fmt::Debug for ChunkRing {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ChunkRing").finish_non_exhaustive()
    }
}

impl ChunkRing {
    fn new() -> Self {
        Self {
            state: Mutex::new(ChunkRingState {
                chunks: VecDeque::new(),
                terminal: SlotTerminal::Open,
            }),
            reader_waker: AtomicWaker::new(),
        }
    }

    fn push(&self, chunk: ChunkLease) -> Result<(), NetworkError> {
        let mut state = self.state.lock().map_err(|_| {
            mux_error(
                "queue owned logical stream chunk",
                "chunk ring lock poisoned",
            )
        })?;
        if !matches!(state.terminal, SlotTerminal::Open) {
            return Err(mux_error(
                "queue owned logical stream chunk",
                "logical stream is terminal",
            ));
        }
        state.chunks.push_back(chunk);
        drop(state);
        self.reader_waker.wake();
        Ok(())
    }

    fn finish(&self) {
        if let Ok(mut state) = self.state.lock()
            && matches!(state.terminal, SlotTerminal::Open)
        {
            state.terminal = SlotTerminal::Finished;
        }
        self.reader_waker.wake();
    }

    fn reset(&self, detail: impl Into<String>) {
        if let Ok(mut state) = self.state.lock() {
            state.terminal = SlotTerminal::Reset(detail.into());
            state.chunks.clear();
        }
        self.reader_waker.wake();
    }

    fn fail_if_open(&self, detail: &str) {
        if let Ok(mut state) = self.state.lock()
            && matches!(state.terminal, SlotTerminal::Open)
        {
            state.terminal = SlotTerminal::Reset(detail.to_string());
            state.chunks.clear();
        }
        self.reader_waker.wake();
    }

    fn poll_recv(
        &self,
        context: &mut Context<'_>,
    ) -> Poll<Result<Option<ChunkLease>, NetworkError>> {
        // Register before inspecting the ring. A producer racing this poll
        // either wakes the registered task or leaves a chunk/terminal state
        // for the check below; no readiness token or per-record future is
        // allocated.
        self.reader_waker.register(context.waker());
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => {
                return Poll::Ready(Err(mux_error(
                    "read owned logical stream chunk",
                    "chunk ring lock poisoned",
                )));
            }
        };
        if let Some(chunk) = state.chunks.pop_front() {
            return Poll::Ready(Ok(Some(chunk)));
        }
        match &state.terminal {
            SlotTerminal::Open => Poll::Pending,
            SlotTerminal::Finished => Poll::Ready(Ok(None)),
            SlotTerminal::Reset(detail) => {
                Poll::Ready(Err(mux_error("read owned logical stream chunk", detail)))
            }
        }
    }

    async fn recv(&self) -> Result<Option<ChunkLease>, NetworkError> {
        std::future::poll_fn(|context| self.poll_recv(context)).await
    }
}

#[derive(Debug)]
struct RemoteByteCredit {
    available: AtomicUsize,
    limit: usize,
    closed: AtomicBool,
    changed: Notify,
}

impl RemoteByteCredit {
    fn new(limit: usize) -> Self {
        Self {
            available: AtomicUsize::new(limit),
            limit,
            closed: AtomicBool::new(false),
            changed: Notify::new(),
        }
    }

    async fn acquire<F>(&self, bytes: usize, blocked: F) -> Result<(), NetworkError>
    where
        F: FnOnce(),
    {
        if bytes > self.limit {
            return Err(NetworkError::ResourceExhausted {
                operation: "reserve owned logical stream remote byte credit".to_string(),
                requested_bytes: bytes,
            });
        }
        let mut blocked = Some(blocked);
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(mux_error(
                    "reserve owned logical stream remote byte credit",
                    "carrier closed",
                ));
            }
            let notified = self.changed.notified();
            let mut available = self.available.load(Ordering::Acquire);
            while available >= bytes {
                match self.available.compare_exchange_weak(
                    available,
                    available - bytes,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Ok(()),
                    Err(actual) => available = actual,
                }
            }
            if let Some(blocked) = blocked.take() {
                blocked();
            }
            notified.await;
        }
    }

    fn add(&self, bytes: usize) -> Result<(), NetworkError> {
        let mut available = self.available.load(Ordering::Acquire);
        loop {
            let next = available
                .checked_add(bytes)
                .ok_or_else(|| NetworkError::Serialization {
                    detail: "owned logical stream byte credit overflow".to_string(),
                })?;
            if next > self.limit {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "owned logical stream byte credit exceeded limit {}",
                        self.limit
                    ),
                });
            }
            match self.available.compare_exchange_weak(
                available,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.changed.notify_waiters();
                    return Ok(());
                }
                Err(actual) => available = actual,
            }
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
}

#[derive(Debug)]
struct StreamSlot {
    stream_id: u64,
    inbound: ChunkRing,
    receive_credit: ByteCredit,
    remote_credit: RemoteByteCredit,
    returned_credit: AtomicUsize,
    remote_send_finished: AtomicBool,
    tx: Weak<TxScheduler>,
    connection: Weak<CarrierCredits>,
}

impl StreamSlot {
    fn new(stream_id: u64, tx: &Arc<TxScheduler>, connection: &Arc<CarrierCredits>) -> Arc<Self> {
        Arc::new(Self {
            stream_id,
            inbound: ChunkRing::new(),
            receive_credit: ByteCredit::new(STREAM_WINDOW_BYTES),
            remote_credit: RemoteByteCredit::new(STREAM_WINDOW_BYTES),
            returned_credit: AtomicUsize::new(0),
            remote_send_finished: AtomicBool::new(false),
            tx: Arc::downgrade(tx),
            connection: Arc::downgrade(connection),
        })
    }

    async fn enqueue_received(self: &Arc<Self>, mut chunk: OwnedChunk) -> Result<(), NetworkError> {
        if chunk.class() == ChunkClass::Control {
            // Control slabs have their own bounded connection budget and must
            // remain deliverable while body-data credit is exhausted. Their
            // allocator permit follows the lease until the control record is
            // consumed, so skipping the data WINDOW does not make memory
            // unbounded.
            return self.inbound.push(chunk.into_lease());
        }
        let charged_bytes = chunk.class().resident_bytes();
        let stream_permit = self
            .receive_credit
            .acquire(charged_bytes, "reserve owned stream receive bytes")
            .await?;
        let connection = self.connection.upgrade().ok_or_else(|| {
            mux_error(
                "reserve owned carrier receive bytes",
                "carrier already closed",
            )
        })?;
        let connection_permit = connection
            .receive_credit
            .acquire(charged_bytes, "reserve owned carrier receive bytes")
            .await?;
        let weak_slot = Arc::downgrade(self);
        let weak_connection = Arc::downgrade(&connection);
        let lease = ReceiveCreditLease::new(
            charged_bytes,
            Some(stream_permit),
            Some(connection_permit),
            Some(Arc::new(move |bytes| {
                if let Some(slot) = weak_slot.upgrade() {
                    slot.return_stream_receive_credit(bytes);
                }
                // A delivered body chunk can intentionally outlive its logical
                // stream slot. Connection flow control spans every stream on
                // the persistent carrier, so its credit must not depend on
                // that per-stream allocation still being registered.
                if let Some(connection) = weak_connection.upgrade() {
                    connection.return_receive_credit(bytes);
                }
            })),
        );
        chunk.replace_receive_credit(lease);
        self.inbound.push(chunk.into_lease())
    }

    fn return_stream_receive_credit(&self, bytes: usize) {
        // Stream IDs are never reused. Once the peer has authenticated FINISH,
        // it cannot spend any more per-stream credit, so returning a WINDOW
        // would only put an extra encrypted record ahead of the response head.
        // Connection credit is returned independently by the same lease.
        if self.remote_send_finished.load(Ordering::Acquire) {
            return;
        }
        let returned = self
            .returned_credit
            .fetch_add(bytes, Ordering::AcqRel)
            .saturating_add(bytes);
        if returned >= WINDOW_RETURN_THRESHOLD_BYTES
            && let Some(tx) = self.tx.upgrade()
        {
            let bytes = self.returned_credit.swap(0, Ordering::AcqRel);
            if bytes != 0 {
                tx.enqueue_control(ControlCommand::window(self.stream_id, bytes));
            }
        }
    }

    fn mark_remote_send_finished(&self) {
        self.remote_send_finished.store(true, Ordering::Release);
        // Any accumulated stream credit became irrelevant at FINISH. Dropping
        // it avoids a redundant WINDOW; the connection-level credit lease
        // remains responsible for the resident allocation.
        self.returned_credit.store(0, Ordering::Release);
    }

    fn finish_inbound(&self) {
        self.mark_remote_send_finished();
        self.inbound.finish();
    }

    fn reset(&self, detail: impl Into<String>) {
        self.remote_credit.close();
        self.mark_remote_send_finished();
        self.inbound.reset(detail);
    }

    fn flush_returned_credit(&self) {
        if self.remote_send_finished.load(Ordering::Acquire) {
            self.returned_credit.store(0, Ordering::Release);
            return;
        }
        let bytes = self.returned_credit.swap(0, Ordering::AcqRel);
        if bytes != 0
            && let Some(tx) = self.tx.upgrade()
        {
            tx.enqueue_control(ControlCommand::window(self.stream_id, bytes));
        }
    }
}

#[derive(Debug)]
struct CarrierCredits {
    receive_credit: ByteCredit,
    remote_credit: RemoteByteCredit,
    returned_credit: AtomicUsize,
    tx: Weak<TxScheduler>,
}

impl CarrierCredits {
    fn new(tx: &Arc<TxScheduler>) -> Arc<Self> {
        Arc::new(Self {
            receive_credit: ByteCredit::new(CONNECTION_WINDOW_BYTES),
            remote_credit: RemoteByteCredit::new(CONNECTION_WINDOW_BYTES),
            returned_credit: AtomicUsize::new(0),
            tx: Arc::downgrade(tx),
        })
    }

    fn return_receive_credit(&self, bytes: usize) {
        let returned = self
            .returned_credit
            .fetch_add(bytes, Ordering::AcqRel)
            .saturating_add(bytes);
        if returned >= CONNECTION_WINDOW_RETURN_THRESHOLD_BYTES {
            self.flush_returned_credit();
        }
    }

    fn flush_returned_credit(&self) {
        let bytes = self.returned_credit.swap(0, Ordering::AcqRel);
        if bytes != 0
            && let Some(tx) = self.tx.upgrade()
        {
            tx.enqueue_control(ControlCommand::connection_window(bytes));
        }
    }

    fn close(&self) {
        self.remote_credit.close();
        self.flush_returned_credit();
    }
}

#[derive(Debug)]
struct ControlCommand {
    kind: u8,
    stream_id: u64,
    credit_bytes: Option<usize>,
}

impl ControlCommand {
    fn reset(stream_id: u64) -> Self {
        Self {
            kind: FRAME_RESET,
            stream_id,
            credit_bytes: None,
        }
    }

    fn window(stream_id: u64, bytes: usize) -> Self {
        Self {
            kind: FRAME_WINDOW,
            stream_id,
            credit_bytes: Some(bytes),
        }
    }

    fn connection_window(bytes: usize) -> Self {
        Self {
            kind: FRAME_CONNECTION_WINDOW,
            stream_id: 0,
            credit_bytes: Some(bytes),
        }
    }

    fn window_request(stream_id: u64) -> Self {
        Self {
            kind: FRAME_WINDOW_REQUEST,
            stream_id,
            credit_bytes: None,
        }
    }

    fn connection_window_request() -> Self {
        Self {
            kind: FRAME_CONNECTION_WINDOW_REQUEST,
            stream_id: 0,
            credit_bytes: None,
        }
    }
}

#[derive(Debug)]
struct StreamTxQueue {
    chunks: VecDeque<OwnedChunk>,
    deficit: usize,
    opened: bool,
    finish_requested: bool,
    finish_completion: Option<tokio::sync::oneshot::Sender<()>>,
    active: bool,
}

#[derive(Debug)]
struct TxState {
    controls: VecDeque<ControlCommand>,
    streams: BTreeMap<u64, StreamTxQueue>,
    active: VecDeque<u64>,
    closed: bool,
}

struct TxScheduler {
    state: Mutex<TxState>,
    writer_waker: AtomicWaker,
}

impl std::fmt::Debug for TxScheduler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TxScheduler")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
enum TxWork {
    Data {
        kind: u8,
        stream_id: u64,
        chunk: OwnedChunk,
        finish_completion: Option<tokio::sync::oneshot::Sender<()>>,
    },
    Control {
        command: ControlCommand,
        finish_completion: Option<tokio::sync::oneshot::Sender<()>>,
    },
}

impl TxScheduler {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(TxState {
                controls: VecDeque::new(),
                streams: BTreeMap::new(),
                active: VecDeque::new(),
                closed: false,
            }),
            writer_waker: AtomicWaker::new(),
        })
    }

    fn register_stream(&self, stream_id: u64, already_open: bool) -> Result<(), NetworkError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| mux_error("register owned logical stream", "transmitter lock poisoned"))?;
        if state.closed || state.streams.contains_key(&stream_id) {
            return Err(mux_error(
                "register owned logical stream",
                "duplicate stream or closed carrier",
            ));
        }
        state.streams.insert(
            stream_id,
            StreamTxQueue {
                chunks: VecDeque::new(),
                deficit: 0,
                opened: already_open,
                finish_requested: false,
                finish_completion: None,
                active: false,
            },
        );
        Ok(())
    }

    fn activate(state: &mut TxState, stream_id: u64) {
        if let Some(queue) = state.streams.get_mut(&stream_id)
            && !queue.active
        {
            queue.active = true;
            state.active.push_back(stream_id);
        }
    }

    fn enqueue_data(&self, stream_id: u64, chunk: OwnedChunk) -> Result<(), NetworkError> {
        let mut state = self.state.lock().map_err(|_| {
            mux_error(
                "queue owned logical stream data",
                "transmitter lock poisoned",
            )
        })?;
        let queue = state.streams.get_mut(&stream_id).ok_or_else(|| {
            mux_error(
                "queue owned logical stream data",
                "logical stream is not registered",
            )
        })?;
        if queue.finish_requested {
            return Err(mux_error(
                "queue owned logical stream data",
                "write direction is finished",
            ));
        }
        queue.chunks.push_back(chunk);
        Self::activate(&mut state, stream_id);
        drop(state);
        self.writer_waker.wake();
        Ok(())
    }

    fn finish_stream(
        &self,
        stream_id: u64,
    ) -> Result<tokio::sync::oneshot::Receiver<()>, NetworkError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| mux_error("finish owned logical stream", "transmitter lock poisoned"))?;
        let queue = state.streams.get_mut(&stream_id).ok_or_else(|| {
            mux_error(
                "finish owned logical stream",
                "logical stream is not registered",
            )
        })?;
        let (completion, completed) = tokio::sync::oneshot::channel();
        queue.finish_requested = true;
        queue.finish_completion = Some(completion);
        Self::activate(&mut state, stream_id);
        drop(state);
        self.writer_waker.wake();
        Ok(completed)
    }

    fn reset_stream(&self, stream_id: u64) {
        if let Ok(mut state) = self.state.lock()
            && let Some(queue) = state.streams.remove(&stream_id)
            && queue.opened
            && state.controls.len() < MAX_CONTROL_COMMANDS
        {
            state.controls.push_back(ControlCommand::reset(stream_id));
        }
        self.writer_waker.wake();
    }

    fn enqueue_control(&self, control: ControlCommand) {
        if let Ok(mut state) = self.state.lock() {
            if state.controls.len() >= MAX_CONTROL_COMMANDS {
                state.closed = true;
            } else if !state.closed {
                state.controls.push_back(control);
            }
        }
        self.writer_waker.wake();
    }

    fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            state.streams.clear();
            state.active.clear();
        }
        self.writer_waker.wake();
    }

    fn drain_and_close(&self) {
        if let Ok(mut state) = self.state.lock() {
            // Reject new work, but leave already-owned records in their
            // queues. The persistent writer must stage them on the socket
            // before it emits the authenticated carrier finish.
            state.closed = true;
        }
        self.writer_waker.wake();
    }

    fn data_record_kind(queue: &mut StreamTxQueue, last: bool) -> u8 {
        if !queue.opened {
            queue.opened = true;
            if last {
                FRAME_OPEN_DATA_FINISH
            } else {
                FRAME_OPEN_DATA
            }
        } else if last {
            FRAME_DATA_FINISH
        } else {
            FRAME_DATA
        }
    }

    /// Selects transport-control slabs and ready stream FINISH records before
    /// ordinary body data without changing the order inside any one stream.
    ///
    /// Object-RPC heads, authenticated finishes, and rejection/error records
    /// use the control slab class. They may pass body data from other streams,
    /// but they never pass an earlier record in their own stream queue.
    fn priority_stream_work(state: &mut TxState) -> Option<TxWork> {
        let candidates = state.active.len();
        for _ in 0..candidates {
            let stream_id = state.active.pop_front()?;
            let mut remove = false;
            let mut requeue = false;
            let mut work = None;
            if let Some(queue) = state.streams.get_mut(&stream_id) {
                queue.active = false;
                if queue
                    .chunks
                    .front()
                    .is_some_and(|chunk| chunk.class() == ChunkClass::Control)
                {
                    let chunk = queue.chunks.pop_front().expect("checked control chunk");
                    let last = queue.chunks.is_empty() && queue.finish_requested;
                    let kind = Self::data_record_kind(queue, last);
                    remove = last;
                    requeue = !remove && (!queue.chunks.is_empty() || queue.finish_requested);
                    work = Some(TxWork::Data {
                        kind,
                        stream_id,
                        chunk,
                        finish_completion: if last {
                            queue.finish_completion.take()
                        } else {
                            None
                        },
                    });
                } else if queue.chunks.is_empty() && queue.finish_requested {
                    remove = true;
                    if queue.opened {
                        work = Some(TxWork::Control {
                            command: ControlCommand {
                                kind: FRAME_FINISH,
                                stream_id,
                                credit_bytes: None,
                            },
                            finish_completion: queue.finish_completion.take(),
                        });
                    } else if let Some(completion) = queue.finish_completion.take() {
                        // A never-written logical stream was never published
                        // to the remote carrier.
                        let _ = completion.send(());
                    }
                } else {
                    requeue = !queue.chunks.is_empty() || queue.finish_requested;
                }
            }
            if remove {
                state.streams.remove(&stream_id);
            } else if requeue {
                Self::activate(state, stream_id);
            }
            if work.is_some() {
                return work;
            }
        }
        None
    }

    fn select_next(state: &mut TxState) -> Option<TxWork> {
        if let Some(control) = state.controls.pop_front() {
            return Some(TxWork::Control {
                command: control,
                finish_completion: None,
            });
        }
        if let Some(work) = Self::priority_stream_work(state) {
            return Some(work);
        }

        // Advance deficits synchronously until at least one already-queued
        // record is eligible. A full body record therefore does not require
        // several Tokio task yields merely because it exceeds one quantum.
        // The loop is bounded by the maximum mux record size.
        loop {
            let candidates = state.active.len();
            if candidates == 0 {
                return None;
            }
            let mut needs_more_deficit = false;
            for _ in 0..candidates {
                let Some(stream_id) = state.active.pop_front() else {
                    break;
                };
                let mut remove = false;
                let mut requeue = false;
                let mut work = None;
                if let Some(queue) = state.streams.get_mut(&stream_id) {
                    queue.active = false;
                    queue.deficit = queue.deficit.saturating_add(TX_QUANTUM_BYTES);
                    if let Some(front) = queue.chunks.front() {
                        if front.len() <= queue.deficit {
                            let chunk = queue.chunks.pop_front().expect("checked mux chunk");
                            queue.deficit -= chunk.len();
                            let last = queue.chunks.is_empty() && queue.finish_requested;
                            let kind = Self::data_record_kind(queue, last);
                            remove = last;
                            requeue =
                                !remove && (!queue.chunks.is_empty() || queue.finish_requested);
                            work = Some(TxWork::Data {
                                kind,
                                stream_id,
                                chunk,
                                finish_completion: if last {
                                    queue.finish_completion.take()
                                } else {
                                    None
                                },
                            });
                        } else {
                            requeue = true;
                            needs_more_deficit = true;
                        }
                    } else if queue.finish_requested {
                        if queue.opened {
                            remove = true;
                            work = Some(TxWork::Control {
                                command: ControlCommand {
                                    kind: FRAME_FINISH,
                                    stream_id,
                                    credit_bytes: None,
                                },
                                finish_completion: queue.finish_completion.take(),
                            });
                        } else {
                            // A never-written logical stream was never
                            // published to the remote carrier.
                            remove = true;
                            if let Some(completion) = queue.finish_completion.take() {
                                let _ = completion.send(());
                            }
                        }
                    }
                }
                if remove {
                    state.streams.remove(&stream_id);
                } else if requeue {
                    Self::activate(state, stream_id);
                }
                if work.is_some() {
                    return work;
                }
            }
            if !needs_more_deficit {
                return None;
            }
        }
    }

    fn poll_next(&self, context: &mut Context<'_>) -> Poll<Result<Option<TxWork>, NetworkError>> {
        // Register before locking so enqueue/close racing this poll either
        // wakes the persistent writer or leaves visible scheduler state.
        self.writer_waker.register(context.waker());
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => {
                return Poll::Ready(Err(mux_error(
                    "schedule owned mux record",
                    "transmitter lock poisoned",
                )));
            }
        };
        if let Some(work) = Self::select_next(&mut state) {
            return Poll::Ready(Ok(Some(work)));
        }
        if state.closed {
            return Poll::Ready(Ok(None));
        }
        Poll::Pending
    }

    async fn next(&self) -> Result<Option<TxWork>, NetworkError> {
        std::future::poll_fn(|context| self.poll_next(context)).await
    }
}

struct LogicalChunkReader {
    slot: Arc<StreamSlot>,
    allocator: ChunkAllocator,
    _owner: Arc<OwnedCarrierOwner>,
}

impl AuthenticatedChunkReader for LogicalChunkReader {
    fn allocator(&self) -> &ChunkAllocator {
        &self.allocator
    }

    fn max_payload_bytes(&self) -> usize {
        RPC_RECORD_MAX
    }

    fn recv_chunk(&mut self) -> ChunkStreamFuture<'_, Option<ChunkLease>> {
        Box::pin(self.slot.inbound.recv())
    }
}

struct LogicalChunkWriter {
    slot: Arc<StreamSlot>,
    allocator: ChunkAllocator,
    connection: Arc<CarrierCredits>,
    finished: bool,
    _owner: Arc<OwnedCarrierOwner>,
}

impl Drop for LogicalChunkWriter {
    fn drop(&mut self) {
        if !self.finished
            && let Some(tx) = self.slot.tx.upgrade()
        {
            tracing::debug!(
                stream_id = self.slot.stream_id,
                "resetting owned logical stream because its writer was dropped before finish"
            );
            tx.reset_stream(self.slot.stream_id);
        }
    }
}

impl AuthenticatedChunkWriter for LogicalChunkWriter {
    fn max_payload_bytes(&self) -> usize {
        RPC_RECORD_MAX
    }

    fn allocator(&self) -> &ChunkAllocator {
        &self.allocator
    }

    fn send_chunk(&mut self, chunk: OwnedChunk) -> ChunkStreamFuture<'_, ()> {
        Box::pin(async move {
            if self.finished {
                return Err(mux_error(
                    "write owned logical stream",
                    "write direction is finished",
                ));
            }
            if chunk.len() > RPC_RECORD_MAX {
                return Err(NetworkError::FrameTooLarge {
                    actual_bytes: chunk.len(),
                    max_bytes: RPC_RECORD_MAX,
                });
            }
            let tx = self.slot.tx.upgrade().ok_or_else(|| {
                mux_error("write owned logical stream", "carrier transmitter closed")
            })?;
            if chunk.class() == ChunkClass::Data {
                let charged_bytes = chunk.class().resident_bytes();
                let stream_id = self.slot.stream_id;
                let stream_tx = Arc::clone(&tx);
                self.slot
                    .remote_credit
                    .acquire(charged_bytes, move || {
                        stream_tx.enqueue_control(ControlCommand::window_request(stream_id));
                    })
                    .await?;
                let connection_tx = Arc::clone(&tx);
                self.connection
                    .remote_credit
                    .acquire(charged_bytes, move || {
                        connection_tx.enqueue_control(ControlCommand::connection_window_request());
                    })
                    .await?;
            }
            tx.enqueue_data(self.slot.stream_id, chunk)
        })
    }

    fn finish(&mut self) -> ChunkStreamFuture<'_, ()> {
        Box::pin(async move {
            if self.finished {
                return Ok(());
            }
            self.finished = true;
            let tx = self.slot.tx.upgrade().ok_or_else(|| {
                mux_error("finish owned logical stream", "carrier transmitter closed")
            })?;
            tx.finish_stream(self.slot.stream_id)?.await.map_err(|_| {
                mux_error(
                    "finish owned logical stream",
                    "carrier stopped before staging the final record",
                )
            })
        })
    }

    fn reset(&mut self, reason: ResetReason) -> ChunkStreamFuture<'_, ()> {
        Box::pin(async move {
            self.finished = true;
            self.slot.reset(reason.detail);
            if let Some(tx) = self.slot.tx.upgrade() {
                tx.reset_stream(self.slot.stream_id);
            }
            Ok(())
        })
    }
}

fn logical_chunk_stream(
    slot: Arc<StreamSlot>,
    allocator: ChunkAllocator,
    connection: Arc<CarrierCredits>,
    owner: Arc<OwnedCarrierOwner>,
) -> Result<AuthenticatedChunkStream, NetworkError> {
    AuthenticatedChunkStream::new(
        Box::new(LogicalChunkReader {
            slot: Arc::clone(&slot),
            allocator: allocator.clone(),
            _owner: Arc::clone(&owner),
        }),
        Box::new(LogicalChunkWriter {
            slot,
            allocator,
            connection,
            finished: false,
            _owner: owner,
        }),
    )
}

fn mux_header(kind: u8, stream_id: u64, payload_bytes: usize) -> Result<[u8; 17], NetworkError> {
    if stream_id == 0
        && !matches!(
            kind,
            FRAME_CONNECTION_WINDOW | FRAME_CONNECTION_WINDOW_REQUEST
        )
    {
        return Err(NetworkError::Serialization {
            detail: "owned mux record used reserved stream id zero".to_string(),
        });
    }
    if stream_id != 0
        && matches!(
            kind,
            FRAME_CONNECTION_WINDOW | FRAME_CONNECTION_WINDOW_REQUEST
        )
    {
        return Err(NetworkError::Serialization {
            detail: "owned connection window used a logical stream id".to_string(),
        });
    }
    let payload_bytes = u32::try_from(payload_bytes).map_err(|_| NetworkError::FrameTooLarge {
        actual_bytes: payload_bytes,
        max_bytes: RPC_RECORD_MAX,
    })?;
    let mut header = [0_u8; MUX_RECORD_HEADER_BYTES];
    header[..4].copy_from_slice(&MUX_MAGIC);
    header[4] = kind;
    header[5..13].copy_from_slice(&stream_id.to_be_bytes());
    header[13..].copy_from_slice(&payload_bytes.to_be_bytes());
    Ok(header)
}

async fn write_tx_work(
    writer: &mut dyn AuthenticatedChunkWriter,
    work: TxWork,
) -> Result<(), NetworkError> {
    let (chunk, finish_completion) = match work {
        TxWork::Data {
            kind,
            stream_id,
            mut chunk,
            finish_completion,
        } => {
            observe_chunk_identity(chunk.slab_id(), "mux-rpc-record");
            let header = mux_header(kind, stream_id, chunk.len())?;
            chunk.prepend(&header)?;
            observe_chunk_identity(chunk.slab_id(), "mux-record-framed");
            (chunk, finish_completion)
        }
        TxWork::Control {
            command: control,
            finish_completion,
        } => {
            let payload_bytes = usize::from(control.credit_bytes.is_some()) * 8;
            let mut chunk = writer.allocator().allocate_control(payload_bytes).await?;
            if let Some(credit_bytes) = control.credit_bytes {
                let encoded =
                    u64::try_from(credit_bytes).map_err(|_| NetworkError::Serialization {
                        detail: "owned mux credit does not fit u64".to_string(),
                    })?;
                chunk.writable_payload()[..8].copy_from_slice(&encoded.to_be_bytes());
                chunk.commit_payload(8)?;
            }
            let header = mux_header(control.kind, control.stream_id, payload_bytes)?;
            chunk.prepend(&header)?;
            (chunk, finish_completion)
        }
    };
    if chunk.len() > writer.max_payload_bytes() {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: chunk.len(),
            max_bytes: writer.max_payload_bytes(),
        });
    }
    writer.send_chunk(chunk).await?;
    if let Some(completion) = finish_completion {
        let _ = completion.send(());
    }
    Ok(())
}

async fn run_owned_mux_writer(
    tx: Arc<TxScheduler>,
    mut writer: Box<dyn AuthenticatedChunkWriter>,
) -> Result<(), NetworkError> {
    while let Some(work) = tx.next().await? {
        write_tx_work(&mut *writer, work).await?;
    }
    writer.finish().await
}

struct ParsedMuxRecord {
    kind: u8,
    stream_id: u64,
    chunk: OwnedChunk,
}

fn parse_mux_record(lease: ChunkLease) -> Result<ParsedMuxRecord, NetworkError> {
    let mut chunk = lease.try_into_owned().map_err(|_| NetworkError::Io {
        operation: "parse owned mux record".to_string(),
        detail: "encrypted record allocation was shared before mux parsing".to_string(),
    })?;
    if chunk.len() < MUX_RECORD_HEADER_BYTES {
        return Err(NetworkError::Serialization {
            detail: "truncated owned mux record".to_string(),
        });
    }
    let header = &chunk.as_slice()[..MUX_RECORD_HEADER_BYTES];
    if header[..4] != MUX_MAGIC {
        return Err(NetworkError::Serialization {
            detail: "invalid owned mux record prefix".to_string(),
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
            | FRAME_OPEN_DATA
            | FRAME_CONNECTION_WINDOW
            | FRAME_WINDOW_REQUEST
            | FRAME_CONNECTION_WINDOW_REQUEST
    ) {
        return Err(NetworkError::Serialization {
            detail: format!("unknown owned mux record kind {kind}"),
        });
    }
    let stream_id = u64::from_be_bytes(header[5..13].try_into().expect("mux stream id"));
    let payload_bytes =
        u32::from_be_bytes(header[13..].try_into().expect("mux payload length")) as usize;
    if payload_bytes != chunk.len() - MUX_RECORD_HEADER_BYTES {
        return Err(NetworkError::Serialization {
            detail: format!(
                "owned mux payload length {payload_bytes} does not match record length {}",
                chunk.len() - MUX_RECORD_HEADER_BYTES
            ),
        });
    }
    mux_header(kind, stream_id, payload_bytes)?;
    match kind {
        FRAME_DATA | FRAME_DATA_FINISH | FRAME_OPEN_DATA | FRAME_OPEN_DATA_FINISH
            if payload_bytes == 0 || payload_bytes > RPC_RECORD_MAX =>
        {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: payload_bytes,
                max_bytes: RPC_RECORD_MAX,
            });
        }
        FRAME_WINDOW | FRAME_CONNECTION_WINDOW if payload_bytes != 8 => {
            return Err(NetworkError::Serialization {
                detail: "owned mux window record did not carry one u64 credit".to_string(),
            });
        }
        FRAME_OPEN
        | FRAME_ACCEPT
        | FRAME_FINISH
        | FRAME_RESET
        | FRAME_WINDOW_REQUEST
        | FRAME_CONNECTION_WINDOW_REQUEST
            if payload_bytes != 0 =>
        {
            return Err(NetworkError::Serialization {
                detail: "owned mux control record unexpectedly carried payload".to_string(),
            });
        }
        _ => {}
    }
    chunk.strip_prefix(MUX_RECORD_HEADER_BYTES)?;
    observe_chunk_identity(chunk.slab_id(), "mux-record-received");
    Ok(ParsedMuxRecord {
        kind,
        stream_id,
        chunk,
    })
}

fn decode_credit(chunk: &OwnedChunk) -> Result<usize, NetworkError> {
    let bytes = u64::from_be_bytes(chunk.as_slice().try_into().map_err(|_| {
        NetworkError::Serialization {
            detail: "owned mux credit is not eight bytes".to_string(),
        }
    })?);
    usize::try_from(bytes).map_err(|_| NetworkError::Serialization {
        detail: "owned mux credit does not fit usize".to_string(),
    })
}

type StreamMap = Arc<Mutex<BTreeMap<u64, Weak<StreamSlot>>>>;

fn lookup_slot(streams: &StreamMap, stream_id: u64) -> Option<Arc<StreamSlot>> {
    streams
        .lock()
        .ok()
        .and_then(|streams| streams.get(&stream_id).cloned())
        .and_then(|slot| slot.upgrade())
}

fn register_slot(
    streams: &StreamMap,
    slot: &Arc<StreamSlot>,
    operation: &'static str,
) -> Result<(), NetworkError> {
    let mut streams = streams
        .lock()
        .map_err(|_| mux_error(operation, "stream map lock poisoned"))?;
    // Stream IDs are never reused, so completed slots have no future lookup
    // value. The carrier itself is intentionally long-lived; prune dead weak
    // registrations whenever either endpoint opens another logical stream so
    // historical RPCs do not accumulate for the carrier's entire lifetime.
    streams.retain(|_, registered| registered.strong_count() != 0);
    streams.insert(slot.stream_id, Arc::downgrade(slot));
    Ok(())
}

fn fail_slots(streams: &StreamMap, detail: &str) {
    if let Ok(mut streams) = streams.lock() {
        for slot in streams.values().filter_map(Weak::upgrade) {
            slot.reset(detail.to_string());
        }
        streams.clear();
    }
}

fn fail_open_slots(streams: &StreamMap, detail: &str) {
    if let Ok(mut streams) = streams.lock() {
        for slot in streams.values().filter_map(Weak::upgrade) {
            // A clean carrier finish follows every queued mux record. Preserve
            // already-finished stream rings so their application readers can
            // consume the final buffered chunks after carrier EOF.
            slot.inbound.fail_if_open(detail);
        }
        streams.clear();
    }
}

#[derive(Clone)]
struct CarrierMetadata {
    local_node_id: NodeId,
    peer_node_id: NodeId,
    session_id: Id,
    channel_binding: [u8; 32],
    protection: crate::RoutedSessionProtection,
    route_trace: RouteTrace,
}

fn peer_stream_for_slot(
    metadata: &CarrierMetadata,
    slot: Arc<StreamSlot>,
    allocator: ChunkAllocator,
    connection: Arc<CarrierCredits>,
    owner: Arc<OwnedCarrierOwner>,
) -> Result<PeerStream, NetworkError> {
    PeerStream::new_owned_with_protection(
        logical_chunk_stream(slot, allocator, connection, owner)?,
        metadata.local_node_id.clone(),
        metadata.peer_node_id.clone(),
        metadata.session_id.clone(),
        metadata.channel_binding,
        metadata.protection,
        metadata.route_trace.clone(),
    )
}

async fn process_common_record(
    record: ParsedMuxRecord,
    streams: &StreamMap,
    tx: &Arc<TxScheduler>,
    connection: &Arc<CarrierCredits>,
) -> Result<bool, NetworkError> {
    match record.kind {
        FRAME_DATA | FRAME_DATA_FINISH => {
            let Some(slot) = lookup_slot(streams, record.stream_id) else {
                tracing::debug!(
                    stream_id = record.stream_id,
                    "resetting peer data for an inactive owned logical stream"
                );
                tx.enqueue_control(ControlCommand::reset(record.stream_id));
                return Ok(true);
            };
            if record.kind == FRAME_DATA_FINISH {
                // Publish terminal state before the lease becomes visible.
                // Another worker may consume and drop this chunk immediately.
                slot.mark_remote_send_finished();
            }
            slot.enqueue_received(record.chunk).await?;
            if record.kind == FRAME_DATA_FINISH {
                slot.finish_inbound();
            }
            Ok(true)
        }
        FRAME_FINISH => {
            if let Some(slot) = lookup_slot(streams, record.stream_id) {
                slot.finish_inbound();
            }
            Ok(true)
        }
        FRAME_RESET => {
            if let Some(slot) = lookup_slot(streams, record.stream_id) {
                slot.reset("peer reset the owned logical stream");
            }
            Ok(true)
        }
        FRAME_WINDOW => {
            if let Some(slot) = lookup_slot(streams, record.stream_id) {
                slot.remote_credit.add(decode_credit(&record.chunk)?)?;
            }
            Ok(true)
        }
        FRAME_CONNECTION_WINDOW => {
            connection
                .remote_credit
                .add(decode_credit(&record.chunk)?)?;
            Ok(true)
        }
        FRAME_WINDOW_REQUEST => {
            if let Some(slot) = lookup_slot(streams, record.stream_id) {
                slot.flush_returned_credit();
            }
            Ok(true)
        }
        FRAME_CONNECTION_WINDOW_REQUEST => {
            connection.flush_returned_credit();
            Ok(true)
        }
        _ => Ok(false),
    }
}

pub(crate) type IncomingLogicalStreamResolver =
    Arc<dyn Fn() -> Option<mpsc::Sender<Result<PeerStream, NetworkError>>> + Send + Sync + 'static>;

#[derive(Clone)]
pub(crate) enum IncomingLogicalStreamSink {
    #[cfg(test)]
    None,
    Fixed(mpsc::Sender<Result<PeerStream, NetworkError>>),
    Resolve(IncomingLogicalStreamResolver),
}

impl IncomingLogicalStreamSink {
    fn sender(&self) -> Option<mpsc::Sender<Result<PeerStream, NetworkError>>> {
        match self {
            #[cfg(test)]
            Self::None => None,
            Self::Fixed(sender) => Some(sender.clone()),
            Self::Resolve(resolve) => resolve(),
        }
    }

    async fn sender_after_activation_grace(
        &self,
    ) -> Option<mpsc::Sender<Result<PeerStream, NetworkError>>> {
        if let Some(sender) = self.sender() {
            return Some(sender);
        }
        if !matches!(self, Self::Resolve(_)) {
            return None;
        }
        let deadline = tokio::time::Instant::now() + INCOMING_DISPATCHER_ACTIVATION_GRACE;
        loop {
            tokio::time::sleep(Duration::from_millis(1)).await;
            if let Some(sender) = self.sender() {
                return Some(sender);
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
        }
    }
}

async fn run_owned_carrier_receiver(
    mut reader: Box<dyn AuthenticatedChunkReader>,
    streams: StreamMap,
    tx: Arc<TxScheduler>,
    connection: Arc<CarrierCredits>,
    owner: Weak<OwnedCarrierOwner>,
    metadata: CarrierMetadata,
    local_stream_parity: u64,
    incoming: IncomingLogicalStreamSink,
    closed: Arc<AtomicBool>,
) -> Result<(), NetworkError> {
    let allocator = reader.allocator().clone();
    loop {
        let Some(lease) = reader.recv_chunk().await? else {
            break;
        };
        let record = parse_mux_record(lease)?;
        if matches!(
            record.kind,
            FRAME_OPEN | FRAME_OPEN_DATA | FRAME_OPEN_DATA_FINISH
        ) {
            if record.stream_id % 2 == local_stream_parity
                || lookup_slot(&streams, record.stream_id).is_some()
            {
                return Err(NetworkError::AuthDenied);
            }
            // During cluster formation the listener can acquire its node
            // identity a few milliseconds before its process-local routed
            // service dispatcher is registered. A stable carrier always hits
            // the immediate branch; only that activation edge waits briefly
            // instead of permanently poisoning the first logical stream.
            let Some(accepted) = incoming.sender_after_activation_grace().await else {
                tracing::warn!(
                    stream_id = record.stream_id,
                    local_node_id = %metadata.local_node_id,
                    peer_node_id = %metadata.peer_node_id,
                    "resetting peer-opened logical stream because no service dispatcher is registered"
                );
                tx.enqueue_control(ControlCommand::reset(record.stream_id));
                continue;
            };
            let permit = match accepted.reserve().await {
                Ok(permit) => permit,
                Err(_) => {
                    tracing::warn!(
                        stream_id = record.stream_id,
                        local_node_id = %metadata.local_node_id,
                        peer_node_id = %metadata.peer_node_id,
                        "resetting peer-opened logical stream because the service dispatcher closed"
                    );
                    tx.enqueue_control(ControlCommand::reset(record.stream_id));
                    continue;
                }
            };
            let owner = owner
                .upgrade()
                .ok_or_else(|| mux_error("accept owned logical stream", "carrier closed"))?;
            let slot = StreamSlot::new(record.stream_id, &tx, &connection);
            tx.register_stream(record.stream_id, true)?;
            register_slot(&streams, &slot, "accept owned logical stream")?;
            if matches!(record.kind, FRAME_OPEN_DATA | FRAME_OPEN_DATA_FINISH) {
                if record.kind == FRAME_OPEN_DATA_FINISH {
                    // Set this before publishing the first lease for the same
                    // race handled by FRAME_DATA_FINISH above.
                    slot.mark_remote_send_finished();
                }
                slot.enqueue_received(record.chunk).await?;
            }
            if record.kind == FRAME_OPEN_DATA_FINISH {
                slot.finish_inbound();
            }
            let stream = peer_stream_for_slot(
                &metadata,
                slot,
                allocator.clone(),
                Arc::clone(&connection),
                owner,
            )?;
            permit.send(Ok(stream));
            continue;
        }
        if process_common_record(record, &streams, &tx, &connection).await? {
            continue;
        }
        return Err(NetworkError::AuthDenied);
    }
    closed.store(true, Ordering::Release);
    connection.close();
    tx.close();
    fail_open_slots(&streams, "owned routed carrier closed");
    Ok(())
}

#[derive(Debug)]
struct OwnedCarrierOwner {
    tx: Arc<TxScheduler>,
    connection: Arc<CarrierCredits>,
    closed: Arc<AtomicBool>,
    reader_task: Mutex<Option<JoinHandle<()>>>,
    writer_task: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for OwnedCarrierOwner {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.connection.close();
        self.tx.drain_and_close();
        if let Some(reader) = self
            .reader_task
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            reader.abort();
        }
        // The transmitter owns every queued slab. Let it drain and return
        // those allocations before it writes the authenticated carrier finish.
        drop(
            self.writer_task
                .get_mut()
                .unwrap_or_else(|error| error.into_inner())
                .take(),
        );
    }
}

pub(crate) struct OwnedRoutedStreamClient {
    metadata: CarrierMetadata,
    allocator: ChunkAllocator,
    tx: Arc<TxScheduler>,
    connection: Arc<CarrierCredits>,
    streams: StreamMap,
    owner: Arc<OwnedCarrierOwner>,
    next_stream_id: AtomicU64,
}

impl OwnedRoutedStreamClient {
    #[cfg(test)]
    pub(crate) fn new(carrier: PeerStream) -> Result<Self, NetworkError> {
        Self::new_with_incoming(carrier, 1, IncomingLogicalStreamSink::None)
    }

    pub(crate) fn new_with_incoming(
        carrier: PeerStream,
        next_stream_id: u64,
        incoming: IncomingLogicalStreamSink,
    ) -> Result<Self, NetworkError> {
        if next_stream_id == 0 || next_stream_id > 2 {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "owned routed carrier initial stream id must be one or two, got {next_stream_id}"
                ),
            });
        }
        let metadata = CarrierMetadata {
            local_node_id: carrier.local_node_id.clone(),
            peer_node_id: carrier.peer_node_id.clone(),
            session_id: carrier.session_id.clone(),
            channel_binding: carrier.channel_binding,
            protection: carrier.protection,
            route_trace: carrier.route_trace.clone(),
        };
        let chunk_stream = carrier.into_chunk_stream()?;
        let (reader, writer) = chunk_stream.into_halves();
        let allocator = writer.allocator().clone();
        let tx = TxScheduler::new();
        let connection = CarrierCredits::new(&tx);
        let streams = Arc::new(Mutex::new(BTreeMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let owner = Arc::new(OwnedCarrierOwner {
            tx: Arc::clone(&tx),
            connection: Arc::clone(&connection),
            closed: Arc::clone(&closed),
            reader_task: Mutex::new(None),
            writer_task: Mutex::new(None),
        });
        let writer_tx = Arc::clone(&tx);
        let writer_closed = Arc::clone(&closed);
        let writer_task = tokio::spawn(async move {
            if let Err(error) = run_owned_mux_writer(writer_tx.clone(), writer).await {
                tracing::debug!(%error, "owned routed mux writer stopped");
            }
            writer_closed.store(true, Ordering::Release);
            writer_tx.close();
        });
        *owner
            .writer_task
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(writer_task);
        let reader_streams = Arc::clone(&streams);
        let reader_tx = Arc::clone(&tx);
        let reader_connection = Arc::clone(&connection);
        let reader_owner = Arc::downgrade(&owner);
        let reader_metadata = metadata.clone();
        let reader_closed = Arc::clone(&closed);
        let local_stream_parity = next_stream_id % 2;
        let reader_task = tokio::spawn(async move {
            if let Err(error) = run_owned_carrier_receiver(
                reader,
                Arc::clone(&reader_streams),
                Arc::clone(&reader_tx),
                Arc::clone(&reader_connection),
                reader_owner,
                reader_metadata,
                local_stream_parity,
                incoming,
                Arc::clone(&reader_closed),
            )
            .await
            {
                tracing::debug!(%error, "owned routed mux reader stopped");
                reader_closed.store(true, Ordering::Release);
                reader_connection.close();
                reader_tx.close();
                fail_slots(&reader_streams, &error.to_string());
            }
        });
        *owner
            .reader_task
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(reader_task);
        Ok(Self {
            metadata,
            allocator,
            tx,
            connection,
            streams,
            owner,
            next_stream_id: AtomicU64::new(next_stream_id),
        })
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.owner.closed.load(Ordering::Acquire)
    }

    pub(crate) fn open(self: &Arc<Self>) -> Result<PeerStream, NetworkError> {
        if self.is_closed() {
            return Err(NetworkError::delivery_failed(
                "open owned routed logical stream",
                NetworkDeliveryState::DefinitelyNotSent,
                "owned routed carrier is closed",
            ));
        }
        let stream_id = self.next_stream_id.fetch_add(2, Ordering::Relaxed);
        if stream_id == 0 || stream_id.checked_add(2).is_none() {
            return Err(NetworkError::ResourceExhausted {
                operation: "allocate owned routed logical stream id".to_string(),
                requested_bytes: std::mem::size_of::<u64>(),
            });
        }
        let slot = StreamSlot::new(stream_id, &self.tx, &self.connection);
        self.tx.register_stream(stream_id, false)?;
        register_slot(&self.streams, &slot, "open owned routed logical stream")?;
        peer_stream_for_slot(
            &self.metadata,
            slot,
            self.allocator.clone(),
            Arc::clone(&self.connection),
            Arc::clone(&self.owner),
        )
    }
}

pub(crate) struct OwnedRoutedStreamAcceptor {
    incoming: mpsc::Receiver<Result<PeerStream, NetworkError>>,
    _client: Arc<OwnedRoutedStreamClient>,
}

impl OwnedRoutedStreamAcceptor {
    pub(crate) async fn accept(&mut self) -> Result<Option<PeerStream>, NetworkError> {
        match self.incoming.recv().await {
            Some(Ok(stream)) => Ok(Some(stream)),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }
}

pub(crate) fn accept_owned_routed_stream_carrier(
    carrier: PeerStream,
) -> Result<OwnedRoutedStreamAcceptor, NetworkError> {
    owned_routed_stream_acceptor_and_client(carrier).map(|(acceptor, _)| acceptor)
}

pub(crate) fn owned_routed_stream_acceptor_and_client(
    carrier: PeerStream,
) -> Result<(OwnedRoutedStreamAcceptor, Arc<OwnedRoutedStreamClient>), NetworkError> {
    let (accepted, incoming) = mpsc::channel(MAX_ACCEPTED_STREAMS);
    let client = Arc::new(OwnedRoutedStreamClient::new_with_incoming(
        carrier,
        2,
        IncomingLogicalStreamSink::Fixed(accepted),
    )?);
    Ok((
        OwnedRoutedStreamAcceptor {
            incoming,
            _client: Arc::clone(&client),
        },
        client,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owned_chunk::{BODY_CHUNK_MAX, ChunkPool, PayloadCopyReason, SECURE_WIRE_MAX};

    #[tokio::test(start_paused = true)]
    async fn resolved_incoming_sink_tolerates_dispatcher_activation_gap() {
        let registration = Arc::new(Mutex::new(None));
        let resolver_registration = Arc::clone(&registration);
        let sink = IncomingLogicalStreamSink::Resolve(Arc::new(move || {
            resolver_registration
                .lock()
                .expect("lock test dispatcher registration")
                .clone()
        }));
        let install_registration = Arc::clone(&registration);
        let (sender, _incoming) = mpsc::channel::<Result<PeerStream, NetworkError>>(1);

        let (resolved, ()) = tokio::join!(sink.sender_after_activation_grace(), async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            *install_registration
                .lock()
                .expect("install test dispatcher registration") = Some(sender);
        });

        assert!(resolved.is_some());
    }

    #[tokio::test]
    async fn lease_drop_returns_stream_and_connection_byte_credit() {
        let tx = TxScheduler::new();
        let connection = CarrierCredits::new(&tx);
        let slot = StreamSlot::new(1, &tx, &connection);
        let allocator = ChunkAllocator::new(
            ChunkPool::new(SECURE_WIRE_MAX * 2, 64 * 1024, 0, 0),
            SECURE_WIRE_MAX * 2,
            64 * 1024,
        );
        let mut chunk = allocator
            .allocate_data(BODY_CHUNK_MAX)
            .await
            .expect("allocate mux test chunk");
        chunk
            .copy_payload_from(b"owned", PayloadCopyReason::SharedSourceNormalization)
            .expect("copy test source");
        let charged = chunk.class().resident_bytes();
        slot.enqueue_received(chunk).await.expect("queue chunk");
        assert_eq!(slot.receive_credit.outstanding(), charged);
        assert_eq!(connection.receive_credit.outstanding(), charged);
        let lease = slot
            .inbound
            .recv()
            .await
            .expect("read chunk")
            .expect("chunk");
        drop(lease);
        assert_eq!(slot.receive_credit.outstanding(), 0);
        assert_eq!(connection.receive_credit.outstanding(), 0);
    }

    #[tokio::test]
    async fn lease_drop_returns_connection_credit_after_stream_slot_is_gone() {
        let tx = TxScheduler::new();
        let connection = CarrierCredits::new(&tx);
        let slot = StreamSlot::new(1, &tx, &connection);
        let allocator = ChunkAllocator::new(
            ChunkPool::new(SECURE_WIRE_MAX * 2, 64 * 1024, 0, 0),
            SECURE_WIRE_MAX * 2,
            64 * 1024,
        );
        let mut chunk = allocator
            .allocate_data(BODY_CHUNK_MAX)
            .await
            .expect("allocate mux test chunk");
        chunk
            .copy_payload_from(b"owned", PayloadCopyReason::SharedSourceNormalization)
            .expect("copy test source");
        let charged = chunk.class().resident_bytes();
        slot.enqueue_received(chunk).await.expect("queue chunk");
        let lease = slot
            .inbound
            .recv()
            .await
            .expect("read chunk")
            .expect("chunk");

        drop(slot);
        drop(lease);

        assert_eq!(connection.receive_credit.outstanding(), 0);
        assert_eq!(
            connection.returned_credit.load(Ordering::Acquire),
            charged,
            "carrier credit return must not depend on the completed stream slot"
        );
    }

    #[test]
    fn stream_registration_prunes_completed_slots_without_touching_live_streams() {
        let tx = TxScheduler::new();
        let connection = CarrierCredits::new(&tx);
        let streams = Arc::new(Mutex::new(BTreeMap::new()));
        let live = StreamSlot::new(1, &tx, &connection);
        register_slot(&streams, &live, "register test live stream").expect("register live stream");

        for stream_id in (3..2051).step_by(2) {
            let completed = StreamSlot::new(stream_id, &tx, &connection);
            register_slot(&streams, &completed, "register test completed stream")
                .expect("register completed stream");
            assert!(Arc::ptr_eq(
                &lookup_slot(&streams, live.stream_id).expect("live stream remains registered"),
                &live,
            ));
            assert!(
                streams.lock().expect("stream map").len() <= 2,
                "one live and at most one newly completed registration remain"
            );
            drop(completed);
        }

        let second_live = StreamSlot::new(2051, &tx, &connection);
        register_slot(&streams, &second_live, "register test second live stream")
            .expect("register second live stream");
        let streams = streams.lock().expect("stream map");
        assert_eq!(streams.len(), 2);
        assert!(
            streams
                .get(&live.stream_id)
                .is_some_and(|registered| registered.ptr_eq(&Arc::downgrade(&live)))
        );
        assert!(
            streams
                .get(&second_live.stream_id)
                .is_some_and(|registered| registered.ptr_eq(&Arc::downgrade(&second_live)))
        );
    }

    #[tokio::test]
    async fn finished_stream_does_not_emit_redundant_window() {
        let tx = TxScheduler::new();
        let connection = CarrierCredits::new(&tx);
        let slot = StreamSlot::new(1, &tx, &connection);
        let allocator = ChunkAllocator::new(
            ChunkPool::new(SECURE_WIRE_MAX * 2, 64 * 1024, 0, 0),
            SECURE_WIRE_MAX * 2,
            64 * 1024,
        );
        let mut chunk = allocator
            .allocate_data(BODY_CHUNK_MAX)
            .await
            .expect("allocate mux test chunk");
        chunk
            .copy_payload_from(b"final", PayloadCopyReason::SharedSourceNormalization)
            .expect("copy test source");
        let charged = chunk.class().resident_bytes();

        // The carrier marks DATA_FINISH before making its lease visible, so a
        // fast consumer cannot race a useless WINDOW ahead of the response.
        slot.mark_remote_send_finished();
        slot.enqueue_received(chunk)
            .await
            .expect("queue final chunk");
        let lease = slot
            .inbound
            .recv()
            .await
            .expect("read final chunk")
            .expect("chunk");
        drop(lease);

        assert_eq!(slot.receive_credit.outstanding(), 0);
        assert_eq!(connection.receive_credit.outstanding(), 0);
        assert_eq!(slot.returned_credit.load(Ordering::Acquire), 0);
        assert_eq!(connection.returned_credit.load(Ordering::Acquire), charged);
        let state = tx.state.lock().expect("transmitter state");
        assert!(
            state.controls.is_empty(),
            "finished stream must not queue a WINDOW record"
        );
    }

    #[tokio::test]
    async fn scheduler_is_byte_fair_across_active_streams() {
        let tx = TxScheduler::new();
        tx.register_stream(1, false).unwrap();
        tx.register_stream(3, false).unwrap();
        let allocator = ChunkAllocator::new(
            ChunkPool::new(SECURE_WIRE_MAX * 4, 64 * 1024, 0, 0),
            SECURE_WIRE_MAX * 4,
            64 * 1024,
        );
        for stream_id in [1, 1, 3] {
            let mut chunk = allocator.allocate_data(1).await.unwrap();
            chunk.commit_payload(1).unwrap();
            tx.enqueue_data(stream_id, chunk).unwrap();
        }
        let first = tx.next().await.unwrap().unwrap();
        let second = tx.next().await.unwrap().unwrap();
        let ids = [first, second].map(|work| match work {
            TxWork::Data { stream_id, .. } => stream_id,
            TxWork::Control { .. } => unreachable!(),
        });
        assert_eq!(ids, [1, 3]);
    }

    #[tokio::test]
    async fn drain_and_close_stages_large_queued_record_before_closing() {
        let tx = TxScheduler::new();
        tx.register_stream(1, false).unwrap();
        let allocator = ChunkAllocator::new(
            ChunkPool::new(SECURE_WIRE_MAX * 2, 64 * 1024, 0, 0),
            SECURE_WIRE_MAX * 2,
            64 * 1024,
        );
        let mut chunk = allocator.allocate_data(BODY_CHUNK_MAX).await.unwrap();
        chunk.commit_payload(BODY_CHUNK_MAX).unwrap();
        tx.enqueue_data(1, chunk).unwrap();
        tx.drain_and_close();

        match tx.next().await.unwrap().unwrap() {
            TxWork::Data {
                stream_id, chunk, ..
            } => {
                assert_eq!(stream_id, 1);
                assert_eq!(chunk.len(), BODY_CHUNK_MAX);
            }
            other => panic!("unexpected queued work: {other:?}"),
        }
        assert!(tx.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn scheduler_prioritizes_front_rpc_control_records_without_reordering_a_stream() {
        let tx = TxScheduler::new();
        tx.register_stream(1, false).unwrap();
        tx.register_stream(3, false).unwrap();
        let allocator = ChunkAllocator::new(
            ChunkPool::new(SECURE_WIRE_MAX * 2, 64 * 1024, 0, 0),
            SECURE_WIRE_MAX * 2,
            64 * 1024,
        );
        let mut body = allocator.allocate_data(1).await.unwrap();
        body.commit_payload(1).unwrap();
        tx.enqueue_data(1, body).unwrap();
        let mut rpc_control = allocator.allocate_control(1).await.unwrap();
        rpc_control.commit_payload(1).unwrap();
        tx.enqueue_data(3, rpc_control).unwrap();

        match tx.next().await.unwrap().unwrap() {
            TxWork::Data {
                stream_id, chunk, ..
            } => {
                assert_eq!(stream_id, 3);
                assert_eq!(chunk.class(), ChunkClass::Control);
            }
            other => panic!("unexpected priority work: {other:?}"),
        }
        match tx.next().await.unwrap().unwrap() {
            TxWork::Data {
                stream_id, chunk, ..
            } => {
                assert_eq!(stream_id, 1);
                assert_eq!(chunk.class(), ChunkClass::Data);
            }
            other => panic!("unexpected body work: {other:?}"),
        }
    }

    #[tokio::test]
    async fn scheduler_prioritizes_ready_stream_finish_over_other_body_data() {
        let tx = TxScheduler::new();
        tx.register_stream(1, false).unwrap();
        tx.register_stream(3, true).unwrap();
        let allocator = ChunkAllocator::new(
            ChunkPool::new(SECURE_WIRE_MAX, 64 * 1024, 0, 0),
            SECURE_WIRE_MAX,
            64 * 1024,
        );
        let mut body = allocator.allocate_data(1).await.unwrap();
        body.commit_payload(1).unwrap();
        tx.enqueue_data(1, body).unwrap();
        let _finish = tx.finish_stream(3).unwrap();

        match tx.next().await.unwrap().unwrap() {
            TxWork::Control {
                command:
                    ControlCommand {
                        kind: FRAME_FINISH,
                        stream_id: 3,
                        credit_bytes: None,
                    },
                finish_completion: Some(_),
            } => {}
            other => panic!("unexpected finish priority work: {other:?}"),
        }
    }

    #[tokio::test]
    async fn blocked_sender_request_flushes_subthreshold_credit_immediately() {
        let tx = TxScheduler::new();
        let connection = CarrierCredits::new(&tx);
        let slot = StreamSlot::new(1, &tx, &connection);
        let streams = Arc::new(Mutex::new(BTreeMap::from([(1, Arc::downgrade(&slot))])));
        let allocator = ChunkAllocator::new(
            ChunkPool::new(SECURE_WIRE_MAX, 64 * 1024, 0, 0),
            SECURE_WIRE_MAX,
            64 * 1024,
        );

        let returned = SECURE_WIRE_MAX;
        slot.returned_credit.store(returned, Ordering::Release);
        let request = ParsedMuxRecord {
            kind: FRAME_WINDOW_REQUEST,
            stream_id: 1,
            chunk: allocator
                .allocate_control(0)
                .await
                .expect("allocate request record"),
        };
        assert!(
            process_common_record(request, &streams, &tx, &connection)
                .await
                .expect("process window request")
        );

        match tx.next().await.expect("schedule response").expect("work") {
            TxWork::Control {
                command:
                    ControlCommand {
                        kind: FRAME_WINDOW,
                        stream_id: 1,
                        credit_bytes: Some(bytes),
                    },
                ..
            } => assert_eq!(bytes, returned),
            other => panic!("unexpected scheduled response: {other:?}"),
        }
        assert_eq!(slot.returned_credit.load(Ordering::Acquire), 0);
    }
}
