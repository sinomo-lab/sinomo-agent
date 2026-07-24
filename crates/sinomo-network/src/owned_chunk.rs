//! Ownership-preserving buffers for the authenticated stream data path.
//!
//! One slab reserves all envelope space up front. Protocol layers prepend or
//! strip their own header and append or strip their own trailer without moving
//! the payload. A slab is mutable only while held by [`OwnedChunk`]; received
//! application data is exposed through immutable [`ChunkLease`] views.

use crate::error::NetworkError;
use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex, OnceLock, Weak,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use tokio::sync::Notify;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChunkIdentityEvent {
    pub slab_id: u64,
    pub stage: &'static str,
}

#[cfg(test)]
fn chunk_identity_observer() -> &'static Mutex<Option<std::sync::mpsc::Sender<ChunkIdentityEvent>>>
{
    static OBSERVER: OnceLock<Mutex<Option<std::sync::mpsc::Sender<ChunkIdentityEvent>>>> =
        OnceLock::new();
    OBSERVER.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
pub(crate) struct ChunkIdentityObserverGuard;

#[cfg(test)]
impl Drop for ChunkIdentityObserverGuard {
    fn drop(&mut self) {
        if let Ok(mut observer) = chunk_identity_observer().lock() {
            *observer = None;
        }
    }
}

#[cfg(test)]
pub(crate) fn install_chunk_identity_observer() -> (
    ChunkIdentityObserverGuard,
    std::sync::mpsc::Receiver<ChunkIdentityEvent>,
) {
    let (sender, receiver) = std::sync::mpsc::channel();
    *chunk_identity_observer()
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(sender);
    (ChunkIdentityObserverGuard, receiver)
}

pub(crate) fn observe_chunk_identity(slab_id: u64, stage: &'static str) {
    #[cfg(test)]
    if let Ok(observer) = chunk_identity_observer().lock()
        && let Some(observer) = observer.as_ref()
    {
        let _ = observer.send(ChunkIdentityEvent { slab_id, stage });
    }
    #[cfg(not(test))]
    let _ = (slab_id, stage);
}

pub(crate) const BODY_CHUNK_MAX: usize = 256 * 1024;
pub(crate) const RPC_RECORD_HEADER_BYTES: usize = 9;
pub(crate) const RPC_CONTROL_PAYLOAD_MAX: usize = 16 * 1024;
pub(crate) const RPC_RECORD_MAX: usize = RPC_RECORD_HEADER_BYTES + BODY_CHUNK_MAX;
pub(crate) const MUX_RECORD_HEADER_BYTES: usize = 17;
pub(crate) const MUX_RECORD_MAX: usize = MUX_RECORD_HEADER_BYTES + RPC_RECORD_MAX;
pub(crate) const SECURE_RECORD_HEADER_BYTES: usize = 16;
pub(crate) const SECURE_RECORD_TAG_BYTES: usize = 16;
pub(crate) const SECURE_PLAINTEXT_MAX: usize = MUX_RECORD_MAX;
pub(crate) const SECURE_WIRE_MAX: usize =
    SECURE_RECORD_HEADER_BYTES + SECURE_PLAINTEXT_MAX + SECURE_RECORD_TAG_BYTES;
pub(crate) const CHUNK_HEADROOM_BYTES: usize =
    SECURE_RECORD_HEADER_BYTES + MUX_RECORD_HEADER_BYTES + RPC_RECORD_HEADER_BYTES;
pub(crate) const CHUNK_TAILROOM_BYTES: usize = SECURE_RECORD_TAG_BYTES;

const CONTROL_SLAB_BYTES: usize =
    CHUNK_HEADROOM_BYTES + RPC_CONTROL_PAYLOAD_MAX + CHUNK_TAILROOM_BYTES;
const DATA_SLAB_BYTES: usize = SECURE_WIRE_MAX;
const DEFAULT_GLOBAL_DATA_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_GLOBAL_CONTROL_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_RETAINED_DATA_SLABS: usize = 128;
const DEFAULT_RETAINED_CONTROL_SLABS: usize = 256;
const DEFAULT_CONNECTION_DATA_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_CONNECTION_CONTROL_BYTES: usize = 2 * 1024 * 1024;

type CreditReturn = Arc<dyn Fn(usize) + Send + Sync + 'static>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChunkClass {
    Data,
    Control,
}

impl ChunkClass {
    pub(crate) const fn resident_bytes(self) -> usize {
        match self {
            Self::Data => DATA_SLAB_BYTES,
            Self::Control => CONTROL_SLAB_BYTES,
        }
    }

    const fn payload_capacity(self) -> usize {
        match self {
            Self::Data => BODY_CHUNK_MAX,
            Self::Control => RPC_CONTROL_PAYLOAD_MAX,
        }
    }

    const fn metric_label(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Control => "control",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PayloadCopyReason {
    SharedSourceNormalization,
    ByteStreamWriteAdapter,
    ByteStreamReadAdapter,
    ContiguousBodyMaterialization,
}

impl PayloadCopyReason {
    const fn metric_label(self) -> &'static str {
        match self {
            Self::SharedSourceNormalization => "shared-source-normalization",
            Self::ByteStreamWriteAdapter => "byte-stream-write-adapter",
            Self::ByteStreamReadAdapter => "byte-stream-read-adapter",
            Self::ContiguousBodyMaterialization => "contiguous-body-materialization",
        }
    }
}

/// Cumulative process-wide measurements for the ownership-native network
/// buffer pool.
///
/// Callers can take snapshots around a transfer to derive payload-copy bytes
/// and allocations per GiB. Peak values are process lifetime high-water marks.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct NetworkChunkMetrics {
    pub allocations: u64,
    pub reuses: u64,
    pub deallocations: u64,
    pub payload_copy_bytes: u64,
    pub shared_source_normalization_copy_bytes: u64,
    pub byte_stream_write_adapter_copy_bytes: u64,
    pub byte_stream_read_adapter_copy_bytes: u64,
    pub contiguous_body_materialization_copy_bytes: u64,
    pub active_bytes: usize,
    pub peak_active_bytes: usize,
    pub allocated_bytes: usize,
    pub peak_allocated_bytes: usize,
    pub retained_bytes: usize,
}

#[derive(Default)]
struct ChunkPoolMetrics {
    allocations: AtomicU64,
    reuses: AtomicU64,
    deallocations: AtomicU64,
    payload_copy_bytes: AtomicU64,
    shared_source_normalization_copy_bytes: AtomicU64,
    byte_stream_write_adapter_copy_bytes: AtomicU64,
    byte_stream_read_adapter_copy_bytes: AtomicU64,
    contiguous_body_materialization_copy_bytes: AtomicU64,
    active_bytes: AtomicUsize,
    peak_active_bytes: AtomicUsize,
    peak_allocated_bytes: AtomicUsize,
}

impl ChunkPoolMetrics {
    fn add_active(&self, bytes: usize) {
        let active = self
            .active_bytes
            .fetch_add(bytes, Ordering::Relaxed)
            .saturating_add(bytes);
        let mut peak = self.peak_active_bytes.load(Ordering::Relaxed);
        while active > peak {
            match self.peak_active_bytes.compare_exchange_weak(
                peak,
                active,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }
    }

    fn remove_active(&self, bytes: usize) {
        self.active_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    fn update_peak_allocated(&self, bytes: usize) {
        let mut peak = self.peak_allocated_bytes.load(Ordering::Relaxed);
        while bytes > peak {
            match self.peak_allocated_bytes.compare_exchange_weak(
                peak,
                bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }
    }
}

struct ChunkPoolState {
    data_free: Vec<Vec<u8>>,
    control_free: Vec<Vec<u8>>,
    data_allocated_bytes: usize,
    control_allocated_bytes: usize,
}

struct ChunkPoolInner {
    state: Mutex<ChunkPoolState>,
    metrics: ChunkPoolMetrics,
    next_slab_id: AtomicU64,
    global_data_bytes: usize,
    global_control_bytes: usize,
    retained_data_slabs: usize,
    retained_control_slabs: usize,
}

impl ChunkPoolInner {
    fn record_copy(&self, bytes: usize, reason: PayloadCopyReason) {
        self.metrics
            .payload_copy_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        let reason_counter = match reason {
            PayloadCopyReason::SharedSourceNormalization => {
                &self.metrics.shared_source_normalization_copy_bytes
            }
            PayloadCopyReason::ByteStreamWriteAdapter => {
                &self.metrics.byte_stream_write_adapter_copy_bytes
            }
            PayloadCopyReason::ByteStreamReadAdapter => {
                &self.metrics.byte_stream_read_adapter_copy_bytes
            }
            PayloadCopyReason::ContiguousBodyMaterialization => {
                &self.metrics.contiguous_body_materialization_copy_bytes
            }
        };
        reason_counter.fetch_add(bytes as u64, Ordering::Relaxed);
        metrics::counter!(
            "sinomo.network.chunk.payload_copy.bytes",
            "reason" => reason.metric_label()
        )
        .increment(bytes as u64);
    }

    fn recycle(&self, class: ChunkClass, bytes: Vec<u8>) {
        let slab_bytes = class.resident_bytes();
        debug_assert_eq!(bytes.len(), slab_bytes);
        self.metrics.remove_active(slab_bytes);
        let mut retained = false;
        let mut bytes = Some(bytes);
        if let Ok(mut state) = self.state.lock() {
            match class {
                ChunkClass::Data if state.data_free.len() < self.retained_data_slabs => {
                    // The initialized contents are deliberately retained. A
                    // future owner exposes only the range it explicitly
                    // commits.
                    state
                        .data_free
                        .push(bytes.take().expect("unrecycled data slab"));
                    retained = true;
                }
                ChunkClass::Control if state.control_free.len() < self.retained_control_slabs => {
                    state
                        .control_free
                        .push(bytes.take().expect("unrecycled control slab"));
                    retained = true;
                }
                ChunkClass::Data => {
                    state.data_allocated_bytes =
                        state.data_allocated_bytes.saturating_sub(slab_bytes);
                }
                ChunkClass::Control => {
                    state.control_allocated_bytes =
                        state.control_allocated_bytes.saturating_sub(slab_bytes);
                }
            }
        }
        if !retained {
            drop(bytes);
            self.metrics.deallocations.fetch_add(1, Ordering::Relaxed);
            metrics::counter!(
                "sinomo.network.chunk.deallocations",
                "class" => class.metric_label()
            )
            .increment(1);
        }
    }

    fn discard_adopted(&self, class: ChunkClass, bytes: Vec<u8>) {
        let slab_bytes = class.resident_bytes();
        self.metrics.remove_active(slab_bytes);
        if let Ok(mut state) = self.state.lock() {
            match class {
                ChunkClass::Data => {
                    state.data_allocated_bytes =
                        state.data_allocated_bytes.saturating_sub(slab_bytes);
                }
                ChunkClass::Control => {
                    state.control_allocated_bytes =
                        state.control_allocated_bytes.saturating_sub(slab_bytes);
                }
            }
        }
        drop(bytes);
        self.metrics.deallocations.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(
            "sinomo.network.chunk.deallocations",
            "class" => class.metric_label(),
            "origin" => "adopted"
        )
        .increment(1);
    }
}

#[derive(Clone)]
pub(crate) struct ChunkPool {
    inner: Arc<ChunkPoolInner>,
}

impl fmt::Debug for ChunkPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChunkPool")
            .field("metrics", &self.metrics())
            .finish()
    }
}

impl Default for ChunkPool {
    fn default() -> Self {
        Self::new(
            DEFAULT_GLOBAL_DATA_BYTES,
            DEFAULT_GLOBAL_CONTROL_BYTES,
            DEFAULT_RETAINED_DATA_SLABS,
            DEFAULT_RETAINED_CONTROL_SLABS,
        )
    }
}

impl ChunkPool {
    pub(crate) fn new(
        global_data_bytes: usize,
        global_control_bytes: usize,
        retained_data_slabs: usize,
        retained_control_slabs: usize,
    ) -> Self {
        Self {
            inner: Arc::new(ChunkPoolInner {
                state: Mutex::new(ChunkPoolState {
                    data_free: Vec::new(),
                    control_free: Vec::new(),
                    data_allocated_bytes: 0,
                    control_allocated_bytes: 0,
                }),
                metrics: ChunkPoolMetrics::default(),
                next_slab_id: AtomicU64::new(1),
                global_data_bytes,
                global_control_bytes,
                retained_data_slabs,
                retained_control_slabs,
            }),
        }
    }

    fn allocate(
        &self,
        class: ChunkClass,
        connection_credit: ByteCreditPermit,
    ) -> Result<OwnedChunk, NetworkError> {
        let slab_bytes = class.resident_bytes();
        let mut state = self.inner.state.lock().map_err(|_| NetworkError::Io {
            operation: "allocate pooled network chunk".to_string(),
            detail: "chunk pool lock poisoned".to_string(),
        })?;
        let recycled = match class {
            ChunkClass::Data => state.data_free.pop(),
            ChunkClass::Control => state.control_free.pop(),
        };
        let bytes = if let Some(bytes) = recycled {
            self.inner.metrics.reuses.fetch_add(1, Ordering::Relaxed);
            metrics::counter!(
                "sinomo.network.chunk.reuses",
                "class" => class.metric_label()
            )
            .increment(1);
            bytes
        } else {
            let (allocated_bytes, global_limit) = match class {
                ChunkClass::Data => (state.data_allocated_bytes, self.inner.global_data_bytes),
                ChunkClass::Control => (
                    state.control_allocated_bytes,
                    self.inner.global_control_bytes,
                ),
            };
            let next = allocated_bytes.saturating_add(slab_bytes);
            if next > global_limit {
                return Err(NetworkError::ResourceExhausted {
                    operation: format!("allocate global {} network slab", class.metric_label()),
                    requested_bytes: slab_bytes,
                });
            }
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(slab_bytes)
                .map_err(|_| NetworkError::ResourceExhausted {
                    operation: format!("allocate {} network slab", class.metric_label()),
                    requested_bytes: slab_bytes,
                })?;
            // This initialization is paid only when the operating system gives
            // the pool a new allocation. Reused slabs are never cleared.
            bytes.resize(slab_bytes, 0);
            match class {
                ChunkClass::Data => state.data_allocated_bytes = next,
                ChunkClass::Control => state.control_allocated_bytes = next,
            }
            self.inner.metrics.update_peak_allocated(
                state
                    .data_allocated_bytes
                    .saturating_add(state.control_allocated_bytes),
            );
            self.inner
                .metrics
                .allocations
                .fetch_add(1, Ordering::Relaxed);
            metrics::counter!(
                "sinomo.network.chunk.allocations",
                "class" => class.metric_label()
            )
            .increment(1);
            bytes
        };
        drop(state);
        self.inner.metrics.add_active(slab_bytes);
        let slab_id = self.inner.next_slab_id.fetch_add(1, Ordering::Relaxed);
        Ok(OwnedChunk {
            slab: Arc::new(PooledSlab {
                id: slab_id,
                class,
                bytes: Some(bytes),
                pool: Arc::downgrade(&self.inner),
                recyclable: true,
                _connection_credit: Some(connection_credit),
            }),
            start: CHUNK_HEADROOM_BYTES,
            end: CHUNK_HEADROOM_BYTES,
            payload_limit: CHUNK_HEADROOM_BYTES + class.payload_capacity(),
            receive_credit: None,
        })
    }

    fn adopt(
        &self,
        class: ChunkClass,
        bytes: Vec<u8>,
        start: usize,
        end: usize,
        connection_credit: ByteCreditPermit,
    ) -> Result<OwnedChunk, NetworkError> {
        let slab_bytes = class.resident_bytes();
        if bytes.capacity() < slab_bytes || start > end || end > bytes.len() {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "cannot adopt {}-byte {} buffer with {}-byte capacity and range {start}..{end}; expected at least {slab_bytes} bytes of capacity and an initialized payload range",
                    bytes.len(),
                    class.metric_label(),
                    bytes.capacity(),
                ),
            });
        }
        let payload_limit = slab_bytes.saturating_sub(CHUNK_TAILROOM_BYTES);
        if start < CHUNK_HEADROOM_BYTES || end > payload_limit {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "adopted {} chunk range {start}..{end} does not preserve {CHUNK_HEADROOM_BYTES} bytes of headroom and {CHUNK_TAILROOM_BYTES} bytes of tailroom",
                    class.metric_label(),
                ),
            });
        }
        let mut state = self.inner.state.lock().map_err(|_| NetworkError::Io {
            operation: "adopt unique network chunk".to_string(),
            detail: "chunk pool lock poisoned".to_string(),
        })?;
        let (allocated, limit) = match class {
            ChunkClass::Data => (
                &mut state.data_allocated_bytes,
                self.inner.global_data_bytes,
            ),
            ChunkClass::Control => (
                &mut state.control_allocated_bytes,
                self.inner.global_control_bytes,
            ),
        };
        let next = allocated.saturating_add(slab_bytes);
        if next > limit {
            return Err(NetworkError::ResourceExhausted {
                operation: format!("adopt global {} network slab", class.metric_label()),
                requested_bytes: slab_bytes,
            });
        }
        *allocated = next;
        self.inner.metrics.update_peak_allocated(
            state
                .data_allocated_bytes
                .saturating_add(state.control_allocated_bytes),
        );
        drop(state);
        self.inner
            .metrics
            .allocations
            .fetch_add(1, Ordering::Relaxed);
        self.inner.metrics.add_active(slab_bytes);
        metrics::counter!(
            "sinomo.network.chunk.allocations",
            "class" => class.metric_label(),
            "origin" => "adopted"
        )
        .increment(1);
        let slab_id = self.inner.next_slab_id.fetch_add(1, Ordering::Relaxed);
        Ok(OwnedChunk {
            slab: Arc::new(PooledSlab {
                id: slab_id,
                class,
                bytes: Some(bytes),
                pool: Arc::downgrade(&self.inner),
                recyclable: false,
                _connection_credit: Some(connection_credit),
            }),
            start,
            end,
            payload_limit,
            receive_credit: None,
        })
    }

    pub(crate) fn metrics(&self) -> NetworkChunkMetrics {
        let (allocated_bytes, retained_bytes) = self
            .inner
            .state
            .lock()
            .map(|state| {
                (
                    state
                        .data_allocated_bytes
                        .saturating_add(state.control_allocated_bytes),
                    state
                        .data_free
                        .len()
                        .saturating_mul(DATA_SLAB_BYTES)
                        .saturating_add(
                            state.control_free.len().saturating_mul(CONTROL_SLAB_BYTES),
                        ),
                )
            })
            .unwrap_or_default();
        NetworkChunkMetrics {
            allocations: self.inner.metrics.allocations.load(Ordering::Relaxed),
            reuses: self.inner.metrics.reuses.load(Ordering::Relaxed),
            deallocations: self.inner.metrics.deallocations.load(Ordering::Relaxed),
            payload_copy_bytes: self
                .inner
                .metrics
                .payload_copy_bytes
                .load(Ordering::Relaxed),
            shared_source_normalization_copy_bytes: self
                .inner
                .metrics
                .shared_source_normalization_copy_bytes
                .load(Ordering::Relaxed),
            byte_stream_write_adapter_copy_bytes: self
                .inner
                .metrics
                .byte_stream_write_adapter_copy_bytes
                .load(Ordering::Relaxed),
            byte_stream_read_adapter_copy_bytes: self
                .inner
                .metrics
                .byte_stream_read_adapter_copy_bytes
                .load(Ordering::Relaxed),
            contiguous_body_materialization_copy_bytes: self
                .inner
                .metrics
                .contiguous_body_materialization_copy_bytes
                .load(Ordering::Relaxed),
            active_bytes: self.inner.metrics.active_bytes.load(Ordering::Relaxed),
            peak_active_bytes: self.inner.metrics.peak_active_bytes.load(Ordering::Relaxed),
            allocated_bytes,
            peak_allocated_bytes: self
                .inner
                .metrics
                .peak_allocated_bytes
                .load(Ordering::Relaxed),
            retained_bytes,
        }
    }
}

pub(crate) fn shared_chunk_pool() -> &'static ChunkPool {
    static POOL: OnceLock<ChunkPool> = OnceLock::new();
    POOL.get_or_init(ChunkPool::default)
}

/// Returns a process-wide snapshot of ownership-native network buffering.
pub fn network_chunk_metrics() -> NetworkChunkMetrics {
    shared_chunk_pool().metrics()
}

pub(crate) fn record_shared_payload_copy(bytes: usize, reason: PayloadCopyReason) {
    shared_chunk_pool().inner.record_copy(bytes, reason);
}

#[derive(Clone)]
pub(crate) struct ByteCredit {
    inner: Arc<ByteCreditInner>,
}

struct ByteCreditInner {
    limit: usize,
    outstanding: AtomicUsize,
    peak_outstanding: AtomicUsize,
    available: Notify,
}

impl fmt::Debug for ByteCredit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ByteCredit")
            .field("limit", &self.inner.limit)
            .field("outstanding", &self.outstanding())
            .field("peak_outstanding", &self.peak_outstanding())
            .finish()
    }
}

impl ByteCredit {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            inner: Arc::new(ByteCreditInner {
                limit,
                outstanding: AtomicUsize::new(0),
                peak_outstanding: AtomicUsize::new(0),
                available: Notify::new(),
            }),
        }
    }

    pub(crate) fn outstanding(&self) -> usize {
        self.inner.outstanding.load(Ordering::Acquire)
    }

    pub(crate) fn peak_outstanding(&self) -> usize {
        self.inner.peak_outstanding.load(Ordering::Relaxed)
    }

    pub(crate) fn try_acquire(&self, bytes: usize) -> Option<ByteCreditPermit> {
        if bytes > self.inner.limit {
            return None;
        }
        let mut outstanding = self.inner.outstanding.load(Ordering::Acquire);
        loop {
            let next = outstanding.checked_add(bytes)?;
            if next > self.inner.limit {
                return None;
            }
            match self.inner.outstanding.compare_exchange_weak(
                outstanding,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let mut peak = self.inner.peak_outstanding.load(Ordering::Relaxed);
                    while next > peak {
                        match self.inner.peak_outstanding.compare_exchange_weak(
                            peak,
                            next,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(actual) => peak = actual,
                        }
                    }
                    return Some(ByteCreditPermit {
                        inner: Arc::clone(&self.inner),
                        bytes,
                    });
                }
                Err(actual) => outstanding = actual,
            }
        }
    }

    pub(crate) async fn acquire(
        &self,
        bytes: usize,
        operation: &'static str,
    ) -> Result<ByteCreditPermit, NetworkError> {
        if bytes > self.inner.limit {
            return Err(NetworkError::ResourceExhausted {
                operation: operation.to_string(),
                requested_bytes: bytes,
            });
        }
        loop {
            let notified = self.inner.available.notified();
            if let Some(permit) = self.try_acquire(bytes) {
                return Ok(permit);
            }
            notified.await;
        }
    }
}

pub(crate) struct ByteCreditPermit {
    inner: Arc<ByteCreditInner>,
    bytes: usize,
}

impl fmt::Debug for ByteCreditPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ByteCreditPermit")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for ByteCreditPermit {
    fn drop(&mut self) {
        self.inner
            .outstanding
            .fetch_sub(self.bytes, Ordering::AcqRel);
        self.inner.available.notify_waiters();
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ChunkAllocator {
    pool: ChunkPool,
    data_credit: ByteCredit,
    control_credit: ByteCredit,
}

impl Default for ChunkAllocator {
    fn default() -> Self {
        Self::new(
            shared_chunk_pool().clone(),
            DEFAULT_CONNECTION_DATA_BYTES,
            DEFAULT_CONNECTION_CONTROL_BYTES,
        )
    }
}

impl ChunkAllocator {
    pub(crate) fn new(
        pool: ChunkPool,
        connection_data_bytes: usize,
        connection_control_bytes: usize,
    ) -> Self {
        Self {
            pool,
            data_credit: ByteCredit::new(connection_data_bytes),
            control_credit: ByteCredit::new(connection_control_bytes),
        }
    }

    pub(crate) async fn allocate_data(&self, capacity: usize) -> Result<OwnedChunk, NetworkError> {
        if capacity > BODY_CHUNK_MAX {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: capacity,
                max_bytes: BODY_CHUNK_MAX,
            });
        }
        let permit = self
            .data_credit
            .acquire(DATA_SLAB_BYTES, "reserve per-connection data slab bytes")
            .await?;
        self.pool.allocate(ChunkClass::Data, permit)
    }

    pub(crate) async fn allocate_control(
        &self,
        capacity: usize,
    ) -> Result<OwnedChunk, NetworkError> {
        if capacity > RPC_CONTROL_PAYLOAD_MAX {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: capacity,
                max_bytes: RPC_CONTROL_PAYLOAD_MAX,
            });
        }
        let permit = self
            .control_credit
            .acquire(
                CONTROL_SLAB_BYTES,
                "reserve per-connection control slab bytes",
            )
            .await?;
        self.pool.allocate(ChunkClass::Control, permit)
    }

    pub(crate) async fn adopt_data(
        &self,
        bytes: Vec<u8>,
        start: usize,
        end: usize,
    ) -> Result<OwnedChunk, NetworkError> {
        let permit = self
            .data_credit
            .acquire(
                DATA_SLAB_BYTES,
                "reserve adopted per-connection data slab bytes",
            )
            .await?;
        let chunk = self
            .pool
            .adopt(ChunkClass::Data, bytes, start, end, permit)?;
        observe_chunk_identity(chunk.slab_id(), "producer-buffer-adopted");
        Ok(chunk)
    }

    #[cfg(test)]
    pub(crate) fn metrics(&self) -> NetworkChunkMetrics {
        self.pool.metrics()
    }

    pub(crate) fn record_payload_copy(&self, bytes: usize, reason: PayloadCopyReason) {
        self.pool.inner.record_copy(bytes, reason);
    }
}

struct PooledSlab {
    id: u64,
    class: ChunkClass,
    bytes: Option<Vec<u8>>,
    pool: Weak<ChunkPoolInner>,
    recyclable: bool,
    _connection_credit: Option<ByteCreditPermit>,
}

impl PooledSlab {
    fn bytes(&self) -> &[u8] {
        self.bytes.as_deref().expect("live pooled slab")
    }

    fn bytes_mut(&mut self) -> &mut [u8] {
        self.bytes.as_deref_mut().expect("live pooled slab")
    }

    fn storage_limit(&self) -> usize {
        self.class.resident_bytes()
    }

    fn ensure_initialized(&mut self, len: usize) {
        let bytes = self.bytes.as_mut().expect("live pooled slab");
        debug_assert!(bytes.capacity() >= len);
        if bytes.len() < len {
            bytes.resize(len, 0);
        }
    }
}

impl fmt::Debug for PooledSlab {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PooledSlab")
            .field("id", &self.id)
            .field("class", &self.class)
            .finish_non_exhaustive()
    }
}

impl Drop for PooledSlab {
    fn drop(&mut self) {
        let Some(bytes) = self.bytes.take() else {
            return;
        };
        if let Some(pool) = self.pool.upgrade() {
            if self.recyclable {
                pool.recycle(self.class, bytes);
            } else {
                pool.discard_adopted(self.class, bytes);
            }
        }
    }
}

struct ReceiveCreditLeaseInner {
    bytes: usize,
    _primary_permit: Option<ByteCreditPermit>,
    _secondary_permit: Option<ByteCreditPermit>,
    return_credit: Option<CreditReturn>,
}

impl fmt::Debug for ReceiveCreditLeaseInner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReceiveCreditLease")
            .field("bytes", &self.bytes)
            .field(
                "permit_count",
                &(usize::from(self._primary_permit.is_some())
                    + usize::from(self._secondary_permit.is_some())),
            )
            .field("returns_remote_credit", &self.return_credit.is_some())
            .finish()
    }
}

impl Drop for ReceiveCreditLeaseInner {
    fn drop(&mut self) {
        if let Some(return_credit) = &self.return_credit {
            return_credit(self.bytes);
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReceiveCreditLease {
    inner: Arc<ReceiveCreditLeaseInner>,
}

impl ReceiveCreditLease {
    pub(crate) fn new(
        bytes: usize,
        primary_permit: Option<ByteCreditPermit>,
        secondary_permit: Option<ByteCreditPermit>,
        return_credit: Option<CreditReturn>,
    ) -> Self {
        Self {
            inner: Arc::new(ReceiveCreditLeaseInner {
                bytes,
                _primary_permit: primary_permit,
                _secondary_permit: secondary_permit,
                return_credit,
            }),
        }
    }

    pub(crate) fn untracked(bytes: usize) -> Self {
        Self::new(bytes, None, None, None)
    }
}

/// A uniquely owned slab whose initialized range can be framed and encrypted
/// in place.
pub(crate) struct OwnedChunk {
    slab: Arc<PooledSlab>,
    start: usize,
    end: usize,
    payload_limit: usize,
    receive_credit: Option<ReceiveCreditLease>,
}

impl fmt::Debug for OwnedChunk {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnedChunk")
            .field("slab_id", &self.slab_id())
            .field("class", &self.slab.class)
            .field("range", &(self.start..self.end))
            .field("payload_limit", &self.payload_limit)
            .finish()
    }
}

impl OwnedChunk {
    pub(crate) fn slab_id(&self) -> u64 {
        self.slab.id
    }

    pub(crate) fn class(&self) -> ChunkClass {
        self.slab.class
    }

    pub(crate) const fn len(&self) -> usize {
        self.end - self.start
    }

    pub(crate) fn tailroom(&self) -> usize {
        self.slab.storage_limit() - self.end
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.slab.bytes()[self.start..self.end]
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        let start = self.start;
        let end = self.end;
        &mut self.unique_slab_mut().bytes_mut()[start..end]
    }

    pub(crate) fn writable_payload(&mut self) -> &mut [u8] {
        let end = self.end;
        let payload_limit = self.payload_limit;
        self.unique_slab_mut().ensure_initialized(payload_limit);
        &mut self.unique_slab_mut().bytes_mut()[end..payload_limit]
    }

    pub(crate) fn commit_payload(&mut self, bytes: usize) -> Result<(), NetworkError> {
        let next = self
            .end
            .checked_add(bytes)
            .ok_or_else(|| NetworkError::ResourceExhausted {
                operation: "commit pooled chunk payload".to_string(),
                requested_bytes: usize::MAX,
            })?;
        if next > self.payload_limit {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: next.saturating_sub(CHUNK_HEADROOM_BYTES),
                max_bytes: self.payload_limit.saturating_sub(CHUNK_HEADROOM_BYTES),
            });
        }
        self.end = next;
        Ok(())
    }

    pub(crate) fn copy_payload_from(
        &mut self,
        bytes: &[u8],
        reason: PayloadCopyReason,
    ) -> Result<(), NetworkError> {
        if bytes.len() > self.writable_payload().len() {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: bytes.len(),
                max_bytes: self.writable_payload().len(),
            });
        }
        self.writable_payload()[..bytes.len()].copy_from_slice(bytes);
        self.commit_payload(bytes.len())?;
        if let Some(pool) = self.slab.pool.upgrade() {
            pool.record_copy(bytes.len(), reason);
        }
        Ok(())
    }

    pub(crate) fn prepend(&mut self, header: &[u8]) -> Result<(), NetworkError> {
        if header.len() > self.start {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: header.len(),
                max_bytes: self.start,
            });
        }
        let previous_start = self.start;
        let start = previous_start - header.len();
        self.unique_slab_mut().bytes_mut()[start..previous_start].copy_from_slice(header);
        self.start = start;
        Ok(())
    }

    pub(crate) fn append(&mut self, trailer: &[u8]) -> Result<(), NetworkError> {
        let next =
            self.end
                .checked_add(trailer.len())
                .ok_or_else(|| NetworkError::ResourceExhausted {
                    operation: "append pooled chunk trailer".to_string(),
                    requested_bytes: usize::MAX,
                })?;
        if next > self.slab.storage_limit() {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: trailer.len(),
                max_bytes: self.tailroom(),
            });
        }
        let end = self.end;
        self.unique_slab_mut().ensure_initialized(next);
        self.unique_slab_mut().bytes_mut()[end..next].copy_from_slice(trailer);
        self.end = next;
        Ok(())
    }

    pub(crate) fn strip_prefix(&mut self, bytes: usize) -> Result<(), NetworkError> {
        if bytes > self.len() {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "cannot strip {bytes} prefix bytes from {}-byte pooled chunk",
                    self.len()
                ),
            });
        }
        self.start += bytes;
        Ok(())
    }

    pub(crate) fn strip_suffix(&mut self, bytes: usize) -> Result<(), NetworkError> {
        if bytes > self.len() {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "cannot strip {bytes} suffix bytes from {}-byte pooled chunk",
                    self.len()
                ),
            });
        }
        self.end -= bytes;
        Ok(())
    }

    pub(crate) fn set_range(&mut self, start: usize, end: usize) -> Result<(), NetworkError> {
        if start > end || end > self.slab.bytes().len() {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "invalid pooled chunk range {start}..{end} for {}-byte slab",
                    self.slab.bytes().len()
                ),
            });
        }
        self.start = start;
        self.end = end;
        self.payload_limit = self
            .slab
            .storage_limit()
            .saturating_sub(CHUNK_TAILROOM_BYTES);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn attach_receive_credit(&mut self, credit: ReceiveCreditLease) {
        debug_assert!(self.receive_credit.is_none());
        self.receive_credit = Some(credit);
    }

    pub(crate) fn replace_receive_credit(&mut self, credit: ReceiveCreditLease) {
        self.receive_credit = Some(credit);
    }

    pub(crate) fn into_lease(self) -> ChunkLease {
        let len = self.len();
        let credit = self
            .receive_credit
            .unwrap_or_else(|| ReceiveCreditLease::untracked(len));
        ChunkLease {
            data: SharedSlabView {
                slab: self.slab,
                start: self.start,
                end: self.end,
            },
            credit,
        }
    }

    fn unique_slab_mut(&mut self) -> &mut PooledSlab {
        Arc::get_mut(&mut self.slab)
            .expect("OwnedChunk must retain unique ownership while it is mutable")
    }
}

#[derive(Clone)]
pub(crate) struct SharedSlabView {
    slab: Arc<PooledSlab>,
    start: usize,
    end: usize,
}

impl SharedSlabView {
    pub(crate) fn slab_id(&self) -> u64 {
        self.slab.id
    }

    pub(crate) const fn len(&self) -> usize {
        self.end - self.start
    }

    pub(crate) fn slice(&self, start: usize, end: usize) -> Result<Self, NetworkError> {
        if start > end || end > self.len() {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "invalid pooled slab view {start}..{end} for {}-byte view",
                    self.len()
                ),
            });
        }
        Ok(Self {
            slab: Arc::clone(&self.slab),
            start: self.start + start,
            end: self.start + end,
        })
    }
}

impl fmt::Debug for SharedSlabView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SharedSlabView")
            .field("slab_id", &self.slab_id())
            .field("range", &(self.start..self.end))
            .finish()
    }
}

impl Deref for SharedSlabView {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.slab.bytes()[self.start..self.end]
    }
}

impl AsRef<[u8]> for SharedSlabView {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

/// Immutable application-visible view plus the receive credit associated with
/// the retained allocation.
#[derive(Clone, Debug)]
pub(crate) struct ChunkLease {
    data: SharedSlabView,
    credit: ReceiveCreditLease,
}

impl ChunkLease {
    pub(crate) fn slab_id(&self) -> u64 {
        self.data.slab_id()
    }

    pub(crate) const fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub(crate) fn slice(&self, start: usize, end: usize) -> Result<Self, NetworkError> {
        Ok(Self {
            data: self.data.slice(start, end)?,
            credit: self.credit.clone(),
        })
    }

    pub(crate) fn try_into_owned(self) -> Result<OwnedChunk, Self> {
        if Arc::strong_count(&self.data.slab) != 1 || Arc::strong_count(&self.credit.inner) != 1 {
            return Err(self);
        }
        let Self { data, credit } = self;
        let payload_limit = data
            .slab
            .storage_limit()
            .saturating_sub(CHUNK_TAILROOM_BYTES);
        Ok(OwnedChunk {
            slab: data.slab,
            start: data.start,
            end: data.end,
            payload_limit,
            receive_credit: Some(credit),
        })
    }
}

impl Deref for ChunkLease {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResetReason {
    pub detail: String,
}

pub(crate) type ChunkStreamFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

/// Receive half of the canonical internal authenticated record stream.
pub(crate) trait AuthenticatedChunkReader: Send {
    fn allocator(&self) -> &ChunkAllocator;
    fn max_payload_bytes(&self) -> usize;
    fn recv_chunk(&mut self) -> ChunkStreamFuture<'_, Option<ChunkLease>>;
}

/// Send half of the canonical internal authenticated record stream.
pub(crate) trait AuthenticatedChunkWriter: Send {
    fn max_payload_bytes(&self) -> usize;
    fn allocator(&self) -> &ChunkAllocator;
    fn send_chunk(&mut self, chunk: OwnedChunk) -> ChunkStreamFuture<'_, ()>;
    fn finish(&mut self) -> ChunkStreamFuture<'_, ()>;
    fn reset(&mut self, reason: ResetReason) -> ChunkStreamFuture<'_, ()>;
}

pub(crate) type BoxedAuthenticatedChunkReader = Box<dyn AuthenticatedChunkReader>;
pub(crate) type BoxedAuthenticatedChunkWriter = Box<dyn AuthenticatedChunkWriter>;

/// Canonical owned-record duplex stream. The halves are ownership-native so
/// the carrier can run one persistent receive task and one persistent transmit
/// task without a shared byte-stream mutex.
pub(crate) struct AuthenticatedChunkStream {
    reader: BoxedAuthenticatedChunkReader,
    writer: BoxedAuthenticatedChunkWriter,
}

impl fmt::Debug for AuthenticatedChunkStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthenticatedChunkStream")
            .field("max_payload_bytes", &self.max_payload_bytes())
            .finish_non_exhaustive()
    }
}

impl AuthenticatedChunkStream {
    pub(crate) fn new(
        reader: BoxedAuthenticatedChunkReader,
        writer: BoxedAuthenticatedChunkWriter,
    ) -> Result<Self, NetworkError> {
        if reader.max_payload_bytes() != writer.max_payload_bytes() {
            return Err(NetworkError::Serialization {
                detail: "authenticated chunk stream halves disagree on payload bounds".to_string(),
            });
        }
        Ok(Self { reader, writer })
    }

    pub(crate) fn max_payload_bytes(&self) -> usize {
        self.reader.max_payload_bytes()
    }

    pub(crate) fn into_halves(
        self,
    ) -> (BoxedAuthenticatedChunkReader, BoxedAuthenticatedChunkWriter) {
        (self.reader, self.writer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn test_pool() -> ChunkPool {
        ChunkPool::new(DATA_SLAB_BYTES * 2, CONTROL_SLAB_BYTES * 2, 1, 1)
    }

    #[tokio::test]
    async fn one_slab_survives_prepend_append_and_body_delivery() {
        let allocator =
            ChunkAllocator::new(test_pool(), DATA_SLAB_BYTES * 2, CONTROL_SLAB_BYTES * 2);
        let mut chunk = allocator
            .allocate_data(BODY_CHUNK_MAX)
            .await
            .expect("allocate data chunk");
        let slab_id = chunk.slab_id();
        chunk
            .copy_payload_from(b"payload", PayloadCopyReason::SharedSourceNormalization)
            .expect("copy test payload");
        chunk.prepend(&[0x33; RPC_RECORD_HEADER_BYTES]).unwrap();
        chunk.prepend(&[0x22; MUX_RECORD_HEADER_BYTES]).unwrap();
        chunk.prepend(&[0x11; SECURE_RECORD_HEADER_BYTES]).unwrap();
        chunk.append(&[0x44; SECURE_RECORD_TAG_BYTES]).unwrap();
        assert_eq!(chunk.slab_id(), slab_id);
        chunk.strip_prefix(SECURE_RECORD_HEADER_BYTES).unwrap();
        chunk.strip_suffix(SECURE_RECORD_TAG_BYTES).unwrap();
        chunk.strip_prefix(MUX_RECORD_HEADER_BYTES).unwrap();
        chunk.strip_prefix(RPC_RECORD_HEADER_BYTES).unwrap();
        let lease = chunk.into_lease();
        assert_eq!(lease.slab_id(), slab_id);
        assert_eq!(lease.as_slice(), b"payload");
    }

    #[tokio::test]
    async fn receive_credit_returns_only_after_the_last_view_drops() {
        let allocator =
            ChunkAllocator::new(test_pool(), DATA_SLAB_BYTES * 2, CONTROL_SLAB_BYTES * 2);
        let stream_credit = ByteCredit::new(BODY_CHUNK_MAX);
        let permit = stream_credit
            .acquire(BODY_CHUNK_MAX, "test receive credit")
            .await
            .expect("reserve receive credit");
        let returned = Arc::new(AtomicUsize::new(0));
        let returned_for_drop = Arc::clone(&returned);
        let credit = ReceiveCreditLease::new(
            BODY_CHUNK_MAX,
            Some(permit),
            None,
            Some(Arc::new(move |bytes| {
                returned_for_drop.fetch_add(bytes, Ordering::Relaxed);
            })),
        );
        let mut chunk = allocator
            .allocate_data(BODY_CHUNK_MAX)
            .await
            .expect("allocate data chunk");
        chunk.commit_payload(BODY_CHUNK_MAX).unwrap();
        chunk.attach_receive_credit(credit);
        let lease = chunk.into_lease();
        let clone = lease.clone();
        drop(lease);
        assert_eq!(stream_credit.outstanding(), BODY_CHUNK_MAX);
        assert_eq!(returned.load(Ordering::Relaxed), 0);
        drop(clone);
        assert_eq!(stream_credit.outstanding(), 0);
        assert_eq!(returned.load(Ordering::Relaxed), BODY_CHUNK_MAX);
    }

    #[tokio::test]
    async fn control_pool_remains_available_when_data_pool_is_exhausted() {
        let pool = ChunkPool::new(DATA_SLAB_BYTES, CONTROL_SLAB_BYTES, 0, 0);
        let allocator = ChunkAllocator::new(pool, DATA_SLAB_BYTES * 2, CONTROL_SLAB_BYTES * 2);
        let _data = allocator
            .allocate_data(BODY_CHUNK_MAX)
            .await
            .expect("allocate only data slab");
        let second = allocator.allocate_data(BODY_CHUNK_MAX).await;
        assert!(matches!(
            second,
            Err(NetworkError::ResourceExhausted { .. })
        ));
        let control = allocator
            .allocate_control(RPC_CONTROL_PAYLOAD_MAX)
            .await
            .expect("control allocation must not depend on data capacity");
        assert_eq!(control.class(), ChunkClass::Control);
    }

    #[tokio::test]
    async fn retained_slab_is_reused_without_counting_another_allocation() {
        let allocator =
            ChunkAllocator::new(test_pool(), DATA_SLAB_BYTES * 2, CONTROL_SLAB_BYTES * 2);
        let first = allocator.allocate_data(1).await.expect("first slab");
        drop(first);
        let after_first = allocator.metrics();
        let second = allocator.allocate_data(1).await.expect("reused slab");
        drop(second);
        let after_second = allocator.metrics();
        assert_eq!(after_second.allocations, after_first.allocations);
        assert_eq!(after_second.reuses, after_first.reuses + 1);
        assert!(after_second.retained_bytes >= DATA_SLAB_BYTES);
        assert_eq!(after_second.allocated_bytes, DATA_SLAB_BYTES);
        assert_eq!(after_second.peak_allocated_bytes, DATA_SLAB_BYTES);
    }

    #[test]
    fn payload_copy_metrics_preserve_the_reason_breakdown() {
        let allocator =
            ChunkAllocator::new(test_pool(), DATA_SLAB_BYTES * 2, CONTROL_SLAB_BYTES * 2);
        allocator.record_payload_copy(11, PayloadCopyReason::SharedSourceNormalization);
        allocator.record_payload_copy(13, PayloadCopyReason::ByteStreamWriteAdapter);
        allocator.record_payload_copy(17, PayloadCopyReason::ByteStreamReadAdapter);
        allocator.record_payload_copy(19, PayloadCopyReason::ContiguousBodyMaterialization);

        let metrics = allocator.metrics();
        assert_eq!(metrics.payload_copy_bytes, 60);
        assert_eq!(metrics.shared_source_normalization_copy_bytes, 11);
        assert_eq!(metrics.byte_stream_write_adapter_copy_bytes, 13);
        assert_eq!(metrics.byte_stream_read_adapter_copy_bytes, 17);
        assert_eq!(metrics.contiguous_body_materialization_copy_bytes, 19);
    }
}
