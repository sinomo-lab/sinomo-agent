use crate::error::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
use crate::io_observer;
use sinomo_object::{BorrowedDocumentBuilder, DecodeLimits, DecodeOptions, LengthMode, serde_ext};
use std::future::Future;
use std::io::IoSlice;
#[cfg(any(test, feature = "test-support"))]
use std::io::{Read, Write};
use std::net::SocketAddr;
#[cfg(any(test, feature = "test-support"))]
use std::net::TcpStream;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const SINOMO_NETWORK_OBJECT_VERSION: u16 = 1;
const OBJECT_FRAME_WIRE_MAGIC: [u8; 4] = *b"SOF2";
const OBJECT_FRAME_WIRE_PREFIX_BYTES: usize = 12;
/// Default limit for establishing a network path and for each period without
/// stream I/O progress. This is a per-phase/per-progress limit, not a total
/// operation deadline.
pub const DEFAULT_NETWORK_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout policy for daemon object-RPC transport.
///
/// Connect/open timeouts bound cases where no usable transport can be established.
/// Idle timeouts are optional transfer-progress limits and restart after each
/// successful read or write. `Default` applies [`DEFAULT_NETWORK_TIMEOUT`] to
/// both phases; use `none()` only when a surrounding absolute deadline is
/// already enforced or for a deliberately long-lived server-side stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DaemonRpcTimeouts {
    pub connect_timeout: Option<Duration>,
    pub idle_timeout: Option<Duration>,
}

impl DaemonRpcTimeouts {
    pub const fn none() -> Self {
        Self {
            connect_timeout: None,
            idle_timeout: None,
        }
    }

    pub const fn connect_only(connect_timeout: Duration) -> Self {
        Self {
            connect_timeout: Some(connect_timeout),
            idle_timeout: None,
        }
    }

    pub const fn idle_only(idle_timeout: Duration) -> Self {
        Self {
            connect_timeout: None,
            idle_timeout: Some(idle_timeout),
        }
    }

    pub const fn connect_and_idle(timeout: Duration) -> Self {
        Self {
            connect_timeout: Some(timeout),
            idle_timeout: Some(timeout),
        }
    }

    pub const fn with_connect_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub const fn with_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.idle_timeout = timeout;
        self
    }
}

impl Default for DaemonRpcTimeouts {
    fn default() -> Self {
        Self::connect_and_idle(DEFAULT_NETWORK_TIMEOUT)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectFrameIoOptions {
    pub idle_timeout: Option<Duration>,
}

/// Allocation-free timestamps returned only to physical carriers that are
/// recording a sampled frame write.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectFrameWriteTiming {
    pub encode_started_monotonic_ns: u64,
    pub encode_completed_monotonic_ns: u64,
    pub socket_write_completed_monotonic_ns: u64,
    pub wire_bytes: u32,
}

/// Allocation-free timestamps for one persistent-carrier frame read. `None`
/// is returned while timeline capture is disabled, so the ordinary path pays
/// only a relaxed capture-state check at each first-read poll.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectFrameReadTiming {
    pub capture_generation: u64,
    pub ready_monotonic_ns: u64,
    pub read_poll_monotonic_ns: u64,
    pub first_read_monotonic_ns: u64,
    pub body_read_completed_monotonic_ns: u64,
    pub decode_started_monotonic_ns: u64,
    pub decoded_monotonic_ns: u64,
    pub wire_bytes: u32,
    pub first_read_bytes: u32,
}

/// Reusable receiver-local readiness observer for a persistent carrier.
///
/// The proxy Waker is allocated once with the carrier. Capture-disabled reads
/// continue to register Tokio's original Waker directly; an active bounded
/// capture registers the proxy only for the first physical read of a frame.
pub struct ObjectFrameReadinessProbe {
    shared: Arc<ObjectFrameReadinessProbeShared>,
    waker: Waker,
}

#[derive(Default)]
struct ObjectFrameReadinessProbeState {
    parent: Option<Waker>,
    capture_generation: u64,
    ready_monotonic_ns: u64,
    armed: bool,
}

#[derive(Default)]
struct ObjectFrameReadinessProbeShared {
    state: Mutex<ObjectFrameReadinessProbeState>,
}

impl ObjectFrameReadinessProbeShared {
    fn state(&self) -> std::sync::MutexGuard<'_, ObjectFrameReadinessProbeState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Wake for ObjectFrameReadinessProbeShared {
    fn wake(self: Arc<Self>) {
        Wake::wake_by_ref(&self);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let parent = {
            let mut state = self.state();
            let active_generation = crate::timeline::network_timeline_capture_generation();
            if state.armed
                && active_generation != 0
                && active_generation == state.capture_generation
                && state.ready_monotonic_ns == 0
            {
                state.ready_monotonic_ns = crate::network_timeline_monotonic_ns();
            }
            // Keep the parent registered until the read completes. Tokio may
            // issue duplicate readiness notifications before the task is
            // polled; each must retain a path to the original task Waker.
            state.parent.clone()
        };
        if let Some(parent) = parent {
            parent.wake();
        }
    }
}

impl ObjectFrameReadinessProbe {
    pub fn new() -> Self {
        let shared = Arc::new(ObjectFrameReadinessProbeShared::default());
        let waker = Waker::from(Arc::clone(&shared));
        Self { shared, waker }
    }

    fn prepare_poll(&self, capture_generation: u64, parent: &Waker) -> Option<u64> {
        let mut state = self.shared.state();
        if !state.armed || state.capture_generation != capture_generation {
            state.capture_generation = capture_generation;
            state.ready_monotonic_ns = 0;
            state.armed = true;
        }
        if !state
            .parent
            .as_ref()
            .is_some_and(|registered| registered.will_wake(parent))
        {
            state.parent = Some(parent.clone());
        }
        let ready_monotonic_ns = state.ready_monotonic_ns;
        state.ready_monotonic_ns = 0;
        (ready_monotonic_ns != 0).then_some(ready_monotonic_ns)
    }

    fn disarm(&self) {
        let mut state = self.shared.state();
        state.parent = None;
        state.capture_generation = 0;
        state.ready_monotonic_ns = 0;
        state.armed = false;
    }
}

impl Default for ObjectFrameReadinessProbe {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ObjectFrameFirstRead {
    bytes: usize,
    capture_generation: u64,
    ready_monotonic_ns: u64,
    read_poll_monotonic_ns: u64,
    first_read_monotonic_ns: u64,
}

struct ObjectFrameFirstReadFuture<'a, R> {
    reader: &'a mut R,
    buffer: &'a mut [u8],
    probe: &'a ObjectFrameReadinessProbe,
    armed: bool,
}

impl<'a, R> ObjectFrameFirstReadFuture<'a, R> {
    fn new(reader: &'a mut R, buffer: &'a mut [u8], probe: &'a ObjectFrameReadinessProbe) -> Self {
        Self {
            reader,
            buffer,
            probe,
            armed: false,
        }
    }
}

impl<R: AsyncRead + Unpin> Future for ObjectFrameFirstReadFuture<'_, R> {
    type Output = std::io::Result<ObjectFrameFirstRead>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let capture_generation = crate::timeline::network_timeline_capture_generation();
        if capture_generation == 0 {
            if this.armed {
                this.probe.disarm();
                this.armed = false;
            }
            let mut read_buffer = tokio::io::ReadBuf::new(this.buffer);
            return match Pin::new(&mut *this.reader).poll_read(cx, &mut read_buffer) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(())) => Poll::Ready(Ok(ObjectFrameFirstRead {
                    bytes: read_buffer.filled().len(),
                    ..ObjectFrameFirstRead::default()
                })),
                Poll::Ready(Err(source)) => Poll::Ready(Err(source)),
            };
        }

        let ready_monotonic_ns = this.probe.prepare_poll(capture_generation, cx.waker());
        this.armed = true;
        let read_poll_monotonic_ns = ready_monotonic_ns
            .is_some()
            .then(crate::network_timeline_monotonic_ns);
        let mut readiness_context = Context::from_waker(&this.probe.waker);
        let mut read_buffer = tokio::io::ReadBuf::new(this.buffer);
        match Pin::new(&mut *this.reader).poll_read(&mut readiness_context, &mut read_buffer) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                this.probe.disarm();
                this.armed = false;
                match result {
                    Ok(()) => {
                        let bytes = read_buffer.filled().len();
                        let generation_still_active =
                            crate::timeline::network_timeline_capture_generation()
                                == capture_generation;
                        let first_read_monotonic_ns = (bytes > 0 && generation_still_active)
                            .then(crate::network_timeline_monotonic_ns)
                            .unwrap_or(0);
                        let ready_monotonic_ns = ready_monotonic_ns
                            .filter(|ready| {
                                generation_still_active
                                    && *ready <= read_poll_monotonic_ns.unwrap_or(0)
                            })
                            .unwrap_or(0);
                        Poll::Ready(Ok(ObjectFrameFirstRead {
                            bytes,
                            capture_generation: generation_still_active
                                .then_some(capture_generation)
                                .unwrap_or(0),
                            ready_monotonic_ns,
                            read_poll_monotonic_ns: (ready_monotonic_ns != 0)
                                .then_some(read_poll_monotonic_ns.unwrap_or(0))
                                .unwrap_or(0),
                            first_read_monotonic_ns,
                        }))
                    }
                    Err(source) => Poll::Ready(Err(source)),
                }
            }
        }
    }
}

impl<R> Drop for ObjectFrameFirstReadFuture<'_, R> {
    fn drop(&mut self) {
        if self.armed {
            self.probe.disarm();
        }
    }
}

/// Maximum encoded SOF2 body accepted by persistent object-RPC channel
/// carriers. Logical control heads are capped at 256 KiB and data quanta at
/// 32 KiB; the remaining space covers the SOBJ document and tunnel envelope
/// without letting a corrupt length prefix reserve arbitrary host memory.
pub const MAX_OBJECT_RPC_CHANNEL_WIRE_FRAME_BYTES: usize = 512 * 1024;

impl Default for ObjectFrameIoOptions {
    fn default() -> Self {
        Self::idle_timeout(DEFAULT_NETWORK_TIMEOUT)
    }
}

impl ObjectFrameIoOptions {
    pub const fn none() -> Self {
        Self { idle_timeout: None }
    }

    pub const fn idle_timeout(idle_timeout: Duration) -> Self {
        Self {
            idle_timeout: Some(idle_timeout),
        }
    }

    pub const fn with_idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }
}

pub type DaemonControlRequest = crate::ObjectRpcRequest;
pub type DaemonControlResponse = crate::ObjectRpcResponse;

static OBJECT_FRAME_BYTE_BUDGET: LazyLock<ObjectFrameByteBudget> =
    LazyLock::new(|| ObjectFrameByteBudget::fixed(MAX_IN_FLIGHT_OBJECT_FRAME_BYTES));

/// Strict process-wide bound for encoded object-frame bodies awaiting decode.
///
/// This admits 128 maximum-sized persistent-carrier frames, or 2,048 normal
/// 32 KiB data quanta, while keeping admission entirely in memory. In
/// particular, frame receipt must never read procfs or serialize unrelated
/// channels on a process-global mutex.
const MAX_IN_FLIGHT_OBJECT_FRAME_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct ObjectFrameByteBudget {
    in_flight_bytes: Arc<AtomicUsize>,
    max_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct ObjectFrameByteBudgetPermit {
    budget: ObjectFrameByteBudget,
    bytes: usize,
}

impl ObjectFrameByteBudget {
    pub(crate) fn fixed(max_bytes: usize) -> Self {
        Self {
            in_flight_bytes: Arc::new(AtomicUsize::new(0)),
            max_bytes,
        }
    }

    pub(crate) fn try_acquire(
        &self,
        frame_bytes: usize,
    ) -> Result<ObjectFrameByteBudgetPermit, NetworkError> {
        match self.in_flight_bytes.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |in_flight_bytes| {
                in_flight_bytes
                    .checked_add(frame_bytes)
                    .filter(|would_use| *would_use <= self.max_bytes)
            },
        ) {
            Ok(_) => Ok(ObjectFrameByteBudgetPermit {
                budget: self.clone(),
                bytes: frame_bytes,
            }),
            Err(in_flight_bytes) => Err(NetworkError::ObjectFrameMemoryUnavailable {
                frame_bytes,
                in_flight_bytes,
                available_memory_bytes: Some(self.max_bytes.saturating_sub(in_flight_bytes)),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn in_flight_bytes(&self) -> usize {
        self.in_flight_bytes.load(Ordering::Acquire)
    }
}

impl Drop for ObjectFrameByteBudgetPermit {
    fn drop(&mut self) {
        let previous = self
            .budget
            .in_flight_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
        debug_assert!(previous >= self.bytes, "object-frame byte budget underflow");
    }
}

fn object_frame_byte_budget() -> &'static ObjectFrameByteBudget {
    &OBJECT_FRAME_BYTE_BUDGET
}

#[derive(Debug)]
pub struct AsyncDaemonControlListener {
    bind_address: String,
    listener: tokio::net::TcpListener,
}

impl AsyncDaemonControlListener {
    pub async fn bind(bind_address: impl Into<String>) -> Result<Self, NetworkError> {
        let bind_address = bind_address.into();
        let listener = tokio::net::TcpListener::bind(&bind_address)
            .await
            .map_err(|source| NetworkError::Io {
                operation: format!("bind async daemon control listener {bind_address}"),
                detail: source.to_string(),
            })?;
        mark_daemon_control_listener_non_inheritable(&listener).map_err(|source| {
            NetworkError::Io {
                operation: format!(
                    "mark async daemon control listener {bind_address} non-inheritable"
                ),
                detail: source.to_string(),
            }
        })?;
        Ok(Self {
            bind_address,
            listener,
        })
    }

    pub fn local_addr_string(&self) -> Result<String, NetworkError> {
        self.listener
            .local_addr()
            .map(|address| address.to_string())
            .map_err(|source| NetworkError::Io {
                operation: format!(
                    "read async daemon control listener address {}",
                    self.bind_address
                ),
                detail: source.to_string(),
            })
    }

    pub async fn accept(&self) -> Result<AsyncDaemonControlStream, NetworkError> {
        self.listener
            .accept()
            .await
            .map_err(|source| NetworkError::Io {
                operation: format!("accept async daemon control listener {}", self.bind_address),
                detail: source.to_string(),
            })
            .and_then(|(stream, _address)| {
                configure_daemon_control_tcp_stream(
                    &stream,
                    &format!("accepted daemon control stream {}", self.bind_address),
                )?;
                Ok(AsyncDaemonControlStream::new(stream))
            })
    }
}

#[cfg(windows)]
fn mark_daemon_control_listener_non_inheritable(
    listener: &tokio::net::TcpListener,
) -> std::io::Result<()> {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawSocket;

    type Handle = *mut c_void;

    unsafe extern "system" {
        fn SetHandleInformation(hObject: Handle, dwMask: u32, dwFlags: u32) -> i32;
    }

    const HANDLE_FLAG_INHERIT: u32 = 0x0000_0001;

    let handle = listener.as_raw_socket() as usize as Handle;
    let result = unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
fn mark_daemon_control_listener_non_inheritable(
    _listener: &tokio::net::TcpListener,
) -> std::io::Result<()> {
    Ok(())
}

fn configure_daemon_control_tcp_stream(
    stream: &tokio::net::TcpStream,
    operation: &str,
) -> Result<(), NetworkError> {
    stream.set_nodelay(true).map_err(|source| NetworkError::Io {
        operation: operation.to_string(),
        detail: source.to_string(),
    })
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug)]
pub struct DaemonControlStream {
    stream: TcpStream,
}

#[cfg(any(test, feature = "test-support"))]
impl DaemonControlStream {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> Result<(), NetworkError> {
        self.stream
            .set_read_timeout(timeout)
            .map_err(|source| NetworkError::Io {
                operation: "configure daemon control read timeout".to_string(),
                detail: source.to_string(),
            })
    }

    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> Result<(), NetworkError> {
        self.stream
            .set_write_timeout(timeout)
            .map_err(|source| NetworkError::Io {
                operation: "configure daemon control write timeout".to_string(),
                detail: source.to_string(),
            })
    }
}

#[derive(Debug)]
pub struct AsyncDaemonControlStream {
    stream: tokio::net::TcpStream,
}

impl AsyncDaemonControlStream {
    pub fn new(stream: tokio::net::TcpStream) -> Self {
        Self { stream }
    }

    pub fn into_inner(self) -> tokio::net::TcpStream {
        self.stream
    }

    /// Peeks without consuming bytes so the daemon can distinguish a canonical
    /// object-RPC session from a narrowly scoped raw bootstrap handshake.
    pub async fn peek_prefix(&self, output: &mut [u8]) -> Result<usize, NetworkError> {
        self.stream
            .peek(output)
            .await
            .map_err(|source| NetworkError::Io {
                operation: "peek async daemon control stream".to_string(),
                detail: source.to_string(),
            })
    }

    pub fn peer_addr(&self) -> Result<SocketAddr, NetworkError> {
        self.stream.peer_addr().map_err(|source| NetworkError::Io {
            operation: "read async daemon control peer address".to_string(),
            detail: source.to_string(),
        })
    }

    pub async fn connect_with_options(
        address: &str,
        timeouts: DaemonRpcTimeouts,
    ) -> Result<Self, NetworkError> {
        let operation = format!("connect async daemon control stream {address}");
        let connect = tokio::net::TcpStream::connect(address);
        let stream = if let Some(timeout) = timeouts.connect_timeout {
            tokio::time::timeout(timeout, connect).await.map_err(|_| {
                NetworkError::timeout(
                    operation.clone(),
                    NetworkTimeoutPhase::Connect,
                    timeout,
                    NetworkDeliveryState::DefinitelyNotSent,
                )
            })?
        } else {
            connect.await
        }
        .map_err(|source| {
            NetworkError::delivery_failed(
                &operation,
                NetworkDeliveryState::DefinitelyNotSent,
                source.to_string(),
            )
        })?;
        configure_daemon_control_tcp_stream(
            &stream,
            &format!("connected daemon control stream {address}"),
        )
        .map_err(|error| {
            network_error_with_delivery_state(
                error,
                "configure async daemon control stream",
                NetworkDeliveryState::DefinitelyNotSent,
            )
        })?;
        Ok(Self::new(stream))
    }

    pub async fn shutdown_write(&mut self, address: &str) -> Result<(), NetworkError> {
        self.stream
            .shutdown()
            .await
            .map_err(|source| NetworkError::Io {
                operation: format!("finish async daemon control stream {address}"),
                detail: source.to_string(),
            })
    }
}

fn network_error_with_delivery_state(
    error: NetworkError,
    fallback_operation: &str,
    delivery_state: NetworkDeliveryState,
) -> NetworkError {
    if error.delivery_state().is_some() {
        return error;
    }
    match error {
        NetworkError::Io { operation, detail } => {
            NetworkError::delivery_failed(operation, delivery_state, detail)
        }
        error => {
            NetworkError::delivery_failed(fallback_operation, delivery_state, error.to_string())
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Read for DaemonControlStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buffer)
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Write for DaemonControlStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.stream.write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SinomoObjectCodec {
    pub version_id: u16,
    pub length_mode: LengthMode,
}

impl Default for SinomoObjectCodec {
    fn default() -> Self {
        Self {
            version_id: SINOMO_NETWORK_OBJECT_VERSION,
            length_mode: LengthMode::U32,
        }
    }
}

impl SinomoObjectCodec {
    pub const fn network_frame() -> Self {
        Self {
            version_id: SINOMO_NETWORK_OBJECT_VERSION,
            length_mode: LengthMode::U64,
        }
    }

    pub fn encode<T: serde::Serialize + ?Sized>(self, value: &T) -> Result<Vec<u8>, NetworkError> {
        serde_ext::to_bytes(value, self.version_id, self.length_mode).map_err(NetworkError::object)
    }

    pub fn append_to_vec<T: serde::Serialize + ?Sized>(
        self,
        out: &mut Vec<u8>,
        value: &T,
    ) -> Result<std::ops::Range<usize>, NetworkError> {
        serde_ext::append_bytes(out, value, self.version_id, self.length_mode)
            .map_err(NetworkError::object)
    }

    pub fn decode<T: serde::de::DeserializeOwned>(self, bytes: &[u8]) -> Result<T, NetworkError> {
        serde_ext::from_bytes_with_options(bytes, self.decode_options(bytes.len())?)
            .map_err(NetworkError::object)
    }

    pub fn decode_borrowed<'de, T>(self, bytes: &'de [u8]) -> Result<T, NetworkError>
    where
        T: serde::Deserialize<'de>,
    {
        serde_ext::from_borrowed_bytes_with_options(bytes, self.decode_options(bytes.len())?)
            .map_err(NetworkError::object)
    }

    fn decode_options(self, document_bytes: usize) -> Result<DecodeOptions, NetworkError> {
        let document_bytes =
            u64::try_from(document_bytes).map_err(|_| NetworkError::FrameTooLarge {
                actual_bytes: usize::MAX,
                max_bytes: usize::MAX,
            })?;
        Ok(DecodeOptions::new(DecodeLimits {
            max_document_bytes: document_bytes,
            max_container_items: document_bytes,
            max_string_bytes: document_bytes,
            max_bytes_value_bytes: document_bytes,
            // The serde network codec only emits dynamic documents. Reject a
            // schema section before its declared counts can amplify allocation.
            max_schema_types: 0,
            max_schema_fields_per_type: 0,
            ..DecodeLimits::default()
        })
        .with_expected_version_id(self.version_id))
    }
}

pub(crate) fn object_frame_len_prefix(
    len: usize,
) -> Result<[u8; OBJECT_FRAME_WIRE_PREFIX_BYTES], NetworkError> {
    let len = u64::try_from(len).map_err(|_| NetworkError::FrameTooLarge {
        actual_bytes: usize::MAX,
        max_bytes: usize::MAX,
    })?;
    let mut prefix = [0_u8; OBJECT_FRAME_WIRE_PREFIX_BYTES];
    prefix[..4].copy_from_slice(&OBJECT_FRAME_WIRE_MAGIC);
    prefix[4..].copy_from_slice(&len.to_be_bytes());
    Ok(prefix)
}

// Unconditional since stage C: registry-aware network-module dispatch frames
// typed requests before handing them to a module, and dylib network modules
// reuse this codec through the sinomo-core facade.
pub fn encode_object_frame_to_vec<T>(value: &T) -> Result<Vec<u8>, NetworkError>
where
    T: serde::Serialize + ?Sized,
{
    let mut frame = Vec::new();
    try_extend_from_slice(
        &mut frame,
        &[0_u8; OBJECT_FRAME_WIRE_PREFIX_BYTES],
        "object frame prefix",
    )?;
    let body = SinomoObjectCodec::network_frame().append_to_vec(&mut frame, value)?;
    let len_prefix = object_frame_len_prefix(body.len())?;
    frame[..OBJECT_FRAME_WIRE_PREFIX_BYTES].copy_from_slice(&len_prefix);
    Ok(frame)
}

#[cfg(test)]
pub fn encode_borrowed_object_frame_to_vec(
    document: &BorrowedDocumentBuilder<'_>,
) -> Result<Vec<u8>, NetworkError> {
    let parts = document.to_parts().map_err(NetworkError::object)?;
    let body_len = parts.document_len().map_err(NetworkError::object)?;
    let len_prefix = object_frame_len_prefix(body_len)?;
    let frame_bytes =
        len_prefix
            .len()
            .checked_add(body_len)
            .ok_or(NetworkError::FrameTooLarge {
                actual_bytes: usize::MAX,
                max_bytes: usize::MAX,
            })?;
    let mut frame = Vec::new();
    frame
        .try_reserve_exact(frame_bytes)
        .map_err(|_| network_allocation_error("encode borrowed object frame", frame_bytes))?;
    try_extend_from_slice(
        &mut frame,
        &len_prefix,
        "encode borrowed object frame prefix",
    )?;
    for slice in parts.as_slices() {
        try_extend_from_slice(&mut frame, slice, "encode borrowed object frame body")?;
    }
    Ok(frame)
}

// Unconditional since stage C: see encode_object_frame_to_vec.
pub fn decode_single_object_frame_from_slice<T>(frame: &[u8]) -> Result<T, NetworkError>
where
    T: serde::de::DeserializeOwned,
{
    if frame.len() < OBJECT_FRAME_WIRE_PREFIX_BYTES {
        return Err(NetworkError::Io {
            operation: "read object frame prefix".to_string(),
            detail: object_frame_eof_detail("prefix", frame.len(), OBJECT_FRAME_WIRE_PREFIX_BYTES),
        });
    }
    validate_object_frame_magic(&frame[..4])?;
    let len_bytes: [u8; 8] = frame[4..OBJECT_FRAME_WIRE_PREFIX_BYTES]
        .try_into()
        .expect("object frame length");
    let body_bytes = object_frame_len_from_u64(u64::from_be_bytes(len_bytes))?;
    let body_end = OBJECT_FRAME_WIRE_PREFIX_BYTES
        .checked_add(body_bytes)
        .ok_or_else(|| network_allocation_error("decode object frame", usize::MAX))?;
    if frame.len() < body_end {
        return Err(NetworkError::Io {
            operation: "read object frame body".to_string(),
            detail: object_frame_eof_detail(
                "body",
                frame.len().saturating_sub(OBJECT_FRAME_WIRE_PREFIX_BYTES),
                body_bytes,
            ),
        });
    }
    if frame.len() != body_end {
        return Err(NetworkError::Io {
            operation: "read object frame".to_string(),
            detail: format!(
                "object frame has {} trailing bytes",
                frame.len().saturating_sub(body_end)
            ),
        });
    }

    SinomoObjectCodec::network_frame().decode(&frame[OBJECT_FRAME_WIRE_PREFIX_BYTES..body_end])
}

fn object_frame_len_from_u64(len: u64) -> Result<usize, NetworkError> {
    usize::try_from(len)
        .map_err(|_| network_allocation_error("decode object frame length", usize::MAX))
}

fn validate_object_frame_magic(magic: &[u8]) -> Result<(), NetworkError> {
    if magic == OBJECT_FRAME_WIRE_MAGIC.as_slice() {
        Ok(())
    } else {
        Err(NetworkError::Io {
            operation: "read object frame prefix".to_string(),
            detail: "invalid SOF2 object frame magic".to_string(),
        })
    }
}

pub(crate) fn network_allocation_error(operation: &str, requested_bytes: usize) -> NetworkError {
    NetworkError::ResourceExhausted {
        operation: operation.to_string(),
        requested_bytes,
    }
}

pub fn try_reserve_vec<T>(
    vec: &mut Vec<T>,
    additional: usize,
    operation: &str,
) -> Result<(), NetworkError> {
    let requested_items = vec
        .len()
        .checked_add(additional)
        .ok_or(NetworkError::FrameTooLarge {
            actual_bytes: usize::MAX,
            max_bytes: usize::MAX,
        })?;
    let requested_bytes = requested_items
        .checked_mul(std::mem::size_of::<T>().max(1))
        .ok_or(NetworkError::FrameTooLarge {
            actual_bytes: usize::MAX,
            max_bytes: usize::MAX,
        })?;
    vec.try_reserve(additional)
        .map_err(|_| network_allocation_error(operation, requested_bytes))
}

pub fn try_zeroed_vec(len: usize, operation: &str) -> Result<Vec<u8>, NetworkError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| network_allocation_error(operation, len))?;
    bytes.resize(len, 0);
    Ok(bytes)
}

#[allow(dead_code)]
pub(crate) fn try_extend_from_slice(
    vec: &mut Vec<u8>,
    bytes: &[u8],
    operation: &str,
) -> Result<(), NetworkError> {
    try_reserve_vec(vec, bytes.len(), operation)?;
    vec.extend_from_slice(bytes);
    Ok(())
}

pub(crate) fn try_clone_string(value: &str, operation: &str) -> Result<String, NetworkError> {
    let mut cloned = String::new();
    cloned
        .try_reserve(value.len())
        .map_err(|_| network_allocation_error(operation, value.len()))?;
    cloned.push_str(value);
    Ok(cloned)
}

pub fn try_clone_id(
    id: &sinomo_base::Id,
    operation: &str,
) -> Result<sinomo_base::Id, NetworkError> {
    let value = try_clone_string(id.as_str(), operation)?;
    sinomo_base::Id::new(value).map_err(|error| NetworkError::Serialization {
        detail: error.to_string(),
    })
}

#[cfg(any(test, feature = "test-support"))]
pub fn write_object_frame_blocking<W, T>(writer: &mut W, value: &T) -> Result<(), NetworkError>
where
    W: Write,
    T: serde::Serialize + ?Sized,
{
    let bytes = SinomoObjectCodec::network_frame().encode(value)?;
    let len_prefix = object_frame_len_prefix(bytes.len())?;
    writer
        .write_all(&len_prefix)
        .and_then(|()| writer.write_all(&bytes))
        .map_err(|source| NetworkError::Io {
            operation: "write object frame".to_string(),
            detail: source.to_string(),
        })?;
    if let Some(observer) = io_observer() {
        observer.on_net_sent(OBJECT_FRAME_WIRE_PREFIX_BYTES as u64 + bytes.len() as u64);
    }
    Ok(())
}

#[cfg(any(test, feature = "test-support"))]
pub fn read_object_frame_blocking<R, T>(reader: &mut R) -> Result<T, NetworkError>
where
    R: Read,
    T: serde::de::DeserializeOwned,
{
    read_object_frame_blocking_with_budget(reader, object_frame_byte_budget())
}

#[cfg(any(test, feature = "test-support"))]
fn read_object_frame_blocking_with_budget<R, T>(
    reader: &mut R,
    budget: &ObjectFrameByteBudget,
) -> Result<T, NetworkError>
where
    R: Read,
    T: serde::de::DeserializeOwned,
{
    let len = read_object_frame_len_blocking(reader)?;
    let _permit = budget.try_acquire(len)?;
    let mut bytes = try_zeroed_vec(len, "read object frame body")?;
    read_exact_blocking_frame(reader, &mut bytes, "read object frame body", "body")?;
    if let Some(observer) = io_observer() {
        observer.on_net_recv(OBJECT_FRAME_WIRE_PREFIX_BYTES as u64 + bytes.len() as u64);
    }
    SinomoObjectCodec::network_frame().decode(&bytes)
}

#[cfg(any(test, feature = "test-support"))]
fn read_object_frame_len_blocking<R: Read>(reader: &mut R) -> Result<usize, NetworkError> {
    let mut prefix = [0_u8; OBJECT_FRAME_WIRE_PREFIX_BYTES];
    read_exact_blocking_frame(reader, &mut prefix, "read object frame prefix", "prefix")?;
    validate_object_frame_magic(&prefix[..4])?;
    let len_bytes: [u8; 8] = prefix[4..].try_into().expect("object frame length");
    object_frame_len_from_u64(u64::from_be_bytes(len_bytes))
}

#[cfg(any(test, feature = "test-support"))]
fn read_exact_blocking_frame<R: Read>(
    reader: &mut R,
    buf: &mut [u8],
    operation: &'static str,
    part: &'static str,
) -> Result<(), NetworkError> {
    let expected = buf.len();
    let mut read = 0_usize;
    while read < expected {
        match reader.read(&mut buf[read..]) {
            Ok(0) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: object_frame_eof_detail(part, read, expected),
                });
            }
            Ok(n) => read = read.saturating_add(n),
            Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: source.to_string(),
                });
            }
        }
    }
    Ok(())
}

pub async fn write_object_frame_async<W, T>(
    writer: &mut W,
    value: &T,
    options: ObjectFrameIoOptions,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
    T: serde::Serialize + ?Sized,
{
    let bytes = SinomoObjectCodec::network_frame().encode(value)?;
    let len_prefix = object_frame_len_prefix(bytes.len())?;
    write_all_vectored_async_frame(
        writer,
        &len_prefix,
        &bytes,
        options.idle_timeout,
        "write async object frame",
    )
    .await?;
    if let Some(observer) = io_observer() {
        observer.on_net_sent(OBJECT_FRAME_WIRE_PREFIX_BYTES as u64 + bytes.len() as u64);
    }
    Ok(())
}

/// Writes one object frame and returns separate SOBJ encode/allocation and
/// physical write/flush timestamps. Physical carriers call this only when a
/// sampled frame already has timeline correlation; ordinary writes retain the
/// allocation-free [`write_object_frame_async`] API and path.
pub async fn write_object_frame_async_timed<W, T>(
    writer: &mut W,
    value: &T,
    options: ObjectFrameIoOptions,
) -> (Result<(), NetworkError>, Option<ObjectFrameWriteTiming>)
where
    W: AsyncWrite + Unpin,
    T: serde::Serialize + ?Sized,
{
    let encode_started_monotonic_ns = crate::network_timeline_monotonic_ns();
    let bytes = match SinomoObjectCodec::network_frame().encode(value) {
        Ok(bytes) => bytes,
        Err(error) => return (Err(error), None),
    };
    let len_prefix = match object_frame_len_prefix(bytes.len()) {
        Ok(prefix) => prefix,
        Err(error) => return (Err(error), None),
    };
    let encode_completed_monotonic_ns = crate::network_timeline_monotonic_ns();
    let result = write_all_vectored_async_frame(
        writer,
        &len_prefix,
        &bytes,
        options.idle_timeout,
        "write async object frame",
    )
    .await;
    let socket_write_completed_monotonic_ns = crate::network_timeline_monotonic_ns();
    if result.is_ok()
        && let Some(observer) = io_observer()
    {
        observer.on_net_sent(OBJECT_FRAME_WIRE_PREFIX_BYTES as u64 + bytes.len() as u64);
    }
    let wire_bytes = OBJECT_FRAME_WIRE_PREFIX_BYTES.saturating_add(bytes.len());
    (
        result,
        Some(ObjectFrameWriteTiming {
            encode_started_monotonic_ns,
            encode_completed_monotonic_ns,
            socket_write_completed_monotonic_ns,
            wire_bytes: u32::try_from(wire_bytes).unwrap_or(u32::MAX),
        }),
    )
}

pub async fn write_borrowed_object_frame_async<W>(
    writer: &mut W,
    document: &BorrowedDocumentBuilder<'_>,
    options: ObjectFrameIoOptions,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    let parts = document.to_parts().map_err(NetworkError::object)?;
    let body_len = parts.document_len().map_err(NetworkError::object)?;
    let len_prefix = object_frame_len_prefix(body_len)?;
    write_all_async_frame(
        writer,
        &len_prefix,
        options.idle_timeout,
        "write async borrowed object frame",
        "length",
    )
    .await?;
    write_all_async_frame(
        writer,
        &parts.prefix,
        options.idle_timeout,
        "write async borrowed object frame",
        "body prefix",
    )
    .await?;
    write_all_async_frame(
        writer,
        parts.payload,
        options.idle_timeout,
        "write async borrowed object frame",
        "body payload",
    )
    .await?;
    if let Some(observer) = io_observer() {
        observer.on_net_sent(OBJECT_FRAME_WIRE_PREFIX_BYTES as u64 + body_len as u64);
    }
    Ok(())
}

pub async fn read_object_frame_async<R, T>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
) -> Result<T, NetworkError>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    read_object_frame_async_with_budget(reader, options, object_frame_byte_budget()).await
}

/// Reads one object frame while rejecting an encoded body above `max_bytes`
/// immediately after its length prefix, before reserving or allocating the
/// declared body. Canonical object-frame readers deliberately remain
/// unrestricted; persistent bounded carriers opt into this limit.
pub async fn read_object_frame_async_with_max_bytes<R, T>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
    max_bytes: usize,
) -> Result<T, NetworkError>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    let len = read_object_frame_len_async(reader, options.idle_timeout).await?;
    if len > max_bytes {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: len,
            max_bytes,
        });
    }
    read_object_frame_body_async(reader, options, object_frame_byte_budget(), len).await
}

/// Reads one bounded object frame while allowing a persistent carrier to stay
/// idle indefinitely between frames.
///
/// The first byte of the next frame is awaited without a timeout. Once that
/// byte arrives, `options.idle_timeout` applies to every remaining prefix and
/// body read, resetting after each successful read. This lets long-lived
/// carriers remain idle without allowing a peer to strand one with a partial
/// frame.
pub async fn read_object_frame_async_with_max_bytes_after_first_byte<R, T>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
    max_bytes: usize,
) -> Result<T, NetworkError>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    read_object_frame_async_with_max_bytes_after_first_byte_inner(
        reader,
        options,
        max_bytes,
        || false,
        None,
    )
    .await
    .map(|(value, _timing)| value)
}

/// Persistent-carrier variant that returns first-read and decode-completion
/// timestamps only when the bounded network timeline is active at the first
/// successful socket read. The decoded value and wire behavior are identical
/// to [`read_object_frame_async_with_max_bytes_after_first_byte`].
pub async fn read_object_frame_async_with_max_bytes_after_first_byte_timed<R, T>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
    max_bytes: usize,
    readiness_probe: &ObjectFrameReadinessProbe,
) -> Result<(T, Option<ObjectFrameReadTiming>), NetworkError>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    read_object_frame_async_with_max_bytes_after_first_byte_inner(
        reader,
        options,
        max_bytes,
        crate::network_timeline_capture_is_active,
        Some(readiness_probe),
    )
    .await
}

async fn read_object_frame_async_with_max_bytes_after_first_byte_inner<R, T, F>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
    max_bytes: usize,
    timing_enabled: F,
    readiness_probe: Option<&ObjectFrameReadinessProbe>,
) -> Result<(T, Option<ObjectFrameReadTiming>), NetworkError>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
    F: FnOnce() -> bool,
{
    // Read up to the complete prefix in one ordinary socket read. Besides
    // avoiding a one-byte syscall on the hot path, this is the exact point at
    // which an idle carrier becomes an in-progress frame subject to the
    // no-progress policy below.
    let mut prefix_start = [0_u8; OBJECT_FRAME_WIRE_PREFIX_BYTES];
    let first_read = loop {
        let result = match readiness_probe {
            Some(readiness_probe) => {
                ObjectFrameFirstReadFuture::new(reader, &mut prefix_start, readiness_probe).await
            }
            None => reader
                .read(&mut prefix_start)
                .await
                .map(|bytes| ObjectFrameFirstRead {
                    bytes,
                    ..ObjectFrameFirstRead::default()
                }),
        };
        match result {
            Ok(ObjectFrameFirstRead { bytes: 0, .. }) => {
                return Err(NetworkError::Io {
                    operation: "read async object frame prefix".to_string(),
                    detail: object_frame_eof_detail("prefix", 0, OBJECT_FRAME_WIRE_PREFIX_BYTES),
                });
            }
            Ok(read) => break read,
            Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(NetworkError::Io {
                    operation: "read async object frame prefix".to_string(),
                    detail: source.to_string(),
                });
            }
        }
    };
    let prefix_start_len = first_read.bytes;
    let timing_enabled = timing_enabled();
    let first_read_monotonic_ns = timing_enabled.then(|| {
        (first_read.first_read_monotonic_ns != 0)
            .then_some(first_read.first_read_monotonic_ns)
            .unwrap_or_else(crate::network_timeline_monotonic_ns)
    });
    let prefix_start = std::io::Cursor::new(&prefix_start[..prefix_start_len]);
    let mut reader = tokio::io::AsyncReadExt::chain(prefix_start, reader);
    let len = read_object_frame_len_async(&mut reader, options.idle_timeout).await?;
    if len > max_bytes {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: len,
            max_bytes,
        });
    }
    let (
        value,
        body_read_completed_monotonic_ns,
        decode_started_monotonic_ns,
        decoded_monotonic_ns,
    ) = if first_read_monotonic_ns.is_some() {
        read_object_frame_body_async_timed(&mut reader, options, object_frame_byte_budget(), len)
            .await?
    } else {
        (
            read_object_frame_body_async(&mut reader, options, object_frame_byte_budget(), len)
                .await?,
            0,
            0,
            0,
        )
    };
    let timing = first_read_monotonic_ns.map(|first_read_monotonic_ns| ObjectFrameReadTiming {
        capture_generation: first_read.capture_generation,
        ready_monotonic_ns: first_read.ready_monotonic_ns,
        read_poll_monotonic_ns: first_read.read_poll_monotonic_ns,
        first_read_monotonic_ns,
        body_read_completed_monotonic_ns,
        decode_started_monotonic_ns,
        decoded_monotonic_ns,
        wire_bytes: u32::try_from(OBJECT_FRAME_WIRE_PREFIX_BYTES.saturating_add(len))
            .unwrap_or(u32::MAX),
        first_read_bytes: u32::try_from(prefix_start_len).unwrap_or(u32::MAX),
    });
    Ok((value, timing))
}

async fn read_object_frame_async_with_budget<R, T>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
    budget: &ObjectFrameByteBudget,
) -> Result<T, NetworkError>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    let len = read_object_frame_len_async(reader, options.idle_timeout).await?;
    read_object_frame_body_async(reader, options, budget, len).await
}

async fn read_object_frame_body_async<R, T>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
    budget: &ObjectFrameByteBudget,
    len: usize,
) -> Result<T, NetworkError>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    let _permit = budget.try_acquire(len)?;
    let mut bytes = try_zeroed_vec(len, "read async object frame body")?;
    read_exact_async_frame(
        reader,
        &mut bytes,
        options.idle_timeout,
        "read async object frame body",
        "body",
    )
    .await?;
    if let Some(observer) = io_observer() {
        observer.on_net_recv(OBJECT_FRAME_WIRE_PREFIX_BYTES as u64 + bytes.len() as u64);
    }
    SinomoObjectCodec::network_frame().decode(&bytes)
}

async fn read_object_frame_body_async_timed<R, T>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
    budget: &ObjectFrameByteBudget,
    len: usize,
) -> Result<(T, u64, u64, u64), NetworkError>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    let _permit = budget.try_acquire(len)?;
    let mut bytes = try_zeroed_vec(len, "read async object frame body")?;
    read_exact_async_frame(
        reader,
        &mut bytes,
        options.idle_timeout,
        "read async object frame body",
        "body",
    )
    .await?;
    let body_read_completed_monotonic_ns = crate::network_timeline_monotonic_ns();
    if let Some(observer) = io_observer() {
        observer.on_net_recv(OBJECT_FRAME_WIRE_PREFIX_BYTES as u64 + bytes.len() as u64);
    }
    let decode_started_monotonic_ns = crate::network_timeline_monotonic_ns();
    let value = SinomoObjectCodec::network_frame().decode(&bytes)?;
    let decoded_monotonic_ns = crate::network_timeline_monotonic_ns();
    Ok((
        value,
        body_read_completed_monotonic_ns,
        decode_started_monotonic_ns,
        decoded_monotonic_ns,
    ))
}

async fn read_object_frame_len_async<R: AsyncRead + Unpin>(
    reader: &mut R,
    idle_timeout: Option<Duration>,
) -> Result<usize, NetworkError> {
    let mut prefix = [0_u8; OBJECT_FRAME_WIRE_PREFIX_BYTES];
    read_exact_async_frame(
        reader,
        &mut prefix,
        idle_timeout,
        "read async object frame prefix",
        "prefix",
    )
    .await?;
    validate_object_frame_magic(&prefix[..4])?;
    let len_bytes: [u8; 8] = prefix[4..].try_into().expect("object frame length");
    object_frame_len_from_u64(u64::from_be_bytes(len_bytes))
}

pub(crate) async fn write_all_async_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    buf: &[u8],
    idle_timeout: Option<Duration>,
    operation: &str,
    part: &str,
) -> Result<(), NetworkError> {
    write_all_async_frame_unflushed(writer, buf, idle_timeout, operation, part).await?;
    flush_async_frame(writer, idle_timeout, operation).await?;
    Ok(())
}

async fn write_all_vectored_async_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    prefix: &[u8],
    body: &[u8],
    idle_timeout: Option<Duration>,
    operation: &str,
) -> Result<(), NetworkError> {
    write_all_vectored_async_frame_unflushed(writer, prefix, body, idle_timeout, operation).await?;
    flush_async_frame(writer, idle_timeout, operation).await?;
    Ok(())
}

async fn write_all_vectored_async_frame_unflushed<W: AsyncWrite + Unpin>(
    writer: &mut W,
    prefix: &[u8],
    body: &[u8],
    idle_timeout: Option<Duration>,
    operation: &str,
) -> Result<(), NetworkError> {
    let expected = prefix
        .len()
        .checked_add(body.len())
        .ok_or(NetworkError::FrameTooLarge {
            actual_bytes: usize::MAX,
            max_bytes: usize::MAX,
        })?;
    let mut written = 0_usize;
    while written < expected {
        let part = if written < prefix.len() {
            "length"
        } else {
            "body"
        };
        let (part_written, part_expected) = if written < prefix.len() {
            (written, prefix.len())
        } else {
            (written.saturating_sub(prefix.len()), body.len())
        };
        let slices = if written < prefix.len() {
            [IoSlice::new(&prefix[written..]), IoSlice::new(body)]
        } else {
            [
                IoSlice::new(&body[written - prefix.len()..]),
                IoSlice::new(&[]),
            ]
        };
        let write_result = if let Some(timeout) = idle_timeout {
            match tokio::time::timeout(timeout, writer.write_vectored(&slices)).await {
                Ok(result) => result,
                Err(_) => {
                    return Err(NetworkError::Io {
                        operation: operation.to_string(),
                        detail: format!(
                            "timed out after {}ms without {part} write progress",
                            timeout.as_millis()
                        ),
                    });
                }
            }
        } else {
            writer.write_vectored(&slices).await
        };
        match write_result {
            Ok(0) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: format!(
                        "peer stopped accepting object frame {part}: wrote {part_written} of {part_expected} bytes"
                    ),
                });
            }
            Ok(bytes) if bytes <= expected - written => {
                written = written.saturating_add(bytes);
            }
            Ok(bytes) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: format!(
                        "object frame vectored writer reported {bytes} bytes for {} bytes remaining",
                        expected - written
                    ),
                });
            }
            Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: source.to_string(),
                });
            }
        }
    }
    Ok(())
}

async fn write_all_async_frame_unflushed<W: AsyncWrite + Unpin>(
    writer: &mut W,
    buf: &[u8],
    idle_timeout: Option<Duration>,
    operation: &str,
    part: &str,
) -> Result<(), NetworkError> {
    let expected = buf.len();
    let mut written = 0_usize;
    while written < expected {
        let write_result = if let Some(timeout) = idle_timeout {
            match tokio::time::timeout(timeout, writer.write(&buf[written..])).await {
                Ok(result) => result,
                Err(_) => {
                    return Err(NetworkError::Io {
                        operation: operation.to_string(),
                        detail: format!(
                            "timed out after {}ms without {part} write progress",
                            timeout.as_millis()
                        ),
                    });
                }
            }
        } else {
            writer.write(&buf[written..]).await
        };
        match write_result {
            Ok(0) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: format!(
                        "peer stopped accepting object frame {part}: wrote {written} of {expected} bytes"
                    ),
                });
            }
            Ok(n) => written = written.saturating_add(n),
            Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: source.to_string(),
                });
            }
        }
    }
    Ok(())
}

async fn flush_async_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    idle_timeout: Option<Duration>,
    operation: &str,
) -> Result<(), NetworkError> {
    let flush_result = if let Some(timeout) = idle_timeout {
        match tokio::time::timeout(timeout, writer.flush()).await {
            Ok(result) => result,
            Err(_) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: format!(
                        "timed out after {}ms without flush progress",
                        timeout.as_millis()
                    ),
                });
            }
        }
    } else {
        writer.flush().await
    };
    flush_result.map_err(|source| NetworkError::Io {
        operation: operation.to_string(),
        detail: source.to_string(),
    })?;
    Ok(())
}

async fn read_exact_async_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
    idle_timeout: Option<Duration>,
    operation: &'static str,
    part: &'static str,
) -> Result<(), NetworkError> {
    let expected = buf.len();
    let mut read = 0_usize;
    while read < expected {
        let read_result = if let Some(timeout) = idle_timeout {
            match tokio::time::timeout(timeout, reader.read(&mut buf[read..])).await {
                Ok(result) => result,
                Err(_) => {
                    return Err(NetworkError::Io {
                        operation: operation.to_string(),
                        detail: format!(
                            "timed out after {}ms without {part} read progress",
                            timeout.as_millis()
                        ),
                    });
                }
            }
        } else {
            reader.read(&mut buf[read..]).await
        };
        match read_result {
            Ok(0) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: object_frame_eof_detail(part, read, expected),
                });
            }
            Ok(n) => read = read.saturating_add(n),
            Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: source.to_string(),
                });
            }
        }
    }
    Ok(())
}

fn object_frame_eof_detail(part: &str, read: usize, expected: usize) -> String {
    if read == 0 {
        format!("peer closed before object frame {part} ({expected} bytes expected)")
    } else {
        format!("peer closed during object frame {part}: read {read} of {expected} bytes")
    }
}

#[cfg(test)]
mod serde_bytes_tests {
    use super::{
        AsyncDaemonControlListener, AsyncDaemonControlStream, DaemonRpcTimeouts,
        ObjectFrameIoOptions, ObjectFrameReadinessProbe, SINOMO_NETWORK_OBJECT_VERSION,
        SinomoObjectCodec, decode_single_object_frame_from_slice,
        encode_borrowed_object_frame_to_vec, encode_object_frame_to_vec, read_object_frame_async,
        read_object_frame_blocking, write_all_async_frame, write_borrowed_object_frame_async,
        write_object_frame_async, write_object_frame_async_timed, write_object_frame_blocking,
    };
    use crate::error::{NetworkDeliveryState, NetworkError};
    use serde::{Deserialize, Serialize};
    use sinomo_object::{BorrowedDocumentBuilder, DocumentView, LengthMode};
    use std::io::Cursor;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::Duration;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct BundleRecord {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none", with = "serde_bytes")]
        payload: Option<Vec<u8>>,
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct BorrowedBundleRecord<'a> {
        name: &'a str,
        #[serde(with = "serde_bytes")]
        payload: &'a [u8],
    }

    #[test]
    fn serde_bytes_option_round_trips_through_sobj_as_raw_blob() {
        // Representative binary payload: full byte range, not valid UTF-8.
        let payload: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let record = BundleRecord {
            name: "bundle".to_string(),
            payload: Some(payload.clone()),
        };

        let encoded = SinomoObjectCodec::default()
            .encode(&record)
            .expect("encode");
        let decoded: BundleRecord = SinomoObjectCodec::default()
            .decode(&encoded)
            .expect("decode");
        assert_eq!(decoded, record);

        // Stored as a native SOBJ bytes blob, not base64: the encoding stays
        // close to the raw payload (base64 of 4096 would be ~5462 bytes)...
        assert!(
            encoded.len() < payload.len() + 256,
            "expected ~raw payload size (no base64 inflation), got {} for {} payload bytes",
            encoded.len(),
            payload.len()
        );
        // ...and the raw bytes appear verbatim in the stream (impossible if base64'd).
        assert!(
            encoded
                .windows(payload.len())
                .any(|w| w == payload.as_slice()),
            "raw payload bytes must be embedded verbatim"
        );

        // `None` is skipped on the wire and round-trips.
        let none = BundleRecord {
            name: "x".to_string(),
            payload: None,
        };
        let encoded_none = SinomoObjectCodec::default()
            .encode(&none)
            .expect("encode none");
        let decoded_none: BundleRecord = SinomoObjectCodec::default()
            .decode(&encoded_none)
            .expect("decode none");
        assert_eq!(decoded_none, none);
    }

    #[test]
    fn object_codec_can_decode_borrowed_strings_and_bytes() {
        #[derive(Serialize)]
        struct OwnedBundleRecord<'a> {
            name: &'a str,
            #[serde(with = "serde_bytes")]
            payload: &'a [u8],
        }

        let payload: Vec<u8> = (0..=255u8).cycle().take(32 * 1024).collect();
        let encoded = SinomoObjectCodec::default()
            .encode(&OwnedBundleRecord {
                name: "borrowed-bundle",
                payload: &payload,
            })
            .expect("encode");

        let decoded: BorrowedBundleRecord<'_> = SinomoObjectCodec::default()
            .decode_borrowed(&encoded)
            .expect("borrowed decode");

        assert_eq!(decoded.name, "borrowed-bundle");
        assert_eq!(decoded.payload, payload.as_slice());
        assert!(slice_is_borrowed_from(decoded.name.as_bytes(), &encoded));
        assert!(slice_is_borrowed_from(decoded.payload, &encoded));
    }

    #[test]
    fn object_codec_rejects_a_document_from_another_version() {
        let encoded = SinomoObjectCodec {
            version_id: SINOMO_NETWORK_OBJECT_VERSION + 1,
            length_mode: LengthMode::U64,
        }
        .encode(&"future-version")
        .expect("encode future version");

        let error = SinomoObjectCodec::network_frame()
            .decode::<String>(&encoded)
            .expect_err("network codec must enforce its configured version");
        assert!(
            matches!(error, NetworkError::Serialization { ref detail }
                if detail.contains("unexpected version id")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn blocking_object_frame_reports_closed_before_prefix() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let error = read_object_frame_blocking::<_, BundleRecord>(&mut cursor).expect_err("eof");

        assert_eq!(
            error,
            NetworkError::Io {
                operation: "read object frame prefix".to_string(),
                detail: "peer closed before object frame prefix (12 bytes expected)".to_string(),
            }
        );
    }

    #[test]
    fn blocking_object_frame_reports_partial_body() {
        let mut bytes = super::object_frame_len_prefix(8)
            .expect("frame prefix")
            .to_vec();
        bytes.extend_from_slice(b"abc");
        let mut cursor = Cursor::new(bytes);
        let error = read_object_frame_blocking::<_, BundleRecord>(&mut cursor).expect_err("eof");

        assert_eq!(
            error,
            NetworkError::Io {
                operation: "read object frame body".to_string(),
                detail: "peer closed during object frame body: read 3 of 8 bytes".to_string(),
            }
        );
    }

    #[test]
    fn object_frame_writer_uses_fixed_sof2_u64_prefix() {
        let record = BundleRecord {
            name: "arbitrary-size-ready".to_string(),
            payload: Some(vec![1, 2, 3, 4]),
        };
        let mut frame = Vec::new();
        write_object_frame_blocking(&mut frame, &record).expect("write frame");

        assert_eq!(&frame[..4], b"SOF2");
        let body_len = u64::from_be_bytes(frame[4..12].try_into().expect("length")) as usize;
        let body = &frame[12..];
        assert_eq!(body.len(), body_len);
        let view = DocumentView::parse(body).expect("parse sobj");
        assert_eq!(view.metadata.length_mode, LengthMode::U64);

        let mut cursor = Cursor::new(frame);
        let decoded: BundleRecord = read_object_frame_blocking(&mut cursor).expect("read frame");
        assert_eq!(decoded, record);
    }

    #[test]
    fn memory_object_frame_round_trips_single_frame_without_io_observer() {
        let record = BundleRecord {
            name: "memory-frame".to_string(),
            payload: Some(vec![1, 3, 5, 7]),
        };

        let frame = encode_object_frame_to_vec(&record).expect("encode frame");
        let decoded: BundleRecord =
            decode_single_object_frame_from_slice(&frame).expect("decode frame");

        assert_eq!(decoded, record);
    }

    #[test]
    fn object_frame_megabyte_serde_bytes_payload_baseline_is_compact() {
        let payload: Vec<u8> = (0..=255u8).cycle().take(1024 * 1024).collect();
        let record = BundleRecord {
            name: "megabyte-frame".to_string(),
            payload: Some(payload.clone()),
        };

        let frame = encode_object_frame_to_vec(&record).expect("encode frame");
        assert_eq!(&frame[..4], b"SOF2");
        let body_len = u64::from_be_bytes(frame[4..12].try_into().expect("length")) as usize;
        let body = &frame[12..];

        assert_eq!(body.len(), body_len);
        assert!(
            frame.len() < payload.len() + 512,
            "expected compact SOBJ bytes payload, got {} frame bytes for {} payload bytes",
            frame.len(),
            payload.len()
        );
        assert!(
            body.windows(payload.len()).any(|w| w == payload.as_slice()),
            "payload should be embedded as raw bytes in the current contiguous baseline"
        );

        let view = DocumentView::parse(body).expect("parse body");
        assert_eq!(view.metadata.length_mode, LengthMode::U64);

        let decoded: BundleRecord =
            decode_single_object_frame_from_slice(&frame).expect("decode frame");
        assert_eq!(decoded, record);
    }

    #[test]
    fn object_frame_vec_encoder_matches_manual_prefix_and_body() {
        let record = BundleRecord {
            name: "manual-frame".to_string(),
            payload: Some(vec![1, 2, 3, 4, 5]),
        };

        let body = SinomoObjectCodec::network_frame()
            .encode(&record)
            .expect("body");
        let mut expected = Vec::new();
        expected.extend_from_slice(b"SOF2");
        expected.extend_from_slice(&(body.len() as u64).to_be_bytes());
        expected.extend_from_slice(&body);

        let frame = encode_object_frame_to_vec(&record).expect("frame");
        assert_eq!(frame, expected);
    }

    #[test]
    fn borrowed_object_frame_vec_fallback_preserves_sobj_layout() {
        let payload: Vec<u8> = (0..=255u8).cycle().take(1024 * 1024).collect();
        let document = BorrowedDocumentBuilder::dynamic_bytes(
            SINOMO_NETWORK_OBJECT_VERSION,
            LengthMode::U64,
            &payload,
        );
        let parts = document.to_parts().expect("parts");
        assert_eq!(parts.payload.as_ptr(), payload.as_ptr());

        let frame = encode_borrowed_object_frame_to_vec(&document).expect("encode frame");
        assert_eq!(&frame[..4], b"SOF2");
        let body_len = u64::from_be_bytes(frame[4..12].try_into().expect("length")) as usize;
        let body = &frame[12..];

        assert_eq!(body.len(), body_len);
        assert!(
            frame.len() < payload.len() + 128,
            "expected compact borrowed SOBJ bytes frame, got {} frame bytes for {} payload bytes",
            frame.len(),
            payload.len()
        );
        let view = DocumentView::parse(body).expect("parse body");
        assert_eq!(view.metadata.length_mode, LengthMode::U64);
        let decoded: serde_bytes::ByteBuf = SinomoObjectCodec::network_frame()
            .decode(body)
            .expect("decode body");
        assert_eq!(decoded.as_ref(), payload.as_slice());
    }

    #[tokio::test]
    async fn async_borrowed_object_frame_writer_round_trips() {
        let payload: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let document = BorrowedDocumentBuilder::dynamic_bytes(
            SINOMO_NETWORK_OBJECT_VERSION,
            LengthMode::U64,
            &payload,
        );
        let parts = document.to_parts().expect("parts");
        assert_eq!(parts.payload.as_ptr(), payload.as_ptr());

        let (mut writer, mut reader) = tokio::io::duplex(256);
        let write = write_borrowed_object_frame_async(
            &mut writer,
            &document,
            ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
        );
        let read = read_object_frame_async::<_, serde_bytes::ByteBuf>(
            &mut reader,
            ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
        );
        let (write_result, read_result) = tokio::join!(write, read);

        write_result.expect("write borrowed frame");
        assert_eq!(
            read_result.expect("read borrowed frame").as_ref(),
            payload.as_slice()
        );
    }

    #[tokio::test]
    async fn timed_object_frame_io_splits_encode_first_read_and_decode() {
        let record = BundleRecord {
            name: "timed-frame".to_string(),
            payload: Some(vec![3, 1, 4, 1, 5, 9]),
        };
        let (mut writer, mut reader) = tokio::io::duplex(256);
        let write = write_object_frame_async_timed(
            &mut writer,
            &record,
            ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
        );
        let read = super::read_object_frame_async_with_max_bytes_after_first_byte_inner::<
            _,
            BundleRecord,
            _,
        >(
            &mut reader,
            ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
            super::MAX_OBJECT_RPC_CHANNEL_WIRE_FRAME_BYTES,
            || true,
            None,
        );
        let ((write_result, write_timing), read_result) = tokio::join!(write, read);

        write_result.expect("timed frame write");
        let write_timing = write_timing.expect("write timing");
        let (decoded, read_timing) = read_result.expect("timed frame read");
        let read_timing = read_timing.expect("read timing");
        assert_eq!(decoded, record);
        assert_eq!(read_timing.wire_bytes, write_timing.wire_bytes);
        assert!(read_timing.first_read_bytes > 0);
        assert!(read_timing.first_read_bytes as usize <= super::OBJECT_FRAME_WIRE_PREFIX_BYTES);
        assert!(
            write_timing.encode_completed_monotonic_ns >= write_timing.encode_started_monotonic_ns
        );
        assert!(
            write_timing.socket_write_completed_monotonic_ns
                >= write_timing.encode_completed_monotonic_ns
        );
        assert!(
            read_timing.body_read_completed_monotonic_ns >= read_timing.first_read_monotonic_ns
        );
        assert!(
            read_timing.decode_started_monotonic_ns >= read_timing.body_read_completed_monotonic_ns
        );
        assert!(read_timing.decoded_monotonic_ns >= read_timing.decode_started_monotonic_ns);
    }

    #[test]
    fn readiness_probe_duplicate_wakes_retain_the_parent_waker() {
        #[derive(Default)]
        struct CountingWake(AtomicUsize);

        impl Wake for CountingWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let probe = ObjectFrameReadinessProbe::new();
        let count = Arc::new(CountingWake::default());
        let parent = Waker::from(Arc::clone(&count));
        assert_eq!(probe.prepare_poll(1, &parent), None);

        Wake::wake_by_ref(&probe.shared);
        Wake::wake_by_ref(&probe.shared);

        assert_eq!(count.0.load(Ordering::Relaxed), 2);
        probe.disarm();
    }

    #[tokio::test]
    async fn readiness_probe_records_async_wake_to_first_read() {
        struct AnnouncedReader {
            inner: tokio::io::DuplexStream,
            first_poll: Option<tokio::sync::oneshot::Sender<()>>,
        }

        impl AsyncRead for AnnouncedReader {
            fn poll_read(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buffer: &mut ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                let this = self.get_mut();
                if let Some(first_poll) = this.first_poll.take() {
                    let _ = first_poll.send(());
                }
                Pin::new(&mut this.inner).poll_read(cx, buffer)
            }
        }

        let _capture_guard = crate::timeline::network_timeline_test_capture_guard();
        if crate::network_timeline_capture_is_active() {
            let _ = crate::stop_network_timeline_capture();
        }
        crate::start_network_timeline_capture(32).expect("start readiness capture");

        let record = BundleRecord {
            name: "readiness-frame".to_string(),
            payload: Some(vec![2, 7, 1, 8, 2, 8]),
        };
        let (writer, reader) = tokio::io::duplex(256);
        let (first_poll_tx, first_poll_rx) = tokio::sync::oneshot::channel();
        let mut reader = AnnouncedReader {
            inner: reader,
            first_poll: Some(first_poll_tx),
        };
        let writer_record = BundleRecord {
            name: record.name.clone(),
            payload: record.payload.clone(),
        };
        let writer = tokio::spawn(async move {
            first_poll_rx.await.expect("reader first poll");
            let mut writer = writer;
            write_object_frame_async(
                &mut writer,
                &writer_record,
                ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
            )
            .await
        });
        let readiness_probe = ObjectFrameReadinessProbe::new();
        let read_result = super::read_object_frame_async_with_max_bytes_after_first_byte_timed::<
            _,
            BundleRecord,
        >(
            &mut reader,
            ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
            super::MAX_OBJECT_RPC_CHANNEL_WIRE_FRAME_BYTES,
            &readiness_probe,
        )
        .await;
        let write_result = writer.await.expect("writer task");
        let _snapshot = crate::stop_network_timeline_capture().expect("stop readiness capture");

        write_result.expect("write readiness frame");
        let (decoded, timing) = read_result.expect("read readiness frame");
        let timing = timing.expect("readiness timing");
        assert_eq!(decoded, record);
        assert_ne!(timing.capture_generation, 0);
        assert_ne!(timing.ready_monotonic_ns, 0);
        assert!(timing.read_poll_monotonic_ns >= timing.ready_monotonic_ns);
        assert!(timing.first_read_monotonic_ns >= timing.read_poll_monotonic_ns);
    }

    #[tokio::test]
    async fn async_object_frame_writer_handles_partial_vectored_writes_and_flushes_once() {
        struct PartialVectoredWriter {
            bytes: Vec<u8>,
            max_write_bytes: usize,
            scalar_writes: usize,
            vectored_writes: usize,
            flushes: usize,
            bytes_at_flush: Option<usize>,
        }

        impl tokio::io::AsyncWrite for PartialVectoredWriter {
            fn poll_write(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                buf: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                self.scalar_writes += 1;
                let accepted = self.max_write_bytes.min(buf.len());
                self.bytes.extend_from_slice(&buf[..accepted]);
                std::task::Poll::Ready(Ok(accepted))
            }

            fn poll_write_vectored(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                bufs: &[std::io::IoSlice<'_>],
            ) -> std::task::Poll<std::io::Result<usize>> {
                self.vectored_writes += 1;
                let mut accepted = 0_usize;
                for buf in bufs {
                    let remaining = self.max_write_bytes.saturating_sub(accepted);
                    if remaining == 0 {
                        break;
                    }
                    let chunk_bytes = remaining.min(buf.len());
                    self.bytes.extend_from_slice(&buf[..chunk_bytes]);
                    accepted += chunk_bytes;
                }
                std::task::Poll::Ready(Ok(accepted))
            }

            fn poll_flush(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                self.flushes += 1;
                self.bytes_at_flush = Some(self.bytes.len());
                std::task::Poll::Ready(Ok(()))
            }

            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
        }

        let record = BundleRecord {
            name: "partial-vectored-frame".to_string(),
            payload: Some((0..=255_u8).cycle().take(1024).collect()),
        };
        let expected = encode_object_frame_to_vec(&record).expect("expected frame");
        let mut writer = PartialVectoredWriter {
            bytes: Vec::new(),
            max_write_bytes: 7,
            scalar_writes: 0,
            vectored_writes: 0,
            flushes: 0,
            bytes_at_flush: None,
        };

        write_object_frame_async(
            &mut writer,
            &record,
            ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
        )
        .await
        .expect("write partial vectored frame");

        assert_eq!(writer.bytes, expected);
        assert_eq!(writer.scalar_writes, 0);
        assert!(writer.vectored_writes > 1);
        assert_eq!(writer.flushes, 1);
        assert_eq!(writer.bytes_at_flush, Some(writer.bytes.len()));
    }

    #[test]
    fn memory_object_frame_rejects_trailing_bytes() {
        let record = BundleRecord {
            name: "memory-trailing".to_string(),
            payload: None,
        };
        let mut frame = encode_object_frame_to_vec(&record).expect("encode frame");
        frame.extend_from_slice(b"junk");

        let error = decode_single_object_frame_from_slice::<BundleRecord>(&frame)
            .expect_err("trailing bytes");

        assert_eq!(
            error,
            NetworkError::Io {
                operation: "read object frame".to_string(),
                detail: "object frame has 4 trailing bytes".to_string(),
            }
        );
    }

    #[test]
    fn blocking_object_frame_reads_fixed_sof2_prefix() {
        let record = BundleRecord {
            name: "extended".to_string(),
            payload: Some(vec![5, 6, 7, 8]),
        };
        let body = SinomoObjectCodec::network_frame()
            .encode(&record)
            .expect("encode body");
        let mut frame = super::object_frame_len_prefix(body.len())
            .expect("frame prefix")
            .to_vec();
        frame.extend_from_slice(&body);

        let mut cursor = Cursor::new(frame);
        let decoded: BundleRecord =
            read_object_frame_blocking(&mut cursor).expect("read SOF2 frame");

        assert_eq!(decoded, record);
    }

    #[test]
    fn object_frame_rejects_legacy_u32_prefix() {
        let record = BundleRecord {
            name: "legacy-prefix".to_string(),
            payload: Some(vec![1, 2, 3, 4]),
        };
        let body = SinomoObjectCodec::network_frame()
            .encode(&record)
            .expect("encode body");
        let mut frame = (body.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&body);

        let error = decode_single_object_frame_from_slice::<BundleRecord>(&frame)
            .expect_err("legacy prefix must not be accepted");
        assert_eq!(
            error,
            NetworkError::Io {
                operation: "read object frame prefix".to_string(),
                detail: "invalid SOF2 object frame magic".to_string(),
            }
        );
    }

    #[test]
    fn network_timeout_defaults_are_shared_and_finite() {
        assert_eq!(
            DaemonRpcTimeouts::default(),
            DaemonRpcTimeouts::connect_and_idle(super::DEFAULT_NETWORK_TIMEOUT)
        );
        assert_eq!(
            ObjectFrameIoOptions::default().idle_timeout,
            Some(super::DEFAULT_NETWORK_TIMEOUT)
        );
        assert_eq!(DaemonRpcTimeouts::none().connect_timeout, None);
        assert_eq!(DaemonRpcTimeouts::none().idle_timeout, None);
    }

    #[test]
    fn object_frame_byte_budget_releases_in_flight_permits() {
        let budget = super::ObjectFrameByteBudget::fixed(10);
        let first = budget.try_acquire(6).expect("first frame budget");
        assert_eq!(budget.in_flight_bytes(), 6);

        assert_eq!(
            budget.try_acquire(5).expect_err("budget exceeded"),
            NetworkError::ObjectFrameMemoryUnavailable {
                frame_bytes: 5,
                in_flight_bytes: 6,
                available_memory_bytes: Some(4),
            }
        );

        drop(first);
        assert_eq!(budget.in_flight_bytes(), 0);
        let second = budget.try_acquire(10).expect("replacement frame budget");
        assert_eq!(budget.in_flight_bytes(), 10);
        drop(second);
        assert_eq!(budget.in_flight_bytes(), 0);
    }

    #[test]
    fn blocking_object_frame_budget_rejects_before_body_read() {
        let budget = super::ObjectFrameByteBudget::fixed(4);
        let mut frame = super::object_frame_len_prefix(8)
            .expect("frame prefix")
            .to_vec();
        frame.extend_from_slice(b"abcdefgh");
        let mut cursor = Cursor::new(frame);

        let error =
            super::read_object_frame_blocking_with_budget::<_, BundleRecord>(&mut cursor, &budget)
                .expect_err("frame exceeds available in-flight budget");

        assert_eq!(
            error,
            NetworkError::ObjectFrameMemoryUnavailable {
                frame_bytes: 8,
                in_flight_bytes: 0,
                available_memory_bytes: Some(4),
            }
        );
        assert_eq!(
            cursor.position(),
            12,
            "budget rejection should happen before reading the frame body"
        );
        assert_eq!(budget.in_flight_bytes(), 0);
    }

    #[tokio::test]
    async fn async_object_frame_budget_rejects_before_body_read() {
        let budget = super::ObjectFrameByteBudget::fixed(4);
        let mut frame = super::object_frame_len_prefix(8)
            .expect("frame prefix")
            .to_vec();
        frame.extend_from_slice(b"abcdefgh");
        let mut cursor = Cursor::new(frame);

        let error = super::read_object_frame_async_with_budget::<_, BundleRecord>(
            &mut cursor,
            ObjectFrameIoOptions::none(),
            &budget,
        )
        .await
        .expect_err("frame exceeds available in-flight budget");

        assert_eq!(
            error,
            NetworkError::ObjectFrameMemoryUnavailable {
                frame_bytes: 8,
                in_flight_bytes: 0,
                available_memory_bytes: Some(4),
            }
        );
        assert_eq!(
            cursor.position(),
            12,
            "budget rejection should happen before reading the frame body"
        );
        assert_eq!(budget.in_flight_bytes(), 0);
    }

    #[tokio::test]
    async fn bounded_async_object_frame_rejects_length_before_body_read() {
        let mut frame = super::object_frame_len_prefix(8)
            .expect("frame prefix")
            .to_vec();
        frame.extend_from_slice(b"abcdefgh");
        let mut cursor = Cursor::new(frame);

        let error = super::read_object_frame_async_with_max_bytes::<_, BundleRecord>(
            &mut cursor,
            ObjectFrameIoOptions::none(),
            4,
        )
        .await
        .expect_err("frame exceeds carrier limit");

        assert_eq!(
            error,
            NetworkError::FrameTooLarge {
                actual_bytes: 8,
                max_bytes: 4,
            }
        );
        assert_eq!(
            cursor.position(),
            12,
            "carrier limit must reject immediately after the SOF2 length prefix"
        );
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn blocking_object_frame_reports_resource_exhaustion_before_huge_body_read() {
        let mut frame = b"SOF2".to_vec();
        frame.extend_from_slice(&u64::MAX.to_be_bytes());
        let mut cursor = Cursor::new(frame);

        let error = read_object_frame_blocking::<_, BundleRecord>(&mut cursor)
            .expect_err("huge frame body cannot be allocated");

        assert!(
            matches!(
                error,
                NetworkError::ResourceExhausted { .. }
                    | NetworkError::ObjectFrameMemoryUnavailable { .. }
            ),
            "unexpected error for huge frame body: {error:?}"
        );
        assert_eq!(
            cursor.position(),
            12,
            "frame reader should fail allocation before reading the declared body"
        );
    }

    #[tokio::test]
    async fn async_object_frame_reads_fixed_sof2_prefix_with_idle_timeout() {
        let record = BundleRecord {
            name: "async-extended".to_string(),
            payload: Some(vec![9, 10, 11, 12]),
        };
        let body = SinomoObjectCodec::network_frame()
            .encode(&record)
            .expect("encode body");
        let mut frame = super::object_frame_len_prefix(body.len())
            .expect("frame prefix")
            .to_vec();
        frame.extend_from_slice(&body);
        let mut cursor = Cursor::new(frame);

        let decoded: BundleRecord = read_object_frame_async(
            &mut cursor,
            ObjectFrameIoOptions::idle_timeout(Duration::from_millis(20)),
        )
        .await
        .expect("read async SOF2 frame");

        assert_eq!(decoded, record);
    }

    #[tokio::test]
    async fn async_object_frame_reports_partial_body() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let prefix = super::object_frame_len_prefix(8).expect("frame prefix");
        server.write_all(&prefix).await.expect("write prefix");
        server.write_all(b"abc").await.expect("write partial body");
        drop(server);

        let error =
            read_object_frame_async::<_, BundleRecord>(&mut client, ObjectFrameIoOptions::none())
                .await
                .expect_err("eof");

        assert_eq!(
            error,
            NetworkError::Io {
                operation: "read async object frame body".to_string(),
                detail: "peer closed during object frame body: read 3 of 8 bytes".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn async_idle_write_timeout_tracks_write_progress_not_total_duration() {
        let payload = vec![7_u8; 32];
        let (mut writer, mut reader) = tokio::io::duplex(4);
        let expected = payload.clone();
        let reader_task = tokio::spawn(async move {
            let mut received = Vec::new();
            let mut byte = [0_u8; 1];
            while received.len() < expected.len() {
                let read = reader.read(&mut byte).await.expect("read byte");
                if read == 0 {
                    break;
                }
                received.push(byte[0]);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            received
        });

        write_all_async_frame(
            &mut writer,
            &payload,
            Some(Duration::from_millis(20)),
            "write async test frame",
            "body",
        )
        .await
        .expect("progress should keep idle timeout alive");
        drop(writer);

        assert_eq!(reader_task.await.expect("reader"), payload);
    }

    #[tokio::test]
    async fn async_idle_write_timeout_applies_to_final_flush() {
        struct StuckFlushWriter;

        impl tokio::io::AsyncWrite for StuckFlushWriter {
            fn poll_write(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                buf: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                std::task::Poll::Ready(Ok(buf.len()))
            }

            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Pending
            }

            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
        }

        let mut writer = StuckFlushWriter;
        let error = write_all_async_frame(
            &mut writer,
            b"payload",
            Some(Duration::from_millis(5)),
            "write async test frame",
            "body",
        )
        .await
        .expect_err("flush should be bounded by idle timeout");

        assert_eq!(
            error,
            NetworkError::Io {
                operation: "write async test frame".to_string(),
                detail: "timed out after 5ms without flush progress".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn raw_bootstrap_frame_round_trips_over_tcp_listener() {
        let listener = AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let server = tokio::spawn(async move {
            let mut stream = listener.accept().await.expect("accept").into_inner();
            let request: crate::ObjectRpcRequest = read_object_frame_async(
                &mut stream,
                ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
            )
            .await
            .expect("request");
            assert_eq!(request.route, crate::CLUSTER_TCP_TUNNEL_OPEN_ROUTE);
            assert_eq!(request.category, crate::CLUSTER_TCP_TUNNEL_RPC_CATEGORY);
            let open: crate::ClusterTcpTunnelOpenRequest =
                request.decode_body().expect("tunnel open request");
            assert_eq!(open, crate::ClusterTcpTunnelOpenRequest::default());
            let response = crate::ObjectRpcResponse::encode_body(
                crate::CLUSTER_TCP_TUNNEL_OPEN_ROUTE,
                &crate::ClusterTcpTunnelOpenResponse::accepted(),
            )
            .expect("response");
            write_object_frame_async(
                &mut stream,
                &response,
                ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
            )
            .await
            .expect("response");
        });

        let options = ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1));
        let mut client = AsyncDaemonControlStream::connect_with_options(
            &address,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
        )
        .await
        .expect("connect bootstrap stream")
        .into_inner();
        write_object_frame_async(
            &mut client,
            &crate::ObjectRpcRequest::encode_body(
                crate::CLUSTER_TCP_TUNNEL_OPEN_ROUTE,
                crate::CLUSTER_TCP_TUNNEL_RPC_CATEGORY,
                &crate::ClusterTcpTunnelOpenRequest::default(),
            )
            .expect("request"),
            options,
        )
        .await
        .expect("write bootstrap request");
        let response: crate::ObjectRpcResponse = read_object_frame_async(&mut client, options)
            .await
            .expect("bootstrap response");

        assert_eq!(
            response
                .decode_body::<crate::ClusterTcpTunnelOpenResponse>()
                .expect("body"),
            crate::ClusterTcpTunnelOpenResponse::accepted()
        );
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn daemon_connect_socket_error_is_definitely_not_sent() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("listener address");
        drop(listener);

        let error = AsyncDaemonControlStream::connect_with_options(
            &address.to_string(),
            DaemonRpcTimeouts::connect_only(Duration::from_secs(1)),
        )
        .await
        .expect_err("closed listener should reject connect");
        assert!(matches!(
            error,
            NetworkError::DeliveryFailed {
                delivery_state: NetworkDeliveryState::DefinitelyNotSent,
                ..
            }
        ));
    }

    fn slice_is_borrowed_from(slice: &[u8], source: &[u8]) -> bool {
        let source_start = source.as_ptr() as usize;
        let source_end = source_start + source.len();
        let slice_start = slice.as_ptr() as usize;
        let slice_end = slice_start + slice.len();
        slice_start >= source_start && slice_end <= source_end
    }
}
