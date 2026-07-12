use crate::error::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
use crate::io_observer;
use sinomo_object::{BorrowedDocumentBuilder, DecodeLimits, DecodeOptions, LengthMode, serde_ext};
use std::collections::HashMap;
#[cfg(any(test, feature = "test-support"))]
use std::io::{Read, Write};
use std::net::SocketAddr;
#[cfg(any(test, feature = "test-support"))]
use std::net::TcpStream;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const SINOMO_NETWORK_OBJECT_VERSION: u16 = 1;
pub const OBJECT_STREAM_DEFAULT_CHUNK_BYTES: usize = 1024 * 1024;
pub const OBJECT_STREAM_MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;
pub const OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL: u32 = u32::MAX;
pub const DAEMON_CONTROL_RPC_MAX_FRAME_BYTES: usize = usize::MAX;
/// Default limit for establishing a network path and for each period without
/// stream I/O progress. This is a per-phase/per-progress limit, not a total
/// operation deadline.
pub const DEFAULT_NETWORK_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const DAEMON_CONTROL_REUSE_STREAM_METADATA_KEY: &str =
    "sinomo.daemon-control.reuse-stream";
pub(crate) const DAEMON_CONTROL_REUSE_STREAM_METADATA_VALUE: &str = "1";
pub(crate) const DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_KEY: &str =
    "sinomo.daemon-control.raw-rpc-response";
pub(crate) const DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_VALUE: &str = "1";
pub(crate) const DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY: &str =
    "sinomo.daemon-control.streamed-rpc-response";
pub(crate) const DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE: &str = "raw-byte-chunks-v1";
const DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES: usize = 8;

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
    LazyLock::new(ObjectFrameByteBudget::available_memory);

#[derive(Clone, Debug)]
pub(crate) struct ObjectFrameByteBudget {
    inner: Arc<Mutex<ObjectFrameByteBudgetState>>,
    limit: ObjectFrameByteBudgetLimit,
}

#[derive(Clone, Copy, Debug)]
enum ObjectFrameByteBudgetLimit {
    AvailableMemory,
    #[cfg(any(test, feature = "test-support"))]
    Fixed(usize),
}

#[derive(Debug, Default)]
struct ObjectFrameByteBudgetState {
    in_flight_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct ObjectFrameByteBudgetPermit {
    budget: ObjectFrameByteBudget,
    bytes: usize,
}

impl ObjectFrameByteBudget {
    fn available_memory() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ObjectFrameByteBudgetState::default())),
            limit: ObjectFrameByteBudgetLimit::AvailableMemory,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn fixed(max_bytes: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ObjectFrameByteBudgetState::default())),
            limit: ObjectFrameByteBudgetLimit::Fixed(max_bytes),
        }
    }

    pub(crate) fn try_acquire(
        &self,
        frame_bytes: usize,
    ) -> Result<ObjectFrameByteBudgetPermit, NetworkError> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let available_memory_bytes = match self.limit {
            ObjectFrameByteBudgetLimit::AvailableMemory => current_available_memory_bytes(),
            #[cfg(any(test, feature = "test-support"))]
            ObjectFrameByteBudgetLimit::Fixed(max_bytes) => {
                Some(max_bytes.saturating_sub(state.in_flight_bytes))
            }
        };
        let would_use = state.in_flight_bytes.checked_add(frame_bytes).ok_or(
            NetworkError::ObjectFrameMemoryUnavailable {
                frame_bytes,
                in_flight_bytes: state.in_flight_bytes,
                available_memory_bytes,
            },
        )?;
        if let Some(available) = available_memory_bytes {
            let max_in_flight = state
                .in_flight_bytes
                .checked_add(available)
                .unwrap_or(usize::MAX);
            if frame_bytes > available || would_use > max_in_flight {
                return Err(NetworkError::ObjectFrameMemoryUnavailable {
                    frame_bytes,
                    in_flight_bytes: state.in_flight_bytes,
                    available_memory_bytes: Some(available),
                });
            }
        }
        state.in_flight_bytes = would_use;
        Ok(ObjectFrameByteBudgetPermit {
            budget: self.clone(),
            bytes: frame_bytes,
        })
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn in_flight_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .in_flight_bytes
    }
}

impl Drop for ObjectFrameByteBudgetPermit {
    fn drop(&mut self) {
        let mut state = self
            .budget
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.in_flight_bytes = state.in_flight_bytes.saturating_sub(self.bytes);
    }
}

fn object_frame_byte_budget() -> &'static ObjectFrameByteBudget {
    &OBJECT_FRAME_BYTE_BUDGET
}

#[cfg(target_os = "linux")]
fn current_available_memory_bytes() -> Option<usize> {
    use std::io::Read as _;

    let mut file = std::fs::File::open("/proc/meminfo").ok()?;
    let mut buf = [0_u8; 4096];
    let len = file.read(&mut buf).ok()?;
    let text = std::str::from_utf8(&buf[..len]).ok()?;
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("MemAvailable:") else {
            continue;
        };
        let kb = rest.split_whitespace().next()?.parse::<usize>().ok()?;
        return kb.checked_mul(1024);
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn current_available_memory_bytes() -> Option<usize> {
    None
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

#[derive(Debug)]
pub struct ObjectRpcStreamResponse {
    pub response: crate::ObjectRpcResponse,
    pub body: ObjectRpcStreamBody,
}

impl ObjectRpcStreamResponse {
    pub fn from_buffered_response(
        mut response: crate::ObjectRpcResponse,
        options: RawByteStreamChunkOptions,
    ) -> Self {
        let body = if object_rpc_response_has_streamed_daemon_body(&response)
            && !response.body.is_empty()
        {
            ObjectRpcStreamBody::Buffered {
                bytes: std::mem::take(&mut response.body),
                offset: 0,
                options,
            }
        } else {
            ObjectRpcStreamBody::Empty
        };
        Self { response, body }
    }

    fn from_daemon_stream_response(
        response: crate::ObjectRpcResponse,
        address: impl Into<String>,
        stream: AsyncDaemonControlStream,
        options: RawByteStreamChunkOptions,
    ) -> Self {
        let body = if object_rpc_response_has_streamed_daemon_body(&response) {
            ObjectRpcStreamBody::DaemonControlRaw {
                address: address.into(),
                stream: Some(stream),
                options,
                total_read: 0,
            }
        } else {
            ObjectRpcStreamBody::Empty
        };
        Self { response, body }
    }

    /// Attaches the absolute deadline for the whole RPC call to the response
    /// body. A body keeps the earliest deadline when multiple layers attach one.
    pub fn with_deadline(self, deadline: Option<tokio::time::Instant>) -> Self {
        Self {
            response: self.response,
            body: self.body.with_deadline(deadline),
        }
    }

    pub async fn collect_body_to_vec(mut self) -> Result<crate::ObjectRpcResponse, NetworkError> {
        let body = self.body.collect_to_vec().await?;
        self.response.body = body;
        Ok(self.response)
    }
}

#[derive(Debug)]
pub enum ObjectRpcStreamBody {
    Empty,
    Buffered {
        bytes: Vec<u8>,
        offset: usize,
        options: RawByteStreamChunkOptions,
    },
    DaemonControlRaw {
        address: String,
        stream: Option<AsyncDaemonControlStream>,
        options: RawByteStreamChunkOptions,
        total_read: u64,
    },
    Deadline {
        body: Box<ObjectRpcStreamBody>,
        deadline: tokio::time::Instant,
    },
}

impl ObjectRpcStreamBody {
    /// Attaches an absolute whole-call deadline without changing the existing
    /// per-chunk idle timeout. The earlier timeout wins while reading.
    pub fn with_deadline(self, deadline: Option<tokio::time::Instant>) -> Self {
        let Some(deadline) = deadline else {
            return self;
        };
        match self {
            ObjectRpcStreamBody::Deadline {
                body,
                deadline: existing,
            } => ObjectRpcStreamBody::Deadline {
                body,
                deadline: existing.min(deadline),
            },
            body => ObjectRpcStreamBody::Deadline {
                body: Box::new(body),
                deadline,
            },
        }
    }

    pub async fn read_next_chunk(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
        let (body, deadline) = self.unwrapped_body_and_deadline();
        let Some(deadline) = deadline else {
            return body.read_next_chunk_without_deadline().await;
        };
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(object_rpc_stream_overall_timeout(remaining));
        }
        match tokio::time::timeout_at(deadline, body.read_next_chunk_without_deadline()).await {
            Ok(result) => result,
            Err(_) => Err(object_rpc_stream_overall_timeout(remaining)),
        }
    }

    fn unwrapped_body_and_deadline(
        &mut self,
    ) -> (&mut ObjectRpcStreamBody, Option<tokio::time::Instant>) {
        let mut body = self;
        let mut earliest: Option<tokio::time::Instant> = None;
        loop {
            match body {
                ObjectRpcStreamBody::Deadline {
                    body: inner,
                    deadline,
                } => {
                    earliest = Some(earliest.map_or(*deadline, |current| current.min(*deadline)));
                    body = inner.as_mut();
                }
                _ => return (body, earliest),
            }
        }
    }

    async fn read_next_chunk_without_deadline(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
        match self {
            ObjectRpcStreamBody::Empty => Ok(None),
            ObjectRpcStreamBody::Buffered {
                bytes,
                offset,
                options,
            } => {
                let chunk_bytes = validate_raw_byte_stream_chunk_options(*options)?;
                if *offset >= bytes.len() {
                    Ok(None)
                } else {
                    let end = offset.saturating_add(chunk_bytes).min(bytes.len());
                    checked_object_stream_total(0, end, options.max_total_bytes)?;
                    let len = end.saturating_sub(*offset);
                    let mut chunk = Vec::new();
                    try_reserve_vec(&mut chunk, len, "read buffered raw byte stream chunk")?;
                    chunk.extend_from_slice(&bytes[*offset..end]);
                    *offset = end;
                    Ok(Some(chunk))
                }
            }
            ObjectRpcStreamBody::DaemonControlRaw {
                address,
                stream,
                options,
                total_read,
            } => {
                let Some(stream_ref) = stream.as_mut() else {
                    return Ok(None);
                };
                let result =
                    read_raw_byte_stream_chunk_from_async_stream(&mut stream_ref.stream, options)
                        .await
                        .map_err(|error| {
                            map_daemon_control_frame_error(
                                error,
                                format!("read async daemon control stream {address}"),
                            )
                        })?;
                let Some(bytes) = result else {
                    *stream = None;
                    return Ok(None);
                };
                *total_read =
                    checked_object_stream_total(*total_read, bytes.len(), options.max_total_bytes)?;
                Ok(Some(bytes))
            }
            ObjectRpcStreamBody::Deadline { .. } => {
                unreachable!("stream deadline wrappers are removed before reading")
            }
        }
    }

    pub async fn collect_to_vec(&mut self) -> Result<Vec<u8>, NetworkError> {
        let mut output = Vec::new();
        while let Some(chunk) = self.read_next_chunk().await? {
            try_reserve_vec(&mut output, chunk.len(), "collect raw byte stream body")?;
            output.extend_from_slice(&chunk);
        }
        Ok(output)
    }
}

fn object_rpc_stream_overall_timeout(timeout: Duration) -> NetworkError {
    NetworkError::timeout(
        "read object rpc stream body",
        NetworkTimeoutPhase::Overall,
        timeout,
        NetworkDeliveryState::PossiblyApplied,
    )
}

const ASYNC_DAEMON_CONTROL_STREAM_POOL_MAX_IDLE_PER_ADDRESS: usize = 64;
// The daemon closes a connection after DEFAULT_NETWORK_TIMEOUT without frame
// progress. Retire client-side idle entries first so normal reuse never races
// the server's idle close.
const ASYNC_DAEMON_CONTROL_STREAM_POOL_IDLE_EXPIRY_MARGIN: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct PooledAsyncDaemonControlStream {
    stream: AsyncDaemonControlStream,
    returned_at: Instant,
}

static ASYNC_DAEMON_CONTROL_STREAM_POOL: LazyLock<
    Mutex<HashMap<String, Vec<PooledAsyncDaemonControlStream>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DaemonRequestDispatch {
    DefinitelyNotSent,
    PossiblySent,
}

#[derive(Debug)]
struct DaemonControlAttemptError {
    source: NetworkError,
    dispatch: DaemonRequestDispatch,
}

impl DaemonControlAttemptError {
    fn after_request_write(source: NetworkError) -> Self {
        Self {
            source,
            dispatch: DaemonRequestDispatch::PossiblySent,
        }
    }

    fn into_source(self) -> NetworkError {
        let delivery_state = self.delivery_state();
        network_error_with_delivery_state(
            self.source,
            "send daemon control request",
            delivery_state,
        )
    }

    fn into_stale_pooled_connection(self) -> NetworkError {
        NetworkError::StalePooledConnection {
            operation: "reuse daemon control connection".to_string(),
            delivery_state: self.delivery_state(),
            detail: self.source.to_string(),
        }
    }

    fn delivery_state(&self) -> NetworkDeliveryState {
        self.source.delivery_state().unwrap_or(match self.dispatch {
            DaemonRequestDispatch::DefinitelyNotSent => NetworkDeliveryState::DefinitelyNotSent,
            DaemonRequestDispatch::PossiblySent => NetworkDeliveryState::PossiblyApplied,
        })
    }
}

struct DaemonRequestWriteTracker<'a> {
    stream: &'a mut tokio::net::TcpStream,
    wrote_bytes: bool,
}

impl DaemonRequestWriteTracker<'_> {
    fn dispatch(&self) -> DaemonRequestDispatch {
        if self.wrote_bytes {
            DaemonRequestDispatch::PossiblySent
        } else {
            DaemonRequestDispatch::DefinitelyNotSent
        }
    }
}

impl AsyncWrite for DaemonRequestWriteTracker<'_> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match std::pin::Pin::new(&mut *this.stream).poll_write(cx, buf) {
            std::task::Poll::Ready(Ok(written)) => {
                // Once the local socket accepts any byte, the peer may have
                // received enough of this request to act on it.
                this.wrote_bytes |= written != 0;
                std::task::Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut *self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut *self.get_mut().stream).poll_shutdown(cx)
    }
}

impl AsyncDaemonControlStream {
    pub fn new(stream: tokio::net::TcpStream) -> Self {
        Self { stream }
    }

    pub fn into_inner(self) -> tokio::net::TcpStream {
        self.stream
    }

    pub fn peer_addr(&self) -> Result<SocketAddr, NetworkError> {
        self.stream.peer_addr().map_err(|source| NetworkError::Io {
            operation: "read async daemon control peer address".to_string(),
            detail: source.to_string(),
        })
    }

    pub async fn connect(address: &str) -> Result<Self, NetworkError> {
        Self::connect_with_options(address, DaemonRpcTimeouts::default()).await
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

    pub async fn write_request(
        &mut self,
        address: &str,
        request: &DaemonControlRequest,
        options: ObjectFrameIoOptions,
    ) -> Result<(), NetworkError> {
        self.write_frame(address, request, options).await
    }

    async fn write_request_tracking_dispatch(
        &mut self,
        address: &str,
        request: &DaemonControlRequest,
        options: ObjectFrameIoOptions,
    ) -> Result<(), DaemonControlAttemptError> {
        let mut writer = DaemonRequestWriteTracker {
            stream: &mut self.stream,
            wrote_bytes: false,
        };
        let result = write_object_frame_async(&mut writer, request, options).await;
        let dispatch = writer.dispatch();
        result.map_err(|error| DaemonControlAttemptError {
            source: map_daemon_control_frame_error(
                error,
                format!("write async daemon control stream {address}"),
            ),
            dispatch,
        })
    }

    pub async fn write_response(
        &mut self,
        address: &str,
        response: &DaemonControlResponse,
        options: ObjectFrameIoOptions,
    ) -> Result<(), NetworkError> {
        self.write_frame(address, response, options).await
    }

    pub async fn write_rpc_response(
        &mut self,
        address: &str,
        response: &crate::ObjectRpcResponse,
        options: ObjectFrameIoOptions,
    ) -> Result<(), NetworkError> {
        self.write_response(address, response, options).await
    }

    pub async fn write_raw_byte_stream_chunk(
        &mut self,
        address: &str,
        bytes: &[u8],
        options: ObjectFrameIoOptions,
    ) -> Result<(), NetworkError> {
        let result = write_raw_byte_stream_chunk_async(&mut self.stream, bytes, options).await;
        result.map_err(|error| {
            map_daemon_control_frame_error(
                error,
                format!("write async daemon control stream {address}"),
            )
        })
    }

    pub async fn forward_raw_byte_stream_chunks_to(
        &mut self,
        source_address: &str,
        sink: &mut AsyncDaemonControlStream,
        sink_address: &str,
        options: RawByteStreamChunkOptions,
    ) -> Result<u64, NetworkError> {
        forward_raw_byte_stream_chunks_async(&mut self.stream, &mut sink.stream, options)
            .await
            .map_err(|error| match error {
                NetworkError::Io { detail, .. } => NetworkError::Io {
                    operation: format!(
                        "forward raw byte stream chunks {source_address} to {sink_address}"
                    ),
                    detail,
                },
                other => other,
            })
    }

    async fn write_frame<T>(
        &mut self,
        address: &str,
        value: &T,
        options: ObjectFrameIoOptions,
    ) -> Result<(), NetworkError>
    where
        T: serde::Serialize + ?Sized,
    {
        let result = write_object_frame_async(&mut self.stream, value, options).await;
        result.map_err(|error| {
            map_daemon_control_frame_error(
                error,
                format!("write async daemon control stream {address}"),
            )
        })
    }

    pub async fn read_request(
        &mut self,
        address: &str,
        max_bytes: usize,
        options: ObjectFrameIoOptions,
    ) -> Result<DaemonControlRequest, NetworkError> {
        self.read_frame(address, max_bytes, options).await
    }

    pub async fn read_response(
        &mut self,
        address: &str,
        max_bytes: usize,
        options: ObjectFrameIoOptions,
    ) -> Result<DaemonControlResponse, NetworkError> {
        self.read_frame(address, max_bytes, options).await
    }

    pub async fn read_rpc_response(
        &mut self,
        address: &str,
        max_bytes: usize,
        options: ObjectFrameIoOptions,
    ) -> Result<crate::ObjectRpcResponse, NetworkError> {
        self.read_frame(address, max_bytes, options).await
    }

    async fn read_frame<T>(
        &mut self,
        address: &str,
        max_bytes: usize,
        options: ObjectFrameIoOptions,
    ) -> Result<T, NetworkError>
    where
        T: serde::de::DeserializeOwned,
    {
        let result = read_object_frame_async(&mut self.stream, max_bytes, options).await;
        result.map_err(|error| {
            map_daemon_control_frame_error(
                error,
                format!("read async daemon control stream {address}"),
            )
        })
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

pub(crate) async fn send_daemon_control_request_async(
    address: &str,
    request: &DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
    max_response_bytes: usize,
) -> Result<DaemonControlResponse, NetworkError> {
    if daemon_control_request_allows_stream_reuse(request) {
        return send_reusable_daemon_control_request_async(
            address,
            request,
            timeouts,
            max_response_bytes,
        )
        .await;
    }
    let mut stream = AsyncDaemonControlStream::connect_with_options(address, timeouts).await?;
    send_daemon_control_request_on_stream_async(
        address,
        request,
        timeouts,
        max_response_bytes,
        &mut stream,
    )
    .await
    .map_err(DaemonControlAttemptError::into_source)
}

pub async fn send_daemon_control_rpc_request_async(
    address: &str,
    request: &DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
    max_response_bytes: usize,
) -> Result<crate::ObjectRpcResponse, NetworkError> {
    let response = if object_rpc_request_expects_raw_daemon_response(request) {
        send_daemon_control_rpc_request_raw_response_async(
            address,
            request,
            timeouts,
            max_response_bytes,
        )
        .await?
    } else {
        send_daemon_control_request_async(address, request, timeouts, max_response_bytes).await?
    };
    response.error_detail()?;
    Ok(response)
}

pub async fn send_daemon_control_rpc_request_stream_response_async(
    address: &str,
    request: &DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
    max_response_bytes: usize,
) -> Result<ObjectRpcStreamResponse, NetworkError> {
    let mut stream = AsyncDaemonControlStream::connect_with_options(address, timeouts).await?;
    let frame_options = ObjectFrameIoOptions::none().with_idle_timeout(timeouts.idle_timeout);
    stream
        .write_request_tracking_dispatch(address, request, frame_options)
        .await
        .map_err(DaemonControlAttemptError::into_source)?;
    let response = stream
        .read_rpc_response(address, max_response_bytes, frame_options)
        .await
        .map_err(DaemonControlAttemptError::after_request_write)
        .map_err(DaemonControlAttemptError::into_source)?;
    let body_options = RawByteStreamChunkOptions {
        chunk_bytes: OBJECT_STREAM_MAX_CHUNK_BYTES,
        max_total_bytes: u64::try_from(max_response_bytes).ok(),
        idle_timeout: timeouts.idle_timeout,
    };
    Ok(ObjectRpcStreamResponse::from_daemon_stream_response(
        response,
        address,
        stream,
        body_options,
    ))
}

pub(crate) async fn send_daemon_control_rpc_request_raw_response_async(
    address: &str,
    request: &DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
    max_response_bytes: usize,
) -> Result<crate::ObjectRpcResponse, NetworkError> {
    if daemon_control_request_allows_stream_reuse(request) {
        return send_reusable_daemon_control_rpc_request_raw_response_async(
            address,
            request,
            timeouts,
            max_response_bytes,
        )
        .await;
    }
    let mut stream = AsyncDaemonControlStream::connect_with_options(address, timeouts).await?;
    send_daemon_control_rpc_request_raw_response_on_stream_async(
        address,
        request,
        timeouts,
        max_response_bytes,
        &mut stream,
    )
    .await
    .map_err(DaemonControlAttemptError::into_source)
}

async fn send_reusable_daemon_control_request_async(
    address: &str,
    request: &DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
    max_response_bytes: usize,
) -> Result<DaemonControlResponse, NetworkError> {
    let (mut stream, reused) = match take_pooled_daemon_control_stream(address) {
        Some(stream) => (stream, true),
        None => (
            AsyncDaemonControlStream::connect_with_options(address, timeouts).await?,
            false,
        ),
    };
    match send_daemon_control_request_on_stream_async(
        address,
        request,
        timeouts,
        max_response_bytes,
        &mut stream,
    )
    .await
    {
        Ok(response) => {
            return_pooled_daemon_control_stream(address, stream);
            Ok(response)
        }
        Err(error) => {
            // A frame failure means every idle socket for this address may
            // predate the same peer restart. Dropping the whole address pool
            // also covers EOF, connection reset, and broken pipe failures.
            purge_pooled_daemon_control_streams(address);
            if reused && reused_stream_failure_is_retry_safe(request, error.dispatch) {
                let mut stream =
                    AsyncDaemonControlStream::connect_with_options(address, timeouts).await?;
                match send_daemon_control_request_on_stream_async(
                    address,
                    request,
                    timeouts,
                    max_response_bytes,
                    &mut stream,
                )
                .await
                {
                    Ok(response) => {
                        return_pooled_daemon_control_stream(address, stream);
                        Ok(response)
                    }
                    Err(error) => {
                        purge_pooled_daemon_control_streams(address);
                        Err(error.into_source())
                    }
                }
            } else if reused {
                Err(error.into_stale_pooled_connection())
            } else {
                Err(error.into_source())
            }
        }
    }
}

async fn send_reusable_daemon_control_rpc_request_raw_response_async(
    address: &str,
    request: &DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
    max_response_bytes: usize,
) -> Result<crate::ObjectRpcResponse, NetworkError> {
    let (mut stream, reused) = match take_pooled_daemon_control_stream(address) {
        Some(stream) => (stream, true),
        None => (
            AsyncDaemonControlStream::connect_with_options(address, timeouts).await?,
            false,
        ),
    };
    match send_daemon_control_rpc_request_raw_response_on_stream_async(
        address,
        request,
        timeouts,
        max_response_bytes,
        &mut stream,
    )
    .await
    {
        Ok(response) => {
            return_pooled_daemon_control_stream(address, stream);
            Ok(response)
        }
        Err(error) => {
            purge_pooled_daemon_control_streams(address);
            if reused && reused_stream_failure_is_retry_safe(request, error.dispatch) {
                let mut stream =
                    AsyncDaemonControlStream::connect_with_options(address, timeouts).await?;
                match send_daemon_control_rpc_request_raw_response_on_stream_async(
                    address,
                    request,
                    timeouts,
                    max_response_bytes,
                    &mut stream,
                )
                .await
                {
                    Ok(response) => {
                        return_pooled_daemon_control_stream(address, stream);
                        Ok(response)
                    }
                    Err(error) => {
                        purge_pooled_daemon_control_streams(address);
                        Err(error.into_source())
                    }
                }
            } else if reused {
                Err(error.into_stale_pooled_connection())
            } else {
                Err(error.into_source())
            }
        }
    }
}

async fn send_daemon_control_request_on_stream_async(
    address: &str,
    request: &DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
    max_response_bytes: usize,
    stream: &mut AsyncDaemonControlStream,
) -> Result<DaemonControlResponse, DaemonControlAttemptError> {
    let frame_options = ObjectFrameIoOptions::none().with_idle_timeout(timeouts.idle_timeout);
    stream
        .write_request_tracking_dispatch(address, request, frame_options)
        .await?;
    stream
        .read_response(address, max_response_bytes, frame_options)
        .await
        .map_err(DaemonControlAttemptError::after_request_write)
}

async fn send_daemon_control_rpc_request_raw_response_on_stream_async(
    address: &str,
    request: &DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
    max_response_bytes: usize,
    stream: &mut AsyncDaemonControlStream,
) -> Result<crate::ObjectRpcResponse, DaemonControlAttemptError> {
    let frame_options = ObjectFrameIoOptions::none().with_idle_timeout(timeouts.idle_timeout);
    stream
        .write_request_tracking_dispatch(address, request, frame_options)
        .await?;
    let mut response = stream
        .read_rpc_response(address, max_response_bytes, frame_options)
        .await
        .map_err(DaemonControlAttemptError::after_request_write)?;
    if object_rpc_response_has_streamed_daemon_body(&response) {
        let max_total_bytes = u64::try_from(max_response_bytes).ok();
        let mut body = Vec::new();
        read_raw_byte_stream_chunks_to_vec_async(
            &mut stream.stream,
            &mut body,
            RawByteStreamChunkOptions {
                chunk_bytes: OBJECT_STREAM_MAX_CHUNK_BYTES,
                max_total_bytes,
                idle_timeout: timeouts.idle_timeout,
            },
        )
        .await
        .map_err(|error| {
            map_daemon_control_frame_error(
                error,
                format!("read async daemon control stream {address}"),
            )
        })
        .map_err(DaemonControlAttemptError::after_request_write)?;
        response.body = body;
    }
    Ok(response)
}

fn reused_stream_failure_is_retry_safe(
    request: &DaemonControlRequest,
    dispatch: DaemonRequestDispatch,
) -> bool {
    // Replacing a stale pooled socket is unconditionally safe only before this
    // request made write progress. An unsigned local request may opt into an
    // ambiguous redispatch. A signed request must return to the route layer so
    // the retry gets a fresh daemon-auth nonce.
    request.auth.is_none()
        && (dispatch == DaemonRequestDispatch::DefinitelyNotSent
            || request.is_response_retry_safe())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectRpcRequestStreamMode {
    Buffered,
    ReuseDaemonStream,
    RawStreamedDaemonResponse,
}

impl ObjectRpcRequestStreamMode {
    pub const fn reuses_daemon_stream(self) -> bool {
        matches!(
            self,
            Self::ReuseDaemonStream | Self::RawStreamedDaemonResponse
        )
    }

    pub const fn expects_raw_streamed_response(self) -> bool {
        matches!(self, Self::RawStreamedDaemonResponse)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectRpcResponseStreamMode {
    Buffered,
    RawStreamedDaemonBody,
}

pub fn set_object_rpc_request_stream_mode(
    request: &mut crate::ObjectRpcRequest,
    mode: ObjectRpcRequestStreamMode,
) {
    request
        .metadata
        .remove(DAEMON_CONTROL_REUSE_STREAM_METADATA_KEY);
    request
        .metadata
        .remove(DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_KEY);
    request
        .metadata
        .remove(DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY);

    match mode {
        ObjectRpcRequestStreamMode::Buffered => {}
        ObjectRpcRequestStreamMode::ReuseDaemonStream => {
            mark_object_rpc_request_reuses_daemon_stream(request);
        }
        ObjectRpcRequestStreamMode::RawStreamedDaemonResponse => {
            mark_object_rpc_request_reuses_daemon_stream(request);
            mark_object_rpc_request_accepts_raw_streamed_daemon_response(request);
        }
    }
}

pub fn object_rpc_request_stream_mode(
    request: &crate::ObjectRpcRequest,
) -> ObjectRpcRequestStreamMode {
    if object_rpc_request_expects_raw_daemon_response(request)
        && object_rpc_request_accepts_streamed_daemon_response(request)
    {
        return ObjectRpcRequestStreamMode::RawStreamedDaemonResponse;
    }
    if object_rpc_request_reuses_daemon_stream(request) {
        return ObjectRpcRequestStreamMode::ReuseDaemonStream;
    }
    ObjectRpcRequestStreamMode::Buffered
}

pub fn set_object_rpc_response_stream_mode(
    response: &mut crate::ObjectRpcResponse,
    mode: ObjectRpcResponseStreamMode,
) {
    response
        .metadata
        .remove(DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY);
    match mode {
        ObjectRpcResponseStreamMode::Buffered => {}
        ObjectRpcResponseStreamMode::RawStreamedDaemonBody => {
            mark_object_rpc_response_has_streamed_daemon_body(response);
        }
    }
}

pub fn object_rpc_response_stream_mode(
    response: &crate::ObjectRpcResponse,
) -> ObjectRpcResponseStreamMode {
    if object_rpc_response_has_streamed_daemon_body(response) {
        ObjectRpcResponseStreamMode::RawStreamedDaemonBody
    } else {
        ObjectRpcResponseStreamMode::Buffered
    }
}

pub(crate) fn object_rpc_request_expects_raw_daemon_response(
    request: &crate::ObjectRpcRequest,
) -> bool {
    request
        .metadata
        .get(DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_KEY)
        .is_some_and(|value| {
            value == DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_VALUE
                || value.eq_ignore_ascii_case("true")
        })
}

pub(crate) fn object_rpc_request_accepts_streamed_daemon_response(
    request: &crate::ObjectRpcRequest,
) -> bool {
    request
        .metadata
        .get(DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY)
        .is_some_and(|value| {
            value == DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE
                || value.eq_ignore_ascii_case("true")
        })
}

pub(crate) fn mark_object_rpc_request_reuses_daemon_stream(request: &mut crate::ObjectRpcRequest) {
    request.metadata.insert(
        DAEMON_CONTROL_REUSE_STREAM_METADATA_KEY.to_string(),
        DAEMON_CONTROL_REUSE_STREAM_METADATA_VALUE.to_string(),
    );
}

pub(crate) fn object_rpc_request_reuses_daemon_stream(request: &crate::ObjectRpcRequest) -> bool {
    request
        .metadata
        .get(DAEMON_CONTROL_REUSE_STREAM_METADATA_KEY)
        .is_some_and(|value| {
            value == DAEMON_CONTROL_REUSE_STREAM_METADATA_VALUE
                || value.eq_ignore_ascii_case("true")
        })
}

pub(crate) fn mark_object_rpc_request_accepts_raw_streamed_daemon_response(
    request: &mut crate::ObjectRpcRequest,
) {
    request.metadata.insert(
        DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_KEY.to_string(),
        DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_VALUE.to_string(),
    );
    request.metadata.insert(
        DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY.to_string(),
        DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE.to_string(),
    );
}

pub(crate) fn mark_object_rpc_response_has_streamed_daemon_body(
    response: &mut crate::ObjectRpcResponse,
) {
    response.metadata.insert(
        DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY.to_string(),
        DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE.to_string(),
    );
}

pub(crate) fn object_rpc_response_has_streamed_daemon_body(
    response: &crate::ObjectRpcResponse,
) -> bool {
    response
        .metadata
        .get(DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY)
        .is_some_and(|value| value == DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE)
}

#[cfg(any(test, feature = "test-support"))]
pub fn daemon_control_request_expects_raw_rpc_response(request: &DaemonControlRequest) -> bool {
    object_rpc_request_expects_raw_daemon_response(request)
}

fn daemon_control_request_allows_stream_reuse(request: &DaemonControlRequest) -> bool {
    request.route == crate::CLUSTER_NETWORK_PING_ROUTE
        || request.route == crate::CLUSTER_RELAY_ROUTE
        || object_rpc_request_reuses_daemon_stream(request)
}

fn pooled_daemon_control_stream_max_idle() -> Duration {
    DEFAULT_NETWORK_TIMEOUT.saturating_sub(ASYNC_DAEMON_CONTROL_STREAM_POOL_IDLE_EXPIRY_MARGIN)
}

fn pooled_daemon_control_stream_is_expired(returned_at: Instant, now: Instant) -> bool {
    now.saturating_duration_since(returned_at) >= pooled_daemon_control_stream_max_idle()
}

fn take_pooled_daemon_control_stream(address: &str) -> Option<AsyncDaemonControlStream> {
    let mut pool = ASYNC_DAEMON_CONTROL_STREAM_POOL.lock().ok()?;
    let streams = pool.get_mut(address)?;
    let now = Instant::now();
    let mut stream = None;
    while let Some(pooled) = streams.pop() {
        if !pooled_daemon_control_stream_is_expired(pooled.returned_at, now) {
            stream = Some(pooled.stream);
            break;
        }
    }
    if streams.is_empty() {
        pool.remove(address);
    }
    stream
}

fn return_pooled_daemon_control_stream(address: &str, stream: AsyncDaemonControlStream) {
    return_pooled_daemon_control_stream_at(address, stream, Instant::now());
}

fn return_pooled_daemon_control_stream_at(
    address: &str,
    stream: AsyncDaemonControlStream,
    returned_at: Instant,
) {
    let Ok(mut pool) = ASYNC_DAEMON_CONTROL_STREAM_POOL.lock() else {
        return;
    };
    let streams = pool.entry(address.to_string()).or_default();
    if streams.len() < ASYNC_DAEMON_CONTROL_STREAM_POOL_MAX_IDLE_PER_ADDRESS {
        streams.push(PooledAsyncDaemonControlStream {
            stream,
            returned_at,
        });
    }
}

fn purge_pooled_daemon_control_streams(address: &str) {
    if let Ok(mut pool) = ASYNC_DAEMON_CONTROL_STREAM_POOL.lock() {
        pool.remove(address);
    }
}

fn map_daemon_control_frame_error(error: NetworkError, operation: String) -> NetworkError {
    match error {
        NetworkError::Io { detail, .. } => NetworkError::Io { operation, detail },
        other => other,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ObjectFrameLenPrefix {
    bytes: [u8; 12],
    len: usize,
}

impl ObjectFrameLenPrefix {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }

    pub(crate) const fn len(&self) -> usize {
        self.len
    }
}

pub(crate) fn object_frame_len_prefix(len: usize) -> Result<ObjectFrameLenPrefix, NetworkError> {
    let len = u64::try_from(len).map_err(|_| NetworkError::FrameTooLarge {
        actual_bytes: usize::MAX,
        max_bytes: usize::MAX,
    })?;
    let extended = len >= u64::from(OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL);
    let mut prefix = ObjectFrameLenPrefix {
        bytes: [0; 12],
        len: if extended { 12 } else { 4 },
    };
    if extended {
        prefix.bytes[..4].copy_from_slice(&OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL.to_be_bytes());
        prefix.bytes[4..12].copy_from_slice(&len.to_be_bytes());
    } else {
        prefix.bytes[..4].copy_from_slice(&(len as u32).to_be_bytes());
    }
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
    try_extend_from_slice(&mut frame, &0_u32.to_be_bytes(), "object frame length")?;
    let body = SinomoObjectCodec::network_frame().append_to_vec(&mut frame, value)?;
    let body_len = body.len();
    if body_len >= OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL as usize {
        let body_len_u64 = u64::try_from(body_len).map_err(|_| NetworkError::FrameTooLarge {
            actual_bytes: usize::MAX,
            max_bytes: usize::MAX,
        })?;
        let frame_len = frame
            .len()
            .checked_add(8)
            .ok_or(NetworkError::FrameTooLarge {
                actual_bytes: usize::MAX,
                max_bytes: usize::MAX,
            })?;
        let additional_bytes = frame_len.saturating_sub(frame.len());
        try_reserve_vec(&mut frame, additional_bytes, "object frame extended length")?;
        frame.resize(frame_len, 0);
        frame.copy_within(4..body.end, 12);
        frame[..4].copy_from_slice(&OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL.to_be_bytes());
        frame[4..12].copy_from_slice(&body_len_u64.to_be_bytes());
    } else {
        frame[..4].copy_from_slice(&(body_len as u32).to_be_bytes());
    }
    Ok(frame)
}

#[cfg(any(test, feature = "test-support"))]
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
        len_prefix.as_slice(),
        "encode borrowed object frame length",
    )?;
    for slice in parts.as_slices() {
        try_extend_from_slice(&mut frame, slice, "encode borrowed object frame body")?;
    }
    Ok(frame)
}

// Unconditional since stage C: see encode_object_frame_to_vec.
pub fn decode_single_object_frame_from_slice<T>(
    frame: &[u8],
    max_bytes: usize,
) -> Result<T, NetworkError>
where
    T: serde::de::DeserializeOwned,
{
    if frame.len() < 4 {
        return Err(NetworkError::Io {
            operation: "read object frame length".to_string(),
            detail: object_frame_eof_detail("length", frame.len(), 4),
        });
    }

    let len_bytes: [u8; 4] = frame[..4].try_into().expect("length prefix");
    let extended = u32::from_be_bytes(len_bytes) == OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL;
    let (extended_len_bytes, prefix_bytes) = if extended {
        if frame.len() < 12 {
            return Err(NetworkError::Io {
                operation: "read object frame extended length".to_string(),
                detail: object_frame_eof_detail(
                    "extended length",
                    frame.len().saturating_sub(4),
                    8,
                ),
            });
        }
        let bytes: [u8; 8] = frame[4..12].try_into().expect("extended length prefix");
        (Some(bytes), 12_usize)
    } else {
        (None, 4_usize)
    };

    let body_bytes = object_frame_len_from_prefix(len_bytes, extended_len_bytes, max_bytes)?;
    let body_end = prefix_bytes
        .checked_add(body_bytes)
        .ok_or(NetworkError::FrameTooLarge {
            actual_bytes: usize::MAX,
            max_bytes,
        })?;
    if frame.len() < body_end {
        return Err(NetworkError::Io {
            operation: "read object frame body".to_string(),
            detail: object_frame_eof_detail(
                "body",
                frame.len().saturating_sub(prefix_bytes),
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

    SinomoObjectCodec::network_frame().decode(&frame[prefix_bytes..body_end])
}

fn object_frame_len_from_u64(len: u64, max_bytes: usize) -> Result<usize, NetworkError> {
    let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    if len > max_bytes_u64 {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: usize::try_from(len).unwrap_or(usize::MAX),
            max_bytes,
        });
    }
    usize::try_from(len).map_err(|_| NetworkError::FrameTooLarge {
        actual_bytes: usize::MAX,
        max_bytes,
    })
}

pub(crate) fn object_frame_len_from_prefix(
    len_bytes: [u8; 4],
    extended_len_bytes: Option<[u8; 8]>,
    max_bytes: usize,
) -> Result<usize, NetworkError> {
    let len = u32::from_be_bytes(len_bytes);
    if len == OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL {
        let extended_len_bytes = extended_len_bytes.ok_or_else(|| NetworkError::Io {
            operation: "read object frame extended length".to_string(),
            detail: "missing extended object frame length".to_string(),
        })?;
        object_frame_len_from_u64(u64::from_be_bytes(extended_len_bytes), max_bytes)
    } else {
        object_frame_len_from_u64(u64::from(len), max_bytes)
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
        .write_all(len_prefix.as_slice())
        .and_then(|()| writer.write_all(&bytes))
        .map_err(|source| NetworkError::Io {
            operation: "write object frame".to_string(),
            detail: source.to_string(),
        })?;
    if let Some(observer) = io_observer() {
        observer.on_net_sent(len_prefix.len() as u64 + bytes.len() as u64);
    }
    Ok(())
}

#[cfg(any(test, feature = "test-support"))]
pub fn read_object_frame_blocking<R, T>(reader: &mut R, max_bytes: usize) -> Result<T, NetworkError>
where
    R: Read,
    T: serde::de::DeserializeOwned,
{
    read_object_frame_blocking_with_budget(reader, max_bytes, object_frame_byte_budget())
}

#[cfg(any(test, feature = "test-support"))]
fn read_object_frame_blocking_with_budget<R, T>(
    reader: &mut R,
    max_bytes: usize,
    budget: &ObjectFrameByteBudget,
) -> Result<T, NetworkError>
where
    R: Read,
    T: serde::de::DeserializeOwned,
{
    let (len, prefix_bytes) = read_object_frame_len_blocking(reader, max_bytes)?;
    let _permit = budget.try_acquire(len)?;
    let mut bytes = try_zeroed_vec(len, "read object frame body")?;
    read_exact_blocking_frame(reader, &mut bytes, "read object frame body", "body")?;
    if let Some(observer) = io_observer() {
        observer.on_net_recv(prefix_bytes as u64 + bytes.len() as u64);
    }
    SinomoObjectCodec::network_frame().decode(&bytes)
}

#[cfg(any(test, feature = "test-support"))]
fn read_object_frame_len_blocking<R: Read>(
    reader: &mut R,
    max_bytes: usize,
) -> Result<(usize, usize), NetworkError> {
    let mut len_bytes = [0_u8; 4];
    read_exact_blocking_frame(reader, &mut len_bytes, "read object frame length", "length")?;
    let extended = u32::from_be_bytes(len_bytes) == OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL;
    let extended_len_bytes = if extended {
        let mut extended_len_bytes = [0_u8; 8];
        read_exact_blocking_frame(
            reader,
            &mut extended_len_bytes,
            "read object frame extended length",
            "extended length",
        )?;
        Some(extended_len_bytes)
    } else {
        None
    };
    let len = object_frame_len_from_prefix(len_bytes, extended_len_bytes, max_bytes)?;
    Ok((len, if extended { 12 } else { 4 }))
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
    write_all_async_frame(
        writer,
        len_prefix.as_slice(),
        options.idle_timeout,
        "write async object frame",
        "length",
    )
    .await?;
    write_all_async_frame(
        writer,
        &bytes,
        options.idle_timeout,
        "write async object frame",
        "body",
    )
    .await?;
    if let Some(observer) = io_observer() {
        observer.on_net_sent(len_prefix.len() as u64 + bytes.len() as u64);
    }
    Ok(())
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
        len_prefix.as_slice(),
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
        observer.on_net_sent(len_prefix.len() as u64 + body_len as u64);
    }
    Ok(())
}

pub async fn read_object_frame_async<R, T>(
    reader: &mut R,
    max_bytes: usize,
    options: ObjectFrameIoOptions,
) -> Result<T, NetworkError>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    read_object_frame_async_with_budget(reader, max_bytes, options, object_frame_byte_budget())
        .await
}

async fn read_object_frame_async_with_budget<R, T>(
    reader: &mut R,
    max_bytes: usize,
    options: ObjectFrameIoOptions,
    budget: &ObjectFrameByteBudget,
) -> Result<T, NetworkError>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    let (len, prefix_bytes) =
        read_object_frame_len_async(reader, max_bytes, options.idle_timeout).await?;
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
        observer.on_net_recv(prefix_bytes as u64 + bytes.len() as u64);
    }
    SinomoObjectCodec::network_frame().decode(&bytes)
}

async fn read_object_frame_len_async<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_bytes: usize,
    idle_timeout: Option<Duration>,
) -> Result<(usize, usize), NetworkError> {
    let mut len_bytes = [0_u8; 4];
    read_exact_async_frame(
        reader,
        &mut len_bytes,
        idle_timeout,
        "read async object frame length",
        "length",
    )
    .await?;
    let extended = u32::from_be_bytes(len_bytes) == OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL;
    let extended_len_bytes = if extended {
        let mut extended_len_bytes = [0_u8; 8];
        read_exact_async_frame(
            reader,
            &mut extended_len_bytes,
            idle_timeout,
            "read async object frame extended length",
            "extended length",
        )
        .await?;
        Some(extended_len_bytes)
    } else {
        None
    };
    let len = object_frame_len_from_prefix(len_bytes, extended_len_bytes, max_bytes)?;
    Ok((len, if extended { 12 } else { 4 }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawByteStreamChunkOptions {
    pub chunk_bytes: usize,
    pub max_total_bytes: Option<u64>,
    pub idle_timeout: Option<Duration>,
}

impl Default for RawByteStreamChunkOptions {
    fn default() -> Self {
        Self {
            chunk_bytes: OBJECT_STREAM_DEFAULT_CHUNK_BYTES,
            max_total_bytes: None,
            idle_timeout: Some(DEFAULT_NETWORK_TIMEOUT),
        }
    }
}

async fn write_raw_byte_stream_chunk_async<W>(
    writer: &mut W,
    bytes: &[u8],
    options: ObjectFrameIoOptions,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    let len = u64::try_from(bytes.len()).map_err(|_| NetworkError::FrameTooLarge {
        actual_bytes: usize::MAX,
        max_bytes: usize::MAX,
    })?;
    let len_bytes = len.to_be_bytes();
    write_all_async_frame_unflushed(
        writer,
        &len_bytes,
        options.idle_timeout,
        "write raw byte stream chunk",
        "length",
    )
    .await?;
    if !bytes.is_empty() {
        write_all_async_frame_unflushed(
            writer,
            bytes,
            options.idle_timeout,
            "write raw byte stream chunk",
            "body",
        )
        .await?;
    }
    flush_async_frame(writer, options.idle_timeout, "write raw byte stream chunk").await?;
    if let Some(observer) = io_observer() {
        observer.on_net_sent(
            DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES as u64 + bytes.len() as u64,
        );
    }
    Ok(())
}

pub async fn read_raw_byte_stream_chunks_to_vec_async<R>(
    reader: &mut R,
    output: &mut Vec<u8>,
    options: RawByteStreamChunkOptions,
) -> Result<u64, NetworkError>
where
    R: AsyncRead + Unpin,
{
    let chunk_bytes = validate_raw_byte_stream_chunk_options(options)?;
    let mut total = 0_u64;
    loop {
        let mut len_bytes = [0_u8; DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES];
        read_exact_async_frame(
            reader,
            &mut len_bytes,
            options.idle_timeout,
            "read raw byte stream chunk length",
            "length",
        )
        .await?;
        let len = u64::from_be_bytes(len_bytes);
        if len == 0 {
            if let Some(observer) = io_observer() {
                observer.on_net_recv(DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES as u64);
            }
            return Ok(total);
        }
        let len_usize = usize::try_from(len).map_err(|_| NetworkError::FrameTooLarge {
            actual_bytes: usize::MAX,
            max_bytes: chunk_bytes,
        })?;
        if len_usize > chunk_bytes {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: len_usize,
                max_bytes: chunk_bytes,
            });
        }
        total = checked_object_stream_total(total, len_usize, options.max_total_bytes)?;
        let start = output.len();
        let end = start
            .checked_add(len_usize)
            .ok_or(NetworkError::FrameTooLarge {
                actual_bytes: usize::MAX,
                max_bytes: usize::MAX,
            })?;
        try_reserve_vec(output, len_usize, "read raw byte stream chunks")?;
        output.resize(end, 0);
        read_exact_async_frame(
            reader,
            &mut output[start..end],
            options.idle_timeout,
            "read raw byte stream chunk body",
            "body",
        )
        .await?;
        if let Some(observer) = io_observer() {
            observer.on_net_recv(
                DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES as u64 + len_usize as u64,
            );
        }
    }
}

async fn read_raw_byte_stream_chunk_from_async_stream<R>(
    reader: &mut R,
    options: &RawByteStreamChunkOptions,
) -> Result<Option<Vec<u8>>, NetworkError>
where
    R: AsyncRead + Unpin,
{
    let chunk_bytes = validate_raw_byte_stream_chunk_options(*options)?;
    let mut len_bytes = [0_u8; DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES];
    read_exact_async_frame(
        reader,
        &mut len_bytes,
        options.idle_timeout,
        "read raw byte stream chunk length",
        "length",
    )
    .await?;
    let len = u64::from_be_bytes(len_bytes);
    if len == 0 {
        if let Some(observer) = io_observer() {
            observer.on_net_recv(DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES as u64);
        }
        return Ok(None);
    }
    let len_usize = usize::try_from(len).map_err(|_| NetworkError::FrameTooLarge {
        actual_bytes: usize::MAX,
        max_bytes: chunk_bytes,
    })?;
    if len_usize > chunk_bytes {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: len_usize,
            max_bytes: chunk_bytes,
        });
    }
    let mut bytes = try_zeroed_vec(len_usize, "read raw byte stream chunk")?;
    read_exact_async_frame(
        reader,
        &mut bytes,
        options.idle_timeout,
        "read raw byte stream chunk body",
        "body",
    )
    .await?;
    if let Some(observer) = io_observer() {
        observer.on_net_recv(
            DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES as u64 + len_usize as u64,
        );
    }
    Ok(Some(bytes))
}

pub async fn forward_raw_byte_stream_chunks_async<R, W>(
    reader: &mut R,
    writer: &mut W,
    options: RawByteStreamChunkOptions,
) -> Result<u64, NetworkError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let chunk_bytes = validate_raw_byte_stream_chunk_options(options)?;
    let copy_buffer_bytes = chunk_bytes.min(OBJECT_STREAM_DEFAULT_CHUNK_BYTES).max(1);
    let mut buffer = try_zeroed_vec(copy_buffer_bytes, "forward raw byte stream buffer")?;
    let mut total = 0_u64;
    loop {
        let mut len_bytes = [0_u8; DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES];
        read_exact_async_frame(
            reader,
            &mut len_bytes,
            options.idle_timeout,
            "forward raw byte stream chunk length",
            "length",
        )
        .await?;
        let len = u64::from_be_bytes(len_bytes);
        if len == 0 {
            write_all_async_frame(
                writer,
                &len_bytes,
                options.idle_timeout,
                "forward raw byte stream chunk length",
                "length",
            )
            .await?;
            if let Some(observer) = io_observer() {
                observer.on_net_recv(DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES as u64);
                observer.on_net_sent(DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES as u64);
            }
            return Ok(total);
        }
        let len_usize = usize::try_from(len).map_err(|_| NetworkError::FrameTooLarge {
            actual_bytes: usize::MAX,
            max_bytes: chunk_bytes,
        })?;
        if len_usize > chunk_bytes {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: len_usize,
                max_bytes: chunk_bytes,
            });
        }
        total = checked_object_stream_total(total, len_usize, options.max_total_bytes)?;
        write_all_async_frame_unflushed(
            writer,
            &len_bytes,
            options.idle_timeout,
            "forward raw byte stream chunk length",
            "length",
        )
        .await?;
        let mut remaining = len_usize;
        while remaining > 0 {
            let take = remaining.min(buffer.len());
            read_exact_async_frame(
                reader,
                &mut buffer[..take],
                options.idle_timeout,
                "forward raw byte stream chunk body",
                "body",
            )
            .await?;
            write_all_async_frame_unflushed(
                writer,
                &buffer[..take],
                options.idle_timeout,
                "forward raw byte stream chunk body",
                "body",
            )
            .await?;
            remaining -= take;
        }
        flush_async_frame(
            writer,
            options.idle_timeout,
            "forward raw byte stream chunk body",
        )
        .await?;
        if let Some(observer) = io_observer() {
            observer.on_net_recv(
                DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES as u64 + len_usize as u64,
            );
            observer.on_net_sent(
                DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES as u64 + len_usize as u64,
            );
        }
    }
}

fn validate_raw_byte_stream_chunk_options(
    options: RawByteStreamChunkOptions,
) -> Result<usize, NetworkError> {
    if options.chunk_bytes == 0 {
        return Err(NetworkError::Serialization {
            detail: "raw byte stream chunk size must be greater than zero".to_string(),
        });
    }
    if options.chunk_bytes > OBJECT_STREAM_MAX_CHUNK_BYTES {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: options.chunk_bytes,
            max_bytes: OBJECT_STREAM_MAX_CHUNK_BYTES,
        });
    }
    Ok(options.chunk_bytes)
}

fn checked_object_stream_total(
    current: u64,
    next_chunk_bytes: usize,
    max_total_bytes: Option<u64>,
) -> Result<u64, NetworkError> {
    let total = current
        .checked_add(next_chunk_bytes as u64)
        .ok_or_else(|| NetworkError::FrameTooLarge {
            actual_bytes: usize::MAX,
            max_bytes: usize::MAX,
        })?;
    if let Some(max_total_bytes) = max_total_bytes
        && total > max_total_bytes
    {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: usize::try_from(total).unwrap_or(usize::MAX),
            max_bytes: usize::try_from(max_total_bytes).unwrap_or(usize::MAX),
        });
    }
    Ok(total)
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
        ObjectFrameIoOptions, ObjectRpcRequestStreamMode, ObjectRpcResponseStreamMode,
        ObjectRpcStreamBody, ObjectRpcStreamResponse, RawByteStreamChunkOptions,
        SINOMO_NETWORK_OBJECT_VERSION, SinomoObjectCodec, decode_single_object_frame_from_slice,
        encode_borrowed_object_frame_to_vec, encode_object_frame_to_vec,
        forward_raw_byte_stream_chunks_async, object_rpc_request_accepts_streamed_daemon_response,
        object_rpc_request_expects_raw_daemon_response, object_rpc_request_reuses_daemon_stream,
        object_rpc_request_stream_mode, object_rpc_response_stream_mode, read_object_frame_async,
        read_object_frame_blocking, read_raw_byte_stream_chunks_to_vec_async,
        send_daemon_control_request_async, send_daemon_control_rpc_request_stream_response_async,
        set_object_rpc_request_stream_mode, set_object_rpc_response_stream_mode,
        write_all_async_frame, write_borrowed_object_frame_async, write_object_frame_blocking,
        write_raw_byte_stream_chunk_async,
    };
    use crate::error::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
    use serde::{Deserialize, Serialize};
    use sinomo_object::{BorrowedDocumentBuilder, DocumentView, LengthMode};
    use std::io::Cursor;
    use std::time::{Duration, Instant};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
    fn blocking_object_frame_reports_closed_before_length() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let error =
            read_object_frame_blocking::<_, BundleRecord>(&mut cursor, 1024).expect_err("eof");

        assert_eq!(
            error,
            NetworkError::Io {
                operation: "read object frame length".to_string(),
                detail: "peer closed before object frame length (4 bytes expected)".to_string(),
            }
        );
    }

    #[test]
    fn blocking_object_frame_reports_partial_body() {
        let mut bytes = 8_u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(b"abc");
        let mut cursor = Cursor::new(bytes);
        let error =
            read_object_frame_blocking::<_, BundleRecord>(&mut cursor, 1024).expect_err("eof");

        assert_eq!(
            error,
            NetworkError::Io {
                operation: "read object frame body".to_string(),
                detail: "peer closed during object frame body: read 3 of 8 bytes".to_string(),
            }
        );
    }

    #[test]
    fn object_frame_writer_uses_u64_network_frame_codec() {
        let record = BundleRecord {
            name: "large-ready".to_string(),
            payload: Some(vec![1, 2, 3, 4]),
        };
        let mut frame = Vec::new();
        write_object_frame_blocking(&mut frame, &record).expect("write frame");

        let body_len = u32::from_be_bytes(frame[..4].try_into().expect("length")) as usize;
        let body = &frame[4..];
        assert_eq!(body.len(), body_len);
        let view = DocumentView::parse(body).expect("parse sobj");
        assert_eq!(view.metadata.length_mode, LengthMode::U64);

        let mut cursor = Cursor::new(frame);
        let decoded: BundleRecord =
            read_object_frame_blocking(&mut cursor, 1024).expect("read frame");
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
            decode_single_object_frame_from_slice(&frame, 1024).expect("decode frame");

        assert_eq!(decoded, record);
    }

    #[test]
    fn object_frame_large_serde_bytes_payload_baseline_is_compact() {
        let payload: Vec<u8> = (0..=255u8).cycle().take(1024 * 1024).collect();
        let record = BundleRecord {
            name: "large-frame".to_string(),
            payload: Some(payload.clone()),
        };

        let frame = encode_object_frame_to_vec(&record).expect("encode frame");
        let body_len = u32::from_be_bytes(frame[..4].try_into().expect("length")) as usize;
        let body = &frame[4..];

        assert_eq!(body.len(), body_len);
        assert!(
            frame.len() < payload.len() + 512,
            "expected compact SOBJ bytes payload, got {} frame bytes for {} payload bytes",
            frame.len(),
            payload.len()
        );
        assert!(
            body.windows(payload.len()).any(|w| w == payload.as_slice()),
            "large payload should be embedded as raw bytes in the current contiguous baseline"
        );

        let view = DocumentView::parse(body).expect("parse body");
        assert_eq!(view.metadata.length_mode, LengthMode::U64);

        let decoded: BundleRecord =
            decode_single_object_frame_from_slice(&frame, frame.len()).expect("decode frame");
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
        expected.extend_from_slice(&(body.len() as u32).to_be_bytes());
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
        let body_len = u32::from_be_bytes(frame[..4].try_into().expect("length")) as usize;
        let body = &frame[4..];

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
            8192,
            ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
        );
        let (write_result, read_result) = tokio::join!(write, read);

        write_result.expect("write borrowed frame");
        assert_eq!(
            read_result.expect("read borrowed frame").as_ref(),
            payload.as_slice()
        );
    }

    #[test]
    fn memory_object_frame_rejects_trailing_bytes() {
        let record = BundleRecord {
            name: "memory-trailing".to_string(),
            payload: None,
        };
        let mut frame = encode_object_frame_to_vec(&record).expect("encode frame");
        frame.extend_from_slice(b"junk");

        let error = decode_single_object_frame_from_slice::<BundleRecord>(&frame, 1024)
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
    fn blocking_object_frame_reads_extended_length_prefix() {
        let record = BundleRecord {
            name: "extended".to_string(),
            payload: Some(vec![5, 6, 7, 8]),
        };
        let body = SinomoObjectCodec::network_frame()
            .encode(&record)
            .expect("encode body");
        let mut frame = super::OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL
            .to_be_bytes()
            .to_vec();
        frame.extend_from_slice(&(body.len() as u64).to_be_bytes());
        frame.extend_from_slice(&body);

        let mut cursor = Cursor::new(frame);
        let decoded: BundleRecord =
            read_object_frame_blocking(&mut cursor, 1024).expect("read extended frame");

        assert_eq!(decoded, record);
    }

    #[test]
    fn blocking_object_frame_rejects_extended_length_above_max_before_body_read() {
        let mut frame = super::OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL
            .to_be_bytes()
            .to_vec();
        frame.extend_from_slice(&1025_u64.to_be_bytes());
        let mut cursor = Cursor::new(frame);

        let error =
            read_object_frame_blocking::<_, BundleRecord>(&mut cursor, 1024).expect_err("too big");

        assert_eq!(
            error,
            NetworkError::FrameTooLarge {
                actual_bytes: 1025,
                max_bytes: 1024,
            }
        );
        assert_eq!(
            cursor.position(),
            12,
            "frame reader should reject an oversized extended length before reading the body"
        );
    }

    #[test]
    fn daemon_control_default_frame_limit_is_pointer_sized() {
        assert_eq!(super::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES, usize::MAX);
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
        assert_eq!(
            RawByteStreamChunkOptions::default().idle_timeout,
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
        let mut frame = 8_u32.to_be_bytes().to_vec();
        frame.extend_from_slice(b"abcdefgh");
        let mut cursor = Cursor::new(frame);

        let error = super::read_object_frame_blocking_with_budget::<_, BundleRecord>(
            &mut cursor,
            1024,
            &budget,
        )
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
            4,
            "budget rejection should happen before reading the frame body"
        );
        assert_eq!(budget.in_flight_bytes(), 0);
    }

    #[tokio::test]
    async fn async_object_frame_budget_rejects_before_body_read() {
        let budget = super::ObjectFrameByteBudget::fixed(4);
        let mut frame = 8_u32.to_be_bytes().to_vec();
        frame.extend_from_slice(b"abcdefgh");
        let mut cursor = Cursor::new(frame);

        let error = super::read_object_frame_async_with_budget::<_, BundleRecord>(
            &mut cursor,
            1024,
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
            4,
            "budget rejection should happen before reading the frame body"
        );
        assert_eq!(budget.in_flight_bytes(), 0);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn blocking_object_frame_reports_resource_exhaustion_before_huge_body_read() {
        let mut frame = super::OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL
            .to_be_bytes()
            .to_vec();
        frame.extend_from_slice(&u64::MAX.to_be_bytes());
        let mut cursor = Cursor::new(frame);

        let error = read_object_frame_blocking::<_, BundleRecord>(&mut cursor, usize::MAX)
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
    async fn async_object_frame_reads_extended_length_prefix_with_idle_timeout() {
        let record = BundleRecord {
            name: "async-extended".to_string(),
            payload: Some(vec![9, 10, 11, 12]),
        };
        let body = SinomoObjectCodec::network_frame()
            .encode(&record)
            .expect("encode body");
        let mut frame = super::OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL
            .to_be_bytes()
            .to_vec();
        frame.extend_from_slice(&(body.len() as u64).to_be_bytes());
        frame.extend_from_slice(&body);
        let mut cursor = Cursor::new(frame);

        let decoded: BundleRecord = read_object_frame_async(
            &mut cursor,
            1024,
            ObjectFrameIoOptions::idle_timeout(Duration::from_millis(20)),
        )
        .await
        .expect("read async extended frame");

        assert_eq!(decoded, record);
    }

    #[tokio::test]
    async fn async_object_frame_reports_partial_body() {
        let (mut client, mut server) = tokio::io::duplex(64);
        server
            .write_all(&8_u32.to_be_bytes())
            .await
            .expect("write length");
        server.write_all(b"abc").await.expect("write partial body");
        drop(server);

        let error = read_object_frame_async::<_, BundleRecord>(
            &mut client,
            1024,
            ObjectFrameIoOptions::none(),
        )
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
    async fn async_daemon_control_request_round_trips_over_tcp_listener() {
        let listener = AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let server = tokio::spawn(async move {
            let mut stream = listener.accept().await.expect("accept");
            let request = stream
                .read_request(
                    "listener",
                    1024,
                    ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                )
                .await
                .expect("request");
            assert_eq!(request.route, "/daemon-control/ping");
            let response =
                crate::ObjectRpcResponse::encode_body("/daemon-control/ping", &"pong".to_string())
                    .expect("response");
            stream
                .write_response(
                    "listener",
                    &response,
                    ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                )
                .await
                .expect("response");
        });

        let response = send_daemon_control_request_async(
            &address,
            &crate::ObjectRpcRequest::encode_body("/daemon-control/ping", "daemon-control", &())
                .expect("request"),
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            1024,
        )
        .await
        .expect("control response");

        assert_eq!(response.decode_body::<String>().expect("body"), "pong");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn daemon_control_pool_expires_before_server_idle_timeout_and_purges_address() {
        let listener = AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let (client, server) = tokio::join!(
            AsyncDaemonControlStream::connect(&address),
            listener.accept()
        );
        let client = client.expect("client");
        let server = server.expect("server");
        let max_idle = super::pooled_daemon_control_stream_max_idle();
        assert!(max_idle < super::DEFAULT_NETWORK_TIMEOUT);
        let expired_at = Instant::now()
            .checked_sub(max_idle)
            .expect("expired instant");
        super::return_pooled_daemon_control_stream_at(&address, client, expired_at);
        assert!(
            super::take_pooled_daemon_control_stream(&address).is_none(),
            "a pooled stream must expire before the server's idle deadline"
        );
        drop(server);

        let (client, server) = tokio::join!(
            AsyncDaemonControlStream::connect(&address),
            listener.accept()
        );
        let client = client.expect("client");
        let server = server.expect("server");
        super::return_pooled_daemon_control_stream(&address, client);
        super::purge_pooled_daemon_control_streams(&address);
        assert!(
            super::take_pooled_daemon_control_stream(&address).is_none(),
            "purging an address must discard all of its pooled streams"
        );
        drop(server);
    }

    async fn exercise_ambiguous_reused_stream_failure(
        stream_mode: ObjectRpcRequestStreamMode,
        response_retry_safe: bool,
        signed: bool,
    ) -> (Result<crate::ObjectRpcResponse, NetworkError>, bool) {
        let listener = AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let route =
            format!("/test/reused-stream-retry/{stream_mode:?}/{response_retry_safe}/{signed}");
        let server_route = route.clone();
        let server = tokio::spawn(async move {
            let frame_options = ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1));
            let mut stream = listener.accept().await.expect("accept initial stream");
            let first_request = stream
                .read_request("listener", 1024, frame_options)
                .await
                .expect("first request");
            assert_eq!(first_request.route, server_route);
            let first_response =
                crate::ObjectRpcResponse::encode_body(&first_request.route, &"first".to_string())
                    .expect("first response");
            stream
                .write_rpc_response("listener", &first_response, frame_options)
                .await
                .expect("write first response");

            let second_request = stream
                .read_request("listener", 1024, frame_options)
                .await
                .expect("second request");
            assert_eq!(second_request.route, server_route);
            drop(stream);

            let mut retry_stream =
                match tokio::time::timeout(Duration::from_millis(250), listener.accept()).await {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(error)) => panic!("accept retry stream: {error}"),
                    Err(_) => return false,
                };
            let retry_request = retry_stream
                .read_request("listener", 1024, frame_options)
                .await
                .expect("retry request");
            assert_eq!(retry_request.route, server_route);
            let retry_response =
                crate::ObjectRpcResponse::encode_body(&retry_request.route, &"retried".to_string())
                    .expect("retry response");
            retry_stream
                .write_rpc_response("listener", &retry_response, frame_options)
                .await
                .expect("write retry response");
            true
        });

        let mut request = crate::ObjectRpcRequest::from_encoded_body(route, "test", Vec::new());
        set_object_rpc_request_stream_mode(&mut request, stream_mode);
        if response_retry_safe {
            request = request.with_response_retry_safe();
        }
        if signed {
            request = request.with_auth(vec![1, 2, 3]);
        }

        let first = super::send_daemon_control_rpc_request_async(
            &address,
            &request,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            1024,
        )
        .await
        .expect("first response");
        assert_eq!(first.decode_body::<String>().expect("first body"), "first");
        let second = super::send_daemon_control_rpc_request_async(
            &address,
            &request,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            1024,
        )
        .await;
        let retried = server.await.expect("server task");
        (second, retried)
    }

    #[test]
    fn reused_stream_retry_safety_requires_opt_in_after_dispatch() {
        let request =
            crate::ObjectRpcRequest::from_encoded_body("/test/retry-safety", "test", Vec::new());
        assert!(super::reused_stream_failure_is_retry_safe(
            &request,
            super::DaemonRequestDispatch::DefinitelyNotSent,
        ));
        assert!(!super::reused_stream_failure_is_retry_safe(
            &request,
            super::DaemonRequestDispatch::PossiblySent,
        ));

        let request = request.with_response_retry_safe();
        assert!(super::reused_stream_failure_is_retry_safe(
            &request,
            super::DaemonRequestDispatch::PossiblySent,
        ));
        assert!(!super::reused_stream_failure_is_retry_safe(
            &request.clone().with_auth(vec![1, 2, 3]),
            super::DaemonRequestDispatch::PossiblySent,
        ));
        assert!(!super::reused_stream_failure_is_retry_safe(
            &request.with_auth(vec![1, 2, 3]),
            super::DaemonRequestDispatch::DefinitelyNotSent,
        ));
    }

    #[test]
    fn daemon_attempt_error_exposes_dispatch_certainty_and_preserves_typed_state() {
        for (dispatch, expected) in [
            (
                super::DaemonRequestDispatch::DefinitelyNotSent,
                NetworkDeliveryState::DefinitelyNotSent,
            ),
            (
                super::DaemonRequestDispatch::PossiblySent,
                NetworkDeliveryState::PossiblyApplied,
            ),
        ] {
            let error = super::DaemonControlAttemptError {
                source: NetworkError::Io {
                    operation: "test daemon request".to_string(),
                    detail: "failed".to_string(),
                },
                dispatch,
            }
            .into_source();
            assert!(matches!(
                error,
                NetworkError::DeliveryFailed {
                    delivery_state,
                    ..
                } if delivery_state == expected
            ));
        }

        let typed = NetworkError::timeout(
            "test typed timeout",
            NetworkTimeoutPhase::Idle,
            Duration::from_millis(10),
            NetworkDeliveryState::PossiblyApplied,
        );
        let mapped = super::DaemonControlAttemptError {
            source: typed.clone(),
            dispatch: super::DaemonRequestDispatch::DefinitelyNotSent,
        }
        .into_source();
        assert_eq!(mapped, typed);
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

    #[tokio::test]
    async fn reused_stream_does_not_redispatch_ambiguous_request_without_opt_in() {
        for stream_mode in [
            ObjectRpcRequestStreamMode::ReuseDaemonStream,
            ObjectRpcRequestStreamMode::RawStreamedDaemonResponse,
        ] {
            let (response, retried) =
                exercise_ambiguous_reused_stream_failure(stream_mode, false, false).await;
            assert!(response.is_err(), "{stream_mode:?} should remain failed");
            assert!(!retried, "{stream_mode:?} should not be redispatched");
        }
    }

    #[tokio::test]
    async fn reused_stream_redispatches_ambiguous_request_with_explicit_opt_in() {
        for stream_mode in [
            ObjectRpcRequestStreamMode::ReuseDaemonStream,
            ObjectRpcRequestStreamMode::RawStreamedDaemonResponse,
        ] {
            let (response, retried) =
                exercise_ambiguous_reused_stream_failure(stream_mode, true, false).await;
            let response = response.expect("retry-safe response");
            assert_eq!(
                response.decode_body::<String>().expect("retry body"),
                "retried"
            );
            assert!(retried, "{stream_mode:?} should be redispatched");
        }
    }

    #[tokio::test]
    async fn signed_reused_stream_failure_returns_to_route_for_fresh_auth() {
        for stream_mode in [
            ObjectRpcRequestStreamMode::ReuseDaemonStream,
            ObjectRpcRequestStreamMode::RawStreamedDaemonResponse,
        ] {
            let (response, retried) =
                exercise_ambiguous_reused_stream_failure(stream_mode, true, true).await;
            assert!(matches!(
                response,
                Err(NetworkError::StalePooledConnection {
                    delivery_state: NetworkDeliveryState::PossiblyApplied,
                    ..
                })
            ));
            assert!(
                !retried,
                "{stream_mode:?} must be re-signed before a fresh connection retry"
            );
        }
    }

    #[tokio::test]
    async fn reusable_daemon_control_request_reuses_ping_route_stream() {
        let listener = AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let server = tokio::spawn(async move {
            let mut stream = listener.accept().await.expect("accept");
            for expected in ["one", "two"] {
                let request = stream
                    .read_request(
                        "listener",
                        1024,
                        ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                    )
                    .await
                    .expect("request");
                assert_eq!(request.route, crate::CLUSTER_NETWORK_PING_ROUTE);
                let response = crate::ObjectRpcResponse::encode_body(
                    crate::CLUSTER_NETWORK_PING_ROUTE,
                    &expected.to_string(),
                )
                .expect("response");
                stream
                    .write_response(
                        "listener",
                        &response,
                        ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                    )
                    .await
                    .expect("response");
            }
        });
        let request = crate::ObjectRpcRequest {
            route: crate::CLUSTER_NETWORK_PING_ROUTE.to_string(),
            category: crate::CLUSTER_NETWORK_RPC_CATEGORY.to_string(),
            metadata: std::collections::BTreeMap::new(),
            auth: None,
            body: Vec::new(),
        };

        let first = send_daemon_control_request_async(
            &address,
            &request,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            1024,
        )
        .await
        .expect("first response");
        let second = send_daemon_control_request_async(
            &address,
            &request,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            1024,
        )
        .await
        .expect("second response");

        assert_eq!(first.decode_body::<String>().expect("first body"), "one");
        assert_eq!(second.decode_body::<String>().expect("second body"), "two");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn reusable_daemon_control_request_reuses_metadata_opt_in_stream() {
        let listener = AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let server = tokio::spawn(async move {
            let mut stream = listener.accept().await.expect("accept");
            for expected in ["one", "two"] {
                let request = stream
                    .read_request(
                        "listener",
                        1024,
                        ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                    )
                    .await
                    .expect("request");
                assert_eq!(request.route, "/test/reusable");
                let response =
                    crate::ObjectRpcResponse::encode_body("/test/reusable", &expected.to_string())
                        .expect("response");
                stream
                    .write_response(
                        "listener",
                        &response,
                        ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                    )
                    .await
                    .expect("response");
            }
        });
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert(
            super::DAEMON_CONTROL_REUSE_STREAM_METADATA_KEY.to_string(),
            super::DAEMON_CONTROL_REUSE_STREAM_METADATA_VALUE.to_string(),
        );
        let request = crate::ObjectRpcRequest {
            route: "/test/reusable".to_string(),
            category: "test".to_string(),
            metadata,
            auth: None,
            body: Vec::new(),
        };

        let first = send_daemon_control_request_async(
            &address,
            &request,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            1024,
        )
        .await
        .expect("first response");
        let second = send_daemon_control_request_async(
            &address,
            &request,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            1024,
        )
        .await
        .expect("second response");

        assert_eq!(first.decode_body::<String>().expect("first body"), "one");
        assert_eq!(second.decode_body::<String>().expect("second body"), "two");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn reusable_daemon_control_request_can_read_object_rpc_response() {
        let listener = AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let server = tokio::spawn(async move {
            let mut stream = listener.accept().await.expect("accept");
            for expected in ["one", "two"] {
                let request = stream
                    .read_request(
                        "listener",
                        1024,
                        ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                    )
                    .await
                    .expect("request");
                assert_eq!(request.route, "/test/reusable-rpc-response");
                let response = crate::ObjectRpcResponse::encode_body(
                    "/test/reusable-rpc-response",
                    &expected.to_string(),
                )
                .expect("response");
                stream
                    .write_rpc_response(
                        "listener",
                        &response,
                        ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                    )
                    .await
                    .expect("response");
            }
        });
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert(
            super::DAEMON_CONTROL_REUSE_STREAM_METADATA_KEY.to_string(),
            super::DAEMON_CONTROL_REUSE_STREAM_METADATA_VALUE.to_string(),
        );
        let request = crate::ObjectRpcRequest {
            route: "/test/reusable-rpc-response".to_string(),
            category: "test".to_string(),
            metadata,
            auth: None,
            body: Vec::new(),
        };

        let first = send_daemon_control_request_async(
            &address,
            &request,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            1024,
        )
        .await
        .expect("first response");
        let second = send_daemon_control_request_async(
            &address,
            &request,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            1024,
        )
        .await
        .expect("second response");

        assert_eq!(first.decode_body::<String>().expect("first body"), "one");
        assert_eq!(second.decode_body::<String>().expect("second body"), "two");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn raw_rpc_response_can_stream_body_chunks() {
        let listener = AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let server = tokio::spawn(async move {
            let mut stream = listener.accept().await.expect("accept");
            let request = stream
                .read_request(
                    "listener",
                    1024,
                    ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                )
                .await
                .expect("request");
            assert_eq!(request.route, "/test/raw-stream");
            assert!(super::daemon_control_request_expects_raw_rpc_response(
                &request
            ));
            assert!(super::object_rpc_request_accepts_streamed_daemon_response(
                &request
            ));
            let mut response =
                crate::ObjectRpcResponse::from_encoded_body("/test/raw-stream", Vec::new());
            response.metadata.insert(
                super::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY.to_string(),
                super::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE.to_string(),
            );
            stream
                .write_rpc_response(
                    "listener",
                    &response,
                    ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                )
                .await
                .expect("response");
            for bytes in [b"one".as_slice(), b"two".as_slice()] {
                stream
                    .write_raw_byte_stream_chunk(
                        "listener",
                        bytes,
                        ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                    )
                    .await
                    .expect("chunk");
            }
            stream
                .write_raw_byte_stream_chunk(
                    "listener",
                    &[],
                    ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                )
                .await
                .expect("final chunk");
        });
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert(
            super::DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_KEY.to_string(),
            super::DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_VALUE.to_string(),
        );
        metadata.insert(
            super::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY.to_string(),
            super::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE.to_string(),
        );
        let request = crate::DaemonControlRequest {
            route: "/test/raw-stream".to_string(),
            category: "test".to_string(),
            metadata,
            auth: None,
            body: Vec::new(),
        };

        let response = super::send_daemon_control_rpc_request_raw_response_async(
            &address,
            &request,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            1024,
        )
        .await
        .expect("streamed response");

        assert_eq!(response.body, b"onetwo");
        server.await.expect("server task");
    }

    fn assert_possibly_applied_overall_stream_timeout(error: NetworkError) {
        match error {
            NetworkError::Timeout {
                operation,
                phase,
                delivery_state,
                ..
            } => {
                assert_eq!(operation, "read object rpc stream body");
                assert_eq!(phase, NetworkTimeoutPhase::Overall);
                assert_eq!(delivery_state, NetworkDeliveryState::PossiblyApplied);
            }
            other => panic!("expected typed overall stream timeout, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn expired_stream_deadline_rejects_buffered_and_empty_body_reads() {
        let options = RawByteStreamChunkOptions {
            chunk_bytes: 4,
            max_total_bytes: Some(1024),
            idle_timeout: Some(Duration::from_secs(1)),
        };
        let expired = tokio::time::Instant::now()
            .checked_sub(Duration::from_millis(1))
            .expect("expired deadline");
        let mut response = crate::ObjectRpcResponse::from_encoded_body(
            "/test/buffered-deadline",
            b"body".to_vec(),
        );
        set_object_rpc_response_stream_mode(
            &mut response,
            ObjectRpcResponseStreamMode::RawStreamedDaemonBody,
        );
        let mut response = ObjectRpcStreamResponse::from_buffered_response(response, options)
            .with_deadline(Some(expired));
        let error = response
            .body
            .read_next_chunk()
            .await
            .expect_err("expired buffered body deadline");
        assert_possibly_applied_overall_stream_timeout(error);

        let mut empty = ObjectRpcStreamBody::Empty.with_deadline(Some(expired));
        let error = empty
            .read_next_chunk()
            .await
            .expect_err("expired empty body deadline");
        assert_possibly_applied_overall_stream_timeout(error);
    }

    #[tokio::test]
    async fn raw_stream_body_deadline_bounds_subsequent_chunk_read() {
        let listener = AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let server = tokio::spawn(async move {
            let frame_options = ObjectFrameIoOptions::idle_timeout(Duration::from_secs(2));
            let mut stream = listener.accept().await.expect("accept");
            stream
                .read_request("listener", 1024, frame_options)
                .await
                .expect("request");
            let mut response =
                crate::ObjectRpcResponse::from_encoded_body("/test/body-deadline", Vec::new());
            set_object_rpc_response_stream_mode(
                &mut response,
                ObjectRpcResponseStreamMode::RawStreamedDaemonBody,
            );
            stream
                .write_rpc_response("listener", &response, frame_options)
                .await
                .expect("response");
            stream
                .write_raw_byte_stream_chunk("listener", b"first", frame_options)
                .await
                .expect("first chunk");
            tokio::time::sleep(Duration::from_millis(400)).await;
        });
        let request =
            crate::ObjectRpcRequest::from_encoded_body("/test/body-deadline", "test", Vec::new());
        let mut response = send_daemon_control_rpc_request_stream_response_async(
            &address,
            &request,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(2)),
            1024,
        )
        .await
        .expect("stream response")
        .with_deadline(Some(
            tokio::time::Instant::now() + Duration::from_millis(150),
        ));

        let first = response
            .body
            .read_next_chunk()
            .await
            .expect("first chunk read")
            .expect("first chunk");
        assert_eq!(first, b"first");
        let error = response
            .body
            .read_next_chunk()
            .await
            .expect_err("whole-call deadline should beat idle timeout");
        assert_possibly_applied_overall_stream_timeout(error);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn high_level_daemon_rpc_uses_raw_streamed_response_reader() {
        let listener = AsyncDaemonControlListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr_string().expect("listener address");
        let server = tokio::spawn(async move {
            let mut stream = listener.accept().await.expect("accept");
            let request = stream
                .read_request(
                    "listener",
                    1024,
                    ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                )
                .await
                .expect("request");
            assert_eq!(request.route, "/test/high-level-raw-stream");
            assert!(super::daemon_control_request_expects_raw_rpc_response(
                &request
            ));
            let mut response = crate::ObjectRpcResponse::from_encoded_body(
                "/test/high-level-raw-stream",
                Vec::new(),
            );
            response.metadata.insert(
                super::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY.to_string(),
                super::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE.to_string(),
            );
            stream
                .write_rpc_response(
                    "listener",
                    &response,
                    ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                )
                .await
                .expect("response");
            stream
                .write_raw_byte_stream_chunk(
                    "listener",
                    b"streamed",
                    ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                )
                .await
                .expect("chunk");
            stream
                .write_raw_byte_stream_chunk(
                    "listener",
                    &[],
                    ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                )
                .await
                .expect("final chunk");
        });
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert(
            super::DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_KEY.to_string(),
            super::DAEMON_CONTROL_RAW_RPC_RESPONSE_METADATA_VALUE.to_string(),
        );
        metadata.insert(
            super::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_KEY.to_string(),
            super::DAEMON_CONTROL_STREAMED_RPC_RESPONSE_METADATA_VALUE.to_string(),
        );
        let request = crate::ObjectRpcRequest {
            route: "/test/high-level-raw-stream".to_string(),
            category: "test".to_string(),
            metadata,
            auth: None,
            body: Vec::new(),
        };

        let response = super::send_daemon_control_rpc_request_async(
            &address,
            &request,
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            crate::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
        )
        .await
        .expect("streamed response");

        assert_eq!(response.body, b"streamed");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn raw_byte_stream_chunks_can_forward_without_collecting_body() {
        let (mut source_writer, mut source_reader) = tokio::io::duplex(128);
        let (mut sink_writer, mut sink_reader) = tokio::io::duplex(128);
        let options = RawByteStreamChunkOptions {
            chunk_bytes: 8,
            max_total_bytes: Some(16),
            idle_timeout: Some(Duration::from_secs(1)),
        };

        let producer = tokio::spawn(async move {
            for bytes in [b"abcd".as_slice(), b"efgh".as_slice()] {
                write_raw_byte_stream_chunk_async(
                    &mut source_writer,
                    bytes,
                    ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                )
                .await
                .expect("write source chunk");
            }
            write_raw_byte_stream_chunk_async(
                &mut source_writer,
                &[],
                ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
            )
            .await
            .expect("write source terminator");
        });
        let forwarder = tokio::spawn(async move {
            forward_raw_byte_stream_chunks_async(&mut source_reader, &mut sink_writer, options)
                .await
                .expect("forward chunks")
        });

        let mut forwarded = Vec::new();
        let read = read_raw_byte_stream_chunks_to_vec_async(
            &mut sink_reader,
            &mut forwarded,
            RawByteStreamChunkOptions {
                chunk_bytes: 8,
                max_total_bytes: Some(16),
                idle_timeout: Some(Duration::from_secs(1)),
            },
        )
        .await
        .expect("read forwarded chunks");

        producer.await.expect("producer");
        let forwarded_total = forwarder.await.expect("forwarder");
        assert_eq!(forwarded_total, 8);
        assert_eq!(read, 8);
        assert_eq!(forwarded, b"abcdefgh");
    }

    #[tokio::test]
    async fn raw_byte_stream_reader_rejects_oversized_chunk_before_body_read() {
        let mut frame = 9_u64.to_be_bytes().to_vec();
        frame.extend_from_slice(b"ignored body");
        let mut cursor = Cursor::new(frame);
        let mut output = Vec::new();

        let error = read_raw_byte_stream_chunks_to_vec_async(
            &mut cursor,
            &mut output,
            RawByteStreamChunkOptions {
                chunk_bytes: 8,
                max_total_bytes: None,
                idle_timeout: None,
            },
        )
        .await
        .expect_err("oversized raw stream chunk");

        assert_eq!(
            error,
            NetworkError::FrameTooLarge {
                actual_bytes: 9,
                max_bytes: 8,
            }
        );
        assert_eq!(
            cursor.position(),
            super::DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES as u64,
            "oversized chunk rejection should happen before reading the body"
        );
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn raw_byte_stream_forwarder_rejects_oversized_chunk_before_body_read() {
        let mut frame = 9_u64.to_be_bytes().to_vec();
        frame.extend_from_slice(b"ignored body");
        let mut source = Cursor::new(frame);
        let mut sink = tokio::io::sink();

        let error = forward_raw_byte_stream_chunks_async(
            &mut source,
            &mut sink,
            RawByteStreamChunkOptions {
                chunk_bytes: 8,
                max_total_bytes: None,
                idle_timeout: None,
            },
        )
        .await
        .expect_err("oversized raw stream chunk");

        assert_eq!(
            error,
            NetworkError::FrameTooLarge {
                actual_bytes: 9,
                max_bytes: 8,
            }
        );
        assert_eq!(
            source.position(),
            super::DAEMON_CONTROL_RAW_BYTE_STREAM_CHUNK_HEADER_BYTES as u64,
            "oversized forwarded chunk rejection should happen before reading the body"
        );
    }

    #[test]
    fn object_rpc_stream_modes_round_trip_metadata() {
        let mut request =
            crate::ObjectRpcRequest::from_encoded_body("/test/stream-mode", "test", Vec::new());
        assert_eq!(
            object_rpc_request_stream_mode(&request),
            ObjectRpcRequestStreamMode::Buffered
        );

        set_object_rpc_request_stream_mode(
            &mut request,
            ObjectRpcRequestStreamMode::ReuseDaemonStream,
        );
        assert_eq!(
            object_rpc_request_stream_mode(&request),
            ObjectRpcRequestStreamMode::ReuseDaemonStream
        );
        assert!(object_rpc_request_reuses_daemon_stream(&request));
        assert!(!object_rpc_request_expects_raw_daemon_response(&request));

        set_object_rpc_request_stream_mode(
            &mut request,
            ObjectRpcRequestStreamMode::RawStreamedDaemonResponse,
        );
        assert_eq!(
            object_rpc_request_stream_mode(&request),
            ObjectRpcRequestStreamMode::RawStreamedDaemonResponse
        );
        assert!(object_rpc_request_reuses_daemon_stream(&request));
        assert!(object_rpc_request_expects_raw_daemon_response(&request));
        assert!(object_rpc_request_accepts_streamed_daemon_response(
            &request
        ));

        set_object_rpc_request_stream_mode(&mut request, ObjectRpcRequestStreamMode::Buffered);
        assert_eq!(
            object_rpc_request_stream_mode(&request),
            ObjectRpcRequestStreamMode::Buffered
        );
        assert!(request.metadata.is_empty());

        let mut response =
            crate::ObjectRpcResponse::from_encoded_body("/test/stream-mode", Vec::new());
        assert_eq!(
            object_rpc_response_stream_mode(&response),
            ObjectRpcResponseStreamMode::Buffered
        );
        set_object_rpc_response_stream_mode(
            &mut response,
            ObjectRpcResponseStreamMode::RawStreamedDaemonBody,
        );
        assert_eq!(
            object_rpc_response_stream_mode(&response),
            ObjectRpcResponseStreamMode::RawStreamedDaemonBody
        );
        set_object_rpc_response_stream_mode(&mut response, ObjectRpcResponseStreamMode::Buffered);
        assert_eq!(
            object_rpc_response_stream_mode(&response),
            ObjectRpcResponseStreamMode::Buffered
        );
        assert!(response.metadata.is_empty());
    }

    fn slice_is_borrowed_from(slice: &[u8], source: &[u8]) -> bool {
        let source_start = source.as_ptr() as usize;
        let source_end = source_start + source.len();
        let slice_start = slice.as_ptr() as usize;
        let slice_end = slice_start + slice.len();
        slice_start >= source_start && slice_end <= source_end
    }
}
