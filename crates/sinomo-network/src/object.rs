use crate::error::NetworkError;
use sinomo_object::{LengthMode, serde_ext};
#[cfg(test)]
use std::io::{Read, Write};
#[cfg(test)]
use std::net::TcpStream;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const SINOMO_NETWORK_OBJECT_VERSION: u16 = 1;
pub const OBJECT_STREAM_DEFAULT_CHUNK_BYTES: usize = 1024 * 1024;
pub const OBJECT_STREAM_MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;
pub const OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL: u32 = u32::MAX;
#[cfg(target_pointer_width = "64")]
pub const DAEMON_CONTROL_RPC_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024 * 1024;
#[cfg(not(target_pointer_width = "64"))]
pub const DAEMON_CONTROL_RPC_MAX_FRAME_BYTES: usize = usize::MAX;
const OBJECT_STREAM_CHUNK_FRAME_OVERHEAD_BYTES: usize = 4096;

/// Timeout policy for daemon object-RPC transport.
///
/// Connect/open timeouts bound cases where no usable transport can be established.
/// Idle timeouts are optional transfer-progress limits. Callers that do not pass an
/// idle timeout get normal async frame I/O with no application-level body deadline.
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
        Self::none()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectFrameIoOptions {
    pub idle_timeout: Option<Duration>,
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

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DaemonControlRequest {
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc: Option<crate::ObjectRpcRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "serde_bytes")]
    pub auth: Option<Vec<u8>>,
}

impl DaemonControlRequest {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            rpc: None,
            auth: None,
        }
    }

    pub fn rpc(command: impl Into<String>, rpc: crate::ObjectRpcRequest) -> Self {
        Self {
            command: command.into(),
            rpc: Some(rpc),
            auth: None,
        }
    }

    pub fn authenticated_rpc(
        command: impl Into<String>,
        rpc: crate::ObjectRpcRequest,
        auth: Vec<u8>,
    ) -> Self {
        Self {
            command: command.into(),
            rpc: Some(rpc),
            auth: Some(auth),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DaemonControlResponse {
    pub body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc: Option<crate::ObjectRpcResponse>,
}

impl DaemonControlResponse {
    pub fn new(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            rpc: None,
        }
    }

    pub fn rpc(body: impl Into<String>, rpc: crate::ObjectRpcResponse) -> Self {
        Self {
            body: body.into(),
            rpc: Some(rpc),
        }
    }
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
            .map(|(stream, _address)| AsyncDaemonControlStream::new(stream))
            .map_err(|source| NetworkError::Io {
                operation: format!("accept async daemon control listener {}", self.bind_address),
                detail: source.to_string(),
            })
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct DaemonControlStream {
    stream: TcpStream,
}

#[cfg(test)]
impl DaemonControlStream {
    pub(crate) fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub(crate) fn set_read_timeout(&self, timeout: Option<Duration>) -> Result<(), NetworkError> {
        self.stream
            .set_read_timeout(timeout)
            .map_err(|source| NetworkError::Io {
                operation: "configure daemon control read timeout".to_string(),
                detail: source.to_string(),
            })
    }

    pub(crate) fn set_write_timeout(&self, timeout: Option<Duration>) -> Result<(), NetworkError> {
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

    pub async fn connect(address: &str) -> Result<Self, NetworkError> {
        Self::connect_with_options(address, DaemonRpcTimeouts::none()).await
    }

    pub async fn connect_with_options(
        address: &str,
        timeouts: DaemonRpcTimeouts,
    ) -> Result<Self, NetworkError> {
        let connect = tokio::net::TcpStream::connect(address);
        let stream = if let Some(timeout) = timeouts.connect_timeout {
            tokio::time::timeout(timeout, connect)
                .await
                .map_err(|_| NetworkError::Io {
                    operation: format!("connect async daemon control stream {address}"),
                    detail: daemon_control_timeout_detail(timeout),
                })?
        } else {
            connect.await
        }
        .map_err(|source| NetworkError::Io {
            operation: format!("connect async daemon control stream {address}"),
            detail: source.to_string(),
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

    pub async fn write_response(
        &mut self,
        address: &str,
        response: &DaemonControlResponse,
        options: ObjectFrameIoOptions,
    ) -> Result<(), NetworkError> {
        self.write_frame(address, response, options).await
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

pub async fn send_daemon_control_request_async(
    address: &str,
    request: &DaemonControlRequest,
    timeouts: DaemonRpcTimeouts,
    max_response_bytes: usize,
) -> Result<DaemonControlResponse, NetworkError> {
    let mut stream = AsyncDaemonControlStream::connect_with_options(address, timeouts).await?;
    let frame_options = ObjectFrameIoOptions::none().with_idle_timeout(timeouts.idle_timeout);
    stream
        .write_request(address, request, frame_options)
        .await?;
    stream
        .read_response(address, max_response_bytes, frame_options)
        .await
}

fn daemon_control_timeout_detail(timeout: Duration) -> String {
    format!("timed out after {}ms", timeout.as_millis())
}

fn map_daemon_control_frame_error(error: NetworkError, operation: String) -> NetworkError {
    match error {
        NetworkError::Io { detail, .. } => NetworkError::Io { operation, detail },
        other => other,
    }
}

#[cfg(test)]
impl Read for DaemonControlStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buffer)
    }
}

#[cfg(test)]
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

    pub fn decode<T: serde::de::DeserializeOwned>(self, bytes: &[u8]) -> Result<T, NetworkError> {
        serde_ext::from_bytes(bytes).map_err(NetworkError::object)
    }
}

pub(crate) fn object_frame_len_prefix(len: usize) -> Result<Vec<u8>, NetworkError> {
    let len = u64::try_from(len).map_err(|_| NetworkError::FrameTooLarge {
        actual_bytes: usize::MAX,
        max_bytes: usize::MAX,
    })?;
    let extended = len >= u64::from(OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL);
    let mut prefix = Vec::with_capacity(if extended { 12 } else { 4 });
    if extended {
        prefix.extend_from_slice(&OBJECT_FRAME_EXTENDED_LENGTH_SENTINEL.to_be_bytes());
        prefix.extend_from_slice(&len.to_be_bytes());
    } else {
        prefix.extend_from_slice(&(len as u32).to_be_bytes());
    }
    Ok(prefix)
}

#[cfg(any(feature = "iroh-transport", test))]
pub(crate) fn object_frame_total_bytes(
    prefix_bytes: usize,
    body_bytes: usize,
) -> Result<usize, NetworkError> {
    prefix_bytes
        .checked_add(body_bytes)
        .ok_or(NetworkError::FrameTooLarge {
            actual_bytes: usize::MAX,
            max_bytes: usize::MAX,
        })
}

pub fn encode_object_frame_to_vec<T>(value: &T) -> Result<Vec<u8>, NetworkError>
where
    T: serde::Serialize + ?Sized,
{
    let bytes = SinomoObjectCodec::network_frame().encode(value)?;
    let len_prefix = object_frame_len_prefix(bytes.len())?;
    let frame_bytes =
        len_prefix
            .len()
            .checked_add(bytes.len())
            .ok_or(NetworkError::FrameTooLarge {
                actual_bytes: usize::MAX,
                max_bytes: usize::MAX,
            })?;
    let mut frame = Vec::with_capacity(frame_bytes);
    frame.extend_from_slice(&len_prefix);
    frame.extend_from_slice(&bytes);
    Ok(frame)
}

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

#[cfg(test)]
pub(crate) fn write_object_frame_blocking<W, T>(
    writer: &mut W,
    value: &T,
) -> Result<(), NetworkError>
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
    if let Some(observer) = crate::io_observer() {
        observer.on_net_sent(len_prefix.len() as u64 + bytes.len() as u64);
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn read_object_frame_blocking<R, T>(
    reader: &mut R,
    max_bytes: usize,
) -> Result<T, NetworkError>
where
    R: Read,
    T: serde::de::DeserializeOwned,
{
    let (len, prefix_bytes) = read_object_frame_len_blocking(reader, max_bytes)?;
    let mut bytes = vec![0_u8; len];
    read_exact_blocking_frame(reader, &mut bytes, "read object frame body", "body")?;
    if let Some(observer) = crate::io_observer() {
        observer.on_net_recv(prefix_bytes as u64 + bytes.len() as u64);
    }
    SinomoObjectCodec::network_frame().decode(&bytes)
}

#[cfg(test)]
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

#[cfg(test)]
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
        &len_prefix,
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
    if let Some(observer) = crate::io_observer() {
        observer.on_net_sent(len_prefix.len() as u64 + bytes.len() as u64);
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
    let (len, prefix_bytes) =
        read_object_frame_len_async(reader, max_bytes, options.idle_timeout).await?;
    let mut bytes = vec![0_u8; len];
    read_exact_async_frame(
        reader,
        &mut bytes,
        options.idle_timeout,
        "read async object frame body",
        "body",
    )
    .await?;
    if let Some(observer) = crate::io_observer() {
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
pub struct ObjectStreamChunkOptions {
    pub chunk_bytes: usize,
    pub max_total_bytes: Option<u64>,
    pub idle_timeout: Option<Duration>,
}

impl Default for ObjectStreamChunkOptions {
    fn default() -> Self {
        Self {
            chunk_bytes: OBJECT_STREAM_DEFAULT_CHUNK_BYTES,
            max_total_bytes: None,
            idle_timeout: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ObjectStreamChunk {
    pub sequence: u64,
    pub final_chunk: bool,
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

pub async fn write_object_stream_chunks_async<R, W>(
    reader: &mut R,
    writer: &mut W,
    options: ObjectStreamChunkOptions,
) -> Result<u64, NetworkError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let chunk_bytes = validate_object_stream_chunk_options(options)?;
    let mut buffer = vec![0_u8; chunk_bytes];
    let mut sequence = 0_u64;
    let mut total = 0_u64;
    loop {
        let read =
            read_object_stream_source_chunk(reader, &mut buffer, options.idle_timeout).await?;
        if read == 0 {
            write_object_stream_chunk(
                writer,
                &ObjectStreamChunk {
                    sequence,
                    final_chunk: true,
                    bytes: Vec::new(),
                },
                options.idle_timeout,
            )
            .await?;
            return Ok(total);
        }
        total = checked_object_stream_total(total, read, options.max_total_bytes)?;
        write_object_stream_chunk(
            writer,
            &ObjectStreamChunk {
                sequence,
                final_chunk: false,
                bytes: buffer[..read].to_vec(),
            },
            options.idle_timeout,
        )
        .await?;
        sequence = sequence.saturating_add(1);
    }
}

pub async fn read_object_stream_chunks_async<R, W>(
    reader: &mut R,
    writer: &mut W,
    options: ObjectStreamChunkOptions,
) -> Result<u64, NetworkError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let chunk_bytes = validate_object_stream_chunk_options(options)?;
    let max_frame_bytes = chunk_bytes.saturating_add(OBJECT_STREAM_CHUNK_FRAME_OVERHEAD_BYTES);
    let mut expected_sequence = 0_u64;
    let mut total = 0_u64;
    loop {
        let chunk: ObjectStreamChunk =
            read_object_stream_chunk(reader, max_frame_bytes, options.idle_timeout).await?;
        if chunk.sequence != expected_sequence {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "object stream chunk sequence mismatch: expected {expected_sequence}, got {}",
                    chunk.sequence
                ),
            });
        }
        if chunk.bytes.len() > chunk_bytes {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: chunk.bytes.len(),
                max_bytes: chunk_bytes,
            });
        }
        total = checked_object_stream_total(total, chunk.bytes.len(), options.max_total_bytes)?;
        write_object_stream_sink_chunk(writer, &chunk.bytes, options.idle_timeout).await?;
        if chunk.final_chunk {
            flush_object_stream_sink(writer, options.idle_timeout).await?;
            return Ok(total);
        }
        expected_sequence = expected_sequence.saturating_add(1);
    }
}

fn validate_object_stream_chunk_options(
    options: ObjectStreamChunkOptions,
) -> Result<usize, NetworkError> {
    if options.chunk_bytes == 0 {
        return Err(NetworkError::Serialization {
            detail: "object stream chunk size must be greater than zero".to_string(),
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

async fn read_object_stream_source_chunk<R: AsyncRead + Unpin>(
    reader: &mut R,
    buffer: &mut [u8],
    idle_timeout: Option<Duration>,
) -> Result<usize, NetworkError> {
    let read = if let Some(timeout) = idle_timeout {
        tokio::time::timeout(timeout, reader.read(buffer))
            .await
            .map_err(|_| NetworkError::Io {
                operation: "read object stream source chunk".to_string(),
                detail: format!("timed out after {}ms", timeout.as_millis()),
            })?
    } else {
        reader.read(buffer).await
    }
    .map_err(|source| NetworkError::Io {
        operation: "read object stream source chunk".to_string(),
        detail: source.to_string(),
    })?;
    Ok(read)
}

async fn write_object_stream_chunk<W: AsyncWrite + Unpin>(
    writer: &mut W,
    chunk: &ObjectStreamChunk,
    idle_timeout: Option<Duration>,
) -> Result<(), NetworkError> {
    write_object_frame_async(
        writer,
        chunk,
        ObjectFrameIoOptions::none().with_idle_timeout(idle_timeout),
    )
    .await
}

async fn read_object_stream_chunk<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_frame_bytes: usize,
    idle_timeout: Option<Duration>,
) -> Result<ObjectStreamChunk, NetworkError> {
    read_object_frame_async(
        reader,
        max_frame_bytes,
        ObjectFrameIoOptions::none().with_idle_timeout(idle_timeout),
    )
    .await
}

async fn write_object_stream_sink_chunk<W: AsyncWrite + Unpin>(
    writer: &mut W,
    bytes: &[u8],
    idle_timeout: Option<Duration>,
) -> Result<(), NetworkError> {
    if bytes.is_empty() {
        return Ok(());
    }
    write_all_async_frame(
        writer,
        bytes,
        idle_timeout,
        "write object stream sink chunk",
        "body",
    )
    .await
}

async fn flush_object_stream_sink<W: AsyncWrite + Unpin>(
    writer: &mut W,
    idle_timeout: Option<Duration>,
) -> Result<(), NetworkError> {
    if let Some(timeout) = idle_timeout {
        tokio::time::timeout(timeout, writer.flush())
            .await
            .map_err(|_| NetworkError::Io {
                operation: "flush object stream sink".to_string(),
                detail: format!("timed out after {}ms", timeout.as_millis()),
            })?
    } else {
        writer.flush().await
    }
    .map_err(|source| NetworkError::Io {
        operation: "flush object stream sink".to_string(),
        detail: source.to_string(),
    })
}

pub(crate) async fn write_all_async_frame<W: AsyncWrite + Unpin>(
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
        AsyncDaemonControlListener, DaemonControlRequest, DaemonControlResponse, DaemonRpcTimeouts,
        ObjectFrameIoOptions, ObjectStreamChunk, ObjectStreamChunkOptions, SinomoObjectCodec,
        decode_single_object_frame_from_slice, encode_object_frame_to_vec, read_object_frame_async,
        read_object_frame_blocking, read_object_stream_chunks_async,
        send_daemon_control_request_async, write_all_async_frame, write_object_frame_async,
        write_object_frame_blocking, write_object_stream_chunks_async,
    };
    use crate::error::NetworkError;
    use serde::{Deserialize, Serialize};
    use sinomo_object::{DocumentView, LengthMode};
    use std::io::Cursor;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct BundleRecord {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none", with = "serde_bytes")]
        payload: Option<Vec<u8>>,
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
    async fn async_object_stream_chunks_round_trip_without_single_large_frame() {
        let payload: Vec<u8> = (0..=255u8).cycle().take(4099).collect();
        let options = ObjectStreamChunkOptions {
            chunk_bytes: 257,
            max_total_bytes: Some(payload.len() as u64),
            idle_timeout: None,
        };
        let mut source = Cursor::new(payload.clone());
        let mut wire = Vec::new();

        let written = write_object_stream_chunks_async(&mut source, &mut wire, options)
            .await
            .expect("write chunks");

        let mut received = Vec::new();
        let mut wire_reader = Cursor::new(wire);
        let read = read_object_stream_chunks_async(&mut wire_reader, &mut received, options)
            .await
            .expect("read chunks");

        assert_eq!(written, payload.len() as u64);
        assert_eq!(read, payload.len() as u64);
        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn async_object_stream_chunks_enforce_total_limit() {
        let payload: Vec<u8> = (0..32_u8).collect();
        let options = ObjectStreamChunkOptions {
            chunk_bytes: 8,
            max_total_bytes: Some(16),
            idle_timeout: None,
        };
        let mut source = Cursor::new(payload);
        let mut wire = Vec::new();

        let error = write_object_stream_chunks_async(&mut source, &mut wire, options)
            .await
            .expect_err("stream should exceed total limit");

        assert_eq!(
            error,
            NetworkError::FrameTooLarge {
                actual_bytes: 24,
                max_bytes: 16,
            }
        );
    }

    #[tokio::test]
    async fn async_object_stream_chunks_reject_sequence_gap() {
        let mut wire = Vec::new();
        write_object_frame_async(
            &mut wire,
            &ObjectStreamChunk {
                sequence: 1,
                final_chunk: true,
                bytes: b"bad".to_vec(),
            },
            ObjectFrameIoOptions::none(),
        )
        .await
        .expect("write chunk");
        let mut wire_reader = Cursor::new(wire);
        let mut received = Vec::new();

        let error = read_object_stream_chunks_async(
            &mut wire_reader,
            &mut received,
            ObjectStreamChunkOptions {
                chunk_bytes: 64,
                max_total_bytes: None,
                idle_timeout: None,
            },
        )
        .await
        .expect_err("sequence gap");

        assert_eq!(
            error,
            NetworkError::Serialization {
                detail: "object stream chunk sequence mismatch: expected 0, got 1".to_string(),
            }
        );
        assert!(received.is_empty());
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
            assert_eq!(request.command, "ping");
            stream
                .write_response(
                    "listener",
                    &DaemonControlResponse::new("pong"),
                    ObjectFrameIoOptions::idle_timeout(Duration::from_secs(1)),
                )
                .await
                .expect("response");
        });

        let response = send_daemon_control_request_async(
            &address,
            &DaemonControlRequest::new("ping"),
            DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(1)),
            1024,
        )
        .await
        .expect("control response");

        assert_eq!(response.body, "pong");
        server.await.expect("server task");
    }
}
