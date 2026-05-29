use crate::error::NetworkError;
use sinomo_object::{LengthMode, serde_ext};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const SINOMO_NETWORK_OBJECT_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DaemonControlRequest {
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc: Option<crate::ObjectRpcRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
pub struct DaemonControlListener {
    bind_address: String,
    listener: TcpListener,
}

impl DaemonControlListener {
    pub fn bind(bind_address: impl Into<String>) -> Result<Self, NetworkError> {
        let bind_address = bind_address.into();
        let listener = TcpListener::bind(&bind_address).map_err(|source| NetworkError::Io {
            operation: format!("bind daemon control listener {bind_address}"),
            detail: source.to_string(),
        })?;
        Ok(Self {
            bind_address,
            listener,
        })
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> Result<(), NetworkError> {
        self.listener
            .set_nonblocking(nonblocking)
            .map_err(|source| NetworkError::Io {
                operation: format!("configure daemon control listener {}", self.bind_address),
                detail: source.to_string(),
            })
    }

    pub fn local_addr_string(&self) -> Result<String, NetworkError> {
        self.listener
            .local_addr()
            .map(|address| address.to_string())
            .map_err(|source| NetworkError::Io {
                operation: format!("read daemon control listener address {}", self.bind_address),
                detail: source.to_string(),
            })
    }

    pub fn accept(&self) -> Result<Option<DaemonControlStream>, NetworkError> {
        match self.listener.accept() {
            Ok((stream, _address)) => Ok(Some(DaemonControlStream::new(stream))),
            Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(source) => Err(NetworkError::Io {
                operation: format!("accept daemon control listener {}", self.bind_address),
                detail: source.to_string(),
            }),
        }
    }
}

#[derive(Debug)]
pub struct DaemonControlStream {
    stream: TcpStream,
}

impl DaemonControlStream {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub fn connect(address: &str) -> Result<Self, NetworkError> {
        TcpStream::connect(address)
            .map(Self::new)
            .map_err(|source| NetworkError::Io {
                operation: format!("connect daemon control stream {address}"),
                detail: source.to_string(),
            })
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> Result<(), NetworkError> {
        self.stream
            .set_nonblocking(nonblocking)
            .map_err(|source| NetworkError::Io {
                operation: "configure daemon control stream".to_string(),
                detail: source.to_string(),
            })
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

    pub fn shutdown_both(&self) {
        let _ = self.stream.shutdown(Shutdown::Both);
    }

    pub fn shutdown_write(&self) {
        let _ = self.stream.shutdown(Shutdown::Write);
    }
}

impl Read for DaemonControlStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buffer)
    }
}

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
    pub fn encode<T: serde::Serialize + ?Sized>(self, value: &T) -> Result<Vec<u8>, NetworkError> {
        serde_ext::to_bytes(value, self.version_id, self.length_mode).map_err(NetworkError::object)
    }

    pub fn decode<T: serde::de::DeserializeOwned>(self, bytes: &[u8]) -> Result<T, NetworkError> {
        serde_ext::from_bytes(bytes).map_err(NetworkError::object)
    }
}

pub fn write_object_frame_blocking<W, T>(writer: &mut W, value: &T) -> Result<(), NetworkError>
where
    W: Write,
    T: serde::Serialize + ?Sized,
{
    let bytes = SinomoObjectCodec::default().encode(value)?;
    let len = u32::try_from(bytes.len()).map_err(|_| NetworkError::FrameTooLarge {
        actual_bytes: bytes.len(),
        max_bytes: u32::MAX as usize,
    })?;
    writer
        .write_all(&len.to_be_bytes())
        .and_then(|()| writer.write_all(&bytes))
        .map_err(|source| NetworkError::Io {
            operation: "write object frame".to_string(),
            detail: source.to_string(),
        })?;
    if let Some(observer) = crate::io_observer() {
        observer.on_net_sent(4 + bytes.len() as u64);
    }
    Ok(())
}

pub fn read_object_frame_blocking<R, T>(reader: &mut R, max_bytes: usize) -> Result<T, NetworkError>
where
    R: Read,
    T: serde::de::DeserializeOwned,
{
    let mut len_bytes = [0_u8; 4];
    reader
        .read_exact(&mut len_bytes)
        .map_err(|source| NetworkError::Io {
            operation: "read object frame length".to_string(),
            detail: source.to_string(),
        })?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > max_bytes {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: len,
            max_bytes,
        });
    }
    let mut bytes = vec![0_u8; len];
    reader
        .read_exact(&mut bytes)
        .map_err(|source| NetworkError::Io {
            operation: "read object frame body".to_string(),
            detail: source.to_string(),
        })?;
    if let Some(observer) = crate::io_observer() {
        observer.on_net_recv(4 + bytes.len() as u64);
    }
    SinomoObjectCodec::default().decode(&bytes)
}

pub async fn write_object_frame_async<W, T>(writer: &mut W, value: &T) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
    T: serde::Serialize + ?Sized,
{
    let bytes = SinomoObjectCodec::default().encode(value)?;
    let len = u32::try_from(bytes.len()).map_err(|_| NetworkError::FrameTooLarge {
        actual_bytes: bytes.len(),
        max_bytes: u32::MAX as usize,
    })?;
    writer
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|source| NetworkError::Io {
            operation: "write async object frame".to_string(),
            detail: source.to_string(),
        })?;
    writer
        .write_all(&bytes)
        .await
        .map_err(|source| NetworkError::Io {
            operation: "write async object frame".to_string(),
            detail: source.to_string(),
        })?;
    if let Some(observer) = crate::io_observer() {
        observer.on_net_sent(4 + bytes.len() as u64);
    }
    Ok(())
}

pub async fn read_object_frame_async<R, T>(
    reader: &mut R,
    max_bytes: usize,
) -> Result<T, NetworkError>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    let mut len_bytes = [0_u8; 4];
    reader
        .read_exact(&mut len_bytes)
        .await
        .map_err(|source| NetworkError::Io {
            operation: "read async object frame length".to_string(),
            detail: source.to_string(),
        })?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > max_bytes {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: len,
            max_bytes,
        });
    }
    let mut bytes = vec![0_u8; len];
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(|source| NetworkError::Io {
            operation: "read async object frame body".to_string(),
            detail: source.to_string(),
        })?;
    if let Some(observer) = crate::io_observer() {
        observer.on_net_recv(4 + bytes.len() as u64);
    }
    SinomoObjectCodec::default().decode(&bytes)
}
