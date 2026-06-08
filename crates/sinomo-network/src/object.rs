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

    pub fn read_timeout(&self) -> Result<Option<Duration>, NetworkError> {
        self.stream
            .read_timeout()
            .map_err(|source| NetworkError::Io {
                operation: "read daemon control read timeout".to_string(),
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

    pub fn write_timeout(&self) -> Result<Option<Duration>, NetworkError> {
        self.stream
            .write_timeout()
            .map_err(|source| NetworkError::Io {
                operation: "read daemon control write timeout".to_string(),
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
    read_exact_blocking_frame(reader, &mut len_bytes, "read object frame length", "length")?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > max_bytes {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: len,
            max_bytes,
        });
    }
    let mut bytes = vec![0_u8; len];
    read_exact_blocking_frame(reader, &mut bytes, "read object frame body", "body")?;
    if let Some(observer) = crate::io_observer() {
        observer.on_net_recv(4 + bytes.len() as u64);
    }
    SinomoObjectCodec::default().decode(&bytes)
}

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

pub async fn write_object_frame_async_with_idle_timeout<W, T>(
    writer: &mut W,
    value: &T,
    idle_timeout: Duration,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
    T: serde::Serialize + ?Sized,
{
    let bytes = SinomoObjectCodec::default().encode(value)?;
    let len = u32::try_from(bytes.len()).map_err(|_| NetworkError::FrameTooLarge {
        actual_bytes: bytes.len(),
        max_bytes: u32::MAX as usize,
    })?;
    write_all_async_frame_with_idle_timeout(
        writer,
        &len.to_be_bytes(),
        idle_timeout,
        "write async object frame",
        "length",
    )
    .await?;
    write_all_async_frame_with_idle_timeout(
        writer,
        &bytes,
        idle_timeout,
        "write async object frame",
        "body",
    )
    .await?;
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
    read_exact_async_frame(
        reader,
        &mut len_bytes,
        "read async object frame length",
        "length",
    )
    .await?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > max_bytes {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: len,
            max_bytes,
        });
    }
    let mut bytes = vec![0_u8; len];
    read_exact_async_frame(reader, &mut bytes, "read async object frame body", "body").await?;
    if let Some(observer) = crate::io_observer() {
        observer.on_net_recv(4 + bytes.len() as u64);
    }
    SinomoObjectCodec::default().decode(&bytes)
}

pub async fn read_object_frame_async_with_idle_timeout<R, T>(
    reader: &mut R,
    max_bytes: usize,
    idle_timeout: Duration,
) -> Result<T, NetworkError>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    let mut len_bytes = [0_u8; 4];
    read_exact_async_frame_with_idle_timeout(
        reader,
        &mut len_bytes,
        idle_timeout,
        "read async object frame length",
        "length",
    )
    .await?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > max_bytes {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: len,
            max_bytes,
        });
    }
    let mut bytes = vec![0_u8; len];
    read_exact_async_frame_with_idle_timeout(
        reader,
        &mut bytes,
        idle_timeout,
        "read async object frame body",
        "body",
    )
    .await?;
    if let Some(observer) = crate::io_observer() {
        observer.on_net_recv(4 + bytes.len() as u64);
    }
    SinomoObjectCodec::default().decode(&bytes)
}

async fn write_all_async_frame_with_idle_timeout<W: AsyncWrite + Unpin>(
    writer: &mut W,
    buf: &[u8],
    idle_timeout: Duration,
    operation: &'static str,
    part: &'static str,
) -> Result<(), NetworkError> {
    let expected = buf.len();
    let mut written = 0_usize;
    while written < expected {
        match tokio::time::timeout(idle_timeout, writer.write(&buf[written..])).await {
            Ok(Ok(0)) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: format!(
                        "peer stopped accepting object frame {part}: wrote {written} of {expected} bytes"
                    ),
                });
            }
            Ok(Ok(n)) => written = written.saturating_add(n),
            Ok(Err(source)) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Ok(Err(source)) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: source.to_string(),
                });
            }
            Err(_) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: format!(
                        "timed out after {}ms without {part} write progress",
                        idle_timeout.as_millis()
                    ),
                });
            }
        }
    }
    writer.flush().await.map_err(|source| NetworkError::Io {
        operation: operation.to_string(),
        detail: source.to_string(),
    })?;
    Ok(())
}

async fn read_exact_async_frame_with_idle_timeout<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
    idle_timeout: Duration,
    operation: &'static str,
    part: &'static str,
) -> Result<(), NetworkError> {
    let expected = buf.len();
    let mut read = 0_usize;
    while read < expected {
        match tokio::time::timeout(idle_timeout, reader.read(&mut buf[read..])).await {
            Ok(Ok(0)) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: object_frame_eof_detail(part, read, expected),
                });
            }
            Ok(Ok(n)) => read = read.saturating_add(n),
            Ok(Err(source)) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Ok(Err(source)) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: source.to_string(),
                });
            }
            Err(_) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: format!(
                        "timed out after {}ms without {part} read progress",
                        idle_timeout.as_millis()
                    ),
                });
            }
        }
    }
    Ok(())
}

async fn read_exact_async_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
    operation: &'static str,
    part: &'static str,
) -> Result<(), NetworkError> {
    let expected = buf.len();
    let mut read = 0_usize;
    while read < expected {
        match reader.read(&mut buf[read..]).await {
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
    use super::{SinomoObjectCodec, read_object_frame_async, read_object_frame_blocking};
    use crate::error::NetworkError;
    use serde::{Deserialize, Serialize};
    use std::io::Cursor;
    use tokio::io::AsyncWriteExt;

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

    #[tokio::test]
    async fn async_object_frame_reports_partial_body() {
        let (mut client, mut server) = tokio::io::duplex(64);
        server
            .write_all(&8_u32.to_be_bytes())
            .await
            .expect("write length");
        server.write_all(b"abc").await.expect("write partial body");
        drop(server);

        let error = read_object_frame_async::<_, BundleRecord>(&mut client, 1024)
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
}
