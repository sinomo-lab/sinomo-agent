//! Private physical wire representation for multiplexed channel body data.
//!
//! Control frames remain ordinary SOBJ (`SOF2`) documents. Body data uses a
//! fixed header so the carrier can validate routing and allocation bounds
//! before acquiring a pooled receive buffer, then move the resulting `Bytes`
//! directly into channel ingress.

use crate::channel::{OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES, OBJECT_RPC_CHANNEL_MAX_CALLS};
use crate::object::write_all_vectored_async_frame;
use crate::{
    MAX_OBJECT_RPC_CHANNEL_WIRE_FRAME_BYTES, NetworkError, OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES,
    ObjectFrameIoOptions, ObjectFrameReadTiming, ObjectFrameWriteTiming,
    ObjectRpcChannelBodyDirection, ObjectRpcChannelCallId, ObjectRpcChannelWireFrame,
    read_object_frame_async_with_max_bytes, write_object_frame_async,
    write_object_frame_async_timed,
};
use bytes::{Bytes, BytesMut};
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Magic identifying a fixed channel Data frame.
pub const OBJECT_RPC_CHANNEL_DATA_MAGIC: [u8; 4] = *b"SCDF";
/// Version of the fixed channel Data header.
pub const OBJECT_RPC_CHANNEL_DATA_VERSION: u16 = 1;
/// Fixed bytes preceding every raw channel Data payload.
pub const OBJECT_RPC_CHANNEL_DATA_HEADER_BYTES: usize = 32;
/// Maximum active receive-buffer memory held by one carrier.
///
/// Every admitted call may retain one quantum in the application adapter and
/// one in its isolated direct-delivery task. The final slab is reserved for
/// the physical reader, allowing it to consume the next Data frame and reach
/// following cancellation, reset, and flow-control frames even when every
/// logical body is stalled.
pub const OBJECT_RPC_CHANNEL_RECEIVE_BUFFER_BYTES: usize =
    (2 * OBJECT_RPC_CHANNEL_MAX_CALLS + 1) * OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES;
/// Released slabs retained for reuse by an otherwise idle carrier. Active
/// traffic may grow to the bound above, but idle peers do not pin that peak.
pub const OBJECT_RPC_CHANNEL_RECEIVE_BUFFER_CACHE_BYTES: usize =
    2 * OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES;

const OBJECT_FRAME_MAGIC: [u8; 4] = *b"SOF2";
const DIRECTION_REQUEST: u8 = 0;
const DIRECTION_RESPONSE: u8 = 1;
const HEADER_FLAGS_NONE: u8 = 0;

/// Fixed-slab receive pool owned by one physical peer carrier.
///
/// Each active `Bytes` retains its slab permit. `Bytes::slice` and clones keep
/// the same owner alive, so the carrier cannot reuse or replace the allocation
/// until the final downstream reference is dropped. Idle slabs remain cached,
/// but the sum of active and cached slabs never exceeds `max_bytes`.
#[derive(Debug)]
pub struct ObjectRpcChannelReceiveBufferPool {
    slots: Arc<Semaphore>,
    buffers: Mutex<Vec<BytesMut>>,
    max_slots: usize,
    max_cached_slots: usize,
}

impl ObjectRpcChannelReceiveBufferPool {
    pub fn new(max_bytes: usize) -> Result<Arc<Self>, NetworkError> {
        let max_slots = max_bytes / OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES;
        if max_slots == 0 {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "channel receive-buffer pool must hold at least one {}-byte Data slab",
                    OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES
                ),
            });
        }
        Ok(Arc::new(Self {
            slots: Arc::new(Semaphore::new(max_slots)),
            buffers: Mutex::new(Vec::new()),
            max_slots,
            max_cached_slots: max_slots.min(
                OBJECT_RPC_CHANNEL_RECEIVE_BUFFER_CACHE_BYTES
                    / OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES,
            ),
        }))
    }

    pub fn default_for_carrier() -> Arc<Self> {
        Self::new(OBJECT_RPC_CHANNEL_RECEIVE_BUFFER_BYTES)
            .expect("default channel receive-buffer pool is valid")
    }

    pub fn max_bytes(&self) -> usize {
        self.max_slots
            .saturating_mul(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES)
    }

    async fn acquire(
        self: &Arc<Self>,
        len: usize,
    ) -> Result<(BytesMut, OwnedSemaphorePermit), NetworkError> {
        let permit = Arc::clone(&self.slots).acquire_owned().await.map_err(|_| {
            NetworkError::ResourceExhausted {
                operation: "acquire channel receive buffer".to_string(),
                requested_bytes: len,
            }
        })?;
        let mut buffer = self
            .buffers
            .lock()
            .ok()
            .and_then(|mut buffers| buffers.pop())
            .unwrap_or_else(|| BytesMut::with_capacity(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES));
        buffer.clear();
        if buffer.capacity() < OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES {
            buffer.reserve(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES - buffer.capacity());
        }
        buffer.resize(len, 0);
        Ok((buffer, permit))
    }

    fn into_bytes(self: &Arc<Self>, buffer: BytesMut, permit: OwnedSemaphorePermit) -> Bytes {
        Bytes::from_owner(PooledChannelBytes {
            buffer: Some(buffer),
            pool: Arc::clone(self),
            _permit: permit,
        })
    }

    fn release(&self, mut buffer: BytesMut) {
        buffer.clear();
        let Ok(mut buffers) = self.buffers.lock() else {
            return;
        };
        if buffers.len() < self.max_cached_slots {
            buffers.push(buffer);
        }
    }

    #[cfg(test)]
    fn cached_buffers(&self) -> usize {
        self.buffers
            .lock()
            .map(|buffers| buffers.len())
            .unwrap_or_default()
    }
}

#[derive(Debug)]
struct PooledChannelBytes {
    buffer: Option<BytesMut>,
    pool: Arc<ObjectRpcChannelReceiveBufferPool>,
    _permit: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for PooledChannelBytes {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_deref().unwrap_or_default()
    }
}

impl Drop for PooledChannelBytes {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.release(buffer);
        }
    }
}

/// Allocation-free, structurally validated metadata for one raw Data payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectRpcChannelDataHeader {
    pub call_id: ObjectRpcChannelCallId,
    pub direction: ObjectRpcChannelBodyDirection,
    pub offset: u64,
    pub payload_len: u32,
}

impl ObjectRpcChannelDataHeader {
    pub fn new(
        call_id: ObjectRpcChannelCallId,
        direction: ObjectRpcChannelBodyDirection,
        offset: u64,
        payload_len: usize,
    ) -> Result<Self, NetworkError> {
        let payload_len = u32::try_from(payload_len).map_err(|_| NetworkError::FrameTooLarge {
            actual_bytes: payload_len,
            max_bytes: OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES,
        })?;
        let header = Self {
            call_id,
            direction,
            offset,
            payload_len,
        };
        header.validate_structure()?;
        Ok(header)
    }

    pub fn payload_len(self) -> usize {
        self.payload_len as usize
    }

    pub fn encode(self) -> Result<[u8; OBJECT_RPC_CHANNEL_DATA_HEADER_BYTES], NetworkError> {
        self.validate_structure()?;
        let mut bytes = [0_u8; OBJECT_RPC_CHANNEL_DATA_HEADER_BYTES];
        bytes[..4].copy_from_slice(&OBJECT_RPC_CHANNEL_DATA_MAGIC);
        bytes[4..6].copy_from_slice(&OBJECT_RPC_CHANNEL_DATA_VERSION.to_be_bytes());
        bytes[6] = encode_direction(self.direction);
        bytes[7] = HEADER_FLAGS_NONE;
        bytes[8..16].copy_from_slice(&self.call_id.0.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.offset.to_be_bytes());
        bytes[24..28].copy_from_slice(&self.payload_len.to_be_bytes());
        // bytes[28..32] are reserved and must remain zero.
        Ok(bytes)
    }

    pub fn decode(
        bytes: &[u8; OBJECT_RPC_CHANNEL_DATA_HEADER_BYTES],
    ) -> Result<Self, NetworkError> {
        if bytes[..4] != OBJECT_RPC_CHANNEL_DATA_MAGIC {
            return Err(invalid_data_header("invalid SCDF channel Data magic"));
        }
        let version = u16::from_be_bytes(bytes[4..6].try_into().expect("SCDF version"));
        if version != OBJECT_RPC_CHANNEL_DATA_VERSION {
            return Err(invalid_data_header(format!(
                "unsupported SCDF channel Data version {version}"
            )));
        }
        if bytes[7] != HEADER_FLAGS_NONE {
            return Err(invalid_data_header("SCDF channel Data flags must be zero"));
        }
        if bytes[28..32] != [0_u8; 4] {
            return Err(invalid_data_header(
                "SCDF channel Data reserved bytes must be zero",
            ));
        }
        let header = Self {
            call_id: ObjectRpcChannelCallId(u64::from_be_bytes(
                bytes[8..16].try_into().expect("SCDF call id"),
            )),
            direction: decode_direction(bytes[6])?,
            offset: u64::from_be_bytes(bytes[16..24].try_into().expect("SCDF offset")),
            payload_len: u32::from_be_bytes(bytes[24..28].try_into().expect("SCDF payload length")),
        };
        header.validate_structure()?;
        Ok(header)
    }

    fn validate_structure(self) -> Result<(), NetworkError> {
        if self.call_id.0 == 0 {
            return Err(invalid_data_header(
                "SCDF channel Data call id must be nonzero",
            ));
        }
        let payload_len = self.payload_len();
        if payload_len == 0 || payload_len > OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: payload_len,
                max_bytes: OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES,
            });
        }
        self.offset
            .checked_add(u64::from(self.payload_len))
            .ok_or_else(|| invalid_data_header("SCDF channel Data offset overflow"))?;
        Ok(())
    }
}

/// Ownership-bearing raw Data frame used only at a private carrier boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRpcChannelDataFrame {
    pub header: ObjectRpcChannelDataHeader,
    pub bytes: Bytes,
}

impl ObjectRpcChannelDataFrame {
    pub fn new(
        call_id: ObjectRpcChannelCallId,
        direction: ObjectRpcChannelBodyDirection,
        offset: u64,
        bytes: Bytes,
    ) -> Result<Self, NetworkError> {
        let header = ObjectRpcChannelDataHeader::new(call_id, direction, offset, bytes.len())?;
        Ok(Self { header, bytes })
    }

    pub fn validate(&self) -> Result<(), NetworkError> {
        self.header.validate_structure()?;
        if self.header.payload_len() != self.bytes.len() {
            return Err(invalid_data_header(format!(
                "SCDF channel Data payload length {} does not match {} bytes",
                self.header.payload_len(),
                self.bytes.len()
            )));
        }
        Ok(())
    }
}

/// One physical channel frame: SOBJ for control, fixed SCDF for Data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObjectRpcChannelTransportFrame {
    Control(ObjectRpcChannelWireFrame),
    Data(ObjectRpcChannelDataFrame),
}

/// Reads one mixed SOBJ-control/SCDF-Data carrier frame.
///
/// `validate_data_header` runs after fixed-header validation and before a
/// receive buffer is acquired. Channel ingress uses it to reject unknown calls,
/// invalid directions, offsets, or receive-window violations without spending
/// payload memory.
pub async fn read_object_rpc_channel_transport_frame_async<R, F>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
    receive_pool: &Arc<ObjectRpcChannelReceiveBufferPool>,
    validate_data_header: F,
) -> Result<ObjectRpcChannelTransportFrame, NetworkError>
where
    R: AsyncRead + Unpin,
    F: FnOnce(&ObjectRpcChannelDataHeader) -> Result<(), NetworkError>,
{
    read_object_rpc_channel_transport_frame_async_inner(
        reader,
        options,
        receive_pool,
        validate_data_header,
        false,
    )
    .await
    .map(|(frame, _timing)| frame)
}

/// Timed mixed-frame reader used by persistent carriers during a bounded
/// network timeline capture. Control frames deliberately retain their
/// existing reader and timing behavior; only fixed SCDF Data returns timing.
pub async fn read_object_rpc_channel_transport_frame_async_timed<R, F>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
    receive_pool: &Arc<ObjectRpcChannelReceiveBufferPool>,
    validate_data_header: F,
) -> Result<
    (
        ObjectRpcChannelTransportFrame,
        Option<ObjectFrameReadTiming>,
    ),
    NetworkError,
>
where
    R: AsyncRead + Unpin,
    F: FnOnce(&ObjectRpcChannelDataHeader) -> Result<(), NetworkError>,
{
    read_object_rpc_channel_transport_frame_async_inner(
        reader,
        options,
        receive_pool,
        validate_data_header,
        true,
    )
    .await
}

async fn read_object_rpc_channel_transport_frame_async_inner<R, F>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
    receive_pool: &Arc<ObjectRpcChannelReceiveBufferPool>,
    validate_data_header: F,
    time_fixed_data: bool,
) -> Result<
    (
        ObjectRpcChannelTransportFrame,
        Option<ObjectFrameReadTiming>,
    ),
    NetworkError,
>
where
    R: AsyncRead + Unpin,
    F: FnOnce(&ObjectRpcChannelDataHeader) -> Result<(), NetworkError>,
{
    let prefix = read_frame_magic(reader, options.idle_timeout).await?;
    if prefix == OBJECT_FRAME_MAGIC {
        let prefix = Cursor::new(prefix);
        let mut control_reader = prefix.chain(reader);
        let frame: ObjectRpcChannelWireFrame = read_object_frame_async_with_max_bytes(
            &mut control_reader,
            options,
            MAX_OBJECT_RPC_CHANNEL_WIRE_FRAME_BYTES,
        )
        .await?;
        return Ok((ObjectRpcChannelTransportFrame::Control(frame), None));
    }
    if prefix != OBJECT_RPC_CHANNEL_DATA_MAGIC {
        return Err(invalid_data_header(format!(
            "invalid channel carrier magic {:?}",
            String::from_utf8_lossy(&prefix)
        )));
    }

    let capture_generation = if time_fixed_data {
        crate::timeline::network_timeline_capture_generation()
    } else {
        0
    };
    let first_read_monotonic_ns = (capture_generation != 0)
        .then(crate::network_timeline_monotonic_ns)
        .unwrap_or(0);
    let mut encoded = [0_u8; OBJECT_RPC_CHANNEL_DATA_HEADER_BYTES];
    encoded[..4].copy_from_slice(&prefix);
    read_exact_with_idle(
        reader,
        &mut encoded[4..],
        options.idle_timeout,
        "read SCDF channel Data header",
        "header",
    )
    .await?;
    let header = ObjectRpcChannelDataHeader::decode(&encoded)?;
    validate_data_header(&header)?;

    let (mut buffer, permit) = receive_pool.acquire(header.payload_len()).await?;
    read_exact_with_idle(
        reader,
        &mut buffer,
        options.idle_timeout,
        "read SCDF channel Data payload",
        "payload",
    )
    .await?;
    if let Some(observer) = crate::io_observer() {
        observer.on_net_recv(
            u64::try_from(OBJECT_RPC_CHANNEL_DATA_HEADER_BYTES + buffer.len()).unwrap_or(u64::MAX),
        );
    }
    let body_read_completed_monotonic_ns = (capture_generation != 0)
        .then(crate::network_timeline_monotonic_ns)
        .unwrap_or(0);
    let decode_started_monotonic_ns = body_read_completed_monotonic_ns;
    let frame = ObjectRpcChannelDataFrame {
        header,
        bytes: receive_pool.into_bytes(buffer, permit),
    };
    // This final validation is allocation-free and keeps FRAME_DECODE ordered
    // after the complete payload read. Header structure and call/window state
    // were already validated before the receive pool was acquired.
    frame.validate()?;
    let decoded_monotonic_ns = (capture_generation != 0)
        .then(crate::network_timeline_monotonic_ns)
        .unwrap_or(0);
    let timing = (capture_generation != 0).then_some(ObjectFrameReadTiming {
        capture_generation,
        ready_monotonic_ns: 0,
        read_poll_monotonic_ns: 0,
        first_read_monotonic_ns,
        body_read_completed_monotonic_ns,
        decode_started_monotonic_ns,
        decoded_monotonic_ns,
        wire_bytes: u32::try_from(OBJECT_RPC_CHANNEL_DATA_HEADER_BYTES + frame.bytes.len())
            .unwrap_or(u32::MAX),
        first_read_bytes: u32::try_from(OBJECT_RPC_CHANNEL_DATA_MAGIC.len()).unwrap_or(u32::MAX),
    });
    Ok((ObjectRpcChannelTransportFrame::Data(frame), timing))
}

/// Writes one mixed carrier frame. SCDF Data uses one partial-write-safe
/// vectored header/payload loop; controls retain the ordinary SOBJ writer.
pub async fn write_object_rpc_channel_transport_frame_async<W>(
    writer: &mut W,
    frame: &ObjectRpcChannelTransportFrame,
    options: ObjectFrameIoOptions,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    match frame {
        ObjectRpcChannelTransportFrame::Control(frame) => {
            write_object_frame_async(writer, frame, options).await
        }
        ObjectRpcChannelTransportFrame::Data(frame) => {
            write_object_rpc_channel_data_frame_async(writer, frame, options).await
        }
    }
}

/// Timed counterpart used only for sampled physical writes.
pub async fn write_object_rpc_channel_transport_frame_async_timed<W>(
    writer: &mut W,
    frame: &ObjectRpcChannelTransportFrame,
    options: ObjectFrameIoOptions,
) -> (Result<(), NetworkError>, Option<ObjectFrameWriteTiming>)
where
    W: AsyncWrite + Unpin,
{
    match frame {
        ObjectRpcChannelTransportFrame::Control(frame) => {
            write_object_frame_async_timed(writer, frame, options).await
        }
        ObjectRpcChannelTransportFrame::Data(frame) => {
            let started = crate::network_timeline_monotonic_ns();
            let encoded = crate::network_timeline_monotonic_ns();
            let result = write_object_rpc_channel_data_frame_async(writer, frame, options).await;
            let completed = crate::network_timeline_monotonic_ns();
            let wire_bytes = OBJECT_RPC_CHANNEL_DATA_HEADER_BYTES.saturating_add(frame.bytes.len());
            (
                result,
                Some(ObjectFrameWriteTiming {
                    encode_started_monotonic_ns: started,
                    encode_completed_monotonic_ns: encoded,
                    socket_write_completed_monotonic_ns: completed,
                    wire_bytes: u32::try_from(wire_bytes).unwrap_or(u32::MAX),
                }),
            )
        }
    }
}

pub async fn write_object_rpc_channel_data_frame_async<W>(
    writer: &mut W,
    frame: &ObjectRpcChannelDataFrame,
    options: ObjectFrameIoOptions,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    frame.validate()?;
    let header = frame.header.encode()?;
    write_all_vectored_async_frame(
        writer,
        &header,
        &frame.bytes,
        options.idle_timeout,
        "write SCDF channel Data frame",
    )
    .await?;
    if let Some(observer) = crate::io_observer() {
        observer.on_net_sent(
            u64::try_from(OBJECT_RPC_CHANNEL_DATA_HEADER_BYTES + frame.bytes.len())
                .unwrap_or(u64::MAX),
        );
    }
    Ok(())
}

fn encode_direction(direction: ObjectRpcChannelBodyDirection) -> u8 {
    match direction {
        ObjectRpcChannelBodyDirection::Request => DIRECTION_REQUEST,
        ObjectRpcChannelBodyDirection::Response => DIRECTION_RESPONSE,
    }
}

fn decode_direction(direction: u8) -> Result<ObjectRpcChannelBodyDirection, NetworkError> {
    match direction {
        DIRECTION_REQUEST => Ok(ObjectRpcChannelBodyDirection::Request),
        DIRECTION_RESPONSE => Ok(ObjectRpcChannelBodyDirection::Response),
        _ => Err(invalid_data_header(format!(
            "invalid SCDF channel Data direction {direction}"
        ))),
    }
}

async fn read_frame_magic<R: AsyncRead + Unpin>(
    reader: &mut R,
    idle_timeout: Option<Duration>,
) -> Result<[u8; 4], NetworkError> {
    let mut magic = [0_u8; 4];
    let first = loop {
        match reader.read(&mut magic).await {
            Ok(0) => {
                return Err(NetworkError::Io {
                    operation: "read channel carrier magic".to_string(),
                    detail: "peer closed before the next channel frame".to_string(),
                });
            }
            Ok(bytes) => break bytes,
            Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(NetworkError::Io {
                    operation: "read channel carrier magic".to_string(),
                    detail: source.to_string(),
                });
            }
        }
    };
    read_exact_with_idle(
        reader,
        &mut magic[first..],
        idle_timeout,
        "read channel carrier magic",
        "magic",
    )
    .await?;
    Ok(magic)
}

async fn read_exact_with_idle<R: AsyncRead + Unpin>(
    reader: &mut R,
    bytes: &mut [u8],
    idle_timeout: Option<Duration>,
    operation: &str,
    part: &str,
) -> Result<(), NetworkError> {
    let expected = bytes.len();
    let mut read = 0_usize;
    while read < expected {
        let result = if let Some(timeout) = idle_timeout {
            match tokio::time::timeout(timeout, reader.read(&mut bytes[read..])).await {
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
            reader.read(&mut bytes[read..]).await
        };
        match result {
            Ok(0) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: format!(
                        "peer closed during channel frame {part}: read {read} of {expected} bytes"
                    ),
                });
            }
            Ok(bytes_read) => read = read.saturating_add(bytes_read),
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

fn invalid_data_header(detail: impl Into<String>) -> NetworkError {
    NetworkError::Serialization {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn test_header(payload_len: usize) -> ObjectRpcChannelDataHeader {
        ObjectRpcChannelDataHeader::new(
            ObjectRpcChannelCallId(17),
            ObjectRpcChannelBodyDirection::Response,
            4 * 1024 * 1024,
            payload_len,
        )
        .expect("header")
    }

    #[test]
    fn fixed_header_round_trips_all_fields() {
        let header = test_header(256 * 1024);
        let encoded = header.encode().expect("encode");
        assert_eq!(encoded.len(), OBJECT_RPC_CHANNEL_DATA_HEADER_BYTES);
        assert_eq!(&encoded[..4], b"SCDF");
        assert_eq!(ObjectRpcChannelDataHeader::decode(&encoded), Ok(header));
    }

    #[test]
    fn fixed_header_rejects_nonzero_flags_reserved_and_invalid_direction() {
        let encoded = test_header(1024).encode().expect("encode");
        for (index, value) in [(6, 9), (7, 1), (31, 1)] {
            let mut malformed = encoded;
            malformed[index] = value;
            assert!(ObjectRpcChannelDataHeader::decode(&malformed).is_err());
        }
    }

    #[tokio::test]
    async fn semantic_header_rejection_happens_before_payload_read() {
        let (mut local, mut peer) = tokio::io::duplex(64);
        let encoded = test_header(1024).encode().expect("encode");
        peer.write_all(&encoded).await.expect("header");
        let receive_pool =
            ObjectRpcChannelReceiveBufferPool::new(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES)
                .expect("one-slab pool");
        let (held_buffer, held_permit) = receive_pool
            .acquire(1024)
            .await
            .expect("occupy receive pool");
        let held = receive_pool.into_bytes(held_buffer, held_permit);
        let error = tokio::time::timeout(
            Duration::from_millis(100),
            read_object_rpc_channel_transport_frame_async(
                &mut local,
                ObjectFrameIoOptions::none(),
                &receive_pool,
                |_| Err(invalid_data_header("inactive call")),
            ),
        )
        .await
        .expect("validation must not wait for payload")
        .expect_err("invalid call");
        assert!(error.to_string().contains("inactive call"));
        drop(held);
    }

    #[tokio::test]
    async fn raw_data_write_and_mixed_read_preserve_payload_ownership() {
        let payload = Bytes::from(vec![0x5a; 32 * 1024]);
        let frame = ObjectRpcChannelDataFrame::new(
            ObjectRpcChannelCallId(7),
            ObjectRpcChannelBodyDirection::Request,
            64,
            payload.clone(),
        )
        .expect("frame");
        let (mut local, mut peer) = tokio::io::duplex(64 * 1024);
        let receive_pool = ObjectRpcChannelReceiveBufferPool::default_for_carrier();
        let write = tokio::spawn(async move {
            write_object_rpc_channel_data_frame_async(
                &mut peer,
                &frame,
                ObjectFrameIoOptions::none(),
            )
            .await
        });
        let decoded = read_object_rpc_channel_transport_frame_async(
            &mut local,
            ObjectFrameIoOptions::none(),
            &receive_pool,
            |_| Ok(()),
        )
        .await
        .expect("read");
        let ObjectRpcChannelTransportFrame::Data(decoded) = decoded else {
            panic!("expected raw Data frame");
        };
        assert_eq!(decoded.bytes, payload);
        write.await.expect("write task").expect("write");
    }

    #[tokio::test]
    async fn receive_pool_reuses_one_slab_and_waits_for_the_last_bytes_clone() {
        let pool = ObjectRpcChannelReceiveBufferPool::new(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES)
            .expect("one-slab pool");
        assert_eq!(pool.max_bytes(), OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES);

        let (mut first_buffer, first_permit) = pool.acquire(1024).await.expect("first acquire");
        first_buffer[0] = 0x5a;
        let first = pool.into_bytes(first_buffer, first_permit);
        let first_ptr = first.as_ptr();
        let retained_slice = first.slice(..1);
        drop(first);

        assert!(
            tokio::time::timeout(Duration::from_millis(20), pool.acquire(1024))
                .await
                .is_err(),
            "a Bytes slice must retain the pool permit"
        );
        drop(retained_slice);
        assert_eq!(pool.cached_buffers(), 1);

        let (second_buffer, second_permit) =
            tokio::time::timeout(Duration::from_millis(100), pool.acquire(1024))
                .await
                .expect("released permit")
                .expect("second acquire");
        assert_eq!(second_buffer.as_ptr(), first_ptr, "slab must be reused");
        drop(pool.into_bytes(second_buffer, second_permit));
        assert_eq!(pool.cached_buffers(), 1);
    }

    #[tokio::test]
    async fn every_stalled_call_leaves_one_work_slab_and_controls_need_no_slab() {
        let pool = ObjectRpcChannelReceiveBufferPool::default_for_carrier();
        assert_eq!(
            pool.max_bytes(),
            (2 * OBJECT_RPC_CHANNEL_MAX_CALLS + 1) * OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES
        );

        let stalled_frames = 2 * OBJECT_RPC_CHANNEL_MAX_CALLS;
        let mut stalled_bodies = Vec::with_capacity(stalled_frames);
        for _ in 0..stalled_frames {
            let (buffer, permit) = pool
                .acquire(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES)
                .await
                .expect("stalled call receive slab");
            stalled_bodies.push(pool.into_bytes(buffer, permit));
        }

        let unrelated = ObjectRpcChannelDataFrame::new(
            ObjectRpcChannelCallId(29),
            ObjectRpcChannelBodyDirection::Response,
            0,
            Bytes::from(vec![0x29; OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES]),
        )
        .expect("unrelated Data");
        let following_control =
            ObjectRpcChannelTransportFrame::Control(ObjectRpcChannelWireFrame::BodyEnd {
                call_id: ObjectRpcChannelCallId(29),
                direction: ObjectRpcChannelBodyDirection::Response,
                total_bytes: OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES as u64,
            });
        let (mut local, mut peer) = tokio::io::duplex(64 * 1024);
        let writer = tokio::spawn(async move {
            write_object_rpc_channel_data_frame_async(
                &mut peer,
                &unrelated,
                ObjectFrameIoOptions::none(),
            )
            .await?;
            write_object_rpc_channel_transport_frame_async(
                &mut peer,
                &following_control,
                ObjectFrameIoOptions::none(),
            )
            .await
        });

        let work_frame = tokio::time::timeout(
            Duration::from_millis(250),
            read_object_rpc_channel_transport_frame_async(
                &mut local,
                ObjectFrameIoOptions::none(),
                &pool,
                |header| {
                    assert_eq!(header.call_id, ObjectRpcChannelCallId(29));
                    assert_eq!(header.offset, 0);
                    Ok(())
                },
            ),
        )
        .await
        .expect("unrelated Data must use the final work slab")
        .expect("read unrelated Data");
        let ObjectRpcChannelTransportFrame::Data(work_frame) = work_frame else {
            panic!("expected unrelated Data frame");
        };
        assert_eq!(
            work_frame.bytes.len(),
            OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES
        );

        let control = tokio::time::timeout(
            Duration::from_millis(250),
            read_object_rpc_channel_transport_frame_async(
                &mut local,
                ObjectFrameIoOptions::none(),
                &pool,
                |_| Ok(()),
            ),
        )
        .await
        .expect("following control must not need a receive slab")
        .expect("read following control");
        assert!(matches!(
            control,
            ObjectRpcChannelTransportFrame::Control(ObjectRpcChannelWireFrame::BodyEnd {
                call_id: ObjectRpcChannelCallId(29),
                ..
            })
        ));

        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                pool.acquire(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES),
            )
            .await
            .is_err(),
            "both receive windows must remain inside the carrier memory bound"
        );
        drop(work_frame);
        let (buffer, permit) = tokio::time::timeout(
            Duration::from_millis(100),
            pool.acquire(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES),
        )
        .await
        .expect("dropping unrelated Data releases one reserve slab")
        .expect("reacquire reserve slab");
        drop(pool.into_bytes(buffer, permit));
        drop(stalled_bodies);
        writer.await.expect("writer task").expect("write frames");

        assert_eq!(
            pool.cached_buffers(),
            OBJECT_RPC_CHANNEL_RECEIVE_BUFFER_CACHE_BYTES / OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES,
            "an idle carrier must retain only the smaller cache bound"
        );
    }

    #[tokio::test]
    async fn control_frames_remain_sof2() {
        let frame = ObjectRpcChannelTransportFrame::Control(ObjectRpcChannelWireFrame::BodyEnd {
            call_id: ObjectRpcChannelCallId(9),
            direction: ObjectRpcChannelBodyDirection::Request,
            total_bytes: 42,
        });
        let (mut local, mut peer) = tokio::io::duplex(1024);
        write_object_rpc_channel_transport_frame_async(
            &mut local,
            &frame,
            ObjectFrameIoOptions::none(),
        )
        .await
        .expect("write control");
        let mut magic = [0_u8; 4];
        peer.read_exact(&mut magic).await.expect("magic");
        assert_eq!(&magic, b"SOF2");
    }
}
