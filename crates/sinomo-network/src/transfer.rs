//! Private transfer-session foundation.
//!
//! Raw frame helpers are not an independent transport and must only be wired
//! beneath an authenticated object-RPC session that binds Open, data digests,
//! Finish, and the completion receipt to the signed request transcript. Until
//! that integration lands, higher-level crates can only use the exported call
//! policy and transfer identity/receipt types.

use crate::object::{DEFAULT_NETWORK_TIMEOUT, DaemonRpcTimeouts};
use crate::{NetworkDeliveryState, NetworkError, NetworkTimeoutPhase, SinomoObjectCodec};
use serde::{Deserialize, Serialize};
use sinomo_base::{Id, IdError};
use std::fmt;
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const DEFAULT_TRANSFER_FRAME_BYTES: u32 = 1024 * 1024;
pub const MAX_TRANSFER_FRAME_BYTES: u32 = 16 * 1024 * 1024;
pub const DEFAULT_TRANSFER_WINDOW_BYTES: u64 = 8 * 1024 * 1024;
pub const MAX_TRANSFER_WINDOW_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_TRANSFER_CONTROL_FRAME_BYTES: usize = 64 * 1024;
pub const TRANSFER_COPY_BUFFER_BYTES: usize = 64 * 1024;
const TRANSFER_WIRE_MAGIC: [u8; 4] = *b"SXFR";
const TRANSFER_WIRE_PREFIX_BYTES: usize = 9;
const TRANSFER_WIRE_CONTROL_KIND: u8 = 1;
const TRANSFER_WIRE_DATA_KIND: u8 = 2;

/// Health limits for one network path. These limits restart after validated
/// progress and never act as an implicit whole-call deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkHealthPolicy {
    pub connect_timeout: Duration,
    pub no_progress_timeout: Duration,
}

impl NetworkHealthPolicy {
    pub const fn new(connect_timeout: Duration, no_progress_timeout: Duration) -> Self {
        Self {
            connect_timeout,
            no_progress_timeout,
        }
    }
}

impl Default for NetworkHealthPolicy {
    fn default() -> Self {
        Self::new(DEFAULT_NETWORK_TIMEOUT, DEFAULT_NETWORK_TIMEOUT)
    }
}

impl From<NetworkHealthPolicy> for DaemonRpcTimeouts {
    fn from(policy: NetworkHealthPolicy) -> Self {
        Self {
            connect_timeout: Some(policy.connect_timeout),
            idle_timeout: Some(policy.no_progress_timeout),
        }
    }
}

/// Lifetime policy for an RPC session. Calls are unbounded by default while
/// the health policy continues to observe meaningful progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub struct NetworkCallPolicy {
    pub health: NetworkHealthPolicy,
    pub overall_timeout: Option<Duration>,
}

impl NetworkCallPolicy {
    pub const fn with_overall_timeout(mut self, timeout: Duration) -> Self {
        self.overall_timeout = Some(timeout);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct TransferId(Id);

impl TransferId {
    pub fn generate() -> Self {
        Self(Id::generate())
    }

    pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
        Id::new(value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for TransferId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for TransferId {
    type Err = IdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct TransferDigest([u8; 32]);

impl TransferDigest {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn blake3(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransferFlowWindow {
    pub max_in_flight_bytes: u64,
    pub max_frame_bytes: u32,
}

impl TransferFlowWindow {
    pub fn new(max_in_flight_bytes: u64, max_frame_bytes: u32) -> Result<Self, TransferError> {
        let window = Self {
            max_in_flight_bytes,
            max_frame_bytes,
        };
        window.validate()?;
        Ok(window)
    }

    pub fn validate(self) -> Result<(), TransferError> {
        if self.max_frame_bytes == 0 || self.max_frame_bytes > MAX_TRANSFER_FRAME_BYTES {
            return Err(TransferError::InvalidFlowWindow {
                max_in_flight_bytes: self.max_in_flight_bytes,
                max_frame_bytes: self.max_frame_bytes,
            });
        }
        if self.max_in_flight_bytes < u64::from(self.max_frame_bytes)
            || self.max_in_flight_bytes > MAX_TRANSFER_WINDOW_BYTES
        {
            return Err(TransferError::InvalidFlowWindow {
                max_in_flight_bytes: self.max_in_flight_bytes,
                max_frame_bytes: self.max_frame_bytes,
            });
        }
        Ok(())
    }

    pub fn available(
        self,
        sent_offset: u64,
        acknowledged_offset: u64,
    ) -> Result<u64, TransferError> {
        self.validate()?;
        let in_flight = sent_offset.checked_sub(acknowledged_offset).ok_or(
            TransferError::InvalidAcknowledgement {
                acknowledged_offset,
                sent_offset,
            },
        )?;
        Ok(self.max_in_flight_bytes.saturating_sub(in_flight))
    }
}

impl Default for TransferFlowWindow {
    fn default() -> Self {
        Self {
            max_in_flight_bytes: DEFAULT_TRANSFER_WINDOW_BYTES,
            max_frame_bytes: DEFAULT_TRANSFER_FRAME_BYTES,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransferOpen {
    pub transfer_id: TransferId,
    pub route: String,
    pub operation_id: Option<String>,
    pub total_bytes: Option<u64>,
    pub expected_digest: Option<TransferDigest>,
    pub replayable: bool,
    pub requested_window: TransferFlowWindow,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransferOpenAck {
    pub transfer_id: TransferId,
    pub resume_offset: u64,
    pub receive_window: TransferFlowWindow,
}

/// Header for a raw data frame. Payload bytes are deliberately separate so a
/// transport can write this header and an existing file/buffer slice with
/// scatter/gather I/O instead of embedding the payload in SOBJ or copying it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransferDataHeader {
    pub transfer_id: TransferId,
    pub offset: u64,
    pub payload_bytes: u32,
    pub payload_digest: TransferDigest,
}

#[derive(Debug)]
pub struct TransferDataFrame<'a> {
    pub header: TransferDataHeader,
    pub payload: &'a [u8],
}

impl<'a> TransferDataFrame<'a> {
    pub fn new(
        transfer_id: TransferId,
        offset: u64,
        payload: &'a [u8],
        window: TransferFlowWindow,
    ) -> Result<Self, TransferError> {
        window.validate()?;
        let payload_bytes =
            u32::try_from(payload.len()).map_err(|_| TransferError::FrameTooLarge {
                actual_bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
                max_bytes: window.max_frame_bytes,
            })?;
        if payload_bytes == 0 || payload_bytes > window.max_frame_bytes {
            return Err(TransferError::FrameTooLarge {
                actual_bytes: u64::from(payload_bytes),
                max_bytes: window.max_frame_bytes,
            });
        }
        Ok(Self {
            header: TransferDataHeader {
                transfer_id,
                offset,
                payload_bytes,
                payload_digest: TransferDigest::blake3(payload),
            },
            payload,
        })
    }

    pub fn validate_payload(&self) -> Result<(), TransferError> {
        if self.payload.len() != self.header.payload_bytes as usize {
            return Err(TransferError::PayloadLengthMismatch {
                declared_bytes: self.header.payload_bytes,
                actual_bytes: self.payload.len(),
            });
        }
        let actual = TransferDigest::blake3(self.payload);
        if actual != self.header.payload_digest {
            return Err(TransferError::DigestMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransferAck {
    pub transfer_id: TransferId,
    pub durable_contiguous_offset: u64,
    pub receive_window: TransferFlowWindow,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransferFinish {
    pub transfer_id: TransferId,
    pub total_bytes: u64,
    pub digest: TransferDigest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransferReceipt {
    pub transfer_id: TransferId,
    pub committed_bytes: u64,
    pub digest: TransferDigest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TransferControl {
    Open(TransferOpen),
    OpenAck(TransferOpenAck),
    Ack(TransferAck),
    Finish(TransferFinish),
    Complete(TransferReceipt),
    Cancel {
        transfer_id: TransferId,
        reason: String,
    },
    Reset {
        transfer_id: TransferId,
        reason: String,
    },
    Resume {
        transfer_id: TransferId,
        durable_contiguous_offset: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransferWireFrameHeader {
    Control(TransferControl),
    Data(TransferDataHeader),
}

/// Writes a small SOBJ transfer-control frame. The call has no total deadline;
/// each stalled write or flush is bounded by `health.no_progress_timeout`.
pub async fn write_transfer_control_frame<W>(
    writer: &mut W,
    control: &TransferControl,
    health: NetworkHealthPolicy,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    let header = SinomoObjectCodec::network_frame().encode(control)?;
    write_transfer_header(writer, TRANSFER_WIRE_CONTROL_KIND, &header, health).await
}

/// Writes a data header followed by an existing payload slice without nesting
/// or copying the payload into an object document.
pub async fn write_transfer_data_frame<W>(
    writer: &mut W,
    frame: &TransferDataFrame<'_>,
    health: NetworkHealthPolicy,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    if frame.payload.len() != frame.header.payload_bytes as usize {
        return Err(transfer_error_as_network_error(
            TransferError::PayloadLengthMismatch {
                declared_bytes: frame.header.payload_bytes,
                actual_bytes: frame.payload.len(),
            },
        ));
    }
    let header = SinomoObjectCodec::network_frame().encode(&frame.header)?;
    let prefix = transfer_wire_prefix(TRANSFER_WIRE_DATA_KIND, header.len())?;
    transfer_write_all(writer, &prefix, health, "write transfer data prefix").await?;
    transfer_write_all(writer, &header, health, "write transfer data header").await?;
    transfer_write_all(writer, frame.payload, health, "write transfer data payload").await
}

/// Reads only the next control or data header. For a data frame, the caller
/// must consume exactly `payload_bytes` with [`receive_transfer_data_payload`]
/// before reading another header. `header_buffer` is reused across frames.
pub async fn read_transfer_frame_header<R>(
    reader: &mut R,
    header_buffer: &mut Vec<u8>,
    health: NetworkHealthPolicy,
) -> Result<TransferWireFrameHeader, NetworkError>
where
    R: AsyncRead + Unpin,
{
    let mut prefix = [0_u8; TRANSFER_WIRE_PREFIX_BYTES];
    transfer_read_exact(reader, &mut prefix, health, "read transfer frame prefix").await?;
    if prefix[..4] != TRANSFER_WIRE_MAGIC {
        return Err(NetworkError::Serialization {
            detail: "invalid transfer frame magic".to_string(),
        });
    }
    let kind = prefix[4];
    let header_len = u32::from_be_bytes(prefix[5..9].try_into().expect("fixed transfer prefix"));
    let header_len = header_len as usize;
    if header_len == 0 || header_len > MAX_TRANSFER_CONTROL_FRAME_BYTES {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: header_len,
            max_bytes: MAX_TRANSFER_CONTROL_FRAME_BYTES,
        });
    }
    resize_transfer_buffer(header_buffer, header_len, "transfer frame header")?;
    transfer_read_exact(reader, header_buffer, health, "read transfer frame header").await?;
    match kind {
        TRANSFER_WIRE_CONTROL_KIND => SinomoObjectCodec::network_frame()
            .decode(header_buffer)
            .map(TransferWireFrameHeader::Control),
        TRANSFER_WIRE_DATA_KIND => {
            let header: TransferDataHeader =
                SinomoObjectCodec::network_frame().decode(header_buffer)?;
            if header.payload_bytes == 0 || header.payload_bytes > MAX_TRANSFER_FRAME_BYTES {
                return Err(NetworkError::FrameTooLarge {
                    actual_bytes: header.payload_bytes as usize,
                    max_bytes: MAX_TRANSFER_FRAME_BYTES as usize,
                });
            }
            Ok(TransferWireFrameHeader::Data(header))
        }
        _ => Err(NetworkError::Serialization {
            detail: format!("invalid transfer frame kind {kind}"),
        }),
    }
}

/// Copies one raw data payload into a staging sink with bounded memory while
/// validating the digest carried by its header. `copy_buffer` is reused across
/// frames and never grows beyond [`TRANSFER_COPY_BUFFER_BYTES`]. This does not
/// flush or declare durability: the session adapter must flush/checkpoint its
/// sink before committing the pending frame and issuing an acknowledgement.
pub async fn receive_transfer_data_payload<R, W>(
    reader: &mut R,
    writer: &mut W,
    pending: TransferPendingData,
    copy_buffer: &mut Vec<u8>,
    health: NetworkHealthPolicy,
) -> Result<TransferVerifiedData, NetworkError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let header = pending.header();
    let buffer_len = usize::min(header.payload_bytes as usize, TRANSFER_COPY_BUFFER_BYTES);
    resize_transfer_buffer(copy_buffer, buffer_len, "transfer copy buffer")?;
    let mut remaining = u64::from(header.payload_bytes);
    let mut hasher = blake3::Hasher::new();
    while remaining > 0 {
        let chunk_len = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(copy_buffer.len());
        let chunk = &mut copy_buffer[..chunk_len];
        transfer_read_exact(reader, chunk, health, "read transfer data payload").await?;
        hasher.update(chunk);
        transfer_write_all(writer, chunk, health, "write transfer data sink").await?;
        remaining -= chunk_len as u64;
    }
    if TransferDigest::new(*hasher.finalize().as_bytes()) != header.payload_digest {
        return Err(NetworkError::Serialization {
            detail: TransferError::DigestMismatch.to_string(),
        });
    }
    Ok(TransferVerifiedData { pending })
}

async fn write_transfer_header<W>(
    writer: &mut W,
    kind: u8,
    header: &[u8],
    health: NetworkHealthPolicy,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    let prefix = transfer_wire_prefix(kind, header.len())?;
    transfer_write_all(writer, &prefix, health, "write transfer control prefix").await?;
    transfer_write_all(writer, header, health, "write transfer control header").await?;
    transfer_flush(writer, health, "flush transfer control frame").await
}

fn transfer_wire_prefix(kind: u8, header_len: usize) -> Result<[u8; 9], NetworkError> {
    if header_len == 0 || header_len > MAX_TRANSFER_CONTROL_FRAME_BYTES {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: header_len,
            max_bytes: MAX_TRANSFER_CONTROL_FRAME_BYTES,
        });
    }
    let header_len = u32::try_from(header_len).map_err(|_| NetworkError::FrameTooLarge {
        actual_bytes: header_len,
        max_bytes: MAX_TRANSFER_CONTROL_FRAME_BYTES,
    })?;
    let mut prefix = [0_u8; TRANSFER_WIRE_PREFIX_BYTES];
    prefix[..4].copy_from_slice(&TRANSFER_WIRE_MAGIC);
    prefix[4] = kind;
    prefix[5..].copy_from_slice(&header_len.to_be_bytes());
    Ok(prefix)
}

fn resize_transfer_buffer(
    buffer: &mut Vec<u8>,
    len: usize,
    operation: &'static str,
) -> Result<(), NetworkError> {
    if buffer.len() < len {
        let additional = len - buffer.len();
        buffer
            .try_reserve(additional)
            .map_err(|_| NetworkError::ResourceExhausted {
                operation: operation.to_string(),
                requested_bytes: len,
            })?;
    }
    buffer.resize(len, 0);
    Ok(())
}

async fn transfer_write_all<W>(
    writer: &mut W,
    bytes: &[u8],
    health: NetworkHealthPolicy,
    operation: &'static str,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    let mut written = 0;
    while written < bytes.len() {
        let result =
            tokio::time::timeout(health.no_progress_timeout, writer.write(&bytes[written..]))
                .await
                .map_err(|_| transfer_idle_timeout(operation, health.no_progress_timeout))?;
        match result {
            Ok(0) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: format!("peer stopped accepting bytes after {written}"),
                });
            }
            Ok(count) => written += count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: error.to_string(),
                });
            }
        }
    }
    Ok(())
}

async fn transfer_flush<W>(
    writer: &mut W,
    health: NetworkHealthPolicy,
    operation: &'static str,
) -> Result<(), NetworkError>
where
    W: AsyncWrite + Unpin,
{
    tokio::time::timeout(health.no_progress_timeout, writer.flush())
        .await
        .map_err(|_| transfer_idle_timeout(operation, health.no_progress_timeout))?
        .map_err(|error| NetworkError::Io {
            operation: operation.to_string(),
            detail: error.to_string(),
        })
}

async fn transfer_read_exact<R>(
    reader: &mut R,
    bytes: &mut [u8],
    health: NetworkHealthPolicy,
    operation: &'static str,
) -> Result<(), NetworkError>
where
    R: AsyncRead + Unpin,
{
    let mut read = 0;
    while read < bytes.len() {
        let result =
            tokio::time::timeout(health.no_progress_timeout, reader.read(&mut bytes[read..]))
                .await
                .map_err(|_| transfer_idle_timeout(operation, health.no_progress_timeout))?;
        match result {
            Ok(0) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: format!("peer closed after {read} of {} transfer bytes", bytes.len()),
                });
            }
            Ok(count) => read += count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => {
                return Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: error.to_string(),
                });
            }
        }
    }
    Ok(())
}

fn transfer_idle_timeout(operation: &'static str, timeout: Duration) -> NetworkError {
    NetworkError::timeout(
        operation,
        NetworkTimeoutPhase::Idle,
        timeout,
        NetworkDeliveryState::PossiblyApplied,
    )
}

fn transfer_error_as_network_error(error: TransferError) -> NetworkError {
    NetworkError::Serialization {
        detail: error.to_string(),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferSenderPhase {
    New,
    Opening,
    Sending,
    Finishing,
    AwaitingReceipt,
    Reconnecting,
    Complete,
    Cancelled,
}

#[derive(Clone, Debug)]
pub struct TransferSenderState {
    transfer_id: TransferId,
    phase: TransferSenderPhase,
    window: TransferFlowWindow,
    total_bytes: Option<u64>,
    next_offset: u64,
    acknowledged_offset: u64,
    final_digest: Option<TransferDigest>,
}

impl TransferSenderState {
    pub fn new(
        transfer_id: TransferId,
        total_bytes: Option<u64>,
        window: TransferFlowWindow,
    ) -> Result<Self, TransferError> {
        window.validate()?;
        Ok(Self {
            transfer_id,
            phase: TransferSenderPhase::New,
            window,
            total_bytes,
            next_offset: 0,
            acknowledged_offset: 0,
            final_digest: None,
        })
    }

    pub const fn phase(&self) -> TransferSenderPhase {
        self.phase
    }

    pub const fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub const fn acknowledged_offset(&self) -> u64 {
        self.acknowledged_offset
    }

    pub fn mark_opening(&mut self) -> Result<(), TransferError> {
        self.require_phase(&[TransferSenderPhase::New, TransferSenderPhase::Reconnecting])?;
        self.phase = TransferSenderPhase::Opening;
        Ok(())
    }

    pub fn accept_open_ack(&mut self, ack: &TransferOpenAck) -> Result<(), TransferError> {
        self.require_phase(&[TransferSenderPhase::Opening])?;
        self.require_id(&ack.transfer_id)?;
        ack.receive_window.validate()?;
        if self
            .total_bytes
            .is_some_and(|total| ack.resume_offset > total)
        {
            return Err(TransferError::OffsetBeyondTotal {
                offset: ack.resume_offset,
                total_bytes: self.total_bytes.unwrap_or_default(),
            });
        }
        self.window = ack.receive_window;
        self.next_offset = ack.resume_offset;
        self.acknowledged_offset = ack.resume_offset;
        self.phase = TransferSenderPhase::Sending;
        Ok(())
    }

    pub fn reserve_data<'a>(
        &mut self,
        payload: &'a [u8],
    ) -> Result<TransferDataFrame<'a>, TransferError> {
        self.require_phase(&[TransferSenderPhase::Sending])?;
        let frame = TransferDataFrame::new(
            self.transfer_id.clone(),
            self.next_offset,
            payload,
            self.window,
        )?;
        let length = u64::from(frame.header.payload_bytes);
        if self
            .window
            .available(self.next_offset, self.acknowledged_offset)?
            < length
        {
            return Err(TransferError::FlowControlBlocked {
                requested_bytes: length,
                available_bytes: self
                    .window
                    .available(self.next_offset, self.acknowledged_offset)?,
            });
        }
        let next_offset = self
            .next_offset
            .checked_add(length)
            .ok_or(TransferError::OffsetOverflow)?;
        if let Some(total_bytes) = self.total_bytes
            && next_offset > total_bytes
        {
            return Err(TransferError::OffsetBeyondTotal {
                offset: next_offset,
                total_bytes,
            });
        }
        self.next_offset = next_offset;
        Ok(frame)
    }

    pub fn accept_ack(&mut self, ack: &TransferAck) -> Result<(), TransferError> {
        self.require_phase(&[
            TransferSenderPhase::Sending,
            TransferSenderPhase::Finishing,
            TransferSenderPhase::AwaitingReceipt,
        ])?;
        self.require_id(&ack.transfer_id)?;
        ack.receive_window.validate()?;
        if ack.durable_contiguous_offset < self.acknowledged_offset
            || ack.durable_contiguous_offset > self.next_offset
        {
            return Err(TransferError::InvalidAcknowledgement {
                acknowledged_offset: ack.durable_contiguous_offset,
                sent_offset: self.next_offset,
            });
        }
        self.acknowledged_offset = ack.durable_contiguous_offset;
        self.window = ack.receive_window;
        Ok(())
    }

    pub fn finish(&mut self, digest: TransferDigest) -> Result<TransferFinish, TransferError> {
        self.require_phase(&[TransferSenderPhase::Sending])?;
        if let Some(total_bytes) = self.total_bytes
            && self.next_offset != total_bytes
        {
            return Err(TransferError::IncompleteTransfer {
                expected_bytes: total_bytes,
                actual_bytes: self.next_offset,
            });
        }
        self.phase = TransferSenderPhase::Finishing;
        self.final_digest = Some(digest);
        Ok(TransferFinish {
            transfer_id: self.transfer_id.clone(),
            total_bytes: self.next_offset,
            digest,
        })
    }

    pub fn mark_finish_sent(&mut self) -> Result<(), TransferError> {
        self.require_phase(&[TransferSenderPhase::Finishing])?;
        self.phase = TransferSenderPhase::AwaitingReceipt;
        Ok(())
    }

    pub fn accept_receipt(&mut self, receipt: &TransferReceipt) -> Result<(), TransferError> {
        self.require_phase(&[TransferSenderPhase::AwaitingReceipt])?;
        self.require_id(&receipt.transfer_id)?;
        if receipt.committed_bytes != self.next_offset {
            return Err(TransferError::IncompleteTransfer {
                expected_bytes: self.next_offset,
                actual_bytes: receipt.committed_bytes,
            });
        }
        if self.final_digest != Some(receipt.digest) {
            return Err(TransferError::DigestMismatch);
        }
        self.acknowledged_offset = receipt.committed_bytes;
        self.phase = TransferSenderPhase::Complete;
        Ok(())
    }

    pub fn reconnect(&mut self) -> Result<(), TransferError> {
        self.require_phase(&[
            TransferSenderPhase::Sending,
            TransferSenderPhase::Finishing,
            TransferSenderPhase::AwaitingReceipt,
        ])?;
        self.phase = TransferSenderPhase::Reconnecting;
        Ok(())
    }

    fn require_id(&self, transfer_id: &TransferId) -> Result<(), TransferError> {
        if &self.transfer_id != transfer_id {
            return Err(TransferError::TransferIdMismatch);
        }
        Ok(())
    }

    fn require_phase(&self, expected: &[TransferSenderPhase]) -> Result<(), TransferError> {
        if !expected.contains(&self.phase) {
            return Err(TransferError::InvalidSenderState { actual: self.phase });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferReceiverPhase {
    Receiving,
    Verifying,
    Committed,
    Cancelled,
}

#[derive(Debug, Eq, PartialEq)]
pub struct TransferPendingData {
    header: TransferDataHeader,
    next_offset: u64,
}

impl TransferPendingData {
    pub fn header(&self) -> &TransferDataHeader {
        &self.header
    }
}

/// Proof that the payload for a pending data frame was read completely and its
/// digest matched the frame header. Only [`receive_transfer_data_payload`] can
/// construct this token; receiver state cannot advance from a header alone.
#[derive(Debug, Eq, PartialEq)]
pub struct TransferVerifiedData {
    pending: TransferPendingData,
}

impl TransferVerifiedData {
    pub fn header(&self) -> &TransferDataHeader {
        self.pending.header()
    }

    pub fn payload_bytes(&self) -> u64 {
        u64::from(self.pending.header.payload_bytes)
    }
}

#[derive(Clone, Debug)]
pub struct TransferReceiverState {
    transfer_id: TransferId,
    phase: TransferReceiverPhase,
    window: TransferFlowWindow,
    total_bytes: Option<u64>,
    expected_digest: Option<TransferDigest>,
    received_offset: u64,
    durable_offset: u64,
    finish_digest: Option<TransferDigest>,
}

impl TransferReceiverState {
    pub fn from_open(
        open: &TransferOpen,
        window: TransferFlowWindow,
    ) -> Result<Self, TransferError> {
        open.requested_window.validate()?;
        window.validate()?;
        Ok(Self {
            transfer_id: open.transfer_id.clone(),
            phase: TransferReceiverPhase::Receiving,
            window,
            total_bytes: open.total_bytes,
            expected_digest: open.expected_digest,
            received_offset: 0,
            durable_offset: 0,
            finish_digest: None,
        })
    }

    pub const fn phase(&self) -> TransferReceiverPhase {
        self.phase
    }

    pub const fn received_offset(&self) -> u64 {
        self.received_offset
    }

    pub const fn durable_offset(&self) -> u64 {
        self.durable_offset
    }

    pub fn open_ack(&self) -> TransferOpenAck {
        TransferOpenAck {
            transfer_id: self.transfer_id.clone(),
            resume_offset: self.durable_offset,
            receive_window: self.window,
        }
    }

    /// Validates a data header without advancing receive state. The caller must
    /// stage and verify the payload before passing the returned ticket to
    /// [`Self::commit_data`].
    pub fn begin_data(
        &self,
        header: &TransferDataHeader,
    ) -> Result<TransferPendingData, TransferError> {
        if self.phase != TransferReceiverPhase::Receiving {
            return Err(TransferError::InvalidReceiverState { actual: self.phase });
        }
        if header.transfer_id != self.transfer_id {
            return Err(TransferError::TransferIdMismatch);
        }
        if header.offset != self.received_offset {
            return Err(TransferError::UnexpectedOffset {
                expected: self.received_offset,
                actual: header.offset,
            });
        }
        if header.payload_bytes == 0 || header.payload_bytes > self.window.max_frame_bytes {
            return Err(TransferError::FrameTooLarge {
                actual_bytes: u64::from(header.payload_bytes),
                max_bytes: self.window.max_frame_bytes,
            });
        }
        let available = self
            .window
            .available(self.received_offset, self.durable_offset)?;
        if u64::from(header.payload_bytes) > available {
            return Err(TransferError::FlowControlBlocked {
                requested_bytes: u64::from(header.payload_bytes),
                available_bytes: available,
            });
        }
        let next_offset = self
            .received_offset
            .checked_add(u64::from(header.payload_bytes))
            .ok_or(TransferError::OffsetOverflow)?;
        if let Some(total_bytes) = self.total_bytes
            && next_offset > total_bytes
        {
            return Err(TransferError::OffsetBeyondTotal {
                offset: next_offset,
                total_bytes,
            });
        }
        Ok(TransferPendingData {
            header: header.clone(),
            next_offset,
        })
    }

    pub fn commit_data(&mut self, verified: TransferVerifiedData) -> Result<(), TransferError> {
        let pending = verified.pending;
        if self.phase != TransferReceiverPhase::Receiving {
            return Err(TransferError::InvalidReceiverState { actual: self.phase });
        }
        if pending.header.transfer_id != self.transfer_id {
            return Err(TransferError::TransferIdMismatch);
        }
        if pending.header.offset != self.received_offset {
            return Err(TransferError::UnexpectedOffset {
                expected: self.received_offset,
                actual: pending.header.offset,
            });
        }
        self.received_offset = pending.next_offset;
        Ok(())
    }

    pub fn acknowledge_durable(&mut self, offset: u64) -> Result<TransferAck, TransferError> {
        if offset < self.durable_offset || offset > self.received_offset {
            return Err(TransferError::InvalidAcknowledgement {
                acknowledged_offset: offset,
                sent_offset: self.received_offset,
            });
        }
        self.durable_offset = offset;
        Ok(TransferAck {
            transfer_id: self.transfer_id.clone(),
            durable_contiguous_offset: offset,
            receive_window: self.window,
        })
    }

    pub fn accept_finish(&mut self, finish: &TransferFinish) -> Result<(), TransferError> {
        if self.phase != TransferReceiverPhase::Receiving {
            return Err(TransferError::InvalidReceiverState { actual: self.phase });
        }
        if finish.transfer_id != self.transfer_id {
            return Err(TransferError::TransferIdMismatch);
        }
        if finish.total_bytes != self.received_offset {
            return Err(TransferError::IncompleteTransfer {
                expected_bytes: self.received_offset,
                actual_bytes: finish.total_bytes,
            });
        }
        if let Some(total_bytes) = self.total_bytes
            && finish.total_bytes != total_bytes
        {
            return Err(TransferError::IncompleteTransfer {
                expected_bytes: total_bytes,
                actual_bytes: finish.total_bytes,
            });
        }
        if self
            .expected_digest
            .is_some_and(|expected| expected != finish.digest)
        {
            return Err(TransferError::DigestMismatch);
        }
        self.finish_digest = Some(finish.digest);
        self.phase = TransferReceiverPhase::Verifying;
        Ok(())
    }

    pub fn commit(
        &mut self,
        computed_digest: TransferDigest,
    ) -> Result<TransferReceipt, TransferError> {
        if self.phase != TransferReceiverPhase::Verifying {
            return Err(TransferError::InvalidReceiverState { actual: self.phase });
        }
        if self.finish_digest != Some(computed_digest) {
            return Err(TransferError::DigestMismatch);
        }
        self.durable_offset = self.received_offset;
        self.phase = TransferReceiverPhase::Committed;
        Ok(TransferReceipt {
            transfer_id: self.transfer_id.clone(),
            committed_bytes: self.received_offset,
            digest: computed_digest,
        })
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TransferError {
    #[error(
        "invalid flow window: {max_in_flight_bytes} in-flight bytes, {max_frame_bytes} frame bytes"
    )]
    InvalidFlowWindow {
        max_in_flight_bytes: u64,
        max_frame_bytes: u32,
    },
    #[error("transfer frame is {actual_bytes} bytes; maximum is {max_bytes}")]
    FrameTooLarge { actual_bytes: u64, max_bytes: u32 },
    #[error("transfer payload length mismatch: declared {declared_bytes}, actual {actual_bytes}")]
    PayloadLengthMismatch {
        declared_bytes: u32,
        actual_bytes: usize,
    },
    #[error("transfer digest mismatch")]
    DigestMismatch,
    #[error("transfer id mismatch")]
    TransferIdMismatch,
    #[error("unexpected transfer offset: expected {expected}, actual {actual}")]
    UnexpectedOffset { expected: u64, actual: u64 },
    #[error("transfer offset {offset} exceeds declared total {total_bytes}")]
    OffsetBeyondTotal { offset: u64, total_bytes: u64 },
    #[error("transfer offset overflow")]
    OffsetOverflow,
    #[error("invalid durable acknowledgement {acknowledged_offset} for sent offset {sent_offset}")]
    InvalidAcknowledgement {
        acknowledged_offset: u64,
        sent_offset: u64,
    },
    #[error("flow control blocked {requested_bytes} bytes; {available_bytes} bytes available")]
    FlowControlBlocked {
        requested_bytes: u64,
        available_bytes: u64,
    },
    #[error("incomplete transfer: expected {expected_bytes} bytes, actual {actual_bytes}")]
    IncompleteTransfer {
        expected_bytes: u64,
        actual_bytes: u64,
    },
    #[error("invalid sender transfer state {actual:?}")]
    InvalidSenderState { actual: TransferSenderPhase },
    #[error("invalid receiver transfer state {actual:?}")]
    InvalidReceiverState { actual: TransferReceiverPhase },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    #[derive(Default)]
    struct VecAsyncWriter(Vec<u8>);

    impl AsyncWrite for VecAsyncWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.0.extend_from_slice(bytes);
            Poll::Ready(Ok(bytes.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn open(transfer_id: TransferId, total_bytes: u64, digest: TransferDigest) -> TransferOpen {
        TransferOpen {
            transfer_id,
            route: "/test/transfer".to_string(),
            operation_id: Some("operation-1".to_string()),
            total_bytes: Some(total_bytes),
            expected_digest: Some(digest),
            replayable: true,
            requested_window: TransferFlowWindow::default(),
        }
    }

    #[test]
    fn default_call_policy_has_health_limits_without_an_overall_deadline() {
        let policy = NetworkCallPolicy::default();
        assert_eq!(policy.health.connect_timeout, DEFAULT_NETWORK_TIMEOUT);
        assert_eq!(policy.health.no_progress_timeout, DEFAULT_NETWORK_TIMEOUT);
        assert_eq!(policy.overall_timeout, None);
    }

    #[test]
    fn transfer_controls_round_trip_as_small_sobj_documents() {
        let transfer_id = TransferId::generate();
        let control =
            TransferControl::Open(open(transfer_id, 42, TransferDigest::blake3(b"payload")));
        let bytes = SinomoObjectCodec::network_frame()
            .encode(&control)
            .expect("encode transfer control");
        let decoded: TransferControl = SinomoObjectCodec::network_frame()
            .decode(&bytes)
            .expect("decode transfer control");
        assert_eq!(decoded, control);
    }

    #[tokio::test]
    async fn transfer_control_wire_frame_round_trips() {
        let control = TransferControl::Cancel {
            transfer_id: TransferId::generate(),
            reason: "test cancellation".to_string(),
        };
        let (mut sender, mut receiver) = tokio::io::duplex(4096);
        let expected = control.clone();
        let send = tokio::spawn(async move {
            write_transfer_control_frame(&mut sender, &control, NetworkHealthPolicy::default())
                .await
        });
        let mut header_buffer = Vec::new();
        let header = read_transfer_frame_header(
            &mut receiver,
            &mut header_buffer,
            NetworkHealthPolicy::default(),
        )
        .await
        .expect("read control header");
        assert_eq!(header, TransferWireFrameHeader::Control(expected));
        send.await.expect("sender task").expect("send control");
        assert!(header_buffer.len() <= MAX_TRANSFER_CONTROL_FRAME_BYTES);
    }

    #[tokio::test]
    async fn transfer_data_wire_frame_uses_a_bounded_copy_buffer() {
        let payload = vec![0x5a; TRANSFER_COPY_BUFFER_BYTES * 3 + 17];
        let expected_payload = payload.clone();
        let transfer_id = TransferId::generate();
        let digest = TransferDigest::blake3(&expected_payload);
        let mut state = TransferReceiverState::from_open(
            &open(transfer_id.clone(), expected_payload.len() as u64, digest),
            TransferFlowWindow::default(),
        )
        .expect("receiver state");
        let (mut sender, mut receiver) = tokio::io::duplex(32 * 1024);
        let send = tokio::spawn(async move {
            let frame =
                TransferDataFrame::new(transfer_id, 0, &payload, TransferFlowWindow::default())
                    .expect("data frame");
            write_transfer_data_frame(&mut sender, &frame, NetworkHealthPolicy::default()).await
        });
        let mut header_buffer = Vec::new();
        let header = read_transfer_frame_header(
            &mut receiver,
            &mut header_buffer,
            NetworkHealthPolicy::default(),
        )
        .await
        .expect("read data header");
        let TransferWireFrameHeader::Data(header) = header else {
            panic!("expected data header");
        };
        let pending = state.begin_data(&header).expect("pending data");
        let mut sink = VecAsyncWriter::default();
        let mut copy_buffer = Vec::new();
        let verified = receive_transfer_data_payload(
            &mut receiver,
            &mut sink,
            pending,
            &mut copy_buffer,
            NetworkHealthPolicy::default(),
        )
        .await
        .expect("receive data payload");
        let copied = verified.payload_bytes();
        state.commit_data(verified).expect("commit verified data");
        send.await.expect("sender task").expect("send data");
        assert_eq!(copied, expected_payload.len() as u64);
        assert_eq!(state.received_offset(), copied);
        assert_eq!(sink.0, expected_payload);
        assert_eq!(copy_buffer.len(), TRANSFER_COPY_BUFFER_BYTES);
    }

    #[tokio::test]
    async fn truncated_payload_does_not_advance_receiver_state() {
        let transfer_id = TransferId::generate();
        let digest = TransferDigest::blake3(b"abcd");
        let open = open(transfer_id.clone(), 4, digest);
        let state = TransferReceiverState::from_open(&open, TransferFlowWindow::default())
            .expect("receiver state");
        let frame = TransferDataFrame::new(transfer_id, 0, b"abcd", TransferFlowWindow::default())
            .expect("data frame");
        let header_bytes = SinomoObjectCodec::network_frame()
            .encode(&frame.header)
            .expect("encode header");
        let prefix =
            transfer_wire_prefix(TRANSFER_WIRE_DATA_KIND, header_bytes.len()).expect("wire prefix");
        let (mut sender, mut receiver) = tokio::io::duplex(4096);
        let send = tokio::spawn(async move {
            sender.write_all(&prefix).await.expect("prefix");
            sender.write_all(&header_bytes).await.expect("header");
            sender.write_all(b"ab").await.expect("partial payload");
        });

        let mut header_buffer = Vec::new();
        let TransferWireFrameHeader::Data(header) = read_transfer_frame_header(
            &mut receiver,
            &mut header_buffer,
            NetworkHealthPolicy::default(),
        )
        .await
        .expect("read data header") else {
            panic!("expected data header");
        };
        let pending = state.begin_data(&header).expect("pending data");
        let mut sink = VecAsyncWriter::default();
        let mut copy_buffer = Vec::new();
        receive_transfer_data_payload(
            &mut receiver,
            &mut sink,
            pending,
            &mut copy_buffer,
            NetworkHealthPolicy::default(),
        )
        .await
        .expect_err("truncated frame must fail");
        send.await.expect("sender task");

        assert_eq!(state.received_offset(), 0);
        assert_eq!(state.durable_offset(), 0);
        state
            .begin_data(&header)
            .expect("the same offset remains retryable");
    }

    #[tokio::test]
    async fn corrupt_payload_does_not_produce_a_verified_commit() {
        let transfer_id = TransferId::generate();
        let digest = TransferDigest::blake3(b"abcd");
        let open = open(transfer_id.clone(), 4, digest);
        let state = TransferReceiverState::from_open(&open, TransferFlowWindow::default())
            .expect("receiver state");
        let valid = TransferDataFrame::new(transfer_id, 0, b"abcd", TransferFlowWindow::default())
            .expect("data frame");
        let corrupt = TransferDataFrame {
            header: valid.header.clone(),
            payload: b"wxyz",
        };
        let (mut sender, mut receiver) = tokio::io::duplex(4096);
        let send = tokio::spawn(async move {
            write_transfer_data_frame(&mut sender, &corrupt, NetworkHealthPolicy::default()).await
        });

        let mut header_buffer = Vec::new();
        let TransferWireFrameHeader::Data(header) = read_transfer_frame_header(
            &mut receiver,
            &mut header_buffer,
            NetworkHealthPolicy::default(),
        )
        .await
        .expect("read data header") else {
            panic!("expected data header");
        };
        let pending = state.begin_data(&header).expect("pending data");
        let mut sink = VecAsyncWriter::default();
        let mut copy_buffer = Vec::new();
        let error = receive_transfer_data_payload(
            &mut receiver,
            &mut sink,
            pending,
            &mut copy_buffer,
            NetworkHealthPolicy::default(),
        )
        .await
        .expect_err("corrupt frame must not yield a verified token");
        send.await.expect("sender task").expect("send data");

        assert!(matches!(error, NetworkError::Serialization { ref detail }
            if detail.contains("digest mismatch")));
        assert_eq!(state.received_offset(), 0);
        assert_eq!(state.durable_offset(), 0);
        state
            .begin_data(&header)
            .expect("the same offset remains retryable");
    }

    #[tokio::test(start_paused = true)]
    async fn transfer_header_stall_is_a_typed_no_progress_timeout() {
        let (_sender, mut receiver) = tokio::io::duplex(64);
        let mut header_buffer = Vec::new();
        let error = read_transfer_frame_header(
            &mut receiver,
            &mut header_buffer,
            NetworkHealthPolicy::default(),
        )
        .await
        .expect_err("silent transfer must time out");
        assert!(matches!(
            error,
            NetworkError::Timeout {
                phase: NetworkTimeoutPhase::Idle,
                timeout: DEFAULT_NETWORK_TIMEOUT,
                ..
            }
        ));
    }

    #[test]
    fn raw_data_frame_borrows_and_hashes_the_payload() {
        let payload = b"borrowed transfer payload";
        let frame = TransferDataFrame::new(
            TransferId::generate(),
            0,
            payload,
            TransferFlowWindow::default(),
        )
        .expect("data frame");
        assert_eq!(frame.payload.as_ptr(), payload.as_ptr());
        frame.validate_payload().expect("valid payload");

        let corrupt = TransferDataFrame {
            header: frame.header,
            payload: b"corrupt transfer payload!",
        };
        assert_eq!(
            corrupt.validate_payload(),
            Err(TransferError::DigestMismatch)
        );
    }

    #[test]
    fn sender_window_requires_durable_ack_progress() {
        let window = TransferFlowWindow::new(8, 4).expect("small window");
        let transfer_id = TransferId::generate();
        let mut sender =
            TransferSenderState::new(transfer_id.clone(), Some(12), window).expect("sender");
        sender.mark_opening().expect("open");
        sender
            .accept_open_ack(&TransferOpenAck {
                transfer_id: transfer_id.clone(),
                resume_offset: 0,
                receive_window: window,
            })
            .expect("open ack");
        sender.reserve_data(b"abcd").expect("first frame");
        sender.reserve_data(b"efgh").expect("second frame");
        assert_eq!(
            sender.reserve_data(b"ijkl").expect_err("window full"),
            TransferError::FlowControlBlocked {
                requested_bytes: 4,
                available_bytes: 0,
            }
        );
        sender
            .accept_ack(&TransferAck {
                transfer_id: transfer_id.clone(),
                durable_contiguous_offset: 4,
                receive_window: window,
            })
            .expect("durable ack");
        sender.reserve_data(b"ijkl").expect("window reopened");
        let digest = TransferDigest::blake3(b"abcdefghijkl");
        let finish = sender.finish(digest).expect("finish transfer");
        assert_eq!(finish.total_bytes, 12);
        assert_eq!(sender.phase(), TransferSenderPhase::Finishing);
        sender.mark_finish_sent().expect("finish sent");
        sender
            .accept_receipt(&TransferReceipt {
                transfer_id,
                committed_bytes: 12,
                digest,
            })
            .expect("completion receipt");
        assert_eq!(sender.phase(), TransferSenderPhase::Complete);
    }

    #[test]
    fn receiver_rejects_out_of_order_and_commits_only_after_digest_verification() {
        let bytes = b"abcdefgh";
        let digest = TransferDigest::blake3(bytes);
        let transfer_id = TransferId::generate();
        let window = TransferFlowWindow::new(8, 4).expect("small window");
        let mut receiver =
            TransferReceiverState::from_open(&open(transfer_id.clone(), 8, digest), window)
                .expect("receiver");

        let second =
            TransferDataFrame::new(transfer_id.clone(), 4, b"efgh", window).expect("second frame");
        assert_eq!(
            receiver.begin_data(&second.header),
            Err(TransferError::UnexpectedOffset {
                expected: 0,
                actual: 4,
            })
        );

        let first =
            TransferDataFrame::new(transfer_id.clone(), 0, b"abcd", window).expect("first frame");
        let first = receiver.begin_data(&first.header).expect("first data");
        receiver
            .commit_data(TransferVerifiedData { pending: first })
            .expect("commit first data");
        receiver
            .acknowledge_durable(4)
            .expect("durable first frame");
        let second = receiver.begin_data(&second.header).expect("second data");
        receiver
            .commit_data(TransferVerifiedData { pending: second })
            .expect("commit second data");
        receiver
            .accept_finish(&TransferFinish {
                transfer_id,
                total_bytes: 8,
                digest,
            })
            .expect("finish");
        assert_eq!(
            receiver.commit(TransferDigest::blake3(b"wrong")),
            Err(TransferError::DigestMismatch)
        );
        let receipt = receiver.commit(digest).expect("commit");
        assert_eq!(receipt.committed_bytes, 8);
        assert_eq!(receiver.phase(), TransferReceiverPhase::Committed);
    }

    #[test]
    fn flow_window_rejects_zero_oversized_and_inverted_limits() {
        assert!(TransferFlowWindow::new(1024, 0).is_err());
        assert!(TransferFlowWindow::new(1024, 2048).is_err());
        assert!(
            TransferFlowWindow::new(MAX_TRANSFER_WINDOW_BYTES + 1, DEFAULT_TRANSFER_FRAME_BYTES,)
                .is_err()
        );
    }
}
