//! Additive, checksummed frames for owner-managed append files.
//!
//! This format is independent of existing length-prefixed records and migration
//! streams. It does not change their bytes or recovery policies. A frame contains
//! a bounded opaque header and a streaming payload; the domain owns header
//! interpretation, operation identity, generation fencing, quota and GC roots.
//!
//! A complete invalid frame is corruption even at EOF. An incomplete frame is
//! reported separately and is never silently discarded by a reader. The owner
//! may explicitly recover an incomplete suffix, subject to its durable frontier.
//! A checksum cannot distinguish an externally truncated acknowledged frame
//! from an interrupted append, so that frontier remains the owner's obligation.
//!
//! [`FrameAppender`] requires exclusive mutation ownership of its file and
//! pathname, including across recovery. It does not implement a cross-process
//! lock, layout activation or a generation-pointer transaction. Rotation must
//! create a different file under the owner's fence; do not replace the pathname
//! behind a live appender. Returned tokens are local sync evidence, not domain
//! publication authority. Reader offsets refer to the exact file validated;
//! its owner must retain that generation while a reader uses them.
//! The owner must durably establish the parent-directory hierarchy; this module
//! synchronizes the immediate file parent using the existing storage helpers.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

use crate::durable_file::{
    DurableFileError, DurableFileSync, DurableReadChunk, open_file_for_append,
    read_full_chunk_with_observer, sync_directory, sync_open_file, truncate_open_file_and_sync,
};

const MAGIC: &[u8; 8] = b"SFRM0001";
const TRAILER_MAGIC: &[u8; 8] = b"SFRMEND1";
const VERSION: u16 = 1;
const PREFIX_FIELDS_BYTES: usize = 24;
const PREFIX_BYTES: usize = PREFIX_FIELDS_BYTES + 32;
const TRAILER_BYTES: usize = 32 + 8;
const COPY_BUFFER_BYTES: usize = 16 * 1024;
const PREFIX_HASH_CONTEXT: &str = "sinomo.storage.checksummed_frame.v1.prefix";
const FRAME_HASH_CONTEXT: &str = "sinomo.storage.checksummed_frame.v1.frame";
const LOG_HASH_CONTEXT: &str = "sinomo.storage.checksummed_frame.v1.prefix_sequence";

/// Hard format bounds. Payload validation never allocates payload-sized buffers.
pub const MAX_HEADER_BYTES: u32 = 1024 * 1024;
pub const MAX_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;
pub const FRAME_OVERHEAD_BYTES: u64 = (PREFIX_BYTES + TRAILER_BYTES) as u64;
/// A batch is also bounded in aggregate, independently of per-frame limits.
pub const MAX_BATCH_FRAMES: usize = 1024;
pub const MAX_BATCH_ENCODED_BYTES: u64 =
    MAX_PAYLOAD_BYTES + MAX_HEADER_BYTES as u64 + FRAME_OVERHEAD_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameLimits {
    max_header_bytes: u32,
    max_payload_bytes: u64,
}

impl FrameLimits {
    pub fn new(max_header_bytes: u32, max_payload_bytes: u64) -> Result<Self, FrameError> {
        if max_header_bytes > MAX_HEADER_BYTES || max_payload_bytes > MAX_PAYLOAD_BYTES {
            return Err(FrameError::InvalidLimits);
        }
        Ok(Self {
            max_header_bytes,
            max_payload_bytes,
        })
    }

    fn permits(self, header_bytes: u64, payload_bytes: u64) -> bool {
        header_bytes <= u64::from(self.max_header_bytes) && payload_bytes <= self.max_payload_bytes
    }
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            max_header_bytes: MAX_HEADER_BYTES,
            max_payload_bytes: MAX_PAYLOAD_BYTES,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncompleteStage {
    Prefix,
    Header,
    Payload,
    Trailer,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncompleteFrame {
    pub frame_start: u64,
    pub observed_end: u64,
    pub expected_end: Option<u64>,
    pub stage: IncompleteStage,
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum FrameCorruption {
    #[error("invalid frame magic")]
    Magic,
    #[error("unsupported frame version {0}")]
    Version(u16),
    #[error("nonzero reserved frame flags")]
    Flags,
    #[error("frame prefix checksum mismatch")]
    PrefixChecksum,
    #[error("frame header/payload lengths exceed configured limits")]
    LengthLimit,
    #[error("frame offset overflow")]
    OffsetOverflow,
    #[error("frame checksum mismatch")]
    Checksum,
    #[error("invalid complete-frame trailer")]
    Trailer,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorruptFrame {
    pub frame_start: u64,
    pub reason: FrameCorruption,
}

/// Bytes were validated, but no durability or domain-authority claim is made.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedFrame {
    header: Vec<u8>,
    frame_start: u64,
    payload_offset: u64,
    payload_bytes: u64,
    frame_end: u64,
    checksum: [u8; 32],
    payload_checksum: [u8; 32],
}

impl ValidatedFrame {
    pub fn header(&self) -> &[u8] {
        &self.header
    }
    pub fn frame_start(&self) -> u64 {
        self.frame_start
    }
    pub fn payload_offset(&self) -> u64 {
        self.payload_offset
    }
    pub fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }
    pub fn frame_end(&self) -> u64 {
        self.frame_end
    }
    pub fn checksum(&self) -> &[u8; 32] {
        &self.checksum
    }
    /// Plain BLAKE3 of payload bytes, computed in the same streaming pass. The
    /// domain can compare its opaque header's content identity without rereading
    /// the payload. This module does not interpret or authorize that header.
    pub fn payload_checksum(&self) -> &[u8; 32] {
        &self.payload_checksum
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FrameRead {
    CleanEof { offset: u64 },
    Complete(ValidatedFrame),
    Incomplete(IncompleteFrame),
    Corrupt(CorruptFrame),
}

#[derive(Debug, Error)]
pub enum FrameError {
    #[error(transparent)]
    Storage(#[from] DurableFileError),
    #[error("frame limits exceed the supported header/payload bounds")]
    InvalidLimits,
    #[error("header or payload exceeds the configured frame limit")]
    LengthLimit,
    #[error("frame batch exceeds its supported count or encoded byte bound")]
    BatchLimit,
    #[error("incomplete append frame: {0:?}")]
    Incomplete(IncompleteFrame),
    #[error("corrupt append frame: {0:?}")]
    Corrupt(CorruptFrame),
    #[error("append file needs validated recovery after an uncertain write or sync")]
    RecoveryRequired,
    #[error("append file length changed: expected {expected}, observed {observed}")]
    LengthChanged { expected: u64, observed: u64 },
    #[error(
        "validated prefix ends at {validated_end}, before required durable frontier {minimum_end}"
    )]
    DurablePrefixLost {
        minimum_end: u64,
        validated_end: u64,
    },
    #[error("payload source ended after {received} of {expected} declared bytes")]
    PayloadEnded { expected: u64, received: u64 },
    #[error("frame offset or count overflow")]
    Overflow,
}

fn io_error(path: &Path, source: io::Error) -> FrameError {
    DurableFileError::Io {
        path: path.display().to_string(),
        kind: source.kind(),
        detail: source.to_string(),
    }
    .into()
}

fn header_copy(path: &Path, bytes: &[u8]) -> Result<Vec<u8>, FrameError> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(bytes.len())
        .map_err(|_| DurableFileError::ResourceExhausted {
            path: path.display().to_string(),
            operation: "copy bounded frame header".into(),
            requested_bytes: bytes.len(),
        })?;
    result.extend_from_slice(bytes);
    Ok(result)
}

fn reserved_batch_vec<T>(path: &Path, count: usize) -> Result<Vec<T>, FrameError> {
    let requested_bytes = count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(FrameError::Overflow)?;
    let mut result = Vec::new();
    result
        .try_reserve_exact(count)
        .map_err(|_| DurableFileError::ResourceExhausted {
            path: path.display().to_string(),
            operation: "reserve bounded frame batch".into(),
            requested_bytes,
        })?;
    Ok(result)
}

fn prefix(header_bytes: u32, payload_bytes: u64) -> [u8; PREFIX_BYTES] {
    let mut prefix = [0; PREFIX_BYTES];
    prefix[..8].copy_from_slice(MAGIC);
    prefix[8..10].copy_from_slice(&VERSION.to_le_bytes());
    // Bytes 10..12 are reserved, zero flags.
    prefix[12..16].copy_from_slice(&header_bytes.to_le_bytes());
    prefix[16..24].copy_from_slice(&payload_bytes.to_le_bytes());
    let checksum = blake3::derive_key(PREFIX_HASH_CONTEXT, &prefix[..PREFIX_FIELDS_BYTES]);
    prefix[PREFIX_FIELDS_BYTES..].copy_from_slice(&checksum);
    prefix
}

fn incomplete<R: Seek>(
    reader: &mut R,
    path: &Path,
    frame_start: u64,
    expected_end: Option<u64>,
    stage: IncompleteStage,
) -> Result<FrameRead, FrameError> {
    Ok(FrameRead::Incomplete(IncompleteFrame {
        frame_start,
        observed_end: reader
            .stream_position()
            .map_err(|error| io_error(path, error))?,
        expected_end,
        stage,
    }))
}

/// Validate one frame at the current position using fixed-size payload reads.
///
/// The complete bounded header is returned, while payload bytes are only hashed.
/// Only `CleanEof` means no frame began. Incomplete/corrupt outcomes leave the
/// reader after the bytes examined; the caller must stop or explicitly seek.
/// I/O failures remain errors, never EOF/corruption classifications.
pub fn read_frame<R: Read + Seek>(
    reader: &mut R,
    path: &Path,
    limits: FrameLimits,
) -> Result<FrameRead, FrameError> {
    let start = reader
        .stream_position()
        .map_err(|error| io_error(path, error))?;
    let mut encoded_prefix = [0; PREFIX_BYTES];
    match read_full_chunk_with_observer(path.display(), reader, &mut encoded_prefix, |_| {})? {
        DurableReadChunk::CleanEof => return Ok(FrameRead::CleanEof { offset: start }),
        DurableReadChunk::Truncated => {
            return incomplete(reader, path, start, None, IncompleteStage::Prefix);
        }
        DurableReadChunk::Complete => {}
    }
    let corrupt = |reason| -> Result<FrameRead, FrameError> {
        Ok(FrameRead::Corrupt(CorruptFrame {
            frame_start: start,
            reason,
        }))
    };
    if &encoded_prefix[..8] != MAGIC {
        return corrupt(FrameCorruption::Magic);
    }
    let version = u16::from_le_bytes(encoded_prefix[8..10].try_into().expect("fixed field"));
    if version != VERSION {
        return corrupt(FrameCorruption::Version(version));
    }
    if encoded_prefix[10..12] != [0, 0] {
        return corrupt(FrameCorruption::Flags);
    }
    if encoded_prefix[PREFIX_FIELDS_BYTES..]
        != blake3::derive_key(PREFIX_HASH_CONTEXT, &encoded_prefix[..PREFIX_FIELDS_BYTES])
    {
        return corrupt(FrameCorruption::PrefixChecksum);
    }
    let header_bytes = u32::from_le_bytes(encoded_prefix[12..16].try_into().expect("fixed field"));
    let payload_bytes = u64::from_le_bytes(encoded_prefix[16..24].try_into().expect("fixed field"));
    if !limits.permits(u64::from(header_bytes), payload_bytes) {
        return corrupt(FrameCorruption::LengthLimit);
    }
    let Some(payload_offset) = start
        .checked_add(PREFIX_BYTES as u64)
        .and_then(|offset| offset.checked_add(u64::from(header_bytes)))
    else {
        return corrupt(FrameCorruption::OffsetOverflow);
    };
    let Some(end) = payload_offset
        .checked_add(payload_bytes)
        .and_then(|offset| offset.checked_add(TRAILER_BYTES as u64))
    else {
        return corrupt(FrameCorruption::OffsetOverflow);
    };
    let mut header = Vec::new();
    header
        .try_reserve_exact(header_bytes as usize)
        .map_err(|_| DurableFileError::ResourceExhausted {
            path: path.display().to_string(),
            operation: "read bounded frame header".into(),
            requested_bytes: header_bytes as usize,
        })?;
    header.resize(header_bytes as usize, 0);
    if read_full_chunk_with_observer(path.display(), reader, &mut header, |_| {})?
        != DurableReadChunk::Complete
    {
        return incomplete(reader, path, start, Some(end), IncompleteStage::Header);
    }
    let mut digest = blake3::Hasher::new_derive_key(FRAME_HASH_CONTEXT);
    digest.update(&encoded_prefix);
    digest.update(&header);
    let mut buffer = [0; COPY_BUFFER_BYTES];
    let mut remaining = payload_bytes;
    let mut payload_digest = blake3::Hasher::new();
    while remaining > 0 {
        let chunk = remaining.min(buffer.len() as u64) as usize;
        if read_full_chunk_with_observer(path.display(), reader, &mut buffer[..chunk], |_| {})?
            != DurableReadChunk::Complete
        {
            return incomplete(reader, path, start, Some(end), IncompleteStage::Payload);
        }
        digest.update(&buffer[..chunk]);
        payload_digest.update(&buffer[..chunk]);
        remaining -= chunk as u64;
    }
    let mut trailer = [0; TRAILER_BYTES];
    if read_full_chunk_with_observer(path.display(), reader, &mut trailer, |_| {})?
        != DurableReadChunk::Complete
    {
        return incomplete(reader, path, start, Some(end), IncompleteStage::Trailer);
    }
    if &trailer[32..] != TRAILER_MAGIC {
        return corrupt(FrameCorruption::Trailer);
    }
    let checksum = *digest.finalize().as_bytes();
    if trailer[..32] != checksum {
        return corrupt(FrameCorruption::Checksum);
    }
    Ok(FrameRead::Complete(ValidatedFrame {
        header,
        frame_start: start,
        payload_offset,
        payload_bytes,
        frame_end: end,
        checksum,
        payload_checksum: *payload_digest.finalize().as_bytes(),
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TailRecovery {
    /// Refuse any incomplete suffix. This is the default open policy.
    RequireClean,
    /// Truncate an incomplete suffix after a validated prefix, never corruption.
    /// The owner supplies its independently established minimum durable end.
    TruncateIncomplete { minimum_durable_end: u64 },
}

/// Evidence minted only by a successful file and required directory sync.
/// It is intentionally neither constructible nor serializable by callers.
#[derive(Clone, Debug)]
pub struct DurablePrefix {
    owner: Arc<()>,
    end: u64,
    frames: u64,
    digest: [u8; 32],
}

impl DurablePrefix {
    pub fn end(&self) -> u64 {
        self.end
    }
    pub fn frames(&self) -> u64 {
        self.frames
    }
    pub fn digest(&self) -> &[u8; 32] {
        &self.digest
    }
    /// Recovery creates a new local certificate generation, invalidating old
    /// appender associations even when its path and offset happen to match.
    pub fn belongs_to(&self, appender: &FrameAppender) -> bool {
        !appender.needs_recovery && Arc::ptr_eq(&self.owner, &appender.owner)
    }
}

#[derive(Clone, Debug)]
pub struct DurableFrame {
    frame: ValidatedFrame,
    prefix: DurablePrefix,
}

impl DurableFrame {
    pub fn frame(&self) -> &ValidatedFrame {
        &self.frame
    }
    pub fn prefix(&self) -> &DurablePrefix {
        &self.prefix
    }
}

/// Single-owner append handle. Any uncertain write/sync error requires a full
/// validated recovery and a fresh sync before appends can resume.
#[derive(Debug)]
pub struct FrameAppender {
    file: File,
    path: PathBuf,
    limits: FrameLimits,
    owner: Arc<()>,
    prefix: DurablePrefix,
    digest: blake3::Hasher,
    needs_recovery: bool,
}

impl FrameAppender {
    pub fn open(path: &Path, limits: FrameLimits) -> Result<Self, FrameError> {
        Self::open_with_recovery(path, limits, TailRecovery::RequireClean)
    }

    pub fn open_with_recovery(
        path: &Path,
        limits: FrameLimits,
        policy: TailRecovery,
    ) -> Result<Self, FrameError> {
        let opened = open_file_for_append(path)?;
        let owner = Arc::new(());
        let digest = blake3::Hasher::new_derive_key(LOG_HASH_CONTEXT);
        let mut appender = Self {
            file: opened.file,
            path: path.to_owned(),
            limits,
            owner: owner.clone(),
            prefix: DurablePrefix {
                owner,
                end: 0,
                frames: 0,
                digest: *digest.finalize().as_bytes(),
            },
            digest,
            needs_recovery: true,
        };
        appender.recover(policy)?;
        Ok(appender)
    }

    /// Last successfully synchronized prefix. It is not a certificate for any
    /// currently visible tail after a failed append.
    pub fn durable_prefix(&self) -> &DurablePrefix {
        &self.prefix
    }

    pub fn append_bytes(
        &mut self,
        header: &[u8],
        payload: &[u8],
    ) -> Result<DurableFrame, FrameError> {
        self.append(header, &mut io::Cursor::new(payload), payload.len() as u64)
    }

    /// Append a bounded group with one file sync. Every returned frame carries
    /// its own certified prefix, minted only after the entire group is synced.
    /// An interrupted group may leave a recoverable complete prefix; domain
    /// atomicity and retry identities remain the caller's responsibility.
    /// Limits are checked before writing. An empty group performs no I/O and
    /// cannot clear uncertainty from an earlier failed append.
    pub fn append_batch_bytes(
        &mut self,
        records: &[(&[u8], &[u8])],
    ) -> Result<Vec<DurableFrame>, FrameError> {
        self.append_batch_bytes_with_sync(records, |file, path| {
            sync_open_file(file, path, DurableFileSync::All)
        })
    }

    fn append_batch_bytes_with_sync<F>(
        &mut self,
        records: &[(&[u8], &[u8])],
        sync: F,
    ) -> Result<Vec<DurableFrame>, FrameError>
    where
        F: FnOnce(&mut File, &Path) -> Result<(), DurableFileError>,
    {
        if self.needs_recovery {
            return Err(FrameError::RecoveryRequired);
        }
        if records.len() > MAX_BATCH_FRAMES {
            return Err(FrameError::BatchLimit);
        }
        let mut encoded_bytes = 0_u64;
        for (header, payload) in records {
            if !self
                .limits
                .permits(header.len() as u64, payload.len() as u64)
            {
                return Err(FrameError::LengthLimit);
            }
            encoded_bytes = encoded_bytes
                .checked_add(FRAME_OVERHEAD_BYTES)
                .and_then(|bytes| bytes.checked_add(header.len() as u64))
                .and_then(|bytes| bytes.checked_add(payload.len() as u64))
                .ok_or(FrameError::Overflow)?;
            if encoded_bytes > MAX_BATCH_ENCODED_BYTES {
                return Err(FrameError::BatchLimit);
            }
        }
        self.prefix
            .end
            .checked_add(encoded_bytes)
            .ok_or(FrameError::Overflow)?;
        self.prefix
            .frames
            .checked_add(records.len() as u64)
            .ok_or(FrameError::Overflow)?;
        if records.is_empty() {
            return Ok(Vec::new());
        }
        // Reserve all returned evidence before I/O. Headers are the only
        // copied input; payloads remain borrowed and use the streaming writer.
        let mut headers = reserved_batch_vec::<Vec<u8>>(&self.path, records.len())?;
        for (header, _) in records {
            headers.push(header_copy(&self.path, header)?);
        }
        let mut written = reserved_batch_vec::<ValidatedFrame>(&self.path, records.len())?;
        let mut certified = reserved_batch_vec::<DurableFrame>(&self.path, records.len())?;
        let mut start = self.prefix.end;
        self.needs_recovery = true;
        for ((_, payload), header) in records.iter().zip(headers) {
            let frame = self.write_unconfirmed_frame(
                start,
                header,
                &mut io::Cursor::new(*payload),
                payload.len() as u64,
            )?;
            start = frame.frame_end;
            written.push(frame);
        }
        sync(&mut self.file, &self.path)?;
        for frame in written {
            certified.push(self.certify_written_frame(frame)?);
        }
        self.needs_recovery = false;
        Ok(certified)
    }

    /// Consume exactly `payload_bytes` bytes, leaving subsequent reader bytes
    /// untouched. A successful call performs the file durability barrier before
    /// returning a token. Existing segment-directory durability was established
    /// by open/recovery; ordinary appends do not repeat that directory sync.
    pub fn append<R: Read>(
        &mut self,
        header: &[u8],
        payload: &mut R,
        payload_bytes: u64,
    ) -> Result<DurableFrame, FrameError> {
        self.append_with_sync(header, payload, payload_bytes, |file, path| {
            sync_open_file(file, path, DurableFileSync::All)
        })
    }

    fn append_with_sync<R: Read, F>(
        &mut self,
        header: &[u8],
        payload: &mut R,
        payload_bytes: u64,
        sync: F,
    ) -> Result<DurableFrame, FrameError>
    where
        F: FnOnce(&mut File, &Path) -> Result<(), DurableFileError>,
    {
        if self.needs_recovery {
            return Err(FrameError::RecoveryRequired);
        }
        if !self.limits.permits(header.len() as u64, payload_bytes) {
            return Err(FrameError::LengthLimit);
        }
        let header = header_copy(&self.path, header)?;
        let start = self.prefix.end;
        start
            .checked_add(FRAME_OVERHEAD_BYTES)
            .and_then(|end| end.checked_add(header.len() as u64))
            .and_then(|end| end.checked_add(payload_bytes))
            .ok_or(FrameError::Overflow)?;
        self.prefix
            .frames
            .checked_add(1)
            .ok_or(FrameError::Overflow)?;
        self.needs_recovery = true;
        let frame = self.write_unconfirmed_frame(start, header, payload, payload_bytes)?;
        sync(&mut self.file, &self.path)?;
        let frame = self.certify_written_frame(frame)?;
        self.needs_recovery = false;
        Ok(frame)
    }

    /// Write and validate framing without minting durability evidence or
    /// advancing the last certified prefix. The caller owns the final sync.
    fn write_unconfirmed_frame<R: Read>(
        &mut self,
        start: u64,
        header: Vec<u8>,
        payload: &mut R,
        payload_bytes: u64,
    ) -> Result<ValidatedFrame, FrameError> {
        debug_assert!(self.needs_recovery);
        let payload_offset = start
            .checked_add(PREFIX_BYTES as u64)
            .and_then(|offset| offset.checked_add(header.len() as u64))
            .ok_or(FrameError::Overflow)?;
        let end = payload_offset
            .checked_add(payload_bytes)
            .and_then(|offset| offset.checked_add(TRAILER_BYTES as u64))
            .ok_or(FrameError::Overflow)?;
        let observed = self
            .file
            .metadata()
            .map_err(|error| io_error(&self.path, error))?
            .len();
        if observed != start {
            return Err(FrameError::LengthChanged {
                expected: start,
                observed,
            });
        }
        self.file
            .seek(SeekFrom::End(0))
            .map_err(|error| io_error(&self.path, error))?;
        let encoded_prefix = prefix(header.len() as u32, payload_bytes);
        let mut frame_digest = blake3::Hasher::new_derive_key(FRAME_HASH_CONTEXT);
        frame_digest.update(&encoded_prefix);
        frame_digest.update(&header);
        crate::io_trace::write_all(&mut self.file, &encoded_prefix)
            .and_then(|()| crate::io_trace::write_all(&mut self.file, &header))
            .map_err(|error| io_error(&self.path, error))?;
        let mut remaining = payload_bytes;
        let mut buffer = [0; COPY_BUFFER_BYTES];
        let mut payload_digest = blake3::Hasher::new();
        while remaining > 0 {
            let chunk = remaining.min(buffer.len() as u64) as usize;
            let received = payload
                .read(&mut buffer[..chunk])
                .map_err(|error| io_error(&self.path, error))?;
            if received == 0 {
                return Err(FrameError::PayloadEnded {
                    expected: payload_bytes,
                    received: payload_bytes - remaining,
                });
            }
            crate::io_trace::write_all(&mut self.file, &buffer[..received])
                .map_err(|error| io_error(&self.path, error))?;
            frame_digest.update(&buffer[..received]);
            payload_digest.update(&buffer[..received]);
            remaining -= received as u64;
        }
        let checksum = *frame_digest.finalize().as_bytes();
        crate::io_trace::write_all(&mut self.file, &checksum)
            .and_then(|()| crate::io_trace::write_all(&mut self.file, TRAILER_MAGIC))
            .map_err(|error| io_error(&self.path, error))?;
        let observed = self
            .file
            .metadata()
            .map_err(|error| io_error(&self.path, error))?
            .len();
        if observed != end {
            return Err(FrameError::LengthChanged {
                expected: end,
                observed,
            });
        }
        Ok(ValidatedFrame {
            header,
            frame_start: start,
            payload_offset,
            payload_bytes,
            frame_end: end,
            checksum,
            payload_checksum: *payload_digest.finalize().as_bytes(),
        })
    }

    /// Called only after the barrier covering this frame has succeeded.
    fn certify_written_frame(&mut self, frame: ValidatedFrame) -> Result<DurableFrame, FrameError> {
        let frames = self
            .prefix
            .frames
            .checked_add(1)
            .ok_or(FrameError::Overflow)?;
        let mut digest = self.digest.clone();
        extend_prefix_digest(&mut digest, &frame);
        let prefix = DurablePrefix {
            owner: self.owner.clone(),
            end: frame.frame_end,
            frames,
            digest: *digest.finalize().as_bytes(),
        };
        self.digest = digest;
        self.prefix = prefix.clone();
        Ok(DurableFrame { frame, prefix })
    }

    /// Validate the entire current file, optionally remove only an incomplete
    /// suffix, then synchronize before minting a fresh durable-prefix token.
    /// Complete bytes observed after a failed sync never bypass this barrier.
    pub fn recover(&mut self, policy: TailRecovery) -> Result<DurablePrefix, FrameError> {
        self.recover_with_sync(policy, |file, path| {
            sync_open_file(file, path, DurableFileSync::All)
        })
    }

    fn recover_with_sync<F>(
        &mut self,
        policy: TailRecovery,
        sync: F,
    ) -> Result<DurablePrefix, FrameError>
    where
        F: FnOnce(&mut File, &Path) -> Result<(), DurableFileError>,
    {
        self.needs_recovery = true;
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|error| io_error(&self.path, error))?;
        let mut digest = blake3::Hasher::new_derive_key(LOG_HASH_CONTEXT);
        let mut end = 0;
        let mut frames = 0_u64;
        let incomplete = loop {
            match read_frame(&mut self.file, &self.path, self.limits)? {
                FrameRead::Complete(frame) => {
                    extend_prefix_digest(&mut digest, &frame);
                    end = frame.frame_end;
                    frames = frames.checked_add(1).ok_or(FrameError::Overflow)?;
                }
                FrameRead::CleanEof { .. } => break false,
                FrameRead::Incomplete(tail) => {
                    if policy == TailRecovery::RequireClean {
                        return Err(FrameError::Incomplete(tail));
                    }
                    break true;
                }
                FrameRead::Corrupt(frame) => return Err(FrameError::Corrupt(frame)),
            }
        };
        if let TailRecovery::TruncateIncomplete {
            minimum_durable_end,
        } = policy
            && end < minimum_durable_end
        {
            return Err(FrameError::DurablePrefixLost {
                minimum_end: minimum_durable_end,
                validated_end: end,
            });
        }
        if incomplete {
            // The shared helper owns Windows append-handle truncation behavior.
            truncate_open_file_and_sync(&mut self.file, &self.path, end, DurableFileSync::All)?;
        }
        // A separate required sync deliberately remains after truncation: the
        // recovery token has one uniform final barrier, including injected I/O
        // failures. This is an exceptional recovery path, not ordinary append.
        sync(&mut self.file, &self.path)?;
        sync_directory(self.path.parent().unwrap_or_else(|| Path::new(".")))?;
        self.file
            .seek(SeekFrom::End(0))
            .map_err(|error| io_error(&self.path, error))?;
        let owner = Arc::new(());
        let prefix = DurablePrefix {
            owner: owner.clone(),
            end,
            frames,
            digest: *digest.finalize().as_bytes(),
        };
        self.owner = owner;
        self.prefix = prefix.clone();
        self.digest = digest;
        self.needs_recovery = false;
        Ok(prefix)
    }
}

fn extend_prefix_digest(digest: &mut blake3::Hasher, frame: &ValidatedFrame) {
    digest.update(&frame.frame_start.to_le_bytes());
    digest.update(&frame.frame_end.to_le_bytes());
    digest.update(&frame.checksum);
}

#[cfg(test)]
mod tests;
