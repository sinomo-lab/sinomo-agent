use crate::{
    ChunkedBlobAssembler, DfsError, DfsFileSystemId, DfsObjectId, DfsObjectMetadata,
    DfsTransferProfile, DfsVersion,
};
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

pub(crate) const DFS_BLOB_CHUNK_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const DFS_STAGE_CHUNK_WINDOW_SIZE: u32 = 4;
pub(crate) const DFS_READ_CHUNK_WINDOW_SIZE: u32 = 4;
pub(crate) const DFS_FETCH_CHUNK_WINDOW_SIZE: u32 = 4;
pub(crate) const DFS_LAN_CHUNK_WINDOW_SIZE: u32 = 16;
pub(crate) const DFS_LOCAL_CHUNK_WINDOW_SIZE: u32 = 16;
pub(crate) const DFS_ADAPTIVE_TRANSFER_WINDOW_MAX_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const DFS_REPAIR_RETRY_SUPPRESSED_TICK: u64 = u64::MAX;
const DFS_REPAIR_RETRY_TICK_DURATION: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub(crate) struct PendingNetworkedWrite {
    pub(crate) object_id: DfsObjectId,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: String,
    pub(crate) size_bytes: u64,
    pub(crate) patch: Option<PendingNetworkedWritePatch>,
    pub(crate) stage_retries: BTreeMap<NodeId, RepairRetryState>,
    pub(crate) repair_retries: BTreeMap<NodeId, RepairRetryState>,
    pub(crate) stage_inflight_request_ids: BTreeMap<NodeId, String>,
    pub(crate) stage_next_chunk_indexes: BTreeMap<NodeId, u32>,
    pub(crate) repair_inflight_request_ids: BTreeMap<NodeId, String>,
    pub(crate) repair_next_chunk_indexes: BTreeMap<NodeId, u32>,
    pub(crate) fetch_inflight_from: BTreeSet<NodeId>,
    pub(crate) fetch_inflight_request_ids: BTreeMap<NodeId, String>,
    pub(crate) fetch_inflight_started_ticks: BTreeMap<NodeId, u64>,
    pub(crate) fetch_next_chunk_indexes: BTreeMap<NodeId, u32>,
    pub(crate) fetch_window_end_chunk_indexes: BTreeMap<NodeId, u32>,
    pub(crate) fetch_failed_from: BTreeSet<NodeId>,
}

impl PendingNetworkedWrite {
    pub(crate) fn new(
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: String,
        size_bytes: u64,
    ) -> Self {
        Self::with_patch(object_id, version, content_hash, size_bytes, None)
    }

    pub(crate) fn with_patch(
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: String,
        size_bytes: u64,
        patch: Option<PendingNetworkedWritePatch>,
    ) -> Self {
        Self {
            object_id,
            version,
            content_hash,
            size_bytes,
            patch,
            stage_retries: BTreeMap::new(),
            repair_retries: BTreeMap::new(),
            stage_inflight_request_ids: BTreeMap::new(),
            stage_next_chunk_indexes: BTreeMap::new(),
            repair_inflight_request_ids: BTreeMap::new(),
            repair_next_chunk_indexes: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
            fetch_next_chunk_indexes: BTreeMap::new(),
            fetch_window_end_chunk_indexes: BTreeMap::new(),
            fetch_failed_from: BTreeSet::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PendingNetworkedWritePatch {
    pub(crate) base_version: DfsVersion,
    pub(crate) offset: u64,
    pub(crate) patch_hash: String,
    pub(crate) patch_size_bytes: u64,
    pub(crate) full_stage_fallback_nodes: BTreeSet<NodeId>,
}

#[derive(Clone, Debug)]
pub(crate) struct PendingNetworkedRead {
    pub(crate) object_id: DfsObjectId,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: Option<String>,
    pub(crate) content_size_bytes: Option<u64>,
    pub(crate) range_offset: Option<u64>,
    pub(crate) range_max_bytes: Option<usize>,
    pub(crate) next_chunk_index: u32,
    pub(crate) window_end_chunk_index: Option<u32>,
    pub(crate) target_node_id: NodeId,
    pub(crate) next_retry_at: Instant,
    pub(crate) cache_response: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RepairRetryState {
    pub(crate) attempts: u32,
    pub(crate) next_retry_tick: u64,
    pub(crate) inflight: bool,
}

pub(crate) fn repair_retry_delay_ticks(attempts: u32) -> u64 {
    1_u64 << attempts.saturating_sub(1).min(6)
}

pub(crate) fn repair_retry_delay_nanos_for_ticks(ticks: u64) -> u64 {
    let tick_nanos = u64::try_from(DFS_REPAIR_RETRY_TICK_DURATION.as_nanos()).unwrap_or(u64::MAX);
    ticks.saturating_mul(tick_nanos)
}

pub(crate) fn repair_retry_state_is_suppressed(state: &RepairRetryState) -> bool {
    state.next_retry_tick == DFS_REPAIR_RETRY_SUPPRESSED_TICK
}

pub(crate) fn wake_repair_retry_state(state: &mut RepairRetryState) {
    if !repair_retry_state_is_suppressed(state) {
        state.next_retry_tick = 0;
    }
}

pub(crate) fn stage_inflight_retry_delay_ticks(attempts: u32) -> u64 {
    repair_retry_delay_ticks(attempts).saturating_add(1)
}

pub(crate) fn fetch_inflight_timeout_ticks() -> u64 {
    stage_inflight_retry_delay_ticks(2)
}

pub(crate) fn repair_transfer_inflight_timeout_ticks(total_size_bytes: u64) -> u64 {
    let chunk_count = total_size_bytes
        .div_ceil(DFS_BLOB_CHUNK_BYTES as u64)
        .max(1);
    let window_chunks = u64::from(DFS_STAGE_CHUNK_WINDOW_SIZE.max(1));
    let repair_windows = chunk_count.div_ceil(window_chunks).max(1);
    fetch_inflight_timeout_ticks().saturating_mul(repair_windows)
}

#[derive(Debug)]
pub(crate) struct ChunkedTransferState {
    pub(crate) total_size_bytes: u64,
    pub(crate) last_updated_at: Instant,
    pub(crate) assembler: ChunkedBlobAssembler,
    label: &'static str,
}

impl ChunkedTransferState {
    pub(crate) fn new(
        label: &'static str,
        total_size_bytes: u64,
        total_chunks: u32,
        assembler: ChunkedBlobAssembler,
    ) -> Result<Self, DfsError> {
        validate_chunked_transfer_shape(label, total_size_bytes, total_chunks)?;
        Ok(Self {
            total_size_bytes,
            last_updated_at: Instant::now(),
            assembler,
            label,
        })
    }

    pub(crate) fn insert_chunk(
        &mut self,
        chunk_index: u32,
        total_chunks: u32,
        bytes: Vec<u8>,
    ) -> Result<bool, DfsError> {
        validate_chunk_bytes(
            self.label,
            self.total_size_bytes,
            chunk_index,
            total_chunks,
            &bytes,
        )?;
        self.last_updated_at = Instant::now();
        self.assembler
            .insert_chunk(chunk_index, &bytes)
            .map_err(DfsError::from)
    }

    pub(crate) fn contiguous_received_chunk_count(&self) -> u32 {
        self.assembler.contiguous_received_chunk_count()
    }
}

macro_rules! impl_chunked_transfer_common {
    ($transfer:ty) => {
        impl $transfer {
            pub(crate) fn total_size_bytes(&self) -> u64 {
                self.chunks.total_size_bytes
            }

            pub(crate) fn last_updated_at(&self) -> Instant {
                self.chunks.last_updated_at
            }

            pub(crate) fn insert_chunk(
                &mut self,
                chunk_index: u32,
                total_chunks: u32,
                bytes: Vec<u8>,
            ) -> Result<bool, DfsError> {
                self.chunks.insert_chunk(chunk_index, total_chunks, bytes)
            }

            pub(crate) fn contiguous_received_chunk_count(&self) -> u32 {
                self.chunks.contiguous_received_chunk_count()
            }

            #[cfg(test)]
            #[allow(dead_code)]
            pub(crate) fn set_last_updated_at_for_test(&mut self, last_updated_at: Instant) {
                self.chunks.last_updated_at = last_updated_at;
            }
        }
    };
}

#[derive(Debug)]
pub(crate) struct InProgressReadTransfer {
    pub(crate) object_id: DfsObjectId,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: String,
    pub(crate) chunks: ChunkedTransferState,
}

impl InProgressReadTransfer {
    pub(crate) fn new(
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: String,
        total_size_bytes: u64,
        total_chunks: u32,
        assembler: ChunkedBlobAssembler,
    ) -> Result<Self, DfsError> {
        Ok(Self {
            object_id,
            version,
            content_hash,
            chunks: ChunkedTransferState::new(
                "chunked DFS read response",
                total_size_bytes,
                total_chunks,
                assembler,
            )?,
        })
    }
}

impl_chunked_transfer_common!(InProgressReadTransfer);

#[derive(Debug)]
pub(crate) struct InProgressReadMemoryTransfer {
    pub(crate) object_id: DfsObjectId,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: String,
    total_size_bytes: u64,
    total_chunks: u32,
    chunk_size_bytes: usize,
    received_chunks: Vec<Option<Vec<u8>>>,
    received_count: u32,
    last_updated_at: Instant,
}

impl InProgressReadMemoryTransfer {
    pub(crate) fn new(
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: String,
        total_size_bytes: u64,
        total_chunks: u32,
        chunk_size_bytes: usize,
    ) -> Result<Self, DfsError> {
        validate_chunked_transfer_shape(
            "chunked DFS read response",
            total_size_bytes,
            total_chunks,
        )?;
        let received_len = usize::try_from(total_chunks).map_err(|_| DfsError::Distributed {
            message: format!("chunk count is too large for this platform: {total_chunks}"),
        })?;
        Ok(Self {
            object_id,
            version,
            content_hash,
            total_size_bytes,
            total_chunks,
            chunk_size_bytes,
            received_chunks: vec![None; received_len],
            received_count: 0,
            last_updated_at: Instant::now(),
        })
    }

    pub(crate) fn total_size_bytes(&self) -> u64 {
        self.total_size_bytes
    }

    pub(crate) fn last_updated_at(&self) -> Instant {
        self.last_updated_at
    }

    pub(crate) fn insert_chunk(
        &mut self,
        chunk_index: u32,
        total_chunks: u32,
        bytes: Vec<u8>,
    ) -> Result<bool, DfsError> {
        if total_chunks != self.total_chunks {
            return Err(DfsError::Distributed {
                message: format!(
                    "chunked DFS read response chunk count changed: expected {}, actual {total_chunks}",
                    self.total_chunks
                ),
            });
        }
        validate_chunk_bytes(
            "chunked DFS read response",
            self.total_size_bytes,
            chunk_index,
            total_chunks,
            &bytes,
        )?;
        let index = usize::try_from(chunk_index).map_err(|_| DfsError::Distributed {
            message: format!("chunk index is too large for this platform: {chunk_index}"),
        })?;
        self.last_updated_at = Instant::now();
        if let Some(existing) = self.received_chunks.get(index).and_then(Option::as_ref) {
            if existing != &bytes {
                return Err(DfsError::ContentHashMismatch {
                    expected: format!("consistent chunk {chunk_index}"),
                    actual: "conflicting duplicate chunk".to_string(),
                });
            }
            return Ok(self.is_complete());
        }
        let Some(slot) = self.received_chunks.get_mut(index) else {
            return Err(DfsError::Distributed {
                message: format!(
                    "chunk index out of range: {chunk_index}/{}",
                    self.total_chunks
                ),
            });
        };
        *slot = Some(bytes);
        self.received_count += 1;
        Ok(self.is_complete())
    }

    pub(crate) fn contiguous_received_chunk_count(&self) -> u32 {
        self.received_chunks
            .iter()
            .take_while(|chunk| chunk.is_some())
            .count()
            .try_into()
            .expect("chunk count fits in u32")
    }

    pub(crate) fn into_bytes(self) -> Result<Vec<u8>, DfsError> {
        if !self.is_complete() {
            return Err(DfsError::Distributed {
                message: format!(
                    "incomplete chunked DFS read response for object {}, version {}",
                    self.object_id, self.version
                ),
            });
        }
        let capacity =
            usize::try_from(self.total_size_bytes).map_err(|_| DfsError::Distributed {
                message: format!(
                    "chunked DFS read response is too large for this platform: {}",
                    self.total_size_bytes
                ),
            })?;
        let mut bytes = Vec::with_capacity(capacity);
        for (chunk_index, chunk) in self.received_chunks.into_iter().enumerate() {
            let Some(chunk) = chunk else {
                return Err(DfsError::Distributed {
                    message: format!("missing chunked DFS read response chunk {chunk_index}"),
                });
            };
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() as u64 != self.total_size_bytes {
            return Err(DfsError::Distributed {
                message: format!(
                    "assembled chunked DFS read response has wrong size: expected {}, actual {}",
                    self.total_size_bytes,
                    bytes.len()
                ),
            });
        }
        let actual_hash = chunk_hash(&bytes);
        if actual_hash != self.content_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: self.content_hash,
                actual: actual_hash,
            });
        }
        Ok(bytes)
    }

    fn is_complete(&self) -> bool {
        self.received_count == self.total_chunks
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn set_last_updated_at_for_test(&mut self, last_updated_at: Instant) {
        self.last_updated_at = last_updated_at;
    }

    #[allow(dead_code)]
    fn chunk_size_bytes(&self) -> usize {
        self.chunk_size_bytes
    }
}

#[derive(Debug)]
pub(crate) struct InProgressFetchBlobTransfer {
    pub(crate) object_id: DfsObjectId,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: String,
    pub(crate) chunks: ChunkedTransferState,
}

impl InProgressFetchBlobTransfer {
    pub(crate) fn new(
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: String,
        total_size_bytes: u64,
        total_chunks: u32,
        assembler: ChunkedBlobAssembler,
    ) -> Result<Self, DfsError> {
        Ok(Self {
            object_id,
            version,
            content_hash,
            chunks: ChunkedTransferState::new(
                "chunked DFS fetch response",
                total_size_bytes,
                total_chunks,
                assembler,
            )?,
        })
    }
}

impl_chunked_transfer_common!(InProgressFetchBlobTransfer);

#[derive(Debug)]
pub(crate) struct InProgressBlobTransfer {
    pub(crate) request_id: String,
    pub(crate) intent_id: String,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) object_id: DfsObjectId,
    pub(crate) expected_base_version: Option<DfsVersion>,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: String,
    pub(crate) object_metadata: Option<DfsObjectMetadata>,
    pub(crate) chunks: ChunkedTransferState,
}

impl InProgressBlobTransfer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        request_id: String,
        intent_id: String,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: DfsVersion,
        content_hash: String,
        object_metadata: Option<DfsObjectMetadata>,
        total_size_bytes: u64,
        total_chunks: u32,
        assembler: ChunkedBlobAssembler,
    ) -> Result<Self, DfsError> {
        Ok(Self {
            request_id,
            intent_id,
            file_system_id,
            object_id,
            expected_base_version,
            version,
            content_hash,
            object_metadata,
            chunks: ChunkedTransferState::new(
                "chunked DFS blob transfer",
                total_size_bytes,
                total_chunks,
                assembler,
            )?,
        })
    }
}

impl_chunked_transfer_common!(InProgressBlobTransfer);

#[derive(Debug)]
pub(crate) struct InProgressPatchTransfer {
    pub(crate) request_id: String,
    pub(crate) intent_id: String,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) object_id: DfsObjectId,
    pub(crate) base_version: DfsVersion,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: String,
    pub(crate) offset: u64,
    pub(crate) patch_hash: String,
    pub(crate) chunks: ChunkedTransferState,
}

impl InProgressPatchTransfer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        request_id: String,
        intent_id: String,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        base_version: DfsVersion,
        version: DfsVersion,
        content_hash: String,
        offset: u64,
        patch_hash: String,
        total_size_bytes: u64,
        total_chunks: u32,
        assembler: ChunkedBlobAssembler,
    ) -> Result<Self, DfsError> {
        Ok(Self {
            request_id,
            intent_id,
            file_system_id,
            object_id,
            base_version,
            version,
            content_hash,
            offset,
            patch_hash,
            chunks: ChunkedTransferState::new(
                "chunked DFS patch transfer",
                total_size_bytes,
                total_chunks,
                assembler,
            )?,
        })
    }
}

impl_chunked_transfer_common!(InProgressPatchTransfer);

pub(crate) fn blob_chunk_count(bytes: &[u8]) -> Result<u32, DfsError> {
    blob_chunk_count_for_size(bytes.len() as u64)
}

pub(crate) fn blob_chunk_count_for_size(size_bytes: u64) -> Result<u32, DfsError> {
    let chunk_size = DFS_BLOB_CHUNK_BYTES as u64;
    let chunks = size_bytes.div_ceil(chunk_size);
    u32::try_from(chunks).map_err(|_| DfsError::Distributed {
        message: format!("DFS blob is too large to transfer in {DFS_BLOB_CHUNK_BYTES}-byte chunks"),
    })
}

pub(crate) fn dfs_adaptive_chunk_window_size(
    profile: DfsTransferProfile,
    total_size_bytes: u64,
    base_window_chunks: u32,
) -> u32 {
    let total_chunks = total_size_bytes
        .div_ceil(DFS_BLOB_CHUNK_BYTES as u64)
        .clamp(1, u64::from(u32::MAX)) as u32;
    let profile_window = match profile {
        DfsTransferProfile::Conservative => base_window_chunks,
        DfsTransferProfile::Lan => base_window_chunks.max(DFS_LAN_CHUNK_WINDOW_SIZE),
        DfsTransferProfile::Local => base_window_chunks.max(DFS_LOCAL_CHUNK_WINDOW_SIZE),
    };
    let budget_window = (DFS_ADAPTIVE_TRANSFER_WINDOW_MAX_BYTES / DFS_BLOB_CHUNK_BYTES as u64)
        .clamp(1, u64::from(u32::MAX)) as u32;
    profile_window.min(budget_window).min(total_chunks).max(1)
}

pub(crate) fn blob_chunk_len(total_size_bytes: u64, chunk_index: u32) -> Result<usize, DfsError> {
    let chunk_offset = u64::from(chunk_index) * DFS_BLOB_CHUNK_BYTES as u64;
    let chunk_len = total_size_bytes
        .saturating_sub(chunk_offset)
        .min(DFS_BLOB_CHUNK_BYTES as u64);
    usize::try_from(chunk_len).map_err(|_| DfsError::Distributed {
        message: format!("DFS blob chunk is too large to fit in memory: {chunk_len} bytes"),
    })
}

pub(crate) fn chunk_prefix_bytes(total_size_bytes: u64, chunk_count: u32) -> u64 {
    total_size_bytes.min(u64::from(chunk_count).saturating_mul(DFS_BLOB_CHUNK_BYTES as u64))
}

pub(crate) fn pending_stage_transfer_size_bytes(
    pending: &PendingNetworkedWrite,
    replica_node_id: &NodeId,
) -> u64 {
    pending
        .patch
        .as_ref()
        .filter(|patch| !patch.full_stage_fallback_nodes.contains(replica_node_id))
        .map(|patch| patch.patch_size_bytes)
        .unwrap_or(pending.size_bytes)
}

pub(crate) fn range_transfer_size_bytes(
    object_total_size_bytes: u64,
    requested_offset: u64,
    requested_max_bytes: usize,
) -> u64 {
    let start = requested_offset.min(object_total_size_bytes);
    let max_bytes = u64::try_from(requested_max_bytes).unwrap_or(u64::MAX);
    object_total_size_bytes.saturating_sub(start).min(max_bytes)
}

pub(crate) fn dfs_byte_count_to_usize(label: &str, value: u64) -> Result<usize, DfsError> {
    usize::try_from(value).map_err(|_| DfsError::Distributed {
        message: format!("{label} byte count is too large for this platform: {value}"),
    })
}

pub(crate) fn fetch_response_window_end(
    start_chunk_index: u32,
    total_chunks: u32,
    max_chunks: u32,
) -> u32 {
    if max_chunks == 0 {
        return total_chunks;
    }
    start_chunk_index
        .saturating_add(max_chunks)
        .min(total_chunks)
}

pub(crate) fn repair_replica_chunk_start_and_window(
    requested_start_chunk_index: u32,
    requested_max_chunks: u32,
    total_chunks: u32,
) -> (u32, u32) {
    if requested_start_chunk_index >= total_chunks {
        return (total_chunks, 1);
    }
    if requested_start_chunk_index
        .checked_add(1)
        .is_some_and(|next| next == total_chunks)
    {
        return (requested_start_chunk_index, 1);
    }
    (
        requested_start_chunk_index,
        requested_max_chunks
            .max(1)
            .min(total_chunks.saturating_sub(requested_start_chunk_index)),
    )
}

pub(crate) fn read_response_window_range_len(
    start_chunk_index: u32,
    total_size_bytes: u64,
    max_chunks: u32,
) -> Result<usize, DfsError> {
    let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
    if start_chunk_index >= total_chunks {
        return Err(DfsError::Distributed {
            message: format!(
                "DFS read chunk window starts outside object: start={}, total_chunks={total_chunks}",
                start_chunk_index
            ),
        });
    }
    let end_chunk_index = fetch_response_window_end(start_chunk_index, total_chunks, max_chunks);
    let start_offset = u64::from(start_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64;
    let end_offset = if end_chunk_index >= total_chunks {
        total_size_bytes
    } else {
        u64::from(end_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64
    };
    usize::try_from(end_offset.saturating_sub(start_offset)).map_err(|_| DfsError::Distributed {
        message: format!("DFS read chunk window is too large: {start_offset}..{end_offset}"),
    })
}

pub(crate) fn validate_chunked_transfer_shape(
    label: &str,
    total_size_bytes: u64,
    total_chunks: u32,
) -> Result<usize, DfsError> {
    if total_chunks == 0 {
        return Err(DfsError::Distributed {
            message: format!("{label} has no chunks"),
        });
    }
    usize::try_from(total_size_bytes).map_err(|_| DfsError::Distributed {
        message: format!("{label} is too large: {total_size_bytes} bytes"),
    })?;
    let chunk_size = DFS_BLOB_CHUNK_BYTES as u64;
    let expected_chunks = total_size_bytes.div_ceil(chunk_size);
    if expected_chunks == 0 || expected_chunks > u32::MAX as u64 {
        return Err(DfsError::Distributed {
            message: format!("{label} cannot be represented in {DFS_BLOB_CHUNK_BYTES}-byte chunks"),
        });
    }
    if expected_chunks != total_chunks as u64 {
        return Err(DfsError::Distributed {
            message: format!(
                "{label} chunk count does not match size: expected {expected_chunks}, actual {total_chunks}"
            ),
        });
    }
    Ok(total_chunks as usize)
}

pub(crate) fn validate_chunk_bytes(
    label: &str,
    total_size_bytes: u64,
    chunk_index: u32,
    total_chunks: u32,
    bytes: &[u8],
) -> Result<(), DfsError> {
    validate_chunked_transfer_shape(label, total_size_bytes, total_chunks)?;
    if chunk_index >= total_chunks {
        return Err(DfsError::Distributed {
            message: format!("{label} chunk index out of range: {chunk_index}/{total_chunks}"),
        });
    }
    let chunk_size = DFS_BLOB_CHUNK_BYTES as u64;
    let expected_len = if chunk_index + 1 == total_chunks {
        // `validate_chunked_transfer_shape` guarantees this stays in range, but use
        // saturating arithmetic so a malformed shape can never overflow or underflow.
        let consumed = chunk_size.saturating_mul(u64::from(chunk_index));
        total_size_bytes.saturating_sub(consumed)
    } else {
        chunk_size
    };
    if bytes.len() as u64 != expected_len {
        return Err(DfsError::Distributed {
            message: format!(
                "{label} chunk length mismatch at {chunk_index}: expected {expected_len}, actual {}",
                bytes.len()
            ),
        });
    }
    Ok(())
}

pub(crate) fn validate_chunk_hash(
    label: &str,
    chunk_index: u32,
    bytes: &[u8],
    expected_hash: Option<&str>,
) -> Result<(), DfsError> {
    let Some(expected_hash) = expected_hash else {
        return Ok(());
    };
    let actual_hash = chunk_hash(bytes);
    if actual_hash != expected_hash {
        return Err(DfsError::ContentHashMismatch {
            expected: format!("{label} chunk {chunk_index} hash {expected_hash}"),
            actual: actual_hash,
        });
    }
    Ok(())
}

pub(crate) fn chunk_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

pub(crate) fn blob_transfer_key(
    kind: &str,
    source_node_id: &NodeId,
    intent_id: &str,
    request_id: &str,
) -> String {
    format!("{kind}:{source_node_id}:{intent_id}:{request_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_chunk_hash_accepts_absent_or_matching_hash_and_rejects_mismatch() {
        let bytes = b"checksummed DFS transfer chunk";
        validate_chunk_hash("chunked DFS blob transfer", 0, bytes, None)
            .expect("legacy chunk without hash remains compatible");
        validate_chunk_hash(
            "chunked DFS blob transfer",
            0,
            bytes,
            Some(chunk_hash(bytes).as_str()),
        )
        .expect("matching chunk hash");

        let error = validate_chunk_hash(
            "chunked DFS blob transfer",
            0,
            bytes,
            Some(chunk_hash(b"different chunk").as_str()),
        )
        .expect_err("mismatched chunk hash");
        assert!(matches!(error, DfsError::ContentHashMismatch { .. }));
    }
}
