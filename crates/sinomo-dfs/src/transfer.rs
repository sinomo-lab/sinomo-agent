use crate::{
    ChunkedBlobAssembler, DFS_BLOB_CHUNK_BYTES, DfsError, DfsFileSystemId, DfsObjectId, DfsVersion,
};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

#[derive(Clone, Debug)]
pub(crate) struct PendingNetworkedWrite {
    pub(crate) object_id: DfsObjectId,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: String,
    pub(crate) size_bytes: u64,
    pub(crate) stage_retries: BTreeMap<NodeId, RepairRetryState>,
    pub(crate) repair_retries: BTreeMap<NodeId, RepairRetryState>,
    pub(crate) stage_inflight_request_ids: BTreeMap<NodeId, String>,
    pub(crate) stage_next_chunk_indexes: BTreeMap<NodeId, u32>,
    pub(crate) repair_inflight_request_ids: BTreeMap<NodeId, String>,
    pub(crate) repair_next_chunk_indexes: BTreeMap<NodeId, u32>,
    pub(crate) fetch_inflight_from: BTreeSet<NodeId>,
    pub(crate) fetch_inflight_request_ids: BTreeMap<NodeId, String>,
    pub(crate) fetch_inflight_started_ticks: BTreeMap<NodeId, u64>,
    pub(crate) fetch_failed_from: BTreeSet<NodeId>,
}

#[derive(Clone, Debug)]
pub(crate) struct PendingNetworkedRead {
    pub(crate) object_id: DfsObjectId,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: Option<String>,
    pub(crate) content_size_bytes: Option<u64>,
    pub(crate) range_offset: Option<u64>,
    pub(crate) range_max_bytes: Option<usize>,
    pub(crate) target_node_id: NodeId,
    pub(crate) next_retry_at: Instant,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RepairRetryState {
    pub(crate) attempts: u32,
    pub(crate) next_retry_tick: u64,
    pub(crate) inflight: bool,
}

#[derive(Debug)]
pub(crate) struct InProgressReadTransfer {
    pub(crate) object_id: DfsObjectId,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: String,
    pub(crate) total_size_bytes: u64,
    pub(crate) last_updated_at: Instant,
    pub(crate) assembler: ChunkedBlobAssembler,
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
        validate_chunked_transfer_shape(
            "chunked DFS read response",
            total_size_bytes,
            total_chunks,
        )?;
        Ok(Self {
            object_id,
            version,
            content_hash,
            total_size_bytes,
            last_updated_at: Instant::now(),
            assembler,
        })
    }

    pub(crate) fn insert_chunk(
        &mut self,
        chunk_index: u32,
        total_chunks: u32,
        bytes: Vec<u8>,
    ) -> Result<bool, DfsError> {
        validate_chunk_bytes(
            "chunked DFS read response",
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
}

#[derive(Debug)]
pub(crate) struct InProgressFetchBlobTransfer {
    pub(crate) object_id: DfsObjectId,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: String,
    pub(crate) total_size_bytes: u64,
    pub(crate) last_updated_at: Instant,
    pub(crate) assembler: ChunkedBlobAssembler,
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
        validate_chunked_transfer_shape(
            "chunked DFS fetch response",
            total_size_bytes,
            total_chunks,
        )?;
        Ok(Self {
            object_id,
            version,
            content_hash,
            total_size_bytes,
            last_updated_at: Instant::now(),
            assembler,
        })
    }

    pub(crate) fn insert_chunk(
        &mut self,
        chunk_index: u32,
        total_chunks: u32,
        bytes: Vec<u8>,
    ) -> Result<bool, DfsError> {
        validate_chunk_bytes(
            "chunked DFS fetch response",
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
}

#[derive(Debug)]
pub(crate) struct InProgressBlobTransfer {
    pub(crate) request_id: String,
    pub(crate) intent_id: String,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) object_id: DfsObjectId,
    pub(crate) expected_base_version: Option<DfsVersion>,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: String,
    pub(crate) total_size_bytes: u64,
    pub(crate) last_updated_at: Instant,
    pub(crate) assembler: ChunkedBlobAssembler,
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
        total_size_bytes: u64,
        total_chunks: u32,
        assembler: ChunkedBlobAssembler,
    ) -> Result<Self, DfsError> {
        validate_chunked_transfer_shape(
            "chunked DFS blob transfer",
            total_size_bytes,
            total_chunks,
        )?;
        Ok(Self {
            request_id,
            intent_id,
            file_system_id,
            object_id,
            expected_base_version,
            version,
            content_hash,
            total_size_bytes,
            last_updated_at: Instant::now(),
            assembler,
        })
    }

    pub(crate) fn insert_chunk(
        &mut self,
        chunk_index: u32,
        total_chunks: u32,
        bytes: Vec<u8>,
    ) -> Result<bool, DfsError> {
        validate_chunk_bytes(
            "chunked DFS blob transfer",
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
}

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
