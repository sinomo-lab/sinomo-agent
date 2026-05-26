use crate::{
    DFS_BLOB_CHUNK_BYTES, DfsError, DfsFileSystemId, DfsObjectId, DfsObjectMetadata, DfsVersion,
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
    pub(crate) repair_inflight_request_ids: BTreeMap<NodeId, String>,
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
    pub(crate) target_node_id: NodeId,
    pub(crate) next_retry_at: Instant,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RepairRetryState {
    pub(crate) attempts: u32,
    pub(crate) next_retry_tick: u64,
    pub(crate) inflight: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct InProgressReadTransfer {
    pub(crate) object_id: DfsObjectId,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: Option<String>,
    pub(crate) total_size_bytes: u64,
    pub(crate) last_updated_at: Instant,
    pub(crate) chunks: Vec<Option<Vec<u8>>>,
}

impl InProgressReadTransfer {
    pub(crate) fn new(
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: Option<String>,
        total_size_bytes: u64,
        total_chunks: u32,
    ) -> Result<Self, DfsError> {
        let total_chunks_len = validate_chunked_transfer_shape(
            "chunked DFS read response",
            total_size_bytes,
            total_chunks,
        )?;
        usize::try_from(total_size_bytes).map_err(|_| DfsError::Distributed {
            message: format!("chunked DFS read response is too large: {total_size_bytes} bytes"),
        })?;
        Ok(Self {
            object_id,
            version,
            content_hash,
            total_size_bytes,
            last_updated_at: Instant::now(),
            chunks: vec![None; total_chunks_len],
        })
    }

    pub(crate) fn insert_chunk(
        &mut self,
        chunk_index: u32,
        total_chunks: u32,
        bytes: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, DfsError> {
        insert_chunk_into(
            "chunked DFS read response",
            self.total_size_bytes,
            &mut self.last_updated_at,
            &mut self.chunks,
            chunk_index,
            total_chunks,
            bytes,
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct InProgressFetchBlobTransfer {
    pub(crate) object_id: DfsObjectId,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: String,
    pub(crate) total_size_bytes: u64,
    pub(crate) last_updated_at: Instant,
    pub(crate) chunks: Vec<Option<Vec<u8>>>,
}

impl InProgressFetchBlobTransfer {
    pub(crate) fn new(
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: String,
        total_size_bytes: u64,
        total_chunks: u32,
    ) -> Result<Self, DfsError> {
        let total_chunks_len = validate_chunked_transfer_shape(
            "chunked DFS fetch response",
            total_size_bytes,
            total_chunks,
        )?;
        usize::try_from(total_size_bytes).map_err(|_| DfsError::Distributed {
            message: format!("chunked DFS fetch response is too large: {total_size_bytes} bytes"),
        })?;
        Ok(Self {
            object_id,
            version,
            content_hash,
            total_size_bytes,
            last_updated_at: Instant::now(),
            chunks: vec![None; total_chunks_len],
        })
    }

    pub(crate) fn insert_chunk(
        &mut self,
        chunk_index: u32,
        total_chunks: u32,
        bytes: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, DfsError> {
        insert_chunk_into(
            "chunked DFS fetch response",
            self.total_size_bytes,
            &mut self.last_updated_at,
            &mut self.chunks,
            chunk_index,
            total_chunks,
            bytes,
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct InProgressBlobTransfer {
    pub(crate) request_id: String,
    pub(crate) intent_id: String,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) object_id: DfsObjectId,
    pub(crate) expected_base_version: Option<DfsVersion>,
    pub(crate) version: DfsVersion,
    pub(crate) content_hash: String,
    pub(crate) total_size_bytes: u64,
    pub(crate) object_metadata: Option<DfsObjectMetadata>,
    pub(crate) last_updated_at: Instant,
    pub(crate) chunks: Vec<Option<Vec<u8>>>,
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
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<Self, DfsError> {
        let total_chunks_len = validate_chunked_transfer_shape(
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
            object_metadata,
            last_updated_at: Instant::now(),
            chunks: vec![None; total_chunks_len],
        })
    }

    pub(crate) fn insert_chunk(
        &mut self,
        chunk_index: u32,
        total_chunks: u32,
        bytes: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, DfsError> {
        insert_chunk_into(
            "chunked DFS blob transfer",
            self.total_size_bytes,
            &mut self.last_updated_at,
            &mut self.chunks,
            chunk_index,
            total_chunks,
            bytes,
        )
    }
}

pub(crate) fn blob_chunk_count(bytes: &[u8]) -> Result<u32, DfsError> {
    let chunks = bytes.len().div_ceil(DFS_BLOB_CHUNK_BYTES);
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
        total_size_bytes - chunk_size * u64::from(chunk_index)
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

pub(crate) fn blob_transfer_key(
    kind: &str,
    source_node_id: &NodeId,
    intent_id: &str,
    request_id: &str,
) -> String {
    format!("{kind}:{source_node_id}:{intent_id}:{request_id}")
}

fn insert_chunk_into(
    label: &str,
    total_size_bytes: u64,
    last_updated_at: &mut Instant,
    chunks: &mut [Option<Vec<u8>>],
    chunk_index: u32,
    total_chunks: u32,
    bytes: Vec<u8>,
) -> Result<Option<Vec<u8>>, DfsError> {
    validate_chunk_bytes(label, total_size_bytes, chunk_index, total_chunks, &bytes)?;
    if total_chunks as usize != chunks.len() {
        return Err(DfsError::Distributed {
            message: format!(
                "{label} chunk count changed: expected {}, actual {}",
                chunks.len(),
                total_chunks
            ),
        });
    }
    let Some(slot) = chunks.get_mut(chunk_index as usize) else {
        return Err(DfsError::Distributed {
            message: format!("{label} index out of range: {chunk_index}/{total_chunks}"),
        });
    };
    if let Some(existing) = slot.as_ref() {
        if existing == &bytes {
            return Ok(None);
        }
        return Err(DfsError::Distributed {
            message: format!("conflicting duplicate {label} index: {chunk_index}"),
        });
    }
    *slot = Some(bytes);
    *last_updated_at = Instant::now();
    if chunks.iter().any(Option::is_none) {
        return Ok(None);
    }

    let mut assembled = Vec::with_capacity(usize::try_from(total_size_bytes).map_err(|_| {
        DfsError::Distributed {
            message: format!("{label} is too large: {total_size_bytes} bytes"),
        }
    })?);
    for chunk in chunks {
        assembled.extend_from_slice(chunk.as_ref().expect("all chunks present"));
    }
    if assembled.len() as u64 != total_size_bytes {
        return Err(DfsError::Distributed {
            message: format!(
                "{label} size mismatch: expected {}, actual {}",
                total_size_bytes,
                assembled.len()
            ),
        });
    }
    Ok(Some(assembled))
}
