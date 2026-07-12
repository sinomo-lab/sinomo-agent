use crate::{DfsError, DfsObjectId, DfsVersion};
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

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
    pub(crate) repair_inflight_request_ids: BTreeMap<NodeId, String>,
    pub(crate) fetch_inflight_from: BTreeSet<NodeId>,
    pub(crate) fetch_inflight_request_ids: BTreeMap<NodeId, String>,
    pub(crate) fetch_inflight_started_ticks: BTreeMap<NodeId, u64>,
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
            repair_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_from: BTreeSet::new(),
            fetch_inflight_request_ids: BTreeMap::new(),
            fetch_inflight_started_ticks: BTreeMap::new(),
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

pub(crate) fn repair_transfer_inflight_timeout_ticks(_total_size_bytes: u64) -> u64 {
    fetch_inflight_timeout_ticks()
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
