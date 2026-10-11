use crate::DfsRepairAttemptRecord;

pub(crate) fn repair_attempt_record_indicates_lost_authority(
    record: &DfsRepairAttemptRecord,
) -> bool {
    record
        .last_error
        .as_deref()
        .is_some_and(repair_rejection_indicates_lost_authority)
}

pub(crate) fn repair_attempt_record_indicates_transient_route_failure(
    record: &DfsRepairAttemptRecord,
) -> bool {
    record
        .last_error
        .as_deref()
        .is_some_and(repair_error_indicates_transient_route_failure)
}

pub(crate) fn repair_error_indicates_transient_route_failure(reason: &str) -> bool {
    let reason = reason.to_ascii_lowercase();
    reason.contains("timed out routing")
        || reason.contains("network error at cluster target rpc")
        || reason.contains("network i/o error")
        || reason.contains("target was not routable")
        || reason.contains("not routable")
        || reason.contains("no usable route")
        || reason.contains("no routable dfs repair blob alternate source")
        || reason.contains("unknown node")
}

pub(crate) fn repair_rejection_indicates_lost_authority(reason: &str) -> bool {
    reason.contains("source is not current metadata shard primary")
        || reason.contains("source is not authorized to fetch DFS blob")
}

#[cfg(test)]
pub(crate) fn repair_rejection_indicates_stale_replica_base(reason: &str) -> bool {
    reason.contains("stale DFS repair base")
}

/// Pure scheduling eligibility DATA; never transfer or publication permission.
pub(crate) fn retained_repair_schedule_can_wake(record: &DfsRepairAttemptRecord) -> bool {
    record.next_retry_unix_nanos == u64::MAX
        && !crate::service_repair::repair_transfer_custody_unresolved(record)
        && !repair_attempt_record_indicates_lost_authority(record)
        && record.last_error.as_deref()
            != Some(crate::maintenance::DFS_REPAIR_EXECUTION_BLOCKED_BY_CONNECTIVITY)
}

/// A current usable-route projection can wake only this exact scheduling block.
/// Unknown or unresolved transfer custody is never a scheduling permission.
pub(crate) fn connectivity_repair_schedule_can_wake(record: &DfsRepairAttemptRecord) -> bool {
    record.next_retry_unix_nanos == u64::MAX
        && !crate::service_repair::repair_transfer_custody_unresolved(record)
        && record.last_error.as_deref()
            == Some(crate::maintenance::DFS_REPAIR_EXECUTION_BLOCKED_BY_CONNECTIVITY)
}
