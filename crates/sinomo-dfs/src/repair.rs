use crate::DfsRepairAttemptRecord;

pub(crate) fn repair_attempt_record_indicates_lost_authority(
    record: &DfsRepairAttemptRecord,
) -> bool {
    record
        .last_error
        .as_deref()
        .is_some_and(repair_rejection_indicates_lost_authority)
}

pub(crate) fn repair_rejection_indicates_lost_authority(reason: &str) -> bool {
    reason.contains("source is not current metadata shard primary")
        || reason.contains("source is not authorized to fetch DFS blob")
}

pub(crate) fn repair_rejection_indicates_stale_replica_base(reason: &str) -> bool {
    reason.contains("stale DFS repair base")
}

pub(crate) fn repair_rejection_indicates_stale_chunk_resume(reason: &str) -> bool {
    reason.contains("repair replica chunk transfer")
        && reason.contains("started at chunk")
        && reason.contains("without earlier chunks")
}
