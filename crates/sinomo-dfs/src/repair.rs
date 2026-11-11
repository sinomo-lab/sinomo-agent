pub(crate) fn repair_rejection_indicates_lost_authority(reason: &str) -> bool {
    reason.contains("source is not current metadata shard primary")
        || reason.contains("source is not authorized to fetch DFS blob")
}

#[cfg(test)]
pub(crate) fn repair_rejection_indicates_stale_replica_base(reason: &str) -> bool {
    reason.contains("stale DFS repair base")
}
