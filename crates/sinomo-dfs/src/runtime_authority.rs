/// Background publication uses the same admission and durable validation as
/// foreground replication, but must not cancel the maintenance which authored
/// it. Mixed-purpose, legacy and synchronous barrier requests keep preemption.
pub(crate) fn dfs_metadata_replication_can_wait_for_maintenance(
    command: &crate::DfsMetadataJournalBatchCommand,
    authenticated_source_node_id: &sinomo_core::node::NodeId,
    local_node_id: &sinomo_core::node::NodeId,
) -> bool {
    if dfs_metadata_prefix_probe_is_verification_only(
        command,
        authenticated_source_node_id,
        local_node_id,
    ) {
        return true;
    }
    command
        .durable_replication
        .as_ref()
        .is_some_and(|replication| {
            replication.background_publication
                && !replication.confirmation_id.is_empty()
                && !command.batch.journal_frames.is_empty()
        })
        && command.source_node_id.as_ref() == Some(authenticated_source_node_id)
        && command.file_system_id.is_some()
        && command.authority_epoch.is_some()
        && !command.verify_prefix_only
        && command.prefix_probe.is_none()
        && command.authority_probe.is_none()
        && !command.batch.journal_frames.is_empty()
        && command.request_id
            == format!(
                "dfs_meta_{}_{}_{}",
                authenticated_source_node_id, local_node_id, command.batch.next_after_sequence,
            )
}

/// Whether a metadata prefix probe is a non-mutating verification request.
/// Exact validation lives with DFS protocol semantics so hosts cannot disagree
/// about which background probes may avoid foreground-maintenance preemption.
///
/// A write-set probe (one naming a record generation, or an epoch start to
/// align with) is not one: a write-set join sends it while holding its own
/// node's global admission, so a member maintenance pass waiting on that
/// node must yield to it, as it yields to a handoff probe.
pub(crate) fn dfs_metadata_prefix_probe_is_verification_only(
    command: &crate::DfsMetadataJournalBatchCommand,
    authenticated_source_node_id: &sinomo_core::node::NodeId,
    local_node_id: &sinomo_core::node::NodeId,
) -> bool {
    let Some(prefix_probe) = command.prefix_probe.as_ref() else {
        return false;
    };
    command.verify_prefix_only
        && command.source_node_id.as_ref() == Some(authenticated_source_node_id)
        && prefix_probe.primary_handoff_id.is_none()
        && prefix_probe.write_set_generation.is_none()
        && prefix_probe.epoch_start_alignment.is_none()
        && !prefix_probe.barrier_id.is_empty()
        && !prefix_probe.state_fingerprint.is_empty()
        && command.file_system_id.as_ref() == prefix_probe.file_system_id.as_ref()
        && command.file_system_id.is_some()
        && command.authority_epoch == prefix_probe.authority_epoch
        && command.authority_epoch.is_some()
        && command.batch.shard_id == prefix_probe.shard_id
        && command.batch.next_after_sequence == prefix_probe.journal_sequence
        && command.request_id
            == format!(
                "dfs_meta_probe_{}_{}_{}",
                authenticated_source_node_id, local_node_id, prefix_probe.barrier_id
            )
        && command.authority_probe.is_none()
        && command.durable_replication.is_none()
        && command.batch.journal_frames.is_empty()
}
