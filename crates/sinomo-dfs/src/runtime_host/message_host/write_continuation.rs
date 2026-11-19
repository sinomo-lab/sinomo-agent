//! A live Main request can wait for a committed write-set change after an
//! unknown prepare. This holds only its original command, digest and response;
//! the native prepared record remains the durable recovery owner.
use super::*;

pub(super) fn candidate(
    authority: &Arc<crate::DfsMetadataAuthorityRuntime>,
    command: &DfsMessageCommand,
) -> Option<crate::DfsMetadataShardWriteSet> {
    if !matches!(
        &command.routed_message.message,
        crate::DfsNetworkMessage::DistributedWriteRequest(_)
            | crate::DfsNetworkMessage::ExclusiveCreateRequest(_)
            | crate::DfsNetworkMessage::NamespaceRequest(crate::DfsNamespaceCommand {
                operation: crate::DfsNamespaceOperation::WriteFile { .. },
                ..
            })
    ) {
        return None;
    }
    let snapshot = authority.checked_configuration_snapshot().ok()??;
    candidate_from_snapshot(&snapshot, command)
}

fn candidate_from_snapshot(
    snapshot: &crate::DfsConfigurationSnapshot,
    command: &DfsMessageCommand,
) -> Option<crate::DfsMetadataShardWriteSet> {
    let shard = match &command.routed_message.message {
        crate::DfsNetworkMessage::DistributedWriteRequest(w)
            if !w.exclusive_create && w.patch.is_none() && w.publication.is_current() =>
        {
            w.authority_shard.as_ref()?
        }
        crate::DfsNetworkMessage::ExclusiveCreateRequest(w)
            if w.exclusive_create && w.patch.is_none() && w.publication.is_current() =>
        {
            w.authority_shard.as_ref()?
        }
        crate::DfsNetworkMessage::NamespaceRequest(ns)
            if matches!(ns.operation, crate::DfsNamespaceOperation::WriteFile { .. }) =>
        {
            let [shard] = ns.authority_shards.as_slice() else {
                return None;
            };
            shard
        }
        _ => return None,
    };
    let entries =
        snapshot.entries(crate::metadata_authority_runtime::DFS_METADATA_AUTHORITY_PREFIX)?;
    let context = crate::metadata_replica_context_from_authority_entries(
        entries,
        &command.routed_message.target_node_id,
        &shard.file_system_id,
        &crate::DfsMetadataShardId::new(&shard.shard_id),
    )
    .ok()??;
    if context.shard != *shard
        || context.shard.primary_node_id != command.routed_message.target_node_id
        || context.pending_membership_handoff.is_some()
    {
        return None;
    }
    context.write_set
}

/// Subscription precedes the first check so an already published shrink is
/// observed as well. Every wake rechecks coherent authority; unrelated deltas
/// cannot redispatch the write. Shutdown/cancellation drops this request only.
pub(super) async fn wait(
    authority: &Arc<crate::DfsMetadataAuthorityRuntime>,
    command: &DfsMessageCommand,
    previous: &crate::DfsMetadataShardWriteSet,
    host: &DfsRuntimeHost,
) -> bool {
    let mut published = authority.subscribe_configuration_snapshot();
    let wait = async {
        loop {
            published.borrow_and_update();
            match authority.checked_configuration_snapshot() {
                Ok(Some(snapshot)) => match candidate_from_snapshot(&snapshot, command) {
                    Some(current)
                        if current.epoch == previous.epoch
                            && current.primary_node_id == previous.primary_node_id =>
                    {
                        if current.generation > previous.generation {
                            return true;
                        }
                    }
                    _ => return false,
                },
                Ok(None) => {}
                Err(_) => return false,
            }
            if published.changed().await.is_err() {
                return false;
            }
        }
    };
    // The original RPC future owns the caller deadline, including time spent
    // ingesting attachments. A preflight timeout is not a new write budget.
    tokio::select! {
        changed = wait => changed,
        () = host.wait_for_shutdown() => false,
    }
}
