use crate::{
    DfsAuthoritativePrefixProjection, DfsMessageExecutionScope, DfsMetadataShardConfig,
    DfsMetadataShardId, DfsMutationLaneKey, DfsMutationScheduler,
    dfs_metadata_shard_config_key_is_exact,
};
use sinomo_core::node::NodeId;
use std::future::Future;
use std::sync::Arc;

pub(crate) enum DfsMetadataPublishPlanningAttempt<T> {
    Complete(T),
    WaitForResidentShard(crate::ResidentLocalMetadataShardHandle),
}

pub(crate) fn metadata_publish_lanes_from_authority(
    local_node_id: &NodeId,
    authoritative: &DfsAuthoritativePrefixProjection,
) -> Result<Vec<DfsMutationLaneKey>, String> {
    let mut lanes = authoritative
        .entries()
        .iter()
        .filter(|entry| dfs_metadata_shard_config_key_is_exact(&entry.key))
        .map(|entry| {
            entry
                .decode::<DfsMetadataShardConfig>()
                .map_err(|error| format!("DFS metadata shard topology is malformed: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|shard| shard.primary_node_id == *local_node_id)
        .map(|shard| DfsMutationLaneKey {
            file_system_id: shard.file_system_id,
            shard_id: DfsMetadataShardId::new(shard.shard_id),
        })
        .collect::<Vec<_>>();
    lanes.sort();
    lanes.dedup();
    Ok(lanes)
}

#[cfg(test)]
pub(crate) type DfsMetadataPublishPlanningAdmission = Arc<crate::DfsMutationAdmission>;
#[cfg(not(test))]
pub(crate) type DfsMetadataPublishPlanningAdmission = crate::DfsMutationAdmission;

pub(crate) async fn run_metadata_publish_lane_planning_with_admission<T, F>(
    mutation_scheduler: Arc<DfsMutationScheduler>,
    lane: DfsMutationLaneKey,
    task_owner: &crate::runtime_host::DfsRuntimeHost,
    mut plan: F,
) -> Result<T, String>
where
    T: Send + 'static,
    F: FnMut(
            &DfsMetadataPublishPlanningAdmission,
        ) -> Result<DfsMetadataPublishPlanningAttempt<T>, String>
        + Send
        + 'static,
{
    let execution_scope = DfsMessageExecutionScope::ExactShard {
        file_system_id: lane.file_system_id,
        shard_id: lane.shard_id,
    };
    loop {
        let admission = mutation_scheduler
            .admit(&execution_scope)
            .await
            .map_err(|error| format!("DFS metadata publish admission failed: {error}"))?;
        // Test observers borrow this actual guard through Weak; they do not
        // acquire another permit or require unrelated native owners to be idle.
        #[cfg(test)]
        let admission = Arc::new(admission);
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let task = tokio::task::spawn_blocking(move || {
            // A cancelled scheduler future cannot release admission while its
            // blocking durable-view refresh is still reading the shard.
            let attempt = plan(&admission);
            // Preserve the prior JoinHandle boundary: the returned plan is
            // consumed only after exact admission has been released.
            drop(admission);
            let _ = result_tx.send((plan, attempt));
        });
        // Register the same blocking child before the first await. Cancelling
        // its outer publisher must not detach a still-running FD/guard owner.
        task_owner.own_task(task);
        let (returned_plan, attempt) = result_rx.await.map_err(|error| {
            format!("DFS metadata publish planning task ended without a result: {error}")
        })?;
        plan = returned_plan;
        match attempt? {
            DfsMetadataPublishPlanningAttempt::Complete(result) => return Ok(result),
            DfsMetadataPublishPlanningAttempt::WaitForResidentShard(shard) => shard
                .wait_until_checkout_available()
                .await
                .map_err(|error| {
                    format!("DFS metadata publish resident-shard wait failed: {error}")
                })?,
        }
    }
}

pub(crate) async fn run_metadata_publish_blocking_execution<G, T, Fut>(
    runtime: tokio::runtime::Handle,
    task_owner: &crate::runtime_host::DfsRuntimeHost,
    execution_guard: G,
    execute: Fut,
) -> Result<T, String>
where
    G: Send + 'static,
    T: Send + 'static,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let task = tokio::task::spawn_blocking(move || {
        // Tokio cannot cancel an already-running blocking closure. Retain the
        // host capability until nested async transport/cursor commits finish.
        let result = runtime.block_on(execute);
        // Match the former JoinHandle boundary before exposing any result.
        drop(execution_guard);
        let _ = result_tx.send(result);
    });
    task_owner.own_task(task);
    result_rx.await.map_err(|error| {
        format!("DFS metadata publish execution task ended without a result: {error}")
    })?
}
