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

pub(crate) async fn run_metadata_publish_lane_planning_with_admission<T, F>(
    mutation_scheduler: Arc<DfsMutationScheduler>,
    lane: DfsMutationLaneKey,
    mut plan: F,
) -> Result<T, String>
where
    T: Send + 'static,
    F: FnMut() -> Result<DfsMetadataPublishPlanningAttempt<T>, String> + Send + 'static,
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
        let (returned_plan, attempt) = tokio::task::spawn_blocking(move || {
            // A cancelled scheduler future cannot release admission while its
            // blocking durable-view refresh is still reading the shard.
            let _admission = admission;
            let attempt = plan();
            (plan, attempt)
        })
        .await
        .map_err(|error| format!("DFS metadata publish planning task failed: {error}"))?;
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
    execution_guard: G,
    execute: Fut,
) -> Result<T, String>
where
    G: Send + 'static,
    T: Send + 'static,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        // Tokio cannot cancel an already-running blocking closure. Retain the
        // host capability until nested async transport/cursor commits finish.
        let _execution_guard = execution_guard;
        runtime.block_on(execute)
    })
    .await
    .map_err(|error| format!("DFS metadata publish execution task failed: {error}"))?
}
