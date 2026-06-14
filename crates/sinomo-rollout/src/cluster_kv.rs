use sinomo_core::{
    ClusterModuleRolloutDesiredRecord, ClusterModuleRolloutReconciliationRecord,
    ClusterModuleRolloutStatusRecord, ClusterRolloutDesiredRecord, Id as NodeId,
    cluster_module_rollout_desired_key, cluster_module_rollout_desired_prefix,
    cluster_module_rollout_node_key, cluster_module_rollout_node_prefix,
    cluster_module_rollout_reconcile_key, cluster_module_rollout_reconcile_prefix,
    cluster_rollout_desired_key,
};
use sinomo_kv::{KvCommand, KvOp, KvSnapshot};
use sinomo_network::{ClusterPaths, NetworkError};

pub fn read_cluster_rollout_desired_from_kv(
    paths: &ClusterPaths,
) -> Result<Option<ClusterRolloutDesiredRecord>, NetworkError> {
    let snapshot = read_current_cluster_kv_snapshot(paths)?;
    snapshot
        .get(&cluster_rollout_desired_key())
        .map(|entry| entry.decode())
        .transpose()
        .map_err(Into::into)
}

pub fn sync_cluster_rollout_desired_to_kv(
    paths: &ClusterPaths,
    desired: &ClusterRolloutDesiredRecord,
) -> Result<KvSnapshot, NetworkError> {
    put_cluster_kv_value(paths, cluster_rollout_desired_key(), desired)
}

pub fn read_cluster_module_rollout_statuses_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterModuleRolloutStatusRecord>, NetworkError> {
    let Some(snapshot) = read_cached_cluster_kv_snapshot_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let mut statuses: Vec<ClusterModuleRolloutStatusRecord> = Vec::new();
    for entry in snapshot.range_prefix_limited(
        &cluster_module_rollout_node_prefix(),
        sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    )? {
        statuses.push(entry.decode()?);
    }
    statuses.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then_with(|| left.module_id.cmp(&right.module_id))
            .then_with(|| left.rollout_plan_id.cmp(&right.rollout_plan_id))
    });
    Ok(statuses)
}

pub fn sync_cluster_module_rollout_node_to_kv(
    paths: &ClusterPaths,
    status: &ClusterModuleRolloutStatusRecord,
) -> Result<KvSnapshot, NetworkError> {
    put_cluster_kv_value(
        paths,
        cluster_module_rollout_node_key(&status.node_id, &status.module_id),
        status,
    )
}

pub fn read_cluster_module_rollout_desired_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterModuleRolloutDesiredRecord>, NetworkError> {
    let Some(snapshot) = read_cached_cluster_kv_snapshot_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let mut desired: Vec<ClusterModuleRolloutDesiredRecord> = Vec::new();
    for entry in snapshot.range_prefix_limited(
        &cluster_module_rollout_desired_prefix(),
        sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    )? {
        desired.push(entry.decode()?);
    }
    desired.sort_by(|left, right| {
        left.rollout_plan
            .module_id
            .cmp(&right.rollout_plan.module_id)
            .then_with(|| {
                left.rollout_plan
                    .rollout_plan_id
                    .cmp(&right.rollout_plan.rollout_plan_id)
            })
    });
    Ok(desired)
}

pub fn sync_cluster_module_rollout_desired_to_kv(
    paths: &ClusterPaths,
    desired: &ClusterModuleRolloutDesiredRecord,
) -> Result<KvSnapshot, NetworkError> {
    put_cluster_kv_value(
        paths,
        cluster_module_rollout_desired_key(
            &desired.rollout_plan.module_id,
            &desired.rollout_plan.rollout_plan_id,
        ),
        desired,
    )
}

pub fn read_cluster_module_rollout_reconciliations_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterModuleRolloutReconciliationRecord>, NetworkError> {
    let Some(snapshot) = read_cached_cluster_kv_snapshot_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let mut reconciliations: Vec<ClusterModuleRolloutReconciliationRecord> = Vec::new();
    for entry in snapshot.range_prefix_limited(
        &cluster_module_rollout_reconcile_prefix(),
        sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    )? {
        reconciliations.push(entry.decode()?);
    }
    reconciliations.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then_with(|| left.module_id.cmp(&right.module_id))
            .then_with(|| left.rollout_plan_id.cmp(&right.rollout_plan_id))
    });
    Ok(reconciliations)
}

pub fn sync_cluster_module_rollout_reconciliation_to_kv(
    paths: &ClusterPaths,
    reconciliation: &ClusterModuleRolloutReconciliationRecord,
) -> Result<KvSnapshot, NetworkError> {
    put_cluster_kv_value(
        paths,
        cluster_module_rollout_reconcile_key(
            &reconciliation.node_id,
            &reconciliation.module_id,
            &reconciliation.rollout_plan_id,
        ),
        reconciliation,
    )
}

fn read_current_cluster_kv_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, NetworkError> {
    let state = sinomo_network::read_cluster_state_without_kv(paths)?;
    sinomo_kv::read_cluster_kv_current_snapshot_for_node(&paths.kv_paths(), state.local_node_id)
        .map_err(Into::into)
}

fn read_cached_cluster_kv_snapshot_if_present(
    paths: &ClusterPaths,
) -> Result<Option<KvSnapshot>, NetworkError> {
    paths
        .kv_paths()
        .local_cache()
        .cached_snapshot_arc_if_present()
        .map(|snapshot| snapshot.map(|snapshot| (*snapshot).clone()))
        .map_err(Into::into)
}

fn put_cluster_kv_value<T: serde::Serialize + ?Sized>(
    paths: &ClusterPaths,
    key: String,
    value: &T,
) -> Result<KvSnapshot, NetworkError> {
    let state = sinomo_network::read_cluster_state_without_kv(paths)?;
    let op = KvOp::put_serde(key, value, None)?;
    append_cluster_kv_command(paths, &state.local_node_id, KvCommand::Op(op))
}

fn append_cluster_kv_command(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvSnapshot, NetworkError> {
    paths
        .kv_paths()
        .raft_store()
        .append_command_and_publish(node_id, command, |_| false)
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{RollbackPolicy, RolloutPlan, RolloutStrategy, TrustRing};
    use sinomo_local::SinomoDataDir;
    use time::OffsetDateTime;

    #[test]
    fn rollout_cluster_kv_helpers_own_rollout_records() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-cluster-kv");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::Id::from_legacy_named("rollout_cluster_kv").expect("cluster id");
        let node_id =
            sinomo_core::Id::from_legacy_named("rollout_cluster_kv_node").expect("node id");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "rollout cluster kv",
            node_id.clone(),
            "rollout node",
            "public-key",
            now,
        );
        sinomo_network::write_cluster_state(&paths, &state).expect("write state");
        sinomo_network::sync_cluster_state_to_local_raft_unchecked(&paths, &state)
            .expect("sync state");

        let desired = ClusterRolloutDesiredRecord {
            cluster_id: cluster_id.clone(),
            state: "active".to_string(),
            source: Some("git".to_string()),
            mode: Some("cluster-remote-build".to_string()),
            source_head: Some("abc123".to_string()),
            source_bundle: None,
            force_source_overwrite: true,
            remote_repo: None,
            host: Some("127.0.0.1".to_string()),
            port: Some(21353),
            shadow_port: Some(21354),
            requested_at: Some("2026-06-04T00:00:00Z".to_string()),
            updated_at: now,
        };
        sync_cluster_rollout_desired_to_kv(&paths, &desired).expect("sync rollout desired");
        assert_eq!(
            read_cluster_rollout_desired_from_kv(&paths).expect("read rollout desired"),
            Some(desired.clone())
        );

        let module_id = sinomo_core::Id::from_legacy_named("module.rollout_test").expect("module");
        let rollout_plan_id =
            sinomo_core::Id::from_legacy_named("rollout_plan_test").expect("rollout");
        let artifact_id =
            sinomo_core::Id::from_legacy_named("artifact_candidate").expect("artifact");
        let active_artifact_id =
            sinomo_core::Id::from_legacy_named("artifact_active").expect("active");
        let module_status = ClusterModuleRolloutStatusRecord {
            cluster_id: cluster_id.clone(),
            node_id: node_id.clone(),
            rollout_plan_id: rollout_plan_id.clone(),
            module_id: module_id.clone(),
            artifact_id: artifact_id.clone(),
            stage: "canary".to_string(),
            current_weight_bps: 500,
            active_artifact_id: active_artifact_id.clone(),
            previous_artifact_id: None,
            candidate_artifact_id: Some(artifact_id.clone()),
            shadow_artifact_id: None,
            hot_swap_state: "active-candidate".to_string(),
            telemetry_kind: Some("module.invocation.decision".to_string()),
            telemetry_last_consumed_sequence: Some(12),
            last_transition_record_id: Some(
                sinomo_core::Id::from_legacy_named("transition_candidate").expect("transition"),
            ),
            updated_at: now,
        };
        sync_cluster_module_rollout_node_to_kv(&paths, &module_status).expect("sync module status");

        let module_desired = ClusterModuleRolloutDesiredRecord {
            cluster_id: cluster_id.clone(),
            state: "active".to_string(),
            rollout_plan: RolloutPlan {
                rollout_plan_id: rollout_plan_id.clone(),
                artifact_id: artifact_id.clone(),
                module_id: module_id.clone(),
                target_ring: TrustRing::R3SandboxedModule,
                strategy: RolloutStrategy::Canary,
                target_nodes: vec![node_id.clone()],
                initial_weight_bps: 500,
                promotion_steps_bps: vec![1_000, 5_000],
                rollback_policy: RollbackPolicy::default(),
            },
            requested_by_node_id: node_id.clone(),
            release_id: Some(sinomo_core::Id::from_legacy_named("release_job").expect("release")),
            release_material_hash: Some("blake3:release".to_string()),
            evidence_refs: vec!["registry://artifact-evidence/release_job".to_string()],
            requested_at: Some("2026-06-04T00:00:00Z".to_string()),
            updated_at: now,
        };
        sync_cluster_module_rollout_desired_to_kv(&paths, &module_desired)
            .expect("sync module desired");

        let reconciliation = ClusterModuleRolloutReconciliationRecord {
            cluster_id,
            node_id: node_id.clone(),
            rollout_plan_id,
            module_id,
            artifact_id,
            state: "ready".to_string(),
            reason: "candidate material verified".to_string(),
            release_id: module_desired.release_id.clone(),
            release_material_hash: module_desired.release_material_hash.clone(),
            local_runtime_stage: Some("loaded".to_string()),
            updated_at: now,
        };
        sync_cluster_module_rollout_reconciliation_to_kv(&paths, &reconciliation)
            .expect("sync reconciliation");

        assert_eq!(
            read_cluster_module_rollout_statuses_from_kv(&paths).expect("read module statuses"),
            vec![module_status]
        );
        assert_eq!(
            read_cluster_module_rollout_desired_from_kv(&paths).expect("read module desired"),
            vec![module_desired]
        );
        assert_eq!(
            read_cluster_module_rollout_reconciliations_from_kv(&paths)
                .expect("read reconciliations"),
            vec![reconciliation]
        );
    }
}
