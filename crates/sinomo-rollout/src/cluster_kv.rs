use sinomo_core::kv::{KvError, KvOp, KvSnapshot};
use sinomo_core::{
    ClusterModuleRolloutDesiredRecord, ClusterModuleRolloutReconciliationRecord,
    ClusterModuleRolloutStatusRecord, ClusterRolloutDesiredRecord,
    cluster_module_rollout_desired_key, cluster_module_rollout_desired_prefix,
    cluster_module_rollout_node_key, cluster_module_rollout_node_prefix,
    cluster_module_rollout_reconcile_key, cluster_module_rollout_reconcile_prefix,
    cluster_rollout_desired_key,
};

/// Projects cluster rollout intent from a caller-supplied KV snapshot.
///
/// This function does not establish freshness or authority. Control-plane
/// callers must supply an authoritative snapshot; explicitly local status and
/// diagnostics callers may supply a local snapshot.
pub fn project_cluster_rollout_desired_from_snapshot(
    snapshot: &KvSnapshot,
) -> Result<Option<ClusterRolloutDesiredRecord>, KvError> {
    snapshot
        .get(&cluster_rollout_desired_key())
        .map(|entry| entry.decode())
        .transpose()
}

/// Builds the storage operation for publishing cluster rollout intent.
///
/// The caller owns request identity, quorum commit, retry, and outcome lookup.
pub fn build_cluster_rollout_desired_put_op(
    desired: &ClusterRolloutDesiredRecord,
) -> Result<KvOp, KvError> {
    KvOp::put_serde(cluster_rollout_desired_key(), desired, None)
}

/// Projects per-node module rollout status from a caller-supplied KV snapshot.
///
/// This function does not establish freshness or authority. A local snapshot
/// is appropriate only for explicitly operational status and diagnostics.
pub fn project_cluster_module_rollout_statuses_from_snapshot(
    snapshot: &KvSnapshot,
) -> Result<Vec<ClusterModuleRolloutStatusRecord>, KvError> {
    let mut statuses: Vec<ClusterModuleRolloutStatusRecord> = Vec::new();
    for entry in snapshot.range_prefix_limited(
        &cluster_module_rollout_node_prefix(),
        sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
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

/// Builds the storage operation for publishing one node's module rollout status.
///
/// The caller owns request identity, quorum commit, retry, and outcome lookup.
pub fn build_cluster_module_rollout_status_put_op(
    status: &ClusterModuleRolloutStatusRecord,
) -> Result<KvOp, KvError> {
    KvOp::put_serde(
        cluster_module_rollout_node_key(&status.node_id, &status.module_id),
        status,
        None,
    )
}

/// Projects module rollout intent from a caller-supplied KV snapshot.
///
/// This function does not establish freshness or authority. Control-plane
/// callers must supply an authoritative snapshot.
pub fn project_cluster_module_rollout_desired_from_snapshot(
    snapshot: &KvSnapshot,
) -> Result<Vec<ClusterModuleRolloutDesiredRecord>, KvError> {
    let mut desired: Vec<ClusterModuleRolloutDesiredRecord> = Vec::new();
    for entry in snapshot.range_prefix_limited(
        &cluster_module_rollout_desired_prefix(),
        sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
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

/// Builds the storage operation for publishing module rollout intent.
///
/// The caller owns request identity, quorum commit, retry, and outcome lookup.
pub fn build_cluster_module_rollout_desired_put_op(
    desired: &ClusterModuleRolloutDesiredRecord,
) -> Result<KvOp, KvError> {
    KvOp::put_serde(
        cluster_module_rollout_desired_key(
            &desired.rollout_plan.module_id,
            &desired.rollout_plan.rollout_plan_id,
        ),
        desired,
        None,
    )
}

/// Projects module rollout reconciliation evidence from a caller-supplied KV snapshot.
///
/// This function does not establish freshness or authority. Callers using the
/// evidence to advance or finish a rollout must supply an authoritative snapshot;
/// explicitly operational status callers may supply a local snapshot.
pub fn project_cluster_module_rollout_reconciliations_from_snapshot(
    snapshot: &KvSnapshot,
) -> Result<Vec<ClusterModuleRolloutReconciliationRecord>, KvError> {
    let mut reconciliations: Vec<ClusterModuleRolloutReconciliationRecord> = Vec::new();
    for entry in snapshot.range_prefix_limited(
        &cluster_module_rollout_reconcile_prefix(),
        sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
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

/// Builds the storage operation for publishing module rollout reconciliation evidence.
///
/// The caller owns request identity, quorum commit, retry, and outcome lookup.
pub fn build_cluster_module_rollout_reconciliation_put_op(
    reconciliation: &ClusterModuleRolloutReconciliationRecord,
) -> Result<KvOp, KvError> {
    KvOp::put_serde(
        cluster_module_rollout_reconcile_key(
            &reconciliation.node_id,
            &reconciliation.module_id,
            &reconciliation.rollout_plan_id,
        ),
        reconciliation,
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::kv::{InProcessKvStore, KvCommand};
    use sinomo_core::{RollbackPolicy, RolloutPlan, RolloutStrategy, TrustRing};
    use time::OffsetDateTime;

    fn apply_built_op(store: &mut InProcessKvStore, op: Result<KvOp, KvError>) {
        store
            .apply_command(KvCommand::Op(op.expect("build rollout KV operation")))
            .expect("apply rollout KV operation");
    }

    #[test]
    fn rollout_cluster_kv_builders_and_snapshot_projections_round_trip_records() {
        let mut store = InProcessKvStore::new();
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            sinomo_core::Id::from_legacy_named("rollout_cluster_kv").expect("cluster id");
        let node_id =
            sinomo_core::Id::from_legacy_named("rollout_cluster_kv_node").expect("node id");

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
        apply_built_op(&mut store, build_cluster_rollout_desired_put_op(&desired));
        let snapshot = store.snapshot().expect("snapshot rollout desired");
        assert_eq!(
            project_cluster_rollout_desired_from_snapshot(&snapshot)
                .expect("project rollout desired"),
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
        apply_built_op(
            &mut store,
            build_cluster_module_rollout_status_put_op(&module_status),
        );

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
        apply_built_op(
            &mut store,
            build_cluster_module_rollout_desired_put_op(&module_desired),
        );

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
        apply_built_op(
            &mut store,
            build_cluster_module_rollout_reconciliation_put_op(&reconciliation),
        );

        let snapshot = store.snapshot().expect("snapshot rollout records");
        assert_eq!(
            project_cluster_module_rollout_statuses_from_snapshot(&snapshot)
                .expect("project module statuses"),
            vec![module_status]
        );
        assert_eq!(
            project_cluster_module_rollout_desired_from_snapshot(&snapshot)
                .expect("project module desired"),
            vec![module_desired]
        );
        assert_eq!(
            project_cluster_module_rollout_reconciliations_from_snapshot(&snapshot)
                .expect("project reconciliations"),
            vec![reconciliation]
        );
    }
}
