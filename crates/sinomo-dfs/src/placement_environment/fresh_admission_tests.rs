use super::*;
use crate::write_configuration_inputs::{DfsAdmittedContentTarget, DfsAdmittedContentTargets};
use sinomo_core::kv::KvRevision;

struct Fixture {
    a: NodeId,
    b: NodeId,
    c: NodeId,
    request: DfsReplicaPlacementRequest,
    inventory: DfsAdmittedContentTargets,
    snapshot: DfsPlacementSnapshot,
}

fn node(name: &str) -> NodeId {
    NodeId::from_legacy_named(name).unwrap()
}

impl Fixture {
    fn new() -> Self {
        let a = node("fresh-admission-a");
        let b = node("fresh-admission-b");
        let c = node("fresh-admission-c");
        let now = OffsetDateTime::now_utc();
        // Controlled planning DATA only. Separate inventory tests execute its
        // actual four-scope proof/delta/floor decoder; this is no native permit.
        let targets = [&a, &b, &c]
            .into_iter()
            .map(|id| {
                let mut advertisement = DfsNodeAdvertisement::content(id.clone());
                advertisement.storage_health = DfsHealthState::Healthy;
                advertisement.storage_freshness = DfsObservationFreshness::Fresh;
                advertisement.updated_at = now;
                advertisement.expires_at = Some(now + time::Duration::minutes(5));
                advertisement.zone = Some(if id == &a { "east" } else { "west" }.into());
                DfsAdmittedContentTarget {
                    advertisement,
                    provider_observed_ready: id != &a,
                }
            })
            .collect::<Vec<_>>();
        let mut policy = crate::DfsReplicationPolicy::new(2, 3).unwrap();
        policy.node_zones = targets
            .iter()
            .map(|target| {
                (
                    target.advertisement.node_id.clone(),
                    target.advertisement.zone.clone().unwrap(),
                )
            })
            .collect();
        let request = DfsReplicaPlacementRequest {
            file_system_id: crate::sifs_file_system_id(),
            object_id: crate::DfsObjectId::from_legacy_named("fresh-admission-object").unwrap(),
            target_version: crate::DfsVersion::from_legacy_named("fresh-admission-version")
                .unwrap(),
            coordinator_node_id: b.clone(),
            policy,
            candidates: [&a, &b, &c]
                .into_iter()
                .map(|id| DfsReplicaCandidate::new(id.clone()))
                .collect(),
        };
        let snapshot = DfsPlacementSnapshot {
            schema_version: DFS_PLACEMENT_SNAPSHOT_SCHEMA_VERSION,
            generation: 7,
            primary_node_id: b.clone(),
            observed_at: now,
            expires_at: now + time::Duration::minutes(5),
            nodes: targets
                .iter()
                .map(|target| {
                    DfsPlacementNodeObservation::from_advertisement(&target.advertisement)
                })
                .collect(),
            paths: [
                (&a, DfsPlacementPathState::Unreachable),
                (&c, DfsPlacementPathState::Healthy),
            ]
            .into_iter()
            .map(|(target, state)| DfsPlacementPathObservation {
                source_node_id: b.clone(),
                target_node_id: target.clone(),
                state,
                latency_micros: None,
                delivered_bytes_per_second: None,
                metering: DfsNetworkMetering::Unknown,
                observed_at: now,
                expires_at: now + time::Duration::minutes(5),
            })
            .collect(),
        };
        Self {
            a,
            b,
            c,
            request,
            inventory: DfsAdmittedContentTargets {
                configuration_revision: KvRevision(11),
                targets,
            },
            snapshot,
        }
    }

    fn plan(
        &self,
        current: &[NodeId],
        pinned: &[NodeId],
    ) -> Result<DfsFreshContentAdmissionPlan, DfsDistributedError> {
        plan_fresh_eventual_replicas_with_admitted_targets(
            &self.request,
            &self.inventory,
            &self.snapshot,
            current,
            pinned,
        )
    }
}

#[test]
fn fresh_admission_retains_n_and_defers_optional_unavailable_target() {
    let f = Fixture::new();
    let plan = f.plan(&[], &[]).unwrap();
    assert_eq!(
        plan.placement.desired_replica_nodes,
        vec![f.b.clone(), f.c.clone(), f.a.clone()]
    );
    assert_eq!(
        plan.placement.preferred_commit_nodes,
        vec![f.b.clone(), f.c.clone()]
    );
    assert_eq!(plan.observed_ready_node_ids, vec![f.b.clone(), f.c.clone()]);
    assert_eq!(plan.attemptable_node_ids, vec![f.b, f.c]);
    assert_eq!(plan.deferred_desired_node_ids, vec![f.a]);
    assert_eq!(plan.configuration_revision, KvRevision(11));
    assert_eq!(f.request.policy.desired_replicas, 3);
    assert_eq!(f.request.policy.min_commit_replicas, 2);
}

#[test]
fn fresh_admission_rejects_insufficient_writable_w_without_shrinking_n() {
    let mut f = Fixture::new();
    f.inventory
        .targets
        .iter_mut()
        .find(|target| target.advertisement.node_id == f.c)
        .unwrap()
        .provider_observed_ready = false;
    assert!(f.plan(&[], &[]).is_err());
    assert_eq!(f.request.policy.desired_replicas, 3);
}

#[test]
fn fresh_admission_required_pinned_and_forbidden_constraints_stay_hard() {
    let mut f = Fixture::new();
    assert!(f.plan(&[], std::slice::from_ref(&f.a)).is_err());
    f.request.policy.required_node_ids = vec![f.a.clone()];
    assert!(f.plan(&[], &[]).is_err());
    f.request.policy.required_node_ids.clear();
    f.request.policy.forbidden_node_ids = vec![f.a.clone()];
    assert!(f.plan(&[], &[]).is_err());
    f.request.policy.forbidden_node_ids.clear();
    let pinned = f.plan(&[], std::slice::from_ref(&f.c)).unwrap();
    assert!(pinned.placement.desired_replica_nodes.contains(&f.c));
    assert_eq!(pinned.placement.desired_replica_nodes.len(), 3);
}

#[test]
fn fresh_admission_current_locations_and_candidate_hints_cannot_add_targets() {
    let mut f = Fixture::new();
    let foreign = node("fresh-admission-not-configured");
    f.request
        .candidates
        .push(DfsReplicaCandidate::new(foreign.clone()));
    assert!(
        !f.plan(std::slice::from_ref(&foreign), &[])
            .unwrap()
            .placement
            .desired_replica_nodes
            .contains(&foreign)
    );
    f.inventory
        .targets
        .retain(|target| target.advertisement.node_id != f.a);
    assert!(
        f.plan(std::slice::from_ref(&f.a), &[]).is_err(),
        "recorded A cannot replace revoked inventory admission"
    );
}

#[test]
fn fresh_admission_explicit_storage_refusal_or_critical_target_stays_rejected() {
    for critical in [false, true] {
        let mut f = Fixture::new();
        let target = f
            .inventory
            .targets
            .iter_mut()
            .find(|target| target.advertisement.node_id == f.a)
            .unwrap();
        if critical {
            target.advertisement.storage_health = DfsHealthState::Critical;
        } else {
            target.advertisement.accepts_content = false;
        }
        assert!(f.plan(&[], &[]).is_err());
    }
}

#[test]
fn fresh_admission_preserves_distinct_desired_zones_with_deferred_target() {
    let mut f = Fixture::new();
    f.request.policy.min_distinct_zones = 2;
    let plan = f.plan(&[], &[]).unwrap();
    assert_eq!(plan.placement.desired_replica_nodes.len(), 3);
    assert_eq!(plan.deferred_desired_node_ids, vec![f.a.clone()]);
    f.inventory
        .targets
        .iter_mut()
        .find(|target| target.advertisement.node_id == f.a)
        .unwrap()
        .advertisement
        .zone = None;
    f.request.policy.node_zones.remove(&f.a);
    assert!(f.plan(&[], &[]).is_err());
}

#[test]
fn fresh_admission_unknown_or_stale_endpoint_evidence_does_not_declare_node_failed() {
    for stale in [false, true] {
        let mut f = Fixture::new();
        f.inventory
            .targets
            .iter_mut()
            .find(|target| target.advertisement.node_id == f.a)
            .unwrap()
            .provider_observed_ready = true;
        let path = f
            .snapshot
            .paths
            .iter_mut()
            .find(|path| path.target_node_id == f.a)
            .unwrap();
        if stale {
            path.expires_at = OffsetDateTime::now_utc() - time::Duration::seconds(1);
        } else {
            path.state = DfsPlacementPathState::Unknown;
        }
        let plan = f.plan(&[], &[]).unwrap();
        assert!(plan.deferred_desired_node_ids.is_empty());
        assert_eq!(plan.attemptable_node_ids.len(), 3);
        assert!(!plan.observed_ready_node_ids.contains(&f.a));
    }
}

#[test]
fn fresh_admission_quiet_stale_survivors_remain_attemptable_without_refresh() {
    let mut f = Fixture::new();
    let expired = OffsetDateTime::now_utc() - time::Duration::minutes(10);
    for target in &mut f.inventory.targets {
        if target.advertisement.node_id != f.a {
            target.advertisement.storage_freshness = DfsObservationFreshness::Stale;
            target.advertisement.updated_at = expired;
            target.advertisement.expires_at = Some(expired);
        }
    }
    for path in &mut f.snapshot.paths {
        if path.target_node_id == f.c {
            path.expires_at = expired;
        }
    }
    let plan = f.plan(&[], &[]).unwrap();
    assert_eq!(plan.attemptable_node_ids, vec![f.b.clone(), f.c.clone()]);
    assert!(plan.observed_ready_node_ids.is_empty());
    assert_eq!(plan.deferred_desired_node_ids, vec![f.a]);
    assert_eq!(plan.placement.desired_replica_nodes.len(), 3);
    assert_eq!(plan.placement.preferred_commit_nodes.len(), 2);
}

#[test]
fn fresh_admission_missing_remote_path_stays_attemptable_without_ready_claim() {
    let mut f = Fixture::new();
    f.snapshot.paths.retain(|path| path.target_node_id != f.c);
    let plan = f.plan(&[], &[]).unwrap();
    assert_eq!(plan.attemptable_node_ids, vec![f.b.clone(), f.c.clone()]);
    assert_eq!(plan.observed_ready_node_ids, vec![f.b]);
    assert_eq!(plan.deferred_desired_node_ids, vec![f.a]);
}

#[test]
fn fresh_admission_unknown_required_and_pinned_targets_keep_existing_eligibility() {
    let mut f = Fixture::new();
    f.inventory
        .targets
        .iter_mut()
        .find(|target| target.advertisement.node_id == f.a)
        .unwrap()
        .provider_observed_ready = true;
    let path = f
        .snapshot
        .paths
        .iter_mut()
        .find(|path| path.target_node_id == f.a)
        .unwrap();
    path.state = DfsPlacementPathState::Unknown;
    f.request.policy.required_node_ids = vec![f.a.clone()];
    let plan = f.plan(&[], std::slice::from_ref(&f.a)).unwrap();
    assert!(plan.placement.desired_replica_nodes.contains(&f.a));
    assert!(plan.attemptable_node_ids.contains(&f.a));
    assert!(!plan.observed_ready_node_ids.contains(&f.a));
}

#[test]
fn fresh_admission_conflicting_zone_sources_cannot_satisfy_persisted_policy() {
    let mut f = Fixture::new();
    f.request.policy.min_distinct_zones = 2;
    f.inventory
        .targets
        .iter_mut()
        .find(|target| target.advertisement.node_id == f.a)
        .unwrap()
        .advertisement
        .zone = Some("west".into());
    let original_policy = f.request.policy.clone();
    assert!(
        f.plan(&[], &[]).is_err(),
        "all actual targets are west; stale east policy cannot satisfy minzones"
    );
    assert_eq!(
        f.request.policy, original_policy,
        "selector cannot silently repair the persisted operation body"
    );
}

#[test]
fn fresh_admission_cannot_replace_strict_w_equals_n_selector() {
    let mut f = Fixture::new();
    assert!(plan_replicas_with_placement_snapshot(&f.request, &f.snapshot, &[], &[]).is_err());
    f.request.policy.min_commit_replicas = 3;
    assert!(f.plan(&[], &[]).is_err());
    assert!(plan_replicas_with_placement_snapshot(&f.request, &f.snapshot, &[], &[]).is_err());
}
