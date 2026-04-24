use sinomo_core::{
    CapabilityDiff, CapabilityName, Confidence, DeviceCapability, EvaluationGate,
    EvaluationGateResult, EvaluationMetric, EvaluationOutcome, EvaluationScorecard, EventEnvelope,
    EventId, EventKind, EvolutionJob, EvolutionJobId, EvolutionJobStatus, EvolutionScope,
    LatencyClass, MetricValue, ModuleId, ModuleManifest, NodeCapability, PolicyAction,
    PolicyDecision, PolicyDecisionId, PrivacyClass, RolloutPlan, RolloutPlanId, RolloutStrategy,
    RoutingHint, RoutingMetadata, RoutingScope, Salience, ScorecardId, SessionId, Source,
    TrustRing,
};
use sinomo_device::{DevicePermission, DeviceProfile};
use sinomo_events::{EventStore, InMemoryEventStore, ReplayQuery};
use sinomo_node::{HostedService, NodeProfile};
use time::OffsetDateTime;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let event_service_capability = CapabilityName::new("service.events.local")?;
    let evolution_capability = CapabilityName::new("evolution.job.scoped")?;
    let module_loader_capability = CapabilityName::new("module.loader.local")?;

    let node = NodeProfile::local("local workstation")?
        .with_capability(NodeCapability::new(event_service_capability.clone()))
        .with_capability(NodeCapability::new(evolution_capability))
        .with_capability(NodeCapability::new(module_loader_capability))
        .with_service(HostedService::new(event_service_capability));

    let workspace_device = DeviceProfile::new(
        node.node_id.clone(),
        "local evolution workspace",
        sinomo_device::DeviceKind::Filesystem,
    )
    .with_capability(DeviceCapability::new(CapabilityName::new(
        "storage.workspace.local",
    )?))
    .with_permission(DevicePermission::Observe);

    let session_id = SessionId::generate();
    let module_id = ModuleId::new("module.trigger.silence_policy")?;
    let module_manifest = ModuleManifest::r3_wasm(module_id.clone(), "0.3.7", "sinomo-trigger");
    let scorecard = EvaluationScorecard {
        scorecard_id: ScorecardId::generate(),
        baseline_artifact_id: None,
        candidate_artifact_id: None,
        outcome: EvaluationOutcome::Passed,
        gates: vec![EvaluationGateResult {
            gate: EvaluationGate::ManifestValidation,
            outcome: EvaluationOutcome::Passed,
            summary: "candidate manifest is compatible with R3 sandbox constraints".to_string(),
        }],
        metrics: vec![EvaluationMetric {
            name: "latency_p95_ms".to_string(),
            value: MetricValue::Integer(41),
            sample_count: Some(120),
        }],
    };
    let policy_decision = PolicyDecision {
        decision_id: PolicyDecisionId::generate(),
        target_ring: TrustRing::R3SandboxedModule,
        target_artifact_id: sinomo_core::ArtifactId::generate(),
        action: PolicyAction::RequireMoreEvidence,
        capability_diff: CapabilityDiff::default(),
        required_additional_evals: vec![EvaluationGate::ReplayTests],
        max_rollout_weight_bps: Some(500),
        reason: "phase one smoke path records policy shape before loading behavior".to_string(),
    };
    let rollout_plan = RolloutPlan {
        rollout_plan_id: RolloutPlanId::generate(),
        artifact_id: policy_decision.target_artifact_id.clone(),
        module_id: module_id.clone(),
        target_ring: TrustRing::R3SandboxedModule,
        strategy: RolloutStrategy::Shadow,
        target_nodes: vec![node.node_id.clone()],
        initial_weight_bps: 0,
        promotion_steps_bps: vec![500, 2_500, 10_000],
        rollback_policy: module_manifest.rollout.rollback_policy.clone(),
    };
    let evolution_job = EvolutionJob {
        job_id: EvolutionJobId::generate(),
        target_ring: TrustRing::R3SandboxedModule,
        target_module_id: Some(module_id.clone()),
        objective: "Reduce false positive interruptions during low-salience screen changes"
            .to_string(),
        scope: EvolutionScope {
            allowed_paths: vec![
                "crates/sinomo-trigger/**".to_string(),
                "modules/trigger/**".to_string(),
            ],
            forbidden_paths: vec![
                "crates/sinomo-core/**".to_string(),
                "docs/security/**".to_string(),
            ],
            workspace_id: None,
        },
        status: EvolutionJobStatus::Scoped,
        required_gates: vec![
            EvaluationGate::ManifestValidation,
            EvaluationGate::ReplayTests,
        ],
        rollback_policy: rollout_plan.rollback_policy.clone(),
    };

    let event = EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        node.node_id.clone(),
        Source::new("evolution.local.orchestrator")?,
        EventKind::new("evolution.job.scoped")?,
        Salience::new(0.86)?,
        Confidence::new(0.97)?,
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Background,
            PrivacyClass::Derived,
        )
        .with_hint(RoutingHint::new("target:ring:r3")?)
        .with_hint(RoutingHint::new("requires:replay-eval")?),
    )
    .with_origin_device(workspace_device.device_id.clone())
    .with_session(session_id.clone())
    .with_payload(serde_json::json!({
        "job": evolution_job,
        "module_manifest": module_manifest,
        "scorecard": scorecard,
        "policy_decision": policy_decision,
        "rollout_plan": rollout_plan
    }));

    let mut store = InMemoryEventStore::new();
    let receipt = store.append(event)?;
    let replayed = store.replay(ReplayQuery::by_session(session_id))?;

    println!(
        "sinomo-app: appended {} sequence={} kind=evolution.job.scoped",
        receipt.event_id, receipt.sequence.0
    );
    println!(
        "sinomo-app: replayed {} self-evolution event(s)",
        replayed.len()
    );

    Ok(())
}
