use sinomo_core::{
    ActorKind, ActorRef, ArtifactId, ArtifactManifest, ArtifactProvenance, CapabilityDiff,
    CapabilityName, Confidence, DeviceCapability, DfsObjectId, DfsObjectPolicy, DfsVersion,
    DfsWriteRequest, EvaluationGate, EvaluationGateResult, EvaluationMetric, EvaluationOutcome,
    EvaluationScorecard, EventEnvelope, EventId, EventKind, EvolutionJob, EvolutionJobId,
    EvolutionJobStatus, EvolutionScope, LatencyClass, MetricValue, ModuleId, ModuleManifest,
    NodeCapability, PrivacyClass, RolloutPlan, RolloutPlanId, RolloutStrategy, RoutingHint,
    RoutingMetadata, RoutingScope, Salience, ScorecardId, SessionId, Source, SyncPolicy, TrustRing,
    WorkspaceId,
};
use sinomo_device::{DevicePermission, DeviceProfile};
use sinomo_dfs::{
    DfsMetadataStore, DfsObjectKind, DfsObjectMetadata, MountKind, Workspace, WorkspaceMount,
};
use sinomo_events::{EventSequence, EventStore, InMemoryEventStore, ReplayQuery};
use sinomo_module::{LoadRequest, ModuleLoader};
use sinomo_network::{InProcessRouter, PeerRegistry};
use sinomo_node::{HostedService, NodeProfile};
use sinomo_policy::{PolicyEngine, PolicyInput};
use sinomo_registry::{ArtifactRegistry, RegistryEntry, RegistryStatus};
use sinomo_sync::{ParticipantRole, SessionParticipant, SessionSyncState, SyncRegistry};
use time::OffsetDateTime;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let event_service_capability = CapabilityName::new("service.events.local")?;
    let evolution_capability = CapabilityName::new("evolution.job.scoped")?;
    let module_loader_capability = CapabilityName::new("module.loader.local")?;
    let registry_capability = CapabilityName::new("registry.artifact.durable")?;

    let mut workstation = NodeProfile::local("local workstation")?
        .with_capability(NodeCapability::new(event_service_capability.clone()))
        .with_capability(NodeCapability::new(evolution_capability))
        .with_capability(NodeCapability::new(module_loader_capability.clone()))
        .with_service(HostedService::new(event_service_capability));
    workstation.node_id = sinomo_core::NodeId::new("node_desktop")?;

    let mut home_server = NodeProfile::local("home server")?
        .with_capability(NodeCapability::new(registry_capability))
        .with_capability(NodeCapability::new(module_loader_capability));
    home_server.node_id = sinomo_core::NodeId::new("node_server")?;

    let workspace_device = DeviceProfile::new(
        workstation.node_id.clone(),
        "local evolution workspace",
        sinomo_device::DeviceKind::Filesystem,
    )
    .with_capability(DeviceCapability::new(CapabilityName::new(
        "storage.workspace.local",
    )?))
    .with_permission(DevicePermission::Observe);

    let session_id = SessionId::generate();
    let mut sync_registry = SyncRegistry::new();
    let mut sync_state = SessionSyncState::new(session_id.clone(), SyncPolicy::Session);
    sync_state.add_participant(SessionParticipant {
        node_id: workstation.node_id.clone(),
        active_devices: vec![workspace_device.device_id.clone()],
        role: ParticipantRole::Origin,
        joined_at: OffsetDateTime::now_utc(),
    });
    sync_state.add_participant(SessionParticipant {
        node_id: home_server.node_id.clone(),
        active_devices: Vec::new(),
        role: ParticipantRole::MemoryHome,
        joined_at: OffsetDateTime::now_utc(),
    });
    sync_registry.upsert_session(sync_state);

    let workspace_id = WorkspaceId::new("workspace_evolution_trigger")?;
    let candidate_object_id = DfsObjectId::new("obj_trigger_candidate")?;
    let mut dfs = DfsMetadataStore::new();
    dfs.add_workspace(Workspace {
        workspace_id: workspace_id.clone(),
        display_name: "trigger evolution candidate".to_string(),
        owner_node_id: workstation.node_id.clone(),
        mounts: vec![WorkspaceMount {
            path: "modules/trigger".to_string(),
            kind: MountKind::TaskStaging,
            policy: DfsObjectPolicy::LocalOwned,
        }],
    });
    dfs.add_object(DfsObjectMetadata {
        object_id: candidate_object_id.clone(),
        workspace_id: workspace_id.clone(),
        path: "modules/trigger/silence_policy.wasm".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: workstation.node_id.clone(),
        current_locations: vec![workstation.node_id.clone()],
        content_hash: Some("sha256:phase2-candidate".to_string()),
        version: DfsVersion::new("version_1")?,
        policy: DfsObjectPolicy::LocalOwned,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    })?;

    let module_id = ModuleId::new("module.trigger.silence_policy")?;
    let module_manifest = ModuleManifest::r3_wasm(module_id.clone(), "0.3.7", "sinomo-trigger");
    let candidate_artifact_id = ArtifactId::generate();
    let scorecard = EvaluationScorecard {
        scorecard_id: ScorecardId::generate(),
        baseline_artifact_id: None,
        candidate_artifact_id: Some(candidate_artifact_id.clone()),
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
    let rollout_plan = RolloutPlan {
        rollout_plan_id: RolloutPlanId::generate(),
        artifact_id: candidate_artifact_id.clone(),
        module_id: module_id.clone(),
        target_ring: TrustRing::R3SandboxedModule,
        strategy: RolloutStrategy::Shadow,
        target_nodes: vec![workstation.node_id.clone()],
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
            workspace_id: Some(workspace_id.clone()),
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
        workstation.node_id.clone(),
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
        "rollout_plan": rollout_plan
    }));

    let mut store = InMemoryEventStore::new();
    let receipt = store.append(event)?;
    let replayed = store.replay(ReplayQuery::by_session(session_id.clone()))?;
    let stored_event = store
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))?
        .pop()
        .expect("just-appended event should replay");

    let mut peer_registry = PeerRegistry::new();
    peer_registry.register_node(workstation.clone());
    peer_registry.register_node(home_server.clone());
    peer_registry.join_session(session_id, workstation.node_id.clone())?;
    peer_registry.join_session(
        stored_event.event.session_id.clone().expect("session id"),
        home_server.node_id.clone(),
    )?;
    let mut router = InProcessRouter::new(peer_registry);
    let deliveries = router.route_event(&stored_event)?;

    let write_route = dfs.route_write(
        DfsWriteRequest {
            workspace_id,
            object_id: candidate_object_id,
            target_version: DfsVersion::new("version_2")?,
            requesting_node_id: home_server.node_id.clone(),
        },
        &home_server.node_id,
    )?;

    let policy_decision = PolicyEngine::new().decide(PolicyInput {
        target_ring: TrustRing::R3SandboxedModule,
        target_artifact_id: candidate_artifact_id.clone(),
        capability_diff: CapabilityDiff::default(),
        scorecard: Some(scorecard.clone()),
        rollout_plan: Some(rollout_plan.clone()),
        recent_incidents: Vec::new(),
    });

    let artifact = ArtifactManifest {
        artifact_id: candidate_artifact_id.clone(),
        module_id: module_id.clone(),
        target_ring: TrustRing::R3SandboxedModule,
        content_hash: "sha256:phase2-candidate".to_string(),
        signature: Some("local-dev-signature".to_string()),
        source_workspace: Some(WorkspaceId::new("workspace_evolution_trigger")?),
        source_revision: Some("git:phase2-smoke".to_string()),
        module_manifest,
        provenance: ArtifactProvenance {
            produced_by: ActorRef {
                kind: ActorKind::EvolutionOrchestrator,
                id: "sinomo-app-phase2".to_string(),
            },
            source_event_id: Some(receipt.event_id.clone()),
            build_log_artifact_id: None,
            attestations: Vec::new(),
        },
    };

    let mut registry = ArtifactRegistry::new();
    registry.register(RegistryEntry {
        artifact: artifact.clone(),
        policy_decision: policy_decision.clone(),
        rollout_plan: Some(rollout_plan.clone()),
        status: RegistryStatus::Approved,
        registered_at: OffsetDateTime::now_utc(),
    })?;

    let load_decision =
        ModuleLoader::new(workstation.node_id.clone()).evaluate_load(LoadRequest {
            node_id: workstation.node_id.clone(),
            artifact,
            policy_decision,
            rollout_plan,
        })?;

    println!(
        "sinomo-app: appended {} sequence={} kind=evolution.job.scoped",
        receipt.event_id, receipt.sequence.0
    );
    println!(
        "sinomo-app: replayed {} self-evolution event(s), routed {} delivery record(s)",
        replayed.len(),
        deliveries.len()
    );
    println!(
        "sinomo-app: dfs write route={:?}; loader action={:?}",
        write_route.action, load_decision.action
    );

    Ok(())
}
