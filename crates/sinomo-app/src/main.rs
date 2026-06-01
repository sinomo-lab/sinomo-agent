use sinomo_actions::{ActionExecutor, ActionSandboxPolicy};
use sinomo_agent::{BoundedContextBuilder, InteractiveAgent};
use sinomo_audio::AudioPerceptionWorker;
use sinomo_deliberation::{
    DeliberationJobRecord, DeliberationQueue, DeliberationWorker, WorkspaceSnapshot,
};
use sinomo_evolution::{EvolutionOpportunity, EvolutionPlanner, TaskId, WorkspaceId};
use sinomo_fusion::FusionWorker;
use sinomo_gateway::{InputAdapter, SyntheticInputAdapter};
use sinomo_lm::{
    AnthropicConfig, AnthropicProvider, ClaudeCodeConfig, ClaudeCodeProvider, CodexCliConfig,
    CodexCliProvider, CommandLanguageModelProvider, GeminiConfig, GeminiProvider,
    GitHubCopilotCliConfig, GitHubCopilotCliProvider, GitHubCopilotConfig, GitHubCopilotProvider,
    LanguageModelProvider, OpenAiConfig, OpenAiProvider, PROVIDER_DEFAULT_MODEL,
};
use sinomo_memory::{MemoryManager, MemoryPolicy};
use sinomo_runtime::api::ApiService;
use sinomo_runtime::core::{
    ActorKind, ActorRef, ArtifactManifest, ArtifactProvenance, CapabilityDiff, CapabilityName,
    DeviceCapability, DfsObjectPolicy, DfsWriteRequest, EvaluationGate, ModuleManifest,
    ModuleResourceLimits, NodeCapability, RolloutPlan, RolloutStrategy, SyncPolicy, TrustRing,
};
use sinomo_runtime::device::{DevicePermission, DeviceProfile};
use sinomo_runtime::dfs::{
    DfsFileSystem, DfsMetadataStore, DfsObjectId, DfsObjectKind, DfsObjectMetadata,
    DfsSubtreePolicy, DfsVersion, sifs_file_system_id,
};
use sinomo_runtime::eval::{
    CommandEvaluationSpec, CommandEvaluationSuite, LocalEvalHarness, ReplayFixtureRef,
};
use sinomo_runtime::events::{
    EventSequence, EventStore, InMemoryEventStore, ReplayQuery, SessionId, StoredEvent,
};
use sinomo_runtime::module::{ArtifactId, LoadRequest, ModuleId, ModuleLoader, RolloutPlanId};
use sinomo_runtime::network::{InProcessRouter, PeerRegistry};
use sinomo_runtime::node::{HostedService, NodeId, NodeProfile};
use sinomo_runtime::policy::{PolicyEngine, PolicyInput};
use sinomo_runtime::registry::{ArtifactRegistry, CandidateRecord, RegistryEntry, RegistryStatus};
use sinomo_runtime::rollout::{RolloutController, RolloutObservation};
use sinomo_runtime::sandbox::{
    SandboxCommand, SandboxHarness, SandboxRunRequest, SandboxRunSubject, SandboxWorkspaceSnapshot,
};
use sinomo_runtime::sync::{ParticipantRole, SessionParticipant, SessionSyncState, SyncRegistry};
use sinomo_trigger::RuleBasedTrigger;
use sinomo_video::VideoPerceptionWorker;
use sinomo_voice::{SpeechOutputRequest, VoiceOutputService};
use std::env;
use time::OffsetDateTime;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    if !args.is_empty() {
        sinomo_cli::run(args)?;
        return Ok(());
    }

    let event_service_capability = CapabilityName::new("service.events.local")?;
    let evolution_capability = CapabilityName::new("evolution.job.scoped")?;
    let module_loader_capability = CapabilityName::new("module.loader.local")?;
    let registry_capability = CapabilityName::new("registry.artifact.durable")?;
    let action_capability = CapabilityName::new("action.desktop.local")?;
    let voice_capability = CapabilityName::new("voice.output.local")?;
    let api_capability = CapabilityName::new("api.operator.local")?;

    let mut workstation = NodeProfile::local("local workstation")?
        .with_capability(NodeCapability::new(event_service_capability.clone()))
        .with_capability(NodeCapability::new(evolution_capability))
        .with_capability(NodeCapability::new(module_loader_capability.clone()))
        .with_capability(NodeCapability::new(action_capability.clone()))
        .with_capability(NodeCapability::new(voice_capability.clone()))
        .with_capability(NodeCapability::new(api_capability.clone()))
        .with_service(HostedService::new(event_service_capability));
    workstation.node_id = NodeId::from_legacy_named("node_desktop")?;

    let mut home_server = NodeProfile::local("home server")?
        .with_capability(NodeCapability::new(registry_capability))
        .with_capability(NodeCapability::new(module_loader_capability));
    home_server.node_id = NodeId::from_legacy_named("node_server")?;

    let workspace_device = DeviceProfile::new(
        workstation.node_id.clone(),
        "local evolution workspace",
        sinomo_runtime::device::DeviceKind::Filesystem,
    )
    .with_capability(DeviceCapability::new(CapabilityName::new(
        "storage.workspace.local",
    )?))
    .with_permission(DevicePermission::Observe);
    let microphone = DeviceProfile::mock_microphone(workstation.node_id.clone())
        .with_capability(DeviceCapability::new(CapabilityName::new(
            "input.audio.synthetic",
        )?))
        .with_permission(DevicePermission::Observe);
    let screen = DeviceProfile::mock_screen(workstation.node_id.clone())
        .with_capability(DeviceCapability::new(CapabilityName::new(
            "input.video.screen_ocr",
        )?))
        .with_permission(DevicePermission::Observe)
        .with_permission(DevicePermission::ExecuteAction);
    let speaker = DeviceProfile::new(
        workstation.node_id.clone(),
        "local speaker",
        sinomo_runtime::device::DeviceKind::Speaker,
    )
    .with_capability(DeviceCapability::new(CapabilityName::new(
        "voice.output.synthetic",
    )?))
    .with_permission(DevicePermission::EmitOutput);

    let session_id = SessionId::generate();
    let mut sync_registry = SyncRegistry::new();
    let mut sync_state = SessionSyncState::new(session_id.clone(), SyncPolicy::Session);
    sync_state.add_participant(SessionParticipant {
        node_id: workstation.node_id.clone(),
        active_devices: vec![
            workspace_device.device_id.clone(),
            microphone.device_id.clone(),
            screen.device_id.clone(),
            speaker.device_id.clone(),
        ],
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

    let workspace_id = WorkspaceId::from_legacy_named("workspace_evolution_trigger")?;
    let candidate_object_id = DfsObjectId::from_legacy_named("obj_trigger_candidate")?;
    let sifs_id = sifs_file_system_id();
    let mut dfs = DfsMetadataStore::new();
    dfs.add_file_system(DfsFileSystem {
        file_system_id: sifs_id.clone(),
        display_name: "trigger evolution candidate".to_string(),
        owner_node_id: workstation.node_id.clone(),
        subtree_policies: vec![DfsSubtreePolicy {
            path_prefix: "modules/trigger".to_string(),
            policy: DfsObjectPolicy::LocalOwned,
        }],
    });
    dfs.add_object(DfsObjectMetadata {
        object_id: candidate_object_id.clone(),
        file_system_id: sifs_id.clone(),
        path: "modules/trigger/silence_policy.wasm".to_string(),
        kind: DfsObjectKind::Artifact,
        owner_node_id: workstation.node_id.clone(),
        current_locations: vec![workstation.node_id.clone()],
        content_hash: Some("sha256:phase2-candidate".to_string()),
        content_size_bytes: None,
        link_target_object_id: None,
        mode: 0o644,
        uid: 0,
        gid: 0,
        accessed_at_unix_nanos: 0,
        modified_at_unix_nanos: 0,
        created_at_unix_nanos: 0,
        changed_at_unix_nanos: 0,
        version: DfsVersion::from_legacy_named("version_1")?,
        policy: DfsObjectPolicy::LocalOwned,
        created_by_event_id: None,
        last_modified_by_event_id: None,
    })?;

    let module_id = ModuleId::from_legacy_named("module.trigger.silence_policy")?;
    let module_manifest = ModuleManifest::r3_wasm(module_id.clone(), "0.3.7", "sinomo-trigger");
    let candidate_artifact_id = ArtifactId::generate();
    let planner = EvolutionPlanner::new(workstation.node_id.clone());
    let (evolution_job, candidate_workspace) = planner.scope_job(EvolutionOpportunity {
        observed_event_id: None,
        target_ring: TrustRing::R3SandboxedModule,
        target_module_id: Some(module_id.clone()),
        objective: "Reduce false positive interruptions during low-salience screen changes"
            .to_string(),
        workspace_id: workspace_id.clone(),
        allowed_paths: vec![
            "crates/sinomo-trigger/**".to_string(),
            "modules/trigger/**".to_string(),
        ],
        forbidden_paths: vec![
            "crates/sinomo-core/**".to_string(),
            "docs/security/**".to_string(),
        ],
        priority: 0.86,
    });
    let sandbox = SandboxHarness::new(workstation.node_id.clone());
    let sandbox_result = sandbox.run(SandboxRunRequest {
        subject: SandboxRunSubject {
            subject_id: evolution_job.job_id.clone(),
            workspace_id: evolution_job.scope.workspace_id.clone(),
            description: evolution_job.objective.clone(),
        },
        candidate_artifact_id: candidate_artifact_id.clone(),
        workspace_snapshot: SandboxWorkspaceSnapshot {
            workspace_id: workspace_id.clone(),
            object_ids: vec![candidate_object_id.clone()],
            source_event_id: None,
        },
        limits: ModuleResourceLimits::default(),
        commands: vec![SandboxCommand {
            name: "trigger replay unit tests".to_string(),
            command: "cargo test -p sinomo-trigger".to_string(),
            expected_exit_code: 0,
            observed_exit_code: 0,
        }],
    });
    let eval_harness = LocalEvalHarness::new(home_server.node_id.clone());
    let scorecard = eval_harness.run(CommandEvaluationSuite {
        candidate_artifact_id: candidate_artifact_id.clone(),
        baseline_artifact_id: None,
        specs: vec![
            CommandEvaluationSpec {
                gate: EvaluationGate::ManifestValidation,
                command: "validate module manifest".to_string(),
                expected_exit_code: 0,
                observed_exit_code: 0,
                replay_fixtures: Vec::new(),
                log_excerpt: Some("manifest compatible with R3 sandbox constraints".to_string()),
            },
            CommandEvaluationSpec {
                gate: EvaluationGate::ReplayTests,
                command: "cargo test -p sinomo-trigger".to_string(),
                expected_exit_code: 0,
                observed_exit_code: 0,
                replay_fixtures: vec![ReplayFixtureRef {
                    name: "low salience alert replay".to_string(),
                    fixture_uri: "fixtures/replay/low-salience-alert.jsonl".to_string(),
                    query: ReplayQuery::by_session(session_id.clone()),
                }],
                log_excerpt: Some("trigger replay passed".to_string()),
            },
        ],
    })?;
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

    let event = planner
        .job_event(&evolution_job, &candidate_workspace)
        .with_origin_device(workspace_device.device_id.clone())
        .with_session(session_id.clone())
        .with_payload(serde_json::json!({
            "job": evolution_job,
            "candidate_workspace": candidate_workspace,
            "module_manifest": module_manifest,
            "sandbox_result": sandbox_result,
            "scorecard": scorecard,
            "rollout_plan": rollout_plan
        }));

    let mut store = InMemoryEventStore::new();
    let receipt = store.append(event)?;
    store.append(sandbox.result_event(&sandbox_result))?;
    store.append(eval_harness.scorecard_event(Some(evolution_job.job_id.clone()), &scorecard))?;
    let mut audio_adapter = SyntheticInputAdapter::audio_transcript(
        &microphone,
        stored_session(&store, &receipt)?,
        "Please check the alert and tell me if it is urgent",
    )?;
    let mut screen_adapter = SyntheticInputAdapter::screen_observation(
        &screen,
        stored_session(&store, &receipt)?,
        "Alert dialog visible in the deployment dashboard",
    )?;
    let audio_chunk = audio_adapter
        .next_chunk()?
        .expect("synthetic audio adapter has one chunk");
    let screen_chunk = screen_adapter
        .next_chunk()?
        .expect("synthetic screen adapter has one chunk");
    let audio_events = AudioPerceptionWorker::new().process_chunk(&audio_chunk);
    let video_events = VideoPerceptionWorker::new().process_chunk(&screen_chunk);
    let mut perception_events = Vec::new();
    perception_events.extend(audio_events);
    perception_events.extend(video_events);
    let fused_event = FusionWorker::new()
        .align(&perception_events)
        .expect("audio and screen observations fuse");
    let perception_count = perception_events.len();
    let mut trigger_source = None;
    for perception_event in perception_events {
        let is_transcript = perception_event.kind.as_str() == "speech.transcript.final";
        let perception_receipt = store.append(perception_event.clone())?;
        if is_transcript {
            trigger_source = Some(StoredEvent {
                sequence: perception_receipt.sequence,
                appended_at: perception_receipt.appended_at,
                event: perception_event,
            });
        }
    }
    store.append(fused_event)?;
    let trigger_source = trigger_source.expect("synthetic audio produced transcript event");
    let trigger = RuleBasedTrigger::default();
    let trigger_decision = trigger.evaluate(&trigger_source);
    let trigger_event = trigger.decision_event(&trigger_decision, &trigger_source.event);
    store.append(trigger_event)?;

    let context = BoundedContextBuilder::new(8).build(&store, session_id.clone())?;
    let turn = InteractiveAgent::new(workstation.node_id.clone(), vec![screen.clone()])
        .handle_trigger(&trigger_decision, &context);
    let action_target = turn
        .action_request
        .as_ref()
        .and_then(|request| request.target_node_id.clone());
    let agent_trace_receipt = store.append(turn.trace_event.clone())?;
    let action_executor = ActionExecutor::new(
        ActionSandboxPolicy::local(workstation.node_id.clone()).allow_kind("ui.inspect.alert"),
    )
    .with_device(screen.clone());
    let action_record = turn
        .action_request
        .clone()
        .map(|request| {
            let record = action_executor.execute(request);
            let event = action_executor.execution_event(&record, Some(context.session_id.clone()));
            store.append(event)?;
            Ok::<_, sinomo_runtime::events::EventStoreError>(record)
        })
        .transpose()?;
    let mut voice =
        VoiceOutputService::new(workstation.node_id.clone()).with_speaker(speaker.clone());
    let voice_record = voice.speak(SpeechOutputRequest {
        request_id: TaskId::generate(),
        origin_node_id: workstation.node_id.clone(),
        target_node_id: workstation.node_id.clone(),
        target_device_id: speaker.device_id.clone(),
        session_id: Some(context.session_id.clone()),
        text: turn.response.text.clone(),
        allow_barge_in: true,
    });
    store.append(voice.output_event(&voice_record))?;

    let mut memory = MemoryManager::new(MemoryPolicy {
        hot_event_limit: 16,
        warm_salience_threshold: 0.7,
    });
    for stored in store.replay(ReplayQuery::by_session(session_id.clone()))? {
        memory.ingest(stored);
    }
    let memory_packet = memory.context_packet(&session_id);
    let mut deliberation_queue = DeliberationQueue::new();
    let deliberation_job = DeliberationJobRecord::new(
        trigger_decision.trigger_event_id.clone(),
        Some(session_id.clone()),
        "Review the visible alert, preserve context, and prepare a slower follow-up.",
        memory_packet.clone(),
        Some(WorkspaceSnapshot {
            workspace_id: workspace_id.clone(),
            object_ids: vec![candidate_object_id.clone()],
            source_event_id: Some(agent_trace_receipt.event_id.clone()),
        }),
    );
    deliberation_queue.enqueue(deliberation_job);
    let running_job = deliberation_queue
        .next_job()
        .expect("queued deliberation job should run");
    let worker = DeliberationWorker::new(home_server.node_id.clone());
    let completed_job = if let Some(configured_model) = configured_deliberation_model()? {
        worker.summarize_with_model(
            running_job,
            configured_model.provider_client.as_ref(),
            configured_model.provider,
            configured_model.model,
        )?
    } else {
        worker.summarize(running_job)
    };
    let summary_event = worker.summary_event(&completed_job)?;
    let summary_receipt = store.append(summary_event.clone())?;
    deliberation_queue.complete(
        completed_job,
        summary_event
            .payload
            .get("summary")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .expect("summary event contains deliberation summary"),
    );
    let summary_stored = store
        .replay(ReplayQuery::all().after(EventSequence(summary_receipt.sequence.0 - 1)))?
        .pop()
        .expect("summary event should replay");
    memory.ingest(summary_stored);
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
            file_system_id: sifs_id,
            object_id: candidate_object_id,
            target_version: DfsVersion::from_legacy_named("version_2")?,
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
        source_workspace: Some(WorkspaceId::from_legacy_named(
            "workspace_evolution_trigger",
        )?),
        source_revision: Some("git:phase2-smoke".to_string()),
        module_manifest,
        provenance: ArtifactProvenance {
            produced_by: ActorRef {
                kind: ActorKind::Operator,
                id: "sinomo-app-phase2".to_string(),
            },
            source_event_id: Some(receipt.event_id.clone()),
            build_log_artifact_id: None,
            attestations: Vec::new(),
        },
    };

    let mut registry = ArtifactRegistry::new();
    registry.register_candidate(CandidateRecord {
        artifact: artifact.clone(),
        source_job_id: Some(evolution_job.job_id.clone()),
        scorecard: Some(scorecard.clone()),
        evidence_refs: vec![format!("sandbox:{:?}", sandbox_result.status)],
        registered_at: OffsetDateTime::now_utc(),
    })?;
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
            rollout_plan: rollout_plan.clone(),
        })?;
    let rollout_controller = RolloutController::new(workstation.node_id.clone());
    let rollout_state = rollout_controller.initial_state(
        rollout_plan.clone(),
        ArtifactId::from_legacy_named("artifact_active_trigger").expect("artifact"),
    );
    let rollout_decision = rollout_controller.advance(
        rollout_state,
        RolloutObservation {
            scorecard: Some(scorecard.clone()),
            incident_event_ids: Vec::new(),
            user_correction_count: 0,
            rollback_triggers: Vec::new(),
        },
    );
    store.append(rollout_controller.decision_event(&rollout_decision))?;
    let api = ApiService::new(workstation.node_id.clone());
    let health = api.health(&[workstation.clone(), home_server.clone()]);
    let api_replay = api.replay(
        &store,
        ReplayQuery::by_session(stored_event.event.session_id.clone().expect("session id"))
            .limit(5),
    )?;
    let supervision_snapshot = api.supervision_snapshot(&store)?;

    println!(
        "sinomo-app: appended {} sequence={} kind=evolution.job.scoped",
        receipt.event_id, receipt.sequence.0
    );
    println!(
        "sinomo-app: replayed {} session event(s), routed {} delivery record(s)",
        replayed.len(),
        deliveries.len()
    );
    println!(
        "sinomo-app: appended {} perception event(s) plus 1 fusion event",
        perception_count
    );
    println!(
        "sinomo-app: trigger action={:?}; agent response={}; action target={:?}",
        trigger_decision.action, turn.response.text, action_target
    );
    println!(
        "sinomo-app: action status={:?}; voice status={:?}; api health={:?}; api replay={}",
        action_record.as_ref().map(|record| record.status.clone()),
        voice_record.status,
        health.status,
        api_replay.len()
    );
    println!(
        "sinomo-app: hot memory={} warm memory={} deliberation completed={}",
        memory.hot().len(),
        memory.warm().entries().len(),
        deliberation_queue.completed().len()
    );
    println!(
        "sinomo-app: dfs write route={:?}; loader action={:?}; rollout={:?}; incidents={}",
        write_route.action,
        load_decision.action,
        rollout_decision.action,
        supervision_snapshot.incidents.len()
    );

    Ok(())
}

struct ConfiguredDeliberationModel {
    provider_client: Box<dyn LanguageModelProvider>,
    provider: String,
    model: String,
}

fn configured_deliberation_model()
-> Result<Option<ConfiguredDeliberationModel>, Box<dyn std::error::Error>> {
    let provider =
        env::var("SINOMO_DELIBERATION_LM_PROVIDER").unwrap_or_else(|_| "command".to_string());
    if provider == GitHubCopilotProvider::PROVIDER_ID {
        let model = GitHubCopilotConfig::default().default_model;
        let api_base_url = env::var("SINOMO_COPILOT_API_BASE_URL")
            .unwrap_or_else(|_| GitHubCopilotConfig::default().api_base_url);
        let github_host = env::var("SINOMO_COPILOT_GITHUB_HOST")
            .unwrap_or_else(|_| GitHubCopilotConfig::default().github_host);
        let client_id = env::var("SINOMO_COPILOT_CLIENT_ID")
            .unwrap_or_else(|_| GitHubCopilotConfig::default().client_id);
        let provider_client = GitHubCopilotProvider::new(GitHubCopilotConfig {
            client_id,
            github_host,
            api_base_url,
            default_model: model.clone(),
            token_store_path: GitHubCopilotConfig::default().token_store_path,
        });

        return Ok(Some(ConfiguredDeliberationModel {
            provider_client: Box::new(provider_client),
            provider,
            model,
        }));
    }

    if provider == GitHubCopilotCliProvider::PROVIDER_ID {
        let model = PROVIDER_DEFAULT_MODEL.to_string();
        let program = env::var("SINOMO_DELIBERATION_LM_PROGRAM")
            .unwrap_or_else(|_| GitHubCopilotCliConfig::default().program);
        let extra_args: Vec<String> = match env::var("SINOMO_DELIBERATION_LM_ARGS_JSON") {
            Ok(value) => serde_json::from_str(&value)?,
            Err(_) => Vec::new(),
        };
        let config_model = if model == PROVIDER_DEFAULT_MODEL {
            GitHubCopilotCliConfig::default().model
        } else {
            model.clone()
        };
        let provider_client = GitHubCopilotCliProvider::new(GitHubCopilotCliConfig {
            program,
            model: config_model,
            extra_args,
            ..GitHubCopilotCliConfig::default()
        });

        return Ok(Some(ConfiguredDeliberationModel {
            provider_client: Box::new(provider_client),
            provider,
            model,
        }));
    }

    if provider == CodexCliProvider::PROVIDER_ID {
        let model = PROVIDER_DEFAULT_MODEL.to_string();
        let program = env::var("SINOMO_DELIBERATION_LM_PROGRAM")
            .unwrap_or_else(|_| CodexCliConfig::default().program);
        let extra_args: Vec<String> = match env::var("SINOMO_DELIBERATION_LM_ARGS_JSON") {
            Ok(value) => serde_json::from_str(&value)?,
            Err(_) => Vec::new(),
        };
        let config_model = if model == PROVIDER_DEFAULT_MODEL {
            CodexCliConfig::default().model
        } else {
            model.clone()
        };
        let provider_client = CodexCliProvider::new(CodexCliConfig {
            program,
            model: config_model,
            extra_args,
            ..CodexCliConfig::default()
        });

        return Ok(Some(ConfiguredDeliberationModel {
            provider_client: Box::new(provider_client),
            provider: CodexCliProvider::PROVIDER_ID.to_string(),
            model,
        }));
    }

    if provider == ClaudeCodeProvider::PROVIDER_ID || provider == "anthropic-claude-code" {
        let model = PROVIDER_DEFAULT_MODEL.to_string();
        let program = env::var("SINOMO_DELIBERATION_LM_PROGRAM")
            .unwrap_or_else(|_| ClaudeCodeConfig::default().program);
        let extra_args: Vec<String> = match env::var("SINOMO_DELIBERATION_LM_ARGS_JSON") {
            Ok(value) => serde_json::from_str(&value)?,
            Err(_) => Vec::new(),
        };
        let config_model = if model == PROVIDER_DEFAULT_MODEL {
            ClaudeCodeConfig::default().model
        } else {
            model.clone()
        };
        let provider_client = ClaudeCodeProvider::new(ClaudeCodeConfig {
            program,
            model: config_model,
            extra_args,
            ..ClaudeCodeConfig::default()
        });

        return Ok(Some(ConfiguredDeliberationModel {
            provider_client: Box::new(provider_client),
            provider: ClaudeCodeProvider::PROVIDER_ID.to_string(),
            model,
        }));
    }

    if provider == AnthropicProvider::PROVIDER_ID || provider == "claude" {
        let model = AnthropicConfig::default().default_model;
        let api_base_url = env::var("SINOMO_ANTHROPIC_API_BASE_URL")
            .or_else(|_| env::var("SINOMO_CLAUDE_API_BASE_URL"))
            .unwrap_or_else(|_| AnthropicConfig::default().api_base_url);
        let provider_client = AnthropicProvider::new(AnthropicConfig {
            api_base_url,
            default_model: model.clone(),
            api_key: None,
            credentials_path: AnthropicConfig::default().credentials_path,
        });

        return Ok(Some(ConfiguredDeliberationModel {
            provider_client: Box::new(provider_client),
            provider: AnthropicProvider::PROVIDER_ID.to_string(),
            model,
        }));
    }

    if provider == GeminiProvider::PROVIDER_ID || provider == "google-gemini" {
        let model = GeminiConfig::default().default_model;
        let api_base_url = env::var("SINOMO_GEMINI_API_BASE_URL")
            .unwrap_or_else(|_| GeminiConfig::default().api_base_url);
        let provider_client = GeminiProvider::new(GeminiConfig {
            api_base_url,
            default_model: model.clone(),
            api_key: None,
            credentials_path: GeminiConfig::default().credentials_path,
        });

        return Ok(Some(ConfiguredDeliberationModel {
            provider_client: Box::new(provider_client),
            provider: GeminiProvider::PROVIDER_ID.to_string(),
            model,
        }));
    }

    if provider == OpenAiProvider::PROVIDER_ID || provider == "openai-api" {
        let model = OpenAiConfig::default().default_model;
        let api_base_url = env::var("SINOMO_OPENAI_API_BASE_URL")
            .or_else(|_| env::var("SINOMO_CODEX_API_BASE_URL"))
            .unwrap_or_else(|_| OpenAiConfig::default().api_base_url);
        let provider_client = OpenAiProvider::new(OpenAiConfig {
            api_base_url,
            default_model: model.clone(),
            api_key: None,
            credentials_path: OpenAiConfig::default().credentials_path,
        });

        return Ok(Some(ConfiguredDeliberationModel {
            provider_client: Box::new(provider_client),
            provider: OpenAiProvider::PROVIDER_ID.to_string(),
            model,
        }));
    }

    let Ok(program) = env::var("SINOMO_DELIBERATION_LM_PROGRAM") else {
        return Ok(None);
    };
    let args: Vec<String> = match env::var("SINOMO_DELIBERATION_LM_ARGS_JSON") {
        Ok(value) => serde_json::from_str(&value)?,
        Err(_) => Vec::new(),
    };
    let model = program.clone();
    let mode = env::var("SINOMO_DELIBERATION_LM_MODE").unwrap_or_else(|_| "plain".to_string());
    let provider_client = match mode.as_str() {
        "json" => CommandLanguageModelProvider::json_request(
            program,
            args,
            provider.clone(),
            model.clone(),
        ),
        _ => CommandLanguageModelProvider::plain_prompt(
            program,
            args,
            provider.clone(),
            model.clone(),
        ),
    };

    Ok(Some(ConfiguredDeliberationModel {
        provider_client: Box::new(provider_client),
        provider,
        model,
    }))
}

fn stored_session(
    store: &InMemoryEventStore,
    receipt: &sinomo_runtime::events::AppendReceipt,
) -> Result<SessionId, Box<dyn std::error::Error>> {
    let event = store
        .replay(ReplayQuery::all().after(EventSequence(receipt.sequence.0 - 1)))?
        .into_iter()
        .find(|stored| stored.sequence == receipt.sequence)
        .expect("stored event");
    Ok(event
        .event
        .session_id
        .expect("phase smoke event has session"))
}
