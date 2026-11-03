use super::*;
use crate::{WorkspaceId, read_stored_job, write_stored_job};
use sinomo_lm::{
    LanguageModelHostAccess, LanguageModelProviderInfo, LanguageModelResponse, LanguageModelUsage,
    PROVIDER_DEFAULT_MODEL,
};
use sinomo_runtime::core::{
    Confidence, EvaluationOutcome, EventEnvelope, EventKind, Id, LatencyClass, PrivacyClass,
    RollbackPolicy, RoutingMetadata, RoutingScope, Salience, Source,
};
use sinomo_runtime::eval::LocalCommandRunner;
use sinomo_runtime::events::EventId;
use sinomo_runtime::module::{
    MODULE_INVOCATION_DECISION_EVENT_KIND, MODULE_TIMEOUT_EVENT_KIND, ModuleId,
};
use sinomo_runtime::node::{LocalNodePermission, LocalNodePermissionStore, LocalNodePermissions};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type ScorecardId = Id;

#[test]
fn campaign_proposal_contains_inspectable_phase_zero_plan() {
    let proposal = propose_evolution_campaign(CampaignProposalRequest {
        objective: "improve the self-evolution scheduler so it can pause and resume around resource budgets".to_string(),
        horizon: "30 days".to_string(),
        budget_policy: "free-and-prepaid".to_string(),
        reserve_floors: vec![ReserveFloor {
            resource: "coding-plan-weekly".to_string(),
            floor_percent: 30,
        }],
        approval_policy: "stage-rollout".to_string(),
        max_wall_time: Some("10m".to_string()),
        pause_after_proposal: true,
        dry_run: true,
        approved: false,
        target_ring: TrustRing::R1TrustedRuntime,
        source_repo: Some("/workspace/sinomo".to_string()),
    });

    assert!(proposal.campaign_id.starts_with("camp_"));
    assert_eq!(proposal.status, EvolutionCampaignStatus::AwaitingApproval);
    assert_eq!(proposal.objective_card.initial_horizon, "30 days");
    assert!(
        proposal
            .objective_card
            .failure_conditions
            .iter()
            .any(|condition| condition.contains("reserve"))
    );
    assert!(proposal.stages.len() >= 3);
    assert!(
        proposal
            .stages
            .iter()
            .all(|stage| !stage.acceptance_metrics.is_empty())
    );
    assert!(
        proposal
            .stages
            .iter()
            .all(|stage| !stage.failure_conditions.is_empty())
    );
    assert!(proposal.near_term_tasks.len() >= 2);
    assert_eq!(proposal.budget_policy.reserve_floors[0].floor_percent, 30);
    assert!(
        proposal
            .budget_policy
            .scheduling_notes
            .iter()
            .any(|note| note.contains("max wall time: 10m"))
    );
    assert!(
        proposal
            .budget_policy
            .scheduling_notes
            .iter()
            .any(|note| note.contains("pause after proposal"))
    );
    assert_eq!(
        proposal.iteration_results,
        Vec::<EvolutionIterationResult>::new()
    );
    assert!(
        proposal
            .decisions
            .iter()
            .any(|decision| decision.kind == CampaignDecisionKind::ProposalCreated)
    );
    assert_eq!(
        proposal.approval_checkpoint.status,
        ApprovalCheckpointStatus::AwaitingHuman
    );
    assert!(proposal.capability_registry_required == Some(false));
}

#[test]
fn external_workload_helpers_create_defaults_and_reuse_existing_records() {
    let mut workloads = Vec::new();
    let workload = ensure_cluster_long_horizon_workload(&mut workloads, "backup", "snapshot-01");
    workload.resource_estimates.push(ResourceUsageEstimate {
        task_id: "snapshot-01".to_string(),
        resource: "coding-plan-weekly".to_string(),
        estimated_units: 10,
        actual_units: None,
    });

    let same = ensure_cluster_long_horizon_workload(&mut workloads, "backup", "snapshot-01");

    assert_eq!(same.title, "snapshot-01");
    assert_eq!(same.status, ClusterLongHorizonWorkloadStatus::Ready);
    assert_eq!(same.resource_estimates.len(), 1);
    assert_eq!(workloads.len(), 1);
}

#[test]
fn external_workload_human_checkpoint_pauses_ready_workload() {
    let mut workload = new_cluster_long_horizon_workload(
        "curation",
        "dataset-42",
        "Dataset 42",
        ClusterLongHorizonWorkloadStatus::Ready,
        Vec::new(),
    );

    require_cluster_long_horizon_workload_human_checkpoint(&mut workload);

    assert!(workload.human_checkpoint_required);
    assert_eq!(
        workload.status,
        ClusterLongHorizonWorkloadStatus::PausedHuman
    );
    assert_eq!(cluster_workload_key(&workload), "curation:dataset-42");
}

#[test]
fn external_workload_parser_merges_estimates_and_checkpoints() {
    let workloads = parse_background_tick_external_workloads(
        &["backup:snapshot-01:ready:Backup snapshot".to_string()],
        &["backup:snapshot-01:cluster-hours:40".to_string()],
        &["curation:dataset-42".to_string()],
    )
    .expect("workloads");

    assert_eq!(workloads.len(), 2);
    assert_eq!(workloads[0].system, "backup");
    assert_eq!(workloads[0].title, "Backup snapshot");
    assert_eq!(workloads[0].resource_estimates[0].resource, "cluster-hours");
    assert_eq!(workloads[0].resource_estimates[0].estimated_units, 40);
    assert_eq!(workloads[1].system, "curation");
    assert!(workloads[1].human_checkpoint_required);
    assert_eq!(
        workloads[1].status,
        ClusterLongHorizonWorkloadStatus::PausedHuman
    );
}

#[test]
fn external_workload_parser_rejects_duplicate_specs() {
    let error = parse_background_tick_external_workloads(
        &[
            "backup:snapshot-01:ready".to_string(),
            "backup:snapshot-01:running".to_string(),
        ],
        &[],
        &[],
    )
    .expect_err("duplicate workload");

    assert!(matches!(
        error,
        EvolutionInputParseError::Invalid { detail } if detail.contains("duplicate external workload")
    ));
}

#[test]
fn reserve_floor_parser_accepts_percent_and_rejects_invalid_values() {
    let floors = parse_reserve_floors(&["coding-plan-weekly:30%".to_string()]).expect("floors");
    assert_eq!(
        floors,
        vec![ReserveFloor {
            resource: "coding-plan-weekly".to_string(),
            floor_percent: 30
        }]
    );

    let error = parse_reserve_floor("coding-plan-weekly:130%").expect_err("invalid floor");
    assert!(matches!(
        error,
        EvolutionInputParseError::Invalid { detail } if detail.contains("0-100 percent")
    ));
}

#[test]
fn campaign_proposal_request_preparation_applies_defaults() {
    let request = prepare_campaign_proposal_request(CampaignProposalInput {
        objective: "improve the scheduler".to_string(),
        horizon: None,
        budget_policy: None,
        reserve_floor_specs: vec!["coding-plan-weekly:25%".to_string()],
        approval_policy: None,
        max_wall_time: Some("10m".to_string()),
        pause_after_proposal: true,
        dry_run: true,
        approved: false,
        target_ring: TrustRing::R1TrustedRuntime,
        source_repo: Some("/workspace/sinomo".to_string()),
    })
    .expect("request");

    assert_eq!(request.horizon, "30 days");
    assert_eq!(request.budget_policy, "free-and-prepaid");
    assert_eq!(request.approval_policy, "stage-rollout");
    assert_eq!(request.reserve_floors[0].resource, "coding-plan-weekly");
    assert_eq!(request.reserve_floors[0].floor_percent, 25);
    assert_eq!(request.max_wall_time.as_deref(), Some("10m"));
    assert!(request.pause_after_proposal);
    assert!(request.dry_run);
}

#[test]
fn evolution_request_preparation_selects_model_defaults_and_verification() {
    let prepared = prepare_evolution_request(EvolutionRequestInput {
        objective: "refactor the scheduler".to_string(),
        target_ring: TrustRing::R1TrustedRuntime,
        intended_paths: vec!["modules/evolution/sinomo-evolution".to_string()],
        verification_commands: vec!["cargo test -p sinomo-evolution".to_string()],
        benchmark_specs: vec!["swebench:psf__requests-1142".to_string()],
        module_replay_report_specs: vec!["trigger=reports/trigger-replay.json".to_string()],
        provider: None,
        model: None,
        default_provider: Some("google-gemini".to_string()),
        default_model: Some("gemini-2.5-pro".to_string()),
        dry_run: false,
    })
    .expect("prepared request");

    assert_eq!(prepared.provider, GeminiProvider::PROVIDER_ID);
    assert_eq!(prepared.model, "gemini-2.5-pro");
    assert_eq!(
        prepared.request.model_provider.as_deref(),
        Some(GeminiProvider::PROVIDER_ID)
    );
    assert_eq!(prepared.request.model.as_deref(), Some("gemini-2.5-pro"));
    assert_eq!(prepared.request.verification_commands.len(), 1);
    assert_eq!(prepared.request.verification_commands[0].name, "verify-1");
    assert_eq!(
        prepared.request.verification_commands[0].gate,
        EvaluationGate::StaticChecks
    );
    assert_eq!(prepared.request.benchmark_gates.len(), 1);
    assert!(matches!(
        &prepared.request.benchmark_gates[0].kind,
        BenchmarkGateKind::SweBench {
            instance_id: Some(instance_id),
            provider,
            model,
            evaluate,
            ..
        } if instance_id == "psf__requests-1142"
            && provider == GeminiProvider::PROVIDER_ID
            && model == "gemini-2.5-pro"
            && *evaluate
    ));
    assert_eq!(prepared.request.module_replay_reports.len(), 1);
    assert_eq!(prepared.request.module_replay_reports[0].name, "trigger");
    assert_eq!(
        prepared.request.module_replay_reports[0].report_path,
        "reports/trigger-replay.json"
    );
}

#[test]
fn bootstrap_job_preserves_rust_default_verification_for_sinomo_repo() {
    let root = temp_dir("sinomo-project-profile");
    fs::create_dir_all(
        root.join("modules")
            .join("evolution")
            .join("sinomo-evolution"),
    )
    .expect("module dir");
    fs::create_dir_all(root.join("docs")).expect("docs dir");
    fs::write(root.join("Cargo.toml"), "[workspace]\n").expect("cargo toml");
    fs::write(
        root.join("docs").join("self-evolution-architecture.md"),
        "# Self Evolution\n",
    )
    .expect("self evolution doc");
    let request = EvolutionRequest {
        objective: "keep Sinomo defaults".to_string(),
        target_ring: TrustRing::R1TrustedRuntime,
        intended_paths: Vec::new(),
        verification_commands: Vec::new(),
        benchmark_gates: Vec::new(),
        module_replay_reports: Vec::new(),
        model_provider: None,
        model: None,
        dry_run: true,
    };

    let job = create_bootstrap_job(&request, &root);

    assert_eq!(job.verification.commands.len(), 1);
    assert_eq!(job.verification.commands[0].command, "cargo check");
    assert!(job.edit_scope.notes.contains("Sinomo self-evolution"));
}

#[test]
fn bootstrap_job_uses_generic_verification_for_external_python_repo() {
    let root = temp_dir("external-python-project-profile");
    fs::write(
        root.join("pyproject.toml"),
        "[project]\nname = 'external'\n",
    )
    .expect("pyproject");
    let request = EvolutionRequest {
        objective: "improve external project".to_string(),
        target_ring: TrustRing::R2TrustedService,
        intended_paths: Vec::new(),
        verification_commands: Vec::new(),
        benchmark_gates: Vec::new(),
        module_replay_reports: Vec::new(),
        model_provider: None,
        model: None,
        dry_run: true,
    };

    let job = create_bootstrap_job(&request, &root);
    let packet = mutation_packet_for_job(&job);
    let prompt = render_mutation_prompt(&packet);

    assert_eq!(job.verification.commands.len(), 1);
    assert_eq!(job.verification.commands[0].command, "git diff --check");
    assert!(job.edit_scope.notes.contains("external Python project"));
    assert!(prompt.contains("# Evolution Mutation Packet"));
    assert!(prompt.contains("target_project:"));
    assert!(prompt.contains("(python)"));
}

#[test]
fn external_campaign_uses_target_project_verification_defaults() {
    let root = temp_dir("external-campaign-project");
    fs::write(
        root.join("package.json"),
        "{\"scripts\":{\"test\":\"node test.js\"}}\n",
    )
    .expect("package json");

    let campaign = propose_evolution_campaign(CampaignProposalRequest {
        objective: "improve another project".to_string(),
        horizon: "7 days".to_string(),
        budget_policy: "free-and-prepaid".to_string(),
        reserve_floors: Vec::new(),
        approval_policy: "stage-rollout".to_string(),
        max_wall_time: None,
        pause_after_proposal: true,
        dry_run: true,
        approved: false,
        target_ring: TrustRing::R2TrustedService,
        source_repo: Some(root.display().to_string()),
    });

    assert!(
        campaign
            .near_term_tasks
            .iter()
            .all(|task| task.verification_commands == vec!["git diff --check".to_string()])
    );
    assert!(
        campaign
            .objective_card
            .constraints
            .iter()
            .any(|constraint| constraint.contains("(node)"))
    );
}

#[test]
fn orchestrator_dry_run_supports_external_python_git_repo_without_cargo() {
    let source = git_repo("external-python-dry-run-source");
    fs::write(
        source.join("pyproject.toml"),
        "[project]\nname = 'external'\n",
    )
    .expect("pyproject");
    fs::write(source.join("tool.py"), "print('ok')\n").expect("python source");
    run_in(&source, "git add pyproject.toml tool.py").expect("git add");
    run_in(&source, "git commit -m init").expect("git commit");
    let jobs_root = temp_dir("external-python-dry-run-jobs");
    let orchestrator = EvolutionOrchestrator::new(
        GitWorkspaceAdapter::new(&source, &jobs_root),
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(&jobs_root),
    );

    let report = orchestrator
        .propose(
            EvolutionRequest {
                objective: "dry run external python repo".to_string(),
                target_ring: TrustRing::R2TrustedService,
                intended_paths: vec!["*.py".to_string()],
                verification_commands: Vec::new(),
                benchmark_gates: Vec::new(),
                module_replay_reports: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: true,
            },
            None,
        )
        .expect("external dry run");

    assert_eq!(
        report
            .scorecard
            .as_ref()
            .map(|scorecard| &scorecard.outcome),
        Some(&EvaluationOutcome::Passed)
    );
    assert_eq!(
        report
            .bundle
            .command_logs
            .first()
            .map(|log| log.command.as_str()),
        Some("git diff --check")
    );
    assert!(
        report
            .job
            .edit_scope
            .notes
            .contains("external Python project")
    );
    let _ = run_in(
        &source,
        &format!(
            "git worktree remove --force {}",
            report.candidate_workspace.path.display()
        ),
    );
}

#[test]
fn swebench_benchmark_gate_records_progress_path() {
    let root = temp_dir("swebench-benchmark-progress-path");
    let request = EvolutionRequest {
        objective: "measure one SWE-bench task".to_string(),
        target_ring: TrustRing::R1TrustedRuntime,
        intended_paths: vec!["crates/sinomo-agent/**".to_string()],
        verification_commands: Vec::new(),
        benchmark_gates: parse_benchmark_gate_specs(
            &["swebench:psf__requests-1142".to_string()],
            "deepseek",
            "deepseek-v4-pro",
        )
        .expect("benchmark spec"),
        module_replay_reports: Vec::new(),
        model_provider: None,
        model: None,
        dry_run: false,
    };
    let job = create_bootstrap_job(&request, &root);
    let store = LocalCandidateBundleStore::new(root.join("jobs"));
    let bundle = store.create(&job).expect("bundle");
    let executions = prepare_benchmark_gate_executions(&request, &bundle);

    assert_eq!(executions.len(), 1);
    let execution = &executions[0];
    assert!(execution.command.command.contains("--summary-out"));
    assert!(execution.command.command.contains("--progress-out"));
    assert!(
        execution
            .report
            .progress_path
            .as_deref()
            .is_some_and(|path| path.ends_with("progress.jsonl"))
    );
    assert!(
        execution
            .report
            .progress_path
            .as_ref()
            .is_some_and(|path| execution.command.command.contains(path))
    );
}

#[test]
fn module_replay_report_specs_accept_named_and_default_refs() {
    let refs = parse_module_replay_report_specs(&[
        "trigger=reports/trigger.json".to_string(),
        "reports/default.json".to_string(),
    ])
    .expect("module replay specs");

    assert_eq!(refs[0].name, "trigger");
    assert_eq!(refs[0].report_path, "reports/trigger.json");
    assert_eq!(refs[1].name, "module-replay-2");
    assert_eq!(refs[1].report_path, "reports/default.json");
}

#[test]
fn evolution_request_preparation_rejects_unknown_provider() {
    let error = prepare_evolution_request(EvolutionRequestInput {
        objective: "refactor the scheduler".to_string(),
        target_ring: TrustRing::R1TrustedRuntime,
        intended_paths: Vec::new(),
        verification_commands: Vec::new(),
        benchmark_specs: Vec::new(),
        module_replay_report_specs: Vec::new(),
        provider: Some("unknown-provider".to_string()),
        model: None,
        default_provider: None,
        default_model: None,
        dry_run: true,
    })
    .expect_err("unknown provider");

    assert!(matches!(
        error,
        EvolutionInputParseError::Invalid { detail } if detail.contains("unknown provider")
    ));
}

#[test]
fn stage_evaluation_input_parsers_accept_known_formats() {
    assert_eq!(
        parse_failure_class("rollout-failure").expect("failure class"),
        EvolutionFailureClass::RolloutFailure
    );
    let usage = parse_stage_resource_usage("task-1:cluster-hours:8:6", None).expect("usage");
    assert_eq!(usage.task_id, "task-1");
    assert_eq!(usage.resource, "cluster-hours");
    assert_eq!(usage.estimated_units, 8);
    assert_eq!(usage.actual_units, Some(6));

    let inferred = parse_stage_resource_usage("cluster-hours:5", Some("task-2")).expect("inferred");
    assert_eq!(inferred.task_id, "task-2");
    assert_eq!(inferred.actual_units, None);
}

#[test]
fn stage_resource_usage_parser_requires_task_context() {
    let error = parse_stage_resource_usage("cluster-hours:5", None).expect_err("missing task");
    assert!(matches!(
        error,
        EvolutionInputParseError::Invalid { detail } if detail.contains("needs --task first")
    ));
}

#[test]
fn external_workload_constructor_marks_terminal_statuses() {
    let workload = new_cluster_long_horizon_workload(
        "ops",
        "complete-01",
        "complete-01",
        ClusterLongHorizonWorkloadStatus::Complete,
        Vec::new(),
    );

    assert!(workload.terminal);
}

#[test]
fn resource_classes_can_be_derived_from_reserve_floors() {
    let resources = resource_classes_from_reserve_floors(
        &[ReserveFloor {
            resource: "coding-plan-weekly".to_string(),
            floor_percent: 30,
        }],
        "derived in test",
    );

    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].resource_id, "coding-plan-weekly");
    assert_eq!(resources[0].total_units, 100);
    assert_eq!(resources[0].reserve_floor_percent, 30);
    assert_eq!(resources[0].notes, "derived in test");
}

#[test]
fn background_resource_classes_merge_duplicate_reserve_floors() {
    let mut campaign_a = propose_evolution_campaign(CampaignProposalRequest {
        objective: "improve the scheduler".to_string(),
        horizon: "30 days".to_string(),
        budget_policy: "free-and-prepaid".to_string(),
        reserve_floors: vec![ReserveFloor {
            resource: "coding-plan-weekly".to_string(),
            floor_percent: 30,
        }],
        approval_policy: "stage-rollout".to_string(),
        max_wall_time: None,
        pause_after_proposal: false,
        dry_run: true,
        approved: true,
        target_ring: TrustRing::R1TrustedRuntime,
        source_repo: None,
    });
    let mut campaign_b = campaign_a.clone();
    campaign_a.campaign_id = "camp_a".to_string();
    campaign_b.campaign_id = "camp_b".to_string();
    campaign_b.budget_policy.reserve_floors[0].floor_percent = 45;

    let resources = background_resource_classes_from_campaign_reserve_floors(
        &[campaign_a, campaign_b],
        "derived in test",
    );

    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].resource_id, "coding-plan-weekly");
    assert_eq!(resources[0].reserve_floor_percent, 45);
}

#[test]
fn default_resource_usage_estimates_use_first_task_and_resource() {
    let campaign = propose_evolution_campaign(CampaignProposalRequest {
        objective: "improve the scheduler".to_string(),
        horizon: "30 days".to_string(),
        budget_policy: "free-and-prepaid".to_string(),
        reserve_floors: vec![ReserveFloor {
            resource: "coding-plan-weekly".to_string(),
            floor_percent: 30,
        }],
        approval_policy: "stage-rollout".to_string(),
        max_wall_time: None,
        pause_after_proposal: false,
        dry_run: true,
        approved: true,
        target_ring: TrustRing::R1TrustedRuntime,
        source_repo: None,
    });
    let resources = resource_classes_from_reserve_floors(
        &campaign.budget_policy.reserve_floors,
        "derived in test",
    );

    let estimates = default_resource_usage_estimates(&campaign, &resources);

    assert_eq!(estimates.len(), 1);
    assert_eq!(estimates[0].task_id, campaign.near_term_tasks[0].task_id);
    assert_eq!(estimates[0].resource, "coding-plan-weekly");
    assert_eq!(estimates[0].estimated_units, 10);
}

#[test]
fn schedule_resource_parser_uses_campaign_reserve_floor() {
    let campaign = propose_evolution_campaign(CampaignProposalRequest {
        objective: "improve the scheduler".to_string(),
        horizon: "30 days".to_string(),
        budget_policy: "free-and-prepaid".to_string(),
        reserve_floors: vec![ReserveFloor {
            resource: "coding-plan-weekly".to_string(),
            floor_percent: 35,
        }],
        approval_policy: "stage-rollout".to_string(),
        max_wall_time: None,
        pause_after_proposal: false,
        dry_run: true,
        approved: true,
        target_ring: TrustRing::R1TrustedRuntime,
        source_repo: None,
    });

    let resource =
        parse_schedule_resource("coding-plan-weekly:75/100:weekly", &campaign).expect("resource");

    assert_eq!(resource.resource_id, "coding-plan-weekly");
    assert_eq!(resource.used_units, 75);
    assert_eq!(resource.total_units, 100);
    assert_eq!(resource.reserve_floor_percent, 35);
    assert_eq!(resource.reset_window, "weekly");
}

#[test]
fn schedule_parsers_derive_defaults_from_campaign() {
    let campaign = propose_evolution_campaign(CampaignProposalRequest {
        objective: "improve the scheduler".to_string(),
        horizon: "30 days".to_string(),
        budget_policy: "free-and-prepaid".to_string(),
        reserve_floors: vec![ReserveFloor {
            resource: "coding-plan-weekly".to_string(),
            floor_percent: 30,
        }],
        approval_policy: "stage-rollout".to_string(),
        max_wall_time: None,
        pause_after_proposal: false,
        dry_run: true,
        approved: true,
        target_ring: TrustRing::R1TrustedRuntime,
        source_repo: None,
    });

    let resources = parse_schedule_resources(&[], &campaign).expect("resources");
    let estimates = parse_schedule_estimates(&[], &campaign, &resources).expect("estimates");

    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].resource_id, "coding-plan-weekly");
    assert_eq!(resources[0].reserve_floor_percent, 30);
    assert_eq!(estimates.len(), 1);
    assert_eq!(estimates[0].task_id, campaign.near_term_tasks[0].task_id);
    assert_eq!(estimates[0].resource, "coding-plan-weekly");
    assert_eq!(estimates[0].estimated_units, 10);
}

#[test]
fn background_tick_estimates_require_campaign_context() {
    let error = parse_background_tick_estimates(&["coding-plan-weekly:10".to_string()], &[])
        .expect_err("missing campaign");

    assert!(matches!(
        error,
        EvolutionInputParseError::Invalid { detail } if detail.contains("at least one campaign")
    ));
}

#[test]
fn evolution_language_model_mutation_model_reports_unknown_provider() {
    let root = temp_dir("unknown-provider-model");
    let packet = MutationPacket {
        job_id: EvolutionJobId::from_legacy_named("evojob_unknown_provider").expect("job"),
        objective: "test unknown provider".to_string(),
        target_ring: TrustRing::R1TrustedRuntime,
        edit_scope: EditScope {
            workspace_root: root.display().to_string(),
            intended_paths: vec!["README.md".to_string()],
            notes: "test".to_string(),
        },
        relevant_files: Vec::new(),
        verification: VerificationPlan {
            commands: Vec::new(),
            module_replay_reports: Vec::new(),
        },
        output_contract: MutationOutputContract::default(),
        recovery: RecoveryPlan::default(),
    };
    let model = EvolutionLanguageModelMutationModel {
        provider: "unknown-provider".to_string(),
        model: "test-model".to_string(),
        data_dir: SinomoDataDir::new(root.join("data")),
    };

    let error = model
        .mutate(&packet, &candidate_workspace(&root), None)
        .expect_err("unknown provider should fail");

    assert!(matches!(
        error,
        MutationModelError::LanguageModel(LanguageModelError::ProviderInvocationFailed {
            provider,
            model,
            ..
        }) if provider == "unknown-provider" && model == "test-model"
    ));
}

#[test]
fn background_tick_fallback_campaign_describes_scheduler_context() {
    let campaign = background_tick_fallback_campaign();

    assert_eq!(campaign.campaign_id, "background_tick");
    assert_eq!(campaign.status, EvolutionCampaignStatus::Scheduled);
    assert_eq!(campaign.objective_card.resource_policy, "background");
    assert_eq!(
        campaign.approval_checkpoint.status,
        ApprovalCheckpointStatus::Approved
    );
    assert_eq!(campaign.target_ring, TrustRing::R1TrustedRuntime);
    assert!(campaign.dry_run);
}

#[test]
fn resource_scheduler_dry_run_denies_leases_that_violate_reserve_floor() {
    let campaign = propose_evolution_campaign(CampaignProposalRequest {
        objective: "improve the self-evolution scheduler so it can pause and resume around resource budgets".to_string(),
        horizon: "30 days".to_string(),
        budget_policy: "free-and-prepaid".to_string(),
        reserve_floors: vec![ReserveFloor {
            resource: "coding-plan-weekly".to_string(),
            floor_percent: 30,
        }],
        approval_policy: "stage-rollout".to_string(),
        max_wall_time: Some("10m".to_string()),
        pause_after_proposal: false,
        dry_run: true,
        approved: true,
        target_ring: TrustRing::R1TrustedRuntime,
        source_repo: Some("/workspace/sinomo".to_string()),
    });
    let task_id = campaign.near_term_tasks[0].task_id.clone();

    let dry_run = dry_run_resource_scheduler(ResourceSchedulerDryRunRequest {
        campaign: campaign.clone(),
        preview_horizon: "24h".to_string(),
        resources: vec![ResourceClassDefinition {
            resource_id: "coding-plan-weekly".to_string(),
            unit: "percent".to_string(),
            total_units: 100,
            used_units: 65,
            reserve_floor_percent: 30,
            reset_window: "weekly reset window".to_string(),
            notes: "free coding plan budget".to_string(),
        }],
        task_estimates: vec![ResourceUsageEstimate {
            task_id: task_id.clone(),
            resource: "coding-plan-weekly".to_string(),
            estimated_units: 10,
            actual_units: Some(65),
        }],
    });

    assert_eq!(
        dry_run.aggregate_decision,
        CampaignScheduleDecision::PausedBudget
    );
    assert_eq!(dry_run.resource_ledger.resources[0].used_units, 65);
    assert_eq!(dry_run.resource_ledger.usage_records[0].estimated_units, 10);
    assert_eq!(
        dry_run.resource_ledger.usage_records[0].actual_units,
        Some(65)
    );
    assert_eq!(
        dry_run.lease_decisions[0].outcome,
        ResourceLeaseOutcome::Denied
    );
    assert!(dry_run.lease_decisions[0].reason.contains("reserve floor"));
    assert_eq!(dry_run.task_decisions[0].task_id, task_id);
    assert_eq!(
        dry_run.task_decisions[0].decision,
        TaskScheduleDecisionKind::PausedBudget
    );
}

#[test]
fn resource_scheduler_dry_run_grants_leases_when_reserve_floor_remains_safe() {
    let campaign = propose_evolution_campaign(CampaignProposalRequest {
        objective: "improve the self-evolution scheduler so it can pause and resume around resource budgets".to_string(),
        horizon: "30 days".to_string(),
        budget_policy: "free-and-prepaid".to_string(),
        reserve_floors: vec![ReserveFloor {
            resource: "coding-plan-weekly".to_string(),
            floor_percent: 30,
        }],
        approval_policy: "stage-rollout".to_string(),
        max_wall_time: Some("10m".to_string()),
        pause_after_proposal: false,
        dry_run: true,
        approved: true,
        target_ring: TrustRing::R1TrustedRuntime,
        source_repo: Some("/workspace/sinomo".to_string()),
    });
    let task_id = campaign.near_term_tasks[0].task_id.clone();

    let dry_run = dry_run_resource_scheduler(ResourceSchedulerDryRunRequest {
        campaign,
        preview_horizon: "24h".to_string(),
        resources: vec![ResourceClassDefinition {
            resource_id: "coding-plan-weekly".to_string(),
            unit: "percent".to_string(),
            total_units: 100,
            used_units: 40,
            reserve_floor_percent: 30,
            reset_window: "weekly reset window".to_string(),
            notes: "free coding plan budget".to_string(),
        }],
        task_estimates: vec![ResourceUsageEstimate {
            task_id: task_id.clone(),
            resource: "coding-plan-weekly".to_string(),
            estimated_units: 10,
            actual_units: Some(40),
        }],
    });

    assert_eq!(
        dry_run.aggregate_decision,
        CampaignScheduleDecision::Scheduled
    );
    assert_eq!(
        dry_run.lease_decisions[0].outcome,
        ResourceLeaseOutcome::Granted
    );
    assert_eq!(
        dry_run.task_decisions[0].decision,
        TaskScheduleDecisionKind::RunNow
    );
    assert!(dry_run.next_24h_preview.iter().any(|entry| {
        entry.task_id == task_id
            && entry
                .explanation
                .contains("reserve floor remains protected")
    }));
}

#[test]
fn candidate_resource_usage_maps_agent_trace_to_campaign_budget_estimate() {
    let campaign = propose_evolution_campaign(CampaignProposalRequest {
        objective: "account for real agent usage in the campaign scheduler".to_string(),
        horizon: "30 days".to_string(),
        budget_policy: "free-and-prepaid".to_string(),
        reserve_floors: vec![ReserveFloor {
            resource: "coding-plan-weekly".to_string(),
            floor_percent: 30,
        }],
        approval_policy: "stage-rollout".to_string(),
        max_wall_time: Some("10m".to_string()),
        pause_after_proposal: false,
        dry_run: true,
        approved: true,
        target_ring: TrustRing::R1TrustedRuntime,
        source_repo: Some("/workspace/sinomo".to_string()),
    });
    let task_id = campaign.near_term_tasks[0].task_id.clone();
    let summary = CandidateResourceUsageSummary {
        job_id: EvolutionJobId::generate(),
        generated_at: "2026-05-02T00:00:00Z".to_string(),
        records: vec![
            resource_usage_record(
                "agent-input-tokens",
                1_100,
                "tokens",
                ResourceUsageConfidence::ProviderReported,
                "test",
            ),
            resource_usage_record(
                "agent-output-tokens",
                400,
                "tokens",
                ResourceUsageConfidence::ProviderReported,
                "test",
            ),
            resource_usage_record(
                "agent-model-turns",
                3,
                "turns",
                ResourceUsageConfidence::Exact,
                "test",
            ),
            resource_usage_record(
                "local-compute-ms",
                61_000,
                "ms",
                ResourceUsageConfidence::Estimated,
                "test",
            ),
        ],
        scheduler_estimates: Vec::new(),
    };

    let usage = candidate_resource_usage_estimates(&campaign, &task_id, &summary);

    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].task_id, task_id);
    assert_eq!(usage[0].resource, "coding-plan-weekly");
    assert_eq!(usage[0].estimated_units, 10);
    assert_eq!(usage[0].actual_units, Some(7));
}

#[test]
fn scheduled_iteration_prepares_dry_run_agent_handoff_from_approved_campaign() {
    let mut campaign = propose_evolution_campaign(CampaignProposalRequest {
        objective:
            "improve the self-evolution scheduler so it can dispatch one approved task safely"
                .to_string(),
        horizon: "30 days".to_string(),
        budget_policy: "free-and-prepaid".to_string(),
        reserve_floors: vec![ReserveFloor {
            resource: "coding-plan-weekly".to_string(),
            floor_percent: 30,
        }],
        approval_policy: "stage-rollout".to_string(),
        max_wall_time: Some("10m".to_string()),
        pause_after_proposal: false,
        dry_run: true,
        approved: true,
        target_ring: TrustRing::R1TrustedRuntime,
        source_repo: Some("/workspace/sinomo".to_string()),
    });
    campaign.status = EvolutionCampaignStatus::Scheduled;
    let task_id = campaign.near_term_tasks[0].task_id.clone();

    let plan = prepare_scheduled_iteration(ScheduledIterationRequest {
        campaign: campaign.clone(),
        task_id: Some(task_id.clone()),
        intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
        extra_verification_commands: vec!["cargo test -p sinomo-evolution --lib".to_string()],
        dry_run: true,
    })
    .expect("approved campaign can prepare one iteration");

    assert_eq!(plan.campaign_id, campaign.campaign_id);
    assert_eq!(plan.task.task_id, task_id);
    assert!(plan.evolution_request.dry_run);
    assert!(plan.evolution_request.objective.contains("Campaign"));
    assert!(
        plan.evolution_request
            .objective
            .contains(&plan.task.task_id)
    );
    assert_eq!(
        plan.evolution_request.intended_paths,
        vec!["modules/evolution/sinomo-evolution/**".to_string()]
    );
    assert!(
        plan.evolution_request
            .verification_commands
            .iter()
            .any(
                |command| command.command == "cargo test -p sinomo-evolution --lib"
                    && command.gate == EvaluationGate::StaticChecks
            )
    );

    let unapproved = propose_evolution_campaign(CampaignProposalRequest {
        approved: false,
        ..CampaignProposalRequest {
            objective: "unapproved campaign must not dispatch".to_string(),
            horizon: "30 days".to_string(),
            budget_policy: "free-and-prepaid".to_string(),
            reserve_floors: Vec::new(),
            approval_policy: "stage-rollout".to_string(),
            max_wall_time: None,
            pause_after_proposal: true,
            dry_run: true,
            approved: false,
            target_ring: TrustRing::R1TrustedRuntime,
            source_repo: Some("/workspace/sinomo".to_string()),
        }
    });
    assert!(matches!(
        prepare_scheduled_iteration(ScheduledIterationRequest {
            campaign: unapproved,
            task_id: None,
            intended_paths: Vec::new(),
            extra_verification_commands: Vec::new(),
            dry_run: true,
        }),
        Err(CampaignIterationError::CampaignNotApproved { .. })
    ));
}

#[test]
fn scheduled_iteration_attaches_prior_memory_to_agent_handoff() {
    let mut campaign = sample_approved_campaign("reuse prior lesson memory in next attempt");
    campaign.status = EvolutionCampaignStatus::Scheduled;
    let task_id = campaign.near_term_tasks[0].task_id.clone();
    let stage_id = campaign.near_term_tasks[0].stage_id.clone();
    let plan = prepare_scheduled_iteration(ScheduledIterationRequest {
        campaign: campaign.clone(),
        task_id: Some(task_id.clone()),
        intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
        extra_verification_commands: Vec::new(),
        dry_run: false,
    })
    .expect("iteration plan");
    let lesson = EvolutionLessonMemoryRecord {
        memory_id: "lesson_memory_eval_timeout".to_string(),
        campaign_id: "camp_previous".to_string(),
        adaptation_id: "adapt_previous".to_string(),
        stage_id: stage_id.clone(),
        failed_iteration_id: "iter_previous".to_string(),
        failed_task_id: task_id.clone(),
        failure_class: EvolutionFailureClass::EvalRegression,
        root_cause_summary: "module replay timeout still reproduces".to_string(),
        what_was_learned: vec!["minimize the replay before retry".to_string()],
        next_task_simplification: "create narrower replay".to_string(),
        new_eval_or_diagnostic: "ReplayTests private gate".to_string(),
        foundation_issue_detected: false,
        budget_adjustment: "keep scheduler gate".to_string(),
        human_attention_needed: false,
        replacement_task_candidate: None,
        proposed_regression_gate_ids: vec!["gate_timeout_replay".to_string()],
        evidence_refs: vec!["evolution/memory/lessons/lesson.json".to_string()],
        campaign_bundle_path: "/tmp/evolution/campaigns/camp_previous".to_string(),
        recorded_at: "2026-05-02T03:20:00Z".to_string(),
    };
    let gate = EvolutionRegressionGateMemoryRecord {
        memory_id: "gate_memory_timeout_replay".to_string(),
        campaign_id: "camp_previous".to_string(),
        adaptation_id: "adapt_previous".to_string(),
        stage_id,
        failed_task_id: task_id,
        status: "proposed".to_string(),
        proposed_gate: CampaignProposedRegressionGate {
            gate_id: "gate_timeout_replay".to_string(),
            gate: EvaluationGate::ReplayTests,
            source: "module-replay-report".to_string(),
            evidence_ref: "replays/timeout-replay.json".to_string(),
            command: Some("cargo test -p sinomo-evolution timeout_replay_fixture".to_string()),
            rationale: "keep timeout replay as a future private gate".to_string(),
        },
        evidence_refs: vec!["evolution/memory/proposed-regression-gates/gate.json".to_string()],
        campaign_bundle_path: "/tmp/evolution/campaigns/camp_previous".to_string(),
        recorded_at: "2026-05-02T03:20:00Z".to_string(),
    };

    let plan = attach_evolution_memory_to_scheduled_iteration(plan, &[lesson], &[gate], 8);

    assert_eq!(plan.memory_context.len(), 2);
    assert!(
        plan.evolution_request
            .objective
            .contains("Prior self-evolution memory")
    );
    assert!(
        plan.evolution_request
            .objective
            .contains("module replay timeout still reproduces")
    );
    assert!(
        plan.evolution_request
            .objective
            .contains("timeout_replay_fixture")
    );
    assert!(
        plan.evolution_request
            .verification_commands
            .iter()
            .any(|command| {
                command.command == "cargo test -p sinomo-evolution timeout_replay_fixture"
                    && command.gate == EvaluationGate::ReplayTests
            })
    );
    assert!(
        plan.evolution_request
            .module_replay_reports
            .iter()
            .any(|report| report.report_path == "replays/timeout-replay.json"
                && report.name.contains("gate_memory_timeout_replay"))
    );
    assert!(
        plan.operator_notes.iter().any(|note| {
            note.contains("attached 2 prior self-evolution lesson/gate memory item")
        })
    );
    assert!(plan.operator_notes.iter().any(|note| {
        note.contains("materialized 1 memory command gate") && note.contains("1 replay fixture")
    }));
}

#[test]
fn scheduled_iteration_materializes_standalone_candidate_gate_with_selected_lesson() {
    let mut campaign =
        sample_approved_campaign("reuse failed standalone candidate gate in campaign");
    campaign.status = EvolutionCampaignStatus::Scheduled;
    let task_id = campaign.near_term_tasks[0].task_id.clone();
    let plan = prepare_scheduled_iteration(ScheduledIterationRequest {
        campaign,
        task_id: Some(task_id),
        intended_paths: vec!["modules/trigger/**".to_string()],
        extra_verification_commands: Vec::new(),
        dry_run: false,
    })
    .expect("iteration plan");
    let lesson = EvolutionLessonMemoryRecord {
        memory_id: "candidate_lesson_memory_job_timeout".to_string(),
        campaign_id: "standalone-candidate".to_string(),
        adaptation_id: "candidate_job_timeout".to_string(),
        stage_id: "standalone-candidate".to_string(),
        failed_iteration_id: "job_timeout".to_string(),
        failed_task_id: "job_timeout".to_string(),
        failure_class: EvolutionFailureClass::EvalRegression,
        root_cause_summary: "standalone timeout replay still fails".to_string(),
        what_was_learned: vec![
            "candidate follow-up: minimize timeout replay before retry".to_string(),
        ],
        next_task_simplification: "minimize timeout replay before retry".to_string(),
        new_eval_or_diagnostic: "candidate regression evidence includes reusable gate commands"
            .to_string(),
        foundation_issue_detected: false,
        budget_adjustment: "normal scheduler lease still required".to_string(),
        human_attention_needed: true,
        replacement_task_candidate: None,
        proposed_regression_gate_ids: vec!["gate_job_timeout_1".to_string()],
        evidence_refs: vec!["evolution-lesson:/tmp/bundle/evolution-lesson.json".to_string()],
        campaign_bundle_path: "/tmp/bundle".to_string(),
        recorded_at: "2026-05-02T04:00:00Z".to_string(),
    };
    let matching_gate = EvolutionRegressionGateMemoryRecord {
        memory_id: "candidate_gate_memory_job_timeout_gate_1".to_string(),
        campaign_id: "standalone-candidate".to_string(),
        adaptation_id: "candidate_job_timeout".to_string(),
        stage_id: "standalone-candidate".to_string(),
        failed_task_id: "job_timeout".to_string(),
        status: "proposed".to_string(),
        proposed_gate: CampaignProposedRegressionGate {
            gate_id: "gate_job_timeout_1".to_string(),
            gate: EvaluationGate::ReplayTests,
            source: "module-replay-report".to_string(),
            evidence_ref: "replays/standalone-timeout.json".to_string(),
            command: Some("cargo test -p sinomo-evolution standalone_timeout".to_string()),
            rationale: "reuse failed standalone replay gate".to_string(),
        },
        evidence_refs: vec!["regression-evidence:/tmp/bundle/regression-evidence.json".to_string()],
        campaign_bundle_path: "/tmp/bundle".to_string(),
        recorded_at: "2026-05-02T04:00:00Z".to_string(),
    };
    let unrelated_gate = EvolutionRegressionGateMemoryRecord {
        memory_id: "candidate_gate_memory_other_gate_1".to_string(),
        campaign_id: "standalone-candidate".to_string(),
        adaptation_id: "candidate_other".to_string(),
        stage_id: "standalone-candidate".to_string(),
        failed_task_id: "job_other".to_string(),
        status: "proposed".to_string(),
        proposed_gate: CampaignProposedRegressionGate {
            gate_id: "gate_other_1".to_string(),
            gate: EvaluationGate::ReplayTests,
            source: "module-replay-report".to_string(),
            evidence_ref: "replays/unrelated.json".to_string(),
            command: Some("cargo test -p sinomo-evolution unrelated".to_string()),
            rationale: "unrelated standalone gate should not attach without its lesson".to_string(),
        },
        evidence_refs: Vec::new(),
        campaign_bundle_path: "/tmp/other-bundle".to_string(),
        recorded_at: "2026-05-02T04:00:00Z".to_string(),
    };

    let plan = attach_evolution_memory_to_scheduled_iteration(
        plan,
        &[lesson],
        &[matching_gate, unrelated_gate],
        8,
    );

    assert_eq!(plan.memory_context.len(), 2);
    assert!(
        plan.evolution_request
            .objective
            .contains("standalone timeout replay still fails")
    );
    assert!(
        plan.evolution_request
            .verification_commands
            .iter()
            .any(|command| command.command == "cargo test -p sinomo-evolution standalone_timeout")
    );
    assert!(
        plan.evolution_request
            .module_replay_reports
            .iter()
            .any(|report| report.report_path == "replays/standalone-timeout.json")
    );
    assert!(
        !plan
            .evolution_request
            .verification_commands
            .iter()
            .any(|command| command.command.contains("unrelated"))
    );
}

#[test]
fn scheduled_iteration_attaches_rollout_telemetry_window_gate_memory_without_executing_it() {
    let mut campaign = sample_approved_campaign("reuse rollback telemetry gate memory");
    campaign.status = EvolutionCampaignStatus::Scheduled;
    let task_id = campaign.near_term_tasks[0].task_id.clone();
    let stage_id = campaign.near_term_tasks[0].stage_id.clone();
    let plan = prepare_scheduled_iteration(ScheduledIterationRequest {
        campaign,
        task_id: Some(task_id.clone()),
        intended_paths: vec!["crates/sinomo-rollout/**".to_string()],
        extra_verification_commands: Vec::new(),
        dry_run: false,
    })
    .expect("iteration plan");
    let telemetry_window = "rollout-telemetry://window?after=0&through=42&limit=1000";
    let lesson = EvolutionLessonMemoryRecord {
        memory_id: "lesson_memory_rollout_timeout".to_string(),
        campaign_id: "camp_previous".to_string(),
        adaptation_id: "adapt_rollout_timeout".to_string(),
        stage_id: stage_id.clone(),
        failed_iteration_id: "iter_previous".to_string(),
        failed_task_id: task_id.clone(),
        failure_class: EvolutionFailureClass::RolloutFailure,
        root_cause_summary: "canary rollback timeout still lacks a minimized replay".to_string(),
        what_was_learned: vec![
            "rollout telemetry window must become a private gate before retry".to_string(),
        ],
        next_task_simplification: "minimize rollout telemetry window".to_string(),
        new_eval_or_diagnostic: "ReplayTests telemetry-window gate".to_string(),
        foundation_issue_detected: false,
        budget_adjustment: "normal scheduler lease still required".to_string(),
        human_attention_needed: false,
        replacement_task_candidate: None,
        proposed_regression_gate_ids: vec!["gate_rollout_window".to_string()],
        evidence_refs: vec!["evolution/memory/lessons/rollout.json".to_string()],
        campaign_bundle_path: "/tmp/evolution/campaigns/camp_previous".to_string(),
        recorded_at: "2026-05-02T03:20:00Z".to_string(),
    };
    let gate = EvolutionRegressionGateMemoryRecord {
        memory_id: "gate_memory_rollout_window".to_string(),
        campaign_id: "camp_previous".to_string(),
        adaptation_id: "adapt_rollout_timeout".to_string(),
        stage_id,
        failed_task_id: task_id,
        status: "proposed".to_string(),
        proposed_gate: CampaignProposedRegressionGate {
            gate_id: "gate_rollout_window".to_string(),
            gate: EvaluationGate::ReplayTests,
            source: "rollout-telemetry-window".to_string(),
            evidence_ref: telemetry_window.to_string(),
            command: None,
            rationale: "convert timeout rollback telemetry into a minimized replay gate"
                .to_string(),
        },
        evidence_refs: vec!["regression-evidence:/tmp/bundle/regression-evidence.json".to_string()],
        campaign_bundle_path: "/tmp/evolution/campaigns/camp_previous".to_string(),
        recorded_at: "2026-05-02T03:20:00Z".to_string(),
    };

    let plan = attach_evolution_memory_to_scheduled_iteration(plan, &[lesson], &[gate], 8);

    assert_eq!(plan.memory_context.len(), 2);
    assert!(
        plan.evolution_request
            .objective
            .contains("Prior rollout telemetry regression windows")
    );
    assert!(plan.evolution_request.objective.contains(telemetry_window));
    assert!(
        plan.evolution_request
            .objective
            .contains("convert this telemetry window into a minimized replay fixture")
    );
    assert!(plan.evolution_request.module_replay_reports.is_empty());
    assert!(
        !plan
            .evolution_request
            .verification_commands
            .iter()
            .any(|command| command.command.contains("rollout-telemetry://window"))
    );
    assert!(plan.operator_notes.iter().any(|note| {
        note.contains("attached 1 rollout telemetry window gate")
            && note.contains("before treating them as executable gates")
    }));
}

#[test]
fn scheduled_iteration_attaches_module_telemetry_gate_memory_without_executing_it() {
    let mut campaign = sample_approved_campaign("reuse module telemetry gate memory");
    campaign.status = EvolutionCampaignStatus::Scheduled;
    let task_id = campaign.near_term_tasks[0].task_id.clone();
    let stage_id = campaign.near_term_tasks[0].stage_id.clone();
    let plan = prepare_scheduled_iteration(ScheduledIterationRequest {
        campaign,
        task_id: Some(task_id.clone()),
        intended_paths: vec!["crates/sinomo-module/**".to_string()],
        extra_verification_commands: Vec::new(),
        dry_run: false,
    })
    .expect("iteration plan");
    let telemetry_ref = "module-telemetry://timeout-window";
    let lesson = EvolutionLessonMemoryRecord {
        memory_id: "lesson_memory_module_timeout".to_string(),
        campaign_id: "camp_previous".to_string(),
        adaptation_id: "adapt_module_timeout".to_string(),
        stage_id: stage_id.clone(),
        failed_iteration_id: "iter_previous".to_string(),
        failed_task_id: task_id.clone(),
        failure_class: EvolutionFailureClass::EvalRegression,
        root_cause_summary: "module timeout telemetry still lacks a minimized replay".to_string(),
        what_was_learned: vec![
            "module telemetry must become a private replay gate before retry".to_string(),
        ],
        next_task_simplification: "minimize module timeout telemetry".to_string(),
        new_eval_or_diagnostic: "ReplayTests telemetry gate".to_string(),
        foundation_issue_detected: false,
        budget_adjustment: "normal scheduler lease still required".to_string(),
        human_attention_needed: false,
        replacement_task_candidate: None,
        proposed_regression_gate_ids: vec!["gate_module_timeout_window".to_string()],
        evidence_refs: vec!["evolution/memory/lessons/module-timeout.json".to_string()],
        campaign_bundle_path: "/tmp/evolution/campaigns/camp_previous".to_string(),
        recorded_at: "2026-05-02T03:20:00Z".to_string(),
    };
    let gate = EvolutionRegressionGateMemoryRecord {
        memory_id: "gate_memory_module_timeout_window".to_string(),
        campaign_id: "camp_previous".to_string(),
        adaptation_id: "adapt_module_timeout".to_string(),
        stage_id,
        failed_task_id: task_id,
        status: "proposed".to_string(),
        proposed_gate: CampaignProposedRegressionGate {
            gate_id: "gate_module_timeout_window".to_string(),
            gate: EvaluationGate::ReplayTests,
            source: "module-telemetry-window".to_string(),
            evidence_ref: telemetry_ref.to_string(),
            command: None,
            rationale: "convert module timeout telemetry into a minimized replay gate".to_string(),
        },
        evidence_refs: vec!["regression-evidence:/tmp/bundle/regression-evidence.json".to_string()],
        campaign_bundle_path: "/tmp/evolution/campaigns/camp_previous".to_string(),
        recorded_at: "2026-05-02T03:20:00Z".to_string(),
    };

    let plan = attach_evolution_memory_to_scheduled_iteration(plan, &[lesson], &[gate], 8);

    assert_eq!(plan.memory_context.len(), 2);
    assert!(
        plan.evolution_request
            .objective
            .contains("Prior telemetry regression evidence")
    );
    assert!(plan.evolution_request.objective.contains(telemetry_ref));
    assert!(
        plan.evolution_request
            .objective
            .contains("convert this telemetry/event evidence into a minimized replay fixture")
    );
    assert!(plan.evolution_request.module_replay_reports.is_empty());
    assert!(
        !plan
            .evolution_request
            .verification_commands
            .iter()
            .any(|command| command.command.contains("module-telemetry://"))
    );
    assert!(plan.operator_notes.iter().any(|note| {
        note.contains("attached 1 telemetry/event trace gate")
            && note.contains("before treating them as executable gates")
    }));
}

#[test]
fn scheduled_iteration_result_pauses_campaign_for_rollout_review() {
    let mut campaign = propose_evolution_campaign(CampaignProposalRequest {
        objective:
            "improve the self-evolution scheduler so it can evaluate one implementation candidate"
                .to_string(),
        horizon: "30 days".to_string(),
        budget_policy: "free-and-prepaid".to_string(),
        reserve_floors: vec![ReserveFloor {
            resource: "coding-plan-weekly".to_string(),
            floor_percent: 30,
        }],
        approval_policy: "stage-rollout".to_string(),
        max_wall_time: Some("10m".to_string()),
        pause_after_proposal: false,
        dry_run: true,
        approved: true,
        target_ring: TrustRing::R1TrustedRuntime,
        source_repo: Some("/workspace/sinomo".to_string()),
    });
    campaign.status = EvolutionCampaignStatus::Scheduled;
    let task_id = campaign.near_term_tasks[0].task_id.clone();
    let plan = prepare_scheduled_iteration(ScheduledIterationRequest {
        campaign: campaign.clone(),
        task_id: Some(task_id.clone()),
        intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
        extra_verification_commands: vec!["cargo test -p sinomo-evolution --lib".to_string()],
        dry_run: true,
    })
    .expect("plan");
    let job_id = EvolutionJobId::generate();
    let report = EvolutionReport {
        job: EvolutionJob {
            job_id: job_id.clone(),
            objective: plan.evolution_request.objective,
            target_ring: campaign.target_ring.clone(),
            target_module_id: None,
            edit_scope: EditScope {
                workspace_root: "/workspace/sinomo".to_string(),
                intended_paths: plan.evolution_request.intended_paths.clone(),
                notes: "campaign scheduled iteration".to_string(),
            },
            verification: VerificationPlan {
                commands: plan.evolution_request.verification_commands.clone(),
                module_replay_reports: plan.evolution_request.module_replay_reports.clone(),
            },
            recovery: RecoveryPlan::default(),
            scope: EvolutionScope {
                allowed_paths: plan.evolution_request.intended_paths.clone(),
                forbidden_paths: Vec::new(),
                workspace_id: Some(
                    WorkspaceId::from_legacy_named("workspace_campaign").expect("workspace"),
                ),
            },
            status: EvolutionJobStatus::AwaitingApproval,
            required_gates: vec![EvaluationGate::StaticChecks],
            rollback_policy: RollbackPolicy::default(),
        },
        candidate_workspace: CandidateWorkspace {
            job_id: job_id.clone(),
            path: PathBuf::from("/tmp/sinomo-candidate-worktree"),
            branch_name: "evolution/test-campaign-task".to_string(),
            source_revision: Some("abc123".to_string()),
        },
        bundle: CandidateBundleManifest {
            job_id,
            bundle_path: "/tmp/sinomo-candidate-bundle".to_string(),
            created_at: time::OffsetDateTime::now_utc(),
            mutation_packet_path: "/tmp/sinomo-candidate-bundle/mutation-packet.json".to_string(),
            job_path: Some("/tmp/sinomo-candidate-bundle/job.json".to_string()),
            model_output_path: Some("/tmp/sinomo-candidate-bundle/model-output.json".to_string()),
            agent_run_path: None,
            agent_summary_path: None,
            benchmark_results_path: None,
            resource_usage_path: None,
            content_manifest_path: None,
            attestation_path: None,
            registry_record_path: None,
            bundle_content_hash: None,
            changed_files_path: Some("/tmp/sinomo-candidate-bundle/changed-files.json".to_string()),
            diff_path: Some("/tmp/sinomo-candidate-bundle/diff.patch".to_string()),
            scorecard_path: Some("/tmp/sinomo-candidate-bundle/scorecard.json".to_string()),
            candidate_summary_path: None,
            decision_path: None,
            artifact_refs: Vec::new(),
            command_logs: Vec::new(),
        },
        scorecard: Some(EvaluationScorecard {
            scorecard_id: ScorecardId::generate(),
            baseline_artifact_id: None,
            candidate_artifact_id: None,
            outcome: EvaluationOutcome::Passed,
            gates: Vec::new(),
            metrics: Vec::new(),
        }),
        changed_files: vec!["modules/evolution/sinomo-evolution/src/lib.rs".to_string()],
    };

    let updated = record_scheduled_iteration_result(
        campaign,
        &task_id,
        &report,
        vec![ResourceUsageEstimate {
            task_id: task_id.clone(),
            resource: "coding-plan-weekly".to_string(),
            estimated_units: 10,
            actual_units: Some(8),
        }],
    );

    assert_eq!(updated.status, EvolutionCampaignStatus::PausedHuman);
    assert_eq!(updated.iteration_results.len(), 1);
    assert_eq!(updated.iteration_results[0].task_id, task_id);
    assert_eq!(
        updated.iteration_results[0].status,
        EvolutionIterationStatus::AwaitingRolloutApproval
    );
    assert_eq!(updated.iteration_results[0].resource_usage.len(), 1);
    assert_eq!(
        updated.iteration_results[0].resource_usage[0].actual_units,
        Some(8)
    );
    assert!(
        updated.iteration_results[0]
            .evidence
            .iter()
            .any(|evidence| evidence.contains("scorecard"))
    );
    assert!(updated.decisions.iter().any(|decision| {
        decision.kind == CampaignDecisionKind::TaskSelected
            && decision
                .rationale
                .contains("rollout still requires existing approval")
    }));
}

#[test]
fn stage_evaluation_promotes_only_when_declared_metrics_pass() {
    let mut campaign = sample_approved_campaign("improve staged evaluation for self evolution");
    campaign.status = EvolutionCampaignStatus::Scheduled;
    let task_id = campaign.near_term_tasks[0].task_id.clone();
    let stage_id = campaign.near_term_tasks[0].stage_id.clone();
    let report = sample_iteration_report(&campaign, &task_id, EvaluationOutcome::Passed);

    let update = evaluate_campaign_stage(StageEvaluationRequest {
        campaign,
        task_id: task_id.clone(),
        report,
        resource_usage: Vec::new(),
        failure_class: None,
        root_cause_summary: None,
        what_was_learned: Vec::new(),
    });

    assert_eq!(update.stage_scorecard.stage_id, stage_id);
    assert_eq!(update.stage_scorecard.outcome, EvaluationOutcome::Passed);
    assert_eq!(
        update.stage_scorecard.promotion,
        StagePromotionDecision::Promote
    );
    assert!(update.stage_scorecard.missing_metrics.is_empty());
    assert_eq!(
        update.campaign.status,
        EvolutionCampaignStatus::StageComplete
    );
    assert!(update.campaign.decisions.iter().any(|decision| {
        decision.kind == CampaignDecisionKind::StagePromoted
            && decision.summary.contains("Stage metrics met")
    }));
}

#[test]
fn stage_evaluation_records_adaptation_and_proposes_foundation_task_after_repeated_failure() {
    let mut campaign = sample_approved_campaign("adapt failed staged evaluation attempts");
    campaign.status = EvolutionCampaignStatus::Scheduled;
    let task_id = campaign.near_term_tasks[0].task_id.clone();
    let stage_id = campaign.near_term_tasks[0].stage_id.clone();
    campaign.iteration_results.push(EvolutionIterationResult {
        iteration_id: "iter_previous_failure".to_string(),
        task_id: task_id.clone(),
        result_kind: EvolutionResultKind::Patch,
        status: EvolutionIterationStatus::Adapted,
        resource_usage: Vec::new(),
        evidence: vec!["scorecard:/tmp/previous-scorecard.json".to_string()],
        summary: Some("previous attempt failed".to_string()),
    });
    let report = sample_iteration_report(&campaign, &task_id, EvaluationOutcome::Failed);

    let update = evaluate_campaign_stage(StageEvaluationRequest {
        campaign,
        task_id: task_id.clone(),
        report,
        resource_usage: Vec::new(),
        failure_class: Some(EvolutionFailureClass::FoundationBlocker),
        root_cause_summary: Some("the campaign packet does not expose stage evidence".to_string()),
        what_was_learned: vec!["stage evidence must be visible before retrying".to_string()],
    });

    let adaptation = update.adaptation.expect("failed repeated task adapts");
    assert_eq!(adaptation.failed_task_id, task_id);
    assert_eq!(adaptation.stage_id, stage_id);
    assert_eq!(
        adaptation.failure_class,
        EvolutionFailureClass::FoundationBlocker
    );
    assert!(adaptation.foundation_issue_detected);
    assert!(adaptation.human_attention_needed);
    assert!(adaptation.next_task_simplification.contains("foundation"));
    let replacement = adaptation
        .replacement_task_candidate
        .expect("foundation blocker creates explicit task");
    assert_eq!(
        replacement.expected_result_kind,
        EvolutionResultKind::FoundationRepair
    );
    assert_eq!(
        update.campaign.near_term_tasks[0].task_id,
        replacement.task_id
    );
    assert_eq!(update.campaign.status, EvolutionCampaignStatus::PausedHuman);
    assert_eq!(
        update.stage_scorecard.promotion,
        StagePromotionDecision::HoldFailed
    );
}

#[test]
fn stage_evaluation_uses_failed_candidate_lesson_for_adaptation() {
    let root = temp_dir("campaign-lesson-adaptation");
    let bundle_dir = root.join("bundle");
    fs::create_dir_all(&bundle_dir).expect("bundle dir");
    let mut campaign = sample_approved_campaign("adapt from candidate lesson evidence");
    campaign.status = EvolutionCampaignStatus::Scheduled;
    let task_id = campaign.near_term_tasks[0].task_id.clone();
    let mut report = sample_iteration_report(&campaign, &task_id, EvaluationOutcome::Failed);
    report.bundle.bundle_path = bundle_dir.display().to_string();
    report.bundle.scorecard_path = Some(bundle_dir.join("scorecard.json").display().to_string());
    report.bundle.changed_files_path =
        Some(bundle_dir.join("changed-files.json").display().to_string());
    let lesson = CandidateEvolutionLessonRecord {
        lesson_id: "lesson_failed_eval".to_string(),
        job_id: report.job.job_id.clone(),
        objective: report.job.objective.clone(),
        outcome: EvaluationOutcome::Failed,
        failure_class: EvolutionFailureClass::EvalRegression,
        root_cause_summary: "module replay timeout still reproduces".to_string(),
        failed_assumptions: vec![
            "the replay gate would pass after the candidate patch".to_string(),
        ],
        rollback_evidence: vec!["no rollout attempted".to_string()],
        suggested_follow_up_tasks: vec![
            "minimize the replay into a private regression gate".to_string(),
        ],
        reusable_evidence_refs: vec!["replays/trigger-timeout.json".to_string()],
        recorded_at: "2026-05-02T01:20:00Z".to_string(),
    };
    fs::write(
        bundle_dir.join(CANDIDATE_EVOLUTION_LESSON_RELATIVE_PATH),
        serde_json::to_string_pretty(&lesson).expect("lesson json"),
    )
    .expect("write lesson");

    let update = evaluate_campaign_stage(StageEvaluationRequest {
        campaign,
        task_id: task_id.clone(),
        report,
        resource_usage: Vec::new(),
        failure_class: None,
        root_cause_summary: None,
        what_was_learned: Vec::new(),
    });

    let adaptation = update.adaptation.expect("lesson drives adaptation");
    assert_eq!(adaptation.failed_task_id, task_id);
    assert_eq!(
        adaptation.failure_class,
        EvolutionFailureClass::EvalRegression
    );
    assert_eq!(
        adaptation.root_cause_summary,
        "module replay timeout still reproduces"
    );
    assert!(
        adaptation
            .what_was_learned
            .iter()
            .any(|learned| learned.contains("failed assumption"))
    );
    assert!(
        adaptation
            .what_was_learned
            .iter()
            .any(|learned| learned.contains("private regression gate"))
    );
    assert!(adaptation.evidence_refs.iter().any(|evidence| {
        evidence.contains("evolution-lesson:") && evidence.contains("evolution-lesson.json")
    }));
    assert!(
        update.campaign.iteration_results[0]
            .evidence
            .iter()
            .any(|evidence| {
                evidence.contains("evolution-lesson:") && evidence.contains("evolution-lesson.json")
            })
    );
}

#[test]
fn repeated_failed_campaign_iteration_proposes_reusable_regression_gates() {
    let root = temp_dir("campaign-regression-gate-adaptation");
    let bundle_dir = root.join("bundle");
    fs::create_dir_all(&bundle_dir).expect("bundle dir");
    let mut campaign = sample_approved_campaign("adapt repeated failure into private gates");
    campaign.status = EvolutionCampaignStatus::Scheduled;
    let task_id = campaign.near_term_tasks[0].task_id.clone();
    campaign.iteration_results.push(EvolutionIterationResult {
        iteration_id: "iter_previous_eval_failure".to_string(),
        task_id: task_id.clone(),
        result_kind: EvolutionResultKind::Patch,
        status: EvolutionIterationStatus::Adapted,
        resource_usage: Vec::new(),
        evidence: vec!["scorecard:/tmp/previous-scorecard.json".to_string()],
        summary: Some("previous eval attempt failed".to_string()),
    });
    let mut report = sample_iteration_report(&campaign, &task_id, EvaluationOutcome::Failed);
    report.bundle.bundle_path = bundle_dir.display().to_string();
    report.bundle.scorecard_path = Some(bundle_dir.join("scorecard.json").display().to_string());
    report.bundle.changed_files_path =
        Some(bundle_dir.join("changed-files.json").display().to_string());
    let evidence = CandidateRegressionEvidenceSummary {
        job_id: report.job.job_id.clone(),
        objective: report.job.objective.clone(),
        target_ring: report.job.target_ring.clone(),
        outcome: EvaluationOutcome::Failed,
        bundle_path: report.bundle.bundle_path.clone(),
        scorecard_path: report.bundle.scorecard_path.clone(),
        diff_path: report.bundle.diff_path.clone(),
        changed_files: report.changed_files.clone(),
        failed_gates: vec![RegressionGateEvidence {
            gate: EvaluationGate::ReplayTests,
            outcome: EvaluationOutcome::Failed,
            summary: "timeout replay still fails".to_string(),
        }],
        failed_commands: vec![RegressionCommandFailure {
            name: "focused-replay".to_string(),
            command: "cargo test -p sinomo-evolution timeout_replay_fixture".to_string(),
            expected_exit_code: 0,
            exit_code: 101,
            duration_ms: 1250,
            command_log_path: Some(
                bundle_dir
                    .join("command-logs/focused-replay.log")
                    .display()
                    .to_string(),
            ),
            stdout_excerpt: String::new(),
            stderr_excerpt: "timeout replay still fails".to_string(),
            rerun_command: "cargo test -p sinomo-evolution timeout_replay_fixture"
                .to_string(),
        }],
        failed_benchmarks: Vec::new(),
        trace_fixtures: vec![TraceDerivedRegressionFixture {
            name: "timeout-replay".to_string(),
            source: "module-replay-report".to_string(),
            report_path: "replays/timeout-replay.json".to_string(),
            baseline_artifact_id: None,
            candidate_artifact_id: None,
            minimization_hint:
                "reduce timeout replay to the shortest trace that still times out".to_string(),
            suggested_gate: EvaluationGate::ReplayTests,
        }],
        replay_minimization: ReplayMinimizationPlan {
            status: "ready-for-minimization".to_string(),
            retained_evidence_refs: vec!["replays/timeout-replay.json".to_string()],
            minimization_steps: vec![
                "reduce `replays/timeout-replay.json` to the shortest replay trace that still fails"
                    .to_string(),
            ],
            candidate_gate_commands: vec![
                "cargo test -p sinomo-evolution timeout_replay_fixture".to_string(),
            ],
        },
        reusable_gate_suggestions: vec![
            "keep replay fixture `timeout-replay` at `replays/timeout-replay.json` as a ReplayTests gate"
                .to_string(),
        ],
        generated_at: "2026-05-02T02:00:00Z".to_string(),
    };
    fs::write(
        bundle_dir.join(CANDIDATE_REGRESSION_EVIDENCE_RELATIVE_PATH),
        serde_json::to_string_pretty(&evidence).expect("evidence json"),
    )
    .expect("write regression evidence");

    let update = evaluate_campaign_stage(StageEvaluationRequest {
        campaign,
        task_id: task_id.clone(),
        report,
        resource_usage: Vec::new(),
        failure_class: Some(EvolutionFailureClass::EvalRegression),
        root_cause_summary: None,
        what_was_learned: Vec::new(),
    });

    let adaptation = update
        .adaptation
        .expect("repeated failed eval creates adaptation");
    assert!(
        adaptation
            .proposed_regression_gates
            .iter()
            .any(|gate| gate.source == "failed-command"
                && gate.command.as_deref()
                    == Some("cargo test -p sinomo-evolution timeout_replay_fixture"))
    );
    assert!(
        adaptation
            .proposed_regression_gates
            .iter()
            .any(|gate| gate.gate == EvaluationGate::ReplayTests
                && gate.evidence_ref == "replays/timeout-replay.json")
    );
    assert!(
        adaptation
            .new_eval_or_diagnostic
            .contains("proposed reusable gates")
    );
    assert!(
        adaptation
            .what_was_learned
            .iter()
            .any(|learned| learned.contains("candidate gate suggestion"))
    );
    let replacement = adaptation
        .replacement_task_candidate
        .expect("repeated failure proposes diagnostic task");
    assert!(
        replacement
            .required_evidence
            .iter()
            .any(|evidence| evidence.contains("ReplayTests"))
    );
    assert!(
        replacement
            .verification_commands
            .iter()
            .any(|command| { command == "cargo test -p sinomo-evolution timeout_replay_fixture" })
    );
    assert_eq!(
        update.campaign.near_term_tasks[0].task_id,
        replacement.task_id
    );
    assert!(
        update
            .campaign
            .stages
            .iter()
            .flat_map(|stage| stage.regression_gates.iter())
            .any(|gate| gate.contains("replays/timeout-replay.json"))
    );
}

#[test]
fn background_scheduler_resumes_budget_paused_campaign_after_reset() {
    let mut campaign = sample_approved_campaign("make campaign scheduling durable");
    campaign.status = EvolutionCampaignStatus::PausedBudget;
    let task_id = campaign.near_term_tasks[0].task_id.clone();

    let plan = plan_background_scheduling_tick(BackgroundSchedulingRequest {
        scheduler_service_id: "sinomo-cluster-scheduler".to_string(),
        campaigns: vec![campaign.clone()],
        opportunity_events: Vec::new(),
        previously_intaken_opportunity_event_ids: Vec::new(),
        opportunity_detection_policy: None,
        opportunity_planner_node_id: None,
        external_workloads: Vec::new(),
        resources: vec![ResourceClassDefinition {
            resource_id: "coding-plan-weekly".to_string(),
            unit: "percent".to_string(),
            total_units: 100,
            used_units: 95,
            reserve_floor_percent: 30,
            reset_window: "weekly".to_string(),
            notes: "before reset this campaign would be paused".to_string(),
        }],
        task_estimates: vec![ResourceUsageEstimate {
            task_id: task_id.clone(),
            resource: "coding-plan-weekly".to_string(),
            estimated_units: 10,
            actual_units: None,
        }],
        max_concurrent_campaigns: 1,
        max_concurrent_workloads: 1,
        idle_window_open: false,
        reset_window_open: true,
        dry_run: true,
        now: "2026-05-02T00:00:00Z".to_string(),
    });

    assert_eq!(plan.concurrency_cap, 1);
    assert_eq!(plan.active_campaigns, vec![campaign.campaign_id.clone()]);
    assert_eq!(
        plan.campaigns[0].previous_status,
        EvolutionCampaignStatus::PausedBudget
    );
    assert_eq!(
        plan.campaigns[0].action,
        BackgroundSchedulingAction::DispatchIterationDryRun
    );
    assert_eq!(
        plan.campaigns[0].campaign.status,
        EvolutionCampaignStatus::Scheduled
    );
    assert!(plan.campaigns[0].scheduled_iteration.is_some());
    assert!(plan.timer_events.iter().any(|event| {
        event.kind == BackgroundTimerEventKind::ResetWindowOpened
            && event.summary.contains("reset window opened")
    }));
}

#[test]
fn campaign_checkpoint_approval_enables_scheduled_background_dispatch() {
    let campaign = propose_evolution_campaign(CampaignProposalRequest {
        objective: "review telemetry-created opportunity before dispatch".to_string(),
        horizon: "14 days".to_string(),
        budget_policy: "background-opportunity".to_string(),
        reserve_floors: vec![ReserveFloor {
            resource: "coding-plan-weekly".to_string(),
            floor_percent: 30,
        }],
        approval_policy: "human-gated-background-opportunity".to_string(),
        max_wall_time: Some("30m".to_string()),
        pause_after_proposal: true,
        dry_run: true,
        approved: false,
        target_ring: TrustRing::R2TrustedService,
        source_repo: Some("/workspace/sinomo".to_string()),
    });
    assert_eq!(campaign.status, EvolutionCampaignStatus::AwaitingApproval);

    let campaign = decide_campaign_checkpoint(
        campaign,
        CampaignCheckpointDecision::Approve,
        Some("operator reviewed scoped telemetry evidence".to_string()),
        "test-operator",
    );
    assert_eq!(campaign.status, EvolutionCampaignStatus::Scheduled);
    assert_eq!(
        campaign.approval_checkpoint.status,
        ApprovalCheckpointStatus::Approved
    );
    assert!(campaign.decisions.iter().any(|decision| {
        decision.kind == CampaignDecisionKind::Approved
            && decision.rationale.contains("operator reviewed")
    }));

    let task_id = campaign.near_term_tasks[0].task_id.clone();
    let plan = plan_background_scheduling_tick(BackgroundSchedulingRequest {
        scheduler_service_id: "sinomo-cluster-scheduler".to_string(),
        campaigns: vec![campaign.clone()],
        opportunity_events: Vec::new(),
        previously_intaken_opportunity_event_ids: Vec::new(),
        opportunity_detection_policy: None,
        opportunity_planner_node_id: None,
        external_workloads: Vec::new(),
        resources: vec![ResourceClassDefinition {
            resource_id: "coding-plan-weekly".to_string(),
            unit: "percent".to_string(),
            total_units: 100,
            used_units: 0,
            reserve_floor_percent: 30,
            reset_window: "weekly".to_string(),
            notes: "test dispatch budget".to_string(),
        }],
        task_estimates: vec![ResourceUsageEstimate {
            task_id,
            resource: "coding-plan-weekly".to_string(),
            estimated_units: 10,
            actual_units: None,
        }],
        max_concurrent_campaigns: 1,
        max_concurrent_workloads: 1,
        idle_window_open: true,
        reset_window_open: false,
        dry_run: false,
        now: "2026-05-02T00:05:00Z".to_string(),
    });

    assert!(!plan.dry_run);
    assert_eq!(
        plan.campaigns[0].action,
        BackgroundSchedulingAction::DispatchIteration
    );
    let scheduled = plan.campaigns[0]
        .scheduled_iteration
        .as_ref()
        .expect("approved campaign receives dispatch plan");
    assert!(!scheduled.evolution_request.dry_run);
    assert!(
        plan.campaigns[0]
            .reason
            .contains("rollout still requires explicit approval")
    );
}

#[test]
fn background_scheduler_budgets_external_workloads_and_evolution_campaigns_together() {
    let mut campaign = sample_approved_campaign("share scheduler budget with other systems");
    campaign.status = EvolutionCampaignStatus::Scheduled;
    let task_id = campaign.near_term_tasks[0].task_id.clone();

    let plan = plan_background_scheduling_tick(BackgroundSchedulingRequest {
        scheduler_service_id: "sinomo-cluster-scheduler".to_string(),
        campaigns: vec![campaign.clone()],
        opportunity_events: Vec::new(),
        previously_intaken_opportunity_event_ids: Vec::new(),
        opportunity_detection_policy: None,
        opportunity_planner_node_id: None,
        external_workloads: vec![ClusterLongHorizonWorkload {
            workload_id: "backup-index-01".to_string(),
            system: "backup".to_string(),
            title: "index archive snapshots".to_string(),
            status: ClusterLongHorizonWorkloadStatus::Ready,
            resource_estimates: vec![ResourceUsageEstimate {
                task_id: "backup-index-01".to_string(),
                resource: "cluster-background-hours".to_string(),
                estimated_units: 50,
                actual_units: None,
            }],
            human_checkpoint_required: false,
            terminal: false,
            evidence_refs: vec!["backup/schedule.json".to_string()],
        }],
        resources: vec![ResourceClassDefinition {
            resource_id: "cluster-background-hours".to_string(),
            unit: "hours".to_string(),
            total_units: 100,
            used_units: 0,
            reserve_floor_percent: 30,
            reset_window: "daily".to_string(),
            notes: "cluster-level budget shared across long-horizon systems".to_string(),
        }],
        task_estimates: vec![ResourceUsageEstimate {
            task_id: task_id.clone(),
            resource: "cluster-background-hours".to_string(),
            estimated_units: 30,
            actual_units: None,
        }],
        max_concurrent_campaigns: 4,
        max_concurrent_workloads: 4,
        idle_window_open: true,
        reset_window_open: false,
        dry_run: true,
        now: "2026-05-02T00:20:00Z".to_string(),
    });

    assert_eq!(plan.scheduler_service_id, "sinomo-cluster-scheduler");
    assert_eq!(
        plan.external_workloads[0].action,
        BackgroundSchedulingAction::DispatchExternalWorkloadDryRun
    );
    assert_eq!(
        plan.campaigns[0].action,
        BackgroundSchedulingAction::PauseForBudget
    );
    assert_eq!(
        plan.campaigns[0].campaign.status,
        EvolutionCampaignStatus::PausedBudget
    );
    let cluster_resource = plan
        .cluster_resources_after_tick
        .iter()
        .find(|resource| resource.resource_id == "cluster-background-hours")
        .expect("cluster resource");
    assert_eq!(cluster_resource.used_units, 50);
    assert!(
        plan.active_workloads
            .contains(&"backup:backup-index-01".to_string())
    );
    assert!(plan.held_campaigns.contains(&campaign.campaign_id));
}

#[test]
fn background_scheduler_accounts_previous_iteration_usage_before_granting_new_lease() {
    let mut campaign = sample_approved_campaign("budget repeated campaign iterations");
    campaign.status = EvolutionCampaignStatus::Scheduled;
    let task_id = campaign.near_term_tasks[0].task_id.clone();
    campaign.iteration_results.push(EvolutionIterationResult {
        iteration_id: "iter_previous".to_string(),
        task_id: task_id.clone(),
        result_kind: EvolutionResultKind::Patch,
        status: EvolutionIterationStatus::AwaitingRolloutApproval,
        resource_usage: vec![ResourceUsageEstimate {
            task_id: task_id.clone(),
            resource: "coding-plan-weekly".to_string(),
            estimated_units: 10,
            actual_units: Some(65),
        }],
        evidence: vec!["resource-usage:/tmp/resource-usage.json".to_string()],
        summary: Some("previous iteration consumed budget".to_string()),
    });

    let plan = plan_background_scheduling_tick(BackgroundSchedulingRequest {
        scheduler_service_id: "sinomo-cluster-scheduler".to_string(),
        campaigns: vec![campaign.clone()],
        opportunity_events: Vec::new(),
        previously_intaken_opportunity_event_ids: Vec::new(),
        opportunity_detection_policy: None,
        opportunity_planner_node_id: None,
        external_workloads: Vec::new(),
        resources: background_resource_classes_from_campaign_reserve_floors(
            &[campaign.clone()],
            "test resources",
        ),
        task_estimates: vec![ResourceUsageEstimate {
            task_id: task_id.clone(),
            resource: "coding-plan-weekly".to_string(),
            estimated_units: 10,
            actual_units: None,
        }],
        max_concurrent_campaigns: 1,
        max_concurrent_workloads: 1,
        idle_window_open: true,
        reset_window_open: false,
        dry_run: true,
        now: "2026-05-02T00:20:00Z".to_string(),
    });

    assert_eq!(
        plan.campaigns[0].action,
        BackgroundSchedulingAction::PauseForBudget
    );
    assert_eq!(
        plan.campaigns[0].campaign.status,
        EvolutionCampaignStatus::PausedBudget
    );
    assert!(
        plan.campaigns[0]
            .evidence_refs
            .iter()
            .any(|evidence| evidence.contains("reserve floor"))
    );
    let resource = plan
        .cluster_resources_after_tick
        .iter()
        .find(|resource| resource.resource_id == "coding-plan-weekly")
        .expect("resource");
    assert_eq!(resource.used_units, 65);
}

#[test]
fn background_scheduler_holds_external_workloads_at_human_checkpoints() {
    let plan = plan_background_scheduling_tick(BackgroundSchedulingRequest {
        scheduler_service_id: "sinomo-cluster-scheduler".to_string(),
        campaigns: Vec::new(),
        opportunity_events: Vec::new(),
        previously_intaken_opportunity_event_ids: Vec::new(),
        opportunity_detection_policy: None,
        opportunity_planner_node_id: None,
        external_workloads: vec![ClusterLongHorizonWorkload {
            workload_id: "dataset-curation-42".to_string(),
            system: "memory".to_string(),
            title: "curate high-salience session memory".to_string(),
            status: ClusterLongHorizonWorkloadStatus::PausedHuman,
            resource_estimates: vec![ResourceUsageEstimate {
                task_id: "dataset-curation-42".to_string(),
                resource: "cluster-background-hours".to_string(),
                estimated_units: 5,
                actual_units: None,
            }],
            human_checkpoint_required: true,
            terminal: false,
            evidence_refs: Vec::new(),
        }],
        resources: vec![ResourceClassDefinition {
            resource_id: "cluster-background-hours".to_string(),
            unit: "hours".to_string(),
            total_units: 100,
            used_units: 0,
            reserve_floor_percent: 30,
            reset_window: "daily".to_string(),
            notes: "cluster-level budget".to_string(),
        }],
        task_estimates: Vec::new(),
        max_concurrent_campaigns: 4,
        max_concurrent_workloads: 4,
        idle_window_open: true,
        reset_window_open: false,
        dry_run: true,
        now: "2026-05-02T00:25:00Z".to_string(),
    });

    assert_eq!(
        plan.external_workloads[0].action,
        BackgroundSchedulingAction::HoldHumanCheckpoint
    );
    assert!(
        plan.held_workloads
            .contains(&"memory:dataset-curation-42".to_string())
    );
    assert!(plan.human_checkpoints.iter().any(|checkpoint| {
        checkpoint.campaign_id == "memory:dataset-curation-42"
            && checkpoint.reason.contains("external workload")
    }));
}

#[test]
fn background_scheduler_honors_concurrency_cap_and_human_checkpoints() {
    let mut first = sample_approved_campaign("run first background campaign");
    first.status = EvolutionCampaignStatus::Scheduled;
    let mut second = sample_approved_campaign("hold second background campaign");
    second.status = EvolutionCampaignStatus::Scheduled;
    let mut checkpoint = sample_approved_campaign("wait for human checkpoint");
    checkpoint.status = EvolutionCampaignStatus::StageComplete;
    let first_id = first.campaign_id.clone();
    let second_id = second.campaign_id.clone();
    let checkpoint_id = checkpoint.campaign_id.clone();

    let plan = plan_background_scheduling_tick(BackgroundSchedulingRequest {
        scheduler_service_id: "sinomo-cluster-scheduler".to_string(),
        campaigns: vec![first, second, checkpoint],
        opportunity_events: Vec::new(),
        previously_intaken_opportunity_event_ids: Vec::new(),
        opportunity_detection_policy: None,
        opportunity_planner_node_id: None,
        external_workloads: Vec::new(),
        resources: vec![ResourceClassDefinition {
            resource_id: "coding-plan-weekly".to_string(),
            unit: "percent".to_string(),
            total_units: 100,
            used_units: 0,
            reserve_floor_percent: 30,
            reset_window: "weekly".to_string(),
            notes: "test budget".to_string(),
        }],
        task_estimates: Vec::new(),
        max_concurrent_campaigns: 1,
        max_concurrent_workloads: 1,
        idle_window_open: true,
        reset_window_open: false,
        dry_run: true,
        now: "2026-05-02T00:10:00Z".to_string(),
    });

    assert_eq!(plan.active_campaigns, vec![first_id]);
    assert!(plan.held_campaigns.contains(&second_id));
    assert!(plan.human_checkpoints.iter().any(|checkpoint| {
        checkpoint.campaign_id == checkpoint_id && checkpoint.reason.contains("human checkpoint")
    }));
    let held = plan
        .campaigns
        .iter()
        .find(|update| update.campaign.campaign_id == second_id)
        .expect("held campaign update");
    assert_eq!(held.action, BackgroundSchedulingAction::HoldConcurrencyCap);
    assert!(held.scheduled_iteration.is_none());
}

fn sample_approved_campaign(objective: &str) -> EvolutionCampaign {
    propose_evolution_campaign(CampaignProposalRequest {
        objective: objective.to_string(),
        horizon: "30 days".to_string(),
        budget_policy: "free-and-prepaid".to_string(),
        reserve_floors: vec![ReserveFloor {
            resource: "coding-plan-weekly".to_string(),
            floor_percent: 30,
        }],
        approval_policy: "stage-rollout".to_string(),
        max_wall_time: Some("10m".to_string()),
        pause_after_proposal: false,
        dry_run: true,
        approved: true,
        target_ring: TrustRing::R1TrustedRuntime,
        source_repo: Some("/workspace/sinomo".to_string()),
    })
}

fn sample_iteration_report(
    campaign: &EvolutionCampaign,
    task_id: &str,
    outcome: EvaluationOutcome,
) -> EvolutionReport {
    let plan = prepare_scheduled_iteration(ScheduledIterationRequest {
        campaign: campaign.clone(),
        task_id: Some(task_id.to_string()),
        intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
        extra_verification_commands: vec!["cargo test -p sinomo-evolution --lib".to_string()],
        dry_run: true,
    })
    .expect("plan");
    let job_id = EvolutionJobId::generate();
    EvolutionReport {
        job: EvolutionJob {
            job_id: job_id.clone(),
            objective: plan.evolution_request.objective,
            target_ring: campaign.target_ring.clone(),
            target_module_id: None,
            edit_scope: EditScope {
                workspace_root: "/workspace/sinomo".to_string(),
                intended_paths: plan.evolution_request.intended_paths.clone(),
                notes: "campaign scheduled iteration".to_string(),
            },
            verification: VerificationPlan {
                commands: plan.evolution_request.verification_commands.clone(),
                module_replay_reports: plan.evolution_request.module_replay_reports.clone(),
            },
            recovery: RecoveryPlan::default(),
            scope: EvolutionScope {
                allowed_paths: plan.evolution_request.intended_paths.clone(),
                forbidden_paths: Vec::new(),
                workspace_id: Some(
                    WorkspaceId::from_legacy_named("workspace_campaign").expect("workspace"),
                ),
            },
            status: EvolutionJobStatus::AwaitingApproval,
            required_gates: vec![EvaluationGate::StaticChecks],
            rollback_policy: RollbackPolicy::default(),
        },
        candidate_workspace: CandidateWorkspace {
            job_id: job_id.clone(),
            path: PathBuf::from("/tmp/sinomo-candidate-worktree"),
            branch_name: "evolution/test-campaign-task".to_string(),
            source_revision: Some("abc123".to_string()),
        },
        bundle: CandidateBundleManifest {
            job_id,
            bundle_path: "/tmp/sinomo-candidate-bundle".to_string(),
            created_at: time::OffsetDateTime::now_utc(),
            mutation_packet_path: "/tmp/sinomo-candidate-bundle/mutation-packet.json".to_string(),
            job_path: Some("/tmp/sinomo-candidate-bundle/job.json".to_string()),
            model_output_path: Some("/tmp/sinomo-candidate-bundle/model-output.json".to_string()),
            agent_run_path: None,
            agent_summary_path: None,
            benchmark_results_path: None,
            resource_usage_path: None,
            content_manifest_path: None,
            attestation_path: None,
            registry_record_path: None,
            bundle_content_hash: None,
            changed_files_path: Some("/tmp/sinomo-candidate-bundle/changed-files.json".to_string()),
            diff_path: Some("/tmp/sinomo-candidate-bundle/diff.patch".to_string()),
            scorecard_path: Some("/tmp/sinomo-candidate-bundle/scorecard.json".to_string()),
            candidate_summary_path: None,
            decision_path: None,
            artifact_refs: Vec::new(),
            command_logs: Vec::new(),
        },
        scorecard: Some(EvaluationScorecard {
            scorecard_id: ScorecardId::generate(),
            baseline_artifact_id: None,
            candidate_artifact_id: None,
            outcome,
            gates: Vec::new(),
            metrics: Vec::new(),
        }),
        changed_files: vec!["modules/evolution/sinomo-evolution/src/lib.rs".to_string()],
    }
}

#[test]
fn r1_job_receives_continuity_gates_and_workspace_scope() {
    let planner = EvolutionPlanner::new(NodeId::from_legacy_named("node_desktop").expect("node"));
    let (job, workspace) = planner.scope_job(EvolutionOpportunity {
        observed_event_id: None,
        target_ring: TrustRing::R1TrustedRuntime,
        target_module_id: None,
        objective: "harden event store".to_string(),
        workspace_id: WorkspaceId::from_legacy_named("workspace_runtime").expect("workspace"),
        allowed_paths: vec!["crates/sinomo-events/**".to_string()],
        forbidden_paths: vec!["crates/sinomo-policy/**".to_string()],
        priority: 0.9,
    });

    assert!(
        job.required_gates
            .contains(&EvaluationGate::ContinuityChecks)
    );
    assert_eq!(job.scope.workspace_id, Some(workspace.workspace_id));
    assert_eq!(
        workspace.branch_name,
        format!("evolution/{}", job.job_id.as_str())
    );
}

#[test]
fn opportunity_detector_scopes_module_timeout_event_to_r3_candidate_job() {
    let module_id = ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
    let workspace_id =
        WorkspaceId::from_legacy_named("workspace_self_evolution").expect("workspace");
    let event = opportunity_detection_event(
        MODULE_INVOCATION_DECISION_EVENT_KIND,
        serde_json::json!({
            "module_id": module_id.as_str(),
            "primary_error_kind": "timeout",
            "suggested_paths": ["modules/trigger/**"],
            "evidence_refs": ["module-telemetry://timeout-window"]
        }),
    );
    let detections = detect_evolution_opportunities_from_events(
        std::slice::from_ref(&event),
        &EvolutionOpportunityDetectionPolicy {
            workspace_id: workspace_id.clone(),
            default_target_ring: TrustRing::R2TrustedService,
            default_allowed_paths: vec!["crates/sinomo-module/**".to_string()],
            default_forbidden_paths: vec!["crates/sinomo-policy/**".to_string()],
        },
    );

    assert_eq!(detections.len(), 1);
    let detection = &detections[0];
    assert_eq!(
        detection.signal_kind,
        EvolutionOpportunitySignalKind::ModuleTimeout
    );
    assert_eq!(
        detection.opportunity.observed_event_id,
        Some(event.event_id)
    );
    assert_eq!(
        detection.opportunity.target_ring,
        TrustRing::R3SandboxedModule
    );
    assert_eq!(
        detection.opportunity.target_module_id.as_ref(),
        Some(&module_id)
    );
    assert_eq!(detection.opportunity.workspace_id, workspace_id);
    assert_eq!(
        detection.opportunity.allowed_paths,
        vec!["modules/trigger/**".to_string()]
    );
    assert!(
        detection
            .evidence_refs
            .iter()
            .any(|evidence| evidence == "module-telemetry://timeout-window")
    );

    let planner = EvolutionPlanner::new(NodeId::from_legacy_named("node_desktop").expect("node"));
    let (job, _workspace) = planner.scope_job(detection.opportunity.clone());
    assert_eq!(job.target_ring, TrustRing::R3SandboxedModule);
    assert!(job.required_gates.contains(&EvaluationGate::ReplayTests));
    assert!(
        !job.required_gates
            .contains(&EvaluationGate::ContinuityChecks)
    );
}

#[test]
fn opportunity_detector_scopes_rollout_rollback_event_with_transition_evidence() {
    let module_id = ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
    let workspace_id =
        WorkspaceId::from_legacy_named("workspace_self_evolution").expect("workspace");
    let event = opportunity_detection_event(
        "rollout.transition.completed",
        serde_json::json!({
            "module_id": module_id.as_str(),
            "action": "roll-back",
            "stage": "rolled-back",
            "triggering_evidence_refs": [
                "rollout-telemetry://active-rollback-trigger/timeout",
                "rollout-telemetry://window?after=0&through=42&limit=1000"
            ]
        }),
    );
    let event_ref = format!("event://{}", event.event_id);
    let detections = detect_evolution_opportunities_from_events(
        std::slice::from_ref(&event),
        &EvolutionOpportunityDetectionPolicy {
            workspace_id: workspace_id.clone(),
            default_target_ring: TrustRing::R2TrustedService,
            default_allowed_paths: vec!["modules/trigger/**".to_string()],
            default_forbidden_paths: vec!["crates/sinomo-policy/**".to_string()],
        },
    );

    assert_eq!(detections.len(), 1);
    let detection = &detections[0];
    assert_eq!(
        detection.signal_kind,
        EvolutionOpportunitySignalKind::RolloutRollback
    );
    assert_eq!(
        detection.opportunity.target_module_id.as_ref(),
        Some(&module_id)
    );
    assert_eq!(
        detection.opportunity.target_ring,
        TrustRing::R3SandboxedModule
    );
    assert!(
        detection
            .opportunity
            .objective
            .contains("Investigate rollback")
    );
    assert!(detection.evidence_refs.contains(&event_ref));
    assert!(
        detection
            .evidence_refs
            .iter()
            .any(|evidence| { evidence == "rollout-telemetry://active-rollback-trigger/timeout" })
    );
    assert!(detection.evidence_refs.iter().any(|evidence| {
        evidence == "rollout-telemetry://window?after=0&through=42&limit=1000"
    }));
    assert_eq!(
        detection.opportunity.allowed_paths,
        vec!["modules/trigger/**".to_string()]
    );
}

#[test]
fn background_tick_turns_rollout_rollback_into_guard_follow_up() {
    let module_id = ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
    let workspace_id =
        WorkspaceId::from_legacy_named("workspace_self_evolution").expect("workspace");
    let event = opportunity_detection_event(
        "rollout.transition.completed",
        serde_json::json!({
            "module_id": module_id.as_str(),
            "target_ring": "r3-sandboxed-module",
            "action": "roll-back",
            "stage": "rolled-back",
            "triggering_evidence_refs": [
                "rollout-telemetry://active-rollback-trigger/timeout",
                "rollout-telemetry://window?after=0&through=42&limit=1000"
            ]
        }),
    );
    let event_ref = format!("event://{}", event.event_id);

    let plan = plan_background_scheduling_tick(BackgroundSchedulingRequest {
        scheduler_service_id: "sinomo-cluster-scheduler".to_string(),
        campaigns: Vec::new(),
        opportunity_events: vec![event],
        previously_intaken_opportunity_event_ids: Vec::new(),
        opportunity_detection_policy: Some(EvolutionOpportunityDetectionPolicy {
            workspace_id,
            default_target_ring: TrustRing::R2TrustedService,
            default_allowed_paths: vec!["modules/trigger/**".to_string()],
            default_forbidden_paths: vec!["crates/sinomo-policy/**".to_string()],
        }),
        opportunity_planner_node_id: Some(NodeId::from_legacy_named("node_desktop").expect("node")),
        external_workloads: Vec::new(),
        resources: Vec::new(),
        task_estimates: Vec::new(),
        max_concurrent_campaigns: 1,
        max_concurrent_workloads: 1,
        idle_window_open: true,
        reset_window_open: false,
        dry_run: true,
        now: "2026-05-02T00:35:00Z".to_string(),
    });

    assert_eq!(plan.opportunity_intake.len(), 1);
    let intake = &plan.opportunity_intake[0];
    assert_eq!(
        intake.detection.signal_kind,
        EvolutionOpportunitySignalKind::RolloutRollback
    );
    assert!(intake.evidence_refs.contains(&event_ref));
    assert!(
        intake
            .evidence_refs
            .iter()
            .any(|evidence| { evidence == "rollout-telemetry://active-rollback-trigger/timeout" })
    );

    let campaign_update = &plan.campaigns[0];
    assert_eq!(
        campaign_update.action,
        BackgroundSchedulingAction::HoldHumanCheckpoint
    );
    assert_eq!(
        campaign_update.campaign.status,
        EvolutionCampaignStatus::AwaitingApproval
    );
    assert!(
        campaign_update
            .campaign
            .objective_card
            .constraints
            .iter()
            .any(|constraint| constraint == "background-context-signal=rollout-rollback")
    );
    assert!(
        campaign_update
            .campaign
            .objective_card
            .success_metrics
            .iter()
            .any(|metric| metric.contains("minimized replay"))
    );
    let task = &campaign_update.campaign.near_term_tasks[0];
    assert_eq!(task.expected_result_kind, EvolutionResultKind::RolloutGuard);
    assert!(task.objective.contains("minimized replay"));
    assert!(task.required_evidence.iter().any(|evidence| {
        evidence == "background-context-required-output=rollout-guard-or-minimized-regression-gate"
    }));
    assert!(task.required_evidence.iter().any(|evidence| {
        evidence == "rollout-telemetry://window?after=0&through=42&limit=1000"
    }));
    assert!(task.approval_required);
    assert!(campaign_update.scheduled_iteration.is_none());

    let approved_campaign = decide_campaign_checkpoint(
        campaign_update.campaign.clone(),
        CampaignCheckpointDecision::Approve,
        Some("operator reviewed rollback transition evidence".to_string()),
        "test-operator",
    );
    let task_id = approved_campaign.near_term_tasks[0].task_id.clone();
    let dispatch_plan = plan_background_scheduling_tick(BackgroundSchedulingRequest {
        scheduler_service_id: "sinomo-cluster-scheduler".to_string(),
        campaigns: vec![approved_campaign],
        opportunity_events: Vec::new(),
        previously_intaken_opportunity_event_ids: Vec::new(),
        opportunity_detection_policy: None,
        opportunity_planner_node_id: None,
        external_workloads: Vec::new(),
        resources: vec![ResourceClassDefinition {
            resource_id: "coding-plan-weekly".to_string(),
            unit: "percent".to_string(),
            total_units: 100,
            used_units: 0,
            reserve_floor_percent: 30,
            reset_window: "weekly".to_string(),
            notes: "test dispatch budget".to_string(),
        }],
        task_estimates: vec![ResourceUsageEstimate {
            task_id,
            resource: "coding-plan-weekly".to_string(),
            estimated_units: 10,
            actual_units: None,
        }],
        max_concurrent_campaigns: 1,
        max_concurrent_workloads: 1,
        idle_window_open: true,
        reset_window_open: false,
        dry_run: false,
        now: "2026-05-02T00:40:00Z".to_string(),
    });

    let dispatch_update = &dispatch_plan.campaigns[0];
    assert_eq!(
        dispatch_update.action,
        BackgroundSchedulingAction::DispatchIteration
    );
    let scheduled = dispatch_update
        .scheduled_iteration
        .as_ref()
        .expect("approved rollback guard gets scheduled handoff");
    assert!(
        scheduled
            .evolution_request
            .objective
            .contains("Rollout guard handoff")
    );
    assert!(
        scheduled
            .evolution_request
            .objective
            .contains("post-rollback regression mining")
    );
    assert!(
        scheduled
            .evolution_request
            .objective
            .contains("rollout-telemetry://active-rollback-trigger/timeout")
    );
    assert!(
        scheduled
            .evolution_request
            .intended_paths
            .contains(&"crates/sinomo-rollout/**".to_string())
    );
    assert!(
        scheduled
            .operator_notes
            .iter()
            .any(|note| note.contains("rollout-guard iterations"))
    );
}

#[test]
fn background_tick_intakes_opportunity_events_as_human_gated_campaigns() {
    let module_id = ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
    let workspace_id =
        WorkspaceId::from_legacy_named("workspace_self_evolution").expect("workspace");
    let event = opportunity_detection_event(
        MODULE_TIMEOUT_EVENT_KIND,
        serde_json::json!({
            "module_id": module_id.as_str(),
            "suggested_paths": ["modules/trigger/**"],
            "evidence_refs": ["module-telemetry://timeout-window"]
        }),
    );
    let event_ref = format!("event://{}", event.event_id);

    let plan = plan_background_scheduling_tick(BackgroundSchedulingRequest {
        scheduler_service_id: "sinomo-cluster-scheduler".to_string(),
        campaigns: Vec::new(),
        opportunity_events: vec![event],
        previously_intaken_opportunity_event_ids: Vec::new(),
        opportunity_detection_policy: Some(EvolutionOpportunityDetectionPolicy {
            workspace_id: workspace_id.clone(),
            default_target_ring: TrustRing::R2TrustedService,
            default_allowed_paths: vec!["crates/sinomo-module/**".to_string()],
            default_forbidden_paths: vec!["crates/sinomo-policy/**".to_string()],
        }),
        opportunity_planner_node_id: Some(NodeId::from_legacy_named("node_desktop").expect("node")),
        external_workloads: Vec::new(),
        resources: Vec::new(),
        task_estimates: Vec::new(),
        max_concurrent_campaigns: 1,
        max_concurrent_workloads: 1,
        idle_window_open: true,
        reset_window_open: false,
        dry_run: true,
        now: "2026-05-02T00:35:00Z".to_string(),
    });

    assert_eq!(plan.opportunity_intake.len(), 1);
    let intake = &plan.opportunity_intake[0];
    assert_eq!(
        intake.detection.signal_kind,
        EvolutionOpportunitySignalKind::ModuleTimeout
    );
    assert_eq!(
        intake.detection.opportunity.target_ring,
        TrustRing::R3SandboxedModule
    );
    assert_eq!(intake.detection.opportunity.workspace_id, workspace_id);
    assert!(intake.risk_gate.accepted);
    assert!(intake.risk_gate.human_checkpoint_required);
    assert!(!intake.risk_gate.dispatch_allowed);
    assert!(intake.risk_gate.evidence_gate.contains("event"));
    assert!(intake.risk_gate.budget_gate.contains("30m"));
    assert!(intake.risk_gate.risk_filter.contains("human checkpoint"));
    assert_eq!(intake.scoped_job.target_ring, TrustRing::R3SandboxedModule);
    assert!(
        intake
            .scoped_job
            .required_gates
            .contains(&EvaluationGate::ReplayTests)
    );
    assert!(intake.evidence_refs.contains(&event_ref));
    assert!(
        intake
            .evidence_refs
            .contains(&"module-telemetry://timeout-window".to_string())
    );

    assert_eq!(plan.campaigns.len(), 1);
    let campaign_update = &plan.campaigns[0];
    assert_eq!(
        campaign_update.action,
        BackgroundSchedulingAction::HoldHumanCheckpoint
    );
    assert_eq!(
        campaign_update.campaign.status,
        EvolutionCampaignStatus::AwaitingApproval
    );
    assert!(campaign_update.scheduled_iteration.is_none());
    assert!(plan.active_campaigns.is_empty());
    assert!(
        plan.human_checkpoints
            .iter()
            .any(|checkpoint| checkpoint.campaign_id == intake.campaign_id)
    );
    assert!(
        campaign_update
            .campaign
            .near_term_tasks
            .iter()
            .all(|task| task.approval_required)
    );
    assert!(
        campaign_update
            .campaign
            .objective_card
            .constraints
            .iter()
            .any(|constraint| { constraint == "background-risk-dispatch-allowed-at-intake=false" })
    );
    assert!(
        campaign_update
            .campaign
            .objective_card
            .constraints
            .iter()
            .any(|constraint| constraint.contains("background-risk-evidence-gate="))
    );
    assert!(campaign_update.campaign.near_term_tasks.iter().any(|task| {
        task.required_evidence.contains(&event_ref)
            && task
                .required_evidence
                .iter()
                .any(|evidence| evidence.contains("background-risk-filter="))
    }));
    assert!(
        campaign_update.evidence_refs.contains(&event_ref)
            && campaign_update.evidence_refs.iter().any(|evidence| {
                evidence.contains("background-risk-human-checkpoint-required=true")
            })
    );
}

#[test]
fn background_tick_skips_previously_intaken_opportunity_events() {
    let module_id = ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
    let event = opportunity_detection_event(
        MODULE_TIMEOUT_EVENT_KIND,
        serde_json::json!({
            "module_id": module_id.as_str(),
            "evidence_refs": ["module-telemetry://timeout-window"]
        }),
    );
    let event_id = event.event_id.to_string();

    let plan = plan_background_scheduling_tick(BackgroundSchedulingRequest {
        scheduler_service_id: "sinomo-cluster-scheduler".to_string(),
        campaigns: Vec::new(),
        opportunity_events: vec![event],
        previously_intaken_opportunity_event_ids: vec![event_id.clone()],
        opportunity_detection_policy: Some(EvolutionOpportunityDetectionPolicy {
            workspace_id: WorkspaceId::from_legacy_named("workspace_self_evolution")
                .expect("workspace"),
            default_target_ring: TrustRing::R2TrustedService,
            default_allowed_paths: vec!["crates/sinomo-module/**".to_string()],
            default_forbidden_paths: vec!["crates/sinomo-policy/**".to_string()],
        }),
        opportunity_planner_node_id: Some(NodeId::from_legacy_named("node_desktop").expect("node")),
        external_workloads: Vec::new(),
        resources: Vec::new(),
        task_estimates: Vec::new(),
        max_concurrent_campaigns: 1,
        max_concurrent_workloads: 1,
        idle_window_open: true,
        reset_window_open: false,
        dry_run: true,
        now: "2026-05-02T00:36:00Z".to_string(),
    });

    assert!(plan.opportunity_intake.is_empty());
    assert!(plan.campaigns.is_empty());
    assert_eq!(
        plan.opportunity_events_skipped,
        vec![BackgroundOpportunityEventSkip {
            event_id,
            reason: "event already recorded in background opportunity intake history".to_string()
        }]
    );
}

#[test]
fn approved_background_opportunity_dispatch_preserves_risk_and_telemetry_evidence() {
    let module_id = ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
    let workspace_id =
        WorkspaceId::from_legacy_named("workspace_self_evolution").expect("workspace");
    let event = opportunity_detection_event(
        MODULE_TIMEOUT_EVENT_KIND,
        serde_json::json!({
            "module_id": module_id.as_str(),
            "suggested_paths": ["modules/trigger/**"],
            "evidence_refs": ["module-telemetry://timeout-window"]
        }),
    );
    let event_ref = format!("event://{}", event.event_id);

    let intake_plan = plan_background_scheduling_tick(BackgroundSchedulingRequest {
        scheduler_service_id: "sinomo-cluster-scheduler".to_string(),
        campaigns: Vec::new(),
        opportunity_events: vec![event],
        previously_intaken_opportunity_event_ids: Vec::new(),
        opportunity_detection_policy: Some(EvolutionOpportunityDetectionPolicy {
            workspace_id,
            default_target_ring: TrustRing::R2TrustedService,
            default_allowed_paths: vec!["crates/sinomo-module/**".to_string()],
            default_forbidden_paths: vec!["crates/sinomo-policy/**".to_string()],
        }),
        opportunity_planner_node_id: Some(NodeId::from_legacy_named("node_desktop").expect("node")),
        external_workloads: Vec::new(),
        resources: Vec::new(),
        task_estimates: Vec::new(),
        max_concurrent_campaigns: 1,
        max_concurrent_workloads: 1,
        idle_window_open: true,
        reset_window_open: false,
        dry_run: true,
        now: "2026-05-02T00:35:00Z".to_string(),
    });
    let campaign = decide_campaign_checkpoint(
        intake_plan.campaigns[0].campaign.clone(),
        CampaignCheckpointDecision::Approve,
        Some("operator reviewed telemetry evidence".to_string()),
        "test-operator",
    );
    let task_id = campaign.near_term_tasks[0].task_id.clone();

    let dispatch_plan = plan_background_scheduling_tick(BackgroundSchedulingRequest {
        scheduler_service_id: "sinomo-cluster-scheduler".to_string(),
        campaigns: vec![campaign],
        opportunity_events: Vec::new(),
        previously_intaken_opportunity_event_ids: Vec::new(),
        opportunity_detection_policy: None,
        opportunity_planner_node_id: None,
        external_workloads: Vec::new(),
        resources: vec![ResourceClassDefinition {
            resource_id: "coding-plan-weekly".to_string(),
            unit: "percent".to_string(),
            total_units: 100,
            used_units: 0,
            reserve_floor_percent: 30,
            reset_window: "weekly".to_string(),
            notes: "test dispatch budget".to_string(),
        }],
        task_estimates: vec![ResourceUsageEstimate {
            task_id,
            resource: "coding-plan-weekly".to_string(),
            estimated_units: 10,
            actual_units: None,
        }],
        max_concurrent_campaigns: 1,
        max_concurrent_workloads: 1,
        idle_window_open: true,
        reset_window_open: false,
        dry_run: false,
        now: "2026-05-02T00:40:00Z".to_string(),
    });

    let update = &dispatch_plan.campaigns[0];
    assert_eq!(update.action, BackgroundSchedulingAction::DispatchIteration);
    assert!(update.evidence_refs.contains(&event_ref));
    assert!(
        update
            .evidence_refs
            .contains(&"module-telemetry://timeout-window".to_string())
    );
    assert!(
        update.evidence_refs.iter().any(|evidence| {
            evidence.contains("background-risk-dispatch-allowed-at-intake=false")
        })
    );
    let scheduled = update
        .scheduled_iteration
        .as_ref()
        .expect("approved opportunity dispatch gets scheduled iteration");
    assert!(
        scheduled
            .evolution_request
            .objective
            .contains("background-risk-evidence-gate=")
    );
    assert!(
        scheduled
            .evolution_request
            .objective
            .contains("module-telemetry://timeout-window")
    );
}

#[test]
fn opportunity_detector_ignores_successful_invocation_events() {
    let module_id = ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
    let event = opportunity_detection_event(
        MODULE_INVOCATION_DECISION_EVENT_KIND,
        serde_json::json!({
            "module_id": module_id.as_str(),
            "primary_error_kind": null,
            "primary_lane": "active"
        }),
    );
    let detections = detect_evolution_opportunities_from_events(
        &[event],
        &EvolutionOpportunityDetectionPolicy {
            workspace_id: WorkspaceId::from_legacy_named("workspace_self_evolution")
                .expect("workspace"),
            default_target_ring: TrustRing::R3SandboxedModule,
            default_allowed_paths: vec!["modules/trigger/**".to_string()],
            default_forbidden_paths: Vec::new(),
        },
    );

    assert!(detections.is_empty());
}

fn opportunity_detection_event(kind: &str, payload: serde_json::Value) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        NodeId::from_legacy_named("node_desktop").expect("node"),
        Source::new("module.live").expect("source"),
        EventKind::new(kind).expect("kind"),
        Salience::new(0.83).expect("salience"),
        Confidence::new(0.96).expect("confidence"),
        RoutingMetadata::new(
            RoutingScope::TrustedNetwork,
            LatencyClass::Background,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(payload)
}

#[test]
fn git_status_parser_handles_modified_untracked_and_renamed_paths() {
    let paths = parse_git_status_paths(
        " M crates/a.rs\nM agent-target.md\n?? docs/new.md\nR  old.rs -> new.rs\n",
    )
    .expect("status paths");

    assert_eq!(
        paths,
        vec![
            "agent-target.md".to_string(),
            "crates/a.rs".to_string(),
            "docs/new.md".to_string(),
            "new.rs".to_string()
        ]
    );
}

#[test]
fn mutation_prompt_contains_objective_verification_and_recovery() {
    let packet = MutationPacket {
        job_id: EvolutionJobId::from_legacy_named("evojob_prompt").expect("job"),
        objective: "improve bundle summaries".to_string(),
        target_ring: TrustRing::R1TrustedRuntime,
        edit_scope: EditScope {
            workspace_root: "candidate".to_string(),
            intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
            notes: "keep it focused".to_string(),
        },
        relevant_files: Vec::new(),
        verification: VerificationPlan {
            commands: vec![VerificationCommand {
                name: "check".to_string(),
                command: "cargo check".to_string(),
                expected_exit_code: 0,
                gate: EvaluationGate::StaticChecks,
            }],
            module_replay_reports: Vec::new(),
        },
        output_contract: MutationOutputContract::default(),
        recovery: RecoveryPlan::default(),
    };

    let prompt = render_mutation_prompt(&packet);

    assert!(prompt.contains("improve bundle summaries"));
    assert!(prompt.contains("cargo check"));
    assert!(prompt.contains("recovery notes"));
    assert!(prompt.contains("restore the disposable VM snapshot"));
}

#[test]
fn bridge_prompt_exposes_candidate_workspace_tools_and_json_contract() {
    let root = temp_dir("bridge-prompt");
    let workspace = candidate_workspace(&root);
    let packet = MutationPacket {
        job_id: workspace.job_id.clone(),
        objective: "add mutation evidence".to_string(),
        target_ring: TrustRing::R1TrustedRuntime,
        edit_scope: EditScope {
            workspace_root: root.display().to_string(),
            intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
            notes: "keep it focused".to_string(),
        },
        relevant_files: Vec::new(),
        verification: VerificationPlan {
            commands: Vec::new(),
            module_replay_reports: Vec::new(),
        },
        output_contract: MutationOutputContract::default(),
        recovery: RecoveryPlan::default(),
    };

    let prompt = render_mutation_bridge_prompt(
        &packet,
        &workspace,
        &EvolutionToolRegistry::bootstrap().descriptors(),
        true,
        false,
    );

    assert!(prompt.contains("Candidate Workspace"));
    assert!(prompt.contains(&root.display().to_string()));
    assert!(prompt.contains("apply_patch"));
    assert!(prompt.contains("interrupt_command"));
    assert!(prompt.contains("kill_command"));
    assert!(prompt.contains("no further stdout/stderr"));
    assert!(prompt.contains("Required JSON Response"));
    assert!(prompt.contains("json object"));
}

#[test]
fn structured_mutation_output_parses_json_and_patch_fences() {
    let structured = parse_structured_mutation_output(
        r#"{"changed_files":["src/lib.rs"],"patch":"diff --git a/src/lib.rs b/src/lib.rs","patch_summary":"updated bridge"}"#,
    )
    .expect("structured output");

    assert_eq!(structured.changed_files, vec!["src/lib.rs".to_string()]);
    assert_eq!(structured.patch_summary.as_deref(), Some("updated bridge"));

    let patch = extract_patch_from_text(
        "notes\n```diff\ndiff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n```\n",
    )
    .expect("patch");
    assert!(patch.starts_with("diff --git"));

    let fenced = parse_structured_mutation_output(
        "```json\n{\"tool_calls\":[{\"tool\":\"read_file\",\"input\":{\"path\":\"Cargo.toml\"}}],\"changed_files\":[]}\n```",
    )
    .expect("fenced json");
    assert_eq!(fenced.tool_calls.len(), 1);
    assert_eq!(fenced.tool_calls[0].tool, "read_file");

    let array_notes = parse_structured_mutation_output(
        r#"{"changed_files":["agent-target.md"],"patch":"diff --git a/agent-target.md b/agent-target.md","verification_notes":["ran grep","passed"],"experiment_notes":["none"],"recovery_notes":["checkout file"]}"#,
    )
    .expect("array notes json");
    assert_eq!(
        array_notes.verification_notes.as_deref(),
        Some("ran grep\npassed")
    );
    assert_eq!(array_notes.experiment_notes.as_deref(), Some("none"));
    assert_eq!(array_notes.recovery_notes.as_deref(), Some("checkout file"));
}

#[test]
fn mutation_packet_collects_relevant_source_excerpts_from_intended_paths() {
    let root = temp_dir("packet-excerpts");
    fs::create_dir_all(root.join("crates/example/src")).expect("dirs");
    fs::write(
        root.join("crates/example/Cargo.toml"),
        "[package]\nname = \"example\"\n",
    )
    .expect("manifest");
    fs::write(
        root.join("crates/example/src/lib.rs"),
        "pub fn example() {}\n",
    )
    .expect("lib");
    let job = create_bootstrap_job(
        &EvolutionRequest {
            objective: "collect context".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            intended_paths: vec!["crates/example/**".to_string()],
            verification_commands: Vec::new(),
            benchmark_gates: Vec::new(),
            module_replay_reports: Vec::new(),
            model_provider: None,
            model: None,
            dry_run: true,
        },
        &root,
    );

    let packet = mutation_packet_for_job(&job);

    assert!(
        packet
            .relevant_files
            .iter()
            .any(|file| file.path == "crates/example/Cargo.toml")
    );
    assert!(
        packet
            .relevant_files
            .iter()
            .any(|file| file.path == "crates/example/src/lib.rs")
    );
}

#[test]
fn language_model_bridge_requests_json_and_applies_returned_patch() {
    let root = git_repo("bridge-applies-patch");
    fs::write(root.join("patched.txt"), "old\n").expect("write tracked");
    run_in(&root, "git add patched.txt").expect("git add");
    run_in(&root, "git commit -m init").expect("git commit");
    let workspace = candidate_workspace(&root);
    let provider = FakeLanguageModelProvider::new(
        LanguageModelProviderInfo {
            provider: "fake-json".to_string(),
            default_model: "fake-model".to_string(),
            invocation: sinomo_lm::LanguageModelInvocation::Http,
            host_access: LanguageModelHostAccess::NetworkOnly,
            capabilities: sinomo_lm::LanguageModelProviderCapabilities::default(),
            supports_json_object: true,
            supports_tool_permissions: false,
        },
        serde_json::json!({
            "changed_files": ["patched.txt"],
            "patch": "diff --git a/patched.txt b/patched.txt\n--- a/patched.txt\n+++ b/patched.txt\n@@ -1 +1 @@\n-old\n+new\n",
            "patch_summary": "updated patched.txt",
            "verification_notes": "not run",
            "experiment_notes": "unit test fixture",
            "recovery_notes": "restore temp repo"
        })
        .to_string(),
    );
    let requests = provider.requests.clone();
    let bridge = LanguageModelMutationBridge::new(provider, PROVIDER_DEFAULT_MODEL);
    let packet = MutationPacket {
        job_id: workspace.job_id.clone(),
        objective: "apply patch from json provider".to_string(),
        target_ring: TrustRing::R1TrustedRuntime,
        edit_scope: EditScope {
            workspace_root: root.display().to_string(),
            intended_paths: vec!["patched.txt".to_string()],
            notes: String::new(),
        },
        relevant_files: Vec::new(),
        verification: VerificationPlan {
            commands: Vec::new(),
            module_replay_reports: Vec::new(),
        },
        output_contract: MutationOutputContract::default(),
        recovery: RecoveryPlan::default(),
    };

    let result = bridge.mutate(&packet, &workspace, None).expect("mutate");

    assert_eq!(
        requests.borrow()[0].response_format,
        LanguageModelResponseFormat::JsonObject
    );
    assert_eq!(result.summary.as_deref(), Some("updated patched.txt"));
    assert!(result.parsed_patch.as_deref().is_some());
    assert_eq!(result.tool_results[0].status, ToolCallStatus::Succeeded);
    let patched = fs::read_to_string(root.join("patched.txt")).expect("read patched");
    assert_eq!(patched.replace("\r\n", "\n"), "new\n");
}

#[test]
fn language_model_bridge_runs_tool_rounds_before_final_patch() {
    let root = git_repo("bridge-tool-rounds");
    fs::write(root.join("patched.txt"), "old\n").expect("write tracked");
    run_in(&root, "git add patched.txt").expect("git add");
    run_in(&root, "git commit -m init").expect("git commit");
    let workspace = candidate_workspace(&root);
    let provider = FakeLanguageModelProvider::with_responses(
        LanguageModelProviderInfo {
            provider: "fake-tool-loop".to_string(),
            default_model: "fake-model".to_string(),
            invocation: sinomo_lm::LanguageModelInvocation::Http,
            host_access: LanguageModelHostAccess::NetworkOnly,
            capabilities: sinomo_lm::LanguageModelProviderCapabilities::default(),
            supports_json_object: true,
            supports_tool_permissions: false,
        },
        vec![
            serde_json::json!({
                "tool_calls": [
                    { "tool": "read_file", "input": { "path": "patched.txt" } }
                ],
                "changed_files": []
            })
            .to_string(),
            serde_json::json!({
                "tool_calls": [],
                "changed_files": ["patched.txt"],
                "patch": "diff --git a/patched.txt b/patched.txt\n--- a/patched.txt\n+++ b/patched.txt\n@@ -1 +1 @@\n-old\n+new\n",
                "patch_summary": "updated after reading file"
            })
            .to_string(),
        ],
    );
    let requests = provider.requests.clone();
    let bridge = LanguageModelMutationBridge::new(provider, PROVIDER_DEFAULT_MODEL);
    let packet = MutationPacket {
        job_id: workspace.job_id.clone(),
        objective: "use tools before patch".to_string(),
        target_ring: TrustRing::R3SandboxedModule,
        edit_scope: EditScope {
            workspace_root: root.display().to_string(),
            intended_paths: vec!["patched.txt".to_string()],
            notes: String::new(),
        },
        relevant_files: Vec::new(),
        verification: VerificationPlan {
            commands: Vec::new(),
            module_replay_reports: Vec::new(),
        },
        output_contract: MutationOutputContract::default(),
        recovery: RecoveryPlan::default(),
    };

    let result = bridge.mutate(&packet, &workspace, None).expect("mutate");

    assert_eq!(requests.borrow().len(), 2);
    assert!(requests.borrow()[0].metadata["target_ring"].is_null());
    assert_eq!(
        requests.borrow()[0].metadata["target_ring_policy"],
        "ignored-during-bootstrap"
    );
    assert!(
        requests.borrow()[1]
            .messages
            .last()
            .unwrap()
            .content
            .contains("Tool results")
    );
    assert_eq!(result.model_turns.len(), 2);
    assert_eq!(result.tool_results[0].tool, "read_file");
    assert_eq!(result.tool_results[1].tool, "apply_patch");
    let patched = fs::read_to_string(root.join("patched.txt")).expect("read patched");
    assert_eq!(patched.replace("\r\n", "\n"), "new\n");
}

#[test]
fn language_model_bridge_treats_http_tool_providers_as_structured_patch_mode() {
    let root = git_repo("bridge-http-tool-provider");
    fs::write(root.join("patched.txt"), "old\n").expect("write tracked");
    run_in(&root, "git add patched.txt").expect("git add");
    run_in(&root, "git commit -m init").expect("git commit");
    let workspace = candidate_workspace(&root);
    let provider = FakeLanguageModelProvider::new(
        LanguageModelProviderInfo {
            provider: "fake-http-tools".to_string(),
            default_model: "fake-model".to_string(),
            invocation: sinomo_lm::LanguageModelInvocation::Http,
            host_access: LanguageModelHostAccess::NetworkOnly,
            capabilities: sinomo_lm::LanguageModelProviderCapabilities::default(),
            supports_json_object: true,
            supports_tool_permissions: true,
        },
        serde_json::json!({
            "tool_calls": [],
            "changed_files": ["patched.txt"],
            "patch": "diff --git a/patched.txt b/patched.txt\n--- a/patched.txt\n+++ b/patched.txt\n@@ -1 +1 @@\n-old\n+new\n",
            "patch_summary": "patched tracked file"
        })
        .to_string(),
    );
    let requests = provider.requests.clone();
    let bridge = LanguageModelMutationBridge::new(provider, PROVIDER_DEFAULT_MODEL);
    let packet = MutationPacket {
        job_id: workspace.job_id.clone(),
        objective: "patch file".to_string(),
        target_ring: TrustRing::R1TrustedRuntime,
        edit_scope: EditScope {
            workspace_root: root.display().to_string(),
            intended_paths: vec!["patched.txt".to_string()],
            notes: String::new(),
        },
        relevant_files: Vec::new(),
        verification: VerificationPlan {
            commands: Vec::new(),
            module_replay_reports: Vec::new(),
        },
        output_contract: MutationOutputContract::default(),
        recovery: RecoveryPlan::default(),
    };

    let result = bridge.mutate(&packet, &workspace, None).expect("mutation");

    let request = &requests.borrow()[0];
    assert_eq!(request.metadata["direct_edit"], false);
    assert_eq!(
        request.response_format,
        LanguageModelResponseFormat::JsonObject
    );
    assert_eq!(
        result
            .structured_output
            .as_ref()
            .map(|output| output.changed_files.clone()),
        Some(vec!["patched.txt".to_string()])
    );
    let patched = fs::read_to_string(root.join("patched.txt")).expect("read patched");
    assert_eq!(patched.replace("\r\n", "\n"), "new\n");
}

#[test]
fn unrestricted_language_model_bridge_fails_closed_without_permission_context() {
    let root = temp_dir("bridge-unrestricted-no-permission-context");
    let workspace = candidate_workspace(&root);
    let provider = FakeLanguageModelProvider::new(
        LanguageModelProviderInfo {
            provider: "fake-unrestricted".to_string(),
            default_model: "fake-model".to_string(),
            invocation: sinomo_lm::LanguageModelInvocation::Process,
            host_access: LanguageModelHostAccess::Unrestricted,
            capabilities: sinomo_lm::LanguageModelProviderCapabilities::default(),
            supports_json_object: false,
            supports_tool_permissions: true,
        },
        "unreachable".to_string(),
    );
    let requests = provider.requests.clone();
    let bridge = LanguageModelMutationBridge::new(provider, PROVIDER_DEFAULT_MODEL);

    let error = bridge
        .mutate(&bridge_permission_test_packet(&workspace), &workspace, None)
        .expect_err("unrestricted provider must require authoritative local consent");

    assert!(requests.borrow().is_empty());
    assert!(error.to_string().contains("remote-management"));
    assert!(error.to_string().contains("no authoritative"));
}

#[test]
fn unrestricted_language_model_bridge_rechecks_permission_between_provider_calls() {
    let root = temp_dir("bridge-unrestricted-revocation");
    fs::write(root.join("context.txt"), "permission boundary\n").expect("write context");
    let workspace = candidate_workspace(&root);
    let data_dir = SinomoDataDir::new(root.join("node-data"));
    set_remote_management(&data_dir, true);
    let requests = Rc::new(RefCell::new(Vec::new()));
    let provider = RevokingLanguageModelProvider {
        store: LocalNodePermissionStore::new(data_dir.root()),
        requests: requests.clone(),
    };
    let bridge =
        LanguageModelMutationBridge::new_for_data_dir(provider, PROVIDER_DEFAULT_MODEL, &data_dir);

    let error = bridge
        .mutate(&bridge_permission_test_packet(&workspace), &workspace, None)
        .expect_err("revocation must stop the next provider invocation");

    assert_eq!(requests.borrow().len(), 1);
    assert!(error.to_string().contains("remote-management"));
    assert!(!LocalNodePermissionStore::new(data_dir.root()).remote_management_enabled());
}

#[test]
fn language_model_bridge_errors_when_final_patch_fails_to_apply() {
    let root = git_repo("bridge-bad-patch");
    fs::write(root.join("patched.txt"), "old\n").expect("write tracked");
    run_in(&root, "git add patched.txt").expect("git add");
    run_in(&root, "git commit -m init").expect("git commit");
    let workspace = candidate_workspace(&root);
    let provider = FakeLanguageModelProvider::new(
        LanguageModelProviderInfo {
            provider: "fake-json".to_string(),
            default_model: "fake-model".to_string(),
            invocation: sinomo_lm::LanguageModelInvocation::Http,
            host_access: LanguageModelHostAccess::NetworkOnly,
            capabilities: sinomo_lm::LanguageModelProviderCapabilities::default(),
            supports_json_object: true,
            supports_tool_permissions: false,
        },
        serde_json::json!({
            "tool_calls": [],
            "changed_files": ["missing.txt"],
            "patch": "diff --git a/missing.txt b/missing.txt\n--- a/missing.txt\n+++ b/missing.txt\n@@ -1 +1 @@\n-old\n+new\n"
        })
        .to_string(),
    );
    let bridge = LanguageModelMutationBridge::new(provider, PROVIDER_DEFAULT_MODEL);
    let packet = MutationPacket {
        job_id: workspace.job_id.clone(),
        objective: "bad patch".to_string(),
        target_ring: TrustRing::R1TrustedRuntime,
        edit_scope: EditScope {
            workspace_root: root.display().to_string(),
            intended_paths: vec!["missing.txt".to_string()],
            notes: String::new(),
        },
        relevant_files: Vec::new(),
        verification: VerificationPlan {
            commands: Vec::new(),
            module_replay_reports: Vec::new(),
        },
        output_contract: MutationOutputContract::default(),
        recovery: RecoveryPlan::default(),
    };

    let error = bridge
        .mutate(&packet, &workspace, None)
        .expect_err("bad patch");

    assert!(matches!(error, MutationModelError::ToolExecution { .. }));
}

#[test]
fn local_bundle_store_writes_packet_and_manifest_paths() {
    let root = temp_dir("bundle-store");
    let store = LocalCandidateBundleStore::new(root.join("candidates"));
    let job = create_bootstrap_job(
        &EvolutionRequest {
            objective: "write bundle".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            intended_paths: Vec::new(),
            verification_commands: Vec::new(),
            benchmark_gates: Vec::new(),
            module_replay_reports: Vec::new(),
            model_provider: None,
            model: None,
            dry_run: true,
        },
        &root,
    );
    let manifest = store.create(&job).expect("bundle create");
    let packet = mutation_packet_for_job(&job);

    store
        .write_json(&manifest, "mutation-packet.json", &packet)
        .expect("packet write");

    assert!(PathBuf::from(&manifest.bundle_path).exists());
    assert!(PathBuf::from(&manifest.mutation_packet_path).exists());
}

#[test]
fn orchestrator_dry_run_writes_bundle_and_runs_verification() {
    let root = temp_dir("orchestrator");
    let candidate = root.join("candidate");
    fs::create_dir_all(&candidate).expect("candidate dir");
    let orchestrator = EvolutionOrchestrator::new(
        FakeWorkspace {
            root: root.to_path_buf(),
            candidate: candidate.clone(),
        },
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(root.join("candidates")),
    );

    let report = orchestrator
        .propose(
            EvolutionRequest {
                objective: "dry run".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
                verification_commands: vec![VerificationCommand {
                    name: "success".to_string(),
                    command: "exit 0".to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }],
                benchmark_gates: Vec::new(),
                module_replay_reports: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: true,
            },
            None,
        )
        .expect("dry run propose");

    assert_eq!(report.job.status, EvolutionJobStatus::AwaitingApproval);
    assert_eq!(
        report.scorecard.as_ref().expect("scorecard").outcome,
        EvaluationOutcome::Passed
    );
    assert!(PathBuf::from(&report.bundle.mutation_packet_path).exists());
}

#[test]
fn orchestrator_lifecycle_observer_records_durable_statuses() {
    let root = temp_dir("orchestrator-lifecycle");
    let candidate = root.join("candidate");
    fs::create_dir_all(&candidate).expect("candidate dir");
    let orchestrator = EvolutionOrchestrator::new(
        FakeWorkspace {
            root: root.to_path_buf(),
            candidate: candidate.clone(),
        },
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(root.join("candidates")),
    );
    let observer = RecordingLifecycleObserver::default();

    let report = orchestrator
        .propose_with_lifecycle(
            EvolutionRequest {
                objective: "observe lifecycle".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
                verification_commands: vec![VerificationCommand {
                    name: "success".to_string(),
                    command: "exit 0".to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }],
                benchmark_gates: Vec::new(),
                module_replay_reports: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: true,
            },
            None,
            Some(&observer),
        )
        .expect("dry run propose");

    assert_eq!(report.job.status, EvolutionJobStatus::AwaitingApproval);
    let updates = observer.updates.borrow();
    let statuses = updates
        .iter()
        .map(|update| update.job.status.clone())
        .collect::<Vec<_>>();
    assert_eq!(statuses.first(), Some(&EvolutionJobStatus::Created));
    assert_eq!(statuses.last(), Some(&EvolutionJobStatus::AwaitingApproval));
    for expected in [
        EvolutionJobStatus::PacketWritten,
        EvolutionJobStatus::MutationFinished,
        EvolutionJobStatus::Implemented,
        EvolutionJobStatus::Built,
        EvolutionJobStatus::Evaluated,
    ] {
        assert!(statuses.contains(&expected), "missing {expected:?}");
    }
    assert!(updates.iter().any(|update| {
        update.job.status == EvolutionJobStatus::Evaluated
            && update.verification_outcome == Some(EvaluationOutcome::Passed)
    }));
}

#[test]
fn orchestrator_writes_model_turn_and_tool_evidence_artifacts() {
    let root = temp_dir("orchestrator-model-evidence");
    let candidate = root.join("candidate");
    fs::create_dir_all(&candidate).expect("candidate dir");
    let orchestrator = EvolutionOrchestrator::new(
        FakeWorkspace {
            root: root.to_path_buf(),
            candidate: candidate.clone(),
        },
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(root.join("jobs")),
    );
    let model = FakeMutationModel;

    let report = orchestrator
        .propose(
            EvolutionRequest {
                objective: "write model evidence".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
                verification_commands: vec![VerificationCommand {
                    name: "success".to_string(),
                    command: "exit 0".to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }],
                benchmark_gates: Vec::new(),
                module_replay_reports: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: false,
            },
            Some(&model),
        )
        .expect("propose");
    let bundle = PathBuf::from(&report.bundle.bundle_path);

    assert!(bundle.join("model-output.txt").exists());
    assert!(bundle.join("model-result.json").exists());
    assert!(bundle.join("model-turns.json").exists());
    assert!(bundle.join("model-tool-results.json").exists());
}

#[test]
fn orchestrator_writes_agent_run_artifacts() {
    let root = temp_dir("orchestrator-agent-evidence");
    let candidate = root.join("candidate");
    fs::create_dir_all(&candidate).expect("candidate dir");
    let orchestrator = EvolutionOrchestrator::new(
        FakeWorkspace {
            root: root.to_path_buf(),
            candidate: candidate.clone(),
        },
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(root.join("jobs")),
    );
    let model = FakeAgentArtifactMutationModel;

    let report = orchestrator
        .propose(
            EvolutionRequest {
                objective: "write agent evidence".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
                verification_commands: Vec::new(),
                benchmark_gates: Vec::new(),
                module_replay_reports: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: false,
            },
            Some(&model),
        )
        .expect("propose");
    let bundle = PathBuf::from(&report.bundle.bundle_path);

    assert!(bundle.join("agent-run.json").exists());
    assert!(bundle.join("agent-summary.json").exists());
    assert!(bundle.join("agent-events.jsonl").exists());
    assert!(bundle.join("coding-run-state.json").exists());
    assert!(bundle.join("patch-snapshots.json").exists());
    assert!(bundle.join("validation-records.json").exists());
    assert!(bundle.join("resource-usage.json").exists());
    assert!(bundle.join("bundle-content-manifest.json").exists());
    assert!(bundle.join("bundle-attestation.json").exists());
    let expected_agent_run_path = bundle.join("agent-run.json").display().to_string();
    let expected_agent_summary_path = bundle.join("agent-summary.json").display().to_string();
    let expected_agent_events_path = bundle.join("agent-events.jsonl").display().to_string();
    let expected_coding_run_state_path = bundle.join("coding-run-state.json").display().to_string();
    let expected_patch_snapshots_path = bundle.join("patch-snapshots.json").display().to_string();
    let expected_validation_records_path =
        bundle.join("validation-records.json").display().to_string();
    let expected_resource_usage_path = bundle.join("resource-usage.json").display().to_string();
    let expected_content_manifest_path = bundle
        .join("bundle-content-manifest.json")
        .display()
        .to_string();
    let expected_attestation_path = bundle.join("bundle-attestation.json").display().to_string();
    assert_eq!(
        report.bundle.agent_run_path.as_deref(),
        Some(expected_agent_run_path.as_str())
    );
    assert_eq!(
        report.bundle.agent_summary_path.as_deref(),
        Some(expected_agent_summary_path.as_str())
    );
    assert!(
        report
            .bundle
            .artifact_refs
            .iter()
            .any(|reference| reference == &expected_agent_events_path)
    );
    assert!(
        report
            .bundle
            .artifact_refs
            .iter()
            .any(|reference| reference == &expected_coding_run_state_path)
    );
    assert!(
        report
            .bundle
            .artifact_refs
            .iter()
            .any(|reference| reference == &expected_patch_snapshots_path)
    );
    assert!(
        report
            .bundle
            .artifact_refs
            .iter()
            .any(|reference| reference == &expected_validation_records_path)
    );
    let agent_summary: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(&expected_agent_summary_path).expect("agent summary JSON"),
    )
    .expect("parse agent summary JSON");
    assert_eq!(
        agent_summary["model_metrics"]["native_tool_call_turn_count"],
        serde_json::json!(1)
    );
    assert_eq!(
        agent_summary["model_metrics"]["final_decision_source"],
        serde_json::json!("strict_json_decision")
    );
    assert_eq!(
        agent_summary["tool_pair_integrity"]["tool_calls"],
        serde_json::json!(1)
    );
    assert_eq!(
        agent_summary["tool_pair_integrity"]["tool_results"],
        serde_json::json!(1)
    );
    assert_eq!(
        agent_summary["tool_pair_integrity"]["orphan_tool_calls"],
        serde_json::json!(0)
    );
    assert_eq!(
        agent_summary["tool_pair_integrity"]["orphan_tool_results"],
        serde_json::json!(0)
    );
    let agent_events =
        fs::read_to_string(bundle.join("agent-events.jsonl")).expect("agent events JSONL");
    let agent_event_lines = agent_events.lines().collect::<Vec<_>>();
    assert_eq!(agent_event_lines.len(), 1);
    let agent_event: serde_json::Value =
        serde_json::from_str(agent_event_lines[0]).expect("agent event JSON line");
    assert_eq!(
        agent_event["payload"]["final_decision_source"],
        serde_json::json!("strict_json_decision")
    );
    let coding_run_state: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(bundle.join("coding-run-state.json")).expect("coding run state JSON"),
    )
    .expect("parse coding run state JSON");
    assert_eq!(
        coding_run_state["stop_recommendation"],
        serde_json::json!("finish_with_patch")
    );
    let patch_snapshots: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(bundle.join("patch-snapshots.json")).expect("patch snapshots JSON"),
    )
    .expect("parse patch snapshots JSON");
    assert_eq!(
        patch_snapshots[0]["PatchSnapshot"]["changed_files"],
        serde_json::json!(["hello.txt"])
    );
    let validation_records: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(bundle.join("validation-records.json"))
            .expect("validation records JSON"),
    )
    .expect("parse validation records JSON");
    assert_eq!(
        validation_records[0]["command"],
        serde_json::json!("cargo test -p sinomo-evolution")
    );
    assert_eq!(validation_records[0]["status"], serde_json::json!("passed"));
    assert_eq!(
        report.bundle.resource_usage_path.as_deref(),
        Some(expected_resource_usage_path.as_str())
    );
    assert_eq!(
        report.bundle.content_manifest_path.as_deref(),
        Some(expected_content_manifest_path.as_str())
    );
    assert_eq!(
        report.bundle.attestation_path.as_deref(),
        Some(expected_attestation_path.as_str())
    );
    assert!(
        report
            .bundle
            .bundle_content_hash
            .as_deref()
            .is_some_and(|hash| hash.starts_with("blake3:"))
    );
    let usage: CandidateResourceUsageSummary =
        serde_json::from_str(&fs::read_to_string(bundle.join("resource-usage.json")).unwrap())
            .expect("resource usage");
    assert!(usage.records.iter().any(|record| {
        record.resource == "agent-input-tokens"
            && record.units == 5
            && record.confidence == ResourceUsageConfidence::ProviderReported
    }));
    let content_manifest: CandidateBundleContentManifest = serde_json::from_str(
        &fs::read_to_string(bundle.join("bundle-content-manifest.json")).unwrap(),
    )
    .expect("content manifest");
    assert_eq!(
        report.bundle.bundle_content_hash.as_deref(),
        Some(content_manifest.bundle_content_hash.as_str())
    );
    assert!(
        content_manifest
            .files
            .iter()
            .any(|file| file.path == "agent-run.json" && file.digest.starts_with("blake3:"))
    );
    assert!(
        content_manifest
            .files
            .iter()
            .any(|file| file.path == "agent-events.jsonl" && file.digest.starts_with("blake3:"))
    );
    assert!(content_manifest.files.iter().any(|file| {
        file.path == "coding-run-state.json" && file.digest.starts_with("blake3:")
    }));
    assert!(
        content_manifest.files.iter().any(|file| {
            file.path == "patch-snapshots.json" && file.digest.starts_with("blake3:")
        })
    );
    assert!(content_manifest.files.iter().any(|file| {
        file.path == "validation-records.json" && file.digest.starts_with("blake3:")
    }));
    assert!(
        !content_manifest
            .files
            .iter()
            .any(|file| file.path == "manifest.json")
    );
    let attestation: CandidateBundleAttestation =
        serde_json::from_str(&fs::read_to_string(bundle.join("bundle-attestation.json")).unwrap())
            .expect("attestation");
    assert_eq!(
        attestation.subject.digest,
        content_manifest.bundle_content_hash
    );
    assert_eq!(attestation.predicate.mutation_worker, "agent-loop");
    assert!(
        attestation
            .predicate
            .artifact_refs
            .iter()
            .any(|reference| reference.ends_with("agent-run.json"))
    );
    assert!(
        attestation
            .predicate
            .artifact_refs
            .iter()
            .any(|reference| reference.ends_with("agent-events.jsonl"))
    );
    assert!(
        attestation
            .predicate
            .artifact_refs
            .iter()
            .any(|reference| reference.ends_with("coding-run-state.json"))
    );
    assert!(
        attestation
            .predicate
            .artifact_refs
            .iter()
            .any(|reference| reference.ends_with("patch-snapshots.json"))
    );
    assert!(
        attestation
            .predicate
            .artifact_refs
            .iter()
            .any(|reference| reference.ends_with("validation-records.json"))
    );
}

#[test]
fn orchestrator_records_benchmark_summary_metrics() {
    let root = temp_dir("orchestrator-benchmark-evidence");
    let candidate = root.join("candidate");
    fs::create_dir_all(&candidate).expect("candidate dir");
    let summary_path = candidate.join("benchmark-summary.json");
    let progress_path = candidate.join("benchmark-progress.jsonl");
    fs::write(
        &progress_path,
        "{\"event\":\"suite-selected\"}\n{\"event\":\"benchmark-complete\"}\n",
    )
    .expect("progress");
    let summary_json = r#"{
        "benchmark": "swebench",
        "total_instances": 1,
        "host_dependencies": {
            "blocking_issue_count": 0
        },
        "sinomo": {
            "resolved_instances": 1,
            "error_instances": 0,
            "official_completed_instances": 1,
            "official_empty_patch_instances": 0,
            "patch_bytes": 591,
            "changed_files": 1,
            "initial_post_patch_check_failures": 1,
            "post_patch_check_failures": 0,
            "repair_attempts": 1,
            "post_patch_repair_attempts": 1,
            "empty_patch_repair_attempts": 0,
            "repair_model_turns": 2,
            "repair_tool_results": 1,
            "deterministic_repair_attempts": 1,
            "deterministic_repair_changed_files": 1,
            "agent_model_turns": 3,
            "agent_tool_results": 5,
            "agent_event_count": 8,
            "agent_item_count": 19,
            "agent_tool_call_items": 5,
            "agent_tool_result_items": 5,
            "orphan_tool_calls": 0,
            "orphan_tool_results": 0,
            "agent_input_tokens": 11,
            "agent_output_tokens": 7,
            "agent_model_response_count": 4,
            "agent_model_response_parse_failures": 1,
            "agent_strict_json_decision_turns": 2,
            "agent_text_fallback_turns": 1,
            "agent_native_tool_call_turns": 1,
            "agent_native_tool_calls": 2,
            "agent_vendor_fallback_tool_call_turns": 1,
            "agent_vendor_fallback_tool_calls": 1,
            "agent_model_repair_attempts": 1,
            "agent_repair_request_model_visible_bytes": 128,
            "instances_with_successful_patch": 1,
            "first_successful_patch_turn_sum": 2,
            "first_successful_patch_turn_min": 2,
            "first_successful_patch_turn_max": 2,
            "command_observation_turns": 2,
            "command_tool_results": 3,
            "cancelled_tool_results": 1,
            "cancelled_runs": 1,
            "extra_turns_after_patch": 4,
            "coding_validation_attempts": 2,
            "coding_validation_status_counts": {
                "failed_environment": 1
            },
            "coding_stop_recommendation_counts": {
                "finish_with_patch": 1
            },
            "coding_validation_failure_kind_counts": {
                "missing_dependency": 1,
                "patch_relevant": 1
            }
        }
    }"#
    .to_string();
    fs::write(&summary_path, summary_json).expect("benchmark summary");
    let benchmark_command = "exit 0".to_string();
    let orchestrator = EvolutionOrchestrator::new(
        FakeWorkspace {
            root: root.to_path_buf(),
            candidate: candidate.clone(),
        },
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(root.join("jobs")),
    );
    let model = FakeAgentArtifactMutationModel;

    let report = orchestrator
        .propose(
            EvolutionRequest {
                objective: "write benchmark evidence".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
                verification_commands: vec![VerificationCommand {
                    name: "success".to_string(),
                    command: "exit 0".to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }],
                benchmark_gates: vec![BenchmarkGateRequest {
                    name: "swebench-smoke".to_string(),
                    expected_exit_code: 0,
                    kind: BenchmarkGateKind::Command {
                        command: benchmark_command,
                        summary_path: Some(summary_path.display().to_string()),
                        results_dir: Some(candidate.display().to_string()),
                        progress_path: Some(progress_path.display().to_string()),
                    },
                }],
                module_replay_reports: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: false,
            },
            Some(&model),
        )
        .expect("propose");
    let benchmark_results_path = report
        .bundle
        .benchmark_results_path
        .as_ref()
        .map(PathBuf::from)
        .expect("benchmark results path");
    let benchmark_reports: Vec<BenchmarkGateReport> = serde_json::from_str(
        &fs::read_to_string(&benchmark_results_path).expect("benchmark results"),
    )
    .expect("benchmark report json");
    let scorecard = report.scorecard.as_ref().expect("scorecard");

    assert_eq!(scorecard.outcome, EvaluationOutcome::Passed);
    assert_eq!(benchmark_reports.len(), 1);
    assert_eq!(benchmark_reports[0].outcome, EvaluationOutcome::Passed);
    assert_eq!(
        benchmark_reports[0].summary_verdict.as_deref(),
        Some("SWE-bench resolved 1/1 instance(s)")
    );
    assert_eq!(benchmark_reports[0].progress_event_count, Some(2));
    assert_eq!(
        benchmark_reports[0]
            .progress_last_event
            .as_ref()
            .and_then(|event| event.get("event"))
            .and_then(|event| event.as_str()),
        Some("benchmark-complete")
    );
    assert!(benchmark_reports[0].command_log_path.is_some());
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_total_instances" && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_input_tokens" && metric.value == MetricValue::Integer(11)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_model_response_parse_failures"
            && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_native_tool_call_turns" && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_event_count" && metric.value == MetricValue::Integer(8)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_item_count" && metric.value == MetricValue::Integer(19)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_tool_call_items" && metric.value == MetricValue::Integer(5)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_tool_result_items" && metric.value == MetricValue::Integer(5)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_orphan_tool_calls" && metric.value == MetricValue::Integer(0)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_orphan_tool_results" && metric.value == MetricValue::Integer(0)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_vendor_fallback_tool_call_turns"
            && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_model_repair_attempts" && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_instances_with_successful_patch"
            && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_first_successful_patch_turn_min"
            && metric.value == MetricValue::Integer(2)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_coding_validation_attempts"
            && metric.value == MetricValue::Integer(2)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_coding_finish_with_patch_instances"
            && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_coding_failed_environment_instances"
            && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_coding_missing_dependency_failures"
            && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_command_observation_turns"
            && metric.value == MetricValue::Integer(2)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_command_tool_results" && metric.value == MetricValue::Integer(3)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_cancelled_tool_results" && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_cancelled_runs" && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_extra_turns_after_patch" && metric.value == MetricValue::Integer(4)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_host_dependency_blocking_issues"
            && metric.value == MetricValue::Integer(0)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_official_completed_instances"
            && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_official_empty_patch_instances"
            && metric.value == MetricValue::Integer(0)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_patch_bytes" && metric.value == MetricValue::Integer(591)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_post_patch_check_failures"
            && metric.value == MetricValue::Integer(0)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_initial_post_patch_check_failures"
            && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_repair_attempts" && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_post_patch_repair_attempts"
            && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_empty_patch_repair_attempts"
            && metric.value == MetricValue::Integer(0)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_repair_model_turns" && metric.value == MetricValue::Integer(2)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_repair_tool_results" && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_deterministic_repair_attempts"
            && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_deterministic_repair_changed_files"
            && metric.value == MetricValue::Integer(1)
    }));
    assert!(benchmark_results_path.exists());
}

#[test]
fn orchestrator_marks_host_blocked_swebench_summary_inconclusive() {
    let root = temp_dir("orchestrator-benchmark-host-blocked");
    let candidate = root.join("candidate");
    fs::create_dir_all(&candidate).expect("candidate dir");
    let summary_path = candidate.join("benchmark-summary.json");
    let summary_json = serde_json::json!({
        "benchmark": "swebench",
        "total_instances": 1,
        "harness_status": "host-dependency-blocked",
        "host_dependencies": {
            "blocking_issue_count": 1,
            "blocking_issues": [
                "mini-extra CLI is required for mini-SWE-agent baseline mode"
            ]
        }
    })
    .to_string();
    fs::write(&summary_path, summary_json).expect("benchmark summary");
    let benchmark_command = "exit 1".to_string();
    let orchestrator = EvolutionOrchestrator::new(
        FakeWorkspace {
            root: root.to_path_buf(),
            candidate: candidate.clone(),
        },
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(root.join("jobs")),
    );
    let model = FakeAgentArtifactMutationModel;

    let report = orchestrator
        .propose(
            EvolutionRequest {
                objective: "record host-blocked benchmark evidence".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
                verification_commands: vec![VerificationCommand {
                    name: "success".to_string(),
                    command: "exit 0".to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }],
                benchmark_gates: vec![BenchmarkGateRequest {
                    name: "swebench-host-blocked".to_string(),
                    expected_exit_code: 0,
                    kind: BenchmarkGateKind::Command {
                        command: benchmark_command,
                        summary_path: Some(summary_path.display().to_string()),
                        results_dir: Some(candidate.display().to_string()),
                        progress_path: None,
                    },
                }],
                module_replay_reports: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: false,
            },
            Some(&model),
        )
        .expect("propose");
    let benchmark_results_path = report
        .bundle
        .benchmark_results_path
        .as_ref()
        .map(PathBuf::from)
        .expect("benchmark results path");
    let benchmark_reports: Vec<BenchmarkGateReport> = serde_json::from_str(
        &fs::read_to_string(&benchmark_results_path).expect("benchmark results"),
    )
    .expect("benchmark report json");
    let scorecard = report.scorecard.as_ref().expect("scorecard");

    assert_eq!(scorecard.outcome, EvaluationOutcome::Inconclusive);
    assert_eq!(benchmark_reports.len(), 1);
    assert_eq!(benchmark_reports[0].exit_code, Some(1));
    assert_eq!(
        benchmark_reports[0].outcome,
        EvaluationOutcome::Inconclusive
    );
    assert_eq!(
        benchmark_reports[0].summary_verdict.as_deref(),
        Some("SWE-bench host dependencies blocked 1 issue(s)")
    );
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_host_dependency_blocking_issues"
            && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.gates.iter().any(|gate| {
        gate.gate == EvaluationGate::PrivateRegressionEvals
            && gate.outcome == EvaluationOutcome::Inconclusive
            && gate.summary.contains("host dependencies blocked")
    }));
}

#[test]
fn orchestrator_marks_harness_incomplete_swebench_summary_inconclusive() {
    let root = temp_dir("orchestrator-benchmark-harness-incomplete");
    let candidate = root.join("candidate");
    fs::create_dir_all(&candidate).expect("candidate dir");
    let summary_path = candidate.join("benchmark-summary.json");
    let summary_json = serde_json::json!({
        "benchmark": "swebench",
        "total_instances": 2,
        "sinomo": {
            "harness_blocked_instances": 1,
            "missing_report_instances": 1,
            "error_instances": 2
        }
    })
    .to_string();
    fs::write(&summary_path, summary_json).expect("benchmark summary");
    let benchmark_command = "exit 0".to_string();
    let orchestrator = EvolutionOrchestrator::new(
        FakeWorkspace {
            root: root.to_path_buf(),
            candidate: candidate.clone(),
        },
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(root.join("jobs")),
    );
    let model = FakeAgentArtifactMutationModel;

    let report = orchestrator
        .propose(
            EvolutionRequest {
                objective: "record incomplete benchmark harness evidence".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
                verification_commands: vec![VerificationCommand {
                    name: "success".to_string(),
                    command: "exit 0".to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }],
                benchmark_gates: vec![BenchmarkGateRequest {
                    name: "swebench-harness-incomplete".to_string(),
                    expected_exit_code: 0,
                    kind: BenchmarkGateKind::Command {
                        command: benchmark_command,
                        summary_path: Some(summary_path.display().to_string()),
                        results_dir: Some(candidate.display().to_string()),
                        progress_path: None,
                    },
                }],
                module_replay_reports: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: false,
            },
            Some(&model),
        )
        .expect("propose");
    let benchmark_results_path = report
        .bundle
        .benchmark_results_path
        .as_ref()
        .map(PathBuf::from)
        .expect("benchmark results path");
    let benchmark_reports: Vec<BenchmarkGateReport> = serde_json::from_str(
        &fs::read_to_string(&benchmark_results_path).expect("benchmark results"),
    )
    .expect("benchmark report json");
    let scorecard = report.scorecard.as_ref().expect("scorecard");

    assert_eq!(scorecard.outcome, EvaluationOutcome::Inconclusive);
    assert_eq!(benchmark_reports.len(), 1);
    assert_eq!(
        benchmark_reports[0].outcome,
        EvaluationOutcome::Inconclusive
    );
    assert_eq!(
        benchmark_reports[0].summary_verdict.as_deref(),
        Some(
            "SWE-bench harness evidence incomplete: 1 harness-blocked instance(s), 1 missing report instance(s)"
        )
    );
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_harness_blocked_instances"
            && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "swebench_missing_report_instances"
            && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.gates.iter().any(|gate| {
        gate.gate == EvaluationGate::PrivateRegressionEvals
            && gate.outcome == EvaluationOutcome::Inconclusive
            && gate.summary.contains("harness evidence incomplete")
    }));
}

#[test]
fn orchestrator_fails_swebench_summary_with_orphan_tool_pairs() {
    let root = temp_dir("orchestrator-benchmark-orphan-tool-pairs");
    let candidate = root.join("candidate");
    fs::create_dir_all(&candidate).expect("candidate dir");
    let summary_path = candidate.join("benchmark-summary.json");
    let summary_json = serde_json::json!({
        "benchmark": "swebench",
        "total_instances": 1,
        "sinomo": {
            "resolved_instances": 1,
            "error_instances": 0,
            "orphan_tool_calls": 1,
            "orphan_tool_results": 2
        }
    })
    .to_string();
    fs::write(&summary_path, summary_json).expect("benchmark summary");
    let benchmark_command = "exit 0".to_string();
    let orchestrator = EvolutionOrchestrator::new(
        FakeWorkspace {
            root: root.to_path_buf(),
            candidate: candidate.clone(),
        },
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(root.join("jobs")),
    );
    let model = FakeAgentArtifactMutationModel;

    let report = orchestrator
        .propose(
            EvolutionRequest {
                objective: "record orphan tool-pair benchmark evidence".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
                verification_commands: vec![VerificationCommand {
                    name: "success".to_string(),
                    command: "exit 0".to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }],
                benchmark_gates: vec![BenchmarkGateRequest {
                    name: "swebench-orphan-tool-pairs".to_string(),
                    expected_exit_code: 0,
                    kind: BenchmarkGateKind::Command {
                        command: benchmark_command,
                        summary_path: Some(summary_path.display().to_string()),
                        results_dir: Some(candidate.display().to_string()),
                        progress_path: None,
                    },
                }],
                module_replay_reports: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: false,
            },
            Some(&model),
        )
        .expect("propose");
    let benchmark_results_path = report
        .bundle
        .benchmark_results_path
        .as_ref()
        .map(PathBuf::from)
        .expect("benchmark results path");
    let benchmark_reports: Vec<BenchmarkGateReport> = serde_json::from_str(
        &fs::read_to_string(&benchmark_results_path).expect("benchmark results"),
    )
    .expect("benchmark report json");
    let scorecard = report.scorecard.as_ref().expect("scorecard");

    assert_eq!(scorecard.outcome, EvaluationOutcome::Failed);
    assert_eq!(benchmark_reports.len(), 1);
    assert_eq!(benchmark_reports[0].outcome, EvaluationOutcome::Failed);
    assert_eq!(
        benchmark_reports[0].summary_verdict.as_deref(),
        Some("SWE-bench reported 1 orphan tool call(s) and 2 orphan tool result(s)")
    );
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_orphan_tool_calls" && metric.value == MetricValue::Integer(1)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_orphan_tool_results" && metric.value == MetricValue::Integer(2)
    }));
    assert!(scorecard.gates.iter().any(|gate| {
        gate.gate == EvaluationGate::PrivateRegressionEvals
            && gate.outcome == EvaluationOutcome::Failed
            && gate.summary.contains("orphan tool call")
    }));
}

#[test]
fn orchestrator_fails_swebench_summary_with_tool_pair_count_mismatch() {
    let root = temp_dir("orchestrator-benchmark-tool-pair-mismatch");
    let candidate = root.join("candidate");
    fs::create_dir_all(&candidate).expect("candidate dir");
    let summary_path = candidate.join("benchmark-summary.json");
    let summary_json = serde_json::json!({
        "benchmark": "swebench",
        "total_instances": 1,
        "sinomo": {
            "resolved_instances": 1,
            "error_instances": 0,
            "agent_tool_call_items": 3,
            "agent_tool_result_items": 2,
            "orphan_tool_calls": 0,
            "orphan_tool_results": 0
        }
    })
    .to_string();
    fs::write(&summary_path, summary_json).expect("benchmark summary");
    let benchmark_command = "exit 0".to_string();
    let orchestrator = EvolutionOrchestrator::new(
        FakeWorkspace {
            root: root.to_path_buf(),
            candidate: candidate.clone(),
        },
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(root.join("jobs")),
    );
    let model = FakeAgentArtifactMutationModel;

    let report = orchestrator
        .propose(
            EvolutionRequest {
                objective: "record tool-pair mismatch benchmark evidence".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
                verification_commands: vec![VerificationCommand {
                    name: "success".to_string(),
                    command: "exit 0".to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }],
                benchmark_gates: vec![BenchmarkGateRequest {
                    name: "swebench-tool-pair-mismatch".to_string(),
                    expected_exit_code: 0,
                    kind: BenchmarkGateKind::Command {
                        command: benchmark_command,
                        summary_path: Some(summary_path.display().to_string()),
                        results_dir: Some(candidate.display().to_string()),
                        progress_path: None,
                    },
                }],
                module_replay_reports: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: false,
            },
            Some(&model),
        )
        .expect("propose");
    let benchmark_results_path = report
        .bundle
        .benchmark_results_path
        .as_ref()
        .map(PathBuf::from)
        .expect("benchmark results path");
    let benchmark_reports: Vec<BenchmarkGateReport> = serde_json::from_str(
        &fs::read_to_string(&benchmark_results_path).expect("benchmark results"),
    )
    .expect("benchmark report json");
    let scorecard = report.scorecard.as_ref().expect("scorecard");

    assert_eq!(scorecard.outcome, EvaluationOutcome::Failed);
    assert_eq!(benchmark_reports.len(), 1);
    assert_eq!(benchmark_reports[0].outcome, EvaluationOutcome::Failed);
    assert_eq!(
        benchmark_reports[0].summary_verdict.as_deref(),
        Some("SWE-bench reported tool call/result item mismatch: 3 call item(s), 2 result item(s)")
    );
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_tool_call_items" && metric.value == MetricValue::Integer(3)
    }));
    assert!(scorecard.metrics.iter().any(|metric| {
        metric.name == "agent_tool_result_items" && metric.value == MetricValue::Integer(2)
    }));
    assert!(scorecard.gates.iter().any(|gate| {
        gate.gate == EvaluationGate::PrivateRegressionEvals
            && gate.outcome == EvaluationOutcome::Failed
            && gate.summary.contains("tool call/result item mismatch")
    }));
}

#[test]
fn orchestrator_fails_unresolved_swebench_summary() {
    let root = temp_dir("orchestrator-benchmark-unresolved");
    let candidate = root.join("candidate");
    fs::create_dir_all(&candidate).expect("candidate dir");
    let summary_path = candidate.join("benchmark-summary.json");
    let summary_json = serde_json::json!({
        "benchmark": "swebench",
        "total_instances": 1,
        "sinomo": {
            "resolved_instances": 0,
            "error_instances": 0
        }
    })
    .to_string();
    fs::write(&summary_path, summary_json).expect("benchmark summary");
    let benchmark_command = "exit 0".to_string();
    let orchestrator = EvolutionOrchestrator::new(
        FakeWorkspace {
            root: root.to_path_buf(),
            candidate: candidate.clone(),
        },
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(root.join("jobs")),
    );
    let model = FakeAgentArtifactMutationModel;

    let report = orchestrator
        .propose(
            EvolutionRequest {
                objective: "write benchmark evidence".to_string(),
                target_ring: TrustRing::R1TrustedRuntime,
                intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
                verification_commands: vec![VerificationCommand {
                    name: "success".to_string(),
                    command: "exit 0".to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }],
                benchmark_gates: vec![BenchmarkGateRequest {
                    name: "swebench-smoke".to_string(),
                    expected_exit_code: 0,
                    kind: BenchmarkGateKind::Command {
                        command: benchmark_command,
                        summary_path: Some(summary_path.display().to_string()),
                        results_dir: Some(candidate.display().to_string()),
                        progress_path: None,
                    },
                }],
                module_replay_reports: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: false,
            },
            Some(&model),
        )
        .expect("propose");
    let benchmark_results_path = report
        .bundle
        .benchmark_results_path
        .as_ref()
        .map(PathBuf::from)
        .expect("benchmark results path");
    let benchmark_reports: Vec<BenchmarkGateReport> = serde_json::from_str(
        &fs::read_to_string(&benchmark_results_path).expect("benchmark results"),
    )
    .expect("benchmark report json");
    let scorecard = report.scorecard.as_ref().expect("scorecard");

    assert_eq!(scorecard.outcome, EvaluationOutcome::Failed);
    assert_eq!(benchmark_reports.len(), 1);
    assert_eq!(benchmark_reports[0].outcome, EvaluationOutcome::Failed);
    assert_eq!(
        benchmark_reports[0].summary_verdict.as_deref(),
        Some("SWE-bench resolved 0/1 instance(s)")
    );
    assert!(scorecard.gates.iter().any(|gate| {
        gate.gate == EvaluationGate::PrivateRegressionEvals
            && gate.outcome == EvaluationOutcome::Failed
            && gate.summary.contains("resolved 0/1")
    }));
}

#[test]
fn swebench_summary_verdict_fails_post_patch_check_failures() {
    let summary = serde_json::json!({
        "benchmark": "swebench",
        "total_instances": 1,
        "sinomo": {
            "resolved_instances": 1,
            "error_instances": 0,
            "post_patch_check_failures": 1
        }
    });

    let verdict = benchmark_summary_verdict(&summary).expect("verdict");

    assert_eq!(verdict.outcome, EvaluationOutcome::Failed);
    assert!(verdict.summary.contains("1 post-patch check failure"));
}

#[test]
fn orchestrator_writes_reusable_regression_evidence_and_lesson_for_failed_candidate() {
    let root = temp_dir("orchestrator-regression-evidence");
    let candidate = root.join("candidate");
    fs::create_dir_all(&candidate).expect("candidate dir");
    let orchestrator = EvolutionOrchestrator::new(
        FakeWorkspace {
            root: root.to_path_buf(),
            candidate: candidate.clone(),
        },
        ExecutingEvalHarness::new(LocalCommandRunner::new()),
        LocalCandidateBundleStore::new(root.join("jobs")),
    );

    let report = orchestrator
        .propose(
            EvolutionRequest {
                objective: "capture failed regression evidence".to_string(),
                target_ring: TrustRing::R2TrustedService,
                intended_paths: vec!["modules/evolution/sinomo-evolution/**".to_string()],
                verification_commands: vec![VerificationCommand {
                    name: "failing-static-check".to_string(),
                    command: failing_verification_command().to_string(),
                    expected_exit_code: 0,
                    gate: EvaluationGate::StaticChecks,
                }],
                benchmark_gates: Vec::new(),
                module_replay_reports: Vec::new(),
                model_provider: None,
                model: None,
                dry_run: true,
            },
            None,
        )
        .expect("failed candidate still records evidence");
    let bundle = PathBuf::from(&report.bundle.bundle_path);
    let evidence_path = bundle.join(CANDIDATE_REGRESSION_EVIDENCE_RELATIVE_PATH);
    let lesson_path = bundle.join(CANDIDATE_EVOLUTION_LESSON_RELATIVE_PATH);
    let summary_path = bundle.join(CANDIDATE_EVIDENCE_SUMMARY_RELATIVE_PATH);
    assert!(evidence_path.exists());
    assert!(lesson_path.exists());
    assert!(summary_path.exists());

    let evidence: CandidateRegressionEvidenceSummary =
        serde_json::from_str(&fs::read_to_string(&evidence_path).expect("evidence json"))
            .expect("regression evidence");
    assert_eq!(evidence.outcome, EvaluationOutcome::Failed);
    assert_eq!(evidence.failed_commands.len(), 1);
    assert_eq!(evidence.failed_commands[0].exit_code, 7);
    assert!(
        evidence.failed_commands[0]
            .stderr_excerpt
            .contains("failure-details")
    );
    assert!(
        evidence
            .replay_minimization
            .candidate_gate_commands
            .iter()
            .any(|command| command.contains("exit 7"))
    );

    let lesson: CandidateEvolutionLessonRecord =
        serde_json::from_str(&fs::read_to_string(&lesson_path).expect("lesson json"))
            .expect("lesson");
    assert_eq!(
        lesson.failure_class,
        EvolutionFailureClass::BuildTestFailure
    );
    assert!(lesson.root_cause_summary.contains("verification command"));
    assert!(
        lesson
            .suggested_follow_up_tasks
            .iter()
            .any(|task| task.contains("focused build"))
    );

    let content_manifest: CandidateBundleContentManifest = serde_json::from_str(
        &fs::read_to_string(bundle.join("bundle-content-manifest.json")).unwrap(),
    )
    .expect("content manifest");
    assert!(content_manifest.files.iter().any(|file| {
        file.path == CANDIDATE_REGRESSION_EVIDENCE_RELATIVE_PATH
            && file.digest.starts_with("blake3:")
    }));
    assert!(content_manifest.files.iter().any(|file| {
        file.path == CANDIDATE_EVOLUTION_LESSON_RELATIVE_PATH && file.digest.starts_with("blake3:")
    }));
    assert!(
        fs::read_to_string(summary_path)
            .expect("summary")
            .contains("Replay Minimization")
    );
}

#[test]
fn regression_evidence_summary_preserves_trace_fixture_refs_for_future_gates() {
    let job_id = EvolutionJobId::generate();
    let job = EvolutionJob {
        job_id: job_id.clone(),
        objective: "reuse replay evidence".to_string(),
        target_ring: TrustRing::R3SandboxedModule,
        target_module_id: None,
        edit_scope: EditScope {
            workspace_root: "/workspace/sinomo".to_string(),
            intended_paths: vec!["modules/trigger/**".to_string()],
            notes: String::new(),
        },
        verification: VerificationPlan::default(),
        recovery: RecoveryPlan::default(),
        scope: EvolutionScope {
            allowed_paths: vec!["modules/trigger/**".to_string()],
            forbidden_paths: Vec::new(),
            workspace_id: None,
        },
        status: EvolutionJobStatus::Evaluated,
        required_gates: vec![EvaluationGate::ReplayTests],
        rollback_policy: RollbackPolicy::default(),
    };
    let bundle = CandidateBundleManifest {
        job_id: job_id.clone(),
        bundle_path: "/tmp/bundle".to_string(),
        created_at: OffsetDateTime::now_utc(),
        mutation_packet_path: "/tmp/bundle/mutation-packet.json".to_string(),
        job_path: None,
        model_output_path: None,
        agent_run_path: None,
        agent_summary_path: None,
        benchmark_results_path: None,
        resource_usage_path: None,
        content_manifest_path: None,
        attestation_path: None,
        registry_record_path: None,
        bundle_content_hash: None,
        changed_files_path: Some("/tmp/bundle/changed-files.json".to_string()),
        diff_path: Some("/tmp/bundle/diff.patch".to_string()),
        scorecard_path: Some("/tmp/bundle/scorecard.json".to_string()),
        candidate_summary_path: None,
        decision_path: None,
        artifact_refs: Vec::new(),
        command_logs: Vec::new(),
    };
    let request = EvolutionRequest {
        objective: job.objective.clone(),
        target_ring: job.target_ring.clone(),
        intended_paths: job.edit_scope.intended_paths.clone(),
        verification_commands: Vec::new(),
        benchmark_gates: Vec::new(),
        module_replay_reports: vec![ModuleReplayReportRef {
            name: "trigger-timeout-replay".to_string(),
            report_path: "replays/trigger-timeout.json".to_string(),
            baseline_artifact_id: None,
            candidate_artifact_id: None,
        }],
        model_provider: None,
        model: None,
        dry_run: false,
    };
    let scorecard = EvaluationScorecard {
        scorecard_id: ScorecardId::generate(),
        baseline_artifact_id: None,
        candidate_artifact_id: None,
        outcome: EvaluationOutcome::Failed,
        gates: vec![sinomo_runtime::core::EvaluationGateResult {
            gate: EvaluationGate::ReplayTests,
            outcome: EvaluationOutcome::Failed,
            summary: "module replay trigger-timeout failed".to_string(),
        }],
        metrics: Vec::new(),
    };

    let evidence = candidate_regression_evidence_summary(
        &job,
        &bundle,
        &request,
        &scorecard,
        &["modules/trigger/src/lib.rs".to_string()],
        &[],
        &[],
    );

    assert_eq!(evidence.trace_fixtures.len(), 1);
    assert_eq!(
        evidence.trace_fixtures[0].report_path,
        "replays/trigger-timeout.json"
    );
    assert!(
        evidence
            .replay_minimization
            .retained_evidence_refs
            .contains(&"replays/trigger-timeout.json".to_string())
    );
    assert!(
        evidence
            .reusable_gate_suggestions
            .iter()
            .any(|suggestion| suggestion.contains("ReplayTests"))
    );
}

#[test]
fn regression_evidence_summary_turns_rollout_telemetry_windows_into_trace_fixtures() {
    let job_id = EvolutionJobId::generate();
    let job = EvolutionJob {
        job_id: job_id.clone(),
        objective: "mine rollback evidence".to_string(),
        target_ring: TrustRing::R3SandboxedModule,
        target_module_id: None,
        edit_scope: EditScope {
            workspace_root: "/workspace/sinomo".to_string(),
            intended_paths: vec!["crates/sinomo-rollout/**".to_string()],
            notes: String::new(),
        },
        verification: VerificationPlan::default(),
        recovery: RecoveryPlan::default(),
        scope: EvolutionScope {
            allowed_paths: vec!["crates/sinomo-rollout/**".to_string()],
            forbidden_paths: Vec::new(),
            workspace_id: None,
        },
        status: EvolutionJobStatus::Evaluated,
        required_gates: vec![EvaluationGate::ReplayTests],
        rollback_policy: RollbackPolicy::default(),
    };
    let bundle = CandidateBundleManifest {
        job_id: job_id.clone(),
        bundle_path: "/tmp/bundle".to_string(),
        created_at: OffsetDateTime::now_utc(),
        mutation_packet_path: "/tmp/bundle/mutation-packet.json".to_string(),
        job_path: None,
        model_output_path: None,
        agent_run_path: None,
        agent_summary_path: None,
        benchmark_results_path: None,
        resource_usage_path: None,
        content_manifest_path: None,
        attestation_path: None,
        registry_record_path: None,
        bundle_content_hash: None,
        changed_files_path: Some("/tmp/bundle/changed-files.json".to_string()),
        diff_path: Some("/tmp/bundle/diff.patch".to_string()),
        scorecard_path: Some("/tmp/bundle/scorecard.json".to_string()),
        candidate_summary_path: None,
        decision_path: None,
        artifact_refs: Vec::new(),
        command_logs: Vec::new(),
    };
    let telemetry_window = "rollout-telemetry://window?after=0&through=42&limit=1000";
    let request = EvolutionRequest {
        objective: format!(
            "Rollout guard handoff\nRollback evidence refs: event://evt_1; rollout-telemetry://active-rollback-trigger/timeout; {telemetry_window}"
        ),
        target_ring: job.target_ring.clone(),
        intended_paths: job.edit_scope.intended_paths.clone(),
        verification_commands: Vec::new(),
        benchmark_gates: Vec::new(),
        module_replay_reports: Vec::new(),
        model_provider: None,
        model: None,
        dry_run: false,
    };
    let scorecard = EvaluationScorecard {
        scorecard_id: ScorecardId::generate(),
        baseline_artifact_id: None,
        candidate_artifact_id: None,
        outcome: EvaluationOutcome::Failed,
        gates: vec![sinomo_runtime::core::EvaluationGateResult {
            gate: EvaluationGate::ReplayTests,
            outcome: EvaluationOutcome::Failed,
            summary: "rollback telemetry still reproduces timeout".to_string(),
        }],
        metrics: Vec::new(),
    };

    let evidence = candidate_regression_evidence_summary(
        &job,
        &bundle,
        &request,
        &scorecard,
        &["crates/sinomo-rollout/src/controller.rs".to_string()],
        &[],
        &[],
    );

    assert_eq!(evidence.trace_fixtures.len(), 2);
    assert!(evidence.trace_fixtures.iter().any(|fixture| {
        fixture.source == "rollout-telemetry-window" && fixture.report_path == telemetry_window
    }));
    assert!(evidence.trace_fixtures.iter().any(|fixture| {
        fixture.source == "event-log-signal" && fixture.report_path == "event://evt_1"
    }));
    assert!(
        evidence
            .trace_fixtures
            .iter()
            .all(|fixture| fixture.suggested_gate == EvaluationGate::ReplayTests)
    );
    assert!(
        evidence
            .replay_minimization
            .retained_evidence_refs
            .contains(&telemetry_window.to_string())
    );
    assert!(
        evidence
            .replay_minimization
            .minimization_steps
            .iter()
            .any(|step| step.contains("shortest replay trace") && step.contains(telemetry_window))
    );
    let summary = render_candidate_evidence_summary(&evidence);
    assert!(summary.contains("rollout-telemetry-window-1"));
    assert!(summary.contains(telemetry_window));
}

#[test]
fn regression_evidence_summary_preserves_telemetry_and_event_refs_as_trace_fixtures() {
    let job_id = EvolutionJobId::generate();
    let job = EvolutionJob {
        job_id: job_id.clone(),
        objective: "preserve telemetry-backed regression evidence".to_string(),
        target_ring: TrustRing::R3SandboxedModule,
        target_module_id: None,
        edit_scope: EditScope {
            workspace_root: "/workspace/sinomo".to_string(),
            intended_paths: vec!["crates/sinomo-module/**".to_string()],
            notes: String::new(),
        },
        verification: VerificationPlan::default(),
        recovery: RecoveryPlan::default(),
        scope: EvolutionScope {
            allowed_paths: vec!["crates/sinomo-module/**".to_string()],
            forbidden_paths: Vec::new(),
            workspace_id: None,
        },
        status: EvolutionJobStatus::Evaluated,
        required_gates: vec![EvaluationGate::ReplayTests],
        rollback_policy: RollbackPolicy::default(),
    };
    let bundle = CandidateBundleManifest {
        job_id: job_id.clone(),
        bundle_path: "/tmp/bundle".to_string(),
        created_at: OffsetDateTime::now_utc(),
        mutation_packet_path: "/tmp/bundle/mutation-packet.json".to_string(),
        job_path: None,
        model_output_path: None,
        agent_run_path: None,
        agent_summary_path: None,
        benchmark_results_path: None,
        resource_usage_path: None,
        content_manifest_path: None,
        attestation_path: None,
        registry_record_path: None,
        bundle_content_hash: None,
        changed_files_path: Some("/tmp/bundle/changed-files.json".to_string()),
        diff_path: Some("/tmp/bundle/diff.patch".to_string()),
        scorecard_path: Some("/tmp/bundle/scorecard.json".to_string()),
        candidate_summary_path: None,
        decision_path: None,
        artifact_refs: Vec::new(),
        command_logs: Vec::new(),
    };
    let module_ref = "module-telemetry://timeout-window";
    let event_ref = "event://evt_module_timeout";
    let request = EvolutionRequest {
        objective: format!(
            "Task objective: reduce timeout failures\nRequired evidence: {module_ref}; {event_ref}"
        ),
        target_ring: job.target_ring.clone(),
        intended_paths: job.edit_scope.intended_paths.clone(),
        verification_commands: Vec::new(),
        benchmark_gates: Vec::new(),
        module_replay_reports: Vec::new(),
        model_provider: None,
        model: None,
        dry_run: false,
    };
    let scorecard = EvaluationScorecard {
        scorecard_id: ScorecardId::generate(),
        baseline_artifact_id: None,
        candidate_artifact_id: None,
        outcome: EvaluationOutcome::Failed,
        gates: vec![sinomo_runtime::core::EvaluationGateResult {
            gate: EvaluationGate::ReplayTests,
            outcome: EvaluationOutcome::Failed,
            summary: "telemetry-backed replay is not minimized yet".to_string(),
        }],
        metrics: Vec::new(),
    };

    let evidence = candidate_regression_evidence_summary(
        &job,
        &bundle,
        &request,
        &scorecard,
        &["crates/sinomo-module/src/lib.rs".to_string()],
        &[],
        &[],
    );

    assert_eq!(evidence.trace_fixtures.len(), 2);
    assert!(evidence.trace_fixtures.iter().any(|fixture| {
        fixture.source == "module-telemetry-window" && fixture.report_path == module_ref
    }));
    assert!(evidence.trace_fixtures.iter().any(|fixture| {
        fixture.source == "event-log-signal" && fixture.report_path == event_ref
    }));
    assert!(
        evidence
            .replay_minimization
            .retained_evidence_refs
            .contains(&module_ref.to_string())
    );
    assert!(
        evidence
            .replay_minimization
            .retained_evidence_refs
            .contains(&event_ref.to_string())
    );
    assert!(
        evidence
            .reusable_gate_suggestions
            .iter()
            .any(|suggestion| suggestion.contains(module_ref))
    );
    let summary = render_candidate_evidence_summary(&evidence);
    assert!(summary.contains("module-telemetry-window-1"));
    assert!(summary.contains("event-log-signal-1"));
}

#[test]
fn agent_mutation_worker_binds_local_permission_store_for_host_commands() {
    let root = temp_dir("agent-mutation-worker");
    let data_dir = SinomoDataDir::new(root.join("node-data"));
    set_remote_management(&data_dir, true);
    fs::write(root.join("hello.txt"), "old\n").expect("fixture");
    let job = create_bootstrap_job(
        &EvolutionRequest {
            objective: "replace hello fixture".to_string(),
            target_ring: TrustRing::R2TrustedService,
            intended_paths: vec!["hello.txt".to_string()],
            verification_commands: Vec::new(),
            benchmark_gates: Vec::new(),
            module_replay_reports: Vec::new(),
            model_provider: None,
            model: None,
            dry_run: false,
        },
        &root,
    );
    let workspace = CandidateWorkspace {
        job_id: job.job_id.clone(),
        path: root.to_path_buf(),
        branch_name: "codex/evolve/agent-worker".to_string(),
        source_revision: Some("test".to_string()),
    };
    let bundle_dir = root.join("bundle");
    fs::create_dir_all(&bundle_dir).expect("bundle");
    let bundle = CandidateBundleManifest {
        job_id: job.job_id.clone(),
        bundle_path: bundle_dir.display().to_string(),
        created_at: OffsetDateTime::now_utc(),
        mutation_packet_path: bundle_dir
            .join("mutation-packet.json")
            .display()
            .to_string(),
        job_path: None,
        model_output_path: None,
        agent_run_path: None,
        agent_summary_path: None,
        benchmark_results_path: None,
        resource_usage_path: None,
        content_manifest_path: None,
        attestation_path: None,
        registry_record_path: None,
        bundle_content_hash: None,
        changed_files_path: None,
        diff_path: None,
        scorecard_path: None,
        candidate_summary_path: None,
        decision_path: None,
        artifact_refs: Vec::new(),
        command_logs: Vec::new(),
    };
    let patch = "*** Begin Patch\n*** Update File: hello.txt\n@@\n-old\n+new\n*** End Patch";
    let command = agent_output_command();
    let model = FakeInteractionModel::new([
        serde_json::json!({
            "response_mode": "defer",
            "text": "Applying the requested source edit.",
            "tool_calls": [
                {
                    "tool": "apply_patch",
                    "input": { "patch": patch },
                    "async": false,
                    "summary": "Update hello fixture."
                },
                {
                    "tool": "run_command",
                    "input": { "command": command },
                    "async": false,
                    "summary": "Verify command execution with local consent."
                }
            ],
            "background_handoff": null,
            "next_state": "waiting"
        })
        .to_string(),
        serde_json::json!({
            "response_mode": "answer",
            "text": "Changed hello.txt from old to new. Verification not requested.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        })
        .to_string(),
    ]);
    let worker = AgentMutationWorker::new(model, "fake", "fake")
        .with_local_permission_data_dir(data_dir)
        .with_max_steps(Some(4));

    let result = worker
        .mutate(&mutation_packet_for_job(&job), &workspace, Some(&bundle))
        .expect("agent mutation");

    assert_eq!(
        fs::read_to_string(root.join("hello.txt")).expect("fixture"),
        "new\n"
    );
    assert!(result.agent_run.is_some());
    assert_eq!(
        result
            .agent_summary
            .as_ref()
            .map(|summary| summary.tool_results),
        Some(2)
    );
    let run = result.agent_run.as_ref().expect("serialized agent run");
    let command_outcome = run["tool_results"]
        .as_array()
        .expect("tool results")
        .iter()
        .find(|outcome| outcome["call"]["tool"] == "run_command")
        .expect("run command outcome");
    assert_eq!(
        command_outcome["status"],
        serde_json::Value::String("succeeded".to_string()),
        "unexpected run-command outcome: {command_outcome}"
    );
}

#[test]
fn internal_evolution_tool_projection_only_advertises_backed_recall_tools() {
    let tools = evolution_agent_mutation_tools(true);
    let names = tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();

    assert!(names.contains(&"memory_search"));
    assert!(names.contains(&"memory_read"));
    assert!(names.contains(&"memory_write"));
    assert!(
        !names.contains(&"session_history_search"),
        "the internal mutation harness must not advertise history until a stable host backend is injected"
    );

    let unbacked_tools = evolution_agent_mutation_tools(false);
    let unbacked_names = unbacked_tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    for memory_tool in ["memory_search", "memory_read", "memory_write"] {
        assert!(
            !unbacked_names.contains(&memory_tool),
            "the internal mutation harness must not advertise {memory_tool} without an authoritative data directory"
        );
    }
}

#[test]
fn git_workspace_adapter_places_worktree_inside_job_folder() {
    let source = git_repo("git-workspace-layout-source");
    fs::write(source.join("README.md"), "# source\n").expect("readme");
    run_in(&source, "git add README.md").expect("git add");
    run_in(&source, "git commit -m init").expect("git commit");
    let jobs_root = temp_dir("git-workspace-layout-jobs");
    let job = create_bootstrap_job(
        &EvolutionRequest {
            objective: "layout".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            intended_paths: vec!["README.md".to_string()],
            verification_commands: Vec::new(),
            benchmark_gates: Vec::new(),
            module_replay_reports: Vec::new(),
            model_provider: None,
            model: None,
            dry_run: true,
        },
        &source,
    );
    let adapter = GitWorkspaceAdapter::new(&source, &jobs_root);

    let workspace = adapter.prepare_candidate(&job).expect("candidate");

    assert_eq!(
        workspace.path,
        jobs_root.join(job.job_id.as_str()).join("worktree")
    );
    assert!(workspace.path.exists());
    let _ = run_in(
        &source,
        &format!("git worktree remove --force {}", workspace.path.display()),
    );
}

#[test]
fn git_workspace_adapter_absolutizes_relative_worktree_root() {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos();
    let root = PathBuf::from("target")
        .join("tmp")
        .join(format!("sinomo-evolution-relative-worktree-{stamp}"));
    let source = root.join("source");
    let jobs_root = root.join("data").join("evolution").join("jobs");
    fs::create_dir_all(&source).expect("source dir");
    run_in(&source, "git init").expect("git init");
    run_in(&source, "git config user.email sinomo@example.test").expect("git email");
    run_in(&source, "git config user.name Sinomo").expect("git name");
    fs::write(source.join("README.md"), "# source\n").expect("readme");
    run_in(&source, "git add README.md").expect("git add");
    run_in(&source, "git commit -m init").expect("git commit");
    let job = create_bootstrap_job(
        &EvolutionRequest {
            objective: "layout".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            intended_paths: vec!["README.md".to_string()],
            verification_commands: Vec::new(),
            benchmark_gates: Vec::new(),
            module_replay_reports: Vec::new(),
            model_provider: None,
            model: None,
            dry_run: true,
        },
        &source,
    );
    let adapter = GitWorkspaceAdapter::new(&source, &jobs_root);

    let workspace = adapter.prepare_candidate(&job).expect("candidate");

    assert!(workspace.path.is_absolute());
    assert!(
        workspace
            .path
            .ends_with(jobs_root.join(job.job_id.as_str()).join("worktree"))
    );
    assert!(workspace.path.exists());
    assert!(!source.join(&jobs_root).exists());
    let _ = run_in(
        &source,
        &format!("git worktree remove --force {}", workspace.path.display()),
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn verify_stored_evolution_job_runs_plan_and_records_scorecard() {
    let source = git_repo("verify-stored-job-source");
    fs::write(source.join("README.md"), "# source\n").expect("readme");
    run_in(&source, "git add README.md").expect("git add");
    run_in(&source, "git commit -m init").expect("git commit");
    let data_root = temp_dir("verify-stored-job-data");
    let data_dir = sinomo_runtime::local::SinomoDataDir::new(&data_root);
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let jobs_root = temp_dir("verify-stored-job-jobs");
    let job = create_bootstrap_job(
        &EvolutionRequest {
            objective: "verify stored candidate".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            intended_paths: vec!["README.md".to_string()],
            verification_commands: vec![VerificationCommand {
                name: "verify".to_string(),
                command: "printf verified".to_string(),
                expected_exit_code: 0,
                gate: EvaluationGate::StaticChecks,
            }],
            benchmark_gates: Vec::new(),
            module_replay_reports: Vec::new(),
            model_provider: None,
            model: None,
            dry_run: true,
        },
        &source,
    );
    let workspace = GitWorkspaceAdapter::new(&source, &jobs_root)
        .prepare_candidate(&job)
        .expect("candidate");
    let bundle_dir = paths.job_dir(&job.job_id).join("bundle");
    fs::create_dir_all(&bundle_dir).expect("bundle");
    let record = StoredEvolutionJob {
        job: job.clone(),
        candidate_workspace: workspace.clone(),
        bundle: CandidateBundleManifest {
            job_id: job.job_id.clone(),
            bundle_path: bundle_dir.display().to_string(),
            created_at: OffsetDateTime::now_utc(),
            mutation_packet_path: String::new(),
            job_path: None,
            model_output_path: None,
            agent_run_path: None,
            agent_summary_path: None,
            benchmark_results_path: None,
            resource_usage_path: None,
            content_manifest_path: None,
            attestation_path: None,
            registry_record_path: None,
            bundle_content_hash: None,
            changed_files_path: None,
            diff_path: None,
            scorecard_path: None,
            candidate_summary_path: None,
            decision_path: None,
            artifact_refs: Vec::new(),
            command_logs: Vec::new(),
        },
        verification_outcome: None,
        changed_files: Vec::new(),
        decision: None,
    };
    let harness = ExecutingEvalHarness::new(LocalCommandRunner::new());

    let recorded = verify_stored_evolution_job(&paths, record, &harness).expect("verification");

    assert_eq!(
        recorded.record.verification_outcome,
        Some(EvaluationOutcome::Passed)
    );
    assert!(recorded.scorecard_path.exists());
    assert_eq!(recorded.command_log_paths.len(), 1);
    assert!(
        fs::read_to_string(&recorded.command_log_paths[0])
            .expect("command log")
            .contains("verified")
    );
    let stored = read_stored_job(&paths.job_path(&job.job_id)).expect("stored");
    assert_eq!(stored.verification_outcome, Some(EvaluationOutcome::Passed));
    let _ = run_in(
        &source,
        &format!("git worktree remove --force {}", workspace.path.display()),
    );
}

#[test]
fn decide_stored_evolution_job_records_rejected_decision() {
    let data_root = temp_dir("decide-stored-job-data");
    let data_dir = sinomo_runtime::local::SinomoDataDir::new(&data_root);
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let job = create_bootstrap_job(
        &EvolutionRequest {
            objective: "decide stored candidate".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            intended_paths: vec!["README.md".to_string()],
            verification_commands: Vec::new(),
            benchmark_gates: Vec::new(),
            module_replay_reports: Vec::new(),
            model_provider: None,
            model: None,
            dry_run: true,
        },
        &data_root,
    );
    let bundle_dir = paths.job_dir(&job.job_id).join("bundle");
    fs::create_dir_all(&bundle_dir).expect("bundle");
    let record = StoredEvolutionJob {
        job: job.clone(),
        candidate_workspace: CandidateWorkspace {
            job_id: job.job_id.clone(),
            path: data_root.join("worktree"),
            branch_name: "codex/evolve/decide-stored".to_string(),
            source_revision: None,
        },
        bundle: CandidateBundleManifest {
            job_id: job.job_id.clone(),
            bundle_path: bundle_dir.display().to_string(),
            created_at: OffsetDateTime::now_utc(),
            mutation_packet_path: String::new(),
            job_path: None,
            model_output_path: None,
            agent_run_path: None,
            agent_summary_path: None,
            benchmark_results_path: None,
            resource_usage_path: None,
            content_manifest_path: None,
            attestation_path: None,
            registry_record_path: None,
            bundle_content_hash: None,
            changed_files_path: None,
            diff_path: None,
            scorecard_path: None,
            candidate_summary_path: None,
            decision_path: None,
            artifact_refs: Vec::new(),
            command_logs: Vec::new(),
        },
        verification_outcome: None,
        changed_files: Vec::new(),
        decision: None,
    };

    let recorded = decide_stored_evolution_job(
        &paths,
        record,
        CandidateDecisionOutcome::Rejected,
        "test-reviewer",
        "not ready",
    )
    .expect("decision");

    assert_eq!(recorded.record.job.status, EvolutionJobStatus::Rejected);
    assert!(recorded.decision_path.exists());
    let stored = read_stored_job(&paths.job_path(&job.job_id)).expect("stored");
    assert_eq!(stored.job.status, EvolutionJobStatus::Rejected);
    assert_eq!(stored.decision.expect("decision").note, "not ready");
}

#[test]
fn approve_refuses_failed_preflight_without_override() {
    let (source, _jobs_root, data_root, record) =
        approval_record_fixture("approve-refuses-preflight", EvaluationOutcome::Failed);
    let error = decide_stored_evolution_job(
        &EvolutionPaths::new(&sinomo_runtime::local::SinomoDataDir::new(&data_root)),
        record.clone(),
        CandidateDecisionOutcome::Approved,
        "test-reviewer",
        "",
    )
    .expect_err("approval preflight fails");

    assert!(matches!(error, EvolutionError::ApprovalPreflight { .. }));
    assert!(
        !PathBuf::from(&record.bundle.bundle_path)
            .join("approval-decision.json")
            .exists()
    );
    let _ = run_in(
        &source,
        &format!(
            "git worktree remove --force {}",
            record.candidate_workspace.path.display()
        ),
    );
}

#[test]
fn approve_refuses_tampered_bundle_integrity_without_override() {
    let (source, _jobs_root, data_root, record) =
        approval_record_fixture("approve-refuses-tampered-bundle", EvaluationOutcome::Passed);
    fs::write(
        &record.bundle.mutation_packet_path,
        "{\"objective\":\"tampered after attestation\"}\n",
    )
    .expect("tamper mutation packet");

    let error = decide_stored_evolution_job(
        &EvolutionPaths::new(&sinomo_runtime::local::SinomoDataDir::new(&data_root)),
        record.clone(),
        CandidateDecisionOutcome::Approved,
        "test-reviewer",
        "",
    )
    .expect_err("approval preflight fails");

    assert!(matches!(error, EvolutionError::ApprovalPreflight { .. }));
    let detail = error.to_string();
    assert!(detail.contains("candidate-bundle-integrity"));
    assert!(
        !PathBuf::from(&record.bundle.bundle_path)
            .join("approval-decision.json")
            .exists()
    );
    let _ = run_in(
        &source,
        &format!(
            "git worktree remove --force {}",
            record.candidate_workspace.path.display()
        ),
    );
}

#[test]
fn approve_override_records_preflight_and_candidate_summary() {
    let (source, _jobs_root, data_root, record) =
        approval_record_fixture("approve-override-preflight", EvaluationOutcome::Failed);
    let paths = EvolutionPaths::new(&sinomo_runtime::local::SinomoDataDir::new(&data_root));

    let recorded = decide_stored_evolution_job_with_options(
        &paths,
        record.clone(),
        CandidateDecisionOutcome::Approved,
        "test-reviewer",
        "accepting known verification failure for test",
        CandidateDecisionOptions {
            override_preflight: true,
        },
    )
    .expect("override approval");

    let decision = recorded.record.decision.as_ref().expect("decision");
    let preflight = decision
        .approval_preflight
        .as_ref()
        .expect("approval preflight");
    assert!(preflight.override_used);
    assert!(!preflight.passed);
    assert!(preflight.checks.iter().any(|check| {
        check.name == "verification-passed" && check.outcome == EvaluationOutcome::Failed
    }));
    assert!(preflight.checks.iter().any(|check| {
        check.name == "candidate-bundle-integrity" && check.outcome == EvaluationOutcome::Passed
    }));
    let registry_record_path = recorded
        .record
        .bundle
        .registry_record_path
        .as_ref()
        .map(PathBuf::from)
        .expect("registry record path");
    assert!(registry_record_path.exists());
    let registry_record =
        fs::read_to_string(&registry_record_path).expect("registry record artifact");
    assert!(registry_record.contains("\"content_hash\""));
    let durable_registry_record_path =
        sinomo_runtime::registry::artifact_evidence_record_path(&data_root, &record.job.job_id);
    assert!(durable_registry_record_path.exists());
    assert!(
        fs::read_to_string(&durable_registry_record_path)
            .expect("durable registry record")
            .contains("\"content_hash\"")
    );
    let summary_path = recorded
        .record
        .bundle
        .candidate_summary_path
        .as_ref()
        .map(PathBuf::from)
        .expect("candidate summary path");
    assert!(summary_path.exists());
    let candidate_summary = fs::read_to_string(&summary_path).expect("candidate summary");
    assert!(candidate_summary.contains("candidate-bundle-integrity"));
    assert!(candidate_summary.contains("artifact-evidence-record.json"));
    assert!(candidate_summary.contains("## Agent Run"));
    assert!(candidate_summary.contains("agent-summary.json"));
    assert!(candidate_summary.contains("final_decision_source: strict_json_decision"));
    assert!(
        candidate_summary
            .contains("tool_pair_integrity: calls=1, results=1, orphan_calls=0, orphan_results=0")
    );
    assert!(
        fs::read_to_string(recorded.decision_path)
            .expect("decision")
            .contains("\"approval_preflight\"")
    );
    assert_eq!(
        normalize_newlines(&fs::read_to_string(source.join("README.md")).expect("source readme")),
        "# source\n\napproved\n"
    );
    let _ = run_in(
        &source,
        &format!(
            "git worktree remove --force {}",
            record.candidate_workspace.path.display()
        ),
    );
}

#[test]
fn approve_materializes_verified_rollout_material_from_candidate_bundle() {
    let (source, _jobs_root, data_root, mut record) =
        approval_record_fixture("approve-rollout-material", EvaluationOutcome::Passed);
    let paths = EvolutionPaths::new(&sinomo_runtime::local::SinomoDataDir::new(&data_root));
    let (material, signer) =
        rollout_material_fixture(&data_root, &record.job.job_id, "approval-rollout-signer");
    sinomo_runtime::registry::write_trusted_artifact_signers(
        &data_root,
        std::slice::from_ref(&signer),
    )
    .expect("write trusted signer");
    let material_path = candidate_bundle_release_material_path(&record.bundle);
    fs::write(
        &material_path,
        serde_json::to_string_pretty(&material).expect("material json"),
    )
    .expect("write candidate rollout material");
    finalize_candidate_bundle_integrity(
        &mut record.bundle,
        &record.job,
        &record.candidate_workspace,
        None,
        EvaluationOutcome::Passed,
        &record.changed_files,
    )
    .expect("refresh bundle integrity");

    let recorded = decide_stored_evolution_job(
        &paths,
        record.clone(),
        CandidateDecisionOutcome::Approved,
        "test-reviewer",
        "approve rollout material",
    )
    .expect("approve candidate with rollout material");

    let durable_material_path =
        sinomo_runtime::registry::verified_release_material_path(&data_root, &record.job.job_id);
    assert!(durable_material_path.exists());
    let durable_material =
        sinomo_runtime::registry::read_verified_release_material(&data_root, &record.job.job_id)
            .expect("read durable material")
            .expect("durable material");
    assert_eq!(durable_material.release_id, record.job.job_id);
    assert_eq!(
        durable_material.artifact.artifact_id,
        material.artifact.artifact_id
    );
    assert!(
        durable_material
            .evidence_refs
            .iter()
            .any(|evidence| evidence
                == &format!("registry://artifact-evidence/{}", record.job.job_id))
    );
    let preflight = recorded
        .record
        .decision
        .as_ref()
        .and_then(|decision| decision.approval_preflight.as_ref())
        .expect("approval preflight");
    assert!(preflight.checks.iter().any(|check| {
        check.name == "candidate-verified-rollout-material"
            && check.outcome == EvaluationOutcome::Passed
    }));
    let summary_path = recorded
        .record
        .bundle
        .candidate_summary_path
        .as_ref()
        .map(PathBuf::from)
        .expect("candidate summary");
    assert!(
        fs::read_to_string(summary_path)
            .expect("candidate summary")
            .contains(&durable_material_path.display().to_string())
    );
    let _ = run_in(
        &source,
        &format!(
            "git worktree remove --force {}",
            record.candidate_workspace.path.display()
        ),
    );
}

#[test]
fn approve_override_skips_invalid_rollout_material_materialization() {
    let (source, _jobs_root, data_root, mut record) = approval_record_fixture(
        "approve-invalid-rollout-material",
        EvaluationOutcome::Passed,
    );
    let paths = EvolutionPaths::new(&sinomo_runtime::local::SinomoDataDir::new(&data_root));
    let (material, _signer) =
        rollout_material_fixture(&data_root, &record.job.job_id, "approval-untrusted-signer");
    let material_path = candidate_bundle_release_material_path(&record.bundle);
    fs::write(
        &material_path,
        serde_json::to_string_pretty(&material).expect("material json"),
    )
    .expect("write candidate rollout material");
    finalize_candidate_bundle_integrity(
        &mut record.bundle,
        &record.job,
        &record.candidate_workspace,
        None,
        EvaluationOutcome::Passed,
        &record.changed_files,
    )
    .expect("refresh bundle integrity");

    let recorded = decide_stored_evolution_job_with_options(
        &paths,
        record.clone(),
        CandidateDecisionOutcome::Approved,
        "test-reviewer",
        "approving source candidate but not invalid rollout material",
        CandidateDecisionOptions {
            override_preflight: true,
        },
    )
    .expect("override invalid rollout material");

    let preflight = recorded
        .record
        .decision
        .as_ref()
        .and_then(|decision| decision.approval_preflight.as_ref())
        .expect("approval preflight");
    assert!(preflight.checks.iter().any(|check| {
        check.name == "candidate-verified-rollout-material"
            && check.outcome == EvaluationOutcome::Failed
    }));
    assert!(
        sinomo_runtime::registry::read_verified_release_material(&data_root, &record.job.job_id,)
            .expect("read durable material")
            .is_none()
    );
    let _ = run_in(
        &source,
        &format!(
            "git worktree remove --force {}",
            record.candidate_workspace.path.display()
        ),
    );
}

#[test]
fn approved_candidate_commit_fast_forwards_source_checkout() {
    let source = git_repo("approved-candidate-source");
    fs::write(source.join("README.md"), "# source\n").expect("readme");
    run_in(&source, "git add README.md").expect("git add");
    run_in(&source, "git commit -m init").expect("git commit");
    let jobs_root = temp_dir("approved-candidate-jobs");
    let job = create_bootstrap_job(
        &EvolutionRequest {
            objective: "apply approved candidate".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            intended_paths: vec!["README.md".to_string()],
            verification_commands: Vec::new(),
            benchmark_gates: Vec::new(),
            module_replay_reports: Vec::new(),
            model_provider: None,
            model: None,
            dry_run: true,
        },
        &source,
    );
    let adapter = GitWorkspaceAdapter::new(&source, &jobs_root);
    let workspace = adapter.prepare_candidate(&job).expect("candidate");
    fs::write(workspace.path.join("README.md"), "# source\n\napproved\n").expect("candidate");
    let record = StoredEvolutionJob {
        job: job.clone(),
        candidate_workspace: workspace.clone(),
        bundle: CandidateBundleManifest {
            job_id: job.job_id.clone(),
            bundle_path: jobs_root
                .join(job.job_id.as_str())
                .join("bundle")
                .display()
                .to_string(),
            created_at: OffsetDateTime::now_utc(),
            mutation_packet_path: String::new(),
            job_path: None,
            model_output_path: None,
            agent_run_path: None,
            agent_summary_path: None,
            benchmark_results_path: None,
            resource_usage_path: None,
            content_manifest_path: None,
            attestation_path: None,
            registry_record_path: None,
            bundle_content_hash: None,
            changed_files_path: None,
            diff_path: None,
            scorecard_path: None,
            candidate_summary_path: None,
            decision_path: None,
            artifact_refs: Vec::new(),
            command_logs: Vec::new(),
        },
        verification_outcome: None,
        changed_files: vec!["README.md".to_string()],
        decision: None,
    };

    commit_and_fast_forward_approved_candidate(&record).expect("approved candidate");

    assert_eq!(
        normalize_newlines(&fs::read_to_string(source.join("README.md")).expect("source readme")),
        "# source\n\napproved\n"
    );
    assert_eq!(
        run_git_command(&source, &["log", "-1", "--pretty=%s"]).expect("log"),
        format!("Apply evolution job {}", job.job_id)
    );
    let _ = run_in(
        &source,
        &format!("git worktree remove --force {}", workspace.path.display()),
    );
}

#[test]
fn cleanup_stored_evolution_job_removes_worktree_job_and_bundle_dirs() {
    let source = git_repo("cleanup-candidate-source");
    fs::write(source.join("README.md"), "# source\n").expect("readme");
    run_in(&source, "git add README.md").expect("git add");
    run_in(&source, "git commit -m init").expect("git commit");
    let worktrees_root = temp_dir("cleanup-candidate-worktrees");
    let data_root = temp_dir("cleanup-candidate-data");
    let data_dir = sinomo_runtime::local::SinomoDataDir::new(&data_root);
    let paths = EvolutionPaths::new(&data_dir);
    paths.ensure().expect("paths");
    let job = create_bootstrap_job(
        &EvolutionRequest {
            objective: "cleanup approved candidate".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            intended_paths: vec!["README.md".to_string()],
            verification_commands: Vec::new(),
            benchmark_gates: Vec::new(),
            module_replay_reports: Vec::new(),
            model_provider: None,
            model: None,
            dry_run: true,
        },
        &source,
    );
    let adapter = GitWorkspaceAdapter::new(&source, &worktrees_root);
    let workspace = adapter.prepare_candidate(&job).expect("candidate");
    let job_dir = paths.job_dir(&job.job_id);
    fs::create_dir_all(&job_dir).expect("job dir");
    fs::write(job_dir.join("record.json"), "{}").expect("record");
    let bundle_dir = data_root.join("external-bundle");
    fs::create_dir_all(&bundle_dir).expect("bundle dir");
    fs::write(bundle_dir.join("manifest.json"), "{}").expect("manifest");
    let record = StoredEvolutionJob {
        job: job.clone(),
        candidate_workspace: workspace.clone(),
        bundle: CandidateBundleManifest {
            job_id: job.job_id.clone(),
            bundle_path: bundle_dir.display().to_string(),
            created_at: OffsetDateTime::now_utc(),
            mutation_packet_path: String::new(),
            job_path: None,
            model_output_path: None,
            agent_run_path: None,
            agent_summary_path: None,
            benchmark_results_path: None,
            resource_usage_path: None,
            content_manifest_path: None,
            attestation_path: None,
            registry_record_path: None,
            bundle_content_hash: None,
            changed_files_path: None,
            diff_path: None,
            scorecard_path: None,
            candidate_summary_path: None,
            decision_path: None,
            artifact_refs: Vec::new(),
            command_logs: Vec::new(),
        },
        verification_outcome: None,
        changed_files: Vec::new(),
        decision: None,
    };

    let summary = cleanup_stored_evolution_job(&paths, &record).expect("cleanup");

    assert_eq!(summary.job_id, job.job_id);
    assert_eq!(summary.worktree_path, workspace.path);
    assert_eq!(summary.job_dir, job_dir);
    assert_eq!(summary.bundle_path, Some(bundle_dir.clone()));
    assert!(!summary.worktree_path.exists());
    assert!(!summary.job_dir.exists());
    assert!(!bundle_dir.exists());
}

#[test]
fn bootstrap_tool_registry_exposes_minimal_phase_three_tools() {
    let registry = EvolutionToolRegistry::bootstrap();
    let names = registry
        .descriptors()
        .into_iter()
        .map(|descriptor| descriptor.name)
        .collect::<Vec<_>>();

    for expected in [
        "read_file",
        "search",
        "apply_patch",
        "run_command",
        "interrupt_command",
        "kill_command",
    ] {
        assert!(names.iter().any(|name| name == expected), "{expected}");
    }
}

fn evolution_registry_with_remote_management(root: &Path) -> EvolutionToolRegistry {
    let data_dir = SinomoDataDir::new(root.join("node-data"));
    set_remote_management(&data_dir, true);
    EvolutionToolRegistry::bootstrap_for_data_dir(data_dir)
}

fn set_remote_management(data_dir: &SinomoDataDir, enabled: bool) {
    let store = LocalNodePermissionStore::new(data_dir.root());
    let mut permissions = LocalNodePermissions::default();
    permissions.set_permission(LocalNodePermission::RemoteManagement, enabled);
    store
        .save(&permissions)
        .expect("write remote-management grant");
}

#[test]
fn run_command_tool_fails_closed_without_local_remote_management_permission() {
    let root = temp_dir("run-command-local-permission");
    let workspace = candidate_workspace(&root);
    let registry = EvolutionToolRegistry::bootstrap();
    let descriptor = registry
        .descriptors()
        .into_iter()
        .find(|descriptor| descriptor.name == "run_command")
        .expect("run command descriptor");

    assert_eq!(
        descriptor.required_local_permissions,
        vec![LocalNodePermission::RemoteManagement.as_str().to_string()]
    );
    let result = registry.execute(
        &workspace,
        EvolutionToolCall {
            tool: "run_command".to_string(),
            input: serde_json::json!({ "command": output_command() }),
        },
    );
    assert_eq!(result.status, ToolCallStatus::Failed);
    assert!(result.evidence.summary.contains("node-local permission"));
}

#[test]
fn read_file_tool_returns_content_and_path_evidence() {
    let root = temp_dir("read-file-tool");
    fs::write(root.join("notes.txt"), "hello tools").expect("write note");
    let workspace = candidate_workspace(&root);
    let result = EvolutionToolRegistry::bootstrap().execute(
        &workspace,
        EvolutionToolCall {
            tool: "read_file".to_string(),
            input: serde_json::json!({ "path": "notes.txt" }),
        },
    );

    assert_eq!(result.status, ToolCallStatus::Succeeded);
    assert_eq!(result.output["content"], "hello tools");
    assert_eq!(result.evidence.paths, vec!["notes.txt".to_string()]);
}

#[test]
fn tool_registry_can_emit_execution_event_with_result_and_changed_files() {
    let root = git_repo("tool-event");
    fs::write(root.join("tracked.txt"), "before\n").expect("write tracked");
    run_in(&root, "git add tracked.txt").expect("git add");
    run_in(&root, "git commit -m init").expect("git commit");
    fs::write(root.join("tracked.txt"), "after\n").expect("modify tracked");
    let workspace = candidate_workspace(&root);
    let context = EvolutionToolEventContext::new(
        NodeId::from_legacy_named("node_tool_test").expect("node"),
        workspace.job_id.clone(),
    );

    let (result, event) = EvolutionToolRegistry::bootstrap().execute_with_event(
        &workspace,
        &context,
        EvolutionToolCall {
            tool: "read_file".to_string(),
            input: serde_json::json!({ "path": "tracked.txt" }),
        },
    );

    assert_eq!(result.status, ToolCallStatus::Succeeded);
    assert_eq!(event.kind.as_str(), "evolution.tool.executed");
    assert_eq!(event.payload["tool"], "read_file");
    assert_eq!(event.payload["result"]["status"], "succeeded");
    assert_eq!(
        event.payload["changed_files"],
        serde_json::json!(["tracked.txt"])
    );
    assert_eq!(event.evidence.refs.len(), 1);
}

#[test]
fn run_command_tool_captures_exit_code_stdout_and_stderr() {
    let root = temp_dir("run-command-tool");
    let workspace = candidate_workspace(&root);
    let result = evolution_registry_with_remote_management(&root).execute(
        &workspace,
        EvolutionToolCall {
            tool: "run_command".to_string(),
            input: serde_json::json!({ "command": output_command() }),
        },
    );

    assert_eq!(result.status, ToolCallStatus::Succeeded);
    assert_eq!(result.output["exit_code"], 0);
    assert!(result.output["run_id"].as_str().is_some());
    assert!(
        result.output["stdout"]
            .as_str()
            .unwrap()
            .contains("sinomo-stdout")
    );
    assert!(
        result.output["stderr"]
            .as_str()
            .unwrap()
            .contains("sinomo-stderr")
    );
    assert!(result.evidence.command.is_some());
}

#[test]
fn command_control_tool_reports_unknown_run_id() {
    let root = temp_dir("command-control-tool");
    let workspace = candidate_workspace(&root);
    let result = EvolutionToolRegistry::bootstrap().execute(
        &workspace,
        EvolutionToolCall {
            tool: "interrupt_command".to_string(),
            input: serde_json::json!({ "run_id": "cmd_missing" }),
        },
    );

    assert_eq!(result.status, ToolCallStatus::Failed);
    assert!(
        result.evidence.summary.contains("cmd_missing"),
        "{}",
        result.evidence.summary
    );
}

#[test]
#[cfg(unix)]
fn kill_command_tool_can_terminate_active_run_command() {
    use std::thread;

    let root = temp_dir("kill-command-tool");
    let workspace = candidate_workspace(&root);
    let registry = std::sync::Arc::new(evolution_registry_with_remote_management(&root));
    let running_registry = registry.clone();
    let running_workspace = workspace.clone();
    let handle = thread::spawn(move || {
        running_registry.execute(
            &running_workspace,
            EvolutionToolCall {
                tool: "run_command".to_string(),
                input: serde_json::json!({ "command": "sleep 5" }),
            },
        )
    });
    let run_id = wait_for_active_command(&registry);

    let control = registry.execute(
        &workspace,
        EvolutionToolCall {
            tool: "kill_command".to_string(),
            input: serde_json::json!({ "run_id": run_id }),
        },
    );
    let result = handle.join().expect("run command thread");

    assert_eq!(control.status, ToolCallStatus::Succeeded);
    assert_ne!(result.output["exit_code"], 0);
}

#[cfg(unix)]
fn wait_for_active_command(registry: &EvolutionToolRegistry) -> String {
    for _ in 0..100 {
        if let Some(run_id) = registry.active_command_run_ids().into_iter().next() {
            return run_id;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("expected an active command run");
}

#[test]
fn run_command_can_report_git_status_and_diff_from_candidate_workspace() {
    let root = git_repo("git-tools");
    fs::write(root.join("tracked.txt"), "before\n").expect("write tracked");
    run_in(&root, "git add tracked.txt").expect("git add");
    run_in(&root, "git commit -m init").expect("git commit");
    fs::write(root.join("tracked.txt"), "after\n").expect("modify tracked");
    let workspace = candidate_workspace(&root);
    let registry = evolution_registry_with_remote_management(&root);

    let status = registry.execute(
        &workspace,
        EvolutionToolCall {
            tool: "run_command".to_string(),
            input: serde_json::json!({ "command": "git status --short" }),
        },
    );
    let diff = registry.execute(
        &workspace,
        EvolutionToolCall {
            tool: "run_command".to_string(),
            input: serde_json::json!({ "command": "git diff --binary HEAD --" }),
        },
    );

    assert_eq!(status.status, ToolCallStatus::Succeeded);
    assert!(
        status.output["stdout"]
            .as_str()
            .unwrap()
            .contains("tracked.txt")
    );
    assert_eq!(diff.status, ToolCallStatus::Succeeded);
    assert!(diff.output["stdout"].as_str().unwrap().contains("+after"));
}

#[test]
fn apply_patch_tool_applies_unified_diff_in_candidate_workspace() {
    let root = git_repo("apply-patch-tool");
    fs::write(root.join("patched.txt"), "old\n").expect("write tracked");
    run_in(&root, "git add patched.txt").expect("git add");
    run_in(&root, "git commit -m init").expect("git commit");
    let workspace = candidate_workspace(&root);
    let patch = "\
diff --git a/patched.txt b/patched.txt
index 3b18e51..6320cd2 100644
--- a/patched.txt
+++ b/patched.txt
@@ -1 +1 @@
-old
+new
";
    let result = EvolutionToolRegistry::bootstrap().execute(
        &workspace,
        EvolutionToolCall {
            tool: "apply_patch".to_string(),
            input: serde_json::json!({ "patch": patch }),
        },
    );

    assert_eq!(result.status, ToolCallStatus::Succeeded);
    let patched = fs::read_to_string(root.join("patched.txt")).expect("read patched");
    assert_eq!(patched.replace("\r\n", "\n"), "new\n");
}

#[test]
fn apply_patch_tool_applies_v4a_update_patch_in_candidate_workspace() {
    let root = git_repo("apply-v4a-patch-tool");
    fs::write(root.join("patched.txt"), "# Title\n\nold\n").expect("write tracked");
    run_in(&root, "git add patched.txt").expect("git add");
    run_in(&root, "git commit -m init").expect("git commit");
    let workspace = candidate_workspace(&root);
    let patch = concat!(
        "*** Begin Patch\n",
        "*** Update File: patched.txt\n",
        "@@\n",
        " # Title\n",
        " \n",
        "-old\n",
        "+new\n",
        "*** End Patch\n"
    );
    let result = EvolutionToolRegistry::bootstrap().execute(
        &workspace,
        EvolutionToolCall {
            tool: "apply_patch".to_string(),
            input: serde_json::json!({ "patch": patch }),
        },
    );

    assert_eq!(result.status, ToolCallStatus::Succeeded);
    let patched = fs::read_to_string(root.join("patched.txt")).expect("read patched");
    assert_eq!(patched.replace("\r\n", "\n"), "# Title\n\nnew\n");
}

#[derive(Clone, Debug)]
struct FakeLanguageModelProvider {
    info: LanguageModelProviderInfo,
    responses: Vec<String>,
    requests: Rc<RefCell<Vec<LanguageModelRequest>>>,
}

impl FakeLanguageModelProvider {
    fn new(info: LanguageModelProviderInfo, response: String) -> Self {
        Self::with_responses(info, vec![response])
    }

    fn with_responses(info: LanguageModelProviderInfo, responses: Vec<String>) -> Self {
        Self {
            info,
            responses,
            requests: Rc::new(RefCell::new(Vec::new())),
        }
    }
}

impl LanguageModelProvider for FakeLanguageModelProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        self.info.clone()
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let response_index = self.requests.borrow().len();
        self.requests.borrow_mut().push(request.clone());
        Ok(LanguageModelResponse {
            provider: self.info.provider.clone(),
            model: self.info.default_model.clone(),
            content: self
                .responses
                .get(response_index)
                .or_else(|| self.responses.last())
                .cloned()
                .unwrap_or_default(),
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: Some(LanguageModelUsage {
                input_tokens: Some(10),
                output_tokens: Some(20),
                ..Default::default()
            }),
        })
    }
}

#[derive(Clone, Debug)]
struct RevokingLanguageModelProvider {
    store: LocalNodePermissionStore,
    requests: Rc<RefCell<Vec<LanguageModelRequest>>>,
}

impl LanguageModelProvider for RevokingLanguageModelProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: "fake-revoking-unrestricted".to_string(),
            default_model: "fake-model".to_string(),
            invocation: sinomo_lm::LanguageModelInvocation::Process,
            host_access: LanguageModelHostAccess::Unrestricted,
            capabilities: sinomo_lm::LanguageModelProviderCapabilities::default(),
            supports_json_object: true,
            supports_tool_permissions: false,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.requests.borrow_mut().push(request.clone());
        self.store
            .mutate(|permissions| {
                Ok(permissions.set_permission(LocalNodePermission::RemoteManagement, false))
            })
            .expect("revoke remote-management after first provider call");
        Ok(LanguageModelResponse {
            provider: "fake-revoking-unrestricted".to_string(),
            model: "fake-model".to_string(),
            content: serde_json::json!({
                "tool_calls": [
                    { "tool": "read_file", "input": { "path": "context.txt" } }
                ],
                "changed_files": []
            })
            .to_string(),
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

fn bridge_permission_test_packet(workspace: &CandidateWorkspace) -> MutationPacket {
    MutationPacket {
        job_id: workspace.job_id.clone(),
        objective: "exercise provider permission boundary".to_string(),
        target_ring: TrustRing::R1TrustedRuntime,
        edit_scope: EditScope {
            workspace_root: workspace.path.display().to_string(),
            intended_paths: vec!["context.txt".to_string()],
            notes: String::new(),
        },
        relevant_files: Vec::new(),
        verification: VerificationPlan {
            commands: Vec::new(),
            module_replay_reports: Vec::new(),
        },
        output_contract: MutationOutputContract::default(),
        recovery: RecoveryPlan::default(),
    }
}

#[derive(Clone, Debug)]
struct FakeMutationModel;

impl MutationModel for FakeMutationModel {
    fn mutate(
        &self,
        _packet: &MutationPacket,
        _workspace: &CandidateWorkspace,
        _bundle: Option<&CandidateBundleManifest>,
    ) -> Result<ModelMutationResult, MutationModelError> {
        let tool_result = EvolutionToolResult {
            tool: "read_file".to_string(),
            status: ToolCallStatus::Succeeded,
            output: serde_json::json!({ "content": "evidence" }),
            evidence: ToolEvidence {
                summary: "read evidence".to_string(),
                command: None,
                exit_code: None,
                duration_ms: None,
                paths: vec!["evidence.txt".to_string()],
            },
        };
        Ok(ModelMutationResult {
            provider: "fake".to_string(),
            model: "fake-model".to_string(),
            raw_output: "{\"patch_summary\":\"fake mutation\"}".to_string(),
            parsed_patch: None,
            summary: Some("fake mutation".to_string()),
            structured_output: Some(StructuredMutationOutput {
                patch_summary: Some("fake mutation".to_string()),
                ..StructuredMutationOutput::default()
            }),
            tool_results: vec![tool_result.clone()],
            model_turns: vec![MutationModelTurn {
                turn: 1,
                raw_output: "{\"patch_summary\":\"fake mutation\"}".to_string(),
                structured_output: Some(StructuredMutationOutput {
                    patch_summary: Some("fake mutation".to_string()),
                    ..StructuredMutationOutput::default()
                }),
                tool_results: vec![tool_result],
            }],
            agent_summary: None,
            agent_resource_usage: None,
            agent_run: None,
            agent_events: Vec::new(),
            coding_run_state: None,
            patch_snapshots: Vec::new(),
            validation_records: Vec::new(),
        })
    }
}

#[derive(Clone, Default)]
struct RecordingLifecycleObserver {
    updates: Rc<RefCell<Vec<EvolutionLifecycleUpdate>>>,
}

impl EvolutionLifecycleObserver for RecordingLifecycleObserver {
    fn on_update(&self, update: &EvolutionLifecycleUpdate) -> Result<(), String> {
        self.updates.borrow_mut().push(update.clone());
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct FakeAgentArtifactMutationModel;

impl MutationModel for FakeAgentArtifactMutationModel {
    fn mutate(
        &self,
        _packet: &MutationPacket,
        _workspace: &CandidateWorkspace,
        _bundle: Option<&CandidateBundleManifest>,
    ) -> Result<ModelMutationResult, MutationModelError> {
        Ok(ModelMutationResult {
            provider: "fake".to_string(),
            model: "fake-model".to_string(),
            raw_output: "agent completed".to_string(),
            parsed_patch: None,
            summary: Some("agent completed".to_string()),
            structured_output: None,
            tool_results: Vec::new(),
            model_turns: Vec::new(),
            agent_summary: Some(AgentMutationSummary {
                stop_reason: "final-decision".to_string(),
                model_turns: 2,
                tool_results: 1,
                input_tokens: 5,
                output_tokens: 7,
                model_metrics: Some(serde_json::json!({
                    "model_turn_count": 2,
                    "model_response_count": 2,
                    "strict_json_decision_turn_count": 1,
                    "native_tool_call_turn_count": 1,
                    "final_decision_source": "strict_json_decision",
                })),
                tool_pair_integrity: Some(AgentToolPairIntegrity {
                    tool_calls: 1,
                    tool_results: 1,
                    orphan_tool_calls: 0,
                    orphan_tool_results: 0,
                }),
                final_text: "agent completed".to_string(),
            }),
            agent_resource_usage: Some(agent_resource_usage_summary(
                _packet.job_id.clone(),
                5,
                7,
                2,
                1,
            )),
            agent_run: Some(serde_json::json!({
                "final_decision": {
                    "text": "agent completed"
                },
                "stop_reason": "final-decision"
            })),
            agent_events: vec![serde_json::json!({
                "kind": "agent.loop.completed",
                "payload": {
                    "final_decision_source": "strict_json_decision"
                }
            })],
            coding_run_state: Some(serde_json::json!({
                "patch_status": "applied",
                "validation_attempts": [{
                    "step_index": 1,
                    "call_id": "01hxy6qa9k0t9vr4g9n8d3q5d4",
                    "command": "cargo test -p sinomo-evolution",
                    "status": "passed",
                    "summary": "validation passed",
                    "exit_code": 0,
                    "timed_out": false
                }],
                "validation_status": "passed",
                "stop_recommendation": "finish_with_patch"
            })),
            patch_snapshots: vec![serde_json::json!({
                "PatchSnapshot": {
                    "item_id": "01hxy6qa9k0t9vr4g9n8d3q5d5",
                    "step_index": 0,
                    "call_id": "01hxy6qa9k0t9vr4g9n8d3q5d6",
                    "status": "succeeded",
                    "summary": "patched hello.txt",
                    "patch_ref": "patches/hello.patch",
                    "changed_files": ["hello.txt"]
                }
            })],
            validation_records: vec![serde_json::json!({
                "step_index": 1,
                "call_id": "01hxy6qa9k0t9vr4g9n8d3q5d4",
                "command": "cargo test -p sinomo-evolution",
                "status": "passed",
                "summary": "validation passed",
                "exit_code": 0,
                "timed_out": false
            })],
        })
    }
}

#[derive(Clone, Debug)]
struct FakeInteractionModel {
    responses: Arc<Mutex<Vec<String>>>,
}

impl FakeInteractionModel {
    fn new(responses: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into_iter().map(Into::into).collect())),
        }
    }
}

impl InteractionModel for FakeInteractionModel {
    fn complete(
        &self,
        _request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let mut responses = self.responses.lock().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: "fake".to_string(),
                model: "fake".to_string(),
                detail: format!("fake response queue lock failed: {source}"),
            }
        })?;
        if responses.is_empty() {
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: "fake".to_string(),
                model: "fake".to_string(),
                detail: "no fake response queued".to_string(),
            });
        }
        let response = responses.remove(0);
        Ok(LanguageModelResponse {
            provider: "fake".to_string(),
            model: "fake".to_string(),
            content: response,
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

#[derive(Clone, Debug)]
struct FakeWorkspace {
    root: PathBuf,
    candidate: PathBuf,
}

impl WorkspaceAdapter for FakeWorkspace {
    fn root(&self) -> &Path {
        &self.root
    }

    fn prepare_candidate(&self, job: &EvolutionJob) -> Result<CandidateWorkspace, WorkspaceError> {
        Ok(CandidateWorkspace {
            job_id: job.job_id.clone(),
            path: self.candidate.clone(),
            branch_name: "codex/evolve/test".to_string(),
            source_revision: Some("test".to_string()),
        })
    }

    fn changed_files(
        &self,
        _workspace: &CandidateWorkspace,
    ) -> Result<Vec<String>, WorkspaceError> {
        Ok(Vec::new())
    }

    fn diff(&self, _workspace: &CandidateWorkspace) -> Result<String, WorkspaceError> {
        Ok(String::new())
    }

    fn status(&self, _workspace: &CandidateWorkspace) -> Result<String, WorkspaceError> {
        Ok(String::new())
    }
}

fn temp_dir(name: &str) -> sinomo_testdir::TestDir {
    sinomo_testdir::TestDir::new(&format!("sinomo-evolution-{name}"))
}

fn candidate_workspace(root: &Path) -> CandidateWorkspace {
    CandidateWorkspace {
        job_id: EvolutionJobId::from_legacy_named("evojob_tools").expect("job"),
        path: root.to_path_buf(),
        branch_name: "codex/evolve/tools".to_string(),
        source_revision: Some("test".to_string()),
    }
}

fn approval_record_fixture(
    name: &str,
    verification_outcome: EvaluationOutcome,
) -> (
    sinomo_testdir::TestDir,
    sinomo_testdir::TestDir,
    sinomo_testdir::TestDir,
    StoredEvolutionJob,
) {
    let source = git_repo(&format!("{name}-source"));
    fs::write(source.join("README.md"), "# source\n").expect("readme");
    run_in(&source, "git add README.md").expect("git add");
    run_in(&source, "git commit -m init").expect("git commit");
    let jobs_root = temp_dir(&format!("{name}-jobs"));
    let data_root = temp_dir(&format!("{name}-data"));
    let paths = EvolutionPaths::new(&sinomo_runtime::local::SinomoDataDir::new(&data_root));
    paths.ensure().expect("paths");
    let job = create_bootstrap_job(
        &EvolutionRequest {
            objective: "apply approved candidate".to_string(),
            target_ring: TrustRing::R1TrustedRuntime,
            intended_paths: vec!["README.md".to_string()],
            verification_commands: Vec::new(),
            benchmark_gates: Vec::new(),
            module_replay_reports: Vec::new(),
            model_provider: None,
            model: None,
            dry_run: true,
        },
        &source,
    );
    let adapter = GitWorkspaceAdapter::new(&source, &jobs_root);
    let workspace = adapter.prepare_candidate(&job).expect("candidate");
    fs::write(workspace.path.join("README.md"), "# source\n\napproved\n").expect("candidate");
    let bundle_dir = paths.job_dir(&job.job_id).join("bundle");
    fs::create_dir_all(&bundle_dir).expect("bundle");
    let mutation_packet_path = bundle_dir.join("mutation-packet.json");
    fs::write(
        &mutation_packet_path,
        "{\"objective\":\"apply approved candidate\"}\n",
    )
    .expect("mutation packet");
    let diff_path = bundle_dir.join("diff.patch");
    fs::write(
        &diff_path,
        "diff --git a/README.md b/README.md\n--- a/README.md\n+++ b/README.md\n@@\n approved\n",
    )
    .expect("diff");
    let agent_summary_path = bundle_dir.join("agent-summary.json");
    let agent_summary = AgentMutationSummary {
        stop_reason: "final-decision".to_string(),
        model_turns: 2,
        tool_results: 1,
        input_tokens: 5,
        output_tokens: 7,
        model_metrics: Some(serde_json::json!({
            "final_decision_source": "strict_json_decision",
            "native_tool_call_turn_count": 1,
        })),
        tool_pair_integrity: Some(AgentToolPairIntegrity {
            tool_calls: 1,
            tool_results: 1,
            orphan_tool_calls: 0,
            orphan_tool_results: 0,
        }),
        final_text: "approved candidate".to_string(),
    };
    fs::write(
        &agent_summary_path,
        serde_json::to_string_pretty(&agent_summary).expect("agent summary JSON"),
    )
    .expect("agent summary");
    let mut record = StoredEvolutionJob {
        job: job.clone(),
        candidate_workspace: workspace,
        bundle: CandidateBundleManifest {
            job_id: job.job_id.clone(),
            bundle_path: bundle_dir.display().to_string(),
            created_at: OffsetDateTime::now_utc(),
            mutation_packet_path: mutation_packet_path.display().to_string(),
            job_path: None,
            model_output_path: None,
            agent_run_path: None,
            agent_summary_path: Some(agent_summary_path.display().to_string()),
            benchmark_results_path: None,
            resource_usage_path: None,
            content_manifest_path: None,
            attestation_path: None,
            registry_record_path: None,
            bundle_content_hash: None,
            changed_files_path: None,
            diff_path: Some(diff_path.display().to_string()),
            scorecard_path: None,
            candidate_summary_path: None,
            decision_path: None,
            artifact_refs: Vec::new(),
            command_logs: Vec::new(),
        },
        verification_outcome: Some(verification_outcome.clone()),
        changed_files: vec!["README.md".to_string()],
        decision: None,
    };
    finalize_candidate_bundle_integrity(
        &mut record.bundle,
        &record.job,
        &record.candidate_workspace,
        None,
        verification_outcome,
        &record.changed_files,
    )
    .expect("bundle integrity");
    write_stored_job(&paths, &record).expect("stored record");
    (source, jobs_root, data_root, record)
}

fn rollout_material_fixture(
    root: &Path,
    job_id: &EvolutionJobId,
    signer_id: &str,
) -> (
    sinomo_runtime::registry::VerifiedReleaseMaterialRecord,
    sinomo_runtime::registry::TrustedArtifactSigner,
) {
    let identity_path = root.join(format!("{signer_id}.identity.sobj"));
    let identity =
        sinomo_runtime::node::load_or_create_node_identity(&identity_path).expect("identity");
    let module_id =
        sinomo_runtime::module::ModuleId::from_legacy_named("module.trigger.silence_policy")
            .expect("module");
    let artifact_id =
        sinomo_runtime::module::ArtifactId::from_legacy_named("artifact_rollout_candidate")
            .expect("artifact");
    let active_artifact_id =
        Id::from_legacy_named("artifact_rollout_active").expect("active artifact");
    let mut artifact = sinomo_runtime::core::ArtifactManifest {
        artifact_id: artifact_id.clone(),
        module_id: module_id.clone(),
        target_ring: TrustRing::R3SandboxedModule,
        content_hash: "blake3:candidate-rollout".to_string(),
        signature: None,
        source_workspace: None,
        source_revision: None,
        module_manifest: sinomo_runtime::core::ModuleManifest::r3_wasm(
            module_id.clone(),
            "0.1.0",
            "trigger",
        ),
        provenance: sinomo_runtime::core::ArtifactProvenance {
            produced_by: sinomo_runtime::core::ActorRef {
                kind: sinomo_runtime::core::ActorKind::Operator,
                id: "sinomo-evolution-test".to_string(),
            },
            source_event_id: None,
            build_log_artifact_id: None,
            attestations: Vec::new(),
        },
    };
    artifact.signature = Some(
        sinomo_runtime::module::sign_artifact_manifest_with_node_identity(&artifact, &identity)
            .expect("sign artifact"),
    );
    let signer = sinomo_runtime::registry::TrustedArtifactSigner::local_ed25519(
        signer_id,
        identity.public_key,
        "approval test signer",
    );
    let material = sinomo_runtime::registry::VerifiedReleaseMaterialRecord {
        release_id: job_id.clone(),
        release_material_hash: String::new(),
        artifact: artifact.clone(),
        policy_decision: sinomo_runtime::core::PolicyDecision {
            decision_id: Id::from_legacy_named("policy_rollout_material").expect("policy"),
            target_ring: artifact.target_ring.clone(),
            target_artifact_id: artifact.artifact_id.clone(),
            action: sinomo_runtime::core::PolicyAction::Allow,
            capability_diff: sinomo_runtime::core::CapabilityDiff::default(),
            required_additional_evals: Vec::new(),
            max_rollout_weight_bps: Some(500),
            reason: "approved low-risk module candidate".to_string(),
        },
        rollout_plan: sinomo_runtime::core::RolloutPlan {
            rollout_plan_id: Id::from_legacy_named("rollout_material_approval").expect("rollout"),
            artifact_id,
            module_id,
            target_ring: artifact.target_ring.clone(),
            strategy: sinomo_runtime::core::RolloutStrategy::Shadow,
            target_nodes: Vec::new(),
            initial_weight_bps: 0,
            promotion_steps_bps: vec![500],
            rollback_policy: RollbackPolicy::default(),
        },
        active_artifact_id,
        evidence_refs: vec![format!("candidate-bundle://{job_id}")],
        registered_at: OffsetDateTime::now_utc(),
    };
    (material, signer)
}

fn git_repo(name: &str) -> sinomo_testdir::TestDir {
    let root = temp_dir(name);
    run_in(&root, "git init").expect("git init");
    run_in(&root, "git config user.email sinomo@example.test").expect("git email");
    run_in(&root, "git config user.name Sinomo").expect("git name");
    root
}

fn run_in(cwd: &Path, command: &str) -> Result<(), String> {
    let output = super::shell_command(command)
        .current_dir(cwd)
        .output()
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).to_string())
    }
}

#[cfg(windows)]
fn output_command() -> &'static str {
    "Write-Output sinomo-stdout; [Console]::Error.WriteLine('sinomo-stderr')"
}

fn normalize_newlines(value: &str) -> String {
    value.replace("\r\n", "\n")
}

#[cfg(not(windows))]
fn output_command() -> &'static str {
    "printf sinomo-stdout; printf sinomo-stderr >&2"
}

#[cfg(windows)]
fn agent_output_command() -> &'static str {
    "echo sinomo-stdout & echo sinomo-stderr 1>&2"
}

#[cfg(not(windows))]
fn agent_output_command() -> &'static str {
    output_command()
}

#[cfg(windows)]
fn failing_verification_command() -> &'static str {
    "[Console]::Error.Write('failure-details'); exit 7"
}

#[cfg(not(windows))]
fn failing_verification_command() -> &'static str {
    "printf failure-details >&2; exit 7"
}
