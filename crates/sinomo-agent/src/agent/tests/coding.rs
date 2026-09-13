use super::*;

#[test]
fn coding_run_state_projection_bounds_model_visible_history() {
    let state = CodingRunState {
        objective: Some("fix the validation failure".to_string()),
        changed_files: (0..30)
            .map(|index| format!("src/file_{index}.rs"))
            .collect(),
        validation_attempts: (0..5)
            .map(|index| CodingValidationAttempt {
                step_index: index,
                call_id: TaskId::generate(),
                command: format!("cargo test case-{index}"),
                status: CodingValidationStatus::FailedPatchRelevant,
                summary: format!("failure summary {index}"),
                exit_code: Some(101),
                timed_out: false,
                failure_kind: Some(CodingCommandFailureKind::PatchRelevant),
            })
            .collect(),
        open_risks: (0..7).map(|index| format!("risk {index}")).collect(),
        outstanding_evidence: (0..6).map(|index| format!("evidence {index}")).collect(),
        patch_status: CodingPatchStatus::Applied,
        validation_status: CodingValidationStatus::FailedPatchRelevant,
        stop_recommendation: CodingStopDecision::Continue,
        ..CodingRunState::default()
    };
    assert_eq!(state.validation_attempts.len(), 5);
    let item = AgentItem::CodingRunState {
        item_id: TaskId::generate(),
        step_index: 9,
        state,
    };

    let transcript_item = project_agent_item_to_transcript_item(item)
        .expect("coding state projects")
        .expect("render coding state");
    let rendered: serde_json::Value = serde_json::from_str(&transcript_item.content).expect("json");
    let projected = rendered.get("state").expect("state");

    assert_eq!(
        projected
            .get("projection")
            .and_then(serde_json::Value::as_str),
        Some("model_visible_bounded")
    );
    assert_eq!(
        projected
            .get("validation_attempt_count")
            .and_then(serde_json::Value::as_u64),
        Some(5)
    );
    assert_eq!(
        projected
            .get("omitted_validation_attempt_count")
            .and_then(serde_json::Value::as_u64),
        Some(2)
    );
    let attempts = projected
        .get("validation_attempts")
        .and_then(serde_json::Value::as_array)
        .expect("attempts");
    assert_eq!(attempts.len(), 3);
    assert!(!transcript_item.content.contains("cargo test case-0"));
    assert!(transcript_item.content.contains("cargo test case-4"));
    assert_eq!(
        projected
            .get("changed_file_count")
            .and_then(serde_json::Value::as_u64),
        Some(30)
    );
    assert_eq!(
        projected
            .get("omitted_changed_file_count")
            .and_then(serde_json::Value::as_u64),
        Some(5)
    );
    assert!(!transcript_item.content.contains("risk 0"));
    assert!(transcript_item.content.contains("risk 6"));
    assert_eq!(
        projected
            .get("omitted_open_risk_count")
            .and_then(serde_json::Value::as_u64),
        Some(2)
    );
    assert_eq!(
        projected
            .get("omitted_outstanding_evidence_count")
            .and_then(serde_json::Value::as_u64),
        Some(1)
    );
}

#[test]
fn command_failure_classifier_separates_environment_causes() {
    let missing_dependency = serde_json::json!({
        "success": false,
        "timed_out": false,
        "stderr": "ModuleNotFoundError: No module named 'pytest'\n",
        "stdout": "",
    });
    let dependency_classification = classify_command_failure(
        &AgentToolStatus::Failed,
        "pytest failed",
        &missing_dependency,
    );
    assert_eq!(
        dependency_classification.status,
        CodingValidationStatus::FailedEnvironment
    );
    assert_eq!(
        dependency_classification.failure_kind,
        Some(CodingCommandFailureKind::MissingDependency)
    );

    let network = serde_json::json!({
        "success": false,
        "timed_out": false,
        "stderr": "curl: (6) Could not resolve host: example.invalid\n",
        "stdout": "",
    });
    let network_classification =
        classify_command_failure(&AgentToolStatus::Failed, "download failed", &network);
    assert_eq!(
        network_classification.status,
        CodingValidationStatus::FailedEnvironment
    );
    assert_eq!(
        network_classification.failure_kind,
        Some(CodingCommandFailureKind::NetworkUnavailable)
    );
}

#[test]
fn coding_state_requires_validation_after_the_latest_patch() {
    let state = coding_state_from_items(&[
        AgentItem::VerificationResult {
            item_id: TaskId::generate(),
            step_index: 0,
            call_id: TaskId::generate(),
            command: "python --version && node --version".to_string(),
            status: AgentVerificationState::Passed,
            summary: "toolchains are installed".to_string(),
        },
        AgentItem::PatchSnapshot {
            item_id: TaskId::generate(),
            step_index: 1,
            call_id: TaskId::generate(),
            status: AgentToolStatus::Succeeded,
            summary: "created frontend/package.json".to_string(),
            patch_ref: None,
            changed_files: vec!["frontend/package.json".to_string()],
        },
    ]);

    assert_eq!(state.patch_status, CodingPatchStatus::Applied);
    assert_eq!(state.validation_status, CodingValidationStatus::NotRun);
    assert!(state.validation_attempts.is_empty());
    assert_eq!(state.stop_recommendation, CodingStopDecision::Continue);
    assert!(
        state
            .outstanding_evidence
            .iter()
            .any(|evidence| evidence.contains("current patch"))
    );
    assert!(!coding_state_should_stop_loop(&state));
}

#[test]
fn coding_stop_policy_blocks_network_validation_even_with_patch() {
    let state = finalize_coding_state(CodingRunState {
        changed_files: vec!["src/lib.rs".to_string()],
        patch_status: CodingPatchStatus::Applied,
        validation_status: CodingValidationStatus::FailedEnvironment,
        validation_attempts: vec![CodingValidationAttempt {
            step_index: 1,
            call_id: TaskId::generate(),
            command: "cargo test".to_string(),
            status: CodingValidationStatus::FailedEnvironment,
            summary: "failed to download crate index".to_string(),
            exit_code: Some(101),
            timed_out: false,
            failure_kind: Some(CodingCommandFailureKind::NetworkUnavailable),
        }],
        ..CodingRunState::default()
    });

    assert_eq!(state.stop_recommendation, CodingStopDecision::FinishBlocked);
    assert!(coding_state_should_stop_loop(&state));
    let decision = coding_final_decision_from_state(&state);
    assert!(decision.text.contains("Coding run is blocked"));
    assert!(decision.text.contains("network unavailable"));
    assert_eq!(decision.response_mode, crate::AgentLoopResponseMode::Answer);
}

#[test]
fn coding_final_decision_reports_missing_dependency_validation() {
    let state = finalize_coding_state(CodingRunState {
        changed_files: vec!["src/lib.rs".to_string()],
        patch_status: CodingPatchStatus::Applied,
        validation_status: CodingValidationStatus::FailedEnvironment,
        validation_attempts: vec![CodingValidationAttempt {
            step_index: 1,
            call_id: TaskId::generate(),
            command: "pytest".to_string(),
            status: CodingValidationStatus::FailedEnvironment,
            summary: "ModuleNotFoundError: No module named 'pytest'".to_string(),
            exit_code: Some(1),
            timed_out: false,
            failure_kind: Some(CodingCommandFailureKind::MissingDependency),
        }],
        ..CodingRunState::default()
    });

    assert_eq!(
        state.stop_recommendation,
        CodingStopDecision::FinishWithPatch
    );
    let decision = coding_final_decision_from_state(&state);
    assert!(decision.text.contains("Patch is ready"));
    assert!(decision.text.contains("missing dependency"));
}

#[test]
fn coding_stop_policy_continues_on_patch_relevant_validation_failure() {
    let state = finalize_coding_state(CodingRunState {
        changed_files: vec!["src/lib.rs".to_string()],
        patch_status: CodingPatchStatus::Applied,
        validation_status: CodingValidationStatus::FailedPatchRelevant,
        validation_attempts: vec![CodingValidationAttempt {
            step_index: 1,
            call_id: TaskId::generate(),
            command: "cargo test focused".to_string(),
            status: CodingValidationStatus::FailedPatchRelevant,
            summary: "assertion failed in changed behavior".to_string(),
            exit_code: Some(101),
            timed_out: false,
            failure_kind: Some(CodingCommandFailureKind::PatchRelevant),
        }],
        ..CodingRunState::default()
    });

    assert_eq!(state.stop_recommendation, CodingStopDecision::Continue);
    assert!(!coding_state_should_stop_loop(&state));
}

#[test]
fn coding_stop_policy_requires_classified_environment_validation() {
    let state = finalize_coding_state(CodingRunState {
        changed_files: vec!["src/lib.rs".to_string()],
        patch_status: CodingPatchStatus::Applied,
        validation_status: CodingValidationStatus::FailedEnvironment,
        validation_attempts: vec![CodingValidationAttempt {
            step_index: 1,
            call_id: TaskId::generate(),
            command: "cargo test focused".to_string(),
            status: CodingValidationStatus::FailedEnvironment,
            summary: "validation blocked".to_string(),
            exit_code: None,
            timed_out: false,
            failure_kind: None,
        }],
        ..CodingRunState::default()
    });

    assert_eq!(state.stop_recommendation, CodingStopDecision::Continue);
    assert!(!coding_state_should_stop_loop(&state));
}

#[test]
fn openai_coding_agent_executes_apply_patch_without_disclosure() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-openai-apply-patch-run");
    let evidence_dir = root.join("evidence");
    fs::create_dir_all(&root).expect("workspace");
    fs::write(root.join("hello.txt"), "old\n").expect("seed file");
    let patch = "*** Begin Patch\n*** Update File: hello.txt\n@@\n-old\n+new\n*** End Patch";
    let tool_turn = serde_json::json!({
        "response_mode": "defer",
        "text": "I will patch the file.",
        "tool_calls": [{
            "tool": "apply_patch",
            "input": {"patch": patch},
            "async": false,
            "summary": "Patch hello.txt."
        }],
        "background_handoff": null,
        "next_state": "waiting"
    })
    .to_string();
    let model = SequencedModel::new([
        tool_turn,
        r#"{
            "response_mode": "answer",
            "text": "Patched.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#
        .to_string(),
    ]);
    let agent_config = AgentConfig::new("openai", "gpt-5.2-codex")
        .with_agent_definition(AgentDefinition::coding())
        .with_workspace_root(&root);
    let workspace_config = LocalWorkspaceAgentConfig::new(&root)
        .allowing_file_writes(true)
        .with_evidence_dir(&evidence_dir);
    let agent = Agent::new(model, "openai", "gpt-5.2-codex")
        .with_config(agent_config)
        .with_tool_registry(AgentToolRegistry::local_workspace_with_config(
            workspace_config,
        ));

    let run = agent.run(&agent_input("patch file")).expect("run");

    assert_eq!(
        fs::read_to_string(root.join("hello.txt")).expect("file"),
        "new\n"
    );
    assert_eq!(run.tool_results.len(), 1);
    assert_eq!(run.tool_results[0].status, AgentToolStatus::Succeeded);
    assert_eq!(run.tool_results[0].call.tool, "apply_patch");
    let orchestration = run.tool_results[0]
        .evidence
        .metadata
        .get("tool_orchestration")
        .expect("tool orchestration metadata");
    assert_eq!(
        orchestration
            .get("descriptor_visible")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert!(
        orchestration
            .get("visible_toolsets")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|toolsets| toolsets
                .iter()
                .any(|toolset| toolset.as_str() == Some("provider_specific")))
    );
}

#[test]
fn coding_state_stops_after_patch_with_environment_blocked_validation() {
    let root = sinomo_testdir::TestDir::new("sinomo-agent-coding-stop-env");
    let evidence_dir = root.join("evidence");
    fs::create_dir_all(&root).expect("workspace");
    fs::write(root.join("hello.txt"), "old\n").expect("seed file");
    let patch_turn = serde_json::json!({
        "response_mode": "defer",
        "text": "I will edit the file.",
        "tool_calls": [{
            "tool": "edit_file",
            "input": {
                "path": "hello.txt",
                "old_text": "old\n",
                "new_text": "new\n"
            },
            "async": false,
            "summary": "Edit hello.txt."
        }],
        "background_handoff": null,
        "next_state": "waiting"
    })
    .to_string();
    let validation_turn = serde_json::json!({
        "response_mode": "defer",
        "text": "I will validate the patch.",
        "tool_calls": [{
            "tool": "run_command",
            "input": {"command": "missing-sinomo-test-validator"},
            "async": false,
            "summary": "Run validation."
        }],
        "background_handoff": null,
        "next_state": "waiting"
    })
    .to_string();
    let model = SequencedModel::new([patch_turn, validation_turn]);
    let config = LocalWorkspaceAgentConfig::new(&root)
        .allowing_file_writes(true)
        .using_local_process_commands()
        .allowing_shell_commands(true)
        .with_evidence_dir(&evidence_dir);
    let agent = Agent::new(model, "fake", "fake").with_tool_registry(
        with_remote_management_for_test(AgentToolRegistry::local_workspace_with_config(config)),
    );

    let run = agent.run(&agent_input("patch and validate")).expect("run");
    let coding_state = run.coding_state.as_ref().expect("coding state");

    assert_eq!(run.stop_reason, AgentStopReason::CodingStopRecommended);
    assert_eq!(run.model_turns.len(), 2);
    assert_eq!(
        run.final_decision.response_mode,
        crate::AgentLoopResponseMode::Answer
    );
    assert!(run.final_decision.text.contains("Patch is ready"));
    assert!(run.final_decision.text.contains("missing command"));
    assert_eq!(coding_state.patch_status, CodingPatchStatus::Applied);
    assert_eq!(
        coding_state.validation_status,
        CodingValidationStatus::FailedEnvironment
    );
    assert_eq!(
        coding_state.stop_recommendation,
        CodingStopDecision::FinishWithPatch
    );
    let validation_attempt = coding_state
        .validation_attempts
        .last()
        .expect("validation attempt");
    assert_eq!(
        validation_attempt.failure_kind,
        Some(CodingCommandFailureKind::MissingCommand)
    );
    assert_eq!(coding_state.changed_files, vec!["hello.txt".to_string()]);
    assert!(
        run.items
            .iter()
            .any(|item| matches!(item, AgentItem::CodingRunState { .. }))
    );
}
