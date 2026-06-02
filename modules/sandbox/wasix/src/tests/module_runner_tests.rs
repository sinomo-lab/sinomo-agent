use super::*;
use sinomo_core::{EvaluationOutcome, HotSwapState, ModuleLoaderState};
use sinomo_module::{
    ModuleArtifactInvocation, ModuleArtifactRunner, ModuleId, ModuleInvocationLane,
    ModuleLoaderRuntimeState, ModuleReplayCase, ModuleReplayFixture, RolloutPlanId,
    SINOMO_MODULE_V1_INTERFACE_VERSION, SinomoModuleV1Result, write_module_loader_runtime_state,
};

#[test]
fn sandbox_wasm_module_runner_executes_registered_artifact() {
    let root = unique_test_dir("sandbox-wasm-module-runner");
    fs::create_dir_all(&root).expect("root");
    let artifact_id = ArtifactId::from_legacy_named("artifact_sandbox_wasm").expect("artifact");
    let wasm_path = root.join("artifact.wasm");
    fs::write(&wasm_path, wasi_stdin_to_stdout_module()).expect("write wasm");
    let registry = SandboxWasmModuleArtifactRegistry {
        artifacts: vec![SandboxWasmModuleArtifact {
            artifact_id: artifact_id.clone(),
            wasm_path,
            content_hash: sandbox_module_hash(wasi_stdin_to_stdout_module().as_bytes()),
            interface_version: None,
            component_model: false,
            abi: SandboxWasiAbi::Wasix,
            sandbox_id: None,
            argv: vec!["module.wasm".to_string()],
            cwd: None,
            env: BTreeMap::new(),
            limits: SandboxResourceLimits {
                wall_time_ms: 1_000,
                max_output_bytes: 16 * 1024,
                ..SandboxResourceLimits::default()
            },
            timeout_ms: Some(1_000),
        }],
    };
    let runner = SandboxWasmModuleArtifactRunner::new(registry, root.join("runtime"));

    let output = runner
        .invoke(&ModuleArtifactInvocation {
            node_id: NodeId::from_legacy_named("node_desktop").expect("node"),
            module_id: ModuleId::from_legacy_named("module.trigger.silence_policy")
                .expect("module"),
            artifact_id,
            lane: ModuleInvocationLane::Active,
            input: "payload\n".to_string(),
        })
        .expect("invoke");

    assert_eq!(output, "payload\n");
    fs::remove_dir_all(root).ok();
}

#[test]
fn sandbox_wasm_silence_policy_module_replays_shadow_against_wit_v1_contract() {
    let root = unique_test_dir("sandbox-wasm-silence-policy-replay");
    fs::create_dir_all(&root).expect("root");
    let module_id = ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
    let active_artifact_id =
        ArtifactId::from_legacy_named("artifact_silence_policy_active").expect("active");
    let shadow_artifact_id =
        ArtifactId::from_legacy_named("artifact_silence_policy_shadow").expect("shadow");
    let module_output = serde_json::json!({
        "status": "ok",
        "events_json": [
            serde_json::json!({
                "kind": "trigger.decision.created",
                "action": "stay-silent",
                "reason": "fixture event did not cross trigger thresholds"
            }).to_string()
        ],
        "metrics_json": serde_json::json!({
            "trigger_decisions": 1,
            "interruptions": 0
        }).to_string()
    })
    .to_string();
    let wasm = wasi_fixed_stdout_module(&format!("{module_output}\n"));
    let active_path = root.join("sandbox").join("artifacts").join("active.wasm");
    let shadow_path = root.join("sandbox").join("artifacts").join("shadow.wasm");
    fs::create_dir_all(active_path.parent().expect("active parent")).expect("artifact dir");
    fs::write(&active_path, &wasm).expect("active wasm");
    fs::write(&shadow_path, &wasm).expect("shadow wasm");
    write_sandbox_wasm_module_artifact_registry(
        &root,
        &SandboxWasmModuleArtifactRegistry {
            artifacts: vec![
                silence_policy_artifact(&root, active_artifact_id.clone(), &active_path, &wasm),
                silence_policy_artifact(&root, shadow_artifact_id.clone(), &shadow_path, &wasm),
            ],
        },
    )
    .expect("artifact registry");
    write_module_loader_runtime_state(
        &root,
        &ModuleLoaderRuntimeState {
            node_id: NodeId::from_legacy_named("node_desktop").expect("node"),
            rollout_plan_id: RolloutPlanId::from_legacy_named("rollout_silence_policy")
                .expect("rollout"),
            module_id: module_id.clone(),
            loader_state: ModuleLoaderState {
                module_id: module_id.clone(),
                active_artifact_id: active_artifact_id.clone(),
                previous_artifact_id: None,
                shadow_artifact_id: Some(shadow_artifact_id.clone()),
                candidate_artifact_id: Some(shadow_artifact_id.clone()),
                hot_swap_state: HotSwapState::Shadowing,
            },
            traffic_weight_bps: 0,
            last_rollout_action: "start-shadow".to_string(),
            last_rollout_stage: "shadow".to_string(),
            last_transition_record_id: Id::from_legacy_named("transition_silence_policy")
                .expect("transition"),
            reason: "sandbox WIT replay fixture".to_string(),
            applied_at: OffsetDateTime::now_utc().format(&Rfc3339).expect("time"),
        },
    )
    .expect("loader state");
    let fixture = ModuleReplayFixture {
        fixture_id: Id::from_legacy_named("fixture_silence_policy_shadow").expect("fixture"),
        module_id,
        cases: vec![
            ModuleReplayCase::from_event(
                "quiet-transcript",
                "sess-quiet",
                replay_event("event_quiet_transcript", "speech.transcript.final", 0.25),
            ),
            ModuleReplayCase::from_event(
                "ambient-fusion",
                "sess-ambient",
                replay_event("event_ambient_fusion", "fusion.observation.created", 0.45),
            ),
        ],
    };

    let report = run_sandbox_wasm_module_replay_fixture(&root, &fixture).expect("replay");
    let scorecard = report.scorecard(Some(active_artifact_id), Some(shadow_artifact_id));

    assert_eq!(report.replay_event_count, 2);
    assert_eq!(report.missing_route_count, 0);
    assert_eq!(report.primary_error_count, 0);
    assert_eq!(report.shadow_error_count, 0);
    assert_eq!(report.shadow_mismatch_count, 0);
    assert!(
        report
            .cases
            .iter()
            .all(|case| case.shadow_matched == Some(true))
    );
    assert_eq!(report.events.len(), 2);
    assert_eq!(scorecard.outcome, EvaluationOutcome::Passed);
    assert!(
        report.cases[0]
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.primary.output.as_deref())
            .is_some_and(|output| output.contains("stay-silent"))
    );
    fs::remove_dir_all(root).ok();
}

#[test]
fn sandbox_component_silence_policy_module_replays_shadow_against_wit_v1_contract() {
    let root = unique_test_dir("sandbox-component-silence-policy-replay");
    fs::create_dir_all(&root).expect("root");
    let module_id = ModuleId::from_legacy_named("module.trigger.silence_policy").expect("module");
    let active_artifact_id =
        ArtifactId::from_legacy_named("artifact_silence_policy_component_active").expect("active");
    let shadow_artifact_id =
        ArtifactId::from_legacy_named("artifact_silence_policy_component_shadow").expect("shadow");
    let component = sinomo_module_v1_silence_policy_component();
    validate_sinomo_module_v1_component_contract(&component).expect("component contract");
    let active_path = root
        .join("sandbox")
        .join("artifacts")
        .join("active.component.wasm");
    let shadow_path = root
        .join("sandbox")
        .join("artifacts")
        .join("shadow.component.wasm");
    fs::create_dir_all(active_path.parent().expect("active parent")).expect("artifact dir");
    fs::write(&active_path, &component).expect("active component");
    fs::write(&shadow_path, &component).expect("shadow component");
    write_sandbox_wasm_module_artifact_registry(
        &root,
        &SandboxWasmModuleArtifactRegistry {
            artifacts: vec![
                silence_policy_component_artifact(
                    &root,
                    active_artifact_id.clone(),
                    &active_path,
                    &component,
                ),
                silence_policy_component_artifact(
                    &root,
                    shadow_artifact_id.clone(),
                    &shadow_path,
                    &component,
                ),
            ],
        },
    )
    .expect("artifact registry");
    write_module_loader_runtime_state(
        &root,
        &ModuleLoaderRuntimeState {
            node_id: NodeId::from_legacy_named("node_desktop").expect("node"),
            rollout_plan_id: RolloutPlanId::from_legacy_named("rollout_silence_policy_component")
                .expect("rollout"),
            module_id: module_id.clone(),
            loader_state: ModuleLoaderState {
                module_id: module_id.clone(),
                active_artifact_id: active_artifact_id.clone(),
                previous_artifact_id: None,
                shadow_artifact_id: Some(shadow_artifact_id.clone()),
                candidate_artifact_id: Some(shadow_artifact_id.clone()),
                hot_swap_state: HotSwapState::Shadowing,
            },
            traffic_weight_bps: 0,
            last_rollout_action: "start-shadow".to_string(),
            last_rollout_stage: "shadow".to_string(),
            last_transition_record_id: Id::from_legacy_named("transition_silence_policy_component")
                .expect("transition"),
            reason: "component-model WIT replay fixture".to_string(),
            applied_at: OffsetDateTime::now_utc().format(&Rfc3339).expect("time"),
        },
    )
    .expect("loader state");
    let fixture = ModuleReplayFixture {
        fixture_id: Id::from_legacy_named("fixture_silence_policy_component_shadow")
            .expect("fixture"),
        module_id,
        cases: vec![
            ModuleReplayCase::from_event(
                "component-quiet-transcript",
                "sess-component-quiet",
                replay_event("event_component_quiet", "speech.transcript.final", 0.20),
            ),
            ModuleReplayCase::from_event(
                "component-ambient-fusion",
                "sess-component-ambient",
                replay_event(
                    "event_component_ambient",
                    "fusion.observation.created",
                    0.40,
                ),
            ),
        ],
    };

    let report = run_sandbox_wasm_module_replay_fixture(&root, &fixture).expect("replay");
    let scorecard = report.scorecard(Some(active_artifact_id), Some(shadow_artifact_id));
    let primary_result = report.cases[0]
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.primary.output.as_deref())
        .map(SinomoModuleV1Result::from_json_output)
        .expect("primary output")
        .expect("module result");

    assert_eq!(report.replay_event_count, 2);
    assert_eq!(report.missing_route_count, 0);
    assert_eq!(report.primary_error_count, 0);
    assert_eq!(report.shadow_error_count, 0);
    assert_eq!(report.shadow_mismatch_count, 0);
    assert!(
        report
            .cases
            .iter()
            .all(|case| case.shadow_matched == Some(true))
    );
    assert_eq!(report.events.len(), 2);
    assert_eq!(scorecard.outcome, EvaluationOutcome::Passed);
    assert_eq!(primary_result.status, "ok");
    assert_eq!(primary_result.events_json.len(), 1);
    assert!(primary_result.events_json[0].contains("stay-silent"));
    assert!(primary_result.metrics_json.contains("trigger_decisions"));
    fs::remove_dir_all(root).ok();
}

#[test]
fn sandbox_wasm_module_runner_rejects_hash_mismatch_before_execution() {
    let root = unique_test_dir("sandbox-wasm-module-runner-hash-mismatch");
    fs::create_dir_all(&root).expect("root");
    let artifact_id = ArtifactId::from_legacy_named("artifact_sandbox_wasm").expect("artifact");
    let wasm_path = root.join("artifact.wasm");
    fs::write(&wasm_path, wasi_stdin_to_stdout_module()).expect("write wasm");
    let registry = SandboxWasmModuleArtifactRegistry {
        artifacts: vec![SandboxWasmModuleArtifact {
            artifact_id: artifact_id.clone(),
            wasm_path,
            content_hash: sandbox_module_hash(b"different bytes"),
            interface_version: None,
            component_model: false,
            abi: SandboxWasiAbi::Wasix,
            sandbox_id: None,
            argv: vec!["module.wasm".to_string()],
            cwd: None,
            env: BTreeMap::new(),
            limits: SandboxResourceLimits::default(),
            timeout_ms: Some(1_000),
        }],
    };
    let runner = SandboxWasmModuleArtifactRunner::new(registry, root.join("runtime"));

    let error = runner
        .invoke(&ModuleArtifactInvocation {
            node_id: NodeId::from_legacy_named("node_desktop").expect("node"),
            module_id: ModuleId::from_legacy_named("module.trigger.silence_policy")
                .expect("module"),
            artifact_id,
            lane: ModuleInvocationLane::Active,
            input: "payload\n".to_string(),
        })
        .expect_err("hash mismatch");

    assert!(error.contains("hash mismatch"));
    fs::remove_dir_all(root).ok();
}

#[test]
fn sandbox_wasm_module_runner_rejects_invalid_wit_v1_contract_output() {
    let root = unique_test_dir("sandbox-wasm-module-runner-invalid-contract");
    fs::create_dir_all(&root).expect("root");
    let artifact_id =
        ArtifactId::from_legacy_named("artifact_sandbox_wasm_invalid_contract").expect("artifact");
    let wasm = wasi_fixed_stdout_module("not-json\n");
    let wasm_path = root.join("artifact.wasm");
    fs::write(&wasm_path, &wasm).expect("write wasm");
    let registry = SandboxWasmModuleArtifactRegistry {
        artifacts: vec![SandboxWasmModuleArtifact {
            artifact_id: artifact_id.clone(),
            wasm_path,
            content_hash: sandbox_module_hash(wasm.as_bytes()),
            interface_version: Some(SINOMO_MODULE_V1_INTERFACE_VERSION.to_string()),
            component_model: false,
            abi: SandboxWasiAbi::Wasix,
            sandbox_id: None,
            argv: vec!["module.wasm".to_string()],
            cwd: None,
            env: BTreeMap::new(),
            limits: SandboxResourceLimits {
                wall_time_ms: 1_000,
                max_output_bytes: 16 * 1024,
                ..SandboxResourceLimits::default()
            },
            timeout_ms: Some(1_000),
        }],
    };
    let runner = SandboxWasmModuleArtifactRunner::new(registry, root.join("runtime"));

    let error = runner
        .invoke(&ModuleArtifactInvocation {
            node_id: NodeId::from_legacy_named("node_desktop").expect("node"),
            module_id: ModuleId::from_legacy_named("module.trigger.silence_policy")
                .expect("module"),
            artifact_id,
            lane: ModuleInvocationLane::Active,
            input: "{}\n".to_string(),
        })
        .expect_err("invalid contract output");

    assert!(error.contains("declared sinomo-module-v1 but returned invalid module result"));
    fs::remove_dir_all(root).ok();
}

#[test]
fn sandbox_rejects_component_without_sinomo_module_v1_export() {
    let component = wat::parse_str(
        r#"
        (component
          (core module $m)
        )
        "#,
    )
    .expect("component wat");
    let error = validate_sinomo_module_v1_component_contract(&component)
        .expect_err("missing module export");

    assert!(matches!(
        error,
        SandboxModuleComponentContractError::MissingExport { .. }
    ));
}

#[test]
fn sandbox_rejects_component_with_wrong_sinomo_module_v1_run_signature() {
    let component = wat::parse_str(
        r#"
        (component
          (core module $m
            (func (export "run")))
          (core instance $i (instantiate $m))
          (func $run (canon lift (core func $i "run")))
          (instance $module
            (export "run" (func $run)))
          (export "sinomo:module-v1/module" (instance $module))
        )
        "#,
    )
    .expect("component wat");
    let error =
        validate_sinomo_module_v1_component_contract(&component).expect_err("wrong run signature");

    assert!(matches!(
        error,
        SandboxModuleComponentContractError::InvalidExportShape { .. }
    ));
}

#[test]
fn sandbox_component_model_artifact_invokes_typed_run() {
    let root = unique_test_dir("sandbox-component-module-runner");
    fs::create_dir_all(&root).expect("root");
    let artifact_id = ArtifactId::from_legacy_named("artifact_sandbox_component_static_result")
        .expect("artifact");
    let component = sinomo_module_v1_static_result_component();
    validate_sinomo_module_v1_component_contract(&component).expect("valid component contract");
    let wasm_path = root.join("artifact.component.wasm");
    fs::write(&wasm_path, &component).expect("write component");
    let registry = SandboxWasmModuleArtifactRegistry {
        artifacts: vec![SandboxWasmModuleArtifact {
            artifact_id: artifact_id.clone(),
            wasm_path,
            content_hash: sandbox_module_hash(&component),
            interface_version: Some(SINOMO_MODULE_V1_INTERFACE_VERSION.to_string()),
            component_model: true,
            abi: SandboxWasiAbi::Wasix,
            sandbox_id: None,
            argv: Vec::new(),
            cwd: None,
            env: BTreeMap::new(),
            limits: SandboxResourceLimits {
                wall_time_ms: 1_000,
                ..SandboxResourceLimits::default()
            },
            timeout_ms: Some(1_000),
        }],
    };
    let runner = SandboxWasmModuleArtifactRunner::new(registry, root.join("runtime"));

    let output = runner
        .invoke(&ModuleArtifactInvocation {
            node_id: NodeId::from_legacy_named("node_desktop").expect("node"),
            module_id: ModuleId::from_legacy_named("module.trigger.silence_policy")
                .expect("module"),
            artifact_id,
            lane: ModuleInvocationLane::Active,
            input: "{}".to_string(),
        })
        .expect("component run");
    let result = SinomoModuleV1Result::from_json_output(&output).expect("module result");

    assert_eq!(result.status, "ok");
    assert!(result.events_json.is_empty());
    assert_eq!(result.metrics_json, "{}");
    fs::remove_dir_all(root).ok();
}

fn silence_policy_artifact(
    root: &Path,
    artifact_id: ArtifactId,
    wasm_path: &Path,
    wasm: &str,
) -> SandboxWasmModuleArtifact {
    SandboxWasmModuleArtifact {
        artifact_id,
        wasm_path: wasm_path
            .strip_prefix(root)
            .expect("relative wasm")
            .to_path_buf(),
        content_hash: sandbox_module_hash(wasm.as_bytes()),
        interface_version: Some(SINOMO_MODULE_V1_INTERFACE_VERSION.to_string()),
        component_model: false,
        abi: SandboxWasiAbi::Wasix,
        sandbox_id: None,
        argv: vec!["silence-policy.wasm".to_string()],
        cwd: None,
        env: BTreeMap::new(),
        limits: SandboxResourceLimits {
            wall_time_ms: 1_000,
            max_output_bytes: 16 * 1024,
            ..SandboxResourceLimits::default()
        },
        timeout_ms: Some(1_000),
    }
}

fn silence_policy_component_artifact(
    root: &Path,
    artifact_id: ArtifactId,
    wasm_path: &Path,
    component: &[u8],
) -> SandboxWasmModuleArtifact {
    SandboxWasmModuleArtifact {
        artifact_id,
        wasm_path: wasm_path
            .strip_prefix(root)
            .expect("relative component")
            .to_path_buf(),
        content_hash: sandbox_module_hash(component),
        interface_version: Some(SINOMO_MODULE_V1_INTERFACE_VERSION.to_string()),
        component_model: true,
        abi: SandboxWasiAbi::Wasix,
        sandbox_id: None,
        argv: Vec::new(),
        cwd: None,
        env: BTreeMap::new(),
        limits: SandboxResourceLimits {
            wall_time_ms: 1_000,
            max_output_bytes: 16 * 1024,
            ..SandboxResourceLimits::default()
        },
        timeout_ms: Some(1_000),
    }
}

fn sinomo_module_v1_static_result_component() -> Vec<u8> {
    sinomo_module_v1_static_result_component_for_result(&SinomoModuleV1Result {
        status: "ok".to_string(),
        events_json: Vec::new(),
        metrics_json: "{}".to_string(),
    })
}

fn sinomo_module_v1_silence_policy_component() -> Vec<u8> {
    sinomo_module_v1_static_result_component_for_result(&SinomoModuleV1Result {
        status: "ok".to_string(),
        events_json: vec![
            serde_json::json!({
                "kind": "trigger.decision.created",
                "action": "stay-silent",
                "reason": "component fixture event did not cross trigger thresholds"
            })
            .to_string(),
        ],
        metrics_json: serde_json::json!({
            "trigger_decisions": 1,
            "interruptions": 0,
            "component_model": true
        })
        .to_string(),
    })
}

fn sinomo_module_v1_static_result_component_for_result(result: &SinomoModuleV1Result) -> Vec<u8> {
    let result_data_segments = sinomo_module_v1_result_data_segments(result);
    wat::parse_str(
        &format!(
            r#"
        (component
          (core module $main
            (type $realloc-ty (func (param i32 i32 i32 i32) (result i32)))
            (type $run-ty
              (func
                (param i32 i32 i32 i32 i32 i32 i64 i32 i32 i32 i32)
                (result i32)))
            (type $post-return-ty (func (param i32)))
            (memory 1)
            {result_data_segments}
            (export "memory" (memory 0))
            (export "cabi_realloc" (func $realloc))
            (export "sinomo:module-v1/module#run" (func $run))
            (export "cabi_post_sinomo:module-v1/module#run" (func $post-return))
            (func $realloc (type $realloc-ty)
              (param i32 i32 i32 i32)
              (result i32)
              i32.const 1024)
            (func $run (type $run-ty)
              (param i32 i32 i32 i32 i32 i32 i64 i32 i32 i32 i32)
              (result i32)
              i32.const 2048)
            (func $post-return (type $post-return-ty)
              (param i32)))
          (core instance $main-instance (instantiate $main))
          (alias core export $main-instance "memory" (core memory $memory))
          (alias core export $main-instance "cabi_realloc" (core func $cabi-realloc))
          (alias core export $main-instance "sinomo:module-v1/module#run" (core func $run-core))
          (alias core export $main-instance "cabi_post_sinomo:module-v1/module#run" (core func $run-post-return))
          (type $module-context
            (record
              (field "node-id" string)
              (field "module-id" string)
              (field "module-version" string)
              (field "budget-ms" u64)
              (field "capabilities-json" string)))
          (type $events-json (list string))
          (type $module-result
            (record
              (field "status" string)
              (field "events-json" $events-json)
              (field "metrics-json" string)))
          (type $run-ty
            (func
              (param "context" $module-context)
              (param "input-json" string)
              (result $module-result)))
          (func $run
            (type $run-ty)
            (canon lift
              (core func $run-core)
              (memory $memory)
              (realloc $cabi-realloc)
              string-encoding=utf8
              (post-return $run-post-return)))
          (component $module-shim
            (type (;0;)
              (record
                (field "node-id" string)
                (field "module-id" string)
                (field "module-version" string)
                (field "budget-ms" u64)
                (field "capabilities-json" string)))
            (import "import-type-module-context" (type (;1;) (eq 0)))
            (type (;2;) (list string))
            (import "import-type-events-json" (type (;3;) (eq 2)))
            (type (;4;)
              (record
                (field "status" string)
                (field "events-json" 3)
                (field "metrics-json" string)))
            (import "import-type-module-result" (type (;5;) (eq 4)))
            (type (;6;)
              (func
                (param "context" 1)
                (param "input-json" string)
                (result 5)))
            (import "import-func-run" (func (;0;) (type 6)))
            (type (;7;)
              (record
                (field "node-id" string)
                (field "module-id" string)
                (field "module-version" string)
                (field "budget-ms" u64)
                (field "capabilities-json" string)))
            (export (;8;) "module-context" (type 7))
            (type (;9;) (list string))
            (export (;10;) "events-json" (type 9))
            (type (;11;)
              (record
                (field "status" string)
                (field "events-json" 10)
                (field "metrics-json" string)))
            (export (;12;) "module-result" (type 11))
            (type (;13;)
              (func
                (param "context" 8)
                (param "input-json" string)
                (result 12)))
            (export "run" (func 0) (func (type 13))))
          (instance $module-instance
            (instantiate $module-shim
              (with "import-type-module-context" (type $module-context))
              (with "import-type-events-json" (type $events-json))
              (with "import-type-module-result" (type $module-result))
              (with "import-func-run" (func $run))))
          (export "sinomo:module-v1/module" (instance $module-instance))
        )
        "#,
        ),
    )
    .expect("component wat")
}

fn sinomo_module_v1_result_data_segments(result: &SinomoModuleV1Result) -> String {
    result.validate().expect("valid static component result");
    const RECORD_PTR: u32 = 2048;
    const EVENT_LIST_PTR: u32 = 2080;
    let mut next_string_ptr = 2300_u32;
    let status_ptr = next_string_ptr;
    let status_bytes = result.status.as_bytes();
    next_string_ptr = next_string_ptr.saturating_add(status_bytes.len() as u32);
    let event_ptrs = result
        .events_json
        .iter()
        .map(|event_json| {
            let ptr = next_string_ptr;
            next_string_ptr = next_string_ptr.saturating_add(event_json.len() as u32);
            (ptr, event_json.as_bytes())
        })
        .collect::<Vec<_>>();
    let metrics_ptr = next_string_ptr;
    let metrics_bytes = result.metrics_json.as_bytes();

    let mut record = Vec::new();
    push_wit_u32(&mut record, status_ptr);
    push_wit_u32(&mut record, status_bytes.len() as u32);
    push_wit_u32(
        &mut record,
        if event_ptrs.is_empty() {
            0
        } else {
            EVENT_LIST_PTR
        },
    );
    push_wit_u32(&mut record, event_ptrs.len() as u32);
    push_wit_u32(&mut record, metrics_ptr);
    push_wit_u32(&mut record, metrics_bytes.len() as u32);

    let mut segments = Vec::new();
    segments.push(format!(
        r#"(data (i32.const {RECORD_PTR}) "{}")"#,
        wat_bytes(&record)
    ));
    if !event_ptrs.is_empty() {
        let mut event_list = Vec::new();
        for (ptr, bytes) in &event_ptrs {
            push_wit_u32(&mut event_list, *ptr);
            push_wit_u32(&mut event_list, bytes.len() as u32);
        }
        segments.push(format!(
            r#"(data (i32.const {EVENT_LIST_PTR}) "{}")"#,
            wat_bytes(&event_list)
        ));
    }
    segments.push(format!(
        r#"(data (i32.const {status_ptr}) "{}")"#,
        wat_bytes(status_bytes)
    ));
    for (ptr, bytes) in event_ptrs {
        segments.push(format!(
            r#"(data (i32.const {ptr}) "{}")"#,
            wat_bytes(bytes)
        ));
    }
    segments.push(format!(
        r#"(data (i32.const {metrics_ptr}) "{}")"#,
        wat_bytes(metrics_bytes)
    ));
    segments.join("\n            ")
}

fn push_wit_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn wasi_fixed_stdout_module(output: &str) -> String {
    format!(
        r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "proc_exit"
            (func $proc_exit (param i32)))
          (memory (export "memory") 1 128)
          (data (i32.const 64) "{output_bytes}")
          (func $exit_if_errno (param $errno i32)
            local.get $errno
            i32.eqz
            if
            else
              local.get $errno
              call $proc_exit
            end)
          (func (export "_start")
            i32.const 0
            i32.const 64
            i32.store
            i32.const 4
            i32.const {output_len}
            i32.store
            i32.const 1
            i32.const 0
            i32.const 1
            i32.const 20
            call $fd_write
            call $exit_if_errno))
        "#,
        output_bytes = wat_bytes(output.as_bytes()),
        output_len = output.len(),
    )
}

fn replay_event(name: &str, kind: &str, salience: f32) -> EventEnvelope {
    EventEnvelope::new(
        EventId::from_legacy_named(name).expect("event"),
        OffsetDateTime::now_utc(),
        NodeId::from_legacy_named("node_desktop").expect("node"),
        Source::new("sandbox.module_replay.test").expect("source"),
        EventKind::new(kind).expect("kind"),
        Salience::new(salience).expect("salience"),
        Confidence::new(0.95).expect("confidence"),
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Interactive,
            PrivacyClass::Derived,
        ),
    )
    .with_payload(serde_json::json!({
        "text": "background conversation",
        "fixture": name
    }))
}
