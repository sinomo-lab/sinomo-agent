use crate::tools::local_workspace::provision_default_agent_session_sandbox;
use crate::{
    AGENT_DEFINITION_SIFS_ROOT, AGENT_OBJECT_RPC_ROUTES, AGENT_RPC_CATEGORY,
    AGENT_RUNTIME_CONTROL_OBJECT_RPC_ROUTES, AGENT_SESSION_FOLLOW_UP_ROUTE,
    AGENT_SESSION_INSPECT_ROUTE, AGENT_SESSION_INTERRUPT_ROUTE, AGENT_SESSION_PROGRESS_ROUTE,
    AGENT_SESSION_RESUME_ROUTE, AGENT_SESSION_STEER_ROUTE, AGENT_SESSION_TERMINAL_RESULTS_ROUTE,
    Agent, AgentCancellationToken, AgentCatalog, AgentCompactor, AgentConfig,
    AgentContextAttachment, AgentDefinition, AgentDefinitionSifsStore, AgentDefinitionStore,
    AgentDefinitionValidationError, AgentItem, AgentLoopInput, AgentLoopResponseMode,
    AgentModelDecisionSource, AgentModelEvent, AgentModuleToolProjection, AgentOp,
    AgentOutputFormat, AgentPackageProvenance, AgentPackageSource, AgentPlacementMetadata,
    AgentProcessLifecycleStatus, AgentRemoteObjectRpcFuture, AgentRemoteObjectRpcTool,
    AgentRemoteObjectRpcTransport, AgentRole, AgentRun, AgentRunContext, AgentRuntimeControlError,
    AgentRuntimeControlHost, AgentRuntimeControlRegistry, AgentSession,
    AgentSessionControlEventCursor, AgentSessionControlResponse, AgentSessionControlResult,
    AgentSessionControlSnapshot, AgentSessionFollowUpRequest, AgentSessionInspectRequest,
    AgentSessionInspectResponse, AgentSessionInterruptRequest, AgentSessionProgressRequest,
    AgentSessionProgressResponse, AgentSessionReattachState, AgentSessionRemoveQueuedInputRequest,
    AgentSessionResumeRequest, AgentSessionResumeResponse, AgentSessionRuntime,
    AgentSessionSnapshot, AgentSessionSteerRequest, AgentSessionStore, AgentSessionStoreRecord,
    AgentSessionStoreRecordKind, AgentSessionTerminalResultsRequest,
    AgentSessionTerminalResultsResponse, AgentSkillPlan, AgentStopReason, AgentTaskLifecycleStatus,
    AgentTaskRef, AgentTaskSnapshot, AgentToolCall, AgentToolCapability, AgentToolEvidence,
    AgentToolGateway, AgentToolGatewayCancellation, AgentToolGatewayTask, AgentToolKind,
    AgentToolOrchestrator, AgentToolPairIntegrity, AgentToolPlanSource, AgentToolPlanner,
    AgentToolProgressEvent, AgentToolProgressSink, AgentToolRegistry, AgentToolRequest,
    AgentToolResult, AgentToolSpec, AgentToolStatus, AgentToolSurface, AgentTranscript,
    AsyncTaskNextAction, AsyncTaskStatus, AsyncTaskUpdate, CodingCommandFailureKind,
    CodingPatchStatus, CodingStopDecision, CodingValidationStatus, FileAgentSessionStore,
    InteractionModel, InteractionSession, InteractionSurface, InteractionSurfaceKind,
    LocalWorkspaceAgentConfig, TaskId, WorkspaceRef, parse_agent_loop_response,
    user_interaction_input_event,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_lm::{
    LanguageModelCustomToolFormat, LanguageModelCustomToolSpec, LanguageModelError,
    LanguageModelErrorCategory, LanguageModelProviderToolSpec, LanguageModelRequest,
    LanguageModelResponse, LanguageModelStreamEvent, LanguageModelToolAdapterSource,
    LanguageModelToolCall, LanguageModelToolSpec, classify_language_model_error_detail,
    collect_language_model_tool_calls,
};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{
    ObjectRpcCall, ObjectRpcCancelRequest, ObjectRpcIncomingBody, ObjectRpcOutgoingResponse,
    ObjectRpcRequest, ObjectRpcResponse, ObjectRpcResult,
};
use sinomo_runtime::node::{LocalNodePermission, NodeId};
use sinomo_runtime::service::{
    ResponseSink, ServiceContext, ServiceError, ServiceRegistry, ServiceSigner,
};
use std::collections::VecDeque;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentReadinessStatus {
    Ready,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentReadinessCheckStatus {
    Passed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentReadinessCheck {
    pub id: String,
    pub status: AgentReadinessCheckStatus,
    pub summary: String,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub evidence: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentReadinessReport {
    pub status: AgentReadinessStatus,
    pub workspace_root: String,
    pub checks: Vec<AgentReadinessCheck>,
}

impl AgentReadinessReport {
    pub fn is_ready(&self) -> bool {
        self.status == AgentReadinessStatus::Ready
    }

    pub fn failed_checks(&self) -> impl Iterator<Item = &AgentReadinessCheck> {
        self.checks
            .iter()
            .filter(|check| check.status == AgentReadinessCheckStatus::Failed)
    }
}

// Keeping each probe on its own line makes this long diagnostic catalog easier
// to audit and extend than one very large `vec!` initializer.
#[allow(clippy::vec_init_then_push)]
pub fn run_local_agent_readiness_gate(workspace_root: impl Into<PathBuf>) -> AgentReadinessReport {
    let workspace_root = workspace_root.into();
    let mut checks = Vec::new();
    checks.push(check_built_in_package_validation());
    checks.push(check_package_json_round_trip());
    checks.push(check_package_artifact_store_smoke());
    checks.push(check_package_rejects_malformed_definitions());
    checks.push(check_reserved_identity_and_role_exposure_smoke());
    checks.push(check_model_derived_context_budget_smoke());
    checks.push(check_continuous_runtime_limits_smoke());
    checks.push(check_prompt_decomposition_smoke());
    checks.push(check_agent_architecture_doc_smoke(&workspace_root));
    checks.push(check_provider_boundary_smoke(&workspace_root));
    checks.push(check_tool_surface_compatibility_smoke());
    checks.push(check_local_tool_readiness(
        "coding-agent",
        AgentDefinition::coding(),
        &workspace_root,
    ));
    checks.push(check_local_tool_readiness(
        "evolution-agent",
        AgentDefinition::evolution(),
        &workspace_root,
    ));
    checks.push(check_progressive_disclosure_bridges(&workspace_root));
    checks.push(check_tool_disclosure_hidden_tool_guard_smoke(
        &workspace_root,
    ));
    checks.push(check_tool_projection_guard_smoke());
    checks.push(check_run_context_provenance_smoke(&workspace_root));
    checks.push(check_prompt_reference_bounding_smoke());
    checks.push(check_instruction_file_bounding_smoke());
    checks.push(check_context_attachment_bounding_smoke());
    checks.push(check_async_update_bounding_smoke());
    checks.push(check_empty_optional_context_budget_smoke());
    checks.push(check_git_diff_large_output_disclosure_smoke());
    checks.push(check_tool_cancellation_boundary_smoke(&workspace_root));
    checks.push(check_version_control_active_cancellation_smoke());
    checks.push(run_agent_simple_sandbox_active_cancellation_readiness());
    checks.push(check_sandbox_container_active_cancellation_smoke());
    checks.push(check_host_gateway_active_cancellation_smoke());
    checks.push(check_remote_object_rpc_active_cancellation_smoke());
    checks.push(check_skill_disclosure_smoke());
    checks.push(check_module_projection_disclosure_smoke());
    checks.push(check_json_decision_contract());
    checks.push(check_model_response_repair_smoke());
    checks.push(check_provider_error_classification_smoke());
    checks.push(check_native_tool_adapter_smoke());
    checks.push(check_model_metrics_smoke(&workspace_root));
    checks.push(check_tool_pair_integrity_smoke());
    checks.push(check_compaction_projection_smoke());
    checks.push(check_compaction_eval_harness_smoke(&workspace_root));
    checks.push(check_session_snapshot_resume_smoke());
    checks.push(check_agent_runtime_service_routes());
    checks.push(check_agent_runtime_empty_routes_smoke());
    checks.push(check_agent_runtime_persisted_routes_smoke());
    checks.push(check_agent_runtime_control_service_routes());
    checks.push(check_agent_runtime_live_reattach_smoke());
    checks.push(check_agent_runtime_resume_metadata_fallback());
    checks.push(check_agent_runtime_resume_missing_metadata());
    checks.push(check_coding_state_projection_bounds_smoke());
    checks.push(check_coding_state_environment_validation_smoke());
    checks.push(check_coding_state_missing_dependency_validation_smoke());
    checks.push(check_coding_state_terminal_blocker_smoke());
    checks.push(check_model_visible_tool_call_input_bounding_smoke());
    checks.push(check_model_visible_read_file_bounding_smoke());
    checks.push(check_model_visible_tool_results_aggregate_budget_smoke());
    checks.push(check_local_command_evidence_smoke(&workspace_root));
    checks.push(check_sifs_command_evidence_smoke(&workspace_root));
    checks.push(check_model_visible_evidence_redaction_smoke(
        &workspace_root,
    ));
    let status = if checks
        .iter()
        .any(|check| check.status == AgentReadinessCheckStatus::Failed)
    {
        AgentReadinessStatus::Failed
    } else {
        AgentReadinessStatus::Ready
    };
    AgentReadinessReport {
        status,
        workspace_root: workspace_root.display().to_string(),
        checks,
    }
}

/// Readiness registries execute only commands fixed by this local diagnostic
/// harness. Explicitly grant its probes without weakening production agent
/// registries, which remain fail-closed until their host configures a checker.
fn fixed_command_readiness_registry(config: LocalWorkspaceAgentConfig) -> AgentToolRegistry {
    AgentToolRegistry::local_workspace_with_config(config).with_local_permission_checker(
        |permission: LocalNodePermission| Ok(permission == LocalNodePermission::RemoteManagement),
    )
}

fn passed(
    id: impl Into<String>,
    summary: impl Into<String>,
    evidence: serde_json::Value,
) -> AgentReadinessCheck {
    AgentReadinessCheck {
        id: id.into(),
        status: AgentReadinessCheckStatus::Passed,
        summary: summary.into(),
        evidence,
    }
}

fn failed(
    id: impl Into<String>,
    summary: impl Into<String>,
    evidence: serde_json::Value,
) -> AgentReadinessCheck {
    AgentReadinessCheck {
        id: id.into(),
        status: AgentReadinessCheckStatus::Failed,
        summary: summary.into(),
        evidence,
    }
}

fn check_built_in_package_validation() -> AgentReadinessCheck {
    let catalog = AgentCatalog::built_ins();
    let mut ids = Vec::new();
    let mut failures = Vec::new();
    for definition in catalog.list() {
        ids.push(definition.key().to_string());
        if let Err(error) = definition.validate() {
            failures.push(serde_json::json!({
                "agent": definition.key(),
                "error": error.to_string(),
            }));
        }
    }
    ids.sort();
    if failures.is_empty() {
        passed(
            "built_in_package_validation",
            "built-in agent packages validate",
            serde_json::json!({ "agents": ids }),
        )
    } else {
        failed(
            "built_in_package_validation",
            "one or more built-in agent packages failed validation",
            serde_json::json!({ "agents": ids, "failures": failures }),
        )
    }
}

fn check_reserved_identity_and_role_exposure_smoke() -> AgentReadinessCheck {
    let mut catalog = AgentCatalog::built_ins();
    let forged = AgentDefinition::custom("main-agent", AgentRole::Main, "Replace main.");
    let reserved_rejected = matches!(
        catalog.upsert(forged),
        Err(AgentDefinitionValidationError::ReservedBuiltInId { .. })
    );
    let canonical_preserved = catalog.get("main-agent") == Some(&AgentDefinition::main());
    let interactive = AgentDefinition::built_ins()
        .into_iter()
        .filter(AgentDefinition::supports_interactive_chat)
        .map(|definition| definition.key().to_string())
        .collect::<Vec<_>>();
    let roles_correct = interactive == vec!["main-agent", "coding-agent"];

    if reserved_rejected && canonical_preserved && roles_correct {
        passed(
            "reserved_identity_and_role_exposure_smoke",
            "built-in principals cannot be shadowed and only interactive roles reach chat selectors",
            serde_json::json!({
                "reserved_id": "main-agent",
                "interactive_agents": interactive,
                "dedicated_execution_roles": ["background", "evolution"],
            }),
        )
    } else {
        failed(
            "reserved_identity_and_role_exposure_smoke",
            "built-in identity reservation or role-aware exposure regressed",
            serde_json::json!({
                "reserved_rejected": reserved_rejected,
                "canonical_preserved": canonical_preserved,
                "interactive_agents": interactive,
            }),
        )
    }
}

fn check_model_derived_context_budget_smoke() -> AgentReadinessCheck {
    let main = AgentConfig::new("readiness", "unknown-model");
    let coding = main
        .clone()
        .with_agent_definition(AgentDefinition::coding());
    let main_max = main.context_budget.max_context_bytes;
    let coding_max = coding.context_budget.max_context_bytes;
    let bounded = main_max.is_some()
        && main.context_budget.compaction_threshold_bytes().is_some()
        && coding_max.is_some();
    let output_reserve_applied = main_max
        .zip(coding_max)
        .is_some_and(|(main, coding)| main < coding);

    if bounded && output_reserve_applied {
        passed(
            "model_derived_context_budget_smoke",
            "every turn receives a model-derived context bound with output-token reserve and compaction threshold",
            serde_json::json!({
                "unknown_model_fallback_max_bytes": main_max,
                "coding_max_bytes": coding_max,
                "main_output_reserve_tokens": main.max_output_tokens,
                "compaction_threshold_bytes": main.context_budget.compaction_threshold_bytes(),
            }),
        )
    } else {
        failed(
            "model_derived_context_budget_smoke",
            "model-derived per-turn context bounds are incomplete",
            serde_json::json!({
                "main_max_bytes": main_max,
                "coding_max_bytes": coding_max,
                "main_compaction_threshold_bytes": main.context_budget.compaction_threshold_bytes(),
                "bounded": bounded,
                "output_reserve_applied": output_reserve_applied,
            }),
        )
    }
}

fn check_prompt_decomposition_smoke() -> AgentReadinessCheck {
    let forbidden_phrases = [
        "knowledge cutoff",
        "current model",
        "model version",
        "runtime profile",
        "runtime-profile",
        "tool policy",
        "tool execution, policy",
    ];
    let mut checked_prompts = Vec::new();
    let mut failures = Vec::new();
    for definition in AgentDefinition::built_ins() {
        let id = definition.key().to_string();
        let prompt = normalize_prompt_text(&definition.system_prompt);
        checked_prompts.push(id.clone());
        for phrase in forbidden_phrases {
            if prompt.contains(phrase) {
                failures.push(serde_json::json!({
                    "agent": id,
                    "phrase": phrase,
                    "error": "package prompt contains runtime/profile/tool-policy wording",
                }));
            }
        }
        if matches!(
            definition.role,
            AgentRole::Deliberation | AgentRole::Evolution
        ) && !prompt.contains("tool-surface availability")
        {
            failures.push(serde_json::json!({
                "agent": id,
                "phrase": "tool-surface availability",
                "error": "package prompt does not name tool-surface availability",
            }));
        }
    }
    checked_prompts.sort();
    if failures.is_empty() {
        passed(
            "prompt_decomposition_smoke",
            "built-in package prompts keep runtime facts and tool availability terminology separated",
            serde_json::json!({
                "checked_prompts": checked_prompts,
                "forbidden_phrases": forbidden_phrases,
            }),
        )
    } else {
        failed(
            "prompt_decomposition_smoke",
            "one or more built-in package prompts contain legacy runtime/profile/tool-policy wording",
            serde_json::json!({
                "checked_prompts": checked_prompts,
                "failures": failures,
            }),
        )
    }
}

fn check_agent_architecture_doc_smoke(workspace_root: &Path) -> AgentReadinessCheck {
    let docs_path = agent_system_architecture_path(workspace_root);
    let docs = match fs::read_to_string(&docs_path) {
        Ok(docs) => docs,
        Err(error) => {
            return failed(
                "agent_architecture_doc_smoke",
                "agent-system architecture doc could not be read for terminology checks",
                serde_json::json!({
                    "path": docs_path,
                    "error": error.to_string(),
                }),
            );
        }
    };
    let forbidden_terms = [
        "ToolApproval",
        "ToolApprovalDecision",
        "AgentPolicyDecision",
    ];
    let mut failures = forbidden_terms
        .iter()
        .filter(|term| docs.contains(**term))
        .map(|term| {
            serde_json::json!({
                "term": term,
                "error": "architecture doc uses obsolete tool approval or agent policy terminology",
            })
        })
        .collect::<Vec<_>>();
    let required_markers = [
        "single source of truth",
        "AgentDefinition` is the agent package artifact",
        "There is no extra runtime profile layer",
        "Provider-specific behavior belongs in `sinomo-lm`",
        "The primary availability surface is `AgentToolSurface`",
        "`sinomo agent readiness [--json]` runs the deterministic local readiness gate",
        "Model quality is eval work, not ordinary Rust unit testing",
        "Self-evolution is outside the agent runtime core",
    ];
    failures.extend(
        required_markers
            .iter()
            .filter(|marker| !docs.contains(**marker))
            .map(|marker| {
                serde_json::json!({
                    "marker": marker,
                    "error": "single-source agent architecture doc is missing required current-implementation coverage",
                })
            }),
    );
    let removed_docs = [
        workspace_file_path(workspace_root, &["docs", "agent-system-implementation.md"]),
        workspace_file_path(
            workspace_root,
            &["docs", "plans", "coding-agent-improvement-plan.md"],
        ),
    ];
    failures.extend(
        removed_docs
            .iter()
            .filter(|path| path.exists())
            .map(|path| {
                serde_json::json!({
                    "path": path,
                    "error": "agent-system doc cleanup expects architecture doc to be the only agent-system source",
                })
            }),
    );
    if failures.is_empty() {
        passed(
            "agent_architecture_doc_smoke",
            "agent-system architecture doc is the single current source for package, tool-surface, provider, readiness, compaction, and self-evolution boundaries",
            serde_json::json!({
                "path": docs_path,
                "forbidden_terms": forbidden_terms,
                "required_markers": required_markers,
                "removed_docs": removed_docs,
            }),
        )
    } else {
        failed(
            "agent_architecture_doc_smoke",
            "agent-system architecture doc cleanup checks failed",
            serde_json::json!({
                "path": docs_path,
                "failures": failures,
            }),
        )
    }
}

fn agent_system_architecture_path(workspace_root: &Path) -> PathBuf {
    workspace_file_path(workspace_root, &["docs", "agent-system-architecture.md"])
}

fn check_provider_boundary_smoke(workspace_root: &Path) -> AgentReadinessCheck {
    let agent_source_path = agent_source_file_path(workspace_root, "agent.rs");
    let source = match fs::read_to_string(&agent_source_path) {
        Ok(source) => source,
        Err(error) => {
            return failed(
                "provider_boundary_smoke",
                "agent source could not be read for provider-boundary checks",
                serde_json::json!({
                    "path": agent_source_path,
                    "error": error.to_string(),
                }),
            );
        }
    };
    let required_delegation = "language_model_system_instructions(";
    let forbidden_symbols = [
        "prefers_native_tool_protocol(",
        "language_model_prefers_native_tool_protocol(",
    ];
    let mut failures = Vec::new();
    if !source.contains(required_delegation) {
        failures.push(serde_json::json!({
            "symbol": required_delegation,
            "error": "agent request rendering no longer delegates provider protocol instructions to sinomo-lm",
        }));
    }
    failures.extend(
        forbidden_symbols
            .iter()
            .filter(|symbol| source.contains(**symbol))
            .map(|symbol| {
                serde_json::json!({
                    "symbol": symbol,
                    "error": "provider or model protocol preference logic belongs in sinomo-lm, not sinomo-agent",
                })
            }),
    );
    if failures.is_empty() {
        passed(
            "provider_boundary_smoke",
            "sinomo-agent delegates provider protocol instructions to sinomo-lm",
            serde_json::json!({
                "path": agent_source_path,
                "required_delegation": required_delegation,
                "forbidden_symbols": forbidden_symbols,
            }),
        )
    } else {
        failed(
            "provider_boundary_smoke",
            "sinomo-agent provider-boundary checks failed",
            serde_json::json!({
                "path": agent_source_path,
                "failures": failures,
            }),
        )
    }
}

fn agent_source_file_path(workspace_root: &Path, file_name: &str) -> PathBuf {
    let repo_style = workspace_file_path(
        workspace_root,
        &["crates", "sinomo-agent", "src", file_name],
    );
    if repo_style.exists() {
        repo_style
    } else {
        workspace_root.join("src").join(file_name)
    }
}

fn workspace_file_path(workspace_root: &Path, components: &[&str]) -> PathBuf {
    let mut direct = workspace_root.to_path_buf();
    for component in components {
        direct.push(component);
    }
    if direct.exists() {
        return direct;
    }
    workspace_root
        .parent()
        .and_then(Path::parent)
        .map(|root| {
            let mut path = root.to_path_buf();
            for component in components {
                path.push(component);
            }
            path
        })
        .unwrap_or(direct)
}

fn normalize_prompt_text(prompt: &str) -> String {
    prompt
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn check_tool_surface_compatibility_smoke() -> AgentReadinessCheck {
    let rendered_surface = serde_json::to_value(
        AgentToolSurface::new("main-default", AgentRole::Main).with_denied_tools(["run_command"]),
    )
    .expect("agent tool surface serializes");
    let legacy_surface: AgentToolSurface = match serde_json::from_value(serde_json::json!({
        "policy_id": "legacy-main",
        "role": "main",
        "tools": {
            "denied_tools": ["run_command"]
        }
    })) {
        Ok(surface) => surface,
        Err(error) => {
            return failed(
                "tool_surface_compatibility_smoke",
                "legacy tool-surface persisted aliases failed to deserialize",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let rendered_evidence = serde_json::to_value(AgentToolEvidence::availability_only(
        false,
        "hidden by surface",
    ))
    .expect("agent tool evidence serializes");
    let legacy_evidence: AgentToolEvidence = match serde_json::from_value(serde_json::json!({
        "policy": {
            "allowed": false,
            "reason": "legacy hidden tool"
        }
    })) {
        Ok(evidence) => evidence,
        Err(error) => {
            return failed(
                "tool_surface_compatibility_smoke",
                "legacy tool-evidence persisted aliases failed to deserialize",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };

    let primary_surface_names = rendered_surface.get("surface_id").is_some()
        && rendered_surface.get("policy_id").is_none()
        && rendered_evidence.get("availability").is_some()
        && rendered_evidence.get("policy").is_none();
    let legacy_aliases_load = legacy_surface.surface_id == "legacy-main"
        && legacy_surface.tools.denied_tools == vec!["run_command".to_string()]
        && !legacy_evidence.availability.allowed
        && legacy_evidence.availability.reason == "legacy hidden tool";

    if primary_surface_names && legacy_aliases_load {
        passed(
            "tool_surface_compatibility_smoke",
            "tool-surface and availability names are primary while persisted legacy aliases still load",
            serde_json::json!({
                "primary_surface_field": "surface_id",
                "primary_evidence_field": "availability",
                "legacy_input_aliases": ["policy_id", "policy"],
                "deprecated_type_aliases": [
                    "AgentPolicyBundle",
                    "AgentToolPolicy",
                    "AgentToolPolicyDecision"
                ],
            }),
        )
    } else {
        failed(
            "tool_surface_compatibility_smoke",
            "tool-surface primary names or persisted legacy aliases regressed",
            serde_json::json!({
                "rendered_surface": rendered_surface,
                "legacy_surface_id": legacy_surface.surface_id,
                "rendered_evidence": rendered_evidence,
                "legacy_evidence_availability": legacy_evidence.availability,
                "primary_surface_names": primary_surface_names,
                "legacy_aliases_load": legacy_aliases_load,
            }),
        )
    }
}

fn check_continuous_runtime_limits_smoke() -> AgentReadinessCheck {
    let mut checked_agents = Vec::new();
    let mut failures = Vec::new();
    let base_config = AgentConfig::new("readiness", "continuous-defaults");
    if base_config.max_steps.is_some()
        || base_config.max_tool_calls_per_step.is_some()
        || base_config.max_model_repair_attempts.is_some()
    {
        failures.push(serde_json::json!({
            "agent": "AgentConfig::new",
            "max_steps": base_config.max_steps,
            "max_tool_calls_per_step": base_config.max_tool_calls_per_step,
            "max_model_repair_attempts": base_config.max_model_repair_attempts,
            "error": "default agent config imposes a runtime limit",
        }));
    }

    for definition in AgentDefinition::built_ins() {
        let id = definition.key().to_string();
        let config = AgentConfig::new("readiness", "continuous-defaults")
            .with_agent_definition(definition.clone());
        checked_agents.push(id.clone());
        if !definition.runtime_limits.is_default() {
            failures.push(serde_json::json!({
                "agent": id,
                "runtime_limits": definition.runtime_limits,
                "error": "built-in package declares a default runtime limit",
            }));
        }
        if config.max_steps.is_some()
            || config.max_tool_calls_per_step.is_some()
            || config.max_model_repair_attempts.is_some()
        {
            failures.push(serde_json::json!({
                "agent": definition.key(),
                "max_steps": config.max_steps,
                "max_tool_calls_per_step": config.max_tool_calls_per_step,
                "max_model_repair_attempts": config.max_model_repair_attempts,
                "error": "effective built-in agent config imposes a runtime limit",
            }));
        }
    }
    checked_agents.sort();

    let capped_config = AgentConfig::new("readiness", "continuous-defaults")
        .with_max_steps(2)
        .with_max_tool_calls_per_step(3)
        .with_max_model_repair_attempts(1);
    let explicit_caps_work = capped_config.max_steps == Some(2)
        && capped_config.max_tool_calls_per_step == Some(3)
        && capped_config.max_model_repair_attempts == Some(1);
    if !explicit_caps_work {
        failures.push(serde_json::json!({
            "agent": "AgentConfig explicit caps",
            "max_steps": capped_config.max_steps,
            "max_tool_calls_per_step": capped_config.max_tool_calls_per_step,
            "max_model_repair_attempts": capped_config.max_model_repair_attempts,
            "error": "explicit runtime caps did not apply",
        }));
    }

    let mut capped_definition = AgentDefinition::custom(
        "readiness-capped-agent",
        AgentRole::Deliberation,
        "Use caps.",
    );
    capped_definition.runtime_limits.max_steps = Some(1);
    capped_definition.runtime_limits.max_tool_calls_per_step = Some(2);
    capped_definition.runtime_limits.max_model_repair_attempts = Some(3);
    let package_derived_config = AgentConfig::new("readiness", "continuous-defaults")
        .with_agent_definition(capped_definition.clone());
    let package_derived_value = match serde_json::to_value(&package_derived_config) {
        Ok(value) => {
            if value.get("max_steps_explicit_override").is_some()
                || value
                    .get("max_tool_calls_per_step_explicit_override")
                    .is_some()
                || value
                    .get("max_model_repair_attempts_explicit_override")
                    .is_some()
            {
                failures.push(serde_json::json!({
                    "agent": "AgentConfig package-derived cap persistence",
                    "config": value,
                    "error": "package-derived runtime caps serialized as explicit caller overrides",
                }));
            }
            Some(value)
        }
        Err(error) => {
            failures.push(serde_json::json!({
                "agent": "AgentConfig package-derived cap persistence",
                "error": format!("package-derived runtime caps did not serialize: {error}"),
            }));
            None
        }
    };
    if let Some(value) = package_derived_value {
        match serde_json::from_value::<AgentConfig>(value) {
            Ok(config) => {
                let swapped = config.with_agent_definition(AgentDefinition::coding());
                if swapped.max_steps.is_some()
                    || swapped.max_tool_calls_per_step.is_some()
                    || swapped.max_model_repair_attempts.is_some()
                {
                    failures.push(serde_json::json!({
                        "agent": "AgentConfig package-derived cap persistence",
                        "max_steps": swapped.max_steps,
                        "max_tool_calls_per_step": swapped.max_tool_calls_per_step,
                        "max_model_repair_attempts": swapped.max_model_repair_attempts,
                        "error": "persisted package-derived runtime caps became sticky after package swap",
                    }));
                }
            }
            Err(error) => {
                failures.push(serde_json::json!({
                    "agent": "AgentConfig package-derived cap persistence",
                    "error": format!("package-derived runtime caps did not deserialize: {error}"),
                }));
            }
        }
    }

    let swapped_to_unbounded = AgentConfig::new("readiness", "continuous-defaults")
        .with_agent_definition(capped_definition.clone())
        .with_agent_definition(AgentDefinition::coding());
    if swapped_to_unbounded.max_steps.is_some()
        || swapped_to_unbounded.max_tool_calls_per_step.is_some()
        || swapped_to_unbounded.max_model_repair_attempts.is_some()
    {
        failures.push(serde_json::json!({
            "agent": "AgentConfig package swap",
            "max_steps": swapped_to_unbounded.max_steps,
            "max_tool_calls_per_step": swapped_to_unbounded.max_tool_calls_per_step,
            "max_model_repair_attempts": swapped_to_unbounded.max_model_repair_attempts,
            "error": "package-derived runtime caps leaked after switching back to an unbounded package",
        }));
    }

    let explicit_override_config = AgentConfig::new("readiness", "continuous-defaults")
        .with_agent_definition(capped_definition.clone())
        .with_max_steps(1)
        .with_max_tool_calls_per_step(2)
        .with_max_model_repair_attempts(3);
    let explicit_override_value = match serde_json::to_value(&explicit_override_config) {
        Ok(value) => {
            let override_flags_persist = value
                .get("max_steps_explicit_override")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
                && value
                    .get("max_tool_calls_per_step_explicit_override")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
                && value
                    .get("max_model_repair_attempts_explicit_override")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true);
            if !override_flags_persist {
                failures.push(serde_json::json!({
                    "agent": "AgentConfig explicit cap persistence",
                    "config": value,
                    "error": "explicit runtime cap override markers were not serialized",
                }));
            }
            Some(value)
        }
        Err(error) => {
            failures.push(serde_json::json!({
                "agent": "AgentConfig explicit cap persistence",
                "error": format!("explicit runtime cap overrides did not serialize: {error}"),
            }));
            None
        }
    };
    if let Some(value) = explicit_override_value {
        match serde_json::from_value::<AgentConfig>(value) {
            Ok(config) => {
                let swapped = config.with_agent_definition(AgentDefinition::coding());
                if swapped.max_steps != Some(1)
                    || swapped.max_tool_calls_per_step != Some(2)
                    || swapped.max_model_repair_attempts != Some(3)
                {
                    failures.push(serde_json::json!({
                        "agent": "AgentConfig explicit cap persistence",
                        "max_steps": swapped.max_steps,
                        "max_tool_calls_per_step": swapped.max_tool_calls_per_step,
                        "max_model_repair_attempts": swapped.max_model_repair_attempts,
                        "error": "persisted explicit runtime cap overrides were lost after package swap",
                    }));
                }
            }
            Err(error) => {
                failures.push(serde_json::json!({
                    "agent": "AgentConfig explicit cap persistence",
                    "error": format!("explicit runtime cap overrides did not deserialize: {error}"),
                }));
            }
        }
    }

    let overridden_then_swapped = AgentConfig::new("readiness", "continuous-defaults")
        .with_agent_definition(capped_definition)
        .with_max_steps(1)
        .with_max_tool_calls_per_step(2)
        .with_max_model_repair_attempts(3)
        .with_agent_definition(AgentDefinition::coding());
    let explicit_caps_survive_swap = overridden_then_swapped.max_steps == Some(1)
        && overridden_then_swapped.max_tool_calls_per_step == Some(2)
        && overridden_then_swapped.max_model_repair_attempts == Some(3);
    if !explicit_caps_survive_swap {
        failures.push(serde_json::json!({
            "agent": "AgentConfig explicit cap package swap",
            "max_steps": overridden_then_swapped.max_steps,
            "max_tool_calls_per_step": overridden_then_swapped.max_tool_calls_per_step,
            "max_model_repair_attempts": overridden_then_swapped.max_model_repair_attempts,
            "error": "explicit runtime cap overrides were lost during package swap",
        }));
    }

    if failures.is_empty() {
        passed(
            "continuous_runtime_limits_smoke",
            "built-in agents keep continuous unbounded runtime limits by default while explicit caps remain opt-in",
            serde_json::json!({
                "checked_agents": checked_agents,
                "default_limit_policy": "unbounded-default",
                "explicit_cap_example": {
                    "max_steps": capped_config.max_steps,
                    "max_tool_calls_per_step": capped_config.max_tool_calls_per_step,
                    "max_model_repair_attempts": capped_config.max_model_repair_attempts,
                },
                "package_swap_policy": "definition-derived caps clear with the old package; explicit overrides survive",
                "persistence_policy": "package-derived caps stay package-derived across JSON; explicit override markers survive JSON",
            }),
        )
    } else {
        failed(
            "continuous_runtime_limits_smoke",
            "continuous agent runtime limits regressed",
            serde_json::json!({
                "checked_agents": checked_agents,
                "failures": failures,
            }),
        )
    }
}

fn check_package_json_round_trip() -> AgentReadinessCheck {
    let mut ids = Vec::new();
    let mut failures = Vec::new();
    for definition in AgentDefinition::built_ins() {
        ids.push(definition.key().to_string());
        match serde_json::to_vec(&definition)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<AgentDefinition>(&bytes).ok())
        {
            Some(round_trip) if round_trip.key() == definition.key() => {
                if let Err(error) = round_trip.validate() {
                    failures.push(serde_json::json!({
                        "agent": definition.key(),
                        "error": error.to_string(),
                    }));
                }
            }
            _ => failures.push(serde_json::json!({
                "agent": definition.key(),
                "error": "package JSON did not round-trip",
            })),
        }
    }
    ids.sort();
    if failures.is_empty() {
        passed(
            "package_json_round_trip",
            "agent packages serialize and import cleanly",
            serde_json::json!({ "agents": ids }),
        )
    } else {
        failed(
            "package_json_round_trip",
            "one or more agent packages failed JSON import/export",
            serde_json::json!({ "agents": ids, "failures": failures }),
        )
    }
}

fn check_package_artifact_store_smoke() -> AgentReadinessCheck {
    let root = std::env::temp_dir().join(format!(
        "sinomo-agent-package-readiness-{}",
        TaskId::generate()
    ));
    let data_dir = sinomo_runtime::local::SinomoDataDir::new(&root);
    let store = match AgentDefinitionSifsStore::open(Some(data_dir.root())) {
        Ok(store) => store,
        Err(error) => {
            let _ = fs::remove_dir_all(&root);
            return failed(
                "package_artifact_store_smoke",
                "SIFS-backed agent package store failed to open",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let source_path = "agent-packages/readiness-imported-agent.json";
    let definition = AgentDefinition::custom(
        "readiness-imported-agent",
        AgentRole::Deliberation,
        "Readiness package import/export smoke.",
    )
    .with_package_version("1.2.3")
    .with_package_provenance(AgentPackageProvenance::imported(source_path))
    .with_tool(AgentToolSpec::new(
        AgentToolKind::ReadFile,
        "read_file",
        "Read a workspace file.",
        false,
    ))
    .with_skill("readiness.skill")
    .with_temperature(Some(0.3))
    .with_max_output_tokens(Some(512));

    let saved_path = match store.save_definition(&definition) {
        Ok(path) => path,
        Err(error) => {
            return failed(
                "package_artifact_store_smoke",
                "SIFS-backed agent package save failed",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let loaded = match store.load_definitions() {
        Ok(definitions) => definitions,
        Err(error) => {
            return failed(
                "package_artifact_store_smoke",
                "SIFS-backed agent package load failed",
                serde_json::json!({
                    "saved_path": saved_path,
                    "error": error.to_string(),
                }),
            );
        }
    };
    let loaded_definition = loaded
        .iter()
        .find(|candidate| candidate.key() == definition.key());
    let catalog = match AgentCatalog::load_from_data_dir(&data_dir) {
        Ok(catalog) => catalog,
        Err(error) => {
            return failed(
                "package_artifact_store_smoke",
                "agent catalog failed to load SIFS-backed package",
                serde_json::json!({
                    "saved_path": saved_path,
                    "error": error.to_string(),
                }),
            );
        }
    };
    let catalog_definition = catalog.get(definition.key());
    let saved_in_sifs = loaded_definition.is_some();
    let expected_sifs_path = format!("{}/{}.json", AGENT_DEFINITION_SIFS_ROOT, definition.key());
    let saved_path_matches = saved_path == expected_sifs_path;
    let loaded_matches = loaded_definition.is_some_and(|loaded| {
        loaded.package.version == "1.2.3"
            && loaded.package.provenance.source == AgentPackageSource::Imported
            && loaded.package.provenance.path.as_deref() == Some(source_path)
            && loaded.tools.iter().any(|tool| tool.name == "read_file")
            && loaded.skills == vec!["readiness.skill".to_string()]
            && loaded.model_policy.temperature == Some(0.3)
            && loaded.model_policy.max_output_tokens == Some(512)
    });
    let catalog_matches = catalog_definition.is_some_and(|loaded| {
        loaded.package.provenance.source == AgentPackageSource::Imported
            && loaded.package.provenance.path.as_deref() == Some(source_path)
    });
    let _ = fs::remove_dir_all(&root);

    if saved_in_sifs && saved_path_matches && loaded_matches && catalog_matches {
        passed(
            "package_artifact_store_smoke",
            "SIFS-backed agent package artifacts save, load, and merge into the catalog with import provenance",
            serde_json::json!({
                "agent": definition.key(),
                "saved_path": expected_sifs_path,
                "package_version": "1.2.3",
                "provenance": "imported",
                "source_path": source_path,
            }),
        )
    } else {
        failed(
            "package_artifact_store_smoke",
            "SIFS-backed agent package artifact round-trip lost package data",
            serde_json::json!({
                "agent": definition.key(),
                "saved_path": saved_path,
                "saved_in_sifs": saved_in_sifs,
                "saved_path_matches": saved_path_matches,
                "loaded_matches": loaded_matches,
                "catalog_matches": catalog_matches,
                "loaded_count": loaded.len(),
            }),
        )
    }
}

fn check_package_rejects_malformed_definitions() -> AgentReadinessCheck {
    let mut invalid_package_version =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    invalid_package_version.package.version = "bad version".to_string();

    let mut missing_provenance_module_id =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    missing_provenance_module_id.package.provenance.source = AgentPackageSource::Module;

    let mut invalid_provenance_module_id =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    invalid_provenance_module_id.package.provenance.source = AgentPackageSource::Module;
    invalid_provenance_module_id.package.provenance.module_id = Some("bad module".to_string());

    let mut invalid_tool_name =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    invalid_tool_name.tools = vec![AgentToolSpec::new(
        AgentToolKind::ReadFile,
        "bad tool",
        "Read a file.",
        false,
    )];

    let mut duplicate_tool_name =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    duplicate_tool_name.tools = vec![
        AgentToolSpec::new(AgentToolKind::ReadFile, "read_file", "Read a file.", false),
        AgentToolSpec::new(
            AgentToolKind::RunShellCommand,
            "read_file",
            "Run a command.",
            true,
        ),
    ];

    let mut invalid_tool_surface =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    invalid_tool_surface.tool_surface.visible_toolsets = vec!["bad toolset".to_string()];

    let mut invalid_skill =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    invalid_skill.skills = vec!["bad skill".to_string()];

    let mut invalid_context_provider =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    invalid_context_provider.context.providers = vec!["bad provider".to_string()];

    let mut unsupported_context_provider =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    unsupported_context_provider.context.providers = vec!["context.provider.valid".to_string()];

    let mut invalid_model_preference =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    invalid_model_preference.model_policy.provider = Some("bad provider".to_string());

    let mut invalid_temperature =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    invalid_temperature.temperature = Some(-0.1);

    let mut conflicting_model_policy =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    conflicting_model_policy.temperature = Some(0.1);
    conflicting_model_policy.model_policy.temperature = Some(0.2);

    let mut unsupported_vendor_options =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    unsupported_vendor_options.model_policy.vendor_options = serde_json::json!({"effort": "high"});

    let mut invalid_runtime_limit =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    invalid_runtime_limit.runtime_limits.max_steps = Some(0);

    let exhausted_context_window =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.")
            .with_max_output_tokens(Some(32 * 1024));

    let mut invalid_tool_call_limit =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    invalid_tool_call_limit
        .runtime_limits
        .max_tool_calls_per_step = Some(0);

    let mut unknown_context_pipeline =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    unknown_context_pipeline.context.pipeline_id = "context.pipeline.missing".to_string();

    let mut unknown_compaction_strategy =
        AgentDefinition::custom("malformed-agent", AgentRole::Main, "Be helpful.");
    unknown_compaction_strategy.compaction.strategy_id = "compaction.strategy.missing".to_string();

    let cases = [
        (
            "invalid_package_version",
            invalid_package_version,
            "InvalidPackageVersion",
        ),
        (
            "missing_provenance_module_id",
            missing_provenance_module_id,
            "MissingProvenanceModuleId",
        ),
        (
            "invalid_provenance_module_id",
            invalid_provenance_module_id,
            "InvalidProvenanceModuleId",
        ),
        ("invalid_tool_name", invalid_tool_name, "InvalidToolName"),
        (
            "duplicate_tool_name",
            duplicate_tool_name,
            "DuplicateToolName",
        ),
        (
            "invalid_tool_surface_entry",
            invalid_tool_surface,
            "InvalidToolSurfaceEntry",
        ),
        ("invalid_skill", invalid_skill, "InvalidSkill"),
        (
            "invalid_context_provider",
            invalid_context_provider,
            "InvalidContextProvider",
        ),
        (
            "unsupported_context_provider",
            unsupported_context_provider,
            "UnsupportedContextProviders",
        ),
        (
            "invalid_model_preference",
            invalid_model_preference,
            "InvalidModelPreference",
        ),
        (
            "invalid_temperature",
            invalid_temperature,
            "InvalidTemperature",
        ),
        (
            "conflicting_model_policy",
            conflicting_model_policy,
            "ConflictingModelPolicyField",
        ),
        (
            "unsupported_vendor_options",
            unsupported_vendor_options,
            "UnsupportedModelVendorOptions",
        ),
        (
            "invalid_runtime_limit",
            invalid_runtime_limit,
            "InvalidRuntimeLimit",
        ),
        (
            "exhausted_context_window",
            exhausted_context_window,
            "OutputReserveExhaustsContextWindow",
        ),
        (
            "invalid_tool_call_limit",
            invalid_tool_call_limit,
            "InvalidRuntimeLimit",
        ),
        (
            "unknown_context_pipeline",
            unknown_context_pipeline,
            "UnknownContextPipeline",
        ),
        (
            "unknown_compaction_strategy",
            unknown_compaction_strategy,
            "UnknownCompactionStrategy",
        ),
    ];

    let mut rejected = Vec::new();
    let mut failures = Vec::new();
    for (case, definition, expected_error) in cases {
        match definition.validate() {
            Err(error) if agent_definition_validation_error_kind(&error) == expected_error => {
                rejected.push(serde_json::json!({
                    "case": case,
                    "error_kind": expected_error,
                }));
            }
            Err(error) => failures.push(serde_json::json!({
                "case": case,
                "expected_error_kind": expected_error,
                "actual_error_kind": agent_definition_validation_error_kind(&error),
                "error": error.to_string(),
            })),
            Ok(()) => failures.push(serde_json::json!({
                "case": case,
                "expected_error_kind": expected_error,
                "error": "malformed package validated successfully",
            })),
        }
    }

    if failures.is_empty() {
        passed(
            "package_rejects_malformed_definitions",
            "malformed agent packages are rejected across metadata, provenance, tool, skill, model, context, compaction, and runtime-limit fields",
            serde_json::json!({ "rejected": rejected }),
        )
    } else {
        failed(
            "package_rejects_malformed_definitions",
            "one or more malformed agent package fields were not rejected as expected",
            serde_json::json!({
                "rejected": rejected,
                "failures": failures,
            }),
        )
    }
}

fn agent_definition_validation_error_kind(error: &AgentDefinitionValidationError) -> &'static str {
    match error {
        AgentDefinitionValidationError::MissingId => "MissingId",
        AgentDefinitionValidationError::InvalidId { .. } => "InvalidId",
        AgentDefinitionValidationError::ReservedBuiltInId { .. } => "ReservedBuiltInId",
        AgentDefinitionValidationError::MissingName { .. } => "MissingName",
        AgentDefinitionValidationError::MissingSystemPrompt { .. } => "MissingSystemPrompt",
        AgentDefinitionValidationError::MissingPackageVersion { .. } => "MissingPackageVersion",
        AgentDefinitionValidationError::InvalidPackageVersion { .. } => "InvalidPackageVersion",
        AgentDefinitionValidationError::InvalidToolName { .. } => "InvalidToolName",
        AgentDefinitionValidationError::DuplicateToolName { .. } => "DuplicateToolName",
        AgentDefinitionValidationError::InvalidToolSurfaceEntry { .. } => "InvalidToolSurfaceEntry",
        AgentDefinitionValidationError::InvalidSkill { .. } => "InvalidSkill",
        AgentDefinitionValidationError::InvalidContextProvider { .. } => "InvalidContextProvider",
        AgentDefinitionValidationError::UnsupportedContextProviders { .. } => {
            "UnsupportedContextProviders"
        }
        AgentDefinitionValidationError::InvalidModelPreference { .. } => "InvalidModelPreference",
        AgentDefinitionValidationError::InvalidTemperature { .. } => "InvalidTemperature",
        AgentDefinitionValidationError::ConflictingModelPolicyField { .. } => {
            "ConflictingModelPolicyField"
        }
        AgentDefinitionValidationError::UnsupportedModelVendorOptions { .. } => {
            "UnsupportedModelVendorOptions"
        }
        AgentDefinitionValidationError::InvalidRuntimeLimit { .. } => "InvalidRuntimeLimit",
        AgentDefinitionValidationError::OutputReserveExhaustsContextWindow { .. } => {
            "OutputReserveExhaustsContextWindow"
        }
        AgentDefinitionValidationError::InvalidSandboxRuntimeNodeId { .. } => {
            "InvalidSandboxRuntimeNodeId"
        }
        AgentDefinitionValidationError::InvalidMcpServer { .. } => "InvalidMcpServer",
        AgentDefinitionValidationError::DuplicateMcpServer { .. } => "DuplicateMcpServer",
        AgentDefinitionValidationError::InvalidHomeServerNodeId { .. } => "InvalidHomeServerNodeId",
        AgentDefinitionValidationError::MissingProvenanceModuleId { .. } => {
            "MissingProvenanceModuleId"
        }
        AgentDefinitionValidationError::InvalidProvenanceModuleId { .. } => {
            "InvalidProvenanceModuleId"
        }
        AgentDefinitionValidationError::UnknownContextPipeline { .. } => "UnknownContextPipeline",
        AgentDefinitionValidationError::UnknownCompactionStrategy { .. } => {
            "UnknownCompactionStrategy"
        }
        AgentDefinitionValidationError::UnknownRole { .. } => "UnknownRole",
        AgentDefinitionValidationError::UnknownToolKind { .. } => "UnknownToolKind",
    }
}

fn check_local_tool_readiness(
    id: &str,
    definition: AgentDefinition,
    workspace_root: &Path,
) -> AgentReadinessCheck {
    let registry = readiness_local_registry(workspace_root);
    let plan = AgentToolPlanner::plan(
        registry.descriptors(),
        &AgentModuleToolProjection::new(),
        Vec::<AgentToolSpec>::new(),
        definition.tools.clone(),
        true,
        definition.tool_surface.visible_toolsets.clone(),
        registry.generation(),
    );
    let issues = plan.readiness_issues();
    let visible_tools = plan
        .model_visible_descriptors()
        .into_iter()
        .map(|descriptor| descriptor.spec.name)
        .collect::<Vec<_>>();
    let check_id = format!("local_tool_readiness.{id}");
    if issues.is_empty() {
        passed(
            check_id,
            format!("{id} visible local tools have executors"),
            serde_json::json!({
                "visible_tools": visible_tools,
                "registry_generation": plan.registry_generation,
            }),
        )
    } else {
        failed(
            check_id,
            format!("{id} has visible tools without local executors"),
            serde_json::json!({
                "visible_tools": visible_tools,
                "issues": issues,
            }),
        )
    }
}

fn check_progressive_disclosure_bridges(workspace_root: &Path) -> AgentReadinessCheck {
    let registry = readiness_local_registry(workspace_root);
    let definition = AgentDefinition::coding();
    let plan = AgentToolPlanner::plan(
        registry.descriptors(),
        &AgentModuleToolProjection::new(),
        Vec::<AgentToolSpec>::new(),
        definition.tools,
        true,
        Vec::<String>::new(),
        registry.generation(),
    );
    let bridge_tools = [
        "tool_search",
        "tool_describe",
        "tool_call",
        "skill_search",
        "skill_describe",
        "skill_activate",
    ];
    let missing = bridge_tools
        .iter()
        .filter(|tool| !plan.is_model_visible(tool) || !plan.is_executable(tool))
        .copied()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        passed(
            "progressive_disclosure_bridges",
            "tool and skill disclosure bridges are visible and executable",
            serde_json::json!({ "bridge_tools": bridge_tools }),
        )
    } else {
        failed(
            "progressive_disclosure_bridges",
            "one or more disclosure bridges are not visible and executable",
            serde_json::json!({ "missing_or_unexecutable": missing }),
        )
    }
}

fn check_tool_disclosure_hidden_tool_guard_smoke(workspace_root: &Path) -> AgentReadinessCheck {
    let registry = readiness_local_registry(workspace_root);
    let plan = AgentToolPlanner::plan(
        registry.descriptors(),
        &AgentModuleToolProjection::new(),
        Vec::<AgentToolSpec>::new(),
        Vec::<AgentToolSpec>::new(),
        false,
        ["discovery"],
        registry.generation(),
    );
    let session = readiness_session();
    let run_context = AgentRunContext::default();
    let tool_surface = AgentToolSurface::default_for_role(AgentRole::Deliberation);
    let orchestrator = AgentToolOrchestrator::new(&tool_surface, None, &run_context);
    let hidden_tool = "read_file";
    let hidden_planned = plan.entry(hidden_tool).is_some();
    let hidden_executable = plan.is_executable(hidden_tool);
    let hidden_model_visible = plan.is_model_visible(hidden_tool);
    let search = orchestrator.execute(
        &registry,
        &session,
        &plan,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "tool_search".to_string(),
            input: serde_json::json!({ "query": hidden_tool, "max_results": 5 }),
            r#async: false,
            summary: "readiness hidden tool search".to_string(),
        },
    );
    let describe = orchestrator.execute(
        &registry,
        &session,
        &plan,
        0,
        1,
        &AgentToolCall {
            call_id: None,
            tool: "tool_describe".to_string(),
            input: serde_json::json!({ "tool": hidden_tool }),
            r#async: false,
            summary: "readiness hidden tool describe".to_string(),
        },
    );
    let bridge_call = orchestrator.execute(
        &registry,
        &session,
        &plan,
        0,
        2,
        &AgentToolCall {
            call_id: None,
            tool: "tool_call".to_string(),
            input: serde_json::json!({
                "tool": hidden_tool,
                "input": { "path": "Cargo.toml", "max_bytes": 64 },
                "summary": "read Cargo manifest through disclosure bridge"
            }),
            r#async: false,
            summary: "readiness hidden tool bridge call".to_string(),
        },
    );
    let direct_call = orchestrator.execute(
        &registry,
        &session,
        &plan,
        0,
        3,
        &AgentToolCall {
            call_id: None,
            tool: hidden_tool.to_string(),
            input: serde_json::json!({ "path": "Cargo.toml", "max_bytes": 64 }),
            r#async: false,
            summary: "readiness guessed hidden tool".to_string(),
        },
    );
    let search_found_hidden =
        output_matches_tool_visibility(&search.output, hidden_tool, Some(false));
    let describe_found_hidden = describe
        .output
        .pointer("/descriptor/spec/name")
        .and_then(serde_json::Value::as_str)
        == Some(hidden_tool)
        && describe
            .output
            .pointer("/model_visible")
            .and_then(serde_json::Value::as_bool)
            == Some(false);
    let bridge_called_hidden = bridge_call.status == AgentToolStatus::Succeeded
        && bridge_call
            .output
            .pointer("/_tool_call/target_tool")
            .and_then(serde_json::Value::as_str)
            == Some(hidden_tool);
    let direct_guarded = direct_call.status == AgentToolStatus::Blocked
        && direct_call
            .output
            .get("error")
            .and_then(serde_json::Value::as_str)
            == Some("tool_not_visible")
        && !direct_call.evidence.availability.allowed;
    if hidden_planned
        && hidden_executable
        && !hidden_model_visible
        && search.status == AgentToolStatus::Succeeded
        && search_found_hidden
        && describe.status == AgentToolStatus::Succeeded
        && describe_found_hidden
        && bridge_called_hidden
        && direct_guarded
    {
        passed(
            "tool_disclosure_hidden_tool_guard_smoke",
            "hidden tools are discoverable through bridges and blocked when guessed directly",
            serde_json::json!({
                "tool": hidden_tool,
                "planned": hidden_planned,
                "executable": hidden_executable,
                "model_visible": hidden_model_visible,
                "search_found_hidden": search_found_hidden,
                "describe_found_hidden": describe_found_hidden,
                "bridge_call_status": bridge_call.status,
                "direct_call_status": direct_call.status,
                "direct_call_error": direct_call.output.get("error"),
                "direct_call_availability_allowed": direct_call.evidence.availability.allowed,
            }),
        )
    } else {
        failed(
            "tool_disclosure_hidden_tool_guard_smoke",
            "hidden tool disclosure or direct-call guard failed",
            serde_json::json!({
                "tool": hidden_tool,
                "planned": hidden_planned,
                "executable": hidden_executable,
                "model_visible": hidden_model_visible,
                "search_status": search.status,
                "search_output": search.output,
                "describe_status": describe.status,
                "describe_output": describe.output,
                "bridge_call_status": bridge_call.status,
                "bridge_call_output": bridge_call.output,
                "direct_call_status": direct_call.status,
                "direct_call_output": direct_call.output,
                "direct_call_availability": direct_call.evidence.availability,
            }),
        )
    }
}

fn check_tool_projection_guard_smoke() -> AgentReadinessCheck {
    let registry = AgentToolRegistry::new();
    let plan = AgentToolPlanner::plan(
        registry.descriptors(),
        &AgentModuleToolProjection::new(),
        Vec::<AgentToolSpec>::new(),
        [AgentToolSpec::new(
            AgentToolKind::ReadFile,
            "read_file",
            "Read a file declared by package metadata but not backed by this runtime.",
            false,
        )],
        true,
        ["coding"],
        registry.generation(),
    );
    let session = readiness_session();
    let run_context = AgentRunContext::default();
    let tool_surface = AgentToolSurface::default_for_role(AgentRole::Deliberation);
    let orchestrator = AgentToolOrchestrator::new(&tool_surface, None, &run_context);
    let missing_executor_call = orchestrator.execute(
        &registry,
        &session,
        &plan,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "read_file".to_string(),
            input: serde_json::json!({ "path": "Cargo.toml" }),
            r#async: false,
            summary: "readiness visible missing executor".to_string(),
        },
    );
    let stale_call = orchestrator.execute(
        &registry,
        &session,
        &plan,
        0,
        1,
        &AgentToolCall {
            call_id: None,
            tool: "stale_unplanned_tool".to_string(),
            input: serde_json::json!({}),
            r#async: false,
            summary: "readiness stale unplanned tool".to_string(),
        },
    );
    let missing_executor_guarded = missing_executor_call.status == AgentToolStatus::Blocked
        && missing_executor_call
            .output
            .get("error")
            .and_then(serde_json::Value::as_str)
            == Some("missing_executor")
        && !missing_executor_call.evidence.availability.allowed;
    let stale_failed_visible = stale_call.status == AgentToolStatus::Failed
        && stale_call
            .output
            .get("error")
            .and_then(serde_json::Value::as_str)
            == Some("unknown_tool")
        && stale_call.evidence.availability.allowed;
    let missing_executor_orchestration = missing_executor_call
        .evidence
        .metadata
        .get("tool_orchestration")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let stale_orchestration = stale_call
        .evidence
        .metadata
        .get("tool_orchestration")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let orchestration_preserves_projection = missing_executor_orchestration
        .get("registry_generation")
        .and_then(serde_json::Value::as_u64)
        == Some(plan.registry_generation)
        && stale_orchestration
            .get("registry_generation")
            .and_then(serde_json::Value::as_u64)
            == Some(plan.registry_generation)
        && missing_executor_orchestration
            .get("visible_toolsets")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|toolsets| {
                toolsets
                    .iter()
                    .any(|toolset| toolset.as_str() == Some("coding"))
            });
    if plan.is_model_visible("read_file")
        && !plan.is_executable("read_file")
        && missing_executor_guarded
        && stale_failed_visible
        && orchestration_preserves_projection
    {
        passed(
            "tool_projection_guard_smoke",
            "visible missing executors and stale unplanned tools produce typed model-visible failures with projection evidence",
            serde_json::json!({
                "registry_generation": plan.registry_generation,
                "visible_toolsets": plan.visible_toolsets,
                "missing_executor_tool": "read_file",
                "missing_executor_model_visible": plan.is_model_visible("read_file"),
                "missing_executor_executable": plan.is_executable("read_file"),
                "missing_executor_status": missing_executor_call.status,
                "missing_executor_error": missing_executor_call.output.get("error"),
                "missing_executor_orchestration": missing_executor_orchestration,
                "stale_tool": "stale_unplanned_tool",
                "stale_tool_status": stale_call.status,
                "stale_tool_error": stale_call.output.get("error"),
                "stale_tool_orchestration": stale_orchestration,
            }),
        )
    } else {
        failed(
            "tool_projection_guard_smoke",
            "tool projection guards did not produce expected typed failures",
            serde_json::json!({
                "missing_executor_model_visible": plan.is_model_visible("read_file"),
                "missing_executor_executable": plan.is_executable("read_file"),
                "missing_executor_status": missing_executor_call.status,
                "missing_executor_output": missing_executor_call.output,
                "missing_executor_availability": missing_executor_call.evidence.availability,
                "missing_executor_orchestration": missing_executor_orchestration,
                "stale_tool_status": stale_call.status,
                "stale_tool_output": stale_call.output,
                "stale_tool_availability": stale_call.evidence.availability,
                "stale_tool_orchestration": stale_orchestration,
            }),
        )
    }
}

fn check_run_context_provenance_smoke(workspace_root: &Path) -> AgentReadinessCheck {
    let task = AgentTaskRef::new("coding", "readiness run-context provenance")
        .with_owner_role(AgentRole::Deliberation);
    let workspace = WorkspaceRef::local_path(workspace_root)
        .with_owner_node("node.readiness.owner")
        .with_write_policy("replicated-confirmed");
    let placement = AgentPlacementMetadata::local_current_process()
        .with_origin_node("node.readiness.origin")
        .with_target_node("node.readiness.owner")
        .with_sandbox_class("local-process");
    let inspect_turn = serde_json::json!({
        "response_mode": "defer",
        "text": "I will inspect the package manifest.",
        "tool_calls": [{
            "tool": "read_file",
            "input": {
                "path": "Cargo.toml",
                "max_bytes": 16
            },
            "async": false,
            "summary": "Inspect package manifest."
        }],
        "background_handoff": null,
        "next_state": "waiting"
    })
    .to_string();
    let final_turn = serde_json::json!({
        "response_mode": "answer",
        "text": "Run context provenance was recorded.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    })
    .to_string();
    let model = ReadinessSequencedModel::new([inspect_turn, final_turn]);
    let requests = Arc::clone(&model.requests);
    let config = AgentConfig::new("readiness", "run-context").with_run_context(
        AgentRunContext::default()
            .with_task(task.clone())
            .with_workspace(workspace.clone())
            .with_placement(placement.clone()),
    );
    let agent = Agent::new(model, "readiness", "run-context")
        .with_config(config)
        .with_tool_registry(fixed_command_readiness_registry(
            LocalWorkspaceAgentConfig::new(workspace_root),
        ));
    let run = match agent.run(&readiness_agent_loop_input(
        "Inspect the workspace with provenance metadata.",
    )) {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "run_context_provenance_smoke",
                "run-context provenance run failed",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let requests = requests.lock().expect("readiness model requests");
    let first_request = requests.first();
    let first_request_text = first_request
        .map(|request| {
            request
                .messages
                .iter()
                .map(|message| message.content.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    let request_has_task = first_request.is_some_and(|request| {
        request.metadata["run_context"]["task"]["task_id"]
            .as_str()
            .is_some_and(|value| value == task.task_id.as_str())
            && request.metadata["run_context"]["task"]["kind"] == "coding"
    });
    let request_has_workspace = first_request.is_some_and(|request| {
        request.metadata["run_context"]["workspace"]["owner_node"] == "node.readiness.owner"
            && request.metadata["run_context"]["workspace"]["write_policy"]
                == "replicated-confirmed"
    });
    let request_has_placement = first_request.is_some_and(|request| {
        request.metadata["run_context"]["placement"]["origin_node"] == "node.readiness.origin"
            && request.metadata["run_context"]["placement"]["target_node"] == "node.readiness.owner"
            && request.metadata["run_context"]["placement"]["sandbox_class"] == "local-process"
    });
    let prompt_mentions_run_context = first_request_text.contains("\"run_context\"")
        && first_request_text.contains("readiness run-context provenance")
        && first_request_text.contains("node.readiness.owner")
        && first_request_text.contains("local-process");
    let event_has_run_context = run.events.iter().any(|event| {
        event.payload["run_context"]["task"]["task_id"]
            .as_str()
            .is_some_and(|value| value == task.task_id.as_str())
            && event.payload["run_context"]["workspace"]["owner_node"] == "node.readiness.owner"
            && event.payload["run_context"]["placement"]["target_node"] == "node.readiness.owner"
    });
    let orchestration = run
        .tool_results
        .first()
        .and_then(|outcome| outcome.evidence.metadata.get("tool_orchestration"));
    let tool_evidence_has_run_context = orchestration.is_some_and(|record| {
        record["run_context"]["task"]["task_id"]
            .as_str()
            .is_some_and(|value| value == task.task_id.as_str())
            && record["run_context"]["workspace"]["write_policy"] == "replicated-confirmed"
            && record["run_context"]["placement"]["sandbox_class"] == "local-process"
            && record["executor_available"] == true
    });

    if run.stop_reason == AgentStopReason::FinalDecision
        && run.model_turns.len() == 2
        && run.tool_results.len() == 1
        && run.tool_results[0].status == AgentToolStatus::Succeeded
        && run.run_context.task.as_ref().map(|task| &task.task_id) == Some(&task.task_id)
        && request_has_task
        && request_has_workspace
        && request_has_placement
        && prompt_mentions_run_context
        && event_has_run_context
        && tool_evidence_has_run_context
    {
        passed(
            "run_context_provenance_smoke",
            "task, workspace, and placement provenance reach model requests, events, and tool orchestration evidence",
            serde_json::json!({
                "task_id": task.task_id,
                "workspace_owner": workspace.owner_node,
                "workspace_write_policy": workspace.write_policy,
                "placement_origin": placement.origin_node,
                "placement_target": placement.target_node,
                "placement_sandbox_class": placement.sandbox_class,
                "model_turns": run.model_turns.len(),
                "tool_result_status": run.tool_results[0].status,
            }),
        )
    } else {
        failed(
            "run_context_provenance_smoke",
            "run-context provenance was not preserved across model request, events, and tool evidence",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "model_turns": run.model_turns.len(),
                "tool_results": run.tool_results.len(),
                "run_context": run.run_context,
                "request_has_task": request_has_task,
                "request_has_workspace": request_has_workspace,
                "request_has_placement": request_has_placement,
                "prompt_mentions_run_context": prompt_mentions_run_context,
                "event_has_run_context": event_has_run_context,
                "tool_evidence_has_run_context": tool_evidence_has_run_context,
                "first_request_metadata": first_request.map(|request| request.metadata.clone()),
                "tool_orchestration": orchestration,
            }),
        )
    }
}

fn check_prompt_reference_bounding_smoke() -> AgentReadinessCheck {
    let root = std::env::temp_dir().join(format!(
        "sinomo-agent-prompt-reference-readiness-{}",
        TaskId::generate()
    ));
    if let Err(error) = fs::create_dir_all(root.join("many")) {
        return failed(
            "prompt_reference_bounding_smoke",
            "failed to create prompt-reference readiness workspace",
            serde_json::json!({ "error": error.to_string(), "root": root }),
        );
    }
    let config =
        AgentConfig::new("readiness", "prompt-reference-bounding").with_workspace_root(&root);
    let Some(max_file_bytes) = config.prompt_reference_max_bytes else {
        return failed(
            "prompt_reference_bounding_smoke",
            "prompt-reference file cap is not configured by default",
            serde_json::json!({ "prompt_reference_max_bytes": null }),
        );
    };
    let Some(max_directory_entries) = config.prompt_reference_max_entries else {
        return failed(
            "prompt_reference_bounding_smoke",
            "prompt-reference directory cap is not configured by default",
            serde_json::json!({ "prompt_reference_max_entries": null }),
        );
    };
    let tail = "PROMPT_REFERENCE_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let note = format!(
        "{}{tail}{}",
        "p".repeat(max_file_bytes + 512),
        "P".repeat(1024)
    );
    if let Err(error) = fs::write(root.join("large.txt"), note) {
        return failed(
            "prompt_reference_bounding_smoke",
            "failed to write large prompt-reference file",
            serde_json::json!({ "error": error.to_string(), "root": root }),
        );
    }
    for index in 0..(max_directory_entries + 5) {
        if let Err(error) = fs::write(root.join("many").join(format!("{index:03}.txt")), "x") {
            return failed(
                "prompt_reference_bounding_smoke",
                "failed to seed prompt-reference directory",
                serde_json::json!({ "error": error.to_string(), "root": root, "index": index }),
            );
        }
    }
    let input = readiness_agent_loop_input("inspect @large.txt and @many");
    let agent = Agent::new(
        ReadinessSequencedModel::new(std::iter::empty::<String>()),
        "readiness",
        "prompt-reference-bounding",
    )
    .with_config(config);
    let context = match agent.assemble_context(&input, &[], 0) {
        Ok(context) => context,
        Err(error) => {
            return failed(
                "prompt_reference_bounding_smoke",
                "failed to assemble prompt-reference context",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let request = match agent.request(&input, &[], 0) {
        Ok(request) => request,
        Err(error) => {
            return failed(
                "prompt_reference_bounding_smoke",
                "failed to render prompt-reference request",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let request_text = request
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let file_reference = context
        .prompt_references
        .iter()
        .find(|reference| reference.path == "large.txt");
    let directory_reference = context
        .prompt_references
        .iter()
        .find(|reference| reference.path == "many");
    let file_bounded = file_reference.is_some_and(|reference| {
        reference.bytes == max_file_bytes
            && reference.truncated
            && !reference
                .content
                .as_deref()
                .unwrap_or_default()
                .contains(tail)
    });
    let directory_bounded = directory_reference.is_some_and(|reference| {
        reference.entries.len() == max_directory_entries && reference.truncated
    });
    let request_bounded =
        request_text.contains("\"truncated\": true") && !request_text.contains(tail);

    if file_bounded && directory_bounded && request_bounded {
        passed(
            "prompt_reference_bounding_smoke",
            "default prompt-reference expansion bounds large files and directory listings before model request rendering",
            serde_json::json!({
                "max_file_bytes": max_file_bytes,
                "max_directory_entries": max_directory_entries,
                "prompt_reference_count": context.prompt_references.len(),
                "request_len": request_text.len(),
            }),
        )
    } else {
        failed(
            "prompt_reference_bounding_smoke",
            "prompt-reference expansion was not bounded before model request rendering",
            serde_json::json!({
                "max_file_bytes": max_file_bytes,
                "max_directory_entries": max_directory_entries,
                "prompt_reference_count": context.prompt_references.len(),
                "file_bounded": file_bounded,
                "directory_bounded": directory_bounded,
                "request_bounded": request_bounded,
                "request_contains_tail": request_text.contains(tail),
                "request_contains_truncated": request_text.contains("\"truncated\": true"),
                "file_reference": file_reference,
                "directory_entry_count": directory_reference.map(|reference| reference.entries.len()),
            }),
        )
    }
}

fn check_instruction_file_bounding_smoke() -> AgentReadinessCheck {
    let root = std::env::temp_dir().join(format!(
        "sinomo-agent-instruction-file-readiness-{}",
        TaskId::generate()
    ));
    if let Err(error) = fs::create_dir_all(&root) {
        return failed(
            "instruction_file_bounding_smoke",
            "failed to create instruction-file readiness workspace",
            serde_json::json!({ "error": error.to_string(), "root": root }),
        );
    }
    let config =
        AgentConfig::new("readiness", "instruction-file-bounding").with_workspace_root(&root);
    let Some(max_instruction_bytes) = config.instruction_file_max_bytes else {
        return failed(
            "instruction_file_bounding_smoke",
            "workspace instruction file cap is not configured by default",
            serde_json::json!({ "instruction_file_max_bytes": null }),
        );
    };
    let tail = "INSTRUCTION_FILE_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let content = format!(
        "# Repo Instructions\n\n{}{tail}\n",
        "i".repeat(max_instruction_bytes + 512)
    );
    if let Err(error) = fs::write(root.join("AGENTS.md"), &content) {
        return failed(
            "instruction_file_bounding_smoke",
            "failed to write large instruction file",
            serde_json::json!({ "error": error.to_string(), "root": root }),
        );
    }
    let input = readiness_agent_loop_input("inspect workspace instructions");
    let agent = Agent::new(
        ReadinessSequencedModel::new(std::iter::empty::<String>()),
        "readiness",
        "instruction-file-bounding",
    )
    .with_config(config);
    let context = match agent.assemble_context(&input, &[], 0) {
        Ok(context) => context,
        Err(error) => {
            return failed(
                "instruction_file_bounding_smoke",
                "failed to assemble instruction-file context",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let request = match agent.request(&input, &[], 0) {
        Ok(request) => request,
        Err(error) => {
            return failed(
                "instruction_file_bounding_smoke",
                "failed to render instruction-file request",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let request_text = request
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let instruction_file = context
        .loaded_instruction_files
        .iter()
        .find(|file| file.path == "AGENTS.md");
    let file_bounded = instruction_file.is_some_and(|file| {
        file.bytes == max_instruction_bytes
            && file.original_bytes == Some(content.len())
            && file.truncated
            && !file.content.contains(tail)
    });
    let request_bounded =
        request_text.contains("\"truncated\": true") && !request_text.contains(tail);

    if file_bounded && request_bounded {
        passed(
            "instruction_file_bounding_smoke",
            "default workspace instruction loading bounds large AGENTS.md content before model request rendering",
            serde_json::json!({
                "max_instruction_bytes": max_instruction_bytes,
                "loaded_instruction_files": context.loaded_instruction_files.len(),
                "request_len": request_text.len(),
            }),
        )
    } else {
        failed(
            "instruction_file_bounding_smoke",
            "workspace instruction loading was not bounded before model request rendering",
            serde_json::json!({
                "max_instruction_bytes": max_instruction_bytes,
                "loaded_instruction_files": context.loaded_instruction_files.len(),
                "file_bounded": file_bounded,
                "request_bounded": request_bounded,
                "request_contains_tail": request_text.contains(tail),
                "request_contains_truncated": request_text.contains("\"truncated\": true"),
                "instruction_file": instruction_file,
            }),
        )
    }
}

fn check_context_attachment_bounding_smoke() -> AgentReadinessCheck {
    let config = AgentConfig::new("readiness", "context-attachment-bounding");
    let Some(max_attachment_bytes) = config.context_attachment_max_bytes else {
        return failed(
            "context_attachment_bounding_smoke",
            "context attachment payload cap is not configured by default",
            serde_json::json!({ "context_attachment_max_bytes": null }),
        );
    };
    let tail = "CONTEXT_ATTACHMENT_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let payload = format!(
        "{}{tail}{}",
        "c".repeat(max_attachment_bytes + 512),
        "C".repeat(1024)
    );
    let mut input = readiness_agent_loop_input("inspect attached context");
    input.context_attachments.push(AgentContextAttachment {
        kind: "sinomo.readiness.large_context".to_string(),
        summary: "large readiness context attachment".to_string(),
        payload_json: serde_json::json!({
            "log": payload,
            "status": "passed",
        }),
        payload_original_bytes: None,
        payload_model_visible_truncated: false,
    });
    let durable_attachment_has_tail = input
        .context_attachments
        .first()
        .and_then(|attachment| attachment.payload_json.get("log"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| value.contains(tail));
    let agent = Agent::new(
        ReadinessSequencedModel::new(std::iter::empty::<String>()),
        "readiness",
        "context-attachment-bounding",
    )
    .with_config(config);
    let context = match agent.assemble_context(&input, &[], 0) {
        Ok(context) => context,
        Err(error) => {
            return failed(
                "context_attachment_bounding_smoke",
                "failed to assemble context-attachment context",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let request = match agent.request(&input, &[], 0) {
        Ok(request) => request,
        Err(error) => {
            return failed(
                "context_attachment_bounding_smoke",
                "failed to render context-attachment request",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let request_text = request
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let attachment = context.context_attachments.first();
    let projected_attachment_payload_bytes = attachment
        .and_then(|attachment| serde_json::to_vec(&attachment.payload_json).ok())
        .map(|bytes| bytes.len())
        .unwrap_or_default();
    let context_bounded = attachment.is_some_and(|attachment| {
        attachment.payload_model_visible_truncated
            && attachment.payload_original_bytes.is_some()
            && attachment
                .payload_json
                .get("model_visible_truncated")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
            && attachment
                .payload_json
                .get("payload_json_excerpt")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| !value.contains(tail))
            && projected_attachment_payload_bytes <= max_attachment_bytes
    });
    let request_bounded = request_text.contains("payload_model_visible_truncated")
        && request_text.contains("payload_json_excerpt")
        && !request_text.contains(tail);

    if durable_attachment_has_tail && context_bounded && request_bounded {
        passed(
            "context_attachment_bounding_smoke",
            "durable context attachments remain complete while model-visible turn context bounds oversized payloads",
            serde_json::json!({
                "max_attachment_bytes": max_attachment_bytes,
                "durable_attachment_has_tail": durable_attachment_has_tail,
                "context_attachment_count": context.context_attachments.len(),
                "projected_attachment_payload_bytes": projected_attachment_payload_bytes,
                "request_len": request_text.len(),
            }),
        )
    } else {
        failed(
            "context_attachment_bounding_smoke",
            "context attachment payload was not bounded before model request rendering",
            serde_json::json!({
                "max_attachment_bytes": max_attachment_bytes,
                "durable_attachment_has_tail": durable_attachment_has_tail,
                "context_attachment_count": context.context_attachments.len(),
                "context_bounded": context_bounded,
                "projected_attachment_payload_bytes": projected_attachment_payload_bytes,
                "request_bounded": request_bounded,
                "request_contains_tail": request_text.contains(tail),
                "request_contains_excerpt": request_text.contains("payload_json_excerpt"),
                "attachment": attachment,
            }),
        )
    }
}

fn check_async_update_bounding_smoke() -> AgentReadinessCheck {
    let config = AgentConfig::new("readiness", "async-update-bounding");
    let Some(max_update_bytes) = config.async_update_max_bytes else {
        return failed(
            "async_update_bounding_smoke",
            "async task update cap is not configured by default",
            serde_json::json!({ "async_update_max_bytes": null }),
        );
    };
    let tail = "ASYNC_UPDATE_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let summary = format!(
        "{}{tail}{}",
        "a".repeat(max_update_bytes + 512),
        "A".repeat(1024)
    );
    let result_ref = format!(
        "{}{tail}{}",
        "r".repeat(max_update_bytes + 512),
        "R".repeat(1024)
    );
    let mut input = readiness_agent_loop_input("inspect async task update");
    input.async_update = Some(AsyncTaskUpdate {
        kind: "coding.validation".to_string(),
        task_id: TaskId::generate(),
        status: AsyncTaskStatus::Progress,
        summary,
        needs_user_attention: true,
        result_ref: Some(result_ref),
        next_action_hint: AsyncTaskNextAction::TellUser,
    });
    let durable_update_has_tail = input.async_update.as_ref().is_some_and(|update| {
        update.summary.contains(tail)
            && update
                .result_ref
                .as_deref()
                .is_some_and(|result_ref| result_ref.contains(tail))
    });
    let agent = Agent::new(
        ReadinessSequencedModel::new(std::iter::empty::<String>()),
        "readiness",
        "async-update-bounding",
    )
    .with_config(config);
    let context = match agent.assemble_context(&input, &[], 0) {
        Ok(context) => context,
        Err(error) => {
            return failed(
                "async_update_bounding_smoke",
                "failed to assemble async-update context",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let request = match agent.request(&input, &[], 0) {
        Ok(request) => request,
        Err(error) => {
            return failed(
                "async_update_bounding_smoke",
                "failed to render async-update request",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let request_text = request
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let update = context.async_update.as_ref();
    let projected_update_bytes = update
        .and_then(|update| serde_json::to_vec(update).ok())
        .map(|bytes| bytes.len())
        .unwrap_or_default();
    let context_bounded = update.is_some_and(|update| {
        update
            .get("model_visible_truncated")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
            && update
                .get("summary_excerpt")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| !value.contains(tail))
            && update
                .get("result_ref_excerpt")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| !value.contains(tail))
            && projected_update_bytes <= max_update_bytes
    });
    let request_bounded = request_text.contains("async_update_original_bytes")
        && request_text.contains("summary_excerpt")
        && !request_text.contains(tail);

    if durable_update_has_tail && context_bounded && request_bounded {
        passed(
            "async_update_bounding_smoke",
            "durable async task updates remain complete while model-visible turn context bounds oversized payloads",
            serde_json::json!({
                "max_update_bytes": max_update_bytes,
                "durable_update_has_tail": durable_update_has_tail,
                "async_update_bytes": context.budget_estimate.async_update_bytes,
                "projected_update_bytes": projected_update_bytes,
                "request_len": request_text.len(),
            }),
        )
    } else {
        failed(
            "async_update_bounding_smoke",
            "async task update was not bounded before model request rendering",
            serde_json::json!({
                "max_update_bytes": max_update_bytes,
                "durable_update_has_tail": durable_update_has_tail,
                "context_bounded": context_bounded,
                "projected_update_bytes": projected_update_bytes,
                "request_bounded": request_bounded,
                "request_contains_tail": request_text.contains(tail),
                "request_contains_excerpt": request_text.contains("summary_excerpt"),
                "async_update": update,
            }),
        )
    }
}

fn check_empty_optional_context_budget_smoke() -> AgentReadinessCheck {
    let config = AgentConfig::new("readiness", "empty-context-budget")
        .with_workspace_instruction_files(Vec::<PathBuf>::new());
    let input = readiness_agent_loop_input("plain readiness request");
    let agent = Agent::new(
        ReadinessSequencedModel::new(std::iter::empty::<String>()),
        "readiness",
        "empty-context-budget",
    )
    .with_config(config);
    let context = match agent.assemble_context(&input, &[], 0) {
        Ok(context) => context,
        Err(error) => {
            return failed(
                "empty_optional_context_budget_smoke",
                "failed to assemble empty optional context budget",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let optional_bytes_zero = context.budget_estimate.instruction_file_bytes == 0
        && context.budget_estimate.prompt_reference_bytes == 0
        && context.budget_estimate.context_attachment_bytes == 0
        && context.budget_estimate.async_update_bytes == 0;
    let optional_lanes_absent = !context.budget_estimate.lanes.iter().any(|lane| {
        matches!(
            lane.kind.as_str(),
            "prompt_references" | "context_attachments" | "async_update"
        )
    });
    let optional_sources_absent = !context.report.included.iter().any(|source| {
        matches!(
            source.kind.as_str(),
            "prompt_references" | "context_attachments" | "async_update"
        )
    });

    if optional_bytes_zero && optional_lanes_absent && optional_sources_absent {
        passed(
            "empty_optional_context_budget_smoke",
            "empty optional context projections do not create budget bytes, lanes, or included sources",
            serde_json::json!({
                "instruction_file_bytes": context.budget_estimate.instruction_file_bytes,
                "prompt_reference_bytes": context.budget_estimate.prompt_reference_bytes,
                "context_attachment_bytes": context.budget_estimate.context_attachment_bytes,
                "async_update_bytes": context.budget_estimate.async_update_bytes,
                "lane_count": context.budget_estimate.lanes.len(),
            }),
        )
    } else {
        failed(
            "empty_optional_context_budget_smoke",
            "empty optional context projections were counted as model-visible budget",
            serde_json::json!({
                "optional_bytes_zero": optional_bytes_zero,
                "optional_lanes_absent": optional_lanes_absent,
                "optional_sources_absent": optional_sources_absent,
                "instruction_file_bytes": context.budget_estimate.instruction_file_bytes,
                "prompt_reference_bytes": context.budget_estimate.prompt_reference_bytes,
                "context_attachment_bytes": context.budget_estimate.context_attachment_bytes,
                "async_update_bytes": context.budget_estimate.async_update_bytes,
                "lanes": context.budget_estimate.lanes,
                "included": context.report.included,
            }),
        )
    }
}

fn check_git_diff_large_output_disclosure_smoke() -> AgentReadinessCheck {
    let root = std::env::temp_dir().join(format!(
        "sinomo-agent-git-diff-readiness-{}",
        TaskId::generate()
    ));
    let evidence_dir = root.join("evidence");
    if let Err(error) = fs::create_dir_all(&root) {
        return failed(
            "git_diff_large_output_disclosure_smoke",
            "failed to create git-diff readiness workspace",
            serde_json::json!({ "error": error.to_string(), "root": root }),
        );
    }
    let original = (0..80)
        .map(|index| format!("line {index:02} old\n"))
        .collect::<String>();
    let modified = (0..80)
        .map(|index| format!("line {index:02} new\n"))
        .collect::<String>();
    if let Err(error) = fs::write(root.join("large.txt"), original) {
        return failed(
            "git_diff_large_output_disclosure_smoke",
            "failed to seed git-diff readiness workspace",
            serde_json::json!({ "error": error.to_string(), "root": root }),
        );
    }
    for args in [
        vec!["init"],
        vec!["config", "user.email", "sinomo@example.invalid"],
        vec!["config", "user.name", "Sinomo Readiness"],
        vec!["add", "large.txt"],
        vec!["commit", "-m", "seed"],
    ] {
        if let Err(error) = run_readiness_git(&root, &args) {
            return failed(
                "git_diff_large_output_disclosure_smoke",
                "failed to initialize git-diff readiness repository",
                serde_json::json!({ "error": error, "args": args, "root": root }),
            );
        }
    }
    if let Err(error) = fs::write(root.join("large.txt"), modified) {
        return failed(
            "git_diff_large_output_disclosure_smoke",
            "failed to modify git-diff readiness workspace",
            serde_json::json!({ "error": error.to_string(), "root": root }),
        );
    }

    let registry = fixed_command_readiness_registry(
        LocalWorkspaceAgentConfig::new(&root)
            .using_local_process_commands()
            .with_evidence_dir(&evidence_dir),
    );
    let plan = AgentToolPlanner::plan(
        registry.descriptors(),
        &AgentModuleToolProjection::new(),
        Vec::<AgentToolSpec>::new(),
        Vec::<AgentToolSpec>::new(),
        false,
        ["discovery"],
        registry.generation(),
    );
    let session = readiness_session();
    let run_context = AgentRunContext::default();
    let tool_surface = AgentToolSurface::default_for_role(AgentRole::Deliberation);
    let orchestrator = AgentToolOrchestrator::new(&tool_surface, None, &run_context);
    let search = orchestrator.execute(
        &registry,
        &session,
        &plan,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "tool_search".to_string(),
            input: serde_json::json!({ "query": "git_diff", "max_results": 5 }),
            r#async: false,
            summary: "readiness git diff search".to_string(),
        },
    );
    let diff = orchestrator.execute(
        &registry,
        &session,
        &plan,
        0,
        1,
        &AgentToolCall {
            call_id: None,
            tool: "tool_call".to_string(),
            input: serde_json::json!({
                "tool": "git_diff",
                "input": {
                    "path": "large.txt",
                    "max_bytes": 64,
                    "timeout_ms": 5000
                },
                "summary": "Read bounded diff through disclosure bridge."
            }),
            r#async: false,
            summary: "readiness git diff bridge call".to_string(),
        },
    );
    let search_found_hidden =
        output_matches_tool_visibility(&search.output, "git_diff", Some(false));
    let diff_preview = diff
        .output
        .get("diff")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let diff_truncated = diff
        .output
        .get("diff_truncated")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let full_diff_ref = diff
        .output
        .get("full_diff_ref")
        .and_then(serde_json::Value::as_str);
    let full_diff_contains_tail = full_diff_ref
        .and_then(|path| fs::read_to_string(path).ok())
        .is_some_and(|content| {
            content.contains("-line 79 old") && content.contains("+line 79 new")
        });
    let changed_files = diff
        .output
        .get("changed_files")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let coding_record = diff.evidence.record_value("coding");
    let coding_diff_ref_matches = coding_record
        .and_then(|record| record.get("diff_ref"))
        .and_then(serde_json::Value::as_str)
        == full_diff_ref;
    let coding_changed_file = coding_record
        .and_then(|record| record.get("changed_files"))
        .and_then(serde_json::Value::as_array)
        .is_some_and(|files| files.iter().any(|file| file == "large.txt"));
    let evidence_has_diff_artifact =
        diff.evidence.artifacts.iter().any(|artifact| {
            artifact.kind == "diff" && Some(artifact.uri.as_str()) == full_diff_ref
        });

    if plan.entry("git_diff").is_some()
        && !plan.is_model_visible("git_diff")
        && plan.is_executable("git_diff")
        && search.status == AgentToolStatus::Succeeded
        && search_found_hidden
        && diff.status == AgentToolStatus::Succeeded
        && diff
            .output
            .pointer("/_tool_call/target_tool")
            .and_then(serde_json::Value::as_str)
            == Some("git_diff")
        && changed_files.iter().any(|file| file == "large.txt")
        && diff_preview.len() <= 64
        && diff_truncated
        && full_diff_contains_tail
        && coding_diff_ref_matches
        && coding_changed_file
        && evidence_has_diff_artifact
    {
        passed(
            "git_diff_large_output_disclosure_smoke",
            "hidden git_diff is reachable through disclosure with bounded preview and durable full-diff evidence",
            serde_json::json!({
                "git_diff_planned": plan.entry("git_diff").is_some(),
                "git_diff_model_visible": plan.is_model_visible("git_diff"),
                "git_diff_executable": plan.is_executable("git_diff"),
                "search_found_hidden": search_found_hidden,
                "diff_preview_bytes": diff_preview.len(),
                "diff_truncated": diff_truncated,
                "full_diff_ref": full_diff_ref,
                "changed_files": changed_files,
                "coding_diff_ref_matches": coding_diff_ref_matches,
            }),
        )
    } else {
        failed(
            "git_diff_large_output_disclosure_smoke",
            "git_diff disclosure or full-diff evidence smoke failed",
            serde_json::json!({
                "git_diff_planned": plan.entry("git_diff").is_some(),
                "git_diff_model_visible": plan.is_model_visible("git_diff"),
                "git_diff_executable": plan.is_executable("git_diff"),
                "search_status": search.status,
                "search_output": search.output,
                "search_found_hidden": search_found_hidden,
                "diff_status": diff.status,
                "diff_summary": diff.summary,
                "diff_output": diff.output,
                "diff_artifacts": diff.evidence.artifacts,
                "diff_preview_bytes": diff_preview.len(),
                "full_diff_contains_tail": full_diff_contains_tail,
                "coding_record": coding_record,
                "coding_diff_ref_matches": coding_diff_ref_matches,
                "coding_changed_file": coding_changed_file,
                "evidence_has_diff_artifact": evidence_has_diff_artifact,
            }),
        )
    }
}

fn run_readiness_git(root: &Path, args: &[&str]) -> Result<(), String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .map_err(|source| format!("failed to run git {}: {source}", args.join(" ")))?;
    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "git {} exited with {:?}: stdout={} stderr={}",
        args.join(" "),
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
}

fn check_tool_cancellation_boundary_smoke(workspace_root: &Path) -> AgentReadinessCheck {
    let registry = AgentToolRegistry::local_workspace(workspace_root);
    let session = readiness_session();
    let cancellation_token = AgentCancellationToken::new();
    cancellation_token.cancel();
    let call = AgentToolCall {
        call_id: None,
        tool: "read_file".to_string(),
        input: serde_json::json!({ "path": "Cargo.toml" }),
        r#async: false,
        summary: "readiness cancelled read".to_string(),
    };

    let outcome = registry.execute_with_cancellation(&session, 0, 0, &call, cancellation_token);
    let cancelled = outcome.status == AgentToolStatus::Cancelled
        && outcome
            .output
            .get("error")
            .and_then(serde_json::Value::as_str)
            == Some("tool_cancelled")
        && !outcome.evidence.availability.allowed
        && outcome
            .evidence
            .availability
            .reason
            .contains("turn cancellation requested");
    if cancelled {
        passed(
            "tool_cancellation_boundary_smoke",
            "registry-level cancellation prevents non-command tool execution",
            serde_json::json!({
                "tool": call.tool,
                "status": outcome.status,
                "availability_allowed": outcome.evidence.availability.allowed,
                "reason": outcome.evidence.availability.reason,
            }),
        )
    } else {
        failed(
            "tool_cancellation_boundary_smoke",
            "registry-level cancellation did not block a non-command tool",
            serde_json::json!({
                "tool": call.tool,
                "status": outcome.status,
                "output": outcome.output,
                "availability": outcome.evidence.availability,
            }),
        )
    }
}

#[cfg(unix)]
fn check_version_control_active_cancellation_smoke() -> AgentReadinessCheck {
    let workspace_root = std::env::temp_dir().join(format!(
        "sinomo-agent-vcs-cancel-readiness-{}",
        TaskId::generate()
    ));
    if let Err(error) = fs::create_dir_all(&workspace_root) {
        return failed(
            "version_control_active_cancellation_smoke",
            "failed to create readiness workspace for version-control cancellation",
            serde_json::json!({ "error": error.to_string() }),
        );
    }
    let fake_git = workspace_root.join("slow-git");
    if let Err(error) = fs::write(&fake_git, "#!/bin/sh\nsleep 5\n") {
        return failed(
            "version_control_active_cancellation_smoke",
            "failed to create fake git executable",
            serde_json::json!({ "error": error.to_string() }),
        );
    }
    let mut permissions = match fs::metadata(&fake_git).map(|metadata| metadata.permissions()) {
        Ok(permissions) => permissions,
        Err(error) => {
            return failed(
                "version_control_active_cancellation_smoke",
                "failed to inspect fake git executable",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
    if let Err(error) = fs::set_permissions(&fake_git, permissions) {
        return failed(
            "version_control_active_cancellation_smoke",
            "failed to mark fake git executable",
            serde_json::json!({ "error": error.to_string() }),
        );
    }
    let fake_git_program = fake_git.display().to_string();
    let registry = fixed_command_readiness_registry(
        LocalWorkspaceAgentConfig::new(&workspace_root)
            .using_local_process_commands()
            .with_git_program(fake_git_program.clone())
            .enabling_git_tools(),
    );
    let session = readiness_session();
    let cancellation_token = AgentCancellationToken::new();
    let token_for_sink = cancellation_token.clone();
    let progress_sink = AgentToolProgressSink::new(move |event| {
        if matches!(event, AgentToolProgressEvent::ProcessStarted { .. }) {
            token_for_sink.cancel();
        }
    });
    let call = AgentToolCall {
        call_id: None,
        tool: "changed_files".to_string(),
        input: serde_json::json!({}),
        r#async: false,
        summary: "readiness cancelled changed files".to_string(),
    };

    let outcome = registry.execute_with_cancellation_and_progress(
        &session,
        0,
        0,
        &call,
        cancellation_token,
        progress_sink,
    );
    let cancelled = outcome.status == AgentToolStatus::Cancelled
        && outcome
            .output
            .get("cancelled")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        && outcome
            .output
            .get("program")
            .and_then(serde_json::Value::as_str)
            == Some(fake_git_program.as_str());
    if cancelled {
        passed(
            "version_control_active_cancellation_smoke",
            "version-control helper subprocesses are cancellable after process start",
            serde_json::json!({
                "tool": call.tool,
                "status": outcome.status,
                "cancelled": outcome.output.get("cancelled"),
                "program": outcome.output.get("program"),
            }),
        )
    } else {
        failed(
            "version_control_active_cancellation_smoke",
            "version-control helper subprocess cancellation failed",
            serde_json::json!({
                "tool": call.tool,
                "status": outcome.status,
                "output": outcome.output,
            }),
        )
    }
}

#[cfg(not(unix))]
fn check_version_control_active_cancellation_smoke() -> AgentReadinessCheck {
    passed(
        "version_control_active_cancellation_smoke",
        "version-control active subprocess cancellation smoke is not available on this platform",
        serde_json::json!({ "platform": std::env::consts::OS }),
    )
}

#[cfg(unix)]
pub fn run_agent_simple_sandbox_active_cancellation_readiness() -> AgentReadinessCheck {
    const CHECK_ID: &str = "agent_simple_sandbox_active_cancellation_smoke";
    let readiness_root = std::env::temp_dir().join(format!(
        "sinomo-agent-simple-sandbox-cancel-readiness-{}",
        TaskId::generate()
    ));
    let workspace_root = readiness_root.join("workspace");
    let runtime_root = readiness_root.join("sandbox-runtime");
    if let Err(error) = fs::create_dir_all(&workspace_root) {
        return failed(
            CHECK_ID,
            "failed to create the simple-sandbox readiness workspace",
            serde_json::json!({ "error": error.to_string() }),
        );
    }

    let session = readiness_session();
    let sandbox_id = sinomo_runtime::sandbox::SandboxId::derived(
        "sinomo-agent-simple-sandbox-cancellation-readiness",
        TaskId::generate().to_string().as_bytes(),
    );
    let config = LocalWorkspaceAgentConfig::new(&workspace_root)
        .with_sandbox_runtime_root(&runtime_root)
        .with_sandbox_instance_id(sandbox_id.clone())
        .allowing_unenforced_simple_sandbox(true)
        .allowing_shell_commands(true);
    if let Err(error) = provision_default_agent_session_sandbox(&config, Vec::new()) {
        return failed(
            CHECK_ID,
            "failed to provision the simple-sandbox readiness instance",
            serde_json::json!({
                "error": error,
                "sandbox_id": sandbox_id,
            }),
        );
    }

    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(&runtime_root);
    let layout = match store.load(&sandbox_id) {
        Ok((_manifest, layout)) => layout,
        Err(error) => {
            return failed(
                CHECK_ID,
                "failed to load the provisioned simple-sandbox readiness instance",
                serde_json::json!({
                    "error": error.to_string(),
                    "sandbox_id": sandbox_id,
                }),
            );
        }
    };
    let started_marker = layout.work.join("readiness-started");
    let progress_marker = layout.work.join("readiness-progress");
    let registry = fixed_command_readiness_registry(config);
    let cancellation_token = AgentCancellationToken::new();
    let token_for_run = cancellation_token.clone();
    let lifecycle = Arc::new(Mutex::new(Vec::<AgentProcessLifecycleStatus>::new()));
    let lifecycle_for_sink = Arc::clone(&lifecycle);
    let progress_sink = AgentToolProgressSink::new(move |event| {
        let process = match event {
            AgentToolProgressEvent::ProcessStarted { process }
            | AgentToolProgressEvent::ProcessCompleted { process } => Some(process),
            AgentToolProgressEvent::ProcessOutput { .. }
            | AgentToolProgressEvent::Message { .. } => None,
        };
        if let Some(process) = process
            && let Ok(mut lifecycle) = lifecycle_for_sink.lock()
        {
            lifecycle.push(process.status);
        }
    });
    let call = AgentToolCall {
        call_id: None,
        tool: "run_command".to_string(),
        input: serde_json::json!({
            "command": "printf ready > \"$SINOMO_WORK/readiness-started\"; while :; do printf x >> \"$SINOMO_WORK/readiness-progress\"; sleep 0.02; done",
            "timeout_ms": 8_000,
        }),
        r#async: false,
        summary: "readiness cancel active simple-sandbox work".to_string(),
    };
    let (outcome_sender, outcome_receiver) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let outcome = registry.execute_with_cancellation_and_progress(
            &session,
            0,
            0,
            &call,
            token_for_run,
            progress_sink,
        );
        let _ = outcome_sender.send(outcome);
    });

    let start_deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while (!started_marker.exists()
        || fs::metadata(&progress_marker).map_or(0, |metadata| metadata.len()) == 0)
        && std::time::Instant::now() < start_deadline
    {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let command_became_active = started_marker.exists()
        && fs::metadata(&progress_marker).map_or(0, |metadata| metadata.len()) > 0;
    let cancelled_at = std::time::Instant::now();
    cancellation_token.cancel();
    let outcome = outcome_receiver.recv_timeout(std::time::Duration::from_secs(10));
    let worker_joined = worker.join().is_ok();
    let cancellation_duration_ms = cancelled_at.elapsed().as_millis() as u64;
    let progress_bytes_after_cancel =
        fs::metadata(&progress_marker).map_or(0, |metadata| metadata.len());
    std::thread::sleep(std::time::Duration::from_millis(150));
    let progress_bytes_after_settle =
        fs::metadata(&progress_marker).map_or(0, |metadata| metadata.len());
    let lifecycle = lifecycle
        .lock()
        .map_or_else(|_| Vec::new(), |lifecycle| lifecycle.clone());
    let manifest = store.load(&sandbox_id).map(|(manifest, _layout)| manifest);

    let Ok(outcome) = outcome else {
        return failed(
            CHECK_ID,
            "simple-sandbox command did not settle after cancellation",
            serde_json::json!({
                "command_became_active": command_became_active,
                "worker_joined": worker_joined,
                "cancellation_duration_ms": cancellation_duration_ms,
                "lifecycle": lifecycle,
            }),
        );
    };
    let (manifest_stopped, active_process_count) =
        manifest.map_or((false, usize::MAX), |manifest| {
            (
                manifest.lifecycle.status
                    == sinomo_runtime::sandbox::SandboxInstanceLifecycleStatus::Stopped,
                manifest.active_processes.len(),
            )
        });
    let observed_running = lifecycle.contains(&AgentProcessLifecycleStatus::Running);
    let observed_cancelled = lifecycle.contains(&AgentProcessLifecycleStatus::Cancelled);
    let cancelled = outcome.status == AgentToolStatus::Cancelled
        && outcome
            .output
            .get("cancelled")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        && outcome
            .output
            .get("cleanup_pending")
            .and_then(serde_json::Value::as_bool)
            == Some(false);
    let process_stopped = progress_bytes_after_cancel == progress_bytes_after_settle;
    let bounded = cancellation_duration_ms < 3_000;
    let evidence = serde_json::json!({
        "sandbox_id": sandbox_id,
        "command_became_active": command_became_active,
        "cancelled": cancelled,
        "worker_joined": worker_joined,
        "cancellation_duration_ms": cancellation_duration_ms,
        "observed_running": observed_running,
        "observed_cancelled": observed_cancelled,
        "manifest_stopped": manifest_stopped,
        "active_process_count": active_process_count,
        "progress_bytes_after_cancel": progress_bytes_after_cancel,
        "progress_bytes_after_settle": progress_bytes_after_settle,
        "process_stopped": process_stopped,
        "bounded": bounded,
        "status": outcome.status,
        "output": outcome.output,
    });
    if command_became_active
        && cancelled
        && worker_joined
        && bounded
        && observed_running
        && observed_cancelled
        && manifest_stopped
        && active_process_count == 0
        && process_stopped
    {
        passed(
            CHECK_ID,
            "agent simple-sandbox work is cancellable after the child process becomes active",
            evidence,
        )
    } else {
        failed(
            CHECK_ID,
            "agent simple-sandbox active cancellation failed",
            evidence,
        )
    }
}

#[cfg(not(unix))]
pub fn run_agent_simple_sandbox_active_cancellation_readiness() -> AgentReadinessCheck {
    passed(
        "agent_simple_sandbox_active_cancellation_smoke",
        "agent simple-sandbox active cancellation readiness is not available on this platform",
        serde_json::json!({ "platform": std::env::consts::OS }),
    )
}

#[cfg(unix)]
fn check_sandbox_container_active_cancellation_smoke() -> AgentReadinessCheck {
    use std::os::unix::fs::PermissionsExt;

    let workspace_root = std::env::temp_dir().join(format!(
        "sinomo-agent-sandbox-cancel-readiness-{}",
        TaskId::generate()
    ));
    if let Err(error) = fs::create_dir_all(&workspace_root) {
        return failed(
            "sandbox_container_active_cancellation_smoke",
            "failed to create readiness workspace for sandbox cancellation",
            serde_json::json!({ "error": error.to_string() }),
        );
    }
    let fake_runtime = workspace_root.join("fake-container-runtime.sh");
    let cancel_marker = workspace_root.join("container-cancelled");
    if let Err(error) = fs::write(
        &fake_runtime,
        format!(
            r#"#!/bin/sh
marker='{}'
if [ "$1" = "rm" ]; then
  touch "$marker"
  exit 0
fi
while [ "$#" -gt 0 ]; do
  case "$1" in
    fake-image) shift; break ;;
    --name|--network|--cap-drop|--security-opt|--memory|--stop-timeout|--volume|--workdir|--env) shift 2 ;;
    --rm|--pull=never|--read-only|-i) shift ;;
    *) shift ;;
  esac
done
while [ ! -f "$marker" ]; do
  sleep 0.05
done
exit 0
"#,
            cancel_marker.display()
        ),
    ) {
        return failed(
            "sandbox_container_active_cancellation_smoke",
            "failed to create fake sandbox runtime executable",
            serde_json::json!({ "error": error.to_string() }),
        );
    }
    let mut permissions = match fs::metadata(&fake_runtime).map(|metadata| metadata.permissions()) {
        Ok(permissions) => permissions,
        Err(error) => {
            return failed(
                "sandbox_container_active_cancellation_smoke",
                "failed to inspect fake sandbox runtime executable",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    permissions.set_mode(0o755);
    if let Err(error) = fs::set_permissions(&fake_runtime, permissions) {
        return failed(
            "sandbox_container_active_cancellation_smoke",
            "failed to mark fake sandbox runtime executable",
            serde_json::json!({ "error": error.to_string() }),
        );
    }

    let store = sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(&workspace_root);
    let sandbox_id =
        match sinomo_runtime::sandbox::SandboxId::from_legacy_named("agent_sandbox_cancel") {
            Ok(sandbox_id) => sandbox_id,
            Err(error) => {
                return failed(
                    "sandbox_container_active_cancellation_smoke",
                    "failed to create sandbox id",
                    serde_json::json!({ "error": error.to_string() }),
                );
            }
        };
    let toolchain_id =
        match sinomo_runtime::sandbox::SandboxToolchainId::from_legacy_named("agent_tools") {
            Ok(toolchain_id) => toolchain_id,
            Err(error) => {
                return failed(
                    "sandbox_container_active_cancellation_smoke",
                    "failed to create sandbox toolchain id",
                    serde_json::json!({ "error": error.to_string() }),
                );
            }
        };
    if let Err(error) = store.create(
        sandbox_id.clone(),
        toolchain_id,
        time::OffsetDateTime::now_utc(),
    ) {
        return failed(
            "sandbox_container_active_cancellation_smoke",
            "failed to create sandbox instance",
            serde_json::json!({ "error": error.to_string() }),
        );
    }
    let config = match sinomo_runtime::sandbox::SandboxContainerBackendConfig::new(
        sinomo_runtime::sandbox::SandboxContainerRuntime::Command(fake_runtime.clone()),
        "fake-image",
    ) {
        Ok(config) => config,
        Err(error) => {
            return failed(
                "sandbox_container_active_cancellation_smoke",
                "failed to create sandbox container config",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let runner = sinomo_runtime::sandbox::SandboxContainerCommandRunner::new(config);
    let cancellation_token = sinomo_runtime::sandbox::SandboxCancellationToken::new();
    let token_for_run = cancellation_token.clone();
    let runner_for_run = runner.clone();
    let store_for_run = store.clone();
    let sandbox_id_for_run = sandbox_id.clone();
    let handle = std::thread::spawn(move || {
        runner_for_run.run_toolchain_command_with_stdin_and_output_sink_and_cancellation(
            &store_for_run,
            &sandbox_id_for_run,
            "sh",
            vec!["sh".to_string(), "-c".to_string(), "sleep 30".to_string()],
            None,
            sinomo_runtime::sandbox::SANDBOX_UNLIMITED_WALL_TIME_MS,
            None,
            token_for_run,
        )
    });
    std::thread::sleep(std::time::Duration::from_millis(150));
    cancellation_token.cancel();
    let result = match handle.join() {
        Ok(result) => result,
        Err(_) => {
            return failed(
                "sandbox_container_active_cancellation_smoke",
                "sandbox cancellation worker panicked",
                serde_json::json!({}),
            );
        }
    };
    let cancelled = matches!(
        result,
        Err(sinomo_runtime::sandbox::SandboxCommandExecutionError::Wasi(
            sinomo_runtime::sandbox::SandboxWasiError::Cancelled
        ))
    ) && cancel_marker.exists();
    if cancelled {
        passed(
            "sandbox_container_active_cancellation_smoke",
            "sandbox container execution is cancellable after runtime start",
            serde_json::json!({
                "runtime": fake_runtime.display().to_string(),
                "cancel_marker": cancel_marker.exists(),
            }),
        )
    } else {
        failed(
            "sandbox_container_active_cancellation_smoke",
            "sandbox container active cancellation failed",
            serde_json::json!({
                "runtime": fake_runtime.display().to_string(),
                "cancel_marker": cancel_marker.exists(),
                "result": format!("{result:?}"),
            }),
        )
    }
}

#[cfg(not(unix))]
fn check_sandbox_container_active_cancellation_smoke() -> AgentReadinessCheck {
    passed(
        "sandbox_container_active_cancellation_smoke",
        "sandbox container active cancellation smoke is not available on this platform",
        serde_json::json!({ "platform": std::env::consts::OS }),
    )
}

struct ReadinessHostGateway {
    started: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    cancel_calls: Arc<AtomicUsize>,
}

impl AgentToolGateway for ReadinessHostGateway {
    fn descriptor(&self) -> crate::tools::AgentToolDescriptor {
        crate::tools::AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ModuleObjectRpc,
            "host_gateway_readiness_probe",
            "fake host-provided gateway readiness probe",
            false,
        ))
        .with_capabilities([AgentToolCapability::CallModule])
    }

    fn start(
        &self,
        _request: &AgentToolRequest<'_>,
    ) -> Result<AgentToolGatewayTask, AgentToolResult> {
        self.started.store(true, Ordering::SeqCst);
        let cancelled_for_wait = Arc::clone(&self.cancelled);
        let cancelled_for_cancel = Arc::clone(&self.cancelled);
        let cancel_calls = Arc::clone(&self.cancel_calls);
        Ok(AgentToolGatewayTask::new(
            move || {
                while !cancelled_for_wait.load(Ordering::SeqCst) {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                AgentToolResult::cancelled(
                    "host gateway task cancelled",
                    serde_json::json!({
                        "cancelled": true,
                        "source": "readiness_host_gateway",
                    }),
                )
            },
            move || {
                cancel_calls.fetch_add(1, Ordering::SeqCst);
                cancelled_for_cancel.store(true, Ordering::SeqCst);
                AgentToolGatewayCancellation::accepted(
                    "readiness host gateway cancellation accepted",
                )
            },
        ))
    }
}

fn check_host_gateway_active_cancellation_smoke() -> AgentReadinessCheck {
    let started = Arc::new(AtomicBool::new(false));
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel_calls = Arc::new(AtomicUsize::new(0));
    let registry = AgentToolRegistry::new().with_gateway(ReadinessHostGateway {
        started: Arc::clone(&started),
        cancelled: Arc::clone(&cancelled),
        cancel_calls: Arc::clone(&cancel_calls),
    });
    let cancellation_token = AgentCancellationToken::new();
    let token_for_sink = cancellation_token.clone();
    let progress_sink = AgentToolProgressSink::new(move |event| {
        if matches!(
            event,
            AgentToolProgressEvent::Message { ref summary, .. }
                if summary == "gateway task started"
        ) {
            token_for_sink.cancel();
        }
    });
    let call = AgentToolCall {
        call_id: None,
        tool: "host_gateway_readiness_probe".to_string(),
        input: serde_json::json!({}),
        r#async: false,
        summary: "readiness cancelled host gateway".to_string(),
    };

    let outcome = registry.execute_with_cancellation_and_progress(
        &readiness_session(),
        0,
        0,
        &call,
        cancellation_token,
        progress_sink,
    );
    let cancel_calls = cancel_calls.load(Ordering::SeqCst);
    let cancelled = outcome.status == AgentToolStatus::Cancelled
        && started.load(Ordering::SeqCst)
        && cancelled.load(Ordering::SeqCst)
        && cancel_calls == 1
        && outcome
            .output
            .get("cancelled")
            .and_then(serde_json::Value::as_bool)
            == Some(true);
    if cancelled {
        passed(
            "host_gateway_active_cancellation_smoke",
            "host-provided tool gateways can cancel already-running work",
            serde_json::json!({
                "tool": call.tool,
                "status": outcome.status,
                "cancel_calls": cancel_calls,
            }),
        )
    } else {
        failed(
            "host_gateway_active_cancellation_smoke",
            "host-provided tool gateway active cancellation failed",
            serde_json::json!({
                "tool": call.tool,
                "status": outcome.status,
                "cancel_calls": cancel_calls,
                "started": started.load(Ordering::SeqCst),
                "output": outcome.output,
            }),
        )
    }
}

#[derive(Default)]
struct ReadinessRemoteObjectRpcTransport {
    cancelled: AtomicBool,
    start_operation_id: Mutex<Option<String>>,
    cancel_operation_id: Mutex<Option<String>>,
    cancel_calls: AtomicUsize,
}

async fn materialize_readiness_value_call(call: ObjectRpcCall) -> Result<ObjectRpcRequest, String> {
    let streamed_value = call.has_streamed_value_body();
    let ObjectRpcCall { mut request, body } = call;
    if body.is_empty() {
        return Ok(request);
    }
    if !streamed_value {
        return Err("readiness transport received an unexpected attachment".to_string());
    }
    request.body = body
        .collect_to_vec(usize::MAX)
        .await
        .map_err(|error| error.to_string())?;
    Ok(request)
}

impl AgentRemoteObjectRpcTransport for ReadinessRemoteObjectRpcTransport {
    fn call<'a>(&'a self, call: ObjectRpcCall) -> AgentRemoteObjectRpcFuture<'a> {
        Box::pin(async move {
            let request = materialize_readiness_value_call(call).await?;
            let response = match request.route.as_str() {
                "/agent/readiness/remote-tool/run" => {
                    *self.start_operation_id.lock().expect("start op lock") =
                        request.operation_id().map(str::to_string);
                    while !self.cancelled.load(Ordering::SeqCst) {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    ObjectRpcResponse::encode_body(
                        "/agent/readiness/remote-tool/run",
                        &serde_json::json!({ "remote": "cancelled" }),
                    )
                    .map_err(|error| error.to_string())?
                }
                "/agent/readiness/remote-tool/cancel" => {
                    self.cancel_calls.fetch_add(1, Ordering::SeqCst);
                    *self.cancel_operation_id.lock().expect("cancel op lock") =
                        request.cancels_operation_id().map(str::to_string);
                    let body: ObjectRpcCancelRequest =
                        request.decode_body().map_err(|error| error.to_string())?;
                    if Some(body.operation_id.as_str()) == request.cancels_operation_id() {
                        self.cancelled.store(true, Ordering::SeqCst);
                        ObjectRpcResponse::encode_body(
                            "/agent/readiness/remote-tool/cancel",
                            &serde_json::json!({ "accepted": true }),
                        )
                        .map_err(|error| error.to_string())?
                    } else {
                        ObjectRpcResponse::encode_error(
                            "/agent/readiness/remote-tool/cancel",
                            "operation id mismatch",
                        )
                        .map_err(|error| error.to_string())?
                    }
                }
                other => return Err(format!("unexpected route {other}")),
            };
            Ok(ObjectRpcResult::value(response))
        })
    }
}

fn check_remote_object_rpc_active_cancellation_smoke() -> AgentReadinessCheck {
    let transport = Arc::new(ReadinessRemoteObjectRpcTransport::default());
    let tool = AgentRemoteObjectRpcTool::json_module_tool(
        "remote_object_rpc_readiness_probe",
        "fake remote object-RPC readiness probe",
        "/agent/readiness/remote-tool/run",
        "agent.readiness",
        "/agent/readiness/remote-tool/cancel",
        "agent.readiness",
        transport.clone(),
    );
    let registry = AgentToolRegistry::new().with_gateway(tool);
    let cancellation_token = AgentCancellationToken::new();
    let token_for_sink = cancellation_token.clone();
    let progress_sink = AgentToolProgressSink::new(move |event| {
        if matches!(
            event,
            AgentToolProgressEvent::Message { ref summary, .. }
                if summary == "gateway task started"
        ) {
            token_for_sink.cancel();
        }
    });
    let call = AgentToolCall {
        call_id: None,
        tool: "remote_object_rpc_readiness_probe".to_string(),
        input: serde_json::json!({ "input": { "probe": "cancel" } }),
        r#async: false,
        summary: "readiness cancelled remote object rpc".to_string(),
    };
    let outcome = registry.execute_with_cancellation_and_progress(
        &readiness_session(),
        0,
        0,
        &call,
        cancellation_token,
        progress_sink,
    );
    let start_id = transport
        .start_operation_id
        .lock()
        .expect("start op lock")
        .clone();
    let cancel_id = transport
        .cancel_operation_id
        .lock()
        .expect("cancel op lock")
        .clone();
    let cancel_calls = transport.cancel_calls.load(Ordering::SeqCst);
    let cancelled = outcome.status == AgentToolStatus::Cancelled
        && transport.cancelled.load(Ordering::SeqCst)
        && cancel_calls == 1
        && start_id.is_some()
        && start_id == cancel_id
        && outcome
            .output
            .get("cancelled")
            .and_then(serde_json::Value::as_bool)
            == Some(true);
    if cancelled {
        passed(
            "remote_object_rpc_active_cancellation_smoke",
            "remote object-RPC tool transport propagates cancellation by operation id",
            serde_json::json!({
                "tool": call.tool,
                "status": outcome.status,
                "cancel_calls": cancel_calls,
                "operation_id_matched": start_id == cancel_id,
            }),
        )
    } else {
        failed(
            "remote_object_rpc_active_cancellation_smoke",
            "remote object-RPC tool transport cancellation failed",
            serde_json::json!({
                "tool": call.tool,
                "status": outcome.status,
                "cancel_calls": cancel_calls,
                "start_operation_id": start_id,
                "cancel_operation_id": cancel_id,
                "output": outcome.output,
            }),
        )
    }
}

fn check_skill_disclosure_smoke() -> AgentReadinessCheck {
    let package_skill = "rust-coding";
    let skill_plan = AgentSkillPlan::from_package_and_modules(
        [package_skill],
        &AgentModuleToolProjection::new(),
        ["coding-agent"],
    );
    let registry = AgentToolRegistry::new();
    let plan = AgentToolPlanner::plan(
        registry.descriptors(),
        &AgentModuleToolProjection::new(),
        std::iter::empty::<AgentToolSpec>(),
        std::iter::empty::<AgentToolSpec>(),
        false,
        ["discovery"],
        registry.generation(),
    );
    let run_context = AgentRunContext::default();
    let tool_surface = AgentToolSurface::default_for_role(AgentRole::Deliberation);
    let orchestrator =
        AgentToolOrchestrator::new(&tool_surface, None, &run_context).with_skill_plan(skill_plan);
    let session = readiness_session();
    let skill_search = orchestrator.execute(
        &registry,
        &session,
        &plan,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "skill_search".to_string(),
            input: serde_json::json!({ "query": "rust" }),
            r#async: false,
            summary: "readiness package skill search".to_string(),
        },
    );
    let skill_describe = orchestrator.execute(
        &registry,
        &session,
        &plan,
        0,
        1,
        &AgentToolCall {
            call_id: None,
            tool: "skill_describe".to_string(),
            input: serde_json::json!({ "skill": package_skill }),
            r#async: false,
            summary: "readiness package skill describe".to_string(),
        },
    );
    let skill_activate = orchestrator.execute(
        &registry,
        &session,
        &plan,
        0,
        2,
        &AgentToolCall {
            call_id: None,
            tool: "skill_activate".to_string(),
            input: serde_json::json!({ "skill": package_skill, "reason": "readiness activation check" }),
            r#async: false,
            summary: "readiness package skill activation".to_string(),
        },
    );
    let search_found =
        output_matches_string_field(&skill_search.output, "/matches", "id", package_skill);
    let describe_rejected = skill_describe.status == AgentToolStatus::Failed
        && skill_describe
            .output
            .get("error")
            .and_then(serde_json::Value::as_str)
            == Some("invalid_agent_skill")
        && skill_describe
            .output
            .get("skill")
            .and_then(serde_json::Value::as_str)
            == Some(package_skill);
    let search_hides_unreadable_reference = !search_found
        && skill_search
            .output
            .get("count")
            .and_then(serde_json::Value::as_u64)
            == Some(0);
    let activate_blocked = skill_activate.status == AgentToolStatus::Blocked
        && skill_activate
            .output
            .get("error")
            .and_then(serde_json::Value::as_str)
            == Some("skill_content_unavailable")
        && skill_activate
            .output
            .pointer("/activation/mode")
            .and_then(serde_json::Value::as_str)
            == Some("dedicated_tool")
        && skill_activate
            .output
            .pointer("/activation/available")
            .and_then(serde_json::Value::as_bool)
            == Some(false)
        && skill_activate
            .evidence
            .record_value("skill_disclosure")
            .is_some();

    if skill_search.status == AgentToolStatus::Succeeded
        && search_hides_unreadable_reference
        && describe_rejected
        && activate_blocked
    {
        passed(
            "skill_disclosure_smoke",
            "package-declared names without a valid SKILL.md stay out of standard discovery and fail closed",
            serde_json::json!({
                "skill": package_skill,
                "skill_search_count": skill_search.output.get("count"),
                "source": "package",
                "standard_skill_available": false,
                "activation_mode": "dedicated_tool",
                "activation_available": false,
                "skill_describe_status": skill_describe.status,
                "skill_activate_status": skill_activate.status,
            }),
        )
    } else {
        failed(
            "skill_disclosure_smoke",
            "unreadable package skill references did not fail closed",
            serde_json::json!({
                "skill": package_skill,
                "skill_search_status": skill_search.status,
                "skill_search_output": skill_search.output,
                "skill_describe_status": skill_describe.status,
                "skill_describe_output": skill_describe.output,
                "skill_activate_status": skill_activate.status,
                "skill_activate_output": skill_activate.output,
                "search_found": search_found,
                "search_hides_unreadable_reference": search_hides_unreadable_reference,
                "describe_rejected": describe_rejected,
                "activate_blocked": activate_blocked,
            }),
        )
    }
}

fn check_module_projection_disclosure_smoke() -> AgentReadinessCheck {
    let module_id = TaskId::generate();
    let projected_tool = "module_rpc_agent_readiness_status";
    let projected_skill = "agent-readiness";
    let registration = sinomo_runtime::module::ModuleRegistration::native_rust(
        module_id.clone(),
        "0.1.0",
        "agent-readiness",
        "agent-readiness",
        "agent-readiness-v1",
        sinomo_runtime::core::TrustRing::R2TrustedService,
        "agent.readiness",
    )
    .with_surfaces(
        sinomo_runtime::module::ModuleSurfaces::default()
            .with_object_rpc_route(sinomo_runtime::module::ModuleObjectRpcRoute::new(
                "/agent/readiness/status",
                "agent.readiness",
                "status",
                sinomo_runtime::module::ModuleRouteAuth::LocalOrClusterMember,
            ))
            .with_declarative_resource(
                sinomo_runtime::module::ModuleDeclarativeResourceExport::skill(
                    projected_skill,
                    "skills/agent-readiness/SKILL.md",
                )
                .with_description(
                    "Exercise agent module skill disclosure. Use for readiness verification.",
                )
                .with_target_agent("coding-agent")
                .with_activation_tag("readiness")
                .with_embedded_text(
                    "---\nname: agent-readiness\ndescription: Exercise agent module skill disclosure. Use for readiness verification.\n---\n\n# Agent readiness\nCheck module-projected skill disclosure.",
                ),
            ),
    );
    let projection = AgentModuleToolProjection::new()
        .with_registration(registration)
        .with_loader_state(sinomo_runtime::core::ModuleLoaderState {
            module_id: module_id.clone(),
            active_artifact_id: TaskId::generate(),
            previous_artifact_id: None,
            shadow_artifact_id: None,
            candidate_artifact_id: None,
            hot_swap_state: sinomo_runtime::core::HotSwapState::Stable,
        });
    let registry = AgentToolRegistry::new();
    let plan = AgentToolPlanner::plan(
        registry.descriptors(),
        &projection,
        std::iter::empty::<AgentToolSpec>(),
        std::iter::empty::<AgentToolSpec>(),
        false,
        ["discovery"],
        registry.generation(),
    );
    let skill_plan = AgentSkillPlan::from_package_and_modules(
        std::iter::empty::<String>(),
        &projection,
        ["coding-agent"],
    );
    let run_context = AgentRunContext::default();
    let tool_surface = AgentToolSurface::default_for_role(AgentRole::Deliberation);
    let orchestrator =
        AgentToolOrchestrator::new(&tool_surface, None, &run_context).with_skill_plan(skill_plan);
    let session = readiness_session();

    let direct_projection_ok = plan.entry(projected_tool).is_some_and(|entry| {
        entry.source == AgentToolPlanSource::Module
            && !entry.executable
            && !plan.is_model_visible(projected_tool)
    });
    let tool_search = orchestrator.execute(
        &registry,
        &session,
        &plan,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "tool_search".to_string(),
            input: serde_json::json!({ "query": "readiness" }),
            r#async: false,
            summary: "readiness module tool projection search".to_string(),
        },
    );
    let skill_search = orchestrator.execute(
        &registry,
        &session,
        &plan,
        0,
        1,
        &AgentToolCall {
            call_id: None,
            tool: "skill_search".to_string(),
            input: serde_json::json!({ "query": "readiness" }),
            r#async: false,
            summary: "readiness module skill projection search".to_string(),
        },
    );
    let skill_describe = orchestrator.execute(
        &registry,
        &session,
        &plan,
        0,
        2,
        &AgentToolCall {
            call_id: None,
            tool: "skill_describe".to_string(),
            input: serde_json::json!({ "skill": projected_skill }),
            r#async: false,
            summary: "readiness module skill projection describe".to_string(),
        },
    );
    let tool_search_found =
        output_matches_string_field(&tool_search.output, "/matches", "name", projected_tool);
    let skill_search_found =
        output_matches_string_field(&skill_search.output, "/matches", "id", projected_skill);
    let skill_describe_ok = skill_describe
        .output
        .pointer("/descriptor/id")
        .and_then(serde_json::Value::as_str)
        == Some(projected_skill)
        && skill_describe
            .output
            .pointer("/descriptor/source")
            .and_then(serde_json::Value::as_str)
            == Some("module")
        && skill_describe
            .output
            .pointer("/descriptor/path")
            .and_then(serde_json::Value::as_str)
            == Some("skills/agent-readiness/SKILL.md")
        && skill_describe
            .output
            .pointer("/descriptor/has_skill_content")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        && skill_describe
            .output
            .pointer("/activation/mode")
            .and_then(serde_json::Value::as_str)
            == Some("dedicated_tool")
        && skill_describe
            .output
            .pointer("/activation/available")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        && skill_describe
            .evidence
            .record_value("skill_disclosure")
            .is_some();
    if direct_projection_ok
        && tool_search.status == AgentToolStatus::Succeeded
        && skill_search.status == AgentToolStatus::Succeeded
        && skill_describe.status == AgentToolStatus::Succeeded
        && tool_search_found
        && skill_search_found
        && skill_describe_ok
    {
        passed(
            "module_projection_disclosure_smoke",
            "module-projected tools and skills are searchable, and module skills are describable with embedded text",
            serde_json::json!({
                "module_id": module_id,
                "projected_tool": projected_tool,
                "projected_skill": projected_skill,
                "tool_search_count": tool_search.output.get("count"),
                "skill_search_count": skill_search.output.get("count"),
                "skill_describe_source": "module",
                "skill_describe_has_content": true,
                "skill_activation_mode": "dedicated_tool",
                "skill_activation_available": true,
            }),
        )
    } else {
        failed(
            "module_projection_disclosure_smoke",
            "module-projected tool or skill disclosure failed",
            serde_json::json!({
                "module_id": module_id,
                "projected_tool": projected_tool,
                "projected_skill": projected_skill,
                "direct_projection_ok": direct_projection_ok,
                "tool_search_status": tool_search.status,
                "tool_search_output": tool_search.output,
                "skill_search_status": skill_search.status,
                "skill_search_output": skill_search.output,
                "skill_describe_status": skill_describe.status,
                "skill_describe_output": skill_describe.output,
                "skill_describe_ok": skill_describe_ok,
            }),
        )
    }
}

fn output_matches_string_field(
    output: &serde_json::Value,
    array_pointer: &str,
    field: &str,
    expected: &str,
) -> bool {
    output
        .pointer(array_pointer)
        .and_then(serde_json::Value::as_array)
        .is_some_and(|matches| {
            matches
                .iter()
                .any(|entry| entry.get(field).and_then(serde_json::Value::as_str) == Some(expected))
        })
}

fn output_matches_tool_visibility(
    output: &serde_json::Value,
    expected_tool: &str,
    expected_model_visible: Option<bool>,
) -> bool {
    output
        .pointer("/matches")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|matches| {
            matches.iter().any(|entry| {
                entry.get("name").and_then(serde_json::Value::as_str) == Some(expected_tool)
                    && expected_model_visible.is_none_or(|expected| {
                        entry
                            .get("model_visible")
                            .and_then(serde_json::Value::as_bool)
                            == Some(expected)
                    })
            })
        })
}

fn check_json_decision_contract() -> AgentReadinessCheck {
    let decision = parse_agent_loop_response(
        &AgentOutputFormat::JsonDecision,
        r#"{"response_mode":"answer","text":"ready","tool_calls":[],"background_handoff":null,"next_state":"listening"}"#,
    );
    let fallback = parse_agent_loop_response(&AgentOutputFormat::JsonDecision, "plain answer");
    match (decision, fallback) {
        (Ok(decision), Ok(fallback))
            if decision.response_mode == AgentLoopResponseMode::Answer
                && decision.text == "ready"
                && fallback.response_mode == AgentLoopResponseMode::Answer
                && fallback.text == "plain answer" =>
        {
            passed(
                "json_decision_contract",
                "JSON decisions and plain-text fallback parse correctly",
                serde_json::json!({
                    "json_response_mode": decision.response_mode,
                    "fallback_response_mode": fallback.response_mode,
                }),
            )
        }
        (decision, fallback) => failed(
            "json_decision_contract",
            "JSON decision contract parsing failed",
            serde_json::json!({
                "json_error": decision.err().map(|error| error.to_string()),
                "fallback_error": fallback.err().map(|error| error.to_string()),
            }),
        ),
    }
}

fn check_model_response_repair_smoke() -> AgentReadinessCheck {
    let model = ReadinessSequencedModel::new([
        "not json",
        r#"{
            "response_mode": "answer",
            "text": "Recovered readiness response.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#,
    ]);
    let requests = Arc::clone(&model.requests);
    let agent = Agent::new(model, "readiness", "repair-smoke").with_config(
        AgentConfig::new("readiness", "repair-smoke").with_max_model_repair_attempts(1),
    );
    let run = match agent.run(&readiness_agent_loop_input("recover malformed output")) {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "model_response_repair_smoke",
                "model response repair path failed",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let requests = requests.lock().expect("readiness model requests");
    let repair_message_has_marker = requests
        .get(1)
        .and_then(|request| request.messages.last())
        .is_some_and(|message| message.content.contains("agent_model_response_repair"));
    let request_metadata_has_attempt = requests.get(1).is_some_and(|request| {
        request
            .metadata
            .get("model_response_repair_attempt")
            .and_then(serde_json::Value::as_u64)
            == Some(1)
    });
    let recorded_parse_error = run.model_events.iter().any(
        |event| matches!(event, AgentModelEvent::Error { detail, .. } if detail.contains("invalid JSON")),
    );
    if run.stop_reason == AgentStopReason::FinalDecision
        && run.final_decision.text == "Recovered readiness response."
        && run.model_metrics.repair_attempt_count == 1
        && run.model_metrics.model_response_count == 2
        && run.model_metrics.model_response_parse_failure_count == 1
        && run.model_metrics.strict_json_decision_turn_count == 1
        && requests.len() == 2
        && repair_message_has_marker
        && request_metadata_has_attempt
        && recorded_parse_error
    {
        passed(
            "model_response_repair_smoke",
            "malformed model output is repaired through the real agent loop",
            serde_json::json!({
                "request_count": requests.len(),
                "final_text": run.final_decision.text,
                "repair_attempt": 1,
                "model_metrics": run.model_metrics,
                "recorded_parse_error": recorded_parse_error,
            }),
        )
    } else {
        failed(
            "model_response_repair_smoke",
            "model response repair path missed expected evidence",
            serde_json::json!({
                "request_count": requests.len(),
                "stop_reason": run.stop_reason,
                "final_text": run.final_decision.text,
                "model_metrics": run.model_metrics,
                "repair_message_has_marker": repair_message_has_marker,
                "request_metadata_has_attempt": request_metadata_has_attempt,
                "recorded_parse_error": recorded_parse_error,
            }),
        )
    }
}

fn check_provider_error_classification_smoke() -> AgentReadinessCheck {
    let prompt_detail = "context_length_exceeded: maximum context length";
    let output_detail = "completion token limit exceeded";
    let prompt_category = classify_language_model_error_detail(prompt_detail);
    let output_category = classify_language_model_error_detail(output_detail);
    if prompt_category == Some(LanguageModelErrorCategory::PromptTooLarge)
        && output_category == Some(LanguageModelErrorCategory::OutputTooLarge)
        && prompt_category != output_category
    {
        passed(
            "provider_error_classification_smoke",
            "provider prompt-overflow and output-overflow errors classify distinctly",
            serde_json::json!({
                "prompt_detail": prompt_detail,
                "prompt_category": prompt_category,
                "output_detail": output_detail,
                "output_category": output_category,
            }),
        )
    } else {
        failed(
            "provider_error_classification_smoke",
            "provider overflow error classification lost prompt/output distinction",
            serde_json::json!({
                "prompt_detail": prompt_detail,
                "prompt_category": prompt_category,
                "output_detail": output_detail,
                "output_category": output_category,
            }),
        )
    }
}

fn check_model_metrics_smoke(workspace_root: &Path) -> AgentReadinessCheck {
    let json_run = match Agent::new(
        ReadinessSequencedModel::new([r#"{
            "response_mode": "answer",
            "text": "JSON metrics ready.",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening"
        }"#]),
        "readiness",
        "metrics-json",
    )
    .run(&readiness_agent_loop_input("record json metrics"))
    {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "model_metrics_smoke",
                "JSON model metrics run failed",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };

    let text_definition = AgentDefinition::coding().with_output_format(AgentOutputFormat::Text);
    let text_run = match Agent::new(
        ReadinessSequencedModel::new(["Plain readiness metrics answer."]),
        "readiness",
        "metrics-text",
    )
    .with_config(
        AgentConfig::new("readiness", "metrics-text").with_agent_definition(text_definition),
    )
    .run(&readiness_agent_loop_input("record text metrics"))
    {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "model_metrics_smoke",
                "text fallback model metrics run failed",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };

    let native_model = ReadinessNativeToolModel::new();
    let native_requests = Arc::clone(&native_model.requests);
    let native_run = match Agent::new(native_model, "readiness", "metrics-native")
        .with_config(
            AgentConfig::new("readiness", "metrics-native").with_workspace_root(workspace_root),
        )
        .with_tool_registry(readiness_local_registry(workspace_root))
        .run(&readiness_agent_loop_input("record native tool metrics"))
    {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "model_metrics_smoke",
                "native tool model metrics run failed",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let native_request_count = native_requests
        .lock()
        .expect("native metrics model requests")
        .len();

    let fallback_model = ReadinessVendorFallbackToolModel::new();
    let fallback_requests = Arc::clone(&fallback_model.requests);
    let fallback_run = match Agent::new(fallback_model, "readiness", "metrics-vendor-fallback")
        .with_config(
            AgentConfig::new("readiness", "metrics-vendor-fallback")
                .with_workspace_root(workspace_root),
        )
        .with_tool_registry(readiness_local_registry(workspace_root))
        .run(&readiness_agent_loop_input(
            "record vendor fallback tool metrics",
        )) {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "model_metrics_smoke",
                "vendor fallback tool-call model metrics run failed",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let fallback_request_count = fallback_requests
        .lock()
        .expect("vendor fallback metrics model requests")
        .len();

    let repair_model = ReadinessSequencedUsageModel::new([
        (
            "not json",
            Some(sinomo_lm::LanguageModelUsage {
                input_tokens: Some(10),
                output_tokens: Some(1),
            }),
        ),
        (
            r#"{
                "response_mode": "answer",
                "text": "Repair metrics ready.",
                "tool_calls": [],
                "background_handoff": null,
                "next_state": "listening"
            }"#,
            Some(sinomo_lm::LanguageModelUsage {
                input_tokens: Some(20),
                output_tokens: Some(2),
            }),
        ),
    ]);
    let repair_requests = Arc::clone(&repair_model.requests);
    let repair_run = match Agent::new(repair_model, "readiness", "metrics-repair")
        .with_config(
            AgentConfig::new("readiness", "metrics-repair").with_max_model_repair_attempts(1),
        )
        .run(&readiness_agent_loop_input("record repair metrics"))
    {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "model_metrics_smoke",
                "repair model metrics run failed",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let repair_request_message_bytes = repair_requests
        .lock()
        .expect("repair metrics model requests")
        .get(1)
        .and_then(|request| request.messages.last())
        .map(|message| message.content.len())
        .unwrap_or_default();
    let json_event_sources = model_completed_decision_sources(&json_run);
    let text_event_sources = model_completed_decision_sources(&text_run);
    let native_event_sources = model_completed_decision_sources(&native_run);
    let fallback_event_sources = model_completed_decision_sources(&fallback_run);
    let repair_event_sources = model_completed_decision_sources(&repair_run);

    let json_ok = json_run.model_metrics.output_format == AgentOutputFormat::JsonDecision
        && json_run.model_metrics.model_turn_count == 1
        && json_run.model_metrics.model_response_count == 1
        && json_run.model_metrics.model_response_parse_failure_count == 0
        && json_run.model_metrics.strict_json_decision_turn_count == 1
        && json_run.model_metrics.text_fallback_turn_count == 0
        && json_run.model_metrics.native_tool_call_turn_count == 0
        && json_run.model_metrics.repair_attempt_count == 0
        && json_run.model_metrics.final_decision_source
            == Some(AgentModelDecisionSource::StrictJsonDecision)
        && json_event_sources == ["strict_json_decision"];
    let text_ok = text_run.model_metrics.output_format == AgentOutputFormat::Text
        && text_run.model_metrics.model_turn_count == 1
        && text_run.model_metrics.model_response_count == 1
        && text_run.model_metrics.model_response_parse_failure_count == 0
        && text_run.model_metrics.strict_json_decision_turn_count == 0
        && text_run.model_metrics.text_fallback_turn_count == 1
        && text_run.model_metrics.native_tool_call_turn_count == 0
        && text_run.model_metrics.repair_attempt_count == 0
        && text_run.model_metrics.final_decision_source
            == Some(AgentModelDecisionSource::TextFallback)
        && text_event_sources == ["text_fallback"];
    let native_ok = native_run.stop_reason == AgentStopReason::FinalDecision
        && native_run.model_metrics.output_format == AgentOutputFormat::JsonDecision
        && native_run.model_metrics.model_turn_count == 2
        && native_run.model_metrics.model_response_count == 2
        && native_run.model_metrics.model_response_parse_failure_count == 0
        && native_run.model_metrics.strict_json_decision_turn_count == 1
        && native_run.model_metrics.native_tool_call_turn_count == 1
        && native_run.model_metrics.native_tool_call_count == 1
        && native_run.model_metrics.repair_attempt_count == 0
        && native_run.model_metrics.final_decision_source
            == Some(AgentModelDecisionSource::StrictJsonDecision)
        && native_run.tool_results.len() == 1
        && native_run.tool_results[0].status == AgentToolStatus::Succeeded
        && native_request_count == 2
        && native_event_sources == ["native_tool_calls", "strict_json_decision"];
    let fallback_ok = fallback_run.stop_reason == AgentStopReason::FinalDecision
        && fallback_run.model_metrics.output_format == AgentOutputFormat::JsonDecision
        && fallback_run.model_metrics.model_turn_count == 2
        && fallback_run.model_metrics.model_response_count == 2
        && fallback_run
            .model_metrics
            .model_response_parse_failure_count
            == 0
        && fallback_run.model_metrics.strict_json_decision_turn_count == 1
        && fallback_run.model_metrics.native_tool_call_turn_count == 0
        && fallback_run.model_metrics.native_tool_call_count == 0
        && fallback_run
            .model_metrics
            .vendor_fallback_tool_call_turn_count
            == 1
        && fallback_run.model_metrics.vendor_fallback_tool_call_count == 1
        && fallback_run.model_metrics.repair_attempt_count == 0
        && fallback_run.model_metrics.final_decision_source
            == Some(AgentModelDecisionSource::StrictJsonDecision)
        && fallback_run.model_turns[0].decision_source
            == AgentModelDecisionSource::VendorFallbackToolCalls
        && fallback_run.tool_results.len() == 1
        && fallback_run.tool_results[0].status == AgentToolStatus::Succeeded
        && fallback_request_count == 2
        && fallback_event_sources == ["vendor_fallback_tool_calls", "strict_json_decision"];
    let repair_ok = repair_run.model_metrics.output_format == AgentOutputFormat::JsonDecision
        && repair_run.model_metrics.model_turn_count == 1
        && repair_run.model_metrics.model_response_count == 2
        && repair_run.model_metrics.model_response_parse_failure_count == 1
        && repair_run.model_metrics.strict_json_decision_turn_count == 1
        && repair_run.model_metrics.text_fallback_turn_count == 0
        && repair_run.model_metrics.native_tool_call_turn_count == 0
        && repair_run.model_metrics.repair_attempt_count == 1
        && repair_run.model_metrics.repair_request_model_visible_bytes
            == repair_request_message_bytes
        && repair_run.model_metrics.repair_request_model_visible_bytes > 0
        && repair_run.model_metrics.reported_usage.response_count == 2
        && repair_run.model_metrics.reported_usage.input_tokens == 30
        && repair_run.model_metrics.reported_usage.output_tokens == 3
        && repair_run
            .model_metrics
            .repair_reported_usage
            .response_count
            == 1
        && repair_run.model_metrics.repair_reported_usage.input_tokens == 20
        && repair_run.model_metrics.repair_reported_usage.output_tokens == 2
        && repair_run.model_metrics.final_decision_source
            == Some(AgentModelDecisionSource::StrictJsonDecision)
        && repair_event_sources == ["strict_json_decision"];

    if json_ok && text_ok && native_ok && fallback_ok && repair_ok {
        passed(
            "model_metrics_smoke",
            "run-level model metrics and model-completed events cover strict JSON, text fallback, native tool calls, vendor fallback tool calls, repair attempts, repair bytes, and reported repair usage",
            serde_json::json!({
                "json": json_run.model_metrics,
                "text": text_run.model_metrics,
                "native": native_run.model_metrics,
                "vendor_fallback": fallback_run.model_metrics,
                "repair": repair_run.model_metrics,
                "json_event_sources": json_event_sources,
                "text_event_sources": text_event_sources,
                "native_event_sources": native_event_sources,
                "vendor_fallback_event_sources": fallback_event_sources,
                "repair_event_sources": repair_event_sources,
                "native_request_count": native_request_count,
                "vendor_fallback_request_count": fallback_request_count,
                "repair_request_message_bytes": repair_request_message_bytes,
            }),
        )
    } else {
        failed(
            "model_metrics_smoke",
            "run-level model metrics missed an expected decision source or counter",
            serde_json::json!({
                "json_ok": json_ok,
                "text_ok": text_ok,
                "native_ok": native_ok,
                "vendor_fallback_ok": fallback_ok,
                "repair_ok": repair_ok,
                "json": json_run.model_metrics,
                "text": text_run.model_metrics,
                "native": native_run.model_metrics,
                "vendor_fallback": fallback_run.model_metrics,
                "repair": repair_run.model_metrics,
                "json_event_sources": json_event_sources,
                "text_event_sources": text_event_sources,
                "native_event_sources": native_event_sources,
                "vendor_fallback_event_sources": fallback_event_sources,
                "repair_event_sources": repair_event_sources,
                "native_stop_reason": native_run.stop_reason,
                "native_tool_result_count": native_run.tool_results.len(),
                "native_request_count": native_request_count,
                "vendor_fallback_stop_reason": fallback_run.stop_reason,
                "vendor_fallback_tool_result_count": fallback_run.tool_results.len(),
                "vendor_fallback_request_count": fallback_request_count,
                "repair_request_message_bytes": repair_request_message_bytes,
            }),
        )
    }
}

fn model_completed_decision_sources(run: &AgentRun) -> Vec<&str> {
    run.events
        .iter()
        .filter(|event| event.kind.as_str() == "agent.loop.model.completed")
        .filter_map(|event| event.payload["decision_source"].as_str())
        .collect()
}

fn check_tool_pair_integrity_smoke() -> AgentReadinessCheck {
    let balanced_items = vec![
        readiness_tool_call_item("read_file", 0, 0),
        readiness_tool_result_item("read_file", 0, 0),
    ];
    let balanced = AgentToolPairIntegrity::from_items(&balanced_items);
    let mixed_items = vec![
        readiness_tool_call_item("read_file", 0, 0),
        readiness_tool_result_item("read_file", 0, 0),
        readiness_tool_call_item("search", 1, 0),
        readiness_tool_result_item("read_file", 2, 0),
    ];
    let mixed = AgentToolPairIntegrity::from_items(&mixed_items);
    let balanced_ok = balanced.tool_calls == 1
        && balanced.tool_results == 1
        && balanced.orphan_tool_calls == 0
        && balanced.orphan_tool_results == 0
        && balanced.is_balanced();
    let mixed_ok = mixed.tool_calls == 2
        && mixed.tool_results == 2
        && mixed.orphan_tool_calls == 1
        && mixed.orphan_tool_results == 1
        && !mixed.is_balanced();

    if balanced_ok && mixed_ok {
        passed(
            "tool_pair_integrity_smoke",
            "tool-call/tool-result integrity pairs runtime items by step/call slot and detects orphan slots",
            serde_json::json!({
                "balanced": balanced,
                "mixed": mixed,
            }),
        )
    } else {
        failed(
            "tool_pair_integrity_smoke",
            "tool-call/tool-result integrity did not preserve expected pairing semantics",
            serde_json::json!({
                "balanced_ok": balanced_ok,
                "mixed_ok": mixed_ok,
                "balanced": balanced,
                "mixed": mixed,
            }),
        )
    }
}

fn readiness_tool_call_item(tool: &str, step_index: usize, call_index: usize) -> AgentItem {
    AgentItem::ToolCall {
        item_id: TaskId::generate(),
        step_index,
        call_index,
        call: AgentToolCall {
            call_id: None,
            tool: tool.to_string(),
            input: serde_json::json!({ "path": "src/lib.rs" }),
            r#async: false,
            summary: format!("{tool} readiness call"),
        },
    }
}

fn readiness_tool_result_item(tool: &str, step_index: usize, call_index: usize) -> AgentItem {
    AgentItem::ToolResult {
        item_id: TaskId::generate(),
        step_index,
        call_index,
        call_id: TaskId::generate(),
        native_call_id: None,
        tool_name: Some(tool.to_string()),
        status: AgentToolStatus::Succeeded,
        summary: format!("{tool} readiness result"),
        output: serde_json::json!({ "ok": true }),
        evidence: AgentToolEvidence::default(),
    }
}

struct ReadinessSequencedModel {
    responses: Mutex<VecDeque<String>>,
    requests: Arc<Mutex<Vec<LanguageModelRequest>>>,
}

impl ReadinessSequencedModel {
    fn new(responses: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().map(Into::into).collect()),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl InteractionModel for ReadinessSequencedModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.requests
            .lock()
            .expect("readiness model requests")
            .push(request.clone());
        let response = self
            .responses
            .lock()
            .expect("readiness model responses")
            .pop_front()
            .ok_or_else(|| LanguageModelError::ProviderInvocationFailed {
                provider: request.provider.clone(),
                model: request.model.clone(),
                detail: "no readiness model response queued".to_string(),
            })?;
        Ok(LanguageModelResponse {
            provider: request.provider.clone(),
            model: request.model.clone(),
            content: response,
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

struct ReadinessSequencedUsageModel {
    responses: Mutex<VecDeque<(String, Option<sinomo_lm::LanguageModelUsage>)>>,
    requests: Arc<Mutex<Vec<LanguageModelRequest>>>,
}

impl ReadinessSequencedUsageModel {
    fn new<S>(
        responses: impl IntoIterator<Item = (S, Option<sinomo_lm::LanguageModelUsage>)>,
    ) -> Self
    where
        S: Into<String>,
    {
        Self {
            responses: Mutex::new(
                responses
                    .into_iter()
                    .map(|(response, usage)| (response.into(), usage))
                    .collect(),
            ),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl InteractionModel for ReadinessSequencedUsageModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.requests
            .lock()
            .expect("readiness model requests")
            .push(request.clone());
        let (response, usage) = self
            .responses
            .lock()
            .expect("readiness model responses")
            .pop_front()
            .ok_or_else(|| LanguageModelError::ProviderInvocationFailed {
                provider: request.provider.clone(),
                model: request.model.clone(),
                detail: "no readiness model response queued".to_string(),
            })?;
        Ok(LanguageModelResponse {
            provider: request.provider.clone(),
            model: request.model.clone(),
            content: response,
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage,
        })
    }
}

#[derive(Clone)]
struct ReadinessNativeToolModel {
    requests: Arc<Mutex<Vec<LanguageModelRequest>>>,
}

impl ReadinessNativeToolModel {
    fn new() -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl InteractionModel for ReadinessNativeToolModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let turn_index = {
            let mut requests = self.requests.lock().expect("native metrics model requests");
            let turn_index = requests.len();
            requests.push(request.clone());
            turn_index
        };
        if turn_index == 0 {
            return Ok(LanguageModelResponse {
                provider: request.provider.clone(),
                model: request.model.clone(),
                content: String::new(),
                items: Vec::new(),
                tool_calls: vec![LanguageModelToolCall {
                    call_id: Some("call_metrics_read".to_string()),
                    name: "read_file".to_string(),
                    input: serde_json::json!({ "path": "Cargo.toml" }),
                    summary: Some("Read workspace manifest for metrics smoke.".to_string()),
                }],
                usage: None,
            });
        }
        Ok(LanguageModelResponse {
            provider: request.provider.clone(),
            model: request.model.clone(),
            content: serde_json::json!({
                "response_mode": "answer",
                "text": "Native metrics ready.",
                "tool_calls": [],
                "background_handoff": null,
                "next_state": "listening"
            })
            .to_string(),
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

#[derive(Clone)]
struct ReadinessVendorFallbackToolModel {
    requests: Arc<Mutex<Vec<LanguageModelRequest>>>,
}

impl ReadinessVendorFallbackToolModel {
    fn new() -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl InteractionModel for ReadinessVendorFallbackToolModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let turn_index = {
            let mut requests = self
                .requests
                .lock()
                .expect("vendor fallback metrics model requests");
            let turn_index = requests.len();
            requests.push(request.clone());
            turn_index
        };
        if turn_index == 0 {
            let dsml_bar = "\u{ff5c}";
            let content = format!(
                "I will inspect the workspace manifest.\n<{bar}{bar}DSML{bar}{bar}tool_calls><{bar}{bar}DSML{bar}{bar}invoke name=\"read_file\"><{bar}{bar}DSML{bar}{bar}parameter name=\"path\">Cargo.toml</{bar}{bar}DSML{bar}{bar}parameter></{bar}{bar}DSML{bar}{bar}invoke></{bar}{bar}DSML{bar}{bar}tool_calls>",
                bar = dsml_bar,
            );
            return Ok(LanguageModelResponse {
                provider: request.provider.clone(),
                model: request.model.clone(),
                content,
                items: Vec::new(),
                tool_calls: Vec::new(),
                usage: None,
            });
        }
        Ok(LanguageModelResponse {
            provider: request.provider.clone(),
            model: request.model.clone(),
            content: serde_json::json!({
                "response_mode": "answer",
                "text": "Vendor fallback metrics ready.",
                "tool_calls": [],
                "background_handoff": null,
                "next_state": "listening"
            })
            .to_string(),
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

#[derive(Clone)]
struct ReadinessBlockingModel {
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl ReadinessBlockingModel {
    fn new() -> Self {
        Self {
            release: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    fn release(&self) {
        let (released, changed) = &*self.release;
        *released
            .lock()
            .expect("readiness blocking model release lock") = true;
        changed.notify_all();
    }
}

impl InteractionModel for ReadinessBlockingModel {
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let (released, changed) = &*self.release;
        let mut released = released.lock().expect("readiness blocking model wait lock");
        while !*released {
            released = changed
                .wait(released)
                .expect("readiness blocking model wait");
        }
        Ok(LanguageModelResponse {
            provider: request.provider.clone(),
            model: request.model.clone(),
            content: serde_json::json!({
                "response_mode": "answer",
                "text": "Runtime live reattach completed.",
                "tool_calls": [],
                "background_handoff": null,
                "next_state": "listening"
            })
            .to_string(),
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }

    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation_token: &AgentCancellationToken,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        on_event(LanguageModelStreamEvent::Started {
            provider: request.provider.clone(),
            model: request.model.clone(),
        });
        let (released, changed) = &*self.release;
        let mut released = released.lock().expect("readiness blocking model wait lock");
        while !*released && !cancellation_token.is_cancelled() {
            let (next_released, _timeout) = changed
                .wait_timeout(released, std::time::Duration::from_millis(10))
                .expect("readiness blocking model wait");
            released = next_released;
        }
        if cancellation_token.is_cancelled() {
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: request.provider.clone(),
                model: request.model.clone(),
                detail: "cancelled by readiness runtime-control token".to_string(),
            });
        }
        drop(released);
        self.complete(request)
    }
}

fn check_native_tool_adapter_smoke() -> AgentReadinessCheck {
    let tool = LanguageModelToolSpec {
        name: "apply_patch".to_string(),
        description: "Apply a patch.".to_string(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "patch": { "type": "string" }
            }
        }),
        output_schema: serde_json::Value::Null,
        supports_parallel: false,
        provider_specs: vec![LanguageModelProviderToolSpec {
            provider: "openai".to_string(),
            custom: Some(LanguageModelCustomToolSpec {
                format: LanguageModelCustomToolFormat::Grammar {
                    syntax: "lark".to_string(),
                    definition: "start: /.+/".to_string(),
                },
                raw_input_argument: Some("patch".to_string()),
            }),
        }],
    };
    let response = LanguageModelResponse {
        provider: "openai".to_string(),
        model: "gpt-5.2-codex".to_string(),
        content: String::new(),
        items: Vec::new(),
        tool_calls: vec![LanguageModelToolCall {
            call_id: Some("call_patch".to_string()),
            name: "apply_patch".to_string(),
            input: serde_json::json!("*** Begin Patch\n*** End Patch\n"),
            summary: None,
        }],
        usage: None,
    };
    let output =
        collect_language_model_tool_calls("openai", "gpt-5.2-codex", &response, &[], &[tool]);
    if output.source == LanguageModelToolAdapterSource::Native
        && output.tool_calls.len() == 1
        && output.tool_calls[0].input["patch"] == "*** Begin Patch\n*** End Patch\n"
    {
        passed(
            "native_tool_adapter_smoke",
            "provider-native tool calls normalize into agent tool input",
            serde_json::json!({
                "source": output.source,
                "tool": output.tool_calls[0].name,
            }),
        )
    } else {
        failed(
            "native_tool_adapter_smoke",
            "provider-native tool call normalization failed",
            serde_json::json!({
                "source": output.source,
                "tool_call_count": output.tool_calls.len(),
            }),
        )
    }
}

fn check_compaction_projection_smoke() -> AgentReadinessCheck {
    let mut transcript = AgentTranscript::new();
    transcript.push(AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "do not lose this constraint".to_string(),
        event_id: None,
    });
    transcript.push(AgentItem::AssistantText {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "constraint recorded".to_string(),
    });
    let summary = AgentCompactor::default().compact_retaining_tail_count(
        &transcript,
        "readiness smoke",
        100,
        1,
    );
    if summary.projection.source_item_count == 2
        && summary.projection.retained_item_ids.len() == 1
        && summary
            .policy_constraints
            .iter()
            .any(|constraint| constraint.contains("runtime tool surface"))
    {
        passed(
            "compaction_projection_smoke",
            "deterministic compaction preserves typed projection invariants",
            serde_json::json!({
                "source_item_count": summary.projection.source_item_count,
                "retained_item_count": summary.projection.retained_item_ids.len(),
                "strategy_id": summary.strategy_id,
            }),
        )
    } else {
        failed(
            "compaction_projection_smoke",
            "deterministic compaction projection missed expected invariants",
            serde_json::json!({
                "source_item_count": summary.projection.source_item_count,
                "retained_item_count": summary.projection.retained_item_ids.len(),
                "policy_constraints": summary.policy_constraints,
            }),
        )
    }
}

fn check_compaction_eval_harness_smoke(workspace_root: &Path) -> AgentReadinessCheck {
    let script_path = workspace_file_path(
        workspace_root,
        &[
            "modules",
            "benchmark",
            "sinomo-benchmarks",
            "agent",
            "run_compaction_eval.py",
        ],
    );
    let catalog_path = workspace_file_path(
        workspace_root,
        &["modules", "benchmark", "sinomo-benchmarks", "src", "lib.rs"],
    );
    let docs_path = agent_system_architecture_path(workspace_root);
    let mut failures = Vec::new();
    let script = read_readiness_text(&script_path, "compaction eval script", &mut failures);
    let catalog = read_readiness_text(&catalog_path, "benchmark catalog", &mut failures);
    let docs = read_readiness_text(&docs_path, "agent system architecture docs", &mut failures);

    let required_scenarios = [
        "long-session-resume",
        "constraint-retention",
        "coding-after-compaction",
        "cluster-ops-after-compaction",
    ];
    for scenario in required_scenarios {
        if !script.contains(scenario) {
            failures.push(serde_json::json!({
                "path": script_path,
                "scenario": scenario,
                "error": "compaction eval script is missing a required scenario",
            }));
        }
    }

    let required_script_markers = [
        "REQUIRED_SCENARIO_IDS",
        "EVAL_PHASES",
        "\"compaction-summary\"",
        "\"post-compaction-continuation\"",
        "requires_live_cluster=True",
        "validate_scenario_definitions",
        "choices=[\"plan\", \"fixture\", \"run\"]",
        "continuation_resumes_durable_cli_session",
        "continuation_does_not_reinject_summary",
        "phase_one_invokes_agent_op_compact",
        "phase_one_requires_persisted_projection_evidence",
        "--compact-after",
        "missing_marker_phases",
    ];
    for marker in required_script_markers {
        if !script.contains(marker) {
            failures.push(serde_json::json!({
                "path": script_path,
                "marker": marker,
                "error": "compaction eval script is missing required plan, fixture, run, or scoring structure",
            }));
        }
    }

    let required_catalog_markers = [
        "agent_compaction_quality_plan",
        "agent-compaction-quality",
        "agent/run_compaction_eval.py",
        "--mode fixture",
        "--mode run",
        "live/model eval harness",
    ];
    for marker in required_catalog_markers {
        if !catalog.contains(marker) {
            failures.push(serde_json::json!({
                "path": catalog_path,
                "marker": marker,
                "error": "benchmark catalog no longer exposes the compaction-quality eval plan",
            }));
        }
    }

    let required_doc_markers = [
        "Model quality is eval work, not ordinary Rust unit testing",
        "run_compaction_eval.py --mode plan",
        "run_compaction_eval.py --mode fixture",
        "run_compaction_eval.py --mode run",
        "Live/model results are research and eval evidence",
        "local readiness remains responsible only for deterministic projection and persistence invariants",
    ];
    for marker in required_doc_markers {
        if !docs.contains(marker) {
            failures.push(serde_json::json!({
                "path": docs_path,
                "marker": marker,
                "error": "agent architecture docs no longer describe the compaction eval/readiness boundary",
            }));
        }
    }

    if failures.is_empty() {
        passed(
            "compaction_eval_harness_smoke",
            "compaction quality stays in the benchmark eval harness with required scenarios, durable CLI resume, and plan/fixture/run modes",
            serde_json::json!({
                "script_path": script_path,
                "catalog_path": catalog_path,
                "docs_path": docs_path,
                "required_scenarios": required_scenarios,
                "required_script_markers": required_script_markers,
            }),
        )
    } else {
        failed(
            "compaction_eval_harness_smoke",
            "compaction eval harness readiness checks failed",
            serde_json::json!({
                "script_path": script_path,
                "catalog_path": catalog_path,
                "docs_path": docs_path,
                "failures": failures,
            }),
        )
    }
}

fn read_readiness_text(path: &Path, label: &str, failures: &mut Vec<serde_json::Value>) -> String {
    match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) => {
            failures.push(serde_json::json!({
                "path": path,
                "label": label,
                "error": error.to_string(),
            }));
            String::new()
        }
    }
}

fn check_session_snapshot_resume_smoke() -> AgentReadinessCheck {
    let session_id = SessionId::from_legacy_named("agent_readiness_session").expect("session id");
    let mut session = AgentSession::new(session_id.clone(), crate::AgentRole::Deliberation);
    session.transcript.push(AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 0,
        content: "resume me".to_string(),
        event_id: None,
    });
    let snapshot = AgentSessionSnapshot::new(session);
    match serde_json::to_vec(&snapshot)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<AgentSessionSnapshot>(&bytes).ok())
    {
        Some(round_trip)
            if round_trip.session.session_id == session_id
                && round_trip.session.transcript.items.len() == 1 =>
        {
            passed(
                "session_snapshot_resume_smoke",
                "session snapshots round-trip with transcript state",
                serde_json::json!({
                    "session_id": session_id,
                    "item_count": round_trip.session.transcript.items.len(),
                }),
            )
        }
        _ => failed(
            "session_snapshot_resume_smoke",
            "session snapshot round-trip failed",
            serde_json::json!({ "session_id": session_id }),
        ),
    }
}

fn check_agent_runtime_service_routes() -> AgentReadinessCheck {
    let registry = crate::register_services(ServiceRegistry::builder()).build();
    let route_categories = registry
        .route_categories()
        .into_iter()
        .map(|route| {
            serde_json::json!({
                "route": route.route,
                "category": route.category,
                "auth_scope": route.auth_scope,
                "required_capabilities": route.required_capabilities,
                "concurrency_limit": route.concurrency_limit,
                "timeout_ms": route.timeout_ms,
            })
        })
        .collect::<Vec<_>>();
    let missing = AGENT_OBJECT_RPC_ROUTES
        .iter()
        .copied()
        .filter(|route| !registry.owns(route))
        .collect::<Vec<_>>();
    let wrong_categories = AGENT_OBJECT_RPC_ROUTES
        .iter()
        .copied()
        .filter(|route| registry.category_for_route(route) != Some(AGENT_RPC_CATEGORY))
        .collect::<Vec<_>>();
    if missing.is_empty() && wrong_categories.is_empty() {
        passed(
            "agent_runtime_service_routes",
            "agent runtime inspect/progress/result routes are registered",
            serde_json::json!({
                "routes": AGENT_OBJECT_RPC_ROUTES,
                "route_categories": route_categories,
            }),
        )
    } else {
        failed(
            "agent_runtime_service_routes",
            "agent runtime service route registration failed",
            serde_json::json!({
                "missing": missing,
                "wrong_categories": wrong_categories,
                "route_categories": route_categories,
            }),
        )
    }
}

fn check_agent_runtime_empty_routes_smoke() -> AgentReadinessCheck {
    let data_root = std::env::temp_dir().join(format!(
        "sinomo-agent-runtime-empty-routes-readiness-{}",
        TaskId::generate()
    ));
    let data_dir = Arc::new(SinomoDataDir::new(&data_root));
    let session_id =
        SessionId::from_legacy_named("sess_agent_readiness_runtime_empty").expect("session");
    let result = (|| -> Result<
        (
            AgentSessionInspectResponse,
            AgentSessionProgressResponse,
            AgentSessionTerminalResultsResponse,
        ),
        String,
    > {
        let registry = crate::register_services(ServiceRegistry::builder()).build();
        let inspect = dispatch_readiness_rpc::<AgentSessionInspectResponse, _>(
            &registry,
            Arc::clone(&data_dir),
            AGENT_SESSION_INSPECT_ROUTE,
            &AgentSessionInspectRequest {
                session_id: session_id.clone(),
            },
        )?;
        let progress = dispatch_readiness_rpc::<AgentSessionProgressResponse, _>(
            &registry,
            Arc::clone(&data_dir),
            AGENT_SESSION_PROGRESS_ROUTE,
            &AgentSessionProgressRequest {
                session_id: session_id.clone(),
                cursor: None,
            },
        )?;
        let terminal_results = dispatch_readiness_rpc::<AgentSessionTerminalResultsResponse, _>(
            &registry,
            Arc::clone(&data_dir),
            AGENT_SESSION_TERMINAL_RESULTS_ROUTE,
            &AgentSessionTerminalResultsRequest {
                session_id: session_id.clone(),
                task_id: None,
            },
        )?;
        Ok((inspect, progress, terminal_results))
    })();

    match result {
        Ok((inspect, progress, terminal_results))
            if inspect.session_id == session_id
                && inspect.session.is_none()
                && inspect.control_snapshot.is_none()
                && inspect.event_cursor.offset == 0
                && inspect.terminal_results.is_empty()
                && inspect.persisted_event_count == 0
                && progress.session_id == session_id
                && progress.batch.events.is_empty()
                && progress.batch.task_updates.is_empty()
                && progress.batch.terminal_results.is_empty()
                && progress.batch.next_cursor.offset == 0
                && terminal_results.session_id == session_id
                && terminal_results.terminal_results.is_empty() =>
        {
            passed(
                "agent_runtime_empty_routes_smoke",
                "agent runtime inspect/progress/result object-RPC routes return well-formed empty state",
                serde_json::json!({
                    "session_id": session_id,
                    "routes": {
                        "inspect": AGENT_SESSION_INSPECT_ROUTE,
                        "progress": AGENT_SESSION_PROGRESS_ROUTE,
                        "terminal_results": AGENT_SESSION_TERMINAL_RESULTS_ROUTE,
                    },
                    "has_session": inspect.session.is_some(),
                    "has_control_snapshot": inspect.control_snapshot.is_some(),
                    "persisted_event_count": inspect.persisted_event_count,
                    "inspect_terminal_result_count": inspect.terminal_results.len(),
                    "progress_event_count": progress.batch.events.len(),
                    "progress_task_update_count": progress.batch.task_updates.len(),
                    "progress_terminal_result_count": progress.batch.terminal_results.len(),
                    "terminal_result_count": terminal_results.terminal_results.len(),
                }),
            )
        }
        Ok((inspect, progress, terminal_results)) => failed(
            "agent_runtime_empty_routes_smoke",
            "agent runtime empty route smoke returned unexpected metadata",
            serde_json::json!({
                "session_id": session_id,
                "inspect_session_id": inspect.session_id,
                "progress_session_id": progress.session_id,
                "terminal_results_session_id": terminal_results.session_id,
                "has_session": inspect.session.is_some(),
                "has_control_snapshot": inspect.control_snapshot.is_some(),
                "persisted_event_count": inspect.persisted_event_count,
                "event_cursor_offset": inspect.event_cursor.offset,
                "inspect_terminal_result_count": inspect.terminal_results.len(),
                "progress_event_count": progress.batch.events.len(),
                "progress_task_update_count": progress.batch.task_updates.len(),
                "progress_terminal_result_count": progress.batch.terminal_results.len(),
                "progress_next_cursor_offset": progress.batch.next_cursor.offset,
                "terminal_result_count": terminal_results.terminal_results.len(),
            }),
        ),
        Err(error) => failed(
            "agent_runtime_empty_routes_smoke",
            "agent runtime empty route smoke failed",
            serde_json::json!({
                "session_id": session_id,
                "error": error,
            }),
        ),
    }
}

fn check_agent_runtime_persisted_routes_smoke() -> AgentReadinessCheck {
    let data_root = std::env::temp_dir().join(format!(
        "sinomo-agent-runtime-persisted-routes-readiness-{}",
        TaskId::generate()
    ));
    let data_dir = Arc::new(SinomoDataDir::new(&data_root));
    let store = FileAgentSessionStore::for_data_dir(data_dir.as_ref());
    let session_id =
        SessionId::from_legacy_named("sess_agent_readiness_runtime_routes").expect("session");
    let session = AgentSession::new(session_id.clone(), AgentRole::Main);
    let archived_task = AgentTaskRef::new("coding", "readiness archived runtime route")
        .with_owner_role(AgentRole::Main);
    let archived_op_id =
        TaskId::from_legacy_named("op_agent_readiness_runtime_routes_archived").expect("op");
    let mut archived_task_snapshot =
        AgentTaskSnapshot::new(archived_task.clone(), AgentTaskLifecycleStatus::Completed);
    archived_task_snapshot.progress_summary =
        Some("completed before the latest control snapshot".to_string());
    let archived_event = crate::AgentEvent::new(
        archived_op_id,
        session_id.clone(),
        crate::AgentEventPayload::TaskStatusChanged {
            snapshot: archived_task_snapshot,
        },
    );
    let task = AgentTaskRef::new("coding", "readiness persisted runtime route")
        .with_owner_role(AgentRole::Main);
    let op_id = TaskId::from_legacy_named("op_agent_readiness_runtime_routes").expect("op");
    let mut task_snapshot =
        AgentTaskSnapshot::new(task.clone(), AgentTaskLifecycleStatus::Completed);
    task_snapshot.progress_summary = Some("completed through readiness route smoke".to_string());
    let event = crate::AgentEvent::new(
        op_id,
        session_id.clone(),
        crate::AgentEventPayload::TaskStatusChanged {
            snapshot: task_snapshot.clone(),
        },
    );
    let event_id = event.event_id.clone();
    let unrelated_task_id =
        TaskId::from_legacy_named("task_agent_readiness_unrelated").expect("unrelated task");
    let control_snapshot = AgentSessionControlSnapshot {
        session_id: session_id.clone(),
        next_sequence: 1,
        active_turn: None,
        tasks: vec![task_snapshot],
        buffered_event_count: 0,
    };
    let result = (|| -> Result<
        (
            AgentSessionInspectResponse,
            AgentSessionProgressResponse,
            AgentSessionProgressResponse,
            AgentSessionTerminalResultsResponse,
            AgentSessionTerminalResultsResponse,
            AgentSessionTerminalResultsResponse,
        ),
        String,
    > {
        store
            .save_session(&session)
            .map_err(|error| error.to_string())?;
        store
            .append_record(&AgentSessionStoreRecord::new(
                session_id.clone(),
                AgentSessionStoreRecordKind::ControlEventPublished {
                    event: archived_event,
                },
            ))
            .map_err(|error| error.to_string())?;
        store
            .append_record(&AgentSessionStoreRecord::new(
                session_id.clone(),
                AgentSessionStoreRecordKind::ControlEventPublished { event },
            ))
            .map_err(|error| error.to_string())?;
        store
            .append_record(&AgentSessionStoreRecord::new(
                session_id.clone(),
                AgentSessionStoreRecordKind::ControlSnapshotSaved {
                    snapshot: control_snapshot,
                },
            ))
            .map_err(|error| error.to_string())?;
        let registry = crate::register_services(ServiceRegistry::builder()).build();
        let inspect = dispatch_readiness_rpc::<AgentSessionInspectResponse, _>(
            &registry,
            Arc::clone(&data_dir),
            AGENT_SESSION_INSPECT_ROUTE,
            &AgentSessionInspectRequest {
                session_id: session_id.clone(),
            },
        )?;
        let progress = dispatch_readiness_rpc::<AgentSessionProgressResponse, _>(
            &registry,
            Arc::clone(&data_dir),
            AGENT_SESSION_PROGRESS_ROUTE,
            &AgentSessionProgressRequest {
                session_id: session_id.clone(),
                cursor: None,
            },
        )?;
        let progress_after_cursor = dispatch_readiness_rpc::<AgentSessionProgressResponse, _>(
            &registry,
            Arc::clone(&data_dir),
            AGENT_SESSION_PROGRESS_ROUTE,
            &AgentSessionProgressRequest {
                session_id: session_id.clone(),
                cursor: Some(AgentSessionControlEventCursor {
                    offset: 2,
                    last_event_id: Some(event_id.clone()),
                }),
            },
        )?;
        let terminal_results = dispatch_readiness_rpc::<AgentSessionTerminalResultsResponse, _>(
            &registry,
            Arc::clone(&data_dir),
            AGENT_SESSION_TERMINAL_RESULTS_ROUTE,
            &AgentSessionTerminalResultsRequest {
                session_id: session_id.clone(),
                task_id: None,
            },
        )?;
        let filtered_terminal_results =
            dispatch_readiness_rpc::<AgentSessionTerminalResultsResponse, _>(
                &registry,
                Arc::clone(&data_dir),
                AGENT_SESSION_TERMINAL_RESULTS_ROUTE,
                &AgentSessionTerminalResultsRequest {
                    session_id: session_id.clone(),
                    task_id: Some(task.task_id.clone()),
                },
            )?;
        let unrelated_terminal_results =
            dispatch_readiness_rpc::<AgentSessionTerminalResultsResponse, _>(
                &registry,
                Arc::clone(&data_dir),
                AGENT_SESSION_TERMINAL_RESULTS_ROUTE,
                &AgentSessionTerminalResultsRequest {
                    session_id: session_id.clone(),
                    task_id: Some(unrelated_task_id.clone()),
                },
            )?;
        Ok((
            inspect,
            progress,
            progress_after_cursor,
            terminal_results,
            filtered_terminal_results,
            unrelated_terminal_results,
        ))
    })();

    match result {
        Ok((
            inspect,
            progress,
            progress_after_cursor,
            terminal_results,
            filtered_terminal_results,
            unrelated_terminal_results,
        )) if inspect.session.is_some()
            && inspect.control_snapshot.is_some()
            && inspect.persisted_event_count == 2
            && inspect.event_cursor.offset == 2
            && inspect.terminal_results.len() == 2
            && inspect.terminal_results[0].task.task_id == task.task_id
            && inspect
                .terminal_results
                .iter()
                .any(|result| result.task.task_id == archived_task.task_id)
            && progress.batch.events.len() == 2
            && progress.batch.next_cursor.last_event_id.as_ref() == Some(&event_id)
            && progress.batch.task_updates.len() == 2
            && progress.batch.terminal_results.len() == 2
            && progress.batch.next_cursor.offset == 2
            && progress_after_cursor.batch.events.is_empty()
            && progress_after_cursor.batch.task_updates.is_empty()
            && progress_after_cursor.batch.terminal_results.is_empty()
            && progress_after_cursor.batch.next_cursor.offset == 2
            && progress_after_cursor
                .batch
                .next_cursor
                .last_event_id
                .as_ref()
                == Some(&event_id)
            && terminal_results.terminal_results.len() == 2
            && terminal_results.terminal_results[0].task.task_id == task.task_id
            && terminal_results
                .terminal_results
                .iter()
                .any(|result| result.task.task_id == archived_task.task_id)
            && filtered_terminal_results.terminal_results.len() == 1
            && filtered_terminal_results.terminal_results[0].task.task_id == task.task_id
            && unrelated_terminal_results.terminal_results.is_empty() =>
        {
            passed(
                "agent_runtime_persisted_routes_smoke",
                "agent runtime inspect/progress/result object-RPC routes read persisted session state with cursor and task filters",
                serde_json::json!({
                    "session_id": session_id,
                    "routes": {
                        "inspect": AGENT_SESSION_INSPECT_ROUTE,
                        "progress": AGENT_SESSION_PROGRESS_ROUTE,
                        "terminal_results": AGENT_SESSION_TERMINAL_RESULTS_ROUTE,
                    },
                    "persisted_event_count": inspect.persisted_event_count,
                    "event_cursor_offset": inspect.event_cursor.offset,
                    "progress_event_count": progress.batch.events.len(),
                    "progress_task_update_count": progress.batch.task_updates.len(),
                    "progress_terminal_result_count": progress.batch.terminal_results.len(),
                    "progress_after_cursor_event_count": progress_after_cursor.batch.events.len(),
                    "progress_after_cursor_terminal_result_count": progress_after_cursor.batch.terminal_results.len(),
                    "progress_after_cursor_next_cursor_last_event_id": progress_after_cursor.batch.next_cursor.last_event_id,
                    "terminal_result_count": terminal_results.terminal_results.len(),
                    "filtered_terminal_result_count": filtered_terminal_results.terminal_results.len(),
                    "unrelated_terminal_result_count": unrelated_terminal_results.terminal_results.len(),
                }),
            )
        }
        Ok((
            inspect,
            progress,
            progress_after_cursor,
            terminal_results,
            filtered_terminal_results,
            unrelated_terminal_results,
        )) => failed(
            "agent_runtime_persisted_routes_smoke",
            "agent runtime persisted route smoke returned unexpected metadata",
            serde_json::json!({
                "session_id": session_id,
                "has_session": inspect.session.is_some(),
                "has_control_snapshot": inspect.control_snapshot.is_some(),
                "persisted_event_count": inspect.persisted_event_count,
                "event_cursor_offset": inspect.event_cursor.offset,
                "inspect_terminal_result_count": inspect.terminal_results.len(),
                "progress_event_count": progress.batch.events.len(),
                "progress_task_update_count": progress.batch.task_updates.len(),
                "progress_terminal_result_count": progress.batch.terminal_results.len(),
                "progress_next_cursor_offset": progress.batch.next_cursor.offset,
                "progress_next_cursor_last_event_id": progress.batch.next_cursor.last_event_id,
                "progress_after_cursor_event_count": progress_after_cursor.batch.events.len(),
                "progress_after_cursor_task_update_count": progress_after_cursor.batch.task_updates.len(),
                "progress_after_cursor_terminal_result_count": progress_after_cursor.batch.terminal_results.len(),
                "progress_after_cursor_next_cursor_offset": progress_after_cursor.batch.next_cursor.offset,
                "terminal_result_count": terminal_results.terminal_results.len(),
                "filtered_terminal_result_count": filtered_terminal_results.terminal_results.len(),
                "unrelated_terminal_result_count": unrelated_terminal_results.terminal_results.len(),
            }),
        ),
        Err(error) => failed(
            "agent_runtime_persisted_routes_smoke",
            "agent runtime persisted route smoke failed",
            serde_json::json!({
                "session_id": session_id,
                "error": error,
            }),
        ),
    }
}

fn check_agent_runtime_control_service_routes() -> AgentReadinessCheck {
    let registry = crate::register_runtime_control_services(
        ServiceRegistry::builder(),
        Arc::new(ReadinessAgentRuntimeControlHost),
    )
    .build();
    let route_categories = registry
        .route_categories()
        .into_iter()
        .map(|route| {
            serde_json::json!({
                "route": route.route,
                "category": route.category,
                "auth_scope": route.auth_scope,
                "required_capabilities": route.required_capabilities,
                "concurrency_limit": route.concurrency_limit,
                "timeout_ms": route.timeout_ms,
            })
        })
        .collect::<Vec<_>>();
    let missing = AGENT_RUNTIME_CONTROL_OBJECT_RPC_ROUTES
        .iter()
        .copied()
        .filter(|route| !registry.owns(route))
        .collect::<Vec<_>>();
    let wrong_categories = AGENT_RUNTIME_CONTROL_OBJECT_RPC_ROUTES
        .iter()
        .copied()
        .filter(|route| registry.category_for_route(route) != Some(AGENT_RPC_CATEGORY))
        .collect::<Vec<_>>();
    let unexpected_read_routes = AGENT_OBJECT_RPC_ROUTES
        .iter()
        .copied()
        .filter(|route| registry.owns(route))
        .collect::<Vec<_>>();
    if missing.is_empty() && wrong_categories.is_empty() && unexpected_read_routes.is_empty() {
        passed(
            "agent_runtime_control_service_routes",
            "agent runtime control routes are available only when a host is injected",
            serde_json::json!({
                "routes": AGENT_RUNTIME_CONTROL_OBJECT_RPC_ROUTES,
                "route_categories": route_categories,
            }),
        )
    } else {
        failed(
            "agent_runtime_control_service_routes",
            "agent runtime control route registration failed",
            serde_json::json!({
                "missing": missing,
                "wrong_categories": wrong_categories,
                "unexpected_read_routes": unexpected_read_routes,
                "route_categories": route_categories,
            }),
        )
    }
}

#[derive(Clone, Default)]
struct ReadinessResponseSink {
    frames: Arc<Mutex<Vec<ObjectRpcOutgoingResponse>>>,
}

impl ReadinessResponseSink {
    fn take_frames(&self) -> Vec<ObjectRpcOutgoingResponse> {
        std::mem::take(&mut *self.frames.lock().expect("readiness response frames"))
    }
}

impl ResponseSink for ReadinessResponseSink {
    fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
        self.frames
            .lock()
            .expect("readiness response frames")
            .push(response);
        Ok(())
    }
}

fn dispatch_readiness_rpc<T, B>(
    registry: &ServiceRegistry,
    data_dir: Arc<SinomoDataDir>,
    route: &str,
    body: &B,
) -> Result<T, String>
where
    T: DeserializeOwned,
    B: Serialize + ?Sized,
{
    let sink = ReadinessResponseSink::default();
    let ctx = ServiceContext::new(
        data_dir,
        None,
        ServiceSigner::Local,
        ObjectRpcIncomingBody::empty(),
        Box::new(sink.clone()),
    );
    let request = ObjectRpcRequest::encode_body(route, AGENT_RPC_CATEGORY, body)
        .map_err(|error| error.to_string())?;
    block_on_readiness_future(registry.dispatch(ctx, request))
        .map_err(|error| error.to_string())?;
    let mut frames = sink.take_frames();
    if frames.len() != 1 {
        return Err(format!(
            "expected one object-RPC response frame for {route}, got {}",
            frames.len()
        ));
    }
    let response = block_on_readiness_future(
        frames
            .pop()
            .expect("checked one readiness response")
            .into_materialized_value_response(usize::MAX),
    )
    .map_err(|error| error.to_string())?;
    response.decode_body().map_err(|error| error.to_string())
}

fn block_on_readiness_future<F: Future>(future: F) -> F::Output {
    if tokio::runtime::Handle::try_current().is_err() {
        return tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build agent readiness Tokio runtime")
            .block_on(future);
    }

    // Readiness can also be invoked by a caller already running on Tokio.
    // Poll in that entered runtime so deferred SOBJ materialization can use
    // its blocking pool without attempting to nest another runtime.
    let waker = readiness_noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match Pin::new(&mut future).poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn readiness_noop_waker() -> Waker {
    unsafe { Waker::from_raw(readiness_noop_raw_waker()) }
}

fn readiness_noop_raw_waker() -> RawWaker {
    RawWaker::new(std::ptr::null(), &READINESS_NOOP_WAKER_VTABLE)
}

static READINESS_NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    readiness_noop_waker_clone,
    readiness_noop_waker_wake,
    readiness_noop_waker_wake,
    readiness_noop_waker_drop,
);

unsafe fn readiness_noop_waker_clone(_: *const ()) -> RawWaker {
    readiness_noop_raw_waker()
}

unsafe fn readiness_noop_waker_wake(_: *const ()) {}

unsafe fn readiness_noop_waker_drop(_: *const ()) {}

fn check_agent_runtime_live_reattach_smoke() -> AgentReadinessCheck {
    let task = AgentTaskRef::new("coding", "live runtime reattach readiness")
        .with_owner_role(AgentRole::Main);
    let input = readiness_agent_loop_input("hold an active turn for live reattach");
    let session_id = input.session.session_id.clone();
    let result = (|| -> Result<
        (
            AgentSessionControlResult,
            AgentSessionControlResult,
            AgentSessionControlResult,
            AgentSessionControlResult,
            AgentSessionResumeResponse,
            AgentTaskLifecycleStatus,
        ),
        String,
    > {
        let model = ReadinessBlockingModel::new();
        let release = model.clone();
        let data_root = std::env::temp_dir().join(format!(
            "sinomo-agent-runtime-control-rpc-readiness-{}",
            TaskId::generate()
        ));
        let data_dir = Arc::new(SinomoDataDir::new(&data_root));
        let config = AgentConfig::new("readiness", "runtime-live-reattach-smoke")
            .with_task_ref(task.clone());
        let agent =
            Agent::new(model, "readiness", "runtime-live-reattach-smoke").with_config(config);
        let runtime = AgentSessionRuntime::new(
            agent,
            AgentSession::new(session_id.clone(), AgentRole::Main),
        );
        let control_registry = AgentRuntimeControlRegistry::new();
        control_registry
            .register_runtime(runtime.clone())
            .map_err(|error| error.to_string())?;
        let service_registry = crate::register_runtime_control_services(
            ServiceRegistry::builder(),
            Arc::new(control_registry.clone()),
        )
        .build();
        let start_result = runtime
            .submit_op(AgentOp::start_turn(input))
            .map_err(|error| error.to_string())?;
        let steer_response = dispatch_readiness_rpc::<AgentSessionControlResponse, _>(
            &service_registry,
            Arc::clone(&data_dir),
            AGENT_SESSION_STEER_ROUTE,
            &AgentSessionSteerRequest {
                session_id: session_id.clone(),
                message: "preserve live steering".to_string(),
            },
        );
        let follow_up_response = dispatch_readiness_rpc::<AgentSessionControlResponse, _>(
            &service_registry,
            Arc::clone(&data_dir),
            AGENT_SESSION_FOLLOW_UP_ROUTE,
            &AgentSessionFollowUpRequest {
                session_id: session_id.clone(),
                message: "queue live follow-up".to_string(),
            },
        );
        let resume_response = dispatch_readiness_rpc::<AgentSessionResumeResponse, _>(
            &service_registry,
            Arc::clone(&data_dir),
            AGENT_SESSION_RESUME_ROUTE,
            &AgentSessionResumeRequest {
                session_id: session_id.clone(),
            },
        );
        let interrupt_response = dispatch_readiness_rpc::<AgentSessionControlResponse, _>(
            &service_registry,
            Arc::clone(&data_dir),
            AGENT_SESSION_INTERRUPT_ROUTE,
            &AgentSessionInterruptRequest {
                session_id: session_id.clone(),
                reason: "readiness interrupt".to_string(),
            },
        );
        release.release();
        let join_result = runtime
            .join_active_turn()
            .map_err(|error| error.to_string());
        let steer_result = steer_response?.result;
        let follow_up_result = follow_up_response?.result;
        let response = resume_response?;
        let interrupt_result = interrupt_response?.result;
        join_result?;
        let terminal_status = runtime
            .terminal_result(&task.task_id)
            .map_err(|error| error.to_string())?
            .map(|terminal| terminal.status)
            .ok_or_else(|| "cancelled runtime task did not produce a terminal result".to_string())?;
        Ok((
            start_result,
            steer_result,
            follow_up_result,
            interrupt_result,
            response,
            terminal_status,
        ))
    })();

    match result {
        Ok((
            start_result,
            steer_result,
            follow_up_result,
            interrupt_result,
            response,
            terminal_status,
        )) if start_result.accepted
            && steer_result.accepted
            && follow_up_result.accepted
            && interrupt_result.accepted
            && response.accepted
            && response.runtime_attached
            && response.session.as_ref().is_some_and(|session| {
                session
                    .steering_queue
                    .iter()
                    .any(|steering| steering.message == "preserve live steering")
                    && session
                        .follow_up_queue
                        .iter()
                        .any(|follow_up| follow_up.message == "queue live follow-up")
            })
            && response
                .control_snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.active_turn.is_some())
            && response.control_snapshot.as_ref().is_some_and(|snapshot| {
                snapshot.tasks.iter().any(|snapshot| {
                    snapshot.task.task_id == task.task_id
                        && snapshot.status == AgentTaskLifecycleStatus::Running
                })
            })
            && response.reattach.state == AgentSessionReattachState::LiveRuntimeAttached
            && response.reattach.active_turn_persisted
            && response.reattach.running_task_count == 1
            && interrupt_result.snapshot.active_turn.is_none()
            && interrupt_result.events.iter().any(|event| {
                matches!(
                    &event.payload,
                    crate::AgentEventPayload::CancellationRequested {
                        reason,
                        active_op_id: Some(_),
                    } if reason == "readiness interrupt"
                )
            })
            && interrupt_result.snapshot.tasks.iter().any(|snapshot| {
                snapshot.task.task_id == task.task_id
                    && snapshot.status == AgentTaskLifecycleStatus::Cancelled
            })
            && terminal_status == AgentTaskLifecycleStatus::Cancelled =>
        {
            let snapshot = response
                .control_snapshot
                .as_ref()
                .expect("snapshot checked");
            passed(
                "agent_runtime_live_reattach_smoke",
                "agent runtime control object-RPC routes queue live control messages, resume, and interrupt an active session",
                serde_json::json!({
                    "session_id": session_id,
                    "routes": {
                        "steer": AGENT_SESSION_STEER_ROUTE,
                        "follow_up": AGENT_SESSION_FOLLOW_UP_ROUTE,
                        "resume": AGENT_SESSION_RESUME_ROUTE,
                        "interrupt": AGENT_SESSION_INTERRUPT_ROUTE,
                    },
                    "steer_accepted": steer_result.accepted,
                    "follow_up_accepted": follow_up_result.accepted,
                    "interrupt_accepted": interrupt_result.accepted,
                    "accepted": response.accepted,
                    "runtime_attached": response.runtime_attached,
                    "has_session": response.session.is_some(),
                    "steering_queue_len": response
                        .session
                        .as_ref()
                        .map(|session| session.steering_queue.len())
                        .unwrap_or(0),
                    "follow_up_queue_len": response
                        .session
                        .as_ref()
                        .map(|session| session.follow_up_queue.len())
                        .unwrap_or(0),
                    "next_sequence": snapshot.next_sequence,
                    "event_cursor_offset": response.event_cursor.offset,
                    "active_turn_persisted": response.reattach.active_turn_persisted,
                    "running_task_count": response.reattach.running_task_count,
                    "reattach_state": response.reattach.state,
                    "reattach_reason": response.reattach.reason,
                    "interrupt_active_turn_present": interrupt_result.snapshot.active_turn.is_some(),
                    "terminal_status": terminal_status,
                }),
            )
        }
        Ok((
            start_result,
            steer_result,
            follow_up_result,
            interrupt_result,
            response,
            terminal_status,
        )) => failed(
            "agent_runtime_live_reattach_smoke",
            "agent runtime control object-RPC routes returned unexpected live metadata",
            serde_json::json!({
                "session_id": session_id,
                "routes": {
                    "steer": AGENT_SESSION_STEER_ROUTE,
                    "follow_up": AGENT_SESSION_FOLLOW_UP_ROUTE,
                    "resume": AGENT_SESSION_RESUME_ROUTE,
                    "interrupt": AGENT_SESSION_INTERRUPT_ROUTE,
                },
                "start_accepted": start_result.accepted,
                "steer_accepted": steer_result.accepted,
                "follow_up_accepted": follow_up_result.accepted,
                "interrupt_accepted": interrupt_result.accepted,
                "accepted": response.accepted,
                "runtime_attached": response.runtime_attached,
                "has_session": response.session.is_some(),
                "steering_queue_len": response
                    .session
                    .as_ref()
                    .map(|session| session.steering_queue.len())
                    .unwrap_or(0),
                "follow_up_queue_len": response
                    .session
                    .as_ref()
                    .map(|session| session.follow_up_queue.len())
                    .unwrap_or(0),
                "has_control_snapshot": response.control_snapshot.is_some(),
                "active_turn_present": response
                    .control_snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.active_turn.is_some()),
                "interrupt_active_turn_present": interrupt_result.snapshot.active_turn.is_some(),
                "interrupt_cancel_event": interrupt_result.events.iter().any(|event| {
                    matches!(
                        &event.payload,
                        crate::AgentEventPayload::CancellationRequested { .. }
                    )
                }),
                "terminal_status": terminal_status,
                "running_task_count": response.reattach.running_task_count,
                "reattach_state": response.reattach.state,
                "active_turn_persisted": response.reattach.active_turn_persisted,
                "reattach_reason": response.reattach.reason,
                "detail": response.detail,
            }),
        ),
        Err(error) => failed(
            "agent_runtime_live_reattach_smoke",
            "agent runtime live reattach smoke failed",
            serde_json::json!({
                "session_id": session_id,
                "error": error,
            }),
        ),
    }
}

fn check_agent_runtime_resume_metadata_fallback() -> AgentReadinessCheck {
    let data_root = std::env::temp_dir().join(format!(
        "sinomo-agent-runtime-resume-readiness-{}",
        TaskId::generate()
    ));
    let data_dir = Arc::new(SinomoDataDir::new(&data_root));
    let store = FileAgentSessionStore::for_data_dir(data_dir.as_ref());
    let session_id = SessionId::from_legacy_named("sess_agent_readiness_resume").expect("session");
    let session = AgentSession::new(session_id.clone(), AgentRole::Main);
    let task =
        AgentTaskRef::new("coding", "readiness restart resume").with_owner_role(AgentRole::Main);
    let active_op_id = TaskId::from_legacy_named("op_agent_readiness_resume").expect("active op");
    let mut task_snapshot = AgentTaskSnapshot::new(task.clone(), AgentTaskLifecycleStatus::Running);
    task_snapshot.active_op_id = Some(active_op_id.clone());
    let control_snapshot = AgentSessionControlSnapshot {
        session_id: session_id.clone(),
        next_sequence: 1,
        active_turn: Some(crate::AgentActiveTurn::new(
            active_op_id,
            0,
            None,
            Some(task.task_id.clone()),
        )),
        tasks: vec![task_snapshot],
        buffered_event_count: 0,
    };
    let result = (|| -> Result<AgentSessionResumeResponse, String> {
        store
            .save_session(&session)
            .map_err(|error| error.to_string())?;
        store
            .append_record(&AgentSessionStoreRecord::new(
                session_id.clone(),
                AgentSessionStoreRecordKind::ControlSnapshotSaved {
                    snapshot: control_snapshot,
                },
            ))
            .map_err(|error| error.to_string())?;
        let service_registry = crate::register_runtime_control_services(
            ServiceRegistry::builder(),
            Arc::new(AgentRuntimeControlRegistry::new()),
        )
        .build();
        dispatch_readiness_rpc::<AgentSessionResumeResponse, _>(
            &service_registry,
            Arc::clone(&data_dir),
            AGENT_SESSION_RESUME_ROUTE,
            &AgentSessionResumeRequest {
                session_id: session_id.clone(),
            },
        )
    })();
    match result {
        Ok(response)
            if !response.accepted
                && !response.runtime_attached
                && response.session.is_some()
                && response
                    .control_snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.active_turn.is_some())
                && response.reattach.state == AgentSessionReattachState::PersistedMetadataOnly
                && response.reattach.active_turn_persisted
                && response.reattach.running_task_count == 1
                && response
                    .detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("live runtime unavailable")) =>
        {
            passed(
                "agent_runtime_resume_metadata_fallback",
                "agent runtime-control resume route returns persisted metadata when no live runtime can reattach",
                serde_json::json!({
                    "session_id": session_id,
                    "route": AGENT_SESSION_RESUME_ROUTE,
                    "accepted": response.accepted,
                    "runtime_attached": response.runtime_attached,
                    "has_session": response.session.is_some(),
                    "has_control_snapshot": response.control_snapshot.is_some(),
                    "event_cursor_offset": response.event_cursor.offset,
                    "reattach_state": response.reattach.state,
                    "active_turn_persisted": response.reattach.active_turn_persisted,
                    "running_task_count": response.reattach.running_task_count,
                    "persisted_event_count": response.reattach.persisted_event_count,
                    "reattach_reason": response.reattach.reason,
                }),
            )
        }
        Ok(response) => failed(
            "agent_runtime_resume_metadata_fallback",
            "agent runtime resume fallback returned unexpected metadata",
            serde_json::json!({
                "session_id": session_id,
                "route": AGENT_SESSION_RESUME_ROUTE,
                "accepted": response.accepted,
                "runtime_attached": response.runtime_attached,
                "has_session": response.session.is_some(),
                "has_control_snapshot": response.control_snapshot.is_some(),
                "reattach_state": response.reattach.state,
                "active_turn_persisted": response.reattach.active_turn_persisted,
                "running_task_count": response.reattach.running_task_count,
                "persisted_event_count": response.reattach.persisted_event_count,
                "reattach_reason": response.reattach.reason,
                "detail": response.detail,
            }),
        ),
        Err(error) => failed(
            "agent_runtime_resume_metadata_fallback",
            "agent runtime resume fallback failed",
            serde_json::json!({
                "session_id": session_id,
                "route": AGENT_SESSION_RESUME_ROUTE,
                "error": error,
            }),
        ),
    }
}

fn check_agent_runtime_resume_missing_metadata() -> AgentReadinessCheck {
    let data_root = std::env::temp_dir().join(format!(
        "sinomo-agent-runtime-resume-missing-readiness-{}",
        TaskId::generate()
    ));
    let data_dir = Arc::new(SinomoDataDir::new(&data_root));
    let session_id =
        SessionId::from_legacy_named("sess_agent_readiness_resume_missing").expect("session");
    let result = {
        let service_registry = crate::register_runtime_control_services(
            ServiceRegistry::builder(),
            Arc::new(AgentRuntimeControlRegistry::new()),
        )
        .build();
        dispatch_readiness_rpc::<AgentSessionResumeResponse, _>(
            &service_registry,
            Arc::clone(&data_dir),
            AGENT_SESSION_RESUME_ROUTE,
            &AgentSessionResumeRequest {
                session_id: session_id.clone(),
            },
        )
    };

    match result {
        Ok(response)
            if !response.accepted
                && !response.runtime_attached
                && response.session.is_none()
                && response.control_snapshot.is_none()
                && response.event_cursor.offset == 0
                && response.reattach.state == AgentSessionReattachState::Missing
                && !response.reattach.active_turn_persisted
                && response.reattach.running_task_count == 0
                && response.reattach.persisted_event_count == 0
                && response.detail.as_deref().is_some_and(|detail| {
                    detail.contains("no persisted session metadata was found")
                }) =>
        {
            passed(
                "agent_runtime_resume_missing_metadata",
                "agent runtime-control resume route returns typed missing metadata when no live or persisted session exists",
                serde_json::json!({
                    "session_id": session_id,
                    "route": AGENT_SESSION_RESUME_ROUTE,
                    "accepted": response.accepted,
                    "runtime_attached": response.runtime_attached,
                    "has_session": response.session.is_some(),
                    "has_control_snapshot": response.control_snapshot.is_some(),
                    "event_cursor_offset": response.event_cursor.offset,
                    "reattach_state": response.reattach.state,
                    "active_turn_persisted": response.reattach.active_turn_persisted,
                    "running_task_count": response.reattach.running_task_count,
                    "persisted_event_count": response.reattach.persisted_event_count,
                    "reattach_reason": response.reattach.reason,
                }),
            )
        }
        Ok(response) => failed(
            "agent_runtime_resume_missing_metadata",
            "agent runtime-control resume route returned unexpected missing metadata",
            serde_json::json!({
                "session_id": session_id,
                "route": AGENT_SESSION_RESUME_ROUTE,
                "accepted": response.accepted,
                "runtime_attached": response.runtime_attached,
                "has_session": response.session.is_some(),
                "has_control_snapshot": response.control_snapshot.is_some(),
                "event_cursor_offset": response.event_cursor.offset,
                "reattach_state": response.reattach.state,
                "active_turn_persisted": response.reattach.active_turn_persisted,
                "running_task_count": response.reattach.running_task_count,
                "persisted_event_count": response.reattach.persisted_event_count,
                "reattach_reason": response.reattach.reason,
                "detail": response.detail,
            }),
        ),
        Err(error) => failed(
            "agent_runtime_resume_missing_metadata",
            "agent runtime-control resume missing-metadata route failed",
            serde_json::json!({
                "session_id": session_id,
                "route": AGENT_SESSION_RESUME_ROUTE,
                "error": error,
            }),
        ),
    }
}

struct ReadinessAgentRuntimeControlHost;

impl AgentRuntimeControlHost for ReadinessAgentRuntimeControlHost {
    fn steer(
        &self,
        _request: AgentSessionSteerRequest,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
        Err(readiness_control_host_error())
    }

    fn follow_up(
        &self,
        _request: AgentSessionFollowUpRequest,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
        Err(readiness_control_host_error())
    }

    fn remove_queued_input(
        &self,
        _request: AgentSessionRemoveQueuedInputRequest,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
        Err(readiness_control_host_error())
    }

    fn interrupt(
        &self,
        _request: AgentSessionInterruptRequest,
    ) -> Result<AgentSessionControlResult, AgentRuntimeControlError> {
        Err(readiness_control_host_error())
    }

    fn resume(
        &self,
        _request: AgentSessionResumeRequest,
    ) -> Result<AgentSessionResumeResponse, AgentRuntimeControlError> {
        Err(readiness_control_host_error())
    }
}

fn readiness_control_host_error() -> AgentRuntimeControlError {
    AgentRuntimeControlError::new("readiness host validates route registration only")
}

fn check_coding_state_projection_bounds_smoke() -> AgentReadinessCheck {
    let root = std::env::temp_dir().join(format!(
        "sinomo-agent-coding-projection-readiness-{}",
        TaskId::generate()
    ));
    let evidence_dir = root.join("evidence");
    let result = (|| -> Result<_, String> {
        fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        fs::write(root.join("hello.txt"), "old\n").map_err(|error| error.to_string())?;
        let mut responses = vec![
            serde_json::json!({
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
            .to_string(),
        ];
        for index in 0..5 {
            let failing_validation_command = if cfg!(windows) {
                format!("echo validation failed attempt {index} 1>&2 & exit /B 101")
            } else {
                format!("printf 'validation failed attempt {index}' >&2; exit 101")
            };
            responses.push(
                serde_json::json!({
                    "response_mode": "defer",
                    "text": format!("I will validate the patch, attempt {index}."),
                    "tool_calls": [{
                        "tool": "run_command",
                        "input": {"command": failing_validation_command},
                        "async": false,
                        "summary": format!("Run patch-relevant validation attempt {index}.")
                    }],
                    "background_handoff": null,
                    "next_state": "waiting"
                })
                .to_string(),
            );
        }
        responses.push(
            serde_json::json!({
                "response_mode": "answer",
                "text": "Validation still points at the patch.",
                "tool_calls": [],
                "background_handoff": null,
                "next_state": "listening"
            })
            .to_string(),
        );
        let model = ReadinessSequencedModel::new(responses);
        let config = LocalWorkspaceAgentConfig::new(&root)
            .allowing_file_writes(true)
            .using_local_process_commands()
            .allowing_shell_commands(true)
            .with_evidence_dir(&evidence_dir);
        let agent = Agent::new(model, "readiness", "coding-projection-smoke")
            .with_tool_registry(fixed_command_readiness_registry(config));
        agent
            .run(&readiness_agent_loop_input("patch and repeatedly validate"))
            .map_err(|error| error.to_string())
    })();
    let run = match result {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "coding_state_projection_bounds_smoke",
                "coding-state bounded projection smoke failed to run",
                serde_json::json!({
                    "workspace": root,
                    "error": error,
                }),
            );
        }
    };
    let projected = run
        .transcript
        .iter()
        .rev()
        .find(|item| item.content.contains("\"coding_run_state\""))
        .and_then(|item| serde_json::from_str::<serde_json::Value>(&item.content).ok())
        .and_then(|value| value.get("state").cloned());
    let Some(projected) = projected else {
        return failed(
            "coding_state_projection_bounds_smoke",
            "coding-state bounded projection smoke did not find model-visible coding state",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "transcript_items": run.transcript.len(),
                "coding_state_present": run.coding_state.is_some(),
            }),
        );
    };
    let attempts = projected
        .get("validation_attempts")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let validation_attempt_count = projected
        .get("validation_attempt_count")
        .and_then(serde_json::Value::as_u64);
    let omitted_validation_attempt_count = projected
        .get("omitted_validation_attempt_count")
        .and_then(serde_json::Value::as_u64);
    let contains_old_attempt = attempts.iter().any(|attempt| {
        attempt
            .get("command")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|command| command.contains("attempt 0") || command.contains("attempt 1"))
    });
    let contains_latest_attempt = attempts.iter().any(|attempt| {
        attempt
            .get("command")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|command| command.contains("attempt 4"))
    });
    let passed_smoke = run.stop_reason == AgentStopReason::FinalDecision
        && run.model_turns.len() == 7
        && run
            .coding_state
            .as_ref()
            .is_some_and(|state| state.validation_attempts.len() == 5)
        && projected
            .get("projection")
            .and_then(serde_json::Value::as_str)
            == Some("model_visible_bounded")
        && validation_attempt_count == Some(5)
        && omitted_validation_attempt_count == Some(2)
        && attempts.len() == 3
        && !contains_old_attempt
        && contains_latest_attempt
        && projected
            .get("changed_file_count")
            .and_then(serde_json::Value::as_u64)
            == Some(1);
    if passed_smoke {
        passed(
            "coding_state_projection_bounds_smoke",
            "model-visible coding-state projection keeps recent validation attempts while preserving full counts",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "model_turns": run.model_turns.len(),
                "projection": projected.get("projection"),
                "validation_attempt_count": validation_attempt_count,
                "omitted_validation_attempt_count": omitted_validation_attempt_count,
                "model_visible_validation_attempts": attempts.len(),
                "contains_latest_attempt": contains_latest_attempt,
                "contains_old_attempt": contains_old_attempt,
                "changed_file_count": projected.get("changed_file_count"),
            }),
        )
    } else {
        failed(
            "coding_state_projection_bounds_smoke",
            "model-visible coding-state projection did not retain bounded recent state",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "model_turns": run.model_turns.len(),
                "projection": projected,
                "model_visible_validation_attempts": attempts.len(),
                "contains_latest_attempt": contains_latest_attempt,
                "contains_old_attempt": contains_old_attempt,
                "coding_state_validation_attempt_count": run.coding_state.as_ref().map(|state| state.validation_attempts.len()),
            }),
        )
    }
}

fn check_coding_state_environment_validation_smoke() -> AgentReadinessCheck {
    let root = std::env::temp_dir().join(format!(
        "sinomo-agent-coding-state-readiness-{}",
        TaskId::generate()
    ));
    let evidence_dir = root.join("evidence");
    let result = (|| -> Result<_, String> {
        fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        fs::write(root.join("hello.txt"), "old\n").map_err(|error| error.to_string())?;
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
        let model = ReadinessSequencedModel::new([patch_turn, validation_turn]);
        let config = LocalWorkspaceAgentConfig::new(&root)
            .allowing_file_writes(true)
            .using_local_process_commands()
            .allowing_shell_commands(true)
            .with_evidence_dir(&evidence_dir);
        let agent = Agent::new(model, "readiness", "coding-state-smoke")
            .with_tool_registry(fixed_command_readiness_registry(config));
        agent
            .run(&readiness_agent_loop_input("patch and validate"))
            .map_err(|error| error.to_string())
    })();
    let run = match result {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "coding_state_environment_validation_smoke",
                "coding-state environment validation smoke failed to run",
                serde_json::json!({
                    "workspace": root,
                    "error": error,
                }),
            );
        }
    };
    let Some(coding_state) = run.coding_state.as_ref() else {
        return failed(
            "coding_state_environment_validation_smoke",
            "coding-state environment validation smoke did not emit coding state",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "final_text": run.final_decision.text,
            }),
        );
    };
    let validation_attempt = coding_state.validation_attempts.last();
    let validation_failure_kind =
        validation_attempt.and_then(|attempt| attempt.failure_kind.clone());
    let validation_command = validation_attempt
        .map(|attempt| attempt.command.clone())
        .unwrap_or_default();
    let passed_smoke = run.stop_reason == AgentStopReason::CodingStopRecommended
        && run.model_turns.len() == 2
        && run.final_decision.response_mode == AgentLoopResponseMode::Answer
        && run.final_decision.text.contains("Patch is ready")
        && run.final_decision.text.contains("missing command")
        && coding_state.patch_status == CodingPatchStatus::Applied
        && coding_state.validation_status == CodingValidationStatus::FailedEnvironment
        && coding_state.stop_recommendation == CodingStopDecision::FinishWithPatch
        && validation_failure_kind == Some(CodingCommandFailureKind::MissingCommand)
        && coding_state.changed_files == vec!["hello.txt".to_string()]
        && run
            .items
            .iter()
            .any(|item| matches!(item, AgentItem::CodingRunState { .. }));
    if passed_smoke {
        passed(
            "coding_state_environment_validation_smoke",
            "coding-state reducer classifies missing validation command as environment-blocked and stops after a narrow patch",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "model_turns": run.model_turns.len(),
                "response_mode": run.final_decision.response_mode,
                "final_text": run.final_decision.text,
                "patch_status": coding_state.patch_status,
                "validation_status": coding_state.validation_status,
                "stop_recommendation": coding_state.stop_recommendation,
                "validation_failure_kind": validation_failure_kind,
                "validation_command": validation_command,
                "changed_files": coding_state.changed_files,
            }),
        )
    } else {
        failed(
            "coding_state_environment_validation_smoke",
            "coding-state reducer did not produce expected environment-blocked stop evidence",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "model_turns": run.model_turns.len(),
                "response_mode": run.final_decision.response_mode,
                "final_text": run.final_decision.text,
                "patch_status": coding_state.patch_status,
                "validation_status": coding_state.validation_status,
                "stop_recommendation": coding_state.stop_recommendation,
                "validation_failure_kind": validation_failure_kind,
                "validation_command": validation_command,
                "changed_files": coding_state.changed_files,
                "validation_attempts": coding_state.validation_attempts,
            }),
        )
    }
}

fn check_coding_state_missing_dependency_validation_smoke() -> AgentReadinessCheck {
    let root = std::env::temp_dir().join(format!(
        "sinomo-agent-coding-missing-dependency-readiness-{}",
        TaskId::generate()
    ));
    let evidence_dir = root.join("evidence");
    let result = (|| -> Result<_, String> {
        fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        fs::write(root.join("hello.txt"), "old\n").map_err(|error| error.to_string())?;
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
        let missing_dependency_command = if cfg!(windows) {
            "echo ModuleNotFoundError: No module named 'pytest' 1>&2 & exit /B 1"
        } else {
            "printf \"ModuleNotFoundError: No module named 'pytest'\" >&2; exit 1"
        };
        let validation_turn = serde_json::json!({
            "response_mode": "defer",
            "text": "I will validate the patch.",
            "tool_calls": [{
                "tool": "run_command",
                "input": {"command": missing_dependency_command},
                "async": false,
                "summary": "Run validation blocked by a missing dependency."
            }],
            "background_handoff": null,
            "next_state": "waiting"
        })
        .to_string();
        let model = ReadinessSequencedModel::new([patch_turn, validation_turn]);
        let config = LocalWorkspaceAgentConfig::new(&root)
            .allowing_file_writes(true)
            .using_local_process_commands()
            .allowing_shell_commands(true)
            .with_evidence_dir(&evidence_dir);
        let agent = Agent::new(model, "readiness", "coding-missing-dependency-smoke")
            .with_tool_registry(fixed_command_readiness_registry(config));
        agent
            .run(&readiness_agent_loop_input(
                "patch and validate with a missing dependency",
            ))
            .map_err(|error| error.to_string())
    })();
    let run = match result {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "coding_state_missing_dependency_validation_smoke",
                "coding-state missing-dependency validation smoke failed to run",
                serde_json::json!({
                    "workspace": root,
                    "error": error,
                }),
            );
        }
    };
    let Some(coding_state) = run.coding_state.as_ref() else {
        return failed(
            "coding_state_missing_dependency_validation_smoke",
            "coding-state missing-dependency smoke did not emit coding state",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "final_text": run.final_decision.text,
            }),
        );
    };
    let validation_attempt = coding_state.validation_attempts.last();
    let validation_failure_kind =
        validation_attempt.and_then(|attempt| attempt.failure_kind.clone());
    let validation_command = validation_attempt
        .map(|attempt| attempt.command.clone())
        .unwrap_or_default();
    let passed_smoke = run.stop_reason == AgentStopReason::CodingStopRecommended
        && run.model_turns.len() == 2
        && run.final_decision.response_mode == AgentLoopResponseMode::Answer
        && run.final_decision.text.contains("Patch is ready")
        && run.final_decision.text.contains("missing dependency")
        && coding_state.patch_status == CodingPatchStatus::Applied
        && coding_state.validation_status == CodingValidationStatus::FailedEnvironment
        && coding_state.stop_recommendation == CodingStopDecision::FinishWithPatch
        && validation_failure_kind == Some(CodingCommandFailureKind::MissingDependency)
        && coding_state.changed_files == vec!["hello.txt".to_string()];
    if passed_smoke {
        passed(
            "coding_state_missing_dependency_validation_smoke",
            "coding-state reducer classifies missing validation dependencies as environment-blocked and stops after a narrow patch",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "model_turns": run.model_turns.len(),
                "response_mode": run.final_decision.response_mode,
                "final_text": run.final_decision.text,
                "patch_status": coding_state.patch_status,
                "validation_status": coding_state.validation_status,
                "stop_recommendation": coding_state.stop_recommendation,
                "validation_failure_kind": validation_failure_kind,
                "validation_command": validation_command,
                "changed_files": coding_state.changed_files,
            }),
        )
    } else {
        failed(
            "coding_state_missing_dependency_validation_smoke",
            "coding-state reducer did not produce expected missing-dependency stop evidence",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "model_turns": run.model_turns.len(),
                "response_mode": run.final_decision.response_mode,
                "final_text": run.final_decision.text,
                "patch_status": coding_state.patch_status,
                "validation_status": coding_state.validation_status,
                "stop_recommendation": coding_state.stop_recommendation,
                "validation_failure_kind": validation_failure_kind,
                "validation_command": validation_command,
                "changed_files": coding_state.changed_files,
                "validation_attempts": coding_state.validation_attempts,
            }),
        )
    }
}

fn check_coding_state_terminal_blocker_smoke() -> AgentReadinessCheck {
    let root = std::env::temp_dir().join(format!(
        "sinomo-agent-coding-terminal-blocker-readiness-{}",
        TaskId::generate()
    ));
    let evidence_dir = root.join("evidence");
    let result = (|| -> Result<_, String> {
        fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        fs::write(root.join("hello.txt"), "old\n").map_err(|error| error.to_string())?;
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
        let network_blocked_command = if cfg!(windows) {
            "echo network is unreachable 1>&2 & exit /B 2"
        } else {
            "printf 'network is unreachable' >&2; exit 2"
        };
        let validation_turn = serde_json::json!({
            "response_mode": "defer",
            "text": "I will validate the patch.",
            "tool_calls": [{
                "tool": "run_command",
                "input": {"command": network_blocked_command},
                "async": false,
                "summary": "Run validation that cannot reach the network."
            }],
            "background_handoff": null,
            "next_state": "waiting"
        })
        .to_string();
        let model = ReadinessSequencedModel::new([patch_turn, validation_turn]);
        let config = LocalWorkspaceAgentConfig::new(&root)
            .allowing_file_writes(true)
            .using_local_process_commands()
            .allowing_shell_commands(true)
            .with_evidence_dir(&evidence_dir);
        let agent = Agent::new(model, "readiness", "coding-terminal-blocker-smoke")
            .with_tool_registry(fixed_command_readiness_registry(config));
        agent
            .run(&readiness_agent_loop_input(
                "patch and validate against a network dependency",
            ))
            .map_err(|error| error.to_string())
    })();
    let run = match result {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "coding_state_terminal_blocker_smoke",
                "coding-state terminal-blocker smoke failed to run",
                serde_json::json!({
                    "workspace": root,
                    "error": error,
                }),
            );
        }
    };
    let Some(coding_state) = run.coding_state.as_ref() else {
        return failed(
            "coding_state_terminal_blocker_smoke",
            "coding-state terminal-blocker smoke did not emit coding state",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "final_text": run.final_decision.text,
            }),
        );
    };
    let validation_attempt = coding_state.validation_attempts.last();
    let validation_failure_kind =
        validation_attempt.and_then(|attempt| attempt.failure_kind.clone());
    let validation_command = validation_attempt
        .map(|attempt| attempt.command.clone())
        .unwrap_or_default();
    let passed_smoke = run.stop_reason == AgentStopReason::CodingStopRecommended
        && run.model_turns.len() == 2
        && run.final_decision.response_mode == AgentLoopResponseMode::Answer
        && run.final_decision.text.contains("Coding run is blocked")
        && run.final_decision.text.contains("network unavailable")
        && coding_state.patch_status == CodingPatchStatus::Applied
        && coding_state.validation_status == CodingValidationStatus::FailedEnvironment
        && coding_state.stop_recommendation == CodingStopDecision::FinishBlocked
        && validation_failure_kind == Some(CodingCommandFailureKind::NetworkUnavailable)
        && coding_state.changed_files == vec!["hello.txt".to_string()];
    if passed_smoke {
        passed(
            "coding_state_terminal_blocker_smoke",
            "coding-state reducer classifies terminal validation blockers as blocked instead of ready",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "model_turns": run.model_turns.len(),
                "response_mode": run.final_decision.response_mode,
                "final_text": run.final_decision.text,
                "patch_status": coding_state.patch_status,
                "validation_status": coding_state.validation_status,
                "stop_recommendation": coding_state.stop_recommendation,
                "validation_failure_kind": validation_failure_kind,
                "validation_command": validation_command,
                "changed_files": coding_state.changed_files,
            }),
        )
    } else {
        failed(
            "coding_state_terminal_blocker_smoke",
            "coding-state reducer did not preserve terminal blocker semantics",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "model_turns": run.model_turns.len(),
                "response_mode": run.final_decision.response_mode,
                "final_text": run.final_decision.text,
                "patch_status": coding_state.patch_status,
                "validation_status": coding_state.validation_status,
                "stop_recommendation": coding_state.stop_recommendation,
                "validation_failure_kind": validation_failure_kind,
                "validation_command": validation_command,
                "changed_files": coding_state.changed_files,
                "validation_attempts": coding_state.validation_attempts,
            }),
        )
    }
}

fn check_local_command_evidence_smoke(workspace_root: &Path) -> AgentReadinessCheck {
    let evidence_dir =
        std::env::temp_dir().join(format!("sinomo-agent-readiness-{}", TaskId::generate()));
    let mut config = LocalWorkspaceAgentConfig::new(workspace_root)
        .allowing_file_writes(true)
        .using_local_process_commands()
        .allowing_shell_commands(true)
        .with_evidence_dir(&evidence_dir);
    config.max_command_output_bytes = 4;
    let registry = fixed_command_readiness_registry(config);
    let session = readiness_session();
    let success_call = AgentToolCall {
        call_id: None,
        tool: "run_command".to_string(),
        input: serde_json::json!({
            "command": "printf ok; printf er >&2",
            "timeout_ms": 5000,
        }),
        r#async: false,
        summary: "run command evidence readiness smoke".to_string(),
    };
    let success = registry.execute(&session, 0, 0, &success_call);
    let success_output = success
        .output
        .get("command_output")
        .unwrap_or(&success.output);
    let success_stdout = success_output
        .get("stdout")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let success_stderr = success_output
        .get("stderr")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let success_exit_code = success_output
        .get("exit_code")
        .and_then(serde_json::Value::as_i64);
    let success_duration_present = success_output.get("duration_ms").is_some();
    let success_timed_out = success_output
        .get("timed_out")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);

    let failed_call = AgentToolCall {
        call_id: None,
        tool: "run_command".to_string(),
        input: serde_json::json!({
            "command": "printf abcdef; printf uvwxyz >&2; exit 2",
            "timeout_ms": 5000,
        }),
        r#async: false,
        summary: "run command failed evidence readiness smoke".to_string(),
    };
    let failed_outcome = registry.execute(&session, 0, 1, &failed_call);
    let failed_output = failed_outcome
        .output
        .get("command_output")
        .unwrap_or(&failed_outcome.output);
    let failed_stdout = failed_output
        .get("stdout")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let failed_stderr = failed_output
        .get("stderr")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let failed_aggregated = failed_output
        .get("aggregated_output")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let failed_exit_code = failed_output
        .get("exit_code")
        .and_then(serde_json::Value::as_i64);
    let failed_timed_out = failed_output
        .get("timed_out")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let stdout_ref = failed_output
        .get("stdout_ref")
        .and_then(|value| value.get("uri"))
        .and_then(serde_json::Value::as_str);
    let stderr_ref = failed_output
        .get("stderr_ref")
        .and_then(|value| value.get("uri"))
        .and_then(serde_json::Value::as_str);
    let stdout_ref_has_full_output = stdout_ref
        .and_then(|path| fs::read_to_string(path).ok())
        .is_some_and(|content| content.contains("abcdef"));
    let stderr_ref_has_full_output = stderr_ref
        .and_then(|path| fs::read_to_string(path).ok())
        .is_some_and(|content| content.contains("uvwxyz"));
    let failed_has_artifact_refs = failed_outcome
        .evidence
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == "stdout")
        && failed_outcome
            .evidence
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == "stderr")
        && failed_outcome
            .evidence
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == "full_output");

    let timeout_command = if cfg!(windows) {
        "echo timeout-start & ping -n 6 127.0.0.1 >NUL"
    } else {
        "printf timeout-start; sleep 3"
    };
    let timeout_call = AgentToolCall {
        call_id: None,
        tool: "run_command".to_string(),
        input: serde_json::json!({
            "command": timeout_command,
            "timeout_ms": 750,
        }),
        r#async: false,
        summary: "run command timeout evidence readiness smoke".to_string(),
    };
    let timeout_outcome = registry.execute(&session, 0, 2, &timeout_call);
    let timeout_output = timeout_outcome
        .output
        .get("command_output")
        .unwrap_or(&timeout_outcome.output);
    let timeout_stdout = timeout_output
        .get("stdout")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let timeout_timed_out = timeout_output
        .get("timed_out")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let timeout_requested_ms = timeout_output
        .get("timeout_ms")
        .or_else(|| timeout_outcome.output.get("timeout_ms"))
        .and_then(serde_json::Value::as_u64);
    let timeout_stdout_ref = timeout_output
        .get("stdout_ref")
        .and_then(|value| value.get("uri"))
        .and_then(serde_json::Value::as_str);
    let timeout_stdout_ref_has_full_output = timeout_stdout_ref
        .and_then(|path| fs::read_to_string(path).ok())
        .is_some_and(|content| content.contains("timeout-start"));
    let timeout_has_artifact_refs = timeout_outcome
        .evidence
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == "stdout")
        && timeout_outcome
            .evidence
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == "full_output");

    if success.status == AgentToolStatus::Succeeded
        && success_stdout == "ok"
        && success_stderr == "er"
        && success_exit_code == Some(0)
        && success_duration_present
        && !success_timed_out
        && success.evidence.availability.allowed
        && success.evidence.execution.is_some()
        && failed_outcome.status == AgentToolStatus::Failed
        && failed_stdout == "abcd"
        && failed_stderr == "uvwx"
        && failed_exit_code == Some(2)
        && !failed_timed_out
        && failed_aggregated.contains("stderr:")
        && stdout_ref_has_full_output
        && stderr_ref_has_full_output
        && failed_has_artifact_refs
        && failed_outcome.evidence.execution.is_some()
        && timeout_outcome.status == AgentToolStatus::Failed
        && timeout_timed_out
        && timeout_requested_ms == Some(750)
        && timeout_stdout == "time"
        && timeout_stdout_ref_has_full_output
        && timeout_has_artifact_refs
        && timeout_outcome.evidence.execution.is_some()
    {
        passed(
            "local_command_evidence_smoke",
            "run_command preserves stdout, stderr, exit status, duration, timeout state, artifacts, and execution evidence across success, failure, and timeout paths",
            serde_json::json!({
                "success_status": success.status,
                "success_stdout": success_stdout,
                "success_stderr": success_stderr,
                "success_exit_code": success_exit_code,
                "success_duration_ms": success_output.get("duration_ms"),
                "success_timed_out": success_timed_out,
                "failed_status": failed_outcome.status,
                "failed_stdout": failed_stdout,
                "failed_stderr": failed_stderr,
                "failed_exit_code": failed_exit_code,
                "failed_timed_out": failed_timed_out,
                "failed_artifact_kinds": failed_outcome.evidence.artifacts.iter().map(|artifact| artifact.kind.clone()).collect::<Vec<_>>(),
                "timeout_status": timeout_outcome.status,
                "timeout_stdout": timeout_stdout,
                "timeout_ms": timeout_requested_ms,
                "timeout_timed_out": timeout_timed_out,
                "timeout_artifact_kinds": timeout_outcome.evidence.artifacts.iter().map(|artifact| artifact.kind.clone()).collect::<Vec<_>>(),
                "execution_evidence": success.evidence.execution.is_some()
                    && failed_outcome.evidence.execution.is_some()
                    && timeout_outcome.evidence.execution.is_some(),
            }),
        )
    } else {
        failed(
            "local_command_evidence_smoke",
            "run_command evidence smoke failed",
            serde_json::json!({
                "success_status": success.status,
                "success_summary": success.summary,
                "success_output": success.output,
                "success_availability": success.evidence.availability,
                "success_execution": success.evidence.execution,
                "failed_status": failed_outcome.status,
                "failed_summary": failed_outcome.summary,
                "failed_output": failed_outcome.output,
                "failed_artifacts": failed_outcome.evidence.artifacts,
                "failed_execution": failed_outcome.evidence.execution,
                "stdout_ref_has_full_output": stdout_ref_has_full_output,
                "stderr_ref_has_full_output": stderr_ref_has_full_output,
                "failed_has_artifact_refs": failed_has_artifact_refs,
                "timeout_status": timeout_outcome.status,
                "timeout_summary": timeout_outcome.summary,
                "timeout_output": timeout_outcome.output,
                "timeout_artifacts": timeout_outcome.evidence.artifacts,
                "timeout_execution": timeout_outcome.evidence.execution,
                "timeout_stdout_ref_has_full_output": timeout_stdout_ref_has_full_output,
                "timeout_has_artifact_refs": timeout_has_artifact_refs,
            }),
        )
    }
}

fn check_model_visible_read_file_bounding_smoke() -> AgentReadinessCheck {
    let root = std::env::temp_dir().join(format!(
        "sinomo-agent-read-file-bounding-readiness-{}",
        TaskId::generate()
    ));
    let tail = "READ_FILE_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let content = format!("{}{}{}", "r".repeat(20 * 1024), tail, "R".repeat(1024));
    if let Err(error) =
        fs::create_dir_all(&root).and_then(|()| fs::write(root.join("large.txt"), &content))
    {
        return failed(
            "model_visible_read_file_bounding_smoke",
            "failed to prepare read_file bounding fixture",
            serde_json::json!({
                "workspace": root.display().to_string(),
                "error": error.to_string(),
            }),
        );
    }
    let read_turn = serde_json::json!({
        "response_mode": "defer",
        "text": "I will read the large file.",
        "tool_calls": [{
            "tool": "read_file",
            "input": {
                "path": "large.txt"
            },
            "async": false,
            "summary": "Read a large file."
        }],
        "background_handoff": null,
        "next_state": "waiting"
    })
    .to_string();
    let final_turn = serde_json::json!({
        "response_mode": "answer",
        "text": "Large file content was projected as a bounded excerpt.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    })
    .to_string();
    let model = ReadinessSequencedModel::new([read_turn, final_turn]);
    let requests = Arc::clone(&model.requests);
    let agent = Agent::new(model, "readiness", "read-file-bounding")
        .with_tool_registry(AgentToolRegistry::local_workspace(&root));
    let run = match agent.run(&readiness_agent_loop_input(
        "Read the large file and summarize it.",
    )) {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "model_visible_read_file_bounding_smoke",
                "model-visible read_file bounding run failed",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let requests = requests.lock().expect("read_file bounding requests");
    let second_request_text = requests
        .get(1)
        .and_then(|request| serde_json::to_string(request).ok())
        .unwrap_or_default();
    let durable_tool_result_has_tail = run
        .tool_results
        .first()
        .and_then(|outcome| outcome.output.get("content"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| value.contains(tail));
    let model_bounds_file_content = second_request_text.contains("content_model_visible_truncated")
        && second_request_text.contains("[truncated]")
        && !second_request_text.contains(tail);

    if run.stop_reason == AgentStopReason::FinalDecision
        && run.model_turns.len() == 2
        && run.tool_results.len() == 1
        && run.tool_results[0].status == AgentToolStatus::Succeeded
        && durable_tool_result_has_tail
        && model_bounds_file_content
    {
        passed(
            "model_visible_read_file_bounding_smoke",
            "durable read_file output remains complete while model-visible transcript bounds file content",
            serde_json::json!({
                "model_turns": run.model_turns.len(),
                "tool_result_status": run.tool_results.first().map(|outcome| &outcome.status),
                "durable_tool_result_has_tail": durable_tool_result_has_tail,
                "model_bounds_file_content": model_bounds_file_content,
                "second_request_len": second_request_text.len(),
            }),
        )
    } else {
        failed(
            "model_visible_read_file_bounding_smoke",
            "model-visible read_file bounding smoke failed",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "model_turns": run.model_turns.len(),
                "tool_results": run.tool_results.len(),
                "tool_result_status": run.tool_results.first().map(|outcome| &outcome.status),
                "durable_tool_result_has_tail": durable_tool_result_has_tail,
                "model_bounds_file_content": model_bounds_file_content,
                "second_request_contains_tail": second_request_text.contains(tail),
                "second_request_contains_content_truncated": second_request_text.contains("content_model_visible_truncated"),
                "second_request_len": second_request_text.len(),
            }),
        )
    }
}

fn check_model_visible_tool_call_input_bounding_smoke() -> AgentReadinessCheck {
    let tail = "TOOL_CALL_INPUT_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let question = format!("{}{}{}", "q".repeat(12 * 1024), tail, "Q".repeat(1024));
    let input_turn = serde_json::json!({
        "response_mode": "defer",
        "text": "I will ask for input.",
        "tool_calls": [{
            "tool": "request_user_input",
            "input": {
                "question": question,
                "choices": ["continue"]
            },
            "async": false,
            "summary": "Ask for user input."
        }],
        "background_handoff": null,
        "next_state": "waiting"
    })
    .to_string();
    let final_turn = serde_json::json!({
        "response_mode": "answer",
        "text": "Tool call input was projected as a bounded excerpt.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    })
    .to_string();
    let model = ReadinessSequencedModel::new([input_turn, final_turn]);
    let requests = Arc::clone(&model.requests);
    let agent = Agent::new(model, "readiness", "tool-call-input-bounding").with_tool_registry(
        readiness_local_registry(Path::new(env!("CARGO_MANIFEST_DIR"))),
    );
    let run = match agent.run(&readiness_agent_loop_input(
        "Ask for input with a large prompt.",
    )) {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "model_visible_tool_call_input_bounding_smoke",
                "model-visible tool-call input bounding run failed",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let requests = requests.lock().expect("tool-call input bounding requests");
    let second_request_text = requests
        .get(1)
        .and_then(|request| serde_json::to_string(request).ok())
        .unwrap_or_default();
    let second_request_field_contains_tail = requests.get(1).map(|request| {
        serde_json::json!({
            "messages": serde_json::to_string(&request.messages)
                .is_ok_and(|text| text.contains(tail)),
            "items": serde_json::to_string(&request.items)
                .is_ok_and(|text| text.contains(tail)),
            "tools": serde_json::to_string(&request.tools)
                .is_ok_and(|text| text.contains(tail)),
            "response_format": serde_json::to_string(&request.response_format)
                .is_ok_and(|text| text.contains(tail)),
            "metadata": serde_json::to_string(&request.metadata)
                .is_ok_and(|text| text.contains(tail)),
        })
    });
    let durable_tool_call_has_tail = run.items.iter().any(|item| {
        matches!(
            item,
            AgentItem::ToolCall { call, .. }
                if call.tool == "request_user_input"
                    && call
                        .input
                        .get("question")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|value| value.contains(tail))
        )
    });
    let model_bounds_tool_call_input = second_request_text
        .contains("question_model_visible_truncated")
        && second_request_text.contains("question_original_bytes")
        && !second_request_text.contains(tail);

    if run.stop_reason == AgentStopReason::FinalDecision
        && run.model_turns.len() == 2
        && run.tool_results.len() == 1
        && run.tool_results[0].status == AgentToolStatus::Blocked
        && durable_tool_call_has_tail
        && model_bounds_tool_call_input
    {
        passed(
            "model_visible_tool_call_input_bounding_smoke",
            "durable tool-call input remains complete while model-visible replay bounds large input fields",
            serde_json::json!({
                "model_turns": run.model_turns.len(),
                "tool_result_status": run.tool_results.first().map(|outcome| &outcome.status),
                "durable_tool_call_has_tail": durable_tool_call_has_tail,
                "model_bounds_tool_call_input": model_bounds_tool_call_input,
                "second_request_len": second_request_text.len(),
            }),
        )
    } else {
        failed(
            "model_visible_tool_call_input_bounding_smoke",
            "model-visible tool-call input bounding smoke failed",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "model_turns": run.model_turns.len(),
                "tool_results": run.tool_results.len(),
                "tool_result_status": run.tool_results.first().map(|outcome| &outcome.status),
                "durable_tool_call_has_tail": durable_tool_call_has_tail,
                "model_bounds_tool_call_input": model_bounds_tool_call_input,
                "second_request_contains_tail": second_request_text.contains(tail),
                "second_request_contains_question_truncated": second_request_text.contains("question_model_visible_truncated"),
                "second_request_field_contains_tail": second_request_field_contains_tail,
                "second_request_len": second_request_text.len(),
            }),
        )
    }
}

fn check_model_visible_tool_results_aggregate_budget_smoke() -> AgentReadinessCheck {
    let root = std::env::temp_dir().join(format!(
        "sinomo-agent-tool-results-budget-readiness-{}",
        TaskId::generate()
    ));
    let tail = "AGGREGATE_TOOL_RESULT_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    if let Err(error) = fs::create_dir_all(&root) {
        return failed(
            "model_visible_tool_results_aggregate_budget_smoke",
            "failed to prepare aggregate tool-result workspace",
            serde_json::json!({
                "workspace": root.display().to_string(),
                "error": error.to_string(),
            }),
        );
    }
    for index in 0..10 {
        let marker = if index == 9 { tail } else { "visible-result" };
        let content = format!("{marker}\n{}", "r".repeat(12 * 1024));
        if let Err(error) = fs::write(root.join(format!("file-{index}.txt")), content) {
            return failed(
                "model_visible_tool_results_aggregate_budget_smoke",
                "failed to prepare aggregate tool-result fixture file",
                serde_json::json!({
                    "workspace": root.display().to_string(),
                    "file": format!("file-{index}.txt"),
                    "error": error.to_string(),
                }),
            );
        }
    }
    let tool_calls = (0..10)
        .map(|index| {
            serde_json::json!({
                "tool": "read_file",
                "input": {
                    "path": format!("file-{index}.txt")
                },
                "async": false,
                "summary": format!("Read file-{index}.txt")
            })
        })
        .collect::<Vec<_>>();
    let read_turn = serde_json::json!({
        "response_mode": "defer",
        "text": "I will read many files.",
        "tool_calls": tool_calls,
        "background_handoff": null,
        "next_state": "waiting"
    })
    .to_string();
    let final_turn = serde_json::json!({
        "response_mode": "answer",
        "text": "Many file results were projected within the aggregate budget.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    })
    .to_string();
    let model = ReadinessSequencedModel::new([read_turn, final_turn]);
    let requests = Arc::clone(&model.requests);
    let agent = Agent::new(model, "readiness", "tool-results-budget")
        .with_tool_registry(AgentToolRegistry::local_workspace(&root));
    let run = match agent.run(&readiness_agent_loop_input(
        "Read the files and summarize them.",
    )) {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "model_visible_tool_results_aggregate_budget_smoke",
                "model-visible aggregate tool-result budget run failed",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let requests = requests.lock().expect("aggregate budget requests");
    let second_request_text = requests
        .get(1)
        .and_then(|request| serde_json::to_string(request).ok())
        .unwrap_or_default();
    let durable_last_result_has_tail = run
        .tool_results
        .get(9)
        .and_then(|outcome| outcome.output.get("content"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| value.contains(tail));
    let model_bounds_aggregate_results = second_request_text
        .contains("model_visible_aggregate_bounded")
        && second_request_text.contains("output_omitted")
        && !second_request_text.contains(tail);

    if run.stop_reason == AgentStopReason::FinalDecision
        && run.model_turns.len() == 2
        && run.tool_results.len() == 10
        && run
            .tool_results
            .iter()
            .all(|outcome| outcome.status == AgentToolStatus::Succeeded)
        && durable_last_result_has_tail
        && model_bounds_aggregate_results
    {
        passed(
            "model_visible_tool_results_aggregate_budget_smoke",
            "durable per-tool outcomes remain complete while one turn's model-visible tool_results message is aggregate-bounded",
            serde_json::json!({
                "model_turns": run.model_turns.len(),
                "tool_results": run.tool_results.len(),
                "durable_last_result_has_tail": durable_last_result_has_tail,
                "model_bounds_aggregate_results": model_bounds_aggregate_results,
                "second_request_len": second_request_text.len(),
            }),
        )
    } else {
        failed(
            "model_visible_tool_results_aggregate_budget_smoke",
            "model-visible aggregate tool-result budget smoke failed",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "model_turns": run.model_turns.len(),
                "tool_results": run.tool_results.len(),
                "durable_last_result_has_tail": durable_last_result_has_tail,
                "model_bounds_aggregate_results": model_bounds_aggregate_results,
                "second_request_contains_tail": second_request_text.contains(tail),
                "second_request_contains_budget": second_request_text.contains("model_visible_aggregate_bounded"),
                "second_request_contains_output_omitted": second_request_text.contains("output_omitted"),
                "second_request_len": second_request_text.len(),
            }),
        )
    }
}

fn check_sifs_command_evidence_smoke(workspace_root: &Path) -> AgentReadinessCheck {
    let root = std::env::temp_dir().join(format!(
        "sinomo-agent-sifs-evidence-readiness-{}",
        TaskId::generate()
    ));
    let sifs_mount = root.join("sifs-mount");
    let mut config = LocalWorkspaceAgentConfig::new(workspace_root)
        .using_local_process_commands()
        .allowing_shell_commands(true)
        .with_sifs_evidence_mount(&sifs_mount, "readiness/agent/evidence");
    config.max_command_output_bytes = 4;
    let registry = fixed_command_readiness_registry(config);
    let session = readiness_session();
    let command = if cfg!(windows) {
        "echo abcdef & echo uvwxyz 1>&2 & exit /B 3"
    } else {
        "printf abcdef; printf uvwxyz >&2; exit 3"
    };
    let call = AgentToolCall {
        call_id: None,
        tool: "run_command".to_string(),
        input: serde_json::json!({
            "command": command,
            "timeout_ms": 5000,
        }),
        r#async: false,
        summary: "run command SIFS evidence readiness smoke".to_string(),
    };
    let outcome = registry.execute(&session, 0, 0, &call);
    let output = outcome
        .output
        .get("command_output")
        .unwrap_or(&outcome.output);
    let bounded_stdout = output
        .get("stdout")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let bounded_stderr = output
        .get("stderr")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let stdout_ref = output
        .get("stdout_ref")
        .and_then(|value| value.get("uri"))
        .and_then(serde_json::Value::as_str);
    let stderr_ref = output
        .get("stderr_ref")
        .and_then(|value| value.get("uri"))
        .and_then(serde_json::Value::as_str);
    let full_output_ref = outcome
        .evidence
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "full_output")
        .map(|artifact| artifact.uri.as_str());
    let stdout_is_sifs = stdout_ref.is_some_and(|uri| uri.starts_with("sifs://"));
    let stderr_is_sifs = stderr_ref.is_some_and(|uri| uri.starts_with("sifs://"));
    let full_output_is_sifs = full_output_ref.is_some_and(|uri| uri.starts_with("sifs://"));
    let stdout_mirror_has_full_output = stdout_ref
        .and_then(|uri| read_sifs_mirror_ref(&sifs_mount, uri))
        .is_some_and(|content| content.contains("abcdef"));
    let stderr_mirror_has_full_output = stderr_ref
        .and_then(|uri| read_sifs_mirror_ref(&sifs_mount, uri))
        .is_some_and(|content| content.contains("uvwxyz"));
    let full_output_mirror_has_json = full_output_ref
        .and_then(|uri| read_sifs_mirror_ref(&sifs_mount, uri))
        .is_some_and(|content| content.contains("\"stdout\"") && content.contains("\"stderr\""));
    let artifact_kinds_are_sifs = outcome.evidence.artifacts.iter().all(|artifact| {
        ["stdout", "stderr", "full_output"].contains(&artifact.kind.as_str())
            && artifact.uri.starts_with("sifs://")
    });

    if outcome.status == AgentToolStatus::Failed
        && bounded_stdout == "abcd"
        && bounded_stderr == "uvwx"
        && stdout_is_sifs
        && stderr_is_sifs
        && full_output_is_sifs
        && stdout_mirror_has_full_output
        && stderr_mirror_has_full_output
        && full_output_mirror_has_json
        && artifact_kinds_are_sifs
    {
        passed(
            "sifs_command_evidence_smoke",
            "run_command can mirror large stdout/stderr evidence to a SIFS-backed artifact ref while keeping bounded model-visible output",
            serde_json::json!({
                "status": outcome.status,
                "bounded_stdout": bounded_stdout,
                "bounded_stderr": bounded_stderr,
                "stdout_ref": stdout_ref,
                "stderr_ref": stderr_ref,
                "full_output_ref": full_output_ref,
                "artifact_kinds": outcome.evidence.artifacts.iter().map(|artifact| artifact.kind.clone()).collect::<Vec<_>>(),
            }),
        )
    } else {
        failed(
            "sifs_command_evidence_smoke",
            "run_command SIFS evidence smoke failed",
            serde_json::json!({
                "status": outcome.status,
                "summary": outcome.summary,
                "output": outcome.output,
                "artifacts": outcome.evidence.artifacts,
                "bounded_stdout": bounded_stdout,
                "bounded_stderr": bounded_stderr,
                "stdout_is_sifs": stdout_is_sifs,
                "stderr_is_sifs": stderr_is_sifs,
                "full_output_is_sifs": full_output_is_sifs,
                "stdout_mirror_has_full_output": stdout_mirror_has_full_output,
                "stderr_mirror_has_full_output": stderr_mirror_has_full_output,
                "full_output_mirror_has_json": full_output_mirror_has_json,
                "artifact_kinds_are_sifs": artifact_kinds_are_sifs,
                "sifs_mount": sifs_mount,
            }),
        )
    }
}

fn read_sifs_mirror_ref(sifs_mount: &Path, uri: &str) -> Option<String> {
    let file_name = uri
        .strip_prefix("sifs://")?
        .rsplit('/')
        .next()
        .filter(|value| !value.is_empty())?;
    fs::read_to_string(sifs_mount.join(file_name)).ok()
}

fn check_model_visible_evidence_redaction_smoke(_workspace_root: &Path) -> AgentReadinessCheck {
    let root = std::env::temp_dir().join(format!(
        "sinomo-agent-evidence-redaction-readiness-{}",
        TaskId::generate()
    ));
    let sifs_mount = root.join("sifs-mount");
    let stdout_tail = "STDOUT_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let stderr_tail = "STDERR_TAIL_SHOULD_NOT_BE_MODEL_VISIBLE";
    let stdout_fixture = format!("{}{}{}", "o".repeat(8500), stdout_tail, "O".repeat(2048));
    let stderr_fixture = format!("{}{}{}", "e".repeat(8500), stderr_tail, "E".repeat(2048));
    if let Err(error) = fs::create_dir_all(&root)
        .and_then(|()| fs::write(root.join("stdout-fixture.txt"), &stdout_fixture))
        .and_then(|()| fs::write(root.join("stderr-fixture.txt"), &stderr_fixture))
    {
        return failed(
            "model_visible_evidence_redaction_smoke",
            "failed to prepare command-output redaction fixtures",
            serde_json::json!({
                "workspace": root.display().to_string(),
                "error": error.to_string(),
            }),
        );
    }
    let command = if cfg!(windows) {
        "type stdout-fixture.txt & type stderr-fixture.txt 1>&2 & exit /B 3"
    } else {
        "cat stdout-fixture.txt; cat stderr-fixture.txt >&2; exit 3"
    };
    let command_turn = serde_json::json!({
        "response_mode": "defer",
        "text": "I will generate command evidence.",
        "tool_calls": [{
            "tool": "run_command",
            "input": {
                "command": command,
                "timeout_ms": 5000
            },
            "async": false,
            "summary": "Generate large command evidence."
        }],
        "background_handoff": null,
        "next_state": "waiting"
    })
    .to_string();
    let final_turn = serde_json::json!({
        "response_mode": "answer",
        "text": "Evidence was summarized without exposing artifact URIs.",
        "tool_calls": [],
        "background_handoff": null,
        "next_state": "listening"
    })
    .to_string();
    let model = ReadinessSequencedModel::new([command_turn, final_turn]);
    let requests = Arc::clone(&model.requests);
    let mut config = LocalWorkspaceAgentConfig::new(&root)
        .using_local_process_commands()
        .allowing_shell_commands(true)
        .with_sifs_evidence_mount(&sifs_mount, "readiness/agent/evidence/redaction");
    config.max_command_output_bytes = 9 * 1024;
    let agent = Agent::new(model, "readiness", "evidence-redaction")
        .with_tool_registry(fixed_command_readiness_registry(config));
    let run = match agent.run(&readiness_agent_loop_input(
        "Generate large command evidence and summarize it.",
    )) {
        Ok(run) => run,
        Err(error) => {
            return failed(
                "model_visible_evidence_redaction_smoke",
                "model-visible evidence redaction run failed",
                serde_json::json!({ "error": error.to_string() }),
            );
        }
    };
    let requests = requests.lock().expect("readiness model requests");
    let second_request_text = requests
        .get(1)
        .and_then(|request| serde_json::to_string(request).ok())
        .unwrap_or_default();
    let durable_artifacts = run
        .tool_results
        .first()
        .map(|outcome| outcome.evidence.artifacts.clone())
        .unwrap_or_default();
    let durable_has_sifs_stdout = durable_artifacts
        .iter()
        .any(|artifact| artifact.kind == "stdout" && artifact.is_sifs());
    let durable_has_sifs_stderr = durable_artifacts
        .iter()
        .any(|artifact| artifact.kind == "stderr" && artifact.is_sifs());
    let durable_has_sifs_full_output = durable_artifacts
        .iter()
        .any(|artifact| artifact.kind == "full_output" && artifact.is_sifs());
    let model_mentions_tool_results = second_request_text.contains("tool_results");
    let model_mentions_availability = second_request_text.contains("stdout_ref_available")
        && second_request_text.contains("stderr_ref_available");
    let model_omits_uris = second_request_text.contains("uri_omitted")
        && !second_request_text.contains("sifs://")
        && !second_request_text.contains("readiness/agent/evidence/redaction")
        && !second_request_text.contains("sifs-mount");
    let model_bounds_raw_command_output = second_request_text
        .contains("stdout_model_visible_truncated")
        && second_request_text.contains("stderr_model_visible_truncated")
        && second_request_text.contains("aggregated_output_model_visible_truncated")
        && !second_request_text.contains(stdout_tail)
        && !second_request_text.contains(stderr_tail);

    if run.stop_reason == AgentStopReason::FinalDecision
        && run.model_turns.len() == 2
        && run.tool_results.len() == 1
        && durable_has_sifs_stdout
        && durable_has_sifs_stderr
        && durable_has_sifs_full_output
        && model_mentions_tool_results
        && model_mentions_availability
        && model_omits_uris
        && model_bounds_raw_command_output
    {
        passed(
            "model_visible_evidence_redaction_smoke",
            "durable command evidence keeps SIFS artifact refs while model-visible transcript omits artifact URIs and bounds raw command output",
            serde_json::json!({
                "model_turns": run.model_turns.len(),
                "tool_result_status": run.tool_results.first().map(|outcome| &outcome.status),
                "durable_artifact_kinds": durable_artifacts.iter().map(|artifact| artifact.kind.clone()).collect::<Vec<_>>(),
                "model_mentions_tool_results": model_mentions_tool_results,
                "model_mentions_availability": model_mentions_availability,
                "model_omits_uris": model_omits_uris,
                "model_bounds_raw_command_output": model_bounds_raw_command_output,
            }),
        )
    } else {
        failed(
            "model_visible_evidence_redaction_smoke",
            "model-visible evidence redaction smoke failed",
            serde_json::json!({
                "stop_reason": run.stop_reason,
                "model_turns": run.model_turns.len(),
                "tool_results": run.tool_results.len(),
                "durable_artifacts": durable_artifacts,
                "durable_has_sifs_stdout": durable_has_sifs_stdout,
                "durable_has_sifs_stderr": durable_has_sifs_stderr,
                "durable_has_sifs_full_output": durable_has_sifs_full_output,
                "model_mentions_tool_results": model_mentions_tool_results,
                "model_mentions_availability": model_mentions_availability,
                "model_omits_uris": model_omits_uris,
                "model_bounds_raw_command_output": model_bounds_raw_command_output,
                "second_request_contains_stdout_tail": second_request_text.contains(stdout_tail),
                "second_request_contains_stderr_tail": second_request_text.contains(stderr_tail),
                "second_request_contains_stdout_model_visible_truncated": second_request_text.contains("stdout_model_visible_truncated"),
                "second_request_contains_stderr_model_visible_truncated": second_request_text.contains("stderr_model_visible_truncated"),
                "second_request_contains_aggregated_model_visible_truncated": second_request_text.contains("aggregated_output_model_visible_truncated"),
                "second_request_contains_sifs_uri": second_request_text.contains("sifs://"),
                "second_request_contains_sifs_root": second_request_text.contains("readiness/agent/evidence/redaction"),
                "second_request_contains_sifs_mount": second_request_text.contains("sifs-mount"),
                "second_request_len": second_request_text.len(),
            }),
        )
    }
}

fn readiness_local_registry(workspace_root: &Path) -> AgentToolRegistry {
    let registry = fixed_command_readiness_registry(
        LocalWorkspaceAgentConfig::new(workspace_root)
            .using_local_process_commands()
            .allowing_file_writes(true)
            .allowing_shell_commands(true),
    );
    register_readiness_recall_tools(&registry);
    registry
}

/// Attach the real host-bound recall executors to deterministic readiness
/// backends. Production agents still have to inject their authorized SIFS and
/// activity-history backends; the readiness harness must exercise the same
/// executor boundary rather than hiding default tools from the plan.
pub(crate) fn register_readiness_recall_tools(registry: &AgentToolRegistry) {
    crate::memory_notes::register_tools(
        registry,
        crate::MemoryNoteAgentToolConfig::from_backend(ReadinessMemoryNoteBackend),
    );
    crate::register_session_history_search_tool(
        registry,
        crate::SessionHistorySearchPrincipal::new("readiness-agent")
            .expect("constant readiness principal"),
        Arc::new(ReadinessSessionHistoryBackend),
    );
}

/// Empty but contract-valid storage for deterministic executor-readiness
/// checks. It does not stand in for production SIFS: search observes an empty
/// authorized note root, reads report absence, and writes report unavailable.
struct ReadinessMemoryNoteBackend;

impl crate::MemoryNoteBackend for ReadinessMemoryNoteBackend {
    fn search(
        &self,
        _request: crate::MemoryNoteSearchRequest,
        cancellation: &AgentCancellationToken,
    ) -> Result<crate::MemoryNoteSearchPage, crate::MemoryNoteBackendError> {
        if cancellation.is_cancelled() {
            return Err(crate::MemoryNoteBackendError::Cancelled);
        }
        Ok(crate::MemoryNoteSearchPage::default())
    }

    fn read(
        &self,
        request: crate::MemoryNoteReadRequest,
        cancellation: &AgentCancellationToken,
    ) -> Result<crate::MemoryNoteDocument, crate::MemoryNoteBackendError> {
        if cancellation.is_cancelled() {
            return Err(crate::MemoryNoteBackendError::Cancelled);
        }
        Err(crate::MemoryNoteBackendError::NotFound { path: request.path })
    }

    fn write(
        &self,
        _request: crate::MemoryNoteWriteRequest,
        cancellation: &AgentCancellationToken,
    ) -> Result<crate::MemoryNoteWriteReceipt, crate::MemoryNoteBackendError> {
        if cancellation.is_cancelled() {
            return Err(crate::MemoryNoteBackendError::Cancelled);
        }
        Err(crate::MemoryNoteBackendError::Unavailable {
            detail: "the deterministic readiness backend does not persist notes".to_string(),
        })
    }
}

struct ReadinessSessionHistoryBackend;

impl crate::SessionHistorySearchBackend for ReadinessSessionHistoryBackend {
    fn search(
        &self,
        _principal: &crate::SessionHistorySearchPrincipal,
        _request: &crate::SessionHistorySearchRequest,
        cancellation: &AgentCancellationToken,
    ) -> Result<crate::SessionHistorySearchPage, crate::SessionHistorySearchError> {
        if cancellation.is_cancelled() {
            return Err(crate::SessionHistorySearchError::Cancelled);
        }
        Ok(crate::SessionHistorySearchPage::complete(Vec::new()))
    }
}

fn readiness_session() -> InteractionSession {
    InteractionSession::new(
        SessionId::from_legacy_named("agent_readiness_tool_session").expect("session id"),
        NodeId::from_legacy_named("agent_readiness_node").expect("node id"),
        InteractionSurface::new("agent.readiness", InteractionSurfaceKind::CommandLine),
    )
}

fn readiness_agent_loop_input(prompt: &str) -> AgentLoopInput {
    let session = readiness_session();
    let latest_event = user_interaction_input_event(&session, prompt);
    AgentLoopInput {
        session: session.clone(),
        latest_event,
        transient_system_context: None,
        async_update: None,
        context_attachments: Vec::new(),
        tool_specs: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compaction_eval_harness_readiness_requires_true_compaction_and_durable_resume() {
        let check = check_compaction_eval_harness_smoke(Path::new(env!("CARGO_MANIFEST_DIR")));

        assert_eq!(
            check.status,
            AgentReadinessCheckStatus::Passed,
            "{check:#?}"
        );
        let markers = check.evidence["required_script_markers"]
            .as_array()
            .expect("required script marker evidence");
        assert!(
            markers.iter().any(|marker| {
                marker.as_str() == Some("continuation_resumes_durable_cli_session")
            })
        );
        assert!(
            markers.iter().any(|marker| {
                marker.as_str() == Some("continuation_does_not_reinject_summary")
            })
        );
        assert!(
            markers
                .iter()
                .any(|marker| { marker.as_str() == Some("phase_one_invokes_agent_op_compact") })
        );
        assert!(markers.iter().any(|marker| {
            marker.as_str() == Some("phase_one_requires_persisted_projection_evidence")
        }));
        assert!(
            markers
                .iter()
                .any(|marker| marker.as_str() == Some("--compact-after"))
        );
        assert!(!markers.iter().any(|marker| {
            marker.as_str() == Some("generated_continuation_uses_compaction_summary")
        }));
    }

    #[test]
    fn local_agent_readiness_gate_passes_on_workspace_root() {
        let report = run_local_agent_readiness_gate(env!("CARGO_MANIFEST_DIR"));

        assert!(report.is_ready(), "{report:#?}");
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "built_in_package_validation")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "package_rejects_malformed_definitions")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "package_artifact_store_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "continuous_runtime_limits_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "prompt_decomposition_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "agent_architecture_doc_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "provider_boundary_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "tool_surface_compatibility_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "local_command_evidence_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "agent_simple_sandbox_active_cancellation_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "sandbox_container_active_cancellation_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "host_gateway_active_cancellation_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "remote_object_rpc_active_cancellation_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "agent_runtime_empty_routes_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "agent_runtime_persisted_routes_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "agent_runtime_live_reattach_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "agent_runtime_resume_missing_metadata")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "compaction_eval_harness_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "module_projection_disclosure_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "model_response_repair_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "provider_error_classification_smoke")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "tool_pair_integrity_smoke")
        );
    }
}
