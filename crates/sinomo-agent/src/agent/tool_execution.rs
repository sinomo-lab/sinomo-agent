//! Tool availability, progressive disclosure, and checked orchestration.

use super::{
    AGENT_SKILL_USAGE_EVIDENCE_KIND, AgentCancellationToken, AgentDefinition, AgentRole,
    AgentRunContext, AgentSkillDescriptor, AgentSkillPlan, AgentToolAvailabilityDecision,
    AgentToolCall, AgentToolCapability, AgentToolDescriptor, AgentToolEvidence, AgentToolOutcome,
    AgentToolPlan, AgentToolProgressEvent, AgentToolProgressSink, AgentToolRegistry,
    AgentToolStatus, ContextOmission, DEFAULT_SKILL_RESOURCE_BYTES, Deserialize, Engine,
    InteractionSession, MAX_SKILL_RESOURCE_BYTES, Serialize, TaskId,
    is_tool_disclosure_bridge_name, standard_skill_document,
};
use crate::{AsyncTaskNextAction, AsyncTaskStatus, AsyncTaskUpdate};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolSurface {
    #[serde(alias = "policy_id")]
    pub surface_id: String,
    pub role: AgentRole,
    #[serde(default)]
    pub tools: ToolAvailability,
}

/// Backward-compatible name for the session tool-surface availability bundle.
#[deprecated(note = "use AgentToolSurface")]
pub type AgentPolicyBundle = AgentToolSurface;

impl AgentToolSurface {
    pub fn new(surface_id: impl Into<String>, role: AgentRole) -> Self {
        Self {
            surface_id: surface_id.into(),
            role,
            tools: ToolAvailability::default(),
        }
    }

    pub fn default_for_role(role: AgentRole) -> Self {
        Self::new(format!("{}-default", role.surface_label()), role)
    }

    pub fn for_definition(definition: &AgentDefinition) -> Self {
        Self::default_for_role(definition.role.clone())
            .with_allowed_tools(definition.tool_surface.allow_tools.iter().cloned())
            .with_denied_tools(definition.tool_surface.deny_tools.iter().cloned())
    }

    pub fn surface_id(&self) -> &str {
        &self.surface_id
    }

    pub fn with_allowed_tools(
        mut self,
        tools: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.tools.allowed_tools = tools.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_denied_tools(mut self, tools: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tools.denied_tools = tools.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_allowed_capabilities(
        mut self,
        capabilities: impl IntoIterator<Item = AgentToolCapability>,
    ) -> Self {
        self.tools.allowed_capabilities = capabilities.into_iter().collect();
        self
    }

    pub fn with_denied_capabilities(
        mut self,
        capabilities: impl IntoIterator<Item = AgentToolCapability>,
    ) -> Self {
        self.tools.denied_capabilities = capabilities.into_iter().collect();
        self
    }

    pub fn evaluate_descriptor(
        &self,
        descriptor: &AgentToolDescriptor,
    ) -> AgentToolAvailabilityDecision {
        self.tools.evaluate_descriptor(&self.surface_id, descriptor)
    }

    pub fn evaluate_tool_call(
        &self,
        descriptor: Option<&AgentToolDescriptor>,
        call: &AgentToolCall,
    ) -> AgentToolAvailabilityDecision {
        if let Some(descriptor) = descriptor {
            return self.evaluate_descriptor(descriptor);
        }
        if self
            .tools
            .denied_tools
            .iter()
            .any(|tool| tool == &call.tool)
        {
            return AgentToolAvailabilityDecision {
                allowed: false,
                reason: format!(
                    "tool `{}` is hidden by agent tool surface `{}`",
                    call.tool, self.surface_id
                ),
            };
        }
        if !self.tools.allowed_tools.is_empty()
            && !self
                .tools
                .allowed_tools
                .iter()
                .any(|tool| tool == &call.tool)
        {
            return AgentToolAvailabilityDecision {
                allowed: false,
                reason: format!(
                    "tool `{}` is not in the visible set for agent tool surface `{}`",
                    call.tool, self.surface_id
                ),
            };
        }
        AgentToolAvailabilityDecision {
            allowed: true,
            reason: format!(
                "tool `{}` has no descriptor and is available to reach the registry by agent tool surface `{}`",
                call.tool, self.surface_id
            ),
        }
    }

    pub fn filter_tool_descriptors(
        &self,
        descriptors: Vec<AgentToolDescriptor>,
    ) -> (Vec<AgentToolDescriptor>, Vec<ContextOmission>) {
        let mut visible = Vec::new();
        let mut omitted = Vec::new();
        for descriptor in descriptors {
            let decision = self.evaluate_descriptor(&descriptor);
            if decision.allowed {
                visible.push(descriptor);
            } else {
                omitted.push(ContextOmission {
                    kind: "tool_schema".to_string(),
                    label: descriptor.spec.name,
                    reason: decision.reason,
                });
            }
        }
        (visible, omitted)
    }
}

impl AgentRole {
    pub(super) fn surface_label(&self) -> &'static str {
        match self {
            AgentRole::Main => "main",
            AgentRole::Background => "background",
            AgentRole::Deliberation => "deliberation",
            AgentRole::Evolution => "evolution",
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ToolAvailability {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denied_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_capabilities: Vec<AgentToolCapability>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denied_capabilities: Vec<AgentToolCapability>,
}

/// Backward-compatible name for tool availability filters.
#[deprecated(note = "use ToolAvailability")]
pub type AgentToolPolicy = ToolAvailability;

impl ToolAvailability {
    pub(super) fn evaluate_descriptor(
        &self,
        surface_id: &str,
        descriptor: &AgentToolDescriptor,
    ) -> AgentToolAvailabilityDecision {
        let tool_name = &descriptor.spec.name;
        if self.denied_tools.iter().any(|tool| tool == tool_name) {
            return AgentToolAvailabilityDecision {
                allowed: false,
                reason: format!(
                    "tool `{tool_name}` is hidden by agent tool surface `{surface_id}`"
                ),
            };
        }
        if !self.allowed_tools.is_empty()
            && !self.allowed_tools.iter().any(|tool| tool == tool_name)
        {
            return AgentToolAvailabilityDecision {
                allowed: false,
                reason: format!(
                    "tool `{tool_name}` is not in the visible set for agent tool surface `{surface_id}`"
                ),
            };
        }
        if let Some(capability) = descriptor
            .capabilities
            .iter()
            .find(|capability| self.denied_capabilities.contains(capability))
        {
            return AgentToolAvailabilityDecision {
                allowed: false,
                reason: format!(
                    "tool `{tool_name}` has unavailable capability `{}` under agent tool surface `{surface_id}`",
                    capability.availability_label()
                ),
            };
        }
        if !self.allowed_capabilities.is_empty()
            && descriptor
                .capabilities
                .iter()
                .any(|capability| !self.allowed_capabilities.contains(capability))
        {
            return AgentToolAvailabilityDecision {
                allowed: false,
                reason: format!(
                    "tool `{tool_name}` has capabilities outside the visible capability set for agent tool surface `{surface_id}`"
                ),
            };
        }
        AgentToolAvailabilityDecision {
            allowed: true,
            reason: format!(
                "tool `{tool_name}` is available under agent tool surface `{surface_id}`"
            ),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentToolOrchestrationRecord {
    #[serde(alias = "policy_id")]
    pub surface_id: String,
    #[serde(default)]
    pub registry_generation: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub visible_toolsets: Vec<String>,
    #[serde(alias = "policy_allowed")]
    pub surface_allowed: bool,
    #[serde(alias = "policy_reason")]
    pub surface_reason: String,
    pub descriptor_visible: bool,
    pub executor_available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_calls_per_step: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "AgentRunContext::is_empty")]
    pub run_context: AgentRunContext,
}

pub struct AgentToolOrchestrator<'a> {
    pub(super) tool_surface: &'a AgentToolSurface,
    pub(super) max_tool_calls_per_step: Option<usize>,
    pub(super) run_context: &'a AgentRunContext,
    pub(super) skill_plan: AgentSkillPlan,
}

impl<'a> AgentToolOrchestrator<'a> {
    pub fn new(
        tool_surface: &'a AgentToolSurface,
        max_tool_calls_per_step: Option<usize>,
        run_context: &'a AgentRunContext,
    ) -> Self {
        Self {
            tool_surface,
            max_tool_calls_per_step,
            run_context,
            skill_plan: AgentSkillPlan::default(),
        }
    }

    pub fn with_skill_plan(mut self, skill_plan: AgentSkillPlan) -> Self {
        self.skill_plan = skill_plan;
        self
    }

    pub fn execute(
        &self,
        registry: &AgentToolRegistry,
        session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
    ) -> AgentToolOutcome {
        self.execute_with_cancellation(
            registry,
            session,
            tool_plan,
            step_index,
            call_index,
            call,
            AgentCancellationToken::new(),
        )
    }

    pub fn execute_with_cancellation(
        &self,
        registry: &AgentToolRegistry,
        session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
        cancellation_token: AgentCancellationToken,
    ) -> AgentToolOutcome {
        self.execute_with_cancellation_and_progress(
            registry,
            session,
            tool_plan,
            step_index,
            call_index,
            call,
            cancellation_token,
            AgentToolProgressSink::default(),
        )
    }

    pub fn execute_with_cancellation_and_progress(
        &self,
        registry: &AgentToolRegistry,
        session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
    ) -> AgentToolOutcome {
        let progress_sink = match progress_sink.tool_call_id() {
            Some(_) => progress_sink,
            None => progress_sink.for_call(step_index, call_index, call),
        };
        let descriptor = tool_plan.descriptor(&call.tool);
        let descriptor_visible = tool_plan.is_model_visible(&call.tool);
        let executor_available = tool_plan.is_executable(&call.tool);
        let availability_descriptor = descriptor.filter(|_| descriptor_visible);
        let availability_decision = self
            .tool_surface
            .evaluate_tool_call(availability_descriptor, call);
        let requested_timeout_ms = call
            .input
            .get("timeout_ms")
            .and_then(serde_json::Value::as_u64);

        let (mut outcome, blocked_reason) = if !availability_decision.allowed {
            let reason = availability_decision.reason.clone();
            (
                AgentToolOutcome {
                    call_id: TaskId::generate(),
                    step_index,
                    call_index,
                    call: call.clone(),
                    status: AgentToolStatus::Blocked,
                    summary: reason.clone(),
                    output: serde_json::json!({
                        "error": "tool_unavailable",
                        "surface_id": self.tool_surface.surface_id.clone(),
                        "tool": call.tool.clone(),
                        "reason": reason,
                    }),
                    event_id: None,
                    evidence: AgentToolEvidence::availability_only(
                        false,
                        availability_decision.reason.clone(),
                    ),
                },
                Some("tool_unavailable".to_string()),
            )
        } else if self
            .max_tool_calls_per_step
            .is_some_and(|max_tool_calls| call_index >= max_tool_calls)
        {
            let max_tool_calls_per_step = self.max_tool_calls_per_step.unwrap_or(0);
            (
                AgentToolOutcome {
                    call_id: TaskId::generate(),
                    step_index,
                    call_index,
                    call: call.clone(),
                    status: AgentToolStatus::Blocked,
                    summary: format!(
                        "tool call limit {} reached for step {step_index}",
                        max_tool_calls_per_step
                    ),
                    output: serde_json::json!({
                        "error": "tool_call_limit_reached",
                        "max_tool_calls_per_step": max_tool_calls_per_step,
                    }),
                    event_id: None,
                    evidence: AgentToolEvidence::availability_only(
                        false,
                        "blocked by per-step tool call limit",
                    ),
                },
                Some("tool_call_limit_reached".to_string()),
            )
        } else if descriptor.is_none() {
            // The plan is the authority for what this turn may call. A name the
            // planner never evaluated must not reach a registry executor, even
            // if one was registered after the plan was built.
            (
                AgentToolOutcome {
                    call_id: TaskId::generate(),
                    step_index,
                    call_index,
                    call: call.clone(),
                    status: AgentToolStatus::Blocked,
                    summary: format!(
                        "tool `{}` is not part of the current agent tool plan",
                        call.tool
                    ),
                    output: serde_json::json!({
                        "error": "tool_not_planned",
                        "tool": call.tool.clone(),
                        "registry_generation": tool_plan.registry_generation,
                    }),
                    event_id: None,
                    evidence: AgentToolEvidence::availability_only(
                        false,
                        "blocked because tool is outside the current agent tool plan",
                    ),
                },
                Some("tool_not_planned".to_string()),
            )
        } else if !descriptor_visible {
            (
                AgentToolOutcome {
                    call_id: TaskId::generate(),
                    step_index,
                    call_index,
                    call: call.clone(),
                    status: AgentToolStatus::Blocked,
                    summary: format!(
                        "tool `{}` is not visible in the current agent tool projection",
                        call.tool
                    ),
                    output: serde_json::json!({
                        "error": "tool_not_visible",
                        "tool": call.tool.clone(),
                        "visible_toolsets": &tool_plan.visible_toolsets,
                    }),
                    event_id: None,
                    evidence: AgentToolEvidence::availability_only(
                        false,
                        "blocked because tool is outside the current model-visible projection",
                    ),
                },
                Some("tool_not_visible".to_string()),
            )
        } else if descriptor_visible && !executor_available {
            (
                AgentToolOutcome {
                    call_id: TaskId::generate(),
                    step_index,
                    call_index,
                    call: call.clone(),
                    status: AgentToolStatus::Blocked,
                    summary: format!(
                        "tool `{}` is visible to the model but has no executor attached to this agent",
                        call.tool
                    ),
                    output: serde_json::json!({
                        "error": "missing_executor",
                        "tool": call.tool.clone(),
                    }),
                    event_id: None,
                    evidence: AgentToolEvidence::availability_only(
                        false,
                        "blocked because planned tool has no runtime executor",
                    ),
                },
                Some("missing_executor".to_string()),
            )
        } else if is_tool_disclosure_bridge_name(&call.tool) {
            (
                self.execute_tool_disclosure_bridge(
                    registry,
                    session,
                    tool_plan,
                    step_index,
                    call_index,
                    call,
                    cancellation_token.clone(),
                    progress_sink.clone(),
                ),
                None,
            )
        } else {
            (
                registry.execute_planned_with_cancellation_and_progress(
                    session,
                    step_index,
                    call_index,
                    call,
                    tool_plan.registry_generation,
                    cancellation_token,
                    progress_sink,
                ),
                None,
            )
        };

        if availability_decision.allowed && outcome.evidence.availability.allowed {
            outcome.evidence.availability = availability_decision.clone();
        }
        let record = AgentToolOrchestrationRecord {
            surface_id: self.tool_surface.surface_id.clone(),
            registry_generation: tool_plan.registry_generation,
            visible_toolsets: tool_plan.visible_toolsets.clone(),
            surface_allowed: availability_decision.allowed,
            surface_reason: availability_decision.reason,
            descriptor_visible,
            executor_available,
            blocked_reason,
            max_tool_calls_per_step: self.max_tool_calls_per_step,
            requested_timeout_ms,
            run_context: self.run_context.clone(),
        };
        outcome.evidence = with_tool_orchestration_record(outcome.evidence, &record);
        outcome
    }

    /// Run an `async` call in the background when the host can continue the
    /// session afterwards. Returns the call's immediate result, or `None` when
    /// the call must run inline (no host support, a bridge tool, or a call the
    /// surface would block, which then reports its block inline).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn detach(
        &self,
        registry: &AgentToolRegistry,
        session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
        progress_sink: AgentToolProgressSink,
    ) -> Option<AgentToolOutcome> {
        let cancellation = progress_sink.detached_tools()?.child_token();
        let tool_call_id = progress_sink.tool_call_id()?.clone();
        if !call.r#async
            || is_tool_disclosure_bridge_name(&call.tool)
            || !tool_plan.is_executable(&call.tool)
        {
            return None;
        }
        let descriptor = tool_plan
            .descriptor(&call.tool)
            .filter(|_| tool_plan.is_model_visible(&call.tool));
        if !self
            .tool_surface
            .evaluate_tool_call(descriptor, call)
            .allowed
        {
            return None;
        }
        let task_id = TaskId::generate();
        progress_sink.emit(AgentToolProgressEvent::DetachedStarted {
            tool_call_id: tool_call_id.clone(),
            task_id: task_id.clone(),
            tool: call.tool.clone(),
            call: None,
        });
        let registry = registry.clone();
        let session = session.clone();
        let detached_call = call.clone();
        let generation = tool_plan.registry_generation;
        let worker_tool_call_id = tool_call_id.clone();
        let worker_task_id = task_id.clone();
        set_detached_call_live(&task_id, true);
        let spawned = std::thread::Builder::new()
            .name("sinomo-agent-detached-tool".to_string())
            .spawn(move || {
                let outcome = registry.execute_planned_with_cancellation_and_progress(
                    &session,
                    step_index,
                    call_index,
                    &detached_call,
                    generation,
                    cancellation,
                    progress_sink.clone(),
                );
                let task_id = worker_task_id.clone();
                progress_sink.emit(AgentToolProgressEvent::DetachedCompleted {
                    update: detached_tool_update(worker_task_id, worker_tool_call_id, &outcome),
                });
                set_detached_call_live(&task_id, false);
            });
        if spawned.is_err() {
            set_detached_call_live(&task_id, false);
            return None;
        }
        Some(AgentToolOutcome {
            call_id: tool_call_id.clone(),
            step_index,
            call_index,
            call: call.clone(),
            status: AgentToolStatus::Succeeded,
            summary: format!(
                "`{}` is running in the background as task {task_id}; its result arrives as an async update",
                call.tool
            ),
            output: serde_json::json!({
                "detached": true,
                "task_id": task_id,
                "tool_call_id": tool_call_id,
            }),
            event_id: None,
            evidence: AgentToolEvidence::availability_only(true, "detached to background"),
        })
    }

    pub(super) fn execute_tool_disclosure_bridge(
        &self,
        registry: &AgentToolRegistry,
        session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
    ) -> AgentToolOutcome {
        match call.tool.as_str() {
            "tool_search" => {
                self.execute_tool_search(session, tool_plan, step_index, call_index, call)
            }
            "tool_describe" => {
                self.execute_tool_describe(session, tool_plan, step_index, call_index, call)
            }
            "skill_search" => self.execute_skill_search(step_index, call_index, call),
            "skill_describe" => self.execute_skill_describe(session, step_index, call_index, call),
            "skill_activate" => self.execute_skill_activate(session, step_index, call_index, call),
            "tool_call" => self.execute_tool_call_bridge(
                registry,
                session,
                tool_plan,
                step_index,
                call_index,
                call,
                cancellation_token,
                progress_sink,
            ),
            _ => unreachable!("tool disclosure bridge names are matched by caller"),
        }
    }

    pub(super) fn execute_skill_activate(
        &self,
        session: &InteractionSession,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
    ) -> AgentToolOutcome {
        let Some(skill_id) = json_input_string(call, "skill") else {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                "skill_activate requires a string skill",
                serde_json::json!({ "error": "missing_skill" }),
            );
        };
        let Some(skill) = self.skill_plan.get(&skill_id) else {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                format!("skill `{skill_id}` is not available to this agent package"),
                serde_json::json!({ "error": "unknown_skill", "skill": skill_id }),
            );
        };
        let Some(markdown) = skill.embedded_text.as_deref() else {
            let activation = skill_activation_metadata(skill);
            let output = serde_json::json!({
                "error": "skill_content_unavailable",
                "skill": skill.id,
                "activation": activation,
                "reason": "the skill descriptor does not include readable SKILL.md content",
            });
            let evidence = AgentToolEvidence::availability_only(
                false,
                "skill activation requires readable SKILL.md content",
            )
            .with_output_value(&output, false)
            .with_record_value(
                "skill_disclosure",
                serde_json::json!({
                    "operation": "activate",
                    "skill": skill.id,
                    "available": false,
                    "mode": "dedicated_tool",
                }),
            )
            .with_record_value(
                AGENT_SKILL_USAGE_EVIDENCE_KIND,
                serde_json::json!({
                    "operation": "activate",
                    "skill": skill.id,
                    "available": false,
                    "mode": "dedicated_tool",
                }),
            );
            return AgentToolOutcome {
                call_id: TaskId::generate(),
                step_index,
                call_index,
                call: call.clone(),
                status: AgentToolStatus::Blocked,
                summary: format!("skill `{}` has no readable SKILL.md content", skill.id),
                output,
                event_id: None,
                evidence,
            };
        };
        let document = match crate::parse_skill_markdown(markdown) {
            Ok(document) if document.name() == skill.id => document,
            Ok(document) => {
                return bridge_failed_outcome(
                    session,
                    step_index,
                    call_index,
                    call,
                    format!(
                        "skill descriptor `{}` does not match SKILL.md name `{}`",
                        skill.id,
                        document.name()
                    ),
                    serde_json::json!({
                        "error": "skill_name_mismatch",
                        "skill": skill.id,
                        "frontmatter_name": document.name(),
                    }),
                );
            }
            Err(error) => {
                return bridge_failed_outcome(
                    session,
                    step_index,
                    call_index,
                    call,
                    format!(
                        "skill `{}` is not a valid Agent Skills package: {error}",
                        skill.id
                    ),
                    serde_json::json!({
                        "error": "invalid_agent_skill",
                        "skill": skill.id,
                        "detail": error.to_string(),
                    }),
                );
            }
        };
        if document.description() != skill.description {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                format!(
                    "skill descriptor `{}` description does not match SKILL.md frontmatter",
                    skill.id
                ),
                serde_json::json!({
                    "error": "skill_description_mismatch",
                    "skill": skill.id,
                }),
            );
        }
        let activation = skill_activation_metadata(skill);
        let requested_resource = json_input_string(call, "resource");
        let output = if let Some(resource_path) = requested_resource.as_deref() {
            let Some(bytes) = skill.resource_contents.get(resource_path) else {
                return bridge_failed_outcome(
                    session,
                    step_index,
                    call_index,
                    call,
                    format!(
                        "skill `{}` does not contain readable resource `{resource_path}`",
                        skill.id
                    ),
                    serde_json::json!({
                        "error": "unknown_skill_resource",
                        "skill": skill.id,
                        "resource": resource_path,
                        "available_resources": &skill.resource_paths,
                    }),
                );
            };
            let max_bytes = json_input_usize(call, "max_bytes")
                .unwrap_or(DEFAULT_SKILL_RESOURCE_BYTES)
                .min(MAX_SKILL_RESOURCE_BYTES);
            let truncated = bytes.len() > max_bytes;
            let visible = &bytes[..bytes.len().min(max_bytes)];
            let (encoding, content) = match std::str::from_utf8(visible) {
                Ok(text) => ("utf-8", text.to_string()),
                Err(_) => (
                    "base64",
                    base64::engine::general_purpose::STANDARD.encode(visible),
                ),
            };
            serde_json::json!({
                "skill": skill.id,
                "activation": activation,
                "resource_content": {
                    "path": resource_path,
                    "byte_len": bytes.len(),
                    "content_bytes": visible.len(),
                    "truncated": truncated,
                    "encoding": encoding,
                    "content": content,
                },
            })
        } else {
            serde_json::json!({
                "skill": skill.id,
                "activation": activation,
                "skill_content": {
                    "name": skill.id,
                    "instructions": document.body,
                    "skill_directory": skill.path.as_deref().map(std::path::Path::new).and_then(std::path::Path::parent).map(|path| path.display().to_string()),
                    "resources": &skill.resource_paths,
                },
            })
        };
        let operation = if requested_resource.is_some() {
            "resource_read"
        } else {
            "activate"
        };
        let evidence = AgentToolEvidence::availability_only(
            true,
            "loaded Agent Skills instructions through the dedicated activation tool",
        )
        .with_output_value(&output, false)
        .with_record_value(
            "skill_disclosure",
            serde_json::json!({
                "operation": operation,
                "skill": skill.id,
                "resource": requested_resource,
                "available": true,
                "mode": "dedicated_tool",
            }),
        )
        .with_record_value(
            AGENT_SKILL_USAGE_EVIDENCE_KIND,
            serde_json::json!({
                "operation": operation,
                "skill": skill.id,
                "resource": requested_resource,
                "available": true,
                "mode": "dedicated_tool",
            }),
        );
        AgentToolOutcome {
            call_id: TaskId::generate(),
            step_index,
            call_index,
            call: call.clone(),
            status: AgentToolStatus::Succeeded,
            summary: requested_resource.map_or_else(
                || format!("activated skill `{}`", skill.id),
                |resource| format!("read skill `{}` resource `{resource}`", skill.id),
            ),
            output,
            event_id: None,
            evidence,
        }
    }

    pub(super) fn execute_skill_search(
        &self,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
    ) -> AgentToolOutcome {
        let query = json_input_string(call, "query");
        let max_results = json_input_usize(call, "max_results").unwrap_or(20).min(50);
        let matches = self
            .skill_plan
            .matching(query.as_deref())
            .into_iter()
            .filter(|skill| standard_skill_document(skill).is_some())
            .take(max_results)
            .map(|skill| {
                serde_json::json!({
                    "id": &skill.id,
                    "description": &skill.description,
                    "license": &skill.license,
                    "compatibility": &skill.compatibility,
                    "allowed_tools": &skill.allowed_tools,
                    "metadata": &skill.metadata,
                    "resources": &skill.resource_paths,
                    "source": &skill.source,
                    "package_declared": skill.package_declared,
                    "module_id": &skill.module_id,
                    "module_version": &skill.module_version,
                    "path": &skill.path,
                    "target_agents": &skill.target_agents,
                    "model_families": &skill.model_families,
                    "activation_tags": &skill.activation_tags,
                    "required_capabilities": &skill.required_capabilities,
                    "catalog_version": &skill.catalog_version,
                    "catalog_content_hash": &skill.catalog_content_hash,
                    "catalog_base_hash": &skill.catalog_base_hash,
                    "catalog_status": &skill.catalog_status,
                    "catalog_manifest_path": &skill.catalog_manifest_path,
                    "activation": skill_activation_metadata(skill),
                    "has_embedded_text": skill.embedded_text.is_some(),
                })
            })
            .collect::<Vec<_>>();
        let match_count = matches.len();
        let output = serde_json::json!({
            "query": query,
            "max_results": max_results,
            "count": match_count,
            "matches": matches,
        });
        let evidence = AgentToolEvidence::availability_only(true, "inspected planned agent skills")
            .with_output_value(&output, false)
            .with_record_value(
                "skill_disclosure",
                serde_json::json!({
                    "operation": "search",
                    "query": output.get("query").cloned().unwrap_or(serde_json::Value::Null),
                    "count": output.get("count").cloned().unwrap_or(serde_json::Value::Null),
                }),
            )
            .with_record_value(
                AGENT_SKILL_USAGE_EVIDENCE_KIND,
                serde_json::json!({
                    "operation": "search",
                    "query": output.get("query").cloned().unwrap_or(serde_json::Value::Null),
                    "count": output.get("count").cloned().unwrap_or(serde_json::Value::Null),
                }),
            );
        AgentToolOutcome {
            call_id: TaskId::generate(),
            step_index,
            call_index,
            call: call.clone(),
            status: AgentToolStatus::Succeeded,
            summary: format!("found {match_count} skill(s)"),
            output,
            event_id: None,
            evidence,
        }
    }

    pub(super) fn execute_skill_describe(
        &self,
        session: &InteractionSession,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
    ) -> AgentToolOutcome {
        let Some(skill_id) = json_input_string(call, "skill") else {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                "skill_describe requires a string skill",
                serde_json::json!({ "error": "missing_skill" }),
            );
        };
        let Some(skill) = self.skill_plan.get(&skill_id) else {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                format!("skill `{skill_id}` is not available to this agent package"),
                serde_json::json!({ "error": "unknown_skill", "skill": skill_id }),
            );
        };
        if standard_skill_document(skill).is_none() {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                format!("skill `{skill_id}` has no valid Agent Skills package"),
                serde_json::json!({
                    "error": "invalid_agent_skill",
                    "skill": skill_id,
                }),
            );
        }
        let output = serde_json::json!({
            "skill": skill_id,
            "descriptor": skill_descriptor_metadata(skill),
            "activation": skill_activation_metadata(skill),
        });
        let evidence = AgentToolEvidence::availability_only(true, "inspected planned agent skill")
            .with_output_value(&output, false)
            .with_record_value(
                "skill_disclosure",
                serde_json::json!({
                    "operation": "describe",
                    "skill": skill.id,
                }),
            )
            .with_record_value(
                AGENT_SKILL_USAGE_EVIDENCE_KIND,
                serde_json::json!({
                    "operation": "describe",
                    "skill": skill.id,
                }),
            );
        AgentToolOutcome {
            call_id: TaskId::generate(),
            step_index,
            call_index,
            call: call.clone(),
            status: AgentToolStatus::Succeeded,
            summary: format!("described skill `{}`", skill.id),
            output,
            event_id: None,
            evidence,
        }
    }

    pub(super) fn execute_tool_search(
        &self,
        _session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
    ) -> AgentToolOutcome {
        let query = json_input_string(call, "query");
        let include_bridge_tools = json_input_bool(call, "include_bridge_tools").unwrap_or(false);
        let max_results = json_input_usize(call, "limit")
            .or_else(|| json_input_usize(call, "max_results"))
            .unwrap_or(20)
            .clamp(1, 50);
        let matches = tool_plan
            .descriptors_matching(query.as_deref(), include_bridge_tools)
            .into_iter()
            .take(max_results)
            .map(|entry| {
                let descriptor = &entry.descriptor;
                serde_json::json!({
                    "name": descriptor.spec.name,
                    "description": descriptor.spec.description,
                    "input_schema": descriptor.input_schema,
                    "output_schema": descriptor.output_schema,
                    "toolsets": descriptor.toolsets,
                    "capabilities": descriptor.capabilities,
                    "source": entry.source,
                    "executable": entry.executable,
                    "model_visible": tool_plan.is_model_visible(&descriptor.spec.name),
                })
            })
            .collect::<Vec<_>>();
        let match_count = matches.len();
        let output = serde_json::json!({
            "query": query,
            "max_results": max_results,
            "include_bridge_tools": include_bridge_tools,
            "count": match_count,
            "registry_generation": tool_plan.registry_generation,
            "visible_toolsets": &tool_plan.visible_toolsets,
            "matches": matches,
        });
        let evidence =
            AgentToolEvidence::availability_only(true, "inspected planned agent tool descriptors")
                .with_output_value(&output, false)
                .with_record_value(
                    "tool_disclosure",
                    serde_json::json!({
                        "operation": "search",
                        "query": output.get("query").cloned().unwrap_or(serde_json::Value::Null),
                        "count": output.get("count").cloned().unwrap_or(serde_json::Value::Null),
                    }),
                );
        AgentToolOutcome {
            call_id: TaskId::generate(),
            step_index,
            call_index,
            call: call.clone(),
            status: AgentToolStatus::Succeeded,
            summary: format!("found {match_count} planned tool(s)"),
            output,
            event_id: None,
            evidence,
        }
    }

    pub(super) fn execute_tool_describe(
        &self,
        session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
    ) -> AgentToolOutcome {
        let Some(tool_name) = json_input_string(call, "tool") else {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                "tool_describe requires a string tool",
                serde_json::json!({ "error": "missing_tool" }),
            );
        };
        let Some(entry) = tool_plan.entry(&tool_name) else {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                format!("tool `{tool_name}` is not planned"),
                serde_json::json!({ "error": "unknown_planned_tool", "tool": tool_name }),
            );
        };
        let output = serde_json::json!({
            "tool": tool_name,
            "descriptor": entry.descriptor,
            "source": entry.source,
            "executable": entry.executable,
            "model_visible": tool_plan.is_model_visible(&entry.descriptor.spec.name),
            "registry_generation": tool_plan.registry_generation,
            "visible_toolsets": &tool_plan.visible_toolsets,
        });
        let evidence =
            AgentToolEvidence::availability_only(true, "inspected planned agent tool descriptor")
                .with_output_value(&output, false)
                .with_record_value(
                    "tool_disclosure",
                    serde_json::json!({
                        "operation": "describe",
                        "tool": entry.descriptor.spec.name,
                    }),
                );
        AgentToolOutcome {
            call_id: TaskId::generate(),
            step_index,
            call_index,
            call: call.clone(),
            status: AgentToolStatus::Succeeded,
            summary: format!("described tool `{}`", entry.descriptor.spec.name),
            output,
            event_id: None,
            evidence,
        }
    }

    pub(super) fn execute_tool_call_bridge(
        &self,
        registry: &AgentToolRegistry,
        session: &InteractionSession,
        tool_plan: &AgentToolPlan,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
    ) -> AgentToolOutcome {
        let Some(target_tool) = json_input_string(call, "tool") else {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                "tool_call requires a string tool",
                serde_json::json!({ "error": "missing_tool" }),
            );
        };
        if is_tool_disclosure_bridge_name(&target_tool) {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                "tool_call cannot recursively call disclosure bridge tools",
                serde_json::json!({ "error": "recursive_tool_disclosure", "tool": target_tool }),
            );
        }
        let Some(entry) = tool_plan.entry(&target_tool) else {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                format!("tool `{target_tool}` is not planned"),
                serde_json::json!({ "error": "unknown_planned_tool", "tool": target_tool }),
            );
        };
        let target_input = call
            .input
            .get("input")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        if !target_input.is_object() {
            return bridge_failed_outcome(
                session,
                step_index,
                call_index,
                call,
                "tool_call input must be an object",
                serde_json::json!({
                    "error": "invalid_target_input",
                    "tool": target_tool,
                }),
            );
        }
        if !entry.executable {
            return bridge_blocked_outcome(
                session,
                step_index,
                call_index,
                call,
                format!("tool `{target_tool}` is planned but has no executor attached"),
                serde_json::json!({
                    "error": "missing_executor",
                    "tool": target_tool,
                    "source": entry.source,
                }),
            );
        }
        let target_call = AgentToolCall {
            call_id: None,
            tool: target_tool.clone(),
            input: target_input,
            r#async: json_input_bool(call, "async").unwrap_or(false),
            summary: json_input_string(call, "summary").unwrap_or_else(|| {
                format!("called through progressive tool disclosure for `{target_tool}`")
            }),
        };
        let target_availability = self
            .tool_surface
            .evaluate_tool_call(Some(&entry.descriptor), &target_call);
        if !target_availability.allowed {
            return bridge_blocked_outcome(
                session,
                step_index,
                call_index,
                call,
                target_availability.reason.clone(),
                serde_json::json!({
                    "error": "tool_unavailable",
                    "surface_id": self.tool_surface.surface_id.clone(),
                    "tool": target_tool,
                    "reason": target_availability.reason,
                }),
            );
        }
        let target_outcome = registry.execute_planned_with_cancellation_and_progress(
            session,
            step_index,
            call_index,
            &target_call,
            tool_plan.registry_generation,
            cancellation_token,
            progress_sink,
        );
        let target_record = AgentToolOrchestrationRecord {
            surface_id: self.tool_surface.surface_id.clone(),
            registry_generation: tool_plan.registry_generation,
            visible_toolsets: tool_plan.visible_toolsets.clone(),
            surface_allowed: target_availability.allowed,
            surface_reason: target_availability.reason,
            descriptor_visible: tool_plan.is_model_visible(&target_tool),
            executor_available: entry.executable,
            blocked_reason: (target_outcome.status == AgentToolStatus::Blocked)
                .then_some("target_blocked".to_string()),
            max_tool_calls_per_step: self.max_tool_calls_per_step,
            requested_timeout_ms: target_call
                .input
                .get("timeout_ms")
                .and_then(serde_json::Value::as_u64),
            run_context: self.run_context.clone(),
        };
        let mut evidence = target_outcome.evidence.clone();
        evidence = with_tool_disclosure_metadata(
            evidence,
            serde_json::json!({
                "operation": "call",
                "target_tool": target_tool,
                "target_source": entry.source,
                "target_model_visible": tool_plan.is_model_visible(&target_call.tool),
                "target_orchestration": target_record,
            }),
        );
        let output = tool_call_bridge_output(&target_outcome);
        AgentToolOutcome {
            call_id: TaskId::generate(),
            step_index,
            call_index,
            call: call.clone(),
            status: target_outcome.status,
            summary: format!(
                "tool_call executed `{}`: {}",
                target_call.tool, target_outcome.summary
            ),
            output,
            event_id: None,
            evidence,
        }
    }
}

pub(super) fn skill_descriptor_metadata(skill: &AgentSkillDescriptor) -> serde_json::Value {
    serde_json::json!({
        "id": &skill.id,
        "description": &skill.description,
        "license": &skill.license,
        "compatibility": &skill.compatibility,
        "allowed_tools": &skill.allowed_tools,
        "metadata": &skill.metadata,
        "resources": &skill.resource_paths,
        "source": &skill.source,
        "package_declared": skill.package_declared,
        "module_id": &skill.module_id,
        "module_version": &skill.module_version,
        "path": &skill.path,
        "target_agents": &skill.target_agents,
        "model_families": &skill.model_families,
        "activation_tags": &skill.activation_tags,
        "required_capabilities": &skill.required_capabilities,
        "catalog_version": &skill.catalog_version,
        "catalog_content_hash": &skill.catalog_content_hash,
        "catalog_base_hash": &skill.catalog_base_hash,
        "catalog_status": &skill.catalog_status,
        "catalog_manifest_path": &skill.catalog_manifest_path,
        "has_skill_content": skill.embedded_text.is_some(),
    })
}

pub(super) fn skill_activation_metadata(skill: &AgentSkillDescriptor) -> serde_json::Value {
    serde_json::json!({
        "mode": "dedicated_tool",
        "available": skill.embedded_text.is_some(),
        "reason": if skill.embedded_text.is_some() {
            "skill_activate loads the SKILL.md instructions and lists bundled resources"
        } else {
            "the descriptor does not include readable SKILL.md content"
        },
        "activation_tags": &skill.activation_tags,
        "required_capabilities": &skill.required_capabilities,
        "allowed_tools": &skill.allowed_tools,
    })
}

pub(super) fn json_input_string(call: &AgentToolCall, key: &str) -> Option<String> {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .map(str::to_string)
        .filter(|value| !value.is_empty())
}

pub(super) fn json_input_bool(call: &AgentToolCall, key: &str) -> Option<bool> {
    call.input.get(key).and_then(serde_json::Value::as_bool)
}

pub(super) fn json_input_usize(call: &AgentToolCall, key: &str) -> Option<usize> {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
}

pub(super) fn bridge_failed_outcome(
    _session: &InteractionSession,
    step_index: usize,
    call_index: usize,
    call: &AgentToolCall,
    summary: impl Into<String>,
    output: serde_json::Value,
) -> AgentToolOutcome {
    let summary = summary.into();
    AgentToolOutcome {
        call_id: TaskId::generate(),
        step_index,
        call_index,
        call: call.clone(),
        status: AgentToolStatus::Failed,
        summary: summary.clone(),
        output: output.clone(),
        event_id: None,
        evidence: AgentToolEvidence::availability_only(true, "tool disclosure bridge failed")
            .with_output_value(&output, false),
    }
}

pub(super) fn bridge_blocked_outcome(
    _session: &InteractionSession,
    step_index: usize,
    call_index: usize,
    call: &AgentToolCall,
    summary: impl Into<String>,
    output: serde_json::Value,
) -> AgentToolOutcome {
    let summary = summary.into();
    AgentToolOutcome {
        call_id: TaskId::generate(),
        step_index,
        call_index,
        call: call.clone(),
        status: AgentToolStatus::Blocked,
        summary: summary.clone(),
        output: output.clone(),
        event_id: None,
        evidence: AgentToolEvidence::availability_only(false, summary)
            .with_output_value(&output, false),
    }
}

pub(super) fn cancelled_tool_outcome(
    step_index: usize,
    call_index: usize,
    call: &AgentToolCall,
    reason: impl Into<String>,
) -> AgentToolOutcome {
    let reason = reason.into();
    let output = serde_json::json!({
        "error": "tool_cancelled",
        "tool": call.tool.clone(),
        "reason": reason,
    });
    AgentToolOutcome {
        call_id: TaskId::generate(),
        step_index,
        call_index,
        call: call.clone(),
        status: AgentToolStatus::Cancelled,
        summary: format!("tool `{}` cancelled", call.tool),
        output: output.clone(),
        event_id: None,
        evidence: AgentToolEvidence::availability_only(false, "turn cancellation requested")
            .with_output_value(&output, false),
    }
}

pub(super) fn tool_call_bridge_output(target_outcome: &AgentToolOutcome) -> serde_json::Value {
    let marker = serde_json::json!({
        "target_tool": target_outcome.call.tool,
        "target_status": target_outcome.status,
        "target_summary": target_outcome.summary,
    });
    match target_outcome.output.clone() {
        serde_json::Value::Object(mut object) => {
            object.insert("_tool_call".to_string(), marker);
            serde_json::Value::Object(object)
        }
        output => serde_json::json!({
            "_tool_call": marker,
            "target_output": output,
        }),
    }
}

pub(super) fn with_tool_disclosure_metadata(
    mut evidence: AgentToolEvidence,
    disclosure: serde_json::Value,
) -> AgentToolEvidence {
    let mut metadata = evidence
        .metadata
        .as_object()
        .cloned()
        .unwrap_or_else(serde_json::Map::new);
    metadata.insert("tool_disclosure".to_string(), disclosure.clone());
    evidence.metadata = serde_json::Value::Object(metadata);
    evidence.with_record_value("tool_disclosure", disclosure)
}

pub(super) fn with_tool_orchestration_record(
    mut evidence: AgentToolEvidence,
    record: &AgentToolOrchestrationRecord,
) -> AgentToolEvidence {
    let record_value =
        serde_json::to_value(record).expect("tool orchestration record serializes to JSON");
    let mut metadata = evidence
        .metadata
        .as_object()
        .cloned()
        .unwrap_or_else(serde_json::Map::new);
    metadata.insert("tool_orchestration".to_string(), record_value);
    evidence.metadata = serde_json::Value::Object(metadata);
    evidence
}

/// Bytes of a detached call's output carried into its async update.
const DETACHED_OUTPUT_PREVIEW_BYTES: usize = 16 * 1024;

/// The async update that delivers a detached call's outcome to its session.
fn detached_tool_update(
    task_id: TaskId,
    tool_call_id: TaskId,
    outcome: &AgentToolOutcome,
) -> AsyncTaskUpdate {
    let mut output = if outcome.output.is_null() {
        String::new()
    } else {
        serde_json::to_string_pretty(&outcome.output).unwrap_or_default()
    };
    if output.len() > DETACHED_OUTPUT_PREVIEW_BYTES {
        let mut end = DETACHED_OUTPUT_PREVIEW_BYTES;
        while !output.is_char_boundary(end) {
            end -= 1;
        }
        output.truncate(end);
        output.push_str("\n[output truncated]");
    }
    let status = match outcome.status {
        AgentToolStatus::Succeeded => AsyncTaskStatus::Completed,
        AgentToolStatus::Cancelled => AsyncTaskStatus::Cancelled,
        AgentToolStatus::Failed | AgentToolStatus::Blocked => AsyncTaskStatus::Failed,
    };
    AsyncTaskUpdate {
        kind: "tool_call".to_string(),
        task_id,
        tool_call_id: Some(tool_call_id),
        status,
        summary: if output.is_empty() {
            format!("`{}`: {}", outcome.call.tool, outcome.summary)
        } else {
            format!("`{}`: {}\n{output}", outcome.call.tool, outcome.summary)
        },
        needs_user_attention: false,
        result_ref: None,
        next_action_hint: AsyncTaskNextAction::InspectResult,
    }
}

/// Detached calls running in this process. Durable records outlive the
/// process; this tells a reattached session which unfinished calls are still
/// running and which were lost with an earlier process.
static LIVE_DETACHED_CALLS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::BTreeSet<TaskId>>,
> = std::sync::LazyLock::new(Default::default);

fn set_detached_call_live(task_id: &TaskId, live: bool) {
    if let Ok(mut calls) = LIVE_DETACHED_CALLS.lock() {
        if live {
            calls.insert(task_id.clone());
        } else {
            calls.remove(task_id);
        }
    }
}

/// Whether the detached call `task_id` is still running in this process.
pub(crate) fn detached_call_is_live(task_id: &TaskId) -> bool {
    LIVE_DETACHED_CALLS
        .lock()
        .is_ok_and(|calls| calls.contains(task_id))
}

/// The completion recorded for a detached call whose process stopped before
/// it finished.
pub(crate) fn interrupted_detached_update(
    task_id: TaskId,
    tool_call_id: TaskId,
    tool: &str,
) -> AsyncTaskUpdate {
    AsyncTaskUpdate {
        kind: "tool_call".to_string(),
        task_id,
        tool_call_id: Some(tool_call_id),
        status: AsyncTaskStatus::Failed,
        summary: format!(
            "`{tool}` was interrupted before it finished: the process running it stopped. It may or may not have taken effect."
        ),
        needs_user_attention: false,
        result_ref: None,
        next_action_hint: AsyncTaskNextAction::InspectResult,
    }
}
