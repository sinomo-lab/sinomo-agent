use super::{
    AgentTool, AgentToolDescriptor, AgentToolEvidence, AgentToolOutcome, AgentToolProgressSink,
    AgentToolRequest, AgentToolResult, AgentToolStatus,
};
use crate::{AgentCancellationToken, AgentToolCall, InteractionSession, TaskId};
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Default)]
pub struct AgentToolRegistry {
    tools: Arc<RwLock<Vec<Arc<dyn AgentTool>>>>,
    generation: Arc<AtomicU64>,
}

impl AgentToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn descriptor_only(descriptors: impl IntoIterator<Item = AgentToolDescriptor>) -> Self {
        let registry = Self::new();
        for descriptor in descriptors {
            registry.register(DescriptorOnlyAgentTool { descriptor });
        }
        registry
    }

    pub fn local_workspace(root: impl Into<std::path::PathBuf>) -> Self {
        Self::local_workspace_with_config(super::LocalWorkspaceAgentConfig::new(root))
    }

    pub fn local_workspace_with_config(config: super::LocalWorkspaceAgentConfig) -> Self {
        super::local_workspace::registry(config)
    }

    pub fn config_management(config: super::ConfigAgentToolConfig) -> Self {
        super::config::registry(config)
    }

    pub fn with_config_management(self, config: super::ConfigAgentToolConfig) -> Self {
        super::config::register_tools(&self, config);
        self
    }

    pub fn with_tool<T: AgentTool + 'static>(self, tool: T) -> Self {
        self.register(tool);
        self
    }

    pub fn register<T: AgentTool + 'static>(&self, tool: T) -> Option<AgentToolDescriptor> {
        self.register_arc(Arc::new(tool))
    }

    pub fn register_arc(&self, tool: Arc<dyn AgentTool>) -> Option<AgentToolDescriptor> {
        let descriptor = tool.descriptor();
        let name = descriptor.spec.name.clone();
        let mut tools = self
            .tools
            .write()
            .expect("agent tool registry lock poisoned");
        if let Some(index) = tools
            .iter()
            .position(|existing| existing.descriptor().spec.name == name)
        {
            let replaced = std::mem::replace(&mut tools[index], tool);
            self.bump_generation();
            return Some(replaced.descriptor());
        }
        tools.push(tool);
        self.bump_generation();
        None
    }

    pub fn remove(&self, tool_name: &str) -> Option<AgentToolDescriptor> {
        let mut tools = self
            .tools
            .write()
            .expect("agent tool registry lock poisoned");
        let index = tools
            .iter()
            .position(|tool| tool.descriptor().spec.name == tool_name)?;
        let descriptor = tools.remove(index).descriptor();
        self.bump_generation();
        Some(descriptor)
    }

    pub fn clear(&self) {
        self.tools
            .write()
            .expect("agent tool registry lock poisoned")
            .clear();
        self.bump_generation();
    }

    pub fn contains(&self, tool_name: &str) -> bool {
        self.tools
            .read()
            .expect("agent tool registry lock poisoned")
            .iter()
            .any(|tool| tool.descriptor().spec.name == tool_name)
    }

    pub fn len(&self) -> usize {
        self.tools
            .read()
            .expect("agent tool registry lock poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn descriptors(&self) -> Vec<AgentToolDescriptor> {
        self.tools
            .read()
            .expect("agent tool registry lock poisoned")
            .iter()
            .map(|tool| tool.descriptor())
            .collect()
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    pub fn execute(
        &self,
        session: &InteractionSession,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
    ) -> AgentToolOutcome {
        self.execute_with_cancellation(
            session,
            step_index,
            call_index,
            call,
            AgentCancellationToken::new(),
        )
    }

    pub fn execute_with_cancellation(
        &self,
        session: &InteractionSession,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
        cancellation_token: AgentCancellationToken,
    ) -> AgentToolOutcome {
        self.execute_with_cancellation_and_progress(
            session,
            step_index,
            call_index,
            call,
            cancellation_token,
            AgentToolProgressSink::default(),
        )
    }

    pub fn execute_with_cancellation_and_progress(
        &self,
        session: &InteractionSession,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
    ) -> AgentToolOutcome {
        let call_id = TaskId::generate();
        let tool = self
            .tools
            .read()
            .expect("agent tool registry lock poisoned")
            .iter()
            .find(|tool| tool.descriptor().spec.name == call.tool)
            .cloned();
        let result = tool
            .map(|tool| {
                tool.execute(AgentToolRequest {
                    call_id: &call_id,
                    session,
                    step_index,
                    call_index,
                    call,
                    cancellation_token,
                    progress_sink,
                })
            })
            .unwrap_or_else(|| {
                AgentToolResult::failed(
                    format!("unknown tool `{}`", call.tool),
                    serde_json::json!({
                        "error": "unknown_tool",
                        "tool": call.tool,
                    }),
                )
            });

        let mut evidence = result.evidence.unwrap_or_else(|| {
            AgentToolEvidence::policy_only(
                result.status != AgentToolStatus::Blocked,
                "executed through registered agent tool registry",
            )
            .with_output_value(&result.output, false)
        });
        if evidence.output.model_visible_bytes == 0 && !result.output.is_null() {
            let truncated = evidence.output.truncated;
            evidence = evidence.with_output_value(&result.output, truncated);
        }

        AgentToolOutcome {
            call_id,
            step_index,
            call_index,
            call: call.clone(),
            status: result.status,
            summary: result.summary,
            output: result.output,
            event_id: None,
            evidence,
        }
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
    }
}

struct DescriptorOnlyAgentTool {
    descriptor: AgentToolDescriptor,
}

impl AgentTool for DescriptorOnlyAgentTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        self.descriptor.clone()
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        AgentToolResult::blocked(
            format!(
                "tool `{}` is described but no executor is attached to this agent",
                request.call.tool
            ),
            serde_json::json!({
                "error": "missing_executor",
                "tool": request.call.tool,
            }),
        )
    }
}
