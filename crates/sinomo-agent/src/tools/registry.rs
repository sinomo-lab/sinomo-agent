use super::{
    AgentTool, AgentToolDescriptor, AgentToolEvidence, AgentToolOutcome, AgentToolRequest,
    AgentToolResult, AgentToolStatus,
};
use crate::{AgentToolCall, InteractionSession, TaskId};
use std::sync::{Arc, RwLock};

#[derive(Clone, Default)]
pub struct AgentToolRegistry {
    tools: Arc<RwLock<Vec<Arc<dyn AgentTool>>>>,
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
            return Some(replaced.descriptor());
        }
        tools.push(tool);
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
        Some(tools.remove(index).descriptor())
    }

    pub fn clear(&self) {
        self.tools
            .write()
            .expect("agent tool registry lock poisoned")
            .clear();
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

    pub fn execute(
        &self,
        session: &InteractionSession,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
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
