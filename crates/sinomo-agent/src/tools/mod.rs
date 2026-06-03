mod registry;
mod types;

pub mod config;
pub mod local_workspace;

pub use config::{ConfigAgentToolConfig, config_tool_descriptors};
pub use local_workspace::LocalWorkspaceAgentConfig;
pub use registry::*;
pub use types::*;

pub fn default_agent_tool_descriptors() -> Vec<AgentToolDescriptor> {
    local_workspace::default_local_workspace_tool_descriptors()
}
