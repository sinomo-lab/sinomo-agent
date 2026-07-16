mod registry;
mod remote;
mod skill;
mod types;

pub mod config;
pub mod local_workspace;

pub use config::{ConfigAgentToolConfig, config_tool_descriptors};
pub use local_workspace::{
    LocalWorkspaceAgentConfig, LocalWorkspaceCommandRuntime, LocalWorkspaceGitToolMode,
    agent_home_sifs_path_for_id,
};
pub use registry::*;
pub use remote::*;
pub use skill::{
    SkillAgentToolConfig, register_tools as register_skill_tools, registry as skill_registry,
    skill_tool_descriptors,
};
pub use types::*;

pub fn default_agent_tool_descriptors() -> Vec<AgentToolDescriptor> {
    let mut descriptors = local_workspace::default_local_workspace_tool_descriptors();
    descriptors.extend(tool_disclosure_descriptors());
    descriptors.extend(skill_tool_descriptors());
    descriptors
}
