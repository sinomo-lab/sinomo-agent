mod hosted_model;
pub mod memory_notes;
mod memory_notes_sifs;
mod registry;
mod remote;
pub mod session_history;
mod skill;
mod types;

pub mod config;
pub mod local_workspace;

pub use config::{ConfigAgentToolConfig, config_tool_descriptors};
pub use hosted_model::{
    HostedModelAgentToolConfig, HostedModelToolBackend, image_generation_descriptor,
    register_hosted_model_tools, web_search_descriptor,
};
pub use local_workspace::{
    LocalWorkspaceAgentConfig, LocalWorkspaceCommandRuntime, LocalWorkspaceGitToolMode,
    agent_home_sifs_path_for_id,
};
pub use memory_notes::*;
pub use memory_notes_sifs::*;
pub use registry::*;
pub use remote::*;
pub use session_history::*;
pub use skill::{
    SkillAgentToolConfig, register_tools as register_skill_tools, registry as skill_registry,
    skill_tool_descriptors,
};
pub use types::*;

pub fn default_agent_tool_descriptors() -> Vec<AgentToolDescriptor> {
    let mut descriptors = local_workspace::default_local_workspace_tool_descriptors();
    descriptors.extend(memory_note_tool_descriptors());
    descriptors.push(session_history_search_descriptor());
    descriptors.extend(tool_disclosure_descriptors());
    descriptors.extend(skill_tool_descriptors());
    descriptors
}
