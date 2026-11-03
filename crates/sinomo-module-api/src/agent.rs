//! Native session tools return typed intentions; the agent host retains final
//! authority, session storage, model execution and cancellation ownership.
use serde::{Deserialize, Serialize};

pub const SESSION_TOOLS_INTERFACE: &str = "sinomo.agent.session-tools";
pub const SESSION_TOOLS_VERSION: &str = "2.0.0";
pub const SESSION_TOOLS_DESCRIBE: &str = "agent.session-tools.describe";
pub const SESSION_TOOLS_TRANSLATE: &str = "agent.session-tools.translate";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
    pub skill_id: String,
    pub skill: String,
}

pub use crate::sessions::SessionCommand;
pub const SESSION_TOOLS_EXECUTE: &str = "agent.session-tools.execute";
