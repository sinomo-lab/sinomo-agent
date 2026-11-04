//! Native tool and skill declarations. Tool execution uses scoped host services;
//! the host retains session state, model execution and cancellation ownership.
use serde::{Deserialize, Serialize};

pub const SESSION_TOOLS_INTERFACE: &str = "sinomo.agent.session-tools";
pub const SESSION_TOOLS_VERSION: &str = "2.0.0";
pub const SESSION_TOOLS_DESCRIBE: &str = "agent.session-tools.describe";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
    /// Discovery groups are owned by the provider. Empty uses the generic
    /// module group; hosts do not infer feature-specific built-in categories.
    #[serde(default)]
    pub toolsets: Vec<String>,
    pub skill_id: String,
    pub skill: String,
}

pub const SESSION_TOOLS_EXECUTE: &str = "agent.session-tools.execute";
