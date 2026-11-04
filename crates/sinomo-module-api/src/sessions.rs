//! Provider-neutral session access. Caller identity, conversation membership and
//! mutation grants are supplied by the host, never by a module request.
use serde::{Deserialize, Serialize};
pub const SESSION_INTERFACE: &str = "sinomo.sessions";
pub const SESSION_VERSION: &str = "2.0.0";
pub const SESSION_ACCESS: &str = "sessions.access";
pub const MAX_SESSION_PAGE: usize = 256;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "request", rename_all = "snake_case")]
pub enum SessionRequest {
    List {
        #[serde(default)]
        include_archived: bool,
        #[serde(default)]
        include_related: bool,
    },
    Inspect {
        session_id: String,
    },
    Messages {
        after_sequence: Option<u64>,
        limit: usize,
    },
    Activity {
        session_id: String,
        after_sequence: Option<u64>,
        #[serde(default)]
        tail: bool,
        limit: usize,
    },
    Command {
        command: SessionCommand,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionCommand {
    Create {
        name: String,
        instructions: String,
        prompt: String,
        idle_timeout_secs: u64,
        #[serde(default)]
        worker: Option<SessionWorkerBinding>,
    },
    List {
        archived: bool,
    },
    Inspect {
        id: String,
        session: bool,
    },
    Message {
        id: String,
        message: String,
    },
    Wait {
        id: String,
        timeout_ms: u64,
    },
    Cancel {
        id: String,
    },
    Result {
        id: String,
    },
    Archive {
        id: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionRelationship {
    pub parent_session_id: String,
    pub kind: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub label: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    pub relationship: Option<SessionRelationship>,
    pub archived: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionActivity {
    pub session_id: String,
    pub sequence: u64,
    pub recorded_at_unix_ms: i64,
    pub operation_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub item: serde_json::Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionActivityPage {
    pub records: Vec<SessionActivity>,
    pub next_sequence: Option<u64>,
    pub has_more: bool,
}
/// The module-side client is borrowed for exactly one native invocation.
pub struct SessionClient<'a> {
    host: &'a crate::host::InvocationHost,
}
impl<'a> SessionClient<'a> {
    /// # Safety
    /// `host` must be the live handle supplied to this native invocation.
    pub unsafe fn new(host: &'a crate::host::InvocationHost) -> Self {
        Self { host }
    }
    pub fn request<T: serde::de::DeserializeOwned>(
        &self,
        request: &SessionRequest,
    ) -> Result<T, crate::NativeModuleApiError> {
        let value: serde_json::Value = unsafe {
            self.host
                .call_sobj(SESSION_INTERFACE, SESSION_VERSION, SESSION_ACCESS, request)
        }?;
        serde_json::from_value(value)
            .map_err(|error| crate::NativeModuleApiError::Runtime(error.to_string()))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConversationMessage {
    pub message_id: String,
    pub sequence: u64,
    pub recorded_at_unix_ms: i64,
    pub sender_session_id: Option<String>,
    pub recipients: Vec<String>,
    pub operation_id: Option<String>,
    pub reply_to: Option<String>,
    pub content: String,
}

/// Common lifecycle acknowledgement. Tool adapters may retain additional
/// fields; modules use the canonical session ID rather than a tool's handle.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionCommandReceipt {
    pub accepted: bool,
    pub session_id: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub archived: bool,
}

/// A module requests its own exported agent package. The host supplies identity,
/// storage and executors; no runtime implementation crosses the native ABI.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionWorkerBinding {
    pub agent_id: String,
    pub provider: String,
    pub model: String,
    pub workspace: String,
    #[serde(default)]
    pub context: Vec<serde_json::Value>,
    pub max_steps: Option<usize>,
}

/// Object-safe request interface shared by native clients and host fixtures.
pub trait SessionService {
    fn request(&self, request: &SessionRequest) -> Result<serde_json::Value, String>;
}
impl SessionService for SessionClient<'_> {
    fn request(&self, request: &SessionRequest) -> Result<serde_json::Value, String> {
        SessionClient::request(self, request).map_err(|e| e.to_string())
    }
}

/// Declared by the activated module, never supplied by an agent's request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SessionWorkerProfile {
    /// Paths relative to SinomoDataDir; workspace admission remains host-owned.
    pub workspace_roots: Vec<String>,
    /// Capability names from the agent tool contract. Empty grants are rejected.
    pub capabilities: Vec<String>,
    pub max_steps: Option<usize>,
}
