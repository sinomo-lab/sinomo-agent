use super::{
    AgentTool, AgentToolCapability, AgentToolDescriptor, AgentToolEvidence,
    AgentToolInvocationMode, AgentToolRequest, AgentToolResult,
};
use crate::{AgentCancellationToken, AgentToolKind, AgentToolSpec, TaskId};
use serde::{Deserialize, Serialize};
use sinomo_runtime::events::SessionId;
use std::sync::Arc;

pub const SESSION_HISTORY_SEARCH_TOOL_NAME: &str = "session_history_search";

const DEFAULT_RESULT_LIMIT: usize = 10;
const DEFAULT_MAX_RESULT_LIMIT: usize = 50;
const DEFAULT_MAX_QUERY_BYTES: usize = 1_024;
const DEFAULT_MAX_CURSOR_BYTES: usize = 4_096;
const DEFAULT_MAX_SCANNED_ACTIVITIES: usize = 256;
const DEFAULT_MAX_SCANNED_ITEMS: usize = 4_096;
const DEFAULT_MAX_SCANNED_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_MAX_EXCERPT_BYTES: usize = 1_024;
const DEFAULT_MAX_TITLE_BYTES: usize = 512;
const DEFAULT_MAX_AUTHOR_BYTES: usize = 256;
// JSON field names, IDs, timestamps, enum tags, escaping, and evidence framing
// all add overhead beyond the three model-visible strings. Keep the default
// output ceiling coherent with the maximum default result page instead of
// imposing an unrelated 64 KiB failure threshold on an otherwise valid page.
const JSON_STRING_WORST_CASE_EXPANSION: usize = 6;
const DEFAULT_PER_MATCH_OUTPUT_OVERHEAD_BYTES: usize = 1_024;
const DEFAULT_OUTPUT_ENVELOPE_OVERHEAD_BYTES: usize = 8 * 1_024;
const DEFAULT_MAX_OUTPUT_BYTES: usize = DEFAULT_MAX_RESULT_LIMIT
    * ((DEFAULT_MAX_EXCERPT_BYTES + DEFAULT_MAX_TITLE_BYTES + DEFAULT_MAX_AUTHOR_BYTES)
        * JSON_STRING_WORST_CASE_EXPANSION
        + DEFAULT_PER_MATCH_OUTPUT_OVERHEAD_BYTES)
    + DEFAULT_MAX_QUERY_BYTES * JSON_STRING_WORST_CASE_EXPANSION
    + DEFAULT_MAX_CURSOR_BYTES * JSON_STRING_WORST_CASE_EXPANSION
    + DEFAULT_OUTPUT_ENVELOPE_OVERHEAD_BYTES;
const MAX_AGENT_KEY_BYTES: usize = 512;

/// Immutable authority injected by the host when the tool is registered.
///
/// The model-facing request deliberately has no agent-id field. A backend must
/// use this principal, together with its own current storage policy, to decide
/// which chats, channels, and background activities may be searched. Raw
/// model-session storage is explicitly outside this tool's contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionHistorySearchPrincipal {
    agent_key: String,
}

impl SessionHistorySearchPrincipal {
    pub fn new(agent_key: impl Into<String>) -> Result<Self, SessionHistorySearchPrincipalError> {
        let agent_key = agent_key.into();
        let trimmed = agent_key.trim();
        if trimmed.is_empty() {
            return Err(SessionHistorySearchPrincipalError::Empty);
        }
        if trimmed.len() > MAX_AGENT_KEY_BYTES {
            return Err(SessionHistorySearchPrincipalError::TooLong {
                max_bytes: MAX_AGENT_KEY_BYTES,
                actual_bytes: trimmed.len(),
            });
        }
        Ok(Self {
            agent_key: trimmed.to_string(),
        })
    }

    pub fn agent_key(&self) -> &str {
        &self.agent_key
    }
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum SessionHistorySearchPrincipalError {
    #[error("session-history principal agent key is empty")]
    Empty,
    #[error("session-history principal agent key is {actual_bytes} bytes; maximum is {max_bytes}")]
    TooLong {
        max_bytes: usize,
        actual_bytes: usize,
    },
}

/// Host-selected hard limits. None of these values can be increased by model
/// input; the effective request passed to the backend carries the bounded
/// values explicitly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionHistorySearchLimits {
    pub default_result_limit: usize,
    pub max_result_limit: usize,
    pub max_query_bytes: usize,
    pub max_cursor_bytes: usize,
    pub max_scanned_activities: usize,
    pub max_scanned_items: usize,
    pub max_scanned_bytes: u64,
    pub max_excerpt_bytes: usize,
    pub max_title_bytes: usize,
    pub max_author_bytes: usize,
    pub max_output_bytes: usize,
}

impl Default for SessionHistorySearchLimits {
    fn default() -> Self {
        Self {
            default_result_limit: DEFAULT_RESULT_LIMIT,
            max_result_limit: DEFAULT_MAX_RESULT_LIMIT,
            max_query_bytes: DEFAULT_MAX_QUERY_BYTES,
            max_cursor_bytes: DEFAULT_MAX_CURSOR_BYTES,
            max_scanned_activities: DEFAULT_MAX_SCANNED_ACTIVITIES,
            max_scanned_items: DEFAULT_MAX_SCANNED_ITEMS,
            max_scanned_bytes: DEFAULT_MAX_SCANNED_BYTES,
            max_excerpt_bytes: DEFAULT_MAX_EXCERPT_BYTES,
            max_title_bytes: DEFAULT_MAX_TITLE_BYTES,
            max_author_bytes: DEFAULT_MAX_AUTHOR_BYTES,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        }
    }
}

impl SessionHistorySearchLimits {
    fn effective_default_result_limit(&self) -> usize {
        self.default_result_limit
            .max(1)
            .min(self.effective_max_result_limit())
    }

    fn effective_max_result_limit(&self) -> usize {
        self.max_result_limit.max(1)
    }
}

/// A fully bounded backend request. The scan and output budgets are host
/// authority, not user preferences.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionHistorySearchRequest {
    pub query: String,
    pub cursor: Option<String>,
    pub limit: usize,
    pub max_scanned_activities: usize,
    pub max_scanned_items: usize,
    pub max_scanned_bytes: u64,
    pub max_excerpt_bytes: usize,
    pub max_title_bytes: usize,
    pub max_author_bytes: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SessionHistorySearchStats {
    pub activities: usize,
    pub items: usize,
    pub bytes: u64,
    /// Items deliberately advanced past without reading their bodies because
    /// an authoritative size bound made them unsafe to materialize.
    pub omitted_items: usize,
}

/// The consistency level the backend could provide for this page.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SessionHistorySearchConsistency {
    /// The page is a bounded view of currently visible records. Concurrent or
    /// delayed records may require a fresh search to discover.
    #[default]
    BestEffort,
    /// The backend pinned one stable logical snapshot for this cursor chain.
    StableSnapshot,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SessionHistoryActivityKind {
    Chat,
    Channel,
    BackgroundActivity,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SessionHistorySearchMatch {
    /// Identifier of the authoritative visible activity, never an identifier
    /// that grants access to a raw private model-session record.
    pub activity_id: SessionId,
    pub activity_kind: SessionHistoryActivityKind,
    pub item_id: TaskId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    pub title: String,
    pub author: String,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
    pub excerpt: String,
    #[serde(default)]
    pub excerpt_truncated: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SessionHistorySearchPage {
    pub matches: Vec<SessionHistorySearchMatch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub complete: bool,
    /// The cursor range was processed best-effort but one or more bounded
    /// records were omitted. `complete` remains solely cursor exhaustion.
    #[serde(default)]
    pub partial: bool,
    #[serde(default)]
    pub consistency: SessionHistorySearchConsistency,
    #[serde(default)]
    pub scanned: SessionHistorySearchStats,
}

impl SessionHistorySearchPage {
    pub fn complete(matches: Vec<SessionHistorySearchMatch>) -> Self {
        Self {
            matches,
            next_cursor: None,
            complete: true,
            partial: false,
            consistency: SessionHistorySearchConsistency::BestEffort,
            scanned: SessionHistorySearchStats::default(),
        }
    }

    pub fn continued(
        matches: Vec<SessionHistorySearchMatch>,
        next_cursor: impl Into<String>,
    ) -> Self {
        Self {
            matches,
            next_cursor: Some(next_cursor.into()),
            complete: false,
            partial: false,
            consistency: SessionHistorySearchConsistency::BestEffort,
            scanned: SessionHistorySearchStats::default(),
        }
    }
}

/// Storage adapter for visible historical activity search.
///
/// Implementations must treat `principal` as the only agent identity, recheck
/// current authorization before reading each result body, bind continuation
/// cursors to the principal and query, and stop at every budget in `request`.
pub trait SessionHistorySearchBackend: Send + Sync {
    fn search(
        &self,
        principal: &SessionHistorySearchPrincipal,
        request: &SessionHistorySearchRequest,
        cancellation: &AgentCancellationToken,
    ) -> Result<SessionHistorySearchPage, SessionHistorySearchError>;
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum SessionHistorySearchError {
    #[error("session history search is unavailable: {detail}")]
    Unavailable { detail: String },
    #[error("session history continuation is invalid: {detail}")]
    InvalidCursor { detail: String },
    #[error("session history access was denied")]
    AccessDenied,
    #[error("session history search was cancelled")]
    Cancelled,
    #[error("session history search failed: {detail}")]
    Backend { detail: String },
}

#[derive(Clone)]
pub struct SessionHistorySearchTool {
    principal: SessionHistorySearchPrincipal,
    backend: Arc<dyn SessionHistorySearchBackend>,
    limits: SessionHistorySearchLimits,
}

impl SessionHistorySearchTool {
    pub fn new(
        principal: SessionHistorySearchPrincipal,
        backend: Arc<dyn SessionHistorySearchBackend>,
    ) -> Self {
        Self {
            principal,
            backend,
            limits: SessionHistorySearchLimits::default(),
        }
    }

    pub fn with_limits(mut self, limits: SessionHistorySearchLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn principal(&self) -> &SessionHistorySearchPrincipal {
        &self.principal
    }
}

impl AgentTool for SessionHistorySearchTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        session_history_search_descriptor_with_limit(self.limits.effective_max_result_limit())
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        if request.cancellation_token.is_cancelled() {
            return cancelled_result();
        }
        let cancellation = request.cancellation_token.clone();
        let request = match self.parse_request(request.call) {
            Ok(request) => request,
            Err(result) => return result,
        };
        let page = match self
            .backend
            .search(&self.principal, &request, &cancellation)
        {
            Ok(page) => page,
            Err(error) => return search_error_result(error),
        };
        if let Err(detail) = validate_page(&page, &request, &self.limits) {
            return AgentToolResult::failed(
                "session history backend returned an invalid bounded page",
                serde_json::json!({
                    "error": "backend_contract_violation",
                    "detail": detail,
                }),
            );
        }
        let output_truncated = !page.complete
            || page.partial
            || page.matches.iter().any(|matched| matched.excerpt_truncated);
        let output = serde_json::json!({
            "query": request.query,
            "matches": page.matches,
            "next_cursor": page.next_cursor,
            "complete": page.complete,
            "partial": page.partial,
            "consistency": page.consistency,
            "scanned": page.scanned,
        });
        let output_bytes = serde_json::to_vec(&output)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if output_bytes > self.limits.max_output_bytes {
            return AgentToolResult::failed(
                "session history backend exceeded the model-visible output budget",
                serde_json::json!({
                    "error": "backend_contract_violation",
                    "detail": format!(
                        "serialized output is {output_bytes} bytes; maximum is {}",
                        self.limits.max_output_bytes
                    ),
                }),
            );
        }
        AgentToolResult::succeeded(
            format!("found {} historical activity match(es)", page.matches.len()),
            output.clone(),
        )
        .with_evidence(
            AgentToolEvidence::availability_only(
                true,
                "searched authorized visible activity history through an injected backend",
            )
            .with_output_value(&output, output_truncated),
        )
    }
}

impl SessionHistorySearchTool {
    fn parse_request(
        &self,
        call: &crate::AgentToolCall,
    ) -> Result<SessionHistorySearchRequest, AgentToolResult> {
        let Some(query) = call.input.get("query").and_then(serde_json::Value::as_str) else {
            return Err(AgentToolResult::failed(
                "session_history_search requires a non-empty string query",
                serde_json::json!({ "error": "missing_query" }),
            ));
        };
        let query = query.trim();
        if query.is_empty() {
            return Err(AgentToolResult::failed(
                "session_history_search requires a non-empty string query",
                serde_json::json!({ "error": "empty_query" }),
            ));
        }
        if query.len() > self.limits.max_query_bytes {
            return Err(AgentToolResult::failed(
                "session history query exceeds the configured byte limit",
                serde_json::json!({
                    "error": "query_too_large",
                    "max_bytes": self.limits.max_query_bytes,
                    "actual_bytes": query.len(),
                }),
            ));
        }
        let cursor = match call.input.get("cursor") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(cursor)) if cursor.trim().is_empty() => None,
            Some(serde_json::Value::String(cursor)) => {
                if cursor.len() > self.limits.max_cursor_bytes {
                    return Err(AgentToolResult::failed(
                        "session history continuation exceeds the configured byte limit",
                        serde_json::json!({
                            "error": "cursor_too_large",
                            "max_bytes": self.limits.max_cursor_bytes,
                            "actual_bytes": cursor.len(),
                        }),
                    ));
                }
                Some(cursor.clone())
            }
            Some(_) => {
                return Err(AgentToolResult::failed(
                    "session_history_search cursor must be a string",
                    serde_json::json!({ "error": "invalid_cursor_type" }),
                ));
            }
        };
        let requested_limit = match call.input.get("limit") {
            None | Some(serde_json::Value::Null) => self.limits.effective_default_result_limit(),
            Some(value) => value
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or_else(|| {
                    AgentToolResult::failed(
                        "session_history_search limit must be a positive integer",
                        serde_json::json!({ "error": "invalid_limit" }),
                    )
                })?,
        };
        Ok(SessionHistorySearchRequest {
            query: query.to_string(),
            cursor,
            limit: requested_limit.min(self.limits.effective_max_result_limit()),
            max_scanned_activities: self.limits.max_scanned_activities,
            max_scanned_items: self.limits.max_scanned_items,
            max_scanned_bytes: self.limits.max_scanned_bytes,
            max_excerpt_bytes: self.limits.max_excerpt_bytes,
            max_title_bytes: self.limits.max_title_bytes,
            max_author_bytes: self.limits.max_author_bytes,
        })
    }
}

fn validate_page(
    page: &SessionHistorySearchPage,
    request: &SessionHistorySearchRequest,
    limits: &SessionHistorySearchLimits,
) -> Result<(), String> {
    if page.matches.len() > request.limit {
        return Err(format!(
            "backend returned {} matches for a limit of {}",
            page.matches.len(),
            request.limit
        ));
    }
    match (&page.next_cursor, page.complete) {
        (Some(_), true) => return Err("complete page includes a continuation cursor".to_string()),
        (None, false) => return Err("incomplete page has no continuation cursor".to_string()),
        _ => {}
    }
    if let Some(cursor) = &page.next_cursor
        && cursor.len() > limits.max_cursor_bytes
    {
        return Err(format!(
            "backend cursor is {} bytes; maximum is {}",
            cursor.len(),
            limits.max_cursor_bytes
        ));
    }
    if request.cursor.is_some() && page.next_cursor == request.cursor {
        return Err("backend returned the same continuation cursor without advancing".to_string());
    }
    if page.scanned.activities > request.max_scanned_activities
        || page.scanned.items > request.max_scanned_items
        || page.scanned.bytes > request.max_scanned_bytes
    {
        return Err("backend reported work beyond the injected scan budget".to_string());
    }
    if page.scanned.omitted_items > page.scanned.items {
        return Err("backend reported more omitted items than scanned items".to_string());
    }
    for matched in &page.matches {
        if matched.excerpt.len() > request.max_excerpt_bytes {
            return Err(format!(
                "match excerpt is {} bytes; maximum is {}",
                matched.excerpt.len(),
                request.max_excerpt_bytes
            ));
        }
        if matched.title.len() > request.max_title_bytes {
            return Err(format!(
                "match title is {} bytes; maximum is {}",
                matched.title.len(),
                request.max_title_bytes
            ));
        }
        if matched.author.len() > request.max_author_bytes {
            return Err(format!(
                "match author is {} bytes; maximum is {}",
                matched.author.len(),
                request.max_author_bytes
            ));
        }
    }
    Ok(())
}

fn search_error_result(error: SessionHistorySearchError) -> AgentToolResult {
    match error {
        SessionHistorySearchError::Unavailable { detail } => AgentToolResult::failed(
            "session history search is unavailable",
            serde_json::json!({ "error": "history_unavailable", "detail": detail }),
        ),
        SessionHistorySearchError::InvalidCursor { detail } => AgentToolResult::failed(
            "session history continuation is invalid",
            serde_json::json!({ "error": "invalid_cursor", "detail": detail }),
        ),
        SessionHistorySearchError::AccessDenied => AgentToolResult::blocked(
            "session history access was denied",
            serde_json::json!({ "error": "access_denied" }),
        ),
        SessionHistorySearchError::Cancelled => cancelled_result(),
        SessionHistorySearchError::Backend { detail } => AgentToolResult::failed(
            "session history search failed",
            serde_json::json!({ "error": "history_search_failed", "detail": detail }),
        ),
    }
}

fn cancelled_result() -> AgentToolResult {
    AgentToolResult::cancelled(
        "session history search was cancelled",
        serde_json::json!({ "error": "cancelled" }),
    )
}

pub fn session_history_search_descriptor() -> AgentToolDescriptor {
    session_history_search_descriptor_with_limit(DEFAULT_MAX_RESULT_LIMIT)
}

fn session_history_search_descriptor_with_limit(max_result_limit: usize) -> AgentToolDescriptor {
    AgentToolDescriptor::new(AgentToolSpec::new(
        AgentToolKind::SessionHistorySearch,
        SESSION_HISTORY_SEARCH_TOOL_NAME,
        "Search chats, channels, and background activities the current agent engaged in using bounded excerpts and an opaque continuation cursor. Raw model-session records are never exposed.",
        false,
    ))
    .with_invocation_modes(AgentToolInvocationMode::all())
    .with_toolsets(["core", "memory"])
    .with_capabilities([AgentToolCapability::ReadSessionHistory])
    .supporting_parallel(true)
    .with_input_schema(serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["query"],
        "properties": {
            "query": {
                "type": "string",
                "minLength": 1,
                "description": "Literal or backend-supported text query over authorized visible chats, channels, and background activities."
            },
            "cursor": {
                "type": "string",
                "description": "Opaque continuation returned by an earlier call with the same query."
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": max_result_limit,
                "description": "Maximum matches to return. The host may apply a lower hard limit."
            }
        }
    }))
    .with_output_schema(serde_json::json!({
        "type": "object",
        "required": ["query", "matches", "complete", "partial", "consistency", "scanned"],
        "properties": {
            "query": { "type": "string" },
            "matches": { "type": "array" },
            "next_cursor": { "type": ["string", "null"] },
            "complete": { "type": "boolean" },
            "partial": { "type": "boolean" },
            "consistency": {
                "type": "string",
                "enum": ["best_effort", "stable_snapshot"]
            },
            "scanned": { "type": "object" }
        }
    }))
}

pub fn register_session_history_search_tool(
    registry: &super::AgentToolRegistry,
    principal: SessionHistorySearchPrincipal,
    backend: Arc<dyn SessionHistorySearchBackend>,
) -> Option<AgentToolDescriptor> {
    registry.register(SessionHistorySearchTool::new(principal, backend))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentToolCall, AgentToolProgressSink, AgentToolStatus, InteractionSession,
        InteractionSurface, InteractionSurfaceKind,
    };
    use sinomo_runtime::node::NodeId;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingBackend {
        calls: Mutex<Vec<(SessionHistorySearchPrincipal, SessionHistorySearchRequest)>>,
        page: Mutex<Option<Result<SessionHistorySearchPage, SessionHistorySearchError>>>,
    }

    impl RecordingBackend {
        fn returning(page: Result<SessionHistorySearchPage, SessionHistorySearchError>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                page: Mutex::new(Some(page)),
            }
        }
    }

    impl SessionHistorySearchBackend for RecordingBackend {
        fn search(
            &self,
            principal: &SessionHistorySearchPrincipal,
            request: &SessionHistorySearchRequest,
            _cancellation: &AgentCancellationToken,
        ) -> Result<SessionHistorySearchPage, SessionHistorySearchError> {
            self.calls
                .lock()
                .expect("calls lock")
                .push((principal.clone(), request.clone()));
            self.page
                .lock()
                .expect("page lock")
                .take()
                .unwrap_or_else(|| Ok(SessionHistorySearchPage::complete(Vec::new())))
        }
    }

    fn test_interaction_session() -> InteractionSession {
        InteractionSession::new(
            SessionId::from_legacy_named("session_history_tool_test").expect("session id"),
            NodeId::from_legacy_named("session_history_tool_node").expect("node id"),
            InteractionSurface::new("session.history.test", InteractionSurfaceKind::CommandLine),
        )
    }

    fn execute(tool: &SessionHistorySearchTool, input: serde_json::Value) -> AgentToolResult {
        let call_id = TaskId::generate();
        let call = AgentToolCall {
            call_id: None,
            tool: SESSION_HISTORY_SEARCH_TOOL_NAME.to_string(),
            input,
            r#async: false,
            summary: "search history".to_string(),
        };
        let session = test_interaction_session();
        tool.execute(AgentToolRequest {
            call_id: &call_id,
            session: &session,
            step_index: 0,
            call_index: 0,
            call: &call,
            cancellation_token: AgentCancellationToken::new(),
            progress_sink: AgentToolProgressSink::default(),
        })
    }

    fn one_match() -> SessionHistorySearchMatch {
        SessionHistorySearchMatch {
            activity_id: SessionId::from_legacy_named("historical_activity").expect("session id"),
            activity_kind: SessionHistoryActivityKind::Channel,
            item_id: TaskId::generate(),
            sequence: Some(7),
            title: "Release discussion".to_string(),
            author: "User".to_string(),
            created_at: time::OffsetDateTime::now_utc(),
            excerpt: "the deployment used the stable artifact".to_string(),
            excerpt_truncated: false,
        }
    }

    #[test]
    fn descriptor_has_no_model_supplied_principal() {
        let descriptor = session_history_search_descriptor();
        assert_eq!(descriptor.spec.kind, AgentToolKind::SessionHistorySearch);
        assert_eq!(descriptor.spec.name, SESSION_HISTORY_SEARCH_TOOL_NAME);
        let properties = descriptor.input_schema["properties"]
            .as_object()
            .expect("input properties");
        assert!(properties.contains_key("query"));
        assert!(properties.contains_key("cursor"));
        assert!(properties.contains_key("limit"));
        assert!(!properties.contains_key("agent_id"));
        assert!(!properties.contains_key("agent_key"));
        assert_eq!(descriptor.input_schema["additionalProperties"], false);
    }

    #[test]
    fn tool_injects_principal_and_clamps_model_limit_and_scan_budgets() {
        let backend = Arc::new(RecordingBackend::returning(Ok(
            SessionHistorySearchPage::complete(vec![one_match()]),
        )));
        let principal = SessionHistorySearchPrincipal::new(" main-agent ").expect("principal");
        let mut limits = SessionHistorySearchLimits::default();
        limits.max_result_limit = 3;
        limits.max_scanned_activities = 11;
        limits.max_scanned_items = 22;
        limits.max_scanned_bytes = 33;
        limits.max_title_bytes = 44;
        limits.max_author_bytes = 55;
        let tool = SessionHistorySearchTool::new(principal, backend.clone()).with_limits(limits);

        let result = execute(
            &tool,
            serde_json::json!({
                "query": "  deployment  ",
                "limit": 999,
                "agent_id": "another-agent",
                "max_scanned_items": 999_999,
            }),
        );

        assert_eq!(result.status, AgentToolStatus::Succeeded);
        let calls = backend.calls.lock().expect("calls lock");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0.agent_key(), "main-agent");
        assert_eq!(calls[0].1.query, "deployment");
        assert_eq!(calls[0].1.limit, 3);
        assert_eq!(calls[0].1.max_scanned_activities, 11);
        assert_eq!(calls[0].1.max_scanned_items, 22);
        assert_eq!(calls[0].1.max_scanned_bytes, 33);
        assert_eq!(calls[0].1.max_title_bytes, 44);
        assert_eq!(calls[0].1.max_author_bytes, 55);
        assert!(result.output.get("agent_id").is_none());
        assert!(result.output.get("agent_key").is_none());
    }

    #[test]
    fn invalid_query_and_cursor_are_rejected_before_backend_execution() {
        let backend = Arc::new(RecordingBackend::default());
        let principal = SessionHistorySearchPrincipal::new("main-agent").expect("principal");
        let mut limits = SessionHistorySearchLimits::default();
        limits.max_query_bytes = 4;
        limits.max_cursor_bytes = 3;
        let tool = SessionHistorySearchTool::new(principal, backend.clone()).with_limits(limits);

        let empty = execute(&tool, serde_json::json!({ "query": "   " }));
        assert_eq!(empty.status, AgentToolStatus::Failed);
        assert_eq!(empty.output["error"], "empty_query");
        let large_query = execute(&tool, serde_json::json!({ "query": "abcde" }));
        assert_eq!(large_query.output["error"], "query_too_large");
        let large_cursor = execute(
            &tool,
            serde_json::json!({ "query": "abcd", "cursor": "1234" }),
        );
        assert_eq!(large_cursor.output["error"], "cursor_too_large");
        assert!(backend.calls.lock().expect("calls lock").is_empty());
    }

    #[test]
    fn continuation_and_scan_metadata_are_returned_without_principal_data() {
        let mut page = SessionHistorySearchPage::continued(vec![one_match()], "opaque-next");
        page.consistency = SessionHistorySearchConsistency::StableSnapshot;
        page.scanned = SessionHistorySearchStats {
            activities: 2,
            items: 30,
            bytes: 800,
            omitted_items: 0,
        };
        let backend = Arc::new(RecordingBackend::returning(Ok(page)));
        let tool = SessionHistorySearchTool::new(
            SessionHistorySearchPrincipal::new("coding-agent").expect("principal"),
            backend,
        );

        let result = execute(&tool, serde_json::json!({ "query": "artifact" }));

        assert_eq!(result.status, AgentToolStatus::Succeeded);
        assert_eq!(result.output["next_cursor"], "opaque-next");
        assert_eq!(result.output["complete"], false);
        assert_eq!(result.output["consistency"], "stable_snapshot");
        assert_eq!(result.output["scanned"]["items"], 30);
        assert!(result.output.get("principal").is_none());
    }

    #[test]
    fn backend_cannot_escape_result_or_continuation_bounds() {
        let backend = Arc::new(RecordingBackend::returning(Ok(
            SessionHistorySearchPage::complete(vec![one_match(), one_match()]),
        )));
        let principal = SessionHistorySearchPrincipal::new("main-agent").expect("principal");
        let tool = SessionHistorySearchTool::new(principal, backend);
        let too_many = execute(
            &tool,
            serde_json::json!({ "query": "deployment", "limit": 1 }),
        );
        assert_eq!(too_many.status, AgentToolStatus::Failed);
        assert_eq!(too_many.output["error"], "backend_contract_violation");

        let backend = Arc::new(RecordingBackend::returning(Ok(SessionHistorySearchPage {
            matches: Vec::new(),
            next_cursor: None,
            complete: false,
            partial: false,
            consistency: SessionHistorySearchConsistency::BestEffort,
            scanned: SessionHistorySearchStats::default(),
        })));
        let tool = SessionHistorySearchTool::new(
            SessionHistorySearchPrincipal::new("main-agent").expect("principal"),
            backend,
        );
        let cursorless = execute(&tool, serde_json::json!({ "query": "deployment" }));
        assert_eq!(cursorless.status, AgentToolStatus::Failed);
        assert_eq!(cursorless.output["error"], "backend_contract_violation");

        let backend = Arc::new(RecordingBackend::returning(Ok(
            SessionHistorySearchPage::continued(Vec::new(), "same-cursor"),
        )));
        let tool = SessionHistorySearchTool::new(
            SessionHistorySearchPrincipal::new("main-agent").expect("principal"),
            backend,
        );
        let stalled = execute(
            &tool,
            serde_json::json!({
                "query": "deployment",
                "cursor": "same-cursor",
            }),
        );
        assert_eq!(stalled.status, AgentToolStatus::Failed);
        assert_eq!(stalled.output["error"], "backend_contract_violation");
    }

    #[test]
    fn backend_access_denial_is_blocked_without_scope_details() {
        let backend = Arc::new(RecordingBackend::returning(Err(
            SessionHistorySearchError::AccessDenied,
        )));
        let tool = SessionHistorySearchTool::new(
            SessionHistorySearchPrincipal::new("main-agent").expect("principal"),
            backend,
        );

        let result = execute(&tool, serde_json::json!({ "query": "secret" }));

        assert_eq!(result.status, AgentToolStatus::Blocked);
        assert_eq!(
            result.output,
            serde_json::json!({ "error": "access_denied" })
        );
    }
}
