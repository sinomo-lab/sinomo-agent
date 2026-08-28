use super::{
    AgentTool, AgentToolCapability, AgentToolDescriptor, AgentToolEvidence,
    AgentToolInvocationMode, AgentToolRequest, AgentToolResult,
};
use crate::{AgentCancellationToken, AgentToolCall, AgentToolKind, AgentToolSpec};
use std::fmt;
use std::sync::Arc;

pub const MAX_MEMORY_NOTE_PATH_BYTES: usize = 1_024;
pub const MAX_MEMORY_NOTE_PATH_SEGMENT_BYTES: usize = 255;
pub const MAX_MEMORY_NOTE_QUERY_BYTES: usize = 4 * 1_024;
pub const MAX_MEMORY_NOTE_SEARCH_RESULTS: usize = 100;
pub const MAX_MEMORY_NOTE_SEARCH_OUTPUT_BYTES: usize = 256 * 1_024;
pub const MAX_MEMORY_NOTE_SEARCH_SNIPPET_BYTES: usize = 4 * 1_024;
pub const MAX_MEMORY_NOTE_READ_BYTES: usize = 1024 * 1024;
pub const MAX_MEMORY_NOTE_WRITE_BYTES: usize = 1024 * 1024;
pub const MAX_MEMORY_NOTE_VERSION_BYTES: usize = 1_024;
pub const MAX_MEMORY_NOTE_SEARCH_CURSOR_BYTES: usize = 128 * 1_024;

const DEFAULT_MEMORY_NOTE_SEARCH_RESULTS: usize = 20;
const DEFAULT_MEMORY_NOTE_READ_BYTES: usize = 128 * 1_024;
// The cursor, maximum escaped query, one match's path/version metadata, and the
// JSON envelope must always fit before any snippet bytes are admitted.
const MIN_MEMORY_NOTE_SEARCH_OUTPUT_BYTES: usize = 160 * 1_024;

/// A normalized path relative to one already-authorized agent memory-note root.
///
/// Paths are deliberately logical rather than host paths. They cannot be
/// absolute, contain parent traversal or platform separators, and always name
/// a lowercase `.md` file. A backend therefore never receives an ambient path
/// chosen by the model.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MemoryNotePath(String);

impl MemoryNotePath {
    pub fn new(value: impl Into<String>) -> Result<Self, MemoryNotePathError> {
        let value = value.into();
        validate_memory_note_path(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MemoryNotePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryNotePathError {
    detail: String,
}

impl MemoryNotePathError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for MemoryNotePathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for MemoryNotePathError {}

/// Opaque optimistic-concurrency token supplied by the storage backend.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MemoryNoteVersion(String);

impl MemoryNoteVersion {
    pub fn new(value: impl Into<String>) -> Result<Self, MemoryNoteVersionError> {
        let value = value.into();
        validate_memory_note_version(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MemoryNoteVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryNoteVersionError {
    detail: String,
}

impl MemoryNoteVersionError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for MemoryNoteVersionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for MemoryNoteVersionError {}

/// Opaque backend continuation for a query-bound memory-note search.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MemoryNoteSearchCursor(String);

impl MemoryNoteSearchCursor {
    pub fn new(value: impl Into<String>) -> Result<Self, MemoryNoteSearchCursorError> {
        let value = value.into();
        if value.is_empty() {
            return Err(MemoryNoteSearchCursorError::new(
                "memory search cursor must not be empty",
            ));
        }
        if value.len() > MAX_MEMORY_NOTE_SEARCH_CURSOR_BYTES {
            return Err(MemoryNoteSearchCursorError::new(format!(
                "memory search cursor exceeds the {MAX_MEMORY_NOTE_SEARCH_CURSOR_BYTES}-byte limit"
            )));
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'"' | b'\\'))
        {
            return Err(MemoryNoteSearchCursorError::new(
                "memory search cursor must be an ASCII token without quotes or backslashes",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MemoryNoteSearchCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryNoteSearchCursorError {
    detail: String,
}

impl MemoryNoteSearchCursorError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl fmt::Display for MemoryNoteSearchCursorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for MemoryNoteSearchCursorError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryNoteSearchRequest {
    pub query: String,
    pub cursor: Option<MemoryNoteSearchCursor>,
    pub max_results: usize,
    pub max_output_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryNoteSearchMatch {
    pub path: MemoryNotePath,
    pub line: Option<u64>,
    pub snippet: String,
    pub version: MemoryNoteVersion,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemoryNoteSearchPage {
    pub matches: Vec<MemoryNoteSearchMatch>,
    pub next_cursor: Option<MemoryNoteSearchCursor>,
    pub truncated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryNoteReadRequest {
    pub path: MemoryNotePath,
    pub offset: u64,
    pub max_bytes: usize,
    pub expected_version: Option<MemoryNoteVersion>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryNoteDocument {
    pub path: MemoryNotePath,
    pub offset: u64,
    pub markdown: String,
    pub version: MemoryNoteVersion,
    pub truncated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemoryNoteWritePrecondition {
    /// Create a new note and fail if the path already exists.
    CreateNew,
    /// Replace a note only while its opaque backend version still matches.
    ReplaceIfVersion { expected_version: MemoryNoteVersion },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryNoteWriteRequest {
    pub path: MemoryNotePath,
    pub markdown: String,
    pub precondition: MemoryNoteWritePrecondition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryNoteWriteReceipt {
    pub path: MemoryNotePath,
    pub version: MemoryNoteVersion,
    pub created: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemoryNoteBackendError {
    Cancelled,
    NotFound {
        path: MemoryNotePath,
    },
    AlreadyExists {
        path: MemoryNotePath,
    },
    Conflict {
        path: MemoryNotePath,
        current_version: Option<MemoryNoteVersion>,
    },
    Invalid {
        detail: String,
    },
    Unavailable {
        detail: String,
    },
}

impl fmt::Display for MemoryNoteBackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("memory note operation was cancelled"),
            Self::NotFound { path } => write!(formatter, "memory note `{path}` was not found"),
            Self::AlreadyExists { path } => {
                write!(formatter, "memory note `{path}` already exists")
            }
            Self::Conflict {
                path,
                current_version,
            } => match current_version {
                Some(version) => write!(
                    formatter,
                    "memory note `{path}` changed; current version is {version}"
                ),
                None => write!(formatter, "memory note `{path}` changed"),
            },
            Self::Invalid { detail } => {
                write!(formatter, "invalid memory note operation: {detail}")
            }
            Self::Unavailable { detail } => {
                write!(formatter, "memory note backend is unavailable: {detail}")
            }
        }
    }
}

impl std::error::Error for MemoryNoteBackendError {}

/// Host-supplied storage and indexing boundary for Markdown memory notes.
///
/// The agent tool surface owns input validation, response bounds and
/// optimistic write intent. The backend owns root confinement, authorization,
/// durable storage, exact version checks and search. A backend may use an
/// index, but the tool contract does not require one.
pub trait MemoryNoteBackend: Send + Sync {
    fn search(
        &self,
        request: MemoryNoteSearchRequest,
        cancellation: &AgentCancellationToken,
    ) -> Result<MemoryNoteSearchPage, MemoryNoteBackendError>;

    fn read(
        &self,
        request: MemoryNoteReadRequest,
        cancellation: &AgentCancellationToken,
    ) -> Result<MemoryNoteDocument, MemoryNoteBackendError>;

    fn write(
        &self,
        request: MemoryNoteWriteRequest,
        cancellation: &AgentCancellationToken,
    ) -> Result<MemoryNoteWriteReceipt, MemoryNoteBackendError>;
}

#[derive(Clone)]
pub struct MemoryNoteAgentToolConfig {
    backend: Arc<dyn MemoryNoteBackend>,
    allow_writes: bool,
    max_search_results: usize,
    max_search_output_bytes: usize,
    max_read_bytes: usize,
    max_write_bytes: usize,
}

impl MemoryNoteAgentToolConfig {
    pub fn new(backend: Arc<dyn MemoryNoteBackend>) -> Self {
        Self {
            backend,
            allow_writes: true,
            max_search_results: DEFAULT_MEMORY_NOTE_SEARCH_RESULTS,
            max_search_output_bytes: MAX_MEMORY_NOTE_SEARCH_OUTPUT_BYTES,
            // Keep ordinary reads compact, but allow an explicit `max_bytes`
            // request to retrieve any note the tool itself can create.
            max_read_bytes: MAX_MEMORY_NOTE_READ_BYTES,
            max_write_bytes: MAX_MEMORY_NOTE_WRITE_BYTES,
        }
    }

    pub fn from_backend<B>(backend: B) -> Self
    where
        B: MemoryNoteBackend + 'static,
    {
        Self::new(Arc::new(backend))
    }

    pub fn allowing_writes(mut self, allow_writes: bool) -> Self {
        self.allow_writes = allow_writes;
        self
    }

    pub fn with_max_search_results(mut self, max_results: usize) -> Self {
        self.max_search_results = max_results.clamp(1, MAX_MEMORY_NOTE_SEARCH_RESULTS);
        self
    }

    pub fn with_max_search_output_bytes(mut self, max_bytes: usize) -> Self {
        self.max_search_output_bytes = max_bytes.clamp(
            MIN_MEMORY_NOTE_SEARCH_OUTPUT_BYTES,
            MAX_MEMORY_NOTE_SEARCH_OUTPUT_BYTES,
        );
        self
    }

    pub fn with_max_read_bytes(mut self, max_bytes: usize) -> Self {
        self.max_read_bytes = max_bytes.clamp(1, MAX_MEMORY_NOTE_READ_BYTES);
        self
    }

    pub fn with_max_write_bytes(mut self, max_bytes: usize) -> Self {
        self.max_write_bytes = max_bytes.clamp(1, MAX_MEMORY_NOTE_WRITE_BYTES);
        self
    }
}

#[derive(Clone)]
pub struct MemorySearchTool {
    config: MemoryNoteAgentToolConfig,
}

impl MemorySearchTool {
    pub fn new(config: MemoryNoteAgentToolConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for MemorySearchTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        memory_search_descriptor()
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        if request.cancellation_token.is_cancelled() {
            return cancelled_result("memory_search");
        }
        let Some(query) = input_raw_string(request.call, "query") else {
            return AgentToolResult::failed(
                "memory_search requires a string query",
                serde_json::json!({ "error": "missing_query" }),
            );
        };
        let query = query.trim().to_string();
        if query.len() > MAX_MEMORY_NOTE_QUERY_BYTES || query.chars().any(char::is_control) {
            return AgentToolResult::failed(
                format!(
                    "memory_search query exceeds the {MAX_MEMORY_NOTE_QUERY_BYTES}-byte limit or contains control characters"
                ),
                serde_json::json!({
                    "error": "invalid_query",
                    "max_bytes": MAX_MEMORY_NOTE_QUERY_BYTES,
                }),
            );
        }
        let cursor = match request.call.input.get("cursor") {
            None => None,
            Some(serde_json::Value::String(cursor)) => {
                match MemoryNoteSearchCursor::new(cursor.clone()) {
                    Ok(cursor) => Some(cursor),
                    Err(error) => {
                        return AgentToolResult::failed(
                            format!("invalid memory_search cursor: {error}"),
                            serde_json::json!({
                                "error": "invalid_cursor",
                                "detail": error.to_string(),
                            }),
                        );
                    }
                }
            }
            Some(_) => {
                return AgentToolResult::failed(
                    "memory_search cursor must be a string",
                    serde_json::json!({ "error": "invalid_cursor" }),
                );
            }
        };
        let requested_limit = match input_positive_usize(request.call, "max_results") {
            Ok(limit) => limit.unwrap_or(self.config.max_search_results),
            Err(detail) => {
                return AgentToolResult::failed(
                    detail.clone(),
                    serde_json::json!({ "error": "invalid_max_results", "detail": detail }),
                );
            }
        };
        let max_results = requested_limit
            .min(self.config.max_search_results)
            .min(MAX_MEMORY_NOTE_SEARCH_RESULTS);
        let mut backend_max_results = max_results;
        loop {
            let backend_request = MemoryNoteSearchRequest {
                query: query.clone(),
                cursor: cursor.clone(),
                max_results: backend_max_results,
                max_output_bytes: self.config.max_search_output_bytes,
            };
            let page = match self
                .config
                .backend
                .search(backend_request, &request.cancellation_token)
            {
                Ok(page) => page,
                Err(error) => return backend_error_result("memory_search", error),
            };
            if request.cancellation_token.is_cancelled() {
                return cancelled_result("memory_search");
            }
            if page.next_cursor.is_some() && page.next_cursor.as_ref() == cursor.as_ref() {
                return backend_contract_result(
                    "memory_search",
                    "search continuation did not advance",
                );
            }
            if page.matches.len() > backend_max_results {
                return backend_contract_result(
                    "memory_search",
                    "search backend returned more matches than requested",
                );
            }
            let next_cursor = page.next_cursor;
            let mut truncated =
                page.truncated || next_cursor.is_some() || backend_max_results < max_results;
            let mut matches = Vec::with_capacity(page.matches.len().min(backend_max_results));
            for result in page.matches.into_iter().take(backend_max_results) {
                if result.line == Some(0) {
                    return backend_contract_result(
                        "memory_search",
                        "search match line numbers must be one-based",
                    );
                }
                let (snippet, snippet_truncated) =
                    truncate_utf8(&result.snippet, MAX_MEMORY_NOTE_SEARCH_SNIPPET_BYTES);
                truncated |= snippet_truncated;
                matches.push(serde_json::json!({
                    "path": result.path.as_str(),
                    "line": result.line,
                    "snippet": snippet,
                    "version": result.version.as_str(),
                }));
            }

            if let Some((output, output_truncated)) = fit_memory_search_output(
                &query,
                &mut matches,
                truncated,
                next_cursor.as_ref(),
                self.config.max_search_output_bytes,
            ) {
                return AgentToolResult::succeeded(
                    format!("found {} memory note match(es)", matches.len()),
                    output.clone(),
                )
                .with_evidence(
                    AgentToolEvidence::availability_only(
                        true,
                        "searched the configured memory-note backend",
                    )
                    .with_output_value(&output, output_truncated),
                );
            }

            if matches.len() > 1 {
                let smaller_page = (matches.len() / 2).max(1);
                if smaller_page < backend_max_results {
                    backend_max_results = smaller_page;
                    continue;
                }
            }
            return AgentToolResult::failed(
                "memory_search response envelope exceeds its configured bound",
                serde_json::json!({
                    "error": "search_response_envelope_too_large",
                    "max_bytes": self.config.max_search_output_bytes,
                }),
            );
        }
    }
}

#[derive(Clone)]
pub struct MemoryReadTool {
    config: MemoryNoteAgentToolConfig,
}

impl MemoryReadTool {
    pub fn new(config: MemoryNoteAgentToolConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for MemoryReadTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        memory_read_descriptor()
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        if request.cancellation_token.is_cancelled() {
            return cancelled_result("memory_read");
        }
        let path = match input_memory_note_path(request.call) {
            Ok(path) => path,
            Err(result) => return result,
        };
        let requested_max_bytes = match input_positive_usize(request.call, "max_bytes") {
            Ok(limit) => {
                limit.unwrap_or(DEFAULT_MEMORY_NOTE_READ_BYTES.min(self.config.max_read_bytes))
            }
            Err(detail) => {
                return AgentToolResult::failed(
                    detail.clone(),
                    serde_json::json!({ "error": "invalid_max_bytes", "detail": detail }),
                );
            }
        };
        let max_bytes = requested_max_bytes
            .min(self.config.max_read_bytes)
            .min(MAX_MEMORY_NOTE_READ_BYTES);
        let offset = match input_nonnegative_u64(request.call, "offset") {
            Ok(offset) => offset.unwrap_or(0),
            Err(detail) => {
                return AgentToolResult::failed(
                    detail.clone(),
                    serde_json::json!({ "error": "invalid_offset", "detail": detail }),
                );
            }
        };
        let expected_version = match input_raw_string(request.call, "expected_version") {
            Some(version) => match MemoryNoteVersion::new(version) {
                Ok(version) => Some(version),
                Err(error) => {
                    return AgentToolResult::failed(
                        format!("memory_read expected_version is invalid: {error}"),
                        serde_json::json!({
                            "error": "invalid_expected_version",
                            "detail": error.to_string(),
                        }),
                    );
                }
            },
            None => None,
        };
        if offset > 0 && expected_version.is_none() {
            return AgentToolResult::failed(
                "memory_read requires expected_version when offset is nonzero",
                serde_json::json!({ "error": "missing_expected_version" }),
            );
        }
        let document = match self.config.backend.read(
            MemoryNoteReadRequest {
                path: path.clone(),
                offset,
                max_bytes,
                expected_version: expected_version.clone(),
            },
            &request.cancellation_token,
        ) {
            Ok(document) => document,
            Err(error) => return backend_error_result("memory_read", error),
        };
        if request.cancellation_token.is_cancelled() {
            return cancelled_result("memory_read");
        }
        if document.path != path {
            return backend_contract_result(
                "memory_read",
                "read response path differs from the requested note path",
            );
        }
        if document.offset != offset {
            return backend_contract_result(
                "memory_read",
                "read response offset differs from the requested byte offset",
            );
        }
        if expected_version
            .as_ref()
            .is_some_and(|expected| expected != &document.version)
        {
            return backend_contract_result(
                "memory_read",
                "read response version differs from the requested continuation version",
            );
        }
        if document.markdown.contains('\0') {
            return backend_contract_result(
                "memory_read",
                "Markdown note content must not contain NUL",
            );
        }
        let (markdown, locally_truncated) = truncate_utf8(&document.markdown, max_bytes);
        let truncated = document.truncated || locally_truncated;
        let returned_bytes = markdown.len();
        if truncated && returned_bytes == 0 {
            return backend_contract_result(
                "memory_read",
                "a truncated read must advance by at least one complete UTF-8 byte sequence",
            );
        }
        let next_offset = if truncated {
            match offset.checked_add(u64::try_from(returned_bytes).unwrap_or(u64::MAX)) {
                Some(next_offset) => Some(next_offset),
                None => {
                    return backend_contract_result(
                        "memory_read",
                        "read response byte continuation overflowed",
                    );
                }
            }
        } else {
            None
        };
        let output = serde_json::json!({
            "path": path.as_str(),
            "offset": offset,
            "markdown": markdown,
            "version": document.version.as_str(),
            "truncated": truncated,
            "returned_bytes": returned_bytes,
            "next_offset": next_offset,
        });
        AgentToolResult::succeeded(format!("read memory note `{path}`"), output.clone())
            .with_evidence(
                AgentToolEvidence::availability_only(true, "read a versioned Markdown memory note")
                    .with_output_value(&output, truncated),
            )
    }
}

#[derive(Clone)]
pub struct MemoryWriteTool {
    config: MemoryNoteAgentToolConfig,
}

impl MemoryWriteTool {
    pub fn new(config: MemoryNoteAgentToolConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for MemoryWriteTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        memory_write_descriptor()
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        if !self.config.allow_writes {
            return AgentToolResult::blocked(
                "memory_write is disabled for this agent runtime",
                serde_json::json!({ "error": "memory_writes_disabled" }),
            );
        }
        if request.cancellation_token.is_cancelled() {
            return cancelled_result("memory_write");
        }
        let path = match input_memory_note_path(request.call) {
            Ok(path) => path,
            Err(result) => return result,
        };
        let Some(markdown) = input_raw_string(request.call, "markdown") else {
            return AgentToolResult::failed(
                "memory_write requires Markdown content",
                serde_json::json!({ "error": "missing_markdown" }),
            );
        };
        if markdown.len() > self.config.max_write_bytes
            || markdown.len() > MAX_MEMORY_NOTE_WRITE_BYTES
        {
            return AgentToolResult::failed(
                format!(
                    "memory_write content exceeds the {}-byte limit",
                    self.config.max_write_bytes.min(MAX_MEMORY_NOTE_WRITE_BYTES)
                ),
                serde_json::json!({
                    "error": "note_too_large",
                    "bytes": markdown.len(),
                    "max_bytes": self.config.max_write_bytes.min(MAX_MEMORY_NOTE_WRITE_BYTES),
                }),
            );
        }
        if markdown.contains('\0') {
            return AgentToolResult::failed(
                "memory_write Markdown must not contain NUL",
                serde_json::json!({ "error": "invalid_markdown" }),
            );
        }
        let precondition = match input_raw_string(request.call, "expected_version") {
            Some(version) => match MemoryNoteVersion::new(version) {
                Ok(expected_version) => {
                    MemoryNoteWritePrecondition::ReplaceIfVersion { expected_version }
                }
                Err(error) => {
                    return AgentToolResult::failed(
                        format!("invalid expected_version: {error}"),
                        serde_json::json!({
                            "error": "invalid_expected_version",
                            "detail": error.to_string(),
                        }),
                    );
                }
            },
            None => MemoryNoteWritePrecondition::CreateNew,
        };
        let expected_created = matches!(&precondition, MemoryNoteWritePrecondition::CreateNew);
        let receipt = match self.config.backend.write(
            MemoryNoteWriteRequest {
                path: path.clone(),
                markdown,
                precondition,
            },
            &request.cancellation_token,
        ) {
            Ok(receipt) => receipt,
            Err(error) => return backend_error_result("memory_write", error),
        };
        if request.cancellation_token.is_cancelled() {
            return cancelled_result("memory_write");
        }
        if receipt.path != path || receipt.created != expected_created {
            return backend_contract_result(
                "memory_write",
                "write receipt path or create/replace outcome differs from the request",
            );
        }
        let operation = if receipt.created {
            "created"
        } else {
            "replaced"
        };
        let output = serde_json::json!({
            "status": operation,
            "path": receipt.path.as_str(),
            "version": receipt.version.as_str(),
        });
        AgentToolResult::succeeded(
            format!("{operation} memory note `{}`", receipt.path),
            output.clone(),
        )
        .with_evidence(
            AgentToolEvidence::availability_only(
                true,
                "wrote a Markdown memory note with an optimistic precondition",
            )
            .with_output_value(&output, false),
        )
    }
}

pub fn memory_note_tool_descriptors() -> Vec<AgentToolDescriptor> {
    vec![
        memory_search_descriptor(),
        memory_read_descriptor(),
        memory_write_descriptor(),
    ]
}

pub fn registry(config: MemoryNoteAgentToolConfig) -> super::AgentToolRegistry {
    let registry = super::AgentToolRegistry::new();
    register_tools(&registry, config);
    registry
}

pub fn register_tools(
    registry: &super::AgentToolRegistry,
    config: MemoryNoteAgentToolConfig,
) -> Vec<Option<AgentToolDescriptor>> {
    vec![
        registry.register(MemorySearchTool::new(config.clone())),
        registry.register(MemoryReadTool::new(config.clone())),
        registry.register(MemoryWriteTool::new(config)),
    ]
}

fn memory_search_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(AgentToolSpec::new(
        AgentToolKind::MemorySearch,
        "memory_search",
        "Search the current agent's Markdown memory notes with bounded results.",
        false,
    ))
    .with_invocation_modes(AgentToolInvocationMode::all())
    .with_toolsets(["core", "memory"])
    .with_capabilities([AgentToolCapability::ReadMemory])
    .with_input_schema(serde_json::json!({
        "type": "object",
        "required": ["query"],
        "properties": {
            "query": {
                "type": "string",
                "maxLength": MAX_MEMORY_NOTE_QUERY_BYTES,
                "description": "Literal or backend-supported text query over this agent's notes. Use an empty string to list notes; control characters are not accepted."
            },
            "max_results": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_MEMORY_NOTE_SEARCH_RESULTS,
                "description": "Maximum matches to return; the runtime may apply a smaller bound."
            },
            "cursor": {
                "type": "string",
                "maxLength": MAX_MEMORY_NOTE_SEARCH_CURSOR_BYTES,
                "description": "Opaque continuation returned by an earlier memory_search call with the same query."
            }
        },
        "additionalProperties": false
    }))
    .with_output_schema(serde_json::json!({
        "type": "object",
        "required": ["query", "count", "matches", "truncated", "next_cursor"],
        "properties": {
            "query": { "type": "string" },
            "count": { "type": "integer" },
            "truncated": { "type": "boolean" },
            "next_cursor": { "type": ["string", "null"] },
            "matches": { "type": "array" }
        }
    }))
}

fn memory_read_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(AgentToolSpec::new(
        AgentToolKind::MemoryRead,
        "memory_read",
        "Read one bounded byte range from a versioned Markdown note in the current agent's memory-note root.",
        false,
    ))
    .with_invocation_modes(AgentToolInvocationMode::all())
    .with_toolsets(["core", "memory"])
    .with_capabilities([AgentToolCapability::ReadMemory])
    .with_input_schema(serde_json::json!({
        "type": "object",
        "required": ["path"],
        "properties": {
            "path": {
                "type": "string",
                "description": "Relative lowercase .md path inside the agent memory-note root."
            },
            "max_bytes": {
                "type": "integer",
                "minimum": 1,
                "maximum": MAX_MEMORY_NOTE_READ_BYTES,
                "description": "Maximum Markdown bytes to return; the runtime may apply a smaller bound."
            },
            "offset": {
                "type": "integer",
                "minimum": 0,
                "description": "Byte offset returned as next_offset by an earlier read; defaults to zero."
            },
            "expected_version": {
                "type": "string",
                "maxLength": MAX_MEMORY_NOTE_VERSION_BYTES,
                "description": "Version returned by the first page. Include it with a nonzero offset so pages cannot mix note versions."
            }
        },
        "additionalProperties": false
    }))
    .with_output_schema(serde_json::json!({
        "type": "object",
        "required": ["path", "offset", "markdown", "version", "truncated", "returned_bytes", "next_offset"],
        "properties": {
            "path": { "type": "string" },
            "offset": { "type": "integer" },
            "markdown": { "type": "string" },
            "version": { "type": "string" },
            "truncated": { "type": "boolean" },
            "returned_bytes": { "type": "integer" },
            "next_offset": { "type": ["integer", "null"] }
        }
    }))
}

fn memory_write_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(AgentToolSpec::new(
        AgentToolKind::MemoryWrite,
        "memory_write",
        "Create or replace one tool-bounded complete Markdown note with an exact version precondition.",
        true,
    ))
    .with_invocation_modes(AgentToolInvocationMode::all())
    .with_toolsets(["core", "memory"])
    .with_capabilities([AgentToolCapability::WriteMemory])
    .with_input_schema(serde_json::json!({
        "type": "object",
        "required": ["path", "markdown"],
        "properties": {
            "path": {
                "type": "string",
                "description": "Relative lowercase .md path inside the agent memory-note root."
            },
            "markdown": {
                "type": "string",
                "maxLength": MAX_MEMORY_NOTE_WRITE_BYTES,
                "description": "Complete Markdown content for this create/replace operation. The current tool-managed operation cap is 1 MiB; it is not a SIFS storage or memory_read size limit."
            },
            "expected_version": {
                "type": "string",
                "description": "Version returned by memory_read. Omit only for exclusive creation; include it for compare-and-replace. Do not reconstruct an existing note from a truncated read unless dropping the unread tail is intentional."
            }
        },
        "additionalProperties": false
    }))
    .with_output_schema(serde_json::json!({
        "type": "object",
        "required": ["status", "path", "version"],
        "properties": {
            "status": { "type": "string", "enum": ["created", "replaced"] },
            "path": { "type": "string" },
            "version": { "type": "string" }
        }
    }))
}

fn input_memory_note_path(call: &AgentToolCall) -> Result<MemoryNotePath, AgentToolResult> {
    let Some(path) = input_raw_string(call, "path") else {
        return Err(AgentToolResult::failed(
            format!("{} requires a string path", call.tool),
            serde_json::json!({ "error": "missing_path" }),
        ));
    };
    MemoryNotePath::new(path).map_err(|error| {
        AgentToolResult::blocked(
            format!("memory note path is not allowed: {error}"),
            serde_json::json!({
                "error": "path_not_allowed",
                "detail": error.to_string(),
            }),
        )
        .with_evidence(AgentToolEvidence::availability_only(
            false,
            "path did not resolve to a safe relative Markdown note",
        ))
    })
}

fn validate_memory_note_path(path: &str) -> Result<(), MemoryNotePathError> {
    if path.is_empty() {
        return Err(MemoryNotePathError::new("path must not be empty"));
    }
    if path != path.trim() {
        return Err(MemoryNotePathError::new(
            "path must not have leading or trailing whitespace",
        ));
    }
    if path.len() > MAX_MEMORY_NOTE_PATH_BYTES {
        return Err(MemoryNotePathError::new(format!(
            "path exceeds the {MAX_MEMORY_NOTE_PATH_BYTES}-byte limit"
        )));
    }
    if path.starts_with('/') || path.starts_with('\\') {
        return Err(MemoryNotePathError::new("path must be relative"));
    }
    if path.contains('\\') || path.contains(':') {
        return Err(MemoryNotePathError::new(
            "path must use logical '/' separators and must not contain drive prefixes",
        ));
    }
    if path.chars().any(char::is_control) {
        return Err(MemoryNotePathError::new(
            "path must not contain control characters",
        ));
    }
    let segments = path.split('/').collect::<Vec<_>>();
    if segments.iter().any(|segment| segment.is_empty()) {
        return Err(MemoryNotePathError::new(
            "path must not contain empty segments",
        ));
    }
    for segment in &segments {
        if matches!(*segment, "." | "..") {
            return Err(MemoryNotePathError::new(
                "path must not contain dot or parent segments",
            ));
        }
        if segment.len() > MAX_MEMORY_NOTE_PATH_SEGMENT_BYTES {
            return Err(MemoryNotePathError::new(format!(
                "path segment exceeds the {MAX_MEMORY_NOTE_PATH_SEGMENT_BYTES}-byte limit"
            )));
        }
    }
    let file_name = segments.last().expect("non-empty segments");
    let Some(stem) = file_name.strip_suffix(".md") else {
        return Err(MemoryNotePathError::new("path must end in lowercase .md"));
    };
    if stem.is_empty() || stem.chars().all(|character| character == '.') {
        return Err(MemoryNotePathError::new(
            "Markdown note file name must have a non-empty stem",
        ));
    }
    Ok(())
}

fn validate_memory_note_version(version: &str) -> Result<(), MemoryNoteVersionError> {
    if version.is_empty() {
        return Err(MemoryNoteVersionError::new(
            "memory note version must not be empty",
        ));
    }
    if version.len() > MAX_MEMORY_NOTE_VERSION_BYTES {
        return Err(MemoryNoteVersionError::new(format!(
            "memory note version exceeds the {MAX_MEMORY_NOTE_VERSION_BYTES}-byte limit"
        )));
    }
    if version.chars().any(char::is_control) {
        return Err(MemoryNoteVersionError::new(
            "memory note version must not contain control characters",
        ));
    }
    Ok(())
}

fn input_raw_string(call: &AgentToolCall, key: &str) -> Option<String> {
    call.input
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn input_positive_usize(call: &AgentToolCall, key: &str) -> Result<Option<usize>, String> {
    let Some(value) = call.input.get(key) else {
        return Ok(None);
    };
    let Some(value) = value.as_u64() else {
        return Err(format!("{key} must be a positive integer"));
    };
    if value == 0 {
        return Err(format!("{key} must be greater than zero"));
    }
    usize::try_from(value)
        .map(Some)
        .map_err(|_| format!("{key} does not fit this platform"))
}

fn input_nonnegative_u64(call: &AgentToolCall, key: &str) -> Result<Option<u64>, String> {
    let Some(value) = call.input.get(key) else {
        return Ok(None);
    };
    value
        .as_u64()
        .map(Some)
        .ok_or_else(|| format!("{key} must be a non-negative integer"))
}

fn truncate_utf8(value: &str, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        return (value.to_string(), false);
    }
    let mut end = max_bytes.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    (value[..end].to_string(), true)
}

fn memory_search_output(
    query: &str,
    matches: &[serde_json::Value],
    truncated: bool,
    next_cursor: Option<&MemoryNoteSearchCursor>,
) -> serde_json::Value {
    serde_json::json!({
        "query": query,
        "count": matches.len(),
        "matches": matches,
        "truncated": truncated,
        "next_cursor": next_cursor.map(MemoryNoteSearchCursor::as_str),
    })
}

fn fit_memory_search_output(
    query: &str,
    matches: &mut [serde_json::Value],
    truncated: bool,
    next_cursor: Option<&MemoryNoteSearchCursor>,
    max_bytes: usize,
) -> Option<(serde_json::Value, bool)> {
    let output = memory_search_output(query, matches, truncated, next_cursor);
    if serialized_json_len(&output) <= max_bytes {
        return Some((output, truncated));
    }
    if matches.len() != 1 {
        return None;
    }

    let original = matches[0].get("snippet")?.as_str()?.to_string();
    matches[0]["snippet"] = serde_json::Value::String(String::new());
    let empty = memory_search_output(query, matches, true, next_cursor);
    if serialized_json_len(&empty) > max_bytes {
        return None;
    }

    let mut boundaries = original
        .char_indices()
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if boundaries.last().copied() != Some(original.len()) {
        boundaries.push(original.len());
    }
    let mut best_end = 0;
    let mut low = 1_usize;
    let mut high = boundaries.len();
    while low < high {
        let middle = low + (high - low) / 2;
        let end = boundaries[middle];
        matches[0]["snippet"] = serde_json::Value::String(original[..end].to_string());
        let candidate = memory_search_output(query, matches, true, next_cursor);
        if serialized_json_len(&candidate) <= max_bytes {
            best_end = end;
            low = middle.saturating_add(1);
        } else {
            high = middle;
        }
    }
    if low < boundaries.len() {
        let end = boundaries[low];
        matches[0]["snippet"] = serde_json::Value::String(original[..end].to_string());
        let candidate = memory_search_output(query, matches, true, next_cursor);
        if serialized_json_len(&candidate) <= max_bytes {
            best_end = end;
        }
    }
    matches[0]["snippet"] = serde_json::Value::String(original[..best_end].to_string());
    let output = memory_search_output(query, matches, true, next_cursor);
    (serialized_json_len(&output) <= max_bytes).then_some((output, true))
}

fn serialized_json_len(value: &serde_json::Value) -> usize {
    serde_json::to_vec(value)
        .map(|encoded| encoded.len())
        .unwrap_or(usize::MAX)
}

fn cancelled_result(tool: &str) -> AgentToolResult {
    AgentToolResult::cancelled(
        format!("{tool} cancelled"),
        serde_json::json!({ "error": "tool_cancelled", "tool": tool }),
    )
}

fn backend_contract_result(tool: &str, detail: &str) -> AgentToolResult {
    AgentToolResult::failed(
        format!("{tool} backend returned an invalid response: {detail}"),
        serde_json::json!({
            "error": "backend_contract_violation",
            "detail": detail,
        }),
    )
}

fn backend_error_result(tool: &str, error: MemoryNoteBackendError) -> AgentToolResult {
    let (code, summary, path, current_version) = match &error {
        MemoryNoteBackendError::Cancelled => return cancelled_result(tool),
        MemoryNoteBackendError::NotFound { path } => (
            "note_not_found",
            format!("{tool} failed: memory note `{path}` was not found"),
            Some(path.as_str()),
            None,
        ),
        MemoryNoteBackendError::AlreadyExists { path } => (
            "note_already_exists",
            format!("{tool} failed: memory note `{path}` already exists"),
            Some(path.as_str()),
            None,
        ),
        MemoryNoteBackendError::Conflict {
            path,
            current_version,
        } => (
            "note_version_conflict",
            format!("{tool} failed: memory note `{path}` changed"),
            Some(path.as_str()),
            current_version.as_ref().map(MemoryNoteVersion::as_str),
        ),
        MemoryNoteBackendError::Invalid { .. } => (
            "invalid_note_operation",
            format!("{tool} failed: memory note operation is invalid"),
            None,
            None,
        ),
        MemoryNoteBackendError::Unavailable { .. } => (
            "memory_backend_unavailable",
            format!("{tool} failed: memory note storage is temporarily unavailable"),
            None,
            None,
        ),
    };
    AgentToolResult::failed(
        summary,
        serde_json::json!({
            "error": code,
            "path": path,
            "current_version": current_version,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentToolStatus, InteractionSession, InteractionSurface, InteractionSurfaceKind};
    use sinomo_runtime::events::SessionId;
    use sinomo_runtime::node::NodeId;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Default)]
    struct FakeMemoryNoteBackend {
        search_requests: Mutex<Vec<MemoryNoteSearchRequest>>,
        honor_search_limit: AtomicBool,
        read_requests: Mutex<Vec<MemoryNoteReadRequest>>,
        write_requests: Mutex<Vec<MemoryNoteWriteRequest>>,
        search_response: Mutex<Option<Result<MemoryNoteSearchPage, MemoryNoteBackendError>>>,
        read_response: Mutex<Option<Result<MemoryNoteDocument, MemoryNoteBackendError>>>,
        write_response: Mutex<Option<Result<MemoryNoteWriteReceipt, MemoryNoteBackendError>>>,
    }

    impl FakeMemoryNoteBackend {
        fn version(value: &str) -> MemoryNoteVersion {
            MemoryNoteVersion::new(value).expect("valid test version")
        }

        fn path(value: &str) -> MemoryNotePath {
            MemoryNotePath::new(value).expect("valid test path")
        }
    }

    impl MemoryNoteBackend for FakeMemoryNoteBackend {
        fn search(
            &self,
            request: MemoryNoteSearchRequest,
            _cancellation: &AgentCancellationToken,
        ) -> Result<MemoryNoteSearchPage, MemoryNoteBackendError> {
            let max_results = request.max_results;
            self.search_requests
                .lock()
                .expect("search requests")
                .push(request);
            let mut response = self
                .search_response
                .lock()
                .expect("search response")
                .clone()
                .unwrap_or_else(|| Ok(MemoryNoteSearchPage::default()));
            if self.honor_search_limit.load(Ordering::Relaxed)
                && let Ok(page) = response.as_mut()
                && page.matches.len() > max_results
            {
                page.matches.truncate(max_results);
                page.truncated = true;
            }
            response
        }

        fn read(
            &self,
            request: MemoryNoteReadRequest,
            _cancellation: &AgentCancellationToken,
        ) -> Result<MemoryNoteDocument, MemoryNoteBackendError> {
            self.read_requests
                .lock()
                .expect("read requests")
                .push(request.clone());
            self.read_response
                .lock()
                .expect("read response")
                .clone()
                .unwrap_or_else(|| {
                    Ok(MemoryNoteDocument {
                        path: request.path,
                        offset: request.offset,
                        markdown: String::new(),
                        version: Self::version("v1"),
                        truncated: false,
                    })
                })
        }

        fn write(
            &self,
            request: MemoryNoteWriteRequest,
            _cancellation: &AgentCancellationToken,
        ) -> Result<MemoryNoteWriteReceipt, MemoryNoteBackendError> {
            self.write_requests
                .lock()
                .expect("write requests")
                .push(request.clone());
            self.write_response
                .lock()
                .expect("write response")
                .clone()
                .unwrap_or_else(|| {
                    Ok(MemoryNoteWriteReceipt {
                        path: request.path,
                        version: Self::version("v2"),
                        created: matches!(
                            &request.precondition,
                            MemoryNoteWritePrecondition::CreateNew
                        ),
                    })
                })
        }
    }

    fn session() -> InteractionSession {
        InteractionSession::new(
            SessionId::from_legacy_named("memory-note-session").expect("session"),
            NodeId::from_legacy_named("memory-note-node").expect("node"),
            InteractionSurface::new("test", InteractionSurfaceKind::CommandLine),
        )
    }

    fn execute(
        registry: &super::super::AgentToolRegistry,
        tool: &str,
        input: serde_json::Value,
    ) -> crate::AgentToolOutcome {
        registry.execute(
            &session(),
            0,
            0,
            &AgentToolCall {
                call_id: None,
                tool: tool.to_string(),
                input,
                r#async: false,
                summary: tool.to_string(),
            },
        )
    }

    #[test]
    fn memory_note_path_accepts_only_safe_relative_lowercase_markdown_paths() {
        assert_eq!(
            MemoryNotePath::new("topics/runtime.md")
                .expect("safe path")
                .as_str(),
            "topics/runtime.md"
        );
        for invalid in [
            "",
            "/absolute.md",
            "../outside.md",
            "topics/../outside.md",
            "topics\\outside.md",
            "C:/outside.md",
            "topics//note.md",
            "note.MD",
            "note.txt",
            ".md",
        ] {
            assert!(
                MemoryNotePath::new(invalid).is_err(),
                "path {invalid:?} must be rejected"
            );
        }
    }

    #[test]
    fn memory_search_cursor_is_bounded_and_needs_no_json_escaping() {
        assert!(
            MemoryNoteSearchCursor::new("c".repeat(MAX_MEMORY_NOTE_SEARCH_CURSOR_BYTES)).is_ok()
        );
        assert!(
            MemoryNoteSearchCursor::new("c".repeat(MAX_MEMORY_NOTE_SEARCH_CURSOR_BYTES + 1))
                .is_err()
        );
        for invalid in ["cursor with spaces", "cursor\"quote", "cursor\\slash"] {
            assert!(MemoryNoteSearchCursor::new(invalid).is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn descriptors_are_bound_to_explicit_memory_capabilities() {
        let descriptors = memory_note_tool_descriptors();
        let names = descriptors
            .iter()
            .map(|descriptor| descriptor.spec.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["memory_search", "memory_read", "memory_write"]);
        assert_eq!(
            descriptors[0].capabilities,
            vec![AgentToolCapability::ReadMemory]
        );
        assert_eq!(
            descriptors[1].capabilities,
            vec![AgentToolCapability::ReadMemory]
        );
        assert_eq!(
            descriptors[2].capabilities,
            vec![AgentToolCapability::WriteMemory]
        );
        assert!(descriptors.iter().all(|descriptor| {
            descriptor.toolsets == ["core".to_string(), "memory".to_string()]
        }));
    }

    #[test]
    fn memory_search_rejects_backend_result_overflow_without_losing_a_continuation() {
        let backend = Arc::new(FakeMemoryNoteBackend::default());
        let matches = (0..5)
            .map(|index| MemoryNoteSearchMatch {
                path: FakeMemoryNoteBackend::path(&format!("notes/{index}.md")),
                line: Some(1),
                snippet: "é".repeat(MAX_MEMORY_NOTE_SEARCH_SNIPPET_BYTES),
                version: FakeMemoryNoteBackend::version("v1"),
            })
            .collect();
        *backend.search_response.lock().expect("search response") =
            Some(Ok(MemoryNoteSearchPage {
                matches,
                next_cursor: None,
                truncated: false,
            }));
        let config = MemoryNoteAgentToolConfig::new(backend.clone()).with_max_search_results(2);
        let registry = registry(config);

        let outcome = execute(
            &registry,
            "memory_search",
            serde_json::json!({ "query": "runtime", "max_results": 99 }),
        );

        assert_eq!(outcome.status, AgentToolStatus::Failed);
        assert_eq!(outcome.output["error"], "backend_contract_violation");
        assert_eq!(
            backend.search_requests.lock().expect("requests")[0].max_results,
            2
        );
    }

    #[test]
    fn memory_search_accepts_an_empty_query_for_note_discovery() {
        let backend = Arc::new(FakeMemoryNoteBackend::default());
        *backend.search_response.lock().expect("search response") =
            Some(Ok(MemoryNoteSearchPage::default()));
        let registry = registry(MemoryNoteAgentToolConfig::new(backend.clone()));

        let outcome = execute(
            &registry,
            "memory_search",
            serde_json::json!({ "query": "" }),
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(
            backend.search_requests.lock().expect("requests")[0].query,
            ""
        );
    }

    #[test]
    fn memory_search_passes_through_opaque_continuations() {
        let backend = Arc::new(FakeMemoryNoteBackend::default());
        *backend.search_response.lock().expect("search response") =
            Some(Ok(MemoryNoteSearchPage {
                matches: Vec::new(),
                next_cursor: Some(
                    MemoryNoteSearchCursor::new("backend-next").expect("next cursor"),
                ),
                truncated: false,
            }));
        let registry = registry(MemoryNoteAgentToolConfig::new(backend.clone()));

        let outcome = execute(
            &registry,
            "memory_search",
            serde_json::json!({ "query": "runtime", "cursor": "backend-previous" }),
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(outcome.output["next_cursor"], "backend-next");
        assert_eq!(outcome.output["truncated"], true);
        assert_eq!(
            backend.search_requests.lock().expect("requests")[0]
                .cursor
                .as_ref()
                .map(MemoryNoteSearchCursor::as_str),
            Some("backend-previous")
        );
    }

    #[test]
    fn memory_search_replays_and_fits_escaped_results_with_a_maximum_cursor() {
        let backend = Arc::new(FakeMemoryNoteBackend::default());
        backend.honor_search_limit.store(true, Ordering::Relaxed);
        let escape_heavy = "\u{1}".repeat(MAX_MEMORY_NOTE_SEARCH_SNIPPET_BYTES);
        let matches = ["one.md", "two.md"]
            .into_iter()
            .map(|path| MemoryNoteSearchMatch {
                path: FakeMemoryNoteBackend::path(path),
                line: Some(1),
                snippet: escape_heavy.clone(),
                version: FakeMemoryNoteBackend::version("v1"),
            })
            .collect();
        let next_cursor = "c".repeat(MAX_MEMORY_NOTE_SEARCH_CURSOR_BYTES);
        *backend.search_response.lock().expect("search response") =
            Some(Ok(MemoryNoteSearchPage {
                matches,
                next_cursor: Some(
                    MemoryNoteSearchCursor::new(next_cursor.clone()).expect("maximum cursor"),
                ),
                truncated: false,
            }));
        let registry = registry(
            MemoryNoteAgentToolConfig::new(backend.clone())
                .with_max_search_results(2)
                .with_max_search_output_bytes(MIN_MEMORY_NOTE_SEARCH_OUTPUT_BYTES),
        );
        let query = "\\\"".repeat(MAX_MEMORY_NOTE_QUERY_BYTES / 2);

        let outcome = execute(
            &registry,
            "memory_search",
            serde_json::json!({
                "query": query,
                "cursor": "previous",
                "max_results": 2,
            }),
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(outcome.output["count"], 1);
        assert_eq!(outcome.output["next_cursor"], next_cursor);
        assert_eq!(outcome.output["truncated"], true);
        assert!(
            outcome.output["matches"][0]["snippet"]
                .as_str()
                .expect("snippet")
                .len()
                < escape_heavy.len()
        );
        assert!(
            serde_json::to_vec(&outcome.output)
                .expect("serialize output")
                .len()
                <= MIN_MEMORY_NOTE_SEARCH_OUTPUT_BYTES
        );
        let requests = backend.search_requests.lock().expect("requests");
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].max_results, 2);
        assert_eq!(requests[1].max_results, 1);
    }

    #[test]
    fn memory_read_is_path_scoped_versioned_and_byte_bounded() {
        let backend = Arc::new(FakeMemoryNoteBackend::default());
        *backend.read_response.lock().expect("read response") = Some(Ok(MemoryNoteDocument {
            path: FakeMemoryNoteBackend::path("notes/runtime.md"),
            offset: 0,
            markdown: "ééé".to_string(),
            version: FakeMemoryNoteBackend::version("object-1/version-4"),
            truncated: false,
        }));
        let registry = registry(MemoryNoteAgentToolConfig::new(backend.clone()));

        let outcome = execute(
            &registry,
            "memory_read",
            serde_json::json!({ "path": "notes/runtime.md", "max_bytes": 5 }),
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(outcome.output["markdown"], "éé");
        assert_eq!(outcome.output["returned_bytes"], 4);
        assert_eq!(outcome.output["truncated"], true);
        assert_eq!(outcome.output["offset"], 0);
        assert_eq!(outcome.output["next_offset"], 4);
        assert_eq!(outcome.output["version"], "object-1/version-4");
        let requests = backend.read_requests.lock().expect("requests");
        assert_eq!(requests[0].max_bytes, 5);
        assert_eq!(requests[0].offset, 0);
        assert_eq!(requests[0].expected_version, None);
    }

    #[test]
    fn memory_read_can_explicitly_exceed_the_compact_default_page() {
        let backend = Arc::new(FakeMemoryNoteBackend::default());
        *backend.read_response.lock().expect("read response") = Some(Ok(MemoryNoteDocument {
            path: FakeMemoryNoteBackend::path("notes/large.md"),
            offset: 0,
            markdown: "large note".to_string(),
            version: FakeMemoryNoteBackend::version("v1"),
            truncated: false,
        }));
        let registry = registry(MemoryNoteAgentToolConfig::new(backend.clone()));
        let requested = DEFAULT_MEMORY_NOTE_READ_BYTES + 1;

        let outcome = execute(
            &registry,
            "memory_read",
            serde_json::json!({ "path": "notes/large.md", "max_bytes": requested }),
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(
            backend.read_requests.lock().expect("requests")[0].max_bytes,
            requested
        );
    }

    #[test]
    fn memory_read_pages_with_an_exact_version_and_advancing_offset() {
        let backend = Arc::new(FakeMemoryNoteBackend::default());
        let version = FakeMemoryNoteBackend::version("object-1/version-4");
        *backend.read_response.lock().expect("read response") = Some(Ok(MemoryNoteDocument {
            path: FakeMemoryNoteBackend::path("notes/large.md"),
            offset: MAX_MEMORY_NOTE_READ_BYTES as u64,
            markdown: "tail after one mebibyte".to_string(),
            version: version.clone(),
            truncated: false,
        }));
        let registry = registry(MemoryNoteAgentToolConfig::new(backend.clone()));

        let outcome = execute(
            &registry,
            "memory_read",
            serde_json::json!({
                "path": "notes/large.md",
                "offset": MAX_MEMORY_NOTE_READ_BYTES,
                "expected_version": version.as_str(),
                "max_bytes": 1024,
            }),
        );

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(outcome.output["offset"], MAX_MEMORY_NOTE_READ_BYTES);
        assert_eq!(outcome.output["next_offset"], serde_json::Value::Null);
        assert_eq!(outcome.output["markdown"], "tail after one mebibyte");
        let requests = backend.read_requests.lock().expect("requests");
        assert_eq!(requests[0].offset, MAX_MEMORY_NOTE_READ_BYTES as u64);
        assert_eq!(requests[0].expected_version.as_ref(), Some(&version));

        drop(requests);
        let missing_version = execute(
            &registry,
            "memory_read",
            serde_json::json!({ "path": "notes/large.md", "offset": 1 }),
        );
        assert_eq!(missing_version.status, AgentToolStatus::Failed);
        assert_eq!(missing_version.output["error"], "missing_expected_version");
    }

    #[test]
    fn memory_read_rejects_a_nonadvancing_truncated_backend_page() {
        let backend = Arc::new(FakeMemoryNoteBackend::default());
        *backend.read_response.lock().expect("read response") = Some(Ok(MemoryNoteDocument {
            path: FakeMemoryNoteBackend::path("notes/unicode.md"),
            offset: 0,
            markdown: String::new(),
            version: FakeMemoryNoteBackend::version("v1"),
            truncated: true,
        }));
        let registry = registry(MemoryNoteAgentToolConfig::new(backend));

        let outcome = execute(
            &registry,
            "memory_read",
            serde_json::json!({ "path": "notes/unicode.md", "max_bytes": 1 }),
        );

        assert_eq!(outcome.status, AgentToolStatus::Failed);
        assert_eq!(outcome.output["error"], "backend_contract_violation");
    }

    #[test]
    fn memory_write_uses_exclusive_create_and_versioned_replace() {
        let backend = Arc::new(FakeMemoryNoteBackend::default());
        let registry = registry(MemoryNoteAgentToolConfig::new(backend.clone()));

        let created = execute(
            &registry,
            "memory_write",
            serde_json::json!({
                "path": "notes/new.md",
                "markdown": "# New\n",
            }),
        );
        let replaced = execute(
            &registry,
            "memory_write",
            serde_json::json!({
                "path": "notes/new.md",
                "markdown": "# Updated\n",
                "expected_version": "v2",
            }),
        );

        assert_eq!(created.status, AgentToolStatus::Succeeded);
        assert_eq!(created.output["status"], "created");
        assert_eq!(replaced.status, AgentToolStatus::Succeeded);
        assert_eq!(replaced.output["status"], "replaced");
        let requests = backend.write_requests.lock().expect("requests");
        assert!(matches!(
            &requests[0].precondition,
            MemoryNoteWritePrecondition::CreateNew
        ));
        assert!(matches!(
            &requests[1].precondition,
            MemoryNoteWritePrecondition::ReplaceIfVersion { expected_version }
                if expected_version.as_str() == "v2"
        ));
    }

    #[test]
    fn memory_write_surfaces_conflict_without_retrying_or_overwriting() {
        let backend = Arc::new(FakeMemoryNoteBackend::default());
        let path = FakeMemoryNoteBackend::path("notes/conflict.md");
        *backend.write_response.lock().expect("write response") =
            Some(Err(MemoryNoteBackendError::Conflict {
                path,
                current_version: Some(FakeMemoryNoteBackend::version("v3")),
            }));
        let registry = registry(MemoryNoteAgentToolConfig::new(backend.clone()));

        let outcome = execute(
            &registry,
            "memory_write",
            serde_json::json!({
                "path": "notes/conflict.md",
                "markdown": "replacement",
                "expected_version": "v2",
            }),
        );

        assert_eq!(outcome.status, AgentToolStatus::Failed);
        assert_eq!(outcome.output["error"], "note_version_conflict");
        assert_eq!(outcome.output["path"], "notes/conflict.md");
        assert_eq!(outcome.output["current_version"], "v3");
        assert!(outcome.output.get("detail").is_none());
        assert_eq!(backend.write_requests.lock().expect("requests").len(), 1);
    }

    #[test]
    fn memory_backend_internal_failures_are_not_model_visible() {
        const PRIVATE: &str =
            "BACKEND_IO_SENTINEL /home/ix/private provider-session=PRIVATE_SESSION_SENTINEL";

        let search_backend = Arc::new(FakeMemoryNoteBackend::default());
        *search_backend
            .search_response
            .lock()
            .expect("search response") = Some(Err(MemoryNoteBackendError::Unavailable {
            detail: PRIVATE.to_string(),
        }));
        let search = execute(
            &registry(MemoryNoteAgentToolConfig::new(search_backend)),
            "memory_search",
            serde_json::json!({ "query": "runtime" }),
        );

        let read_backend = Arc::new(FakeMemoryNoteBackend::default());
        *read_backend.read_response.lock().expect("read response") =
            Some(Err(MemoryNoteBackendError::Invalid {
                detail: PRIVATE.to_string(),
            }));
        let read = execute(
            &registry(MemoryNoteAgentToolConfig::new(read_backend)),
            "memory_read",
            serde_json::json!({ "path": "notes/runtime.md" }),
        );

        let write_backend = Arc::new(FakeMemoryNoteBackend::default());
        *write_backend.write_response.lock().expect("write response") =
            Some(Err(MemoryNoteBackendError::Unavailable {
                detail: PRIVATE.to_string(),
            }));
        let write = execute(
            &registry(MemoryNoteAgentToolConfig::new(write_backend)),
            "memory_write",
            serde_json::json!({
                "path": "notes/runtime.md",
                "markdown": "# Runtime\n",
            }),
        );

        for outcome in [&search, &read, &write] {
            assert_eq!(outcome.status, AgentToolStatus::Failed);
            assert!(outcome.output.get("detail").is_none());
            let visible = format!("{} {}", outcome.summary, outcome.output);
            assert!(!visible.contains("BACKEND_IO_SENTINEL"));
            assert!(!visible.contains("/home/ix/private"));
            assert!(!visible.contains("PRIVATE_SESSION_SENTINEL"));
        }
        assert_eq!(search.output["error"], "memory_backend_unavailable");
        assert_eq!(read.output["error"], "invalid_note_operation");
        assert_eq!(write.output["error"], "memory_backend_unavailable");
    }

    #[test]
    fn memory_not_found_retains_only_the_safe_relative_path() {
        let backend = Arc::new(FakeMemoryNoteBackend::default());
        *backend.read_response.lock().expect("read response") =
            Some(Err(MemoryNoteBackendError::NotFound {
                path: FakeMemoryNoteBackend::path("notes/missing.md"),
            }));
        let outcome = execute(
            &registry(MemoryNoteAgentToolConfig::new(backend)),
            "memory_read",
            serde_json::json!({ "path": "notes/missing.md" }),
        );

        assert_eq!(outcome.status, AgentToolStatus::Failed);
        assert_eq!(outcome.output["error"], "note_not_found");
        assert_eq!(outcome.output["path"], "notes/missing.md");
        assert!(outcome.output.get("detail").is_none());
        assert!(outcome.summary.contains("notes/missing.md"));
    }

    #[test]
    fn memory_write_can_be_disabled_without_calling_backend() {
        let backend = Arc::new(FakeMemoryNoteBackend::default());
        let registry =
            registry(MemoryNoteAgentToolConfig::new(backend.clone()).allowing_writes(false));

        let outcome = execute(
            &registry,
            "memory_write",
            serde_json::json!({
                "path": "notes/blocked.md",
                "markdown": "blocked",
            }),
        );

        assert_eq!(outcome.status, AgentToolStatus::Blocked);
        assert!(backend.write_requests.lock().expect("requests").is_empty());
    }
}
