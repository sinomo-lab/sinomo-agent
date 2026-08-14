use super::{
    AgentTool, AgentToolCapability, AgentToolDescriptor, AgentToolEvidence,
    AgentToolInvocationMode, AgentToolRegistry, AgentToolRequest, AgentToolResult,
};
use crate::{AgentMcpProtocolEra, AgentMcpServerDefinition, AgentToolKind, AgentToolSpec};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use reqwest::StatusCode;
use reqwest::blocking::{Client, RequestBuilder, Response};
use reqwest::header::{
    ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT,
};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

/// Stateless revision: every request carries its own version, identity, and
/// capabilities, and there is no handshake or protocol-level session.
const MCP_MODERN_PROTOCOL_VERSION: &str = "2026-07-28";
const MCP_MODERN_SUPPORTED_PROTOCOL_VERSIONS: [&str; 1] = [MCP_MODERN_PROTOCOL_VERSION];
/// Handshake revisions, preferred first.
const MCP_LEGACY_PROTOCOL_VERSION: &str = "2025-06-18";
const MCP_LEGACY_SUPPORTED_PROTOCOL_VERSIONS: [&str; 3] =
    [MCP_LEGACY_PROTOCOL_VERSION, "2025-03-26", "2024-11-05"];

const MCP_SESSION_ID_HEADER: &str = "mcp-session-id";
const MCP_PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";
const MCP_METHOD_HEADER: &str = "mcp-method";
const MCP_NAME_HEADER: &str = "mcp-name";
const MCP_PARAM_HEADER_PREFIX: &str = "mcp-param-";

const MCP_META_PROTOCOL_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
const MCP_META_CLIENT_INFO: &str = "io.modelcontextprotocol/clientInfo";
const MCP_META_CLIENT_CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";
const MCP_META_SERVER_INFO: &str = "io.modelcontextprotocol/serverInfo";

const MCP_DISCOVER_METHOD: &str = "server/discover";
const MCP_HEADER_ANNOTATION: &str = "x-mcp-header";
const MCP_BASE64_SENTINEL_PREFIX: &str = "=?base64?";
const MCP_BASE64_SENTINEL_SUFFIX: &str = "?=";
const MCP_RESULT_TYPE_COMPLETE: &str = "complete";
const MCP_RESULT_TYPE_INPUT_REQUIRED: &str = "input_required";

/// Codes the specification reserves for itself. Receiving one of these proves
/// the peer speaks a stateless revision, which is what drives era detection.
const MCP_ERROR_HEADER_MISMATCH: i64 = -32020;
const MCP_ERROR_MISSING_CLIENT_CAPABILITY: i64 = -32021;
const MCP_ERROR_UNSUPPORTED_PROTOCOL_VERSION: i64 = -32022;
const JSONRPC_ERROR_METHOD_NOT_FOUND: i64 = -32601;

const MCP_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MCP_MAX_TOOLS: usize = 256;
const MCP_MAX_LIST_PAGES: usize = 20;
const MCP_MAX_DIAGNOSTIC_CHARS: usize = 512;
const EXTERNAL_INTEGRATIONS_PERMISSION: &str = "external-integrations";

#[derive(Debug, Error)]
pub enum AgentMcpError {
    #[error("MCP server `{server}` configuration is invalid: {detail}")]
    Configuration { server: String, detail: String },
    #[error("MCP server `{server}` request failed: {detail}")]
    Request { server: String, detail: String },
    #[error("MCP server `{server}` returned an invalid protocol response: {detail}")]
    Protocol { server: String, detail: String },
    #[error("MCP server `{server}` rejected the request metadata headers: {detail}")]
    HeaderMismatch { server: String, detail: String },
    #[error(
        "MCP server `{server}` supports no protocol version Sinomo speaks; it offers {offered}"
    )]
    UnsupportedProtocolVersion { server: String, offered: String },
    #[error("MCP server `{server}` does not implement the stateless MCP protocol: {detail}")]
    HandshakeOnly { server: String, detail: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentMcpRegistration {
    pub server: String,
    pub tools: Vec<String>,
    /// Protocol revision the transport settled on, empty when the server is disabled.
    pub protocol_version: String,
    /// Self-reported server identity, for display and diagnostics only.
    pub server_info: Option<String>,
    /// Tools the server advertised that this client refused to expose.
    pub rejected_tools: Vec<String>,
}

impl AgentMcpRegistration {
    fn skipped(server: &str) -> Self {
        Self {
            server: server.to_string(),
            tools: Vec::new(),
            protocol_version: String::new(),
            server_info: None,
            rejected_tools: Vec::new(),
        }
    }
}

/// Which generation of the wire protocol the transport settled on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum McpEra {
    /// `2026-07-28` and later: stateless, per-request metadata.
    Modern,
    /// `2025-11-25` and earlier: `initialize` handshake plus session state.
    Legacy,
}

impl McpEra {
    fn as_str(self) -> &'static str {
        match self {
            Self::Modern => "stateless",
            Self::Legacy => "handshake",
        }
    }
}

#[derive(Debug)]
struct McpClientState {
    era: McpEra,
    /// Version advertised on the wire. `None` until a handshake completes;
    /// stateless requests always carry one.
    protocol_version: Option<String>,
    /// Version this client is asking for, before the server has confirmed it.
    requested_version: String,
    /// Handshake-era session identifier; never minted by stateless revisions.
    session_id: Option<String>,
    server_info: Option<String>,
    next_request_id: u64,
}

#[derive(Debug)]
struct StreamableHttpMcpClient {
    server_name: String,
    url: String,
    http: Client,
    default_headers: HeaderMap,
    state: Mutex<McpClientState>,
    /// `Mcp-Param-*` sources per tool name, refreshed by every `tools/list`.
    header_params: Mutex<BTreeMap<String, Vec<McpHeaderParam>>>,
    startup_timeout: Duration,
    tool_timeout: Duration,
}

/// Body fields the Streamable HTTP binding mirrors into request headers.
#[derive(Clone, Debug, Default)]
struct McpRequestHeaders {
    /// Source for `Mcp-Name`: `params.name` or `params.uri`.
    name: Option<String>,
    /// Sources for `Mcp-Param-{name}`, taken from annotated tool arguments.
    params: Vec<(String, String)>,
}

impl StreamableHttpMcpClient {
    fn connect(config: &AgentMcpServerDefinition) -> Result<Arc<Self>, AgentMcpError> {
        config
            .validate()
            .map_err(|detail| AgentMcpError::Configuration {
                server: config.name.clone(),
                detail,
            })?;
        let startup_timeout = Duration::from_millis(config.startup_timeout_ms);
        let tool_timeout = Duration::from_millis(config.tool_timeout_ms);
        let http = Client::builder()
            .connect_timeout(startup_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|source| AgentMcpError::Configuration {
                server: config.name.clone(),
                detail: format!("could not construct HTTP client: {source}"),
            })?;
        let default_headers = resolved_mcp_headers(config)?;
        let client = Arc::new(Self {
            server_name: config.name.clone(),
            url: config.url.clone(),
            http,
            default_headers,
            state: Mutex::new(McpClientState {
                era: McpEra::Modern,
                protocol_version: Some(MCP_MODERN_PROTOCOL_VERSION.to_string()),
                requested_version: MCP_MODERN_PROTOCOL_VERSION.to_string(),
                session_id: None,
                server_info: None,
                next_request_id: 1,
            }),
            header_params: Mutex::new(BTreeMap::new()),
            startup_timeout,
            tool_timeout,
        });
        match config.protocol_era {
            AgentMcpProtocolEra::Legacy => client.initialize_handshake()?,
            AgentMcpProtocolEra::Modern => {
                client.discover(false)?;
            }
            AgentMcpProtocolEra::Auto => {
                if !client.discover(true)? {
                    client.initialize_handshake()?;
                }
            }
        }
        Ok(client)
    }

    /// Probes the stateless protocol with the mandatory `server/discover` RPC.
    ///
    /// Returns `false` only when `allow_handshake_fallback` is set and the
    /// endpoint answered in a way that identifies a handshake-era server.
    fn discover(&self, allow_handshake_fallback: bool) -> Result<bool, AgentMcpError> {
        self.select_modern(MCP_MODERN_PROTOCOL_VERSION)?;
        let mut renegotiated = false;
        loop {
            let outcome = self.rpc(
                MCP_DISCOVER_METHOD,
                None,
                McpRequestHeaders::default(),
                self.startup_timeout,
            );
            let result = match outcome {
                Ok(result) => result,
                Err(AgentMcpError::HandshakeOnly { .. }) if allow_handshake_fallback => {
                    return Ok(false);
                }
                Err(AgentMcpError::UnsupportedProtocolVersion { offered, .. }) if !renegotiated => {
                    let Some(selected) = select_modern_version(offered.split(',')) else {
                        return Err(AgentMcpError::UnsupportedProtocolVersion {
                            server: self.server_name.clone(),
                            offered,
                        });
                    };
                    self.select_modern(&selected)?;
                    renegotiated = true;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let offered = serde_json::from_value::<McpDiscoverResult>(result.clone())
                .map_err(|source| {
                    self.protocol_error(format!(
                        "{MCP_DISCOVER_METHOD} result is invalid: {source}"
                    ))
                })?
                .supported_versions;
            let server_info = implementation_label(
                result
                    .get("_meta")
                    .and_then(|meta| meta.get(MCP_META_SERVER_INFO)),
            );
            self.with_state(|state| state.server_info = server_info)?;
            if offered.is_empty() {
                return Ok(true);
            }
            let Some(selected) = select_modern_version(offered.iter().map(String::as_str)) else {
                return Err(AgentMcpError::UnsupportedProtocolVersion {
                    server: self.server_name.clone(),
                    offered: offered.join(", "),
                });
            };
            self.select_modern(&selected)?;
            return Ok(true);
        }
    }

    /// Negotiates a handshake-era session for `2025-11-25` and earlier servers.
    fn initialize_handshake(&self) -> Result<(), AgentMcpError> {
        self.with_state(|state| {
            state.era = McpEra::Legacy;
            state.protocol_version = None;
            state.requested_version = MCP_LEGACY_PROTOCOL_VERSION.to_string();
            state.session_id = None;
        })?;
        let result = self.rpc(
            "initialize",
            Some(serde_json::json!({
                "protocolVersion": MCP_LEGACY_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": client_info(),
            })),
            McpRequestHeaders::default(),
            self.startup_timeout,
        )?;
        let protocol_version = result
            .get("protocolVersion")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| self.protocol_error("initialize result omitted protocolVersion"))?
            .to_string();
        if !MCP_LEGACY_SUPPORTED_PROTOCOL_VERSIONS.contains(&protocol_version.as_str()) {
            return Err(self.protocol_error(format!(
                "server selected unsupported protocol version `{protocol_version}`"
            )));
        }
        let server_info = implementation_label(result.get("serverInfo"));
        self.with_state(|state| {
            state.protocol_version = Some(protocol_version);
            state.server_info = server_info;
        })?;
        self.notify("notifications/initialized", None, self.startup_timeout)
    }

    fn list_tools(&self) -> Result<McpToolCatalog, AgentMcpError> {
        let mut catalog = McpToolCatalog::default();
        let mut cursor = None;
        for _ in 0..MCP_MAX_LIST_PAGES {
            let params = cursor
                .as_deref()
                .map(|cursor| serde_json::json!({ "cursor": cursor }));
            let value = self.rpc(
                "tools/list",
                params,
                McpRequestHeaders::default(),
                self.startup_timeout,
            )?;
            let page = serde_json::from_value::<McpListToolsResult>(value).map_err(|source| {
                self.protocol_error(format!("tools/list result is invalid: {source}"))
            })?;
            if catalog.tools.len().saturating_add(page.tools.len()) > MCP_MAX_TOOLS {
                return Err(self.protocol_error(format!(
                    "tools/list exceeded the {MCP_MAX_TOOLS}-tool bound"
                )));
            }
            catalog.absorb(page.tools);
            match page.next_cursor {
                Some(next) if Some(next.as_str()) != cursor.as_deref() => cursor = Some(next),
                Some(_) => {
                    return Err(self.protocol_error("tools/list cursor did not advance"));
                }
                None => {
                    self.cache_header_params(&catalog)?;
                    return Ok(catalog);
                }
            }
        }
        Err(self.protocol_error(format!(
            "tools/list exceeded the {MCP_MAX_LIST_PAGES}-page bound"
        )))
    }

    fn call_tool(&self, name: &str, arguments: &Value) -> Result<McpCallToolResult, AgentMcpError> {
        if !arguments.is_object() {
            return Err(self.protocol_error("tool arguments must be a JSON object"));
        }
        match self.call_tool_once(name, arguments) {
            // A header mismatch means our mirrored `Mcp-Param-*` headers no
            // longer agree with the server's schema, so re-list and try once
            // more with the refreshed annotations.
            Err(AgentMcpError::HeaderMismatch { .. }) => {
                self.list_tools()?;
                self.call_tool_once(name, arguments)
            }
            outcome => outcome,
        }
    }

    fn call_tool_once(
        &self,
        name: &str,
        arguments: &Value,
    ) -> Result<McpCallToolResult, AgentMcpError> {
        let headers = McpRequestHeaders {
            name: Some(name.to_string()),
            params: self.mirrored_param_headers(name, arguments)?,
        };
        let value = self.rpc(
            "tools/call",
            Some(serde_json::json!({
                "name": name,
                "arguments": arguments,
            })),
            headers,
            self.tool_timeout,
        )?;
        serde_json::from_value(value).map_err(|source| {
            self.protocol_error(format!("tools/call result is invalid: {source}"))
        })
    }

    fn rpc(
        &self,
        method: &str,
        params: Option<Value>,
        headers: McpRequestHeaders,
        timeout: Duration,
    ) -> Result<Value, AgentMcpError> {
        let (request_id, era, protocol_version, session_id) = self.with_state(|state| {
            let request_id = state.next_request_id;
            state.next_request_id = state.next_request_id.saturating_add(1);
            (
                request_id,
                state.era,
                state.protocol_version.clone(),
                state.session_id.clone(),
            )
        })?;
        let mut payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
        });
        let mut params = params;
        if era == McpEra::Modern {
            let version = protocol_version
                .clone()
                .unwrap_or_else(|| MCP_MODERN_PROTOCOL_VERSION.to_string());
            params = Some(with_request_meta(params, &version));
        }
        if let Some(params) = params {
            payload["params"] = params;
        }
        let response = self.send(
            payload,
            method,
            era,
            protocol_version,
            session_id,
            &headers,
            timeout,
        )?;
        if let Some(session_id) = response.session_id.clone() {
            self.with_state(|state| state.session_id = Some(session_id))?;
        }
        self.decode_result(method, era, request_id, response)
    }

    fn notify(
        &self,
        method: &str,
        params: Option<Value>,
        timeout: Duration,
    ) -> Result<(), AgentMcpError> {
        let (era, protocol_version, session_id) = self.with_state(|state| {
            (
                state.era,
                state.protocol_version.clone(),
                state.session_id.clone(),
            )
        })?;
        let mut payload = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
        });
        if let Some(params) = params {
            payload["params"] = params;
        }
        let response = self.send(
            payload,
            method,
            era,
            protocol_version,
            session_id,
            &McpRequestHeaders::default(),
            timeout,
        )?;
        if response.status.is_success() {
            Ok(())
        } else {
            Err(self.request_error(format!(
                "HTTP {}: {}",
                response.status,
                truncate_for_diagnostics(&response.text)
            )))
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn send(
        &self,
        payload: Value,
        method: &str,
        era: McpEra,
        protocol_version: Option<String>,
        session_id: Option<String>,
        headers: &McpRequestHeaders,
        timeout: Duration,
    ) -> Result<McpHttpResponse, AgentMcpError> {
        let mut request = self
            .http
            .post(&self.url)
            .headers(self.default_headers.clone())
            .timeout(timeout)
            .json(&payload);
        if let Some(protocol_version) = protocol_version {
            request = request.header(MCP_PROTOCOL_VERSION_HEADER, protocol_version);
        }
        request = match era {
            // Stateless revisions mirror body fields into headers so that
            // intermediaries can route without parsing the body.
            McpEra::Modern => self.with_metadata_headers(request, method, headers),
            McpEra::Legacy => match session_id {
                Some(session_id) => request.header(MCP_SESSION_ID_HEADER, session_id),
                None => request,
            },
        };
        let response = request
            .send()
            .map_err(|source| self.request_error(source.to_string()))?;
        read_mcp_response(&self.server_name, response)
    }

    fn with_metadata_headers(
        &self,
        request: RequestBuilder,
        method: &str,
        headers: &McpRequestHeaders,
    ) -> RequestBuilder {
        let mut request = request.header(MCP_METHOD_HEADER, method);
        if let Some(name) = headers.name.as_deref() {
            request = request.header(MCP_NAME_HEADER, encoded_mcp_header_value(name));
        }
        for (header, value) in &headers.params {
            request = request.header(
                format!("{MCP_PARAM_HEADER_PREFIX}{header}"),
                encoded_mcp_header_value(value),
            );
        }
        request
    }

    fn decode_result(
        &self,
        method: &str,
        era: McpEra,
        request_id: u64,
        response: McpHttpResponse,
    ) -> Result<Value, AgentMcpError> {
        // Only the discovery probe can still change our mind about which era
        // the endpoint speaks; every later request has a settled answer.
        let probing = era == McpEra::Modern && method == MCP_DISCOVER_METHOD;
        let message = response
            .messages
            .iter()
            .flat_map(|value| match value {
                Value::Array(values) => values.iter().collect::<Vec<_>>(),
                value => vec![value],
            })
            .find(|value| value.get("id").and_then(Value::as_u64) == Some(request_id));
        let Some(message) = message else {
            let detail = if response.status.is_success() {
                format!(
                    "{method} response omitted JSON-RPC id {request_id}: {}",
                    truncate_for_diagnostics(&response.text)
                )
            } else {
                format!(
                    "HTTP {}: {}",
                    response.status,
                    truncate_for_diagnostics(&response.text)
                )
            };
            if probing {
                return Err(AgentMcpError::HandshakeOnly {
                    server: self.server_name.clone(),
                    detail,
                });
            }
            return Err(if response.status.is_success() {
                self.protocol_error(detail)
            } else {
                self.request_error(detail)
            });
        };
        if let Some(error) = message.get("error") {
            return Err(self.map_rpc_error(method, probing, error));
        }
        let result = message
            .get("result")
            .cloned()
            .ok_or_else(|| self.protocol_error(format!("{method} response omitted result")))?;
        self.ensure_complete_result(method, &result)?;
        Ok(result)
    }

    /// Maps a JSON-RPC error object onto the error this client reports.
    ///
    /// During a probe, only the codes the specification reserves for itself
    /// prove a stateless peer; anything else means we are talking to a
    /// handshake-era server and should fall back.
    fn map_rpc_error(&self, method: &str, probing: bool, error: &Value) -> AgentMcpError {
        let code = error.get("code").and_then(Value::as_i64);
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| error.to_string());
        match code {
            Some(MCP_ERROR_UNSUPPORTED_PROTOCOL_VERSION) => {
                let offered = error
                    .get("data")
                    .and_then(|data| data.get("supported"))
                    .and_then(Value::as_array)
                    .map(|versions| {
                        versions
                            .iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .filter(|offered| !offered.is_empty())
                    .unwrap_or_else(|| "no versions".to_string());
                AgentMcpError::UnsupportedProtocolVersion {
                    server: self.server_name.clone(),
                    offered,
                }
            }
            Some(MCP_ERROR_HEADER_MISMATCH) => AgentMcpError::HeaderMismatch {
                server: self.server_name.clone(),
                detail: message,
            },
            Some(MCP_ERROR_MISSING_CLIENT_CAPABILITY) => AgentMcpError::Configuration {
                server: self.server_name.clone(),
                detail: format!(
                    "{method} needs client capabilities Sinomo's MCP client does not provide: {message}"
                ),
            },
            Some(JSONRPC_ERROR_METHOD_NOT_FOUND) if probing => AgentMcpError::HandshakeOnly {
                server: self.server_name.clone(),
                detail: format!("{MCP_DISCOVER_METHOD} is not implemented: {message}"),
            },
            _ if probing => AgentMcpError::HandshakeOnly {
                server: self.server_name.clone(),
                detail: format!("{MCP_DISCOVER_METHOD} failed: {message}"),
            },
            _ => self.request_error(format!("{method}: {message}")),
        }
    }

    /// Rejects results this client cannot act on. An absent `resultType` means
    /// a pre-`2026-07-28` server, which the specification says to read as
    /// `complete`.
    fn ensure_complete_result(&self, method: &str, result: &Value) -> Result<(), AgentMcpError> {
        match result.get("resultType").and_then(Value::as_str) {
            None | Some(MCP_RESULT_TYPE_COMPLETE) => Ok(()),
            Some(MCP_RESULT_TYPE_INPUT_REQUIRED) => Err(self.request_error(format!(
                "{method} needs additional client input (sampling, elicitation, or roots), which Sinomo's MCP client does not provide"
            ))),
            Some(other) => Err(self.protocol_error(format!(
                "{method} returned unrecognized resultType `{other}`"
            ))),
        }
    }

    fn mirrored_param_headers(
        &self,
        tool: &str,
        arguments: &Value,
    ) -> Result<Vec<(String, String)>, AgentMcpError> {
        if self.with_state(|state| state.era)? != McpEra::Modern {
            return Ok(Vec::new());
        }
        let params = self
            .header_params
            .lock()
            .map_err(|_| self.protocol_error("header parameter cache lock poisoned"))?
            .get(tool)
            .cloned()
            .unwrap_or_default();
        Ok(mcp_param_header_values(&params, arguments))
    }

    fn cache_header_params(&self, catalog: &McpToolCatalog) -> Result<(), AgentMcpError> {
        let mut cache = self
            .header_params
            .lock()
            .map_err(|_| self.protocol_error("header parameter cache lock poisoned"))?;
        cache.clear();
        for (tool, params) in &catalog.header_params {
            cache.insert(tool.clone(), params.clone());
        }
        Ok(())
    }

    fn select_modern(&self, version: &str) -> Result<(), AgentMcpError> {
        self.with_state(|state| {
            state.era = McpEra::Modern;
            state.protocol_version = Some(version.to_string());
            state.requested_version = version.to_string();
            state.session_id = None;
        })
    }

    fn with_state<R>(
        &self,
        apply: impl FnOnce(&mut McpClientState) -> R,
    ) -> Result<R, AgentMcpError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| self.protocol_error("client state lock poisoned"))?;
        Ok(apply(&mut state))
    }

    fn negotiated(&self) -> Result<(String, Option<String>), AgentMcpError> {
        self.with_state(|state| {
            (
                state
                    .protocol_version
                    .clone()
                    .unwrap_or_else(|| state.requested_version.clone()),
                state.server_info.clone(),
            )
        })
    }

    fn era_label(&self) -> &'static str {
        self.state
            .lock()
            .map(|state| state.era.as_str())
            .unwrap_or("unknown")
    }

    fn request_error(&self, detail: impl Into<String>) -> AgentMcpError {
        AgentMcpError::Request {
            server: self.server_name.clone(),
            detail: detail.into(),
        }
    }

    fn protocol_error(&self, detail: impl Into<String>) -> AgentMcpError {
        AgentMcpError::Protocol {
            server: self.server_name.clone(),
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct McpDiscoverResult {
    #[serde(default)]
    supported_versions: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct McpListToolsResult {
    #[serde(default)]
    tools: Vec<McpToolDefinition>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct McpToolDefinition {
    name: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default = "empty_object_schema")]
    input_schema: Value,
    #[serde(default)]
    output_schema: Option<Value>,
    #[serde(default)]
    annotations: McpToolAnnotations,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct McpToolAnnotations {
    #[serde(default)]
    read_only_hint: Option<bool>,
    #[serde(default)]
    destructive_hint: Option<bool>,
    #[serde(default)]
    idempotent_hint: Option<bool>,
    #[serde(default)]
    open_world_hint: Option<bool>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct McpCallToolResult {
    #[serde(default)]
    content: Vec<Value>,
    #[serde(default)]
    structured_content: Option<Value>,
    #[serde(default)]
    is_error: bool,
}

/// Tools accepted from `tools/list`, plus the ones this client refused.
#[derive(Clone, Debug, Default)]
struct McpToolCatalog {
    tools: Vec<McpToolDefinition>,
    header_params: BTreeMap<String, Vec<McpHeaderParam>>,
    rejected: Vec<String>,
}

impl McpToolCatalog {
    /// A malformed `x-mcp-header` annotation invalidates only its own tool, so
    /// one bad definition never hides the rest of the server's catalog.
    fn absorb(&mut self, tools: Vec<McpToolDefinition>) {
        for tool in tools {
            match collect_mcp_header_params(&tool.input_schema) {
                Ok(params) => {
                    self.header_params.insert(tool.name.clone(), params);
                    self.tools.push(tool);
                }
                Err(detail) => self.rejected.push(format!("`{}` ({detail})", tool.name)),
            }
        }
    }
}

/// A tool argument the server asked to have mirrored into an HTTP header.
#[derive(Clone, Debug, Eq, PartialEq)]
struct McpHeaderParam {
    /// The `{name}` portion of `Mcp-Param-{name}`.
    header: String,
    /// Chain of `properties` keys leading to the annotated argument.
    path: Vec<String>,
}

#[derive(Debug)]
struct McpHttpResponse {
    status: StatusCode,
    session_id: Option<String>,
    /// JSON-RPC messages decoded from a JSON body or an SSE stream.
    messages: Vec<Value>,
    /// Raw body, kept so failures can quote what actually came back.
    text: String,
}

#[derive(Clone)]
struct McpAgentTool {
    client: Arc<StreamableHttpMcpClient>,
    definition: McpToolDefinition,
    exposed_name: String,
}

impl AgentTool for McpAgentTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        let label = self
            .definition
            .title
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(self.definition.name.as_str());
        let description = self
            .definition
            .description
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| format!("MCP `{}`: {value}", self.client.server_name))
            .unwrap_or_else(|| {
                format!(
                    "Call `{label}` on MCP server `{}`.",
                    self.client.server_name
                )
            });
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::McpCall,
            self.exposed_name.clone(),
            description,
            false,
        ))
        .with_input_schema(normalized_object_schema(&self.definition.input_schema))
        .with_output_schema(mcp_result_schema(self.definition.output_schema.as_ref()))
        .with_capabilities([
            AgentToolCapability::AccessNetwork,
            AgentToolCapability::CallMcp,
        ])
        .with_invocation_modes(AgentToolInvocationMode::all())
        .requiring_local_permission(EXTERNAL_INTEGRATIONS_PERMISSION)
        .supporting_parallel(self.definition.annotations.read_only_hint == Some(true))
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        if request.cancellation_token.is_cancelled() {
            return AgentToolResult::cancelled(
                format!("MCP tool `{}` was cancelled", self.definition.name),
                serde_json::json!({ "error": "cancelled" }),
            );
        }
        let result = match self
            .client
            .call_tool(&self.definition.name, &request.call.input)
        {
            Ok(result) => result,
            Err(error) => {
                return AgentToolResult::failed(
                    error.to_string(),
                    serde_json::json!({
                        "error": "mcp_call_failed",
                        "server": self.client.server_name,
                        "tool": self.definition.name,
                        "detail": error.to_string(),
                    }),
                );
            }
        };
        if request.cancellation_token.is_cancelled() {
            return AgentToolResult::cancelled(
                format!("MCP tool `{}` was cancelled", self.definition.name),
                serde_json::json!({ "error": "cancelled" }),
            );
        }
        let is_error = result.is_error;
        let output = serde_json::json!({
            "server": self.client.server_name,
            "tool": self.definition.name,
            "content": result.content,
            "structured_content": result.structured_content,
            "is_error": is_error,
        });
        let summary = mcp_result_summary(&output).unwrap_or_else(|| {
            format!(
                "MCP `{}` completed `{}`",
                self.client.server_name, self.definition.name
            )
        });
        let (protocol_version, server_info) = self
            .client
            .negotiated()
            .unwrap_or_else(|_| (String::new(), None));
        let evidence = AgentToolEvidence::availability_only(
            true,
            "executed through a configured Streamable HTTP MCP server after node-local authorization",
        )
        .with_output_value(&output, false)
        .with_metadata(serde_json::json!({
            "mcp_server": self.client.server_name,
            "mcp_tool": self.definition.name,
            "mcp_protocol_version": protocol_version,
            "mcp_protocol_era": self.client.era_label(),
            "mcp_server_info": server_info,
            "read_only_hint": self.definition.annotations.read_only_hint,
            "destructive_hint": self.definition.annotations.destructive_hint,
            "idempotent_hint": self.definition.annotations.idempotent_hint,
            "open_world_hint": self.definition.annotations.open_world_hint,
        }));
        if is_error {
            AgentToolResult::failed(summary, output).with_evidence(evidence)
        } else {
            AgentToolResult::succeeded(summary, output).with_evidence(evidence)
        }
    }
}

pub fn register_mcp_server_tools(
    registry: &AgentToolRegistry,
    config: &AgentMcpServerDefinition,
) -> Result<AgentMcpRegistration, AgentMcpError> {
    if !config.enabled {
        return Ok(AgentMcpRegistration::skipped(&config.name));
    }
    let client = StreamableHttpMcpClient::connect(config)?;
    let allowed = config
        .allowed_tools
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let catalog = client.list_tools()?;
    let McpToolCatalog {
        tools: discovered,
        rejected: rejected_tools,
        ..
    } = catalog;
    for definition in &discovered {
        if definition.name.trim().is_empty()
            || definition.name.len() > 128
            || definition.name.chars().any(char::is_control)
        {
            return Err(AgentMcpError::Protocol {
                server: config.name.clone(),
                detail: format!(
                    "tools/list returned invalid tool name `{}`",
                    definition.name
                ),
            });
        }
        if !definition.input_schema.is_object()
            || definition
                .output_schema
                .as_ref()
                .is_some_and(|schema| !schema.is_object())
        {
            return Err(AgentMcpError::Protocol {
                server: config.name.clone(),
                detail: format!(
                    "tools/list returned a non-object JSON Schema for tool `{}`",
                    definition.name
                ),
            });
        }
    }
    let discovered_names = discovered
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<BTreeSet<_>>();
    if let Some(missing) = allowed.difference(&discovered_names).next() {
        return Err(AgentMcpError::Configuration {
            server: config.name.clone(),
            detail: format!("allowed tool `{missing}` was not advertised by the server"),
        });
    }
    let definitions = discovered
        .into_iter()
        .filter(|tool| allowed.is_empty() || allowed.contains(tool.name.as_str()))
        .collect::<Vec<_>>();
    let existing = registry
        .descriptors()
        .into_iter()
        .map(|descriptor| descriptor.spec.name)
        .collect::<BTreeSet<_>>();
    let mut exposed_names = BTreeSet::new();
    let tools = definitions
        .iter()
        .map(|definition| exposed_mcp_tool_name(&config.name, &definition.name))
        .collect::<Vec<_>>();
    for (definition, exposed_name) in definitions.iter().zip(&tools) {
        if existing.contains(exposed_name) || !exposed_names.insert(exposed_name.clone()) {
            return Err(AgentMcpError::Configuration {
                server: config.name.clone(),
                detail: format!(
                    "MCP tool `{}` maps to duplicate Sinomo tool name `{exposed_name}`",
                    definition.name
                ),
            });
        }
    }
    for (definition, exposed_name) in definitions.into_iter().zip(&tools) {
        registry.register(McpAgentTool {
            client: client.clone(),
            definition,
            exposed_name: exposed_name.clone(),
        });
    }
    let (protocol_version, server_info) = client.negotiated()?;
    Ok(AgentMcpRegistration {
        server: config.name.clone(),
        tools,
        protocol_version,
        server_info,
        rejected_tools,
    })
}

fn client_info() -> Value {
    serde_json::json!({
        "name": "sinomo",
        "version": env!("CARGO_PKG_VERSION"),
    })
}

/// Attaches the per-request protocol metadata every stateless request carries.
fn with_request_meta(params: Option<Value>, protocol_version: &str) -> Value {
    let mut params = match params {
        Some(Value::Object(map)) => Value::Object(map),
        _ => Value::Object(Map::new()),
    };
    let mut meta = Map::new();
    meta.insert(
        MCP_META_PROTOCOL_VERSION.to_string(),
        Value::String(protocol_version.to_string()),
    );
    meta.insert(MCP_META_CLIENT_INFO.to_string(), client_info());
    // Sinomo exposes no roots, sampling, or elicitation to servers.
    meta.insert(
        MCP_META_CLIENT_CAPABILITIES.to_string(),
        Value::Object(Map::new()),
    );
    params["_meta"] = Value::Object(meta);
    params
}

fn select_modern_version<'a>(offered: impl IntoIterator<Item = &'a str>) -> Option<String> {
    let offered = offered
        .into_iter()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>();
    MCP_MODERN_SUPPORTED_PROTOCOL_VERSIONS
        .iter()
        .find(|version| offered.contains(*version))
        .map(|version| version.to_string())
}

fn implementation_label(value: Option<&Value>) -> Option<String> {
    let value = value?;
    let name = value
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())?;
    match value
        .get("version")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|version| !version.is_empty())
    {
        Some(version) => Some(format!("{name} {version}")),
        None => Some(name.to_string()),
    }
}

fn resolved_mcp_headers(config: &AgentMcpServerDefinition) -> Result<HeaderMap, AgentMcpError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/json, text/event-stream"),
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("Sinomo-MCP/0.1"));
    if let Some(variable) = config.bearer_token_env.as_deref() {
        validate_mcp_secret_env_reference(config, variable)?;
        let value = required_env(config, variable)?;
        let value = HeaderValue::from_str(&format!("Bearer {value}")).map_err(|source| {
            AgentMcpError::Configuration {
                server: config.name.clone(),
                detail: format!("bearer token from `{variable}` is not a valid header: {source}"),
            }
        })?;
        headers.insert(AUTHORIZATION, value);
    }
    for (header, variable) in &config.headers_from_env {
        validate_mcp_secret_env_reference(config, variable)?;
        let name = HeaderName::from_bytes(header.as_bytes()).map_err(|source| {
            AgentMcpError::Configuration {
                server: config.name.clone(),
                detail: format!("header `{header}` is invalid: {source}"),
            }
        })?;
        if crate::mcp_header_name_is_transport_controlled(name.as_str()) {
            return Err(AgentMcpError::Configuration {
                server: config.name.clone(),
                detail: format!("header `{header}` is controlled by the MCP transport"),
            });
        }
        let value = HeaderValue::from_str(&required_env(config, variable)?).map_err(|source| {
            AgentMcpError::Configuration {
                server: config.name.clone(),
                detail: format!("value from `{variable}` is not a valid header: {source}"),
            }
        })?;
        headers.insert(name, value);
    }
    Ok(headers)
}

fn validate_mcp_secret_env_reference(
    config: &AgentMcpServerDefinition,
    variable: &str,
) -> Result<(), AgentMcpError> {
    crate::validate_mcp_env_name(variable).map_err(|detail| AgentMcpError::Configuration {
        server: config.name.clone(),
        detail,
    })
}

fn required_env(
    config: &AgentMcpServerDefinition,
    variable: &str,
) -> Result<String, AgentMcpError> {
    std::env::var(variable)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AgentMcpError::Configuration {
            server: config.name.clone(),
            detail: format!("required environment variable `{variable}` is not set"),
        })
}

fn read_mcp_response(server: &str, response: Response) -> Result<McpHttpResponse, AgentMcpError> {
    let status = response.status();
    let session_id = response
        .headers()
        .get(MCP_SESSION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let body = read_bounded_response_body(response).map_err(|detail| AgentMcpError::Protocol {
        server: server.to_string(),
        detail,
    })?;
    let text = String::from_utf8_lossy(&body).into_owned();
    let messages = if let Ok(value) = serde_json::from_slice::<Value>(&body) {
        vec![value]
    } else {
        parse_sse_json_values(&text).unwrap_or_default()
    };
    Ok(McpHttpResponse {
        status,
        session_id,
        messages,
        text,
    })
}

fn read_bounded_response_body(mut response: Response) -> Result<Vec<u8>, String> {
    if response
        .content_length()
        .is_some_and(|length| length > MCP_MAX_RESPONSE_BYTES as u64)
    {
        return Err(format!(
            "response exceeds the {MCP_MAX_RESPONSE_BYTES}-byte bound"
        ));
    }
    let mut body = Vec::new();
    response
        .by_ref()
        .take((MCP_MAX_RESPONSE_BYTES + 1) as u64)
        .read_to_end(&mut body)
        .map_err(|source| format!("could not read response: {source}"))?;
    if body.len() > MCP_MAX_RESPONSE_BYTES {
        return Err(format!(
            "response exceeds the {MCP_MAX_RESPONSE_BYTES}-byte bound"
        ));
    }
    Ok(body)
}

fn parse_sse_json_values(text: &str) -> Result<Vec<Value>, String> {
    let normalized = text.replace("\r\n", "\n");
    let mut values = Vec::new();
    for event in normalized.split("\n\n") {
        let data = event
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(str::trim_start)
            .collect::<Vec<_>>()
            .join("\n");
        if data.is_empty() {
            continue;
        }
        values.push(
            serde_json::from_str(&data)
                .map_err(|source| format!("invalid SSE JSON data: {source}"))?,
        );
    }
    Ok(values)
}

fn truncate_for_diagnostics(text: &str) -> String {
    let text = text.trim();
    if text.chars().count() <= MCP_MAX_DIAGNOSTIC_CHARS {
        return text.to_string();
    }
    let mut truncated = text
        .chars()
        .take(MCP_MAX_DIAGNOSTIC_CHARS)
        .collect::<String>();
    truncated.push('…');
    truncated
}

/// Collects the `x-mcp-header` annotations a tool's input schema declares.
///
/// The annotation is only legal on a property reachable from the schema root
/// through an unbroken chain of `properties` keys; an annotation anywhere else
/// invalidates the whole tool definition.
fn collect_mcp_header_params(schema: &Value) -> Result<Vec<McpHeaderParam>, String> {
    let mut params = Vec::new();
    let mut seen = BTreeSet::new();
    let mut path = Vec::new();
    collect_mcp_header_params_at(schema, &mut path, true, false, &mut params, &mut seen)?;
    Ok(params)
}

fn collect_mcp_header_params_at(
    node: &Value,
    path: &mut Vec<String>,
    reachable: bool,
    is_property: bool,
    params: &mut Vec<McpHeaderParam>,
    seen: &mut BTreeSet<String>,
) -> Result<(), String> {
    match node {
        Value::Object(map) => {
            if let Some(annotation) = map.get(MCP_HEADER_ANNOTATION) {
                let header = annotation
                    .as_str()
                    .ok_or_else(|| format!("`{MCP_HEADER_ANNOTATION}` must be a string"))?;
                if !is_property {
                    return Err(format!(
                        "`{MCP_HEADER_ANNOTATION}` value `{header}` is not on a property reachable through `properties`"
                    ));
                }
                validate_mcp_param_header_name(header)?;
                if !seen.insert(header.to_ascii_lowercase()) {
                    return Err(format!(
                        "`{MCP_HEADER_ANNOTATION}` value `{header}` is used more than once"
                    ));
                }
                if !mcp_header_param_type_is_supported(map) {
                    return Err(format!(
                        "`{MCP_HEADER_ANNOTATION}` value `{header}` is not on a string, integer, or boolean property"
                    ));
                }
                params.push(McpHeaderParam {
                    header: header.to_string(),
                    path: path.clone(),
                });
            }
            for (key, child) in map {
                if key == "properties"
                    && let Value::Object(properties) = child
                {
                    for (name, property) in properties {
                        path.push(name.clone());
                        let outcome = collect_mcp_header_params_at(
                            property, path, reachable, reachable, params, seen,
                        );
                        path.pop();
                        outcome?;
                    }
                    continue;
                }
                collect_mcp_header_params_at(child, path, false, false, params, seen)?;
            }
        }
        Value::Array(values) => {
            for child in values {
                collect_mcp_header_params_at(child, path, false, false, params, seen)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_mcp_param_header_name(value: &str) -> Result<(), String> {
    const TOKEN_SPECIALS: &str = "!#$%&'*+-.^_`|~";
    if value.is_empty() {
        return Err(format!("`{MCP_HEADER_ANNOTATION}` must not be empty"));
    }
    if !value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || TOKEN_SPECIALS.contains(character))
    {
        return Err(format!(
            "`{MCP_HEADER_ANNOTATION}` value `{value}` is not a valid HTTP field-name token"
        ));
    }
    Ok(())
}

fn mcp_header_param_type_is_supported(schema: &Map<String, Value>) -> bool {
    matches!(
        schema.get("type").and_then(Value::as_str),
        Some("string" | "integer" | "boolean")
    )
}

/// Reads the annotated arguments out of a call and renders them as header
/// values. An argument that is absent, null, or not a mirrorable primitive
/// simply yields no header.
fn mcp_param_header_values(params: &[McpHeaderParam], arguments: &Value) -> Vec<(String, String)> {
    params
        .iter()
        .filter_map(|param| {
            let mut node = arguments;
            for key in &param.path {
                node = node.get(key)?;
            }
            let rendered = match node {
                Value::String(value) => value.clone(),
                Value::Bool(value) => value.to_string(),
                Value::Number(number) => number.as_i64().map(|value| value.to_string())?,
                _ => return None,
            };
            Some((param.header.clone(), rendered))
        })
        .collect()
}

/// Header values must be visible ASCII with no leading or trailing whitespace;
/// anything else travels Base64-wrapped in the specification's sentinel.
fn encoded_mcp_header_value(value: &str) -> String {
    if mcp_header_value_is_safe(value) {
        value.to_string()
    } else {
        format!(
            "{MCP_BASE64_SENTINEL_PREFIX}{}{MCP_BASE64_SENTINEL_SUFFIX}",
            BASE64_STANDARD.encode(value.as_bytes())
        )
    }
}

fn mcp_header_value_is_safe(value: &str) -> bool {
    if value.starts_with(MCP_BASE64_SENTINEL_PREFIX) && value.ends_with(MCP_BASE64_SENTINEL_SUFFIX)
    {
        return false;
    }
    if value.trim() != value {
        return false;
    }
    value
        .chars()
        .all(|character| character == '\t' || ('\u{20}'..='\u{7e}').contains(&character))
}

fn exposed_mcp_tool_name(server: &str, tool: &str) -> String {
    let name = format!(
        "mcp__{}__{}",
        sanitize_tool_name_component(server),
        sanitize_tool_name_component(tool)
    );
    if name.len() <= 64 {
        return name;
    }
    let digest = blake3::hash(name.as_bytes()).to_hex().to_string();
    format!("{}__{}", &name[..48], &digest[..14])
}

fn sanitize_tool_name_component(value: &str) -> String {
    let value = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    let value = value.trim_matches('_');
    if value.is_empty() {
        "tool".to_string()
    } else {
        value.to_string()
    }
}

fn empty_object_schema() -> Value {
    serde_json::json!({ "type": "object", "properties": {} })
}

fn mcp_result_schema(structured_content_schema: Option<&Value>) -> Value {
    let structured_content_schema = structured_content_schema
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    serde_json::json!({
        "type": "object",
        "required": ["server", "tool", "content", "is_error"],
        "properties": {
            "server": { "type": "string" },
            "tool": { "type": "string" },
            "content": { "type": "array", "items": {} },
            "structured_content": {
                "anyOf": [structured_content_schema, { "type": "null" }]
            },
            "is_error": { "type": "boolean" }
        },
        "additionalProperties": false
    })
}

fn normalized_object_schema(value: &Value) -> Value {
    if value.is_object() {
        value.clone()
    } else {
        empty_object_schema()
    }
}

fn mcp_result_summary(output: &Value) -> Option<String> {
    let text = output
        .get("content")?
        .as_array()?
        .iter()
        .find_map(|content| {
            (content.get("type").and_then(Value::as_str) == Some("text"))
                .then(|| content.get("text").and_then(Value::as_str))
                .flatten()
        })?
        .trim();
    if text.is_empty() {
        return None;
    }
    let mut summary = text.chars().take(240).collect::<String>();
    if text.chars().count() > 240 {
        summary.push('…');
    }
    Some(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentToolCall, AgentToolStatus, InteractionSession, InteractionSurface,
        InteractionSurfaceKind,
    };
    use sinomo_runtime::events::SessionId;
    use sinomo_runtime::node::{LocalNodePermission, NodeId};
    use std::io::{BufRead, BufReader, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};

    fn read_http_request(stream: &mut TcpStream) -> (String, Value) {
        let mut reader = BufReader::new(stream);
        let mut headers = String::new();
        let mut content_length = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).expect("read HTTP header");
            assert!(!line.is_empty(), "connection closed before HTTP headers");
            if line == "\r\n" {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                content_length = value.trim().parse().expect("content length");
            }
            headers.push_str(&line);
        }
        let mut body = vec![0; content_length];
        reader.read_exact(&mut body).expect("read HTTP body");
        (
            headers.to_ascii_lowercase(),
            serde_json::from_slice(&body).expect("JSON-RPC request"),
        )
    }

    fn respond(stream: &mut TcpStream, status: &str, content_type: &str, body: &str, extra: &str) {
        write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n{extra}\r\n{body}",
            body.len()
        )
        .expect("write HTTP response");
        stream.flush().expect("flush HTTP response");
    }

    fn accept_request(listener: &TcpListener) -> (TcpStream, String, Value) {
        let (mut stream, _) = listener.accept().expect("accept MCP request");
        let (headers, body) = read_http_request(&mut stream);
        (stream, headers, body)
    }

    fn reply(stream: &mut TcpStream, id: &Value, result: Value) {
        let body = serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result }).to_string();
        respond(stream, "200 OK", "application/json", &body, "");
    }

    fn fail(stream: &mut TcpStream, status: &str, id: Value, error: Value) {
        let body = serde_json::json!({ "jsonrpc": "2.0", "id": id, "error": error }).to_string();
        respond(stream, status, "application/json", &body, "");
    }

    fn discover_result() -> Value {
        serde_json::json!({
            "resultType": "complete",
            "supportedVersions": [MCP_MODERN_PROTOCOL_VERSION],
            "capabilities": { "tools": {} },
            "_meta": {
                MCP_META_SERVER_INFO: { "name": "mock", "version": "1.0" }
            },
            "ttlMs": 3_600_000,
            "cacheScope": "public"
        })
    }

    fn search_tool() -> Value {
        serde_json::json!({
            "name": "search",
            "title": "Mock Search",
            "description": "Search the mock index",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "region": { "type": "string", "x-mcp-header": "Region" }
                },
                "required": ["query"]
            },
            "annotations": { "readOnlyHint": true }
        })
    }

    fn test_client(server_name: &str) -> Arc<StreamableHttpMcpClient> {
        Arc::new(StreamableHttpMcpClient {
            server_name: server_name.to_string(),
            url: "http://127.0.0.1:1/mcp".to_string(),
            http: Client::new(),
            default_headers: HeaderMap::new(),
            state: Mutex::new(McpClientState {
                era: McpEra::Modern,
                protocol_version: Some(MCP_MODERN_PROTOCOL_VERSION.to_string()),
                requested_version: MCP_MODERN_PROTOCOL_VERSION.to_string(),
                session_id: None,
                server_info: None,
                next_request_id: 1,
            }),
            header_params: Mutex::new(BTreeMap::new()),
            startup_timeout: Duration::from_secs(1),
            tool_timeout: Duration::from_secs(1),
        })
    }

    fn permissive_registry(permitted: &Arc<AtomicBool>) -> AgentToolRegistry {
        let permission_state = Arc::clone(permitted);
        AgentToolRegistry::new().with_local_permission_checker(
            move |permission: LocalNodePermission| {
                Ok(permission == LocalNodePermission::ExternalIntegrations
                    && permission_state.load(Ordering::SeqCst))
            },
        )
    }

    fn test_session() -> InteractionSession {
        InteractionSession::new(
            SessionId::from_legacy_named("mcp_test_session").expect("session id"),
            NodeId::from_legacy_named("mcp_test_node").expect("node id"),
            InteractionSurface::new("mcp.test", InteractionSurfaceKind::CommandLine),
        )
    }

    fn search_call() -> AgentToolCall {
        AgentToolCall {
            call_id: Some("mcp-call-1".to_string()),
            tool: "mcp__mock__search".to_string(),
            input: serde_json::json!({ "query": "sinomo", "region": "us-west1" }),
            r#async: false,
            summary: "search MCP".to_string(),
        }
    }

    #[test]
    fn sse_parser_collects_json_data_events() {
        let values = parse_sse_json_values(
            "event: message\r\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\r\n\r\n",
        )
        .expect("parse SSE");
        assert_eq!(values.len(), 1);
        assert_eq!(values[0]["id"], 1);
    }

    #[test]
    fn exposed_names_are_namespaced_and_protocol_safe() {
        assert_eq!(
            exposed_mcp_tool_name("GitHub Server", "issues/create"),
            "mcp__github_server__issues_create"
        );
        let long = exposed_mcp_tool_name(
            "long_server_name_that_still_passes_agent_definition_validation",
            "tool_name_that_is_far_too_long_for_provider_function_name_limits",
        );
        assert_eq!(long.len(), 64);
        assert!(long.starts_with("mcp__long_server_name"));
    }

    #[test]
    fn dynamic_descriptor_is_shared_and_requires_local_authority() {
        let tool = McpAgentTool {
            client: test_client("docs"),
            definition: McpToolDefinition {
                name: "search".to_string(),
                title: Some("Doc Search".to_string()),
                description: Some("Search docs".to_string()),
                input_schema: serde_json::json!({ "type": "object" }),
                output_schema: None,
                annotations: McpToolAnnotations {
                    read_only_hint: Some(true),
                    ..McpToolAnnotations::default()
                },
            },
            exposed_name: "mcp__docs__search".to_string(),
        };
        let descriptor = tool.descriptor();
        assert!(descriptor.supports_invocation_mode(AgentToolInvocationMode::TurnBased));
        assert!(descriptor.supports_invocation_mode(AgentToolInvocationMode::Realtime));
        assert_eq!(
            descriptor.required_local_permissions,
            vec![EXTERNAL_INTEGRATIONS_PERMISSION.to_string()]
        );
        assert!(descriptor.supports_parallel);
    }

    #[test]
    fn header_values_use_the_base64_sentinel_only_when_required() {
        assert_eq!(encoded_mcp_header_value("us-west1"), "us-west1");
        assert_eq!(
            encoded_mcp_header_value("Hello, 世界"),
            "=?base64?SGVsbG8sIOS4lueVjA==?="
        );
        assert_eq!(
            encoded_mcp_header_value(" padded "),
            "=?base64?IHBhZGRlZCA=?="
        );
        assert_eq!(
            encoded_mcp_header_value("line1\nline2"),
            "=?base64?bGluZTEKbGluZTI=?="
        );
        assert_eq!(
            encoded_mcp_header_value("=?base64?literal?="),
            "=?base64?PT9iYXNlNjQ/bGl0ZXJhbD89?="
        );
    }

    #[test]
    fn mirrored_header_annotations_are_collected_and_bounded() {
        let params = collect_mcp_header_params(&serde_json::json!({
            "type": "object",
            "properties": {
                "region": { "type": "string", "x-mcp-header": "Region" },
                "page": {
                    "type": "object",
                    "properties": {
                        "index": { "type": "integer", "x-mcp-header": "Page-Index" }
                    }
                },
                "query": { "type": "string" }
            }
        }))
        .expect("collect annotations");
        assert_eq!(
            params,
            vec![
                McpHeaderParam {
                    header: "Page-Index".to_string(),
                    path: vec!["page".to_string(), "index".to_string()],
                },
                McpHeaderParam {
                    header: "Region".to_string(),
                    path: vec!["region".to_string()],
                },
            ]
        );

        let inside_array = collect_mcp_header_params(&serde_json::json!({
            "type": "object",
            "properties": {
                "rows": {
                    "type": "array",
                    "items": { "type": "string", "x-mcp-header": "Row" }
                }
            }
        }));
        assert!(
            inside_array.is_err(),
            "annotations inside `items` are invalid"
        );

        let duplicated = collect_mcp_header_params(&serde_json::json!({
            "type": "object",
            "properties": {
                "left": { "type": "string", "x-mcp-header": "Tenant" },
                "right": { "type": "string", "x-mcp-header": "tenant" }
            }
        }));
        assert!(
            duplicated.is_err(),
            "header names are case-insensitively unique"
        );

        let numeric = collect_mcp_header_params(&serde_json::json!({
            "type": "object",
            "properties": { "ratio": { "type": "number", "x-mcp-header": "Ratio" } }
        }));
        assert!(numeric.is_err(), "`number` parameters may not be mirrored");

        let at_root = collect_mcp_header_params(&serde_json::json!({
            "type": "object",
            "x-mcp-header": "Root"
        }));
        assert!(at_root.is_err(), "the schema root is not a property");
    }

    #[test]
    fn mirrored_header_values_skip_absent_and_non_primitive_arguments() {
        let params = vec![
            McpHeaderParam {
                header: "Region".to_string(),
                path: vec!["region".to_string()],
            },
            McpHeaderParam {
                header: "Retries".to_string(),
                path: vec!["retries".to_string()],
            },
            McpHeaderParam {
                header: "Missing".to_string(),
                path: vec!["missing".to_string()],
            },
            McpHeaderParam {
                header: "Nulled".to_string(),
                path: vec!["nulled".to_string()],
            },
        ];
        let values = mcp_param_header_values(
            &params,
            &serde_json::json!({ "region": "us-west1", "retries": 3, "nulled": null }),
        );
        assert_eq!(
            values,
            vec![
                ("Region".to_string(), "us-west1".to_string()),
                ("Retries".to_string(), "3".to_string()),
            ]
        );
    }

    #[test]
    fn interim_and_unknown_result_types_are_refused() {
        let client = test_client("docs");
        assert!(
            client
                .ensure_complete_result("tools/call", &serde_json::json!({ "content": [] }))
                .is_ok(),
            "an absent resultType means a pre-2026-07-28 server"
        );
        assert!(
            client
                .ensure_complete_result(
                    "tools/call",
                    &serde_json::json!({ "resultType": "complete" })
                )
                .is_ok()
        );
        let interim = client
            .ensure_complete_result(
                "tools/call",
                &serde_json::json!({ "resultType": "input_required" }),
            )
            .expect_err("input_required is not actionable");
        assert!(interim.to_string().contains("additional client input"));
        assert!(
            client
                .ensure_complete_result("tools/call", &serde_json::json!({ "resultType": "later" }))
                .is_err()
        );
    }

    #[test]
    fn stateless_transport_discovers_lists_and_calls_tools_without_a_session() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock MCP server");
        let endpoint = format!(
            "http://{}/mcp",
            listener.local_addr().expect("local address")
        );
        let server = std::thread::spawn(move || {
            let (mut stream, headers, discover) = accept_request(&listener);
            assert_eq!(discover["method"], MCP_DISCOVER_METHOD);
            assert_eq!(
                discover["params"]["_meta"][MCP_META_PROTOCOL_VERSION],
                MCP_MODERN_PROTOCOL_VERSION
            );
            assert_eq!(
                discover["params"]["_meta"][MCP_META_CLIENT_INFO]["name"],
                "sinomo"
            );
            assert!(
                discover["params"]["_meta"][MCP_META_CLIENT_CAPABILITIES].is_object(),
                "client capabilities travel with every request"
            );
            assert!(headers.contains(&format!("mcp-method: {MCP_DISCOVER_METHOD}")));
            assert!(headers.contains(&format!(
                "mcp-protocol-version: {MCP_MODERN_PROTOCOL_VERSION}"
            )));
            reply(&mut stream, &discover["id"], discover_result());

            let (mut stream, headers, list) = accept_request(&listener);
            assert_eq!(list["method"], "tools/list");
            assert!(headers.contains("mcp-method: tools/list"));
            assert!(
                !headers.contains("mcp-session-id"),
                "the stateless revision has no protocol-level session"
            );
            reply(
                &mut stream,
                &list["id"],
                serde_json::json!({
                    "resultType": "complete",
                    "tools": [search_tool()],
                    "ttlMs": 300_000,
                    "cacheScope": "public"
                }),
            );

            let (mut stream, headers, call) = accept_request(&listener);
            assert_eq!(call["method"], "tools/call");
            assert_eq!(call["params"]["name"], "search");
            assert_eq!(call["params"]["arguments"]["query"], "sinomo");
            assert_eq!(
                call["params"]["_meta"][MCP_META_PROTOCOL_VERSION],
                MCP_MODERN_PROTOCOL_VERSION
            );
            assert!(headers.contains("mcp-method: tools/call"));
            assert!(headers.contains("mcp-name: search"));
            assert!(
                headers.contains("mcp-param-region: us-west1"),
                "annotated arguments are mirrored into headers: {headers}"
            );
            let event = serde_json::json!({
                "jsonrpc": "2.0",
                "id": call["id"],
                "result": {
                    "resultType": "complete",
                    "content": [{ "type": "text", "text": "found it" }],
                    "structuredContent": { "matches": 1 }
                }
            });
            let body = format!("event: message\ndata: {event}\n\n");
            respond(&mut stream, "200 OK", "text/event-stream", &body, "");
        });

        let permitted = Arc::new(AtomicBool::new(false));
        let registry = permissive_registry(&permitted);
        let config = AgentMcpServerDefinition::streamable_http("mock", endpoint);
        let registration =
            register_mcp_server_tools(&registry, &config).expect("register MCP tools");
        assert_eq!(registration.tools, vec!["mcp__mock__search"]);
        assert_eq!(registration.protocol_version, MCP_MODERN_PROTOCOL_VERSION);
        assert_eq!(registration.server_info.as_deref(), Some("mock 1.0"));
        assert!(registration.rejected_tools.is_empty());

        let descriptor = registry
            .descriptors()
            .into_iter()
            .find(|descriptor| descriptor.spec.name == "mcp__mock__search")
            .expect("registered descriptor");
        assert!(descriptor.supports_invocation_mode(AgentToolInvocationMode::TurnBased));
        assert!(descriptor.supports_parallel);

        let session = test_session();
        let call = search_call();
        let denied = registry.execute(&session, 0, 0, &call);
        assert_eq!(denied.status, AgentToolStatus::Blocked);
        assert_eq!(denied.output["error"], "local_permission_denied");

        permitted.store(true, Ordering::SeqCst);
        let result = registry.execute(&session, 0, 1, &call);
        assert_eq!(result.status, AgentToolStatus::Succeeded);
        assert_eq!(result.output["content"][0]["text"], "found it");
        assert_eq!(result.output["structured_content"]["matches"], 1);
        server.join().expect("mock MCP server");
    }

    #[test]
    fn handshake_servers_are_detected_and_kept_working() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock MCP server");
        let endpoint = format!(
            "http://{}/mcp",
            listener.local_addr().expect("local address")
        );
        let server = std::thread::spawn(move || {
            // A handshake-era server rejects anything sent before `initialize`.
            let (mut stream, _, discover) = accept_request(&listener);
            assert_eq!(discover["method"], MCP_DISCOVER_METHOD);
            fail(
                &mut stream,
                "400 Bad Request",
                Value::Null,
                serde_json::json!({
                    "code": -32600,
                    "message": "Bad Request: Server not initialized"
                }),
            );

            let (mut stream, _, initialize) = accept_request(&listener);
            assert_eq!(initialize["method"], "initialize");
            assert_eq!(
                initialize["params"]["protocolVersion"],
                MCP_LEGACY_PROTOCOL_VERSION
            );
            assert!(
                initialize["params"]["_meta"].is_null(),
                "handshake requests carry no per-request protocol metadata"
            );
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": initialize["id"],
                "result": {
                    "protocolVersion": MCP_LEGACY_PROTOCOL_VERSION,
                    "capabilities": { "tools": {} },
                    "serverInfo": { "name": "legacy", "version": "0.9" }
                }
            })
            .to_string();
            respond(
                &mut stream,
                "200 OK",
                "application/json",
                &body,
                "mcp-session-id: test-session\r\n",
            );

            let (mut stream, headers, initialized) = accept_request(&listener);
            assert_eq!(initialized["method"], "notifications/initialized");
            assert!(headers.contains("mcp-session-id: test-session"));
            assert!(headers.contains(&format!(
                "mcp-protocol-version: {MCP_LEGACY_PROTOCOL_VERSION}"
            )));
            respond(&mut stream, "202 Accepted", "application/json", "", "");

            let (mut stream, headers, list) = accept_request(&listener);
            assert_eq!(list["method"], "tools/list");
            assert!(headers.contains("mcp-session-id: test-session"));
            assert!(
                !headers.contains("mcp-method:"),
                "handshake requests do not mirror body fields into headers"
            );
            reply(
                &mut stream,
                &list["id"],
                serde_json::json!({ "tools": [search_tool()] }),
            );

            let (mut stream, headers, call) = accept_request(&listener);
            assert_eq!(call["method"], "tools/call");
            assert!(headers.contains("mcp-session-id: test-session"));
            assert!(!headers.contains("mcp-param-region:"));
            reply(
                &mut stream,
                &call["id"],
                serde_json::json!({ "content": [{ "type": "text", "text": "legacy hit" }] }),
            );
        });

        let permitted = Arc::new(AtomicBool::new(true));
        let registry = permissive_registry(&permitted);
        let config = AgentMcpServerDefinition::streamable_http("mock", endpoint);
        let registration =
            register_mcp_server_tools(&registry, &config).expect("register MCP tools");
        assert_eq!(registration.protocol_version, MCP_LEGACY_PROTOCOL_VERSION);
        assert_eq!(registration.server_info.as_deref(), Some("legacy 0.9"));

        let result = registry.execute(&test_session(), 0, 0, &search_call());
        assert_eq!(result.status, AgentToolStatus::Succeeded);
        assert_eq!(result.output["content"][0]["text"], "legacy hit");
        server.join().expect("mock MCP server");
    }

    #[test]
    fn explicit_handshake_era_skips_the_stateless_probe() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock MCP server");
        let endpoint = format!(
            "http://{}/mcp",
            listener.local_addr().expect("local address")
        );
        let server = std::thread::spawn(move || {
            let (mut stream, _, first) = accept_request(&listener);
            assert_eq!(
                first["method"], "initialize",
                "a pinned handshake era must not probe with {MCP_DISCOVER_METHOD}"
            );
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": first["id"],
                "result": {
                    "protocolVersion": MCP_LEGACY_PROTOCOL_VERSION,
                    "capabilities": { "tools": {} }
                }
            })
            .to_string();
            respond(&mut stream, "200 OK", "application/json", &body, "");

            let (mut stream, _, initialized) = accept_request(&listener);
            assert_eq!(initialized["method"], "notifications/initialized");
            respond(&mut stream, "202 Accepted", "application/json", "", "");

            let (mut stream, _, list) = accept_request(&listener);
            assert_eq!(list["method"], "tools/list");
            reply(&mut stream, &list["id"], serde_json::json!({ "tools": [] }));
        });

        let mut config = AgentMcpServerDefinition::streamable_http("mock", endpoint);
        config.protocol_era = AgentMcpProtocolEra::Legacy;
        let registration = register_mcp_server_tools(&AgentToolRegistry::new(), &config)
            .expect("register MCP tools");
        assert!(registration.tools.is_empty());
        assert_eq!(registration.protocol_version, MCP_LEGACY_PROTOCOL_VERSION);
        server.join().expect("mock MCP server");
    }

    #[test]
    fn an_unsupported_protocol_version_is_reported_rather_than_downgraded() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock MCP server");
        let endpoint = format!(
            "http://{}/mcp",
            listener.local_addr().expect("local address")
        );
        let server = std::thread::spawn(move || {
            let (mut stream, _, discover) = accept_request(&listener);
            fail(
                &mut stream,
                "400 Bad Request",
                discover["id"].clone(),
                serde_json::json!({
                    "code": MCP_ERROR_UNSUPPORTED_PROTOCOL_VERSION,
                    "message": "Unsupported protocol version",
                    "data": {
                        "supported": ["2031-01-01"],
                        "requested": MCP_MODERN_PROTOCOL_VERSION
                    }
                }),
            );
        });

        let mut config = AgentMcpServerDefinition::streamable_http("mock", endpoint);
        config.protocol_era = AgentMcpProtocolEra::Modern;
        let error = register_mcp_server_tools(&AgentToolRegistry::new(), &config)
            .expect_err("no mutually supported version exists");
        assert!(
            matches!(error, AgentMcpError::UnsupportedProtocolVersion { .. }),
            "unexpected error: {error}"
        );
        assert!(error.to_string().contains("2031-01-01"));
        server.join().expect("mock MCP server");
    }

    #[test]
    fn tools_with_malformed_header_annotations_are_dropped_individually() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock MCP server");
        let endpoint = format!(
            "http://{}/mcp",
            listener.local_addr().expect("local address")
        );
        let server = std::thread::spawn(move || {
            let (mut stream, _, discover) = accept_request(&listener);
            reply(&mut stream, &discover["id"], discover_result());

            let (mut stream, _, list) = accept_request(&listener);
            reply(
                &mut stream,
                &list["id"],
                serde_json::json!({
                    "resultType": "complete",
                    "tools": [
                        search_tool(),
                        {
                            "name": "broken",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "bad": { "type": "string", "x-mcp-header": "not a token" }
                                }
                            }
                        }
                    ]
                }),
            );
        });

        let config = AgentMcpServerDefinition::streamable_http("mock", endpoint);
        let registration = register_mcp_server_tools(&AgentToolRegistry::new(), &config)
            .expect("register MCP tools");
        assert_eq!(registration.tools, vec!["mcp__mock__search"]);
        assert_eq!(registration.rejected_tools.len(), 1);
        assert!(
            registration.rejected_tools[0].contains("`broken`"),
            "unexpected rejection: {:?}",
            registration.rejected_tools
        );
        server.join().expect("mock MCP server");
    }
}
