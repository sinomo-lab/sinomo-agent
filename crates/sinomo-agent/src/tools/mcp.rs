use super::{
    AgentTool, AgentToolCapability, AgentToolDescriptor, AgentToolEvidence,
    AgentToolInvocationMode, AgentToolRegistry, AgentToolRequest, AgentToolResult,
};
use crate::{AgentMcpServerDefinition, AgentToolKind, AgentToolSpec};
use reqwest::blocking::{Client, Response};
use reqwest::header::{
    ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeSet;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const MCP_SUPPORTED_PROTOCOL_VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];
const MCP_SESSION_ID_HEADER: &str = "mcp-session-id";
const MCP_PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";
const MCP_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MCP_MAX_TOOLS: usize = 256;
const MCP_MAX_LIST_PAGES: usize = 20;
const EXTERNAL_INTEGRATIONS_PERMISSION: &str = "external-integrations";

#[derive(Debug, Error)]
pub enum AgentMcpError {
    #[error("MCP server `{server}` configuration is invalid: {detail}")]
    Configuration { server: String, detail: String },
    #[error("MCP server `{server}` request failed: {detail}")]
    Request { server: String, detail: String },
    #[error("MCP server `{server}` returned an invalid protocol response: {detail}")]
    Protocol { server: String, detail: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentMcpRegistration {
    pub server: String,
    pub tools: Vec<String>,
}

#[derive(Debug, Default)]
struct McpClientState {
    session_id: Option<String>,
    protocol_version: Option<String>,
    next_request_id: u64,
}

#[derive(Debug)]
struct StreamableHttpMcpClient {
    server_name: String,
    url: String,
    http: Client,
    default_headers: HeaderMap,
    state: Mutex<McpClientState>,
    startup_timeout: Duration,
    tool_timeout: Duration,
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
                next_request_id: 1,
                ..McpClientState::default()
            }),
            startup_timeout,
            tool_timeout,
        });
        let result = client.rpc(
            "initialize",
            Some(serde_json::json!({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {
                    "name": "sinomo",
                    "version": env!("CARGO_PKG_VERSION"),
                }
            })),
            startup_timeout,
        )?;
        let protocol_version = result
            .get("protocolVersion")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| AgentMcpError::Protocol {
                server: client.server_name.clone(),
                detail: "initialize result omitted protocolVersion".to_string(),
            })?
            .to_string();
        if !MCP_SUPPORTED_PROTOCOL_VERSIONS.contains(&protocol_version.as_str()) {
            return Err(client.protocol_error(format!(
                "server selected unsupported protocol version `{protocol_version}`"
            )));
        }
        client
            .state
            .lock()
            .map_err(|_| client.protocol_error("client state lock poisoned"))?
            .protocol_version = Some(protocol_version);
        client.notify("notifications/initialized", None, startup_timeout)?;
        Ok(client)
    }

    fn list_tools(&self) -> Result<Vec<McpToolDefinition>, AgentMcpError> {
        let mut tools = Vec::new();
        let mut cursor = None;
        for _ in 0..MCP_MAX_LIST_PAGES {
            let params = cursor
                .as_deref()
                .map(|cursor| serde_json::json!({ "cursor": cursor }));
            let value = self.rpc("tools/list", params, self.startup_timeout)?;
            let page = serde_json::from_value::<McpListToolsResult>(value).map_err(|source| {
                self.protocol_error(format!("tools/list result is invalid: {source}"))
            })?;
            if tools.len().saturating_add(page.tools.len()) > MCP_MAX_TOOLS {
                return Err(self.protocol_error(format!(
                    "tools/list exceeded the {MCP_MAX_TOOLS}-tool bound"
                )));
            }
            tools.extend(page.tools);
            match page.next_cursor {
                Some(next) if Some(next.as_str()) != cursor.as_deref() => cursor = Some(next),
                Some(_) => {
                    return Err(self.protocol_error("tools/list cursor did not advance"));
                }
                None => return Ok(tools),
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
        let value = self.rpc(
            "tools/call",
            Some(serde_json::json!({
                "name": name,
                "arguments": arguments,
            })),
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
        timeout: Duration,
    ) -> Result<Value, AgentMcpError> {
        let (request_id, session_id, protocol_version) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| self.protocol_error("client state lock poisoned"))?;
            let request_id = state.next_request_id;
            state.next_request_id = state.next_request_id.saturating_add(1);
            (
                request_id,
                state.session_id.clone(),
                state.protocol_version.clone(),
            )
        };
        let mut payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
        });
        if let Some(params) = params {
            payload["params"] = params;
        }
        let response = self.send(payload, session_id, protocol_version, timeout)?;
        let (session_id, values) = read_mcp_response(&self.server_name, response)?;
        if let Some(session_id) = session_id {
            self.state
                .lock()
                .map_err(|_| self.protocol_error("client state lock poisoned"))?
                .session_id = Some(session_id);
        }
        let response = values
            .into_iter()
            .flat_map(|value| match value {
                Value::Array(values) => values,
                value => vec![value],
            })
            .find(|value| value.get("id").and_then(Value::as_u64) == Some(request_id))
            .ok_or_else(|| {
                self.protocol_error(format!("response omitted JSON-RPC id {request_id}"))
            })?;
        if let Some(error) = response.get("error") {
            return Err(self.request_error(format!("{method}: {error}")));
        }
        response
            .get("result")
            .cloned()
            .ok_or_else(|| self.protocol_error(format!("{method} response omitted result")))
    }

    fn notify(
        &self,
        method: &str,
        params: Option<Value>,
        timeout: Duration,
    ) -> Result<(), AgentMcpError> {
        let (session_id, protocol_version) = {
            let state = self
                .state
                .lock()
                .map_err(|_| self.protocol_error("client state lock poisoned"))?;
            (state.session_id.clone(), state.protocol_version.clone())
        };
        let mut payload = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
        });
        if let Some(params) = params {
            payload["params"] = params;
        }
        self.send(payload, session_id, protocol_version, timeout)?;
        Ok(())
    }

    fn send(
        &self,
        payload: Value,
        session_id: Option<String>,
        protocol_version: Option<String>,
        timeout: Duration,
    ) -> Result<Response, AgentMcpError> {
        let mut request = self
            .http
            .post(&self.url)
            .headers(self.default_headers.clone())
            .timeout(timeout)
            .json(&payload);
        if let Some(session_id) = session_id {
            request = request.header(MCP_SESSION_ID_HEADER, session_id);
        }
        if let Some(protocol_version) = protocol_version {
            request = request.header(MCP_PROTOCOL_VERSION_HEADER, protocol_version);
        }
        let response = request
            .send()
            .map_err(|source| self.request_error(source.to_string()))?;
        if response.status().is_success() {
            Ok(response)
        } else {
            let status = response.status();
            let body = read_bounded_response_body(response).unwrap_or_default();
            Err(self.request_error(format!(
                "HTTP {status}: {}",
                String::from_utf8_lossy(&body).trim()
            )))
        }
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

#[derive(Clone)]
struct McpAgentTool {
    client: Arc<StreamableHttpMcpClient>,
    definition: McpToolDefinition,
    exposed_name: String,
}

impl AgentTool for McpAgentTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        let description = self
            .definition
            .description
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| format!("MCP `{}`: {value}", self.client.server_name))
            .unwrap_or_else(|| {
                format!(
                    "Call `{}` on MCP server `{}`.",
                    self.definition.name, self.client.server_name
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
        let evidence = AgentToolEvidence::availability_only(
            true,
            "executed through a configured Streamable HTTP MCP server after node-local authorization",
        )
        .with_output_value(&output, false)
        .with_metadata(serde_json::json!({
            "mcp_server": self.client.server_name,
            "mcp_tool": self.definition.name,
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
        return Ok(AgentMcpRegistration {
            server: config.name.clone(),
            tools: Vec::new(),
        });
    }
    let client = StreamableHttpMcpClient::connect(config)?;
    let allowed = config
        .allowed_tools
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let discovered = client.list_tools()?;
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
    Ok(AgentMcpRegistration {
        server: config.name.clone(),
        tools,
    })
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

fn read_mcp_response(
    server: &str,
    response: Response,
) -> Result<(Option<String>, Vec<Value>), AgentMcpError> {
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
    if body.is_empty() {
        return Err(AgentMcpError::Protocol {
            server: server.to_string(),
            detail: "response body was empty".to_string(),
        });
    }
    if let Ok(value) = serde_json::from_slice::<Value>(&body) {
        return Ok((session_id, vec![value]));
    }
    let text = String::from_utf8(body).map_err(|source| AgentMcpError::Protocol {
        server: server.to_string(),
        detail: format!("response was neither JSON nor UTF-8 SSE: {source}"),
    })?;
    let values = parse_sse_json_values(&text).map_err(|detail| AgentMcpError::Protocol {
        server: server.to_string(),
        detail,
    })?;
    if values.is_empty() {
        return Err(AgentMcpError::Protocol {
            server: server.to_string(),
            detail: "SSE response contained no JSON data events".to_string(),
        });
    }
    Ok((session_id, values))
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
        let client = Arc::new(StreamableHttpMcpClient {
            server_name: "docs".to_string(),
            url: "http://127.0.0.1:1/mcp".to_string(),
            http: Client::new(),
            default_headers: HeaderMap::new(),
            state: Mutex::new(McpClientState::default()),
            startup_timeout: Duration::from_secs(1),
            tool_timeout: Duration::from_secs(1),
        });
        let tool = McpAgentTool {
            client,
            definition: McpToolDefinition {
                name: "search".to_string(),
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
    fn streamable_http_initializes_lists_and_calls_tools_with_session_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock MCP server");
        let endpoint = format!(
            "http://{}/mcp",
            listener.local_addr().expect("local address")
        );
        let server = std::thread::spawn(move || {
            let (mut stream, _, initialize) = accept_request(&listener);
            assert_eq!(initialize["method"], "initialize");
            assert_eq!(
                initialize["params"]["protocolVersion"],
                MCP_PROTOCOL_VERSION
            );
            let initialize_id = initialize["id"].as_u64().expect("initialize id");
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": initialize_id,
                "result": {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": { "tools": {} },
                    "serverInfo": { "name": "mock", "version": "1.0" }
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
                "mcp-protocol-version: {}",
                MCP_PROTOCOL_VERSION.to_ascii_lowercase()
            )));
            respond(&mut stream, "202 Accepted", "application/json", "", "");

            let (mut stream, headers, list) = accept_request(&listener);
            assert_eq!(list["method"], "tools/list");
            assert!(headers.contains("mcp-session-id: test-session"));
            let list_id = list["id"].as_u64().expect("list id");
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": list_id,
                "result": {
                    "tools": [{
                        "name": "search",
                        "description": "Search the mock index",
                        "inputSchema": {
                            "type": "object",
                            "properties": { "query": { "type": "string" } },
                            "required": ["query"]
                        },
                        "annotations": { "readOnlyHint": true }
                    }]
                }
            })
            .to_string();
            respond(&mut stream, "200 OK", "application/json", &body, "");

            let (mut stream, headers, call) = accept_request(&listener);
            assert_eq!(call["method"], "tools/call");
            assert_eq!(call["params"]["name"], "search");
            assert_eq!(call["params"]["arguments"]["query"], "sinomo");
            assert!(headers.contains("mcp-session-id: test-session"));
            let call_id = call["id"].as_u64().expect("call id");
            let event = serde_json::json!({
                "jsonrpc": "2.0",
                "id": call_id,
                "result": {
                    "content": [{ "type": "text", "text": "found it" }],
                    "structuredContent": { "matches": 1 }
                }
            });
            let body = format!("event: message\ndata: {event}\n\n");
            respond(&mut stream, "200 OK", "text/event-stream", &body, "");
        });

        let permitted = Arc::new(AtomicBool::new(false));
        let permission_state = Arc::clone(&permitted);
        let registry = AgentToolRegistry::new().with_local_permission_checker(
            move |permission: LocalNodePermission| {
                Ok(permission == LocalNodePermission::ExternalIntegrations
                    && permission_state.load(Ordering::SeqCst))
            },
        );
        let config = AgentMcpServerDefinition::streamable_http("mock", endpoint);
        let registration =
            register_mcp_server_tools(&registry, &config).expect("register MCP tools");
        assert_eq!(registration.tools, vec!["mcp__mock__search"]);
        let descriptor = registry
            .descriptors()
            .into_iter()
            .find(|descriptor| descriptor.spec.name == "mcp__mock__search")
            .expect("registered descriptor");
        assert!(descriptor.supports_invocation_mode(AgentToolInvocationMode::TurnBased));
        assert!(descriptor.supports_invocation_mode(AgentToolInvocationMode::Realtime));
        assert!(descriptor.supports_parallel);

        let session = InteractionSession::new(
            SessionId::from_legacy_named("mcp_test_session").expect("session id"),
            NodeId::from_legacy_named("mcp_test_node").expect("node id"),
            InteractionSurface::new("mcp.test", InteractionSurfaceKind::CommandLine),
        );
        let call = AgentToolCall {
            call_id: Some("mcp-call-1".to_string()),
            tool: "mcp__mock__search".to_string(),
            input: serde_json::json!({ "query": "sinomo" }),
            r#async: false,
            summary: "search MCP".to_string(),
        };
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
}
