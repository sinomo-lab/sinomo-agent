use super::{
    AgentToolCapability, AgentToolDescriptor, AgentToolGateway, AgentToolGatewayCancellation,
    AgentToolGatewayTask, AgentToolRegistry, AgentToolRequest, AgentToolResult,
};
use crate::{AgentToolKind, AgentToolSpec};
use sinomo_runtime::module::{MODULE_NATIVE_DYLIB_CANCEL_ROUTE, MODULE_RPC_CATEGORY};
use sinomo_runtime::network::{ObjectRpcCancelRequest, ObjectRpcRequest, ObjectRpcResponse};
use sinomo_runtime::remote_management::{
    CONFIG_CATALOG_ROUTE, CONFIG_GET_ROUTE, CONFIG_RPC_CATEGORY, CONFIG_SEARCH_ROUTE,
    CONFIG_SET_ROUTE, ConfigCatalogRequest, ConfigCatalogResponse, ConfigGetRequest,
    ConfigGetResponse, ConfigSearchRequest, ConfigSearchResponse, ConfigSetRequest,
    ConfigSetResponse, REMOTE_MANAGEMENT_FS_LIST_ROUTE, REMOTE_MANAGEMENT_FS_READ_ROUTE,
    REMOTE_MANAGEMENT_FS_STAT_ROUTE, REMOTE_MANAGEMENT_FS_WRITE_ROUTE,
    REMOTE_MANAGEMENT_RPC_CATEGORY, REMOTE_MANAGEMENT_SHELL_RUN_ROUTE,
    REMOTE_MANAGEMENT_SYSTEM_INFO_ROUTE, REMOTE_MANAGEMENT_SYSTEM_OBSERVE_ROUTE,
    RemoteManagementFileListRequest, RemoteManagementFileListResponse,
    RemoteManagementFileReadRequest, RemoteManagementFileReadResponse,
    RemoteManagementFileStatRequest, RemoteManagementFileStatResponse,
    RemoteManagementFileWriteRequest, RemoteManagementFileWriteResponse,
    RemoteManagementShellRunRequest, RemoteManagementShellRunResponse,
    RemoteManagementSystemInfoRequest, RemoteManagementSystemInfoResponse,
    RemoteManagementSystemObserveRequest, RemoteManagementSystemObserveResponse,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub trait AgentRemoteObjectRpcTransport: Send + Sync {
    /// Calls an object-RPC route using the transport's legacy/default target.
    ///
    /// This remains the required method so existing closures and transport
    /// implementations stay source compatible.
    fn call(&self, request: ObjectRpcRequest) -> Result<ObjectRpcResponse, String>;

    /// Calls an object-RPC route using explicit cluster-routing hints.
    ///
    /// Legacy transports fall back to [`Self::call`] only when no routing hint
    /// is present; explicit targets fail closed unless a cluster-aware host
    /// overrides this method. The same hints are supplied for the work request
    /// and its cancellation.
    fn call_targeted(
        &self,
        target_node_id: Option<&str>,
        route_key: Option<&str>,
        request: ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, String> {
        if target_node_id.is_some() || route_key.is_some() {
            return Err(
                "remote transport does not support explicit target_node_id or route_key routing"
                    .to_string(),
            );
        }
        self.call(request)
    }
}

impl<F> AgentRemoteObjectRpcTransport for F
where
    F: Fn(ObjectRpcRequest) -> Result<ObjectRpcResponse, String> + Send + Sync,
{
    fn call(&self, request: ObjectRpcRequest) -> Result<ObjectRpcResponse, String> {
        self(request)
    }
}

/// Adapts a target-aware closure without conflicting with the legacy
/// `Fn(ObjectRpcRequest)` transport implementation.
pub struct TargetAwareAgentRemoteObjectRpcTransport<F> {
    call: F,
}

impl<F> TargetAwareAgentRemoteObjectRpcTransport<F> {
    pub fn new(call: F) -> Self {
        Self { call }
    }
}

impl<F> AgentRemoteObjectRpcTransport for TargetAwareAgentRemoteObjectRpcTransport<F>
where
    F: Fn(Option<&str>, Option<&str>, ObjectRpcRequest) -> Result<ObjectRpcResponse, String>
        + Send
        + Sync,
{
    fn call(&self, request: ObjectRpcRequest) -> Result<ObjectRpcResponse, String> {
        (self.call)(None, None, request)
    }

    fn call_targeted(
        &self,
        target_node_id: Option<&str>,
        route_key: Option<&str>,
        request: ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, String> {
        (self.call)(target_node_id, route_key, request)
    }
}

#[derive(Clone)]
pub struct AgentRemoteObjectRpcTool {
    descriptor: AgentToolDescriptor,
    route: String,
    category: String,
    cancel_route: String,
    cancel_category: String,
    transport: Arc<dyn AgentRemoteObjectRpcTransport>,
}

impl AgentRemoteObjectRpcTool {
    pub fn new(
        descriptor: AgentToolDescriptor,
        route: impl Into<String>,
        category: impl Into<String>,
        cancel_route: impl Into<String>,
        cancel_category: impl Into<String>,
        transport: Arc<dyn AgentRemoteObjectRpcTransport>,
    ) -> Self {
        Self {
            descriptor,
            route: route.into(),
            category: category.into(),
            cancel_route: cancel_route.into(),
            cancel_category: cancel_category.into(),
            transport,
        }
    }

    pub fn json_module_tool(
        name: impl Into<String>,
        description: impl Into<String>,
        route: impl Into<String>,
        category: impl Into<String>,
        cancel_route: impl Into<String>,
        cancel_category: impl Into<String>,
        transport: Arc<dyn AgentRemoteObjectRpcTransport>,
    ) -> Self {
        let descriptor = AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ModuleObjectRpc,
            name,
            description,
            false,
        ))
        .with_toolsets(["module", "object_rpc", "remote"])
        .with_capabilities([AgentToolCapability::CallModule])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "properties": {
                "target_node_id": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Optional exact cluster node id for the invocation."
                },
                "route_key": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Optional cluster routing key for service placement."
                },
                "input": {
                    "description": "Route-specific object-RPC request payload."
                }
            },
            "additionalProperties": false
        }))
        .with_output_schema(serde_json::json!({
            "type": "object",
            "properties": {
                "response": {
                    "description": "Route-specific object-RPC response payload."
                }
            }
        }));
        Self::new(
            descriptor,
            route,
            category,
            cancel_route,
            cancel_category,
            transport,
        )
    }
}

/// One executable agent projection of a shared remote-management contract.
///
/// These definitions intentionally carry no initiating-node local permission:
/// the receiving daemon re-checks its own node-local remote-management grant
/// at the final host-effect boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentRemoteManagementToolDefinition {
    pub descriptor: AgentToolDescriptor,
    pub route: &'static str,
    pub category: &'static str,
}

/// Returns the built-in agent surface for remote shell, file transfer,
/// observation, and node configuration.
pub fn remote_management_agent_tool_definitions() -> Vec<AgentRemoteManagementToolDefinition> {
    vec![
        remote_management_tool_definition::<
            RemoteManagementShellRunRequest,
            RemoteManagementShellRunResponse,
        >(
            "remote_shell",
            "Run a shell command on a selected remote node. The receiving node must locally allow remote management.",
            REMOTE_MANAGEMENT_SHELL_RUN_ROUTE,
            REMOTE_MANAGEMENT_RPC_CATEGORY,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string", "minLength": 1 },
                    "cwd": { "type": "string" },
                    "shell": { "type": "string" },
                    "timeout_ms": { "type": "integer", "minimum": 1 },
                    "max_output_bytes": { "type": "integer", "minimum": 1 },
                    "env": { "type": "object", "additionalProperties": { "type": "string" } }
                },
                "required": ["command"],
                "additionalProperties": false
            }),
            "Remote shell outcome including exit status, stdout, stderr, duration, and truncation flags.",
            [
                AgentToolCapability::AccessNetwork,
                AgentToolCapability::CallModule,
                AgentToolCapability::ExecuteProcess,
            ],
        ),
        remote_management_tool_definition::<
            RemoteManagementFileListRequest,
            RemoteManagementFileListResponse,
        >(
            "remote_file_list",
            "List files and directories on a selected remote node.",
            REMOTE_MANAGEMENT_FS_LIST_ROUTE,
            REMOTE_MANAGEMENT_RPC_CATEGORY,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "minLength": 1 },
                    "recursive": { "type": "boolean" },
                    "max_entries": { "type": "integer", "minimum": 1 }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
            "Remote directory entries and whether the bounded listing was truncated.",
            [
                AgentToolCapability::AccessNetwork,
                AgentToolCapability::CallModule,
                AgentToolCapability::ReadWorkspace,
            ],
        ),
        remote_management_tool_definition::<
            RemoteManagementFileStatRequest,
            RemoteManagementFileStatResponse,
        >(
            "remote_file_stat",
            "Inspect metadata and optionally a BLAKE3 digest for a file on a selected remote node.",
            REMOTE_MANAGEMENT_FS_STAT_ROUTE,
            REMOTE_MANAGEMENT_RPC_CATEGORY,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "minLength": 1 },
                    "include_blake3": { "type": "boolean" }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
            "Remote file type, length, permissions, modification time, and optional BLAKE3 digest.",
            [
                AgentToolCapability::AccessNetwork,
                AgentToolCapability::CallModule,
                AgentToolCapability::ReadWorkspace,
            ],
        ),
        remote_management_tool_definition::<
            RemoteManagementFileReadRequest,
            RemoteManagementFileReadResponse,
        >(
            "remote_file_read",
            "Read a bounded byte range from a file on a selected remote node.",
            REMOTE_MANAGEMENT_FS_READ_ROUTE,
            REMOTE_MANAGEMENT_RPC_CATEGORY,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "minLength": 1 },
                    "offset": { "type": "integer", "minimum": 0 },
                    "max_bytes": { "type": "integer", "minimum": 1 }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
            "Remote file bytes, optional UTF-8 text, total length, and truncation state.",
            [
                AgentToolCapability::AccessNetwork,
                AgentToolCapability::CallModule,
                AgentToolCapability::ReadWorkspace,
            ],
        ),
        remote_management_tool_definition::<
            RemoteManagementFileWriteRequest,
            RemoteManagementFileWriteResponse,
        >(
            "remote_file_write",
            "Write a bounded, optionally retry-safe byte chunk to a file on a selected remote node.",
            REMOTE_MANAGEMENT_FS_WRITE_ROUTE,
            REMOTE_MANAGEMENT_RPC_CATEGORY,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "minLength": 1 },
                    "bytes": {
                        "type": "array",
                        "items": { "type": "integer", "minimum": 0, "maximum": 255 }
                    },
                    "create_parent": { "type": "boolean" },
                    "append": { "type": "boolean" },
                    "offset": { "type": "integer", "minimum": 0 },
                    "truncate": { "type": "boolean" }
                },
                "required": ["path", "bytes"],
                "additionalProperties": false
            }),
            "Remote write result including bytes written, effective offset, next offset, and resulting length.",
            [
                AgentToolCapability::AccessNetwork,
                AgentToolCapability::CallModule,
                AgentToolCapability::WriteWorkspace,
            ],
        ),
        remote_management_tool_definition::<
            RemoteManagementSystemInfoRequest,
            RemoteManagementSystemInfoResponse,
        >(
            "remote_system_info",
            "Inspect stable operating-system and process-environment facts on a selected remote node.",
            REMOTE_MANAGEMENT_SYSTEM_INFO_ROUTE,
            REMOTE_MANAGEMENT_RPC_CATEGORY,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "include_env": { "type": "boolean" },
                    "env_names": { "type": "array", "items": { "type": "string" } }
                },
                "additionalProperties": false
            }),
            "Remote OS, architecture, executable, working directory, shell, and explicitly requested environment facts.",
            [
                AgentToolCapability::AccessNetwork,
                AgentToolCapability::CallModule,
                AgentToolCapability::ObserveEnvironment,
            ],
        ),
        remote_management_tool_definition::<
            RemoteManagementSystemObserveRequest,
            RemoteManagementSystemObserveResponse,
        >(
            "remote_system_observe",
            "Observe bounded live CPU, memory, process, disk, network, verified device state, and resource-exposure policy on a selected remote node.",
            REMOTE_MANAGEMENT_SYSTEM_OBSERVE_ROUTE,
            REMOTE_MANAGEMENT_RPC_CATEGORY,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "include_processes": { "type": "boolean" },
                    "max_processes": { "type": "integer", "minimum": 1 },
                    "include_disks": { "type": "boolean" },
                    "include_network": { "type": "boolean" },
                    "include_devices": {
                        "type": "boolean",
                        "description": "Request verified native-device inventory when the receiving host provides it."
                    },
                    "include_resource_exposures": {
                        "type": "boolean",
                        "description": "Request node-local resource-exposure policy declarations separately from verified device inventory."
                    }
                },
                "additionalProperties": false
            }),
            "A bounded remote system observation with verified devices and node-local resource-exposure policies represented separately.",
            [
                AgentToolCapability::AccessNetwork,
                AgentToolCapability::CallModule,
                AgentToolCapability::ObserveEnvironment,
            ],
        ),
        remote_management_tool_definition::<ConfigCatalogRequest, ConfigCatalogResponse>(
            "remote_config_catalog",
            "List configuration definitions known to a selected remote node, optionally filtered by owner.",
            CONFIG_CATALOG_ROUTE,
            CONFIG_RPC_CATEGORY,
            serde_json::json!({
                "type": "object",
                "properties": { "owner": { "type": "string" } },
                "additionalProperties": false
            }),
            "Configuration definitions from the selected node.",
            [
                AgentToolCapability::AccessNetwork,
                AgentToolCapability::ManageConfig,
            ],
        ),
        remote_management_tool_definition::<ConfigSearchRequest, ConfigSearchResponse>(
            "remote_config_search",
            "Search configuration definitions on a selected remote node.",
            CONFIG_SEARCH_ROUTE,
            CONFIG_RPC_CATEGORY,
            serde_json::json!({
                "type": "object",
                "properties": { "query": { "type": "string" } },
                "required": ["query"],
                "additionalProperties": false
            }),
            "Matching configuration definitions from the selected node.",
            [
                AgentToolCapability::AccessNetwork,
                AgentToolCapability::ManageConfig,
            ],
        ),
        remote_management_tool_definition::<ConfigGetRequest, ConfigGetResponse>(
            "remote_config_get",
            "Resolve one configuration value on a selected remote node.",
            CONFIG_GET_ROUTE,
            CONFIG_RPC_CATEGORY,
            serde_json::json!({
                "type": "object",
                "properties": { "key": { "type": "string", "minLength": 1 } },
                "required": ["key"],
                "additionalProperties": false
            }),
            "Resolved configuration value, source, and definition from the selected node.",
            [
                AgentToolCapability::AccessNetwork,
                AgentToolCapability::ManageConfig,
            ],
        ),
        remote_management_tool_definition::<ConfigSetRequest, ConfigSetResponse>(
            "remote_config_set",
            "Set an ordinary configuration value on a selected remote node after that node has locally enabled remote management.",
            CONFIG_SET_ROUTE,
            CONFIG_RPC_CATEGORY,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "key": { "type": "string", "minLength": 1 },
                    "value": {},
                    "target": { "type": "string", "enum": ["auto", "local", "cluster"] }
                },
                "required": ["key", "value"],
                "additionalProperties": false
            }),
            "The configuration value and source written on the selected node.",
            [
                AgentToolCapability::AccessNetwork,
                AgentToolCapability::ManageConfig,
            ],
        ),
    ]
}

/// Returns descriptor-only projections for planners and discovery UIs.
pub fn remote_management_agent_tool_descriptors() -> Vec<AgentToolDescriptor> {
    remote_management_agent_tool_definitions()
        .into_iter()
        .map(|definition| definition.descriptor)
        .collect()
}

/// Builds executable remote-management gateways over one host transport.
pub fn remote_management_agent_tools(
    transport: Arc<dyn AgentRemoteObjectRpcTransport>,
) -> Vec<AgentRemoteObjectRpcTool> {
    remote_management_agent_tool_definitions()
        .into_iter()
        .map(|definition| {
            AgentRemoteObjectRpcTool::new(
                definition.descriptor,
                definition.route,
                definition.category,
                MODULE_NATIVE_DYLIB_CANCEL_ROUTE,
                MODULE_RPC_CATEGORY,
                Arc::clone(&transport),
            )
        })
        .collect()
}

/// Registers the complete executable remote-management surface.
pub fn register_remote_management_agent_tools(
    registry: &AgentToolRegistry,
    transport: Arc<dyn AgentRemoteObjectRpcTransport>,
) {
    for tool in remote_management_agent_tools(transport) {
        registry.register_gateway(tool);
    }
}

fn remote_management_tool_definition<Request, Response>(
    name: &'static str,
    description: &'static str,
    route: &'static str,
    category: &'static str,
    request_schema: serde_json::Value,
    response_description: &'static str,
    capabilities: impl IntoIterator<Item = AgentToolCapability>,
) -> AgentRemoteManagementToolDefinition
where
    Request: serde::Serialize + for<'de> serde::Deserialize<'de>,
    Response: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    // The generic types intentionally keep each agent projection coupled to
    // the authoritative shared wire contract even though models see JSON.
    let _contract = std::marker::PhantomData::<fn() -> (Request, Response)>;
    let descriptor = AgentToolDescriptor::new(AgentToolSpec::new(
        AgentToolKind::ModuleObjectRpc,
        name,
        description,
        false,
    ))
    .with_toolsets(["remote", "object_rpc"])
    .with_capabilities(capabilities)
    .with_input_schema(targeted_remote_input_schema(request_schema))
    .with_output_schema(serde_json::json!({
        "type": "object",
        "properties": {
            "response": {
                "type": "object",
                "description": response_description
            },
            "route": { "type": "string" },
            "metadata": { "type": "object" },
            "cancelled": { "type": "boolean" }
        },
        "required": ["response", "route", "metadata", "cancelled"]
    }));
    AgentRemoteManagementToolDefinition {
        descriptor,
        route,
        category,
    }
}

fn targeted_remote_input_schema(request_schema: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "target_node_id": {
                "type": "string",
                "minLength": 1,
                "description": "Optional exact cluster node id for the invocation."
            },
            "route_key": {
                "type": "string",
                "minLength": 1,
                "description": "Optional cluster routing key for service placement."
            },
            "input": request_schema
        },
        "required": ["input"],
        "additionalProperties": false
    })
}

impl AgentToolGateway for AgentRemoteObjectRpcTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        self.descriptor.clone()
    }

    fn start(
        &self,
        request: &AgentToolRequest<'_>,
    ) -> Result<AgentToolGatewayTask, AgentToolResult> {
        let operation_id = request.call_id.as_str().to_string();
        let target_node_id =
            remote_routing_hint(&request.call.input, "target_node_id", &request.call.tool)?;
        let route_key = remote_routing_hint(&request.call.input, "route_key", &request.call.tool)?;
        let payload = remote_request_payload(&request.call.input);
        let rpc = match ObjectRpcRequest::encode_body(&self.route, &self.category, &payload) {
            Ok(request) => request.with_operation_id(operation_id.clone()),
            Err(error) => {
                return Err(AgentToolResult::failed(
                    format!(
                        "failed to encode remote tool `{}` request",
                        request.call.tool
                    ),
                    serde_json::json!({
                        "error": "remote_request_encode_failed",
                        "tool": request.call.tool,
                        "detail": error.to_string(),
                    }),
                ));
            }
        };
        let start_transport = Arc::clone(&self.transport);
        let cancel_transport = Arc::clone(&self.transport);
        let cancel_route = self.cancel_route.clone();
        let cancel_category = self.cancel_category.clone();
        let tool = request.call.tool.clone();
        let cancel_tool = tool.clone();
        let cancel_target_node_id = target_node_id.clone();
        let cancel_route_key = route_key.clone();
        let cancellation_requested = Arc::new(AtomicBool::new(false));
        let cancellation_for_wait = Arc::clone(&cancellation_requested);
        let cancellation_for_cancel = Arc::clone(&cancellation_requested);
        Ok(AgentToolGatewayTask::new(
            move || match start_transport.call_targeted(
                target_node_id.as_deref(),
                route_key.as_deref(),
                rpc,
            ) {
                Ok(response) => remote_rpc_tool_result(&tool, response, &cancellation_for_wait),
                Err(detail) if cancellation_for_wait.load(Ordering::SeqCst) => {
                    AgentToolResult::cancelled(
                        format!("remote tool `{tool}` cancelled"),
                        serde_json::json!({
                            "error": "remote_call_failed_after_cancel",
                            "tool": tool,
                            "detail": detail,
                            "cancelled": true,
                        }),
                    )
                }
                Err(detail) => AgentToolResult::failed(
                    format!("remote tool `{tool}` failed"),
                    serde_json::json!({
                        "error": "remote_call_failed",
                        "tool": tool,
                        "detail": detail,
                        "cancelled": false,
                    }),
                ),
            },
            move || {
                cancellation_for_cancel.store(true, Ordering::SeqCst);
                let cancel = match ObjectRpcRequest::encode_body(
                    &cancel_route,
                    &cancel_category,
                    &ObjectRpcCancelRequest::new(
                        operation_id.clone(),
                        "agent turn cancellation requested",
                    ),
                ) {
                    Ok(request) => request.with_cancels_operation_id(operation_id.clone()),
                    Err(error) => {
                        cancellation_for_cancel.store(false, Ordering::SeqCst);
                        return AgentToolGatewayCancellation::rejected(format!(
                            "failed to encode remote cancellation request for `{cancel_tool}`: {error}"
                        ));
                    }
                };
                match cancel_transport.call_targeted(
                    cancel_target_node_id.as_deref(),
                    cancel_route_key.as_deref(),
                    cancel,
                ) {
                    Ok(response) => match response.error_detail() {
                        Ok(()) => AgentToolGatewayCancellation::accepted(format!(
                            "remote cancellation accepted for `{cancel_tool}`"
                        )),
                        Err(error) => {
                            cancellation_for_cancel.store(false, Ordering::SeqCst);
                            AgentToolGatewayCancellation::rejected(format!(
                                "remote cancellation rejected for `{cancel_tool}`: {error}"
                            ))
                        }
                    },
                    Err(error) => {
                        cancellation_for_cancel.store(false, Ordering::SeqCst);
                        AgentToolGatewayCancellation::rejected(format!(
                            "remote cancellation failed for `{cancel_tool}`: {error}"
                        ))
                    }
                }
            },
        ))
    }
}

fn remote_routing_hint(
    input: &serde_json::Value,
    field: &'static str,
    tool: &str,
) -> Result<Option<String>, AgentToolResult> {
    let Some(value) = input.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let Some(value) = value.as_str() else {
        return Err(AgentToolResult::failed(
            format!("remote tool `{tool}` has an invalid {field}"),
            serde_json::json!({
                "error": "invalid_remote_routing_hint",
                "tool": tool,
                "field": field,
                "detail": format!("{field} must be a non-empty string"),
            }),
        ));
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(AgentToolResult::failed(
            format!("remote tool `{tool}` has an invalid {field}"),
            serde_json::json!({
                "error": "invalid_remote_routing_hint",
                "tool": tool,
                "field": field,
                "detail": format!("{field} must be a non-empty string"),
            }),
        ));
    }
    Ok(Some(value.to_string()))
}

fn remote_request_payload(input: &serde_json::Value) -> serde_json::Value {
    if let Some(payload) = input.get("input") {
        return payload.clone();
    }
    let mut payload = input.clone();
    if let Some(payload) = payload.as_object_mut() {
        payload.remove("target_node_id");
        payload.remove("route_key");
    }
    payload
}

fn remote_rpc_tool_result(
    tool: &str,
    response: ObjectRpcResponse,
    cancellation_requested: &AtomicBool,
) -> AgentToolResult {
    let cancelled = cancellation_requested.load(Ordering::SeqCst);
    if let Err(error) = response.error_detail() {
        let payload = serde_json::json!({
            "error": "remote_rpc_error",
            "tool": tool,
            "route": response.route,
            "metadata": response.metadata,
            "body_bytes": response.body.len(),
            "detail": error.to_string(),
            "cancelled": cancelled,
        });
        return if cancelled {
            AgentToolResult::cancelled(format!("remote tool `{tool}` cancelled"), payload)
        } else {
            AgentToolResult::failed(format!("remote tool `{tool}` was rejected"), payload)
        };
    }

    match response.decode_body::<serde_json::Value>() {
        Ok(value) => {
            let application_error = remote_application_error(&value).cloned();
            let payload = serde_json::json!({
                "response": value,
                "route": response.route,
                "metadata": response.metadata,
                "cancelled": cancelled,
            });
            if cancelled {
                AgentToolResult::cancelled(format!("remote tool `{tool}` cancelled"), payload)
            } else if let Some(application_error) = application_error {
                AgentToolResult::failed(
                    format!("remote tool `{tool}` returned an application error"),
                    serde_json::json!({
                        "error": "remote_application_error",
                        "tool": tool,
                        "application_error": application_error,
                        "response": payload["response"].clone(),
                        "route": payload["route"].clone(),
                        "metadata": payload["metadata"].clone(),
                        "cancelled": false,
                    }),
                )
            } else {
                AgentToolResult::succeeded(format!("remote tool `{tool}` completed"), payload)
            }
        }
        Err(error) => AgentToolResult::failed(
            format!("remote tool `{tool}` returned an invalid response"),
            serde_json::json!({
                "error": "remote_response_decode_failed",
                "tool": tool,
                "route": response.route,
                "metadata": response.metadata,
                "body_bytes": response.body.len(),
                "detail": error.to_string(),
                "cancelled": cancellation_requested.load(Ordering::SeqCst),
            }),
        ),
    }
}

fn remote_application_error(value: &serde_json::Value) -> Option<&serde_json::Value> {
    value
        .as_object()
        .and_then(|value| value.get("error"))
        .filter(|error| !error.is_null() && !matches!(error, serde_json::Value::Bool(false)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentCancellationToken, AgentToolCall, AgentToolProgressEvent, AgentToolProgressSink,
        AgentToolRegistry, AgentToolStatus, InteractionSession, InteractionSurface,
        InteractionSurfaceKind,
    };
    use sinomo_runtime::events::SessionId;
    use sinomo_runtime::node::NodeId;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    #[derive(Default)]
    struct FakeRemoteTransport {
        cancelled: AtomicBool,
        start_operation_id: Mutex<Option<String>>,
        cancel_operation_id: Mutex<Option<String>>,
        start_target: Mutex<Option<(Option<String>, Option<String>)>>,
        cancel_target: Mutex<Option<(Option<String>, Option<String>)>>,
        cancel_calls: AtomicUsize,
        fail_run_after_cancel: bool,
    }

    impl AgentRemoteObjectRpcTransport for FakeRemoteTransport {
        fn call(&self, request: ObjectRpcRequest) -> Result<ObjectRpcResponse, String> {
            match request.route.as_str() {
                "/remote/tool/run" => {
                    *self.start_operation_id.lock().expect("start op lock") =
                        request.operation_id().map(str::to_string);
                    while !self.cancelled.load(Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    if self.fail_run_after_cancel {
                        return Err("remote route ended after cancellation".to_string());
                    }
                    ObjectRpcResponse::encode_body(
                        "/remote/tool/run",
                        &serde_json::json!({ "remote": "cancelled" }),
                    )
                    .map_err(|error| error.to_string())
                }
                "/remote/tool/cancel" => {
                    self.cancel_calls.fetch_add(1, Ordering::SeqCst);
                    *self.cancel_operation_id.lock().expect("cancel op lock") =
                        request.cancels_operation_id().map(str::to_string);
                    let body: ObjectRpcCancelRequest =
                        request.decode_body().map_err(|error| error.to_string())?;
                    if Some(body.operation_id.as_str()) == request.cancels_operation_id() {
                        self.cancelled.store(true, Ordering::SeqCst);
                        ObjectRpcResponse::encode_body(
                            "/remote/tool/cancel",
                            &serde_json::json!({ "accepted": true }),
                        )
                        .map_err(|error| error.to_string())
                    } else {
                        ObjectRpcResponse::encode_error(
                            "/remote/tool/cancel",
                            "operation id mismatch",
                        )
                        .map_err(|error| error.to_string())
                    }
                }
                other => Err(format!("unexpected route {other}")),
            }
        }

        fn call_targeted(
            &self,
            target_node_id: Option<&str>,
            route_key: Option<&str>,
            request: ObjectRpcRequest,
        ) -> Result<ObjectRpcResponse, String> {
            let target = Some((
                target_node_id.map(str::to_string),
                route_key.map(str::to_string),
            ));
            match request.route.as_str() {
                "/remote/tool/run" => {
                    *self.start_target.lock().expect("start target lock") = target;
                }
                "/remote/tool/cancel" => {
                    *self.cancel_target.lock().expect("cancel target lock") = target;
                }
                _ => {}
            }
            self.call(request)
        }
    }

    fn test_session() -> InteractionSession {
        InteractionSession::new(
            SessionId::from_legacy_named("agent_remote_tool_cancel_session").expect("session"),
            NodeId::from_legacy_named("agent_remote_tool_cancel_node").expect("node"),
            InteractionSurface::new("agent.remote.test", InteractionSurfaceKind::CommandLine),
        )
    }

    #[test]
    fn remote_object_rpc_tool_sends_cancel_rpc_for_active_work() {
        let transport = Arc::new(FakeRemoteTransport::default());
        let tool = AgentRemoteObjectRpcTool::json_module_tool(
            "remote_module_probe",
            "fake remote module probe",
            "/remote/tool/run",
            "remote.tool",
            "/remote/tool/cancel",
            "remote.tool",
            transport.clone(),
        );
        let registry = AgentToolRegistry::new().with_gateway(tool);
        let cancellation_token = AgentCancellationToken::new();
        let token_for_sink = cancellation_token.clone();
        let progress_sink = AgentToolProgressSink::new(move |event| {
            if matches!(
                event,
                AgentToolProgressEvent::Message { ref summary, .. }
                    if summary == "gateway task started"
            ) {
                token_for_sink.cancel();
            }
        });
        let call = AgentToolCall {
            call_id: None,
            tool: "remote_module_probe".to_string(),
            input: serde_json::json!({
                "target_node_id": "node.remote-1",
                "route_key": "service.remote-primary",
                "input": { "query": "status" }
            }),
            r#async: false,
            summary: "cancel remote module probe".to_string(),
        };

        let outcome = registry.execute_with_cancellation_and_progress(
            &test_session(),
            0,
            0,
            &call,
            cancellation_token,
            progress_sink,
        );

        assert_eq!(outcome.status, AgentToolStatus::Cancelled);
        assert_eq!(
            outcome
                .output
                .get("cancelled")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(transport.cancel_calls.load(Ordering::SeqCst), 1);
        let start_id = transport
            .start_operation_id
            .lock()
            .expect("start op lock")
            .clone()
            .expect("start operation id");
        let cancel_id = transport
            .cancel_operation_id
            .lock()
            .expect("cancel op lock")
            .clone()
            .expect("cancel operation id");
        assert_eq!(start_id, cancel_id);
        let expected_target = Some((
            Some("node.remote-1".to_string()),
            Some("service.remote-primary".to_string()),
        ));
        assert_eq!(
            *transport.start_target.lock().expect("start target lock"),
            expected_target
        );
        assert_eq!(
            *transport.cancel_target.lock().expect("cancel target lock"),
            expected_target
        );
    }

    #[test]
    fn remote_object_rpc_tool_reports_cancelled_when_run_errors_after_cancel() {
        let transport = Arc::new(FakeRemoteTransport {
            fail_run_after_cancel: true,
            ..Default::default()
        });
        let tool = AgentRemoteObjectRpcTool::json_module_tool(
            "remote_module_probe",
            "fake remote module probe",
            "/remote/tool/run",
            "remote.tool",
            "/remote/tool/cancel",
            "remote.tool",
            transport.clone(),
        );
        let registry = AgentToolRegistry::new().with_gateway(tool);
        let cancellation_token = AgentCancellationToken::new();
        let token_for_sink = cancellation_token.clone();
        let progress_sink = AgentToolProgressSink::new(move |event| {
            if matches!(
                event,
                AgentToolProgressEvent::Message { ref summary, .. }
                    if summary == "gateway task started"
            ) {
                token_for_sink.cancel();
            }
        });
        let call = AgentToolCall {
            call_id: None,
            tool: "remote_module_probe".to_string(),
            input: serde_json::json!({ "input": { "query": "status" } }),
            r#async: false,
            summary: "cancel remote module probe".to_string(),
        };

        let outcome = registry.execute_with_cancellation_and_progress(
            &test_session(),
            0,
            0,
            &call,
            cancellation_token,
            progress_sink,
        );

        assert_eq!(outcome.status, AgentToolStatus::Cancelled);
        assert_eq!(
            outcome
                .output
                .get("error")
                .and_then(serde_json::Value::as_str),
            Some("remote_call_failed_after_cancel")
        );
        assert_eq!(
            outcome
                .output
                .get("cancelled")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(transport.cancel_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn legacy_fn_transport_fails_closed_for_targeted_tool_input() {
        let received = Arc::new(Mutex::new(None));
        let received_for_transport = Arc::clone(&received);
        let transport: Arc<dyn AgentRemoteObjectRpcTransport> = Arc::new(
            move |request: ObjectRpcRequest| -> Result<ObjectRpcResponse, String> {
                let payload: serde_json::Value =
                    request.decode_body().map_err(|error| error.to_string())?;
                *received_for_transport.lock().expect("received lock") = Some(payload);
                ObjectRpcResponse::encode_body(
                    "/remote/tool/immediate",
                    &serde_json::json!({ "ok": true }),
                )
                .map_err(|error| error.to_string())
            },
        );
        let tool = AgentRemoteObjectRpcTool::json_module_tool(
            "remote_module_immediate",
            "fake immediate remote module call",
            "/remote/tool/immediate",
            "remote.tool",
            "/remote/tool/cancel",
            "remote.tool",
            transport,
        );
        let registry = AgentToolRegistry::new().with_gateway(tool);
        let call = AgentToolCall {
            call_id: None,
            tool: "remote_module_immediate".to_string(),
            input: serde_json::json!({
                "target_node_id": "node.legacy-target",
                "route_key": "legacy-route-key",
                "query": "status"
            }),
            r#async: false,
            summary: "legacy closure compatibility".to_string(),
        };

        let outcome = registry.execute(&test_session(), 0, 0, &call);

        assert_eq!(outcome.status, AgentToolStatus::Failed);
        assert!(
            outcome
                .output
                .get("detail")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|detail| detail.contains("does not support explicit"))
        );
        assert_eq!(*received.lock().expect("received lock"), None);
    }

    #[test]
    fn target_aware_closure_adapter_receives_both_routing_hints() {
        let received = Arc::new(Mutex::new(None));
        let received_for_transport = Arc::clone(&received);
        let transport = TargetAwareAgentRemoteObjectRpcTransport::new(
            move |target_node_id: Option<&str>,
                  route_key: Option<&str>,
                  request: ObjectRpcRequest| {
                *received_for_transport.lock().expect("received lock") = Some((
                    target_node_id.map(str::to_string),
                    route_key.map(str::to_string),
                    request.route,
                ));
                ObjectRpcResponse::encode_body(
                    "/remote/tool/immediate",
                    &serde_json::json!({ "ok": true }),
                )
                .map_err(|error| error.to_string())
            },
        );
        transport
            .call_targeted(
                Some("node.targeted"),
                Some("route.targeted"),
                ObjectRpcRequest::encode_body(
                    "/remote/tool/immediate",
                    "remote.tool",
                    &serde_json::json!({}),
                )
                .expect("request"),
            )
            .expect("targeted call");

        assert_eq!(
            *received.lock().expect("received lock"),
            Some((
                Some("node.targeted".to_string()),
                Some("route.targeted".to_string()),
                "/remote/tool/immediate".to_string(),
            ))
        );
    }

    #[test]
    fn remote_management_tool_set_covers_shared_routes_without_initiator_permission() {
        let definitions = remote_management_agent_tool_definitions();
        assert_eq!(definitions.len(), 11);
        assert_eq!(
            definitions
                .iter()
                .map(|definition| definition.route)
                .collect::<std::collections::BTreeSet<_>>(),
            [
                REMOTE_MANAGEMENT_SHELL_RUN_ROUTE,
                REMOTE_MANAGEMENT_FS_LIST_ROUTE,
                REMOTE_MANAGEMENT_FS_STAT_ROUTE,
                REMOTE_MANAGEMENT_FS_READ_ROUTE,
                REMOTE_MANAGEMENT_FS_WRITE_ROUTE,
                REMOTE_MANAGEMENT_SYSTEM_INFO_ROUTE,
                REMOTE_MANAGEMENT_SYSTEM_OBSERVE_ROUTE,
                CONFIG_CATALOG_ROUTE,
                CONFIG_SEARCH_ROUTE,
                CONFIG_GET_ROUTE,
                CONFIG_SET_ROUTE,
            ]
            .into_iter()
            .collect()
        );
        for definition in definitions {
            assert!(
                definition
                    .descriptor
                    .toolsets
                    .iter()
                    .any(|toolset| toolset == "remote")
            );
            assert!(definition.descriptor.required_local_permissions.is_empty());
            assert!(
                definition.descriptor.input_schema["properties"]
                    .get("target_node_id")
                    .is_some()
            );
            assert!(
                definition.descriptor.input_schema["properties"]
                    .get("route_key")
                    .is_some()
            );
        }
    }

    #[test]
    fn remote_object_rpc_tool_reports_rpc_error_metadata_as_failed() {
        let response = ObjectRpcResponse::encode_error(
            "/remote/tool/immediate",
            "remote management is not enabled on this node",
        )
        .expect("error response");

        let outcome =
            remote_rpc_tool_result("remote_module_immediate", response, &AtomicBool::new(false));

        assert_eq!(outcome.status, AgentToolStatus::Failed);
        assert_eq!(
            outcome
                .output
                .get("error")
                .and_then(serde_json::Value::as_str),
            Some("remote_rpc_error")
        );
        assert!(
            outcome
                .output
                .get("detail")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|detail| detail.contains("remote management is not enabled"))
        );
    }

    #[test]
    fn remote_object_rpc_tool_reports_typed_application_error_as_failed() {
        let response = ObjectRpcResponse::encode_body(
            REMOTE_MANAGEMENT_SHELL_RUN_ROUTE,
            &RemoteManagementShellRunResponse::failure(
                sinomo_runtime::remote_management::RemoteManagementError::new(
                    "remote-management-disabled",
                    "remote management is not enabled on this node",
                ),
            ),
        )
        .expect("typed error response");

        let outcome = remote_rpc_tool_result("remote_shell", response, &AtomicBool::new(false));

        assert_eq!(outcome.status, AgentToolStatus::Failed);
        assert_eq!(
            outcome
                .output
                .get("error")
                .and_then(serde_json::Value::as_str),
            Some("remote_application_error")
        );
        assert_eq!(
            outcome
                .output
                .pointer("/application_error/code")
                .and_then(serde_json::Value::as_str),
            Some("remote-management-disabled")
        );
        assert_eq!(
            outcome
                .output
                .pointer("/response/error/detail")
                .and_then(serde_json::Value::as_str),
            Some("remote management is not enabled on this node")
        );
    }

    #[test]
    fn remote_system_observe_keeps_device_and_resource_policy_requests_distinct() {
        let received = Arc::new(Mutex::new(None));
        let received_for_transport = Arc::clone(&received);
        let transport: Arc<dyn AgentRemoteObjectRpcTransport> = Arc::new(
            TargetAwareAgentRemoteObjectRpcTransport::new(
                move |_target_node_id: Option<&str>,
                      _route_key: Option<&str>,
                      request: ObjectRpcRequest| {
                    let request: RemoteManagementSystemObserveRequest =
                        request.decode_body().map_err(|error| error.to_string())?;
                    *received_for_transport.lock().expect("received lock") = Some(request);
                    let mut observation =
                        sinomo_runtime::remote_management::RemoteManagementSystemObservation::default();
                    observation.devices.push(
                        sinomo_runtime::remote_management::RemoteManagementDeviceObservation {
                            id: "camera.verified".to_string(),
                            kind: "camera".to_string(),
                            name: "Verified camera".to_string(),
                            capabilities: vec!["capture".to_string()],
                            state: Some("available".to_string()),
                        },
                    );
                    observation.resource_exposure_policies.push(
                        sinomo_runtime::remote_management::RemoteManagementResourceExposurePolicy {
                            resource_id: "camera.policy-only".to_string(),
                            allowed_operations: vec!["observe".to_string()],
                        },
                    );
                    ObjectRpcResponse::encode_body(
                        REMOTE_MANAGEMENT_SYSTEM_OBSERVE_ROUTE,
                        &RemoteManagementSystemObserveResponse::success(observation),
                    )
                    .map_err(|error| error.to_string())
                },
            ),
        );
        let definition = remote_management_agent_tool_definitions()
            .into_iter()
            .find(|definition| definition.descriptor.spec.name == "remote_system_observe")
            .expect("remote system observe definition");
        assert!(
            definition.descriptor.input_schema["properties"]["input"]["properties"]
                .get("include_devices")
                .is_some()
        );
        assert!(
            definition.descriptor.input_schema["properties"]["input"]["properties"]
                .get("include_resource_exposures")
                .is_some()
        );
        let tool = remote_management_agent_tools(transport)
            .into_iter()
            .find(|tool| tool.descriptor.spec.name == "remote_system_observe")
            .expect("remote system observe tool");
        let registry = AgentToolRegistry::new().with_gateway(tool);
        let call = AgentToolCall {
            call_id: None,
            tool: "remote_system_observe".to_string(),
            input: serde_json::json!({
                "input": {
                    "include_devices": false,
                    "include_resource_exposures": true
                }
            }),
            r#async: false,
            summary: "observe remote resources".to_string(),
        };

        let outcome = registry.execute(&test_session(), 0, 0, &call);

        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        let received = received
            .lock()
            .expect("received lock")
            .clone()
            .expect("observation request");
        assert!(!received.include_devices);
        assert!(received.include_resource_exposures);
        assert_eq!(
            outcome
                .output
                .pointer("/response/observation/devices/0/id")
                .and_then(serde_json::Value::as_str),
            Some("camera.verified")
        );
        assert_eq!(
            outcome
                .output
                .pointer("/response/observation/resource_exposure_policies/0/resource_id")
                .and_then(serde_json::Value::as_str),
            Some("camera.policy-only")
        );
    }
}
