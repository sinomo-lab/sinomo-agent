use super::{
    AgentToolCapability, AgentToolDescriptor, AgentToolGateway, AgentToolGatewayCancellation,
    AgentToolGatewayTask, AgentToolRegistry, AgentToolRequest, AgentToolResult,
};
use crate::{AgentToolKind, AgentToolSpec};
use serde::Deserialize;
use sinomo_runtime::module::{MODULE_NATIVE_DYLIB_CANCEL_ROUTE, MODULE_RPC_CATEGORY};
use sinomo_runtime::network::{
    ObjectRpcBodySource, ObjectRpcCall, ObjectRpcCancelRequest, ObjectRpcRequest, ObjectRpcResult,
};
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
    RemoteManagementFileTransfer, RemoteManagementFileWriteRequest,
    RemoteManagementFileWriteResponse, RemoteManagementShellRunRequest,
    RemoteManagementShellRunResponse, RemoteManagementSystemInfoRequest,
    RemoteManagementSystemInfoResponse, RemoteManagementSystemObserveRequest,
    RemoteManagementSystemObserveResponse,
};
use std::fs::{self, File};
use std::future::Future;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::AsyncReadExt as _;

const REMOTE_FILE_COPY_BUFFER_BYTES: usize = 1024 * 1024;
static REMOTE_FILE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub type AgentRemoteObjectRpcFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ObjectRpcResult, String>> + Send + 'a>>;

pub trait AgentRemoteObjectRpcTransport: Send + Sync {
    /// Calls one stream-native object-RPC operation using the transport's
    /// default target. Request and response attachments share the same stream.
    fn call<'a>(&'a self, call: ObjectRpcCall) -> AgentRemoteObjectRpcFuture<'a>;

    /// Calls an object-RPC route using explicit cluster-routing hints.
    ///
    /// Default-target transports fall back to [`Self::call`] only when no
    /// routing hint is present; explicit targets fail closed unless a
    /// cluster-aware host overrides this method. The same hints are supplied
    /// for the work request and its cancellation.
    fn call_targeted<'a>(
        &'a self,
        target_node_id: Option<String>,
        route_key: Option<String>,
        call: ObjectRpcCall,
    ) -> AgentRemoteObjectRpcFuture<'a> {
        if target_node_id.is_some() || route_key.is_some() {
            return Box::pin(async {
                Err(
                    "remote transport does not support explicit target_node_id or route_key routing"
                        .to_string(),
                )
            });
        }
        self.call(call)
    }
}

/// Adapts an owned, asynchronous, target-aware object-RPC call closure.
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
    F: Fn(Option<String>, Option<String>, ObjectRpcCall) -> AgentRemoteObjectRpcFuture<'static>
        + Send
        + Sync,
{
    fn call<'a>(&'a self, call: ObjectRpcCall) -> AgentRemoteObjectRpcFuture<'a> {
        (self.call)(None, None, call)
    }

    fn call_targeted<'a>(
        &'a self,
        target_node_id: Option<String>,
        route_key: Option<String>,
        call: ObjectRpcCall,
    ) -> AgentRemoteObjectRpcFuture<'a> {
        (self.call)(target_node_id, route_key, call)
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
            "remote_file_get",
            "Stream one complete file from a selected remote node into an atomic local destination.",
            REMOTE_MANAGEMENT_FS_READ_ROUTE,
            REMOTE_MANAGEMENT_RPC_CATEGORY,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "remote_path": { "type": "string", "minLength": 1 },
                    "local_path": { "type": "string", "minLength": 1 },
                    "create_parent": { "type": "boolean" },
                    "overwrite": {
                        "type": "boolean",
                        "description": "Must be true to replace an existing local file."
                    }
                },
                "required": ["remote_path", "local_path"],
                "additionalProperties": false
            }),
            "Verified complete-file length and BLAKE3 digest plus the committed local path.",
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
            "remote_file_put",
            "Stream one complete local file to a selected remote node in one session.",
            REMOTE_MANAGEMENT_FS_WRITE_ROUTE,
            REMOTE_MANAGEMENT_RPC_CATEGORY,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "local_path": { "type": "string", "minLength": 1 },
                    "remote_path": { "type": "string", "minLength": 1 },
                    "create_parent": { "type": "boolean" },
                    "overwrite": {
                        "type": "boolean",
                        "description": "Must be true to replace an existing remote file."
                    }
                },
                "required": ["local_path", "remote_path"],
                "additionalProperties": false
            }),
            "Verified complete-file length and BLAKE3 digest reported by the receiving node.",
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
        let start_transport = Arc::clone(&self.transport);
        let cancel_transport = Arc::clone(&self.transport);
        let route = self.route.clone();
        let category = self.category.clone();
        let cancel_route = self.cancel_route.clone();
        let cancel_category = self.cancel_category.clone();
        let tool = request.call.tool.clone();
        let cancel_tool = tool.clone();
        let wait_operation_id = operation_id.clone();
        let cancel_target_node_id = target_node_id.clone();
        let cancel_route_key = route_key.clone();
        let cancellation_requested = Arc::new(AtomicBool::new(false));
        let cancellation_for_wait = Arc::clone(&cancellation_requested);
        let cancellation_for_cancel = Arc::clone(&cancellation_requested);
        Ok(AgentToolGatewayTask::new(
            move || {
                execute_remote_tool(
                    &tool,
                    &route,
                    &category,
                    payload,
                    target_node_id,
                    route_key,
                    wait_operation_id,
                    start_transport,
                    &cancellation_for_wait,
                )
            },
            move || {
                cancellation_for_cancel.store(true, Ordering::SeqCst);
                let mut cancel = match ObjectRpcCall::encode_value(
                    &cancel_route,
                    &cancel_category,
                    ObjectRpcCancelRequest::new(
                        operation_id.clone(),
                        "agent turn cancellation requested",
                    ),
                ) {
                    Ok(call) => call,
                    Err(error) => {
                        cancellation_for_cancel.store(false, Ordering::SeqCst);
                        return AgentToolGatewayCancellation::rejected(format!(
                            "failed to encode remote cancellation request for `{cancel_tool}`: {error}"
                        ));
                    }
                };
                cancel.request = cancel
                    .request
                    .with_cancels_operation_id(operation_id.clone());
                let result = run_agent_remote_session(cancel_transport.call_targeted(
                    cancel_target_node_id.clone(),
                    cancel_route_key.clone(),
                    cancel,
                ));
                match result {
                    Ok(result) => match result.response.error_detail() {
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentRemoteFileGetInput {
    remote_path: String,
    local_path: PathBuf,
    #[serde(default)]
    create_parent: bool,
    #[serde(default)]
    overwrite: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentRemoteFilePutInput {
    local_path: PathBuf,
    remote_path: String,
    #[serde(default)]
    create_parent: bool,
    #[serde(default)]
    overwrite: bool,
}

#[allow(clippy::too_many_arguments)]
fn execute_remote_tool(
    tool: &str,
    route: &str,
    category: &str,
    payload: serde_json::Value,
    target_node_id: Option<String>,
    route_key: Option<String>,
    operation_id: String,
    transport: Arc<dyn AgentRemoteObjectRpcTransport>,
    cancellation_requested: &AtomicBool,
) -> AgentToolResult {
    if route == REMOTE_MANAGEMENT_FS_READ_ROUTE {
        return execute_remote_file_get(
            tool,
            category,
            payload,
            target_node_id,
            route_key,
            operation_id,
            transport,
            cancellation_requested,
        );
    }
    if route == REMOTE_MANAGEMENT_FS_WRITE_ROUTE {
        return execute_remote_file_put(
            tool,
            category,
            payload,
            target_node_id,
            route_key,
            operation_id,
            transport,
            cancellation_requested,
        );
    }
    let mut rpc = match ObjectRpcCall::encode_value(route, category, payload) {
        Ok(call) => call,
        Err(error) => return remote_encode_error(tool, error),
    };
    rpc.request = rpc.request.with_operation_id(operation_id);
    match run_agent_remote_future(async {
        let mut response = transport
            .call_targeted(target_node_id, route_key, rpc)
            .await?;
        // The agent tool surface is synchronous and returns a fully owned JSON
        // result. Gather only ordinary streamed values while the transport
        // runtime is alive; semantic attachments remain an error below.
        response
            .materialize_value_body(usize::MAX)
            .await
            .map_err(|source| source.to_string())?;
        Ok(response)
    }) {
        Ok(response) => remote_rpc_tool_result(tool, response, cancellation_requested),
        Err(detail) => remote_transport_error(tool, detail, cancellation_requested),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_remote_file_get(
    tool: &str,
    category: &str,
    payload: serde_json::Value,
    target_node_id: Option<String>,
    route_key: Option<String>,
    operation_id: String,
    transport: Arc<dyn AgentRemoteObjectRpcTransport>,
    cancellation_requested: &AtomicBool,
) -> AgentToolResult {
    let input: AgentRemoteFileGetInput = match serde_json::from_value(payload) {
        Ok(input) => input,
        Err(error) => return remote_file_input_error(tool, error),
    };
    if input.remote_path.trim().is_empty() || input.local_path.as_os_str().is_empty() {
        return remote_file_validation_error(tool, "remote_path and local_path must be non-empty");
    }
    if input.create_parent
        && let Some(parent) = input.local_path.parent()
        && let Err(source) = fs::create_dir_all(parent)
    {
        return remote_file_failure(
            tool,
            format!("create local parent {}: {source}", parent.display()),
        );
    }
    let temp = match AgentRemoteFileTemp::new(&input.local_path) {
        Ok(temp) => temp,
        Err(detail) => return remote_file_failure(tool, detail),
    };
    let mut file =
        match sinomo_runtime::storage::durable_file::create_private_staged_file(temp.path()) {
            Ok(file) => file,
            Err(source) => {
                return remote_file_failure(
                    tool,
                    format!("create transfer file {}: {source}", temp.path().display()),
                );
            }
        };
    let request = RemoteManagementFileReadRequest::new(input.remote_path.clone());
    let mut rpc =
        match ObjectRpcCall::encode_value(REMOTE_MANAGEMENT_FS_READ_ROUTE, category, request) {
            Ok(call) => call,
            Err(error) => return remote_encode_error(tool, error),
        };
    rpc.request = rpc.request.with_operation_id(operation_id);
    let temp_path = temp.path().to_path_buf();
    let remote_path = input.remote_path.clone();
    let transfer = match run_agent_remote_future(async {
        let result = transport
            .call_targeted(target_node_id, route_key, rpc)
            .await?;
        let (response, mut body) = result.into_parts();
        validate_agent_response_route(&response, REMOTE_MANAGEMENT_FS_READ_ROUTE)?;
        let response: RemoteManagementFileReadResponse = response
            .decode_body()
            .map_err(|error| format!("decode remote file get response: {error}"))?;
        let transfer = response.into_result().map_err(|error| error.to_string())?;
        validate_agent_transfer(&transfer, &remote_path)?;
        if body
            .total_bytes()
            .is_some_and(|length| length != transfer.length)
        {
            return Err(format!(
                "remote file attachment declared {} bytes but its head declared {}",
                body.total_bytes().unwrap_or_default(),
                transfer.length
            ));
        }
        let mut hasher = blake3::Hasher::new();
        let mut length = 0_u64;
        let mut buffer = vec![0_u8; REMOTE_FILE_COPY_BUFFER_BYTES];
        loop {
            if cancellation_requested.load(Ordering::SeqCst) {
                return Err("remote file get was cancelled".to_string());
            }
            let read = body
                .read(&mut buffer)
                .await
                .map_err(|source| format!("read remote file attachment: {source}"))?;
            if read == 0 {
                break;
            }
            file.write_all(&buffer[..read]).map_err(|source| {
                format!("write transfer file {}: {source}", temp_path.display())
            })?;
            length = length
                .checked_add(read as u64)
                .ok_or_else(|| "downloaded file length exceeded u64".to_string())?;
            hasher.update(&buffer[..read]);
        }
        let actual_hash = hasher.finalize().to_hex().to_string();
        if length != transfer.length {
            return Err(format!(
                "downloaded file length mismatch: expected {}, got {length}",
                transfer.length
            ));
        }
        ensure_agent_hash_matches(&transfer.blake3, &actual_hash)?;
        Ok(transfer)
    }) {
        Ok(transfer) => transfer,
        Err(detail) => return remote_transport_error(tool, detail, cancellation_requested),
    };
    if let Err(source) = file.sync_all() {
        return remote_file_failure(
            tool,
            format!("sync transfer file {}: {source}", temp.path().display()),
        );
    }
    drop(file);
    if cancellation_requested.load(Ordering::SeqCst) {
        return AgentToolResult::cancelled(
            format!("remote tool `{tool}` cancelled"),
            serde_json::json!({ "cancelled": true, "sessions": 1 }),
        );
    }
    if let Err(detail) = temp.commit(&input.local_path, input.overwrite) {
        return remote_file_failure(tool, detail);
    }
    AgentToolResult::succeeded(
        format!("remote tool `{tool}` completed"),
        serde_json::json!({
            "response": transfer,
            "local_path": input.local_path,
            "sessions": 1,
            "cancelled": false,
        }),
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_remote_file_put(
    tool: &str,
    category: &str,
    payload: serde_json::Value,
    target_node_id: Option<String>,
    route_key: Option<String>,
    operation_id: String,
    transport: Arc<dyn AgentRemoteObjectRpcTransport>,
    cancellation_requested: &AtomicBool,
) -> AgentToolResult {
    let input: AgentRemoteFilePutInput = match serde_json::from_value(payload) {
        Ok(input) => input,
        Err(error) => return remote_file_input_error(tool, error),
    };
    if input.remote_path.trim().is_empty() || input.local_path.as_os_str().is_empty() {
        return remote_file_validation_error(tool, "local_path and remote_path must be non-empty");
    }
    let metadata = match fs::metadata(&input.local_path) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => return remote_file_failure(tool, "local path is not a regular file".to_string()),
        Err(source) => {
            return remote_file_failure(
                tool,
                format!(
                    "inspect local file {}: {source}",
                    input.local_path.display()
                ),
            );
        }
    };
    let (local_length, local_hash) =
        match hash_agent_local_file(&input.local_path, cancellation_requested) {
            Ok(identity) => identity,
            Err(detail) => return remote_file_failure(tool, detail),
        };
    if local_length != metadata.len() {
        return remote_file_failure(tool, "local file changed before upload".to_string());
    }
    let body = match ObjectRpcBodySource::file(input.local_path.clone()) {
        Ok(body) => body,
        Err(error) => return remote_file_failure(tool, error.to_string()),
    };
    if body.total_bytes() != Some(local_length) {
        return remote_file_failure(tool, "local file changed before upload session".to_string());
    }
    let request = RemoteManagementFileWriteRequest::new(
        input.remote_path.clone(),
        local_length,
        local_hash.clone(),
    )
    .creating_parent(input.create_parent)
    .overwriting(input.overwrite);
    let rpc =
        match ObjectRpcRequest::encode_body(REMOTE_MANAGEMENT_FS_WRITE_ROUTE, category, &request) {
            Ok(request) => request.with_operation_id(operation_id),
            Err(error) => return remote_encode_error(tool, error),
        };
    let remote_path = input.remote_path.clone();
    let written = match run_agent_remote_future(async {
        let result = transport
            .call_targeted(target_node_id, route_key, ObjectRpcCall::new(rpc, body))
            .await?;
        validate_agent_response_route(&result.response, REMOTE_MANAGEMENT_FS_WRITE_ROUTE)?;
        let response: RemoteManagementFileWriteResponse = result
            .decode_body()
            .await
            .map_err(|error| format!("decode remote file put response: {error}"))?;
        let written = response.into_result().map_err(|error| error.to_string())?;
        validate_agent_transfer(&written, &remote_path)?;
        Ok(written)
    }) {
        Ok(written) => written,
        Err(detail) => return remote_transport_error(tool, detail, cancellation_requested),
    };
    if written.length != local_length
        || normalize_agent_blake3(&written.blake3) != normalize_agent_blake3(&local_hash)
    {
        return remote_file_failure(
            tool,
            format!(
                "uploaded file identity mismatch: local {local_length} bytes blake3:{local_hash}, remote {} bytes {}",
                written.length, written.blake3
            ),
        );
    }
    if cancellation_requested.load(Ordering::SeqCst) {
        return AgentToolResult::cancelled(
            format!("remote tool `{tool}` cancelled"),
            serde_json::json!({ "cancelled": true, "sessions": 1 }),
        );
    }
    AgentToolResult::succeeded(
        format!("remote tool `{tool}` completed"),
        serde_json::json!({
            "response": written,
            "local_path": input.local_path,
            "sessions": 1,
            "cancelled": false,
        }),
    )
}

fn run_agent_remote_session(
    future: AgentRemoteObjectRpcFuture<'_>,
) -> Result<ObjectRpcResult, String> {
    run_agent_remote_future(future)
}

fn run_agent_remote_future<T, F>(future: F) -> Result<T, String>
where
    T: Send,
    F: Future<Output = Result<T, String>> + Send,
{
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        return if matches!(
            runtime.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            tokio::task::block_in_place(|| runtime.block_on(future))
        } else {
            Err("remote agent tool cannot block inside a current-thread Tokio runtime".to_string())
        };
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| format!("create remote agent tool runtime: {source}"))?;
    runtime.block_on(future)
}

fn validate_agent_response_route(
    response: &sinomo_runtime::network::ObjectRpcResponse,
    route: &str,
) -> Result<(), String> {
    if response.route != route {
        return Err(format!(
            "remote RPC returned route `{}` for request `{route}`",
            response.route
        ));
    }
    response.error_detail().map_err(|error| error.to_string())
}

fn validate_agent_transfer(
    transfer: &RemoteManagementFileTransfer,
    requested_path: &str,
) -> Result<(), String> {
    if transfer.path != requested_path {
        return Err(format!(
            "remote transfer returned path `{}` for `{requested_path}`",
            transfer.path
        ));
    }
    let hash = normalize_agent_blake3(&transfer.blake3);
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!(
            "invalid remote BLAKE3 digest `{}`",
            transfer.blake3
        ));
    }
    Ok(())
}

fn ensure_agent_hash_matches(expected: &str, actual: &str) -> Result<(), String> {
    if normalize_agent_blake3(expected) == normalize_agent_blake3(actual) {
        Ok(())
    } else {
        Err(format!(
            "downloaded file BLAKE3 mismatch: expected {expected}, got {actual}"
        ))
    }
}

fn normalize_agent_blake3(value: &str) -> String {
    value
        .strip_prefix("blake3:")
        .unwrap_or(value)
        .to_ascii_lowercase()
}

fn hash_agent_local_file(
    path: &Path,
    cancellation_requested: &AtomicBool,
) -> Result<(u64, String), String> {
    let mut file = File::open(path)
        .map_err(|source| format!("open local file {}: {source}", path.display()))?;
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    let mut buffer = vec![0_u8; REMOTE_FILE_COPY_BUFFER_BYTES];
    loop {
        if cancellation_requested.load(Ordering::SeqCst) {
            return Err("remote file put was cancelled while hashing".to_string());
        }
        let read = file
            .read(&mut buffer)
            .map_err(|source| format!("read local file {}: {source}", path.display()))?;
        if read == 0 {
            break;
        }
        length = length
            .checked_add(read as u64)
            .ok_or_else(|| "local file length exceeded u64".to_string())?;
        hasher.update(&buffer[..read]);
    }
    Ok((length, hasher.finalize().to_hex().to_string()))
}

struct AgentRemoteFileTemp {
    path: PathBuf,
    committed: bool,
}

impl AgentRemoteFileTemp {
    fn new(destination: &Path) -> Result<Self, String> {
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        let name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("download");
        let sequence = REMOTE_FILE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            ".{name}.sinomo-agent-get-{}-{sequence}.part",
            std::process::id()
        ));
        if path.exists() {
            return Err(format!("transfer temp already exists: {}", path.display()));
        }
        Ok(Self {
            path,
            committed: false,
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn commit(mut self, destination: &Path, overwrite: bool) -> Result<(), String> {
        sinomo_runtime::storage::durable_file::install_staged_file(
            &self.path,
            destination,
            overwrite,
        )
        .map_err(|source| {
            format!(
                "install downloaded file {} (overwrite={overwrite}): {source}",
                destination.display()
            )
        })?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for AgentRemoteFileTemp {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn remote_encode_error(tool: &str, error: impl std::fmt::Display) -> AgentToolResult {
    AgentToolResult::failed(
        format!("failed to encode remote tool `{tool}` request"),
        serde_json::json!({
            "error": "remote_request_encode_failed",
            "tool": tool,
            "detail": error.to_string(),
        }),
    )
}

fn remote_file_input_error(tool: &str, error: serde_json::Error) -> AgentToolResult {
    remote_file_validation_error(tool, &format!("invalid file transfer input: {error}"))
}

fn remote_file_validation_error(tool: &str, detail: &str) -> AgentToolResult {
    AgentToolResult::failed(
        format!("remote tool `{tool}` has invalid file transfer input"),
        serde_json::json!({
            "error": "invalid_remote_file_transfer_input",
            "tool": tool,
            "detail": detail,
        }),
    )
}

fn remote_file_failure(tool: &str, detail: String) -> AgentToolResult {
    AgentToolResult::failed(
        format!("remote tool `{tool}` file transfer failed"),
        serde_json::json!({
            "error": "remote_file_transfer_failed",
            "tool": tool,
            "detail": detail,
            "cancelled": false,
        }),
    )
}

fn remote_transport_error(
    tool: &str,
    detail: String,
    cancellation_requested: &AtomicBool,
) -> AgentToolResult {
    if cancellation_requested.load(Ordering::SeqCst) {
        AgentToolResult::cancelled(
            format!("remote tool `{tool}` cancelled"),
            serde_json::json!({
                "error": "remote_call_failed_after_cancel",
                "tool": tool,
                "detail": detail,
                "cancelled": true,
            }),
        )
    } else {
        AgentToolResult::failed(
            format!("remote tool `{tool}` failed"),
            serde_json::json!({
                "error": "remote_call_failed",
                "tool": tool,
                "detail": detail,
                "cancelled": false,
            }),
        )
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
    result: ObjectRpcResult,
    cancellation_requested: &AtomicBool,
) -> AgentToolResult {
    let (response, body) = result.into_parts();
    let cancelled = cancellation_requested.load(Ordering::SeqCst);
    if !body.is_empty() {
        return AgentToolResult::failed(
            format!("remote tool `{tool}` returned an unexpected attachment"),
            serde_json::json!({
                "error": "unexpected_remote_attachment",
                "tool": tool,
                "route": response.route,
                "cancelled": cancelled,
            }),
        );
    }
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
    use sinomo_runtime::network::{ObjectRpcIncomingBody, ObjectRpcResponse};
    use sinomo_runtime::node::NodeId;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    async fn materialize_test_value_call(call: ObjectRpcCall) -> Result<ObjectRpcRequest, String> {
        let streamed_value = call.has_streamed_value_body();
        let ObjectRpcCall { mut request, body } = call;
        if body.is_empty() {
            return Ok(request);
        }
        if !streamed_value {
            return Err(format!(
                "test route {} received an unexpected semantic attachment",
                request.route
            ));
        }
        request.body = body
            .collect_to_vec(usize::MAX)
            .await
            .map_err(|error| error.to_string())?;
        Ok(request)
    }

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
        fn call<'a>(&'a self, call: ObjectRpcCall) -> AgentRemoteObjectRpcFuture<'a> {
            Box::pin(async move {
                let request = materialize_test_value_call(call).await?;
                let response = match request.route.as_str() {
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
                        .map_err(|error| error.to_string())?
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
                            .map_err(|error| error.to_string())?
                        } else {
                            ObjectRpcResponse::encode_error(
                                "/remote/tool/cancel",
                                "operation id mismatch",
                            )
                            .map_err(|error| error.to_string())?
                        }
                    }
                    other => return Err(format!("unexpected route {other}")),
                };
                Ok(ObjectRpcResult::value(response))
            })
        }

        fn call_targeted<'a>(
            &'a self,
            target_node_id: Option<String>,
            route_key: Option<String>,
            call: ObjectRpcCall,
        ) -> AgentRemoteObjectRpcFuture<'a> {
            let target = Some((target_node_id, route_key));
            match call.request.route.as_str() {
                "/remote/tool/run" => {
                    *self.start_target.lock().expect("start target lock") = target;
                }
                "/remote/tool/cancel" => {
                    *self.cancel_target.lock().expect("cancel target lock") = target;
                }
                _ => {}
            }
            self.call(call)
        }
    }

    #[derive(Default)]
    struct FakeFileTransferTransport {
        remote: Mutex<Vec<u8>>,
        write_calls: AtomicUsize,
        read_calls: AtomicUsize,
        write_overwrites: Mutex<Vec<bool>>,
    }

    impl AgentRemoteObjectRpcTransport for FakeFileTransferTransport {
        fn call<'a>(&'a self, call: ObjectRpcCall) -> AgentRemoteObjectRpcFuture<'a> {
            Box::pin(async move {
                let route = call.request.route.clone();
                match route.as_str() {
                    REMOTE_MANAGEMENT_FS_WRITE_ROUTE => {
                        let ObjectRpcCall { request, body } = call;
                        let write: RemoteManagementFileWriteRequest =
                            request.decode_body().map_err(|error| error.to_string())?;
                        let declared_length = body
                            .total_bytes()
                            .ok_or_else(|| "test upload did not declare a length".to_string())?;
                        let collect_bytes = usize::try_from(declared_length)
                            .map_err(|_| "test upload does not fit this host".to_string())?;
                        let uploaded = body
                            .collect_to_vec(collect_bytes)
                            .await
                            .map_err(|error| error.to_string())?;
                        let uploaded_hash = blake3::hash(&uploaded).to_hex().to_string();
                        if write.expected_len != uploaded.len() as u64
                            || normalize_agent_blake3(&write.expected_blake3) != uploaded_hash
                        {
                            return Err(
                                "agent upload signed head did not match its attachment".to_string()
                            );
                        }
                        self.write_calls.fetch_add(1, Ordering::SeqCst);
                        self.write_overwrites
                            .lock()
                            .expect("overwrite lock")
                            .push(write.overwrite);
                        *self.remote.lock().expect("remote bytes lock") = uploaded;
                        let response = ObjectRpcResponse::encode_body(
                            REMOTE_MANAGEMENT_FS_WRITE_ROUTE,
                            &RemoteManagementFileWriteResponse::success(
                                RemoteManagementFileTransfer {
                                    path: write.path,
                                    length: declared_length,
                                    blake3: uploaded_hash,
                                },
                            ),
                        )
                        .map_err(|error| error.to_string())?;
                        Ok(ObjectRpcResult::value(response))
                    }
                    REMOTE_MANAGEMENT_FS_READ_ROUTE => {
                        let request = materialize_test_value_call(call).await?;
                        let read: RemoteManagementFileReadRequest =
                            request.decode_body().map_err(|error| error.to_string())?;
                        self.read_calls.fetch_add(1, Ordering::SeqCst);
                        let bytes = self.remote.lock().expect("remote bytes lock").clone();
                        let response = ObjectRpcResponse::encode_body(
                            REMOTE_MANAGEMENT_FS_READ_ROUTE,
                            &RemoteManagementFileReadResponse::success(
                                RemoteManagementFileTransfer {
                                    path: read.path,
                                    length: bytes.len() as u64,
                                    blake3: blake3::hash(&bytes).to_hex().to_string(),
                                },
                            ),
                        )
                        .map_err(|error| error.to_string())?;
                        Ok(ObjectRpcResult::new(
                            response,
                            ObjectRpcIncomingBody::bytes(bytes),
                        ))
                    }
                    route => Err(format!("unexpected transfer route {route}")),
                }
            })
        }

        fn call_targeted<'a>(
            &'a self,
            target_node_id: Option<String>,
            route_key: Option<String>,
            call: ObjectRpcCall,
        ) -> AgentRemoteObjectRpcFuture<'a> {
            assert_eq!(target_node_id.as_deref(), Some("node.files"));
            assert!(route_key.is_none());
            self.call(call)
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
    fn target_aware_closure_adapter_receives_both_routing_hints() {
        let received = Arc::new(Mutex::new(None));
        let received_for_transport = Arc::clone(&received);
        let transport = TargetAwareAgentRemoteObjectRpcTransport::new(
            move |target_node_id: Option<String>,
                  route_key: Option<String>,
                  call: ObjectRpcCall| {
                let received = Arc::clone(&received_for_transport);
                Box::pin(async move {
                    *received.lock().expect("received lock") =
                        Some((target_node_id, route_key, call.request.route));
                    let response = ObjectRpcResponse::encode_body(
                        "/remote/tool/immediate",
                        &serde_json::json!({ "ok": true }),
                    )
                    .map_err(|error| error.to_string())?;
                    Ok(ObjectRpcResult::value(response))
                }) as AgentRemoteObjectRpcFuture<'static>
            },
        );
        run_agent_remote_session(
            transport.call_targeted(
                Some("node.targeted".to_string()),
                Some("route.targeted".to_string()),
                ObjectRpcCall::value(
                    ObjectRpcRequest::encode_body(
                        "/remote/tool/immediate",
                        "remote.tool",
                        &serde_json::json!({}),
                    )
                    .expect("request"),
                ),
            ),
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
    fn agent_file_get_and_put_stream_more_than_sixteen_mib_in_one_session_each() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-remote-file-transfer");
        let source = root.join("source.bin");
        let destination = root.join("destination.bin");
        let bytes = (0..(17 * 1024 * 1024 + 37))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&source, &bytes).expect("write source");
        let transport = Arc::new(FakeFileTransferTransport::default());
        let registry = AgentToolRegistry::new();
        register_remote_management_agent_tools(
            &registry,
            Arc::clone(&transport) as Arc<dyn AgentRemoteObjectRpcTransport>,
        );

        let put = AgentToolCall {
            call_id: None,
            tool: "remote_file_put".to_string(),
            input: serde_json::json!({
                "target_node_id": "node.files",
                "input": {
                    "local_path": source,
                    "remote_path": "/remote/large.bin",
                    "overwrite": true
                }
            }),
            r#async: false,
            summary: "upload large file".to_string(),
        };
        let put_outcome = registry.execute(&test_session(), 0, 0, &put);
        assert_eq!(put_outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(transport.write_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *transport.write_overwrites.lock().expect("overwrite lock"),
            [true]
        );
        assert_eq!(*transport.remote.lock().expect("remote bytes lock"), bytes);

        let get = AgentToolCall {
            call_id: None,
            tool: "remote_file_get".to_string(),
            input: serde_json::json!({
                "target_node_id": "node.files",
                "input": {
                    "remote_path": "/remote/large.bin",
                    "local_path": destination
                }
            }),
            r#async: false,
            summary: "download large file".to_string(),
        };
        let get_outcome = registry.execute(&test_session(), 0, 0, &get);
        assert_eq!(get_outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(transport.read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            fs::read(root.join("destination.bin")).expect("download"),
            bytes
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

        let descriptors = remote_management_agent_tool_descriptors();
        for tool in ["remote_file_get", "remote_file_put"] {
            let descriptor = descriptors
                .iter()
                .find(|descriptor| descriptor.spec.name == tool)
                .expect("file transfer descriptor");
            let properties = &descriptor.input_schema["properties"]["input"]["properties"];
            assert!(properties.get("overwrite").is_some());
            for removed in ["bytes", "offset", "max_bytes", "append", "truncate"] {
                assert!(
                    properties.get(removed).is_none(),
                    "{tool} still exposes removed field {removed}"
                );
            }
        }
    }

    #[test]
    fn remote_object_rpc_tool_reports_rpc_error_metadata_as_failed() {
        let response = ObjectRpcResponse::encode_error(
            "/remote/tool/immediate",
            "remote management is not enabled on this node",
        )
        .expect("error response");

        let outcome = remote_rpc_tool_result(
            "remote_module_immediate",
            ObjectRpcResult::value(response),
            &AtomicBool::new(false),
        );

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

        let outcome = remote_rpc_tool_result(
            "remote_shell",
            ObjectRpcResult::value(response),
            &AtomicBool::new(false),
        );

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
                move |_target_node_id: Option<String>,
                      _route_key: Option<String>,
                      call: ObjectRpcCall| {
                    let received = Arc::clone(&received_for_transport);
                    Box::pin(async move {
                        let request: RemoteManagementSystemObserveRequest =
                            materialize_test_value_call(call)
                                .await?
                                .decode_body()
                                .map_err(|error| error.to_string())?;
                        *received.lock().expect("received lock") = Some(request);
                        let mut observation = sinomo_runtime::remote_management::RemoteManagementSystemObservation::default();
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
                        let response = ObjectRpcResponse::encode_body(
                            REMOTE_MANAGEMENT_SYSTEM_OBSERVE_ROUTE,
                            &RemoteManagementSystemObserveResponse::success(observation),
                        )
                        .map_err(|error| error.to_string())?;
                        Ok(ObjectRpcResult::value(response))
                    }) as AgentRemoteObjectRpcFuture<'static>
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
