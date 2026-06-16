use super::{
    AgentToolCapability, AgentToolDescriptor, AgentToolGateway, AgentToolGatewayCancellation,
    AgentToolGatewayTask, AgentToolRequest, AgentToolResult,
};
use crate::{AgentToolKind, AgentToolSpec};
use sinomo_runtime::network::{ObjectRpcCancelRequest, ObjectRpcRequest, ObjectRpcResponse};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub trait AgentRemoteObjectRpcTransport: Send + Sync {
    fn call(&self, request: ObjectRpcRequest) -> Result<ObjectRpcResponse, String>;
}

impl<F> AgentRemoteObjectRpcTransport for F
where
    F: Fn(ObjectRpcRequest) -> Result<ObjectRpcResponse, String> + Send + Sync,
{
    fn call(&self, request: ObjectRpcRequest) -> Result<ObjectRpcResponse, String> {
        self(request)
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
                "input": {
                    "description": "Route-specific object-RPC request payload."
                }
            }
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

impl AgentToolGateway for AgentRemoteObjectRpcTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        self.descriptor.clone()
    }

    fn start(
        &self,
        request: &AgentToolRequest<'_>,
    ) -> Result<AgentToolGatewayTask, AgentToolResult> {
        let operation_id = request.call_id.as_str().to_string();
        let payload = request
            .call
            .input
            .get("input")
            .cloned()
            .unwrap_or_else(|| request.call.input.clone());
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
        let cancellation_requested = Arc::new(AtomicBool::new(false));
        let cancellation_for_wait = Arc::clone(&cancellation_requested);
        let cancellation_for_cancel = Arc::clone(&cancellation_requested);
        Ok(AgentToolGatewayTask::new(
            move || match start_transport.call(rpc) {
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
                match cancel_transport.call(cancel) {
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

fn remote_rpc_tool_result(
    tool: &str,
    response: ObjectRpcResponse,
    cancellation_requested: &AtomicBool,
) -> AgentToolResult {
    match response.decode_body::<serde_json::Value>() {
        Ok(value) => {
            let payload = serde_json::json!({
                "response": value,
                "route": response.route,
                "metadata": response.metadata,
                "cancelled": cancellation_requested.load(Ordering::SeqCst),
            });
            if cancellation_requested.load(Ordering::SeqCst) {
                AgentToolResult::cancelled(format!("remote tool `{tool}` cancelled"), payload)
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
}
