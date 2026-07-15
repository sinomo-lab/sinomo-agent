use super::{
    AgentTool, AgentToolDescriptor, AgentToolEvidence, AgentToolOutcome, AgentToolProgressEvent,
    AgentToolProgressSink, AgentToolRequest, AgentToolResult, AgentToolStatus,
};
use crate::{AgentCancellationToken, AgentToolCall, InteractionSession, TaskId};
use sinomo_runtime::node::{LocalNodePermission, LocalNodePermissionStore};
use std::path::Path;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
    mpsc,
};
use std::time::Duration;

const AGENT_TOOL_GATEWAY_WAIT_POLL_MS: u64 = 50;

#[derive(Clone, Default)]
pub struct AgentToolRegistry {
    tools: Arc<RwLock<Vec<Arc<dyn AgentTool>>>>,
    /// Runtime resources whose lifetime must cover every tool registered in
    /// this registry (for example, a live filesystem mount). Resources are not
    /// tools and therefore never appear in model-visible descriptors.
    resources: Arc<RwLock<Vec<Arc<dyn Send + Sync>>>>,
    local_permission_checker: Arc<RwLock<Option<Arc<dyn AgentToolLocalPermissionChecker>>>>,
    generation: Arc<AtomicU64>,
}

/// Host-provided evaluator for node-local authority grants. The registry
/// parses descriptor tokens into the closed permission enum before invoking
/// this checker.
pub trait AgentToolLocalPermissionChecker: Send + Sync {
    fn permission_granted(&self, permission: LocalNodePermission) -> Result<bool, String>;
}

impl AgentToolLocalPermissionChecker for LocalNodePermissionStore {
    fn permission_granted(&self, permission: LocalNodePermission) -> Result<bool, String> {
        self.try_permission_granted(permission)
            .map_err(|error| error.to_string())
    }
}

impl<F> AgentToolLocalPermissionChecker for F
where
    F: Fn(LocalNodePermission) -> Result<bool, String> + Send + Sync,
{
    fn permission_granted(&self, permission: LocalNodePermission) -> Result<bool, String> {
        self(permission)
    }
}

impl AgentToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn descriptor_only(descriptors: impl IntoIterator<Item = AgentToolDescriptor>) -> Self {
        let registry = Self::new();
        for descriptor in descriptors {
            registry.register(DescriptorOnlyAgentTool { descriptor });
        }
        registry
    }

    pub fn local_workspace(root: impl Into<std::path::PathBuf>) -> Self {
        Self::local_workspace_with_config(super::LocalWorkspaceAgentConfig::new(root))
    }

    pub fn local_workspace_with_config(config: super::LocalWorkspaceAgentConfig) -> Self {
        super::local_workspace::registry(config)
    }

    pub fn config_management(config: super::ConfigAgentToolConfig) -> Self {
        super::config::registry(config)
    }

    pub fn with_config_management(self, config: super::ConfigAgentToolConfig) -> Self {
        super::config::register_tools(&self, config);
        self
    }

    pub fn with_tool<T: AgentTool + 'static>(self, tool: T) -> Self {
        self.register(tool);
        self
    }

    pub fn with_gateway<G: AgentToolGateway + 'static>(self, gateway: G) -> Self {
        self.register_gateway(gateway);
        self
    }

    pub fn with_local_permission_data_dir(self, data_dir: impl AsRef<Path>) -> Self {
        self.set_local_permission_data_dir(data_dir);
        self
    }

    pub fn set_local_permission_data_dir(&self, data_dir: impl AsRef<Path>) {
        self.set_local_permission_checker(LocalNodePermissionStore::new(data_dir));
    }

    pub fn with_local_permission_checker<C>(self, checker: C) -> Self
    where
        C: AgentToolLocalPermissionChecker + 'static,
    {
        self.set_local_permission_checker(checker);
        self
    }

    pub fn set_local_permission_checker<C>(&self, checker: C)
    where
        C: AgentToolLocalPermissionChecker + 'static,
    {
        *self
            .local_permission_checker
            .write()
            .expect("agent tool registry local permission checker lock poisoned") =
            Some(Arc::new(checker));
    }

    pub fn clear_local_permission_checker(&self) {
        *self
            .local_permission_checker
            .write()
            .expect("agent tool registry local permission checker lock poisoned") = None;
    }

    pub fn register<T: AgentTool + 'static>(&self, tool: T) -> Option<AgentToolDescriptor> {
        self.register_arc(Arc::new(tool))
    }

    pub fn register_gateway<G: AgentToolGateway + 'static>(
        &self,
        gateway: G,
    ) -> Option<AgentToolDescriptor> {
        self.register(AgentToolGatewayAdapter::new(gateway))
    }

    pub fn register_arc(&self, tool: Arc<dyn AgentTool>) -> Option<AgentToolDescriptor> {
        let descriptor = tool.descriptor();
        let name = descriptor.spec.name.clone();
        let mut tools = self
            .tools
            .write()
            .expect("agent tool registry lock poisoned");
        if let Some(index) = tools
            .iter()
            .position(|existing| existing.descriptor().spec.name == name)
        {
            let replaced = std::mem::replace(&mut tools[index], tool);
            self.bump_generation();
            return Some(replaced.descriptor());
        }
        tools.push(tool);
        self.bump_generation();
        None
    }

    /// Keep a shared runtime resource alive for as long as this registry (or
    /// any of its clones) remains configured.
    pub fn retain_resource<T>(&self, resource: Arc<T>)
    where
        T: Send + Sync + 'static,
    {
        self.resources
            .write()
            .expect("agent tool registry resource lock poisoned")
            .push(resource);
    }

    pub fn remove(&self, tool_name: &str) -> Option<AgentToolDescriptor> {
        let mut tools = self
            .tools
            .write()
            .expect("agent tool registry lock poisoned");
        let index = tools
            .iter()
            .position(|tool| tool.descriptor().spec.name == tool_name)?;
        let descriptor = tools.remove(index).descriptor();
        self.bump_generation();
        Some(descriptor)
    }

    pub fn clear(&self) {
        let mut tools = self
            .tools
            .write()
            .expect("agent tool registry lock poisoned");
        let mut resources = self
            .resources
            .write()
            .expect("agent tool registry resource lock poisoned");
        tools.clear();
        resources.clear();
        self.bump_generation();
    }

    pub fn contains(&self, tool_name: &str) -> bool {
        self.tools
            .read()
            .expect("agent tool registry lock poisoned")
            .iter()
            .any(|tool| tool.descriptor().spec.name == tool_name)
    }

    pub fn len(&self) -> usize {
        self.tools
            .read()
            .expect("agent tool registry lock poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn descriptors(&self) -> Vec<AgentToolDescriptor> {
        self.tools
            .read()
            .expect("agent tool registry lock poisoned")
            .iter()
            .map(|tool| tool.descriptor())
            .collect()
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    pub fn execute(
        &self,
        session: &InteractionSession,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
    ) -> AgentToolOutcome {
        self.execute_with_cancellation(
            session,
            step_index,
            call_index,
            call,
            AgentCancellationToken::new(),
        )
    }

    pub fn execute_with_cancellation(
        &self,
        session: &InteractionSession,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
        cancellation_token: AgentCancellationToken,
    ) -> AgentToolOutcome {
        self.execute_with_cancellation_and_progress(
            session,
            step_index,
            call_index,
            call,
            cancellation_token,
            AgentToolProgressSink::default(),
        )
    }

    pub fn execute_with_cancellation_and_progress(
        &self,
        session: &InteractionSession,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
        cancellation_token: AgentCancellationToken,
        progress_sink: AgentToolProgressSink,
    ) -> AgentToolOutcome {
        let call_id = TaskId::generate();
        if cancellation_token.is_cancelled() {
            return self.outcome_from_result(
                call_id,
                step_index,
                call_index,
                call,
                cancelled_before_tool_execution_result(call),
            );
        }
        // Acquire both snapshots under the same lock order as `clear`. A
        // reconfiguration can remove a tool while it is running, but must not
        // tear down the filesystem/session resources that invocation uses.
        let tools = self
            .tools
            .read()
            .expect("agent tool registry lock poisoned");
        let resources = self
            .resources
            .read()
            .expect("agent tool registry resource lock poisoned");
        let tool = tools
            .iter()
            .find(|tool| tool.descriptor().spec.name == call.tool)
            .cloned();
        let descriptor = tool.as_ref().map(|tool| tool.descriptor());
        let _resources = resources.clone();
        drop(resources);
        drop(tools);
        let result = tool
            .zip(descriptor)
            .map(|(tool, descriptor)| {
                if let Err(blocked) = self.authorize_local_permissions(&descriptor) {
                    return blocked;
                }
                tool.execute(AgentToolRequest {
                    call_id: &call_id,
                    session,
                    step_index,
                    call_index,
                    call,
                    cancellation_token,
                    progress_sink,
                })
            })
            .unwrap_or_else(|| {
                AgentToolResult::failed(
                    format!("unknown tool `{}`", call.tool),
                    serde_json::json!({
                        "error": "unknown_tool",
                        "tool": call.tool,
                    }),
                )
            });

        self.outcome_from_result(call_id, step_index, call_index, call, result)
    }

    fn authorize_local_permissions(
        &self,
        descriptor: &AgentToolDescriptor,
    ) -> Result<(), AgentToolResult> {
        if descriptor.required_local_permissions.is_empty() {
            return Ok(());
        }

        let mut permissions = Vec::with_capacity(descriptor.required_local_permissions.len());
        for token in &descriptor.required_local_permissions {
            match LocalNodePermission::try_from(token.as_str()) {
                Ok(permission) => permissions.push((token.as_str(), permission)),
                Err(error) => {
                    return Err(local_permission_blocked_result(
                        descriptor,
                        "unknown_local_permission",
                        token,
                        Some(error.to_string()),
                    ));
                }
            }
        }

        let checker = self
            .local_permission_checker
            .read()
            .expect("agent tool registry local permission checker lock poisoned")
            .clone();
        let Some(checker) = checker else {
            return Err(local_permission_blocked_result(
                descriptor,
                "local_permission_context_unconfigured",
                permissions[0].0,
                None,
            ));
        };

        for (token, permission) in permissions {
            match checker.permission_granted(permission) {
                Ok(true) => {}
                Ok(false) => {
                    return Err(local_permission_blocked_result(
                        descriptor,
                        "local_permission_denied",
                        token,
                        None,
                    ));
                }
                Err(error) => {
                    return Err(local_permission_blocked_result(
                        descriptor,
                        "local_permission_check_failed",
                        token,
                        Some(error),
                    ));
                }
            }
        }
        Ok(())
    }

    fn outcome_from_result(
        &self,
        call_id: TaskId,
        step_index: usize,
        call_index: usize,
        call: &AgentToolCall,
        result: AgentToolResult,
    ) -> AgentToolOutcome {
        let mut evidence = result.evidence.unwrap_or_else(|| {
            AgentToolEvidence::availability_only(
                result.status != AgentToolStatus::Blocked,
                "executed through registered agent tool registry",
            )
            .with_output_value(&result.output, false)
        });
        if evidence.output.model_visible_bytes == 0 && !result.output.is_null() {
            let truncated = evidence.output.truncated;
            evidence = evidence.with_output_value(&result.output, truncated);
        }

        AgentToolOutcome {
            call_id,
            step_index,
            call_index,
            call: call.clone(),
            status: result.status,
            summary: result.summary,
            output: result.output,
            event_id: None,
            evidence,
        }
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
    }
}

fn local_permission_blocked_result(
    descriptor: &AgentToolDescriptor,
    error: &'static str,
    permission: &str,
    detail: Option<String>,
) -> AgentToolResult {
    let mut output = serde_json::json!({
        "error": error,
        "tool": descriptor.spec.name,
        "required_local_permission": permission,
        "required_local_permissions": descriptor.required_local_permissions,
    });
    if let Some(detail) = detail {
        output["detail"] = serde_json::Value::String(detail);
    }
    let summary = format!(
        "tool `{}` blocked by node-local permission `{permission}`",
        descriptor.spec.name
    );
    AgentToolResult::blocked(summary.clone(), output)
        .with_evidence(AgentToolEvidence::availability_only(false, summary))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentToolGatewayCancellation {
    pub accepted: bool,
    pub reason: String,
}

impl AgentToolGatewayCancellation {
    pub fn accepted(reason: impl Into<String>) -> Self {
        Self {
            accepted: true,
            reason: reason.into(),
        }
    }

    pub fn rejected(reason: impl Into<String>) -> Self {
        Self {
            accepted: false,
            reason: reason.into(),
        }
    }
}

pub struct AgentToolGatewayTask {
    cancel: Arc<dyn Fn() -> AgentToolGatewayCancellation + Send + Sync>,
    wait: Box<dyn FnOnce() -> AgentToolResult + Send>,
}

impl AgentToolGatewayTask {
    pub fn new(
        wait: impl FnOnce() -> AgentToolResult + Send + 'static,
        cancel: impl Fn() -> AgentToolGatewayCancellation + Send + Sync + 'static,
    ) -> Self {
        Self {
            cancel: Arc::new(cancel),
            wait: Box::new(wait),
        }
    }

    fn cancellation_handle(&self) -> Arc<dyn Fn() -> AgentToolGatewayCancellation + Send + Sync> {
        Arc::clone(&self.cancel)
    }

    fn wait(self) -> AgentToolResult {
        (self.wait)()
    }
}

pub trait AgentToolGateway: Send + Sync {
    fn descriptor(&self) -> AgentToolDescriptor;

    fn start(
        &self,
        request: &AgentToolRequest<'_>,
    ) -> Result<AgentToolGatewayTask, AgentToolResult>;
}

pub struct AgentToolGatewayAdapter<G> {
    gateway: G,
    poll_interval: Duration,
}

impl<G> AgentToolGatewayAdapter<G> {
    pub fn new(gateway: G) -> Self {
        Self {
            gateway,
            poll_interval: Duration::from_millis(AGENT_TOOL_GATEWAY_WAIT_POLL_MS),
        }
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }
}

impl<G> AgentTool for AgentToolGatewayAdapter<G>
where
    G: AgentToolGateway + 'static,
{
    fn descriptor(&self) -> AgentToolDescriptor {
        self.gateway.descriptor()
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        execute_gateway_tool_task(&self.gateway, request, self.poll_interval)
    }
}

fn execute_gateway_tool_task(
    gateway: &dyn AgentToolGateway,
    request: AgentToolRequest<'_>,
    poll_interval: Duration,
) -> AgentToolResult {
    if request.cancellation_token.is_cancelled() {
        return cancelled_before_tool_execution_result(request.call);
    }
    let task = match gateway.start(&request) {
        Ok(task) => task,
        Err(result) => return result,
    };
    request.progress_sink.emit(AgentToolProgressEvent::Message {
        summary: "gateway task started".to_string(),
        payload: serde_json::json!({
            "tool": request.call.tool,
            "call_id": request.call_id,
        }),
    });

    let cancel = task.cancellation_handle();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(task.wait());
    });

    let mut cancellation: Option<AgentToolGatewayCancellation> = None;
    loop {
        if request.cancellation_token.is_cancelled() && cancellation.is_none() {
            let decision = cancel();
            request.progress_sink.emit(AgentToolProgressEvent::Message {
                summary: "gateway task cancellation requested".to_string(),
                payload: serde_json::json!({
                    "tool": request.call.tool,
                    "accepted": decision.accepted,
                    "reason": decision.reason,
                }),
            });
            cancellation = Some(decision);
        }

        match rx.recv_timeout(poll_interval) {
            Ok(result) => return result,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let payload = serde_json::json!({
                    "error": "gateway_task_disconnected",
                    "tool": request.call.tool,
                    "cancellation": cancellation.map(|decision| {
                        serde_json::json!({
                            "accepted": decision.accepted,
                            "reason": decision.reason,
                        })
                    }),
                });
                return AgentToolResult::failed(
                    format!("gateway task for `{}` disconnected", request.call.tool),
                    payload,
                );
            }
        }
    }
}

fn cancelled_before_tool_execution_result(call: &AgentToolCall) -> AgentToolResult {
    let payload = serde_json::json!({
        "error": "tool_cancelled",
        "tool": call.tool,
        "reason": "turn cancellation requested before tool execution",
    });
    AgentToolResult::cancelled(format!("tool `{}` cancelled", call.tool), payload.clone())
        .with_evidence(
            AgentToolEvidence::availability_only(
                false,
                "turn cancellation requested before tool execution",
            )
            .with_output_value(&payload, false),
        )
}

struct DescriptorOnlyAgentTool {
    descriptor: AgentToolDescriptor,
}

impl AgentTool for DescriptorOnlyAgentTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        self.descriptor.clone()
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        AgentToolResult::blocked(
            format!(
                "tool `{}` is described but no executor is attached to this agent",
                request.call.tool
            ),
            serde_json::json!({
                "error": "missing_executor",
                "tool": request.call.tool,
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentToolCapability, AgentToolKind, AgentToolSpec, InteractionSurface,
        InteractionSurfaceKind,
    };
    use sinomo_runtime::events::SessionId;
    use sinomo_runtime::node::{
        LocalNodePermission, LocalNodePermissionStore, LocalNodePermissions, NodeId,
    };
    use std::fs;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    struct FakeGateway {
        started: Arc<AtomicBool>,
        cancelled: Arc<AtomicBool>,
        cancel_calls: Arc<AtomicUsize>,
    }

    impl AgentToolGateway for FakeGateway {
        fn descriptor(&self) -> AgentToolDescriptor {
            AgentToolDescriptor::new(AgentToolSpec::new(
                AgentToolKind::ModuleObjectRpc,
                "gateway_probe",
                "fake host-provided gateway probe",
                false,
            ))
            .with_capabilities([AgentToolCapability::CallModule])
        }

        fn start(
            &self,
            _request: &AgentToolRequest<'_>,
        ) -> Result<AgentToolGatewayTask, AgentToolResult> {
            self.started.store(true, Ordering::SeqCst);
            let cancelled_for_wait = Arc::clone(&self.cancelled);
            let cancelled_for_cancel = Arc::clone(&self.cancelled);
            let cancel_calls = Arc::clone(&self.cancel_calls);
            Ok(AgentToolGatewayTask::new(
                move || {
                    while !cancelled_for_wait.load(Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    AgentToolResult::cancelled(
                        "gateway task cancelled",
                        serde_json::json!({
                            "cancelled": true,
                            "source": "fake_gateway",
                        }),
                    )
                },
                move || {
                    cancel_calls.fetch_add(1, Ordering::SeqCst);
                    cancelled_for_cancel.store(true, Ordering::SeqCst);
                    AgentToolGatewayCancellation::accepted("fake gateway cancellation accepted")
                },
            ))
        }
    }

    fn test_session() -> InteractionSession {
        InteractionSession::new(
            SessionId::from_legacy_named("agent_gateway_cancel_session").expect("session"),
            NodeId::from_legacy_named("agent_gateway_cancel_node").expect("node"),
            InteractionSurface::new("agent.gateway.test", InteractionSurfaceKind::CommandLine),
        )
    }

    struct PermissionProbeTool {
        descriptor: AgentToolDescriptor,
        executions: Arc<AtomicUsize>,
    }

    impl AgentTool for PermissionProbeTool {
        fn descriptor(&self) -> AgentToolDescriptor {
            self.descriptor.clone()
        }

        fn execute(&self, _request: AgentToolRequest<'_>) -> AgentToolResult {
            self.executions.fetch_add(1, Ordering::SeqCst);
            AgentToolResult::succeeded("permission probe executed", serde_json::json!({}))
        }
    }

    fn permission_probe_descriptor(
        required_local_permissions: impl IntoIterator<Item = impl Into<String>>,
    ) -> AgentToolDescriptor {
        AgentToolDescriptor::new(AgentToolSpec::new(
            AgentToolKind::ModuleObjectRpc,
            "permission_probe",
            "node-local permission enforcement probe",
            false,
        ))
        .with_required_local_permissions(required_local_permissions)
    }

    fn permission_probe_call() -> AgentToolCall {
        AgentToolCall {
            call_id: None,
            tool: "permission_probe".to_string(),
            input: serde_json::json!({}),
            r#async: false,
            summary: "check local permission".to_string(),
        }
    }

    #[test]
    fn descriptor_local_permissions_are_backward_compatible_and_buildable() {
        let descriptor = permission_probe_descriptor(["remote-management"]);
        assert_eq!(
            descriptor.required_local_permissions,
            vec!["remote-management"]
        );
        let mut encoded = serde_json::to_value(&descriptor).expect("encode descriptor");
        encoded
            .as_object_mut()
            .expect("descriptor object")
            .remove("required_local_permissions");
        let legacy: AgentToolDescriptor =
            serde_json::from_value(encoded).expect("decode legacy descriptor");
        assert!(legacy.required_local_permissions.is_empty());
    }

    #[test]
    fn final_boundary_blocks_without_context_and_on_unknown_permission() {
        let executions = Arc::new(AtomicUsize::new(0));
        let registry = AgentToolRegistry::new().with_tool(PermissionProbeTool {
            descriptor: permission_probe_descriptor(["remote-management"]),
            executions: Arc::clone(&executions),
        });
        let without_context = registry.execute(&test_session(), 0, 0, &permission_probe_call());
        assert_eq!(without_context.status, AgentToolStatus::Blocked);
        assert_eq!(
            without_context.output["error"],
            "local_permission_context_unconfigured"
        );
        assert_eq!(executions.load(Ordering::SeqCst), 0);

        let unknown_registry = AgentToolRegistry::new()
            .with_local_permission_checker(|_: LocalNodePermission| Ok(true))
            .with_tool(PermissionProbeTool {
                descriptor: permission_probe_descriptor(["future-unbounded-host-control"]),
                executions: Arc::clone(&executions),
            });
        let unknown = unknown_registry.execute(&test_session(), 0, 0, &permission_probe_call());
        assert_eq!(unknown.status, AgentToolStatus::Blocked);
        assert_eq!(unknown.output["error"], "unknown_local_permission");
        assert_eq!(executions.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn node_permission_store_missing_corrupt_and_granted_states_gate_execution() {
        let root = sinomo_testdir::TestDir::new("sinomo-agent-local-permission-gate");
        let store = LocalNodePermissionStore::new(&root);
        let executions = Arc::new(AtomicUsize::new(0));
        let registry = AgentToolRegistry::new()
            .with_local_permission_data_dir(&root)
            .with_tool(PermissionProbeTool {
                descriptor: permission_probe_descriptor(["remote-management"]),
                executions: Arc::clone(&executions),
            });

        let missing = registry.execute(&test_session(), 0, 0, &permission_probe_call());
        assert_eq!(missing.status, AgentToolStatus::Blocked);
        assert_eq!(missing.output["error"], "local_permission_denied");
        assert_eq!(executions.load(Ordering::SeqCst), 0);

        fs::create_dir_all(store.path().parent().expect("permission parent"))
            .expect("create permission parent");
        fs::write(store.path(), b"corrupt permission state").expect("write corrupt state");
        let corrupt = registry.execute(&test_session(), 0, 1, &permission_probe_call());
        assert_eq!(corrupt.status, AgentToolStatus::Blocked);
        assert_eq!(corrupt.output["error"], "local_permission_check_failed");
        assert_eq!(executions.load(Ordering::SeqCst), 0);

        let mut permissions = LocalNodePermissions::new();
        permissions.set_remote_management_enabled(true);
        store.save(&permissions).expect("enable remote management");
        let granted = registry.execute(&test_session(), 0, 2, &permission_probe_call());
        assert_eq!(granted.status, AgentToolStatus::Succeeded);
        assert_eq!(executions.load(Ordering::SeqCst), 1);

        registry.clear_local_permission_checker();
        let cleared = registry.execute(&test_session(), 0, 3, &permission_probe_call());
        assert_eq!(cleared.status, AgentToolStatus::Blocked);
        assert_eq!(
            cleared.output["error"],
            "local_permission_context_unconfigured"
        );
        assert_eq!(executions.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn tools_without_local_permissions_execute_without_a_permission_context() {
        let executions = Arc::new(AtomicUsize::new(0));
        let registry = AgentToolRegistry::new().with_tool(PermissionProbeTool {
            descriptor: permission_probe_descriptor(Vec::<String>::new()),
            executions: Arc::clone(&executions),
        });

        let outcome = registry.execute(&test_session(), 0, 0, &permission_probe_call());
        assert_eq!(outcome.status, AgentToolStatus::Succeeded);
        assert_eq!(executions.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn retained_runtime_resource_follows_registry_lifetime() {
        struct DropProbe(Arc<AtomicBool>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let registry = AgentToolRegistry::new();
        registry.retain_resource(Arc::new(DropProbe(Arc::clone(&dropped))));
        let clone = registry.clone();
        drop(registry);
        assert!(!dropped.load(Ordering::SeqCst));

        clone.clear();
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn clearing_registry_keeps_resources_alive_for_running_tool() {
        struct DropProbe(Arc<AtomicBool>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        struct BlockingTool {
            started: std::sync::mpsc::Sender<()>,
            release: Mutex<std::sync::mpsc::Receiver<()>>,
        }
        impl AgentTool for BlockingTool {
            fn descriptor(&self) -> AgentToolDescriptor {
                AgentToolDescriptor::new(AgentToolSpec::new(
                    AgentToolKind::ModuleObjectRpc,
                    "blocking_probe",
                    "test runtime-resource lifetime",
                    false,
                ))
            }

            fn execute(&self, _request: AgentToolRequest<'_>) -> AgentToolResult {
                self.started.send(()).expect("signal started");
                self.release
                    .lock()
                    .expect("release lock")
                    .recv()
                    .expect("release tool");
                AgentToolResult::succeeded("finished", serde_json::json!({}))
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let registry = AgentToolRegistry::new();
        registry.retain_resource(Arc::new(DropProbe(Arc::clone(&dropped))));
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        registry.register(BlockingTool {
            started: started_tx,
            release: Mutex::new(release_rx),
        });
        let executing_registry = registry.clone();
        let execution = std::thread::spawn(move || {
            executing_registry.execute(
                &test_session(),
                0,
                0,
                &AgentToolCall {
                    call_id: None,
                    tool: "blocking_probe".to_string(),
                    input: serde_json::json!({}),
                    r#async: false,
                    summary: "block".to_string(),
                },
            )
        });
        started_rx.recv().expect("tool started");

        registry.clear();
        assert!(!dropped.load(Ordering::SeqCst));
        release_tx.send(()).expect("release tool");
        execution.join().expect("tool join");
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn gateway_adapter_cancels_active_host_task() {
        let started = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let registry = AgentToolRegistry::new().with_gateway(FakeGateway {
            started: Arc::clone(&started),
            cancelled: Arc::clone(&cancelled),
            cancel_calls: Arc::clone(&cancel_calls),
        });
        let cancellation_token = AgentCancellationToken::new();
        let token_for_sink = cancellation_token.clone();
        let progress = Arc::new(Mutex::new(Vec::new()));
        let progress_for_sink = Arc::clone(&progress);
        let progress_sink = AgentToolProgressSink::new(move |event| {
            if let AgentToolProgressEvent::Message { summary, .. } = event {
                if summary == "gateway task started" {
                    token_for_sink.cancel();
                }
                progress_for_sink
                    .lock()
                    .expect("progress lock")
                    .push(summary);
            }
        });
        let call = AgentToolCall {
            call_id: None,
            tool: "gateway_probe".to_string(),
            input: serde_json::json!({}),
            r#async: false,
            summary: "cancel gateway probe".to_string(),
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
        assert!(started.load(Ordering::SeqCst));
        assert!(cancelled.load(Ordering::SeqCst));
        assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            outcome
                .output
                .get("cancelled")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        let progress = progress.lock().expect("progress lock");
        assert!(
            progress
                .iter()
                .any(|summary| summary == "gateway task started")
        );
        assert!(
            progress
                .iter()
                .any(|summary| summary == "gateway task cancellation requested")
        );
    }
}
