use super::*;

#[derive(Clone)]
pub(super) struct RunCommandTool {
    config: LocalWorkspaceAgentConfig,
}

impl RunCommandTool {
    pub(super) fn new(config: LocalWorkspaceAgentConfig) -> Self {
        Self { config }
    }
}

impl AgentTool for RunCommandTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor_for_config(&self.config)
    }

    fn execute(&self, request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        local_run_command_from_call(
            &self.config,
            request.session,
            request.call,
            request.cancellation_token,
            request.progress_sink,
        )
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    permission_gated_descriptor()
}

pub(super) fn descriptor_for_config(_config: &LocalWorkspaceAgentConfig) -> AgentToolDescriptor {
    permission_gated_descriptor()
}

fn permission_gated_descriptor() -> AgentToolDescriptor {
    base_descriptor().requiring_local_permission(
        sinomo_runtime::node::LocalNodePermission::RemoteManagement.as_str(),
    )
}

fn base_descriptor() -> AgentToolDescriptor {
    descriptor_for_kind(AgentToolKind::RunShellCommand)
        .with_capabilities([AgentToolCapability::ExecuteProcess])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "anyOf":[{"required":["command"]},{"required":["action","execution_id"]}],
            "properties": {
                "action":{"type":"string","enum":["run","start","read","write","cancel"],"description":"Run waits for foreground exit. Start returns an interactive execution ID. Read polls output, write sends stdin, and cancel stops all remaining descendants."},
                "execution_id":{"type":"string"},
                "input":{"type":"string","description":"Exact stdin text, including any desired newline."},
                "close_stdin":{"type":"boolean"},
                "stdout_offset":{"type":"integer","minimum":0},
                "stderr_offset":{"type":"integer","minimum":0},
                "cleanup_on_exit":{"type":"boolean","description":"Terminate remaining descendants when the foreground program exits. Defaults to false."},
                "command": {
                    "type": "string",
                    "description": "Shell command to run inside simple-sandbox with generic host command semantics. It may read or write any location permitted to the Sinomo process; workspace file-tool permissions do not restrict shell commands. Avoid pipelines for verification unless the shell status still reflects the command that matters."
                },
                "cwd": {
                    "type": "string",
                    "description": "Optional command directory. Relative paths use the primary workspace; an absolute path may select any granted workspace. Omit this to run at the primary workspace root; do not use evidence artifact paths."
                },
                "timeout_ms": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional timeout in milliseconds."
                }
            }
        }))
}

pub(super) fn interact(
    config: &LocalWorkspaceAgentConfig,
    session: &InteractionSession,
    call: &AgentToolCall,
    action: &str,
) -> AgentToolResult {
    use sinomo_runtime::sandbox_simple::{
        SandboxNativeInteraction, interact_with_native_execution,
    };
    let failed = |detail: String| {
        AgentToolResult::failed(
            detail.clone(),
            serde_json::json!({"error":"sandbox_interaction_failed","detail":detail}),
        )
    };
    if config.command_runtime != LocalWorkspaceCommandRuntime::SimpleSandbox {
        return failed("Execution handles require the Simple Native sandbox".into());
    }
    let (root, _) = match resolve_workspace_cwd(config, input_string(call, "cwd").as_deref()) {
        Ok(v) => v,
        Err(e) => return failed(e),
    };
    let sandbox_id = agent_sandbox_id(config, &root);
    let node = requested_sandbox_runtime_node(config, session);
    if node != session.node_id {
        return failed("Sandbox is attached to another node".into());
    }
    let store = LocalSandboxInstanceStore::new(&config.sandbox_runtime_root);
    let (manifest, _) = match store.load(&sandbox_id) {
        Ok(v) => v,
        Err(e) => return failed(e.to_string()),
    };
    if let Err(result) = validate_sandbox_runtime_binding(&manifest, &node, config) {
        return result;
    }
    let id = match SandboxCommandId::new(input_string(call, "execution_id").unwrap_or_default()) {
        Ok(v) => v,
        Err(e) => return failed(e.to_string()),
    };
    let stdout = call
        .input
        .get("stdout_offset")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let stderr = call
        .input
        .get("stderr_offset")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let op = match action {
        "cancel" => SandboxNativeInteraction::Cancel,
        "write" => SandboxNativeInteraction::Write {
            bytes: call
                .input
                .get("input")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .as_bytes(),
            close: input_bool(call, "close_stdin").unwrap_or(false),
        },
        _ => SandboxNativeInteraction::Read,
    };
    match interact_with_native_execution(
        &store,
        &sandbox_id,
        &id,
        &node,
        &agent_sandbox_policy(&manifest),
        op,
        stdout,
        stderr,
        config.max_command_output_bytes,
    ) {
        Err(e) => failed(e.to_string()),
        Ok(view) => AgentToolResult::succeeded(
            "Sandbox execution status",
            serde_json::json!({"execution_id":id,"active":view.active,"exit_status":view.exit_status,"error":view.error,"stdout":String::from_utf8_lossy(&view.stdout),"stderr":String::from_utf8_lossy(&view.stderr),"stdout_offset":stdout.saturating_add(view.stdout.len() as u64),"stderr_offset":stderr.saturating_add(view.stderr.len() as u64)}),
        ),
    }
}
